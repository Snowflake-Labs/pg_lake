/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"

#include "access/tupdesc.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "common/hashfn.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/multi_data_file_dest.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/partitioning/partitioned_dest_receiver.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/storage/local_storage.h"
#include "foreign/foreign.h"
#include "storage/fd.h"
#include "tcop/dest.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"

/*
 * Headroom we leave below max_safe_fds when computing how many partition
 * subreceivers we can keep open at once. PostgreSQL's AllocateDesc machinery
 * caps the total number of allocated descriptors at max_safe_fds, and the
 * backend uses some of those for catalog access, temp files, etc. We avoid
 * approaching that cap so we don't trigger "exceeded maxAllocatedDescs"
 * errors mid-write. Each active subreceiver currently holds one CSV file
 * descriptor open via AllocateFile().
 */
#define PARTITIONED_DEST_FD_SAFETY_MARGIN 64


typedef struct PartitionPartitionDestReceiverHashEntry
{
	uint64		hashKey;

	/*
	 * We don't need to know exact definition, all we are interested in is
	 * that this is a DestReceiver.
	 */
	DestReceiver *multiDataFileDestReceiver;
	int			rowCount;
	Partition  *partition;
}			PartitionPartitionDestReceiverHashEntry;

/*
 * Our custom PartitioningDestReceiver structure.
 * We don't know the number of partitions in advance, so we'll store a hash
 * from "partition hash" -> (MultiDataFileDestReceiver *), created lazily.
 */
typedef struct PartitioningDestReceiverData
{
	DestReceiver pub;

	List	   *partitionTransformList; /* List of IcebergPartitionTransform */
	HTAB	   *partitionsHash; /* Maps partitionKey ->
								 * MultiDataFileDestReceiver* */
	MemoryContext parentContext;
	MemoryContext perRowContext;

	/*
	 * List of modifications that have been flushed (e.g., when we reached
	 * MaxOpenFilesForPartitionedWrite).
	 */
	List	   *alreadyFlushedPartitionModifications;

	/* relation to which we are writing */
	Oid			relationId;

	/* the relation's partition spec id */
	int32		currentPartitionSpecId;

	/* target format of the DestReceiver */
	CopyDataFormat targetFormat;

	/* tuple descriptor of the received slots */
	TupleDesc	tupleDesc;

	/* operation of the DestReceiver */
	int			operation;
}			PartitioningDestReceiverData;


static void StartPartitionedDestReceiver(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool PartitionedDestReceiveSlot(TupleTableSlot *slot, DestReceiver *self);
static void ShutdownPartitionedDestReceiver(DestReceiver *self);
static void DestroyPartitionedDestReceiver(DestReceiver *self);

static HTAB *InitializePartitionsHash(MemoryContext parentContext);
static void AssignPartitionForModificationList(List *modifications, int32 partitionSpecId, Partition * partition);
static List *FlushLargestPartitionedDestReceivers(PartitioningDestReceiverData * myState, int maxToFlush);

/*
 * Maximum number of subreceivers FlushLargestPartitionedDestReceivers will
 * flush in a single pass. Picked so we can absorb a sudden drop in the
 * available fd budget without making many full hash scans, while keeping
 * the top-K insertion-sort cheap.
 */
#define PARTITIONED_DEST_MAX_FLUSH_BATCH 5

/* controlled by a GUC */
int			MaxOpenFilesForPartitionedWrite = 5000;


/*
* CreatePartitionedDestReceiver creates a new PartitionedDestReceiver.
*/
DestReceiver *
CreatePartitionedDestReceiver(Oid relationId,
							  CopyDataFormat targetFormat,
							  int32 partitionSpecId)
{
	PartitioningDestReceiverData *self =
		(PartitioningDestReceiverData *) palloc0(sizeof(PartitioningDestReceiverData));

	self->pub.rStartup = StartPartitionedDestReceiver;
	self->pub.receiveSlot = PartitionedDestReceiveSlot;
	self->pub.rShutdown = ShutdownPartitionedDestReceiver;
	self->pub.rDestroy = DestroyPartitionedDestReceiver;
	self->pub.mydest = DestCopyOut;

	/*
	 * We are writing partitions based on the current partition spec, we don't
	 * need the old specs.
	 */
	self->currentPartitionSpecId = partitionSpecId;
	self->partitionTransformList = CurrentPartitionTransformList(relationId);

	self->relationId = relationId;
	self->targetFormat = targetFormat;
	self->alreadyFlushedPartitionModifications = NIL;

	/*
	 * We return modifications to the upper context, so better allocate
	 * everything on that context.
	 */
	self->parentContext = CurrentMemoryContext;
	self->partitionsHash = InitializePartitionsHash(self->parentContext);

	self->perRowContext = AllocSetContextCreate(self->parentContext,
												"PartitionedDestReceiver per row context",
												ALLOCSET_DEFAULT_SIZES);


	return (DestReceiver *) self;
}

/*
 * GetSplitFileUploadDestReceiverModifications returns the modifications generated
 * by this DestReceiver.
 */
List *
GetPartitionedDestReceiverModifications(DestReceiver *dest)
{
	List	   *allModifications = NIL;

	PartitioningDestReceiverData *myState = (PartitioningDestReceiverData *) dest;
	HASH_SEQ_STATUS seqStatus;
	PartitionPartitionDestReceiverHashEntry *ent;

	hash_seq_init(&seqStatus, myState->partitionsHash);
	while ((ent = (PartitionPartitionDestReceiverHashEntry *) hash_seq_search(&seqStatus)) != NULL)
	{
		List	   *modifications =
			GetMultiDataFileDestReceiverModifications((DestReceiver *) ent->multiDataFileDestReceiver);

		AssignPartitionForModificationList(modifications, myState->currentPartitionSpecId, ent->partition);

		allModifications = list_concat(allModifications, modifications);
	}

	return list_concat(myState->alreadyFlushedPartitionModifications, allModifications);
}


static void
AssignPartitionForModificationList(List *modifications, int32 partitionSpecId, Partition * partition)
{
	ListCell   *cell;

	foreach(cell, modifications)
	{
		DataFileModification *modification = lfirst(cell);

		if (modification->type == ADD_DATA_FILE)
		{
			modification->partitionSpecId = partitionSpecId;
			modification->partition = partition;
		}
	}
}


/*
 * StartPartitionedDestReceiver is called when the CreatePartitionedDestReceiver starts.
 */
static void
StartPartitionedDestReceiver(DestReceiver *dest, int operation, TupleDesc tupleDesc)
{
	PartitioningDestReceiverData *self = (PartitioningDestReceiverData *) dest;

	self->operation = operation;
	self->tupleDesc = tupleDesc;
}

/*
 * PartitionedDestReceiveSlot: compute the partition hash, find or create subreceiver,
 * forward the slot.
 */
static bool
PartitionedDestReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
	PartitioningDestReceiverData *myState = (PartitioningDestReceiverData *) self;

	MemoryContext callerContext = MemoryContextSwitchTo(myState->perRowContext);

	Partition  *partition =
		ComputePartitionTupleForTuple(myState->partitionTransformList, slot);
	uint64		partitionHash = ComputePartitionKey(partition);

	/* Lookup or create subreceiver in the hashtable */
	bool		found;
	PartitionPartitionDestReceiverHashEntry *entryPtr =
		hash_search(myState->partitionsHash, &partitionHash,
					HASH_ENTER, &found);

	if (!found)
	{
		/* Create a new subreceiver, plus run its startup. */
		MemoryContext oldcxt = MemoryContextSwitchTo(myState->parentContext);

		/* allocate partition in myState->parentContext */
		entryPtr->partition = CopyPartition(partition);
		entryPtr->rowCount = 0;

		/*
		 * Check whether we have room to open another subreceiver, both
		 * against the user-tunable cap (MaxOpenFilesForPartitionedWrite) and
		 * against the backend's allocated-descriptor budget. Each subreceiver
		 * currently holds one descriptor open, so we can use subreceiver
		 * count as a proxy for fds consumed by this path. We keep flushing
		 * the largest subreceiver in a loop so we still make room even when
		 * many other subsystems have eaten into the budget.
		 *
		 * The downside is that we push data files that have not reached
		 * MaxWriteTempFileSizeMB yet, so we may emit small files. That is
		 * preferable to erroring out with "exceeded maxAllocatedDescs".
		 */
		int			fdBudget = max_safe_fds - PARTITIONED_DEST_FD_SAFETY_MARGIN;

		if (fdBudget < 1)
			fdBudget = 1;

		int			cap = Min(MaxOpenFilesForPartitionedWrite, fdBudget);
		int			over = hash_get_num_entries(myState->partitionsHash) - cap;

		while (over > 0)
		{
			int			batch = Min(over, PARTITIONED_DEST_MAX_FLUSH_BATCH);
			List	   *modifications =
				FlushLargestPartitionedDestReceivers(myState, batch);

			if (modifications == NIL)
				break;

			myState->alreadyFlushedPartitionModifications =
				list_concat(myState->alreadyFlushedPartitionModifications, modifications);

			over = hash_get_num_entries(myState->partitionsHash) - cap;
		}

		DestReceiver *partitionReceiver =
			CreateMultiDataFileDestReceiver(myState->relationId,
											myState->targetFormat,
											MaxWriteTempFileSizeMB,
											myState->currentPartitionSpecId,
											0);

		entryPtr->multiDataFileDestReceiver = partitionReceiver;
		partitionReceiver->rStartup((DestReceiver *) entryPtr->multiDataFileDestReceiver, myState->operation, myState->tupleDesc);

		MemoryContextSwitchTo(oldcxt);
	}

	/* Increment the row count for this partition */
	entryPtr->rowCount++;

	/* Switch back to the caller context context, and delete per-row context */
	MemoryContextSwitchTo(callerContext);
	MemoryContextReset(myState->perRowContext);

	/* Now pass the tuple to that subreceiver. */
	return entryPtr->multiDataFileDestReceiver->receiveSlot(slot, (DestReceiver *) entryPtr->multiDataFileDestReceiver);
}



/*
 * FlushLargestPartitionedDestReceivers flushes up to maxToFlush of the
 * largest partitioned dest receivers in a single pass over the hash. We
 * maintain a top-K array sorted in descending row count by insertion sort
 * (cheap for the small K we expect, see PARTITIONED_DEST_MAX_FLUSH_BATCH),
 * then flush the collected entries afterwards. This is used to limit the
 * number of active subreceivers so we don't exhaust the backend's
 * allocated-descriptor budget or MaxOpenFilesForPartitionedWrite.
 */
static List *
FlushLargestPartitionedDestReceivers(PartitioningDestReceiverData * myState, int maxToFlush)
{
	HTAB	   *partitionsHash = myState->partitionsHash;
	HASH_SEQ_STATUS seqStatus;
	PartitionPartitionDestReceiverHashEntry *ent;
	PartitionPartitionDestReceiverHashEntry *topK[PARTITIONED_DEST_MAX_FLUSH_BATCH];
	int			topKCount = 0;

	if (maxToFlush <= 0)
		return NIL;
	if (maxToFlush > PARTITIONED_DEST_MAX_FLUSH_BATCH)
		maxToFlush = PARTITIONED_DEST_MAX_FLUSH_BATCH;

	hash_seq_init(&seqStatus, partitionsHash);
	while ((ent = (PartitionPartitionDestReceiverHashEntry *) hash_seq_search(&seqStatus)) != NULL)
	{
		/*
		 * Skip already-flushed entries (the hash entry is kept around as a
		 * placeholder until rDestroy time, but its multiDataFileDestReceiver
		 * is NULL after a prior flush).
		 */
		if (ent->multiDataFileDestReceiver == NULL)
			continue;

		/* If full and this entry isn't larger than our smallest, skip it. */
		if (topKCount == maxToFlush &&
			ent->rowCount <= topK[topKCount - 1]->rowCount)
			continue;

		/* Find insertion position to keep topK[] sorted descending. */
		int			pos = topKCount;

		while (pos > 0 && topK[pos - 1]->rowCount < ent->rowCount)
			pos--;

		/* Shift smaller elements right; drop the tail if we're at capacity. */
		int			end = (topKCount < maxToFlush) ? topKCount : maxToFlush - 1;

		for (int i = end; i > pos; i--)
			topK[i] = topK[i - 1];

		topK[pos] = ent;
		if (topKCount < maxToFlush)
			topKCount++;
	}

	List	   *allModifications = NIL;

	for (int i = 0; i < topKCount; i++)
	{
		PartitionPartitionDestReceiverHashEntry *target = topK[i];

		target->multiDataFileDestReceiver->rShutdown((DestReceiver *) target->multiDataFileDestReceiver);

		List	   *modifications =
			GetMultiDataFileDestReceiverModifications((DestReceiver *) target->multiDataFileDestReceiver);

		AssignPartitionForModificationList(modifications, myState->currentPartitionSpecId, target->partition);

		/* also remove from the hash */
		bool		found = false;
		PartitionPartitionDestReceiverHashEntry *entryPtr =
			hash_search(partitionsHash, &target->hashKey, HASH_REMOVE, &found);

		/* indicates a bug, not expected, better than assert */
		if (!found)
			elog(ERROR, "could not find partitioned dest receiver in hash");

		/* indicates a bug, not expected, better than assert */
		if (entryPtr->hashKey != target->hashKey)
			elog(ERROR, "partitioned dest receiver hash key mismatch");

		entryPtr->multiDataFileDestReceiver = NULL;
		entryPtr->rowCount = 0;

		allModifications = list_concat(allModifications, modifications);
	}

	return allModifications;
}

/*
 * rShutdown: call rShutdown for each known partition's subreceiver
 */
static void
ShutdownPartitionedDestReceiver(DestReceiver *self)
{
	PartitioningDestReceiverData *myState = (PartitioningDestReceiverData *) self;
	HASH_SEQ_STATUS seqStatus;
	PartitionPartitionDestReceiverHashEntry *ent;

	hash_seq_init(&seqStatus, myState->partitionsHash);
	while ((ent = (PartitionPartitionDestReceiverHashEntry *) hash_seq_search(&seqStatus)) != NULL)
	{
		ent->multiDataFileDestReceiver->rShutdown((DestReceiver *) ent->multiDataFileDestReceiver);
	}
}


/*
* rDestroy: call rDestroy for each known partition's subreceiver.
*/
static void
DestroyPartitionedDestReceiver(DestReceiver *self)
{
	PartitioningDestReceiverData *myState = (PartitioningDestReceiverData *) self;
	HASH_SEQ_STATUS seqStatus;
	PartitionPartitionDestReceiverHashEntry *ent;

	hash_seq_init(&seqStatus, myState->partitionsHash);
	while ((ent = (PartitionPartitionDestReceiverHashEntry *) hash_seq_search(&seqStatus)) != NULL)
	{
		ent->multiDataFileDestReceiver->rDestroy((DestReceiver *) ent->multiDataFileDestReceiver);
	}
}


/*
 * InitializeQueryPlanCache initialized the session-level query plan
 * cache.
 */
static HTAB *
InitializePartitionsHash(MemoryContext parentContext)
{
	HASHCTL		info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(PartitionPartitionDestReceiverHashEntry);
	info.hash = tag_hash;
	info.hcxt = parentContext;

	int			hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	return hash_create("PgLake partitions hash", 32, &info, hashFlags);
}

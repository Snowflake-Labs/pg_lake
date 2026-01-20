/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "postgres.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/ducklake/catalog.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/transaction/track_ducklake_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"
#include "pg_lake/util/rel_utils.h"

/*
 * TrackedDucklakeChange represents a pending change to a DuckLake table
 */
typedef struct TrackedDucklakeChange
{
	Oid			relationId;
	List	   *addedFiles;		/* List of TableDataFile * */
	List	   *removedFileIds; /* List of int64 data_file_id */
}			TrackedDucklakeChange;

static HTAB *DucklakeChangesHash = NULL;
static bool DucklakeTransactionHookRegistered = false;

static void InitDucklakeChangesHashIfNeeded(void);
static void ApplyTrackedDucklakeChanges(void);
static void DucklakeTransactionCallback(XactEvent event, void *arg);


/*
 * TrackDucklakeTableDataFileAddition tracks a data file addition for commit time
 */
void
TrackDucklakeTableDataFileAddition(Oid relationId, TableDataFile *dataFile)
{
	MemoryContext oldcontext;

	if (!IsDucklakeTable(relationId))
		return;

	InitDucklakeChangesHashIfNeeded();

	bool		found;
	TrackedDucklakeChange *change = hash_search(DucklakeChangesHash,
												&relationId,
												HASH_ENTER,
												&found);

	if (!found)
	{
		change->relationId = relationId;
		change->addedFiles = NIL;
		change->removedFileIds = NIL;
	}

	/* Switch to TopTransactionContext for list operations */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	change->addedFiles = lappend(change->addedFiles, dataFile);
	MemoryContextSwitchTo(oldcontext);
}


/*
 * TrackDucklakeTableDataFileRemoval tracks a data file removal for commit time
 */
void
TrackDucklakeTableDataFileRemoval(Oid relationId, int64 dataFileId)
{
	MemoryContext oldcontext;

	if (!IsDucklakeTable(relationId))
		return;

	InitDucklakeChangesHashIfNeeded();

	bool		found;
	TrackedDucklakeChange *change = hash_search(DucklakeChangesHash,
												&relationId,
												HASH_ENTER,
												&found);

	if (!found)
	{
		change->relationId = relationId;
		change->addedFiles = NIL;
		change->removedFileIds = NIL;
	}

	/* Switch to TopTransactionContext for list operations */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	change->removedFileIds = lappend_int(change->removedFileIds, dataFileId);
	MemoryContextSwitchTo(oldcontext);
}


/*
 * InitDucklakeChangesHashIfNeeded initializes the hash table and registers
 * the transaction callback
 */
static void
InitDucklakeChangesHashIfNeeded(void)
{
	if (DucklakeChangesHash != NULL)
		return;

	HASHCTL		info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(TrackedDucklakeChange);
	info.hcxt = TopTransactionContext;

	DucklakeChangesHash = hash_create("DuckLake Changes",
									  32,
									  &info,
									  HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	if (!DucklakeTransactionHookRegistered)
	{
		RegisterXactCallback(DucklakeTransactionCallback, NULL);
		DucklakeTransactionHookRegistered = true;
	}
}


/*
 * DucklakeTransactionCallback is called on transaction commit/abort
 */
static void
DucklakeTransactionCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT)
	{
		ApplyTrackedDucklakeChanges();
	}
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
	{
		/* Clean up the hash */
		if (DucklakeChangesHash != NULL)
		{
			hash_destroy(DucklakeChangesHash);
			DucklakeChangesHash = NULL;
		}
	}
}


/*
 * TrackDucklakeMetadataChangesInTx is the main entry point for tracking DuckLake metadata
 * changes. Currently, this is a placeholder that ensures the transaction callback is
 * registered. The actual tracking happens in TrackDucklakeTableDataFileAddition and
 * TrackDucklakeTableDataFileRemoval which are called from ApplyDataFileCatalogChanges.
 */
void
TrackDucklakeMetadataChangesInTx(Oid relationId, List *metadataOperationTypes)
{
	if (!IsDucklakeTable(relationId))
		return;

	/* Just initialize the hash to ensure transaction callback is registered */
	InitDucklakeChangesHashIfNeeded();
}


/*
 * ApplyTrackedDucklakeChanges applies all tracked changes at transaction commit
 */
static void
ApplyTrackedDucklakeChanges(void)
{
	if (DucklakeChangesHash == NULL)
		return;

	/*
	 * Push an active snapshot for SPI operations in catalog functions.
	 * During transaction commit, there may not be an active snapshot.
	 */
	bool		pushedSnapshot = false;
	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushedSnapshot = true;
	}

	HASH_SEQ_STATUS status;
	TrackedDucklakeChange *change;

	hash_seq_init(&status, DucklakeChangesHash);

	while ((change = hash_seq_search(&status)) != NULL)
	{
		if (change->addedFiles == NIL && change->removedFileIds == NIL)
			continue;

		/* Get table metadata */
		DucklakeTableMetadata *metadata = DucklakeGetTableMetadata(change->relationId);

		if (!metadata)
		{
			elog(WARNING, "DuckLake metadata not found for relation %u", change->relationId);
			continue;
		}

		/* Create a new snapshot */
		DucklakeSnapshot *newSnapshot = DucklakeCreateSnapshot(
			"INSERT/UPDATE/DELETE operation",
			GetUserNameFromId(GetUserId(), false),
			NULL);

		/* Add new data files */
		ListCell   *cell;

		foreach(cell, change->addedFiles)
		{
			TableDataFile *dataFile = lfirst(cell);

			DucklakeAddDataFile(metadata->tableId,
								dataFile->path,
								dataFile->stats.rowCount,
								dataFile->stats.fileSize,
								0);	/* rowIdStart */
		}

		/* Mark removed files */
		foreach(cell, change->removedFileIds)
		{
			int64		dataFileId = lfirst_int(cell);

			DucklakeRemoveDataFile(dataFileId);
		}

		elog(DEBUG1, "Created DuckLake snapshot %ld for table %u with %d additions and %d removals",
			 newSnapshot->snapshotId,
			 change->relationId,
			 list_length(change->addedFiles),
			 list_length(change->removedFileIds));

		/* Clean up allocated memory */
		if (metadata->tableName)
			pfree(metadata->tableName);
		if (metadata->schemaName)
			pfree(metadata->schemaName);
		if (metadata->path)
			pfree(metadata->path);
		pfree(metadata);
		pfree(newSnapshot);
	}

	/* Pop the snapshot if we pushed it */
	if (pushedSnapshot)
		PopActiveSnapshot();
}

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
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/multi_data_file_dest.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/position_delete_dest.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/storage/local_storage.h"
#include "foreign/foreign.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"

#define FILE_ID_ATTR (1)
#define ROW_NUMBER_ATTR (2)


/*
 * PositionDeleteFileDest is an HTAB entry that keeps a CSV DestReceiver
 * to generate a position delete file.
 */
typedef struct PositionDeleteFileDest
{
	/* data file from which we are deleting */
	int64		dataFileId;

	TableDataFile *dataFile;

	/* destination for the position delete records */
	char	   *csvFilePath;
	DestReceiver *csvDest;

	/* number of deleted rows */
	int64		deletedRowCount;
}			PositionDeleteFileDest;

/*
 * PositionDeleteDestReceiver is a DestReceiver that writes position delete tuples
 * to a CSV files.
 */
typedef struct PositionDeleteDestReceiver
{
	/* function pointers for this DestReceiver */
	DestReceiver pub;

	MemoryContext parentContext;

	/* CSV DestReceivers used as an intermediate position delete format */
	HTAB	   *fileDests;

	/* hash of current data files */
	HTAB	   *dataFilesHash;

	/* tuple descriptor of the position deletes */
	TupleTableSlot *deleteSlot;

	/* tuple descriptor of the received slots */
	TupleDesc	tupleDesc;

	/* operation of the DestReceiver */
	int			operation;

}			PositionDeleteDestReceiver;


static HTAB *CreateFileDestsHash(void);
static PositionDeleteFileDest * GetPositionDeleteFileDest(PositionDeleteDestReceiver * self,
														  int64 fileId);
static void StartDestReceiver(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool ReceiveSlot(TupleTableSlot *slot, DestReceiver *self);
static void PrepareDeletionSlot(Datum pathDatum, Datum rowNumberDatum,
								TupleTableSlot *deleteSlot);
static void ShutdownDestReceiver(DestReceiver *self);
static void DestroyDestReceiver(DestReceiver *self);


/*
 * CreatePositionDeleteDestReceiver creates a PositionDeleteDestReceiver
 * for the given relation.
 */
DestReceiver *
CreatePositionDeleteDestReceiver(Oid relationId)
{
	PositionDeleteDestReceiver *self =
		(PositionDeleteDestReceiver *) palloc0(sizeof(PositionDeleteDestReceiver));

	self->pub.rStartup = StartDestReceiver;
	self->pub.receiveSlot = ReceiveSlot;
	self->pub.rShutdown = ShutdownDestReceiver;
	self->pub.rDestroy = DestroyDestReceiver;
	self->pub.mydest = DestCopyOut;

	self->parentContext = CurrentMemoryContext;

	self->fileDests = CreateFileDestsHash();

	List	   *allTransforms = AllPartitionTransformList(relationId);

	self->dataFilesHash =
		GetTableDataFilesHashFromCatalog(relationId, true, false, false, NULL, NULL, allTransforms);

	/* construct a tuple table slot for position deletes */
	TupleDesc	deleteTupleDesc = CreatePositionDeleteTupleDesc();

	self->deleteSlot = MakeSingleTupleTableSlot(deleteTupleDesc,
												&TTSOpsMinimalTuple);

	return (DestReceiver *) self;
}


/*
 * CreateFileDestsHash creates a hash table that is suitable for storing
 * PositionDeleteFileDest entries
 */
static HTAB *
CreateFileDestsHash(void)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(int64);
	hashCtl.entrysize = sizeof(PositionDeleteFileDest);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *dataFilesHash = hash_create("position delete hash",
											1024,
											&hashCtl,
											HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	return dataFilesHash;
}


/*
 * GetPositionDeleteFileDest looks up the CSV DestReceiver that is used
 * to build the position delete file for a given data file ID.
 */
static PositionDeleteFileDest *
GetPositionDeleteFileDest(PositionDeleteDestReceiver * self, int64 fileId)
{
	bool		fileDestFound = false;
	PositionDeleteFileDest *fileDest =
		hash_search(self->fileDests, &fileId, HASH_ENTER, &fileDestFound);

	if (!fileDestFound)
	{
		bool		fileFound = false;
		TableDataFile *dataFile =
			hash_search(self->dataFilesHash, &fileId, HASH_FIND, &fileFound);

		if (!fileFound)
			elog(ERROR, "position delete for non-existent file " INT64_FORMAT, fileId);

		MemoryContext callerContext = MemoryContextSwitchTo(self->parentContext);

		/* we currently use CSV as a universal intermediate format */
		bool		includeHeader = true;
		List	   *copyOptions = InternalCSVOptions(includeHeader);
		char	   *tempFilePath = GenerateTempFileName("position_deletes", true);

		fileDest->dataFile = dataFile;
		fileDest->deletedRowCount = 0;
		fileDest->csvFilePath = pstrdup(tempFilePath);
		fileDest->csvDest = CreateCSVDestReceiver(tempFilePath, copyOptions,
												  DATA_FORMAT_PARQUET);

		fileDest->csvDest->rStartup(fileDest->csvDest, CMD_DELETE,
									self->deleteSlot->tts_tupleDescriptor);

		MemoryContextSwitchTo(callerContext);
	}

	return fileDest;
}


/*
 * StartDestReceiver is called when the DestReceiver starts.
 */
static void
StartDestReceiver(DestReceiver *dest, int operation, TupleDesc tupleDesc)
{
}


/*
 * ReceiveSlot is called when a slot is received. We pass it on to the child
 * DestReceiver, and rotate to a new file if the file size exceeds the threshold.
 */
static bool
ReceiveSlot(TupleTableSlot *slot, DestReceiver *dest)
{
	PositionDeleteDestReceiver *self = (PositionDeleteDestReceiver *) dest;

	bool		isNull = false;
	int64		fileId = DatumGetInt64(slot_getattr(slot, FILE_ID_ATTR, &isNull));

	Assert(!isNull);

	Datum		rowNumberDatum = slot_getattr(slot, ROW_NUMBER_ATTR, &isNull);

	Assert(!isNull);

	PositionDeleteFileDest *fileDest = GetPositionDeleteFileDest(self, fileId);

	Datum		pathDatum = CStringGetTextDatum(fileDest->dataFile->path);

	PrepareDeletionSlot(pathDatum, rowNumberDatum, self->deleteSlot);

	bool		result = fileDest->csvDest->receiveSlot(self->deleteSlot, fileDest->csvDest);

	fileDest->deletedRowCount++;

	return result;
}


/*
 * PrepareDeletionSlot puts a deletion record for the given file ID and
 * row number in deleteSlot.
 */
static void
PrepareDeletionSlot(Datum pathDatum, Datum rowNumberDatum, TupleTableSlot *deleteSlot)
{
	Datum		datums[] = {
		pathDatum,
		rowNumberDatum,
		0
	};

	bool		nulls[] = {
		false,
		false,
		true
	};

	MinimalTuple deleteTuple = heap_form_minimal_tuple(deleteSlot->tts_tupleDescriptor,
													   datums,
													   nulls
#if PG_VERSION_NUM >= 180000
													   ,0	/* extra */
#endif
		);

	bool		shouldFree = true;

	ExecStoreMinimalTuple(deleteTuple, deleteSlot, shouldFree);
}


/*
 * ShutdownDestReceiver shuts down the fileDest DestReceivers.
 */
static void
ShutdownDestReceiver(DestReceiver *dest)
{
	PositionDeleteDestReceiver *self = (PositionDeleteDestReceiver *) dest;

	HASH_SEQ_STATUS status;
	PositionDeleteFileDest *fileDest = NULL;

	hash_seq_init(&status, self->fileDests);

	while ((fileDest = hash_seq_search(&status)) != NULL)
		fileDest->csvDest->rShutdown(fileDest->csvDest);
}


/*
 * DestroyDestReceiver destroys the fileDest DestReceivers.
 */
static void
DestroyDestReceiver(DestReceiver *dest)
{
	PositionDeleteDestReceiver *self = (PositionDeleteDestReceiver *) dest;

	HASH_SEQ_STATUS status;
	PositionDeleteFileDest *fileDest = NULL;

	hash_seq_init(&status, self->fileDests);

	while ((fileDest = hash_seq_search(&status)) != NULL)
		fileDest->csvDest->rDestroy(fileDest->csvDest);

	hash_destroy(self->dataFilesHash);
}


/*
 * GetPositionDeleteDestReceiverModifications returns the modifications generated
 * by this DestReceiver.
 */
List *
GetPositionDeleteDestReceiverModifications(DestReceiver *dest)
{
	PositionDeleteDestReceiver *self = (PositionDeleteDestReceiver *) dest;

	HASH_SEQ_STATUS status;
	PositionDeleteFileDest *fileDest = NULL;

	hash_seq_init(&status, self->fileDests);

	List	   *modifications = NIL;

	while ((fileDest = hash_seq_search(&status)) != NULL)
	{
		DataFileModification *modification = palloc0(sizeof(DataFileModification));

		modification->type = ADD_DELETION_FILE_FROM_CSV;
		modification->sourcePath = fileDest->dataFile->path;
		modification->sourceRowCount = fileDest->dataFile->stats.rowCount;
		modification->liveRowCount =
			fileDest->dataFile->stats.rowCount - fileDest->dataFile->stats.deletedRowCount;
		modification->deleteFile = fileDest->csvFilePath;
		modification->deletedRowCount = fileDest->deletedRowCount;

		modifications = lappend(modifications, modification);
	}

	return modifications;
}

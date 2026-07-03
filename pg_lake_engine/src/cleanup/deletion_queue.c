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

/*
 * Functions for cleaning up orphaned files.
 */
#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/xact.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/util/array_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/util/string_utils.h"
#include "datatype/timestamp.h"
#include "storage/procarray.h"

#define DELETION_QUEUE_TABLE "lake_engine.deletion_queue"


/* managed by GUC */
int			OrphanedFileRetentionPeriod = 60 * 60 * 24 * 10;	/* 10 days */

/* managed by GUC, not exposed to the users */
int			VacuumFileRemoveMaxRetries = 145;

/*
 * DeletionQueueEntry represents a deletion entry from the
 * deletion queue.
 */
typedef struct DeletionQueueEntry
{
	char	   *path;
	TimestampTz orphanedAt;
	int			retryCount;
	bool		isPrefix;
	bool		resolveMetadata;
}			DeletionQueueEntry;

static void RemoveDeletionQueuePathsFromCatalog(List *filePaths);
static void IncrementDeletionQueueRetryCount(List *failedRemovalPaths);
static bool ExpandMetadataResolveRecord(char *metadataPath);


PG_FUNCTION_INFO_V1(flush_deletion_queue);


/*
 * flush_deletion_queue removes all eligible files from
 * the deletion queue.
 */
Datum
flush_deletion_queue(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* remove all */
	bool		isFull = true;
	bool		isVerbose = false;
	List	   *deletionQueueRecords = GetDeletionQueueRecords(relationId, isFull);

	RemoveDeletionQueueRecords(deletionQueueRecords, isVerbose);

	ListCell   *fileCell = NULL;

	foreach(fileCell, deletionQueueRecords)
	{
		DeletionQueueEntry *deletedFile = lfirst(fileCell);

		Datum		values[] = {CStringGetTextDatum(deletedFile->path)};
		bool		nulls[] = {false};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}

/*
 * RemoveDeletionQueueRecords removes all files that are no longer referenced .
 * Returns true if at least one file was successfully removed.
 */
bool
RemoveDeletionQueueRecords(List *deletionQueueRecords, bool isVerbose)
{
	List	   *deletedFilePathList = NIL;
	List	   *failedFilePathList = NIL;

	/*
	 * Whether we made forward progress by expanding a deferred-drop
	 * resolve_metadata row into per-file rows. Even if that iteration deletes
	 * nothing itself, it produced new work, so the caller should keep
	 * draining.
	 */
	bool		expandedMetadata = false;

	ListCell   *cleanupRecordCell = NULL;

	foreach(cleanupRecordCell, deletionQueueRecords)
	{
		DeletionQueueEntry *entry = lfirst(cleanupRecordCell);

		if (entry->resolveMetadata)
		{
			/*
			 * Deferred-drop row: "path" is a metadata.json, not a file to
			 * delete directly. Resolve it now to the exact set of referenced
			 * files and enqueue those as normal per-file rows, then let the
			 * regular deletion machinery remove them (which is naturally
			 * robust to files that were already deleted). This moves the
			 * expensive object-store walk off the DROP path without changing
			 * the file-accurate deletion semantics.
			 */
			ereport(isVerbose ? INFO : LOG,
					(errmsg("resolving referenced files of dropped table metadata %s",
							entry->path)));

			if (ExpandMetadataResolveRecord(entry->path))
				expandedMetadata = true;
			else
			{
				/*
				 * Could not resolve (e.g. object store unreachable); leave
				 * the row and retry later.
				 */
				failedFilePathList = lappend(failedFilePathList, entry->path);
			}

			continue;
		}

		ereport(isVerbose ? INFO : LOG,
				(errmsg("deleting expired %s %s",
						entry->isPrefix ? "prefix" : "file",
						entry->path)));

		bool		success = false;

		if (entry->isPrefix)
		{
			/* ok, let's try to fetch and delete all of its tree */
			success = DeleteRemotePrefix(entry->path);
		}
		else
		{
			/* remove the file */
			success = DeleteRemoteFile(entry->path);
		}

		if (success)
		{
			/* remove the record */
			deletedFilePathList = lappend(deletedFilePathList, entry->path);
		}
		else
		{
			/* add to failed list */
			failedFilePathList = lappend(failedFilePathList, entry->path);
		}

	}

	if (list_length(deletedFilePathList) > 0)
	{
		RemoveDeletionQueuePathsFromCatalog(deletedFilePathList);
	}

	if (list_length(failedFilePathList) > 0)
	{
		IncrementDeletionQueueRetryCount(failedFilePathList);
	}

	/* keep draining if we removed a file or produced new per-file rows */
	return list_length(deletedFilePathList) > 0 || expandedMetadata;
}


/*
 * ExpandMetadataResolveRecord takes the metadata.json path of a dropped table
 * (queued by a deferred DROP) and turns it into concrete per-file deletion
 * rows:
 *
 *   1. resolve the metadata.json into the list of files it references (data
 *      files, delete files, manifests, manifest lists and the metadata.json
 *      itself) and enqueue them as normal, immediately-eligible deletion rows,
 *   2. convert the resolve_metadata row itself into a normal file row so the
 *      metadata.json is deleted like any other file.
 *
 * The resolution is done by lake_iceberg.find_all_referenced_files(), which
 * lives in the (higher) iceberg layer.  We call it by name via SPI so this
 * lower engine layer needs no link-time dependency on it; a single INSERT ..
 * SELECT does both the walk and the enqueue.  ON CONFLICT DO NOTHING keeps it
 * robust to files (including the metadata.json) already queued by metadata
 * rotation, an earlier partial expansion, or a retry.
 *
 * The referenced files then get deleted by the regular per-file path, which
 * already tolerates files that were removed in the meantime.  The whole
 * expansion runs in its own subtransaction: if the metadata cannot be resolved
 * (e.g. the object store is unreachable), we roll it back and return false so
 * the caller retries this row later without aborting the rest of the drain.
 */
static bool
ExpandMetadataResolveRecord(char *metadataPath)
{
	MemoryContext savedContext = CurrentMemoryContext;
	volatile bool resolved = true;

	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		bool		readOnly = false;

		SPI_START_EXTENSION_OWNER(PgLakeTable);

		/*
		 * orphaned_at is NULL so the files are eligible for deletion right
		 * away: the retention window was already served while this metadata
		 * row waited in the queue.
		 */
		{
			char	   *insertQuery =
				"INSERT INTO " DELETION_QUEUE_TABLE " "
				"(path, table_name, orphaned_at, is_prefix, resolve_metadata) "
				"SELECT f.path, NULL, NULL, false, false "
				"FROM lake_iceberg.find_all_referenced_files($1) f "
				"ON CONFLICT (path) DO NOTHING";

			DECLARE_SPI_ARGS(1);
			SPI_ARG_VALUE(1, TEXTOID, metadataPath, false);

			SPI_EXECUTE(insertQuery, readOnly);
		}

		/*
		 * find_all_referenced_files also returns the metadata.json itself, so
		 * the ON CONFLICT above left our resolve_metadata row untouched.
		 * Convert it in place into a normal, eligible file row.
		 */
		{
			char	   *convertQuery =
				"UPDATE " DELETION_QUEUE_TABLE " "
				"SET resolve_metadata = false, orphaned_at = NULL "
				"WHERE path OPERATOR(pg_catalog.=) $1";

			DECLARE_SPI_ARGS(1);
			SPI_ARG_VALUE(1, TEXTOID, metadataPath, false);

			SPI_EXECUTE(convertQuery, readOnly);
		}

		SPI_END();

		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();

		/* a cancellation must propagate */
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
			ThrowErrorData(edata);

		resolved = false;
	}
	PG_END_TRY();

	return resolved;
}


/*
* RemoveDeletionQueuePathsFromCatalog removes the given paths from the
* deletion queue catalog.
*/
static void
RemoveDeletionQueuePathsFromCatalog(List *filePaths)
{
	ArrayType  *failedRemovalPathsArray = StringListToArray(filePaths);

	char	   *query =
		"DELETE FROM " DELETION_QUEUE_TABLE " "
		"WHERE path OPERATOR(pg_catalog.=) ANY($1)";

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, TEXTARRAYOID, failedRemovalPathsArray, false);

	/* switch to schema owner, we assume callers checked permissions */
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}


/*
* IncrementDeletionQueueRetryCount increments the retry count
* for the given paths in the deletion queue.
*/
static void
IncrementDeletionQueueRetryCount(List *failedRemovalPaths)
{
	ArrayType  *failedRemovalPathsArray = StringListToArray(failedRemovalPaths);
	bool		readOnly = false;

	char	   *updateQuery =
		"UPDATE " DELETION_QUEUE_TABLE " "
		"SET retry_count = retry_count + 1 "
		"WHERE path OPERATOR(pg_catalog.=) ANY($1) ";

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, TEXTARRAYOID, failedRemovalPathsArray, false);

	/* switch to schema owner, we assume callers checked permissions */
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	SPI_EXECUTE(updateQuery, readOnly);

	SPI_END();
}


/*
 * GetDeletionQueueRecords gets a list of paths that are eligible for
 * deletion, meaning delete_after condition is met on DELETION_QUEUE_TABLE.
 */
List *
GetDeletionQueueRecords(Oid relationId, bool isFull)
{
	MemoryContext callerContext = CurrentMemoryContext;
	List	   *result = NIL;

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "WITH del AS (");

	if (OidIsValid(relationId))
	{
		appendStringInfo(query,
						 "    SELECT ctid, path, orphaned_at, retry_count, is_prefix, resolve_metadata "
						 "    FROM " DELETION_QUEUE_TABLE " "
						 "    WHERE (orphaned_at IS NULL or pg_catalog.now() OPERATOR(pg_catalog.>=) (orphaned_at OPERATOR(pg_catalog.+) INTERVAL '%d seconds')) AND "
						 "		  table_name OPERATOR(pg_catalog.=) %d AND retry_count OPERATOR(pg_catalog.<=) %d  FOR UPDATE",
						 OrphanedFileRetentionPeriod, relationId, VacuumFileRemoveMaxRetries);

	}
	else
	{
		/*
		 * This is for dropped tables, so join with pg_class to find all
		 * entries in the DELETION_QUEUE_TABLE that are not associated with
		 * any existing table.
		 */
		appendStringInfo(query,
						 "    SELECT del.ctid, del.path, del.orphaned_at, del.retry_count, del.is_prefix, del.resolve_metadata "
						 "    FROM " DELETION_QUEUE_TABLE " del "
						 "    LEFT JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) del.table_name "
						 "    WHERE (del.orphaned_at IS NULL or pg_catalog.now() OPERATOR(pg_catalog.>=) (del.orphaned_at OPERATOR(pg_catalog.+) INTERVAL '%d seconds')) AND "
						 "          c.oid IS NULL  AND retry_count OPERATOR(pg_catalog.<=) %d FOR UPDATE OF del",
						 OrphanedFileRetentionPeriod, VacuumFileRemoveMaxRetries);

	}

	if (!isFull)
	{
		appendStringInfo(query,
						 "    LIMIT " PG_LAKE_TOSTRING(PER_LOOP_FILE_CLEANUP_LIMIT));
	}

	appendStringInfo(query,
					 ") "
					 "SELECT path, orphaned_at, retry_count, is_prefix, resolve_metadata FROM del");

	/* switch to schema owner, we assume callers checked permissions */
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_execute(query->data, readOnly, 0);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull;
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		DeletionQueueEntry *entry = palloc0(sizeof(DeletionQueueEntry));

		entry->path = GET_SPI_VALUE(TEXTOID, rowIndex, 1, &isNull);
		entry->orphanedAt = GET_SPI_VALUE(TIMESTAMPTZOID, rowIndex, 2, &isNull);
		entry->retryCount = GET_SPI_VALUE(INT4OID, rowIndex, 3, &isNull);
		entry->isPrefix = GET_SPI_VALUE(BOOLOID, rowIndex, 4, &isNull);
		entry->resolveMetadata = GET_SPI_VALUE(BOOLOID, rowIndex, 5, &isNull);

		result = lappend(result, entry);

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return result;
}


/*
* InsertPrefixDeletionRecord adds a prefix into the deletion queue for
* later removal. When the prefix is removed, all files under the prefix
* will be removed.
*/
void
InsertPrefixDeletionRecord(char *path, TimestampTz orphanedAt)
{
	InsertDeletionQueueRecordExtended(path, InvalidOid, orphanedAt, true);
}


/*
 * InsertDeletionQueueRecord adds a path into the deletion queue for
 * later removal.
 */
void
InsertDeletionQueueRecord(char *path, Oid relationId, TimestampTz orphanedAt)
{
	InsertDeletionQueueRecordExtended(path, relationId, orphanedAt, false);
}


/*
 * InsertMetadataResolveRecord queues a table's metadata.json for deferred
 * resolution: instead of walking the object store at DROP time, we record the
 * metadata.json path as a single row. VACUUM later resolves it into the exact
 * set of referenced files (see ExpandMetadataResolveRecord) and deletes those,
 * honouring the normal orphaned_at retention.
 */
void
InsertMetadataResolveRecord(char *metadataPath, Oid relationId, TimestampTz orphanedAt)
{
	char	   *query =
		"insert into " DELETION_QUEUE_TABLE " "
		"(path, table_name, orphaned_at, is_prefix, resolve_metadata) "
		"values ($1,$2,$3,false,true)";

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, TEXTOID, metadataPath, false);
	SPI_ARG_VALUE(2, OIDOID, relationId, false);
	SPI_ARG_VALUE(3, TIMESTAMPTZOID, orphanedAt, orphanedAt == 0);

	/* switch to schema owner, we assume callers checked permissions */
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}

/*
* InsertDeletionQueueRecordExtended is the internal function to insert
* a record into the deletion queue.
*/
void
InsertDeletionQueueRecordExtended(char *path, Oid relationId, TimestampTz orphanedAt,
								  bool isPrefix)
{
	char	   *query =
		"insert into " DELETION_QUEUE_TABLE " "
		"(path, table_name, orphaned_at, is_prefix) "
		"values ($1,$2,$3,$4)";

	DECLARE_SPI_ARGS(4);
	SPI_ARG_VALUE(1, TEXTOID, path, false);
	SPI_ARG_VALUE(2, OIDOID, relationId, false);
	SPI_ARG_VALUE(3, TIMESTAMPTZOID, orphanedAt, orphanedAt == 0);
	SPI_ARG_VALUE(4, BOOLOID, isPrefix, false);

	/* switch to schema owner, we assume callers checked permissions */
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}

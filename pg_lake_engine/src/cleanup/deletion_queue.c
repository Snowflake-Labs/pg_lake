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
#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/xact.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/util/array_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "datatype/timestamp.h"
#include "storage/procarray.h"
#include "utils/tuplestore.h"

#define DELETION_QUEUE_TABLE "lake_engine.deletion_queue"


/* managed by GUC */
int			OrphanedFileRetentionPeriod = 60 * 60 * 24 * 10;	/* 10 days */

/* managed by GUC, not exposed to the users */
int			VacuumFileRemoveMaxRetries = 145;
int			VacuumFileRemoveRetryInterval = 600;	/* 10 minutes */

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
static bool DeleteQueuedPrefix(char *path, bool isVerbose);


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

	/*
	 * The deletes below reach object storage under whatever secrets
	 * pgduck_server already holds, which for these files is the standing
	 * configuration rather than anything vended: credentials are vended only
	 * for read-only tables, and a read-only table owns none of the files it
	 * reads, so it never queues a delete here.
	 *
	 * Resolving here instead is not possible without the engine knowing what
	 * a catalog is, and would not help the case that matters anyway: an "all
	 * tables" drain covers files whose table is already dropped, and a
	 * dropped relation can no longer be resolved.
	 */

	/* remove all */
	bool		isFull = true;
	bool		isVerbose = false;
	List	   *deletionQueueRecords = GetDeletionQueueRecords(relationId, isFull,
															   PER_LOOP_FILE_CLEANUP_LIMIT);

	/* removes everything it can, so there is no budget to report back on */
	RemoveDeletionQueueRecords(deletionQueueRecords, isVerbose, NULL);

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
 *
 * When filesRemoved is given it reports how many of the records were actually
 * removed, which is fewer than the number of records whenever a removal failed.
 * Callers that spend a budget on this count what they removed, not what they
 * claimed, so paths that can never be removed do not consume the budget over
 * and over.
 */
bool
RemoveDeletionQueueRecords(List *deletionQueueRecords, bool isVerbose, int *filesRemoved)
{
	List	   *deletedFilePathList = NIL;
	List	   *failedFilePathList = NIL;

	/*
	 * Set when we expanded a resolve_metadata row into new per-file rows:
	 * that pass may delete nothing itself but produced work, so keep
	 * draining.
	 */
	bool		producedNewDeletionRows = false;

	ListCell   *cleanupRecordCell = NULL;

	/*
	 * The queue holds two kinds of rows. A direct row names an object to
	 * delete now (a file, or the whole tree under a prefix when is_prefix is
	 * set). A deferred-drop row (resolve_metadata) instead names a dropped
	 * table's metadata.json: we resolve it into its referenced files, enqueue
	 * those as direct rows, and convert the metadata.json row into a direct
	 * row -- a following drain pass then deletes them all.
	 *
	 * We persist the resolved files rather than resolving-and-deleting inline
	 * so the expensive metadata walk is paid once: each file then gets the
	 * normal retry_count budget and batching, and an interrupted VACUUM
	 * resumes from committed rows.
	 *
	 * Plain file rows are not deleted one by one. They are collected into
	 * batches of FILE_DELETION_BATCH_SIZE and removed with one object-store
	 * request per batch, because a request per file made draining a large
	 * queue take time proportional to the number of files -- with the whole
	 * vacuum cycle, and therefore catalog publication, waiting behind it.
	 */
	List	   *deletionBatch = NIL;

	foreach(cleanupRecordCell, deletionQueueRecords)
	{
		DeletionQueueEntry *entry = lfirst(cleanupRecordCell);

		if (entry->resolveMetadata)
		{
			ereport(isVerbose ? INFO : LOG,
					(errmsg("resolving referenced files of dropped table metadata %s",
							entry->path)));

			if (ExpandMetadataResolveRecord(entry->path))
				producedNewDeletionRows = true;
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

		if (entry->isPrefix)
		{
			/*
			 * A prefix row expands into an unbounded listing plus delete, so
			 * it stays a request of its own.
			 */
			if (DeleteQueuedPrefix(entry->path, isVerbose))
				deletedFilePathList = lappend(deletedFilePathList, entry->path);
			else
				failedFilePathList = lappend(failedFilePathList, entry->path);

			continue;
		}

		ereport(isVerbose ? INFO : LOG,
				(errmsg("deleting expired file %s", entry->path)));

		deletionBatch = lappend(deletionBatch, entry->path);

		if (list_length(deletionBatch) >= FILE_DELETION_BATCH_SIZE)
		{
			DeleteRemoteFileBatch(deletionBatch, &deletedFilePathList,
								  &failedFilePathList);
			deletionBatch = NIL;

			/*
			 * A batch is a long-running request; give the caller a chance to
			 * cancel the drain between batches.
			 */
			CHECK_FOR_INTERRUPTS();
		}
	}

	/* whatever did not fill a batch */
	DeleteRemoteFileBatch(deletionBatch, &deletedFilePathList, &failedFilePathList);

	if (list_length(deletedFilePathList) > 0)
	{
		RemoveDeletionQueuePathsFromCatalog(deletedFilePathList);
	}

	if (list_length(failedFilePathList) > 0)
	{
		IncrementDeletionQueueRetryCount(failedFilePathList);
	}

	if (filesRemoved != NULL)
	{
		*filesRemoved = list_length(deletedFilePathList);
	}

	/*
	 * Keep draining if we deleted something, or if we produced new per-file
	 * rows that the next pass still has to delete.
	 */
	return list_length(deletedFilePathList) > 0 || producedNewDeletionRows;
}


/*
 * DeleteQueuedPrefix removes the whole tree under the prefix named by a
 * deletion-queue row with is_prefix set, and reports whether the removal
 * succeeded.
 */
static bool
DeleteQueuedPrefix(char *path, bool isVerbose)
{
	ereport(isVerbose ? INFO : LOG,
			(errmsg("deleting expired prefix %s", path)));

	return DeleteRemotePrefix(path);
}


/*
 * ExpandMetadataResolveRecord turns a deferred-drop resolve_metadata row into
 * concrete deletion rows: it resolves the metadata.json into the files it
 * references and enqueues them as normal, immediately-eligible rows, then
 * converts the resolve_metadata row itself into a normal file row (the walk
 * returns the metadata.json too). It only enqueues; a later drain pass does
 * the deletes (see RemoveDeletionQueueRecords for why we persist rather than
 * delete inline).
 *
 * Resolution calls lake_iceberg.find_all_referenced_files() by name over SPI,
 * so this engine layer needs no link-time dependency on the iceberg layer. It
 * runs in its own subtransaction: on failure (e.g. object store unreachable)
 * we roll back and return false so the caller retries this row later without
 * aborting the rest of the drain.
 *
 * The queue may already hold some of these paths (the metadata.json's own row
 * for sure, plus any previous_metadata/rotation leftovers or files shared with
 * another dropped table), so a plain INSERT would hit the primary key and
 * abort. ON CONFLICT (path) DO UPDATE ... WHERE path = $1 handles that in one
 * statement: it converts only the metadata.json row into a normal file row and
 * no-ops every other conflict, leaving those rows' retention untouched. (A
 * DELETE-then-reinsert CTE cannot do this -- WITH sub-statements share one
 * snapshot, so the insert's ON CONFLICT would not see the sibling delete and
 * would drop the metadata.json.)
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
		 *
		 * find_all_referenced_files returns the metadata.json itself, which
		 * is already queued as our resolve_metadata row, so that row needs
		 * converting to a normal file row too. The DO UPDATE ... WHERE does
		 * this in the same statement: it fires only for the metadata.json's
		 * own row and no-ops every other conflict (a previous_metadata/
		 * rotation leftover, or a file shared with another dropped table),
		 * leaving their retention untouched.
		 */
		{
			char	   *insertQuery =
				"INSERT INTO " DELETION_QUEUE_TABLE " "
				"(path, table_name, orphaned_at, is_prefix, resolve_metadata) "
				"SELECT f.path, NULL, NULL, false, false "
				"FROM lake_iceberg.find_all_referenced_files($1) f "
				"ON CONFLICT (path) DO UPDATE "
				"SET resolve_metadata = false, orphaned_at = NULL "
				"WHERE deletion_queue.path OPERATOR(pg_catalog.=) $1";

			DECLARE_SPI_ARGS(1);
			SPI_ARG_VALUE(1, TEXTOID, metadataPath, false);

			SPI_EXECUTE(insertQuery, readOnly);
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

		/*
		 * Surface the failure as a WARNING (a cancellation keeps ERROR and
		 * propagates), then swallow it as resolved = false so the caller
		 * retries this row later and keeps draining the rest of the queue.
		 * The read-only metadata walk touches no tracked-metadata or
		 * REST-catalog state, so there is nothing to reset here.
		 */
		if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
			edata->elevel = WARNING;

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
* for the given paths in the deletion queue, and records when the failed
* attempt happened so the next one can be held off for
* VacuumFileRemoveRetryInterval (see GetDeletionQueueRecords).
*/
static void
IncrementDeletionQueueRetryCount(List *failedRemovalPaths)
{
	ArrayType  *failedRemovalPathsArray = StringListToArray(failedRemovalPaths);
	bool		readOnly = false;

	char	   *updateQuery =
		"UPDATE " DELETION_QUEUE_TABLE " "
		"SET retry_count = retry_count + 1, last_attempt_at = pg_catalog.now() "
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
 *
 * Unless isFull is set, at most maxRecords rows are claimed, capped at
 * PER_LOOP_FILE_CLEANUP_LIMIT so that one pass cannot hold a transaction open
 * for an unbounded amount of work no matter what the caller asks for. Callers
 * that drain in a loop under a budget of their own pass what is left of that
 * budget, so the budget is respected exactly rather than to the nearest pass.
 *
 * A row that failed within the last VacuumFileRemoveRetryInterval is also left
 * alone, so a path that cannot be removed is retried on a clock rather than
 * once per pass. Without it a pass rate the callers do not control decides both
 * halves of the retry budget: the path is re-claimed by every pass, each of
 * those passes pays the cost of finding out which path in its batch failed,
 * and retry_count reaches VacuumFileRemoveMaxRetries in whatever time that many
 * passes take -- minutes when a drain is catching up on a backlog, rather than
 * the day the default is sized for.
 *
 * isFull skips the backoff: that is the manual flush, where the caller is
 * asking to try everything now.
 */
List *
GetDeletionQueueRecords(Oid relationId, bool isFull, int maxRecords)
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
						 "		  table_name OPERATOR(pg_catalog.=) %d AND retry_count OPERATOR(pg_catalog.<=) %d ",
						 OrphanedFileRetentionPeriod, relationId, VacuumFileRemoveMaxRetries);

		if (!isFull)
			appendStringInfo(query,
							 "      AND (last_attempt_at IS NULL OR pg_catalog.now() OPERATOR(pg_catalog.>=) (last_attempt_at OPERATOR(pg_catalog.+) INTERVAL '%d seconds')) ",
							 VacuumFileRemoveRetryInterval);

		appendStringInfoString(query, " FOR UPDATE");
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
						 "          c.oid IS NULL  AND retry_count OPERATOR(pg_catalog.<=) %d ",
						 OrphanedFileRetentionPeriod, VacuumFileRemoveMaxRetries);

		if (!isFull)
			appendStringInfo(query,
							 "      AND (del.last_attempt_at IS NULL OR pg_catalog.now() OPERATOR(pg_catalog.>=) (del.last_attempt_at OPERATOR(pg_catalog.+) INTERVAL '%d seconds')) ",
							 VacuumFileRemoveRetryInterval);

		appendStringInfoString(query, " FOR UPDATE OF del");
	}

	if (!isFull)
	{
		/*
		 * Clamp at 0 as well as at the per-pass limit. No caller asks for
		 * fewer than 0 rows today, but a negative LIMIT is an error rather
		 * than an empty result, and the callers that compute a remaining
		 * budget do so in another module.
		 */
		appendStringInfo(query,
						 "    LIMIT %d",
						 Max(Min(maxRecords, PER_LOOP_FILE_CLEANUP_LIMIT), 0));
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
	InsertDeletionQueueRecordExtended(path, InvalidOid, orphanedAt, true, false);
}


/*
 * InsertDeletionQueueRecord adds a path into the deletion queue for
 * later removal.
 */
void
InsertDeletionQueueRecord(char *path, Oid relationId, TimestampTz orphanedAt)
{
	InsertDeletionQueueRecordExtended(path, relationId, orphanedAt, false, false);
}


/*
 * InsertMetadataResolveRecord queues a dropped table's metadata.json for
 * deferred resolution: VACUUM later resolves it into the exact referenced
 * files and deletes them (see ExpandMetadataResolveRecord).
 */
void
InsertMetadataResolveRecord(char *metadataPath, Oid relationId, TimestampTz orphanedAt)
{
	bool		isPrefix = false;
	bool		resolveMetadata = true;

	InsertDeletionQueueRecordExtended(metadataPath, relationId, orphanedAt,
									  isPrefix, resolveMetadata);
}

/*
* InsertDeletionQueueRecordExtended is the internal function to insert
* a record into the deletion queue. is_prefix marks a whole-prefix delete and
* resolve_metadata marks a metadata.json to be resolved into referenced files
* by VACUUM; the two are mutually exclusive.
*/
void
InsertDeletionQueueRecordExtended(char *path, Oid relationId, TimestampTz orphanedAt,
								  bool isPrefix, bool resolveMetadata)
{
	char	   *query =
		"insert into " DELETION_QUEUE_TABLE " "
		"(path, table_name, orphaned_at, is_prefix, resolve_metadata) "
		"values ($1,$2,$3,$4,$5)";

	DECLARE_SPI_ARGS(5);
	SPI_ARG_VALUE(1, TEXTOID, path, false);
	SPI_ARG_VALUE(2, OIDOID, relationId, false);
	SPI_ARG_VALUE(3, TIMESTAMPTZOID, orphanedAt, orphanedAt == 0);
	SPI_ARG_VALUE(4, BOOLOID, isPrefix, false);
	SPI_ARG_VALUE(5, BOOLOID, resolveMetadata, false);

	/* switch to schema owner, we assume callers checked permissions */
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}

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
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/catalog/row_id_mappings.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/row_ids.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/query/execute.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"


/*
 * EnableRowIdsOnTable performs the steps needed to enable row_ids
 * on an existing table.
 */
void
EnableRowIdsOnTable(Oid relationId)
{
	/* we cannot allow any writes while generating mappings */
	LockRelationOid(relationId, ExclusiveLock);

	CreateRelationRowIdSequence(relationId);

	bool		dataOnly = true;
	bool		newFilesOnly = false;
	List	   *dataFiles = GetTableDataFilesFromCatalog(relationId, dataOnly,
														 newFilesOnly, false, NULL, NULL);

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(dataFileCell);
		int64		rowCount = dataFile->stats.rowCount;

		if (rowCount == 0)
			continue;

		RowIdRangeMapping *rowIdRange =
			CreateRowIdRangeForNewFile(relationId, rowCount, 0);

		InsertSingleRowMapping(relationId, dataFile->fileId,
							   rowIdRange->rowStartId,
							   rowIdRange->rowStartId + rowIdRange->numRows,
							   rowIdRange->rowStartNum);

		UpdateDataFileFirstRowId(relationId, dataFile->fileId, rowIdRange->rowStartId);
	}
}


/*
 * Return the name of the rowid sequence for the given relation.
 */
RangeVar *
RowIdSequenceGetRangeVar(Oid relationId)
{
	char	   *sequenceName = psprintf("rowid_%d_seq", relationId);

	return makeRangeVar(REPLICATION_WRITES_SCHEMA, sequenceName, -1);
}


/*
 * Create a sequence for the given iceberg relation to use for new row ids.
 * Adds a dependency on replayTableId.
 */
void
CreateRelationRowIdSequence(Oid relationId)
{
	/* create sequence as superuser */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	RangeVar   *sequenceName = RowIdSequenceGetRangeVar(relationId);

	/* verify no such sequence already exists */
	bool		missingOk = true;
	Oid			sequenceId = RangeVarGetRelid(sequenceName, NoLock, missingOk);

	if (OidIsValid(sequenceId))
		ereport(ERROR, (errmsg("rowid sequence already exists "
							   "for relation %d", relationId),
						errcode(ERRCODE_DUPLICATE_TABLE)));

	/* generate our create sequence statement */
	CreateSeqStmt *createSeqStmt = makeNode(CreateSeqStmt);

	createSeqStmt->sequence = sequenceName;

	/* run it */
	DefineSequence(NULL, createSeqStmt);

	CommandCounterIncrement();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * Returns the oid for a given relationId rowid sequence, verifying
 * that it is a sequence.
 */
Oid
FindRelationRowIdSequence(Oid relationId)
{
	RangeVar   *sequenceName = RowIdSequenceGetRangeVar(relationId);
	bool		missingOk = false;
	Oid			sequenceId = RangeVarGetRelid(sequenceName, NoLock, missingOk);

	Assert(get_rel_relkind(sequenceId) == RELKIND_SEQUENCE);

	return sequenceId;
}



/*
 * Execute a drop of the row id sequence for the associated relation if it
 * exists; or do nothing if it does not.
 */
void
DropRowIdSequenceForRelation(Oid relationId)
{
	RangeVar   *sequenceName = RowIdSequenceGetRangeVar(relationId);

	/*
	 * Early exit for NoLock existence check; technically a race condition if
	 * we drop from multiple sessions at the same time, but since we do
	 * missing_ok one of them will get a NOTICE.  We thus avoid unnecessary
	 * NOTICES in the common case that we are just dropping a table without a
	 * rowid sequence.
	 */

	Oid			existingOid = RangeVarGetRelid(sequenceName, NoLock, true /* missing_ok */ );

	if (!OidIsValid(existingOid))
		return;

	DropStmt   *dropStmt = makeNode(DropStmt);

	/* unpack the schema/relation */
	dropStmt->objects = list_make1(list_make2(
											  makeString(sequenceName->schemaname),
											  makeString(sequenceName->relname)));
	dropStmt->removeType = OBJECT_SEQUENCE;
	dropStmt->missing_ok = true;

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	/* a sequence is a type of relation */
	RemoveRelations(dropStmt);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * CreateRowIdRangeForNewFile creates a row ID range starting at the current
 * sequence value for a given relation for `rowCount` rows.
 *
 * This is done with the assumption that the new file in the system does not
 * have corresponding rowids already; i.e., a brand-new data file registered for
 * the first time, not a compaction.
 *
 * While this does consume values from the row id sequence, this doesn't
 * actually care if these eventually get stored in the row_mapping table, nor
 * does it care what the underlying file represents, it merely assumes the
 * caller is not lying when it says it won't use more than rowCount rows for
 * its allocation.
 */
RowIdRangeMapping *
CreateRowIdRangeForNewFile(Oid relationId, int64 rowCount, int64 rowIdStart)
{
	Assert(rowCount > 0);

	/* setval checks permissions, so switch to superuser */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	if (rowIdStart == 0)
	{
		/* Lookup the rowid sequence owned by this iceberg table's */
		Oid			sequenceId = FindRelationRowIdSequence(relationId);

		/*
		 * Make sure we don't have other sequence callers while raising the
		 * sequence by rowCount.
		 */
		LockRelationOid(sequenceId, ExclusiveLock);

		/* draw a regular sequence number */
		bool		checkPermissions = false;

		rowIdStart = nextval_internal(sequenceId, checkPermissions);

		/* raise the sequence by rowCount - 1, since we already raised by 1 */
		DirectFunctionCall2(setval_oid,
							ObjectIdGetDatum(sequenceId),
							Int64GetDatum(rowIdStart + rowCount - 1));

		UnlockRelationOid(sequenceId, ExclusiveLock);
	}

	RowIdRangeMapping *rowIdRange = (RowIdRangeMapping *) palloc0(sizeof(RowIdRangeMapping));

	rowIdRange->rowStartId = rowIdStart;
	rowIdRange->rowStartNum = 0;
	rowIdRange->numRows = rowCount;

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return rowIdRange;
}


/*
 * Start our reservation for the underlying rowid range for the given file
 * number.  This will take a lock on the underlying sequence and returns the
 * current value.  Caller will need to finish the reservation (and release the
 * lock) when it knows how many rows it will need.
 */
int64
StartReservingRowIdRange(Oid icebergRelationId)
{
	/* Lookup the rowid sequence owned by this iceberg table's */
	Oid			sequenceId = FindRelationRowIdSequence(icebergRelationId);

	/* take a lock on sequence relation */
	LockRelationOid(sequenceId, ExclusiveLock);

	bool		checkPerms = false;

	return nextval_internal(sequenceId, checkPerms);
}


/*
 * Complete our reservation for the underlying rowid range for the given file
 * number by raising and unlocking the sequence.
 *
 * Since this can be called by unprivileged users via replication code paths,
 * we need to switch to the extension owner.
 */
void
FinishReservingRowIdRange(Oid icebergRelation, int64 rowIdOffset, int64 rowCount)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	Oid			sequenceOid = FindRelationRowIdSequence(icebergRelation);

	if (rowCount > 0)
	{

		/* raise the sequence by rowCount - 1, since we already raised by 1 */
		DirectFunctionCall2(setval_oid,
							ObjectIdGetDatum(sequenceOid),
							Int64GetDatum(rowIdOffset + rowCount - 1));
	}

	/* Release the sequence relation lock */
	UnlockRelationOid(sequenceOid, ExclusiveLock);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


#ifdef USE_ASSERT_CHECKING
/*
 * IsValidRowIdRangeList checks whether the ranges of a RowIdRangeList
 * are coherent.
 */
static bool
IsValidRowIdRangeList(List *rowIdRanges)
{
	ListCell   *rangeCell = NULL;

	int64		lastStartRowId = 0;
	int64		lastRowCount = 0;

	foreach(rangeCell, rowIdRanges)
	{
		RowIdRangeMapping *range = (RowIdRangeMapping *) lfirst(rangeCell);

		/* ensure the specific range is valid; monotonic and non-overlapping */
		if (range->rowStartId < lastStartRowId + lastRowCount)
			return false;

		/* valid number of row number; we do allow 0 */
		if (range->rowStartNum < 0)
			return false;

		/* valid number of rows; we do allow 0 */
		if (range->numRows < 0)
			return false;

		/* update our per-range counters */
		lastStartRowId = range->rowStartId;
		lastRowCount = range->numRows;
	}

	return true;
}


/*
 * GetTotalRowIdRangeRowCount returns the total number of rows in a list of
 * row ID ranges.
 */
int64
GetTotalRowIdRangeRowCount(List *rowIdRanges)
{
	ListCell   *rangeCell = NULL;

	int64		totalRowCount = 0;

	foreach(rangeCell, rowIdRanges)
	{
		RowIdRangeMapping *range = (RowIdRangeMapping *) lfirst(rangeCell);

		totalRowCount += range->numRows;
	}

	return totalRowCount;
}
#endif


/*
 * AddRowIdMaterializationToReadQuery expands a compaction query that calls
 * read_parquet (with filename and file_row_number) to add the row ID of each row.
 *
 * Input files that have been created as a result of compaction (through the query generated
 * by this function) will have an explicit _row_id value and we can copy those.
 *
 * Newly added files may not have a _row_id value, but will have a contiguous row ID
 * range that starts at row_id_start. For those rows, we join with a VALUES clause
 * that contains the row_id_start for each compacted file.
 */
char *
AddRowIdMaterializationToReadQuery(char *readQuery, Oid relationId, List *fileList)
{
	Assert(list_length(fileList) > 0);

	StringInfoData wrappedQuery;

	initStringInfo(&wrappedQuery);

	/*
	 * Files that were previously compacted will have _row_id set, but for new
	 * files we need to get it from the row_id_mappings.
	 */
	appendStringInfo(&wrappedQuery,
					 "select"
					 " data.* exclude(" INTERNAL_FILENAME_COLUMN_NAME ", file_row_number, _row_id), "
					 " coalesce(_row_id, source_files.row_id_start + file_row_number) as _row_id "
					 "from"
					 " (%s) data join (values",
					 readQuery
		);

	ListCell   *fileCell = NULL;

	foreach(fileCell, fileList)
	{
		TableDataFile *dataFile = lfirst(fileCell);

		appendStringInfo(&wrappedQuery, "%s (%s," INT64_FORMAT ")",
						 foreach_current_index(fileCell) > 0 ? "," : "",
						 quote_literal_cstr(dataFile->path),
						 dataFile->stats.rowIdStart);
	}

	appendStringInfo(&wrappedQuery,
					 ") source_files (filename, row_id_start) "
					 "on data." INTERNAL_FILENAME_COLUMN_NAME " = source_files.filename "
		);

	return wrappedQuery.data;
}


/*
 * GetRowIdMappingsFromFile determines the row ID mappings for a file
 * with a _row_id columns.
 */
List *
GetRowIdRangesFromFile(const char *path)
{
	char	   *query = psprintf(
								 "with row_ids as ("

	/*
	 * For each row ID and row number, also get the previous/next and whether
	 * its row ID is consecutive.
	 *
	 * Note: by not specifying ORDER BY in the window function, we preserve
	 * the file_row_number order and can use streaming window execution, which
	 * makes this query quite fast.
	 */
								 " select"
								 "  _row_id,"
								 "  file_row_number,"
								 "  lag(_row_id) over () as prev_row_id,"
								 "  lead(_row_id) over () as next_row_id,"
								 "  (prev_row_id is null or prev_row_id <> _row_id - 1) as is_lower_bound, "
								 "  (next_row_id is null or next_row_id <> _row_id + 1) as is_upper_bound "
								 " from read_parquet(%s, file_row_number=True)"
								 "), "

	/*
	 * Get lower and upper bounds of row ID ranges, add the upper bound to the
	 * lower bound as row_id_end.
	 *
	 * For single row ranges, both is_lower_bound and is_upper_bound are true,
	 * and we take the _row_id as both row_id_start and row_id_end.
	 *
	 * For multi-row ranges, we sort by _row_id (in the window function) such
	 * that lower bounds precede upper bounds. For the lower bounds we then
	 * take the next _row_id (i.e. the upper bound) as the row_id_end. Upper
	 * bounds are filtered out in the next phase.
	 */
								 "row_id_boundaries as ("
								 " select"
								 "  _row_id as row_id_start,"
								 "  case"
								 "   when is_lower_bound and is_upper_bound then _row_id"
								 "   when is_lower_bound then lead(_row_id) over (order by _row_id)"
								 "  end as row_id_end,"
								 "  file_row_number,"
								 "  is_lower_bound"
								 " from row_ids"
								 " where is_lower_bound or is_upper_bound"
								 ")"

	/*
	 * Filter out the upper bounds and compute the row ID ranges.
	 */
								 "select"
								 " row_id_start,"
								 " row_id_end - row_id_start + 1 as num_rows, "
								 " file_row_number as row_number_start "
								 "from row_id_boundaries "
								 "where is_lower_bound",
								 quote_literal_cstr(path)
		);

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query);

	/* throw error if anything failed  */
	CheckPGDuckResult(pgDuckConn, result);

	int			rowCount = PQntuples(result);
	List	   *rowMap = NIL;

	/* make sure we PQclear the result */
	PG_TRY();
	{
		for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			if (PQgetisnull(result, rowIndex, 0) ||
				PQgetisnull(result, rowIndex, 1) ||
				PQgetisnull(result, rowIndex, 2))
			{
				ereport(ERROR, (errmsg("unexpected NULL value in result set")));
			}

			RowIdRangeMapping *rowIdRange = palloc0(sizeof(RowIdRangeMapping));

			rowIdRange->rowStartId = pg_strtoint64(PQgetvalue(result, rowIndex, 0));
			rowIdRange->numRows = pg_strtoint64(PQgetvalue(result, rowIndex, 1));
			rowIdRange->rowStartNum = pg_strtoint64(PQgetvalue(result, rowIndex, 2));

			rowMap = lappend(rowMap, rowIdRange);
		}

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);

	Assert(IsValidRowIdRangeList(rowMap));

	return rowMap;
}


/*
 * AnalyzeRowIdMappings runs analyze lake_table.row_id_mappings.
 */
void
AnalyzeRowIdMappings()
{
	RangeVar   *rangeVar = makeRangeVar(PG_LAKE_TABLE_SCHEMA, ROW_ID_MAPPINGS_TABLE_NAME, -1);
	VacuumRelation *rowIdMappings = makeVacuumRelation(rangeVar, InvalidOid, NIL);

	VacuumStmt *analyzeStmt = makeNode(VacuumStmt);

	analyzeStmt->rels = list_make1(rowIdMappings);
	analyzeStmt->is_vacuumcmd = false;
	analyzeStmt->options = NIL;

	ExecuteInternalDDL((Node *) analyzeStmt, "ANALYZE " ROW_ID_MAPPINGS_QUALIFIED_TABLE_NAME);
}

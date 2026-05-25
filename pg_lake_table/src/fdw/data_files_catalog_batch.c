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
 * Bulk write path for lake_table.files and its satellite catalogs. Used by
 * ApplyDataFileCatalogChanges (data_files_catalog.c) to collapse a run of
 * adjacent same-typed metadata ops into one INSERT per catalog instead of
 * N per-row SPI calls. Today only DATA_FILE_ADD is bulked; the dispatcher
 * ApplyDataFileBatch is structured so adjacent DATA_FILE_REMOVE etc. can
 * slot in later without touching the orchestrator.
 */

#include "postgres.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/data_files_catalog_internal.h"
#include "pg_lake/fdw/data_file_stats_catalog.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/util/array_utils.h"

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"

/* path -> int64 file id, built from RETURNING output of ExecInsertDataFiles */
typedef struct FileIdHashEntry
{
	char		path[MAX_S3_PATH_LENGTH];
	int64		fileId;
}			FileIdHashEntry;

/* caller-above-callee forward decls for the statics below */
static void FlushDataFileAddBatch(Oid relationId, List *addOps);
static HTAB *BulkInsertDataFiles(Oid relationId, List *addOps);
static HTAB *ExecInsertDataFiles(Oid relationId, int fileCount,
								 ArrayType *pathArray, ArrayType *rowCountArray,
								 ArrayType *fileSizeArray, ArrayType *contentArray,
								 ArrayType *firstRowIdArray);
static void BulkInsertDataFileColumnStats(Oid relationId, List *addOps);
static int	AppendColumnStatsForFile(TableMetadataOperation * operation,
									 int statIndex, Datum *pathDatums,
									 Datum *fieldIdDatums, Datum *lowerDatums,
									 bool *lowerNulls, Datum *upperDatums,
									 bool *upperNulls);
static void ExecInsertDataFileColumnStats(Oid relationId, ArrayType *pathArray,
										  ArrayType *fieldIdArray,
										  ArrayType *lowerArray,
										  ArrayType *upperArray);
static void BulkInsertDataFilePartitionValues(Oid relationId, List *addOps,
											  HTAB *pathToFileId);
static int	AppendPartitionValuesForFile(TableMetadataOperation * operation,
										 int64 fileId, List *transforms,
										 int partitionRowIndex,
										 Datum *fileIdDatums,
										 Datum *partitionFieldIdDatums,
										 Datum *valueDatums, bool *valueNulls);
static void ExecInsertDataFilePartitionValues(Oid relationId,
											  ArrayType *fileIdArray,
											  ArrayType *partitionFieldIdArray,
											  ArrayType *valueArray);
static bool AddOpHasPartitionValues(TableMetadataOperation * operation);
static void BulkInsertTrackedFileIds(List *addOps, HTAB *pathToFileId);
static void ExecInsertTrackedFileIds(ArrayType *fileIdArray);
static void CreateTxDataFileIdsTempTableIfNotExists(void);
#ifdef USE_ASSERT_CHECKING
static void AssertAllOpsAreType(List *ops, TableMetadataOperationType type);
#endif


/* Dispatch a run of same-typed ops to the right per-type bulk SQL. */
void
ApplyDataFileBatch(Oid relationId, TableMetadataOperationType type, List *batch)
{
	if (batch == NIL)
		return;

	Assert(BatchableType(type));

	switch (type)
	{
		case DATA_FILE_ADD:
			FlushDataFileAddBatch(relationId, batch);
			break;
		default:

			/*
			 * Future bulk paths slot in as new cases here, e.g. a run of
			 * DATA_FILE_REMOVE collapsing to: DELETE FROM lake_table.files
			 * WHERE table_name = $1 AND path = ANY($2::text[]). When you add
			 * a case, also flip BatchableType to enable it.
			 */
			Assert(false);
	}
}


/* True iff adjacent ops of this type can be collapsed into one bulk SQL. */
bool
BatchableType(TableMetadataOperationType type)
{
	/*
	 * Only DATA_FILE_ADD today. Adjacent DATA_FILE_REMOVE is the obvious next
	 * case; ApplyDataFileBatch holds the matching SQL sketch.
	 */
	return type == DATA_FILE_ADD;
}


/*
 * Apply a run of DATA_FILE_ADD ops via bulk INSERTs into the three lake_table
 * catalogs (files, data_file_column_stats, data_file_partition_values) plus
 * the optional tx_data_file_ids temp table when PgLakeAddDataFileHook opts
 * in. Pays O(catalogs) SPI round trips instead of O(files * (1 + columns +
 * partition_fields)).
 */
static void
FlushDataFileAddBatch(Oid relationId, List *addOps)
{
	if (addOps == NIL)
		return;

#ifdef USE_ASSERT_CHECKING
	AssertAllOpsAreType(addOps, DATA_FILE_ADD);
#endif

	/*
	 * BulkInsertDataFiles runs first; RETURNING captures the
	 * sequence-assigned ids into a path->id hash. The downstream helpers use
	 * that hash for O(1) id lookup instead of JOINing back to
	 * lake_table.files.
	 */
	HTAB	   *pathToFileId = BulkInsertDataFiles(relationId, addOps);

	BulkInsertDataFileColumnStats(relationId, addOps);
	BulkInsertDataFilePartitionValues(relationId, addOps, pathToFileId);
	BulkInsertTrackedFileIds(addOps, pathToFileId);
	hash_destroy(pathToFileId);
}


/*
 * Build parallel arrays from addOps and bulk-insert them into lake_table.files.
 * Returns a path->int64 HTAB of sequence-assigned file ids (via RETURNING).
 * Caller must hash_destroy() the returned table when done.
 */
static HTAB *
BulkInsertDataFiles(Oid relationId, List *addOps)
{
	int			fileCount = list_length(addOps);

	Assert(fileCount > 0);

	Datum	   *pathDatums = palloc(sizeof(Datum) * fileCount);
	Datum	   *rowCountDatums = palloc(sizeof(Datum) * fileCount);
	Datum	   *fileSizeDatums = palloc(sizeof(Datum) * fileCount);
	Datum	   *contentDatums = palloc(sizeof(Datum) * fileCount);
	Datum	   *firstRowIdDatums = palloc(sizeof(Datum) * fileCount);
	bool	   *firstRowIdNulls = palloc(sizeof(bool) * fileCount);
	int			fileIndex = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);
		int64		firstRowId = operation->dataFileStats.rowIdStart;

		pathDatums[fileIndex] = CStringGetTextDatum(operation->path);
		rowCountDatums[fileIndex] = Int64GetDatum(operation->dataFileStats.rowCount);
		fileSizeDatums[fileIndex] = Int64GetDatum(operation->dataFileStats.fileSize);
		contentDatums[fileIndex] = Int32GetDatum((int) operation->content);
		firstRowIdNulls[fileIndex] = (firstRowId == INVALID_ROW_ID);
		firstRowIdDatums[fileIndex] = Int64GetDatum(firstRowId);

		fileIndex++;
	}
	Assert(fileIndex == fileCount);

	ArrayType  *pathArray = MakeArrayFromDatums(pathDatums, NULL, fileCount, TEXTOID);
	ArrayType  *rowCountArray = MakeArrayFromDatums(rowCountDatums, NULL, fileCount, INT8OID);
	ArrayType  *fileSizeArray = MakeArrayFromDatums(fileSizeDatums, NULL, fileCount, INT8OID);
	ArrayType  *contentArray = MakeArrayFromDatums(contentDatums, NULL, fileCount, INT4OID);
	ArrayType  *firstRowIdArray = MakeArrayFromDatums(firstRowIdDatums, firstRowIdNulls,
													  fileCount, INT8OID);

	return ExecInsertDataFiles(relationId, fileCount, pathArray, rowCountArray,
							   fileSizeArray, contentArray, firstRowIdArray);
}


/*
 * INSERT into lake_table.files via unnest and return a path->id HTAB built
 * from the RETURNING clause. fileCount is the number of rows inserted.
 * Must be called outside any SPI session.
 * Caller must hash_destroy() the returned table when done.
 */
static HTAB *
ExecInsertDataFiles(Oid relationId, int fileCount, ArrayType *pathArray,
					ArrayType *rowCountArray, ArrayType *fileSizeArray,
					ArrayType *contentArray, ArrayType *firstRowIdArray)
{
	MemoryContext callerCxt = CurrentMemoryContext;

	/*
	 * RETURNING id, path captures sequence-assigned ids so downstream helpers
	 * can do O(1) hash lookups instead of JOINing back to lake_table.files.
	 *
	 * No ::text[] / ::int8[] casts on the $N placeholders: SPI_ARG_VALUE
	 * already declares each parameter's type to the planner.
	 *
	 * Multi-argument unnest in FROM is SQL syntax, not a resolvable
	 * pg_catalog.unnest(...) overload, so keep unnest unqualified here.
	 */
	char	   *query =
		"INSERT INTO " DATA_FILES_TABLE_QUALIFIED " "
		"(table_name, path, row_count, file_size, content, first_row_id) "
		"SELECT $1, path, row_count, file_size, content, first_row_id "
		"FROM unnest($2, $3, $4, $5, $6) "
		"AS t(path, row_count, file_size, content, first_row_id) "
		"RETURNING id, path";

	DECLARE_SPI_ARGS(6);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTARRAYOID, pathArray, false);
	SPI_ARG_VALUE(3, INT8ARRAYOID, rowCountArray, false);
	SPI_ARG_VALUE(4, INT8ARRAYOID, fileSizeArray, false);
	SPI_ARG_VALUE(5, INT4ARRAYOID, contentArray, false);
	SPI_ARG_VALUE(6, INT8ARRAYOID, firstRowIdArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);
	SPI_EXECUTE(query, /* readOnly = */ false);

	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = MAX_S3_PATH_LENGTH;
	hashCtl.entrysize = sizeof(FileIdHashEntry);
	hashCtl.hcxt = callerCxt;

	HTAB	   *pathToFileId = hash_create("inserted file ids by path",
										   fileCount * 2,
										   &hashCtl,
										   HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	for (int fileIndex = 0; fileIndex < fileCount; fileIndex++)
	{
		bool		idIsNull;
		bool		pathIsNull;

		/* column 1 = id (int8), column 2 = path (text) */
		int64		fileId = GET_SPI_VALUE(INT8OID, fileIndex, 1, &idIsNull);
		char	   *path = GET_SPI_VALUE(TEXTOID, fileIndex, 2, &pathIsNull);

		Assert(!idIsNull && !pathIsNull);

		bool		found;
		FileIdHashEntry *entry = (FileIdHashEntry *)
			hash_search(pathToFileId, path, HASH_ENTER, &found);

		Assert(!found);			/* each path is unique in lake_table.files */
		entry->fileId = fileId;
	}

	SPI_END();

	return pathToFileId;
}


/*
 * Build parallel arrays from addOps and bulk-insert column stats into
 * lake_table.data_file_column_stats. Rows with NULL bounds are skipped
 * entirely, matching the per-row helper.
 */
static void
BulkInsertDataFileColumnStats(Oid relationId, List *addOps)
{
	int			statCapacity = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->content != CONTENT_DATA)
			continue;

		statCapacity += list_length(operation->dataFileStats.columnStats);
	}

	if (statCapacity == 0)
		return;

	Datum	   *pathDatums = palloc(sizeof(Datum) * statCapacity);
	Datum	   *fieldIdDatums = palloc(sizeof(Datum) * statCapacity);
	Datum	   *lowerDatums = palloc(sizeof(Datum) * statCapacity);
	bool	   *lowerNulls = palloc(sizeof(bool) * statCapacity);
	Datum	   *upperDatums = palloc(sizeof(Datum) * statCapacity);
	bool	   *upperNulls = palloc(sizeof(bool) * statCapacity);
	int			statIndex = 0;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->content != CONTENT_DATA)
			continue;

		statIndex = AppendColumnStatsForFile(operation, statIndex,
											 pathDatums, fieldIdDatums,
											 lowerDatums, lowerNulls,
											 upperDatums, upperNulls);
	}
	Assert(statIndex <= statCapacity);

	if (statIndex == 0)
		return;

	ArrayType  *pathArray = MakeArrayFromDatums(pathDatums, NULL, statIndex, TEXTOID);
	ArrayType  *fieldIdArray = MakeArrayFromDatums(fieldIdDatums, NULL, statIndex, INT8OID);
	ArrayType  *lowerArray = MakeArrayFromDatums(lowerDatums, lowerNulls, statIndex, TEXTOID);
	ArrayType  *upperArray = MakeArrayFromDatums(upperDatums, upperNulls, statIndex, TEXTOID);

	ExecInsertDataFileColumnStats(relationId, pathArray, fieldIdArray,
								  lowerArray, upperArray);
}


/*
 * Append column stats for one file to the parallel arrays starting at
 * statIndex. Skips stats with NULL bounds (same as per-row behavior).
 * Returns the updated statIndex.
 */
static int
AppendColumnStatsForFile(TableMetadataOperation * operation, int statIndex,
						 Datum *pathDatums, Datum *fieldIdDatums,
						 Datum *lowerDatums, bool *lowerNulls,
						 Datum *upperDatums, bool *upperNulls)
{
	ListCell   *statsCell = NULL;

	foreach(statsCell, operation->dataFileStats.columnStats)
	{
		DataFileColumnStats *columnStats = lfirst(statsCell);

		/*
		 * Match the per-row code: skip rows with NULL bounds entirely rather
		 * than emitting (NULL, NULL).
		 */
		if (columnStats->lowerBoundText == NULL)
		{
			Assert(columnStats->upperBoundText == NULL);
			continue;
		}

		pathDatums[statIndex] = CStringGetTextDatum(operation->path);
		fieldIdDatums[statIndex] = Int64GetDatum(columnStats->leafField.fieldId);
		lowerDatums[statIndex] = CStringGetTextDatum(columnStats->lowerBoundText);
		lowerNulls[statIndex] = false;
		upperDatums[statIndex] = (columnStats->upperBoundText != NULL)
			? CStringGetTextDatum(columnStats->upperBoundText)
			: (Datum) 0;
		upperNulls[statIndex] = (columnStats->upperBoundText == NULL);

		statIndex++;
	}

	return statIndex;
}


/* INSERT into lake_table.data_file_column_stats via unnest. */
static void
ExecInsertDataFileColumnStats(Oid relationId, ArrayType *pathArray,
							  ArrayType *fieldIdArray, ArrayType *lowerArray,
							  ArrayType *upperArray)
{
	/*
	 * See ExecInsertDataFiles: multi-arg unnest cannot be
	 * pg_catalog-qualified.
	 */
	char	   *query =
		"INSERT INTO " DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " "
		"(table_name, path, field_id, lower_bound, upper_bound) "
		"SELECT $1, path, field_id, lower_bound, upper_bound "
		"FROM unnest($2, $3, $4, $5) "
		"AS t(path, field_id, lower_bound, upper_bound)";

	DECLARE_SPI_ARGS(5);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTARRAYOID, pathArray, false);
	SPI_ARG_VALUE(3, INT8ARRAYOID, fieldIdArray, false);
	SPI_ARG_VALUE(4, TEXTARRAYOID, lowerArray, false);
	SPI_ARG_VALUE(5, TEXTARRAYOID, upperArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);
	SPI_EXECUTE(query, /* readOnly = */ false);
	SPI_END();
}


/*
 * Build parallel arrays from addOps and bulk-insert into
 * lake_table.data_file_partition_values. File ids are resolved via
 * pathToFileId (built from RETURNING in ExecInsertDataFiles).
 * AllPartitionTransformList is called once per batch rather than per file.
 */
static void
BulkInsertDataFilePartitionValues(Oid relationId, List *addOps, HTAB *pathToFileId)
{
	int			partitionRowCapacity = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (AddOpHasPartitionValues(operation))
			partitionRowCapacity += operation->partition->fields_length;
	}

	if (partitionRowCapacity == 0)
		return;

	/*
	 * Per-row helper called AllPartitionTransformList once per file; hoist
	 * the lookup to once per batch.
	 */
	List	   *transforms = AllPartitionTransformList(relationId);

	Datum	   *fileIdDatums = palloc(sizeof(Datum) * partitionRowCapacity);
	Datum	   *partitionFieldIdDatums = palloc(sizeof(Datum) * partitionRowCapacity);
	Datum	   *valueDatums = palloc(sizeof(Datum) * partitionRowCapacity);
	bool	   *valueNulls = palloc(sizeof(bool) * partitionRowCapacity);
	int			partitionRowIndex = 0;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (!AddOpHasPartitionValues(operation))
			continue;

		Assert(operation->partitionSpecId != DEFAULT_SPEC_ID);

		FileIdHashEntry *entry = (FileIdHashEntry *)
			hash_search(pathToFileId, operation->path, HASH_FIND, NULL);

		Assert(entry != NULL);

		partitionRowIndex = AppendPartitionValuesForFile(operation, entry->fileId,
														 transforms,
														 partitionRowIndex,
														 fileIdDatums,
														 partitionFieldIdDatums,
														 valueDatums, valueNulls);
	}
	Assert(partitionRowIndex == partitionRowCapacity);

	ArrayType  *fileIdArray = MakeArrayFromDatums(fileIdDatums, NULL,
												  partitionRowIndex, INT8OID);
	ArrayType  *partitionFieldIdArray = MakeArrayFromDatums(partitionFieldIdDatums, NULL,
															partitionRowIndex, INT4OID);
	ArrayType  *valueArray = MakeArrayFromDatums(valueDatums, valueNulls,
												 partitionRowIndex, TEXTOID);

	ExecInsertDataFilePartitionValues(relationId, fileIdArray,
									  partitionFieldIdArray, valueArray);
}


/*
 * Append one row per partition field for a single file to the parallel arrays
 * starting at partitionRowIndex. Returns the updated partitionRowIndex.
 */
static int
AppendPartitionValuesForFile(TableMetadataOperation * operation, int64 fileId,
							 List *transforms, int partitionRowIndex,
							 Datum *fileIdDatums, Datum *partitionFieldIdDatums,
							 Datum *valueDatums, bool *valueNulls)
{
	int			fieldCount = (int) operation->partition->fields_length;

	for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++)
	{
		PartitionField *partitionField = &operation->partition->fields[fieldIndex];
		bool		errorIfMissing = true;

		IcebergPartitionTransform *transform =
			FindPartitionTransformById(transforms, partitionField->field_id,
									   errorIfMissing);

		const char *partitionValue =
			SerializePartitionValueToPGText(partitionField->value,
											partitionField->value_length,
											transform);

		fileIdDatums[partitionRowIndex] = Int64GetDatum(fileId);
		partitionFieldIdDatums[partitionRowIndex] = Int32GetDatum(partitionField->field_id);
		if (partitionValue == NULL)
		{
			valueDatums[partitionRowIndex] = (Datum) 0;
			valueNulls[partitionRowIndex] = true;
		}
		else
		{
			valueDatums[partitionRowIndex] = CStringGetTextDatum(partitionValue);
			valueNulls[partitionRowIndex] = false;
		}

		partitionRowIndex++;
	}

	return partitionRowIndex;
}


/* INSERT into lake_table.data_file_partition_values via unnest. */
static void
ExecInsertDataFilePartitionValues(Oid relationId, ArrayType *fileIdArray,
								  ArrayType *partitionFieldIdArray,
								  ArrayType *valueArray)
{
	/*
	 * See ExecInsertDataFiles: multi-arg unnest cannot be
	 * pg_catalog-qualified.
	 */
	char	   *query =
		"INSERT INTO " DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED " "
		"(table_name, id, partition_field_id, value) "
		"SELECT $1, id, partition_field_id, value "
		"FROM unnest($2, $3, $4) "
		"AS t(id, partition_field_id, value)";

	DECLARE_SPI_ARGS(4);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT8ARRAYOID, fileIdArray, false);
	SPI_ARG_VALUE(3, INT4ARRAYOID, partitionFieldIdArray, false);
	SPI_ARG_VALUE(4, TEXTARRAYOID, valueArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);
	SPI_EXECUTE(query, /* readOnly = */ false);
	SPI_END();
}


/* Only DATA and POSITION_DELETES content can carry partition values. */
static bool
AddOpHasPartitionValues(TableMetadataOperation * operation)
{
	if (operation->partition == NULL)
		return false;
	if (operation->partition->fields_length == 0)
		return false;
	if (operation->content != CONTENT_DATA &&
		operation->content != CONTENT_POSITION_DELETES)
		return false;
	return true;
}


/*
 * Collect file ids opted in by PgLakeAddDataFileHook and insert them into
 * the tx-scoped temp table. The hook fires once per CONTENT_DATA op; ids are
 * resolved via pathToFileId rather than a JOIN to lake_table.files.
 */
static void
BulkInsertTrackedFileIds(List *addOps, HTAB *pathToFileId)
{
	if (PgLakeAddDataFileHook == NULL)
		return;

	/*
	 * Lazy palloc: most batches won't have any hook-tracked files (the hook
	 * only fires for CONTENT_DATA ops and only when the sibling extension
	 * opts in). Defer the allocation to the first hit so partition-only or
	 * deletes-only runs don't pay for an array they'll never use.
	 */
	int			fileCount = list_length(addOps);
	Datum	   *fileIdDatums = NULL;
	int			trackedFileCount = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->content != CONTENT_DATA)
			continue;
		if (!PgLakeAddDataFileHook())
			continue;

		if (fileIdDatums == NULL)
			fileIdDatums = palloc(sizeof(Datum) * fileCount);

		FileIdHashEntry *entry = (FileIdHashEntry *)
			hash_search(pathToFileId, operation->path, HASH_FIND, NULL);

		Assert(entry != NULL);
		fileIdDatums[trackedFileCount++] = Int64GetDatum(entry->fileId);
	}

	if (trackedFileCount == 0)
		return;

	CreateTxDataFileIdsTempTableIfNotExists();

	ArrayType  *fileIdArray = MakeArrayFromDatums(fileIdDatums, NULL,
												  trackedFileCount, INT8OID);

	ExecInsertTrackedFileIds(fileIdArray);
}


/* INSERT into the tx-scoped temp table via unnest. */
static void
ExecInsertTrackedFileIds(ArrayType *fileIdArray)
{
	char	   *query =
		"INSERT INTO " TX_DATA_FILES_QUALIFIED_TABLE_NAME " (id) "
		"SELECT id FROM pg_catalog.unnest($1) AS t(id)";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, INT8ARRAYOID, fileIdArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);
	SPI_EXECUTE(query, /* readOnly = */ false);
	SPI_END();
}


/*
 * Lazily create the per-tx tracker temp table; rows are auto-dropped at COMMIT.
 *
 * PostgreSQL forbids CREATE TEMP TABLE under SECURITY_RESTRICTED_OPERATION,
 * so we use the SPI_START_EXTENSION_OWNER_ALLOWING_TEMP_OBJECTS variant which
 * omits the restricted-op flag.  The search_path lockdown stays in effect;
 * the DDL is a fixed string with no caller-supplied input.
 */
static void
CreateTxDataFileIdsTempTableIfNotExists(void)
{
	const char *query =
		"create temporary table if not exists " TX_DATA_FILES_QUALIFIED_TABLE_NAME " "
		"(id bigint primary key) USING heap ON COMMIT DELETE ROWS;";

	SPI_START_EXTENSION_OWNER_ALLOWING_TEMP_OBJECTS(PgLakeTable);
	SPI_execute(query, /* readOnly = */ false, 0);
	SPI_END();
}


#ifdef USE_ASSERT_CHECKING
/* Belt-and-suspenders: every op in a batch should already be of `type`. */
static void
AssertAllOpsAreType(List *ops, TableMetadataOperationType type)
{
	ListCell   *opCell = NULL;

	foreach(opCell, ops)
	{
		TableMetadataOperation *operation = lfirst(opCell);

		Assert(operation->type == type);
	}
}
#endif

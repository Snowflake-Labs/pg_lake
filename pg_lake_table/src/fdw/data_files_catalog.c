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
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/fdw/catalog/row_id_mappings.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/data_file_stats_catalog.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/pgduck/delete_data.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/util/array_utils.h"
#include "pg_lake/util/plan_cache.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/util/string_utils.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define DELETION_FILE_MAP_TABLE PG_LAKE_TABLE_SCHEMA ".deletion_file_map"
#define DATA_FILES_ID_SEQUENCE_NAME "files_id_seq"
#define TX_DATA_FILES_QUALIFIED_TABLE_NAME PG_LAKE_TABLE_SCHEMA "tx_data_file_ids"


/* global hook override */
PgLakeAddDataFileHookType PgLakeAddDataFileHook = NULL;


static void FillDataFileColumnStats(TableDataFile * dataFile, int64 fieldId, int rowIndex);
static void FillPartitionFieldFromCatalog(TableDataFile * dataFile, List *partitionTransforms,
										  int64 partitionFieldId, int rowIndex);
static void AddDeletionFileMapping(Oid relationId, const char *path,
								   const char *sourcePath);
static void AddNewRowIdMapping(Oid relationId, const char *path, List *rowIdRanges);
static int64 GetFileIdForPath(Oid relatoinId, const char *path);
static void UpdateDeletedRowCount(Oid relationId, const char *path, int64 deletedRowCount);
static void RemoveDataFileFromTable(Oid relationId, const char *path);
static void RemoveAllDataFilesFromCatalog(Oid relationId);
static HTAB *CreateDataFilesHash(void);
static HTAB *CreateDataFilesByPathHash(void);
static List *TableDataFileHashToList(HTAB *dataFiles);
static bool ColumnStatAlreadyAdded(List *columnStats, int64 fieldId);
static bool PartitionFieldAlreadyAdded(Partition * partition, int64 fieldId);
static void CreateTxDataFileIdsTempTableIfNotExists(void);

/* CreateDataFileColumnStats is exported for the commit-time tracker fast path */

/* Bulk-add path forward decls */
static int64 *BuildBatchFileIds(int count);
static ArrayType *MakeArrayFromDatums(Datum *datums, bool *nulls, int count, Oid elementType);
static void BulkInsertDataFiles(Oid relationId, List *addOps, int64 *fileIds);
static void BulkInsertDataFileColumnStats(Oid relationId, List *addOps);
static bool AddOpHasPartitionValues(TableMetadataOperation * operation);
static void BulkInsertDataFilePartitionValues(Oid relationId, List *addOps, int64 *fileIds);
static void BulkInsertTrackedFileIds(List *addOps, int64 *fileIds);
static void FlushDataFileAddBatch(Oid relationId, List *addOps);

/*
 * GetTableDataFilesFromCatalog returns a list of TableDataFile for each data and deletion file
 * in the table. If dataOnly is true, only data files are returned. The optional snapshot
 * can be used to get a consistent view of the catalog.
 * It returns the data files that were updated before the given timestamp.
 */
List *
GetTableDataFilesFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
							 bool forUpdate, char *orderBy, Snapshot snapshot)
{
	List	   *partitionTransforms = AllPartitionTransformList(relationId);

	HTAB	   *dataFilesHash = GetTableDataFilesHashFromCatalog(relationId, dataOnly,
																 newFilesOnly, forUpdate,
																 orderBy, snapshot,
																 partitionTransforms,
																 false /* skipColumnStats */ );

	List	   *dataFiles = TableDataFileHashToList(dataFilesHash);

	return dataFiles;
}


/*
 * GetTableDataFilesHashFromCatalog returns a hash of path => TableDataFile for
 * the given table.
 *
 * If dataOnly is true, position deletes are excluded.
 * If forUpdate is true, files are locked with FOR UPDATE.
 * If newFilesOnly is true, only data files that are added in the current transaction are returned.
 * If orderBy is not null, it is used to sort results.
 * If snapshot is set, it is used for the query.
 * If skipColumnStats is true, the per-column min/max stats are not loaded.
 * Callers that only need file-level info (path, id, row count, partition) can
 * pass true to avoid the expensive join against data_file_column_stats; stats
 * can be loaded on demand for a subset of files via LoadColumnStatsForFiles().
 */
HTAB *
GetTableDataFilesHashFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
								 bool forUpdate, char *orderBy, Snapshot snapshot,
								 List *partitionTransforms, bool skipColumnStats)
{
	MemoryContext callerContext = CurrentMemoryContext;

	HTAB	   *dataFilesHash = CreateDataFilesHash();

	StringInfoData metadataQuery;

	initStringInfo(&metadataQuery);

	appendStringInfoString(&metadataQuery,
						   "select "
						    /* 1 */ "f.id, "
						    /* 2 */ "f.path, "
						    /* 3 */ "f.content, "
						    /* 4 */ "f.row_count, "
						    /* 5 */ "f.file_size, "
						    /* 6 */ "f.deleted_row_count, "
						    /* 7 */ "f.updated_time, "
						    /* 8 */ "f.first_row_id, ");

	if (!skipColumnStats)
		appendStringInfoString(&metadataQuery,
							    /* 9 */ "sma.field_id, "
							    /* 10 */ "sma.field_pg_type, "
							    /* 11 */ "sma.field_pg_typemod, "
							    /* 12 */ "sma.lower_bound, "
							    /* 13 */ "sma.upper_bound, ");
	else
		appendStringInfoString(&metadataQuery,
							   "NULL::bigint, NULL::oid, NULL::int4, "
							   "NULL::text, NULL::text, ");

	appendStringInfoString(&metadataQuery,
						    /* 14 */ "p.partition_field_id, "
						    /* 15 */ "p.partition_field_name, "
						    /* 16 */ "p.value, "
						    /* 17 */ "p.spec_id "
						   "from (");

	appendStringInfoString(&metadataQuery,
						   "select * from " DATA_FILES_TABLE_QUALIFIED " "
						   "where table_name OPERATOR(pg_catalog.=) $1");

	if (dataOnly)
		appendStringInfo(&metadataQuery, " and content OPERATOR(pg_catalog.=) %d", (int) CONTENT_DATA);

	if (newFilesOnly)
		appendStringInfoString(&metadataQuery, " and id IN (select id from " TX_DATA_FILES_QUALIFIED_TABLE_NAME ")");

	if (forUpdate)
		appendStringInfoString(&metadataQuery, " for update");

	/*
	 * not all tables (or all columns) have the stats. For example, iceberg
	 * tables created before we added this catalog or data types that do not
	 * have min/max or pg_lake tables.
	 */
	appendStringInfoString(&metadataQuery,
						   ") f "
						   "LEFT JOIN (" DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED
						   " JOIN " PARTITION_FIELDS_TABLE_QUALIFIED
						   " USING (table_name, partition_field_id) "
						   ") p USING (table_name, id) ");

	if (!skipColumnStats)
		appendStringInfoString(&metadataQuery,
							   "LEFT JOIN ("
							   DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " s "
							   "JOIN " MAPPING_TABLE_NAME
							   " m USING (table_name, field_id) "
							   "JOIN pg_attribute a ON (a.attrelid OPERATOR(pg_catalog.=) m.table_name "
							   "                       AND a.attnum   OPERATOR(pg_catalog.=) m.pg_attnum "
							   "                       AND NOT a.attisdropped)"
							   ") sma USING (table_name, path)");


	if (orderBy != NULL)
		appendStringInfo(&metadataQuery, " order by %s",
						 quote_identifier(orderBy));


	/*
	 * Although this is a read-only query when !forUpdate, we need the
	 * execution to use the current transaction's snapshot (e.g.,
	 * GetTransactionSnapshot()) to get the snapshot that the current
	 * transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	if (!snapshot)
	{
		SPI_EXECUTE(metadataQuery.data, readOnly);
	}
	else
	{
		SPIPlanPtr	qplan = GetCachedQueryPlan(metadataQuery.data, spiArgCount, spiArgTypes);

		if (qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s while fetching metadata",
				 SPI_result_code_string(SPI_result));

		bool		fireTriggers = true;
		int			spi_result =
			SPI_execute_snapshot(qplan,
								 spiArgValues, spiArgNulls,
								 snapshot,
								 InvalidSnapshot,
								 readOnly, fireTriggers, 0);

		/* Check result */
		if (spi_result != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	}

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		bool		isFileNull = false;
		int64		fileId = GET_SPI_VALUE(INT8OID, rowIndex, 1, &isFileNull);

		bool		fileFound = false;
		TableDataFile *dataFile = hash_search(dataFilesHash, &fileId, HASH_ENTER, &fileFound);

		/*
		 * single file can have stats for many columns, or can have multiple
		 * partition values. Track the file in the map, and add stats or
		 * partition values when needed.
		 */
		if (!fileFound)
		{
			dataFile->fileId = fileId;

			bool		isPathNull = false;

			dataFile->path = GET_SPI_VALUE(TEXTOID, rowIndex, 2, &isPathNull);

			bool		isContentNull = false;

			dataFile->content = (DataFileContent) GET_SPI_VALUE(INT4OID, rowIndex, 3, &isContentNull);

			bool		isRowCountNull = false;

			dataFile->stats.rowCount = GET_SPI_VALUE(INT8OID, rowIndex, 4, &isRowCountNull);

			bool		isFileSizeNull = false;

			dataFile->stats.fileSize = GET_SPI_VALUE(INT8OID, rowIndex, 5, &isFileSizeNull);

			bool		isDeletedRowCountNull = false;

			dataFile->stats.deletedRowCount = GET_SPI_VALUE(INT8OID, rowIndex, 6, &isDeletedRowCountNull);

			bool		isCreationTimeNull = false;

			dataFile->stats.creationTime = GET_SPI_VALUE(TIMESTAMPTZOID, rowIndex, 7, &isCreationTimeNull);

			dataFile->stats.columnStats = NIL;

			bool		isRowIdStartNull = false;

			dataFile->stats.rowIdStart = GET_SPI_VALUE(INT8OID, rowIndex, 8, &isRowIdStartNull);

			if (isRowIdStartNull)
				dataFile->stats.rowIdStart = INVALID_ROW_ID;

			/*
			 * As a convention, we always have a Partition for any data file,
			 * but until we have a partition, we set it to NULL.
			 */
			dataFile->partition = palloc0(sizeof(Partition));
			dataFile->partition->fields_length = 0;
			dataFile->partition->fields = NULL;
			dataFile->partitionSpecId = DEFAULT_SPEC_ID;
		}

		bool		isFieldIdNull = false;
		Datum		fieldIdDatum = GET_SPI_DATUM(rowIndex, 9, &isFieldIdNull);

		/*
		 * when field id is not empty, this means we have column stats in the
		 * row.
		 */
		if (!isFieldIdNull)
		{
			int64		fieldId = DatumGetInt64(fieldIdDatum);

			if (!ColumnStatAlreadyAdded(dataFile->stats.columnStats, fieldId))
			{
				FillDataFileColumnStats(dataFile, fieldId, rowIndex);
			}
		}

		/*
		 * When there is a partition field id, we have a partition value in
		 * the row, so add to the partition.
		 */
		bool		isPartitionFieldIdNull = false;
		Datum		partitionFieldIdDatum = GET_SPI_DATUM(rowIndex, 14, &isPartitionFieldIdNull);

		if (!isPartitionFieldIdNull)
		{
			int64		partitionFieldId = DatumGetInt64(partitionFieldIdDatum);

			if (!PartitionFieldAlreadyAdded(dataFile->partition, partitionFieldId))
			{
				FillPartitionFieldFromCatalog(dataFile, partitionTransforms, partitionFieldId, rowIndex);
			}
		}

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return dataFilesHash;
}


/*
 * GetTableDataFilesByPathHashFromCatalog retrieves the data files for a given table
 * and returns a hash table indexed by file path.
 *
 * See GetTableDataFilesHashFromCatalog for the meaning of skipColumnStats.
 */
HTAB *
GetTableDataFilesByPathHashFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
									   bool forUpdate, char *orderBy, Snapshot snapshot,
									   List *partitionTransforms, bool skipColumnStats)
{
	HTAB	   *filesById = GetTableDataFilesHashFromCatalog(relationId, dataOnly, newFilesOnly,
															 forUpdate, orderBy, snapshot,
															 partitionTransforms, skipColumnStats);

	HTAB	   *filesByPath = CreateDataFilesByPathHash();

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, filesById);

	TableDataFile *dataFile = NULL;
	bool		found = false;

	while ((dataFile = hash_seq_search(&status)) != NULL)
	{
		TableDataFileHashEntry *dataFileEntry = hash_search(filesByPath, dataFile->path, HASH_ENTER, &found);

		if (found)
			elog(ERROR, "duplicate data file path found in catalog: %s", dataFile->path);

		dataFileEntry->dataFile = *dataFile;
	}

	return filesByPath;
}


/*
 * PathToDataFilePtrHashEntry is the entry type for the local
 * path -> TableDataFile* hash used by LoadColumnStatsForFiles to dispatch
 * each (path, field_id) stats row to its caller-owned data file struct
 * in O(1).
 *
 * We can't reuse TableDataFileHashEntry from data_files_catalog.h because
 * that entry embeds the TableDataFile by value and is filled from a SELECT,
 * whereas here we need to mutate the *caller's* TableDataFile (its
 * stats.columnStats list) in place. Hence: a separate entry that just holds
 * a pointer.
 */
typedef struct PathToDataFilePtrHashEntry
{
	char		filePath[MAX_S3_PATH_LENGTH];
	TableDataFile *dataFile;
}			PathToDataFilePtrHashEntry;


/*
 * BuildPathToDataFilePtrHash returns a path-keyed hash that maps each
 * TableDataFile in dataFiles to its pointer in the caller's list. Used by
 * LoadColumnStatsForFiles to look up the target data file in O(1) instead of
 * walking the entire dataFiles list per SPI result row.
 *
 * The hash is sized for the input list; callers are expected to hash_destroy()
 * it when done.
 */
static HTAB *
BuildPathToDataFilePtrHash(List *dataFiles)
{
	HASHCTL		ctl = {0};

	ctl.keysize = MAX_S3_PATH_LENGTH;
	ctl.entrysize = sizeof(PathToDataFilePtrHashEntry);
	ctl.hcxt = CurrentMemoryContext;

	HTAB	   *hash = hash_create("LoadColumnStatsForFiles path lookup",
								   Max(list_length(dataFiles), 16),
								   &ctl,
								   HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	ListCell   *fileCell = NULL;

	foreach(fileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(fileCell);

		PathToDataFilePtrHashEntry *entry =
			(PathToDataFilePtrHashEntry *) hash_search(hash, dataFile->path,
													   HASH_ENTER, NULL);

		entry->dataFile = dataFile;
	}

	return hash;
}


/*
 * LoadColumnStatsForFiles fills in per-column min/max stats for the given
 * data files, targeting only their paths. This lets callers pair a stats-free
 * bulk read (GetTableDataFilesHashFromCatalog with skipColumnStats=true) with
 * a narrow stats load for just the files that actually need them (e.g. the
 * newly added files in a transaction).
 *
 * The dataFiles list must contain TableDataFile pointers whose columnStats
 * are currently NIL; this function appends to dataFile->stats.columnStats.
 *
 * The SPI query returns one row per (path, field_id); to dispatch each row
 * to the right caller-owned struct we build a path -> TableDataFile* hash
 * up front. The dominant cost on a large pre-commit (tens of thousands of
 * files, half a dozen stats columns each) is this dispatch, so O(1) hash
 * lookups here turn an O(N^2) fill loop into O(N * C) with N files and C
 * columns.
 */
void
LoadColumnStatsForFiles(Oid relationId, List *dataFiles)
{
	if (dataFiles == NIL)
		return;

	MemoryContext callerContext = CurrentMemoryContext;

	HTAB	   *filesByPath = BuildPathToDataFilePtrHash(dataFiles);

	List	   *pathList = NIL;
	ListCell   *fileCell = NULL;

	foreach(fileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(fileCell);

		pathList = lappend(pathList, dataFile->path);
	}

	char	   *query =
		"select "
		 /* 1 */ "s.path, "
		 /* 2 */ "s.field_id, "
		 /* 3 */ "m.field_pg_type, "
		 /* 4 */ "m.field_pg_typemod, "
		 /* 5 */ "s.lower_bound, "
		 /* 6 */ "s.upper_bound "
		"from " DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " s "
		"JOIN " MAPPING_TABLE_NAME " m USING (table_name, field_id) "
		"JOIN pg_attribute a ON (a.attrelid OPERATOR(pg_catalog.=) m.table_name "
		"                        AND a.attnum   OPERATOR(pg_catalog.=) m.pg_attnum "
		"                        AND NOT a.attisdropped) "
		"where s.table_name OPERATOR(pg_catalog.=) $1 "
		"  AND s.path       OPERATOR(pg_catalog.=) ANY($2)";

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	DECLARE_SPI_ARGS(2);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTARRAYOID, StringListToArray(pathList), false);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		bool		isPathNull = false;
		char	   *path = GET_SPI_VALUE(TEXTOID, rowIndex, 1, &isPathNull);

		if (isPathNull)
		{
			MemoryContextSwitchTo(spiContext);
			continue;
		}

		bool		isFieldIdNull = false;
		int64		fieldId = GET_SPI_VALUE(INT8OID, rowIndex, 2, &isFieldIdNull);

		if (isFieldIdNull)
		{
			MemoryContextSwitchTo(spiContext);
			continue;
		}

		PathToDataFilePtrHashEntry *entry =
			(PathToDataFilePtrHashEntry *) hash_search(filesByPath, path,
													   HASH_FIND, NULL);

		if (entry == NULL)
		{
			/*
			 * Stats row points at a path that isn't in the caller's list.
			 * Shouldn't happen given the WHERE path = ANY($2) filter, but
			 * tolerate it to avoid crashing on a corrupt catalog.
			 */
			MemoryContextSwitchTo(spiContext);
			continue;
		}

		TableDataFile *dataFile = entry->dataFile;

		if (ColumnStatAlreadyAdded(dataFile->stats.columnStats, fieldId))
		{
			MemoryContextSwitchTo(spiContext);
			continue;
		}

		bool		isPgTypeNull = false;
		bool		isPgTypeModNull = false;

		PGType		pgType = {
			.postgresTypeOid = GET_SPI_VALUE(OIDOID, rowIndex, 3, &isPgTypeNull),
			.postgresTypeMod = GET_SPI_VALUE(INT4OID, rowIndex, 4, &isPgTypeModNull)
		};

		char	   *lowerBoundText = NULL;
		char	   *upperBoundText = NULL;

		bool		isLowerBoundNull = false;
		Datum		lowerBoundDatum = GET_SPI_DATUM(rowIndex, 5, &isLowerBoundNull);

		if (!isLowerBoundNull)
			lowerBoundText = TextDatumGetCString(lowerBoundDatum);

		bool		isUpperBoundNull = false;
		Datum		upperBoundDatum = GET_SPI_DATUM(rowIndex, 6, &isUpperBoundNull);

		if (!isUpperBoundNull)
			upperBoundText = TextDatumGetCString(upperBoundDatum);

		DataFileColumnStats *columnStats =
			CreateDataFileColumnStats(fieldId, pgType, lowerBoundText, upperBoundText);

		dataFile->stats.columnStats = lappend(dataFile->stats.columnStats, columnStats);

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	hash_destroy(filesByPath);
}


/*
* FillDataFileColumnStats fills the column stats for a given data file
* from the catalog. It is a helper function for GetTableDataFilesFromCatalog.
*/
static void
FillDataFileColumnStats(TableDataFile * dataFile, int64 fieldId, int rowIndex)
{
	bool		isPgTypeNull = false;
	bool		isPgTypeModNull = false;

	PGType		pgType = {
		.postgresTypeOid = GET_SPI_VALUE(OIDOID, rowIndex, 10, &isPgTypeNull),
		.postgresTypeMod = GET_SPI_VALUE(INT4OID, rowIndex, 11, &isPgTypeModNull)
	};

	char	   *lowerBoundText = NULL;
	char	   *upperBoundText = NULL;

	bool		isLowerBoundNull = false;
	Datum		lowerBoundDatum = GET_SPI_DATUM(rowIndex, 12, &isLowerBoundNull);

	if (!isLowerBoundNull)
	{
		lowerBoundText = TextDatumGetCString(lowerBoundDatum);
	}

	bool		isUpperBoundNull = false;
	Datum		upperBoundDatum = GET_SPI_DATUM(rowIndex, 13, &isUpperBoundNull);

	if (!isUpperBoundNull)
	{
		upperBoundText = TextDatumGetCString(upperBoundDatum);
	}

	/* create column stats from catalog values */
	DataFileColumnStats *columnStats = CreateDataFileColumnStats(fieldId, pgType, lowerBoundText, upperBoundText);

	dataFile->stats.columnStats = lappend(dataFile->stats.columnStats, columnStats);
}


/*
* FillPartitionFieldFromCatalog fills the partition field for a given data file from
* the catalog. It is a helper function for GetTableDataFilesFromCatalog.
*/
static void
FillPartitionFieldFromCatalog(TableDataFile * dataFile, List *partitionTransforms, int64 partitionFieldId,
							  int rowIndex)
{
	/* not null enforced by the catalog */
	bool		isPartitionFieldNameNull = false;
	char	   *partitionFieldName = GET_SPI_VALUE(TEXTOID, rowIndex, 15, &isPartitionFieldNameNull);

	/* value can be NULL */
	bool		isValueNull = false;
	char	   *valueText = NULL;

	Datum		valueDatum = GET_SPI_DATUM(rowIndex, 16, &isValueNull);

	if (!isValueNull)
	{
		valueText = TextDatumGetCString(valueDatum);
	}

	PartitionField *partitionField = palloc0(sizeof(PartitionField));

	partitionField->field_id = partitionFieldId;
	partitionField->field_name = pstrdup(partitionFieldName);

	bool		errorIfMissing = true;

	IcebergPartitionTransform *partitionTransform =
		FindPartitionTransformById(partitionTransforms, partitionFieldId, errorIfMissing);

	partitionField->value_type = GetTransformResultAvroType(partitionTransform);

	partitionField->value = DeserializePartitionValueFromPGText(partitionTransform, valueText,
																&partitionField->value_length);

	/* now append this to the partition */
	AppendPartitionField(dataFile->partition, partitionField);

	/* set the partition field id of data file */
	bool		isSpecIdNull = false;
	int			partitionSpecId = GET_SPI_VALUE(INT4OID, rowIndex, 17, &isSpecIdNull);

	dataFile->partitionSpecId = partitionSpecId;
}


/*
* PartitionFieldAlreadyAdded checks if the given field id is already
* present in the partition.
*/
static bool
PartitionFieldAlreadyAdded(Partition * partition, int64 fieldId)
{
	for (int i = 0; i < partition->fields_length; i++)
	{
		PartitionField *partitionField = &partition->fields[i];

		if (partitionField->field_id == fieldId)
			return true;
	}

	return false;
}

/*
* ColumnStatAlreadyAdded checks if the given field id is already
* present in the column stats.
*/
static bool
ColumnStatAlreadyAdded(List *columnStats, int64 fieldId)
{
	ListCell   *cell = NULL;

	foreach(cell, columnStats)
	{
		DataFileColumnStats *columnStat = (DataFileColumnStats *) lfirst(cell);

		if (columnStat->leafField.fieldId == fieldId)
			return true;
	}

	return false;
}

/*
 * CreateDataFilesHash creates a hash table of file_id => TableDataFile.
 */
static HTAB *
CreateDataFilesHash(void)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(int64);
	hashCtl.entrysize = sizeof(TableDataFile);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *dataFilesHash = hash_create("data files by file id hash",
											1024,
											&hashCtl,
											HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	return dataFilesHash;
}


/*
 * CreateDataFilesByPathHash creates a hash table of path => TableDataFileHashEntry.
 */
static HTAB *
CreateDataFilesByPathHash(void)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = MAX_S3_PATH_LENGTH;
	hashCtl.entrysize = sizeof(TableDataFileHashEntry);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *dataFilesHash = hash_create("data files by path hash",
											1024,
											&hashCtl,
											HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	return dataFilesHash;
}


/*
 * TableDataFileHashToList converts a hash table of data files to a list.
 */
static List *
TableDataFileHashToList(HTAB *dataFiles)
{
	List	   *result = NIL;

	HASH_SEQ_STATUS status;
	TableDataFile *dataFile;

	hash_seq_init(&status, dataFiles);

	while ((dataFile = hash_seq_search(&status)) != NULL)
	{
		result = lappend(result, dataFile);
	}

	return result;
}


/*
 * GetPossiblePositionDeleteFilesFromCatalog returns a list of position delete files that
 * have to be applied to a given source file list.
 *
 * External writers might add deletion files that span across multiple or unknown
 * source files, in which case deleted_from is NULL. We include these deletion files
 * for any source path.
 *
 * The optional snapshot can be used to get a consistent view of the catalog.
 *
 * includeUnbound specifies whether to include position delete files that are not
 * bound to a specific data file.
 */
List *
GetPossiblePositionDeleteFilesFromCatalog(Oid relationId, List *sourcePathList, Snapshot snapshot)
{
	if (sourcePathList == NIL)
		return NIL;

	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	MemoryContext callerContext = CurrentMemoryContext;

	char	   *query =
		"select "
		 /* 1 */ "path "
		"from " DELETION_FILE_MAP_TABLE " "
		"where table_name OPERATOR(pg_catalog.=) $1 "
		"and deleted_from OPERATOR(pg_catalog.=) ANY($2)";

	SPI_START();

	DECLARE_SPI_ARGS(2);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTARRAYOID, StringListToArray(sourcePathList), false);


	if (!snapshot)
	{
		bool		readOnly = true;

		SPI_EXECUTE(query, readOnly);
	}
	else
	{
		SPIPlanPtr	qplan = GetCachedQueryPlan(query, spiArgCount, spiArgTypes);

		if (qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s while fetching metadata",
				 SPI_result_code_string(SPI_result));

		bool		readOnly = true;
		bool		fireTriggers = true;
		int			spi_result =
			SPI_execute_snapshot(qplan,
								 spiArgValues, spiArgNulls,
								 snapshot,
								 InvalidSnapshot,
								 readOnly, fireTriggers, 0);

		/* Check result */
		if (spi_result != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	}



	List	   *result = NIL;

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull;
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		char	   *positionDeleteFilePath = GET_SPI_VALUE(TEXTOID, rowIndex, 1, &isNull);

		result = lappend(result, positionDeleteFilePath);
		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return result;
}


/*
 * GetTableSizeFromCatalog sums the sizes of the data files in the table.
 *
 * We assume no records indicates that the table is empty (size 0). It is
 * up to the caller to confirm that the relation ID belongs to an actual
 * writable table.
 */
int64
GetTableSizeFromCatalog(Oid relationId)
{
	int64		tableSize = 0;

	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	/* cast sum result to bigint to avoid returning numeric */
	char	   *metadataQuery =
		"select "
		 /* 1 */ "sum(file_size)::bigint "
		"from " DATA_FILES_TABLE_QUALIFIED " "
		"where table_name OPERATOR(pg_catalog.=) $1";

	SPI_START();

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	bool		readOnly = true;

	SPI_EXECUTE(metadataQuery, readOnly);

	if (SPI_processed == 1)
	{
		bool		rowIndex = 0;
		bool		isNull = false;

		Datum		tableSizeDatum = GET_SPI_DATUM(rowIndex, 1, &isNull);

		if (!isNull)
		{
			tableSize = DatumGetInt64(tableSizeDatum);
		}
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return tableSize;
}


/*
 * GetTotalDeletedRowCountFromCatalog sums the deleted_row_count of all data
 * files in the table.  This is the total number of rows that are currently
 * covered by position delete files.
 *
 * Returns 0 when the table is empty or no file has recorded deletions yet.
 */
int64
GetTotalDeletedRowCountFromCatalog(Oid relationId)
{
	int64		totalDeletedRows = 0;

	/* cast sum result to bigint to avoid returning numeric */
	char	   *metadataQuery =
		psprintf("select "
				  /* 1 */ "sum(deleted_row_count)::bigint "
				 "from " DATA_FILES_TABLE_QUALIFIED " "
				 "where table_name OPERATOR(pg_catalog.=) $1 "
				 "and content OPERATOR(pg_catalog.=) %d",
				 (int) CONTENT_DATA);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	bool		readOnly = true;

	SPI_EXECUTE(metadataQuery, readOnly);

	if (SPI_processed == 1)
	{
		bool		rowIndex = 0;
		bool		isNull = false;

		Datum		totalDeletedRowsDatum = GET_SPI_DATUM(rowIndex, 1, &isNull);

		if (!isNull)
		{
			totalDeletedRows = DatumGetInt64(totalDeletedRowsDatum);
		}
	}

	SPI_END();

	return totalDeletedRows;
}


/*
 * GenerateDataFileId returns a unique file number that can be used for insertion
 * into files.
 *
 * The bulk-add path in ApplyDataFileCatalogChanges fetches IDs in batches via
 * BuildBatchFileIds; this single-id variant remains for callers outside the
 * bulk path (e.g. row-id mapping recovery).
 */
int64
GenerateDataFileId(void)
{
	bool		missingOk = false;
	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);
	Oid			sequenceId = get_relname_relid(DATA_FILES_ID_SEQUENCE_NAME, namespaceId);

	return nextval_internal(sequenceId, false);
}


/*
 * AddDeletionFileMapping inserts a new deletion file -> source file mapping
 * that indicates the deletion file has at least 1 deletion from the source
 * file.
 */
static void
AddDeletionFileMapping(Oid relationId, const char *deletionFilePath,
					   const char *dataFilePath)
{
	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"insert into " DELETION_FILE_MAP_TABLE " "
		"(table_name, path, deleted_from) "
		"values ($1,$2,$3)";

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, deletionFilePath, false);
	SPI_ARG_VALUE(3, TEXTOID, dataFilePath, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * AddNewRowIdMapping up the data file for the given relation in the
 * catalog and then inserts the row mapping records.
 */
static void
AddNewRowIdMapping(Oid relationId, const char *path, List *rowIdRanges)
{
	int64		fileId = GetFileIdForPath(relationId, path);

	ListCell   *rangeCell = NULL;

	foreach(rangeCell, rowIdRanges)
	{
		RowIdRangeMapping *range = (RowIdRangeMapping *) lfirst(rangeCell);

		InsertSingleRowMapping(relationId,
							   fileId,
							   range->rowStartId,
							   range->rowStartId + range->numRows,
							   range->rowStartNum);
	}
}


/*
 * GetFileIdForPath returns the file ID belonging to a given path.
 */
static int64
GetFileIdForPath(Oid relationId, const char *path)
{
	/* file may have been inserted by current sttement */
	PushActiveSnapshot(GetLatestSnapshot());

	char	   *query =
		"select "
		" /* 1 */ id "
		"from " DATA_FILES_TABLE_QUALIFIED " "
		"where table_name operator(pg_catalog.=) $1 "
		"and path operator(pg_catalog.=) $2";

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, path, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = true;

	SPI_EXECUTE(query, readOnly);

	if (SPI_processed < 1)
		ereport(ERROR, (errmsg("could not find data file for path %s", path)));

	bool		isNull;

	int64		fileId = GET_SPI_VALUE(INT8OID, 0, 1, &isNull);

	SPI_END();

	PopActiveSnapshot();

	return fileId;
}


/*
 * UpdateDeletedRowCount updates the number of deleted rows for a given
 * file.
 */
static void
UpdateDeletedRowCount(Oid relationId, const char *path, int64 deletedRowCount)
{
	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"update " DATA_FILES_TABLE_QUALIFIED " "
		"set deleted_row_count = $3 "
		"where table_name OPERATOR(pg_catalog.=) $1 and path OPERATOR(pg_catalog.=) $2";

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, path, false);
	SPI_ARG_VALUE(3, INT8OID, deletedRowCount, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * UpdateDataFileFirstRowId updates the first row ID of a file, in case the
 * file is retroactively assigned a row ID range.
 */
void
UpdateDataFileFirstRowId(Oid relationId, int64 fileId, int64 firstRowId)
{
	char	   *query =
		"update " DATA_FILES_TABLE_QUALIFIED " "
		"set first_row_id = $3 "
		"where table_name OPERATOR(pg_catalog.=) $1 and id OPERATOR(pg_catalog.=) $2";

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT8OID, fileId, false);
	SPI_ARG_VALUE(3, INT8OID, firstRowId, firstRowId == INVALID_ROW_ID);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}


/*
 * RemoveDataFileFromTable deletes a data file URL from
 * lake_table.files and also cleans up deletion files.
 */
static void
RemoveDataFileFromTable(Oid relationId, const char *path)
{
	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * A deletion file can delete from 1 or more data files. When adding a
	 * deletion file, we also store mappings to each of the data files it
	 * deletes from to avoid having to inspect it later (deletion files can be
	 * tens of megabytes, and usually only delete from 1 data file).
	 *
	 * Before removing a data file from the catalog, we also remove mappings
	 * of deletion files to that data file, which indicate whether the
	 * deletion file deletes from the data file. If the deletion file does not
	 * affect any other existing data files (which is usually the case because
	 * the deletion files we generate only affect 1 data file), we remove the
	 * the deletion file from the files table as well.
	 *
	 * Hence, we perform 3 steps. 1. Delete deletion file -> data file
	 * mappings and obtain the list of of affected deletion files. 2. Delete
	 * the files record for the data file we're removing 3. Delete the files
	 * record for any deletion files discovered in step 1 that have no other
	 * mappings. We return these deletion files to the caller as metadata
	 * operation.
	 *
	 * Note: We do have an ON CASCADE DELETE foreign key from
	 * deletion_file_map to files, so step 1 would happen automatically in
	 * step 2, but we would not know which deletion files were affected and
	 * would have to scan all of them in the last step.
	 */

	char	   *query =
	/* delete mappings that point to the data file path */
		"with deletion_files as ("
		" delete from " DELETION_FILE_MAP_TABLE
		" where table_name OPERATOR (pg_catalog.=) $1"
		" and deleted_from OPERATOR (pg_catalog.=) $2"
		" returning path"
		"), "
	/* delete the data file with the given path */
		"files as ("
		" delete from " DATA_FILES_TABLE_QUALIFIED
		" where table_name OPERATOR(pg_catalog.=) $1"
		" and path OPERATOR(pg_catalog.=) $2"
		" returning path, id"
		") "
	/* delete affected deletion files that have no other mappings */
		"delete from " DATA_FILES_TABLE_QUALIFIED " "
		"using (select distinct path as deletion_file_path from deletion_files) maps "
		"where table_name OPERATOR(pg_catalog.=) $1 "
		"and path = deletion_file_path "

	/* check for mappings that DO NOT point to the data file path */
		"and not exists ("
		" select 1 from " DELETION_FILE_MAP_TABLE
		" where table_name OPERATOR(pg_catalog.=) $1"
		" and deleted_from OPERATOR(pg_catalog.<>) $2"
		" and path OPERATOR(pg_catalog.=) deletion_file_path"
		")";

	DECLARE_SPI_ARGS(2);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, path, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * RemoveAllDataFileFromTable deletes all data file URL from
 * lake_table.files for a given relation.
 */
static void
RemoveAllDataFilesFromCatalog(Oid relationId)
{
	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"delete from " DATA_FILES_TABLE_QUALIFIED " "
		"where table_name OPERATOR(pg_catalog.=) $1";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * DataFilesCatalogExists returns whether the lake_table.files
 * table exists.
 */
bool
DataFilesCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(PG_LAKE_TABLE_FILES_TABLE_NAME, namespaceId) != InvalidOid;
}


/*
 * PartitionSpecsCatalogExists returns whether the lake_table.partition_specs
 * table exists.
 */
bool
PartitionSpecsCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(PG_LAKE_TABLE_PARTITION_SPECS, namespaceId) != InvalidOid;
}

/*
 * PartitionFieldsCatalogExists returns whether the lake_table.partition_fields
 * table exists.
 */
bool
PartitionFieldsCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(PG_LAKE_TABLE_PARTITION_FIELDS, namespaceId) != InvalidOid;
}

/*
 * DataFilesPartitionValuesCatalogExists returns whether the lake_table.data_file_partition_values
 * table exists.
 */
bool
DataFilesPartitionValuesCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(PG_LAKE_TABLE_DATA_FILE_PARTITION_VALUES_TABLE_NAME, namespaceId) != InvalidOid;
}


/*
 * CreateTxDataFileIdsTempTableIfNotExists creates a temporary table
 * to track data file IDs that are being added in the current transaction.
 */
static void
CreateTxDataFileIdsTempTableIfNotExists(void)
{
	const char *query =
		"create temporary table if not exists " TX_DATA_FILES_QUALIFIED_TABLE_NAME " "
		"(id bigint primary key) USING heap ON COMMIT DELETE ROWS;";

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_execute(query, readOnly, 0);

	SPI_END();
}


/*
 * BuildBatchFileIds reserves count file IDs from the files_id_seq sequence in
 * a single SPI round trip. Returns a freshly palloc'd int64[count].
 *
 * Used by the bulk-add path so we don't pay one nextval call per file in a
 * large transactional write (tens of thousands of files per partitioned
 * iceberg insert).
 */
static int64 *
BuildBatchFileIds(int count)
{
	Assert(count > 0);

	int64	   *fileIds = palloc(sizeof(int64) * count);

	char	   *query =
		"select nextval('" PG_LAKE_TABLE_SCHEMA "." DATA_FILES_ID_SEQUENCE_NAME "') "
		"from generate_series(1, $1)";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, INT4OID, count, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	if (SPI_processed != (uint64) count)
		elog(ERROR, "nextval batch returned %lu rows, expected %d",
			 (unsigned long) SPI_processed, count);

	MemoryContext spiContext = MemoryContextSwitchTo(TopTransactionContext);

	for (int i = 0; i < count; i++)
	{
		bool		isNull = false;

		fileIds[i] = GET_SPI_VALUE(INT8OID, i, 1, &isNull);
		if (isNull)
			elog(ERROR, "nextval batch returned NULL at row %d", i);
	}

	MemoryContextSwitchTo(spiContext);

	SPI_END();

	int64	   *out = palloc(sizeof(int64) * count);

	memcpy(out, fileIds, sizeof(int64) * count);
	pfree(fileIds);
	return out;
}


/*
 * MakeArrayFromDatums wraps construct_md_array for a 1-D nullable Postgres
 * array of the given element type. Caller owns datums/nulls; this returns a
 * freshly palloc'd ArrayType.
 */
static ArrayType *
MakeArrayFromDatums(Datum *datums, bool *nulls, int count, Oid elementType)
{
	int16		typlen;
	bool		typbyval;
	char		typalign;
	int			dims[1] = {count};
	int			lbs[1] = {1};

	get_typlenbyvalalign(elementType, &typlen, &typbyval, &typalign);

	return construct_md_array(datums, nulls, 1, dims, lbs,
							  elementType, typlen, typbyval, typalign);
}


/*
 * BulkInsertDataFiles writes count rows into lake_table.files in a single
 * INSERT ... SELECT ... FROM unnest(...) statement. The fileIds array carries
 * the pre-allocated IDs (see BuildBatchFileIds) so partition_values can refer
 * to them without a follow-up SELECT.
 *
 * Replaces what would otherwise be count calls to AddDataFileToTable.
 */
static void
BulkInsertDataFiles(Oid relationId, List *addOps, int64 *fileIds)
{
	int			count = list_length(addOps);

	if (count == 0)
		return;

	Datum	   *idDatums = palloc(sizeof(Datum) * count);
	Datum	   *pathDatums = palloc(sizeof(Datum) * count);
	Datum	   *rowCountDatums = palloc(sizeof(Datum) * count);
	Datum	   *fileSizeDatums = palloc(sizeof(Datum) * count);
	Datum	   *contentDatums = palloc(sizeof(Datum) * count);
	Datum	   *firstRowIdDatums = palloc(sizeof(Datum) * count);
	bool	   *firstRowIdNulls = palloc(sizeof(bool) * count);

	int			rowIndex = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		idDatums[rowIndex] = Int64GetDatum(fileIds[rowIndex]);
		pathDatums[rowIndex] = CStringGetTextDatum(operation->path);
		rowCountDatums[rowIndex] = Int64GetDatum(operation->dataFileStats.rowCount);
		fileSizeDatums[rowIndex] = Int64GetDatum(operation->dataFileStats.fileSize);
		contentDatums[rowIndex] = Int32GetDatum((int) operation->content);

		int64		firstRowId = operation->dataFileStats.rowIdStart;

		firstRowIdNulls[rowIndex] = (firstRowId == INVALID_ROW_ID);
		firstRowIdDatums[rowIndex] = Int64GetDatum(firstRowId);

		rowIndex++;
	}

	ArrayType  *idArray = MakeArrayFromDatums(idDatums, NULL, count, INT8OID);
	ArrayType  *pathArray = MakeArrayFromDatums(pathDatums, NULL, count, TEXTOID);
	ArrayType  *rowCountArray = MakeArrayFromDatums(rowCountDatums, NULL, count, INT8OID);
	ArrayType  *fileSizeArray = MakeArrayFromDatums(fileSizeDatums, NULL, count, INT8OID);
	ArrayType  *contentArray = MakeArrayFromDatums(contentDatums, NULL, count, INT4OID);
	ArrayType  *firstRowIdArray = MakeArrayFromDatums(firstRowIdDatums, firstRowIdNulls,
													  count, INT8OID);

	char	   *query =
		"INSERT INTO " DATA_FILES_TABLE_QUALIFIED " "
		"(id, table_name, path, row_count, file_size, content, first_row_id) "
		"SELECT id, $1, path, row_count, file_size, content, first_row_id "
		"FROM unnest($2::int8[], $3::text[], $4::int8[], $5::int8[], "
		"$6::int4[], $7::int8[]) "
		"AS t(id, path, row_count, file_size, content, first_row_id)";

	DECLARE_SPI_ARGS(7);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT8ARRAYOID, idArray, false);
	SPI_ARG_VALUE(3, TEXTARRAYOID, pathArray, false);
	SPI_ARG_VALUE(4, INT8ARRAYOID, rowCountArray, false);
	SPI_ARG_VALUE(5, INT8ARRAYOID, fileSizeArray, false);
	SPI_ARG_VALUE(6, INT4ARRAYOID, contentArray, false);
	SPI_ARG_VALUE(7, INT8ARRAYOID, firstRowIdArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}


/*
 * BulkInsertDataFileColumnStats writes per-column min/max stats for all
 * DATA_FILE_ADD ops with content == CONTENT_DATA in a single INSERT.
 * Skips ops with NULL bounds, matching the per-row helper semantics.
 *
 * Replaces what would otherwise be sum(ops_i.column_stats_length) calls to
 * AddDataFileColumnStatsToCatalog. On a partitioned iceberg insert with
 * ~6 stats columns per file this is the largest single source of catalog
 * SPI calls during LOAD.
 */
static void
BulkInsertDataFileColumnStats(Oid relationId, List *addOps)
{
	int			capacity = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->content != CONTENT_DATA)
			continue;

		capacity += list_length(operation->dataFileStats.columnStats);
	}

	if (capacity == 0)
		return;

	Datum	   *pathDatums = palloc(sizeof(Datum) * capacity);
	Datum	   *fieldIdDatums = palloc(sizeof(Datum) * capacity);
	Datum	   *lowerDatums = palloc(sizeof(Datum) * capacity);
	bool	   *lowerNulls = palloc(sizeof(bool) * capacity);
	Datum	   *upperDatums = palloc(sizeof(Datum) * capacity);
	bool	   *upperNulls = palloc(sizeof(bool) * capacity);

	int			rowCount = 0;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->content != CONTENT_DATA)
			continue;

		ListCell   *statsCell = NULL;

		foreach(statsCell, operation->dataFileStats.columnStats)
		{
			DataFileColumnStats *columnStats = lfirst(statsCell);

			/*
			 * Match the per-row code: skip rows with NULL bounds entirely
			 * rather than emitting (NULL, NULL).
			 */
			if (columnStats->lowerBoundText == NULL)
			{
				Assert(columnStats->upperBoundText == NULL);
				continue;
			}

			pathDatums[rowCount] = CStringGetTextDatum(operation->path);
			fieldIdDatums[rowCount] = Int64GetDatum(columnStats->leafField.fieldId);
			lowerDatums[rowCount] = CStringGetTextDatum(columnStats->lowerBoundText);
			lowerNulls[rowCount] = false;
			upperDatums[rowCount] = (columnStats->upperBoundText != NULL)
				? CStringGetTextDatum(columnStats->upperBoundText)
				: (Datum) 0;
			upperNulls[rowCount] = (columnStats->upperBoundText == NULL);

			rowCount++;
		}
	}

	if (rowCount == 0)
		return;

	ArrayType  *pathArray = MakeArrayFromDatums(pathDatums, NULL, rowCount, TEXTOID);
	ArrayType  *fieldIdArray = MakeArrayFromDatums(fieldIdDatums, NULL, rowCount, INT8OID);
	ArrayType  *lowerArray = MakeArrayFromDatums(lowerDatums, lowerNulls, rowCount, TEXTOID);
	ArrayType  *upperArray = MakeArrayFromDatums(upperDatums, upperNulls, rowCount, TEXTOID);

	char	   *query =
		"INSERT INTO " DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " "
		"(table_name, path, field_id, lower_bound, upper_bound) "
		"SELECT $1, path, field_id, lower_bound, upper_bound "
		"FROM unnest($2::text[], $3::int8[], $4::text[], $5::text[]) "
		"AS t(path, field_id, lower_bound, upper_bound)";

	DECLARE_SPI_ARGS(5);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTARRAYOID, pathArray, false);
	SPI_ARG_VALUE(3, INT8ARRAYOID, fieldIdArray, false);
	SPI_ARG_VALUE(4, TEXTARRAYOID, lowerArray, false);
	SPI_ARG_VALUE(5, TEXTARRAYOID, upperArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}


/*
 * AddOpHasPartitionValues mirrors the per-row predicate from the previous
 * implementation: only DATA and POSITION_DELETES content can carry partition
 * values, and the op must actually have a non-empty partition tuple.
 */
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
 * BulkInsertDataFilePartitionValues writes one row per (file, partition
 * field) for all DATA_FILE_ADD ops that carry partition values, in a single
 * INSERT.
 *
 * Replaces what would otherwise be sum(ops_i.partition_fields_length) calls to
 * AddDataFilePartitionValueToCatalog. The per-call helper additionally did its
 * own AllPartitionTransformList(relationId) catalog lookup; we hoist that
 * lookup once per batch.
 */
static void
BulkInsertDataFilePartitionValues(Oid relationId, List *addOps, int64 *fileIds)
{
	int			capacity = 0;
	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (AddOpHasPartitionValues(operation))
			capacity += operation->partition->fields_length;
	}

	if (capacity == 0)
		return;

	List	   *transforms = AllPartitionTransformList(relationId);

	Datum	   *idDatums = palloc(sizeof(Datum) * capacity);
	Datum	   *partitionFieldIdDatums = palloc(sizeof(Datum) * capacity);
	Datum	   *valueDatums = palloc(sizeof(Datum) * capacity);
	bool	   *valueNulls = palloc(sizeof(bool) * capacity);

	int			outRow = 0;
	int			opIndex = 0;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (!AddOpHasPartitionValues(operation))
		{
			opIndex++;
			continue;
		}

		Assert(operation->partitionSpecId != DEFAULT_SPEC_ID);

		int64		fileId = fileIds[opIndex];

		for (size_t fieldIndex = 0; fieldIndex < operation->partition->fields_length; fieldIndex++)
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

			idDatums[outRow] = Int64GetDatum(fileId);
			partitionFieldIdDatums[outRow] = Int32GetDatum(partitionField->field_id);
			if (partitionValue == NULL)
			{
				valueDatums[outRow] = (Datum) 0;
				valueNulls[outRow] = true;
			}
			else
			{
				valueDatums[outRow] = CStringGetTextDatum(partitionValue);
				valueNulls[outRow] = false;
			}

			outRow++;
		}

		opIndex++;
	}

	ArrayType  *idArray = MakeArrayFromDatums(idDatums, NULL, outRow, INT8OID);
	ArrayType  *partitionFieldIdArray = MakeArrayFromDatums(partitionFieldIdDatums, NULL,
															outRow, INT4OID);
	ArrayType  *valueArray = MakeArrayFromDatums(valueDatums, valueNulls, outRow, TEXTOID);

	char	   *query =
		"INSERT INTO " DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED " "
		"(table_name, id, partition_field_id, value) "
		"SELECT $1, id, partition_field_id, value "
		"FROM unnest($2::int8[], $3::int4[], $4::text[]) "
		"AS t(id, partition_field_id, value)";

	DECLARE_SPI_ARGS(4);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT8ARRAYOID, idArray, false);
	SPI_ARG_VALUE(3, INT4ARRAYOID, partitionFieldIdArray, false);
	SPI_ARG_VALUE(4, TEXTARRAYOID, valueArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}


/*
 * BulkInsertTrackedFileIds inserts the file IDs of CONTENT_DATA ops whose
 * PgLakeAddDataFileHook returns true into the per-transaction temp table.
 *
 * The hook gives a sibling extension (snowflake_cdc, currently) the ability
 * to mark which files it wants to track for the rest of the transaction.
 * It must be called once per file (even in batch mode) so the hook can
 * inspect per-file state; we still collect the IDs and issue a single bulk
 * INSERT at the end.
 */
static void
BulkInsertTrackedFileIds(List *addOps, int64 *fileIds)
{
	if (PgLakeAddDataFileHook == NULL)
		return;

	int			count = list_length(addOps);
	int64	   *trackedIds = palloc(sizeof(int64) * count);
	int			trackedCount = 0;
	int			opIndex = 0;

	ListCell   *operationCell = NULL;

	foreach(operationCell, addOps)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->content == CONTENT_DATA && PgLakeAddDataFileHook())
			trackedIds[trackedCount++] = fileIds[opIndex];

		opIndex++;
	}

	if (trackedCount == 0)
	{
		pfree(trackedIds);
		return;
	}

	CreateTxDataFileIdsTempTableIfNotExists();

	Datum	   *idDatums = palloc(sizeof(Datum) * trackedCount);

	for (int i = 0; i < trackedCount; i++)
		idDatums[i] = Int64GetDatum(trackedIds[i]);

	ArrayType  *idArray = MakeArrayFromDatums(idDatums, NULL, trackedCount, INT8OID);

	char	   *query =
		"INSERT INTO " TX_DATA_FILES_QUALIFIED_TABLE_NAME " (id) "
		"SELECT id FROM unnest($1::int8[]) AS t(id)";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, INT8ARRAYOID, idArray, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();

	pfree(trackedIds);
}


/*
 * FlushDataFileAddBatch applies a contiguous batch of DATA_FILE_ADD operations
 * to the three lake_table catalogs (files, data_file_column_stats,
 * data_file_partition_values) using bulk INSERTs, plus the optional
 * tx_data_file_ids temp table when PgLakeAddDataFileHook opts in.
 *
 * Equivalent in effect to looping the per-row helpers for each op, but pays
 * O(catalogs) SPI round trips instead of O(files * (1 + columns + partition_fields)).
 */
static void
FlushDataFileAddBatch(Oid relationId, List *addOps)
{
	if (addOps == NIL)
		return;

	int			count = list_length(addOps);
	int64	   *fileIds = BuildBatchFileIds(count);

	BulkInsertDataFiles(relationId, addOps, fileIds);
	BulkInsertDataFileColumnStats(relationId, addOps);
	BulkInsertDataFilePartitionValues(relationId, addOps, fileIds);
	BulkInsertTrackedFileIds(addOps, fileIds);

	pfree(fileIds);
}


/*
 * ApplyDataFileCatalogChanges is the main work horse for metadata operations
 * on the files catalog.
 *
 * DATA_FILE_ADD operations are bulked into a single round of INSERTs per
 * catalog (see FlushDataFileAddBatch); all other operation types remain on
 * the per-row helpers since they are rare compared to ADDs. We flush a
 * pending ADD batch whenever a non-ADD op is encountered so that observable
 * ordering between ADDs and (e.g.) REMOVEs is preserved.
 */
void
ApplyDataFileCatalogChanges(Oid relationId, List *metadataOperations)
{
	List	   *pendingAdds = NIL;
	ListCell   *operationCell = NULL;

	foreach(operationCell, metadataOperations)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		if (operation->type == DATA_FILE_ADD)
		{
			pendingAdds = lappend(pendingAdds, operation);
			continue;
		}

		if (pendingAdds != NIL)
		{
			FlushDataFileAddBatch(relationId, pendingAdds);
			list_free(pendingAdds);
			pendingAdds = NIL;
		}

		switch (operation->type)
		{
			case DATA_FILE_ADD_DELETE_MAPPING:
				AddDeletionFileMapping(relationId,
									   operation->path,
									   operation->deletedFrom);
				break;

			case DATA_FILE_ADD_ROW_ID_MAPPING:
				AddNewRowIdMapping(relationId,
								   operation->path,
								   operation->rowIdRanges);
				break;

			case DATA_FILE_REMOVE:
				RemoveDataFileFromTable(relationId, operation->path);
				break;
			case DATA_FILE_REMOVE_ALL:
			case DATA_FILE_DROP_TABLE:
				RemoveAllDataFilesFromCatalog(relationId);
				break;
			case DATA_FILE_UPDATE_DELETED_ROW_COUNT:
				UpdateDeletedRowCount(relationId,
									  operation->path,
									  operation->dataFileStats.deletedRowCount);
				break;

			case DATA_FILE_MERGE_MANIFESTS:
			case EXPIRE_OLD_SNAPSHOTS:
			case TABLE_CREATE:
			case TABLE_DDL:
			case TABLE_PARTITION_BY:
				/* no-op for non-iceberg tables */
				/* we don't do anything for EXPIRE_OLD_SNAPSHOTS */
				break;

			case DATA_FILE_ADD:
				/* unreachable: handled above */
				Assert(false);
				break;

			default:
				elog(ERROR, "unsupported operation (%d) on data file catalog",
					 operation->type);
		}
	}

	if (pendingAdds != NIL)
	{
		FlushDataFileAddBatch(relationId, pendingAdds);
		list_free(pendingAdds);
	}
}


/*
* AddDataFileColumnStatsToCatalog inserts the column stats for a data file
 * into the catalog.
 */
void
AddDataFilePartitionValueToCatalog(Oid relationId, int32 partitionSpecId, int64 fileId,
								   Partition * partition)
{
	Assert(partition != NULL);
	Assert(partition->fields_length > 0);
	Assert(partition->fields != NULL);
	Assert(partitionSpecId != DEFAULT_SPEC_ID);

	/* we might be adding a file to an old partition spec */
	List	   *transforms = AllPartitionTransformList(relationId);

	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	SPI_START();

	for (size_t fieldIndex = 0; fieldIndex < partition->fields_length; fieldIndex++)
	{
		PartitionField *partitionField = &partition->fields[fieldIndex];

		bool		errorIfMissing = true;

		IcebergPartitionTransform *transform =
			FindPartitionTransformById(transforms, partitionField->field_id, errorIfMissing);

		const char *partitionValue =
			SerializePartitionValueToPGText(partitionField->value,
											partitionField->value_length,
											transform);

		char	   *query =
			"INSERT INTO " DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED " "
			"(table_name, id, partition_field_id, value) "
			"VALUES ($1,$2,$3,$4)";

		DECLARE_SPI_ARGS(4);

		SPI_ARG_VALUE(1, OIDOID, relationId, false);
		SPI_ARG_VALUE(2, INT8OID, fileId, false);
		SPI_ARG_VALUE(3, INT4OID, partitionField->field_id, false);
		SPI_ARG_VALUE(4, TEXTOID, partitionValue, partitionValue == NULL);

		bool		readOnly = false;

		SPI_EXECUTE(query, readOnly);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * CreateDataFileColumnStats creates a new DataFileColumnStats from the given
 * parameters.
 *
 * Allocates everything in CurrentMemoryContext, including the rebuilt Field
 * inside leafField (via PostgresTypeToIcebergField). Used both by the diff
 * path in LoadColumnStatsForFiles and by the commit-time tracker fast path
 * (track_iceberg_metadata_changes.c) to reconstitute LeafFields from cached
 * pgType / fieldId pairs without retaining pointers into the per-statement
 * memory of the original write.
 */
DataFileColumnStats *
CreateDataFileColumnStats(int fieldId, PGType pgType, char *lowerBoundText, char *upperBoundText)
{
	DataFileColumnStats *columnStats = palloc0(sizeof(DataFileColumnStats));

	columnStats->leafField.fieldId = fieldId;
	columnStats->lowerBoundText = lowerBoundText;
	columnStats->upperBoundText = upperBoundText;
	columnStats->leafField.pgType = pgType;

	bool		forAddColumn = false;
	int			subFieldIndex = fieldId;

	Field	   *field = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

	Assert(field->type == FIELD_TYPE_SCALAR);

	columnStats->leafField.field = field;

	const char *duckTypeName = IcebergTypeNameToDuckdbTypeName(field->field.scalar.typeName);

	columnStats->leafField.duckTypeName = duckTypeName;

	return columnStats;
}

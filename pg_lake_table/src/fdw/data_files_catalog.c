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
static int64 AddDataFileToTable(Oid relationId, const char *path, int64 rowCount,
								int64 fileSize, DataFileContent content, int64 rowIdStart);
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
static void InsertDataFileIdIntoTransactionTable(int64 fileId);
static DataFileColumnStats * CreateDataFileColumnStats(int fieldId, PGType pgType,
													   char *lowerBoundText,
													   char *upperBoundText);

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
																 partitionTransforms);

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
 */
HTAB *
GetTableDataFilesHashFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
								 bool forUpdate, char *orderBy, Snapshot snapshot,
								 List *partitionTransforms)
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
						    /* 8 */ "f.first_row_id, "
						    /* 9 */ "sma.field_id, "
						    /* 10 */ "sma.field_pg_type, "
						    /* 11 */ "sma.field_pg_typemod, "
						    /* 12 */ "sma.lower_bound, "
						    /* 13 */ "sma.upper_bound, "
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
						   ") p USING (table_name, id) "
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
 */
HTAB *
GetTableDataFilesByPathHashFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
									   bool forUpdate, char *orderBy, Snapshot snapshot,
									   List *partitionTransforms)
{
	HTAB	   *filesById = GetTableDataFilesHashFromCatalog(relationId, dataOnly, newFilesOnly,
															 forUpdate, orderBy, snapshot,
															 partitionTransforms);

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
 * AddDataFileToTable inserts a new file URL into lake_table.files
 *
 * content indicates whether this is a data file or deletion file.
 *
 * For deletion files, deletedFrom indicates which file we are deleting from (can
 * be NULL if deleting from multiple files/unknown).
 */
static int64
AddDataFileToTable(Oid relationId, const char *path, int64 rowCount, int64 fileSize,
				   DataFileContent content, int64 rowIdStart)
{
	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable), SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"insert into " DATA_FILES_TABLE_QUALIFIED " "
		"(id, table_name, path, row_count, file_size, content, first_row_id) "
		"values ($1,$2,$3,$4,$5,$6,$7)";

	int64		fileId = GenerateDataFileId();

	DECLARE_SPI_ARGS(7);
	SPI_ARG_VALUE(1, INT8OID, fileId, false);
	SPI_ARG_VALUE(2, OIDOID, relationId, false);
	SPI_ARG_VALUE(3, TEXTOID, path, false);
	SPI_ARG_VALUE(4, INT8OID, rowCount, false);
	SPI_ARG_VALUE(5, INT8OID, fileSize, false);
	SPI_ARG_VALUE(6, INT4OID, (int) content, false);
	SPI_ARG_VALUE(7, INT8OID, rowIdStart, rowIdStart == INVALID_ROW_ID);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return fileId;
}


/*
 * GenerateDataFileId returns a unique file number that can be used for insertion
 * into files.
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

	SPI_START();

	bool		readOnly = false;

	SPI_execute(query, readOnly, 0);

	SPI_END();
}


/*
 * InsertDataFileIdIntoTransactionTable records the data file IDs, that are being
 * added in the current transaction, into tx's temporary table.
 * It creates the table if it does not exist yet.
 */
static void
InsertDataFileIdIntoTransactionTable(int64 fileId)
{
	CreateTxDataFileIdsTempTableIfNotExists();

	char	   *query =
		"insert into " TX_DATA_FILES_QUALIFIED_TABLE_NAME " "
		"(id) "
		"values ($1)";

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, INT8OID, fileId, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}


/*
 * ApplyDataFileCatalogChanges is the main work horse for metadata operations
 * on the files catalog.
 */
void
ApplyDataFileCatalogChanges(Oid relationId, List *metadataOperations)
{
	ListCell   *operationCell = NULL;

	foreach(operationCell, metadataOperations)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		switch (operation->type)
		{
			case DATA_FILE_ADD:
				{
					int64		fileId = AddDataFileToTable(relationId,
															operation->path,
															operation->dataFileStats.rowCount,
															operation->dataFileStats.fileSize,
															operation->content,
															operation->dataFileStats.rowIdStart);

					/* add column stats only for data files */
					List	   *columnStats = operation->dataFileStats.columnStats;

					if (operation->content == CONTENT_DATA && columnStats != NIL)
						AddDataFileColumnStatsToCatalog(relationId,
														operation->path,
														columnStats);

					/*
					 * Add partition values only for data files. Even if the
					 * table is not partitioned, we record the partition spec
					 * id as the table might be partitioned in the future.
					 *
					 * For non-partitioned tables, if the table has never been
					 * partitioned, we record the partition spec id as 0,
					 * which is the default spec id for non-partitioned
					 * tables. If the table has been partitioned before, and
					 * now it is not, we record the partition spec id as the
					 * current/largest spec id.
					 */
					if (operation->partition != NULL &&
						operation->partition->fields_length > 0 &&
						(operation->content == CONTENT_DATA ||
						 operation->content == CONTENT_POSITION_DELETES))
					{
						AddDataFilePartitionValueToCatalog(relationId,
														   operation->partitionSpecId,
														   fileId,
														   operation->partition);
					}

					if (PgLakeAddDataFileHook)
					{
						if (operation->content == CONTENT_DATA && PgLakeAddDataFileHook())
							/* add the file ID to the transaction's temp table */
							InsertDataFileIdIntoTransactionTable(fileId);
					}
					break;
				}

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

			default:
				elog(ERROR, "unsupported operation (%d) on data file catalog",
					 operation->type);
		}
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
 */
static DataFileColumnStats *
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

/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "postgres.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/uuid.h"

#include "pg_lake/ducklake/catalog.h"

DucklakeSnapshot *
DucklakeGetCurrentSnapshot(void)
{
	int ret;
	DucklakeSnapshot *snapshot;

	SPI_connect();
	ret = SPI_exec("SELECT snapshot_id, snapshot_time, schema_version, "
				   "next_catalog_id, next_file_id FROM lake_ducklake.snapshot "
				   "ORDER BY snapshot_id DESC LIMIT 1", 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "Failed to get current snapshot");

	/* Extract values while SPI context is still active */
	bool isnull;
	int64 snapshotId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 1, &isnull));
	TimestampTz snapshotTime = DatumGetTimestampTz(SPI_getbinval(SPI_tuptable->vals[0],
															   SPI_tuptable->tupdesc, 2, &isnull));
	int64 schemaVersion = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
														  SPI_tuptable->tupdesc, 3, &isnull));
	int64 nextCatalogId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
														  SPI_tuptable->tupdesc, 4, &isnull));
	int64 nextFileId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 5, &isnull));

	SPI_finish();

	/* Allocate snapshot in caller's memory context after SPI_finish */
	snapshot = (DucklakeSnapshot *) palloc(sizeof(DucklakeSnapshot));
	snapshot->snapshotId = snapshotId;
	snapshot->snapshotTime = snapshotTime;
	snapshot->schemaVersion = schemaVersion;
	snapshot->nextCatalogId = nextCatalogId;
	snapshot->nextFileId = nextFileId;

	return snapshot;
}

DucklakeSnapshot *
DucklakeCreateSnapshot(const char *changesMade, const char *author, const char *commitMessage)
{
	DucklakeSnapshot *currentSnapshot = DucklakeGetCurrentSnapshot();
	DucklakeSnapshot *newSnapshot;
	StringInfoData query;
	int ret;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.snapshot "
					 "(snapshot_id, schema_version, next_catalog_id, next_file_id) "
					 "VALUES (%ld, %ld, %ld, %ld) RETURNING snapshot_id",
					 currentSnapshot->snapshotId + 1,
					 currentSnapshot->schemaVersion,
					 currentSnapshot->nextCatalogId,
					 currentSnapshot->nextFileId);

	SPI_connect();
	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "Failed to create snapshot");

	/* Extract snapshot ID while SPI context is still active */
	bool isnull;
	int64 newSnapshotId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
														  SPI_tuptable->tupdesc, 1, &isnull));

	SPI_finish();

	/* Allocate new snapshot in caller's memory context after SPI_finish */
	newSnapshot = (DucklakeSnapshot *) palloc(sizeof(DucklakeSnapshot));
	newSnapshot->snapshotId = newSnapshotId;
	newSnapshot->schemaVersion = currentSnapshot->schemaVersion;
	newSnapshot->nextCatalogId = currentSnapshot->nextCatalogId;
	newSnapshot->nextFileId = currentSnapshot->nextFileId;

	pfree(currentSnapshot);
	return newSnapshot;
}

int64
DucklakeGetNextCatalogId(void)
{
	DucklakeSnapshot *snapshot = DucklakeGetCurrentSnapshot();
	int64 nextId = snapshot->nextCatalogId;
	pfree(snapshot);
	return nextId;
}

int64
DucklakeGetNextFileId(void)
{
	DucklakeSnapshot *snapshot = DucklakeGetCurrentSnapshot();
	int64 nextId = snapshot->nextFileId;
	pfree(snapshot);
	return nextId;
}

void
DucklakeRegisterTableColumns(Oid tableOid, int64 tableId)
{
	StringInfoData query;
	DucklakeSnapshot *snapshot;
	int ret;
	int64 numColumns;

	snapshot = DucklakeGetCurrentSnapshot();

	SPI_connect();

	/* First, count the number of columns */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT COUNT(*) FROM pg_attribute "
					 "WHERE attrelid = %u AND attnum > 0 AND NOT attisdropped",
					 tableOid);
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		pfree(snapshot);
		elog(ERROR, "Failed to count table columns");
	}

	bool isnull;
	numColumns = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1, &isnull));

	if (numColumns == 0)
	{
		SPI_finish();
		pfree(snapshot);
		return;
	}

	/* Insert all columns in one query using INSERT...SELECT with generate_series for column_id */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.column "
					 "(column_id, begin_snapshot, table_id, column_order, column_name, column_type, nulls_allowed) "
					 "SELECT "
					 "  %ld + (ROW_NUMBER() OVER (ORDER BY attnum) - 1), "  /* column_id */
					 "  %ld, "                                              /* begin_snapshot */
					 "  %ld, "                                              /* table_id */
					 "  attnum, "                                           /* column_order */
					 "  attname, "                                          /* column_name */
					 "  lake_ducklake.pg_type_to_duckdb_type(format_type(atttypid, atttypmod)), "  /* column_type */
					 "  NOT attnotnull "                                    /* nulls_allowed */
					 "FROM pg_attribute "
					 "WHERE attrelid = %u AND attnum > 0 AND NOT attisdropped "
					 "ORDER BY attnum",
					 snapshot->nextCatalogId,
					 snapshot->snapshotId,
					 tableId,
					 tableOid);

	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT)
	{
		SPI_finish();
		pfree(snapshot);
		elog(ERROR, "Failed to register table columns");
	}

	/* Update snapshot with new next_catalog_id */
	snapshot->nextCatalogId += numColumns;
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_catalog_id = %ld WHERE snapshot_id = %ld",
					 snapshot->nextCatalogId, snapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Create column mapping for field ID mapping.
	 * This is required for DuckDB to map Parquet file columns to table columns.
	 */
	int64 mappingId = snapshot->nextCatalogId;
	snapshot->nextCatalogId++;

	/* Create column_mapping entry */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.column_mapping (mapping_id, table_id, type) "
					 "VALUES (%ld, %ld, 'map_by_name')",
					 mappingId, tableId);
	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT)
	{
		SPI_finish();
		pfree(snapshot);
		elog(ERROR, "Failed to create column mapping");
	}

	/* Create name_mapping entries for each column */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.name_mapping "
					 "(mapping_id, column_id, source_name, target_field_id, is_partition) "
					 "SELECT %ld, column_id, column_name, column_id, false "
					 "FROM lake_ducklake.column "
					 "WHERE table_id = %ld AND begin_snapshot = %ld "
					 "ORDER BY column_order",
					 mappingId, tableId, snapshot->snapshotId);
	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT)
	{
		SPI_finish();
		pfree(snapshot);
		elog(ERROR, "Failed to create name mapping");
	}

	/* Update snapshot with final next_catalog_id */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_catalog_id = %ld WHERE snapshot_id = %ld",
					 snapshot->nextCatalogId, snapshot->snapshotId);
	SPI_exec(query.data, 0);

	SPI_finish();
	pfree(snapshot);
}

int64
DucklakeRegisterTable(const char *schemaName, const char *tableName, const char *path, Oid tableOid)
{
	StringInfoData query;
	int64 schemaId, tableId;
	int ret;
	DucklakeSnapshot *snapshot;
	pg_uuid_t tableUuid;

	elog(LOG, "DucklakeRegisterTable called: schema=%s, table=%s, path=%s",
		 schemaName, tableName, path ? path : "(null)");

	/* Generate new UUIDs */
	pg_uuid_t schemaUuid;
	memset(&schemaUuid, 0, sizeof(pg_uuid_t));
	memset(&tableUuid, 0, sizeof(pg_uuid_t));

	bool isnull;

	snapshot = DucklakeGetCurrentSnapshot();

	SPI_connect();

	/* Check if schema exists */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT schema_id FROM lake_ducklake.schema WHERE schema_name = %s AND end_snapshot IS NULL",
					 quote_literal_cstr(schemaName));
	ret = SPI_exec(query.data, 0);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		schemaId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &isnull));
	}
	else
	{
		/* Create new schema */
		schemaId = snapshot->nextCatalogId++;
		resetStringInfo(&query);
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.schema (schema_id, schema_uuid, begin_snapshot, schema_name) "
						 "VALUES (%ld, '%s', %ld, %s)",
						 schemaId, "00000000-0000-0000-0000-000000000000",
						 snapshot->snapshotId, quote_literal_cstr(schemaName));
		SPI_exec(query.data, 0);
	}

	/* Create new table */
	tableId = snapshot->nextCatalogId++;
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.table (table_id, table_uuid, begin_snapshot, schema_id, table_name, path) "
					 "VALUES (%ld, '%s', %ld, %ld, %s, %s) RETURNING table_id",
					 tableId, "00000000-0000-0000-0000-000000000000",
					 snapshot->snapshotId, schemaId,
					 quote_literal_cstr(tableName), quote_literal_cstr(path));
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "Failed to register table");

	/* Update snapshot with new next_catalog_id */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_catalog_id = %ld WHERE snapshot_id = %ld",
					 snapshot->nextCatalogId, snapshot->snapshotId);
	SPI_exec(query.data, 0);

	SPI_finish();
	pfree(snapshot);

	/* Register table columns */
	if (OidIsValid(tableOid))
		DucklakeRegisterTableColumns(tableOid, tableId);

	return tableId;
}

DucklakeTableMetadata *
DucklakeGetTableMetadata(Oid tableOid)
{
	StringInfoData query;
	DucklakeTableMetadata *metadata;
	int ret;
	char	   *schemaName;
	char	   *tableName;

	/* Get the table and schema name from the PostgreSQL catalog */
	schemaName = get_namespace_name(get_rel_namespace(tableOid));
	tableName = get_rel_name(tableOid);

	if (!schemaName || !tableName)
		return NULL;

	/* Look up the table in DuckLake metadata by schema and table name */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT t.table_id, t.table_uuid, t.schema_id, t.table_name, "
					 "s.schema_name, t.path, t.path_is_relative, t.begin_snapshot, t.end_snapshot "
					 "FROM lake_ducklake.table t "
					 "JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
					 "WHERE s.schema_name = %s AND t.table_name = %s "
					 "AND t.end_snapshot IS NULL",
					 quote_literal_cstr(schemaName), quote_literal_cstr(tableName));

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return NULL;
	}

	/* Extract all values while SPI context is still active */
	bool isnull;
	int64 tableId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));
	int64 schemaId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 3, &isnull));
	char *tableNameStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
															SPI_tuptable->tupdesc, 4, &isnull));
	char *schemaNameStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
															 SPI_tuptable->tupdesc, 5, &isnull));
	Datum pathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &isnull);
	bool pathIsNull = isnull;
	char *pathStr = pathIsNull ? NULL : TextDatumGetCString(pathDatum);
	bool pathIsRelative = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													  SPI_tuptable->tupdesc, 7, &isnull));
	int64 beginSnapshot = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 8, &isnull));

	SPI_finish();

	/* Allocate metadata in caller's memory context after SPI_finish */
	metadata = (DucklakeTableMetadata *) palloc(sizeof(DucklakeTableMetadata));
	metadata->tableId = tableId;
	metadata->schemaId = schemaId;
	metadata->tableName = pstrdup(tableNameStr);
	metadata->schemaName = pstrdup(schemaNameStr);
	metadata->path = pathIsNull ? NULL : pstrdup(pathStr);
	metadata->pathIsRelative = pathIsRelative;
	metadata->beginSnapshot = beginSnapshot;

	return metadata;
}

DucklakeTableMetadata *
DucklakeGetTableMetadataById(int64 tableId)
{
	StringInfoData query;
	DucklakeTableMetadata *metadata;
	int ret;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT t.table_id, t.table_uuid, t.schema_id, t.table_name, "
					 "s.schema_name, t.path, t.path_is_relative, t.begin_snapshot, t.end_snapshot "
					 "FROM lake_ducklake.table t "
					 "JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
					 "WHERE t.table_id = %ld AND t.end_snapshot IS NULL",
					 tableId);

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return NULL;
	}

	/* Extract all values while SPI context is still active */
	bool isnull;
	int64 tableIdResult = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 1, &isnull));
	int64 schemaId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 3, &isnull));
	char *tableNameStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
															SPI_tuptable->tupdesc, 4, &isnull));
	char *schemaNameStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
															 SPI_tuptable->tupdesc, 5, &isnull));
	Datum pathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &isnull);
	bool pathIsNull = isnull;
	char *pathStr = pathIsNull ? NULL : TextDatumGetCString(pathDatum);
	bool pathIsRelative = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													  SPI_tuptable->tupdesc, 7, &isnull));
	int64 beginSnapshot = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 8, &isnull));

	SPI_finish();

	/* Allocate metadata in caller's memory context after SPI_finish */
	metadata = (DucklakeTableMetadata *) palloc(sizeof(DucklakeTableMetadata));
	metadata->tableId = tableIdResult;
	metadata->schemaId = schemaId;
	metadata->tableName = pstrdup(tableNameStr);
	metadata->schemaName = pstrdup(schemaNameStr);
	metadata->path = pathIsNull ? NULL : pstrdup(pathStr);
	metadata->pathIsRelative = pathIsRelative;
	metadata->beginSnapshot = beginSnapshot;

	return metadata;
}

void
DucklakeDropTable(int64 tableId)
{
	StringInfoData query;

	SPI_connect();

	/*
	 * Delete the table record. Due to ON DELETE CASCADE foreign keys,
	 * this will automatically delete all related records (columns, data_files,
	 * delete_files, file_column_stats, column_mapping, name_mapping, etc.)
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "DELETE FROM lake_ducklake.table WHERE table_id = %ld",
					 tableId);
	SPI_exec(query.data, 0);

	SPI_finish();
}

List *
DucklakeGetDataFiles(int64 tableId, int64 snapshotId)
{
	StringInfoData query;
	List *dataFiles = NIL;
	int ret;
	uint64 i;
	MemoryContext oldcontext;

	if (snapshotId < 0)
	{
		DucklakeSnapshot *snapshot = DucklakeGetCurrentSnapshot();
		snapshotId = snapshot->snapshotId;
		pfree(snapshot);
	}

	/* Save the caller's memory context before entering SPI */
	oldcontext = CurrentMemoryContext;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT data_file_id, table_id, begin_snapshot, end_snapshot, "
					 "path, path_is_relative, file_format, record_count, file_size_bytes, "
					 "row_id_start, partition_id "
					 "FROM lake_ducklake.data_file "
					 "WHERE table_id = %ld AND begin_snapshot <= %ld "
					 "AND (end_snapshot IS NULL OR end_snapshot > %ld) "
					 "ORDER BY file_order",
					 tableId, snapshotId, snapshotId);

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		return NIL;
	}

	/*
	 * Switch to caller's memory context before building the result list.
	 * This ensures both the list cells and the data structures persist
	 * after SPI_finish().
	 */
	MemoryContextSwitchTo(oldcontext);

	/* Build the result list directly from SPI results */
	for (i = 0; i < SPI_processed; i++)
	{
		bool isnull;
		DucklakeDataFile *dataFile = (DucklakeDataFile *) palloc0(sizeof(DucklakeDataFile));

		dataFile->dataFileId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
														   SPI_tuptable->tupdesc, 1, &isnull));
		dataFile->tableId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
													   SPI_tuptable->tupdesc, 2, &isnull));
		dataFile->beginSnapshot = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
															  SPI_tuptable->tupdesc, 3, &isnull));

		Datum endSnapshotDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull);
		dataFile->endSnapshot = isnull ? -1 : DatumGetInt64(endSnapshotDatum);

		char *pathStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
														   SPI_tuptable->tupdesc, 5, &isnull));
		dataFile->path = pstrdup(pathStr);
		dataFile->pathIsRelative = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[i],
															  SPI_tuptable->tupdesc, 6, &isnull));
		char *fileFormatStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
																 SPI_tuptable->tupdesc, 7, &isnull));
		dataFile->fileFormat = pstrdup(fileFormatStr);
		dataFile->recordCount = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
														   SPI_tuptable->tupdesc, 8, &isnull));
		dataFile->fileSizeBytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
															  SPI_tuptable->tupdesc, 9, &isnull));

		Datum rowIdStartDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 10, &isnull);
		dataFile->rowIdStart = isnull ? 0 : DatumGetInt64(rowIdStartDatum);

		Datum partitionIdDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 11, &isnull);
		dataFile->partitionId = isnull ? -1 : DatumGetInt64(partitionIdDatum);

		dataFiles = lappend(dataFiles, dataFile);
	}

	SPI_finish();
	return dataFiles;
}

int64
DucklakeAddDataFile(int64 tableId, const char *path, int64 recordCount,
					int64 fileSizeBytes, int64 rowIdStart)
{
	StringInfoData query;
	DucklakeSnapshot *snapshot;
	int64 dataFileId;
	int ret;

	snapshot = DucklakeGetCurrentSnapshot();
	dataFileId = snapshot->nextFileId++;

	SPI_connect();

	/* Look up the mapping_id and table path for this table */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT cm.mapping_id, t.path "
					 "FROM lake_ducklake.column_mapping cm "
					 "JOIN lake_ducklake.table t ON cm.table_id = t.table_id "
					 "WHERE cm.table_id = %ld",
					 tableId);
	ret = SPI_exec(query.data, 0);

	int64 mappingId = -1;
	char *tablePath = NULL;
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		mappingId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));

		/* Get table path (may be NULL) */
		Datum tablePathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
		if (!isnull)
			tablePath = TextDatumGetCString(tablePathDatum);
	}

	/*
	 * For now, always store absolute paths with path_is_relative = false.
	 * This ensures compatibility with the current pg_lake_table query generation.
	 * TODO: Implement proper relative path support in pg_lake_table FDW.
	 */
	const char *storedPath = path;
	bool pathIsRelative = false;

	/* Insert data file with mapping_id and proper path_is_relative */
	resetStringInfo(&query);
	if (mappingId >= 0)
	{
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.data_file "
						 "(data_file_id, table_id, begin_snapshot, path, path_is_relative, file_format, "
						 "record_count, file_size_bytes, row_id_start, mapping_id) "
						 "VALUES (%ld, %ld, %ld, %s, %s, 'parquet', %ld, %ld, %ld, %ld) RETURNING data_file_id",
						 dataFileId, tableId, snapshot->snapshotId,
						 quote_literal_cstr(storedPath), pathIsRelative ? "true" : "false",
						 recordCount, fileSizeBytes, rowIdStart, mappingId);
	}
	else
	{
		/* No mapping found, insert without mapping_id */
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.data_file "
						 "(data_file_id, table_id, begin_snapshot, path, path_is_relative, file_format, "
						 "record_count, file_size_bytes, row_id_start) "
						 "VALUES (%ld, %ld, %ld, %s, %s, 'parquet', %ld, %ld, %ld) RETURNING data_file_id",
						 dataFileId, tableId, snapshot->snapshotId,
						 quote_literal_cstr(storedPath), pathIsRelative ? "true" : "false",
						 recordCount, fileSizeBytes, rowIdStart);
	}

	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
	{
		SPI_finish();
		pfree(snapshot);
		elog(ERROR, "Failed to add data file");
	}

	/* Update snapshot with new next_file_id */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_file_id = %ld WHERE snapshot_id = %ld",
					 snapshot->nextFileId, snapshot->snapshotId);
	SPI_exec(query.data, 0);

	SPI_finish();
	pfree(snapshot);

	return dataFileId;
}

void
DucklakeRemoveDataFile(int64 dataFileId)
{
	StringInfoData query;
	DucklakeSnapshot *snapshot;

	snapshot = DucklakeGetCurrentSnapshot();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.data_file SET end_snapshot = %ld WHERE data_file_id = %ld",
					 snapshot->snapshotId, dataFileId);

	SPI_connect();
	SPI_exec(query.data, 0);
	SPI_finish();

	pfree(snapshot);
}

void
DucklakeRemoveAllDataFiles(int64 tableId)
{
	StringInfoData query;
	DucklakeSnapshot *snapshot;

	snapshot = DucklakeGetCurrentSnapshot();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.data_file SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND end_snapshot IS NULL",
					 snapshot->snapshotId, tableId);

	SPI_connect();
	SPI_exec(query.data, 0);
	SPI_finish();

	pfree(snapshot);
}

List *
DucklakeGetDeleteFiles(int64 tableId, int64 snapshotId)
{
	StringInfoData query;
	List *deleteFiles = NIL;
	int ret;
	uint64 i;

	if (snapshotId < 0)
	{
		DucklakeSnapshot *snapshot = DucklakeGetCurrentSnapshot();
		snapshotId = snapshot->snapshotId;
		pfree(snapshot);
	}

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT delete_file_id, table_id, begin_snapshot, end_snapshot, "
					 "data_file_id, path, path_is_relative, delete_count, file_size_bytes "
					 "FROM lake_ducklake.delete_file "
					 "WHERE table_id = %ld AND begin_snapshot <= %ld "
					 "AND (end_snapshot IS NULL OR end_snapshot > %ld)",
					 tableId, snapshotId, snapshotId);

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		return NIL;
	}

	/* Build the result list directly from SPI results */
	for (i = 0; i < SPI_processed; i++)
	{
		bool isnull;
		DucklakeDeleteFile *deleteFile = (DucklakeDeleteFile *) MemoryContextAlloc(CurrentMemoryContext, sizeof(DucklakeDeleteFile));

		deleteFile->deleteFileId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
															   SPI_tuptable->tupdesc, 1, &isnull));
		deleteFile->tableId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
														  SPI_tuptable->tupdesc, 2, &isnull));
		deleteFile->beginSnapshot = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
																SPI_tuptable->tupdesc, 3, &isnull));

		Datum endSnapshotDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull);
		deleteFile->endSnapshot = isnull ? -1 : DatumGetInt64(endSnapshotDatum);

		Datum dataFileIdDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 5, &isnull);
		deleteFile->dataFileId = isnull ? -1 : DatumGetInt64(dataFileIdDatum);

		char *pathStr = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
														   SPI_tuptable->tupdesc, 6, &isnull));
		deleteFile->path = MemoryContextStrdup(CurrentMemoryContext, pathStr);
		deleteFile->pathIsRelative = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[i],
																SPI_tuptable->tupdesc, 7, &isnull));
		deleteFile->deleteCount = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
															  SPI_tuptable->tupdesc, 8, &isnull));
		deleteFile->fileSizeBytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
																SPI_tuptable->tupdesc, 9, &isnull));

		deleteFiles = lappend(deleteFiles, deleteFile);
	}

	SPI_finish();
	return deleteFiles;
}

int64
DucklakeAddDeleteFile(int64 tableId, int64 dataFileId, const char *path,
					  int64 deleteCount, int64 fileSizeBytes)
{
	StringInfoData query;
	DucklakeSnapshot *snapshot;
	int64 deleteFileId;
	int ret;

	snapshot = DucklakeGetCurrentSnapshot();
	deleteFileId = snapshot->nextFileId++;

	initStringInfo(&query);
	if (dataFileId >= 0)
	{
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.delete_file "
						 "(delete_file_id, table_id, begin_snapshot, data_file_id, path, "
						 "delete_count, file_size_bytes) "
						 "VALUES (%ld, %ld, %ld, %ld, %s, %ld, %ld) RETURNING delete_file_id",
						 deleteFileId, tableId, snapshot->snapshotId, dataFileId,
						 quote_literal_cstr(path), deleteCount, fileSizeBytes);
	}
	else
	{
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.delete_file "
						 "(delete_file_id, table_id, begin_snapshot, path, "
						 "delete_count, file_size_bytes) "
						 "VALUES (%ld, %ld, %ld, %s, %ld, %ld) RETURNING delete_file_id",
						 deleteFileId, tableId, snapshot->snapshotId,
						 quote_literal_cstr(path), deleteCount, fileSizeBytes);
	}

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
	{
		SPI_finish();
		pfree(snapshot);
		elog(ERROR, "Failed to add delete file");
	}

	/* Update snapshot with new next_file_id */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_file_id = %ld WHERE snapshot_id = %ld",
					 snapshot->nextFileId, snapshot->snapshotId);
	SPI_exec(query.data, 0);

	SPI_finish();
	pfree(snapshot);

	return deleteFileId;
}

void
DucklakeAddFileColumnStats(int64 dataFileId, int64 tableId, int columnOrder,
						   const char *minValue, const char *maxValue)
{
	StringInfoData query;
	int			ret;
	int64		columnId = -1;

	SPI_connect();

	/* Look up the column_id from column_order */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT column_id FROM lake_ducklake.column "
					 "WHERE table_id = %ld AND column_order = %d AND end_snapshot IS NULL",
					 tableId, columnOrder);
	ret = SPI_exec(query.data, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool		isnull;

		columnId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &isnull));
		if (isnull)
			columnId = -1;
	}

	if (columnId < 0)
	{
		SPI_finish();
		elog(WARNING, "Could not find column_id for table_id=%ld column_order=%d",
			 tableId, columnOrder);
		return;
	}

	/* Insert file column stats */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.file_column_stats "
					 "(data_file_id, table_id, column_id, min_value, max_value) "
					 "VALUES (%ld, %ld, %ld, %s, %s) "
					 "ON CONFLICT (data_file_id, column_id) DO UPDATE SET "
					 "min_value = EXCLUDED.min_value, max_value = EXCLUDED.max_value",
					 dataFileId, tableId, columnId,
					 minValue ? quote_literal_cstr(minValue) : "NULL",
					 maxValue ? quote_literal_cstr(maxValue) : "NULL");

	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_INSERT)
	{
		elog(WARNING, "Failed to add file column stats for data_file_id=%ld column_id=%ld",
			 dataFileId, columnId);
	}

	SPI_finish();
}

/*
 * DucklakeAddColumn adds a new column to the DuckLake catalog.
 * This is called after PostgreSQL has added the column to the foreign table.
 * Creates a new snapshot for this schema change.
 */
void
DucklakeAddColumn(Oid tableOid, const char *columnName, const char *columnType,
				  bool nullsAllowed)
{
	DucklakeTableMetadata *metadata;
	int ret;
	int64 columnId;
	AttrNumber columnOrder;

	metadata = DucklakeGetTableMetadata(tableOid);
	if (!metadata)
		elog(ERROR, "DuckLake table metadata not found for table OID %u", tableOid);

	/* Get current snapshot to allocate column_id */
	DucklakeSnapshot *currentSnapshot = DucklakeGetCurrentSnapshot();

	/* Get the column order (attnum) for the new column */
	columnOrder = get_attnum(tableOid, columnName);
	if (columnOrder == InvalidAttrNumber)
		elog(ERROR, "Column %s not found in table OID %u", columnName, tableOid);

	/* Allocate new column_id from current snapshot */
	columnId = currentSnapshot->nextCatalogId++;

	/* Update current snapshot with new next_catalog_id */
	SPI_connect();
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_catalog_id = %ld WHERE snapshot_id = %ld",
					 currentSnapshot->nextCatalogId, currentSnapshot->snapshotId);
	SPI_exec(query.data, 0);
	SPI_finish();

	/* Now create a new snapshot for this schema change */
	DucklakeSnapshot *newSnapshot = DucklakeCreateSnapshot("ADD COLUMN", NULL, NULL);

	/* Increment schema_version and insert column - all within same SPI context */
	SPI_connect();
	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET schema_version = schema_version + 1 "
					 "WHERE snapshot_id = %ld",
					 newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/* Insert the new column with the new snapshot, converting PostgreSQL type to DuckLake type */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.column "
					 "(column_id, begin_snapshot, table_id, column_order, column_name, column_type, nulls_allowed) "
					 "VALUES (%ld, %ld, %ld, %d, %s, lake_ducklake.pg_type_to_duckdb_type(%s), %s)",
					 columnId, newSnapshot->snapshotId, metadata->tableId,
					 columnOrder, quote_literal_cstr(columnName),
					 quote_literal_cstr(columnType), nullsAllowed ? "true" : "false");

	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "Failed to add column %s to DuckLake catalog", columnName);

	/* Update name_mapping for the new column */
	int64 mappingId = -1;
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT mapping_id FROM lake_ducklake.column_mapping WHERE table_id = %ld",
					 metadata->tableId);
	ret = SPI_exec(query.data, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool isnull;
		mappingId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 1, &isnull));
	}

	if (mappingId >= 0)
	{
		resetStringInfo(&query);
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.name_mapping "
						 "(mapping_id, column_id, source_name, target_field_id, is_partition) "
						 "VALUES (%ld, %ld, %s, %ld, false)",
						 mappingId, columnId, quote_literal_cstr(columnName), columnId);
		SPI_exec(query.data, 0);
	}

	SPI_finish();

	pfree(metadata);
	pfree(currentSnapshot);
	pfree(newSnapshot);
}

/*
 * DucklakeDropColumn marks a column as dropped in the DuckLake catalog
 * by setting its end_snapshot to a new snapshot created for this schema change.
 */
void
DucklakeDropColumn(Oid tableOid, const char *columnName)
{
	StringInfoData query;
	DucklakeTableMetadata *metadata;
	DucklakeSnapshot *newSnapshot;
	int ret;

	metadata = DucklakeGetTableMetadata(tableOid);
	if (!metadata)
		elog(ERROR, "DuckLake table metadata not found for table OID %u", tableOid);

	/* Create a new snapshot for this schema change */
	newSnapshot = DucklakeCreateSnapshot("DROP COLUMN", NULL, NULL);

	/* Increment schema_version and set end_snapshot on the column */
	SPI_connect();
	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET schema_version = schema_version + 1 "
					 "WHERE snapshot_id = %ld",
					 newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.column SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND column_name = %s AND end_snapshot IS NULL",
					 newSnapshot->snapshotId, metadata->tableId,
					 quote_literal_cstr(columnName));

	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_UPDATE || SPI_processed == 0)
		elog(WARNING, "Column %s not found in DuckLake catalog for drop", columnName);

	SPI_finish();

	pfree(metadata);
	pfree(newSnapshot);
}

/*
 * DucklakeRenameColumn renames a column in the DuckLake catalog.
 */
void
DucklakeRenameColumn(Oid tableOid, const char *oldName, const char *newName)
{
	StringInfoData query;
	DucklakeTableMetadata *metadata;
	int ret;

	metadata = DucklakeGetTableMetadata(tableOid);
	if (!metadata)
		elog(ERROR, "DuckLake table metadata not found for table OID %u", tableOid);

	SPI_connect();

	/* Update the column name */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.column SET column_name = %s "
					 "WHERE table_id = %ld AND column_name = %s AND end_snapshot IS NULL",
					 quote_literal_cstr(newName), metadata->tableId,
					 quote_literal_cstr(oldName));

	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_UPDATE || SPI_processed == 0)
		elog(WARNING, "Column %s not found in DuckLake catalog for rename", oldName);

	/* Also update name_mapping if it exists */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.name_mapping nm SET source_name = %s "
					 "FROM lake_ducklake.column c "
					 "WHERE nm.column_id = c.column_id "
					 "AND c.table_id = %ld AND nm.source_name = %s",
					 quote_literal_cstr(newName), metadata->tableId,
					 quote_literal_cstr(oldName));
	SPI_exec(query.data, 0);

	SPI_finish();

	pfree(metadata);
}

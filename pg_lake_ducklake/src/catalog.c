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

/*
 * StripTablePathPrefix returns the suffix of absolutePath relative to
 * tablePath when absolutePath sits under tablePath, with
 * *pathIsRelativeOut set to true. Otherwise returns a pstrdup of
 * absolutePath unchanged with *pathIsRelativeOut set to false.
 *
 * Trailing-slash-on-tablePath is normalized (so 's3://b/foo' and
 * 's3://b/foo/' both consume the same number of bytes off the
 * candidate). The returned suffix never has a leading '/'.
 */
static char *
StripTablePathPrefix(const char *tablePath, const char *absolutePath,
					 bool *pathIsRelativeOut)
{
	size_t		prefixLen;

	if (tablePath == NULL || absolutePath == NULL)
	{
		*pathIsRelativeOut = false;
		return pstrdup(absolutePath ? absolutePath : "");
	}

	prefixLen = strlen(tablePath);
	while (prefixLen > 0 && tablePath[prefixLen - 1] == '/')
		prefixLen--;

	if (prefixLen == 0 ||
		strncmp(absolutePath, tablePath, prefixLen) != 0 ||
		absolutePath[prefixLen] != '/')
	{
		*pathIsRelativeOut = false;
		return pstrdup(absolutePath);
	}

	*pathIsRelativeOut = true;
	return pstrdup(absolutePath + prefixLen + 1);
}

char *
DucklakeResolvePath(const char *basePath, const char *relPath, bool isRelative)
{
	if (!isRelative || basePath == NULL || basePath[0] == '\0')
		return pstrdup(relPath ? relPath : "");

	if (relPath == NULL)
		return pstrdup(basePath);

	{
		size_t		baseLen = strlen(basePath);
		StringInfoData buf;

		while (baseLen > 0 && basePath[baseLen - 1] == '/')
			baseLen--;

		while (relPath[0] == '/')
			relPath++;

		initStringInfo(&buf);
		appendStringInfo(&buf, "%.*s/%s", (int) baseLen, basePath, relPath);
		return buf.data;
	}
}


/*
 * ResolveAbsoluteTablePath chains data_path → schema.path → table.path
 * into a single absolute URL. Each piece may be NULL (the higher level
 * is treated as the URL for that subtree) or marked relative. Used by
 * DucklakeGetTableMetadata so all downstream consumers see absolute
 * paths regardless of how the catalog stores them.
 */
static char *
ResolveAbsoluteTablePath(const char *dataPath,
						 const char *schemaPath, bool schemaRel,
						 const char *tablePath, bool tableRel)
{
	char	   *schemaAbs = DucklakeResolvePath(dataPath, schemaPath, schemaRel);
	char	   *tableAbs = DucklakeResolvePath(schemaAbs, tablePath, tableRel);

	return tableAbs;
}

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

	/*
	 * Record the change in snapshot_changes. Required by DuckLake v1 spec for
	 * any snapshot beyond the initial one — DuckDB's ducklake extension uses
	 * this row to surface a description of what each snapshot changed.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.snapshot_changes "
					 "(snapshot_id, changes_made, author, commit_message) "
					 "VALUES (%ld, %s, %s, %s)",
					 newSnapshotId,
					 changesMade ? quote_literal_cstr(changesMade) : "NULL",
					 author ? quote_literal_cstr(author) : "NULL",
					 commitMessage ? quote_literal_cstr(commitMessage) : "NULL");
	SPI_exec(query.data, 0);

	SPI_finish();

	/* Allocate new snapshot in caller's memory context after SPI_finish */
	newSnapshot = (DucklakeSnapshot *) palloc(sizeof(DucklakeSnapshot));
	newSnapshot->snapshotId = newSnapshotId;
	newSnapshot->schemaVersion = currentSnapshot->schemaVersion;
	newSnapshot->nextCatalogId = currentSnapshot->nextCatalogId;
	newSnapshot->nextFileId = currentSnapshot->nextFileId;

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

	/*
	 * Capture postgres-side DEFAULTs declared in CREATE TABLE for each
	 * column with one in pg_attrdef, evaluate it to a scalar via SPI,
	 * and update lake_ducklake.column.initial_default / default_value /
	 * default_value_type. Mirrors DucklakeAddColumn — without it, a
	 * CREATE TABLE foo (col TYPE DEFAULT expr) put no default in our
	 * metadata, so reads of pre-INSERT parquet (after ALTER widens the
	 * schema) wouldn't backfill correctly.
	 */
	{
		StringInfoData defaultQ;

		initStringInfo(&defaultQ);
		appendStringInfo(&defaultQ,
						 "SELECT a.attnum, pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) "
						 "FROM pg_catalog.pg_attribute a "
						 "JOIN pg_catalog.pg_attrdef ad "
						 "       ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum "
						 "WHERE a.attrelid = %u AND a.attnum > 0 AND NOT a.attisdropped",
						 tableOid);

		int			drRet = SPI_exec(defaultQ.data, 0);

		if (drRet == SPI_OK_SELECT)
		{
			uint64		nDefaults = SPI_processed;

			for (uint64 i = 0; i < nDefaults; i++)
			{
				bool		dr_isnull;
				int32		attnum = DatumGetInt32(SPI_getbinval(
									SPI_tuptable->vals[i],
									SPI_tuptable->tupdesc, 1, &dr_isnull));
				Datum		exprDat = SPI_getbinval(
									SPI_tuptable->vals[i],
									SPI_tuptable->tupdesc, 2, &dr_isnull);

				if (dr_isnull)
					continue;

				char	   *exprText = TextDatumGetCString(exprDat);

				/*
				 * Evaluate the expression to a scalar text — same trick
				 * as DucklakeAddColumn. Save the rowset before the inner
				 * SPI_exec because SPI_tuptable is shared.
				 */
				char	   *defaultExprCopy = pstrdup(exprText);
				StringInfoData evalQ;

				initStringInfo(&evalQ);
				appendStringInfo(&evalQ, "SELECT (%s)::text", defaultExprCopy);
				int			evalRet = SPI_exec(evalQ.data, 1);
				char	   *resolved = NULL;

				if (evalRet == SPI_OK_SELECT && SPI_processed > 0)
				{
					bool		evalIsNull;
					Datum		v = SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 1, &evalIsNull);

					if (!evalIsNull)
						resolved = pstrdup(TextDatumGetCString(v));
				}

				if (resolved == NULL)
					continue;

				StringInfoData updQ;

				initStringInfo(&updQ);
				appendStringInfo(&updQ,
								 "UPDATE lake_ducklake.column SET "
								 "initial_default = %s, "
								 "default_value = %s, "
								 "default_value_type = 'literal' "
								 "WHERE table_id = %ld "
								 "AND column_order = %d "
								 "AND begin_snapshot = %ld",
								 quote_literal_cstr(resolved),
								 quote_literal_cstr(resolved),
								 tableId, attnum, snapshot->snapshotId);
				SPI_exec(updQ.data, 0);

				/*
				 * Re-run the lookup query so subsequent iterations see
				 * the right SPI_tuptable. We have to re-execute the
				 * outer SELECT every time we burn the rowset.
				 */
				if (i + 1 < nDefaults)
				{
					int			rerun = SPI_exec(defaultQ.data, 0);

					if (rerun != SPI_OK_SELECT)
						break;
				}
			}
		}
	}

	/* Update snapshot with final next_catalog_id */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_catalog_id = %ld WHERE snapshot_id = %ld",
					 snapshot->nextCatalogId, snapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Register an initial schema_versions row for this table at the current
	 * snapshot. v1 spec requires per-table schema-version history; subsequent
	 * ALTERs (DucklakeAddColumn / DucklakeDropColumn) append more rows.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.schema_versions "
					 "(begin_snapshot, schema_version, table_id) "
					 "VALUES (%ld, "
					 "(SELECT schema_version FROM lake_ducklake.snapshot WHERE snapshot_id = %ld), "
					 "%ld)",
					 snapshot->snapshotId, snapshot->snapshotId, tableId);
	SPI_exec(query.data, 0);

	SPI_finish();
}

int64
DucklakeRegisterTable(const char *schemaName, const char *tableName, const char *path, Oid tableOid)
{
	StringInfoData query;
	int64 schemaId, tableId;
	int ret;
	DucklakeSnapshot *snapshot;

	elog(LOG, "DucklakeRegisterTable called: schema=%s, table=%s, path=%s",
		 schemaName, tableName, path ? path : "(null)");

	bool isnull;

	/*
	 * CREATE FOREIGN TABLE IF NOT EXISTS lets ProcessUtility succeed
	 * silently when the postgres-side foreign table already exists, but
	 * our post-process hook still runs and used to call this function
	 * regardless, leading to duplicate (begin_snapshot, table_name) rows
	 * in lake_ducklake.table. Short-circuit here when a live row for
	 * the same (schema_name, table_name) already exists.
	 */
	{
		StringInfoData probeQuery;

		initStringInfo(&probeQuery);
		appendStringInfo(&probeQuery,
						 "SELECT t.table_id FROM lake_ducklake.table t "
						 "JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
						 "WHERE s.schema_name = %s AND t.table_name = %s "
						 "AND t.end_snapshot IS NULL "
						 "AND s.end_snapshot IS NULL",
						 quote_literal_cstr(schemaName), quote_literal_cstr(tableName));

		SPI_connect();
		ret = SPI_exec(probeQuery.data, 0);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			int64		existingTableId =
				DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &isnull));

			SPI_finish();
			elog(LOG,
				 "DucklakeRegisterTable: %s.%s already registered as table_id=%ld; skipping",
				 schemaName, tableName, existingTableId);
			return existingTableId;
		}
		SPI_finish();
	}

	/*
	 * CREATE TABLE in DuckLake v1 must produce a new snapshot so the new
	 * schema/table/column rows have a meaningful begin_snapshot. Without a
	 * fresh snapshot every CREATE TABLE would land on snapshot 0.
	 */
	snapshot = DucklakeCreateSnapshot("CREATE TABLE", NULL, NULL);

	SPI_connect();

	initStringInfo(&query);

	/*
	 * Establish the catalog's data_path. If pg_lake_ducklake.default_location_prefix
	 * is set and metadata.data_path is unset, seed it now so this CREATE
	 * TABLE and every subsequent one can store paths relative to it.
	 */
	if (DucklakeDefaultLocationPrefix != NULL && DucklakeDefaultLocationPrefix[0] != '\0')
	{
		char	   *prefixWithSlash;
		size_t		plen = strlen(DucklakeDefaultLocationPrefix);

		if (plen > 0 && DucklakeDefaultLocationPrefix[plen - 1] == '/')
			prefixWithSlash = pstrdup(DucklakeDefaultLocationPrefix);
		else
			prefixWithSlash = psprintf("%s/", DucklakeDefaultLocationPrefix);

		resetStringInfo(&query);
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.metadata (key, value) "
						 "VALUES ('data_path', %s) "
						 "ON CONFLICT DO NOTHING",
						 quote_literal_cstr(prefixWithSlash));
		SPI_exec(query.data, 0);
	}

	/* Read the (possibly just-seeded) data_path back out. */
	char	   *dataPath = NULL;
	{
		resetStringInfo(&query);
		appendStringInfoString(&query,
							   "SELECT value FROM lake_ducklake.metadata "
							   "WHERE key = 'data_path' LIMIT 1");
		ret = SPI_exec(query.data, 1);
		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 1, &isnull);

			if (!isnull)
				dataPath = TextDatumGetCString(d);
		}
	}

	/* Check if schema exists */
	resetStringInfo(&query);
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
		/*
		 * Create new schema. When data_path is set, store the schema's
		 * own path as '<schema>/' relative so DuckLake-side readers can
		 * compose the full URL the spec way. When data_path is unset
		 * (no GUC, mixed-state legacy catalog), fall back to a NULL
		 * relative path — matches the pre-Phase-2 behaviour.
		 */
		schemaId = snapshot->nextCatalogId++;
		resetStringInfo(&query);
		if (dataPath != NULL)
		{
			char	   *schemaRel = psprintf("%s/", schemaName);

			appendStringInfo(&query,
							 "INSERT INTO lake_ducklake.schema "
							 "(schema_id, schema_uuid, begin_snapshot, schema_name, path, path_is_relative) "
							 "VALUES (%ld, gen_random_uuid(), %ld, %s, %s, true)",
							 schemaId, snapshot->snapshotId,
							 quote_literal_cstr(schemaName),
							 quote_literal_cstr(schemaRel));
		}
		else
		{
			appendStringInfo(&query,
							 "INSERT INTO lake_ducklake.schema "
							 "(schema_id, schema_uuid, begin_snapshot, schema_name) "
							 "VALUES (%ld, gen_random_uuid(), %ld, %s)",
							 schemaId, snapshot->snapshotId,
							 quote_literal_cstr(schemaName));
		}
		SPI_exec(query.data, 0);
	}

	/*
	 * Normalize the user-provided location to end with '/'. DuckLake's
	 * file readers concatenate `table.path || data_file.path` without
	 * inserting a separator, so omitting the trailing slash produces
	 * URLs like 's3://b/footable/data/x.parquet' on the DuckDB side.
	 */
	const char *normalizedPath = path;
	char	   *pathWithSlash = NULL;

	if (path != NULL && *path != '\0' && path[strlen(path) - 1] != '/')
	{
		pathWithSlash = psprintf("%s/", path);
		normalizedPath = pathWithSlash;
	}

	/*
	 * Decide whether the table fits the relative shape: if data_path is
	 * known and the user's normalized location equals
	 * '<data_path><schema>/<table>/', store table.path = '<table>/' with
	 * path_is_relative=true. Otherwise store the absolute location with
	 * path_is_relative=false (the legacy / explicit-location flow).
	 */
	const char *storedTablePath = normalizedPath;
	bool		tablePathIsRelative = false;

	if (dataPath != NULL && normalizedPath != NULL)
	{
		char	   *expected = psprintf("%s%s/%s/", dataPath, schemaName, tableName);

		if (strcmp(normalizedPath, expected) == 0)
		{
			storedTablePath = psprintf("%s/", tableName);
			tablePathIsRelative = true;
		}
		pfree(expected);
	}

	/* Create new table */
	tableId = snapshot->nextCatalogId++;
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.table "
					 "(table_id, table_uuid, begin_snapshot, schema_id, table_name, path, path_is_relative) "
					 "VALUES (%ld, gen_random_uuid(), %ld, %ld, %s, %s, %s) RETURNING table_id",
					 tableId,
					 snapshot->snapshotId, schemaId,
					 quote_literal_cstr(tableName),
					 quote_literal_cstr(storedTablePath),
					 tablePathIsRelative ? "true" : "false");
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
	MemoryContext oldcontext;

	/* Get the table and schema name from the PostgreSQL catalog */
	schemaName = get_namespace_name(get_rel_namespace(tableOid));
	tableName = get_rel_name(tableOid);

	if (!schemaName || !tableName)
		return NULL;

	/* Save the caller's memory context before entering SPI */
	oldcontext = CurrentMemoryContext;

	/*
	 * Look up the table in DuckLake metadata.
	 *
	 * lake_ducklake.table is versioned by (table_id, begin_snapshot),
	 * and DuckDB's ducklake extension can rename a table without
	 * touching pg_class. So a strict match on the current PG name may
	 * miss the live row if it has since been renamed in the catalog
	 * (the row matching pg_class is then end-snapshotted while the
	 * live row carries the new name). The query first finds any
	 * version (live OR historical) for which schema+name match the
	 * caller's identifiers; the outer SELECT then walks back to the
	 * current live row of that table_id.
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "WITH name_match AS ( "
					 "  SELECT t.table_id "
					 "    FROM lake_ducklake.table t "
					 "    JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
					 "   WHERE s.schema_name = %s AND t.table_name = %s "
					 ") "
					 "SELECT t.table_id, t.table_uuid, t.schema_id, t.table_name, "
					 "       s.schema_name, t.path, t.path_is_relative, "
					 "       t.begin_snapshot, t.end_snapshot, "
					 "       s.path, s.path_is_relative, "
					 "       (SELECT value FROM lake_ducklake.metadata "
					 "         WHERE key = 'data_path' LIMIT 1) "
					 "  FROM lake_ducklake.table t "
					 "  JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
					 " WHERE t.table_id IN (SELECT table_id FROM name_match) "
					 "   AND t.end_snapshot IS NULL "
					 " ORDER BY t.begin_snapshot DESC "
					 " LIMIT 1",
					 quote_literal_cstr(schemaName), quote_literal_cstr(tableName));

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return NULL;
	}

	/*
	 * Switch to caller's memory context before extracting values.
	 * This ensures that strings duplicated with pstrdup() persist after SPI_finish().
	 */
	MemoryContextSwitchTo(oldcontext);

	/* Extract all values and duplicate strings in caller's context */
	bool isnull;
	int64 tableId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));
	int64 schemaId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 3, &isnull));
	char *tableNameStr = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
																	SPI_tuptable->tupdesc, 4, &isnull)));
	char *schemaNameStr = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
																	 SPI_tuptable->tupdesc, 5, &isnull)));

	Datum pathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &isnull);
	char *tablePathStr = isnull ? NULL : pstrdup(TextDatumGetCString(pathDatum));
	bool tablePathIsRel = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													 SPI_tuptable->tupdesc, 7, &isnull));
	int64 beginSnapshot = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 8, &isnull));

	Datum schemaPathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 10, &isnull);
	char *schemaPathStr = isnull ? NULL : pstrdup(TextDatumGetCString(schemaPathDatum));
	bool schemaPathIsRel = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													  SPI_tuptable->tupdesc, 11, &isnull));

	Datum dataPathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 12, &isnull);
	char *dataPathStr = isnull ? NULL : pstrdup(TextDatumGetCString(dataPathDatum));

	SPI_finish();

	/*
	 * Resolve to absolute. Downstream consumers receive path as an
	 * absolute URL with pathIsRelative=false regardless of how the
	 * catalog stores it, so all path-using code paths stay agnostic of
	 * the relative/absolute storage shape.
	 */
	char *pathStr = ResolveAbsoluteTablePath(dataPathStr,
											  schemaPathStr, schemaPathIsRel,
											  tablePathStr, tablePathIsRel);

	/* Allocate metadata in caller's memory context after SPI_finish */
	metadata = (DucklakeTableMetadata *) palloc(sizeof(DucklakeTableMetadata));
	metadata->tableId = tableId;
	metadata->schemaId = schemaId;
	metadata->tableName = tableNameStr;
	metadata->schemaName = schemaNameStr;
	metadata->path = pathStr;
	metadata->pathIsRelative = false;
	metadata->beginSnapshot = beginSnapshot;

	return metadata;
}

DucklakeTableMetadata *
DucklakeGetTableMetadataById(int64 tableId)
{
	StringInfoData query;
	DucklakeTableMetadata *metadata;
	int ret;
	MemoryContext oldcontext;

	/* Save the caller's memory context before entering SPI */
	oldcontext = CurrentMemoryContext;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT t.table_id, t.table_uuid, t.schema_id, t.table_name, "
					 "       s.schema_name, t.path, t.path_is_relative, "
					 "       t.begin_snapshot, t.end_snapshot, "
					 "       s.path, s.path_is_relative, "
					 "       (SELECT value FROM lake_ducklake.metadata "
					 "         WHERE key = 'data_path' LIMIT 1) "
					 "  FROM lake_ducklake.table t "
					 "  JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
					 " WHERE t.table_id = %ld AND t.end_snapshot IS NULL",
					 tableId);

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		return NULL;
	}

	/*
	 * Switch to caller's memory context before extracting values.
	 * This ensures that strings duplicated with pstrdup() persist after SPI_finish().
	 */
	MemoryContextSwitchTo(oldcontext);

	/* Extract all values and duplicate strings in caller's context */
	bool isnull;
	int64 tableIdResult = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 1, &isnull));
	int64 schemaId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 3, &isnull));
	char *tableNameStr = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
																	SPI_tuptable->tupdesc, 4, &isnull)));
	char *schemaNameStr = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
																	 SPI_tuptable->tupdesc, 5, &isnull)));
	Datum pathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &isnull);
	char *tablePathStr = isnull ? NULL : pstrdup(TextDatumGetCString(pathDatum));
	bool tablePathIsRel = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													  SPI_tuptable->tupdesc, 7, &isnull));
	int64 beginSnapshot = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc, 8, &isnull));

	Datum schemaPathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 10, &isnull);
	char *schemaPathStr = isnull ? NULL : pstrdup(TextDatumGetCString(schemaPathDatum));
	bool schemaPathIsRel = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													  SPI_tuptable->tupdesc, 11, &isnull));

	Datum dataPathDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 12, &isnull);
	char *dataPathStr = isnull ? NULL : pstrdup(TextDatumGetCString(dataPathDatum));

	SPI_finish();

	char *pathStr = ResolveAbsoluteTablePath(dataPathStr,
											  schemaPathStr, schemaPathIsRel,
											  tablePathStr, tablePathIsRel);

	/* Allocate metadata in caller's memory context after SPI_finish */
	metadata = (DucklakeTableMetadata *) palloc(sizeof(DucklakeTableMetadata));
	metadata->tableId = tableIdResult;
	metadata->schemaId = schemaId;
	metadata->tableName = tableNameStr;
	metadata->schemaName = schemaNameStr;
	metadata->path = pathStr;
	metadata->pathIsRelative = false;
	metadata->beginSnapshot = beginSnapshot;

	return metadata;
}

void
DucklakeDropTable(int64 tableId)
{
	StringInfoData query;
	DucklakeSnapshot *newSnapshot;

	/*
	 * DuckLake v1 keeps dropped tables in metadata so time-travel queries
	 * can still see them. Instead of DELETE we create a new "DROP TABLE"
	 * snapshot and end-snapshot the live row plus all live data/delete
	 * files belonging to it. The old DELETE path lost history and would
	 * have cascade-removed columns/data_files via ON DELETE CASCADE.
	 */
	newSnapshot = DucklakeCreateSnapshot("DROP TABLE", NULL, NULL);

	SPI_connect();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.table SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND end_snapshot IS NULL",
					 newSnapshot->snapshotId, tableId);
	SPI_exec(query.data, 0);

	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.column SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND end_snapshot IS NULL",
					 newSnapshot->snapshotId, tableId);
	SPI_exec(query.data, 0);

	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.data_file SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND end_snapshot IS NULL",
					 newSnapshot->snapshotId, tableId);
	SPI_exec(query.data, 0);

	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.delete_file SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND end_snapshot IS NULL",
					 newSnapshot->snapshotId, tableId);
	SPI_exec(query.data, 0);

	/* Clear the table_stats counters so reads after the drop see zero rows */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.table_stats "
					 "SET record_count = 0, file_size_bytes = 0 "
					 "WHERE table_id = %ld",
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

	/*
	 * Look up the mapping_id and the fully-resolved absolute table path
	 * for this table. table.path may itself be relative (Phase 2), so
	 * we have to chain through schema.path and metadata.data_path.
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT cm.mapping_id, "
					 "       t.path, t.path_is_relative, "
					 "       s.path, s.path_is_relative, "
					 "       (SELECT value FROM lake_ducklake.metadata "
					 "         WHERE key = 'data_path' LIMIT 1) "
					 "  FROM lake_ducklake.column_mapping cm "
					 "  JOIN lake_ducklake.table t ON cm.table_id = t.table_id "
					 "                              AND t.end_snapshot IS NULL "
					 "  JOIN lake_ducklake.schema s ON s.schema_id = t.schema_id "
					 "                              AND s.end_snapshot IS NULL "
					 " WHERE cm.table_id = %ld "
					 " ORDER BY t.begin_snapshot DESC LIMIT 1",
					 tableId);
	ret = SPI_exec(query.data, 0);

	int64 mappingId = -1;
	char *tablePath = NULL;
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool		isnull;
		HeapTuple	row = SPI_tuptable->vals[0];
		TupleDesc	desc = SPI_tuptable->tupdesc;

		mappingId = DatumGetInt64(SPI_getbinval(row, desc, 1, &isnull));

		Datum		d = SPI_getbinval(row, desc, 2, &isnull);
		char	   *tp = isnull ? NULL : TextDatumGetCString(d);
		bool		tpRel = DatumGetBool(SPI_getbinval(row, desc, 3, &isnull));

		d = SPI_getbinval(row, desc, 4, &isnull);
		char	   *sp = isnull ? NULL : TextDatumGetCString(d);
		bool		spRel = DatumGetBool(SPI_getbinval(row, desc, 5, &isnull));

		d = SPI_getbinval(row, desc, 6, &isnull);
		char	   *dp = isnull ? NULL : TextDatumGetCString(d);

		tablePath = ResolveAbsoluteTablePath(dp, sp, spRel, tp, tpRel);
	}

	/*
	 * Store the path relative to the table's path when it sits under it,
	 * so the catalog stays portable across bucket renames. Falls back to
	 * absolute when the file lives outside the table prefix (e.g. an
	 * add_files import) so legacy lookups still work.
	 */
	bool		pathIsRelative = false;
	const char *storedPath = StripTablePathPrefix(tablePath, path, &pathIsRelative);

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

	/*
	 * Re-roll up lake_ducklake.table_stats from live data_file rows. v1
	 * spec exposes per-table totals (record_count, next_row_id,
	 * file_size_bytes) and DuckDB's read path queries this table for
	 * planning. Without it record_count() / SUM(file_size) on a DuckLake
	 * table goes through a full file scan, and DuckDB returns 0 rows for
	 * its built-in metadata views.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.table_stats "
					 "(table_id, record_count, next_row_id, file_size_bytes) "
					 "SELECT %ld, "
					 "       COALESCE(SUM(record_count), 0), "
					 "       COALESCE(MAX(row_id_start + record_count), 0), "
					 "       COALESCE(SUM(file_size_bytes), 0) "
					 "  FROM lake_ducklake.data_file "
					 " WHERE table_id = %ld AND end_snapshot IS NULL "
					 "ON CONFLICT (table_id) DO UPDATE SET "
					 "  record_count = EXCLUDED.record_count, "
					 "  next_row_id = EXCLUDED.next_row_id, "
					 "  file_size_bytes = EXCLUDED.file_size_bytes",
					 tableId, tableId);
	SPI_exec(query.data, 0);

	/*
	 * Roll lake_ducklake.table_column_stats forward from the live
	 * file_column_stats.
	 *
	 * v1 spec uses table_column_stats for cross-file pruning: a query
	 * planner reads the (min, max) bounds and contains_null/contains_nan
	 * flags to decide whether a query that filters on the column needs
	 * to even open the data files for that table.
	 *
	 * Aggregating min/max across files in SQL would require typed MIN/MAX
	 * — text MIN over '5','42' yields '42' (lex order) which is wrong for
	 * any numeric column and silently breaks predicate pushdown. We don't
	 * have the column's storage type cheaply available here, so:
	 *
	 *   - For tables with exactly one live data file we copy the file's
	 *     min/max directly (always correct, no aggregation needed).
	 *   - For tables with multiple live data files we leave min/max NULL.
	 *     "Unknown bounds" is safe — DuckDB / pg_lake disable the column's
	 *     pruning rather than mis-prune.
	 *
	 * We still always insert a row per (table_id, column_id) so DuckDB's
	 * GetGlobalTableStats LEFT JOIN at column_id index 1 doesn't NPE.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "WITH agg AS ( "
					 "  SELECT df.table_id, "
					 "         fcs.column_id, "
					 "         COUNT(*) AS file_count, "
					 "         MIN(fcs.min_value) AS only_min, "
					 "         MAX(fcs.max_value) AS only_max, "
					 "         BOOL_OR(fcs.contains_nan) AS any_nan "
					 "    FROM lake_ducklake.file_column_stats fcs "
					 "    JOIN lake_ducklake.data_file df USING (data_file_id) "
					 "   WHERE df.table_id = %ld AND df.end_snapshot IS NULL "
					 "   GROUP BY df.table_id, fcs.column_id "
					 ") "
					 "INSERT INTO lake_ducklake.table_column_stats "
					 "(table_id, column_id, contains_null, contains_nan, "
					 " min_value, max_value, extra_stats) "
					 "SELECT %ld, c.column_id, NULL, "
					 "       agg.any_nan, "
					 "       CASE WHEN agg.file_count = 1 THEN agg.only_min END, "
					 "       CASE WHEN agg.file_count = 1 THEN agg.only_max END, "
					 "       NULL "
					 "  FROM lake_ducklake.column c "
					 "  LEFT JOIN agg ON agg.column_id = c.column_id "
					 " WHERE c.table_id = %ld AND c.end_snapshot IS NULL "
					 "ON CONFLICT (table_id, column_id) DO UPDATE SET "
					 "  contains_nan = EXCLUDED.contains_nan, "
					 "  min_value = EXCLUDED.min_value, "
					 "  max_value = EXCLUDED.max_value",
					 tableId, tableId, tableId);
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

	/*
	 * Recompute table_stats from live data files now that this one is
	 * end-snapshotted. Joining through data_file gives us the table_id
	 * so we don't have to look it up before SPI_connect.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.table_stats "
					 "(table_id, record_count, next_row_id, file_size_bytes) "
					 "SELECT df_target.table_id, "
					 "       COALESCE(SUM(df_live.record_count), 0), "
					 "       COALESCE(MAX(df_live.row_id_start + df_live.record_count), 0), "
					 "       COALESCE(SUM(df_live.file_size_bytes), 0) "
					 "  FROM lake_ducklake.data_file df_target "
					 "  LEFT JOIN lake_ducklake.data_file df_live "
					 "         ON df_live.table_id = df_target.table_id "
					 "        AND df_live.end_snapshot IS NULL "
					 " WHERE df_target.data_file_id = %ld "
					 " GROUP BY df_target.table_id "
					 "ON CONFLICT (table_id) DO UPDATE SET "
					 "  record_count = EXCLUDED.record_count, "
					 "  next_row_id = EXCLUDED.next_row_id, "
					 "  file_size_bytes = EXCLUDED.file_size_bytes",
					 dataFileId);
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

	/* Table is empty now: zero out table_stats. */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.table_stats "
					 "(table_id, record_count, next_row_id, file_size_bytes) "
					 "VALUES (%ld, 0, 0, 0) "
					 "ON CONFLICT (table_id) DO UPDATE SET "
					 "  record_count = 0, file_size_bytes = 0",
					 tableId);
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

	/*
	 * Capture caller's memory context BEFORE SPI_connect so result list
	 * and its strings outlive SPI_finish; otherwise consumers chase
	 * already-freed memory and crash 100ms+ later.
	 */
	MemoryContext callerContext = CurrentMemoryContext;

	SPI_connect();
	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		return NIL;
	}

	for (i = 0; i < SPI_processed; i++)
	{
		bool isnull;
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);
		DucklakeDeleteFile *deleteFile = palloc0(sizeof(DucklakeDeleteFile));
		MemoryContextSwitchTo(spiContext);

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
		spiContext = MemoryContextSwitchTo(callerContext);
		deleteFile->path = pstrdup(pathStr);
		MemoryContextSwitchTo(spiContext);

		deleteFile->pathIsRelative = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[i],
																SPI_tuptable->tupdesc, 7, &isnull));
		deleteFile->deleteCount = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
															  SPI_tuptable->tupdesc, 8, &isnull));
		deleteFile->fileSizeBytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
																SPI_tuptable->tupdesc, 9, &isnull));

		spiContext = MemoryContextSwitchTo(callerContext);
		deleteFiles = lappend(deleteFiles, deleteFile);
		MemoryContextSwitchTo(spiContext);
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
	char	   *tablePath = NULL;
	bool		pathIsRelative = false;
	const char *storedPath;

	snapshot = DucklakeGetCurrentSnapshot();
	deleteFileId = snapshot->nextFileId++;

	SPI_connect();

	/*
	 * Look up the fully-resolved absolute table path (Phase 2: chain
	 * data_path → schema.path → table.path). StripTablePathPrefix
	 * needs the absolute prefix to do its compare.
	 */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT t.path, t.path_is_relative, "
					 "       s.path, s.path_is_relative, "
					 "       (SELECT value FROM lake_ducklake.metadata "
					 "         WHERE key = 'data_path' LIMIT 1) "
					 "  FROM lake_ducklake.table t "
					 "  JOIN lake_ducklake.schema s ON s.schema_id = t.schema_id "
					 "                              AND s.end_snapshot IS NULL "
					 " WHERE t.table_id = %ld AND t.end_snapshot IS NULL "
					 " ORDER BY t.begin_snapshot DESC LIMIT 1",
					 tableId);
	ret = SPI_exec(query.data, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool		isnull;
		HeapTuple	row = SPI_tuptable->vals[0];
		TupleDesc	desc = SPI_tuptable->tupdesc;

		Datum		d = SPI_getbinval(row, desc, 1, &isnull);
		char	   *tp = isnull ? NULL : TextDatumGetCString(d);
		bool		tpRel = DatumGetBool(SPI_getbinval(row, desc, 2, &isnull));

		d = SPI_getbinval(row, desc, 3, &isnull);
		char	   *sp = isnull ? NULL : TextDatumGetCString(d);
		bool		spRel = DatumGetBool(SPI_getbinval(row, desc, 4, &isnull));

		d = SPI_getbinval(row, desc, 5, &isnull);
		char	   *dp = isnull ? NULL : TextDatumGetCString(d);

		tablePath = ResolveAbsoluteTablePath(dp, sp, spRel, tp, tpRel);
	}

	storedPath = StripTablePathPrefix(tablePath, path, &pathIsRelative);

	resetStringInfo(&query);
	if (dataFileId >= 0)
	{
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.delete_file "
						 "(delete_file_id, table_id, begin_snapshot, data_file_id, path, "
						 " path_is_relative, delete_count, file_size_bytes) "
						 "VALUES (%ld, %ld, %ld, %ld, %s, %s, %ld, %ld) RETURNING delete_file_id",
						 deleteFileId, tableId, snapshot->snapshotId, dataFileId,
						 quote_literal_cstr(storedPath),
						 pathIsRelative ? "true" : "false",
						 deleteCount, fileSizeBytes);
	}
	else
	{
		appendStringInfo(&query,
						 "INSERT INTO lake_ducklake.delete_file "
						 "(delete_file_id, table_id, begin_snapshot, path, "
						 " path_is_relative, delete_count, file_size_bytes) "
						 "VALUES (%ld, %ld, %ld, %s, %s, %ld, %ld) RETURNING delete_file_id",
						 deleteFileId, tableId, snapshot->snapshotId,
						 quote_literal_cstr(storedPath),
						 pathIsRelative ? "true" : "false",
						 deleteCount, fileSizeBytes);
	}

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
DucklakeAddFileColumnStats(int64 dataFileId, int64 tableId, int64 columnId,
						   const char *minValue, const char *maxValue,
						   int64 columnSizeBytes, int64 valueCount, int64 nullCount,
						   int8 containsNan)
{
	StringInfoData query;
	int			ret;

	if (columnId <= 0)
	{
		elog(WARNING, "Invalid column_id=%ld for table_id=%ld", columnId, tableId);
		return;
	}

	SPI_connect();

	initStringInfo(&query);

	/*
	 * Insert per-file column stats. -1 in the int64 fields encodes
	 * "unknown" (the default at construction time), so we write SQL NULL
	 * in that case rather than persisting the sentinel. containsNan is a
	 * tri-state: -1 unknown -> NULL, 0 -> false, 1 -> true.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.file_column_stats "
					 "(data_file_id, table_id, column_id, column_size_bytes, "
					 " value_count, null_count, min_value, max_value, contains_nan) "
					 "VALUES (%ld, %ld, %ld, ",
					 dataFileId, tableId, columnId);

	if (columnSizeBytes < 0)
		appendStringInfoString(&query, "NULL, ");
	else
		appendStringInfo(&query, "%ld, ", columnSizeBytes);

	if (valueCount < 0)
		appendStringInfoString(&query, "NULL, ");
	else
		appendStringInfo(&query, "%ld, ", valueCount);

	if (nullCount < 0)
		appendStringInfoString(&query, "NULL, ");
	else
		appendStringInfo(&query, "%ld, ", nullCount);

	appendStringInfo(&query,
					 "%s, %s, %s) "
					 "ON CONFLICT (data_file_id, column_id) DO UPDATE SET "
					 "min_value = EXCLUDED.min_value, "
					 "max_value = EXCLUDED.max_value, "
					 "column_size_bytes = EXCLUDED.column_size_bytes, "
					 "value_count = EXCLUDED.value_count, "
					 "null_count = EXCLUDED.null_count, "
					 "contains_nan = EXCLUDED.contains_nan",
					 minValue ? quote_literal_cstr(minValue) : "NULL",
					 maxValue ? quote_literal_cstr(maxValue) : "NULL",
					 containsNan < 0 ? "NULL" :
					 (containsNan == 0 ? "FALSE" : "TRUE"));

	ret = SPI_exec(query.data, 0);

	if (ret != SPI_OK_INSERT)
	{
		elog(WARNING, "Failed to add file column stats for data_file_id=%ld column_id=%ld",
			 dataFileId, columnId);
	}

	/*
	 * Roll the new per-file stats up into table_column_stats. DucklakeAddDataFile
	 * seeded a placeholder row with NULL bounds before this column stat
	 * arrived; now that the per-file row exists, re-aggregate so the
	 * single-file case carries verbatim min/max while multi-file cases stay
	 * NULL (text MIN/MAX would mis-prune numerics). See the longer comment
	 * in DucklakeAddDataFile.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "WITH agg AS ( "
					 "  SELECT df.table_id, fcs.column_id, "
					 "         COUNT(*) AS file_count, "
					 "         MIN(fcs.min_value) AS only_min, "
					 "         MAX(fcs.max_value) AS only_max, "
					 "         BOOL_OR(fcs.contains_nan) AS any_nan "
					 "    FROM lake_ducklake.file_column_stats fcs "
					 "    JOIN lake_ducklake.data_file df USING (data_file_id) "
					 "   WHERE df.table_id = %ld AND df.end_snapshot IS NULL "
					 "     AND fcs.column_id = %ld "
					 "   GROUP BY df.table_id, fcs.column_id "
					 ") "
					 "INSERT INTO lake_ducklake.table_column_stats "
					 "(table_id, column_id, contains_null, contains_nan, "
					 " min_value, max_value, extra_stats) "
					 "SELECT %ld, %ld, NULL, "
					 "       agg.any_nan, "
					 "       CASE WHEN agg.file_count = 1 THEN agg.only_min END, "
					 "       CASE WHEN agg.file_count = 1 THEN agg.only_max END, "
					 "       NULL "
					 "  FROM agg "
					 "ON CONFLICT (table_id, column_id) DO UPDATE SET "
					 "  contains_nan = EXCLUDED.contains_nan, "
					 "  min_value = EXCLUDED.min_value, "
					 "  max_value = EXCLUDED.max_value",
					 tableId, columnId, tableId, columnId);
	SPI_exec(query.data, 0);

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

	/* Initialize StringInfo in caller's memory context before SPI operations */
	StringInfoData query;
	initStringInfo(&query);

	/* Update current snapshot with new next_catalog_id */
	SPI_connect();
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET next_catalog_id = %ld WHERE snapshot_id = %ld",
					 currentSnapshot->nextCatalogId, currentSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Capture the postgres-side DEFAULT expression for this attnum and
	 * resolve it to a value text. v1 stores the resolved value in
	 * ducklake_column.initial_default so reads of older parquet files
	 * (written before this ADD COLUMN) can backfill the missing column
	 * with the right scalar instead of NULL.
	 *
	 * pg_get_expr returns the canonical SQL form ("'foo'::text", "99",
	 * "now()") which DuckDB's read_parquet default_value can't always
	 * parse — we therefore execute the expression and cast the result
	 * to text. For static literals this collapses 'unknown'::text to
	 * unknown and 99::int4 to 99; for volatile expressions like now()
	 * it captures the value at ADD-COLUMN time, which is the same
	 * semantics postgres uses for backfilling existing rows.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT (CASE WHEN ad.adbin IS NULL THEN NULL "
					 "             ELSE (pg_catalog.pg_get_expr(ad.adbin, ad.adrelid))::text "
					 "        END) AS expr_text "
					 "FROM (SELECT NULL::text adbin, NULL::oid adrelid) base "
					 "LEFT JOIN pg_catalog.pg_attrdef ad "
					 "       ON ad.adrelid = %u AND ad.adnum = %d",
					 tableOid, columnOrder);

	char	   *initialDefault = NULL;
	int			defaultRet = SPI_exec(query.data, 1);

	if (defaultRet == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
									  1, &isnull);

		if (!isnull)
		{
			char	   *exprText = TextDatumGetCString(d);

			/*
			 * Now actually evaluate the expression text and cast to
			 * text so the stored default is a plain scalar value.
			 */
			StringInfoData evalQ;

			initStringInfo(&evalQ);
			appendStringInfo(&evalQ, "SELECT (%s)::text", exprText);

			int			evalRet = SPI_exec(evalQ.data, 1);

			if (evalRet == SPI_OK_SELECT && SPI_processed > 0)
			{
				bool		evalIsNull;
				Datum		v = SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1, &evalIsNull);

				if (!evalIsNull)
					initialDefault = pstrdup(TextDatumGetCString(v));
			}
		}
	}

	/* Now create a new snapshot for this schema change */
	DucklakeSnapshot *newSnapshot = DucklakeCreateSnapshot("ADD COLUMN", NULL, NULL);

	/* Increment schema_version and insert column - all within same SPI context */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET schema_version = schema_version + 1 "
					 "WHERE snapshot_id = %ld",
					 newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/* Insert the new column with the new snapshot, converting PostgreSQL type to DuckLake type */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.column "
					 "(column_id, begin_snapshot, table_id, column_order, column_name, "
					 "column_type, initial_default, default_value, default_value_type, "
					 "nulls_allowed) "
					 "VALUES (%ld, %ld, %ld, %d, %s, lake_ducklake.pg_type_to_duckdb_type(%s), "
					 "%s, %s, %s, %s)",
					 columnId, newSnapshot->snapshotId, metadata->tableId,
					 columnOrder, quote_literal_cstr(columnName),
					 quote_literal_cstr(columnType),
					 initialDefault ? quote_literal_cstr(initialDefault) : "NULL",
					 initialDefault ? quote_literal_cstr(initialDefault) : "NULL",
					 /*
					  * v1 spec: when a default value is present, the
					  * default_value_type discriminator must say what the
					  * default text means. We resolve postgres expressions
					  * to a literal scalar earlier, so 'literal' is the
					  * correct tag. NULL when there is no default.
					  */
					 initialDefault ? "'literal'" : "NULL",
					 nullsAllowed ? "true" : "false");

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

	/*
	 * Record per-table schema version for this snapshot. v1 spec uses
	 * lake_ducklake.schema_versions for DuckDB compaction and schema lookups.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.schema_versions "
					 "(begin_snapshot, schema_version, table_id) "
					 "VALUES (%ld, "
					 "(SELECT schema_version FROM lake_ducklake.snapshot WHERE snapshot_id = %ld), "
					 "%ld)",
					 newSnapshot->snapshotId, newSnapshot->snapshotId, metadata->tableId);
	SPI_exec(query.data, 0);

	SPI_finish();
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

	/*
	 * Record per-table schema version for this snapshot.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.schema_versions "
					 "(begin_snapshot, schema_version, table_id) "
					 "VALUES (%ld, "
					 "(SELECT schema_version FROM lake_ducklake.snapshot WHERE snapshot_id = %ld), "
					 "%ld)",
					 newSnapshot->snapshotId, newSnapshot->snapshotId, metadata->tableId);
	SPI_exec(query.data, 0);

	SPI_finish();
}

/*
 * DucklakeRenameColumn renames a column in the DuckLake catalog.
 *
 * v1 spec models RENAME as a new column version: end_snapshot the previous
 * (column_id, begin_snapshot) row at the new snapshot and insert a fresh
 * row with the same column_id and the new name.
 */
void
DucklakeRenameColumn(Oid tableOid, const char *oldName, const char *newName)
{
	StringInfoData query;
	DucklakeTableMetadata *metadata;
	DucklakeSnapshot *newSnapshot;
	int ret;

	metadata = DucklakeGetTableMetadata(tableOid);
	if (!metadata)
		elog(ERROR, "DuckLake table metadata not found for table OID %u", tableOid);

	/* Create a new snapshot for this schema change */
	newSnapshot = DucklakeCreateSnapshot("RENAME COLUMN", NULL, NULL);

	SPI_connect();

	/* Increment schema_version on the new snapshot */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET schema_version = schema_version + 1 "
					 "WHERE snapshot_id = %ld",
					 newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Insert a new column-version row reusing the same column_id with the
	 * new name. We INSERT...SELECT from the live row so all other column
	 * attributes (type, defaults, parent_column, etc.) carry forward.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.column "
					 "(column_id, begin_snapshot, end_snapshot, table_id, column_order, "
					 "column_name, column_type, initial_default, default_value, "
					 "default_value_type, default_value_dialect, nulls_allowed, parent_column) "
					 "SELECT column_id, %ld, NULL, table_id, column_order, "
					 "%s, column_type, initial_default, default_value, "
					 "default_value_type, default_value_dialect, nulls_allowed, parent_column "
					 "FROM lake_ducklake.column "
					 "WHERE table_id = %ld AND column_name = %s AND end_snapshot IS NULL",
					 newSnapshot->snapshotId,
					 quote_literal_cstr(newName),
					 metadata->tableId,
					 quote_literal_cstr(oldName));
	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT || SPI_processed == 0)
		elog(WARNING, "Column %s not found in DuckLake catalog for rename", oldName);

	/* End-snapshot the previous live row */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.column SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND column_name = %s AND end_snapshot IS NULL "
					 "AND begin_snapshot < %ld",
					 newSnapshot->snapshotId, metadata->tableId,
					 quote_literal_cstr(oldName), newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Update name_mapping.source_name in place. The (column_id, mapping_id)
	 * identity is unchanged by a rename — only the user-visible name maps
	 * to the new identifier.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.name_mapping nm SET source_name = %s "
					 "FROM lake_ducklake.column c "
					 "WHERE nm.column_id = c.column_id "
					 "AND c.table_id = %ld AND nm.source_name = %s",
					 quote_literal_cstr(newName), metadata->tableId,
					 quote_literal_cstr(oldName));
	SPI_exec(query.data, 0);

	/* Record schema version for this snapshot */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.schema_versions "
					 "(begin_snapshot, schema_version, table_id) "
					 "VALUES (%ld, "
					 "(SELECT schema_version FROM lake_ducklake.snapshot WHERE snapshot_id = %ld), "
					 "%ld)",
					 newSnapshot->snapshotId, newSnapshot->snapshotId, metadata->tableId);
	SPI_exec(query.data, 0);

	SPI_finish();
}


/*
 * DucklakeRenameTable renames a table in the DuckLake catalog.
 *
 * v1 spec models RENAME as a new table version: end_snapshot the previous
 * (table_id, begin_snapshot) row at the new snapshot and insert a fresh
 * row with the same table_id and the new name.
 *
 * Caller must pass the schema name explicitly because by the time this
 * runs, PostgreSQL has already renamed the foreign table — looking up
 * by the new pg_class name would miss the lake_ducklake.table row,
 * which still carries the old name.
 */
void
DucklakeRenameTable(const char *schemaName, const char *oldName, const char *newName)
{
	StringInfoData query;
	DucklakeSnapshot *newSnapshot;
	int64		tableId = -1;
	int			ret;

	/* Look up the table_id for the OLD name within the given schema */
	SPI_connect();
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT t.table_id FROM lake_ducklake.table t "
					 "JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id "
					 "WHERE s.schema_name = %s AND t.table_name = %s "
					 "AND t.end_snapshot IS NULL "
					 "AND s.end_snapshot IS NULL",
					 quote_literal_cstr(schemaName),
					 quote_literal_cstr(oldName));
	ret = SPI_exec(query.data, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		bool		isnull;

		tableId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1, &isnull));
		if (isnull)
			tableId = -1;
	}

	SPI_finish();

	if (tableId < 0)
	{
		elog(WARNING,
			 "DuckLake table %s.%s not found in catalog for rename — "
			 "metadata will not reflect the PG-side rename",
			 schemaName, oldName);
		return;
	}

	/*
	 * Create a new snapshot for this schema change. DucklakeCreateSnapshot
	 * uses its own SPI block, so do it before we re-open SPI below.
	 */
	newSnapshot = DucklakeCreateSnapshot("RENAME TABLE", NULL, NULL);

	SPI_connect();

	/* Bump schema_version on the new snapshot */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.snapshot SET schema_version = schema_version + 1 "
					 "WHERE snapshot_id = %ld",
					 newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Insert a new table-version row reusing the same table_id with the
	 * new name. INSERT...SELECT from the live row so table_uuid,
	 * schema_id, path, path_is_relative carry forward unchanged.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.table "
					 "(table_id, table_uuid, begin_snapshot, end_snapshot, "
					 " schema_id, table_name, path, path_is_relative) "
					 "SELECT table_id, table_uuid, %ld, NULL, "
					 "       schema_id, %s, path, path_is_relative "
					 "FROM lake_ducklake.table "
					 "WHERE table_id = %ld AND end_snapshot IS NULL",
					 newSnapshot->snapshotId,
					 quote_literal_cstr(newName),
					 tableId);
	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT || SPI_processed == 0)
	{
		SPI_finish();
		elog(ERROR, "Failed to insert renamed table row for %s -> %s",
			 oldName, newName);
	}

	/*
	 * End-snapshot the previous live row. Restrict to begin_snapshot <
	 * the new snapshot so we don't accidentally end-snapshot the row we
	 * just inserted.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE lake_ducklake.table SET end_snapshot = %ld "
					 "WHERE table_id = %ld AND end_snapshot IS NULL "
					 "AND begin_snapshot < %ld",
					 newSnapshot->snapshotId, tableId, newSnapshot->snapshotId);
	SPI_exec(query.data, 0);

	/*
	 * Record the schema version pinning this table at the new
	 * snapshot — this is what DuckDB uses to figure out which table
	 * row is current at a given catalog version.
	 */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.schema_versions "
					 "(begin_snapshot, schema_version, table_id) "
					 "VALUES (%ld, "
					 "(SELECT schema_version FROM lake_ducklake.snapshot WHERE snapshot_id = %ld), "
					 "%ld)",
					 newSnapshot->snapshotId, newSnapshot->snapshotId, tableId);
	SPI_exec(query.data, 0);

	SPI_finish();
}

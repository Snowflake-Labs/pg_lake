/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * SQL-callable helpers and INSTEAD OF trigger that replay DuckDB-side
 * DDL into the PG catalog.
 *
 * DuckLake records each catalog mutation as a comma-separated entry in
 * ducklake_snapshot_changes.changes_made (e.g. "created_table:\"sch\".\"t\"",
 * "dropped_table:5", "altered_table:5"). When DuckDB commits a
 * transaction it inserts a row into public.ducklake_snapshot_changes
 * (the writable view over lake_ducklake.snapshot_changes), which fires
 * the snapshot_changes_insert trigger registered here. We parse
 * changes_made and run the matching CREATE / DROP / ALTER FOREIGN
 * TABLE on the PG side so pg_class and pg_attribute stay in lockstep
 * with the catalog.
 *
 * While the replay DDL runs, the process-local DucklakeInDDLReplay
 * flag is set so pg_lake_table's own DDL hooks short-circuit and don't
 * write the change BACK to lake_ducklake.* (which would create a
 * duplicate version row).
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "pg_lake/ducklake/catalog.h"

PG_FUNCTION_INFO_V1(lake_ducklake_set_ddl_replay);
PG_FUNCTION_INFO_V1(lake_ducklake_is_ddl_replay);
PG_FUNCTION_INFO_V1(lake_ducklake_duckdb_type_to_pg_type);
PG_FUNCTION_INFO_V1(lake_ducklake_snapshot_changes_insert);

Datum
lake_ducklake_set_ddl_replay(PG_FUNCTION_ARGS)
{
	DucklakeInDDLReplay = PG_GETARG_BOOL(0);
	PG_RETURN_VOID();
}

Datum
lake_ducklake_is_ddl_replay(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(DucklakeInDDLReplay);
}


/*
 * DuckdbTypeNameToPgTypeName maps a DuckDB / DuckLake type spelling
 * onto a PostgreSQL type spelling we can stamp into a CREATE FOREIGN
 * TABLE / ALTER FOREIGN TABLE … ADD COLUMN clause.
 *
 * The catalog stores types in DuckDB's syntax (INTEGER, BIGINT,
 * VARCHAR, TIMESTAMP, DECIMAL(p,s), etc.). We map back to PG names
 * (integer, bigint, text, timestamp, numeric(p,s), …). Anything we
 * don't know is passed through verbatim -- DDL synthesis treats
 * `format_type`-like fall-through as an acceptable last resort.
 */
static char *
DuckdbTypeNameToPgTypeName(const char *duckTypeName)
{
	if (duckTypeName == NULL)
		return pstrdup("text");

#define DT_EQ(s) (pg_strcasecmp(duckTypeName, (s)) == 0)
#define DT_PFX(s) (pg_strncasecmp(duckTypeName, (s), strlen(s)) == 0)

	if (DT_EQ("tinyint") || DT_EQ("int8") || DT_EQ("uint8") ||
		DT_EQ("smallint") || DT_EQ("int16") || DT_EQ("uint16"))
	{
		if (DT_EQ("uint16"))
			return pstrdup("integer");
		return pstrdup("smallint");
	}
	if (DT_EQ("integer") || DT_EQ("int") || DT_EQ("int32"))
		return pstrdup("integer");
	if (DT_EQ("uint32"))
		return pstrdup("bigint");
	if (DT_EQ("bigint") || DT_EQ("int64"))
		return pstrdup("bigint");
	if (DT_EQ("uint64"))
		return pstrdup("numeric(20)");

	if (DT_EQ("real") || DT_EQ("float") || DT_EQ("float32"))
		return pstrdup("real");
	if (DT_EQ("double") || DT_EQ("float64"))
		return pstrdup("double precision");

	if (DT_PFX("decimal"))
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfoString(&buf, "numeric");
		appendStringInfoString(&buf, duckTypeName + strlen("decimal"));
		return buf.data;
	}
	if (DT_PFX("numeric"))
		return pstrdup(duckTypeName);

	if (DT_EQ("varchar") || DT_EQ("string"))
		return pstrdup("text");
	if (DT_PFX("varchar") || DT_PFX("char"))
		return pstrdup(duckTypeName);

	if (DT_EQ("boolean") || DT_EQ("bool"))
		return pstrdup("boolean");

	if (DT_EQ("date"))
		return pstrdup("date");
	if (DT_EQ("timestamp"))
		return pstrdup("timestamp");
	if (DT_EQ("timestamptz") || DT_EQ("timestamp with time zone"))
		return pstrdup("timestamp with time zone");
	if (DT_EQ("time"))
		return pstrdup("time");
	if (DT_EQ("timetz") || DT_EQ("time with time zone"))
		return pstrdup("time with time zone");

	if (DT_EQ("blob"))
		return pstrdup("bytea");
	if (DT_EQ("uuid"))
		return pstrdup("uuid");
	if (DT_EQ("json"))
		return pstrdup("jsonb");

#undef DT_EQ
#undef DT_PFX

	return pstrdup(duckTypeName);
}


Datum
lake_ducklake_duckdb_type_to_pg_type(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	text	   *input = PG_GETARG_TEXT_PP(0);
	char	   *duckType = text_to_cstring(input);
	char	   *pgType = DuckdbTypeNameToPgTypeName(duckType);

	PG_RETURN_TEXT_P(cstring_to_text(pgType));
}


/* ---- DDL replay helpers --------------------------------------------- */

/*
 * Run a single SQL statement under the DucklakeInDDLReplay guard so
 * pg_lake_table's hooks know not to re-apply the resulting DDL to the
 * catalog. Errors are caught (via an internal sub-transaction) and
 * demoted to LOG: replay is best-effort -- if a foreign table is
 * missing, was renamed concurrently, or our synthesized DDL is
 * invalid, we don't want one bad change to abort the entire DuckLake
 * commit.
 */
static void
RunReplayDDL(const char *sql)
{
	bool		prevReplay = DucklakeInDDLReplay;
	MemoryContext outerContext = CurrentMemoryContext;
	ResourceOwner outerOwner = CurrentResourceOwner;

	DucklakeInDDLReplay = true;
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		SPI_connect();
		SPI_exec(sql, 0);
		SPI_finish();
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(outerContext);
		CurrentResourceOwner = outerOwner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(outerContext);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(outerContext);
		CurrentResourceOwner = outerOwner;
		ereport(LOG,
				(errmsg("DuckLake DDL replay failed: %s", edata->message),
				 errdetail("statement: %s", sql)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
	DucklakeInDDLReplay = prevReplay;
}


/*
 * Lookup (schema_name, table_name) for a table_id, picking the row
 * with the largest begin_snapshot regardless of end_snapshot. The
 * caller (DROP replay) wants the last name pg_class would have known.
 * Returned pointers live in the caller's memory context; NULL on miss.
 */
static bool
LookupTableNameById(int64 tableId, char **schemaNameOut, char **tableNameOut,
					bool liveOnly)
{
	StringInfoData query;
	int			ret;
	bool		found = false;
	MemoryContext callerContext = CurrentMemoryContext;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT s.schema_name, t.table_name "
					 "  FROM lake_ducklake.table t "
					 "  JOIN lake_ducklake.schema s ON s.schema_id = t.schema_id "
					 " WHERE t.table_id = %ld ",
					 tableId);
	if (liveOnly)
		appendStringInfoString(&query, "   AND t.end_snapshot IS NULL ");
	appendStringInfoString(&query, " ORDER BY t.begin_snapshot DESC LIMIT 1");

	SPI_connect();
	ret = SPI_exec(query.data, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		dSchema = SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &isnull);
		Datum		dTable;

		if (!isnull)
		{
			MemoryContext oldContext = MemoryContextSwitchTo(callerContext);

			*schemaNameOut = TextDatumGetCString(dSchema);
			MemoryContextSwitchTo(oldContext);
		}
		else
		{
			*schemaNameOut = NULL;
		}

		dTable = SPI_getbinval(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 2, &isnull);
		if (!isnull)
		{
			MemoryContext oldContext = MemoryContextSwitchTo(callerContext);

			*tableNameOut = TextDatumGetCString(dTable);
			MemoryContextSwitchTo(oldContext);
		}
		else
		{
			*tableNameOut = NULL;
		}

		found = (*schemaNameOut != NULL && *tableNameOut != NULL);
	}
	SPI_finish();
	return found;
}


/*
 * Replay a DuckDB-side DROP TABLE.
 *
 * By the time we get here DuckDB has end-snapshotted every live row
 * for table_id, so pick the row with the largest begin_snapshot --
 * that's the version pg_class still has.
 */
static void
ReplayDropTable(int64 tableId)
{
	char	   *schemaName = NULL;
	char	   *tableName = NULL;
	StringInfoData ddl;

	if (!LookupTableNameById(tableId, &schemaName, &tableName, false))
		return;

	initStringInfo(&ddl);
	appendStringInfo(&ddl, "DROP FOREIGN TABLE IF EXISTS %s.%s",
					 quote_identifier(schemaName),
					 quote_identifier(tableName));
	RunReplayDDL(ddl.data);
}


/*
 * Build the absolute foreign-table location for a freshly-created
 * DuckLake table by combining lake_ducklake.metadata.data_path with
 * the schema and table paths, honoring path_is_relative on each side.
 *
 * Returns NULL if no path can be composed (caller should bail).
 */
static char *
ComputeFullTablePath(const char *schemaPath, bool schemaRel,
					 const char *tablePath, bool tableRel)
{
	StringInfoData buf;
	char	   *dataPath = NULL;
	int			ret;

	if (!tableRel && tablePath != NULL)
		return pstrdup(tablePath);

	if (!schemaRel && schemaPath != NULL)
	{
		initStringInfo(&buf);
		appendStringInfoString(&buf, schemaPath);
		if (tableRel && tablePath != NULL)
			appendStringInfoString(&buf, tablePath);
		return buf.data;
	}

	/*
	 * Either both parts are relative, or both are NULL (a DuckDB-side CREATE
	 * TABLE leaves table.path / schema.path NULL and relies on data_path +
	 * UUIDs at scan time). Either way the catalog's data_path is the only
	 * base we have. Look for a real lake_ducklake.metadata row first, then
	 * fall back to the pg_lake_ducklake.default_location_prefix GUC -- the
	 * catalog can be created (CREATE EXTENSION) before anything has had a
	 * chance to persist data_path, but the user's session may already have
	 * the GUC set. Mirrors the synthetic row exposed by the ducklake_metadata
	 * view.
	 */
	ret = SPI_exec("SELECT value FROM lake_ducklake.metadata "
				   "WHERE key = 'data_path' LIMIT 1",
				   1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			dataPath = TextDatumGetCString(d);
	}

	if (dataPath == NULL &&
		DucklakeDefaultLocationPrefix != NULL &&
		DucklakeDefaultLocationPrefix[0] != '\0')
	{
		size_t		plen = strlen(DucklakeDefaultLocationPrefix);

		if (plen > 0 && DucklakeDefaultLocationPrefix[plen - 1] == '/')
			dataPath = pstrdup(DucklakeDefaultLocationPrefix);
		else
			dataPath = psprintf("%s/", DucklakeDefaultLocationPrefix);
	}
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			dataPath = TextDatumGetCString(d);
	}

	initStringInfo(&buf);
	if (dataPath != NULL)
		appendStringInfoString(&buf, dataPath);
	if (schemaPath != NULL)
		appendStringInfoString(&buf, schemaPath);
	if (tablePath != NULL)
		appendStringInfoString(&buf, tablePath);

	if (buf.len == 0)
	{
		pfree(buf.data);
		return NULL;
	}

	/* Strip trailing slashes -- pg_lake_table validator rejects them. */
	while (buf.len > 0 && buf.data[buf.len - 1] == '/')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	return buf.data;
}


/*
 * Replay a DuckDB-side CREATE TABLE: synthesize a CREATE FOREIGN TABLE
 * with matching columns. pg_class & pg_attribute now mirror the
 * catalog's view of the table.
 */
static void
ReplayCreateTable(const char *schemaName, const char *tableName)
{
	StringInfoData query;
	int			ret;
	int64		tableId = 0;
	char	   *schemaPath = NULL;
	char	   *tablePath = NULL;
	bool		schemaRel = true;
	bool		tableRel = true;
	bool		found = false;
	StringInfoData colList;
	StringInfoData ddl;
	char	   *fullPath;
	MemoryContext callerContext = CurrentMemoryContext;
	MemoryContext oldContext;

	/* Skip if pg_class already has the foreign table. */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT 1 FROM pg_class c "
					 "  JOIN pg_namespace n ON n.oid = c.relnamespace "
					 " WHERE c.relname = %s "
					 "   AND n.nspname = %s "
					 "   AND c.relkind = 'f'",
					 quote_literal_cstr(tableName),
					 quote_literal_cstr(schemaName));

	SPI_connect();
	ret = SPI_exec(query.data, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		SPI_finish();
		return;
	}

	/* Look up the new table's catalog row. */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT t.table_id, t.path, t.path_is_relative, "
					 "       s.path, s.path_is_relative "
					 "  FROM lake_ducklake.table t "
					 "  JOIN lake_ducklake.schema s ON s.schema_id = t.schema_id "
					 " WHERE s.schema_name = %s "
					 "   AND t.table_name = %s "
					 "   AND t.end_snapshot IS NULL "
					 "   AND s.end_snapshot IS NULL "
					 " ORDER BY t.begin_snapshot DESC LIMIT 1",
					 quote_literal_cstr(schemaName),
					 quote_literal_cstr(tableName));
	ret = SPI_exec(query.data, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d;
		HeapTuple	row = SPI_tuptable->vals[0];
		TupleDesc	desc = SPI_tuptable->tupdesc;

		tableId = DatumGetInt64(SPI_getbinval(row, desc, 1, &isnull));
		if (!isnull)
		{
			d = SPI_getbinval(row, desc, 2, &isnull);
			if (!isnull)
			{
				oldContext = MemoryContextSwitchTo(callerContext);
				tablePath = TextDatumGetCString(d);
				MemoryContextSwitchTo(oldContext);
			}
			d = SPI_getbinval(row, desc, 3, &isnull);
			tableRel = !isnull && DatumGetBool(d);
			d = SPI_getbinval(row, desc, 4, &isnull);
			if (!isnull)
			{
				oldContext = MemoryContextSwitchTo(callerContext);
				schemaPath = TextDatumGetCString(d);
				MemoryContextSwitchTo(oldContext);
			}
			d = SPI_getbinval(row, desc, 5, &isnull);
			schemaRel = !isnull && DatumGetBool(d);
			found = true;
		}
	}

	if (!found)
	{
		SPI_finish();
		return;
	}

	fullPath = ComputeFullTablePath(schemaPath, schemaRel, tablePath, tableRel);
	if (fullPath == NULL)
	{
		SPI_finish();
		return;
	}

	/* Build the column list. */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT c.column_name, c.column_type "
					 "  FROM lake_ducklake.column c "
					 " WHERE c.table_id = %ld "
					 "   AND c.end_snapshot IS NULL "
					 "   AND c.parent_column IS NULL "
					 " ORDER BY c.column_order",
					 tableId);
	ret = SPI_exec(query.data, 0);

	initStringInfo(&colList);
	if (ret == SPI_OK_SELECT)
	{
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			HeapTuple	row = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;
			bool		isnull;
			char	   *colName;
			char	   *duckType;
			char	   *pgType;

			colName = TextDatumGetCString(SPI_getbinval(row, desc, 1, &isnull));
			duckType = TextDatumGetCString(SPI_getbinval(row, desc, 2, &isnull));
			pgType = DuckdbTypeNameToPgTypeName(duckType);

			if (colList.len > 0)
				appendStringInfoString(&colList, ", ");
			appendStringInfo(&colList, "%s %s", quote_identifier(colName), pgType);
		}
	}

	if (colList.len == 0)
	{
		SPI_finish();
		return;
	}

	/* CREATE SCHEMA IF NOT EXISTS -- separate SPI run, no replay flag yet. */
	resetStringInfo(&query);
	appendStringInfo(&query, "CREATE SCHEMA IF NOT EXISTS %s",
					 quote_identifier(schemaName));
	SPI_exec(query.data, 0);

	/* Now the actual CREATE FOREIGN TABLE under the replay guard. */
	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "CREATE FOREIGN TABLE %s.%s (%s) "
					 "SERVER pg_lake_ducklake OPTIONS (location %s)",
					 quote_identifier(schemaName),
					 quote_identifier(tableName),
					 colList.data,
					 quote_literal_cstr(fullPath));
	RunReplayDDL(ddl.data);

	SPI_finish();
}


/*
 * Replay a DuckDB-side ALTER TABLE on the columns of `tableId`. We
 * diff lake_ducklake.column (live) against pg_attribute and apply
 * RENAME / ADD / DROP COLUMN as needed. RENAME TO is handled
 * separately by the table-row-insert trigger.
 */
static void
ReplayAlteredTable(int64 tableId)
{
	char	   *schemaName = NULL;
	char	   *tableName = NULL;
	StringInfoData query;
	StringInfoData ddl;
	int			ret;
	Oid			foreignOid = InvalidOid;
	MemoryContext callerContext = CurrentMemoryContext;

	if (!LookupTableNameById(tableId, &schemaName, &tableName, true))
		return;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT c.oid::oid FROM pg_class c "
					 "  JOIN pg_namespace n ON n.oid = c.relnamespace "
					 " WHERE c.relname = %s "
					 "   AND n.nspname = %s "
					 "   AND c.relkind = 'f'",
					 quote_literal_cstr(tableName),
					 quote_literal_cstr(schemaName));

	SPI_connect();
	ret = SPI_exec(query.data, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			foreignOid = DatumGetObjectId(d);
	}

	if (!OidIsValid(foreignOid))
	{
		SPI_finish();
		return;
	}

	/* Step 1 -- RENAME COLUMN. Match prev → live by column_id. */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT live.column_name AS new_name, prev.column_name AS old_name "
					 "  FROM lake_ducklake.column live "
					 "  JOIN lake_ducklake.column prev ON prev.column_id = live.column_id "
					 " WHERE live.table_id = %ld "
					 "   AND live.end_snapshot IS NULL "
					 "   AND prev.end_snapshot IS NOT NULL "
					 "   AND prev.column_name <> live.column_name "
					 "   AND prev.end_snapshot = ( "
					 "       SELECT max(c2.end_snapshot) "
					 "         FROM lake_ducklake.column c2 "
					 "        WHERE c2.column_id = live.column_id "
					 "          AND c2.end_snapshot IS NOT NULL) "
					 "   AND EXISTS ( "
					 "       SELECT 1 FROM pg_attribute a "
					 "        WHERE a.attrelid = %u "
					 "          AND a.attname = prev.column_name "
					 "          AND NOT a.attisdropped AND a.attnum > 0) "
					 "   AND NOT EXISTS ( "
					 "       SELECT 1 FROM pg_attribute a "
					 "        WHERE a.attrelid = %u "
					 "          AND a.attname = live.column_name "
					 "          AND NOT a.attisdropped AND a.attnum > 0)",
					 tableId, foreignOid, foreignOid);
	ret = SPI_exec(query.data, 0);
	if (ret == SPI_OK_SELECT)
	{
		List	   *renames = NIL;
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(callerContext);
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			HeapTuple	row = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;
			bool		isnull;
			char	   *newName = TextDatumGetCString(SPI_getbinval(row, desc, 1, &isnull));
			char	   *oldName = TextDatumGetCString(SPI_getbinval(row, desc, 2, &isnull));

			renames = lappend(renames, list_make2(makeString(oldName),
												  makeString(newName)));
		}
		MemoryContextSwitchTo(oldContext);

		foreach_ptr(List, pair, renames)
		{
			char	   *oldName = strVal(linitial(pair));
			char	   *newName = strVal(lsecond(pair));

			initStringInfo(&ddl);
			appendStringInfo(&ddl,
							 "ALTER FOREIGN TABLE %s.%s RENAME COLUMN %s TO %s",
							 quote_identifier(schemaName), quote_identifier(tableName),
							 quote_identifier(oldName), quote_identifier(newName));
			RunReplayDDL(ddl.data);
		}
	}

	/* Step 2 -- ADD COLUMN. */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT c.column_id, c.column_name, c.column_type "
					 "  FROM lake_ducklake.column c "
					 " WHERE c.table_id = %ld "
					 "   AND c.end_snapshot IS NULL "
					 "   AND c.parent_column IS NULL "
					 "   AND NOT EXISTS ( "
					 "       SELECT 1 FROM pg_attribute a "
					 "        WHERE a.attrelid = %u "
					 "          AND a.attname = c.column_name "
					 "          AND NOT a.attisdropped AND a.attnum > 0) "
					 " ORDER BY c.column_order",
					 tableId, foreignOid);
	ret = SPI_exec(query.data, 0);
	if (ret == SPI_OK_SELECT)
	{
		List	   *adds = NIL;
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(callerContext);
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			HeapTuple	row = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;
			bool		isnull;
			int64		colId = DatumGetInt64(SPI_getbinval(row, desc, 1, &isnull));
			char	   *colName = TextDatumGetCString(SPI_getbinval(row, desc, 2, &isnull));
			char	   *duckType = TextDatumGetCString(SPI_getbinval(row, desc, 3, &isnull));
			char	   *pgType = DuckdbTypeNameToPgTypeName(duckType);

			adds = lappend(adds, list_make3(makeInteger((int) colId),
											makeString(colName),
											makeString(pgType)));
		}
		MemoryContextSwitchTo(oldContext);

		foreach_ptr(List, triple, adds)
		{
			int64		colId = (int64) intVal(linitial(triple));
			char	   *colName = strVal(lsecond(triple));
			char	   *pgType = strVal(lthird(triple));

			initStringInfo(&ddl);
			appendStringInfo(&ddl,
							 "ALTER FOREIGN TABLE %s.%s ADD COLUMN %s %s",
							 quote_identifier(schemaName), quote_identifier(tableName),
							 quote_identifier(colName), pgType);
			RunReplayDDL(ddl.data);

			/*
			 * DuckLake reads parquet by NAME (column_mapping.type =
			 * 'map_by_name'), so a stale name_mapping row pointing at a
			 * previously-dropped column with the same source_name would
			 * surface the dropped column's data into the new column.
			 * DuckDB-side ADD COLUMN doesn't add a fresh name_mapping row, so
			 * do it here: replace any stale row with one that maps
			 * source_name to the new column_id and gives it a target_field_id
			 * equal to its column_id, so the parquet's old field_id no longer
			 * matches.
			 */
			resetStringInfo(&ddl);
			appendStringInfo(&ddl,
							 "DELETE FROM lake_ducklake.name_mapping nm "
							 " USING lake_ducklake.column_mapping cm "
							 " WHERE nm.mapping_id = cm.mapping_id "
							 "   AND cm.table_id = %ld "
							 "   AND nm.source_name = %s",
							 tableId, quote_literal_cstr(colName));
			SPI_exec(ddl.data, 0);

			resetStringInfo(&ddl);
			appendStringInfo(&ddl,
							 "INSERT INTO lake_ducklake.name_mapping "
							 "(mapping_id, column_id, source_name, target_field_id, is_partition) "
							 "SELECT mapping_id, %ld, %s, %ld, false "
							 "  FROM lake_ducklake.column_mapping "
							 " WHERE table_id = %ld",
							 colId, quote_literal_cstr(colName), colId, tableId);
			SPI_exec(ddl.data, 0);
		}
	}

	/* Step 3 -- DROP COLUMN. */
	resetStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT a.attname::text FROM pg_attribute a "
					 " WHERE a.attrelid = %u "
					 "   AND a.attnum > 0 AND NOT a.attisdropped "
					 "   AND NOT EXISTS ( "
					 "       SELECT 1 FROM lake_ducklake.column c "
					 "        WHERE c.table_id = %ld "
					 "          AND c.end_snapshot IS NULL "
					 "          AND c.column_name = a.attname)",
					 foreignOid, tableId);
	ret = SPI_exec(query.data, 0);
	if (ret == SPI_OK_SELECT)
	{
		List	   *drops = NIL;
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(callerContext);
		for (uint64 i = 0; i < SPI_processed; i++)
		{
			HeapTuple	row = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;
			bool		isnull;
			char	   *attName = TextDatumGetCString(SPI_getbinval(row, desc, 1, &isnull));

			drops = lappend(drops, makeString(attName));
		}
		MemoryContextSwitchTo(oldContext);

		foreach_ptr(String, attNameNode, drops)
		{
			char	   *attName = strVal(attNameNode);

			initStringInfo(&ddl);
			appendStringInfo(&ddl,
							 "ALTER FOREIGN TABLE %s.%s DROP COLUMN %s",
							 quote_identifier(schemaName), quote_identifier(tableName),
							 quote_identifier(attName));
			RunReplayDDL(ddl.data);
		}
	}

	SPI_finish();
}


/*
 * Parse a DuckLake quoted identifier pair `"sch"."tbl"` into separate
 * heap-allocated strings. Embedded double quotes are doubled per
 * SQL convention. Returns true on success.
 */
static bool
ParseQualifiedIdent(const char *payload, char **schemaOut, char **tableOut)
{
	const char *p = payload;
	StringInfoData buf;
	char	  **outs[2] = {schemaOut, tableOut};

	for (int i = 0; i < 2; i++)
	{
		if (*p != '"')
			return false;
		p++;

		initStringInfo(&buf);
		while (*p != '\0')
		{
			if (*p == '"')
			{
				if (p[1] == '"')
				{
					appendStringInfoChar(&buf, '"');
					p += 2;
					continue;
				}
				p++;
				break;
			}
			appendStringInfoChar(&buf, *p);
			p++;
		}
		*outs[i] = buf.data;

		if (i == 0)
		{
			if (*p != '.')
				return false;
			p++;
		}
	}

	return *p == '\0';
}


/*
 * INSTEAD OF INSERT trigger on public.ducklake_snapshot_changes.
 *
 * 1. Insert the raw row into lake_ducklake.snapshot_changes.
 * 2. If we're already replaying DDL, don't recurse -- the inner DDL's
 *    own snapshot_changes row would re-enter and loop.
 * 3. Otherwise parse changes_made and dispatch each entry to the
 *    matching replay helper above.
 */
Datum
lake_ducklake_snapshot_changes_insert(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	HeapTuple	newTuple;
	TupleDesc	tupdesc;
	bool		isnull;
	int64		snapshotId;
	char	   *changesMade = NULL;
	char	   *author = NULL;
	char	   *commitMessage = NULL;
	char	   *commitExtraInfo = NULL;
	StringInfoData query;
	int			ret;
	List	   *items = NIL;
	MemoryContext callerContext = CurrentMemoryContext;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR, (errmsg("must be called as trigger")));

	trigdata = (TriggerData *) fcinfo->context;
	newTuple = trigdata->tg_newtuple ? trigdata->tg_newtuple : trigdata->tg_trigtuple;
	tupdesc = trigdata->tg_relation->rd_att;

	snapshotId = DatumGetInt64(SPI_getbinval(newTuple, tupdesc,
											 SPI_fnumber(tupdesc, "snapshot_id"),
											 &isnull));

	{
		Datum		d;

		d = SPI_getbinval(newTuple, tupdesc, SPI_fnumber(tupdesc, "changes_made"), &isnull);
		if (!isnull)
			changesMade = TextDatumGetCString(d);
		d = SPI_getbinval(newTuple, tupdesc, SPI_fnumber(tupdesc, "author"), &isnull);
		if (!isnull)
			author = TextDatumGetCString(d);
		d = SPI_getbinval(newTuple, tupdesc, SPI_fnumber(tupdesc, "commit_message"), &isnull);
		if (!isnull)
			commitMessage = TextDatumGetCString(d);
		d = SPI_getbinval(newTuple, tupdesc, SPI_fnumber(tupdesc, "commit_extra_info"), &isnull);
		if (!isnull)
			commitExtraInfo = TextDatumGetCString(d);
	}

	SPI_connect();

	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO lake_ducklake.snapshot_changes "
					 "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
					 "VALUES (%ld, %s, %s, %s, %s)",
					 snapshotId,
					 changesMade ? quote_literal_cstr(changesMade) : "NULL",
					 author ? quote_literal_cstr(author) : "NULL",
					 commitMessage ? quote_literal_cstr(commitMessage) : "NULL",
					 commitExtraInfo ? quote_literal_cstr(commitExtraInfo) : "NULL");
	ret = SPI_exec(query.data, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "failed to insert into lake_ducklake.snapshot_changes");

	if (changesMade == NULL || changesMade[0] == '\0' || DucklakeInDDLReplay)
	{
		SPI_finish();
		return PointerGetDatum(newTuple);
	}

	/*
	 * Split changes_made into items (comma-separated, with surrounding
	 * whitespace trimmed). Done in callerContext so the strings outlive
	 * SPI_finish.
	 */
	{
		MemoryContext oldContext = MemoryContextSwitchTo(callerContext);
		const char *cursor = changesMade;

		while (*cursor != '\0')
		{
			const char *start;
			const char *end;
			StringInfoData itemBuf;

			while (*cursor == ' ' || *cursor == '\t')
				cursor++;
			start = cursor;
			while (*cursor != '\0' && *cursor != ',')
				cursor++;
			end = cursor;
			while (end > start && (end[-1] == ' ' || end[-1] == '\t'))
				end--;
			if (end > start)
			{
				initStringInfo(&itemBuf);
				appendBinaryStringInfo(&itemBuf, start, end - start);
				items = lappend(items, itemBuf.data);
			}
			if (*cursor == ',')
				cursor++;
		}
		MemoryContextSwitchTo(oldContext);
	}

	SPI_finish();

	/*
	 * Dispatch each parsed item. Replay helpers manage their own
	 * SPI_connect/SPI_finish.
	 */
	foreach_ptr(char, item, items)
	{
		char	   *colon = strchr(item, ':');
		char	   *op;
		char	   *payload;

		if (colon == NULL)
			continue;
		op = pnstrdup(item, colon - item);
		payload = pstrdup(colon + 1);

		if (strcmp(op, "dropped_table") == 0)
		{
			char	   *endp;
			int64		id = strtoll(payload, &endp, 10);

			if (*endp == '\0')
				ReplayDropTable(id);
		}
		else if (strcmp(op, "altered_table") == 0)
		{
			char	   *endp;
			int64		id = strtoll(payload, &endp, 10);

			if (*endp == '\0')
				ReplayAlteredTable(id);
		}
		else if (strcmp(op, "created_table") == 0)
		{
			char	   *schemaName = NULL;
			char	   *tableName = NULL;

			if (ParseQualifiedIdent(payload, &schemaName, &tableName))
				ReplayCreateTable(schemaName, tableName);
		}

		/*
		 * created_view / altered_view / dropped_view / created_schema /
		 * dropped_schema / inserted_into_table / deleted_from_table /
		 * inlined_* / merge_adjacent / rewrite_delete are not yet propagated
		 * to the PG side. Those are tracked as follow-ups.
		 */
	}

	return PointerGetDatum(newTuple);
}

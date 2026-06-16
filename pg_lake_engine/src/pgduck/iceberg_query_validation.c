/*
 * Copyright 2026 Snowflake Inc.
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
 * Query-level Iceberg value clamp/error and type transformations.
 *
 * Two SELECT-wrapping entry points live here, applied to the write
 * query sent to pgduck_server.  See the function-level comments for the
 * exact set of rewrites and why each is required:
 *
 *   - IcebergWrapQueryWithErrorOrClampChecks: clamp/error checks for
 *     out-of-range temporal values and nested-list flattening.
 *   - IcebergWrapQueryWithRewrites: applies a bitmask of per-leaf rewrites
 *     in a single traversal.  Today that is ICEBERG_REWRITE_NATIVE_ENCODE
 *     (INTERVAL/TIMETZ -> Iceberg shape, type-intrinsic) and
 *     ICEBERG_REWRITE_STORAGE_CAST (surface -> persisted storage type for
 *     compatibility divergences such as a nested uuid stored as iceberg
 *     string, driven by the storage schema).  A future rewrite is just
 *     another flag handled in the same pass.
 *
 * All recurse through arrays, composites, maps, and domains.
 *
 * Common validation helpers (policy resolution, IsTemporalType, temporal
 * boundary constants) live in iceberg_validation.c.  Datum-level
 * validation (non-pushdown path) lives in iceberg_datum_validation.c.
 *
 * Temporal boundaries:
 *   - Date: proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 *   - Timestamp/TimestampTZ: 0001-01-01 .. 9999-12-31 23:59:59.999999.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "pg_lake/pgduck/iceberg_query_validation.h"
#include "pg_lake/pgduck/keywords.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"


/* SQL literal boundaries for the query wrapper */
#define ICEBERG_DATE_MIN_LITERAL			"DATE '-4712-01-01'"
#define ICEBERG_DATE_MAX_LITERAL			"DATE '9999-12-31'"
#define ICEBERG_TIMESTAMP_MIN_LITERAL		"TIMESTAMP '0001-01-01 00:00:00'"
#define ICEBERG_TIMESTAMP_MAX_LITERAL		"TIMESTAMP '9999-12-31 23:59:59.999999'"
#define ICEBERG_TIMESTAMPTZ_MIN_LITERAL		"TIMESTAMPTZ '0001-01-01 00:00:00+00'"
#define ICEBERG_TIMESTAMPTZ_MAX_LITERAL		"TIMESTAMPTZ '9999-12-31 23:59:59.999999+00'"

static bool TupleDescNeedsValidation(TupleDesc tupleDesc);
static void GetTemporalLiterals(Oid typeOid,
								const char **minLiteral, const char **maxLiteral,
								const char **typeName, const char **errLabel);
static void AppendClampExpression(StringInfo buf, const char *expr,
								  Oid typeOid);
static void AppendErrorExpression(StringInfo buf, const char *expr,
								  Oid typeOid);
static bool AppendIcebergValidationExpression(StringInfo buf, const char *expr,
											  Oid typeOid, int32 typmod,
											  IcebergOutOfRangePolicy policy,
											  int depth);

static bool TypeNeedsNativeConversion(Oid typeOid);
static bool LeafSubtreeNeedsRewrite(Oid typeOid, int32 typmod, int rewriteKinds,
									Field * storageField);
static void AppendIntervalStructPack(StringInfo buf, const char *expr);
static void AppendTimeTzUtcCast(StringInfo buf, const char *expr);
static bool AppendRewriteExpression(StringInfo buf, const char *expr,
									Oid typeOid, int32 typmod, int depth,
									int rewriteKinds, Field * storageField);


/* ================================================================
 * Query wrapping for Iceberg write validation
 * ================================================================ */

/*
 * TupleDescNeedsValidation returns true if any non-dropped column
 * needs query-level validation: temporal boundary checks (recursing
 * into nested types) or multidimensional array enforcement.
 */
static bool
TupleDescNeedsValidation(TupleDesc tupleDesc)
{
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeNeedsIcebergValidation(attr->atttypid, attr->atttypmod, true))
			return true;
	}

	return false;
}


/*
 * GetTemporalLiterals sets *minLiteral, *maxLiteral, *typeName, and
 * *errLabel for the given temporal type.  For timestamptz the boundaries
 * are in UTC (explicit +00) since Iceberg stores timestamptz as UTC
 * microseconds.
 */
static void
GetTemporalLiterals(Oid typeOid,
					const char **minLiteral, const char **maxLiteral,
					const char **typeName, const char **errLabel)
{
	switch (typeOid)
	{
		case DATEOID:
			*minLiteral = ICEBERG_DATE_MIN_LITERAL;
			*maxLiteral = ICEBERG_DATE_MAX_LITERAL;
			*typeName = "DATE";
			*errLabel = "date";
			break;
		case TIMESTAMPOID:
			*minLiteral = ICEBERG_TIMESTAMP_MIN_LITERAL;
			*maxLiteral = ICEBERG_TIMESTAMP_MAX_LITERAL;
			*typeName = "TIMESTAMP";
			*errLabel = "timestamp";
			break;
		case TIMESTAMPTZOID:
			*minLiteral = ICEBERG_TIMESTAMPTZ_MIN_LITERAL;
			*maxLiteral = ICEBERG_TIMESTAMPTZ_MAX_LITERAL;
			*typeName = "TIMESTAMPTZ";
			*errLabel = "timestamptz";
			break;
		default:
			elog(ERROR, "unexpected temporal type OID: %u", typeOid);
	}
}


/*
 * AppendClampExpression appends a CASE WHEN expression that clamps
 * the named column to its temporal boundary.
 */
static void
AppendClampExpression(StringInfo buf, const char *quotedName, Oid typeOid)
{
	const char *minLiteral;
	const char *maxLiteral;
	const char *typeName;
	const char *errLabel;

	GetTemporalLiterals(typeOid, &minLiteral, &maxLiteral, &typeName, &errLabel);

	appendStringInfo(buf,
					 "CASE WHEN %s < %s THEN %s "
					 "WHEN %s > %s THEN %s "
					 "ELSE %s END",
					 quotedName, minLiteral, minLiteral,
					 quotedName, maxLiteral, maxLiteral,
					 quotedName);
}


/*
 * AppendErrorExpression appends a CASE WHEN expression that raises
 * an error (via DuckDB's error() function) when the column is out of range.
 */
static void
AppendErrorExpression(StringInfo buf, const char *quotedName, Oid typeOid)
{
	const char *minLiteral;
	const char *maxLiteral;
	const char *typeName;
	const char *errLabel;

	GetTemporalLiterals(typeOid, &minLiteral, &maxLiteral, &typeName, &errLabel);

	appendStringInfo(buf,
					 "CASE WHEN %s NOT BETWEEN %s AND %s "
					 "THEN CAST(error(printf('%s out of range: %%s', %s::VARCHAR)) AS %s) "
					 "ELSE %s END",
					 quotedName, minLiteral, maxLiteral,
					 errLabel, quotedName, typeName,
					 quotedName);
}


/*
 * AppendIcebergValidationExpression recursively generates DuckDB SQL
 * that applies Iceberg write validation to an expression of the given
 * type.  Handles temporal boundary clamping/rejection, multidimensional
 * array clamping/rejection via pg_nullify_nested_list() or
 * pg_error_nested_list(), and recurses through arrays (list_transform),
 * composites (struct_pack), maps (map_from_entries + list_transform),
 * and domains.
 *
 * Returns true if a transformed expression was written to buf, false
 * if the type needs no validation (caller should use the original
 * expression).
 *
 * The depth parameter controls lambda variable naming (_x0, _x1, ...)
 * to avoid shadowing in nested list_transform calls.
 */
static bool
AppendIcebergValidationExpression(StringInfo buf, const char *expr,
								  Oid typeOid, int32 typmod,
								  IcebergOutOfRangePolicy policy,
								  int depth)
{
	/* scalar temporal types: emit CASE WHEN expression directly */
	if (IsTemporalType(typeOid))
	{
		if (policy == ICEBERG_OOR_CLAMP)
			AppendClampExpression(buf, expr, typeOid);
		else
			AppendErrorExpression(buf, expr, typeOid);
		return true;
	}

	/*
	 * Array types: clamp (nullify) or reject multidimensional arrays
	 * depending on the policy, then optionally validate elements via
	 * list_transform when the element type needs temporal validation.
	 */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		const char *nestedListFn = (policy == ICEBERG_OOR_CLAMP)
			? "pg_nullify_nested_list"
			: "pg_error_nested_list";

		if (TypeNeedsIcebergValidation(elemType, typmod, true))
		{
			char	   *lambdaVar = psprintf("_x%d", depth);

			appendStringInfo(buf, "list_transform(%s(%s), %s -> ",
							 nestedListFn, expr, lambdaVar);
			AppendIcebergValidationExpression(buf, lambdaVar, elemType, -1,
											  policy, depth + 1);
			appendStringInfoChar(buf, ')');
		}
		else
		{
			appendStringInfo(buf, "%s(%s)", nestedListFn, expr);
		}

		return true;
	}

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);
		bool		keyNeedsValidation = TypeNeedsIcebergValidation(keyType.postgresTypeOid,
																	keyType.postgresTypeMod, true);
		bool		valNeedsValidation = TypeNeedsIcebergValidation(valType.postgresTypeOid,
																	valType.postgresTypeMod, true);

		if (!keyNeedsValidation && !valNeedsValidation)
			return false;

		char	   *lambdaVar = psprintf("_x%d", depth);

		appendStringInfo(buf,
						 "map_from_entries(list_transform(map_entries(%s), %s -> struct_pack(key := ",
						 expr, lambdaVar);

		char	   *keyExpr = psprintf("%s.key", lambdaVar);

		if (keyNeedsValidation)
			AppendIcebergValidationExpression(buf, keyExpr,
											  keyType.postgresTypeOid,
											  keyType.postgresTypeMod,
											  policy, depth + 1);
		else
			appendStringInfoString(buf, keyExpr);

		appendStringInfoString(buf, ", value := ");

		char	   *valExpr = psprintf("%s.value", lambdaVar);

		if (valNeedsValidation)
			AppendIcebergValidationExpression(buf, valExpr,
											  valType.postgresTypeOid,
											  valType.postgresTypeMod,
											  policy, depth + 1);
		else
			appendStringInfoString(buf, valExpr);

		appendStringInfoString(buf, ")))");
		return true;
	}

	/* domain (non-map): unwrap to base type and recurse */
	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
	{
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &typmod);

		return AppendIcebergValidationExpression(buf, expr, baseType, typmod,
												 policy, depth);
	}

	/* composite types: transform fields via struct_pack */
	if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		anyFieldNeedsTransform = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (TypeNeedsIcebergValidation(attr->atttypid, attr->atttypmod,
										   true))
			{
				anyFieldNeedsTransform = true;
				break;
			}
		}

		if (!anyFieldNeedsTransform)
		{
			ReleaseTupleDesc(tupdesc);
			return false;
		}

		appendStringInfo(buf, "CASE WHEN %s IS NOT NULL THEN struct_pack(", expr);

		bool		firstField = true;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (!firstField)
				appendStringInfoString(buf, ", ");

			const char *fieldName = NameStr(attr->attname);
			const char *quotedField = duckdb_quote_identifier(fieldName);
			char	   *fieldExpr = psprintf("%s.%s", expr, quotedField);

			appendStringInfo(buf, "%s := ", quotedField);

			if (!AppendIcebergValidationExpression(buf, fieldExpr,
												   attr->atttypid,
												   attr->atttypmod,
												   policy, depth))
				appendStringInfoString(buf, fieldExpr);

			firstField = false;
		}

		appendStringInfoString(buf, ") ELSE NULL END");
		ReleaseTupleDesc(tupdesc);
		return true;
	}

	return false;
}


/*
 * IcebergWrapQueryWithErrorOrClampChecks wraps a query string with an
 * outer SELECT that applies Iceberg write validation to columns that
 * need it: temporal boundary enforcement (date/timestamp/timestamptz)
 * and multidimensional array enforcement (pg_nullify_nested_list or
 * pg_error_nested_list, depending on the policy).
 *
 * Numeric NaN validation is
 * performed by IcebergErrorOrClampDatum (in iceberg_datum_validation.c)
 * on the PostgreSQL side before the data reaches DuckDB.
 *
 * Returns the original query unchanged if no columns need validation
 * or the policy is ICEBERG_OOR_NONE.
 *
 * Example with clamp policy (table: id int, created_at date):
 *
 *   SELECT id,
 *          CASE WHEN created_at < DATE '-4712-01-01' THEN DATE '-4712-01-01'
 *               WHEN created_at > DATE '9999-12-31' THEN DATE '9999-12-31'
 *               ELSE created_at END AS created_at
 *   FROM (<original_query>) AS __iceberg_oor
 *
 * Example with error policy (same table):
 *
 *   SELECT id,
 *          CASE WHEN created_at NOT BETWEEN DATE '-4712-01-01' AND DATE '9999-12-31'
 *               THEN CAST(error(printf('date out of range: %s', created_at::VARCHAR)) AS DATE)
 *               ELSE created_at END AS created_at
 *   FROM (<original_query>) AS __iceberg_oor
 */
char *
IcebergWrapQueryWithErrorOrClampChecks(char *query, TupleDesc tupleDesc,
									   IcebergOutOfRangePolicy policy,
									   bool queryHasRowId)
{
	if (policy == ICEBERG_OOR_NONE || tupleDesc == NULL ||
		!TupleDescNeedsValidation(tupleDesc))
		return query;

	StringInfoData wrapped;

	initStringInfo(&wrapped);

	appendStringInfoString(&wrapped, "SELECT ");

	bool		firstColumn = true;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");

		const char *quotedName = duckdb_quote_identifier(NameStr(attr->attname));

		StringInfoData exprBuf;

		initStringInfo(&exprBuf);

		if (AppendIcebergValidationExpression(&exprBuf, quotedName,
											  attr->atttypid,
											  attr->atttypmod,
											  policy, 0))
		{
			appendStringInfo(&wrapped, "%s AS %s", exprBuf.data, quotedName);
		}
		else
		{
			appendStringInfoString(&wrapped, quotedName);
		}

		pfree(exprBuf.data);

		firstColumn = false;
	}

	if (queryHasRowId)
	{
		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");
		appendStringInfoString(&wrapped, "_row_id");
	}

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_oor", query);

	return wrapped.data;
}


/* ================================================================
 * Query wrapping for downstream byte-cap size clamp.
 * See IcebergWrapQueryWithSizeClampChecks below.
 * ================================================================ */

/*
 * EscapeColumnNameForErrorMessage returns a palloc'd copy of `name`
 * with single quotes doubled (so it stays inside a SQL string literal)
 * and, when forPrintfFormat is true, percent signs doubled too (so the
 * embedded DuckDB printf format string does not consume the column
 * name as a format specifier).
 *
 * Identifiers can contain `'`, `%`, and other characters when stored
 * quoted (CREATE TABLE t ("o'reilly" text)); without escaping, such
 * names break the wrapper's CASE expression at SQL parse time or
 * surface as a malformed runtime error.  Backslash needs no special
 * handling because DuckDB SQL string literals don't process backslash
 * escapes by default and printf format strings only react to '%'.
 */
static char *
EscapeColumnNameForErrorMessage(const char *name, bool forPrintfFormat)
{
	StringInfoData buf;

	initStringInfo(&buf);

	for (const char *p = name; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoString(&buf, "''");
		else if (forPrintfFormat && *p == '%')
			appendStringInfoString(&buf, "%%");
		else
			appendStringInfoChar(&buf, *p);
	}

	return buf.data;
}


/*
 * AppendIcebergSizeClampExpression emits DuckDB SQL that enforces the
 * size limits on `expr` per the policy:
 *
 *   ICEBERG_OOR_CLAMP — truncate / NULL the value:
 *     - text/varchar/bpchar : iceberg_size_clamp_text(expr, $maxStringBytes)
 *     - bytea               : iceberg_size_clamp_blob(expr, $maxBinaryBytes)
 *     - jsonb / json        : NULL when strlen(expr::VARCHAR) exceeds the
 *                             string limit, since truncating the serialized
 *                             form would yield invalid JSON.
 *     - array / composite /
 *       map                 : NULL when iceberg_byte_size(expr) exceeds
 *                             iceberg_max_nested_type_bytes.
 *
 *   ICEBERG_OOR_ERROR — raise on oversize, including the column name in
 *   the message.  Uses iceberg_size_check_text / _blob for the leaf cases
 *   and DuckDB's built-in error() inside CASE for jsonb/json/containers.
 *
 *   ICEBERG_OOR_NONE — pass through unchanged (function returns false).
 *
 * Returns true when a transformed expression was written to buf; false if
 * the type needs no wrapping (caller emits the bare expression).
 *
 * `columnName` is the source column's name (already SQL-quoted by the
 * caller's call site is not safe — pass the raw string and the helper will
 * single-quote it for embedding into the error message).
 */
static bool
AppendIcebergSizeClampExpression(StringInfo buf, const char *expr,
								 const char *columnName,
								 IcebergOutOfRangePolicy policy,
								 Oid typeOid, int32 typmod)
{
	if (policy == ICEBERG_OOR_NONE)
		return false;

	if (typeOid == TEXTOID || typeOid == VARCHAROID || typeOid == BPCHAROID)
	{
		if (policy == ICEBERG_OOR_ERROR)
		{
			char	   *escapedName = EscapeColumnNameForErrorMessage(columnName, false);

			appendStringInfo(buf,
							 "iceberg_size_check_text(%s, %d, '%s')",
							 expr, IcebergMaxStringBytes, escapedName);
			pfree(escapedName);
		}
		else
			appendStringInfo(buf, "iceberg_size_clamp_text(%s, %d)",
							 expr, IcebergMaxStringBytes);
		return true;
	}

	if (typeOid == BYTEAOID)
	{
		if (policy == ICEBERG_OOR_ERROR)
		{
			char	   *escapedName = EscapeColumnNameForErrorMessage(columnName, false);

			appendStringInfo(buf,
							 "iceberg_size_check_blob(%s, %d, '%s')",
							 expr, IcebergMaxBinaryBytes, escapedName);
			pfree(escapedName);
		}
		else
			appendStringInfo(buf, "iceberg_size_clamp_blob(%s, %d)",
							 expr, IcebergMaxBinaryBytes);
		return true;
	}

	if (typeOid == JSONBOID || typeOid == JSONOID)
	{
		const char *typeLabel = (typeOid == JSONBOID) ? "jsonb" : "json";

		if (policy == ICEBERG_OOR_ERROR)
		{
			char	   *escapedName = EscapeColumnNameForErrorMessage(columnName, true);

			appendStringInfo(buf,
							 "(CASE WHEN strlen(%s::VARCHAR) > %d "
							 "THEN error(printf("
							 "'value of column \"%s\" (%s, %%d bytes) "
							 "exceeds iceberg_max_string_bytes (%d). "
							 "Set out_of_range_values = ''clamp'' on the "
							 "table to truncate oversize values instead "
							 "of erroring.', strlen(%s::VARCHAR))) "
							 "ELSE %s END)",
							 expr, IcebergMaxStringBytes,
							 escapedName, typeLabel,
							 IcebergMaxStringBytes,
							 expr, expr);
			pfree(escapedName);
		}
		else
			appendStringInfo(buf,
							 "(CASE WHEN strlen(%s::VARCHAR) > %d "
							 "THEN NULL ELSE %s END)",
							 expr, IcebergMaxStringBytes, expr);
		return true;
	}

	/*
	 * Containers (array / composite / map): aggregate-only check.  Skip
	 * entirely when the aggregate GUC is disabled.
	 */
	if (IcebergMaxNestedTypeBytes <= 0)
		return false;

	if (OidIsValid(get_element_type(typeOid)) ||
		IsMapTypeOid(typeOid) ||
		get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		const char *typeLabel =
			OidIsValid(get_element_type(typeOid)) ? "array" :
			IsMapTypeOid(typeOid) ? "map" : "composite";

		if (policy == ICEBERG_OOR_ERROR)
		{
			char	   *escapedName = EscapeColumnNameForErrorMessage(columnName, true);

			appendStringInfo(buf,
							 "(CASE WHEN iceberg_byte_size(%s) > %d "
							 "THEN error(printf("
							 "'value of column \"%s\" (%s, %%d bytes) "
							 "exceeds iceberg_max_nested_type_bytes (%d). "
							 "Set out_of_range_values = ''clamp'' on the "
							 "table to truncate oversize values instead "
							 "of erroring.', iceberg_byte_size(%s))) "
							 "ELSE %s END)",
							 expr, IcebergMaxNestedTypeBytes,
							 escapedName, typeLabel,
							 IcebergMaxNestedTypeBytes,
							 expr, expr);
			pfree(escapedName);
		}
		else
			appendStringInfo(buf,
							 "(CASE WHEN iceberg_byte_size(%s) > %d "
							 "THEN NULL ELSE %s END)",
							 expr, IcebergMaxNestedTypeBytes, expr);
		return true;
	}

	/* domain: unwrap to base type and recurse */
	if (get_typtype(typeOid) == TYPTYPE_DOMAIN)
	{
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &typmod);

		return AppendIcebergSizeClampExpression(buf, expr, columnName, policy,
												baseType, typmod);
	}

	return false;
}


/*
 * IcebergWrapQueryWithSizeClampChecks wraps a query with an outer SELECT
 * that enforces the per-column size limits on each clampable column.
 * Behavior on oversize values is selected by `policy`:
 *
 *   - ICEBERG_OOR_ERROR: raise an error identifying the column / type /
 *     exceeded GUC (default for Iceberg tables).
 *   - ICEBERG_OOR_CLAMP: silently truncate / NULL.
 *   - ICEBERG_OOR_NONE: no-op, original query returned.
 *
 * When all GUCs are 0 or no column carries a clampable type, the original
 * query is returned unchanged so the wrapper is free for non-clamping
 * callers.
 *
 * Both the INSERT..SELECT pushdown path (via WriteQueryResultTo) and the
 * snowflake_cdc snapshot path (via AddQueryResultToTable) flow through
 * this wrapper, so they share the same policy-driven behavior as the
 * per-tuple IcebergSizeCheckOrClampSlotInPlace path.
 */
char *
IcebergWrapQueryWithSizeClampChecks(char *query, TupleDesc tupleDesc,
									IcebergOutOfRangePolicy policy,
									bool queryHasRowId)
{
	if (tupleDesc == NULL || policy == ICEBERG_OOR_NONE)
		return query;

	if (IcebergMaxStringBytes == 0 && IcebergMaxBinaryBytes == 0 &&
		IcebergMaxNestedTypeBytes == 0)
		return query;

	bool		needsAnyClamp = false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeNeedsIcebergSizeClamping(attr->atttypid))
		{
			needsAnyClamp = true;
			break;
		}
	}

	if (!needsAnyClamp)
		return query;

	StringInfoData wrapped;

	initStringInfo(&wrapped);
	appendStringInfoString(&wrapped, "SELECT ");

	bool		firstColumn = true;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");

		const char *quotedName =
			duckdb_quote_identifier(NameStr(attr->attname));

		StringInfoData exprBuf;

		initStringInfo(&exprBuf);

		if (AppendIcebergSizeClampExpression(&exprBuf, quotedName,
											 NameStr(attr->attname), policy,
											 attr->atttypid,
											 attr->atttypmod))
		{
			appendStringInfo(&wrapped, "%s AS %s", exprBuf.data, quotedName);
		}
		else
		{
			appendStringInfoString(&wrapped, quotedName);
		}

		pfree(exprBuf.data);

		firstColumn = false;
	}

	if (queryHasRowId)
	{
		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");
		appendStringInfoString(&wrapped, "_row_id");
	}

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_size_clamp", query);

	return wrapped.data;
}


/* ================================================================
 * Query wrapping for native-type -> Iceberg conversion.
 * See IcebergWrapQueryWithNativeTypeConversion below.
 * ================================================================ */

/*
 * TypeNeedsNativeConversion recursively checks whether a type is, or contains,
 * a native DuckDB type (INTERVAL, TIMETZ) whose representation differs from
 * Iceberg's and so must be rewritten on write. This depends only on the type,
 * never on a per-table storage mapping. Recurses through arrays, composites,
 * maps, and domains.
 */
static bool
TypeNeedsNativeConversion(Oid typeOid)
{
	if (typeOid == INTERVALOID || typeOid == TIMETZOID)
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return TypeNeedsNativeConversion(elemType);

	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);

		return TypeNeedsNativeConversion(keyType.postgresTypeOid) ||
			TypeNeedsNativeConversion(valType.postgresTypeOid);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
		return TypeNeedsNativeConversion(getBaseType(typeOid));

	if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		found = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (TypeNeedsNativeConversion(attr->atttypid))
			{
				found = true;
				break;
			}
		}

		ReleaseTupleDesc(tupdesc);
		return found;
	}

	return false;
}


/*
 * AppendIntervalStructPack appends a DuckDB struct_pack expression that
 * decomposes a DuckDB INTERVAL into {months, days, microseconds}.
 *
 * Wraps in CASE WHEN ... IS NOT NULL to preserve NULL semantics (a NULL
 * interval should produce a NULL struct, not a struct of NULL fields).
 */
static void
AppendIntervalStructPack(StringInfo buf, const char *expr)
{
	appendStringInfo(buf,
					 "CASE WHEN %s IS NOT NULL THEN struct_pack("
					 "months := CAST(datepart('year', %s) AS BIGINT) * 12 "
					 "+ CAST(datepart('month', %s) AS BIGINT), "
					 "days := CAST(datepart('day', %s) AS BIGINT), "
					 "microseconds := CAST(datepart('hour', %s) AS BIGINT) * 3600000000 "
					 "+ CAST(datepart('minute', %s) AS BIGINT) * 60000000 "
					 "+ CAST(datepart('microsecond', %s) AS BIGINT)"
					 ") ELSE NULL END",
					 expr, expr, expr, expr, expr, expr, expr);
}


/*
 * AppendTimeTzUtcCast emits the DuckDB expression that produces the
 * Iceberg-storable TIME for a TIMETZ leaf.  See AppendRewriteExpression
 * (ICEBERG_REWRITE_NATIVE_ENCODE) for the why.
 *
 * The inner "CAST(... AS TIMETZ)" is deliberately retained because the
 * leaf expression can arrive at this helper in either of two shapes:
 *
 *   - Top-level TIMETZ columns from read_iceberg / postgres_scan are
 *     already TIME WITH TIME ZONE; the inner cast is a no-op.
 *   - TIMETZ fields living *inside* an Iceberg composite arrive as plain
 *     TIME (Parquet has no time-with-tz type and DuckDB does not recast
 *     struct fields back to TIMETZ on read), and DuckDB has no
 *     timezone(VARCHAR, TIME) overload, so a bare "AT TIME ZONE 'UTC'"
 *     would produce a binder error.  Casting to TIMETZ first lifts the
 *     value to +00 (semantically a no-op under the pg_lake invariant
 *     that those digits are already UTC) and keeps the outer expression
 *     well-typed.
 */
static void
AppendTimeTzUtcCast(StringInfo buf, const char *expr)
{
	appendStringInfo(buf,
					 "CAST(CAST((%s) AS TIMETZ) AT TIME ZONE 'UTC' AS TIME)",
					 expr);
}


/*
 * LeafSubtreeNeedsRewrite returns true when the type subtree rooted at
 * (typeOid, typmod) contains at least one leaf that one of the enabled
 * rewriteKinds must touch.  It is the OR of the per-kind predicates and gates
 * container rebuilds: a struct/array is only repacked when something inside it
 * actually changes, otherwise the whole branch passes through by reference.
 */
static bool
LeafSubtreeNeedsRewrite(Oid typeOid, int32 typmod, int rewriteKinds,
						Field * storageField)
{
	if ((rewriteKinds & ICEBERG_REWRITE_NATIVE_ENCODE) &&
		TypeNeedsNativeConversion(typeOid))
		return true;

	if ((rewriteKinds & ICEBERG_REWRITE_STORAGE_CAST) &&
		TypeHasStorageDivergentLeaf(typeOid, typmod, storageField))
		return true;

	return false;
}


/*
 * AppendRewriteExpression recursively generates the DuckDB expression that
 * applies every enabled rewrite in rewriteKinds to one (sub)value, returning
 * true when it wrote a rewritten expression (false means "emit the value
 * unchanged").  The two rewrite families touch disjoint leaves:
 *
 *   - ICEBERG_REWRITE_NATIVE_ENCODE encodes INTERVAL/TIMETZ leaves into their
 *     Iceberg shape (INTERVAL -> struct(months,days,microseconds), TIMETZ ->
 *     UTC-normalized TIME).  Type-intrinsic, storageSchema unused.
 *   - ICEBERG_REWRITE_STORAGE_CAST casts a surface leaf to the storage type
 *     persisted for it (storageField), e.g. a nested uuid -> string under a
 *     compatibility mode.
 *
 * Containers (array/struct/map/domain) are walked once and rebuilt structurally
 * only where LeafSubtreeNeedsRewrite reports a changing descendant, so every
 * untouched branch is reused by reference.  One traversal handles both
 * families, so a struct mixing an encoded sibling and a storage-cast sibling is
 * rebuilt by a single struct_pack, never two stacked ones.
 *
 * Returns true if a transformed expression was written to buf.
 */
static bool
AppendRewriteExpression(StringInfo buf, const char *expr,
						Oid typeOid, int32 typmod, int depth,
						int rewriteKinds, Field * storageField)
{
	/*
	 * A single scalar leaf is claimed by at most one rewrite family.  The
	 * native leaves (INTERVAL/TIMETZ) are bespoke shapes Iceberg has no type
	 * for, so they are never recorded with a storage divergence in
	 * field_id_mappings, and a storage cast never lands on them.  Handling a
	 * leaf that needs both at once would force us to order the two casts,
	 * which we deliberately do not support today; assert the invariant here
	 * instead of silently applying only one of them.  (On container nodes the
	 * first condition is false, so this is a no-op until we reach an actual
	 * leaf.)
	 */
	Assert(!((typeOid == INTERVALOID || typeOid == TIMETZOID) &&
			 ScalarLeafStorageDiverges(storageField, typeOid, typmod)));

	/* scalar leaf: native encode (INTERVAL/TIMETZ) */
	if (rewriteKinds & ICEBERG_REWRITE_NATIVE_ENCODE)
	{
		if (typeOid == INTERVALOID)
		{
			AppendIntervalStructPack(buf, expr);
			return true;
		}

		if (typeOid == TIMETZOID)
		{
			AppendTimeTzUtcCast(buf, expr);
			return true;
		}
	}

	/* scalar leaf: surface -> storage cast */
	if ((rewriteKinds & ICEBERG_REWRITE_STORAGE_CAST) &&
		ScalarLeafStorageDiverges(storageField, typeOid, typmod))
	{
		DuckDBType	storageDuck =
			GetDuckDBTypeByName(storageField->field.scalar.typeName);

		appendStringInfo(buf, "CAST(%s AS %s)", expr,
						 GetDuckDBTypeName(storageDuck));
		return true;
	}

	/* array types: rewrite elements via list_transform */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		Field	   *elemStorage =
			(storageField != NULL && storageField->type == FIELD_TYPE_LIST) ?
			storageField->field.list.element : NULL;

		/* an array's typmod applies to its element type */
		if (!LeafSubtreeNeedsRewrite(elemType, typmod, rewriteKinds, elemStorage))
			return false;

		char	   *lambdaVar = psprintf("_x%d", depth);

		appendStringInfo(buf, "list_transform(%s, %s -> ", expr, lambdaVar);
		AppendRewriteExpression(buf, lambdaVar, elemType, typmod, depth + 1,
								rewriteKinds, elemStorage);
		appendStringInfoChar(buf, ')');
		return true;
	}

	/*
	 * Map check must precede the generic domain unwrap (maps are domains). A
	 * map only ever carries native encodes: the registration-time DDL rules
	 * forbid the storage-diverging leaves (uuid and friends) inside a map, so
	 * ICEBERG_REWRITE_STORAGE_CAST has nothing to do here.  Gate the whole
	 * branch on the native flag exactly like the scalar native branch above
	 * -- this is the flag check, not a type check: a storage-only pass (the
	 * change-log CSV path, which hands us values already in Iceberg shape)
	 * must leave the map untouched rather than encode an already-encoded
	 * value.
	 */
	if (IsMapTypeOid(typeOid))
	{
		if (!(rewriteKinds & ICEBERG_REWRITE_NATIVE_ENCODE))
			return false;

		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);
		bool		keyNeedsConversion = TypeNeedsNativeConversion(keyType.postgresTypeOid);
		bool		valNeedsConversion = TypeNeedsNativeConversion(valType.postgresTypeOid);

		if (!keyNeedsConversion && !valNeedsConversion)
			return false;

		char	   *lambdaVar = psprintf("_x%d", depth);

		appendStringInfo(buf,
						 "map_from_entries(list_transform(map_entries(%s), %s -> struct_pack(key := ",
						 expr, lambdaVar);

		char	   *keyExpr = psprintf("%s.key", lambdaVar);

		if (keyNeedsConversion)
			AppendRewriteExpression(buf, keyExpr, keyType.postgresTypeOid,
									keyType.postgresTypeMod, depth + 1,
									ICEBERG_REWRITE_NATIVE_ENCODE, NULL);
		else
			appendStringInfoString(buf, keyExpr);

		appendStringInfoString(buf, ", value := ");

		char	   *valExpr = psprintf("%s.value", lambdaVar);

		if (valNeedsConversion)
			AppendRewriteExpression(buf, valExpr, valType.postgresTypeOid,
									valType.postgresTypeMod, depth + 1,
									ICEBERG_REWRITE_NATIVE_ENCODE, NULL);
		else
			appendStringInfoString(buf, valExpr);

		appendStringInfoString(buf, ")))");
		return true;
	}

	/* domain (non-map): unwrap to base type and recurse */
	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
	{
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &typmod);

		return AppendRewriteExpression(buf, expr, baseType, typmod, depth,
									   rewriteKinds, storageField);
	}

	/*
	 * Composite types: rebuild the struct with struct_pack rather than a
	 * direct whole-struct CAST.  struct_pack(field := ...) lets us rewrite
	 * only the fields that change (an encoded INTERVAL/TIMETZ leaf and/or a
	 * storage-cast uuid leaf) while reusing every other field by name and
	 * recursing into nested lists/structs.  Because one traversal handles
	 * both rewrite families, a struct whose fields mix an encoded sibling and
	 * a storage-cast sibling is rebuilt by a single struct_pack here, never
	 * two stacked ones.
	 *
	 * This is a genuine rebuild, not an in-place edit: DuckDB cannot mutate a
	 * struct, so struct_pack produces a brand-new struct (new validity mask +
	 * field set).  Three consequences we rely on: 1. We only reach the
	 * rebuild when the struct actually contains a changing leaf (gated by
	 * LeafSubtreeNeedsRewrite), so unchanged structs are emitted by plain
	 * reference and never repacked.  2. The new struct's validity is reset,
	 * so the surrounding "CASE WHEN <expr> IS NOT NULL THEN struct_pack(...)
	 * ELSE NULL END" restores the parent's NULL-ness (DuckDB's struct IS NOT
	 * NULL is value-level, not the SQL per-field row rule).  3. On write,
	 * Iceberg field ids are reattached to the rebuilt struct's fields by NAME
	 * (see AppendFields in write_data.c), so the new vectors carry the
	 * correct ids regardless of identity or field order.
	 */
	if (typtype == TYPTYPE_COMPOSITE)
	{
		if (!LeafSubtreeNeedsRewrite(typeOid, typmod, rewriteKinds, storageField))
			return false;

		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);

		appendStringInfo(buf, "CASE WHEN %s IS NOT NULL THEN struct_pack(", expr);

		bool		firstField = true;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (!firstField)
				appendStringInfoString(buf, ", ");

			const char *fieldName = NameStr(attr->attname);
			const char *quotedField = duckdb_quote_identifier(fieldName);
			char	   *fieldExpr = psprintf("%s.%s", expr, quotedField);
			Field	   *childStorage =
				StorageStructFieldByName(storageField, fieldName);

			appendStringInfo(buf, "%s := ", quotedField);

			if (!AppendRewriteExpression(buf, fieldExpr, attr->atttypid,
										 attr->atttypmod, depth, rewriteKinds,
										 childStorage))
				appendStringInfoString(buf, fieldExpr);

			firstField = false;
		}

		appendStringInfoString(buf, ") ELSE NULL END");
		ReleaseTupleDesc(tupdesc);
		return true;
	}

	return false;
}


/*
 * IcebergWrapQueryWithRewrites wraps query in an outer SELECT that applies every
 * rewrite in rewriteKinds to each column in a single pass, aliasing every
 * rewritten column back to its original name and passing untouched columns
 * straight through.  AppendRewriteExpression returns whether it actually
 * rewrote a column, which doubles as the "is there any work to do?" check: if no
 * column changes we return the original query untouched (no redundant SELECT
 * nesting).
 *
 * storageSchema is the target table's persisted Iceberg storage schema (or
 * NULL) and is only consulted for the ICEBERG_REWRITE_STORAGE_CAST kind.
 */
char *
IcebergWrapQueryWithRewrites(char *query, TupleDesc tupleDesc,
							 bool queryHasRowId, int rewriteKinds,
							 DataFileSchema * storageSchema)
{
	if (tupleDesc == NULL || rewriteKinds == 0)
		return query;

	StringInfoData wrapped;

	initStringInfo(&wrapped);

	appendStringInfoString(&wrapped, "SELECT ");

	bool		firstColumn = true;
	bool		anyRewritten = false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");

		const char *quotedName = duckdb_quote_identifier(NameStr(attr->attname));
		Field	   *colStorage =
			(rewriteKinds & ICEBERG_REWRITE_STORAGE_CAST) ?
			StorageFieldForColumn(storageSchema, NameStr(attr->attname)) : NULL;

		StringInfoData exprBuf;

		initStringInfo(&exprBuf);

		bool		rewritten = AppendRewriteExpression(&exprBuf, quotedName,
														attr->atttypid,
														attr->atttypmod, 0,
														rewriteKinds,
														colStorage);

		if (rewritten)
		{
			appendStringInfo(&wrapped, "%s AS %s", exprBuf.data, quotedName);
			anyRewritten = true;
		}
		else
		{
			appendStringInfoString(&wrapped, quotedName);
		}

		pfree(exprBuf.data);

		firstColumn = false;
	}

	/* no column needed rewriting: leave the query (and any nesting) untouched */
	if (!anyRewritten)
	{
		pfree(wrapped.data);
		return query;
	}

	if (queryHasRowId)
	{
		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");
		appendStringInfoString(&wrapped, "_row_id");
	}

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_rewrite", query);

	return wrapped.data;
}

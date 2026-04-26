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
 * IcebergWrapQueryWithErrorOrClampChecks embeds validation expressions
 * into the write query sent to pgduck_server.  Currently handles:
 *
 *   - Temporal boundary enforcement (date/timestamp/timestamptz):
 *     clamp or reject values outside the Iceberg-supported range.
 *   - Multidimensional array enforcement: pg_nullify_nested_list()
 *     (clamp) or pg_error_nested_list() (error) for nested lists,
 *     since Iceberg/Parquet only supports flat lists.
 *
 * These checks are applied recursively through arrays, composites,
 * maps, and domains.
 *
 * IcebergWrapQueryWithNativeTypeConversion rewrites DuckDB columns
 * whose native representation differs from the Iceberg target:
 *
 *   - INTERVAL  -> STRUCT(months, days, microseconds) for Iceberg's
 *                  interval representation.
 *   - TIMETZ    -> CAST(x AT TIME ZONE 'UTC' AS TIME), because Iceberg
 *                  has no time-with-timezone type and DuckDB's direct
 *                  CAST(TIMETZ AS TIME) drops the offset without
 *                  shifting the time digits to UTC.
 *
 * Recurses through arrays, composites, maps, and domains.
 *
 * Common validation helpers (policy resolution, IsTemporalType, temporal
 * boundary constants) live in iceberg_validation.c.
 *
 * Datum-level validation (non-pushdown path) lives in
 * iceberg_datum_validation.c.
 *
 * Temporal boundaries:
 *   - Date: proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 *   - Timestamp/TimestampTZ: 0001-01-01 .. 9999-12-31 23:59:59.999999.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "pg_lake/pgduck/iceberg_query_validation.h"
#include "pg_lake/pgduck/map.h"
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
static bool TupleDescHasNativeConversionColumn(TupleDesc tupleDesc);
static void AppendIntervalStructPack(StringInfo buf, const char *expr);
static void AppendTimeTzUtcCast(StringInfo buf, const char *expr);
static bool AppendNativeConversionExpression(StringInfo buf, const char *expr,
											 Oid typeOid, int32 typmod,
											 int depth);


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
			const char *quotedField = quote_identifier(fieldName);
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

		const char *quotedName = quote_identifier(NameStr(attr->attname));

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
 * Query wrapping for native-type -> Iceberg conversion
 *
 * Covers DuckDB types whose on-wire shape differs from Iceberg's:
 *   - INTERVAL: split into STRUCT(months, days, microseconds).
 *   - TIMETZ:   UTC-normalize and cast to TIME, because Iceberg has no
 *               time-with-timezone type and DuckDB's implicit
 *               CAST(TIMETZ AS TIME) drops the offset without shifting
 *               the time digits.
 * ================================================================ */

/*
 * TypeNeedsNativeConversion recursively checks whether a type is, or
 * contains, one of the native DuckDB types that needs rewriting before
 * being written to Iceberg (currently INTERVAL and TIMETZ).  Recurses
 * through arrays, composites, maps, and domains.
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


static bool
TupleDescHasNativeConversionColumn(TupleDesc tupleDesc)
{
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeNeedsNativeConversion(attr->atttypid))
			return true;
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
 * AppendTimeTzUtcCast appends a DuckDB expression that UTC-normalizes a
 * TIMETZ value and casts it to TIME.  This is required because DuckDB's
 * implicit CAST(TIMETZ AS TIME) drops the offset without shifting the
 * time digits, silently corrupting non-UTC values written to Iceberg
 * (which has no time-with-timezone type).
 *
 * The expression we emit deliberately casts the input to TIMETZ first so
 * that it is safe for every source DuckDB can present for a Postgres
 * TIMETZ column:
 *
 *   - Top-level TIMETZ columns from read_iceberg / postgres_scan arrive
 *     as TIME WITH TIME ZONE, where the inner cast is a no-op and AT
 *     TIME ZONE 'UTC' shifts the digits to UTC as required.
 *   - TIMETZ fields living *inside* an Iceberg composite arrive as plain
 *     TIME (Parquet has no time-with-tz type and DuckDB does not recast
 *     struct fields back to TIMETZ on read).  For these, DuckDB has no
 *     timezone(VARCHAR, TIME) overload, so a bare "AT TIME ZONE 'UTC'"
 *     would produce a binder error.  Casting to TIMETZ first lifts the
 *     value to +00 (semantically a no-op, since pg_lake already stored
 *     those digits as UTC), after which the outer AT TIME ZONE 'UTC' /
 *     CAST AS TIME sequence is well-typed.
 *
 * The net effect is the same correct UTC-normalized TIME in both cases.
 */
static void
AppendTimeTzUtcCast(StringInfo buf, const char *expr)
{
	appendStringInfo(buf,
					 "CAST(CAST((%s) AS TIMETZ) AT TIME ZONE 'UTC' AS TIME)",
					 expr);
}


/*
 * AppendNativeConversionExpression recursively generates DuckDB SQL
 * that converts native-only types to their Iceberg-compatible shape:
 *   - INTERVAL -> STRUCT(months, days, microseconds)
 *   - TIMETZ   -> CAST(CAST(<expr> AS TIMETZ) AT TIME ZONE 'UTC' AS TIME)
 *
 * Handles scalars, arrays (list_transform), composites (struct_pack),
 * maps (map_from_entries + list_transform), and domains.
 *
 * Returns true if a transformed expression was written to buf.
 */
static bool
AppendNativeConversionExpression(StringInfo buf, const char *expr,
								 Oid typeOid, int32 typmod,
								 int depth)
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

	/* array types: wrap elements via list_transform */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		if (!TypeNeedsNativeConversion(elemType))
			return false;

		char	   *lambdaVar = psprintf("_x%d", depth);

		appendStringInfo(buf, "list_transform(%s, %s -> ", expr, lambdaVar);
		AppendNativeConversionExpression(buf, lambdaVar, elemType, -1,
										 depth + 1);
		appendStringInfoChar(buf, ')');
		return true;
	}

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
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
			AppendNativeConversionExpression(buf, keyExpr,
											 keyType.postgresTypeOid,
											 keyType.postgresTypeMod,
											 depth + 1);
		else
			appendStringInfoString(buf, keyExpr);

		appendStringInfoString(buf, ", value := ");

		char	   *valExpr = psprintf("%s.value", lambdaVar);

		if (valNeedsConversion)
			AppendNativeConversionExpression(buf, valExpr,
											 valType.postgresTypeOid,
											 valType.postgresTypeMod,
											 depth + 1);
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

		return AppendNativeConversionExpression(buf, expr, baseType, typmod,
												depth);
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

			if (TypeNeedsNativeConversion(attr->atttypid))
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
			const char *quotedField = quote_identifier(fieldName);
			char	   *fieldExpr = psprintf("%s.%s", expr, quotedField);

			appendStringInfo(buf, "%s := ", quotedField);

			if (!AppendNativeConversionExpression(buf, fieldExpr,
												  attr->atttypid,
												  attr->atttypmod,
												  depth))
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
 * IcebergWrapQueryWithNativeTypeConversion wraps a query string with an
 * outer SELECT that rewrites columns whose native DuckDB shape does not
 * match Iceberg's:
 *
 *   - INTERVAL columns are decomposed into
 *     STRUCT(months BIGINT, days BIGINT, microseconds BIGINT).
 *   - TIMETZ columns are UTC-normalized and cast to TIME
 *     (CAST(CAST(<expr> AS TIMETZ) AT TIME ZONE 'UTC' AS TIME)),
 *     because Iceberg has no time-with-timezone type and DuckDB's
 *     direct CAST(TIMETZ AS TIME) drops the offset without shifting
 *     the time digits.  The inner cast to TIMETZ keeps the outer
 *     AT TIME ZONE 'UTC' well-typed when the source is already plain
 *     TIME (e.g. a TIMETZ field read back from an Iceberg composite
 *     column, where the Parquet-level type is TIME).
 *
 * Rewrites recurse through arrays, composites, maps, and domains.
 *
 * Returns the original query unchanged if no column needs conversion.
 */
char *
IcebergWrapQueryWithNativeTypeConversion(char *query, TupleDesc tupleDesc,
										 bool queryHasRowId)
{
	if (tupleDesc == NULL || !TupleDescHasNativeConversionColumn(tupleDesc))
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

		const char *quotedName = quote_identifier(NameStr(attr->attname));

		StringInfoData exprBuf;

		initStringInfo(&exprBuf);

		if (AppendNativeConversionExpression(&exprBuf, quotedName,
											 attr->atttypid,
											 attr->atttypmod, 0))
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

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_native_conv", query);

	return wrapped.data;
}

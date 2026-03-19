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
 * Query-level Iceberg out-of-range validation.
 *
 * IcebergWrapQueryWithErrorOrClampChecks embeds CASE WHEN checks into
 * the write query sent to pgduck_server for temporal boundary
 * enforcement (date/timestamp/timestamptz).
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

static bool TypeContainsTemporal(Oid typeOid);
static bool TupleDescHasTemporalColumn(TupleDesc tupleDesc);
static void GetTemporalLiterals(Oid typeOid,
								const char **minLiteral, const char **maxLiteral,
								const char **typeName, const char **errLabel);
static void AppendClampExpression(StringInfo buf, const char *expr,
								  Oid typeOid);
static void AppendErrorExpression(StringInfo buf, const char *expr,
								  Oid typeOid);
static bool AppendTemporalValidationExpression(StringInfo buf, const char *expr,
											   Oid typeOid, int32 typmod,
											   IcebergOutOfRangePolicy policy,
											   int depth);


/* ================================================================
 * Query wrapping for temporal boundary checks
 * ================================================================ */

/*
 * TypeContainsTemporal recursively checks whether a type contains
 * any temporal component (date/timestamp/timestamptz), including
 * inside arrays, composites, maps, and domains.
 */
static bool
TypeContainsTemporal(Oid typeOid)
{
	if (IsTemporalType(typeOid))
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return TypeContainsTemporal(elemType);

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);

		return TypeContainsTemporal(keyType.postgresTypeOid) ||
			TypeContainsTemporal(valType.postgresTypeOid);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
		return TypeContainsTemporal(getBaseType(typeOid));

	if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		found = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (TypeContainsTemporal(attr->atttypid))
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
 * TupleDescHasTemporalColumn returns true if any non-dropped column
 * in the tuple descriptor contains a temporal type, recursing into
 * nested types (arrays, composites, maps, domains).
 */
static bool
TupleDescHasTemporalColumn(TupleDesc tupleDesc)
{
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeContainsTemporal(attr->atttypid))
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
 * AppendTemporalValidationExpression recursively generates DuckDB SQL
 * that applies temporal boundary validation to an expression of the
 * given type.  Handles scalars, arrays (list_transform), composites
 * (struct_pack), maps (map_from_entries + list_transform), and domains.
 *
 * Returns true if a transformed expression was written to buf, false
 * if the type contains no temporal component (caller should use the
 * original expression).
 *
 * The depth parameter controls lambda variable naming (_x0, _x1, ...)
 * to avoid shadowing in nested list_transform calls.
 */
static bool
AppendTemporalValidationExpression(StringInfo buf, const char *expr,
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

	/* array types: wrap elements via list_transform */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		if (!TypeContainsTemporal(elemType))
			return false;

		char	   *lambdaVar = psprintf("_x%d", depth);

		appendStringInfo(buf, "list_transform(%s, %s -> ", expr, lambdaVar);
		AppendTemporalValidationExpression(buf, lambdaVar, elemType, -1,
										   policy, depth + 1);
		appendStringInfoChar(buf, ')');
		return true;
	}

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);
		bool		keyHasTemporal = TypeContainsTemporal(keyType.postgresTypeOid);
		bool		valHasTemporal = TypeContainsTemporal(valType.postgresTypeOid);

		if (!keyHasTemporal && !valHasTemporal)
			return false;

		char	   *lambdaVar = psprintf("_x%d", depth);

		appendStringInfo(buf,
						 "map_from_entries(list_transform(map_entries(%s), %s -> struct_pack(key := ",
						 expr, lambdaVar);

		char	   *keyExpr = psprintf("%s.key", lambdaVar);

		if (keyHasTemporal)
			AppendTemporalValidationExpression(buf, keyExpr,
											   keyType.postgresTypeOid,
											   keyType.postgresTypeMod,
											   policy, depth + 1);
		else
			appendStringInfoString(buf, keyExpr);

		appendStringInfoString(buf, ", value := ");

		char	   *valExpr = psprintf("%s.value", lambdaVar);

		if (valHasTemporal)
			AppendTemporalValidationExpression(buf, valExpr,
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

		return AppendTemporalValidationExpression(buf, expr, baseType, typmod,
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

			if (TypeContainsTemporal(attr->atttypid))
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

		appendStringInfoString(buf, "struct_pack(");

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

			if (!AppendTemporalValidationExpression(buf, fieldExpr,
													attr->atttypid,
													attr->atttypmod,
													policy, depth))
				appendStringInfoString(buf, fieldExpr);

			firstField = false;
		}

		appendStringInfoChar(buf, ')');
		ReleaseTupleDesc(tupdesc);
		return true;
	}

	return false;
}


/*
 * IcebergWrapQueryWithErrorOrClampChecks wraps a query string with an
 * outer SELECT that applies CASE WHEN checks to temporal columns
 * (date/timestamp/timestamptz) for Iceberg boundary enforcement.
 *
 * Only temporal columns are handled here.  Numeric NaN validation is
 * performed by IcebergErrorOrClampDatum (in iceberg_datum_validation.c)
 * on the PostgreSQL side before the data reaches DuckDB.
 *
 * Returns the original query unchanged if no temporal columns exist
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
	if (policy == ICEBERG_OOR_NONE || tupleDesc == NULL || !TupleDescHasTemporalColumn(tupleDesc))
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

		if (AppendTemporalValidationExpression(&exprBuf, quotedName,
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

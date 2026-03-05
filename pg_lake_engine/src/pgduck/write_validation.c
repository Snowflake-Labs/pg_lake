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
 * Value validation for DuckDB write queries.
 *
 * Wraps a DuckDB query with CASE expressions that validate temporal
 * (date, timestamp, timestamptz) and numeric columns before they are
 * written to Parquet data files.  Depending on the out_of_range_values
 * table or COPY option, out-of-range values either raise an error or
 * are clamped to boundary values.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "pg_lake/pgduck/write_validation.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/numeric.h"
#include "pg_lake/util/rel_utils.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"


/* ================================================================
 * Temporal boundary constants
 *
 * Date supports the full proleptic Gregorian range (-4712 .. 9999).
 * Timestamp/TimestampTZ are limited to the common-era range (1 .. 9999).
 * ================================================================ */
#define TEMPORAL_DATE_MIN_YEAR		"-4712"
#define TEMPORAL_TIMESTAMP_MIN_YEAR	"0001"
#define TEMPORAL_MAX_YEAR			"9999"

#define TEMPORAL_DATE_MIN_LIT		"DATE '" TEMPORAL_DATE_MIN_YEAR "-01-01'"
#define TEMPORAL_DATE_MAX_LIT		"DATE '" TEMPORAL_MAX_YEAR "-12-31'"
#define TEMPORAL_TIMESTAMP_MIN_LIT	"TIMESTAMP '" TEMPORAL_TIMESTAMP_MIN_YEAR "-01-01 00:00:00'"
#define TEMPORAL_TIMESTAMP_MAX_LIT	"TIMESTAMP '" TEMPORAL_MAX_YEAR "-12-31 23:59:59.999999'"
#define TEMPORAL_TIMESTAMPTZ_MIN_LIT	"TIMESTAMPTZ '" TEMPORAL_TIMESTAMP_MIN_YEAR "-01-01 00:00:00+00'"
#define TEMPORAL_TIMESTAMPTZ_MAX_LIT	"TIMESTAMPTZ '" TEMPORAL_MAX_YEAR "-12-31 23:59:59.999999+00'"

#define TYPE_HAS_TEMPORAL	0x1
#define TYPE_HAS_NUMERIC	0x2

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */
static bool IsTemporalType(Oid typeOid);
static int	TypeValidationFlags(Oid typeOid);
static void AppendTemporalRangeCheck(StringInfo buf, const char *expr,
									 Oid typeOid);
static void AppendTemporalLowerBoundCheck(StringInfo buf, const char *expr,
										  Oid typeOid);
static const char *GetTemporalMinLiteral(Oid typeOid);
static const char *GetTemporalMaxLiteral(Oid typeOid);
static void AppendTemporalOutOfRangeAction(StringInfo buf, const char *expr,
										   Oid typeOid, OutOfRangePolicy policy);
static void AppendValidatedColumnExpr(StringInfo buf, const char *expr,
									  Oid typeOid, int typmod, int depth,
									  OutOfRangePolicy policy);
static void AppendValidatedListExpr(StringInfo buf, const char *expr,
									Oid elemTypeOid, int elemTypmod,
									int depth, OutOfRangePolicy policy);
static const char *GetNumericMaxLiteral(int precision, int scale);
static void AppendNumericNaNAction(StringInfo buf, const char *expr,
								   int precision, int scale,
								   OutOfRangePolicy policy);
static void AppendNumericOutOfRangeAction(StringInfo buf, const char *expr,
										  int precision, int scale,
										  const char *errMsg,
										  OutOfRangePolicy policy);
static void AppendNumericDecimalOverflowAction(StringInfo buf, const char *expr,
											   int precision, int scale,
											   OutOfRangePolicy policy);
static void AppendNumericValidationCaseExpr(StringInfo buf, const char *expr,
											int typmod,
											OutOfRangePolicy policy);


/* ================================================================
 * Public API
 * ================================================================ */

/*
 * WrapQueryWithWriteValidation wraps a DuckDB query with validation
 * expressions for temporal and numeric columns when writing data.
 *
 * All writes (direct INSERT, INSERT..SELECT, COPY FROM) flow through
 * WriteQueryResultTo, which calls this wrapper.  It ensures that:
 *
 * Temporal:
 *  - Out-of-range date/timestamp/timestamptz values are rejected or clamped
 *
 * Numeric:
 *  - NaN/Infinity values are rejected or clamped
 *  - Excess fractional digits are rejected or rounded
 *  - Values exceeding DECIMAL(p,s) range are rejected or clamped
 *  - Numeric values are cast from VARCHAR to DECIMAL(p,s)
 *
 * Both temporal and numeric validation handle types at any nesting
 * depth (scalars, arrays, structs, maps).
 *
 * If no columns need validation, the original query is returned as-is.
 */
char *
WrapQueryWithWriteValidation(char *query, TupleDesc tupleDesc,
							 OutOfRangePolicy policy)
{
	if (tupleDesc == NULL || policy == OUT_OF_RANGE_NONE)
		return query;

	bool		needsWrap = false;
	bool		first = true;

	StringInfoData wrapped = *makeStringInfo();

	appendStringInfoString(&wrapped, "SELECT ");

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!first)
			appendStringInfoString(&wrapped, ", ");
		first = false;

		const char *colName = quote_identifier(NameStr(attr->attname));
		Oid			typeId = attr->atttypid;

		int			flags = TypeValidationFlags(typeId);

		if (flags)
		{
			needsWrap = true;

			AppendValidatedColumnExpr(&wrapped, colName, typeId,
									  attr->atttypmod, 0, policy);
			appendStringInfo(&wrapped, " AS %s", colName);
		}
		else
		{
			appendStringInfoString(&wrapped, colName);
		}
	}

	if (!needsWrap)
		return query;

	appendStringInfo(&wrapped, " FROM (%s) AS __write_validation", query);

	return wrapped.data;
}


/*
 * GetOutOfRangePolicyFromOptions reads the "out_of_range_values" option
 * from a list of DefElem options (table options or COPY options).
 *
 * Returns OUT_OF_RANGE_ERROR if the option is set to "error",
 * OUT_OF_RANGE_CLAMP otherwise (including when not present).
 */
OutOfRangePolicy
GetOutOfRangePolicyFromOptions(List *options)
{
	char	   *value = GetStringOption(options, "out_of_range_values", false);

	if (value != NULL && strcmp(value, "error") == 0)
		return OUT_OF_RANGE_ERROR;

	return OUT_OF_RANGE_CLAMP;
}


/*
 * GetOutOfRangePolicyForTable reads the "out_of_range_values" table option
 * for the given relation.  Returns NONE for non-pg_lake / non-iceberg tables.
 */
OutOfRangePolicy
GetOutOfRangePolicyForTable(Oid relationId)
{
	if (!IsPgLakeForeignTableById(relationId) &&
		!IsPgLakeIcebergForeignTableById(relationId))
		return OUT_OF_RANGE_NONE;

	ForeignTable *foreignTable = GetForeignTable(relationId);

	return GetOutOfRangePolicyFromOptions(foreignTable->options);
}


/* ================================================================
 * Temporal validation helpers
 * ================================================================ */

/*
 * IsTemporalType returns true if the given type OID is a date,
 * timestamp, or timestamptz type.
 */
static bool
IsTemporalType(Oid typeOid)
{
	return typeOid == DATEOID ||
		typeOid == TIMESTAMPOID ||
		typeOid == TIMESTAMPTZOID;
}


/*
 * TypeValidationFlags recursively checks whether a PostgreSQL type
 * contains any temporal (date/timestamp/timestamptz) or numeric fields,
 * including fields nested inside structs, maps, and arrays.
 *
 * Returns a bitmask of TYPE_HAS_TEMPORAL / TYPE_HAS_NUMERIC.
 */
static int
TypeValidationFlags(Oid typeOid)
{
	if (IsTemporalType(typeOid))
		return TYPE_HAS_TEMPORAL;

	if (typeOid == NUMERICOID)
		return TYPE_HAS_NUMERIC;

	/* array: check element type */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
		return TypeValidationFlags(elemType);

	/* map: check key and value types */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valueType = GetMapValueType(typeOid);

		return TypeValidationFlags(keyType.postgresTypeOid) |
			TypeValidationFlags(valueType.postgresTypeOid);
	}

	/* composite/struct: check each field */
	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		int			flags = 0;
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
				continue;

			flags |= TypeValidationFlags(att->atttypid);

			if (flags == (TYPE_HAS_TEMPORAL | TYPE_HAS_NUMERIC))
				break;			/* found both, no need to continue */
		}

		ReleaseTupleDesc(tupdesc);
		return flags;
	}

	return 0;
}


/*
 * AppendTemporalRangeCheck appends a DuckDB boolean expression that
 * checks whether a single scalar temporal expression is out of range
 * or +-infinity.
 *
 * Returns true if the expression is out of range, which callers use
 * to trigger error() or clamping.  Does NOT include a NULL check —
 * callers handle NULL propagation.
 *
 * The isfinite() check comes first so that year() is never evaluated
 * on an infinite value, which would be undefined in DuckDB.
 */
static void
AppendTemporalRangeCheck(StringInfo buf, const char *expr, Oid typeOid)
{
	Assert(IsTemporalType(typeOid));

	if (typeOid == DATEOID)
	{
		appendStringInfo(buf,
						 "(NOT isfinite(%s) OR year(%s) < " TEMPORAL_DATE_MIN_YEAR
						 " OR year(%s) > " TEMPORAL_MAX_YEAR ")",
						 expr, expr, expr);
	}
	else if (typeOid == TIMESTAMPTZOID)
	{
		/*
		 * DuckDB's ICU-based year() for TIMESTAMPTZ returns the era-based
		 * year (always positive, e.g. 1 for 1 BC) rather than the ISO year (0
		 * for 1 BC).  Cast to TIMESTAMP first so the core year() function is
		 * used, which returns the correct astronomical year.
		 */
		appendStringInfo(buf,
						 "(NOT isfinite(%s) OR year(%s::TIMESTAMP) < " TEMPORAL_TIMESTAMP_MIN_YEAR
						 " OR year(%s::TIMESTAMP) > " TEMPORAL_MAX_YEAR ")",
						 expr, expr, expr);
	}
	else
	{
		/* TIMESTAMPOID */
		appendStringInfo(buf,
						 "(NOT isfinite(%s) OR year(%s) < " TEMPORAL_TIMESTAMP_MIN_YEAR
						 " OR year(%s) > " TEMPORAL_MAX_YEAR ")",
						 expr, expr, expr);
	}
}


/*
 * AppendTemporalLowerBoundCheck appends a DuckDB boolean expression that
 * checks whether a scalar temporal expression is below the lower bound
 * of the supported range.
 *
 * Uses a direct comparison against the min literal, which correctly
 * handles +-infinity: -infinity < min is true → clamp to min,
 * +infinity < min is false → clamp to max.
 */
static void
AppendTemporalLowerBoundCheck(StringInfo buf, const char *expr, Oid typeOid)
{
	Assert(IsTemporalType(typeOid));

	appendStringInfo(buf, "%s < %s", expr, GetTemporalMinLiteral(typeOid));
}


/*
 * GetTemporalMinLiteral returns the DuckDB literal for the minimum
 * supported temporal value for the given type.
 */
static const char *
GetTemporalMinLiteral(Oid typeOid)
{
	if (typeOid == DATEOID)
		return TEMPORAL_DATE_MIN_LIT;
	if (typeOid == TIMESTAMPTZOID)
		return TEMPORAL_TIMESTAMPTZ_MIN_LIT;
	return TEMPORAL_TIMESTAMP_MIN_LIT;
}


/*
 * GetTemporalMaxLiteral returns the DuckDB literal for the maximum
 * supported temporal value for the given type.
 */
static const char *
GetTemporalMaxLiteral(Oid typeOid)
{
	if (typeOid == DATEOID)
		return TEMPORAL_DATE_MAX_LIT;
	if (typeOid == TIMESTAMPTZOID)
		return TEMPORAL_TIMESTAMPTZ_MAX_LIT;
	return TEMPORAL_TIMESTAMP_MAX_LIT;
}


/*
 * AppendTemporalOutOfRangeAction appends the THEN clause for an
 * out-of-range or infinite temporal value.
 *
 * In error mode, emits: error('...')
 * In clamp mode, emits: CASE WHEN (lower_check) THEN min ELSE max END
 */
static void
AppendTemporalOutOfRangeAction(StringInfo buf, const char *expr, Oid typeOid,
							   OutOfRangePolicy policy)
{
	if (policy == OUT_OF_RANGE_CLAMP)
	{
		appendStringInfoString(buf, "CASE WHEN ");
		AppendTemporalLowerBoundCheck(buf, expr, typeOid);
		appendStringInfo(buf, " THEN %s ELSE %s END",
						 GetTemporalMinLiteral(typeOid),
						 GetTemporalMaxLiteral(typeOid));
	}
	else
	{
		const char *errMsg = (typeOid == DATEOID) ?
			"date out of range" :
			"timestamp out of range";

		appendStringInfo(buf, "error('%s')", errMsg);
	}
}


/*
 * AppendValidatedListExpr wraps array/list elements with validation
 * using DuckDB's list_transform().
 *
 * The 'depth' parameter is used to generate unique lambda variable
 * names (__v0, __v1, ...) to avoid collisions in nested lambdas.
 */
static void
AppendValidatedListExpr(StringInfo buf, const char *expr,
						Oid elemTypeOid, int elemTypmod, int depth,
						OutOfRangePolicy policy)
{
	char	   *lambdaVar = psprintf("__v%d", depth);
	StringInfoData lambdaBody;

	initStringInfo(&lambdaBody);
	AppendValidatedColumnExpr(&lambdaBody, lambdaVar, elemTypeOid,
							  elemTypmod, depth + 1, policy);

	appendStringInfo(buf, "list_transform(%s, %s -> %s)",
					 expr, lambdaVar, lambdaBody.data);

	pfree(lambdaBody.data);
	pfree(lambdaVar);
}


/*
 * AppendValidatedColumnExpr recursively appends a DuckDB expression
 * that validates all temporal and numeric fields within a value of the
 * given type.
 *
 * Handles:
 *   - Scalar temporal types (date, timestamp, timestamptz)
 *   - Scalar numeric types
 *   - Arrays of any type containing temporal or numeric fields
 *   - Structs with temporal/numeric fields at any nesting depth
 *   - Maps whose keys/values contain temporal or numeric fields
 *
 * In error mode, raises an error (via DuckDB's error() function) for
 * out-of-range values.  In clamp mode, clamps them to the nearest
 * valid boundary value.
 */
static void
AppendValidatedColumnExpr(StringInfo buf, const char *expr,
						  Oid typeOid, int typmod, int depth,
						  OutOfRangePolicy policy)
{
	/* scalar temporal → range CASE */
	if (IsTemporalType(typeOid))
	{
		appendStringInfo(buf, "CASE WHEN %s IS NOT NULL AND ", expr);
		AppendTemporalRangeCheck(buf, expr, typeOid);
		appendStringInfoString(buf, " THEN ");
		AppendTemporalOutOfRangeAction(buf, expr, typeOid, policy);
		appendStringInfo(buf, " ELSE %s END", expr);
		return;
	}

	/* scalar numeric → NaN/Inf/range CASE */
	if (typeOid == NUMERICOID)
	{
		int			p,
					s;

		GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(typmod, &p, &s);

		if (!CanPushdownNumericToDuckdb(p, s))
		{
			/*
			 * Precision exceeds DuckDB's 38-digit DECIMAL limit, so we cannot
			 * CAST to DECIMAL(p,s).  Pass through as VARCHAR — the column
			 * is already mapped to VARCHAR in the write path.
			 */
			appendStringInfoString(buf, expr);
			return;
		}

		AppendNumericValidationCaseExpr(buf, expr, typmod, policy);
		return;
	}

	/* array: validate each element */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType) && TypeValidationFlags(elemType))
	{
		/*
		 * For arrays, the column typmod applies to each element (e.g.
		 * numeric(10,2)[] stores the (10,2) typmod on the column).
		 */
		AppendValidatedListExpr(buf, expr, elemType, typmod, depth, policy);
		return;
	}

	/* map: validate keys and/or values via map_entries + list_transform */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);
		bool		keyNeeds = TypeValidationFlags(keyType.postgresTypeOid) != 0;
		bool		valNeeds = TypeValidationFlags(valType.postgresTypeOid) != 0;

		if (keyNeeds || valNeeds)
		{
			char	   *entryVar = psprintf("__v%d", depth);
			StringInfoData keyBody;
			StringInfoData valBody;

			initStringInfo(&keyBody);
			initStringInfo(&valBody);

			if (keyNeeds)
				AppendValidatedColumnExpr(&keyBody,
										  psprintf("%s.key", entryVar),
										  keyType.postgresTypeOid,
										  keyType.postgresTypeMod,
										  depth + 1, policy);
			else
				appendStringInfo(&keyBody, "%s.key", entryVar);

			if (valNeeds)
				AppendValidatedColumnExpr(&valBody,
										  psprintf("%s.value", entryVar),
										  valType.postgresTypeOid,
										  valType.postgresTypeMod,
										  depth + 1, policy);
			else
				appendStringInfo(&valBody, "%s.value", entryVar);

			appendStringInfo(buf,
							 "map_from_entries(list_transform(map_entries(%s), "
							 "%s -> struct_pack(key := %s, value := %s)))",
							 expr, entryVar, keyBody.data, valBody.data);

			pfree(keyBody.data);
			pfree(valBody.data);
			pfree(entryVar);
			return;
		}
	}

	/* composite/struct: rebuild with validated fields */
	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		hasFieldNeedingValidation = false;

		/* first check if any field needs validation */
		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
				continue;

			if (TypeValidationFlags(att->atttypid))
			{
				hasFieldNeedingValidation = true;
				break;
			}
		}

		if (hasFieldNeedingValidation)
		{
			/*
			 * Rebuild the struct with struct_pack, validating temporal and
			 * numeric fields.
			 */
			appendStringInfoString(buf, "struct_pack(");

			bool		first = true;

			for (int i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupdesc, i);

				if (att->attisdropped)
					continue;

				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				const char *fieldName = NameStr(att->attname);
				char	   *fieldExpr = psprintf("%s.%s", expr,
												 quote_identifier(fieldName));

				appendStringInfo(buf, "%s := ", quote_identifier(fieldName));

				if (TypeValidationFlags(att->atttypid))
					AppendValidatedColumnExpr(buf, fieldExpr, att->atttypid,
											  att->atttypmod, depth, policy);
				else
					appendStringInfoString(buf, fieldExpr);

				pfree(fieldExpr);
			}

			appendStringInfoChar(buf, ')');

			ReleaseTupleDesc(tupdesc);
			return;
		}

		ReleaseTupleDesc(tupdesc);
	}

	/* no validation needed, pass through */
	appendStringInfoString(buf, expr);
}


/* ================================================================
 * Numeric validation helpers
 * ================================================================ */

/*
 * GetNumericMaxLiteral returns the maximum positive DECIMAL(p,s) literal.
 *
 * For example, DECIMAL(38,9) → "99999999999999999999999999999.999999999"
 */
static const char *
GetNumericMaxLiteral(int precision, int scale)
{
	StringInfoData result;
	int			integralDigits = precision - scale;

	initStringInfo(&result);

	for (int i = 0; i < integralDigits; i++)
		appendStringInfoChar(&result, '9');

	if (scale > 0)
	{
		appendStringInfoChar(&result, '.');
		for (int i = 0; i < scale; i++)
			appendStringInfoChar(&result, '9');
	}

	return result.data;
}


/*
 * AppendNumericNaNAction appends the THEN clause for a NaN value.
 *
 * In error mode (default), emits: error('...')
 * In clamp mode, emits: NULL::DECIMAL(p,s)
 *
 * NaN has no numeric equivalent, so we clamp to NULL.
 */
static void
AppendNumericNaNAction(StringInfo buf, const char *expr, int precision,
					   int scale, OutOfRangePolicy policy)
{
	if (policy == OUT_OF_RANGE_CLAMP)
	{
		appendStringInfo(buf, "NULL::DECIMAL(%d,%d)", precision, scale);
	}
	else
	{
		appendStringInfo(buf,
						 "error(CONCAT('NaN is not allowed for numeric type: ', "
						 "CAST(%s AS VARCHAR)))",
						 expr);
	}
}


/*
 * AppendNumericOutOfRangeAction appends the THEN clause for an
 * out-of-range numeric value (Infinity or digit overflow).
 *
 * In error mode (default), emits: error('...')
 * In clamp mode, emits a sign-aware expression that returns the
 * maximum or minimum DECIMAL(p,s) literal (negative max for negative
 * values, positive max for positive values).
 */
static void
AppendNumericOutOfRangeAction(StringInfo buf, const char *expr,
							  int precision, int scale,
							  const char *errMsg,
							  OutOfRangePolicy policy)
{
	if (policy == OUT_OF_RANGE_CLAMP)
	{
		const char *maxLit = GetNumericMaxLiteral(precision, scale);

		appendStringInfo(buf,
						 "CASE WHEN STARTS_WITH(LTRIM(CAST(%s AS VARCHAR)), '-') "
						 "THEN CAST('-%s' AS DECIMAL(%d,%d)) "
						 "ELSE CAST('%s' AS DECIMAL(%d,%d)) END",
						 expr, maxLit, precision, scale,
						 maxLit, precision, scale);
	}
	else
	{
		appendStringInfo(buf,
						 "error(CONCAT('%s: ', CAST(%s AS VARCHAR)))",
						 errMsg, expr);
	}
}


/*
 * AppendNumericDecimalOverflowAction appends the THEN clause for a
 * numeric value whose fractional digits exceed the target scale.
 *
 * In error mode, emits: error('...')
 * In clamp mode, emits: CAST to DECIMAL(p,s) which naturally rounds
 * the excess fractional digits (e.g. 1.123456789012 → 1.123456789
 * for scale 9).
 */
static void
AppendNumericDecimalOverflowAction(StringInfo buf, const char *expr,
								   int precision, int scale,
								   OutOfRangePolicy policy)
{
	if (policy == OUT_OF_RANGE_CLAMP)
	{
		appendStringInfo(buf, "CAST(%s AS DECIMAL(%d,%d))",
						 expr, precision, scale);
	}
	else
	{
		appendStringInfo(buf,
						 "error(CONCAT('numeric value exceeds max "
						 "allowed digits %d after decimal point: ', "
						 "CAST(%s AS VARCHAR)))",
						 scale, expr);
	}
}


/*
 * AppendNumericValidationCaseExpr appends a DuckDB CASE expression
 * that validates a single numeric value for DECIMAL compatibility.
 *
 * The generated expression can be used standalone or inside a
 * list_transform lambda for array elements.
 *
 * Checks:
 *  - NaN → error or NULL (NaN has no numeric equivalent)
 *  - Infinity → error or clamp to min/max DECIMAL
 *  - Excess fractional digits → error or round
 *  - Out-of-range values → error or clamp to min/max DECIMAL
 *  - Otherwise: CAST to DECIMAL(precision, scale)
 *
 * In error mode (default), invalid values raise error().
 * In clamp mode, NaN becomes NULL and out-of-range values are clamped
 * to the min/max DECIMAL(p,s) based on sign.
 *
 * Does NOT append an alias — callers add "AS alias" as needed.
 */
static void
AppendNumericValidationCaseExpr(StringInfo buf, const char *expr, int typmod,
								OutOfRangePolicy policy)
{
	int			precision;
	int			scale;

	GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(typmod, &precision, &scale);

	/* check for NaN */
	appendStringInfo(buf,
					 "CASE WHEN LOWER(TRIM(CAST(%s AS VARCHAR))) = 'nan' THEN ",
					 expr);
	AppendNumericNaNAction(buf, expr, precision, scale, policy);

	/* check for Infinity */
	appendStringInfo(buf,
					 " WHEN LOWER(TRIM(CAST(%s AS VARCHAR))) "
					 "IN ('infinity', '+infinity', '-infinity', "
					 "'inf', '+inf', '-inf') THEN ",
					 expr);
	AppendNumericOutOfRangeAction(buf, expr, precision, scale,
								  "Infinity values are not allowed for "
								  "numeric type", policy);

	/* check for values that exceed max allowed digits after decimal point */
	appendStringInfo(buf,
					 " WHEN %s IS NOT NULL AND "
					 "LENGTH(RTRIM(REGEXP_REPLACE(SPLIT_PART("
					 "LTRIM(CAST(%s AS VARCHAR), '+-'), '.', 2), "
					 "'[^0-9]', '', 'g'), '0')) > %d THEN ",
					 expr, expr, scale);
	AppendNumericDecimalOverflowAction(buf, expr, precision, scale, policy);

	/* check for values that exceed max allowed digits before decimal point */
	int			maxPrec = DUCKDB_MAX_NUMERIC_PRECISION;
	const char *maxLit = GetNumericMaxLiteral(precision, scale);

	appendStringInfo(buf,
					 " WHEN %s IS NOT NULL AND ("
					 "TRY_CAST(%s AS DECIMAL(%d,%d)) IS NULL OR "
					 "ABS(TRY_CAST(%s AS DECIMAL(%d,%d))) > "
					 "CAST('%s' AS DECIMAL(%d,%d))) THEN ",
					 expr,
					 expr, maxPrec, scale,
					 expr, maxPrec, scale,
					 maxLit, maxPrec, scale);
	AppendNumericOutOfRangeAction(buf, expr, precision, scale,
								  psprintf("numeric value exceeds max "
										   "allowed digits %d before "
										   "decimal point",
										   precision - scale), policy);

	/* final CAST to DECIMAL(p,s) */
	appendStringInfo(buf,
					 " ELSE CAST(%s AS DECIMAL(%d,%d)) END",
					 expr, precision, scale);
}

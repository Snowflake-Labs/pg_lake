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

#include "varatt.h"

#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/numeric.h"
#include "catalog/pg_type.h"

/* pg_lake_table.unbounded_numeric_default_precision setting */
int			UnboundedNumericDefaultPrecision = UNBOUNDED_NUMERIC_DEFAULT_PRECISION;

/* pg_lake_table.unbounded_numeric_default_scale setting */
int			UnboundedNumericDefaultScale = UNBOUNDED_NUMERIC_DEFAULT_SCALE;

/* pg_lake_iceberg.unsupported_numeric_as_double setting */
bool		UnsupportedNumericAsDouble = true;


/*
 * IsUnboundedNumeric checks if the given type is an unbounded numeric type.
 */
bool
IsUnboundedNumeric(int typOid, int typMod)
{
	return typOid == NUMERICOID && typMod == -1;
}

/*
 * GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod extracts the precision and scale
 * from a numeric type modifier and adjusts them to be valid for duckdb.
 * Adjust 3 cases where duckdb does not support:
 * 1. Unbounded numeric: PostgreSQL supports up to 1000 digits in numeric fields, while
 *   DuckDB supports up to 38.
 * 2. Negative scale: Adjust the precision by adding the absolute value of the scale
 *   to it and set the scale to 0. This is valid since e.g. numeric(5, -2) can store
 *  val < 10^7.
 * 3. Scale > precision: Adjust the precision to be the same as the scale. This is valid
 *  since e.g. numeric(2, 4) can store -0.0099 < val < 0.0099.
 *             numeric(4,4) can store -0.9999 < val < 0.9999, which covers the range of numeric(2,4).
 */
void
GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(int typeMod, int *precision, int *scale)
{
	if (IsUnboundedNumeric(NUMERICOID, typeMod))
	{
		*precision = UnboundedNumericDefaultPrecision;
		*scale = UnboundedNumericDefaultScale;
		return;
	}

	*precision = numeric_typmod_precision(typeMod);
	*scale = numeric_typmod_scale(typeMod);

	if (*scale < 0)
	{
		/* negative scale */
		*precision += abs(*scale);
		*scale = 0;
	}
	/* do not adjust precision if the scale is above the max limit */
	else if (*scale > *precision && *scale <= DUCKDB_MAX_NUMERIC_SCALE)
	{
		*precision = *scale;
	}
}


/*
 * IsUnsupportedNumericForIceberg returns true when typeOid is NUMERICOID and
 * the typmod represents a numeric that cannot be stored as an Iceberg decimal
 * (unbounded, precision > 38, or scale > 38).  Returns false for non-numeric
 * types so the caller does not need a separate type check.
 */
bool
IsUnsupportedNumericForIceberg(Oid typeOid, int typmod)
{
	if (typeOid != NUMERICOID)
		return false;

	if (IsUnboundedNumeric(typeOid, typmod))
		return true;

	int			precision = -1;
	int			scale = -1;

	GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(typmod, &precision, &scale);

	return precision > DUCKDB_MAX_NUMERIC_PRECISION ||
		scale > DUCKDB_MAX_NUMERIC_SCALE;
}

/*
 * A numeric's weight (the base-10000 exponent of its most significant digit)
 * is stored directly in the value's on-disk header, so the number of integral
 * digits can be derived arithmetically instead of formatting the whole value
 * to text with numeric_out.
 *
 * The public numeric.h keeps struct NumericData opaque, so we mirror just the
 * fields we read.  This layout, and the weight/digit macros below, have been
 * stable in PostgreSQL since the base-10000 representation was introduced in
 * 8.3.  DatumGetNumeric() always returns a fully detoasted value with a 4-byte
 * varlena header, so overlaying this struct is safe.  Keep in sync with
 * src/backend/utils/adt/numeric.c.
 */
typedef int16 PgLakeNumericDigit;

#define PG_LAKE_NUMERIC_DEC_DIGITS 4	/* decimal digits per base-10000 digit */

typedef struct PgLakeNumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	PgLakeNumericDigit n_data[FLEXIBLE_ARRAY_MEMBER];
}			PgLakeNumericLong;

typedef struct PgLakeNumericShort
{
	uint16		n_header;
	PgLakeNumericDigit n_data[FLEXIBLE_ARRAY_MEMBER];
}			PgLakeNumericShort;

typedef union PgLakeNumericChoice
{
	uint16		n_header;
	PgLakeNumericLong n_long;
	PgLakeNumericShort n_short;
}			PgLakeNumericChoice;

typedef struct PgLakeNumericData
{
	int32		vl_len_;
	PgLakeNumericChoice choice;
}			PgLakeNumericData;

#define PG_LAKE_NUMERIC_HEADER_IS_SHORT(n) (((n)->choice.n_header & 0x8000) != 0)
#define PG_LAKE_NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (PG_LAKE_NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))
#define PG_LAKE_NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define PG_LAKE_NUMERIC_SHORT_WEIGHT_MASK 0x003F
#define PG_LAKE_NUMERIC_WEIGHT(n) (PG_LAKE_NUMERIC_HEADER_IS_SHORT(n) ? \
	(((n)->choice.n_short.n_header & PG_LAKE_NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
	  ~PG_LAKE_NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & PG_LAKE_NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))
#define PG_LAKE_NUMERIC_DIGITS(n) (PG_LAKE_NUMERIC_HEADER_IS_SHORT(n) ? \
	(n)->choice.n_short.n_data : (n)->choice.n_long.n_data)
#define PG_LAKE_NUMERIC_NDIGITS(n) \
	((VARSIZE(n) - PG_LAKE_NUMERIC_HEADER_SIZE(n)) / sizeof(PgLakeNumericDigit))

/*
 * NumericIntegralDigitCount returns the number of digits numeric_out would
 * emit before the decimal point of `num`, derived from the value's weight
 * rather than by formatting it.  `num` must be a finite value already
 * detoasted via DatumGetNumeric().
 *
 * numeric_out always emits at least one integral digit (a leading "0" for zero
 * and for |value| < 1), which this mirrors so callers match textual output.
 */
int
NumericIntegralDigitCount(Numeric num)
{
	PgLakeNumericData *n = (PgLakeNumericData *) num;
	int			ndigits = PG_LAKE_NUMERIC_NDIGITS(n);
	int			weight;
	PgLakeNumericDigit firstDigit;
	int			firstDigitLen;

	/* zero is stored with no digits and rendered as a single "0" */
	if (ndigits <= 0)
		return 1;

	weight = PG_LAKE_NUMERIC_WEIGHT(n);

	/* |value| < 1 still renders a single leading "0" before the point */
	if (weight < 0)
		return 1;

	/*
	 * Leading zero base-10000 digits are stripped in storage, so the first
	 * digit is in [1, 9999]; its decimal length plus the four decimal digits
	 * contributed by each remaining full weight step gives the integral
	 * count.
	 */
	firstDigit = PG_LAKE_NUMERIC_DIGITS(n)[0];

	if (firstDigit >= 1000)
		firstDigitLen = 4;
	else if (firstDigit >= 100)
		firstDigitLen = 3;
	else if (firstDigit >= 10)
		firstDigitLen = 2;
	else
		firstDigitLen = 1;

	return weight * PG_LAKE_NUMERIC_DEC_DIGITS + firstDigitLen;
}

/*
 * CanPushdownNumericToDuckdb checks if the precision and scale of a numeric type
 * is within the limits of duckdb.
 */
bool
CanPushdownNumericToDuckdb(int precision, int scale)
{
	if (scale < 0)
	{
		return false;
	}

	if (scale > precision)
	{
		return false;
	}

	if (precision > DUCKDB_MAX_NUMERIC_PRECISION)
	{
		return false;
	}

	if (scale > DUCKDB_MAX_NUMERIC_SCALE)
	{
		return false;
	}

	return true;
}

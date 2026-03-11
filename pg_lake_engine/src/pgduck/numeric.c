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

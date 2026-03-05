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

#pragma once

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

/*
 * Behavior for out-of-range temporal values during writes to Iceberg
 * data files.  Applies to date, timestamp, and timestamptz columns.
 *
 * Controlled by the out_of_range_values table option or COPY option.
 *
 * NONE skips validation entirely (used for non-Iceberg tables).
 */
typedef enum IcebergOutOfRangePolicy
{
	ICEBERG_OOR_NONE = 0,
	ICEBERG_OOR_ERROR = 1,
	ICEBERG_OOR_CLAMP = 2,
}			IcebergOutOfRangePolicy;

/*
 * GetIcebergOutOfRangePolicyFromOptions returns the IcebergOutOfRangePolicy
 * from the given option list.  Returns ICEBERG_OOR_CLAMP if
 * the option is not present.
 */
extern PGDLLEXPORT IcebergOutOfRangePolicy GetIcebergOutOfRangePolicyFromOptions(List *options);

/*
 * GetIcebergOutOfRangePolicyForTable returns the IcebergOutOfRangePolicy
 * from the given table's foreign table options.  Returns NONE for
 * non-iceberg tables.
 */
extern PGDLLEXPORT IcebergOutOfRangePolicy GetIcebergOutOfRangePolicyForTable(Oid relationId);

/*
 * IcebergErrorOrClampTemporalDatum validates a date, timestamp, or timestamptz
 * Datum for out-of-range or infinity values.  Returns the original
 * value if it is in range, or a clamped/error value otherwise.
 *
 * Used by partition-transform code to validate temporal values on the
 * PostgreSQL side (e.g. for partition key computation).
 */
extern PGDLLEXPORT Datum IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid,
														  IcebergOutOfRangePolicy policy);

/*
 * IcebergErrorOrClampNumericDatum checks a numeric Datum for NaN.
 * Returns true when NaN is detected and the policy is CLAMP (caller
 * should write NULL); raises an error for ERROR policy.  Returns
 * false when the value is not NaN.
 */
extern PGDLLEXPORT Datum IcebergErrorOrClampNumericDatum(Datum value,
														 IcebergOutOfRangePolicy policy,
														 bool *isNull);

/*
 * IcebergWrapQueryWithErrorOrClampChecks wraps a query with CASE WHEN
 * checks for temporal columns that need Iceberg write-time validation
 * (date/timestamp/timestamptz).
 *
 * For ICEBERG_OOR_CLAMP: out-of-range values are clamped to boundaries.
 * For ICEBERG_OOR_ERROR: out-of-range values trigger a cast error.
 *
 * Returns the original query unchanged if no temporal columns exist or
 * the policy is ICEBERG_OOR_NONE.
 */
extern PGDLLEXPORT char *IcebergWrapQueryWithErrorOrClampChecks(char *query,
																TupleDesc tupleDesc,
																IcebergOutOfRangePolicy policy,
																bool queryHasRowId);

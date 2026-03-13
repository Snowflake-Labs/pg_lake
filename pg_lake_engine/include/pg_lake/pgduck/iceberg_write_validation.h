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

#pragma once

#include "access/tupdesc.h"
#include "utils/date.h"
#include "utils/timestamp.h"

/*
 * Behavior for out-of-range values during writes to Iceberg data files.
 *
 * Applies to:
 *   - Temporal columns (date, timestamp, timestamptz): values beyond
 *     the Iceberg-supported range are clamped or rejected.
 *   - Bounded numeric columns: NaN values are clamped to NULL or rejected.
 *
 * Controlled by the out_of_range_values table option.
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
 * GetIcebergOutOfRangePolicyForTable returns the IcebergOutOfRangePolicy
 * from the given table's foreign table options.  Returns NONE for
 * non-iceberg tables.
 */
extern PGDLLEXPORT IcebergOutOfRangePolicy GetIcebergOutOfRangePolicyForTable(Oid relationId);

/*
 * IcebergErrorOrClampDatum validates a Datum for Iceberg write constraints.
 *
 * Dispatches to temporal validation (date/timestamp/timestamptz) or
 * numeric NaN validation based on typeOid.  For types that need no
 * validation the value is returned unchanged.
 *
 * *isNull is set to true only when a numeric NaN is clamped (the
 * caller should write NULL instead of the original value).
 */
extern PGDLLEXPORT Datum IcebergErrorOrClampDatum(Datum value, Oid typeOid,
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

extern PGDLLEXPORT bool IsTemporalType(Oid typeOid);
extern PGDLLEXPORT int GetYearFromDate(DateADT d);
extern PGDLLEXPORT int GetYearFromTimestamp(Timestamp ts);

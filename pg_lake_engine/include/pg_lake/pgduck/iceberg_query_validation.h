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
#include "pg_lake/pgduck/iceberg_validation.h"

/*
 * IcebergWrapQueryWithErrorOrClampChecks wraps a query with validation
 * expressions for columns that need Iceberg write-time checks: temporal
 * boundary enforcement (date/timestamp/timestamptz) and multidimensional
 * array enforcement (pg_nullify_nested_list or pg_error_nested_list).
 *
 * For ICEBERG_OOR_CLAMP: out-of-range values are clamped to boundaries.
 * For ICEBERG_OOR_ERROR: out-of-range values trigger a cast error.
 *
 * Returns the original query unchanged if no columns need validation
 * or the policy is ICEBERG_OOR_NONE.
 */
extern PGDLLEXPORT char *IcebergWrapQueryWithErrorOrClampChecks(char *query,
																TupleDesc tupleDesc,
																IcebergOutOfRangePolicy policy,
																bool queryHasRowId);

/*
 * IcebergWrapQueryWithSizeClampChecks wraps a query so that values
 * exceeding Snowflake's per-column byte caps are either clamped or
 * rejected, per `policy`:
 *
 *   - ICEBERG_OOR_ERROR (default): raise error identifying the column.
 *   - ICEBERG_OOR_CLAMP: text/varchar/bpchar truncated at a UTF-8
 *     character boundary; bytea byte-truncated; jsonb/json NULLed when
 *     the serialized form exceeds the string limit; arrays/structs/maps
 *     NULLed when their measured byte size exceeds the nested-type cap.
 *   - ICEBERG_OOR_NONE: no-op.
 *
 * Callers gate on compatibility_mode='snowflake' before reaching here;
 * the wrapper itself is unconditional otherwise (returning the original
 * query only when no column carries a clampable type or when all three
 * effective limits are zero — a test-only configuration).
 */
extern PGDLLEXPORT char *IcebergWrapQueryWithSizeClampChecks(char *query,
															 TupleDesc tupleDesc,
															 IcebergOutOfRangePolicy policy,
															 bool queryHasRowId);

/*
 * IcebergWrapQueryWithNativeTypeConversion wraps a query to rewrite
 * columns whose native DuckDB shape does not match Iceberg's.  See the
 * implementation for the set of rewrites and why each is required.
 */
extern PGDLLEXPORT char *IcebergWrapQueryWithNativeTypeConversion(char *query,
																  TupleDesc tupleDesc,
																  bool queryHasRowId);

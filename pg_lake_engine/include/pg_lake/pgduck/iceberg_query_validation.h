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
#include "pg_lake/parquet/field.h"
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
 * IcebergRewriteKind is a bitmask of independent per-leaf rewrites the write
 * query may need before the data matches Iceberg's on-disk shape.  They are
 * applied in a single traversal (see IcebergWrapQueryWithRewrites); combine
 * them with bitwise OR.  Each leaf needs at most one kind (the sets are
 * disjoint), and a future rewrite is just another flag.
 */
typedef enum
{
	/*
	 * Encode INTERVAL/TIMETZ into their Iceberg on-disk shape (interval ->
	 * struct(months,days,microseconds), timetz -> UTC-normalized time).
	 * Type-intrinsic: depends only on the DuckDB type, never on a per-table
	 * storage mapping.  storageSchema unused.
	 */
	ICEBERG_REWRITE_NATIVE_ENCODE = 1 << 0,

	/*
	 * Surface -> persisted storage type wherever the two diverge (e.g. a
	 * nested uuid stored as iceberg string under compatibility_mode).  Driven
	 * entirely by storageSchema; type-agnostic.
	 */
	ICEBERG_REWRITE_STORAGE_CAST = 1 << 1,
}			IcebergRewriteKind;

/*
 * IcebergWrapQueryWithRewrites wraps a query with an outer SELECT that applies
 * every rewrite in rewriteKinds to each column in a single pass, aliasing
 * rewritten columns back to their original names and passing untouched columns
 * through.  storageSchema is the target table's persisted Iceberg storage
 * schema (or NULL); it is only consulted when ICEBERG_REWRITE_STORAGE_CAST is
 * set.
 *
 * Returns the original query unchanged if no column needs any of the requested
 * rewrites.
 */
extern PGDLLEXPORT char *IcebergWrapQueryWithRewrites(char *query,
													  TupleDesc tupleDesc,
													  bool queryHasRowId,
													  int rewriteKinds,
													  DataFileSchema * storageSchema);

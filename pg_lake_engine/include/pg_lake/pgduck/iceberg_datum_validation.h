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

#include "pg_lake/pgduck/iceberg_validation.h"

/*
 * IcebergErrorOrClampDatum validates a Datum for Iceberg write constraints.
 *
 * Recursively handles scalar types (date/timestamp/timestamptz/numeric),
 * multidimensional arrays (clamped to NULL), and nested containers
 * (composites, maps, domains).  For types that need no validation the
 * value is returned unchanged.
 *
 * typmod is used to distinguish bounded numerics (Iceberg decimal) from
 * unbounded ones (mapped to float8).  Only bounded numerics have NaN
 * clamped; unbounded numerics pass through unchanged.
 *
 * *isNull is set to true when a top-level unsupported value is clamped:
 * numeric NaN or a multidimensional array.  The caller should write NULL
 * instead of the original value.  NaN values and multidimensional arrays
 * nested inside containers are absorbed as NULL within the reconstructed
 * container.
 */
extern PGDLLEXPORT Datum IcebergErrorOrClampDatum(Datum value, Oid typeOid,
												  int32 typmod,
												  IcebergOutOfRangePolicy policy,
												  bool *isNull);

/*
 * IcebergSizeClampDatum truncates, NULLs, or errors on a Datum so that
 * string, binary, and nested-type values fit the byte limits expressed by
 * pg_lake_engine.iceberg_max_string_bytes,
 * pg_lake_engine.iceberg_max_binary_bytes, and
 * pg_lake_engine.iceberg_max_nested_type_bytes (0 = no limit).
 *
 * The behavior on an oversize value is selected by `policy`:
 *   - ICEBERG_OOR_ERROR (default for Iceberg tables): raise an error
 *     identifying the column and exceeded GUC.
 *   - ICEBERG_OOR_CLAMP: silently fix up the value as below.
 *   - ICEBERG_OOR_NONE: pass through unchanged.
 *
 * Under CLAMP, lossless types are truncated:
 *   - text/varchar/bpchar  -> trimmed at a UTF-8 character boundary to
 *                             iceberg_max_string_bytes.
 *   - bytea                -> byte-truncated to iceberg_max_binary_bytes.
 *
 * Structured-string types are replaced with NULL via *isNull = true,
 * since truncation would corrupt them:
 *   - jsonb/json
 *
 * Container types (array / composite / map / domain over either) are
 * NULLed when their measured size exceeds iceberg_max_nested_type_bytes.
 * The size is the type's output-function text length when the container
 * has any jsonb/json leaf (so jsonb leaves contribute their JSON-text
 * size, which is what the consumer ultimately sees), and the cheaper
 * varlena content size otherwise.
 *
 * `columnName` is included in the error message under ERROR mode; pass
 * NULL or empty when the column context is unknown.
 *
 * If all three GUCs are 0, the value is returned unchanged regardless of
 * policy or type.
 */
extern PGDLLEXPORT Datum IcebergSizeClampDatum(Datum value, Oid typeOid,
											   int32 typmod,
											   IcebergOutOfRangePolicy policy,
											   const char *columnName,
											   bool *isNull);

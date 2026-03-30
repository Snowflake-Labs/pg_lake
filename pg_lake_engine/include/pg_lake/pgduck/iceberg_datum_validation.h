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
 * *isNull is set to true when a top-level unsupported value is clamped:
 * numeric NaN or a multidimensional array.  The caller should write NULL
 * instead of the original value.  NaN values and multidimensional arrays
 * nested inside containers are absorbed as NULL within the reconstructed
 * container.
 */
extern PGDLLEXPORT Datum IcebergErrorOrClampDatum(Datum value, Oid typeOid,
												  IcebergOutOfRangePolicy policy,
												  bool *isNull);

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
#include "postgres.h"

/*
 * Maximum precision and scale for numeric types in DuckDB.
 */
#define DUCKDB_MAX_NUMERIC_PRECISION 38
#define DUCKDB_MAX_NUMERIC_SCALE 38

/*
 * Default numeric precision and scale for unbounded numeric columns
 * in pg_lake tables and in COPY TO output.
 *
 * precision=18 digits can be fit into 64 bits and scale=3 digits
 * lets us represent 15 digits before the decimal point and 3 digits after.
 * Increasing the scale makes the precision overflow easier since multiplication
 * of two numbers increase the scale by leaving less room for the precision.
 *
 * There corresponding GUCs to change these defaults.
 */
#define UNBOUNDED_NUMERIC_DEFAULT_PRECISION 38
#define UNBOUNDED_NUMERIC_DEFAULT_SCALE 9

extern int	PGDLLEXPORT UnboundedNumericDefaultPrecision;
extern int	PGDLLEXPORT UnboundedNumericDefaultScale;

extern bool PGDLLEXPORT UnsupportedNumericAsDouble;

extern PGDLLEXPORT bool IsUnboundedNumeric(int typOid, int typMod);
extern PGDLLEXPORT void GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(int typeMod,
																			 int *precision,
																			 int *scale);
extern PGDLLEXPORT bool CanPushdownNumericToDuckdb(int precision, int scale);

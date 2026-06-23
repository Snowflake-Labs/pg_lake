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

#include "nodes/pg_list.h"

/*
 * Name of the per-table iceberg option that selects a compatibility mode.
 * Defined here so option validation (pg_lake_table) and the conversion logic
 * share a single source of truth.
 */
#define ICEBERG_COMPATIBILITY_MODE_OPTION "compatibility_mode"

/*
 * IcebergCompatibilityMode selects how column types are rewritten so the
 * resulting Iceberg table is consumable by a particular downstream engine.
 *
 *   ICEBERG_COMPAT_AUTO       default (option unset or 'auto'): let pg_lake
 *                             decide what the table's downstream needs.  Today
 *                             that resolves to no rewrites, but this is the
 *                             hook where future auto-detection (e.g. of a
 *                             Snowflake-managed catalog) would live.
 *   ICEBERG_COMPAT_SNOWFLAKE  rewrite types Snowflake cannot represent inside
 *                             structured types (nested uuid -> text)
 */
typedef enum IcebergCompatibilityMode
{
	ICEBERG_COMPAT_AUTO = 0,
	ICEBERG_COMPAT_SNOWFLAKE,
}			IcebergCompatibilityMode;

extern PGDLLEXPORT IcebergCompatibilityMode ParseIcebergCompatibilityMode(const char *optionValue);
extern PGDLLEXPORT IcebergCompatibilityMode IcebergCompatibilityModeFromCreateOptions(List *options);
extern PGDLLEXPORT IcebergCompatibilityMode IcebergCompatibilityModeFromRelation(Oid relationId);
extern PGDLLEXPORT void ApplyIcebergTableTypeConversions(List *columnDefList,
														 IcebergCompatibilityMode mode);

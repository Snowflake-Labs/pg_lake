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

#include "pg_extension_base/extension_ids.h"

#define PG_LAKE_ICEBERG "pg_lake_iceberg"
#define PG_LAKE_ICEBERG_SCHEMA "lake_iceberg"
#define ICEBERG_INTERNAL_CATALOG_TABLE_NAME "tables_internal"

/* cached extension IDs for pg_lake_iceberg */
extern PGDLLEXPORT CachedExtensionIds * PgLakeIceberg;

extern PGDLLEXPORT void InitializePgLakeIcebergIdCache(void);
extern PGDLLEXPORT Oid IcebergTablesInternalTableId(void);

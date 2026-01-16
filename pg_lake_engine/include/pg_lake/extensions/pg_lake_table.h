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

#define PG_LAKE_TABLE "pg_lake_table"
#define PG_LAKE_TABLE_SCHEMA "lake_table"
#define PG_LAKE_TABLE_FILES_TABLE_NAME "files"
#define PG_LAKE_TABLE_DATA_FILE_COLUMN_STATS_TABLE_NAME "data_file_column_stats"
#define PG_LAKE_TABLE_PARTITION_SPECS "partition_specs"
#define PG_LAKE_TABLE_PARTITION_FIELDS "partition_fields"
#define PG_LAKE_TABLE_DATA_FILE_PARTITION_VALUES_TABLE_NAME "data_file_partition_values"
#define REPLICATION_WRITES_SCHEMA "__pg_lake_table_writes"

extern PGDLLEXPORT CachedExtensionIds * PgLakeTable;

extern PGDLLEXPORT void InitializePgLakeTableIdCache(void);

extern PGDLLEXPORT Oid PgLakeTableSizeFunctionId(void);
extern PGDLLEXPORT Oid IsObjectCreatedByPgLakeFuncId(void);
extern PGDLLEXPORT Oid PgLakeWritesSchemaId(void);

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

/*
 * Module-internal contract between data_files_catalog.c (the orchestrator
 * in ApplyDataFileCatalogChanges) and data_files_catalog_batch.c (the bulk
 * write path). Not part of the public extension API; do not include from
 * other modules.
 */

#pragma once

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/extensions/pg_lake_table.h"

#include "nodes/pg_list.h"

/*
 * Per-tx temp table populated by the bulk path with the ids of newly-added
 * data files that PgLakeAddDataFileHook opted in. Read by the old per-row
 * catalog code in data_files_catalog.c, written by data_files_catalog_batch.c.
 */
#define TX_DATA_FILES_QUALIFIED_TABLE_NAME PG_LAKE_TABLE_SCHEMA "tx_data_file_ids"

/* True when adjacent ops of this type can be collapsed into one bulk SQL. */
bool		BatchableType(TableMetadataOperationType type);

/* Apply a run of same-typed batchable ops to the files catalog. */
void		ApplyDataFileBatch(Oid relationId, TableMetadataOperationType type, List *batch);

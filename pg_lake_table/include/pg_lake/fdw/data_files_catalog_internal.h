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
 * Per-tx temp table populated unconditionally by the bulk add path with the
 * id of every data or position-delete file the current top-level
 * transaction added. Read by GetTableDataFilesHashFromCatalog's
 * newFilesOnly predicate (data_files_catalog.c) and by the append-only
 * commit path (track_iceberg_metadata_changes.c); written by
 * data_files_catalog_batch.c.
 *
 * This is a session temp table, so it lives in pg_temp regardless of what
 * name it is given; the name below is plain, not schema-qualified like
 * DATA_FILES_TABLE_QUALIFIED.
 */
#define TX_DATA_FILES_TABLE_NAME "pg_lake_tx_data_file_ids"

/* True when adjacent ops of this type can be collapsed into one bulk SQL. */
bool		BatchableType(TableMetadataOperationType type);

/* Apply a run of same-typed batchable ops to the files catalog. */
void		ApplyDataFileBatch(Oid relationId, TableMetadataOperationType type, List *batch);

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

#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/iceberg/manifest_spec.h"

#define DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED \
 	PG_LAKE_TABLE_SCHEMA "." PG_LAKE_TABLE_DATA_FILE_COLUMN_STATS_TABLE_NAME

extern void AddDataFileColumnStatsToCatalog(Oid relationId, const char *path, List *columnStatsList);
extern void AddDataFilePartitionValueToCatalog(Oid relationId, int32 partitionSpecId, int64 fileId,
											   Partition * partition);
extern bool DataFileColumnStatsCatalogExists(void);

/*
 * LoadColumnStats fills in per-column min/max stats for the given dataFiles
 * by running a single SPI scan over data_file_column_stats filtered to the
 * file paths in the list. The dataFiles list must contain TableDataFile
 * pointers whose columnStats are NIL on entry; this function appends to
 * each dataFile->stats.columnStats.
 *
 * Pair this with a stats-free bulk read (GetTableDataFilesHashFromCatalog
 * or GetTableDataFilesByPathHashFromCatalog) to keep the bulk-read SPI
 * result set small and let this loader focus the catalog read to exactly
 * the files that need stats. Internally builds a small path->TableDataFile*
 * hash so the per-row apply is O(1) regardless of list size.
 */
extern PGDLLEXPORT void LoadColumnStats(Oid relationId, List *dataFiles);

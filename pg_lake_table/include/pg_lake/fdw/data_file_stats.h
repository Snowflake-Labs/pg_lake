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

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/data_file/data_files.h"

/*
 * ColumnStatsMode describes the mode of column stats.
 * - When truncate mode (default) is used, the column stats are truncated
 *   to the given length.
 * - When none mode is used, the column stats are not collected.
 */
typedef enum ColumnStatsMode
{
	COLUMN_STATS_MODE_TRUNCATE = 0,
	COLUMN_STATS_MODE_NONE = 1,
}			ColumnStatsMode;

/*
 * ColumnStatsConfig describes the configuration for column stats.
 * - mode: the mode of column stats.
 * - truncateLen: the length to truncate the column stats in truncate mode.
 */
typedef struct ColumnStatsConfig
{
	ColumnStatsMode mode;

	/* used for truncate mode */
	size_t		truncateLen;
}			ColumnStatsConfig;

extern PGDLLEXPORT DataFileStats * CreateDataFileStatsForTable(Oid relationId, char *dataFilePath,
															   int64 rowCount, int64 deletedRowCount,
															   DataFileContent content);
extern PGDLLEXPORT DataFileColumnStats * CreateDataFileColumnStats(int fieldId, PGType pgType,
																   char *lowerBoundText,
																   char *upperBoundText);
extern PGDLLEXPORT void ApplyColumnStatsMode(Oid relationId, List *columnStats);
extern PGDLLEXPORT void ApplyColumnStatsModeForAllFileStats(Oid relationId, List *dataFileStats);

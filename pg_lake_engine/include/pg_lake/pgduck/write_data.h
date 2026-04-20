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

#include "access/tupdesc.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/iceberg_validation.h"
#include "nodes/pg_list.h"

/*
 * Prefix for synthetic partition columns in DuckDB COPY TO queries.
 * The PARTITION_BY expressions are aliased as __pglake_part_0, __pglake_part_1, ...
 * and the partition_keys MAP returned by DuckDB uses these as keys.
 */
#define PARTITION_COLUMN_PREFIX "__pglake_part_"

/* pg_lake_table.target_row_group_size_mb */
#define DEFAULT_TARGET_ROW_GROUP_SIZE_MB 512
extern PGDLLEXPORT int TargetRowGroupSizeMB;

typedef enum ParquetVersion
{
	PARQUET_VERSION_V1 = 1,
	PARQUET_VERSION_V2 = 2
} ParquetVersion;

/* pg_lake_table.default_parquet_version */
extern PGDLLEXPORT int DefaultParquetVersion;

extern PGDLLEXPORT StatsCollector * ConvertCSVFileTo(char *csvFilePath,
													 TupleDesc tupleDesc,
													 int maxLineSize,
													 char *destinationPath,
													 CopyDataFormat destinationFormat,
													 CopyDataCompression destinationCompression,
													 List *formatOptions,
													 DataFileSchema * schema,
													 List *leafFields);
extern PGDLLEXPORT StatsCollector * WriteQueryResultTo(char *query,
													   char *destinationPath,
													   CopyDataFormat destinationFormat,
													   CopyDataCompression destinationCompression,
													   List *formatOptions,
													   bool queryHasRowId,
													   DataFileSchema * schema,
													   TupleDesc queryTupleDesc,
													   List *leafFields,
													   IcebergOutOfRangePolicy outOfRangePolicy,
													   bool wrapNativeIntervals,
													   List *partitionByExprs);
extern PGDLLEXPORT void AppendFields(StringInfo map, DataFileSchema * schema);
extern PGDLLEXPORT char *TupleDescToColumnMapForWrite(TupleDesc tupleDesc, CopyDataFormat destinationFormat);
extern PGDLLEXPORT char *TupleDescToProjectionListForWrite(TupleDesc tupleDesc,
														   CopyDataFormat destinationFormat);

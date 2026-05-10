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
#include "tcop/dest.h"

/* pg_lake_table.target_row_group_size_mb */
#define DEFAULT_TARGET_ROW_GROUP_SIZE_MB 512
extern PGDLLEXPORT int TargetRowGroupSizeMB;

/* pg_lake_engine.streaming_writes */
extern PGDLLEXPORT bool StreamingWritesEnabled;

/*
 * Sink-path placeholder substituted server-side by pgduck_server's
 * RECEIVE handler. Must match SINK_PLACEHOLDER in pgduck_server's
 * pgsession.c. Streaming-write callers pass this where the file-based
 * path would pass the local CSV file path; the placeholder is then
 * embedded inside read_csv('<placeholder>', ...) and pgduck swaps in
 * the server-local sink path before running the deferred query.
 */
#define PG_LAKE_RECV_PATH_PLACEHOLDER "@@PG_LAKE_RECV@@"

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
													   bool wrapNativeTypes);
/*
 * BuildCopyToCommandString assembles `COPY (query) TO 'destinationPath'
 * WITH (format ..., compression ..., return_stats, ...)` for the given
 * destination format / compression / format options. The same string
 * builder is used by WriteQueryResultTo (file path), OpenCSVStreamWriter
 * (streaming), and the streaming AddQueryResultToTable variant in
 * writable_table.c.
 */
extern PGDLLEXPORT char *BuildCopyToCommandString(char *query, char *destinationPath,
												  CopyDataFormat destinationFormat,
												  CopyDataCompression destinationCompression,
												  List *formatOptions,
												  bool queryHasRowId,
												  DataFileSchema * schema,
												  TupleDesc queryTupleDesc,
												  IcebergOutOfRangePolicy outOfRangePolicy,
												  bool wrapNativeTypes);

extern PGDLLEXPORT void AppendFields(StringInfo map, DataFileSchema * schema);
extern PGDLLEXPORT char *TupleDescToColumnMapForWrite(TupleDesc tupleDesc, CopyDataFormat destinationFormat);
extern PGDLLEXPORT char *TupleDescToProjectionListForWrite(TupleDesc tupleDesc,
														   CopyDataFormat destinationFormat);

/*
 * Streaming counterpart of ConvertCSVFileTo. See OpenCSVStreamWriter in
 * write_data.c for the contract. The struct is opaque to callers.
 */
typedef struct CSVStreamWriter CSVStreamWriter;

extern PGDLLEXPORT CSVStreamWriter *OpenCSVStreamWriter(TupleDesc csvTupleDesc,
														int maxLineSize,
														char *destinationPath,
														CopyDataFormat destinationFormat,
														CopyDataCompression destinationCompression,
														List *formatOptions,
														DataFileSchema * schema,
														List *leafFields);
extern PGDLLEXPORT DestReceiver *CSVStreamWriterDestReceiver(CSVStreamWriter * writer);
extern PGDLLEXPORT StatsCollector *FinishCSVStreamWriter(CSVStreamWriter * writer);

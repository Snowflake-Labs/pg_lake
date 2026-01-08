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
#include "datatype/timestamp.h"

#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/pgduck/client.h"

 /*
  * DataFileColumnStats stores column statistics for a data file.
  */
typedef struct DataFileColumnStats
{
	/* leaf field */
	LeafField	leafField;

	/* lower bound of the column in text representation of the field's type */
	char	   *lowerBoundText;

	/* upper bound of the column in text representation of the field's type */
	char	   *upperBoundText;
}			DataFileColumnStats;

 /*
  * DataFileStats stores all statistics for a data file.
  */
typedef struct DataFileStats
{
	char	   *dataFilePath;

	/* number of bytes in the file */
	int64		fileSize;

	/* number of rows in the file (-1 for unknown) */
	int64		rowCount;

	/* number of rows deleted from in the file via merge-on-read */
	int64		deletedRowCount;

	/* when the file was created */
	TimestampTz creationTime;

	/* column stats */
	List	   *columnStats;

	/* for a new data file with row IDs, the start of the range */
	int64		rowIdStart;
}			DataFileStats;

typedef struct ColumnStatsCollector
{
	int64 totalRowCount;
	List *dataFileStats;
} ColumnStatsCollector;

extern PGDLLEXPORT DataFileStats * DeepCopyDataFileStats(const DataFileStats * stats);
extern PGDLLEXPORT ColumnStatsCollector *GetDataFileStatsListFromPGResult(PGresult *result,
																		  List *leafFields,
																		  DataFileSchema * schema);
extern PGDLLEXPORT ColumnStatsCollector *ExecuteCopyCommandOnPGDuckConnection(char *copyCommand,
																			  List *leafFields,
																			  DataFileSchema * schema,
																			  bool disablePreserveInsertionOrder,
																			  CopyDataFormat destinationFormat);
extern PGDLLEXPORT bool ShouldSkipStatistics(LeafField * leafField);

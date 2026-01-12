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

#ifndef PG_LAKE_PGDUCK_READ_DATA_H
#define PG_LAKE_PGDUCK_READ_DATA_H

#include "postgres.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/parquet/field.h"

#define READ_DATA_TRANSMIT (1 << 0)
#define READ_DATA_PREFER_VARCHAR (1 << 1)
#define READ_DATA_EMIT_ROW_NUMBER (1 << 2)
#define READ_DATA_READ_ROW_LOCATION (1 << 3)
#define READ_DATA_EXPLICIT_CAST (1 << 4)
#define READ_DATA_EMIT_FILENAME (1 << 5)
#define READ_DATA_EMIT_ROW_LOCATION (READ_DATA_EMIT_FILENAME | READ_DATA_EMIT_ROW_NUMBER)
#define READ_DATA_EMIT_ROW_ID (1 << 6)

/* can be passed instead of ReadDataStats */
#define NO_STATISTICS ((ReadDataStats *) NULL)

#define INTERNAL_FILENAME_COLUMN_NAME "_pg_lake_filename"

/*
 * ReadDataStats instructs the ReadDataSourceQuery function about statistics
 * of the underlying data, which it can inject into the underlying query.
 */
typedef struct ReadDataStats
{
	uint64		sourceRowCount;
	uint64		positionDeleteRowCount;
}			ReadDataStats;

/*
 * ReadRowLocationMode reflects how we want to read filename and file_row_number.
 *
 * We sometimes need to read filename and file_row_number in order to
 * join with it (e.g. for position deletes) and sometimes we also want
 * it in the query result (e.g. for update/delete), so we distinguish
 * 3 cases.
 */
typedef enum ReadRowLocationMode
{
	NO_ROW_LOCATION,
	READ_ROW_LOCATION,
	EMIT_ROW_LOCATION
}			ReadRowLocationMode;


extern PGDLLEXPORT char *ReadDataSourceQuery(List *dataFilePaths,
											 List *positionDeletePaths,
											 CopyDataFormat sourceFormat,
											 CopyDataCompression sourceCompression,
											 TupleDesc expectedDesc,
											 List *formatOptions,
											 DataFileSchema * schema,
											 ReadDataStats * stats,
											 int flags);
extern PGDLLEXPORT char *TupleDescToProjectionList(TupleDesc tupleDesc,
												   CopyDataFormat sourceFormat,
												   List *formatOptions,
												   ReadRowLocationMode readRowLocationMode,
												   bool emitRowId,
												   bool addCast);

extern PGDLLEXPORT char *CopyOptionsToReadCSVParams(List *copyOptions);

#endif

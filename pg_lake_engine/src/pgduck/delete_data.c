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
 * Functions for generating queries for deleting data via pgduck server.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/delete_data.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/util/numeric.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


static char *DeleteFromParquetQuery(char *sourceDataFilePath,
									List *positionDeleteFiles,
									char *deletionFilePath,
									DataFileSchema * schema,
									ReadDataStats * stats);


/*
 * PerformDeleteFromParquet applies a deletion CSV file to a Parquet file
 * and writes the new Parquet file to destinationPath.
 */
StatsCollector *
PerformDeleteFromParquet(char *sourcePath,
						 List *positionDeleteFiles,
						 char *deletionFilePath,
						 char *destinationPath,
						 CopyDataCompression destinationCompression,
						 DataFileSchema * schema,
						 ReadDataStats * stats,
						 List *leafFields)
{
	const char *remainderQuery =
		DeleteFromParquetQuery(sourcePath, positionDeleteFiles, deletionFilePath, schema, stats);

	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "COPY (%s) TO %s",
					 remainderQuery,
					 quote_literal_cstr(destinationPath));

	/* start WITH options */
	appendStringInfoString(&command, " WITH (format 'parquet'");

	if (destinationCompression == DATA_COMPRESSION_NONE)
	{
		/* Parquet format uses uncompressed instead of none */
		appendStringInfo(&command, ", compression 'uncompressed'");
	}
	else
	{
		const char *compressionName =
			CopyDataCompressionToName(destinationCompression);

		appendStringInfo(&command, ", compression %s",
						 quote_literal_cstr(compressionName));
	}

	if (schema && schema->nfields > 0)
	{
		appendStringInfoString(&command, ", field_ids {");
		AppendFields(&command, schema);
		appendStringInfoString(&command, "}");
	}

	appendStringInfoString(&command, ", return_stats");

	/* end WITH options */
	appendStringInfoString(&command, ")");

	return ExecuteCopyToCommandOnPGDuckConnection(command.data,
												  leafFields,
												  schema,
												  destinationPath,
												  DATA_FORMAT_PARQUET);
}


/*
 * DeleteFromParquetQuery returns a query that applies a deletion file to a source file
 * and returns the remaining rows.
 */
static char *
DeleteFromParquetQuery(char *sourceDataFilePath, List *positionDeleteFiles,
					   char *deletionFilePath, DataFileSchema * schema,
					   ReadDataStats * stats)
{
	StringInfoData command;

	initStringInfo(&command);

	TupleDesc	expectedDesc = NULL;
	List	   *formatOptions = NIL;

	char	   *readFileQuery =
		ReadDataSourceQuery(list_make1(sourceDataFilePath), positionDeleteFiles,
							DATA_FORMAT_PARQUET, DATA_COMPRESSION_INVALID,
							expectedDesc, formatOptions, schema,
							stats, READ_DATA_READ_ROW_LOCATION);

	appendStringInfo(&command,
					 "%s %s file_row_number NOT IN ("
					 "  SELECT pos "
					 "  FROM read_csv(%s"
					 ", header=true, delim=',', quote='\"', escape='\"', nullstr='\\N'"
					 ", columns={'file_path':'varchar', 'pos':'bigint', 'row':'varchar'}))",
					 readFileQuery,
	/* position delete files adds a WHERE clause, so then we should use AND */
					 positionDeleteFiles != NIL ? "AND" : "WHERE",
					 quote_literal_cstr(deletionFilePath));

	return command.data;
}

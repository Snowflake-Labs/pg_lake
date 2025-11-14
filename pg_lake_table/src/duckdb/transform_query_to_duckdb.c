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

/*-------------------------------------------------------------------------
 *
 * transform_query_to_duckdb.c
 *		  Apply any transformations to the query before sending it to duckdb.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/relation.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_type_d.h"
#include "pg_lake/fdw/utils.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/util/rel_utils.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "tcop/tcopprot.h"

#include "pg_lake/duckdb/transform_query_to_duckdb.h"
#include "pg_lake/fdw/deparse_ruleutils.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/utils.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/string_utils.h"

static char *BuildReadDataSourceQueryForTableScan(PgLakeTableScan * tableScan,
												  bool skipFullMatchFiles,
												  TupleDesc projection);
static void EnsureServerType(Oid relationId);


/*
 * ReplaceReadTableFunctionCalls replaces the read_table function calls with
 * read_parquet function calls for all relations in the relationOids.
 *
 * If EXPLAIN_REQUEST flag is set, it replaces the read_table function call with
 * the relation name. That's only for the explain output, even for analyze.
 *
 * If SKIP_FULL_MATCH_FILES flag is set, we skip fullyIncluded file scans.
 */
char *
ReplaceReadTableFunctionCalls(char *query,
							  PgLakeScanSnapshot * snapshot,
							  int scanFlags)
{
	bool		explainRequested = (scanFlags & EXPLAIN_REQUESTED) != 0;
	bool		skipFullMatchFiles = (scanFlags & SKIP_FULL_MATCH_FILES) != 0;

	ListCell   *tableCell = NULL;

	foreach(tableCell, snapshot->tableScans)
	{
		PgLakeTableScan *tableScan = lfirst(tableCell);
		Oid			relationId = tableScan->relationId;
		int			uniqueRelationIdentifier = tableScan->uniqueRelationIdentifier;
		char	   *qualifiedRelationName = GetQualifiedRelationName(relationId);

		EnsureServerType(relationId);

		char	   *readFunctionForServer = NULL;

		if (explainRequested)
			readFunctionForServer = qualifiedRelationName;
		else
			readFunctionForServer = BuildReadDataSourceQueryForTableScan(tableScan,
																		 skipFullMatchFiles,
																		 NULL);

		StringInfo	functionCallToReplace = makeStringInfo();

		appendStringInfo(functionCallToReplace, "%s(%s::text, %d)",
						 PG_LAKE_READ_TABLE,
						 quote_literal_cstr(qualifiedRelationName),
						 uniqueRelationIdentifier);

		/*
		 * Finally, we replace the read_table() function call with the
		 * generated read call (e.g., read_parquet()).
		 *
		 * To recap what we did in this function, we replaced the read_table()
		 * function call with a read_[parquet/csv/json] function call with
		 * proper parameters. And, remember that read_table() function call is
		 * just a placeholder to replace the table name with a function call.
		 * read_table() function call is generic function that does not carry
		 * any parameters like the file format, compression, etc. It just
		 * knows the table name. Here in the executor, we know all the details
		 * about the file path, so we could replace the table name.
		 *
		 * So, as an end-to-end summary, we replaced the table name with a
		 * function call to read_[parquet/csv/json] function with proper
		 * parameters.
		 */
		query = PgLakeReplaceText(query, functionCallToReplace->data, readFunctionForServer);
	}

	/*
	 * We have a special provision for now(), which needs to return the same
	 * value for every call in the same transaction, namely the transaction
	 * start time, but would return a different value within DuckDB.
	 *
	 * It is a relatively rare case, because other stable functions relate
	 * primarily to server or session configuration.
	 */
	Datum		timestampDatum = TimestampTzGetDatum(GetCurrentTransactionStartTimestamp());
	Datum		timestampStringDatum = DirectFunctionCall1(timestamptz_out, timestampDatum);
	char	   *timestampString = DatumGetCString(timestampStringDatum);
	char	   *nowReplacementString = psprintf("%s::timestamptz ",
												quote_literal_cstr(timestampString));

	query = PgLakeReplaceText(query, PG_LAKE_NOW_TEMPLATE "()", nowReplacementString);

	return query;
}


/*
 * BuildReadDataSourceQueryForTableScan returns a query fragment to read
 * the files in the given table scan.
 *
 * If parentDesc is specified, use it
 */
static char *
BuildReadDataSourceQueryForTableScan(PgLakeTableScan * tableScan, bool skipFullMatchFiles, TupleDesc projection)
{
	Oid			relationId = tableScan->relationId;
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	DefElem    *pathOption = GetOption(options, "path");

	/*
	 * First, fetch the properties of the file: path, format and compression
	 */
	char	   *path = pathOption != NULL ? defGetString(pathOption) : NULL;
	PgLakeTableType tableType = GetPgLakeTableType(relationId);
	CopyDataFormat format = DATA_FORMAT_INVALID;
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;

	FindDataFormatAndCompression(tableType, path, options,
								 &format, &compression);

	/*
	 * Then, fetch the column definitions of the foreign table. We need to
	 * pass these to the read function to ensure that the data types are
	 * correctly interpreted. Say, if the query has sum(double) we should
	 * ensure that the read function returns a double.
	 */
	Relation	rel = RelationIdGetRelation(relationId);
	TupleDesc	readTupleDesc = projection != NULL ? projection : RelationGetDescr(rel);

	RelationClose(rel);
	ReadDataStats stats = {0, 0};

	/*
	 * Convert our scans to a list of paths. We may skip scans for files that
	 * are known to fully match the WHERE clause of a DELETE operation. We
	 * currently do not filter the position delete file list.
	 */
	List	   *dataFilePaths = GetFileScanPathList(tableScan->fileScans,
													&stats.sourceRowCount,
													skipFullMatchFiles);

	List	   *positionDeletePaths = GetFileScanPathList(tableScan->positionDeleteScans,
														  &stats.positionDeleteRowCount,
														  false);

	/*
	 * As a next step, we generate the relevant read function call (e.g.,
	 * read_parquet(..)) that is compatible with pgduck_server (and DuckDB).
	 *
	 * When isUpdateDelete is true, we ask for filename and file_row_number.
	 */
	int			readFlags = 0;

	if (tableScan->isUpdateDelete)
		readFlags |= READ_DATA_EMIT_ROW_LOCATION;

	DataFileSchema *schema = NULL;

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE || format == DATA_FORMAT_ICEBERG)
	{
		schema = GetDataFileSchemaForTable(relationId);
	}

	char	   *readCall =
		ReadDataSourceQuery(dataFilePaths, positionDeletePaths,
							format, compression, readTupleDesc, options,
							schema, &stats, readFlags);

	StringInfoData command;

	initStringInfo(&command);

	/*
	 * For UPDATE/DELETE, generate a synthetic ctid from the file row number
	 * and the index of the path in the file scans.
	 */
	if (tableScan->isUpdateDelete)
	{
		/*
		 * Open subquery to add synthetic ctid from the filename and
		 * file_row_number columns emitted by read_parquet. We do not want
		 * these columns to show up separately, so we use the DuckDB-specific
		 * EXCLUDE syntax.
		 */
		appendStringInfo(&command,
						 "(SELECT * EXCLUDE (" INTERNAL_FILENAME_COLUMN_NAME ", file_row_number), "
						 "{'filename':" INTERNAL_FILENAME_COLUMN_NAME ","
						 " 'file_row_number':file_row_number} "
						 "AS ctid FROM ");
	}

	appendStringInfoString(&command, "(");
	appendStringInfoString(&command, readCall);

	/*
	 * If there are child scans, include them via UNION ALL. We currently
	 * assume that the format is the same for all children.
	 */
	foreach_ptr(PgLakeTableScan, childScan, tableScan->childScans)
	{
		if (list_length(childScan->fileScans) == 0)
			continue;

		char	   *childQuery = BuildReadDataSourceQueryForTableScan(childScan, skipFullMatchFiles, readTupleDesc);

		appendStringInfo(&command, " UNION ALL %s", childQuery);
	}

	if (tableScan->isUpdateDelete)
	{
		/* end of subquery */
		appendStringInfoString(&command, ")");
	}

	/* end of query */
	appendStringInfoString(&command, ")");

	return command.data;
}


/**
 * EnsureServerType checks that the server type for a given relation is supported.
 *
 * This function checks if the foreign server name associated with the relation
 * matches the expected server type "pg_lake" or "pg_lake_iceberg".
 */
static void
EnsureServerType(Oid relationId)
{
	if (!IsAnyLakeForeignTableById(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("unsupported server type for relation %s",
						GetQualifiedRelationName(relationId)),
				 errhint("Only pg_lake or pg_lake_iceberg "
						 "server type is supported.")));
}

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
 * Hooks to implement COPY in Parquet format.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include "postgres.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include "access/table.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/copy/copy_io.h"
#include "pg_lake/copy/copy.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/describe/describe.h"
#include "pg_lake/extensions/pg_lake_copy.h"
#include "pg_lake/extensions/pg_parquet.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/parsetree/columns.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/planner/insert_select.h"
#include "pg_lake/pgduck/cache_control.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/query/execute.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/numeric.h"
#include "pg_lake/util/rel_utils.h"
#include "executor/executor.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "tcop/cmdtag.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/queryenvironment.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"


#define TEMP_FILE_PATTERN "pg_lake_copy"

/* allow hooking additional validity checks */
PgLakeCopyValidityCheckHookType PgLakeCopyValidityCheckHook = NULL;


static bool IsPgLakeCopy(CopyStmt *copyStmt);
static bool IsCopyFromStdin(CopyStmt *copyStmt);
static bool IsCopyToStdout(CopyStmt *copyStmt);
static void ProcessPgLakeCopyFrom(CopyStmt *copyStmt, ParseState *pstate,
								  Relation relation, Node *whereClause,
								  uint64 *rowsProcessed);
static bool IsCopyFromPushdownable(Relation relation, List *columnNameList,
								   Node *whereClause, CopyDataFormat sourceFormat);
static char *GenerateReadDataSourceQuery(char *sourcePath,
										 CopyDataFormat sourceFormat,
										 CopyDataCompression sourceCompression,
										 TupleDesc tupleDesc,
										 List *options,
										 DataFileSchema * schema,
										 int readFlags);
static List *FindCopyFromWriteOptions(CopyDataFormat format, List *options);
static void ProcessPgLakeCopyTo(CopyStmt *copyStmt, ParseState *pstate,
								Relation relation, RawStmt *query,
								const char *queryString, uint64 *rowsProcessed);
static List *FindCopyToReadOptions(CopyDataFormat format, List *options);
static void EnsureFormatSupported(CopyDataFormat destinationFormat,
								  CopyDataCompression destinationCompression,
								  bool isCopyTo);
static Node *TransformWhereClause(ParseState *pstate, ParseNamespaceItem *nsitem,
								  Node *whereClause);
static void CheckCopyTablePermission(CopyStmt *copyStmt, ParseState *pstate,
									 Relation relation,
									 RTEPermissionInfo *perminfo);
static void CheckCopyTableKind(CopyStmt *copyStmt, ParseState *pstate,
							   Relation relation);
static void ErrorIfCopyFromWithRowLevelSecurityEnabled(PlannedStmt *plannedStmt, Relation relation);
static RawStmt *CreateQueryForCopyToCommand(PlannedStmt *plannedStmt, Relation relation);
static TupleDesc BuildTupleDescriptorForRelation(Relation relation, List *attributeList);
static TupleDesc RemoveDroppedColumnsFromTupleDesc(TupleDesc tupleDesc);
static void VerifyNoDuplicateNames(TupleDesc tupleDesc);
static int	CopyReceivedTransmitDataToBuffer(void *outbuf, int minread, int maxread);
static bool ReceiveCopyData(PGDuckConnection * pgDuckConn, StringInfo buffer);

PG_FUNCTION_INFO_V1(pg_lake_last_copy_pushed_down_test);


/* settings */
bool		EnablePgLakeCopy = true;
bool		EnablePgLakeCopyJson = true;

/*
 * For COPY .. FROM, we convert incoming tuples into CSV format via a callback.
 * We use these global variables to pass state into the callbacks.
 *
 * The connection is closed automatically via PGDuckClientTransactionCallback.
 * The old pointers may remain in place and should always be reset before use.
 */
static PGDuckConnection * CurrentCopyFromConnection = NULL;
static StringInfoData CopyFromBuffer;


/*
 * LastCopyPushedDownTest is used purely for tests do detect whether a COPY command
 * was pushed down.
 */
static bool LastCopyPushedDownTest = false;


/*
 * PgLakeCopyHandler is a utility statement handler for handling
 * statements of the form:
 * - COPY table FROM 's3://...';
 * - COPY table TO 's3://...';
 * - COPY (SELECT ... ) TO 's3://...';
 */
bool
PgLakeCopyHandler(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, CopyStmt))
	{
		return false;
	}

	CopyStmt   *copyStmt = (CopyStmt *) plannedStmt->utilityStmt;

	if (!IsPgLakeCopy(copyStmt))
	{
		return false;
	}

	/* construct a parse state similar to standard_ProcessUtility */
	ParseState *pstate = make_parsestate(NULL);

	pstate->p_sourcetext = params->queryString;
	pstate->p_queryEnv = params->queryEnv;

	/* handle the COPY statement (replaces DoCopy) */
	uint64		rowsProcessed = 0;

	ProcessPgLakeCopy(pstate, plannedStmt, params->queryString, &rowsProcessed);

	if (params->completionTag != NULL)
	{
		SetQueryCompletion(params->completionTag, CMDTAG_COPY, rowsProcessed);
	}

	return true;
}


/*
 * IsPgLakeCopy returns whether a given COPY statement should be
 * handled by our COPY logic.
 */
static bool
IsPgLakeCopy(CopyStmt *copyStmt)
{
	if (!EnablePgLakeCopy)
	{
		/* disabled by the administrator */
		return false;
	}

	if (!IsExtensionCreated(PgLakeCopy) && IsExtensionCreated(PgParquet))
	{
		/* pg_lake_copy does not exist, but pg_parquet does */
		return false;
	}

	if (copyStmt->filename != NULL && !copyStmt->is_program &&
		IsSupportedURL(copyStmt->filename))
	{
		/* the filename is a URL */
		return true;
	}

	CopyDataFormat format = OptionsToCopyDataFormat(copyStmt->options);

	if (format != DATA_FORMAT_PARQUET && format != DATA_FORMAT_JSON)
	{
		/*
		 * Currently we only handle Parquet and JSON via STDIN/STDOUT, since
		 * PostgreSQL can already handle CSV.
		 *
		 * We could probably add some useful features like CSV compression via
		 * STDIN/STDIN, but the user experience of using different CSV parsers
		 * depending on whether compression is enabled would be confusing.
		 */
		return false;
	}

	return true;
}


/*
 * IsCopyFromStdin returns whether the given COPY statement is of
 * the form COPY .. FROM STDIN.
 */
static bool
IsCopyFromStdin(CopyStmt *copyStmt)
{
	return copyStmt->is_from && copyStmt->filename == NULL;
}


/*
 * IsCopyToStdout returns whether the given COPY statement is of
 * the form COPY .. TO STDOUT.
 */
static bool
IsCopyToStdout(CopyStmt *copyStmt)
{
	return !copyStmt->is_from && copyStmt->filename == NULL;
}


/*
 * ProcessPgLakeCopy processes commands of the form COPY .. TO/FROM 's3://...'
 */
void
ProcessPgLakeCopy(ParseState *pstate, PlannedStmt *plannedStmt, const char *queryString,
				  uint64 *rowsProcessed)
{
	CopyStmt   *copyStmt = (CopyStmt *) plannedStmt->utilityStmt;
	Node	   *whereClause = copyStmt->whereClause;

	Relation	relation = NULL;
	RawStmt    *query = NULL;

	if (copyStmt->is_program)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_copy: COPY FROM/TO PROGRAM is not supported")));
	}

	if (copyStmt->relation != NULL)
	{
		/*
		 * Do the initial permissions checks and WHERE clause processing.
		 *
		 * This block is mostly copied from copy.c, but split into several
		 * functions.
		 */

		LOCKMODE	lockmode = copyStmt->is_from ? RowExclusiveLock : AccessShareLock;

		/* open the relation */
		relation = table_openrv(copyStmt->relation, lockmode);

		/* add a synthetic RTE for the relation to use in WHERE */
		ParseNamespaceItem *nsitem;

		nsitem = addRangeTableEntryForRelation(pstate, relation, lockmode,
											   NULL, false, false);

		/* get the required permissions */
		RTEPermissionInfo *perminfo;

		perminfo = nsitem->p_perminfo;
		perminfo->requiredPerms = (copyStmt->is_from ? ACL_INSERT : ACL_SELECT);

		if (whereClause != NULL)
		{
			whereClause = TransformWhereClause(pstate, nsitem, whereClause);
		}

		CheckCopyTablePermission(copyStmt, pstate, relation, perminfo);
		CheckCopyTableKind(copyStmt, pstate, relation);

		/*
		 * We convert the relation to a query for any COPY .. TO statement,
		 * which is necessary because we use the Portal interface to execute
		 * the query and write it to a DestReceiver. This also ensures RLS
		 * works, and implicitly enables COPY foreign_table TO ...
		 */
		if (!copyStmt->is_from)
		{
			query = CreateQueryForCopyToCommand(plannedStmt, relation);

			/*
			 * Close the relation for now, but keep the lock on it to prevent
			 * changes between now and when we start the query-based COPY.
			 *
			 * We'll reopen it later as part of the query-based COPY.
			 */
			table_close(relation, NoLock);
			relation = NULL;
		}
		else
		{
			/* Postgres does not support COPY FROM when rls is enabled */
			ErrorIfCopyFromWithRowLevelSecurityEnabled(plannedStmt, relation);
		}
	}
	else
	{
		query = makeNode(RawStmt);
		query->stmt = copyStmt->query;
		query->stmt_location = plannedStmt->stmt_location;
		query->stmt_len = plannedStmt->stmt_len;
	}


	if (copyStmt->is_from)
	{
		ProcessPgLakeCopyFrom(copyStmt, pstate, relation, whereClause,
							  rowsProcessed);
	}
	else
	{
		ProcessPgLakeCopyTo(copyStmt, pstate, relation, query, queryString,
							rowsProcessed);
	}

	if (relation != NULL)
	{
		table_close(relation, NoLock);
	}
}


/*
 * ProcessPgLakeCopyFrom handles COPY .. FROM .. commands through the following steps:
 * 1. If COPY .. FROM STDIN, receive the Parquet file from the client
 * 2. Convert the Parquet file to a temporary CSV using PGDuck
 * 3. Do an internal COPY .. FROM a temporary CSV
 */
void
ProcessPgLakeCopyFrom(CopyStmt *copyStmt, ParseState *pstate, Relation relation,
					  Node *whereClause, uint64 *rowsProcessed)
{
	Oid			relationId = RelationGetRelid(relation);
	char	   *sourcePath = copyStmt->filename;
	bool		ensureCleanup = true;

	if (PgLakeCopyValidityCheckHook)
		PgLakeCopyValidityCheckHook(relationId);

	/* check read-only transaction and parallel mode */
	if (XactReadOnly)
	{
		PreventCommandIfReadOnly("COPY FROM");
	}

	if (CurrentCopyFromConnection != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_copy: nested COPY is not supported")));
	}

	if (IsSupportedURL(sourcePath))
	{
		CheckURLReadAccess();
	}

	/*
	 * Find the format and compression from the statement options.
	 */
	CopyDataFormat sourceFormat;
	CopyDataCompression sourceCompression;

	FindDataFormatAndCompression(PG_LAKE_INVALID_TABLE_TYPE, sourcePath, copyStmt->options,
								 &sourceFormat, &sourceCompression);

	/*
	 * Error early for unsupported cases
	 */
	EnsureFormatSupported(sourceFormat, sourceCompression, false);

	/*
	 * Find which options should pass through COPY .. FROM and do early
	 * validation.
	 *
	 * Marked volatile for compilers confused by PG_TRY.
	 */
	List	   *writeOptions = FindCopyFromWriteOptions(sourceFormat, copyStmt->options);

	/*
	 * Always include standard CSV file options that correspond to the
	 * transmit stream.
	 */
	bool		includeHeader = false;

	writeOptions = list_concat(writeOptions, InternalCSVOptions(includeHeader));

	/*
	 * Describe the expected input columns using a TupleDesc.
	 */
	TupleDesc	tupleDesc = BuildTupleDescriptorForRelation(relation, copyStmt->attlist);

	if (IsCopyFromStdin(copyStmt))
	{
		sourcePath = GenerateTempFileName(TEMP_FILE_PATTERN, ensureCleanup);

		/*
		 * we send the expected column count to make pedantic clients happy
		 */
		int			columnCount = tupleDesc->natts;

		bool		isBinary = true;

		/*
		 * We copy the incoming bytes to a file first and then try to convert
		 * that file. We could perhaps optimize this in the future by copying
		 * via a named pipe.
		 */
		CopyInputToFile(sourcePath, columnCount, isBinary);
	}

	/*
	 * Construct the pgduck query to read the source path and send it to
	 * pgduck.
	 */
	int			readFlags = 0;

	/*
	 * For regular COPY we add TRANSMIT to get streaming results that CopyFrom
	 * can parse.
	 *
	 * We make an optimization for writable tables where we do COPY (SELECT ..
	 * FROM read_csv) directly in DuckDB, so we do not want TRANSMIT in that
	 * case. However, we do want explicit casts to avoid writing incorrect
	 * types.
	 */
	bool		doCopyPushdown = IsCopyFromPushdownable(relation, copyStmt->attlist,
														whereClause, sourceFormat);

	if (!doCopyPushdown)
		readFlags |= READ_DATA_TRANSMIT;
	else
		readFlags |= READ_DATA_EXPLICIT_CAST;

	/*
	 * For CSV/JSON the values need to be parsed from text. We prefer to pull
	 * the values in as text and parse them in PostgreSQL to get friendlier
	 * parsing errors.
	 */
	if (sourceFormat == DATA_FORMAT_CSV || sourceFormat == DATA_FORMAT_JSON)
		readFlags |= READ_DATA_PREFER_VARCHAR;

	/*
	 * Similarly, for GDAL we prefer to pull values in as raw WKB and parse in
	 * PostGIS.
	 */
	else if (sourceFormat == DATA_FORMAT_GDAL)
	{
		DefElem    *keepWKBOption = makeDefElem("keep_wkb", (Node *) makeBoolean(true), -1);

		copyStmt->options = lappend(copyStmt->options, keepWKBOption);
	}

	DataFileSchema *schema = NULL;

	if (sourceFormat == DATA_FORMAT_ICEBERG)
	{
		/* read up-to date iceberg schema */
		schema = GetDataFileSchemaForExternalIcebergTable(sourcePath);
	}

	char	   *readQuery =
		GenerateReadDataSourceQuery(sourcePath, sourceFormat,
									sourceCompression, tupleDesc,
									copyStmt->options, schema,
									readFlags);

	/*
	 * Remember whether we pushed down for tests.
	 */
	LastCopyPushedDownTest = doCopyPushdown;

	/*
	 * We make an optimization for Iceberg/writable, since we can let DuckDB
	 * convert to Parquet directly and append it to the table.
	 */
	if (doCopyPushdown)
	{
		*rowsProcessed = AddQueryResultToTable(relationId, readQuery, tupleDesc);
		return;
	}

	/*
	 * Get a connection to send the query to pgduck.
	 */
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();

	/* start the transmit command */
	SendQueryToPGDuck(pgDuckConn, readQuery);

	/* check the initial result */
	PGresult   *result = PQgetResult(pgDuckConn->conn);

	if (PQresultStatus(result) != PGRES_COPY_OUT)
	{
		CheckPGDuckResult(pgDuckConn, result);
	}
	PQclear(result);

	/*
	 * Initialize the buffer into which CopyReceivedTransmitDataToBuffer
	 * writes CSV-formatted lines.
	 */
	initStringInfo(&CopyFromBuffer);

	/*
	 * Store the connection in a global variable to pass into
	 * CopyReceivedTransmitDataToBuffer.
	 *
	 * This is preferable over calling GetPGDuckConnection again, since that
	 * might trigger a reconnect, causing lots of confusion.
	 */
	CurrentCopyFromConnection = pgDuckConn;

	/*
	 * Some compilers get confused by using writeOptions after PG_TRY, even
	 * though it's not modified.
	 */
	volatile List *writeOptionsVolatile = writeOptions;

	/*
	 * Do an internal COPY .. FROM <CopyReceivedTransmitDataToBuffer()>
	 *
	 * Eventually, we may want to construct a TupleTableSlot for received
	 * tuples and do direct insertions, but the logic for that is relatively
	 * complex. We pay the price of constructing and subsequently parsing CSV
	 * to utilize COPY's batch write logic.
	 */
	PG_TRY();
	{
		CopyFromState cstate = BeginCopyFrom(pstate, relation, whereClause,
											 NULL, copyStmt->is_program,
											 CopyReceivedTransmitDataToBuffer,
											 copyStmt->attlist,
											 (List *) writeOptionsVolatile);

		*rowsProcessed = CopyFrom(cstate);
		EndCopyFrom(cstate);
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(pgDuckConn);
		CurrentCopyFromConnection = NULL;
	}
	PG_END_TRY();
}


/*
 * IsCopyFromPushdownable determines whether a COPY relation FROM command can
 * be fully pushed down into pgduck.
 */
static bool
IsCopyFromPushdownable(Relation relation, List *columnNameList,
					   Node *whereClause, CopyDataFormat sourceFormat)
{
	/*
	 * WHERE clause pushdown is not yet implemented.
	 */
	if (whereClause != NULL)
		return false;

	/*
	 * Currently we require all columns to be specified in table order, such
	 * that default expressions are not involved.
	 *
	 * We can relax this in the future by adding an additional target list
	 * that selects and orders the columns.
	 */
	if (columnNameList != NIL)
		return false;

	Oid			relationId = RelationGetRelid(relation);

	/*
	 * Only check foreign tables.
	 */
	if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE)
		return false;

	/*
	 * All of this only works for writable pg_lake tables.
	 */
	if (!IsWritablePgLakeTable(relationId) && !IsWritableIcebergTable(relationId))
		return false;

	bool		allowDefaultConsts = false;

	if (!RelationSuitableForPushdown(relation, allowDefaultConsts))
		return false;

	if (!RelationColumnsSuitableForPushdown(relation, sourceFormat))
		return false;

	const char *partitionBy = GetIcebergTablePartitionByOption(relationId);

	if (partitionBy != NULL)
		return false;

	return true;
}


/*
* GenerateReadDataSourceQuery generates the pgduck query to read the given
* source format/path.
*/
static char *
GenerateReadDataSourceQuery(char *sourcePath, CopyDataFormat sourceFormat,
							CopyDataCompression sourceCompression,
							TupleDesc tupleDesc, List *options,
							DataFileSchema * schema, int readFlags)
{

	List	   *dataFileList = NIL;
	List	   *positionDeleteList = NIL;

	if (sourceFormat == DATA_FORMAT_ICEBERG)
	{
		IcebergTableMetadata *metadata = ReadIcebergTableMetadata(sourcePath);

		FetchAllDataAndDeleteFilePathsFromCurrentSnapshot(metadata,
														  &dataFileList,
														  &positionDeleteList);
	}
	else
	{
		dataFileList = list_make1(sourcePath);
	}

	return ReadDataSourceQuery(dataFileList,
							   positionDeleteList,
							   sourceFormat,
							   sourceCompression,
							   tupleDesc,
							   options,
							   schema,
							   NO_STATISTICS,
							   readFlags);


}


/*
 * FindCopyFromWriteOptions returns the options from the given list that
 * need to carried over then writing to the table via COPY FROM.
 */
static List *
FindCopyFromWriteOptions(CopyDataFormat format, List *options)
{
	List	   *writeOptions = NIL;
	ListCell   *optionCell = NULL;

	foreach(optionCell, options)
	{
		DefElem    *option = lfirst(optionCell);

		/* common options that should not pass through */
		if (strcmp(option->defname, "format") == 0 ||
			strcmp(option->defname, "compression") == 0 ||
			strcmp(option->defname, "auto_detect") == 0 ||
			strcmp(option->defname, "filename") == 0)
		{
			continue;
		}

		/* common options that should pass through */
		else if (strcmp(option->defname, "freeze") == 0)
		{
			writeOptions = lappend(writeOptions, option);
			continue;
		}

		/* format-specific options */
		else if (format == DATA_FORMAT_CSV)
		{
			if (strcmp(option->defname, "header") == 0 ||
				strcmp(option->defname, "quote") == 0 ||
				strcmp(option->defname, "escape") == 0 ||
				strcmp(option->defname, "delimiter") == 0 ||
				strcmp(option->defname, "null") == 0)
			{
				/*
				 * When we do the internal COPY .. FROM, we do so from an
				 * intermediate CSV format that uses a standard set of
				 * options, whereas this set of options refers to the user's
				 * CSV, so we don't pass it through.
				 */
				continue;
			}
		}

		else if (format == DATA_FORMAT_GDAL)
		{
			/* custom option we use internally to process GDAL */
			if (strcmp(option->defname, "zip_path") == 0 ||
				strcmp(option->defname, "layer") == 0)
				continue;
		}

		else if (format == DATA_FORMAT_JSON)
		{
			/* custom option we use internally to allow JSON */
			if (strcmp(option->defname, "maximum_object_size") == 0)
				continue;
		}

		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_copy: invalid option \"%s\" "
							   "for COPY FROM with %s format",
							   option->defname,
							   CopyDataFormatToName(format))));
	}

	return writeOptions;
}


/*
 * ProcessPgLakeCopyTo handles COPY .. TO .. commands through the following steps:
 * 1. Do an internal COPY .. TO a temporary CSV
 * 2. Convert the CSV to a Parquet file using PGDuck
 * 3. If COPY .. TO STDOUT, send the Parquet file to the client
 */
static void
ProcessPgLakeCopyTo(CopyStmt *copyStmt, ParseState *pstate, Relation relation,
					RawStmt *rawQuery, const char *queryString, uint64 *rowsProcessed)
{
	if (IsSupportedURL(copyStmt->filename))
	{
		CheckURLWriteAccess();
	}

	/*
	 * Find the format and compression from the statement options.
	 */
	CopyDataFormat destinationFormat;
	CopyDataCompression destinationCompression;

	/*
	 * We currently do not need to know the table type for COPY .. TO because
	 * the options or filename should contain the format and compression.
	 */
	PgLakeTableType tableType = PG_LAKE_INVALID_TABLE_TYPE;

	FindDataFormatAndCompression(tableType, copyStmt->filename, copyStmt->options,
								 &destinationFormat, &destinationCompression);

	/*
	 * Error early for unsupported compression
	 */
	EnsureFormatSupported(destinationFormat, destinationCompression, true);

	/*
	 * Find which options should pass through COPY .. TO and do early
	 * validation.
	 */
	List	   *readOptions = FindCopyToReadOptions(destinationFormat, copyStmt->options);

	/*
	 * Always include standard CSV file options that correspond to the
	 * intermediate CSV file.
	 */
	bool		includeHeader = true;

	readOptions = list_concat(readOptions, InternalCSVOptions(includeHeader));

	/*
	 * COPY commands cannot have query parameters or named tuple stores.
	 */
	const Oid  *paramTypes = NULL;
	int			numParams = 0;
	QueryEnvironment *queryEnv = NULL;

	Assert(rawQuery != NULL);

	/*
	 * Analyze and rewrite the query to obtain the target list and prepare for
	 * planning. RLS rules are added here as well.
	 */
	List	   *rewritten = pg_analyze_and_rewrite_fixedparams(rawQuery,
															   queryString,
															   paramTypes,
															   numParams,
															   queryEnv);

	if (list_length(rewritten) == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("DO INSTEAD NOTHING rules are not supported for COPY")));
	}
	else if (list_length(rewritten) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("multi-statement DO INSTEAD rules are not supported for COPY")));
	}

	Query	   *query = linitial_node(Query, rewritten);
	List	   *targetList = query->targetList;

	/*
	 * Construct a clean TupleDesc that we can use the pass the order of
	 * fields and types into ConvertCSVFileTo.
	 *
	 * We do this before starting the COPY work to error early in case of
	 * duplicate column names.
	 */
	TupleDesc	tupleDesc = BuildTupleDescriptorForTargetList(targetList);

	VerifyNoDuplicateNames(tupleDesc);

	/*
	 * Do an internal COPY .. TO '<tempCSVPath>' to write the table or query
	 * result into a CSV file.
	 *
	 * We call the COPY functions directly because we would not be allowed to
	 * do DoCopy as a non-superuser when writing to a file.
	 *
	 * Instead of writing directly to a file, we use callbacks that write to a
	 * file and measure the longest line length, which is necessary for
	 * DuckDB's read_csv() to work reliably.
	 */
	bool		ensureCleanup = true;
	char	   *tempCSVPath = GenerateTempFileName(TEMP_FILE_PATTERN, ensureCleanup);

	/* execute query and write results to CSV */
	DestReceiver *dest = CreateCSVDestReceiver(tempCSVPath, readOptions, destinationFormat);

	*rowsProcessed = ExecuteQueryToDestReceiver(query, queryString, NULL, dest);

	/* get the maximum line length to correctly read the CSV */
	int			maximumLineLength = GetCSVDestReceiverMaxLineSize(dest);

	char	   *destinationPath = copyStmt->filename;

	if (IsCopyToStdout(copyStmt))
	{
		/*
		 * In case of COPY .. TO STDOUT, we first write to another temporary
		 * file in the target format
		 */
		destinationPath = GenerateTempFileName(TEMP_FILE_PATTERN, ensureCleanup);
	}
	else if (IsSupportedURL(destinationPath))
	{
		/*
		 * Make sure we invalidate the cache in case we are rewriting into an
		 * existing file in the cache. Otherwise, this becomes a no-op.
		 */
		bool		isRemoved = RemoveFileFromCache(destinationPath);

		if (isRemoved)
		{
			ereport(NOTICE, (errmsg("pg_lake_copy: File %s has been removed from "
									"the cache because it was replaced.", destinationPath)));
		}
	}

	/* we do not write Parquet field IDs for regular COPY TO */
	DataFileSchema *schema = NULL;

	/*
	 * Copy the CSV file to the destination path in the desired format.
	 */
	ConvertCSVFileTo(tempCSVPath, tupleDesc, maximumLineLength,
					 destinationPath, destinationFormat, destinationCompression,
					 copyStmt->options, schema);

	if (IsCopyToStdout(copyStmt))
	{
		/* send the temporary file in the target format to the client */
		CopyFileToOutput(destinationPath, tupleDesc->natts, true);
	}
}


/*
 * EnsureFormatSupported checks whether the format & compression combination
 * is supported.
 */
static void
EnsureFormatSupported(CopyDataFormat destinationFormat,
					  CopyDataCompression destinationCompression,
					  bool isCopyTo)
{
	if (destinationFormat == DATA_FORMAT_JSON)
	{
		if (destinationCompression == DATA_COMPRESSION_SNAPPY)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: snappy compression is not "
								   "supported for JSON format")));
		}

		if (!EnablePgLakeCopyJson)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: JSON is experimental "
								   "and currently disabled")));
		}
	}
	else if (destinationFormat == DATA_FORMAT_CSV)
	{
		if (destinationCompression == DATA_COMPRESSION_SNAPPY)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: snappy compression is not "
								   "supported for CSV format")));
		}
	}

	if (isCopyTo)
	{
		if (destinationFormat == DATA_FORMAT_ICEBERG)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: COPY TO in Iceberg format is not "
								   "supported")));

		if (destinationFormat == DATA_FORMAT_DELTA)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: COPY TO in Delta format is not "
								   "supported")));
	}


}


/*
 * FindCopyToReadOptions returns the options from the given list that
 * need to carried over then reading from the table via COPY TO.
 *
 * Currently the resulting list is empty and we only do validation.
 */
static List *
FindCopyToReadOptions(CopyDataFormat format, List *options)
{
	List	   *readOptions = NIL;
	ListCell   *optionCell = NULL;

	foreach(optionCell, options)
	{
		DefElem    *option = lfirst(optionCell);

		/* common options that should not pass through */
		if (strcmp(option->defname, "format") == 0 ||
			strcmp(option->defname, "compression") == 0)
		{
			/* we do not pass on our custom format / compression */
			continue;
		}

		/* format-specific options */
		else if (format == DATA_FORMAT_CSV)
		{
			if (strcmp(option->defname, "header") == 0 ||
				strcmp(option->defname, "quote") == 0 ||
				strcmp(option->defname, "escape") == 0 ||
				strcmp(option->defname, "delimiter") == 0 ||
				strcmp(option->defname, "null") == 0 ||
				strcmp(option->defname, "force_quote") == 0)
			{
				/* should not pass through because we rewrite the CSV */
				continue;
			}
		}

		/*
		 * We currently do not propagate any other options.
		 */
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("pg_lake_copy: invalid option \"%s\" "
							   "for COPY TO with %s format",
							   option->defname,
							   CopyDataFormatToName(format))));
	}

	return readOptions;
}


/*
 * TransformWhereClause is based on a section of DoCopy in PostgreSQL
 * to transform a WHERE clause in COPY.
 */
static Node *
TransformWhereClause(ParseState *pstate, ParseNamespaceItem *nsitem,
					 Node *whereClause)
{
	/* add nsitem to query namespace */
	addNSItemToQuery(pstate, nsitem, false, true, true);

	/* Transform the raw expression tree */
	whereClause = transformExpr(pstate, whereClause, EXPR_KIND_COPY_WHERE);

	/* Make sure it yields a boolean result. */
	whereClause = coerce_to_boolean(pstate, whereClause, "WHERE");

	/* we have to fix its collations too */
	assign_expr_collations(pstate, whereClause);

	whereClause = eval_const_expressions(NULL, whereClause);

	whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
	whereClause = (Node *) make_ands_implicit((Expr *) whereClause);

	return whereClause;
}


/*
 * CheckCopyTablePermission is based on a section of DoCopy in PostgreSQL
 * to check the user's permission on the table.
 */
static void
CheckCopyTablePermission(CopyStmt *copyStmt, ParseState *pstate, Relation relation,
						 RTEPermissionInfo *perminfo)
{
	TupleDesc	tupDesc;
	List	   *attnums;
	ListCell   *cur;

	tupDesc = RelationGetDescr(relation);
	attnums = CopyGetAttnums(tupDesc, relation, copyStmt->attlist);
	foreach(cur, attnums)
	{
		int			attno;
		Bitmapset **bms;

		attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;
		bms = copyStmt->is_from ? &perminfo->insertedCols : &perminfo->selectedCols;

		*bms = bms_add_member(*bms, attno);
	}

	int			gucLevel = NewGUCNestLevel();

	/*
	 * Disable pgaudit during COPY commands. There is an incompatibility
	 * between pgaudit and COPY commands. pgaudit expects its own
	 * prev_standardUtility to be called before the executor permission check
	 * hook is called. However, our COPY command does not call
	 * prev_standardUtility, so pgaudit crashes. Instead, here we disable
	 * pgaudit for the duration of the COPY command.
	 */
	set_config_option("pgaudit.log", "none",
					  PGC_SUSET, PGC_S_SESSION,
					  GUC_ACTION_SAVE, true, 0, false);

	ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), true);

	AtEOXact_GUC(true, gucLevel);
}


/*
 * CheckCopyTableKind -- validate the relation kind
 */
static void
CheckCopyTableKind(CopyStmt *copyStmt, ParseState *pstate, Relation relation)
{
	if (copyStmt->is_from || copyStmt->query != NULL)
		return;

	/*
	 * These checks are required when we are using a relation name instead of
	 * a query
	 */
	if (relation != NULL && relation->rd_rel->relkind != RELKIND_RELATION)
	{
		if (relation->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from view \"%s\"",
							RelationGetRelationName(relation)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (relation->rd_rel->relkind == RELKIND_MATVIEW)
		{
#if PG_VERSION_NUM >= 180000
			if (!RelationIsPopulated(relation))
				ereport(ERROR,
						errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot copy from unpopulated materialized view \"%s\"",
							   RelationGetRelationName(relation)),
						errhint("Use the REFRESH MATERIALIZED VIEW command."));
#else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from materialized view \"%s\"",
							RelationGetRelationName(relation)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
#endif
		}
		else if (relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from foreign table \"%s\"",
							RelationGetRelationName(relation)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (relation->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from sequence \"%s\"",
							RelationGetRelationName(relation))));
		else if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from partitioned table \"%s\"",
							RelationGetRelationName(relation)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from non-table relation \"%s\"",
							RelationGetRelationName(relation))));
	}
}


/*
 * ErrorIfCopyFromWithRowLevelSecurityEnabled is based on a section of DoCopy in PostgreSQL
 * to prevent COPY FROM when row-level security is enabled for given relation
 * and the current user.
 */
static void
ErrorIfCopyFromWithRowLevelSecurityEnabled(PlannedStmt *plannedStmt, Relation relation)
{
	CopyStmt   *copyStmt PG_USED_FOR_ASSERTS_ONLY = (CopyStmt *) plannedStmt->utilityStmt;

	Assert(copyStmt->is_from);

	Oid			relid = RelationGetRelid(relation);
	Oid			checkAsUser = InvalidOid;
	bool		noError = false;

	/* check if RLS is enabled for the relation for current user */
	if (check_enable_rls(relid, checkAsUser, noError) == RLS_ENABLED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY FROM not supported with row-level security"),
				 errhint("Use INSERT statements instead.")));
	}
}


/*
 * CreateQueryForCopyToCommand is based on a section of DoCopy in PostgreSQL
 * to build a query to use in place of a relation when a row-level security
 * policy is in effect.
 */
static RawStmt *
CreateQueryForCopyToCommand(PlannedStmt *plannedStmt, Relation relation)
{
	CopyStmt   *copyStmt = (CopyStmt *) plannedStmt->utilityStmt;

	SelectStmt *select;
	ColumnRef  *cr;
	ResTarget  *target;
	RangeVar   *from;
	List	   *targetList = NIL;

	/*
	 * Build target list
	 *
	 * If no columns are specified in the attribute list of the COPY command,
	 * then the target list is 'all' columns. Therefore, '*' should be used as
	 * the target list for the resulting SELECT statement.
	 *
	 * In the case that columns are specified in the attribute list, create a
	 * ColumnRef and ResTarget for each column and add them to the target list
	 * for the resulting SELECT statement.
	 */
	if (!copyStmt->attlist)
	{
		cr = makeNode(ColumnRef);
		cr->fields = list_make1(makeNode(A_Star));
		cr->location = -1;

		target = makeNode(ResTarget);
		target->name = NULL;
		target->indirection = NIL;
		target->val = (Node *) cr;
		target->location = -1;

		targetList = list_make1(target);
	}
	else
	{
		ListCell   *lc;

		foreach(lc, copyStmt->attlist)
		{
			/*
			 * Build the ColumnRef for each column.  The ColumnRef 'fields'
			 * property is a String node that corresponds to the column name
			 * respectively.
			 */
			cr = makeNode(ColumnRef);
			cr->fields = list_make1(lfirst(lc));
			cr->location = -1;

			/* Build the ResTarget and add the ColumnRef to it. */
			target = makeNode(ResTarget);
			target->name = NULL;
			target->indirection = NIL;
			target->val = (Node *) cr;
			target->location = -1;

			/* Add each column to the SELECT statement's target list */
			targetList = lappend(targetList, target);
		}
	}

	/*
	 * Build RangeVar for from clause, fully qualified based on the relation
	 * which we have opened and locked.  Use "ONLY" so that COPY retrieves
	 * rows from only the target table not any inheritance children, the same
	 * as when RLS doesn't apply.
	 */
	from = makeRangeVar(get_namespace_name(RelationGetNamespace(relation)),
						pstrdup(RelationGetRelationName(relation)),
						-1);
	from->inh = false;			/* apply ONLY */

	/* Build query */
	select = makeNode(SelectStmt);
	select->targetList = targetList;
	select->fromClause = list_make1(from);

	RawStmt    *query = makeNode(RawStmt);

	query->stmt = (Node *) select;
	query->stmt_location = plannedStmt->stmt_location;
	query->stmt_len = plannedStmt->stmt_len;

	return query;
}


/*
 * BuildTupleDescriptorForRelation creates a TupleDesc for a relation that
 * includes only the column whose name appears in attributeList, or
 * all columns if attributeList is empty.
 */
static TupleDesc
BuildTupleDescriptorForRelation(Relation relation, List *attributeList)
{
	TupleDesc	tableDescriptor = RelationGetDescr(relation);

	int			attributeCount = list_length(attributeList);

	if (attributeCount == 0)
	{
		return RemoveDroppedColumnsFromTupleDesc(tableDescriptor);
	}

	TupleDesc	attributeDescriptor = CreateTemplateTupleDesc(attributeCount);

	ListCell   *attributeCell = NULL;
	AttrNumber	attributeNumber = 1;

	foreach(attributeCell, attributeList)
	{
		char	   *attributeName = strVal(lfirst(attributeCell));
		Form_pg_attribute column = NULL;

		int			columnIndex = 0;

		/*
		 * Find the column with the name specified in the attribute list.
		 */
		for (; columnIndex < tableDescriptor->natts; columnIndex++)
		{
			column = TupleDescAttr(tableDescriptor, columnIndex);

			if (column->attisdropped)
				continue;

			if (namestrcmp(&(column->attname), attributeName) == 0)
			{
				break;
			}
		}

		if (columnIndex == tableDescriptor->natts)
		{
			/* should not really get here, since BeginCopyTo already checked */
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("column \"%s\" of relation \"%s\" does not exist",
								   attributeName, RelationGetRelationName(relation))));
		}

		TupleDescInitEntry(attributeDescriptor, (AttrNumber) attributeNumber,
						   attributeName, column->atttypid, column->atttypmod,
						   column->attndims);

		attributeNumber++;
	}

	return attributeDescriptor;
}


/*
 * RemoveDroppedColumnsFromTupleDesc returns a new TupleDesc with the
 * dropped columns removed and the remaining columns renumbered.
 *
 * We use this to construct a TupleDesc that matches the output of
 * the remote query, rather than the local table.
 */
static TupleDesc
RemoveDroppedColumnsFromTupleDesc(TupleDesc tableDescriptor)
{
	int			liveColumnCount = 0;

	/* count number of not-dropped columns */
	for (int columnIndex = 0; columnIndex < tableDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tableDescriptor, columnIndex);

		if (column->attisdropped)
			continue;

		liveColumnCount++;
	}

	TupleDesc	cleanTupleDesc = CreateTemplateTupleDesc(liveColumnCount);

	AttrNumber	attributeNumber = 1;

	for (int columnIndex = 0; columnIndex < tableDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tableDescriptor, columnIndex);

		if (column->attisdropped)
			continue;

		TupleDescInitEntry(cleanTupleDesc, (AttrNumber) attributeNumber,
						   NameStr(column->attname), column->atttypid, column->atttypmod,
						   column->attndims);

		attributeNumber++;
	}

	return cleanTupleDesc;
}

/*
 * VerifyNoDuplicateNames checks whether the given TupleDesc has any
 * duplicate column names, which we do not currently tolerate.
 */
static void
VerifyNoDuplicateNames(TupleDesc tupleDesc)
{
	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute outerColumn = TupleDescAttr(tupleDesc, attnum - 1);

		if (outerColumn->attisdropped)
		{
			continue;
		}

		/* check equality against all remaining columns */
		for (int innerAttnum = attnum + 1; innerAttnum <= tupleDesc->natts; innerAttnum++)
		{
			Form_pg_attribute innerColumn = TupleDescAttr(tupleDesc, innerAttnum - 1);

			if (innerColumn->attisdropped)
			{
				continue;
			}

			char	   *outerName = NameStr(outerColumn->attname);
			char	   *innerName = NameStr(innerColumn->attname);

			if (strcasecmp(outerName, innerName) == 0)
			{
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN),
								errmsg("pg_lake_copy: column \"%s\" specified more than "
									   "once",
									   outerName)));
			}
		}
	}
}


/*
 * CopyReceivedTransmitDataToBuffer receives data after a TRANSMIT command
 * and writes it to the outbuf.
 *
 * We may get data in batches that do not individually fit into the outbuf.
 * Therefore, we first append to CopyFromBuffer and then copy to outbuf
 * until CopyFromBuffer is fully consumed before fetching more data.
 *
 * We ignore minread because it is always 1 and doing so simplifies
 * the code.
 */
static int
CopyReceivedTransmitDataToBuffer(void *outbuf, int minread, int maxread)
{
	/* if there is data remaining in the buffer, copy that */
	int			bytesInBuffer = CopyFromBuffer.len - CopyFromBuffer.cursor;

	if (bytesInBuffer == 0)
	{
		/* buffer is flushed, reset the length and cursor */
		resetStringInfo(&CopyFromBuffer);

		if (!ReceiveCopyData(CurrentCopyFromConnection, &CopyFromBuffer))
		{
			/* no more tuples */
			return 0;
		}

		/* count how many bytes we have available */
		bytesInBuffer = CopyFromBuffer.len - CopyFromBuffer.cursor;
	}

	int			bytesToCopy = Min(maxread, bytesInBuffer);

	memcpy(outbuf, CopyFromBuffer.data + CopyFromBuffer.cursor, bytesToCopy);
	CopyFromBuffer.cursor += bytesToCopy;

	return bytesToCopy;
}


/*
 * ReceiveCopyData receives bytes via CopyData messages and writes
 * them to the buffer.
 */
static bool
ReceiveCopyData(PGDuckConnection * pgDuckConnection, StringInfo buffer)
{
	PGconn	   *conn = pgDuckConnection->conn;
	int			async = 0;
	char	   *copyBuffer = NULL;

	int			bytesReceived = PQgetCopyData(conn, &copyBuffer, async);

	if (bytesReceived < 0)
	{
		/* COPY requires one additional PQgetResult at the end */
		PGresult   *result = PQgetResult(pgDuckConnection->conn);

		if (result == NULL)
		{
			return false;
		}

		/* check for errors in COPY result */
		CheckPGDuckResult(pgDuckConnection, result);
		PQclear(result);

		return false;
	}

	/*
	 * Slightly pedantic, but appendBinaryStringInfo can throw an OOM and OOM
	 * is a horrible time to be leaking large buffers.
	 */
	PG_TRY();
	{
		appendBinaryStringInfo(buffer, copyBuffer, bytesReceived);
	}
	PG_FINALLY();
	{
		PQfreemem(copyBuffer);
	}
	PG_END_TRY();

	/* can only get 0 return value in async mode */
	Assert(bytesReceived != 0);

	return true;
}


/*
 * pg_lake_last_copy_pushed_down_test returns whether the last COPY command used
 * pushdown for testing purposes.
 */
Datum
pg_lake_last_copy_pushed_down_test(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(LastCopyPushedDownTest);
}

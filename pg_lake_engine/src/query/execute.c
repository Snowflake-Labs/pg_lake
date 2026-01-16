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

#include "postgres.h"

#include "pg_lake/csv/csv_options.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/query/execute.h"
#include "pg_extension_base/spi_helpers.h"
#include "executor/executor.h"
#include "optimizer/optimizer.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"


/*
 * ExecuteQueryString executes a query string internally.
 */
uint64
ExecuteInternalCommand(const char *queryString)
{
	return ExecuteQueryStringToDestReceiver(queryString, None_Receiver);
}


/*
 * ExecuteQueryStringToDestReceiver executes a query string internally and
 * writes the result to a DestReceiver.
 */
uint64
ExecuteQueryStringToDestReceiver(const char *queryString,
								 DestReceiver *dest)
{
	List	   *parseTrees = pg_parse_query(queryString);

	if (list_length(parseTrees) != 1)
		ereport(ERROR, (errmsg("can only parse a single query")));

	RawStmt    *rawStmt = (RawStmt *) linitial(parseTrees);

	List	   *queryTreeList =
		pg_analyze_and_rewrite_fixedparams(rawStmt, queryString, NULL, 0, NULL);

	if (list_length(queryTreeList) != 1)
		ereport(ERROR, (errmsg("can only execute a single query")));

	Query	   *query = (Query *) linitial(queryTreeList);

	return ExecuteQueryToDestReceiver(query, queryString, NULL, dest);
}


/*
 * ExecuteQueryToDestReceiver executes a query internally and writes the result
 * to a DestReceiver.
 */
uint64
ExecuteQueryToDestReceiver(Query *query, const char *queryString,
						   ParamListInfo params, DestReceiver *dest)
{
	PlannedStmt *plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, params);

	return ExecutePlanToDestReceiver(plan, queryString, params, dest);
}


/*
 * ExecutePlanToDestReceiver executes a plan and writes results to a DestReceiver.
 */
uint64
ExecutePlanToDestReceiver(PlannedStmt *plan, const char *queryString,
						  ParamListInfo params, DestReceiver *dest)
{
	QueryCompletion completionTag = {
		.commandTag = CMDTAG_SELECT,
		.nprocessed = 0
	};

	DISABLE_QUERY_TRACKING();

	Portal		portal = CreateNewPortal();

	portal->visible = false;

	PortalDefineQuery(portal, NULL, queryString, CMDTAG_SELECT, list_make1(plan), NULL);
	PortalStart(portal, params, 0, GetActiveSnapshot());
	PortalRun(portal,
			  FETCH_ALL,
			  false,
#if PG_VERSION_NUM < 180000
			  true,				/* run_once */
#endif
			  dest,
			  dest,
			  &completionTag);

	PortalDrop(portal, false);

	RESET_QUERY_TRACKING();

	return completionTag.nprocessed;
}


/*
 * ExecuteInternalDDL executes an internal DDL parse tree.
 */
void
ExecuteInternalDDL(Node *parseTree, char *queryString)
{
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);

	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = (Node *) parseTree;

#ifdef USE_ASSERTS
	ereport(DEBUG4, (errmsg("executing internal DDL tree: %s",
							nodeToString(plannedStmt))));
#endif

	ProcessUtility(plannedStmt, queryString, false, PROCESS_UTILITY_QUERY, NULL,
				   NULL, None_Receiver, NULL);
}


/*
 * Parse a simple utility statement
 */
Node *
SimpleParseUtility(char *queryString)
{
	List	   *parseTrees = pg_parse_query(queryString);

	if (list_length(parseTrees) != 1)
		ereport(ERROR, (errmsg("can only parse a single query")));

	RawStmt    *rawStmt = (RawStmt *) linitial(parseTrees);

	List	   *queryTreeList =
		pg_analyze_and_rewrite_fixedparams(rawStmt, queryString, NULL, 0, NULL);

	if (list_length(queryTreeList) != 1)
		ereport(ERROR, (errmsg("can only execute a single query")));

	Query	   *query = (Query *) linitial(queryTreeList);

	if (!query->utilityStmt)
		ereport(ERROR, (errmsg("SimpleParseUtility only works for utility statements")));

	return query->utilityStmt;
}


/*
 * WriteQueryResultToCSV writes the result of a query to destFile and
 * returns the number of rows.
 */
int64
WriteQueryResultToCSV(char *query, char *destFile, int *maxLineSize)
{
	bool		includeHeader = true;
	List	   *writeOptions = InternalCSVOptions(includeHeader);
	DestReceiver *dest = CreateCSVDestReceiver(destFile, writeOptions, DATA_FORMAT_PARQUET);

	int64		rowCount = ExecuteQueryStringToDestReceiver(query, dest);

	if (maxLineSize != NULL)
		*maxLineSize = GetCSVDestReceiverMaxLineSize(dest);

	return rowCount;
}

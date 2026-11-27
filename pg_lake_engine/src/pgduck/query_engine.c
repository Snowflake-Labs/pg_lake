/*
 * Copyright 2026 Snowflake Inc.
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

#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_relation.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/query_engine.h"
#include "pg_lake/util/rel_utils.h"


/*
 * FindQueryEngineContext keeps context during the FindQueryEngineWalker.
 */
typedef struct FindQueryEngineContext
{
	/* permission infos of the query currently being walked */
	List	   *rteperminfos;

	/* query engine of the first table found, which all tables must share */
	QueryEngine queryEngine;
	bool		hasQueryEngine;
}			FindQueryEngineContext;


static bool FindQueryEngineWalker(Node *node, FindQueryEngineContext * context);
static void CheckSameQueryEngine(QueryEngine queryEngine, QueryEngine otherEngine);


/*
 * QueryEnginesEqual returns whether a query can be sent to both engines as
 * one: the same server, read as the same user.
 */
bool
QueryEnginesEqual(QueryEngine left, QueryEngine right)
{
	return left.serverId == right.serverId && left.userId == right.userId;
}


/*
 * GetRelationQueryEngine returns the query engine that serves the given
 * relation when it is read as checkAsUser.
 */
QueryEngine
GetRelationQueryEngine(Oid relationId, Oid checkAsUser)
{
	if (!IsAnyLakeForeignTableById(relationId))
		elog(ERROR, "relation %s is not served by a query engine",
			 GetQualifiedRelationName(relationId));

	return PGDUCK_QUERY_ENGINE;
}


/*
 * GetRteQueryEngine returns the query engine that serves the relation of an
 * RTE of a query with the given permission infos.
 */
QueryEngine
GetRteQueryEngine(RangeTblEntry *rte, List *rteperminfos)
{
	Oid			checkAsUser = InvalidOid;

	if (rte->perminfoindex != 0)
		checkAsUser = getRTEPermissionInfo(rteperminfos, rte)->checkAsUser;

	return GetRelationQueryEngine(rte->relid, checkAsUser);
}


/*
 * GetQueryEngineForRteList returns the query engine that serves the relations
 * in the given list of RTEs, which are all read as checkAsUser. A single
 * query can only be sent to one engine, so all relations must be served by
 * the same engine. A query that reads no relations runs on pgduck_server.
 */
QueryEngine
GetQueryEngineForRteList(List *rteList, Oid checkAsUser)
{
	QueryEngine queryEngine = PGDUCK_QUERY_ENGINE;
	ListCell   *rteCell = NULL;

	foreach(rteCell, rteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
		QueryEngine relationEngine = GetRelationQueryEngine(rte->relid, checkAsUser);

		if (foreach_current_index(rteCell) > 0)
			CheckSameQueryEngine(queryEngine, relationEngine);

		queryEngine = relationEngine;
	}

	return queryEngine;
}


/*
 * GetQueryEngineForQuery returns the query engine that serves the tables of
 * the given query, including those of its subqueries, each read as the user
 * of its own permission info. A query that reads no tables runs on
 * pgduck_server.
 */
QueryEngine
GetQueryEngineForQuery(Query *query)
{
	FindQueryEngineContext context = {
		.rteperminfos = NIL,
		.queryEngine = PGDUCK_QUERY_ENGINE,
		.hasQueryEngine = false
	};

	FindQueryEngineWalker((Node *) query, &context);

	return context.queryEngine;
}


/*
 * FindQueryEngineWalker sets context->queryEngine to the engine of the tables
 * that a query engine serves, and checks that they all share it.
 */
static bool
FindQueryEngineWalker(Node *node, FindQueryEngineContext * context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		List	   *prevRtePermInfos = context->rteperminfos;

		context->rteperminfos = query->rteperminfos;

		bool		found = query_tree_walker(query, FindQueryEngineWalker,
											  context, QTW_EXAMINE_RTES_BEFORE);

		context->rteperminfos = prevRtePermInfos;

		return found;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind != RTE_RELATION || !IsAnyLakeForeignTableById(rte->relid))
			return false;

		QueryEngine relationEngine = GetRteQueryEngine(rte, context->rteperminfos);

		if (context->hasQueryEngine)
			CheckSameQueryEngine(context->queryEngine, relationEngine);

		context->queryEngine = relationEngine;
		context->hasQueryEngine = true;

		return false;
	}

	return expression_tree_walker(node, FindQueryEngineWalker, context);
}


/*
 * CheckSameQueryEngine errors out if the tables of a query are served by
 * different engines, which the planner does not send a query to as a whole.
 */
static void
CheckSameQueryEngine(QueryEngine queryEngine, QueryEngine otherEngine)
{
	if (!QueryEnginesEqual(queryEngine, otherEngine))
		elog(ERROR, "cannot query relations served by different query "
			 "engines together");
}


/*
 * GetQueryEngineConnection returns a new connection to the given query
 * engine. With setSessionTimeZone, the connection evaluates expressions in the
 * session's TimeZone, which a connection that runs a user's query needs.
 */
PGDuckConnection *
GetQueryEngineConnection(QueryEngine queryEngine, bool setSessionTimeZone)
{
	if (!QueryEnginesEqual(queryEngine, PGDUCK_QUERY_ENGINE))
		elog(ERROR, "unknown query engine %u", queryEngine.serverId);

	if (setSessionTimeZone)
		return GetPGDuckConnectionWithSessionTimeZone();

	return GetPGDuckConnection();
}

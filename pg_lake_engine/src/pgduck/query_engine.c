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

#include "nodes/parsenodes.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/query_engine.h"
#include "pg_lake/util/rel_utils.h"


/*
 * GetRelationQueryEngine returns the query engine that serves the given
 * relation.
 */
Oid
GetRelationQueryEngine(Oid relationId)
{
	if (!IsAnyLakeForeignTableById(relationId))
		elog(ERROR, "relation %s is not served by a query engine",
			 GetQualifiedRelationName(relationId));

	return PGDUCK_QUERY_ENGINE;
}


/*
 * GetQueryEngineForRteList returns the query engine that serves the relations
 * in the given list of RTEs. A single query can only be sent to one engine,
 * so all relations must be served by the same engine. A query that reads no
 * relations runs on pgduck_server.
 */
Oid
GetQueryEngineForRteList(List *rteList)
{
	Oid			queryEngine = PGDUCK_QUERY_ENGINE;
	ListCell   *rteCell = NULL;

	foreach(rteCell, rteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
		Oid			relationEngine = GetRelationQueryEngine(rte->relid);

		if (foreach_current_index(rteCell) > 0 && relationEngine != queryEngine)
			elog(ERROR, "cannot query relations served by different query "
				 "engines together");

		queryEngine = relationEngine;
	}

	return queryEngine;
}


/*
 * GetQueryEngineConnection returns a new connection to the given query
 * engine.
 */
PGDuckConnection *
GetQueryEngineConnection(Oid queryEngine)
{
	if (queryEngine != PGDUCK_QUERY_ENGINE)
		elog(ERROR, "unknown query engine %u", queryEngine);

	return GetPGDuckConnection();
}

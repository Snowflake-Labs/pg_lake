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

#pragma once

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "pg_lake/pgduck/client.h"

/*
 * A query engine runs the DuckDB SQL that we generate for a scan.
 * pgduck_server serves all pg_lake tables. A foreign table of the
 * pg_lake_remote foreign data wrapper is served by an external query engine,
 * identified by its foreign server.
 *
 * An external engine is reached with the user mapping of the user that its
 * tables are read as, so a query is only sent to one engine when all its
 * tables have the same server and user. pgduck_server is the same for every
 * user.
 */
typedef struct QueryEngine
{
	/* foreign server of the engine, or InvalidOid for pgduck_server */
	Oid			serverId;

	/*
	 * user that the tables are read as: the owner of a view that reads them,
	 * or InvalidOid for the current user
	 */
	Oid			userId;
}			QueryEngine;

#define PGDUCK_QUERY_ENGINE ((QueryEngine) {InvalidOid, InvalidOid})

#define PG_LAKE_REMOTE_FDW_NAME "pg_lake_remote"

extern PGDLLEXPORT bool IsExternalEngineTable(Oid relationId);
extern PGDLLEXPORT bool QueryEnginesEqual(QueryEngine left, QueryEngine right);
extern PGDLLEXPORT QueryEngine GetRelationQueryEngine(Oid relationId, Oid checkAsUser);
extern PGDLLEXPORT QueryEngine GetRteQueryEngine(RangeTblEntry *rte, List *rteperminfos);
extern PGDLLEXPORT QueryEngine GetQueryEngineForRteList(List *rteList, Oid checkAsUser);
extern PGDLLEXPORT QueryEngine GetQueryEngineForQuery(Query *query);
extern PGDLLEXPORT char *GetQueryEngineName(QueryEngine queryEngine);
extern PGDLLEXPORT PGDuckConnection * GetQueryEngineConnection(QueryEngine queryEngine,
															   bool setSessionTimeZone);

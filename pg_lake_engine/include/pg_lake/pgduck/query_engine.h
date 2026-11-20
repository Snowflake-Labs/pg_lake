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
 * A query engine runs the DuckDB SQL that we generate for a scan, and is
 * identified by an Oid. pgduck_server serves all pg_lake tables. A foreign
 * table of the pg_lake_remote foreign data wrapper is served by an external
 * query engine, identified by the Oid of its foreign server.
 *
 * An external engine is reached with the user mapping of the user that the
 * table is read as: the owner of a view that reads it, or else the current
 * user. A query is sent to an engine as one user, so the same server read as
 * two users counts as two engines.
 */
#define PGDUCK_QUERY_ENGINE InvalidOid

#define PG_LAKE_REMOTE_FDW_NAME "pg_lake_remote"

extern PGDLLEXPORT bool IsExternalEngineTable(Oid relationId);
extern PGDLLEXPORT bool IsQueryEngineTable(RangeTblEntry *rte);
extern PGDLLEXPORT Oid GetRelationQueryEngine(Oid relationId);
extern PGDLLEXPORT Oid GetQueryEngineForRteList(List *rteList);
extern PGDLLEXPORT char *GetQueryEngineName(Oid queryEngine);
extern PGDLLEXPORT PGDuckConnection * GetQueryEngineConnection(Oid queryEngine,
															   Oid checkAsUser);

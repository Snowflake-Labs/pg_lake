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

#include "nodes/pg_list.h"
#include "pg_lake/pgduck/client.h"

/*
 * A query engine runs the DuckDB SQL that we generate for a scan, and is
 * identified by an Oid. All pg_lake tables are served by pgduck_server.
 */
#define PGDUCK_QUERY_ENGINE InvalidOid

extern PGDLLEXPORT Oid GetRelationQueryEngine(Oid relationId);
extern PGDLLEXPORT Oid GetQueryEngineForRteList(List *rteList);
extern PGDLLEXPORT PGDuckConnection * GetQueryEngineConnection(Oid queryEngine,
															   bool setSessionTimeZone);

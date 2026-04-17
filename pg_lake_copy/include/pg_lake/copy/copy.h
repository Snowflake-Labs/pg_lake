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

#ifndef PG_LAKE_COPY_H
#define PG_LAKE_COPY_H

#include "pg_lake/copy/copy_format.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_node.h"
#include "pg_lake/ddl/utility_hook.h"

/* settings */
extern bool EnablePgLakeCopy;
extern bool EnablePgLakeCopyJson;

bool		PgLakeCopyHandler(ProcessUtilityParams *params, void *arg);
void		ProcessPgLakeCopy(ParseState *pstate, PlannedStmt *plannedStmt,
							  const char *queryString, uint64 *rowsProcessed);

/* support additional extension-provided checks for copy from */
typedef void (*PgLakeCopyValidityCheckHookType) (Oid relation);
extern PGDLLEXPORT PgLakeCopyValidityCheckHookType PgLakeCopyValidityCheckHook;

#endif

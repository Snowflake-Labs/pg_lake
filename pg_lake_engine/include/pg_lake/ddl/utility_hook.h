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

#pragma once

#include "tcop/utility.h"

/*
 * ProcessUtilityParams contains all the parameters of the ProcessUtility
 * function, such that they can be passed down into custom handlers.
 */
typedef struct ProcessUtilityParams
{
	PlannedStmt *plannedStmt;
	const char *queryString;
	bool		readOnlyTree;
	ProcessUtilityContext context;
	ParamListInfo params;
	struct QueryEnvironment *queryEnv;
	DestReceiver *dest;
	QueryCompletion *completionTag;
} ProcessUtilityParams;

/*
 * UtilityStatementHandler implementations can handle a utility statement end-to-end,
 * and may internally call PgLakeCommonProcessUtility (with handlers) or
 * PgLakeCommonParentProcessUtility (skip handlers).
 */
typedef bool (*UtilityStatementHandler) (ProcessUtilityParams *processUtilityParams, void *arg);

/*
 * UtilityStatementPostHandler implementations can handle a utility statement after
 * it is handled by UtilityStatementHandlers.
 */
typedef void (*UtilityStatementPostHandler) (ProcessUtilityParams *processUtilityParams, void *arg);

void		InitializeUtilityHook(void);

extern PGDLLEXPORT void PgLakeCommonProcessUtility(ProcessUtilityParams *processUtilityParams);
extern PGDLLEXPORT void PgLakeCommonParentProcessUtility(ProcessUtilityParams *processUtilityParams);
extern PGDLLEXPORT void RegisterUtilityStatementHandler(UtilityStatementHandler callback, void *arg);
extern PGDLLEXPORT void RegisterPostUtilityStatementHandler(UtilityStatementPostHandler callback, void *arg);
extern PGDLLEXPORT Node *CopyUtilityStmt(ProcessUtilityParams *processUtilityParams);

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
 * Top-level utility hook implementation.
 */
#include "postgres.h"
#include "miscadmin.h"

#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "tcop/cmdtag.h"
#include "tcop/utility.h"
#include "utils/queryenvironment.h"
#include "utils/memutils.h"

/*
 * Linked list of utility statement handlers, based on XactCallback.
 */
typedef struct UtilityStatementHandlerItem
{
	/* next handler in the linked list */
	struct UtilityStatementHandlerItem *next;

	/* function to call from ProcessUtility */
	UtilityStatementHandler handler;

	/* optional argument to the handler */
	void	   *arg;
} UtilityStatementHandlerItem;

/*
 * Linked list of post utility statement handlers, based on XactCallback.
 */
typedef struct UtilityStatementPostHandlerItem
{
	/* next handler in the linked list */
	struct UtilityStatementPostHandlerItem *next;

	/* function to call from ProcessUtility */
	UtilityStatementPostHandler handler;

	/* optional argument to the handler */
	void	   *arg;
} UtilityStatementPostHandlerItem;

static void PgLakeCommonProcessUtilityHook(PlannedStmt *plannedStmt,
										   const char *queryString,
										   bool readOnlyTree,
										   ProcessUtilityContext context,
										   ParamListInfo params,
										   struct QueryEnvironment *queryEnv,
										   DestReceiver *dest,
										   QueryCompletion *completionTag);

/* previous hooks */
static ProcessUtility_hook_type PrevProcessUtility = NULL;

/* registered handlers */
static UtilityStatementHandlerItem *UtilityStatementHandlers = NULL;
static UtilityStatementPostHandlerItem *UtilityStatementPostHandlers = NULL;


/*
 * InitializeUtilityHook sets up the process utility hook to
 * intercept DDL commands.
 */
void
InitializeUtilityHook(void)
{
	PrevProcessUtility = (ProcessUtility_hook != NULL) ?
		ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = PgLakeCommonProcessUtilityHook;
}


/*
 * PgLakeCommonProcessUtility modifies the behaviour of DDL Commands
 */
static void
PgLakeCommonProcessUtilityHook(PlannedStmt *plannedStmt,
							   const char *queryString,
							   bool readOnlyTree,
							   ProcessUtilityContext context,
							   ParamListInfo params,
							   struct QueryEnvironment *queryEnv,
							   DestReceiver *dest,
							   QueryCompletion *completionTag)
{
	ProcessUtilityParams processUtilityParams = {
		.plannedStmt = plannedStmt,
		.queryString = queryString,
		.readOnlyTree = readOnlyTree,
		.context = context,
		.params = params,
		.queryEnv = queryEnv,
		.dest = dest,
		.completionTag = completionTag,
	};

	PgLakeCommonProcessUtility(&processUtilityParams);
}


/*
 * PgLakeCommonProcessUtility tries to process a utility statement via
 * an array of handlers, and falls back to the regular process utility
 * logic otherwise.
 */
void
PgLakeCommonProcessUtility(ProcessUtilityParams *processUtilityParams)
{
	Node	   *parsetree = processUtilityParams->plannedStmt->utilityStmt;

	if (IsA(parsetree, TransactionStmt) ||
		IsA(parsetree, ListenStmt) ||
		IsA(parsetree, NotifyStmt) ||
		IsA(parsetree, ExecuteStmt) ||
		IsA(parsetree, PrepareStmt) ||
		IsA(parsetree, DiscardStmt) ||
		IsA(parsetree, DeallocateStmt) ||
		IsA(parsetree, DeclareCursorStmt) ||
		IsA(parsetree, FetchStmt))
	{
		/*
		 * For now, we assume these statements are never overridden by
		 * handlers, and skip additional processing.
		 */
		PgLakeCommonParentProcessUtility(processUtilityParams);
		return;
	}

	UtilityStatementHandlerItem *handlerItem;
	UtilityStatementHandlerItem *nextHandlerItem;

	for (handlerItem = UtilityStatementHandlers; handlerItem; handlerItem = nextHandlerItem)
	{
		/* allow handlers to unregister themselves when called */
		nextHandlerItem = handlerItem->next;

		if (handlerItem->handler(processUtilityParams, handlerItem->arg))
		{
			/*
			 * We stop processing when a handler returns true. Remaining
			 * handlers may still be called recursively.
			 */
			return;
		}
	}

	PgLakeCommonParentProcessUtility(processUtilityParams);

	UtilityStatementPostHandlerItem *postHandlerItem;
	UtilityStatementPostHandlerItem *nextPostHandlerItem;

	for (postHandlerItem = UtilityStatementPostHandlers; postHandlerItem; postHandlerItem = nextPostHandlerItem)
	{
		/* allow handlers to unregister themselves when called */
		nextPostHandlerItem = postHandlerItem->next;

		/* execute each post process utility one by one */
		postHandlerItem->handler(processUtilityParams, postHandlerItem->arg);
	}
}

/*
 * PgLakeCommonParentProcessUtility calls the previous process utility hook
 * (or standard_ProcessUtility), skipping any registered handlers.
 */
void
PgLakeCommonParentProcessUtility(ProcessUtilityParams *processUtilityParams)
{
	PlannedStmt *plannedStmt = processUtilityParams->plannedStmt;
	const char *queryString = processUtilityParams->queryString;
	bool		readOnlyTree = processUtilityParams->readOnlyTree;
	ProcessUtilityContext context = processUtilityParams->context;
	ParamListInfo params = processUtilityParams->params;
	struct QueryEnvironment *queryEnv = processUtilityParams->queryEnv;
	DestReceiver *dest = processUtilityParams->dest;
	QueryCompletion *completionTag = processUtilityParams->completionTag;

	PrevProcessUtility(plannedStmt, queryString, readOnlyTree, context,
					   params, queryEnv, dest, completionTag);
}


/*
 * RegisterUtilityStatementHandler registers a utility statement handler that
 * gets called for every utility statement.
 */
void
RegisterUtilityStatementHandler(UtilityStatementHandler handler, void *arg)
{
	UtilityStatementHandlerItem *item;

	item = (UtilityStatementHandlerItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(UtilityStatementHandlerItem));
	item->handler = handler;
	item->arg = arg;
	item->next = UtilityStatementHandlers;
	UtilityStatementHandlers = item;
}


/*
 * RegisterPostUtilityStatementHandler registers a utility statement handler that
 * gets called for every utility statement after the process utility call.
 */
void
RegisterPostUtilityStatementHandler(UtilityStatementPostHandler handler, void *arg)
{
	UtilityStatementPostHandlerItem *item;

	item = (UtilityStatementPostHandlerItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(UtilityStatementPostHandlerItem));
	item->handler = handler;
	item->arg = arg;
	item->next = UtilityStatementPostHandlers;
	UtilityStatementPostHandlers = item;
}


/*
 * CopyUtilityStmt() copies the PlannedStmt contained in a ProcessUtilityParams
 * and returns the utility statement it contains.
 */
Node *
CopyUtilityStmt(ProcessUtilityParams *processUtilityParams)
{
	processUtilityParams->plannedStmt = copyObject(processUtilityParams->plannedStmt);
	processUtilityParams->readOnlyTree = false;

	return processUtilityParams->plannedStmt->utilityStmt;
}

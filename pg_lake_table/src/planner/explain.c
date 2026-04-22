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
#include "tcop/tcopprot.h"

#include "pg_lake/fdw/shippable.h"
#include "pg_lake/planner/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "pg_lake/planner/query_pushdown.h"
#include "pg_lake/test/hide_lake_objects.h"


static void ExplainNotShippableObjects(HTAB *notShippableObjects, ExplainState *es);
static const char *NotShippableObjectToString(const NotShippableObject *notShippableObject);

ExplainOneQuery_hook_type PrevExplainOneQueryHook = NULL;


/*
 * PgLakeExplainHook is a hook that is called by the Postgres planner to explain
 * the query.
 *
 * The main reason that this hook exists is to provide additional information
 * about why a query is not shippable to pg_lake_table. Other than that,
 * we simply call the previous hook.
 */
void
PgLakeExplainHook(Query *query,
				  int cursorOptions,
				  IntoClause *into,
				  ExplainState *es,
				  const char *queryString,
				  ParamListInfo params,
				  QueryEnvironment *queryEnv)
{
	/* we do not want to show our filter in postgres explain output */
	DisableLocallyHideObjectsCreatedByLakeWhenAlreadyEnabled();

	if (!HasLakeRTE((Node *) query, NULL))
	{
		/* no lake table, bail out early */
		PrevExplainOneQueryHook(query, cursorOptions, into, es, queryString, params, queryEnv);

		return;
	}

	/* explain not shippable objects */
	HTAB	   *notShippableObjects = CollectNotShippableObjects((Node *) query);

	if (notShippableObjects == NULL || hash_get_num_entries(notShippableObjects) == 0)
	{
		/* no not shippable objects, bail out early */
		PrevExplainOneQueryHook(query, cursorOptions, into, es, queryString, params, queryEnv);

		return;
	}

	if (!es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
	{
		/*
		 * We currently only support text format for the additional
		 * information. Also, if the user does not want verbose output, we
		 * bail out early.
		 */
		PrevExplainOneQueryHook(query, cursorOptions, into, es, queryString, params, queryEnv);

		return;
	}

	/* print standard explain output first */
	StandardExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);

	/* then, append the non-shippable objects */
	ExplainNotShippableObjects(notShippableObjects, es);
}


/*
 * ExplainNotShippableObjects prints a list of not shippable objects to the
 * explain output.
 */
static void
ExplainNotShippableObjects(HTAB *notShippableObjects, ExplainState *es)
{
	Assert(hash_get_num_entries(notShippableObjects) > 0);

	ExplainPropertyText("Not Vectorized Constructs", "", es);

	int			msgIndex = 1;

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, notShippableObjects);

	NotShippableObject *notShippableObject = NULL;

	while ((notShippableObject = hash_seq_search(&status)) != NULL)
	{
		const char *message = NotShippableObjectToString(notShippableObject);

		ExplainPropertyText(psprintf("%d", msgIndex), message, es);

		msgIndex++;
	}
}


/*
 * NotShippableObjectToString returns a string of why a particular object was deemed not shippable.
 */
static const char *
NotShippableObjectToString(const NotShippableObject *notShippableObject)
{
	StringInfo	message = makeStringInfo();

	switch (notShippableObject->reason)
	{
		case NOT_SHIPPABLE_SQL_FOR_UPDATE:
		case NOT_SHIPPABLE_SQL_LIMIT_WITH_TIES:
		case NOT_SHIPPABLE_SQL_EMPTY_TARGET_LIST:
		case NOT_SHIPPABLE_SQL_WITH_ORDINALITY:
		case NOT_SHIPPABLE_SQL_JOIN_MERGED_COLUMNS_ALIAS:
		case NOT_SHIPPABLE_SQL_UNNEST_GROUP_BY_OR_WINDOW:
			appendStringInfo(message, "\tSQL Syntax\n");
			break;
		case NOT_SHIPPABLE_SYSTEM_COLUMN:
			appendStringInfo(message, "\tSystem Column\n");
			break;
		case NOT_SHIPPABLE_TABLEFUNC:
			appendStringInfo(message, "\tTable function\n");
			break;
		case NOT_SHIPPABLE_NAMEDTUPLESTORE:
			appendStringInfo(message, "\tNamed tuple store\n");
			break;
		case NOT_SHIPPABLE_MULTIPLE_FUNCTION_TABLE:
			appendStringInfo(message, "\tMultiple function tables\n");
			break;
		case NOT_SHIPPABLE_TABLE:
			appendStringInfo(message, "\tTable\n");
			break;
		case NOT_SHIPPABLE_TYPE:
			appendStringInfo(message, "\tType\n");
			break;
		case NOT_SHIPPABLE_FUNCTION:
			appendStringInfo(message, "\tFunction\n");
			break;
		case NOT_SHIPPABLE_SQL_VALUE_FUNCTION:
			appendStringInfo(message, "\tSQL Value Function\n");
			break;
		case NOT_SHIPPABLE_OPERATOR:
			appendStringInfo(message, "\tOperator\n");
			break;
		case NOT_SHIPPABLE_COLLATION:
			appendStringInfo(message, "\tCollation\n");
			break;
		case NOT_SHIPPABLE_UNKNOWN:
			appendStringInfo(message, "\tUnknown\n");
			break;
		default:
			pg_unreachable();
			break;
	}

	const char *description = GetNotShippableDescription(notShippableObject->reason,
														 notShippableObject->classId,
														 notShippableObject->objectId);

	if (description != NULL)
		appendStringInfo(message, "\tDescription: %s\n", description);

	return message->data;
}


#if PG_VERSION_NUM >= 170000

void
StandardExplainOneQuery(Query *query, int cursorOptions,
						IntoClause *into, ExplainState *es,
						const char *queryString, ParamListInfo params,
						QueryEnvironment *queryEnv)
{
	standard_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
}

#else

/*
 * StandardExplainOneQuery is a wrapper around the standard ExplainOneQuery which does not exist
 * in Postgres versions before 17. Taken from ExplainOneQuery from commands/explain.c.
 */
void
StandardExplainOneQuery(Query *query, int cursorOptions,
						IntoClause *into, ExplainState *es,
						const char *queryString, ParamListInfo params,
						QueryEnvironment *queryEnv)
{

	PlannedStmt *plan;
	instr_time	planstart,
				planduration;
	BufferUsage bufusage_start,
				bufusage;

	if (es->buffers)
		bufusage_start = pgBufferUsage;
	INSTR_TIME_SET_CURRENT(planstart);

	/* plan the query */
	plan = pg_plan_query(query, queryString, cursorOptions, params);

	INSTR_TIME_SET_CURRENT(planduration);
	INSTR_TIME_SUBTRACT(planduration, planstart);

	/* calc differences of buffer counters. */
	if (es->buffers)
	{
		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
	}

	/* run it (if needed) and produce output */
	ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration,
				   (es->buffers ? &bufusage : NULL));
}

#endif

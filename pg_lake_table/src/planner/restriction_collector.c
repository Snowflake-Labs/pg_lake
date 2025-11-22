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
* restriction_collector.c
*		Functions for collecting restrictions from Postgres planner. We first
*       assign unique identifiers to relations in the query tree, and then
*       collect restrictions from the Postgres planner. Later, we pass this
*       information to the executor and do pruning based on the restrictions
*       that Postgres planner knows about.
*/
#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/pg_class.h"
#include "common/hashfn.h"
#include "nodes/primnodes.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/paths.h"
#include "optimizer/optimizer.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/util/rel_utils.h"


/*
* Should be unique per query, so hopefully 1,000,000 is enough.
*/
#define MAX_UNIQUE_RELATION_IDENTIFIER 1000000

static List *plannerRelationRestrictionContextList = NIL;

typedef struct ReplaceExternalParamsContext
{
	bool		withPgDuckConsts;
	ParamListInfo boundParams;

}			ReplaceExternalParamsContext;

set_rel_pathlist_hook_type PrevRelPathlistHook = NULL;

static PlannerRelationRestrictionContext * CurrentRestrictionContext(void);
static int	GenerateNewUniqueRelationIdentifier(void);
static bool RestrictionExists(List *baseRestrictionList, RestrictInfo *restrictInfo);
static Node *ReplaceExternalParamsWithPgConsts(Node *inputNode, ParamListInfo boundParams);
static Node *ReplaceExternalParamsWithConsts(Node *inputNode, ReplaceExternalParamsContext * context);


/*
 * Forward declarations for our custom mini-deparser functions.
 */
static char *GenerateSyntheticTestQueryForRestrictInfo(int logLevel,
													   RangeTblEntry *rte,
													   List *baseRestrictInfoList);

/*
* GetNewUniqueRelationIdentifier returns a unique identifier for a relation.
*/
static int
GenerateNewUniqueRelationIdentifier(void)
{
	static int	uniqueRelationIdentifier = 0;

	/*
	 * After 1,000,000 start again from zero. We picked this number somewhat
	 * randomly as it is unlikely that we will have more than 1,000,000
	 * relations in a single query.
	 */
	if (uniqueRelationIdentifier > MAX_UNIQUE_RELATION_IDENTIFIER)
	{
		uniqueRelationIdentifier = 0;
	}

	return uniqueRelationIdentifier++;
}


/*
* See AdjustParseTreeForPgLake for why we need this hack.
*/
void
AssignUniqueRelationIdentifier(RangeTblEntry *rte)
{
	if (list_length(rte->functions) != 0)
	{
		/*
		 * We only assign unique identifiers once, and in majority of the
		 * cases, we assign once. However, complex extensions might call the
		 * planner hooks multiple times, and we might end up assigning the
		 * unique identifier multiple times. We ignore such cases.
		 */
		return;
	}

	int			uniqueRelationIdentifier = GenerateNewUniqueRelationIdentifier();

	SetUniqueRelationIdentifier(rte, uniqueRelationIdentifier);
}


/*
* See AdjustParseTreeForPgLake for why we need this hack.
*/
void
SetUniqueRelationIdentifier(RangeTblEntry *rte, int uniqueRelationIdentifier)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(list_length(rte->functions) == 0);

	rte->functions = lappend_int(rte->functions, uniqueRelationIdentifier);
}

/*
* GetUniqueRelationIdentifier returns the unique identifier for a relation that
* was assigned by AssignUniqueRelationIdentifier.
*/
int
GetUniqueRelationIdentifier(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(list_length(rte->functions) == 1);

	int			uniqueRelationIdentifier = linitial_int(rte->functions);

	Assert(uniqueRelationIdentifier >= 0 && uniqueRelationIdentifier <= MAX_UNIQUE_RELATION_IDENTIFIER);

	return uniqueRelationIdentifier;
}


/*
* PgLakeRecordPlannerRestrictions records the restrictions that Postgres planner
* knows about a relation. We record the restrictions in a custom node
* PlannerRelationRestriction and push into the current restrictionContext.
*/
void
PgLakeRecordPlannerRestrictions(PlannerInfo *root, RelOptInfo *relOptInfo,
								Index restrictionIndex, RangeTblEntry *rte)
{
	/* call into previous hook if assigned */
	if (PrevRelPathlistHook)
	{
		PrevRelPathlistHook(root, relOptInfo, restrictionIndex, rte);
	}

	if (rte->rtekind != RTE_RELATION)
	{
		/* we only care about relations */
		return;
	}

	if (!IsAnyLakeForeignTable(rte))
	{
		/* we only care about pg_lake relations */
		return;
	}

	/* add the restriction to the current context */
	PlannerRelationRestrictionContext *restrictionContext = CurrentRestrictionContext();

	Assert(restrictionContext != NULL);

	/*
	 * We normally assign relation identifiers upfront, but in some cases new
	 * relation RTEs get injected during planning. In particular, when a SQL
	 * function that queries a relation is inlined.
	 */
	if (list_length(rte->functions) == 0)
		AssignUniqueRelationIdentifier(rte);

	bool		relationFound = false;
	int			uniqueRelationIdentifier = GetUniqueRelationIdentifier(rte);
	RestrictionMapEntry *entry =
		hash_search(restrictionContext->relationRestrictionMap,
					&uniqueRelationIdentifier, HASH_ENTER, &relationFound);

	if (!relationFound)
	{
		/* first time we encounter a table, we set up its relationRestriction */
		entry->relationRestriction = PgLakeMakeNode(PlannerRelationRestriction);
		entry->relationRestriction->rte = rte;
	}

	/*
	 * Add the restriction to the relationRestriction. We only add the
	 * restriction if it is not already added. We use the rinfo_serial to
	 * check if the restriction is already added.
	 */
	ListCell   *restrictionCell = NULL;

	foreach(restrictionCell, relOptInfo->baserestrictinfo)
	{
		RestrictInfo *restrictInfo = lfirst(restrictionCell);

		List	   *storedRestrictionList =
			entry->relationRestriction->baseRestrictionList;

		if (!RestrictionExists(storedRestrictionList, restrictInfo))
		{
			entry->relationRestriction->baseRestrictionList =
				lappend(entry->relationRestriction->baseRestrictionList, restrictInfo);
		}
	}
}


/*
* RestrictionExists checks if the restrictInfo is already in the baseRestrictionList.
*/
static bool
RestrictionExists(List *baseRestrictionList, RestrictInfo *restrictInfo)
{
	ListCell   *restrictionCell = NULL;

	foreach(restrictionCell, baseRestrictionList)
	{
		RestrictInfo *baseRestrictInfo = lfirst(restrictionCell);

		if (baseRestrictInfo->rinfo_serial == restrictInfo->rinfo_serial)
		{
			return true;
		}
	}

	return false;
}


/*
 * CreateAndPushRestrictionContext creates a new restriction context, inserts it to the
 * beginning of the context list, and returns the newly created context.
 */
PlannerRelationRestrictionContext *
CreateAndPushRestrictionContext(void)
{
	PlannerRelationRestrictionContext *restrictionContext =
		palloc0(sizeof(PlannerRelationRestrictionContext));

	HASHCTL		hashCtl;

	hashCtl.keysize = sizeof(int);
	hashCtl.entrysize = sizeof(RestrictionMapEntry);
	hashCtl.hcxt = CurrentMemoryContext;
	hashCtl.hash = tag_hash;

	restrictionContext->relationRestrictionMap =
		hash_create("RestrictionMap", 32, &hashCtl, (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION));

	plannerRelationRestrictionContextList =
		lcons(restrictionContext, plannerRelationRestrictionContextList);

	return restrictionContext;
}


/*
 * CurrentRestrictionContext returns the the last restriction context from the
 * list.
 */
PlannerRelationRestrictionContext *
CurrentRestrictionContext(void)
{
	PlannerRelationRestrictionContext *restrictionContext = NULL;

	Assert(plannerRelationRestrictionContextList != NIL);

	restrictionContext =
		(PlannerRelationRestrictionContext *) linitial(plannerRelationRestrictionContextList);

	return restrictionContext;
}


/*
* GetCurrentRelationRestrictions is a utility function that returns the list of
* PlannerRelationRestriction for the current restriction context.
*/
List *
GetCurrentRelationRestrictions(void)
{
	PlannerRelationRestrictionContext *restrictionContext = CurrentRestrictionContext();

	List	   *result = NIL;

	HASH_SEQ_STATUS hashSeq;
	RestrictionMapEntry *entry = NULL;

	hash_seq_init(&hashSeq, restrictionContext->relationRestrictionMap);

	while ((entry = hash_seq_search(&hashSeq)) != NULL)
	{
		result = lappend(result, entry->relationRestriction);
	}

	return result;
}


/*
 * PopRestrictionContext removes the most recently added restriction context from
 * context list. The function assumes the list is not empty.
 */
void
PopRestrictionContext(void)
{
	plannerRelationRestrictionContextList =
		list_delete_first(plannerRelationRestrictionContextList);
}

/*
* ReplaceParamsInRestrictInfo replaces the external parameters that appears
* in the clause of the RestrictInfo with the corresponding entries in the
* boundParams.
*/
void
ReplaceParamsInRestrictInfo(List *baseRestrictionList, ParamListInfo paramListInfo)
{
	ListCell   *restrictionCell = NULL;

	foreach(restrictionCell, baseRestrictionList)
	{
		RestrictInfo *restrictInfo = lfirst(restrictionCell);

		restrictInfo->clause =
			(Expr *) ReplaceExternalParamsWithPgConsts((Node *) restrictInfo->clause, paramListInfo);
	}
}



/*
* ReplaceExternalParamsWithPgDuckConsts is a wrapper around ReplaceExternalParamsWithConsts,
* see the latter for more details.
 */
Node *
ReplaceExternalParamsWithPgDuckConsts(Node *inputNode, ParamListInfo boundParams)
{
	ReplaceExternalParamsContext context = {
		.withPgDuckConsts = true,
		.boundParams = boundParams
	};

	return ReplaceExternalParamsWithConsts(inputNode, &context);
}


/*
* ReplaceExternalParamsWithPgConsts is a wrapper around ReplaceExternalParamsWithConsts,
* see the latter for more details.
 */
static Node *
ReplaceExternalParamsWithPgConsts(Node *inputNode, ParamListInfo boundParams)
{
	ReplaceExternalParamsContext context = {
		.withPgDuckConsts = false,
		.boundParams = boundParams
	};

	return ReplaceExternalParamsWithConsts(inputNode, &context);
}


/*
 * ReplaceExternalParamsWithPgDuckConsts replaces the external parameters
 * that appears in the inputNode with the entries that have the same
 * parameter ID in the boundParams as long as they are consts.
 *
 * Depending on the withPgDuckConsts parameter, the consts are either re-written
 * to use the DuckDB serialization format or left as consts.
 *
 * When the function operates on nodes that are known to be sent to
 * pgduck_server should pass withPgDuckConsts=true.
 *
 * This function is designed by eval_const_expr() in Postgres
 * optimizer/optimizer.c.
 */
static Node *
ReplaceExternalParamsWithConsts(Node *inputNode, ReplaceExternalParamsContext * context)
{
	if (inputNode == NULL)
	{
		/* end of recursion, no match found */
		return NULL;
	}

	if (IsA(inputNode, Param))
	{
		Param	   *param = (Param *) inputNode;

		if (param->paramkind != PARAM_EXTERN)
		{
			/* only interested in external params */
			return inputNode;
		}

		int			paramId = param->paramid;

		if (paramId < 0)
		{
			return inputNode;
		}

		int			parameterIndex = paramId - 1;
		ParamListInfo boundParams = context->boundParams;

		if (parameterIndex >= boundParams->numParams)
		{
			return inputNode;
		}

		ParamExternData *externParam = &boundParams->params[parameterIndex];

		if (!(externParam->pflags & PARAM_FLAG_CONST))
		{
			/* we only replace params with consts */
			return inputNode;
		}

		Datum		constValue = 0;
		int16		typLen = 0;
		bool		typeByValue = false;

		get_typlenbyval(param->paramtype, &typLen, &typeByValue);

		if (externParam->isnull)
		{
			constValue = 0;
		}
		else if (typeByValue)
		{
			constValue = externParam->value;
		}
		else
		{
			constValue =
				datumCopy(externParam->value, typeByValue, typLen);
		}

		Const	   *pgConst = makeConst(param->paramtype, param->paramtypmod,
										param->paramcollid, typLen, constValue,
										externParam->isnull, typeByValue);

		/* rewrites a constant to use the DuckDB serialization format */
		return context->withPgDuckConsts ? RewriteConst(pgConst) : (Node *) pgConst;
	}
	else if (IsA(inputNode, Query))
	{
		return (Node *) query_tree_mutator((Query *) inputNode, ReplaceExternalParamsWithConsts,
										   context, 0);
	}

	return expression_tree_mutator(inputNode, ReplaceExternalParamsWithConsts, context);
}


/*
* PrettyPrintBaseRestrictInfo prints the restrictions for a relation in a more human
* readable format.
*/
void
PrettyPrintBaseRestrictInfo(int logLevel, RangeTblEntry *rte, List *baseRestrictInfoList)
{
	char	   *syntheticQuery =
		GenerateSyntheticTestQueryForRestrictInfo(logLevel, rte, baseRestrictInfoList);

	elog(logLevel, "Restrictions for relation: %s", syntheticQuery);

}


/*
* For a given RangeTblEntry and a list of RestrictInfo, generate a synthetic
* test query that represents the restrictions.
* The query would look like: SELECT * FROM table WHERE col_1 = X AND col_2 = Y ...
*/
static char *
GenerateSyntheticTestQueryForRestrictInfo(int logLevel, RangeTblEntry *rte,
										  List *baseRestrictInfoList)
{
	/*
	 * We'll use restrictInfo->clause, but we want to operate on a copy.
	 */
	List	   *copyOfRestrictClauses = NIL;
	ListCell   *restrictInfoCell = NULL;

	foreach(restrictInfoCell, baseRestrictInfoList)
	{
		RestrictInfo *restrictInfo = lfirst(restrictInfoCell);
		Expr	   *restrictionClause = copyObject(restrictInfo->clause);

		if (pull_paramids((Expr *) restrictionClause) != NULL)
		{
			/* we cannot deparse sub-plans */
			elog(logLevel, "Skipping a restriction with sub-plan: %s",
				 nodeToString(restrictionClause));
			continue;
		}

		copyOfRestrictClauses = lappend(copyOfRestrictClauses, restrictionClause);
	}

	/*
	 * We'll only have a single RTE in the query, so we can set varno and
	 * varnosyn to 1.
	 */
	List	   *varList = pull_var_clause((Node *) copyOfRestrictClauses, 0);
	ListCell   *cell = NULL;

	foreach(cell, varList)
	{
		Var		   *var = lfirst(cell);

		var->varno = 1;
		var->varnosyn = 1;
	}

	/* create a query tree with SELECT * FROM table WHERE restrictInfos  */
	Query	   *query = makeNode(Query);

	query->commandType = CMD_SELECT;

	RangeTblEntry *rteCopy = copyObject(rte);

	rte->perminfoindex = 1;

	query->rtable = list_make1(rteCopy);

	RangeTblRef *rteRef = makeNode(RangeTblRef);

	rteRef->rtindex = 1;

	Expr	   *andedFilters = make_ands_explicit(copyOfRestrictClauses);

	query->jointree = makeFromExpr(list_make1(rteRef), (Node *) andedFilters);

	return pg_get_querydef(query, false);
}

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
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/catalog.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_inherits.h"
#include "pg_lake/duckdb/transform_query_to_duckdb.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/fdw/deparse_ruleutils.h"
#include "pg_lake/fdw/shippable.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/planner/dbt.h"
#include "pg_lake/planner/explain.h"
#include "pg_lake/planner/pushdown_utils.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/planner/insert_select.h"
#include "pg_lake/planner/query_pushdown.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/pgduck/array_conversion.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/explain.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/test/hide_lake_objects.h"
#include "pg_lake/util/rel_utils.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "executor/tuptable.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "parser/parse_relation.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/typcache.h"

/*
 * IsShippableContext keeps context during the HasNotShippableExpression walker.
 */
typedef struct IsShippableContext
{
	/* whether set-returning functions are allowed in the current subtree */
	bool		srfAllowed;

	/* whether the current query has unnest (special case in DuckDB) */
	bool		hasUnnest;

	/* whether to stop at the first non-shippable object */
	bool		stopAtFirstNotShippable;

	/* map of not shippable objects */
	HTAB	   *notShippableObjects;
}			IsShippableContext;


static PlannedStmt *LakeTablePlanner(Query *parse, const char *queryString,
									 int cursorOptions, ParamListInfo boundParams);
static bool AdjustParseTreeForPgLake(Node *node, void *context);
static bool ProcessNotShippableExpressionWalker(Node *node, IsShippableContext * context);
static bool AddMissingRTEAliasaes(Node *node, void *context);
static bool TargetListHasOnlyResjunk(List *targetList);
static bool AllInheritorsAreLakeTable(Oid relationId);
static bool ExpressionHasCollation(Node *node, IsShippableContext * context);
static bool ExpressionHasNonShippableObject(Node *node, bool srfAllowed, IsShippableContext * context);
static bool ExpressionReturnsNonShippableType(Node *node, IsShippableContext * context);
static PlannedStmt *GenerateInsertSelectPushdownPlan(PlannedStmt *localPlan,
													 Query *query);
static PlannedStmt *GeneratePushdownPlan(PlannedStmt *localPlan, Query *originalQuery);
static CustomScan *GeneratePushdownScan(Query *originalQuery);
static RangeTblEntry *CreateResultRTE(List *columnNameList);
static Node *QueryPushdownCreateScanState(CustomScan *scan);
static void QueryPushdownBeginScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *QueryPushdownExecScan(CustomScanState *node);
static bool QueryPushdownScanRecheck(CustomScanState *node, TupleTableSlot *slot);
static TupleTableSlot *QueryPushdownScanNext(CustomScanState *node);
static TupleTableSlot *QueryPushdownScanNextInternal(CustomScanState *node);
static void QueryPushdownReScan(CustomScanState *node);
static void QueryPushdownEndScan(CustomScanState *node);
static void QueryPushdownExplainScan(CustomScanState *node, List *ancestors,
									 struct ExplainState *es);
static void RemoveUnusedParametersFromList(Query *query, ParamListInfo paramList);
static bool FindAllParams(Node *node, List **allParams);
static List *CreateInternalCustomScanTargetList(List *existingTargetlist);
static List *CreateCustomScanOutputTargetList(List *customScanTargetList);
static void TryRecordNotShippableObject(IsShippableContext * context, Oid objectId,
										Oid classId, NotShippableReason reason);
static void RecordNotShippableObject(IsShippableContext * context, Oid objectId,
									 Oid classId, NotShippableReason reason);
static HTAB *CreateNotShippableObjectsHash(void);


/* pg_lake_table.enable_full_query_pushdown setting */
bool		EnableFullQueryPushdown = true;

static planner_hook_type PreviousPlannerHook = NULL;

static CustomScanMethods QueryPushdownScanMethods =
{
	"Query Pushdown",
	QueryPushdownCreateScanState
};

static CustomExecMethods QueryPushdownCustomExecMethods =
{
	.CustomName = "QueryPushdownScan",
	.BeginCustomScan = QueryPushdownBeginScan,
	.ExecCustomScan = QueryPushdownExecScan,
	.EndCustomScan = QueryPushdownEndScan,
	.ReScanCustomScan = QueryPushdownReScan,
	.ExplainCustomScan = QueryPushdownExplainScan
};

typedef struct QueryPushdownScanState
{
	CustomScanState customScanState;

	/* query to execute */
	Query	   *query;
	List	   *restrictionsList;
	char	   *queryString;

	/* INSERT..SELECT into the following relation, or InvalidOid */
	Oid			insertIntoRelid;

	/* table properties */
	TupleDesc	tupleDesc;
	AttInMetadata *attributeInputMetadata;	/* attribute datatype conversion
											 * metadata */

	/* execution state */
	EState	   *estate;

	/* connection properties */
	PGDuckConnection *connection;
	char	  **receivedValues;

	ParamListInfo paramListInfo;
	int			numParams;
	const char **parameterValues;
}			QueryPushdownScanState;


/*
 * InitializeFullQueryPushdown sets up the global planner hook.
 */
void
InitializeFullQueryPushdown(void)
{
	RegisterCustomScanMethods(&QueryPushdownScanMethods);

	PreviousPlannerHook = planner_hook != NULL ? planner_hook : standard_planner;
	planner_hook = LakeTablePlanner;

	PrevRelPathlistHook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = PgLakeRecordPlannerRestrictions;

	PrevExplainOneQueryHook = ExplainOneQuery_hook != NULL ? ExplainOneQuery_hook : StandardExplainOneQuery;
	ExplainOneQuery_hook = PgLakeExplainHook;
}


/*
* AppendPermInfos adjusts the permInfos and rtables of the pushdown plan
* to include the permInfos and rtables of the local plan.
*
* This is necessary because the pushdown plan is created without any
* permInfos, and permInfos are essential for ensuring permission checks
* are enforced. We also have to rtables because permInfos reference
* rtables.
*
* By adding the permInfos and rtables of the local plan to the pushdown
* plan, we ensure that the permission checks are enforced correctly.
* Doing it on the pushdown plan is hard, because it is not easy to find
* which columns are used for a given RTE in the query, especially when
* subqueries/ctes involved. Postgres finds this information during
* parse analysis, and we can't easily replicate that in the pushdown.
*/
static PlannedStmt *
AppendPermInfos(PlannedStmt *pushdownPlan, PlannedStmt *localPlan)
{
	/*
	 * We create the pushdown plan, so we know that the list length is 0.
	 * However, we prefer to be more generic for future changes.
	 *
	 * The goal is to append the permInfos and rtables.
	 */
	int			pushdownPlanPermInfo = list_length(pushdownPlan->permInfos);

	ListCell   *lc;

	foreach(lc, localPlan->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->perminfoindex != 0)
		{
			rte->perminfoindex = rte->perminfoindex + pushdownPlanPermInfo;
		}
	}

	/* finally, concatenate perminfos as well */
	pushdownPlan->permInfos = list_concat(pushdownPlan->permInfos, localPlan->permInfos);
	pushdownPlan->rtable = list_concat(pushdownPlan->rtable, localPlan->rtable);

	return pushdownPlan;
}

/*
 * LakeTablePlanner overrides the regular planner with logic to (sometimes)
 * push down full SQL queries.
 */
static PlannedStmt *
LakeTablePlanner(Query *parse, const char *queryString,
				 int cursorOptions, ParamListInfo boundParams)
{
	Query	   *originalQuery = NULL;
	bool		hasLakeTable = false;

	if (IsExtensionCreated(PgLakeTable))
	{
		/*
		 * If GUC is set, we prevent queries, which contain catalog tables,
		 * from showing any object created by any pg_lake extensions. The flag
		 * is expected to be set only before postgres tests.
		 */
		HideObjectsCreatedByLakeFromCatalogTables((Node *) parse, NULL);

		AdjustParseTreeForPgLake((Node *) parse, NULL);
	}

	if (IsDBTApplicationName())
	{
		DbtApplicationQueryContext *dbtContext = palloc0(sizeof(DbtApplicationQueryContext));

		AddForeignTablesToRelkindInArrayFilter((Node *) parse, dbtContext);
	}

	if (EnableFullQueryPushdown)
	{
		hasLakeTable = HasLakeRTE((Node *) parse, NULL);
		if (hasLakeTable)
		{
			/* standard_planner will scribble on query tree */
			originalQuery = copyObject(parse);
		}
	}

	CreateAndPushRestrictionContext();

	PlannedStmt *plan = NULL;

	PG_TRY();
	{
		plan = PreviousPlannerHook(parse, queryString, cursorOptions, boundParams);
		if (EnableFullQueryPushdown &&
			hasLakeTable &&
			(cursorOptions & CURSOR_OPT_SCROLL) == 0)
		{
			bool		isPushdownableSelect = FullQueryIsPushdownable(originalQuery);
			bool		isPushdownableInsertSelect = false;

			if (!isPushdownableSelect)
				isPushdownableInsertSelect = IsPushdownableInsertSelectQuery(originalQuery);

			if (isPushdownableSelect || isPushdownableInsertSelect)
			{
				AddMissingRTEAliasaes((Node *) originalQuery, NULL);

				if (isPushdownableSelect)
				{
					PlannedStmt *pushdownPlan = GeneratePushdownPlan(plan, originalQuery);

					plan = AppendPermInfos(pushdownPlan, plan);
				}
				else
					plan = GenerateInsertSelectPushdownPlan(plan, originalQuery);

			}
		}
	}
	PG_FINALLY();
	{
		PopRestrictionContext();
	}
	PG_END_TRY();

	return plan;
}


/*
* For VALUES, subqueries and CTEs, Postgres auto-generates column names
* into rte-eref->colnames.
*
* For Postgres, auto-generated column names are "column1", "column2"
* etc. For DuckDB it is "col0", "col1" etc.
*
* So, here we explicit set the alias to match the eref such that the
* column names are explicitly deparsed, and we are good to pushdown
* queries.
*/
static bool
AddMissingRTEAliasaes(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 AddMissingRTEAliasaes,
								 context,
								 QTW_EXAMINE_RTES_BEFORE);
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->eref && rte->alias && rte->eref->colnames != NIL &&
			rte->alias->colnames == NIL)
		{
			rte->alias = copyObject(rte->eref);
		}

		/* query_tree_walker descends into RTEs */
		return false;
	}

	return expression_tree_walker(node,
								  AddMissingRTEAliasaes,
								  context);
}

/*
 * HasLakeRTE recursively walks the node tree to determine
 * whether there is a pg_lake_table RangeTblEntry.
 */
bool
HasLakeRTE(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 HasLakeRTE,
								 context,
								 QTW_EXAMINE_RTES_BEFORE);
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (IsAnyLakeForeignTable(rte))
			return true;

		/* query_tree_walker descends into RTEs */
		return false;
	}

	return expression_tree_walker(node,
								  HasLakeRTE,
								  context);
}


/*
 * AdjustParseTreeForPgLake recursively walks the node tree to replace
 * the OID of pg_table_size with lake_iceberg.table_size
 */
static bool
AdjustParseTreeForPgLake(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 AdjustParseTreeForPgLake,
								 context,
								 QTW_EXAMINE_RTES_BEFORE);
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_RELATION)
		{
			/*
			 * Generate the unique identifier and set it in the rte.
			 *
			 * Why we need this hack? PostgreSQL doesn't uniquely identify
			 * relations in the query. Especially with multiple levels of
			 * (sub)queries, it is hard to uniquely identify a relation, range
			 * table index (rti) is not enough. For PostgreSQL, that's not an
			 * issue, because it processes each sub-plan separately. But for
			 * us, we need to know which relation we are processing in the
			 * query as we do a single pass over the query, especially for the
			 * query-pushdown flow. Dechipering the query level and rti in the
			 * executor is not feasiable, so we bake this information into the
			 * rte right before the planning. Postgres carries this
			 * information until the query is executed, so we can use it in
			 * the executor.
			 *
			 * Citus uses a similar trick. It bakes this information to
			 * rte->values_lists, and we don't want to conflict with that.
			 * Instead, we use rte->functions.
			 */
			AssignUniqueRelationIdentifier(rte);
		}

		/* query_tree_walker descends into RTEs */
		return false;
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *funcExpr = (FuncExpr *) node;

		if (funcExpr->funcid == F_PG_TABLE_SIZE)
		{
			Oid			pgLakeTableSizeId = PgLakeTableSizeFunctionId();

			/* in case the extension was not upgraded */
			if (pgLakeTableSizeId != InvalidOid)
				funcExpr->funcid = pgLakeTableSizeId;
		}
	}

	return expression_tree_walker(node,
								  AdjustParseTreeForPgLake,
								  context);
}


/*
 * FullQueryIsPushdownable determines whether the whole query can be pushed
 * down to pgduck.
 */
bool
FullQueryIsPushdownable(Query *parse)
{
	if (parse->commandType != CMD_SELECT || parse->hasModifyingCTE ||
		parse->hasForUpdate)
	{
		/* cannot push down writes */
		return false;
	}

	if (HasNotShippableExpression((Node *) parse))
	{
		/* some expression cannot be pushed down */
		return false;
	}

	return true;
}


/*
* HasNotShippableExpression determines whether the given node contains
* any expression that cannot be pushed down to pgduck.
*/
bool
HasNotShippableExpression(Node *node)
{
	IsShippableContext context = {
		.srfAllowed = false,
		.hasUnnest = false,
		.stopAtFirstNotShippable = true,
		.notShippableObjects = NULL
	};

	return ProcessNotShippableExpressionWalker(node, &context);
}


/*
 * CollectNotShippableObjects collects all objects that are not shippable
 * in the given node.
 */
HTAB *
CollectNotShippableObjects(Node *node)
{
	IsShippableContext context = {
		.srfAllowed = false,
		.hasUnnest = false,
		.stopAtFirstNotShippable = false,
		.notShippableObjects = CreateNotShippableObjectsHash()
	};

	ProcessNotShippableExpressionWalker(node, &context);

	return context.notShippableObjects;
}


/*
 * TryRecordNotShippableObject records an object that is not shippable in the
 * given context if the context is configured to record such objects.
 */
static void
TryRecordNotShippableObject(IsShippableContext * context, Oid objectId,
							Oid classId, NotShippableReason reason)
{
	if (context->stopAtFirstNotShippable)
		return;

	RecordNotShippableObject(context, objectId, classId, reason);
}


/*
 * RecordNotShippableObject records an object that is not shippable in the
 * given context.
 */
static void
RecordNotShippableObject(IsShippableContext * context, Oid objectId,
						 Oid classId, NotShippableReason reason)
{
	Assert(context->notShippableObjects != NULL);

	NotShippableObject *notShippableObject = palloc(sizeof(NotShippableObject));

	notShippableObject->objectId = objectId;
	notShippableObject->classId = classId;
	notShippableObject->reason = reason;

	hash_search(context->notShippableObjects, notShippableObject, HASH_ENTER, NULL);
}


/*
 * CreateNotShippableObjectsHash creates a hash table to store not shippable
 * objects.
 */
static HTAB *
CreateNotShippableObjectsHash(void)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(NotShippableObject);
	hashCtl.entrysize = sizeof(NotShippableObject);
	hashCtl.hcxt = CurrentMemoryContext;

	return hash_create("Not Shippable Objects Hash",
					   16,
					   &hashCtl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}


/*
 * ProcessNotShippableExpressionWalker recursively walks the node tree to process
 * not shippable expressions.
 *
 * It continues collecting all not shippable objects when stopAtFirstNotShippable is false.
 * Otherwise, it stops at the first not shippable object and returns true.
 */
static bool
ProcessNotShippableExpressionWalker(Node *node, IsShippableContext * context)
{
	if (node == NULL)
	{
		return false;
	}

	bool		srfAllowed = context->srfAllowed;

	/*
	 * Set-returning functions are only allowed directly under a
	 * RangeTblFunction node, so we immediately set it back before entering
	 * any other walkers.
	 */
	context->srfAllowed = false;

	if (IsA(node, Query))
	{
		bool		prevHasUnnest = context->hasUnnest;
		Query	   *query = (Query *) node;

		if (query->hasForUpdate)
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_FOR_UPDATE);
		}

		/* DuckDB doesn't have this feature, so cannot pushdown */
		if (query->limitOption == LIMIT_OPTION_WITH_TIES)
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_LIMIT_WITH_TIES);
		}

		/* DuckDB doesn't support queries without SELECT list */
		if (query->targetList == NIL || TargetListHasOnlyResjunk(query->targetList))
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_EMPTY_TARGET_LIST);
		}

		if (query_tree_walker(query, ProcessNotShippableExpressionWalker, context, QTW_EXAMINE_RTES_BEFORE))
			return true;

		/*
		 * DuckDB supports unnest in most places, but not GROUP BY or window
		 * functions
		 */
		if (context->hasUnnest &&
			(HasGroupByWithUnnest(query) || HasWindowFunctionWithUnnest(query)))
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_UNNEST_GROUP_BY_OR_WINDOW);
		}

		context->hasUnnest = prevHasUnnest;

		return false;
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		/*
		 * Named tuple stores are not pushdownable as those are internal to
		 * PostgreSQL and not supported by DuckDB. Table functions are not
		 * pushdownable as some of those are not implemented in DuckDB (e.g.,
		 * XML Table).
		 */
		if (rte->rtekind == RTE_NAMEDTUPLESTORE || rte->rtekind == RTE_TABLEFUNC)
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_NAMEDTUPLESTORE);
		}

		/*
		 * Due to
		 * https://github.com/CrunchyData/crunchy_data_warehouse/issues/312 we
		 * cannot push down queries with joinmergedcols > 0 with an alias, as
		 * DuckDB doesn't seem to handle the alias properly in such cases.
		 */
		if (rte->rtekind == RTE_JOIN && rte->joinmergedcols > 0 && rte->alias != NULL)
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_JOIN_MERGED_COLUMNS_ALIAS);
		}

		/* we only support pg_lake relations for the moment */
		if (rte->rtekind == RTE_RELATION && !IsAnyLakeForeignTable(rte))
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, rte->relid, RelationRelationId, NOT_SHIPPABLE_TABLE);
		}

		if (rte->rtekind == RTE_RELATION && has_subclass(rte->relid) &&
			!AllInheritorsAreLakeTable(rte->relid))
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, rte->relid, RelationRelationId, NOT_SHIPPABLE_TABLE);
		}

		if (rte->rtekind == RTE_FUNCTION && list_length(rte->functions) != 1)
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_MULTIPLE_FUNCTION_TABLE);
		}

		if (rte->funcordinality)
		{
			if (context->stopAtFirstNotShippable)
				return true;

			RecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_WITH_ORDINALITY);
		}

		/* query_tree_walker descends into RTEs */
		return false;
	}
	else if (IsA(node, RangeTblFunction))
	{
		/* DuckDB only supports set-returning functions in FROM */
		context->srfAllowed = true;

		/*
		 * The RangeTblFunction itself does not have any properties of
		 * interest, so we recurse immediately. The srfAllowed status is
		 * immediately reset.
		 */
		return expression_tree_walker(node,
									  ProcessNotShippableExpressionWalker,
									  context);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *funcExpr = (FuncExpr *) node;

		/*
		 * unnest cannot appear in the GROUP BY in DuckDB. We check that at
		 * end of the current Query.
		 */
		if (funcExpr->funcid == F_UNNEST_ANYARRAY)
			context->hasUnnest = true;
	}

	if (ExpressionHasNonShippableObject(node, srfAllowed, context))
	{
		if (context->stopAtFirstNotShippable)
			return true;
	}

	if (ExpressionHasCollation(node, context))
	{
		if (context->stopAtFirstNotShippable)
			return true;
	}

	if (ExpressionReturnsNonShippableType(node, context))
	{
		if (context->stopAtFirstNotShippable)
			return true;
	}

	return expression_tree_walker(node,
								  ProcessNotShippableExpressionWalker,
								  context);
}


/*
* ExpressionHasNonShippableObject checks the node for non-shippable
* functions, operators, and aggregates.
* Note that this function does not recurse into sub-expressions.
*/
static bool
ExpressionHasNonShippableObject(Node *node, bool srfAllowed, IsShippableContext * context)
{
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *funcExpr = (FuncExpr *) node;

		/*
		 * Set-returning functions are always not shippable outside of FROM,
		 * though unnest has special handling.
		 */
		if (funcExpr->funcretset && !srfAllowed && funcExpr->funcid != F_UNNEST_ANYARRAY)
		{
			TryRecordNotShippableObject(context, funcExpr->funcid, ProcedureRelationId, NOT_SHIPPABLE_FUNCTION);
			return true;
		}

		if (!is_shippable(funcExpr->funcid, ProcedureRelationId, node))
		{
			TryRecordNotShippableObject(context, funcExpr->funcid, ProcedureRelationId, NOT_SHIPPABLE_FUNCTION);
			return true;
		}
	}
	else if (IsA(node, OpExpr) || IsA(node, DistinctExpr))
	{
		OpExpr	   *opExpr = (OpExpr *) node;

		if (!is_shippable(opExpr->opno, OperatorRelationId, node))
		{
			TryRecordNotShippableObject(context, opExpr->opno, OperatorRelationId, NOT_SHIPPABLE_OPERATOR);
			return true;
		}
	}
	else if (IsA(node, NullIfExpr))
	{
		NullIfExpr *nullIfExpr = (NullIfExpr *) node;

		if (!is_shippable(nullIfExpr->opno, OperatorRelationId, node))
		{
			TryRecordNotShippableObject(context, nullIfExpr->opno, OperatorRelationId, NOT_SHIPPABLE_OPERATOR);
			return true;
		}
	}
	else if (IsA(node, RowCompareExpr))
	{
		RowCompareExpr *rcExpr = (RowCompareExpr *) node;
		ListCell   *opidCell;

		foreach(opidCell, rcExpr->opnos)
		{
			Oid			opId = lfirst_oid(opidCell);

			if (!is_shippable(opId, OperatorRelationId, node))
			{
				TryRecordNotShippableObject(context, opId, OperatorRelationId, NOT_SHIPPABLE_OPERATOR);
				return true;
			}
		}
	}
	else if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *arrayOpExpr = (ScalarArrayOpExpr *) node;

		if (!is_shippable(arrayOpExpr->opno, OperatorRelationId, node))
		{
			TryRecordNotShippableObject(context, arrayOpExpr->opno, OperatorRelationId, NOT_SHIPPABLE_OPERATOR);
			return true;
		}
	}
	else if (IsA(node, Aggref))
	{
		Aggref	   *agg = (Aggref *) node;

		if (!is_shippable(agg->aggfnoid, ProcedureRelationId, node))
		{
			TryRecordNotShippableObject(context, agg->aggfnoid, ProcedureRelationId, NOT_SHIPPABLE_FUNCTION);
			return true;
		}

		/*
		 * For aggorder elements, check whether the sort operator, if
		 * specified, is shippable or not.
		 */
		if (agg->aggorder)
		{
			ListCell   *lc = NULL;

			foreach(lc, agg->aggorder)
			{
				SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
				Oid			sortcoltype;
				TypeCacheEntry *typentry;
				TargetEntry *tle;

				tle = get_sortgroupref_tle(srt->tleSortGroupRef,
										   agg->args);
				sortcoltype = exprType((Node *) tle->expr);
				typentry = lookup_type_cache(sortcoltype,
											 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
				/* Check shippability of non-default sort operator. */
				if (srt->sortop != typentry->lt_opr &&
					srt->sortop != typentry->gt_opr &&
					!is_shippable(srt->sortop, OperatorRelationId, node))
				{
					TryRecordNotShippableObject(context, srt->sortop, OperatorRelationId, NOT_SHIPPABLE_OPERATOR);
					return true;
				}
			}
		}
	}
	else if (IsA(node, WindowFunc))
	{
		WindowFunc *expr = (WindowFunc *) node;

		if (!is_shippable(expr->winfnoid, ProcedureRelationId, node))
		{
			TryRecordNotShippableObject(context, expr->winfnoid, ProcedureRelationId, NOT_SHIPPABLE_FUNCTION);
			return true;
		}
	}
	else if (IsA(node, CoerceViaIO))
	{
		CoerceViaIO *expr = (CoerceViaIO *) node;

		/*
		 * We already keep track of the shippability of types. As long as both
		 * the input and the output types are shippable, we are good. In other
		 * words, here we rely on the shippability of the type, as we have not
		 * allowed all the underlying functions to be shippable (e.g., casting
		 * "int" to "text" requires textin() function).
		 */
		if (!is_shippable(expr->resulttype, TypeRelationId, node))
		{
			TryRecordNotShippableObject(context, expr->resulttype, TypeRelationId, NOT_SHIPPABLE_TYPE);
			return true;
		}

		if (!is_shippable(exprType((Node *) expr->arg), TypeRelationId, node))
		{
			TryRecordNotShippableObject(context, exprType((Node *) expr->arg), TypeRelationId, NOT_SHIPPABLE_TYPE);
			return true;
		}
	}
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/*
		 * System columns are not supported in DuckDB, we fall back to FDW.
		 */
		if (var->varattno <= 0)
		{
			TryRecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SYSTEM_COLUMN);
			return true;
		}
	}

	return false;
}


/*
* UseCollationIfNonDefault returns the collation if it is not the default
* collation, otherwise it returns InvalidOid.
*/
static Oid
UseCollationIfNonDefault(Oid nodeCollation)
{
	if (nodeCollation != InvalidOid &&
		!(nodeCollation == DEFAULT_COLLATION_OID || nodeCollation == C_COLLATION_OID))
	{
		return nodeCollation;
	}
	return InvalidOid;
}


/*
 * TargetListHasOnlyResjunk determines whether there the target list
 * only has resjunk.
 */
static bool
TargetListHasOnlyResjunk(List *targetList)
{
	ListCell   *targetCell = NULL;

	foreach(targetCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetCell);

		if (!targetEntry->resjunk)
			return false;
	}

	return true;
}


/*
 * AllInheritorsAreLakeTable returns whether all inheritors of the given
 * relation are pg_lake (or pg_lake_iceberg) tables.
 */
static bool
AllInheritorsAreLakeTable(Oid relationId)
{
	List	   *inheritors = find_all_inheritors(relationId, NoLock, NULL);

	foreach_oid(inheritorId, inheritors)
	{
		if (!IsAnyLakeForeignTableById(inheritorId))
			return false;
	}

	return true;
}


/*
* ExpressionHasCollation returns whether the given expression
* has a collation. This check is inline with the foreign_expr_walker().
*/
static bool
ExpressionHasCollation(Node *node, IsShippableContext * context)
{
	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_Param:
		case T_Aggref:
		case T_GroupingFunc:
		case T_WindowFunc:
		case T_SubscriptingRef:
		case T_FuncExpr:
		case T_NamedArgExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_ScalarArrayOpExpr:
		case T_BoolExpr:
		case T_SubLink:
		case T_SubPlan:
		case T_AlternativeSubPlan:
		case T_FieldSelect:
		case T_FieldStore:
		case T_RelabelType:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ConvertRowtypeExpr:
		case T_CollateExpr:
		case T_CaseExpr:
		case T_CaseTestExpr:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_RowCompareExpr:
		case T_CoalesceExpr:
		case T_MinMaxExpr:
		case T_XmlExpr:
		case T_JsonValueExpr:
		case T_JsonConstructorExpr:
		case T_JsonIsPredicate:
		case T_NullTest:
		case T_BooleanTest:
		case T_CoerceToDomain:
		case T_CoerceToDomainValue:
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_NextValueExpr:
		case T_InferenceElem:
		case T_PlaceHolderVar:
			{
				Oid			collation = UseCollationIfNonDefault(exprCollation(node));

				if (collation != InvalidOid &&
					!(collation == DEFAULT_COLLATION_OID || collation == C_COLLATION_OID))
				{
					TryRecordNotShippableObject(context, collation, CollationRelationId, NOT_SHIPPABLE_COLLATION);
					return true;
				}

				break;
			}
		default:
			/* not a collatable node */
			break;
	}

	return false;
}


/*
 * ExpressionReturnsNonShippableType returns whether the given expression
 * returns a shippable type, or false if the node is not an expression.
 */
static bool
ExpressionReturnsNonShippableType(Node *node, IsShippableContext * context)
{
	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_Param:
		case T_Aggref:
		case T_GroupingFunc:
		case T_WindowFunc:
		case T_SubscriptingRef:
		case T_FuncExpr:
		case T_NamedArgExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_ScalarArrayOpExpr:
		case T_BoolExpr:
		case T_SubLink:
		case T_SubPlan:
		case T_AlternativeSubPlan:
		case T_FieldSelect:
		case T_FieldStore:
		case T_RelabelType:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ConvertRowtypeExpr:
		case T_CollateExpr:
		case T_CaseExpr:
		case T_CaseTestExpr:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_RowCompareExpr:
		case T_CoalesceExpr:
		case T_MinMaxExpr:
		case T_XmlExpr:
		case T_JsonValueExpr:
		case T_JsonConstructorExpr:
		case T_JsonIsPredicate:
		case T_NullTest:
		case T_BooleanTest:
		case T_CoerceToDomain:
		case T_CoerceToDomainValue:
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_NextValueExpr:
		case T_InferenceElem:
		case T_PlaceHolderVar:
			{
				Oid			typeId = exprType(node);

				/*
				 * The geography type is not shippable, but we allow
				 * geography(geometry) casts to go through to the rewrite
				 * phase where it gets removed. Hence, we can consider it
				 * shippable if the node is a FuncExpr, which has already been
				 * checked separately by ExpressionHasNonShippableObject.
				 */
				if (IsA(node, FuncExpr) && IsGeographyTypeId(typeId))
					return false;

				/*
				 * Our auto-generated map type names are not known by DuckDB
				 * so we cannot cast to/from them.
				 */
				if (IsA(node, RelabelType))
				{
					RelabelType *relabelType = (RelabelType *) node;

					if (IsMapTypeOid(typeId) ||
						IsMapTypeOid(exprType((Node *) relabelType->arg)))
					{
						TryRecordNotShippableObject(context, typeId, TypeRelationId, NOT_SHIPPABLE_TYPE);
						return true;
					}
				}

				/*
				 * Some contexts with user-defined types are not shippable,
				 * but others are, so check those cases here.
				 */

				if (is_non_shippable_udt_context(node))
				{
					TryRecordNotShippableObject(context, typeId, TypeRelationId, NOT_SHIPPABLE_TYPE);
					return true;
				}

				if (!is_shippable(typeId, TypeRelationId, node))
				{
					TryRecordNotShippableObject(context, typeId, TypeRelationId, NOT_SHIPPABLE_TYPE);
					return true;
				}

				return false;
			}
		case T_SQLValueFunction:
			{
				/*
				 * Specific time-based SQLValueFunction ops are shippable,
				 * which will be evaluated as the current time but casted to
				 * the appropriate type.
				 */

				if (!IsShippableSQLValueFunction(((SQLValueFunction *) node)->op))
				{
					TryRecordNotShippableObject(context, InvalidOid, InvalidOid, NOT_SHIPPABLE_SQL_VALUE_FUNCTION);
					return true;
				}

				return false;
			}
		default:
			/* not an expression */
			return false;
	}
}


/*
 * GenerateInsertSelectPushdownPlan generates a PlannedStmt for a top-level
 * insert..select query.
 */
static PlannedStmt *
GenerateInsertSelectPushdownPlan(PlannedStmt *localPlan, Query *query)
{
	CustomScan *customScan = GeneratePushdownScan(query);

	customScan->custom_scan_tlist =
		CreateInternalCustomScanTargetList(localPlan->planTree->targetlist);
	customScan->scan.plan.targetlist =
		CreateCustomScanOutputTargetList(customScan->custom_scan_tlist);

	localPlan->planTree = (Plan *) customScan;

	return localPlan;
}


/*
 * GeneratePushdownPlan generates a PlannedStmt for a top-level query.
 */
static PlannedStmt *
GeneratePushdownPlan(PlannedStmt *localPlan, Query *originalQuery)
{
	CustomScan *customScan = GeneratePushdownScan(originalQuery);

	customScan->custom_scan_tlist =
		CreateInternalCustomScanTargetList(localPlan->planTree->targetlist);
	customScan->scan.plan.targetlist =
		CreateCustomScanOutputTargetList(customScan->custom_scan_tlist);

	List	   *columnNameList = NIL;

	ListCell   *targetEntryCell = NULL;

	foreach(targetEntryCell, customScan->scan.plan.targetlist)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		String	   *columnName = makeString(targetEntry->resname);

		columnNameList = lappend(columnNameList, columnName);
	}

	RangeTblEntry *resultRte = CreateResultRTE(columnNameList);

	PlannedStmt *pushdownPlan = makeNode(PlannedStmt);

	pushdownPlan->planTree = (Plan *) customScan;
	pushdownPlan->canSetTag = true;
	pushdownPlan->queryId = localPlan->queryId;
	pushdownPlan->commandType = localPlan->commandType;
	pushdownPlan->rtable = list_make1(resultRte);

	return pushdownPlan;
}


/*
 * GenerateCustomScan generates a CustomScan that pushes down the given
 * query tree
 */
static CustomScan *
GeneratePushdownScan(Query *originalQuery)
{
	/*
	 * Rewrite any unsupported expression to one that DuckDB understands.
	 */
	Query	   *rewrittenQuery = RewriteQueryTreeForPGDuck(originalQuery);
	List	   *restrictionList = GetCurrentRelationRestrictions();

	CustomScan *customScan = makeNode(CustomScan);

	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN | CUSTOMPATH_SUPPORT_PROJECTION;
	customScan->methods = &QueryPushdownScanMethods;

	Oid			insertIntoRelid =
		IsPushdownableInsertSelectQuery(originalQuery) ?
		GetInsertRelidFromInsertSelect(originalQuery) : InvalidOid;

	customScan->custom_private =
		list_make3(rewrittenQuery, restrictionList, list_make1_oid(insertIntoRelid));

	return customScan;
}


/*
 * CreateResultRTE creates a RangeTblEntry for a column list.
 */
static RangeTblEntry *
CreateResultRTE(List *columnNameList)
{
	RangeTblEntry *resultRte = makeNode(RangeTblEntry);

	resultRte->rtekind = RTE_RESULT;
	resultRte->eref = makeAlias("pushdown_query", columnNameList);
	resultRte->inh = false;
	resultRte->inFromCl = true;

	return resultRte;
}


/*
 * QueryPushdownCreateScanState returns the scan state for a CustomScan.
 */
static Node *
QueryPushdownCreateScanState(CustomScan *scan)
{
	QueryPushdownScanState *scanState = palloc0(sizeof(QueryPushdownScanState));

	scanState->customScanState = *makeNode(CustomScanState);
	scanState->customScanState.methods = &QueryPushdownCustomExecMethods;
	scanState->customScanState.slotOps = &TTSOpsHeapTuple;
	scanState->query = (Query *) linitial(scan->custom_private);
	scanState->restrictionsList = (List *) lsecond(scan->custom_private);

	List	   *insertIntoRelidList = lthird(scan->custom_private);

	scanState->insertIntoRelid = linitial_oid(insertIntoRelidList);

	return (Node *) scanState;
}


/*
 * QueryPushdownBeginScan initializes a pushdown scan by preparing the query string
 * and opening a connection.
 */
static void
QueryPushdownBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	QueryPushdownScanState *scanState = (QueryPushdownScanState *) node;
	List	   *relationRestrictionsList = scanState->restrictionsList;

	/* copyParamList forces to fetch dynamic parameters */
	ParamListInfo paramListInfo = copyParamList(estate->es_param_list_info);

	/* in some scenarios, we modify the query */
	Query	   *scanQuery = copyObject(scanState->query);

	/*
	 * remove unused parameters from the list, and renumber Params in scan
	 * query
	 */
	if (paramListInfo != NULL && paramListInfo->numParams != 0)
		RemoveUnusedParametersFromList(scanQuery, paramListInfo);

	/*
	 * For INSERT..SELECT, we transform the query into a SELECT whose results
	 * will be written to the table. We remember that we want to insert by
	 * setting insertIntoRelid.
	 */
	if (scanState->insertIntoRelid != InvalidOid)
	{
		/*
		 * INSERT .. SELECT pushdown is replaced with a COPY (SELECT ..) TO
		 * ..; query. Given COPY doesn't support parameters, we have to
		 * replace the parameters with constants.
		 *
		 * Also, this happens after the query is rewritten, so we need to
		 * explicitly replace the external parameters with constants.
		 */
		scanQuery =
			(Query *) ReplaceExternalParamsWithPgDuckConsts((Node *) scanQuery, paramListInfo);

		TransformPushdownableInsertSelect(scanQuery);
	}

	/*
	 * Override all the RTE_RELATIONs that are pg_lake tables with
	 * read_table('table_name') function calls.
	 */
	List	   *rteList =
		ReplacePgLakeTableWithReadTableFunc((Node *) scanQuery);

	/* if there are child tables, include them in the snapshot */
	bool		includeChildren = true;

	PgLakeScanSnapshot *snapshot =
		CreatePgLakeScanSnapshot(rteList, relationRestrictionsList, paramListInfo,
								 includeChildren, InvalidOid);

	/*
	 * Deparse the query and replace read_table('table_name') with the
	 * appropriate file scan functions.
	 */
	char	   *pgDuckSQLTemplate = PreparePGDuckSQLTemplate(scanQuery);

	bool		explainRequested = eflags & EXEC_FLAG_EXPLAIN_ONLY;
	char	   *queryString = ReplaceReadTableFunctionCalls(pgDuckSQLTemplate,
															snapshot,
															explainRequested);

	TupleDesc	tupleDesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	scanState->queryString = queryString;
	scanState->connection = GetPGDuckConnection();
	scanState->tupleDesc = tupleDesc;
	scanState->attributeInputMetadata = TupleDescGetAttInMetadata(tupleDesc);
	scanState->estate = estate;
	scanState->receivedValues = (char **) palloc0(tupleDesc->natts * sizeof(char *));
	scanState->paramListInfo = paramListInfo;
	scanState->numParams = 0;
	scanState->parameterValues = 0;

	if (paramListInfo != NULL)
	{
		scanState->numParams = paramListInfo->numParams;
		scanState->parameterValues = palloc0(scanState->numParams * sizeof(char *));

		for (int parameterIndex = 0; parameterIndex < scanState->numParams; parameterIndex++)
		{
			ParamExternData *parameterData = &paramListInfo->params[parameterIndex];

			if (parameterData->isnull)
				scanState->parameterValues[parameterIndex] = NULL;
			else
			{
				FmgrInfo	fmgrInfo;
				Oid			typeOutputFunctionId = InvalidOid;
				bool		variableLengthType = false;

				getTypeOutputInfo(parameterData->ptype, &typeOutputFunctionId,
								  &variableLengthType);

				fmgr_info(typeOutputFunctionId, &fmgrInfo);
				scanState->parameterValues[parameterIndex] =
					PGDuckSerialize(&fmgrInfo, parameterData->ptype, parameterData->value);
			}
		}
	}

	if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY) && scanState->insertIntoRelid == InvalidOid)
	{
		/* if sending fails, throws error */
		SendQueryWithParams(scanState->connection,
							scanState->queryString,
							scanState->numParams,
							scanState->parameterValues);
	}
}


/*
 * QueryPushdownExeScan returns a tuple from the remote node.
 */
static TupleTableSlot *
QueryPushdownExecScan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) QueryPushdownScanNext,
					(ExecScanRecheckMtd) QueryPushdownScanRecheck);
}


/*
 * QueryPushdownScanRecheck is called when additional filtering on the CustomScan
 * output needed. Currently always true, since all filters are pushed down.
 */
static bool
QueryPushdownScanRecheck(CustomScanState *node, TupleTableSlot *slot)
{
	return true;
}


/*
 * QueryPushdownScanNext switches to per-tuple memory context
 * and retrieves a tuple from pgduck.
 */
static TupleTableSlot *
QueryPushdownScanNext(CustomScanState *node)
{
	TupleTableSlot *slot;

	/*
	 * We use this context while converting each row fetched from remote node
	 * into tuple/slot. We do this because the context is reset on every row
	 * by Postgres so we don't need to care about the allocations in
	 * QueryPushdownScanNextInternal().
	 */
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext oldContext =
		MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	slot = QueryPushdownScanNextInternal(node);

	MemoryContextSwitchTo(oldContext);

	return slot;
}

/*
 * QueryPushdownScanNextInternal retrieves a tuple from pgduck.
 */
static TupleTableSlot *
QueryPushdownScanNextInternal(CustomScanState *node)
{
	QueryPushdownScanState *scanState = (QueryPushdownScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupleDesc = scanState->tupleDesc;
	AttInMetadata *attributeInputMetadata = scanState->attributeInputMetadata;

	if (scanState->insertIntoRelid != InvalidOid)
	{
		/*
		 * append the query result to the table and set the number of
		 * processed rows
		 */
		scanState->estate->es_processed =
			AddQueryResultToTable(scanState->insertIntoRelid, scanState->queryString, tupleDesc);

		return NULL;
	}

	PGresult   *result = WaitForResult(scanState->connection);

	if (result == NULL)
	{
		return NULL;
	}

	/* one way of SingleRowMode to indicate we are at the end */
	if (PQresultStatus(result) == PGRES_TUPLES_OK && PQntuples(result) == 0)
	{
		PQclear(result);
		return NULL;
	}
	else if (PQresultStatus(result) != PGRES_SINGLE_TUPLE)
	{
		CheckPGDuckResult(scanState->connection, result);
	}

	PG_TRY();
	{
		/* we should be in single-row mode */
		Assert(PQntuples(result) <= 1);

		int			rowIndex = 0;
		int			columnCount = PQnfields(result);
		int			expectedColumnCount = tupleDesc->natts;
		char	  **receivedValues = scanState->receivedValues;

		if (columnCount != expectedColumnCount)
		{
			ereport(ERROR, (errmsg("unexpected number of columns returned")));
		}

		for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			if (PQgetisnull(result, rowIndex, columnIndex))
			{
				receivedValues[columnIndex] = NULL;
			}
			else
			{
				char	   *value = PQgetvalue(result, rowIndex, columnIndex);

				if (PQfformat(result, columnIndex) == 1)
				{
					ereport(ERROR, (errmsg("unexpected binary result")));
				}
				receivedValues[columnIndex] = value;
			}
		}

		/* construct the tuple from the input strings */
		HeapTuple	heapTuple = BuildTupleFromCStrings(attributeInputMetadata,
													   receivedValues);

		/*
		 * Store the tuple in the tuple table slot. We pass shouldFree=false
		 * because it is allocated in per-row context (ecxt_per_tuple_memory)
		 * and will be freed when the context is reset by Postgres.
		 */
		ExecStoreHeapTuple(heapTuple, slot, false);

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return slot;
}


/*
 * QueryPushdownReScan starts a rescan.
 */
static void
QueryPushdownReScan(CustomScanState *node)
{
	if (node->ss.ps.ps_ResultTupleSlot)
	{
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	}

	ExecScanReScan(&node->ss);
}


/*
 * QueryPushdownEndScan finalizes the pushdown scan.
 */
static void
QueryPushdownEndScan(CustomScanState *node)
{
	QueryPushdownScanState *scanState = (QueryPushdownScanState *) node;

	ReleasePGDuckConnection(scanState->connection);
	scanState->connection = NULL;
}


/*
 * QueryPushdownExplainScan produces explain output for a pushdown scan.
 */
static void
QueryPushdownExplainScan(CustomScanState *node, List *ancestors,
						 struct ExplainState *es)
{
	QueryPushdownScanState *scanState = (QueryPushdownScanState *) node;
	Query	   *scanQuery = copyObject(scanState->query);
	List	   *relationRestrictionsList = copyObject(scanState->restrictionsList);
	ParamListInfo paramListInfo = scanState->paramListInfo;

	if (scanState->insertIntoRelid != InvalidOid)
	{
		TransformPushdownableInsertSelect(scanQuery);
	}

	/*
	 * Override all the RTE_RELATIONs that are pg_lake tables with
	 * read_table('table_name') function calls.
	 */
	List	   *rteList =
		ReplacePgLakeTableWithReadTableFunc((Node *) scanQuery);

	/* if there are child tables, include them in the snapshot */
	bool		includeChildren = true;

	PgLakeScanSnapshot *snapshot =
		CreatePgLakeScanSnapshot(rteList, relationRestrictionsList, paramListInfo,
								 includeChildren, InvalidOid);

	/*
	 * Deparse the query and replace read_table('table_name') with the
	 * appropriate file scan functions.
	 */
	char	   *pgDuckSQLTemplate = PreparePGDuckSQLTemplate(scanQuery);

	bool		explainRequested = true;
	char	   *queryString = ReplaceReadTableFunctionCalls(pgDuckSQLTemplate,
															snapshot,
															explainRequested);

	ExplainPropertyText("Engine", "DuckDB", es);

	if (es->verbose)
	{
		int			dataFileScans = 0;
		int			deleteFileScans = 0;

		SnapshotFilesScanned(snapshot, &dataFileScans, &deleteFileScans);

		char	   *dataFileScansString = psprintf("%d", dataFileScans);
		char	   *deleteFileScansString = psprintf("%d", deleteFileScans);

		ExplainPropertyText("Data Files Scanned", dataFileScansString, es);
		ExplainPropertyText("Deletion Files Scanned", deleteFileScansString, es);

		ExplainPropertyText("Vectorized SQL", queryString, es);
	}
	char	   *realQuery = ReplaceReadTableFunctionCalls(pgDuckSQLTemplate,
														  snapshot, false);

	if (scanState->insertIntoRelid != InvalidOid)
	{
		char	   *insertTableName = get_rel_name(scanState->insertIntoRelid);

		ExplainPropertyText("INSERT INTO", insertTableName, es);
	}


	ExplainPGDuckQuery(realQuery, scanState->numParams, scanState->parameterValues, es);
}


/*
 * RemoveUnusedParametersFromList removes unused parameters from the given
 * paramList.
 */
static void
RemoveUnusedParametersFromList(Query *query, ParamListInfo paramList)
{
	List	   *allParams = NIL;

	FindAllParams((Node *) query, &allParams);

	List	  **paramsById = palloc0(sizeof(List *) * paramList->numParams);
	ListCell   *paramCell = NULL;

	/*
	 * Group all the parameters by their ID. The same parameter ID can be used
	 * multiple times in the query.
	 */
	foreach(paramCell, allParams)
	{
		Param	   *param = (Param *) lfirst(paramCell);

		if (param->paramid < 1 || param->paramid > paramList->numParams)
			elog(ERROR, "parameter out of bounds: %d", param->paramid);

		paramsById[param->paramid - 1] = lappend(paramsById[param->paramid - 1], param);
	}

	/*
	 * Remove unused parameters and adjust the numbering of Param nodes.
	 */
	int			newParamCount = 0;

	for (int paramIndex = 0; paramIndex < paramList->numParams; paramIndex++)
	{
		ParamExternData *paramValue = &paramList->params[paramIndex];

		/*
		 * If the parameter is unused, move on to the next one. newParamCount
		 * stays the same.
		 */
		if (paramsById[paramIndex] == NIL)
			continue;

		if (newParamCount != paramIndex)
		{
			/*
			 * There was an unused parameter, so move this parameter to its
			 * new position.
			 */
			paramList->params[newParamCount] = *paramValue;

			/*
			 * and renumber the Param nodes that reference it.
			 */
			foreach(paramCell, paramsById[paramIndex])
			{
				Param	   *param = (Param *) lfirst(paramCell);

				param->paramid = newParamCount + 1;
			}
		}

		newParamCount++;
	}

	paramList->numParams = newParamCount;
}


/*
 * FindAllParams walks through the query tree and finds all Param
 * nodes.
 */
static bool
FindAllParams(Node *node, List **allParams)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 FindAllParams,
								 allParams,
								 0);
	}
	else if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		*allParams = lappend(*allParams, param);
	}

	return expression_tree_walker(node,
								  FindAllParams,
								  allParams);
}


/*
 * CreateInternalCustomScanTargetList creates the target list for tuples
 * generated by the CustomScan / returned by pgduck. We derive it from the
 * target list of PostgreSQL's plan.
 */
static List *
CreateInternalCustomScanTargetList(List *existingTargetlist)
{
	List	   *customScanTargetList = NIL;

	ListCell   *targetEntryCell = NULL;

	foreach(targetEntryCell, existingTargetlist)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		if (targetEntry->resjunk)
			continue;

		/* build target entry pointing to the single result RTE */
		Var		   *var = makeVarFromTargetEntry(1, targetEntry);

		/*
		 * Given that we cannot pushdown RECORD types, we don't need to bless
		 * them. If we pushdown someday, we need to consider this comment.
		 */
		TargetEntry *newTargetEntry = flatCopyTargetEntry(targetEntry);

		newTargetEntry->expr = (Expr *) var;

		customScanTargetList = lappend(customScanTargetList, newTargetEntry);
	}

	return customScanTargetList;
}


/*
 * CreateCustomScanOutputTargetList creates a target list in which all
 * entries are Vars with INDEX_VAR that point into a custom_scan_tlist.
 */
static List *
CreateCustomScanOutputTargetList(List *customScanTargetList)
{
	List	   *scanTargetList = NIL;
	int			resno = 1;

	ListCell   *targetEntryCell = NULL;

	foreach(targetEntryCell, customScanTargetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		/* use INDEX_VAR to point to CustomScan tuple (see primnodes.h) */
		Var		   *var = makeVarFromTargetEntry(INDEX_VAR, targetEntry);

		TargetEntry *newTargetEntry = makeTargetEntry((Expr *) var, resno,
													  targetEntry->resname,
													  targetEntry->resjunk);

		scanTargetList = lappend(scanTargetList, newTargetEntry);

		resno++;
	}

	return scanTargetList;
}

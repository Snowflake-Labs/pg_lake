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

/*-------------------------------------------------------------------------
 *
 * rewrite_or_join.c
 *		Rewrite an OR of join conditions into a UNION ALL of joins, which
 *		DuckDB can execute as hash joins.
 *
 * DuckDB plans "a JOIN b ON a.x = b.k OR a.y = b.k" as a BLOCKWISE_NL_JOIN,
 * which compares every pair of rows. Each arm of the OR on its own is a hash
 * join, so we run the FROM and WHERE clause once per arm and combine the rows
 * with UNION ALL:
 *
 *   SELECT ... FROM (
 *     SELECT a.x, ... FROM a JOIN b ON a.x = b.k
 *     UNION ALL
 *     SELECT a.x, ... FROM a JOIN b ON a.y = b.k AND (a.x = b.k) IS NOT TRUE
 *   ) pg_lake_or_union(c1, ...)
 *
 * A pair of rows that matches more than one arm would be emitted once per
 * arm, so the branch for arm N also requires that none of the arms before it
 * matched. Each pair then comes out of exactly one branch, for the first arm
 * that matches, which is exactly when the original OR is true.
 *
 * The rest of the query (target list, GROUP BY, HAVING, ...) stays where it
 * was and reads the columns it needs from the UNION ALL.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "pg_lake/pgduck/rewrite_or_join.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"

/* alias of the UNION ALL subquery that replaces the FROM clause */
#define OR_JOIN_UNION_ALIAS "pg_lake_or_union"

/*
 * Every arm of an OR runs the FROM clause once more, and an OR within one of
 * the branches multiplies that again, so we cap the number of branches per
 * query.
 */
#define MAX_OR_JOIN_BRANCHES 16

/* pg_lake_engine.enable_or_join_rewrite setting */
bool		EnableOrJoinRewrite = true;

/*
 * OrJoinCondition describes where we found the OR clause that we split.
 */
typedef struct OrJoinCondition
{
	/* range table index of the join with the OR in its ON clause, or 0 */
	int			joinRtIndex;

	/* position of the OR among the AND-ed conditions of that clause */
	int			qualIndex;

	/* the OR clause itself */
	BoolExpr   *orClause;
}			OrJoinCondition;

/*
 * OuterVarsContext is used to replace the Vars that the rest of the query
 * uses from the FROM clause with Vars of the UNION ALL subquery.
 */
typedef struct OuterVarsContext
{
	/* query level of the Vars we replace, relative to the current node */
	int			sublevelsUp;

	/* range table index of the RTE_GROUP that stays behind, or 0 */
	int			groupRtIndex;

	/* Vars of the FROM clause in the order of the subquery columns */
	List	   *fromVars;

	/* whether we found something that we cannot expose as a column */
	bool		failed;
}			OuterVarsContext;

static void RewriteOrJoinConditionsWithBudget(Query *query, int maxBranches);
static bool FindOrJoinCondition(Query *query, OrJoinCondition * condition);
static bool FindOrJoinConditionInJoinTree(Node *jtnode, List *joinedRelids,
										  OrJoinCondition * condition);
static bool FindOrClause(Node *quals, int joinRtIndex, List *joinedRelids,
						 OrJoinCondition * condition);
static List *CollectJoinedRelids(Node *jtnode, List *joinedRelids);
static List *AddJoinedRelids(Node *quals, List *joinedRelids);
static List *MergeOverlappingRelids(List *joinedRelids);
static bool IsAlreadyJoined(Node *clause, List *joinedRelids);
static bool HasMaterializedCte(Query *query);
static bool IsOrJoinClause(Node *clause);
static bool ContainsJoinEquality(Node *clause);
static bool IsJoinEquality(Node *clause);
static bool FromClauseHasVolatileFunctions(Query *query);
static Node *ReplaceOuterVarsMutator(Node *node, OuterVarsContext * context);
static Query *BuildBranchQuery(Query *query, OrJoinCondition * condition,
							   int branch, List *fromVars, List *colnames);
static Node **FindQualsOfCondition(Query *query, OrJoinCondition * condition);
static JoinExpr *FindJoinExpr(Node *jtnode, int joinRtIndex);
static Query *BuildUnionAllQuery(List *branchQueries, List *fromVars,
								 List *colnames);
static List *MakeTargetList(List *exprs, List *colnames);


/*
 * RewriteOrJoinConditions rewrites an OR of join conditions in the WHERE
 * clause or in an inner join of the given query, without descending into
 * subqueries. See the top of this file.
 */
void
RewriteOrJoinConditions(Query *query)
{
	if (!EnableOrJoinRewrite)
		return;

	RewriteOrJoinConditionsWithBudget(query, MAX_OR_JOIN_BRANCHES);
}


/*
 * RewriteOrJoinConditionsWithBudget splits the first suitable OR in the given
 * query into at most maxBranches UNION ALL branches, and then the ORs within
 * the branches with the budget that is left.
 */
static void
RewriteOrJoinConditionsWithBudget(Query *query, int maxBranches)
{
	OrJoinCondition condition;

	if (query->commandType != CMD_SELECT || query->setOperations != NULL ||
		query->rowMarks != NIL || query->jointree == NULL)
		return;

	if (!FindOrJoinCondition(query, &condition))
		return;

	int			branchCount = list_length(condition.orClause->args);

	if (branchCount > maxBranches)
		return;

	/* every branch would evaluate volatile functions again */
	if (FromClauseHasVolatileFunctions(query))
		return;

	/* every branch gets its own copy of the CTEs, which would run again */
	if (HasMaterializedCte(query))
		return;

	OuterVarsContext context = {
		.sublevelsUp = 0
	};

	/*
	 * In PostgreSQL 18, the target list refers to grouped columns through an
	 * RTE_GROUP, which belongs to this query and not to the FROM clause.
	 */
	List	   *groupRtes = NIL;

#if PG_VERSION_NUM >= 180000
	ListCell   *lc;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_GROUP)
		{
			RangeTblEntry *groupRte = copyObject(rte);

			context.groupRtIndex = foreach_current_index(lc) + 1;
			groupRte->groupexprs = (List *)
				ReplaceOuterVarsMutator((Node *) groupRte->groupexprs, &context);
			groupRtes = list_make1(groupRte);
		}
	}
#endif

	List	   *targetList = (List *)
		ReplaceOuterVarsMutator((Node *) query->targetList, &context);
	Node	   *havingQual =
		ReplaceOuterVarsMutator(query->havingQual, &context);

	if (context.failed)
		return;

	/* a branch needs at least one column */
	if (context.fromVars == NIL)
		context.fromVars = list_make1(makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
												Int32GetDatum(1), false, true));

	List	   *colnames = NIL;

	for (int column = 1; column <= list_length(context.fromVars); column++)
		colnames = lappend(colnames, makeString(psprintf("c%d", column)));

	List	   *branchQueries = NIL;

	for (int branch = 1; branch <= branchCount; branch++)
	{
		Query	   *branchQuery = BuildBranchQuery(query, &condition, branch,
												   context.fromVars, colnames);

		RewriteOrJoinConditionsWithBudget(branchQuery, maxBranches / branchCount);

		/* the branch is two levels below this query, under the UNION ALL */
		IncrementVarSublevelsUp((Node *) branchQuery, 2, 1);

		branchQueries = lappend(branchQueries, branchQuery);
	}

	RangeTblEntry *unionRte = makeNode(RangeTblEntry);

	unionRte->rtekind = RTE_SUBQUERY;
	unionRte->subquery = BuildUnionAllQuery(branchQueries, context.fromVars,
											colnames);
	unionRte->alias = makeAlias(OR_JOIN_UNION_ALIAS, colnames);
	unionRte->eref = copyObject(unionRte->alias);
	unionRte->inFromCl = true;

	RangeTblRef *unionRef = makeNode(RangeTblRef);

	unionRef->rtindex = 1;

	/* the RTE_GROUP comes right after the UNION ALL, see the mutator */
	query->rtable = list_concat(list_make1(unionRte), groupRtes);
	query->rteperminfos = NIL;
	query->jointree = makeFromExpr(list_make1(unionRef), NULL);
	query->targetList = targetList;
	query->havingQual = havingQual;
}


/*
 * FindOrJoinCondition finds an OR of join conditions in the WHERE clause, or
 * else in the ON clause of a join where splitting it gives the same result.
 */
static bool
FindOrJoinCondition(Query *query, OrJoinCondition * condition)
{
	FromExpr   *fromExpr = query->jointree;
	List	   *joinedRelids =
		MergeOverlappingRelids(CollectJoinedRelids((Node *) fromExpr, NIL));
	ListCell   *lc;

	if (FindOrClause(fromExpr->quals, 0, joinedRelids, condition))
		return true;

	foreach(lc, fromExpr->fromlist)
	{
		if (FindOrJoinConditionInJoinTree(lfirst(lc), joinedRelids, condition))
			return true;
	}

	return false;
}


/*
 * CollectJoinedRelids returns, for every equality between two different sets
 * of relations among the AND-ed conditions of the WHERE and ON clauses under
 * the given join tree node, the relations that it references. DuckDB can use
 * those equalities as a hash join key.
 */
static List *
CollectJoinedRelids(Node *jtnode, List *joinedRelids)
{
	ListCell   *lc;

	if (IsA(jtnode, FromExpr))
	{
		FromExpr   *fromExpr = (FromExpr *) jtnode;

		joinedRelids = AddJoinedRelids(fromExpr->quals, joinedRelids);

		foreach(lc, fromExpr->fromlist)
			joinedRelids = CollectJoinedRelids(lfirst(lc), joinedRelids);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *join = (JoinExpr *) jtnode;

		joinedRelids = AddJoinedRelids(join->quals, joinedRelids);
		joinedRelids = CollectJoinedRelids(join->larg, joinedRelids);
		joinedRelids = CollectJoinedRelids(join->rarg, joinedRelids);
	}

	return joinedRelids;
}


/*
 * AddJoinedRelids adds the relations of every join equality among the AND-ed
 * conditions in quals to joinedRelids.
 */
static List *
AddJoinedRelids(Node *quals, List *joinedRelids)
{
	ListCell   *lc;

	foreach(lc, make_ands_implicit((Expr *) quals))
	{
		Node	   *qual = lfirst(lc);

		if (IsJoinEquality(qual))
			joinedRelids = lappend(joinedRelids, pull_varnos(NULL, qual));
	}

	return joinedRelids;
}


/*
 * MergeOverlappingRelids merges the sets of relations in joinedRelids that
 * share a relation, since a chain of equalities such as a.id = b.a_id and
 * b.id = c.b_id hash joins a and c as well.
 */
static List *
MergeOverlappingRelids(List *joinedRelids)
{
	List	   *mergedRelids = NIL;
	ListCell   *lc;

	foreach(lc, joinedRelids)
	{
		Relids		relids = bms_copy((Relids) lfirst(lc));
		List	   *disjointRelids = NIL;
		ListCell   *mergedCell;

		foreach(mergedCell, mergedRelids)
		{
			Relids		otherRelids = (Relids) lfirst(mergedCell);

			if (bms_overlap(relids, otherRelids))
				relids = bms_add_members(relids, otherRelids);
			else
				disjointRelids = lappend(disjointRelids, otherRelids);
		}

		mergedRelids = lappend(disjointRelids, relids);
	}

	return mergedRelids;
}


/*
 * IsAlreadyJoined returns whether the relations of the given clause are all
 * joined, directly or through other relations, by the equalities in
 * joinedRelids. DuckDB then already uses
 * a hash join and evaluates the clause as a filter, so splitting it would
 * only run the join once more per arm.
 */
static bool
IsAlreadyJoined(Node *clause, List *joinedRelids)
{
	Relids		clauseRelids = pull_varnos(NULL, clause);
	ListCell   *lc;

	foreach(lc, joinedRelids)
	{
		if (bms_is_subset(clauseRelids, (Relids) lfirst(lc)))
			return true;
	}

	return false;
}


/*
 * FindOrJoinConditionInJoinTree looks for an OR of join conditions in the ON
 * clauses under the given join tree node.
 *
 * Splitting a join into one join per arm gives the same rows as long as the
 * rows of that join are not NULL-extended further up, since an outer join
 * would otherwise NULL-extend a row once in every branch. So we look into
 * inner joins and into the outer side of left and right joins only.
 */
static bool
FindOrJoinConditionInJoinTree(Node *jtnode, List *joinedRelids,
							  OrJoinCondition * condition)
{
	if (!IsA(jtnode, JoinExpr))
		return false;

	JoinExpr   *join = (JoinExpr *) jtnode;

	switch (join->jointype)
	{
		case JOIN_INNER:
			return FindOrClause(join->quals, join->rtindex, joinedRelids,
								condition) ||
				FindOrJoinConditionInJoinTree(join->larg, joinedRelids,
											  condition) ||
				FindOrJoinConditionInJoinTree(join->rarg, joinedRelids,
											  condition);

		case JOIN_LEFT:
			return FindOrJoinConditionInJoinTree(join->larg, joinedRelids,
												 condition);

		case JOIN_RIGHT:
			return FindOrJoinConditionInJoinTree(join->rarg, joinedRelids,
												 condition);

		default:
			return false;
	}
}


/*
 * FindOrClause looks for an OR of join conditions among the AND-ed
 * conditions in quals, skipping ORs of relations that joinedRelids already
 * joins.
 */
static bool
FindOrClause(Node *quals, int joinRtIndex, List *joinedRelids,
			 OrJoinCondition * condition)
{
	List	   *andedQuals = make_ands_implicit((Expr *) quals);
	ListCell   *lc;

	foreach(lc, andedQuals)
	{
		Node	   *qual = lfirst(lc);

		if (IsOrJoinClause(qual) && !IsAlreadyJoined(qual, joinedRelids))
		{
			condition->joinRtIndex = joinRtIndex;
			condition->qualIndex = foreach_current_index(lc);
			condition->orClause = (BoolExpr *) qual;
			return true;
		}
	}

	return false;
}


/*
 * IsOrJoinClause returns whether the given clause is an OR in which every arm
 * has an equality between two different sets of relations, which DuckDB can
 * execute as a hash join once the arms are split up.
 */
static bool
IsOrJoinClause(Node *clause)
{
	if (!is_orclause(clause))
		return false;

	if (contain_volatile_functions(clause) || checkExprHasSubLink(clause))
		return false;

	ListCell   *lc;

	foreach(lc, ((BoolExpr *) clause)->args)
	{
		if (!ContainsJoinEquality(lfirst(lc)))
			return false;
	}

	return true;
}


/*
 * ContainsJoinEquality returns whether the given clause is, or is an AND
 * with, an equality between two different sets of relations.
 */
static bool
ContainsJoinEquality(Node *clause)
{
	if (is_andclause(clause))
	{
		ListCell   *lc;

		foreach(lc, ((BoolExpr *) clause)->args)
		{
			if (IsJoinEquality(lfirst(lc)))
				return true;
		}

		return false;
	}

	return IsJoinEquality(clause);
}


/*
 * IsJoinEquality returns whether the given clause is a hash-joinable equality
 * between two different sets of relations.
 */
static bool
IsJoinEquality(Node *clause)
{
	if (!IsA(clause, OpExpr) || list_length(((OpExpr *) clause)->args) != 2)
		return false;

	OpExpr	   *opExpr = (OpExpr *) clause;
	Node	   *left = linitial(opExpr->args);
	Node	   *right = lsecond(opExpr->args);

	if (!op_hashjoinable(opExpr->opno, exprType(left)))
		return false;

	Relids		leftRelids = pull_varnos(NULL, left);
	Relids		rightRelids = pull_varnos(NULL, right);

	return !bms_is_empty(leftRelids) && !bms_is_empty(rightRelids) &&
		!bms_overlap(leftRelids, rightRelids);
}


/*
 * FromClauseHasVolatileFunctions returns whether the FROM or WHERE clause of
 * the query, or one of its CTEs, calls a volatile function.
 */
static bool
FromClauseHasVolatileFunctions(Query *query)
{
	ListCell   *lc;

	if (contain_volatile_functions((Node *) query->jointree) ||
		contain_volatile_functions((Node *) query->cteList))
		return true;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		switch (rte->rtekind)
		{
			case RTE_SUBQUERY:
				if (contain_volatile_functions((Node *) rte->subquery))
					return true;
				break;

			case RTE_FUNCTION:
				if (contain_volatile_functions((Node *) rte->functions))
					return true;
				break;

			case RTE_VALUES:
				if (contain_volatile_functions((Node *) rte->values_lists))
					return true;
				break;

			case RTE_TABLEFUNC:
				return true;

			default:
				break;
		}
	}

	return false;
}


/*
 * HasMaterializedCte returns whether the query has a CTE marked MATERIALIZED,
 * which the user wants to run only once.
 */
static bool
HasMaterializedCte(Query *query)
{
	ListCell   *lc;

	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (cte->ctematerialized == CTEMaterializeAlways)
			return true;
	}

	return false;
}


/*
 * ReplaceOuterVarsMutator replaces Vars that refer to the FROM clause of the
 * query with Vars of the UNION ALL subquery, which becomes range table entry
 * 1, and collects the replaced Vars in context->fromVars to build the target
 * list of the branches. Vars of the RTE_GROUP move to range table entry 2.
 */
static Node *
ReplaceOuterVarsMutator(Node *node, OuterVarsContext * context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Query))
	{
		context->sublevelsUp++;

		Node	   *result = (Node *) query_tree_mutator((Query *) node,
														 ReplaceOuterVarsMutator,
														 context, 0);

		context->sublevelsUp--;

		return result;
	}

	if (IsA(node, Var) && ((Var *) node)->varlevelsup == context->sublevelsUp)
	{
		Var		   *var = (Var *) copyObject(node);

		/* the outer joins that NULL the column are now inside the UNION ALL */
		var->varnullingrels = NULL;

		if (var->varno == context->groupRtIndex)
		{
			var->varno = 2;
			var->varnosyn = 2;
			return (Node *) var;
		}

		/* whole-row Vars and system columns are not simple columns */
		if (var->varattno <= 0)
		{
			context->failed = true;
			return (Node *) var;
		}

		int			column = 0;
		ListCell   *lc;

		var->varlevelsup = 0;

		foreach(lc, context->fromVars)
		{
			if (equal(lfirst(lc), var))
			{
				column = foreach_current_index(lc) + 1;
				break;
			}
		}

		if (column == 0)
		{
			context->fromVars = lappend(context->fromVars, var);
			column = list_length(context->fromVars);
		}

		return (Node *) makeVar(1, column, var->vartype, var->vartypmod,
								var->varcollid, context->sublevelsUp);
	}

	/* GROUPING() refers to grouping expressions, which we do not track */
	if (IsA(node, GroupingFunc))
	{
		context->failed = true;
		return node;
	}

	return expression_tree_mutator(node, ReplaceOuterVarsMutator, context);
}


/*
 * BuildBranchQuery builds the UNION ALL branch for the given arm (1-based)
 * of the OR: the FROM and WHERE clause of the query, with the OR replaced by
 * the arm and by a condition that none of the earlier arms matched.
 */
static Query *
BuildBranchQuery(Query *query, OrJoinCondition * condition, int branch,
				 List *fromVars, List *colnames)
{
	Query	   *branchQuery = makeNode(Query);

	branchQuery->commandType = CMD_SELECT;
	branchQuery->querySource = QSRC_ORIGINAL;
	branchQuery->canSetTag = true;
	branchQuery->rtable = copyObject(query->rtable);
	branchQuery->rteperminfos = copyObject(query->rteperminfos);
	branchQuery->jointree = copyObject(query->jointree);
	branchQuery->hasSubLinks = query->hasSubLinks;

	/*
	 * The branch is two query levels below the CTEs of the query, and
	 * IncrementVarSublevelsUp only adjusts references to levels above the
	 * branch, so every branch gets its own copy of the CTEs.
	 */
	branchQuery->cteList = copyObject(query->cteList);
	branchQuery->hasRecursive = query->hasRecursive;
	branchQuery->targetList = MakeTargetList(copyObject(fromVars), colnames);

#if PG_VERSION_NUM >= 180000
	ListCell   *lc;

	/* the RTE_GROUP stays behind, so drop its expressions from this copy */
	foreach(lc, branchQuery->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_GROUP)
			rte->groupexprs = NIL;
	}
#endif

	List	   *arms = condition->orClause->args;
	List	   *branchQuals = list_make1(copyObject(list_nth(arms, branch - 1)));

	if (branch > 1)
	{
		List	   *earlierArms = list_copy_head(arms, branch - 1);
		BooleanTest *notMatched = makeNode(BooleanTest);

		notMatched->arg = list_length(earlierArms) == 1 ?
			copyObject(linitial(earlierArms)) :
			make_orclause(copyObject(earlierArms));
		notMatched->booltesttype = IS_NOT_TRUE;
		notMatched->location = -1;

		branchQuals = lappend(branchQuals, notMatched);
	}

	/* put the conditions of the branch where the OR was */
	Node	  **quals = FindQualsOfCondition(branchQuery, condition);
	List	   *andedQuals = make_ands_implicit((Expr *) *quals);
	List	   *newQuals = list_copy_head(andedQuals, condition->qualIndex);

	newQuals = list_concat(newQuals, branchQuals);
	newQuals = list_concat(newQuals,
						   list_copy_tail(andedQuals, condition->qualIndex + 1));

	*quals = (Node *) make_ands_explicit(newQuals);

	return branchQuery;
}


/*
 * FindQualsOfCondition returns a pointer to the WHERE or ON clause in the
 * given copy of the query that holds the OR clause.
 */
static Node **
FindQualsOfCondition(Query *query, OrJoinCondition * condition)
{
	if (condition->joinRtIndex == 0)
		return &query->jointree->quals;

	ListCell   *lc;

	foreach(lc, query->jointree->fromlist)
	{
		JoinExpr   *join = FindJoinExpr(lfirst(lc), condition->joinRtIndex);

		if (join != NULL)
			return &join->quals;
	}

	elog(ERROR, "could not find join %d in join tree", condition->joinRtIndex);
}


/*
 * FindJoinExpr returns the JoinExpr with the given range table index under the
 * given join tree node, or NULL.
 */
static JoinExpr *
FindJoinExpr(Node *jtnode, int joinRtIndex)
{
	if (!IsA(jtnode, JoinExpr))
		return NULL;

	JoinExpr   *join = (JoinExpr *) jtnode;

	if (join->rtindex == joinRtIndex)
		return join;

	JoinExpr   *found = FindJoinExpr(join->larg, joinRtIndex);

	return found != NULL ? found : FindJoinExpr(join->rarg, joinRtIndex);
}


/*
 * BuildUnionAllQuery builds a UNION ALL of the given branch queries, which
 * all return fromVars.
 */
static Query *
BuildUnionAllQuery(List *branchQueries, List *fromVars, List *colnames)
{
	Query	   *unionQuery = makeNode(Query);
	List	   *colTypes = NIL;
	List	   *colTypmods = NIL;
	List	   *colCollations = NIL;
	List	   *outputVars = NIL;
	ListCell   *lc;

	foreach(lc, fromVars)
	{
		Node	   *fromVar = lfirst(lc);

		colTypes = lappend_oid(colTypes, exprType(fromVar));
		colTypmods = lappend_int(colTypmods, exprTypmod(fromVar));
		colCollations = lappend_oid(colCollations, exprCollation(fromVar));

		/* a UNION ALL returns the columns of its first branch */
		outputVars = lappend(outputVars,
							 makeVar(1, foreach_current_index(lc) + 1,
									 exprType(fromVar), exprTypmod(fromVar),
									 exprCollation(fromVar), 0));
	}

	unionQuery->commandType = CMD_SELECT;
	unionQuery->querySource = QSRC_ORIGINAL;
	unionQuery->canSetTag = true;
	unionQuery->jointree = makeFromExpr(NIL, NULL);
	unionQuery->targetList = MakeTargetList(outputVars, colnames);

	Node	   *setOperations = NULL;

	foreach(lc, branchQueries)
	{
		RangeTblEntry *branchRte = makeNode(RangeTblEntry);

		branchRte->rtekind = RTE_SUBQUERY;
		branchRte->subquery = lfirst(lc);
		branchRte->eref = makeAlias(psprintf("*SELECT* %d",
											 foreach_current_index(lc) + 1),
									copyObject(colnames));

		unionQuery->rtable = lappend(unionQuery->rtable, branchRte);

		RangeTblRef *branchRef = makeNode(RangeTblRef);

		branchRef->rtindex = list_length(unionQuery->rtable);

		if (setOperations == NULL)
		{
			setOperations = (Node *) branchRef;
			continue;
		}

		SetOperationStmt *unionAll = makeNode(SetOperationStmt);

		unionAll->op = SETOP_UNION;
		unionAll->all = true;
		unionAll->larg = setOperations;
		unionAll->rarg = (Node *) branchRef;
		unionAll->colTypes = colTypes;
		unionAll->colTypmods = colTypmods;
		unionAll->colCollations = colCollations;

		setOperations = (Node *) unionAll;
	}

	unionQuery->setOperations = setOperations;

	return unionQuery;
}


/*
 * MakeTargetList builds a target list of the given expressions, with the
 * given column names.
 */
static List *
MakeTargetList(List *exprs, List *colnames)
{
	List	   *targetList = NIL;
	ListCell   *lc;

	foreach(lc, exprs)
	{
		int			column = foreach_current_index(lc) + 1;

		targetList = lappend(targetList,
							 makeTargetEntry(lfirst(lc), column,
											 strVal(list_nth(colnames, column - 1)),
											 false));
	}

	return targetList;
}

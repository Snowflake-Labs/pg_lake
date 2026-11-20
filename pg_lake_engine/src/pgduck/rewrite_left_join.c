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
 * rewrite_left_join.c
 *		Fold ON conditions of a LEFT JOIN that only reference the left side
 *		into the join key, so that DuckDB can use a hash join.
 *
 * DuckDB plans "a LEFT JOIN b ON a.x = 'v' AND a.k = b.k" as a
 * BLOCKWISE_NL_JOIN, which compares every pair of rows. We instead send
 *
 *   a LEFT JOIN b ON CASE WHEN a.x = 'v' THEN a.k END = b.k
 *
 * which is a plain equality between the two sides, so it becomes a hash
 * join. When the condition is not true, the key is NULL and never matches,
 * so the row of a is NULL-extended just like before.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_lake/pgduck/rewrite_left_join.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"

/* pg_lake_engine.enable_left_join_rewrite setting */
bool		EnableLeftJoinRewrite = true;

static void RewriteLeftJoinsInJoinTree(Node *jtnode);
static void RewriteLeftJoin(JoinExpr *join);
static bool IsSafeToEvaluate(Node *expr);
static Relids JoinTreeRelids(Node *jtnode);
static Relids JoinTreeJoinRtIndexes(Node *jtnode);


/*
 * RewriteLeftJoinConditions rewrites the ON clauses of LEFT JOINs in the
 * given query, without descending into subqueries. See the top of this file.
 */
void
RewriteLeftJoinConditions(Query *query)
{
	if (!EnableLeftJoinRewrite || query->jointree == NULL)
		return;

	RewriteLeftJoinsInJoinTree((Node *) query->jointree);
}


/*
 * RewriteLeftJoinsInJoinTree rewrites the LEFT JOINs under the given join
 * tree node.
 */
static void
RewriteLeftJoinsInJoinTree(Node *jtnode)
{
	if (IsA(jtnode, FromExpr))
	{
		ListCell   *lc;

		foreach(lc, ((FromExpr *) jtnode)->fromlist)
			RewriteLeftJoinsInJoinTree(lfirst(lc));
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *join = (JoinExpr *) jtnode;

		RewriteLeftJoinsInJoinTree(join->larg);
		RewriteLeftJoinsInJoinTree(join->rarg);

		if (join->jointype == JOIN_LEFT)
			RewriteLeftJoin(join);
	}
}


/*
 * RewriteLeftJoin folds the ON conditions of the given LEFT JOIN that only
 * reference the left side into a CASE around the left key of an equality
 * between the two sides.
 */
static void
RewriteLeftJoin(JoinExpr *join)
{
	Relids		leftRelids = JoinTreeRelids(join->larg);
	Relids		rightRelids = JoinTreeRelids(join->rarg);
	List	   *leftOnlyQuals = NIL;
	List	   *otherQuals = NIL;
	OpExpr	   *equality = NULL;
	bool		leftKeyIsFirst = false;
	ListCell   *lc;

	foreach(lc, make_ands_implicit((Expr *) join->quals))
	{
		Node	   *qual = lfirst(lc);
		Relids		relids = pull_varnos(NULL, qual);

		if (!bms_is_empty(relids) && bms_is_subset(relids, leftRelids) &&
			IsSafeToEvaluate(qual))
		{
			leftOnlyQuals = lappend(leftOnlyQuals, qual);
			continue;
		}

		if (equality == NULL && IsA(qual, OpExpr) &&
			list_length(((OpExpr *) qual)->args) == 2 &&
			IsSafeToEvaluate(qual))
		{
			OpExpr	   *opExpr = (OpExpr *) qual;
			Relids		firstRelids = pull_varnos(NULL, linitial(opExpr->args));
			Relids		secondRelids = pull_varnos(NULL, lsecond(opExpr->args));

			if (op_hashjoinable(opExpr->opno, exprType(linitial(opExpr->args))) &&
				!bms_is_empty(firstRelids) && !bms_is_empty(secondRelids))
			{
				if (bms_is_subset(firstRelids, leftRelids) &&
					bms_is_subset(secondRelids, rightRelids))
				{
					equality = opExpr;
					leftKeyIsFirst = true;
					continue;
				}
				else if (bms_is_subset(firstRelids, rightRelids) &&
						 bms_is_subset(secondRelids, leftRelids))
				{
					equality = opExpr;
					leftKeyIsFirst = false;
					continue;
				}
			}
		}

		otherQuals = lappend(otherQuals, qual);
	}

	if (leftOnlyQuals == NIL || equality == NULL)
		return;

	Node	   *leftKey = leftKeyIsFirst ?
		linitial(equality->args) : lsecond(equality->args);

	/*
	 * DuckDB only uses an equality as a hash join key when each side refers
	 * to a single relation, so the CASE only helps if the conditions and the
	 * key all come from the same relation. We leave out the outer joins that
	 * pull_varnos reports for Vars that they can NULL.
	 */
	Relids		caseRelids = pull_varnos(NULL, leftKey);

	foreach(lc, leftOnlyQuals)
		caseRelids = bms_add_members(caseRelids, pull_varnos(NULL, lfirst(lc)));

	caseRelids = bms_del_members(caseRelids, JoinTreeJoinRtIndexes(join->larg));

	if (bms_num_members(caseRelids) != 1)
		return;

	CaseWhen   *caseWhen = makeNode(CaseWhen);

	caseWhen->expr = make_ands_explicit(leftOnlyQuals);
	caseWhen->result = (Expr *) leftKey;
	caseWhen->location = -1;

	CaseExpr   *caseExpr = makeNode(CaseExpr);

	caseExpr->casetype = exprType(leftKey);
	caseExpr->casecollid = exprCollation(leftKey);
	caseExpr->args = list_make1(caseWhen);
	caseExpr->defresult = (Expr *) makeNullConst(exprType(leftKey),
												 exprTypmod(leftKey),
												 exprCollation(leftKey));
	caseExpr->location = -1;

	OpExpr	   *newEquality = (OpExpr *) copyObject(equality);

	if (leftKeyIsFirst)
		linitial(newEquality->args) = caseExpr;
	else
		lsecond(newEquality->args) = caseExpr;

	join->quals = (Node *) make_ands_explicit(lcons(newEquality, otherQuals));
}


/*
 * IsSafeToEvaluate returns whether moving the given expression into a CASE
 * keeps its meaning.
 */
static bool
IsSafeToEvaluate(Node *expr)
{
	return !contain_volatile_functions(expr) && !checkExprHasSubLink(expr);
}


/*
 * JoinTreeRelids returns the range table indexes of all relations and joins
 * under the given join tree node, so that Vars that refer to a join alias
 * also count as coming from that side.
 */
static Relids
JoinTreeRelids(Node *jtnode)
{
	if (IsA(jtnode, RangeTblRef))
		return bms_make_singleton(((RangeTblRef *) jtnode)->rtindex);

	if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *join = (JoinExpr *) jtnode;
		Relids		relids = bms_make_singleton(join->rtindex);

		relids = bms_add_members(relids, JoinTreeRelids(join->larg));
		return bms_add_members(relids, JoinTreeRelids(join->rarg));
	}

	elog(ERROR, "unrecognized node type in join tree: %d", (int) nodeTag(jtnode));
}


/*
 * JoinTreeJoinRtIndexes returns the range table indexes of the joins under
 * the given join tree node.
 */
static Relids
JoinTreeJoinRtIndexes(Node *jtnode)
{
	if (!IsA(jtnode, JoinExpr))
		return NULL;

	JoinExpr   *join = (JoinExpr *) jtnode;
	Relids		relids = bms_make_singleton(join->rtindex);

	relids = bms_add_members(relids, JoinTreeJoinRtIndexes(join->larg));
	return bms_add_members(relids, JoinTreeJoinRtIndexes(join->rarg));
}

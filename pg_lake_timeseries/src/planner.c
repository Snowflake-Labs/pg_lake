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
 * The read path of a tiered table: a planner hook that puts the two tiers back
 * together (DESIGN.md section 7).
 *
 * A reference to a registered relation R with cold tier C and boundary B is
 * replaced by
 *
 *     (SELECT <cols> FROM R WHERE time >= B
 *      UNION ALL
 *      SELECT <cols> FROM C WHERE time <  B)
 *
 * which is the same substitution the rewriter performs for a view, done here
 * because B changes as maintenance runs and a view would have to be recreated
 * for every change. Both branches carry an explicit bound, so a row is returned
 * by exactly one of them, and a query with its own predicate on the time column
 * has one branch contradicted and pruned at plan time.
 *
 * Why a planner hook and not a view: R stays the relation the user created.
 * Writes, indexes, tuple routing, TRUNCATE, ALTER TABLE and pg_dump all see an
 * ordinary partitioned table, and nothing has to be renamed to make room for a
 * view. The cost is that only paths through the planner see both tiers, and that
 * this file has to build by hand what the rewriter builds from a stored parse
 * tree. The one read path that skips the planner is COPY <relation> TO, which
 * PostgreSQL does not allow on a partitioned table in the first place; anything
 * that reads R directly, such as logical replication, sees the heap alone.
 *
 * Nothing happens at all unless the query mentions a registered relation:
 * IsTieredTable() answers from a backend-local cache that is empty on clusters
 * that do not use the feature, and the parse tree is only copied once something
 * is going to be rewritten.
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "catalog/namespace.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "pg_lake_timeseries/metadata.h"
#include "pg_lake_timeseries/planner.h"


/* the hook we replaced, called after we have rewritten the parse tree */
static planner_hook_type PreviousPlannerHook = NULL;

static PlannedStmt *TimeseriesPlanner(Query *parse, const char *queryString,
									  int cursorOptions,
									  ParamListInfo boundParams);
static bool FindTieredTableWalker(Node *node, void *context);
static bool ExpandTieredTablesWalker(Node *node, void *context);
static void ExpandTieredTablesInQuery(Query *query);
static bool IsExpandableTieredRTE(Query *query, RangeTblEntry *rte, int rtIndex);
static Query *MakeTierUnion(TieredTable * tieredTable, RangeTblEntry *rte);
static Query *MakeTierBranch(Oid relationId, char relkind, bool inh,
							 Oid checkAsUser, List *targetList, Node *quals,
							 List *colNames);
static Node *MakeBoundaryQual(Oid relationId, char *timeColumn, int varno,
							  const char *operatorName, TimestampTz boundary);
static Oid	TierOwnerId(Oid relationId, const char *tierDescription,
						char *relkind);
static List *RelationColumnNames(TupleDesc tupleDesc);
static AttrNumber TimeColumnNumber(Oid relationId, char *timeColumn);


/*
 * InitializeTimeseriesPlanner installs the planner hook. Called from _PG_init.
 *
 * pg_lake_timeseries has to appear *after* pg_lake_table in
 * shared_preload_libraries: hooks are called last-installed-first, and
 * pg_lake_table decides whether to push a query down whole by walking the parse
 * tree, which has to already contain the cold tier when it looks.
 */
void
InitializeTimeseriesPlanner(void)
{
	PreviousPlannerHook = planner_hook;
	planner_hook = TimeseriesPlanner;
}


/*
 * TimeseriesPlanner expands every reference to a tiered table in the query and
 * then plans it as usual.
 */
static PlannedStmt *
TimeseriesPlanner(Query *parse, const char *queryString, int cursorOptions,
				  ParamListInfo boundParams)
{
	/*
	 * The parse tree may belong to a cached plan source, which will hand us
	 * the same tree again on a later replan, so it must not be modified in
	 * place. Copy it only once we know there is something to rewrite.
	 */
	if (ExpandTieredTables && FindTieredTableWalker((Node *) parse, NULL))
	{
		parse = copyObject(parse);

		ExpandTieredTablesWalker((Node *) parse, NULL);
	}

	if (PreviousPlannerHook != NULL)
		return PreviousPlannerHook(parse, queryString, cursorOptions, boundParams);

	return standard_planner(parse, queryString, cursorOptions, boundParams);
}


/*
 * FindTieredTableWalker returns whether any part of a query reads a tiered
 * table, which is the cheap test that keeps an ordinary query out of the rest of
 * this file.
 */
static bool
FindTieredTableWalker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		ListCell   *rangeTableCell = NULL;
		Index		rtIndex = 0;

		foreach(rangeTableCell, query->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(rangeTableCell);

			rtIndex++;

			if (IsExpandableTieredRTE(query, rte, rtIndex))
				return true;
		}

		return query_tree_walker(query, FindTieredTableWalker, context, 0);
	}

	return expression_tree_walker(node, FindTieredTableWalker, context);
}


/*
 * ExpandTieredTablesWalker rewrites every query in the tree.
 *
 * The recursion happens *before* the query's own range table is rewritten, on
 * purpose: expansion adds subqueries that read the tiers directly, and walking
 * into those would find the same tiered relation again, forever.
 */
static bool
ExpandTieredTablesWalker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		(void) query_tree_walker(query, ExpandTieredTablesWalker, context, 0);

		ExpandTieredTablesInQuery(query);

		return false;
	}

	return expression_tree_walker(node, ExpandTieredTablesWalker, context);
}


/*
 * IsExpandableTieredRTE returns whether one range table entry is a reference to
 * a tiered table that this hook should replace.
 */
static bool
IsExpandableTieredRTE(Query *query, RangeTblEntry *rte, int rtIndex)
{
	if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_PARTITIONED_TABLE)
		return false;

	/*
	 * ONLY excludes the partitions, so it asks for the parent's own rows, of
	 * which there are none. TABLESAMPLE cannot sample an Iceberg table.
	 */
	if (!rte->inh || rte->tablesample != NULL)
		return false;

	/*
	 * The target of an INSERT/UPDATE/DELETE/MERGE stays the relation itself:
	 * PostgreSQL owns everything at or above the boundary, and that is where
	 * every writable row is. A write aimed below the boundary finds no
	 * partition and fails, which is the intended answer rather than a silent
	 * write to the wrong tier.
	 */
	if (rtIndex == query->resultRelation)
		return false;

	/*
	 * FOR UPDATE/SHARE needs a row to lock, and Iceberg rows cannot be
	 * locked.
	 */
	if (get_parse_rowmark(query, rtIndex) != NULL)
		return false;

	return IsTieredTable(rte->relid);
}


/*
 * ExpandTieredTablesInQuery replaces every expandable reference to a tiered
 * table in one query level with a union of its two tiers.
 */
static void
ExpandTieredTablesInQuery(Query *query)
{
	ListCell   *rangeTableCell = NULL;
	Index		rtIndex = 0;

	foreach(rangeTableCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rangeTableCell);

		rtIndex++;

		if (!IsExpandableTieredRTE(query, rte, rtIndex))
			continue;

		TieredTable tieredTable = {0};

		if (!GetTieredTable(rte->relid, &tieredTable))
			continue;

		/*
		 * An empty cold tier owns nothing, so there is nothing to add and the
		 * query is left exactly as it was.
		 */
		if (TIMESTAMP_IS_NOBEGIN(tieredTable.boundary))
			continue;

		Query	   *tierUnion = MakeTierUnion(&tieredTable, rte);

		/*
		 * Turn the relation into a subquery the way the rewriter does it for
		 * a view (ApplyRetrieveRule): relid, rellockmode and perminfoindex
		 * stay, so the relation is still locked before execution and the
		 * caller's privileges on it are still checked.
		 *
		 * relkind becomes RELKIND_VIEW because that is the only relkind
		 * PostgreSQL expects on a subquery range table entry that carries
		 * permission info -- ExecCheckPermissions asserts it -- and because
		 * this range table entry is now precisely what a view's is: the
		 * relation the caller is checked against, expanded into a query.
		 */
		rte->rtekind = RTE_SUBQUERY;
		rte->relkind = RELKIND_VIEW;
		rte->subquery = tierUnion;
		rte->inh = false;
		rte->tablesample = NULL;

		query->hasSubLinks |= tierUnion->hasSubLinks;
	}
}


/*
 * MakeTierUnion builds the UNION ALL of the two tiers of one table.
 *
 * The result has the shape the parser gives a set operation: an empty jointree,
 * one subquery range table entry per branch, a SetOperationStmt over the two,
 * and a target list of Vars referencing the left branch.
 */
static Query *
MakeTierUnion(TieredTable * tieredTable, RangeTblEntry *rte)
{
	char		hotRelkind = 0;
	char		coldRelkind = 0;
	Oid			hotOwnerId = TierOwnerId(tieredTable->relationId, "table",
										 &hotRelkind);

	/*
	 * The cold tier is not in the query yet, so nothing has locked it. The
	 * planner opens every relation it plans with NoLock and expects the
	 * caller to have taken one.
	 */
	LockRelationOid(tieredTable->coldTableId, AccessShareLock);

	(void) TierOwnerId(tieredTable->coldTableId, "cold tier", &coldRelkind);

	Relation	hotRelation = table_open(tieredTable->relationId, NoLock);
	Relation	coldRelation = table_open(tieredTable->coldTableId, NoLock);
	TupleDesc	hotDesc = RelationGetDescr(hotRelation);
	TupleDesc	coldDesc = RelationGetDescr(coldRelation);

	List	   *hotTargetList = NIL;
	List	   *coldTargetList = NIL;
	List	   *colTypes = NIL;
	List	   *colTypmods = NIL;
	List	   *colCollations = NIL;
	AttrNumber	coldAttNumber = 0;

	for (int attIndex = 0; attIndex < hotDesc->natts; attIndex++)
	{
		Form_pg_attribute hotAtt = TupleDescAttr(hotDesc, attIndex);
		AttrNumber	resultNumber = attIndex + 1;
		Expr	   *hotExpr = NULL;
		Expr	   *coldExpr = NULL;
		Oid			colType = INT4OID;
		int32		colTypmod = -1;
		Oid			colCollation = InvalidOid;

		if (hotAtt->attisdropped)
		{
			/*
			 * No Var can reference a dropped column, but the branches still
			 * need a column in that position to keep the ones after it
			 * aligned with the attribute numbers the outer query uses. A NULL
			 * of an arbitrary type is what the planner itself substitutes
			 * here.
			 */
			hotExpr = (Expr *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
										 (Datum) 0, true, true);
			coldExpr = copyObject(hotExpr);
		}
		else
		{
			/* the next live column of the cold tier is the matching one */
			Form_pg_attribute coldAtt = NULL;

			while (coldAttNumber < coldDesc->natts)
			{
				Form_pg_attribute candidate = TupleDescAttr(coldDesc, coldAttNumber);

				coldAttNumber++;

				if (!candidate->attisdropped)
				{
					coldAtt = candidate;
					break;
				}
			}

			if (coldAtt == NULL ||
				coldAtt->atttypid != hotAtt->atttypid ||
				coldAtt->atttypmod != hotAtt->atttypmod)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("tiers of %s no longer have the same columns",
								get_rel_name(tieredTable->relationId)),
						 errdetail("Column %s of %s has no counterpart of the "
								   "same type in %s.",
								   NameStr(hotAtt->attname),
								   get_rel_name(tieredTable->relationId),
								   get_rel_name(tieredTable->coldTableId)),
						 errhint("Undo the change, or bring the tiers back into "
								 "the same shape with ALTER TABLE.")));
			}

			colType = hotAtt->atttypid;
			colTypmod = hotAtt->atttypmod;
			colCollation = hotAtt->attcollation;

			hotExpr = (Expr *) makeVar(1, resultNumber, colType, colTypmod,
									   colCollation, 0);
			coldExpr = (Expr *) makeVar(1, coldAtt->attnum, colType, colTypmod,
										colCollation, 0);
		}

		char	   *colName = pstrdup(NameStr(hotAtt->attname));

		hotTargetList = lappend(hotTargetList,
								makeTargetEntry(hotExpr, resultNumber, colName,
												false));
		coldTargetList = lappend(coldTargetList,
								 makeTargetEntry(coldExpr, resultNumber, colName,
												 false));

		colTypes = lappend_oid(colTypes, colType);
		colTypmods = lappend_int(colTypmods, colTypmod);
		colCollations = lappend_oid(colCollations, colCollation);
	}

	List	   *hotColNames = RelationColumnNames(hotDesc);
	List	   *coldColNames = RelationColumnNames(coldDesc);

	table_close(coldRelation, NoLock);
	table_close(hotRelation, NoLock);

	/*
	 * The bounds are what makes this a partition of the data rather than a
	 * duplication of it: >= B on one side, < B on the other, and NOT NULL on
	 * the time column (required when the table was created) so no row escapes
	 * both.
	 */
	Node	   *hotQual = MakeBoundaryQual(tieredTable->relationId,
										   NameStr(tieredTable->timeColumn), 1,
										   ">=", tieredTable->boundary);
	Node	   *coldQual = MakeBoundaryQual(tieredTable->coldTableId,
											NameStr(tieredTable->timeColumn), 1,
											"<", tieredTable->boundary);

	/*
	 * Both branches are read with the privileges of the owner of the tiered
	 * table, as the branches of a view are read with the view owner's. The
	 * caller's own privileges are checked on the range table entry we are
	 * replacing, which keeps its relid and perminfoindex.
	 */
	Query	   *hotBranch = MakeTierBranch(tieredTable->relationId, hotRelkind,
										   hotRelkind == RELKIND_PARTITIONED_TABLE,
										   hotOwnerId, hotTargetList, hotQual,
										   hotColNames);
	Query	   *coldBranch = MakeTierBranch(tieredTable->coldTableId, coldRelkind,
											coldRelkind == RELKIND_PARTITIONED_TABLE,
											hotOwnerId, coldTargetList, coldQual,
											coldColNames);

	/*
	 * Both branches are named after the relation the outer query asked for,
	 * rather than the "*SELECT* n" the parser gives a set operation, so that
	 * EXPLAIN and any error message read as the table the user wrote.
	 */
	char	   *tableName = rte->eref != NULL && rte->eref->aliasname != NULL ?
		pstrdup(rte->eref->aliasname) : get_rel_name(tieredTable->relationId);

	RangeTblEntry *hotRte = makeNode(RangeTblEntry);

	hotRte->rtekind = RTE_SUBQUERY;
	hotRte->subquery = hotBranch;
	hotRte->eref = makeAlias(tableName, list_copy(hotColNames));

	RangeTblEntry *coldRte = makeNode(RangeTblEntry);

	coldRte->rtekind = RTE_SUBQUERY;
	coldRte->subquery = coldBranch;
	coldRte->eref = makeAlias(tableName, list_copy(hotColNames));

	RangeTblRef *hotRef = makeNode(RangeTblRef);
	RangeTblRef *coldRef = makeNode(RangeTblRef);

	hotRef->rtindex = 1;
	coldRef->rtindex = 2;

	SetOperationStmt *setOperation = makeNode(SetOperationStmt);

	setOperation->op = SETOP_UNION;
	setOperation->all = true;
	setOperation->larg = (Node *) hotRef;
	setOperation->rarg = (Node *) coldRef;
	setOperation->colTypes = colTypes;
	setOperation->colTypmods = colTypmods;
	setOperation->colCollations = colCollations;

	/* no grouping: UNION ALL keeps every row of both branches */
	setOperation->groupClauses = NIL;

	Query	   *tierUnion = makeNode(Query);

	tierUnion->commandType = CMD_SELECT;
	tierUnion->canSetTag = true;
	tierUnion->rtable = list_make2(hotRte, coldRte);
	tierUnion->jointree = makeFromExpr(NIL, NULL);
	tierUnion->setOperations = (Node *) setOperation;

	/*
	 * The target list of a set operation is a set of Vars referencing the
	 * leftmost branch, one per output column.
	 */
	ListCell   *targetCell = NULL;

	foreach(targetCell, hotTargetList)
	{
		TargetEntry *branchEntry = (TargetEntry *) lfirst(targetCell);
		Var		   *outputVar = makeVar(1, branchEntry->resno,
										exprType((Node *) branchEntry->expr),
										exprTypmod((Node *) branchEntry->expr),
										exprCollation((Node *) branchEntry->expr),
										0);

		tierUnion->targetList = lappend(tierUnion->targetList,
										makeTargetEntry((Expr *) outputVar,
														branchEntry->resno,
														branchEntry->resname,
														false));
	}

	return tierUnion;
}


/*
 * MakeTierBranch builds one branch of the union: a SELECT of the given target
 * list from one relation, bounded by the given qual.
 */
static Query *
MakeTierBranch(Oid relationId, char relkind, bool inh, Oid checkAsUser,
			   List *targetList, Node *quals, List *colNames)
{
	Query	   *branch = makeNode(Query);

	branch->commandType = CMD_SELECT;
	branch->canSetTag = true;
	branch->targetList = targetList;

	RangeTblEntry *rte = makeNode(RangeTblEntry);

	rte->rtekind = RTE_RELATION;
	rte->relid = relationId;
	rte->relkind = relkind;
	rte->rellockmode = AccessShareLock;
	rte->inh = inh;
	rte->inFromCl = true;
	rte->alias = NULL;
	rte->eref = makeAlias(get_rel_name(relationId), colNames);

	branch->rtable = list_make1(rte);

	RTEPermissionInfo *permissionInfo = addRTEPermissionInfo(&branch->rteperminfos,
															 rte);

	permissionInfo->requiredPerms = ACL_SELECT;
	permissionInfo->checkAsUser = checkAsUser;

	RangeTblRef *rangeTableRef = makeNode(RangeTblRef);

	rangeTableRef->rtindex = 1;

	branch->jointree = makeFromExpr(list_make1(rangeTableRef), quals);

	return branch;
}


/*
 * MakeBoundaryQual builds "<time column> <operator> <boundary>" over the
 * relation at varno.
 */
static Node *
MakeBoundaryQual(Oid relationId, char *timeColumn, int varno,
				 const char *operatorName, TimestampTz boundary)
{
	AttrNumber	timeColumnNumber = TimeColumnNumber(relationId, timeColumn);

	Var		   *timeVar = makeVar(varno, timeColumnNumber, TIMESTAMPTZOID, -1,
								  InvalidOid, 0);
	Const	   *boundaryConst = makeConst(TIMESTAMPTZOID, -1, InvalidOid,
										  sizeof(TimestampTz),
										  TimestampTzGetDatum(boundary),
										  false, FLOAT8PASSBYVAL);

	/*
	 * Schema-qualified so that a shadowing operator in the caller's
	 * search_path cannot decide which rows belong to which tier.
	 */
	List	   *operatorNameList = list_make2(makeString("pg_catalog"),
											  makeString(pstrdup(operatorName)));
	Oid			operatorId = OpernameGetOprid(operatorNameList, TIMESTAMPTZOID,
											  TIMESTAMPTZOID);

	if (!OidIsValid(operatorId))
		elog(ERROR, "could not find operator pg_catalog.%s(timestamptz, timestamptz)",
			 operatorName);

	return (Node *) make_opclause(operatorId, BOOLOID, false, (Expr *) timeVar,
								  (Expr *) boundaryConst, InvalidOid, InvalidOid);
}


/*
 * TierOwnerId returns the owner of one tier and, through relkind, what kind of
 * relation it is. It refuses a tier that row-level security applies to.
 *
 * The refusal is deliberate and has to be an error rather than a fallback to the
 * unexpanded query: the policies on the relation were applied by the rewriter,
 * before this hook ran, and reading the second tier without them would return
 * rows the policy hides. Losing the cold tier silently would be just as wrong in
 * the other direction, so the query fails instead.
 */
static Oid
TierOwnerId(Oid relationId, const char *tierDescription, char *relkind)
{
	HeapTuple	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));

	if (!HeapTupleIsValid(classTuple))
		elog(ERROR, "cache lookup failed for relation %u", relationId);

	Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTuple);
	Oid			ownerId = classForm->relowner;
	bool		hasRowSecurity = classForm->relrowsecurity;

	*relkind = classForm->relkind;

	ReleaseSysCache(classTuple);

	if (hasRowSecurity)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row-level security is not supported on a tiered table"),
				 errdetail("The %s %s has row-level security enabled.",
						   tierDescription, get_rel_name(relationId)),
				 errhint("Disable row-level security on the tier, or set "
						 "pg_lake_timeseries.expand_tiered_tables to off to "
						 "read the tiers separately.")));
	}

	return ownerId;
}


/*
 * RelationColumnNames returns the column names of a relation as the String list
 * an alias wants, with an empty name for a dropped column.
 */
static List *
RelationColumnNames(TupleDesc tupleDesc)
{
	List	   *colNames = NIL;

	for (int attIndex = 0; attIndex < tupleDesc->natts; attIndex++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, attIndex);

		if (att->attisdropped)
			colNames = lappend(colNames, makeString(pstrdup("")));
		else
			colNames = lappend(colNames, makeString(pstrdup(NameStr(att->attname))));
	}

	return colNames;
}


/*
 * TimeColumnNumber returns the attribute number of the time column of one tier.
 */
static AttrNumber
TimeColumnNumber(Oid relationId, char *timeColumn)
{
	AttrNumber	attributeNumber = get_attnum(relationId, timeColumn);

	if (attributeNumber == InvalidAttrNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("%s has no column %s", get_rel_name(relationId),
						quote_identifier(timeColumn))));
	}

	if (get_atttype(relationId, attributeNumber) != TIMESTAMPTZOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column %s of %s is no longer timestamp with time zone",
						quote_identifier(timeColumn),
						get_rel_name(relationId))));
	}

	return attributeNumber;
}

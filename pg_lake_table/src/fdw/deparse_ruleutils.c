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
 * deparse_ruleutils.h
 *		  Ruleutils based deparser
 *
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_type_d.h"

#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"

#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/pgduck/duckdb_identifier.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/fdw/deparse_ruleutils.h"
#include "pg_lake/parsetree/rte.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/storage/storage_credentials.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/table_type.h"

/*
 * ReplacePgLakeTableContext is used to pass down parameters and
 * return values to ReplacePgLakeTableWalker.
 */
typedef struct ReplacePgLakeTableContext
{
	/* OID of the read_table(..) function */
	Oid			readTableFunctionId;

	/* relation rtes to find */
	List	   *relationRteList;

	/* whether a ctid Var is present */
	bool		hasCtidVar;
}			ReplacePgLakeTableContext;

typedef struct FixCtidVarContext
{
	/* OID of the read_table(..) function */
	Oid			readTableFunctionId;

	/* range table of the current query */
	List	   *rtable;
}			FixCtidVarContext;

static char *DeparseQualifiedQuery(Query *query);
static bool ReplacePgLakeTableWalker(Node *node,
									 ReplacePgLakeTableContext * context);
static bool FixCtidVarWalker(Node *node,
							 FixCtidVarContext * context);

/*
 * ReplacePgLakeTableWithReadTableFunc replaces all occurrences of
 * pg_lake tables with read_table(..) function calls.
 */
List *
ReplacePgLakeTableWithReadTableFunc(Node *node)
{
	Oid			readTableFunctionId = ReadTableFunctionId();

	ReplacePgLakeTableContext context = {
		.readTableFunctionId = readTableFunctionId,
		.relationRteList = NIL,
		.hasCtidVar = false
	};

	ReplacePgLakeTableWalker(node, &context);

	/*
	 * If we have a ctid Var we should correct its attribute number, since
	 * functions cannot return system columns and we'll return it as the last
	 * attribute.
	 */
	if (context.hasCtidVar)
	{
		FixCtidVarContext ctidContext = {
			.readTableFunctionId = readTableFunctionId,
			.rtable = NIL
		};

		FixCtidVarWalker(node, &ctidContext);
	}

	return context.relationRteList;
}


/*
* RteListToOidList converts a list of RangeTblEntry to a list of Oids.
*/
List *
RteListToOidList(List *rteList)
{
	List	   *oidList = NIL;

	ListCell   *lc;

	foreach(lc, rteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		oidList = lappend_oid(oidList, rte->relid);
	}

	return oidList;
}


/*
* ReplacePgLakeTableWalker replaces the relation with a call to
* read_table function in the given node. It also stores the relation oid in
* the context.
*/
static bool
ReplacePgLakeTableWalker(Node *node, ReplacePgLakeTableContext * context)
{
	if (node == NULL)
	{
		return false;
	}

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, ReplacePgLakeTableWalker,
								 context, QTW_EXAMINE_RTES_BEFORE);
	}
	else if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varattno == SelfItemPointerAttributeNumber)
		{
			context->hasCtidVar = true;
		}
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node, ReplacePgLakeTableWalker,
									  context);
	}

	RangeTblEntry *rte = (RangeTblEntry *) node;

	if (IsAnyLakeForeignTable(rte))
	{
		char	   *qualifiedRelationName = GetQualifiedRelationName(rte->relid);

#ifdef USE_ASSERT_CHECKING
		elog(DEBUG2, "replacing relation %s with read_table('%s') in the query tree",
			 qualifiedRelationName, qualifiedRelationName);
#endif

		/* store the copy of the rte for later use */
		context->relationRteList = lappend(context->relationRteList, copyObject(rte));

		Const	   *tableNameParam = makeNode(Const);

		tableNameParam->constvalue = CStringGetTextDatum(qualifiedRelationName);
		tableNameParam->consttype = TEXTOID;
		tableNameParam->consttypmod = -1;
		tableNameParam->constbyval = false;
		tableNameParam->constlen = get_typlen(TEXTOID);
		tableNameParam->location = -1;

		Const	   *uniqueRelationId = makeNode(Const);
		int			uniqueRelationIdentifier = GetUniqueRelationIdentifier(rte);

		uniqueRelationId->constvalue = Int32GetDatum(uniqueRelationIdentifier);
		uniqueRelationId->consttype = INT4OID;
		uniqueRelationId->consttypmod = -1;
		uniqueRelationId->constbyval = true;
		uniqueRelationId->constlen = 4;
		uniqueRelationId->location = -1;

		/* create function expression to store our faux arguments in */
		FuncExpr   *readTableFuncExpr = makeNode(FuncExpr);

		readTableFuncExpr->funcid = context->readTableFunctionId;
		readTableFuncExpr->funcresulttype = RECORDOID;
		readTableFuncExpr->funcretset = true;
		readTableFuncExpr->location = -1;
		readTableFuncExpr->args = list_make2(tableNameParam, uniqueRelationId);

		RangeTblFunction *readTableFunction = makeNode(RangeTblFunction);

		readTableFunction->funcexpr = (Node *) readTableFuncExpr;

		/* set the column count to pass ruleutils checks */
		Relation	rel = RelationIdGetRelation(rte->relid);

		readTableFunction->funccolcount = RelationGetNumberOfAttributes(rel);
		RelationClose(rel);

		rte->functions = list_make1(readTableFunction);

		/* finally set the RTEKind */
		rte->rtekind = RTE_FUNCTION;



		return false;
	}

	return false;
}


/*
* FixCtidVarWalker fixes ctid Vars that point to a read_table call,
* since they cannot have attribute number -1.
*/
static bool
FixCtidVarWalker(Node *node, FixCtidVarContext * context)
{
	if (node == NULL)
	{
		return false;
	}

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		/* save the range table list to evaluate Vars */
		context->rtable = query->rtable;

		return query_tree_walker((Query *) node, FixCtidVarWalker,
								 context, 0);
	}
	else if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/*
		 * Adjust ctid varattno (-1) when referencing a read_table function
		 * call.		 *
		 *
		 * We do this primarily because pg_get_querydef errors for varattno ==
		 * -1 when referencing a function RTE, since set-returning functions
		 * do not have ctid or any other system columns.
		 *
		 * For now, we ignore varlevelsup > 0, since we only care about how
		 * the ctid is used in update scenarios. If that becomes relevant
		 * we'll need to keep a stack of range tables, similar to ruleutils.c.
		 */
		if (var->varattno == SelfItemPointerAttributeNumber &&
			var->varlevelsup == 0)
		{
			RangeTblEntry *varRte = rt_fetch(var->varno, context->rtable);

			/*
			 * Check whether the reference RTE is a read_table call.
			 */
			if (IsFunctionRTE(varRte, context->readTableFunctionId))
			{
				RangeTblFunction *readTableFunction = linitial(varRte->functions);

				/* make emitting the ctid column explicit in the RTE */
				readTableFunction->funccolcount++;

				varRte->eref->colnames = lappend(varRte->eref->colnames,
												 makeString("ctid"));

				/*
				 * BuildReadDataSourceQueryForTableScan puts ctid as the last
				 * column
				 */
				var->varattno = readTableFunction->funccolcount;

				/* reset varnosyn to avoid issues in get_variable  */
				var->varnosyn = 0;
			}
		}
	}

	return expression_tree_walker(node, FixCtidVarWalker,
								  context);
}


/*
 * ParseQuery - parse the query and return the query tree
 *
 * The function parses the query and returns the query tree.
 *
*/
Query *
ParseQuery(char *command, List *paramList)
{
	List	   *parseTreeList = pg_parse_query(command);

	if (list_length(parseTreeList) != 1)
		ereport(ERRCODE_FEATURE_NOT_SUPPORTED,
				(errmsg("pg_lake_table can only execute a single query"),
				 errdetail("The query \"%s\" contains multiple queries.", command)));

	int			paramCount = list_length(paramList);
	Oid		   *paramOidList = (Oid *) palloc(paramCount * sizeof(Oid));

	for (int i = 0; i < paramCount; i++)
	{
		Param	   *param = (Param *) list_nth(paramList, i);

		paramOidList[i] = param->paramtype;
	}

	RawStmt    *rawStmt = (RawStmt *) linitial(parseTreeList);
	List	   *queryTreeList =
		pg_analyze_and_rewrite_fixedparams(rawStmt, command, paramOidList,
										   list_length(paramList), NULL);

	/* we already checked parseTreeList length above */
	Assert(list_length(queryTreeList) == 1);

	return (Query *) linitial(queryTreeList);
}

/*
 * ruleutils prints variable names from eref. A user alias is only a prefix
 * of that list.
 */
typedef struct SchemaCacheEntry
{
	Oid			relid;
	DataFileSchema *schema;
}			SchemaCacheEntry;

typedef struct RewriteState
{
	List	   *schemaCache;
} RewriteState;

typedef struct RewriteContext
{
	struct RewriteContext *parent;
	Query	   *query;
	RewriteState *state;
}			RewriteContext;

static bool RewriteChildQueries(Node *node, RewriteContext * context);
static void RewriteQueryForDuckDBNames(Query *query, RewriteContext * parent);
static void ApplyDuckDBNames(RewriteContext * context);


static bool
NamesDifferOnlyByCase(const char *left, const char *right)
{
	if (left == NULL || right == NULL || left[0] == '\0' || right[0] == '\0')
		return false;

	if (strcmp(left, right) == 0)
		return false;

	return PgLakeAsciiNamesEqual(left, right);
}


static bool
ListHasAsciiCaseCollision(List *names)
{
	ListCell   *leftCell = NULL;

	foreach(leftCell, names)
	{
		const char *left = strVal(lfirst(leftCell));
		ListCell   *rightCell = lnext(names, leftCell);

		for (; rightCell != NULL; rightCell = lnext(names, rightCell))
		{
			if (NamesDifferOnlyByCase(left, strVal(lfirst(rightCell))))
				return true;
		}
	}

	return false;
}


static bool
NameCollidesInList(List *names, const char *name)
{
	ListCell   *cell = NULL;

	foreach(cell, names)
	{
		if (NamesDifferOnlyByCase(strVal(lfirst(cell)), name))
			return true;
	}

	return false;
}


static List *
ErefColumnNames(RangeTblEntry *rte)
{
	if (rte->eref != NULL)
		return rte->eref->colnames;

	return NIL;
}


static void
UniquifyCollidingNames(List **names)
{
	List	   *unique = NIL;
	ListCell   *cell = NULL;
	int			position = 1;

	if (names == NULL || *names == NIL || !ListHasAsciiCaseCollision(*names))
		return;

	foreach(cell, *names)
	{
		const char *name = strVal(lfirst(cell));

		if (NameCollidesInList(*names, name))
			unique = lappend(unique, makeString(psprintf("pg_lake_%d", position)));
		else
			unique = lappend(unique, makeString(pstrdup(name)));

		position++;
	}

	*names = unique;
}


static void
AssignErefNames(RangeTblEntry *rte, List *names)
{
	int			aliasCount = 0;
	int			index = 0;

	if (rte->eref == NULL || names == NIL)
		return;

	rte->eref->colnames = names;

	if (rte->alias == NULL || rte->alias->colnames == NIL)
		return;

	/*
	 * A column alias can name a prefix of the relation. Update that prefix in
	 * place and leave the rest of eref intact, including dropped-column
	 * placeholders and a trailing ctid.
	 */
	aliasCount = list_length(rte->alias->colnames);

	for (index = 0; index < aliasCount && index < list_length(names); index++)
	{
		lfirst(list_nth_cell(rte->alias->colnames, index)) =
			copyObject(list_nth(names, index));
	}
}


static List *
NonJunkResnames(Query *query)
{
	List	   *names = NIL;
	ListCell   *cell = NULL;

	foreach(cell, query->targetList)
	{
		TargetEntry *entry = lfirst(cell);

		if (!entry->resjunk)
			names = lappend(names, makeString(pstrdup(entry->resname)));
	}

	return names;
}


static bool
TargetListCollides(List *targetList)
{
	List	   *names = NIL;
	ListCell   *cell = NULL;

	foreach(cell, targetList)
	{
		TargetEntry *entry = lfirst(cell);

		if (!entry->resjunk)
			names = lappend(names, makeString(entry->resname));
	}

	return ListHasAsciiCaseCollision(names);
}


static bool
ResnameCollides(List *targetList, const char *name)
{
	ListCell   *cell = NULL;

	foreach(cell, targetList)
	{
		TargetEntry *entry = lfirst(cell);

		if (!entry->resjunk && NamesDifferOnlyByCase(entry->resname, name))
			return true;
	}

	return false;
}


static bool
IsLakeScanRte(RangeTblEntry *rte)
{
	return OidIsValid(rte->relid) &&
		(rte->rtekind == RTE_RELATION || rte->rtekind == RTE_FUNCTION) &&
		IsAnyLakeForeignTableById(rte->relid);
}


static DataFileSchema *
SchemaForRelation(RewriteState *state, Oid relid)
{
	ListCell   *cell = NULL;
	SchemaCacheEntry *entry = NULL;

	foreach(cell, state->schemaCache)
	{
		entry = lfirst(cell);

		if (entry->relid == relid)
			return entry->schema;
	}

	if (IsExternalIcebergTable(relid))
		EnsureStorageCredentialsForRelation(relid);

	entry = palloc(sizeof(SchemaCacheEntry));

	entry->relid = relid;
	entry->schema = GetDataFileSchemaForTable(relid);
	state->schemaCache = lappend(state->schemaCache, entry);

	return entry->schema;
}


static bool
NameLooksLikeFieldId(const char *name)
{
	const char *cursor = NULL;

	if (name == NULL || name[0] != 'c')
		return false;

	for (cursor = name + 1; *cursor != '\0'; cursor++)
	{
		if (*cursor < '0' || *cursor > '9')
			return false;
	}

	return cursor > name + 1;
}


static int
AliasCoveredCount(List *names)
{
	int			count = list_length(names);
	int			index = 0;
	int			live = 0;

	if (count > 0 && strcmp(strVal(llast(names)), "ctid") == 0)
		count--;

	for (index = 0; index < count; index++)
	{
		if (strVal(list_nth(names, index))[0] != '\0')
			live++;
	}

	return live;
}


static bool
ErefNeedsDuckRename(RangeTblEntry *rte)
{
	ListCell   *cell = NULL;
	List	   *names = ErefColumnNames(rte);

	if (ListHasAsciiCaseCollision(names))
		return true;

	foreach(cell, names)
	{
		if (NameLooksLikeFieldId(strVal(lfirst(cell))))
			return true;
	}

	if (rte->alias != NULL && rte->alias->colnames != NIL &&
		list_length(rte->alias->colnames) < AliasCoveredCount(names))
		return true;

	return false;
}


static List *
LakeScanNames(RangeTblEntry *rte, DataFileSchema * schema)
{
	List	   *erefNames = ErefColumnNames(rte);
	Relation	rel = RelationIdGetRelation(rte->relid);
	TupleDesc	tupdesc = RelationGetDescr(rel);
	List	   *names = NIL;
	int			attnum = 0;

	for (attnum = 0; attnum < tupdesc->natts && attnum < list_length(erefNames); attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum);
		const char *current = strVal(list_nth(erefNames, attnum));

		if (attr->attisdropped || current[0] == '\0')
			names = lappend(names, makeString(pstrdup("")));
		else
			names = lappend(names,
							makeString(DuckDBNameForColumnName(schema,
															   NameStr(attr->attname))));
	}

	for (; attnum < list_length(erefNames); attnum++)
		names = lappend(names, makeString(pstrdup(strVal(list_nth(erefNames, attnum)))));

	RelationClose(rel);

	return names;
}


static CommonTableExpr *
FindCte(RewriteContext * context, const char *ctename)
{
	RewriteContext *current = NULL;

	for (current = context; current != NULL; current = current->parent)
	{
		ListCell   *cell = NULL;

		foreach(cell, current->query->cteList)
		{
			CommonTableExpr *cte = lfirst(cell);

			if (strcmp(cte->ctename, ctename) == 0)
				return cte;
		}
	}

	return NULL;
}


static List *
CteOutputNames(CommonTableExpr *cte)
{
	if (cte->aliascolnames != NIL)
		return copyObject(cte->aliascolnames);

	return NonJunkResnames(castNode(Query, cte->ctequery));
}


static char *
PrintedVarName(Query *query, Node *expr)
{
	Var		   *var = NULL;
	RangeTblEntry *rte = NULL;
	List	   *names = NIL;

	if (expr == NULL || !IsA(expr, Var))
		return NULL;

	var = (Var *) expr;

	if (var->varlevelsup != 0 || var->varattno <= 0)
		return NULL;

	if (var->varno <= 0 || var->varno > list_length(query->rtable))
		return NULL;

	rte = rt_fetch(var->varno, query->rtable);
	names = ErefColumnNames(rte);

	if (var->varattno > list_length(names))
		return NULL;

	return pstrdup(strVal(list_nth(names, var->varattno - 1)));
}


static void
RewriteTargetListNames(Query *query)
{
	ListCell   *cell = NULL;

	if (!TargetListCollides(query->targetList))
		return;

	foreach(cell, query->targetList)
	{
		TargetEntry *entry = lfirst(cell);
		char	   *name = NULL;

		if (entry->resjunk || !ResnameCollides(query->targetList, entry->resname))
			continue;

		name = PrintedVarName(query, (Node *) entry->expr);

		if (name == NULL)
			name = psprintf("pg_lake_%d", entry->resno);

		entry->resname = name;
	}

	if (!TargetListCollides(query->targetList))
		return;

	foreach(cell, query->targetList)
	{
		TargetEntry *entry = lfirst(cell);

		if (entry->resjunk || !ResnameCollides(query->targetList, entry->resname))
			continue;

		entry->resname = psprintf("pg_lake_%d", entry->resno);
	}
}


static bool
RteWillUseDuckNames(RewriteContext * context, RangeTblEntry *rte)
{
	DataFileSchema *schema = NULL;

	if (rte == NULL || !ErefNeedsDuckRename(rte))
		return false;

	if (!IsLakeScanRte(rte) || !IsIcebergTable(rte->relid))
		return ListHasAsciiCaseCollision(ErefColumnNames(rte));

	schema = SchemaForRelation(context->state, rte->relid);

	return schema != NULL && DataFileSchemaHasCaseCollision(schema);
}


static RangeTblEntry *
JoinInputRte(Query *query, Node *input)
{
	if (input == NULL)
		return NULL;

	if (IsA(input, RangeTblRef))
		return rt_fetch(((RangeTblRef *) input)->rtindex, query->rtable);

	if (IsA(input, JoinExpr) && ((JoinExpr *) input)->rtindex > 0)
		return rt_fetch(((JoinExpr *) input)->rtindex, query->rtable);

	return NULL;
}


static AttrNumber
ErefAttnum(RangeTblEntry *rte, const char *name)
{
	ListCell   *cell = NULL;
	AttrNumber	attnum = 1;

	if (rte == NULL)
		return InvalidAttrNumber;

	foreach(cell, ErefColumnNames(rte))
	{
		if (strcmp(strVal(lfirst(cell)), name) == 0)
			return attnum;

		attnum++;
	}

	return InvalidAttrNumber;
}


static bool
ColumnTypeOfRte(RangeTblEntry *rte, AttrNumber attnum,
				Oid *typeId, int32 *typeMod, Oid *collation)
{
	TargetEntry *entry = NULL;
	ListCell   *cell = NULL;
	AttrNumber	visible = 1;

	*typeId = InvalidOid;
	*typeMod = -1;
	*collation = InvalidOid;

	if (rte == NULL || attnum <= 0)
		return false;

	if ((rte->rtekind == RTE_RELATION || rte->rtekind == RTE_FUNCTION) &&
		OidIsValid(rte->relid))
	{
		get_atttypetypmodcoll(rte->relid, attnum, typeId, typeMod, collation);
		return OidIsValid(*typeId);
	}

	if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL)
	{
		foreach(cell, rte->subquery->targetList)
		{
			entry = lfirst(cell);

			if (entry->resjunk)
				continue;

			if (visible == attnum)
			{
				*typeId = exprType((Node *) entry->expr);
				*typeMod = exprTypmod((Node *) entry->expr);
				*collation = exprCollation((Node *) entry->expr);
				return OidIsValid(*typeId);
			}

			visible++;
		}
	}

	if (rte->joinaliasvars != NIL && attnum <= list_length(rte->joinaliasvars))
	{
		Node	   *aliasVar = list_nth(rte->joinaliasvars, attnum - 1);

		if (aliasVar != NULL)
		{
			*typeId = exprType(aliasVar);
			*typeMod = exprTypmod(aliasVar);
			*collation = exprCollation(aliasVar);
			return OidIsValid(*typeId);
		}
	}

	return false;
}


static Expr *
MakeUsingEquality(RangeTblEntry *leftRte, AttrNumber leftAtt,
				  RangeTblEntry *rightRte, AttrNumber rightAtt,
				  Index leftVarno, Index rightVarno)
{
	Oid			leftType = InvalidOid;
	Oid			rightType = InvalidOid;
	int32		leftMod = -1;
	int32		rightMod = -1;
	Oid			leftColl = InvalidOid;
	Oid			rightColl = InvalidOid;
	Oid			operatorId = InvalidOid;
	Var		   *leftVar = NULL;
	Var		   *rightVar = NULL;
	OpExpr	   *operation = NULL;

	if (!ColumnTypeOfRte(leftRte, leftAtt, &leftType, &leftMod, &leftColl) ||
		!ColumnTypeOfRte(rightRte, rightAtt, &rightType, &rightMod, &rightColl))
		return NULL;

	operatorId = OpernameGetOprid(list_make1(makeString("=")), leftType, rightType);

	if (!OidIsValid(operatorId))
		return NULL;

	leftVar = makeVar(leftVarno, leftAtt, leftType, leftMod, leftColl, 0);
	rightVar = makeVar(rightVarno, rightAtt, rightType, rightMod, rightColl, 0);
	operation = (OpExpr *) make_opclause(operatorId, BOOLOID, false,
										 (Expr *) leftVar, (Expr *) rightVar,
										 InvalidOid, InvalidOid);
	operation->opfuncid = get_opcode(operatorId);
	operation->opresulttype = BOOLOID;

	return (Expr *) operation;
}


static void
ExpandOneUsing(RewriteContext * context, JoinExpr *join)
{
	RangeTblEntry *leftRte = NULL;
	RangeTblEntry *rightRte = NULL;
	Index		leftVarno = 0;
	Index		rightVarno = 0;
	List	   *equalities = NIL;
	ListCell   *cell = NULL;

	if (join->usingClause == NIL)
		return;

	leftRte = JoinInputRte(context->query, join->larg);
	rightRte = JoinInputRte(context->query, join->rarg);

	if (leftRte == NULL || rightRte == NULL)
		return;

	if (!RteWillUseDuckNames(context, leftRte) &&
		!RteWillUseDuckNames(context, rightRte))
		return;

	if (IsA(join->larg, RangeTblRef))
		leftVarno = ((RangeTblRef *) join->larg)->rtindex;
	else if (IsA(join->larg, JoinExpr))
		leftVarno = ((JoinExpr *) join->larg)->rtindex;

	if (IsA(join->rarg, RangeTblRef))
		rightVarno = ((RangeTblRef *) join->rarg)->rtindex;
	else if (IsA(join->rarg, JoinExpr))
		rightVarno = ((JoinExpr *) join->rarg)->rtindex;

	if (leftVarno == 0 || rightVarno == 0)
		return;

	foreach(cell, join->usingClause)
	{
		const char *name = strVal(lfirst(cell));
		AttrNumber	leftAtt = ErefAttnum(leftRte, name);
		AttrNumber	rightAtt = ErefAttnum(rightRte, name);
		Expr	   *equality = NULL;

		if (leftAtt == InvalidAttrNumber || rightAtt == InvalidAttrNumber)
			return;

		equality = MakeUsingEquality(leftRte, leftAtt, rightRte, rightAtt,
									 leftVarno, rightVarno);

		if (equality == NULL)
			return;

		equalities = lappend(equalities, equality);
	}

	join->quals = (Node *) (list_length(equalities) == 1 ?
							linitial(equalities) :
							make_andclause(equalities));
	join->usingClause = NIL;
	join->isNatural = false;
}


static void
ExpandUsingClauses(RewriteContext * context, Node *node)
{
	if (node == NULL)
		return;

	if (IsA(node, FromExpr))
	{
		ListCell   *cell = NULL;

		foreach(cell, ((FromExpr *) node)->fromlist)
			ExpandUsingClauses(context, lfirst(cell));
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr   *join = (JoinExpr *) node;

		ExpandUsingClauses(context, join->larg);
		ExpandUsingClauses(context, join->rarg);
		ExpandOneUsing(context, join);
	}
}


static void
ApplyDuckDBNames(RewriteContext * context)
{
	Query	   *query = context->query;
	ListCell   *cell = NULL;

	if (query->jointree != NULL)
		ExpandUsingClauses(context, (Node *) query->jointree);

	foreach(cell, query->rtable)
	{
		RangeTblEntry *rte = lfirst(cell);

		if (IsLakeScanRte(rte))
		{
			DataFileSchema *schema = NULL;

			if (RteWillUseDuckNames(context, rte))
				schema = SchemaForRelation(context->state, rte->relid);

			if (schema != NULL && DataFileSchemaHasCaseCollision(schema))
				AssignErefNames(rte, LakeScanNames(rte, schema));
		}
		else if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL &&
				 ListHasAsciiCaseCollision(ErefColumnNames(rte)))
		{
			List	   *names = NonJunkResnames(rte->subquery);

			UniquifyCollidingNames(&names);
			AssignErefNames(rte, names);
		}
		else if (rte->rtekind == RTE_CTE &&
				 ListHasAsciiCaseCollision(ErefColumnNames(rte)))
		{
			CommonTableExpr *cte = FindCte(context, rte->ctename);

			if (cte != NULL)
				AssignErefNames(rte, CteOutputNames(cte));
		}
		else if (rte->rtekind == RTE_JOIN &&
				 ListHasAsciiCaseCollision(ErefColumnNames(rte)))
		{
			List	   *names = copyObject(ErefColumnNames(rte));

			UniquifyCollidingNames(&names);
			AssignErefNames(rte, names);
		}
	}

	RewriteTargetListNames(query);
}


static bool
RewriteChildQueries(Node *node, RewriteContext * context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		RewriteQueryForDuckDBNames((Query *) node, context);
		return false;
	}

	return expression_tree_walker(node, RewriteChildQueries, context);
}


static char *
ReplacementResname(RewriteContext * context, Query *query, TargetEntry *entry)
{
	Var		   *var = NULL;
	RangeTblEntry *rte = NULL;
	DataFileSchema *schema = NULL;
	char	   *attributeName = NULL;

	if (!ResnameCollides(query->targetList, entry->resname))
		return pstrdup(entry->resname);

	if (entry->expr != NULL && IsA(entry->expr, Var))
	{
		var = (Var *) entry->expr;

		if (var->varlevelsup == 0 && var->varattno > 0 &&
			var->varno > 0 && var->varno <= list_length(query->rtable))
		{
			rte = rt_fetch(var->varno, query->rtable);

			if (IsLakeScanRte(rte) && IsIcebergTable(rte->relid))
			{
				schema = SchemaForRelation(context->state, rte->relid);
				attributeName = get_attname(rte->relid, var->varattno, true);

				if (schema != NULL && attributeName != NULL &&
					DataFileSchemaHasCaseCollision(schema))
					return DuckDBNameForColumnName(schema, attributeName);
			}
		}
	}

	return psprintf("pg_lake_%d", entry->resno);
}


static List *
CaseFoldSafeOutputNames(RewriteContext * context, Query *query)
{
	List	   *names = NIL;
	ListCell   *cell = NULL;

	if (query == NULL || !TargetListCollides(query->targetList))
		return NIL;

	foreach(cell, query->targetList)
	{
		TargetEntry *entry = lfirst(cell);

		if (!entry->resjunk)
			names = lappend(names,
							makeString(ReplacementResname(context, query, entry)));
	}

	UniquifyCollidingNames(&names);

	return names;
}


/*
 * RewriteQueryForDuckDBNames gives every DuckDB scope unique column names
 * before ruleutils prints the query. Iceberg columns that differ only by
 * ASCII case become c<field-id>. Other colliding outputs become pg_lake_N.
 * PostgreSQL catalogs and Iceberg metadata keep the real names.
 */
static void
RewriteQueryForDuckDBNames(Query *query, RewriteContext * parent)
{
	RewriteState *state = NULL;
	RewriteContext context = {0};
	ListCell   *cell = NULL;

	if (query == NULL)
		return;

	if (parent != NULL)
		state = parent->state;
	else
		state = palloc0(sizeof(RewriteState));

	context.parent = parent;
	context.query = query;
	context.state = state;

	foreach(cell, query->cteList)
	{
		CommonTableExpr *cte = lfirst(cell);
		Query	   *cteQuery = castNode(Query, cte->ctequery);

		/*
		 * A recursive reference is inside the body, so the CTE's output names
		 * have to be unique before the body is rewritten.
		 */
		if (cte->aliascolnames == NIL)
		{
			List	   *outputNames = CaseFoldSafeOutputNames(&context, cteQuery);

			if (outputNames != NIL)
				cte->aliascolnames = outputNames;
		}
		else
			UniquifyCollidingNames(&cte->aliascolnames);

		RewriteQueryForDuckDBNames(cteQuery, &context);
	}

	query_tree_walker(query, RewriteChildQueries, &context,
					  QTW_IGNORE_CTE_SUBQUERIES);

	ApplyDuckDBNames(&context);
}


/*
 * PreparePGDuckSQLTemplate - prepare the query template for duckdb pushdown.
 * We use the Postgres' ruleutils.c to deparse the query as it is a lot more
 * capable than the fdw's deparser.
*/
char *
PreparePGDuckSQLTemplate(Query *query)
{
	RewriteQueryForDuckDBNames(query, NULL);

	char	   *newQuery = DeparseQualifiedQuery(query);

#ifdef USE_ASSERT_CHECKING
	elog(DEBUG2, "deparsed pg_lake_table query: %s", newQuery);
#endif

	return newQuery;
}


/*
* DeparseQualifiedQuery - deparse the query using the Postgres' ruleutils.c
* with pg_catalog search_path.
*/
static char *
DeparseQualifiedQuery(Query *query)
{
	int			save_nestlevel = NewGUCNestLevel();
	StringInfo	searchPath = makeStringInfo();

	appendStringInfoString(searchPath, "pg_catalog");
	appendStringInfoString(searchPath, "," PG_LAKE_INTERNAL_NSP);

	/*
	 * Add PostGIS schema as well, since the equivalent duckdb_spatial
	 * functions will be in the global namespace.
	 */
	if (IsExtensionCreated(Postgis))
	{
		char	   *postgisSchemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		appendStringInfo(searchPath, ",%s", postgisSchemaName);
	}

	(void) set_config_option("search_path", searchPath->data,
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	/*
	 * Force all identifiers to be double-quoted so that DuckDB-reserved
	 * keywords (PIVOT, QUALIFY, LAMBDA, etc.) that PostgreSQL leaves unquoted
	 * are properly escaped for pgduck_server. Use set_config_option with
	 * GUC_ACTION_SAVE so AtEOXact_GUC restores the prior value even if
	 * pg_get_querydef throws.
	 */
	(void) set_config_option("quote_all_identifiers", "on",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	bool		pretty = false;
	char	   *newQuery = pg_get_querydef(query, pretty);

	/*
	 * Restore the GUC variable search_path we set above.
	 */
	AtEOXact_GUC(true, save_nestlevel);

	return newQuery;
}

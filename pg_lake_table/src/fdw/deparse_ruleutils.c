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

#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteManip.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/typcache.h"

#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/fdw/deparse_ruleutils.h"
#include "pg_lake/parsetree/rte.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/util/rel_utils.h"

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

/*
 * CompositeUnnestRte is an unnest(..) RTE over an array of a composite type,
 * together with the composite type that unnest(..) returns.
 */
typedef struct CompositeUnnestRte
{
	RangeTblEntry *rte;
	Oid			elementTypeId;
}			CompositeUnnestRte;

static char *DeparseQualifiedQuery(Query *query);
static bool ReplacePgLakeTableWalker(Node *node,
									 ReplacePgLakeTableContext * context);
static bool FixCtidVarWalker(Node *node,
							 FixCtidVarContext * context);
static bool IsCompositeUnnestRte(RangeTblEntry *rte, Oid *elementTypeId);
static bool FindCompositeUnnestRteWalker(Node *node, List **unnestRteList);
static void ExpandCompositeUnnestRte(RangeTblEntry *rte, Oid elementTypeId);

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
 * IsCompositeUnnestRte returns whether the given RTE is an unnest(..) call over
 * an array of a composite type that the rewrite below can handle, and if so also
 * returns the composite type.
 */
static bool
IsCompositeUnnestRte(RangeTblEntry *rte, Oid *elementTypeId)
{
	if (rte->rtekind != RTE_FUNCTION || list_length(rte->functions) != 1)
		return false;

	/* WITH ORDINALITY adds a column that the rewrite below cannot produce */
	if (rte->funcordinality)
		return false;

	RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(rte->functions);

	/* a column definition list is deparsed with its own syntax */
	if (rtfunc->funccolnames != NIL)
		return false;

	if (!IsA(rtfunc->funcexpr, FuncExpr) ||
		((FuncExpr *) rtfunc->funcexpr)->funcid != F_UNNEST_ANYARRAY)
		return false;

	/*
	 * A domain over a composite type is expanded per field just the same, so
	 * look through the domain. The field references we build below need the
	 * composite type itself in any case, since that is what carries the
	 * fields.
	 */
	Oid			typeId = getBaseType(exprType(rtfunc->funcexpr));

	if (get_typtype(typeId) != TYPTYPE_COMPOSITE)
		return false;

	TupleDesc	tupleDesc = lookup_rowtype_tupdesc(typeId, -1);
	bool		anyFieldDropped = false;

	for (int fieldNumber = 0; fieldNumber < tupleDesc->natts; fieldNumber++)
		anyFieldDropped |= TupleDescAttr(tupleDesc, fieldNumber)->attisdropped;

	int			fieldCount = tupleDesc->natts;

	ReleaseTupleDesc(tupleDesc);

	/*
	 * A dropped field still occupies a column of the RTE, but has no name we
	 * could put in the alias list, so leave these RTEs alone and let the
	 * query fail to bind in DuckDB as it does today.
	 */
	if (anyFieldDropped)
		return false;

	/*
	 * The rewrite below leaves behind an inner unnest(..) RTE that names the
	 * single struct column DuckDB returns rather than one column per field.
	 * Recognizing it here keeps a second pass over the same tree from nesting
	 * another subquery.
	 */
	if (list_length(rte->eref->colnames) != fieldCount)
		return false;

	*elementTypeId = typeId;

	return true;
}


/*
 * FindCompositeUnnestRteWalker collects every unnest(..) RTE over an array of a
 * composite type in the query tree.
 */
static bool
FindCompositeUnnestRteWalker(Node *node, List **unnestRteList)
{
	if (node == NULL)
		return false;

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
		return query_tree_walker((Query *) node, FindCompositeUnnestRteWalker,
								 unnestRteList, QTW_EXAMINE_RTES_BEFORE);

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;
		Oid			elementTypeId = InvalidOid;

		if (IsCompositeUnnestRte(rte, &elementTypeId))
		{
			CompositeUnnestRte *unnestRte = palloc0(sizeof(CompositeUnnestRte));

			unnestRte->rte = rte;
			unnestRte->elementTypeId = elementTypeId;

			*unnestRteList = lappend(*unnestRteList, unnestRte);
		}

		/* the walker descends into the RTE itself */
		return false;
	}

	return expression_tree_walker(node, FindCompositeUnnestRteWalker,
								  unnestRteList);
}


/*
 * ExpandCompositeUnnestRte replaces an unnest(..) RTE over an array of a
 * composite type with an equivalent lateral subquery that selects the fields of
 * the composite one by one:
 *
 *     (SELECT (u.u).code, (u.u).charges FROM unnest(t.arr) u(u)) a
 *
 * The subquery has one column per field, in the same order, so the Vars that
 * reference the RTE, and the aliases of any join above it, stay valid.
 */
static void
ExpandCompositeUnnestRte(RangeTblEntry *rte, Oid elementTypeId)
{
	RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(rte->functions);
	TupleDesc	tupleDesc = lookup_rowtype_tupdesc(elementTypeId, -1);
	List	   *colnames = rte->eref->colnames;

	/* the unnest(..) call, and anything it references, moves a level down */
	IncrementVarSublevelsUp(rtfunc->funcexpr, 1, 0);

	/* to DuckDB, unnest(..) of an array of structs returns a single column */
	rtfunc->funccolcount = 1;

	RangeTblEntry *unnestRte = makeNode(RangeTblEntry);

	unnestRte->rtekind = RTE_FUNCTION;
	unnestRte->functions = rte->functions;

	/*
	 * The inner RTE is the only thing in its own FROM clause, so it never
	 * needs the LATERAL keyword; the outer RTE keeps the flag that covers the
	 * correlation.
	 */
	unnestRte->lateral = false;
	unnestRte->inFromCl = true;
	unnestRte->eref = makeAlias(rte->eref->aliasname,
								list_make1(makeString(rte->eref->aliasname)));

	List	   *targetList = NIL;

	for (int fieldNumber = 1; fieldNumber <= tupleDesc->natts; fieldNumber++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, fieldNumber - 1);
		FieldSelect *fieldSelect = makeNode(FieldSelect);

		fieldSelect->arg = (Expr *) makeVar(1, 1, elementTypeId, -1,
											InvalidOid, 0);
		fieldSelect->fieldnum = fieldNumber;
		fieldSelect->resulttype = attr->atttypid;
		fieldSelect->resulttypmod = attr->atttypmod;
		fieldSelect->resultcollid = attr->attcollation;

		/*
		 * Name the column the way the RTE does, so that references to it keep
		 * resolving once the subquery names its own columns. The gate above
		 * checked that there is a name for every field.
		 */
		char	   *colname = strVal(list_nth(colnames, fieldNumber - 1));

		targetList = lappend(targetList,
							 makeTargetEntry((Expr *) fieldSelect, fieldNumber,
											 pstrdup(colname), false));
	}

	ReleaseTupleDesc(tupleDesc);

	RangeTblRef *rangeTableRef = makeNode(RangeTblRef);

	rangeTableRef->rtindex = 1;

	Query	   *subquery = makeNode(Query);

	subquery->commandType = CMD_SELECT;
	subquery->querySource = QSRC_ORIGINAL;
	subquery->canSetTag = true;
	subquery->rtable = list_make1(unnestRte);
	subquery->jointree = makeFromExpr(list_make1(rangeTableRef), NULL);
	subquery->targetList = targetList;

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->functions = NIL;
}


/*
 * ExpandCompositeUnnest rewrites unnest(..) calls over arrays of a composite
 * type into a form that means the same thing to DuckDB.
 *
 * PostgreSQL expands a function RTE that returns a composite type into one
 * column per field, and ruleutils always emits the full column alias list:
 *
 *     unnest(t.arr) a("code", "charges", "is_waived")
 *
 * DuckDB's unnest() of an array of structs produces a single struct column, so
 * it binds only the first alias: a reference to any other field fails to bind,
 * and a reference to the first field silently returns the whole struct. We
 * therefore select the fields out of that struct ourselves.
 */
static void
ExpandCompositeUnnest(Query *query)
{
	List	   *unnestRteList = NIL;

	FindCompositeUnnestRteWalker((Node *) query, &unnestRteList);

	/*
	 * Rewrite the RTEs after the walk, since the subqueries we build contain
	 * the very unnest(..) calls we are looking for.
	 */
	ListCell   *lc;

	foreach(lc, unnestRteList)
	{
		CompositeUnnestRte *unnestRte = (CompositeUnnestRte *) lfirst(lc);

		ExpandCompositeUnnestRte(unnestRte->rte, unnestRte->elementTypeId);
	}
}


/*
 * PreparePGDuckSQLTemplate - prepare the query template for duckdb pushdown.
 * We use the Postgres' ruleutils.c to deparse the query as it is a lot more
 * capable than the fdw's deparser.
 *
 * The query tree is rewritten in place, so callers must hand over a tree they
 * own rather than a live one.
*/
char *
PreparePGDuckSQLTemplate(Query *query)
{
	ExpandCompositeUnnest(query);

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

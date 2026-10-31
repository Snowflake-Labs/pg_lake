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

#include "pg_lake/pgduck/duckdb_identifier.h"
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

/* Shared state while we pick DuckDB-safe column names for one statement. */
typedef struct DuckDBNamesState
{
	int			nextNameId;
}			DuckDBNamesState;

static void RewriteQueryForDuckDBNames(Query *query, DuckDBNamesState * state);


static bool
ListHasCaseCollision(List *names)
{
	int			count = list_length(names);
	const char **array = NULL;
	bool		collision = false;

	if (count < 2)
		return false;

	array = palloc(count * sizeof(char *));

	for (int i = 0; i < count; i++)
		array[i] = strVal(list_nth(names, i));

	collision = NamesHaveCaseCollision(array, count);
	pfree(array);

	return collision;
}


static bool
NameCollides(List *names, const char *name)
{
	ListCell   *cell = NULL;

	foreach(cell, names)
	{
		const char *other = strVal(lfirst(cell));

		if (strcmp(other, name) != 0 && PgLakeAsciiNamesEqual(other, name))
			return true;
	}

	return false;
}


static bool
NameTaken(List *names, const char *name)
{
	ListCell   *cell = NULL;

	foreach(cell, names)
	{
		if (PgLakeAsciiNamesEqual(strVal(lfirst(cell)), name))
			return true;
	}

	return false;
}


static char *
FreshName(DuckDBNamesState * state, List *names, List *chosen)
{
	for (;;)
	{
		char	   *name = psprintf("pg_lake_%d", ++state->nextNameId);

		if (!NameTaken(names, name) && !NameTaken(chosen, name))
			return name;
	}
}


/* Give a new name to every column whose name differs from another only by case. */
static List *
UniqueNames(DuckDBNamesState * state, List *names)
{
	List	   *unique = NIL;
	ListCell   *cell = NULL;

	foreach(cell, names)
	{
		char	   *name = strVal(lfirst(cell));

		if (name[0] != '\0' && NameCollides(names, name))
			name = FreshName(state, names, unique);

		unique = lappend(unique, makeString(pstrdup(name)));
	}

	return unique;
}


/* Print the new names as a full alias list, so DuckDB matches them by position. */
static void
RenameRteColumns(DuckDBNamesState * state, RangeTblEntry *rte)
{
	List	   *names = NIL;

	if (!ListHasCaseCollision(rte->eref->colnames))
		return;

	names = UniqueNames(state, rte->eref->colnames);
	rte->eref->colnames = names;

	if (rte->alias == NULL)
		rte->alias = makeAlias(rte->eref->aliasname, NIL);

	rte->alias->colnames = copyObject(names);
}


static List *
JoinTreeNames(Query *query, Node *node)
{
	if (IsA(node, RangeTblRef))
	{
		RangeTblEntry *rte = rt_fetch(((RangeTblRef *) node)->rtindex, query->rtable);

		return list_copy(rte->eref->colnames);
	}

	if (IsA(node, JoinExpr))
	{
		JoinExpr   *join = (JoinExpr *) node;
		RangeTblEntry *rte = rt_fetch(join->rtindex, query->rtable);
		List	   *names = list_copy(rte->eref->colnames);

		names = list_concat(names, JoinTreeNames(query, join->larg));
		names = list_concat(names, JoinTreeNames(query, join->rarg));

		return list_concat(names, join->usingClause);
	}

	return NIL;
}


/* ruleutils pushes USING names into both join inputs, so keep them unique too. */
static void
RenameUsingColumns(DuckDBNamesState * state, Query *query, Node *node)
{
	ListCell   *cell = NULL;

	if (IsA(node, FromExpr))
	{
		foreach(cell, ((FromExpr *) node)->fromlist)
			RenameUsingColumns(state, query, lfirst(cell));
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr   *join = (JoinExpr *) node;
		List	   *names = NIL;
		List	   *usingNames = NIL;

		RenameUsingColumns(state, query, join->larg);
		RenameUsingColumns(state, query, join->rarg);

		if (join->usingClause == NIL)
			return;

		names = list_concat(JoinTreeNames(query, join->larg),
							JoinTreeNames(query, join->rarg));
		names = list_concat(names, join->usingClause);

		if (!ListHasCaseCollision(names))
			return;

		foreach(cell, join->usingClause)
		{
			char	   *name = strVal(lfirst(cell));

			if (NameCollides(names, name))
				name = FreshName(state, names, usingNames);

			usingNames = lappend(usingNames, makeString(name));
		}

		join->usingClause = usingNames;
	}
}


static bool
RewriteNamesWalker(Node *node, DuckDBNamesState * state)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		RewriteQueryForDuckDBNames((Query *) node, state);
		return false;
	}

	return expression_tree_walker(node, RewriteNamesWalker, state);
}


/* DuckDB ignores case, so give every FROM item, CTE and USING list case-unique names. */
static void
RewriteQueryForDuckDBNames(Query *query, DuckDBNamesState * state)
{
	ListCell   *cell = NULL;

	foreach(cell, query->cteList)
	{
		CommonTableExpr *cte = lfirst(cell);

		if (ListHasCaseCollision(cte->ctecolnames))
			cte->aliascolnames = UniqueNames(state, cte->ctecolnames);
	}

	query_tree_walker(query, RewriteNamesWalker, state, 0);

	foreach(cell, query->rtable)
	{
		RangeTblEntry *rte = lfirst(cell);

		if (rte->rtekind == RTE_FUNCTION || rte->rtekind == RTE_SUBQUERY ||
			rte->rtekind == RTE_CTE || rte->rtekind == RTE_VALUES)
			RenameRteColumns(state, rte);
	}

	if (query->jointree != NULL)
		RenameUsingColumns(state, query, (Node *) query->jointree);
}


/*
 * PreparePGDuckSQLTemplate - prepare the query template for duckdb pushdown.
 * We use the Postgres' ruleutils.c to deparse the query as it is a lot more
 * capable than the fdw's deparser.
*/
char *
PreparePGDuckSQLTemplate(Query *query)
{
	DuckDBNamesState namesState = {0};

	RewriteQueryForDuckDBNames(query, &namesState);

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

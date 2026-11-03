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

#include "pg_lake/planner/dbt.h"

#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/guc.h"


/*
 * AddForeignTablesToRelkindInArrayFilter replaces relkind = ANY (['r'::"char", 'p'::"char"])
 * to relkind = ANY (['r'::"char", 'p'::"char", 'f'::"char"]). This is only done when
 * the application_name is set to "dbt". dbt queries the pg_tables view, which has
 * the above filter in its definition, to get the list of tables. We replace the filter
 * to include foreign tables for dbt to work with pg_lake tables.
 */
bool
AddForeignTablesToRelkindInArrayFilter(Node *node, void *context)
{
	Assert(IsDBTApplicationName());

	DbtApplicationQueryContext *dbtContext = (DbtApplicationQueryContext *) context;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		DbtApplicationQueryContext queryContext = *dbtContext;

		/* A nested query must not overwrite its parent's range table. */
		queryContext.rtable = query->rtable;

		return query_tree_walker((Query *) node,
								 AddForeignTablesToRelkindInArrayFilter,
								 &queryContext,
								 0);
	}
	else if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *scalarArrayExpr = (ScalarArrayOpExpr *) node;

		Expr	   *leftArg = (Expr *) linitial(scalarArrayExpr->args);

		if (!IsA(leftArg, Var))
		{
			return false;
		}

		Expr	   *rightArg = (Expr *) lsecond(scalarArrayExpr->args);

		if (!IsA(rightArg, ArrayExpr))
		{
			return false;
		}

		Var		   *var = (Var *) leftArg;

		/* Only resolve Vars that belong to the current query's range table. */
		if (var->varlevelsup != 0 || var->varno <= 0 ||
			var->varno > list_length(dbtContext->rtable))
		{
			return false;
		}

		RangeTblEntry *referencedRte = rt_fetch(var->varno, dbtContext->rtable);

		/* left argument should be "pg_class.relkind". */
		if (referencedRte == NULL ||
			referencedRte->rtekind != RTE_RELATION ||
			referencedRte->relid != RelationRelationId ||
			var->varattno != Anum_pg_class_relkind)
		{
			return false;
		}

		ArrayExpr  *arrayExpr = (ArrayExpr *) rightArg;

		if (arrayExpr->element_typeid != CHAROID)
		{
			return false;
		}

		List	   *elements = arrayExpr->elements;

		if (list_length(elements) != 2)
		{
			return false;
		}

		Const	   *const1 = linitial(elements);
		Const	   *const2 = lsecond(elements);

		if (const1 && const2 &&
			const1->consttype == CHAROID &&
			const2->consttype == CHAROID &&
			DatumGetChar(const1->constvalue) == 'r' &&
			DatumGetChar(const2->constvalue) == 'p')
		{
			Const	   *const3 = copyObject(const1);

			const3->constvalue = CharGetDatum('f');

			arrayExpr->elements = lappend(arrayExpr->elements, const3);

			/*
			 * Returning true here would abort the whole walk and leave any
			 * later relkind filter in the statement unrewritten.
			 */
			return false;
		}

		return false;
	}

	return expression_tree_walker(node,
								  AddForeignTablesToRelkindInArrayFilter,
								  context);
}


/*
 * IsDBTApplicationName checks whether the application_name is set to "dbt".
 */
bool
IsDBTApplicationName(void)
{
	return application_name && strcmp(application_name, "dbt") == 0;
}


/*
 * IsDBTTempTable returns whether the given table is temporary and application_name is
 * set to "dbt".
 */
bool
IsDBTTempTable(RangeVar *rangeVar)
{
	if (!IsDBTApplicationName())
	{
		return false;
	}

	/*
	 * dbt should not use iceberg default_access_method for temp or unlogged
	 * tables to prevent disturbing errors.
	 */
	return rangeVar->relpersistence == RELPERSISTENCE_TEMP ||
		rangeVar->relpersistence == RELPERSISTENCE_UNLOGGED;
}

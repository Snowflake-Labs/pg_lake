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

#include "access/table.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/planner/insert_select.h"
#include "pg_lake/planner/query_pushdown.h"
#include "pg_lake/util/numeric.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/rel_utils.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"


static RangeTblEntry *GetSelectRteFromInsertSelect(Query *query);
static RangeTblEntry *GetInsertRteFromInsertSelect(Query *query);

/* pg_lake_table.enable_insert_select_pushdown setting */
bool		EnableInsertSelectPushdown = true;

/*
 * IsPushdownableInsertSelectQuery checks whether the given query is an INSERT..SELECT
 * that can be delegated.
 */
bool
IsPushdownableInsertSelectQuery(Query *query)
{
	if (!IsInsertSelectQuery(query))
		return false;

	if (!EnableInsertSelectPushdown)
		return false;

	if (query->hasModifyingCTE)
	{
		ereport(DEBUG4,
				(errmsg("INSERT..SELECT with modifying CTE is not pushdownable")));
		return false;
	}

	/*
	 * currently we do not support RETURNING (could be doable by scanning new
	 * files)
	 */
	if (query->returningList != NIL)
	{
		ereport(DEBUG4,
				(errmsg("INSERT..SELECT with RETURNING is not pushdownable")));
		return false;
	}


	/*
	 * we generally don't support ON CONFLICT, don't bug the pushdown logic
	 * with it
	 */
	if (query->onConflict != NULL)
	{
		ereport(DEBUG4,
				(errmsg("INSERT..SELECT with ON CONFLICT is not pushdownable")));
		return false;
	}

	Oid			insertIntoRelid = GetInsertRelidFromInsertSelect(query);

	if (!IsWritablePgLakeTable(insertIntoRelid) && !IsWritableIcebergTable(insertIntoRelid))
	{
		ereport(DEBUG4,
				(errmsg("INSERT..SELECT into read-only pg_lake table is not pushdownable")));

		return false;
	}

	Relation	insertRelation = RelationIdGetRelation(insertIntoRelid);

	bool		allowDefaultConsts = true;

	if (!RelationSuitableForPushdown(insertRelation, allowDefaultConsts))
	{
		ereport(DEBUG4, (errmsg("INSERT..SELECT relation is not suitable for pushdown")));

		RelationClose(insertRelation);
		return false;
	}

	PgLakeTableProperties properties =
		GetPgLakeTableProperties(insertIntoRelid);

	if (!RelationColumnsSuitableForPushdown(insertRelation, properties.format))
	{
		ereport(DEBUG4, (errmsg("INSERT..SELECT columns are not suitable for pushdown")));

		RelationClose(insertRelation);
		return false;
	}

	const char *partitionBy = GetIcebergTablePartitionByOption(insertIntoRelid);

	if (partitionBy != NULL)
	{
		ereport(DEBUG4,
				(errmsg("INSERT..SELECT into partitioned table is not pushdownable")));
		RelationClose(insertRelation);
		return false;
	}

	RelationClose(insertRelation);

	/* check whether SELECT is pushdownable */
	RangeTblEntry *selectRte = GetSelectRteFromInsertSelect(query);

	if (!FullQueryIsPushdownable(selectRte->subquery))
	{
		ereport(DEBUG4, (errmsg("SELECT part of INSERT..SELECT is not pushdownable")));
		return false;
	}

	if (HasNotShippableExpression((Node *) query->targetList))
	{
		ereport(DEBUG4, (errmsg("INSERT target list is not pushdownable")));

		/* some target list expressions cannot be pushed down */
		return false;
	}

	ListCell   *cteCell = NULL;

	foreach(cteCell, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);

		if (!FullQueryIsPushdownable((Query *) cte->ctequery))
		{
			ereport(DEBUG4, (errmsg("CTE in INSERT..SELECT is not pushdownable")));
			return false;
		}
	}

	return true;
}


/*
 * IsInsertSelectQuery returns whether the given query is an INSERT..SELECT.
 */
bool
IsInsertSelectQuery(Query *query)
{
	CmdType		commandType = query->commandType;

	if (commandType != CMD_INSERT)
		return false;

	if (query->jointree == NULL || !IsA(query->jointree, FromExpr))
		return false;

	if (list_length(query->jointree->fromlist) != 1)
		return false;

	RangeTblEntry *subqueryRte = GetSelectRteFromInsertSelect(query);

	if (subqueryRte == NULL)
		return false;

	return true;
}


/*
 * GetSelectRteFromInsertSelect extracts the RTE of the SELECT part of
 * an INSERT..SELECT.
 */
RangeTblEntry *
GetSelectRteFromInsertSelect(Query *query)
{
	RangeTblRef *rangeTableRef = linitial(query->jointree->fromlist);

	if (!IsA(rangeTableRef, RangeTblRef))
		return NULL;

	RangeTblEntry *subqueryRte = rt_fetch(rangeTableRef->rtindex, query->rtable);

	if (subqueryRte->rtekind != RTE_SUBQUERY)
		return NULL;

	Assert(subqueryRte->subquery != NULL);

	return subqueryRte;
}


/*
 * GetInsertRteFromInsertSelect extracts the RTE of the INSERT part of
 * an INSERT..SELECT.
 */
static RangeTblEntry *
GetInsertRteFromInsertSelect(Query *query)
{
	return rt_fetch(query->resultRelation, query->rtable);
}


/*
 * GetInsertRelidFromInsertSelect returns the OID of the relation point to by
 * INSERT..SELECT.
 */
Oid
GetInsertRelidFromInsertSelect(Query *query)
{
	RangeTblEntry *insertRte = GetInsertRteFromInsertSelect(query);

	return insertRte->relid;
}


/*
 * TransformPushdownableInsertSelect transforms an INSERT..SELECT command
 * into a SELECT command which outputs the columns in the order of the target
 * table, such that the output can be written to a Parquet file.
 */
void
TransformPushdownableInsertSelect(Query *query)
{
	RangeTblEntry *selectRte = GetSelectRteFromInsertSelect(query);

	/*
	 * Poof! Become a SELECT with the subquery RTE referenced by the FROM
	 * clause.
	 */
	query->commandType = CMD_SELECT;
	selectRte->inFromCl = true;

	/*
	 * The target list of an INSERT..SELECT is in table column order, but
	 * excludes columns that do not have a default value. Expand the list by
	 * adding the remaining columns with NULL constants.
	 */
	RangeTblEntry *insertRte = GetInsertRteFromInsertSelect(query);
	Oid			insertRelid = insertRte->relid;
	Relation	insertRel = table_open(insertRelid, RowExclusiveLock);
	TupleDesc	relationTupleDesc = RelationGetDescr(insertRel);

	List	   *newTargetList = NIL;
	ListCell   *targetEntryCell = list_head(query->targetList);

	for (int columnIndex = 0; columnIndex < relationTupleDesc->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(relationTupleDesc, columnIndex);

		if (column->attisdropped)
			continue;

		TargetEntry *targetEntry = NULL;

		/*
		 * this is false when SELECT has less columns than the insert target
		 * list
		 */
		if (targetEntryCell != NULL)
		{
			targetEntry = lfirst(targetEntryCell);
		}

		if (targetEntry &&
			strcmp(targetEntry->resname, NameStr(column->attname)) == 0)
		{
			/* column has a target list entry, use it */
			newTargetList = lappend(newTargetList, targetEntry);

			/* fix resno */
			targetEntry->resno = columnIndex + 1;

			/* move on to the next item in the INSERT target list */
			targetEntryCell = lnext(query->targetList, targetEntryCell);
		}
		else
		{
			/* adding a regular output column */
			bool		resjunk = false;

			/*
			 * DuckDB does not support JSONB, so we need to convert it to
			 * JSON. We normally do these conversions in rewrite_query.c, but
			 * we need to do it here as well because the transformation to a
			 * SELECT query after before the rewrite phase.
			 */
			Oid			nullAttributeOid =
				column->atttypid == JSONBOID ?
				JSONOID : column->atttypid;

			/* create a NULL value of the column type */
			Const	   *nullConst = makeNullConst(nullAttributeOid,
												  column->atttypmod,
												  column->attcollation);

			/* column does not have a target list entry, create one */
			targetEntry = makeTargetEntry((Expr *) nullConst,
										  columnIndex + 1,
										  NameStr(column->attname),
										  resjunk);
			newTargetList = lappend(newTargetList, targetEntry);
		}
	}

	/* replace the target list with the expanded list */
	query->targetList = newTargetList;

	table_close(insertRel, NoLock);
}


/*
* RelationSuitableForPushdown checks whether a relation is suitable for pushdown.
* It does not check whether the columns are suitable for pushdown, see
* RelationColumnsSuitableForPushdown() for that.
*/
bool
RelationSuitableForPushdown(Relation relation, bool allowDefaultConsts)
{
	/*
	 * Constraints are not checked if we push down. This check also covers
	 * generated columns.
	 */
	if (relation->rd_att->constr != NULL)
	{
		TupleConstr *constr = relation->rd_att->constr;

		if (constr->has_not_null || constr->has_generated_stored ||
			constr->missing != NULL || constr->num_check > 0)
		{
			return false;
		}

		if (allowDefaultConsts && constr->num_defval > 0)
		{
			return true;
		}

		return false;
	}

	/*
	 * Partition bounds are not checked if we push down.
	 */
	if (relation->rd_rel->relispartition)
		return false;

	/*
	 * Triggers are not executed if we push down.
	 */
	if (relation->trigdesc != NULL)
		return false;

	return true;
}


/*
* RelationColumnsSuitableForPushdown checks whether the columns of a relation
* are suitable for pushdown.
*/
bool
RelationColumnsSuitableForPushdown(Relation relation, CopyDataFormat sourceFormat)
{
	TupleDesc	tableDescriptor = RelationGetDescr(relation);

	for (int columnIndex = 0; columnIndex < tableDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tableDescriptor, columnIndex);

		if (column->attisdropped)
			continue;

		Oid			typeId = column->atttypid;

		/*
		 * We only support nested types when the source is Parquet or Iceberg.
		 */
		if (sourceFormat != DATA_FORMAT_PARQUET && sourceFormat != DATA_FORMAT_ICEBERG)
		{
			/* type_is_array also covers pg_map */
			if (type_is_array(typeId) || get_typtype(typeId) == TYPTYPE_COMPOSITE)
			{
				ereport(DEBUG4,
						(errmsg("Array or composite type is not pushdownable "
								"when source is not Parquet or Iceberg")));

				return false;
			}
		}

		/*
		 * The current pushdown implementation does not convert geometry to
		 * the appropriate storage format yet.
		 */
		if (IsGeometryTypeId(typeId))
		{
			ereport(DEBUG4,
					(errmsg("Geometry type is not pushdownable")));

			return false;
		}

		if (typeId == NUMERICOID || typeId == NUMERICARRAYOID)
		{
			/* todo: handle numeric field in composite type */
			int			typmod = column->atttypmod;

			/* may fail if unbounded numeric exceeds duckdb limits (38,38) */
			if (typmod == -1)
				continue;

			int			precision = numeric_typmod_precision(typmod);
			int			scale = numeric_typmod_scale(typmod);

			if (!CanPushdownNumericToDuckdb(precision, scale))
			{
				ereport(DEBUG4,
						(errmsg("Numeric type with precision(%d) and scale(%d) "
								"is not pushdownable", precision, scale)));

				return false;
			}
		}

		/*
		 * Domains may have constraints which are not checked on the DuckDB
		 * side.
		 */
		char		typeType = get_typtype(typeId);

		if (typeType == TYPTYPE_DOMAIN)
		{
			ereport(DEBUG4,
					(errmsg("Domain type is not pushdownable")));
			return false;
		}
	}

	return true;
}

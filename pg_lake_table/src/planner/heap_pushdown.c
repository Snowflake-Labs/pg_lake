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
 * heap_pushdown.c
 *		  Admit plain PostgreSQL tables into whole-query pushdown.
 *
 * Whole-query pushdown normally requires every relation in the query to be a
 * pg_lake table, since pgduck_server can only read what is in object storage.
 * A query that spans a PostgreSQL tier and an Iceberg tier therefore falls
 * back to a plan that scans the lake table through the FDW and aggregates in
 * PostgreSQL, which loses vectorized execution for the lake side.
 *
 * When pg_lake_table.enable_heap_query_pushdown is on, we also admit plain
 * (and partitioned) heap relations. Those are deparsed into the same
 * __lake_read_table(..) placeholder as lake tables, and the placeholder is
 * substituted with a postgres_scan_pushdown(..) call that reads the relation
 * back from PostgreSQL over a loopback connection, pinned to a snapshot
 * exported by the driving backend. The scanner pushes projections and filters
 * into that connection's SELECT, so only the qualifying columns and rows are
 * shipped.
 *
 * Two properties keep this from changing the meaning of a query:
 *
 * - The exported snapshot is the snapshot of the driving transaction, so the
 *   heap side and the lake side see the same instant.
 * - Local privilege checks still happen, because the pushdown plan carries the
 *   original plan's range table and permission infos.
 *
 * The cost is that the driving transaction's own uncommitted writes are
 * invisible to the loopback connection, which is why we refuse to run once
 * the transaction has written anything.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "pg_lake/duckdb/transform_query_to_duckdb.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/fdw/shippable.h"
#include "pg_lake/planner/heap_pushdown.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/string_utils.h"

/* pg_lake_table.enable_heap_query_pushdown setting */
bool		EnableHeapQueryPushdown = false;

/* pg_lake_table.heap_pushdown_dsn setting */
char	   *HeapPushdownDSN = "";

static bool HeapPushdownSnapshotAvailable(void);
static bool HeapRelationIsPushdownable(Oid relationId);
static bool ReplaceHeapTableWalker(Node *node, List **heapRteList);
static char *HeapPushdownExportSnapshot(void);
static char *HeapPushdownConnectionString(void);


/*
 * HeapPushdownSnapshotAvailable determines whether we can currently export a
 * snapshot that the loopback connection can use and that gives the same
 * answers as a local scan.
 *
 * - ExportSnapshot cannot run in a subtransaction, because an importer has no
 *   way to tell that the same subtransaction is still running.
 * - An exported snapshot shows the exporting transaction as still running, so
 *   the loopback connection would not see writes the driving transaction has
 *   already made. We therefore refuse once an XID has been assigned.
 */
static bool
HeapPushdownSnapshotAvailable(void)
{
	if (!IsTransactionState() || IsSubTransaction())
		return false;

	if (GetTopTransactionIdIfAny() != InvalidTransactionId)
		return false;

	return true;
}


/*
 * HeapRteIsPushdownable determines whether the given RTE is a plain
 * PostgreSQL relation that we should admit into whole-query pushdown. This is
 * the planner-side question, so it also covers the setting and whether we can
 * export a snapshot at all.
 */
bool
HeapRteIsPushdownable(RangeTblEntry *rte)
{
	if (!EnableHeapQueryPushdown)
		return false;

	if (!HeapPushdownSnapshotAvailable())
		return false;

	return HeapRteIsRelationPushdownable(rte);
}


/*
 * HeapRteIsRelationPushdownable determines whether the relation behind an RTE
 * can be read back over the loopback connection.
 *
 * This is the executor-side question, deliberately free of any state that can
 * change between planning and execution: once we have a pushdown plan that
 * admitted a heap relation, we have to rewrite that relation, or the deparsed
 * query would name a table pgduck_server has never heard of.
 */
bool
HeapRteIsRelationPushdownable(RangeTblEntry *rte)
{
	if (rte->rtekind != RTE_RELATION)
		return false;

	/* lake tables are read directly from object storage, which is better */
	if (IsAnyLakeForeignTable(rte))
		return false;

	/*
	 * Only ordinary tables and partitioned tables. Views and lake tables are
	 * handled elsewhere, and we do not want to reach a foreign table (which
	 * could even be a lake table) through the loopback connection.
	 */
	if (rte->relkind != RELKIND_RELATION &&
		rte->relkind != RELKIND_PARTITIONED_TABLE)
		return false;

	/* the remote scan would sample a different set of rows */
	if (rte->tablesample != NULL)
		return false;

	/*
	 * Row-level security and other security barriers are enforced by quals
	 * the planner attaches to the RTE, which the deparser does not emit, so
	 * pushing the scan down would bypass them.
	 */
	if (rte->securityQuals != NIL)
		return false;

	if (IsCatalogRelationOid(rte->relid))
		return false;

	if (!HeapRelationIsPushdownable(rte->relid))
		return false;

	if (rte->relkind == RELKIND_PARTITIONED_TABLE)
	{
		/*
		 * A partitioned table has no storage of its own, so the remote SELECT
		 * reads the partitions. They all have to be readable the same way.
		 */
		if (!AllInheritorsArePushdownableHeap(rte->relid))
			return false;
	}
	else if (has_subclass(rte->relid))
	{
		/*
		 * Legacy inheritance: the parent has storage of its own and the
		 * scanner may read it with a ctid range scan, which would miss the
		 * children.
		 */
		return false;
	}

	return true;
}


/*
 * AllInheritorsArePushdownableHeap determines whether every relation in the
 * inheritance tree below (and including) the given relation can be read back
 * over the loopback connection.
 */
bool
AllInheritorsArePushdownableHeap(Oid parentRelationId)
{
	/* the parent is already locked, and we only read the catalogs */
	List	   *inheritorList = find_all_inheritors(parentRelationId, NoLock, NULL);
	ListCell   *inheritorCell = NULL;

	foreach(inheritorCell, inheritorList)
	{
		Oid			inheritorId = lfirst_oid(inheritorCell);
		char		relkind = get_rel_relkind(inheritorId);

		if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
			return false;

		if (IsCatalogRelationOid(inheritorId))
			return false;

		if (!HeapRelationIsPushdownable(inheritorId))
			return false;
	}

	return true;
}


/*
 * HeapRelationIsPushdownable determines whether the storage and the schema of
 * a relation allow reading it back over the loopback connection.
 */
static bool
HeapRelationIsPushdownable(Oid relationId)
{
	Relation	relation = RelationIdGetRelation(relationId);

	if (!RelationIsValid(relation))
		return false;

	bool		pushdownable = true;

	/* another backend cannot see our temporary tables */
	if (relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		pushdownable = false;

	/* the loopback connection would not apply the policies */
	else if (relation->rd_rel->relrowsecurity ||
			 relation->rd_rel->relforcerowsecurity)
		pushdownable = false;

	else
	{
		TupleDesc	tupleDesc = RelationGetDescr(relation);

		for (int attributeIndex = 0; attributeIndex < tupleDesc->natts; attributeIndex++)
		{
			Form_pg_attribute attribute = TupleDescAttr(tupleDesc, attributeIndex);

			/*
			 * The scanner only returns live columns, so a dropped column in
			 * the middle would shift every following column relative to the
			 * attribute numbers the deparsed query uses.
			 */
			if (attribute->attisdropped)
			{
				pushdownable = false;
				break;
			}

			/*
			 * Virtual generated columns are computed on read and are not
			 * guaranteed to survive the way the scanner reads the table.
			 */
			if (attribute->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
			{
				pushdownable = false;
				break;
			}

			/*
			 * The scanner binds the whole relation, not just the columns the
			 * query uses, so every column type has to be one we would ship.
			 */
			if (!is_shippable(attribute->atttypid, TypeRelationId, NULL))
			{
				pushdownable = false;
				break;
			}
		}
	}

	RelationClose(relation);

	return pushdownable;
}


/*
 * ReplaceHeapTableWithReadTableFunc replaces all occurrences of pushdownable
 * plain PostgreSQL relations with __lake_read_table(..) placeholder calls, and
 * returns the list of RTEs it replaced (as they were before the rewrite).
 *
 * This mirrors ReplacePgLakeTableWithReadTableFunc, and is meant to run after
 * it: by then the lake relations are already RTE_FUNCTION, so they are not
 * considered here. The two placeholders are told apart by the unique relation
 * identifier they carry.
 */
List *
ReplaceHeapTableWithReadTableFunc(Node *node)
{
	List	   *heapRteList = NIL;

	ReplaceHeapTableWalker(node, &heapRteList);

	return heapRteList;
}


/*
 * ReplaceHeapTableWalker does the work of
 * ReplaceHeapTableWithReadTableFunc for one node.
 */
static bool
ReplaceHeapTableWalker(Node *node, List **heapRteList)
{
	if (node == NULL)
		return false;

	/* want to look at all RTEs, even in subqueries, CTEs and such */
	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, ReplaceHeapTableWalker,
								 heapRteList, QTW_EXAMINE_RTES_BEFORE);
	}

	if (!IsA(node, RangeTblEntry))
		return expression_tree_walker(node, ReplaceHeapTableWalker, heapRteList);

	RangeTblEntry *rte = (RangeTblEntry *) node;

	if (!HeapRteIsRelationPushdownable(rte))
		return false;

	char	   *qualifiedRelationName = GetQualifiedRelationName(rte->relid);

	/* store a copy of the rte, we need the relation id after the rewrite */
	*heapRteList = lappend(*heapRteList, copyObject(rte));

	Const	   *tableNameParam = makeNode(Const);

	tableNameParam->constvalue = CStringGetTextDatum(qualifiedRelationName);
	tableNameParam->consttype = TEXTOID;
	tableNameParam->consttypmod = -1;
	tableNameParam->constbyval = false;
	tableNameParam->constlen = get_typlen(TEXTOID);
	tableNameParam->location = -1;

	Const	   *uniqueRelationId = makeNode(Const);

	uniqueRelationId->constvalue = Int32GetDatum(GetUniqueRelationIdentifier(rte));
	uniqueRelationId->consttype = INT4OID;
	uniqueRelationId->consttypmod = -1;
	uniqueRelationId->constbyval = true;
	uniqueRelationId->constlen = 4;
	uniqueRelationId->location = -1;

	/* create function expression to store our faux arguments in */
	FuncExpr   *readTableFuncExpr = makeNode(FuncExpr);

	readTableFuncExpr->funcid = ReadTableFunctionId();
	readTableFuncExpr->funcresulttype = RECORDOID;
	readTableFuncExpr->funcretset = true;
	readTableFuncExpr->location = -1;
	readTableFuncExpr->args = list_make2(tableNameParam, uniqueRelationId);

	RangeTblFunction *readTableFunction = makeNode(RangeTblFunction);

	readTableFunction->funcexpr = (Node *) readTableFuncExpr;

	/* set the column count to pass ruleutils checks */
	Relation	relation = RelationIdGetRelation(rte->relid);

	readTableFunction->funccolcount = RelationGetNumberOfAttributes(relation);
	RelationClose(relation);

	rte->functions = list_make1(readTableFunction);

	/* finally set the RTEKind */
	rte->rtekind = RTE_FUNCTION;

	return false;
}


/*
 * ReplaceHeapTableFunctionCalls replaces the __lake_read_table(..) placeholder
 * of every heap relation in heapRteList with a postgres_scan_pushdown(..) call
 * that reads the relation back from PostgreSQL at our snapshot.
 *
 * For EXPLAIN we substitute the relation name instead, like the lake path
 * does, so the output stays readable.
 */
char *
ReplaceHeapTableFunctionCalls(char *query, List *heapRteList,
							  bool explainRequested)
{
	if (heapRteList == NIL)
		return query;

	char	   *connectionString = NULL;
	char	   *snapshotName = NULL;

	if (!explainRequested)
	{
		connectionString = HeapPushdownConnectionString();
		snapshotName = HeapPushdownExportSnapshot();
	}

	ListCell   *rteCell = NULL;

	foreach(rteCell, heapRteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
		char	   *qualifiedRelationName = GetQualifiedRelationName(rte->relid);
		char	   *placeholderCall =
			BuildReadTablePlaceholderCall(qualifiedRelationName,
										  GetUniqueRelationIdentifier(rte));
		char	   *scanCall = NULL;

		if (explainRequested)
			scanCall = qualifiedRelationName;
		else
		{
			char	   *schemaName = get_namespace_name(get_rel_namespace(rte->relid));
			char	   *relationName = get_rel_name(rte->relid);

			scanCall = psprintf("postgres_scan_pushdown(%s, %s, %s, snapshot => %s)",
								EscapedStringLiteral(connectionString),
								EscapedStringLiteral(schemaName),
								EscapedStringLiteral(relationName),
								EscapedStringLiteral(snapshotName));
		}

		query = PgLakeReplaceText(query, placeholderCall, scanCall);
	}

	return query;
}


/*
 * HeapPushdownExportSnapshot exports the snapshot of the current statement and
 * returns its name.
 *
 * We export once per pushed-down query rather than caching one per
 * transaction: under READ COMMITTED each statement gets its own snapshot, and
 * reusing an older export would leave the heap side of the query at an earlier
 * instant than the lake side. PostgreSQL drops the exported snapshots, and
 * their files, when the transaction ends.
 */
static char *
HeapPushdownExportSnapshot(void)
{
	if (!HeapPushdownSnapshotAvailable())
	{
		/*
		 * The plan admitted heap relations, but we can no longer export a
		 * snapshot that would give the right answer. This is reachable with a
		 * cached plan: it was planned in a transaction that had not written
		 * yet, and is now executed in one that has. Returning rows that miss
		 * the transaction's own writes would be worse than refusing.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot push down a query over plain PostgreSQL tables "
						"in this transaction"),
				 errdetail("pgduck_server reads those tables back over a "
						   "separate connection, which cannot see writes or "
						   "subtransactions of the current transaction."),
				 errhint("Set pg_lake_table.enable_heap_query_pushdown to off, "
						 "or run the query in its own transaction.")));
	}

	return ExportSnapshot(GetActiveSnapshot());
}


/*
 * HeapPushdownConnectionString returns the connection string pgduck_server
 * should use to read plain PostgreSQL tables back.
 *
 * Unless it is configured explicitly, we point it at the postmaster's Unix
 * socket in the current database as the current user, which keeps the loopback
 * connection inside the machine and gives it the same identity as the query.
 * Whether it can actually authenticate is up to pg_hba.conf, which is why the
 * setting exists and why the feature is off by default.
 */
static char *
HeapPushdownConnectionString(void)
{
	if (HeapPushdownDSN != NULL && HeapPushdownDSN[0] != '\0')
		return HeapPushdownDSN;

	StringInfo	connectionString = makeStringInfo();

	/*
	 * unix_socket_directories is a list; the postmaster listens on all of
	 * them, so the first one will do. An empty list means TCP only.
	 */
	char	   *socketDirectory = NULL;

	if (Unix_socket_directories != NULL && Unix_socket_directories[0] != '\0')
	{
		char	   *directoryList = pstrdup(Unix_socket_directories);
		char	   *separator = strchr(directoryList, ',');

		if (separator != NULL)
			*separator = '\0';

		socketDirectory = directoryList;
	}

	if (socketDirectory != NULL)
		appendStringInfo(connectionString, "host=%s ", socketDirectory);
	else
		appendStringInfoString(connectionString, "host=localhost ");

	appendStringInfo(connectionString, "port=%d dbname=%s user=%s",
					 PostPortNumber,
					 get_database_name(MyDatabaseId),
					 GetUserNameFromId(GetUserId(), false));

	return connectionString->data;
}

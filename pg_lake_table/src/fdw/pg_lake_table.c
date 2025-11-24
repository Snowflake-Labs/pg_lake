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
 * pg_lake_table.c
 *
 * Portions Copyright (c) 2012-2023, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"

#include <limits.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_opfamily.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/duckdb/transform_query_to_duckdb.h"
#include "pg_lake/fdw/deparse_ruleutils.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/fdw/shippable.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/fdw/update_tracking.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/multi_data_file_dest.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/partitioning/partitioned_dest_receiver.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/permissions/roles.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/pgduck/array_conversion.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/explain.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/item_pointer_utils.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/string_utils.h"

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,

	/* list of relation rtes */
	FdwScanPrivateRteList,

	/* list of restrictions on relations */
	FdwScanPrivateRestrictions,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations
};

/*
 * Execution state of a foreign scan using pg_lake.
 */
typedef struct PgLakeScanState
{
	Relation	rel;			/* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc	tupdesc;		/* tuple descriptor of scan */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char	   *query;			/* text of SELECT command */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */

	/* for remote query execution */
	PGDuckConnection *conn;		/* connection for the scan */
	bool		prepared_statement_sent;	/* have we executed the prepared
											 * statement? */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple  *tuples;			/* array of currently-retrieved tuples */
	int			num_tuples;		/* # of tuples in array */
	int			next_tuple;		/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	bool		eof_reached;	/* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt;	/* context holding current batch of tuples */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* file lists for each relation */
	PgLakeScanSnapshot *scanSnapshot;

	/* result relation in case of a scan for update/delete */
	Oid			resultRelationId;

	/* filename -> index map for update/delete result relation */
	HTAB	   *resultFileIndexes;

	/* record_in function to parse (filename, file_row_number) records  */
	FmgrInfo	recordInFunction;

	/* record type modifier for (filename, file_row_number) records */
	int			rowLocationTypeMod;

	/* number of bits used for file row numbers in 48-bit ctid */
	int			fileRowNumberBits;

	/* when rows are skipped, count how many */
	bool		skipFullMatchFiles;
	uint64		skippableRows;
	uint64		skippableDataFiles;

}			PgLakeScanState;

/*
 * PgLakeFileModifyState holds the state for modifications to an existing
 * file.
 */
typedef struct PgLakeFileModifyState
{
	/* path of the file in Datum form (to avoid frequent conversion) */
	Datum		pathDatum;

	/* number of rows in the source file */
	int64		sourceRowCount;

	/* number of not-deleted rows in the source file */
	int64		liveRowCount;

	/* DestReceiver for deletions in case of UPDATE/DELETE */
	DestReceiver *deleteDest;
	char	   *deleteFile;

	/* number of rows modified by UPDATE/DELETE */
	int64		modifiedRowCount;

}			PgLakeFileModifyState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct PgLakeModifyState
{
	/* relcache entry for the foreign table */
	Relation	rel;

	/* type of modification operation */
	CmdType		operation;

	/* destination for inserts */
	DestReceiver *insertDest;
	uint64		insertedRowCount;

	/* slot used for position deletes */
	TupleTableSlot *deleteSlot;

	/* files that are scanned during UPDATE/DELETE */
	List	   *fileModifyStates;

	/* attribute number of input resjunk ctid column */
	AttrNumber	ctidAttno;

	/* maps planSlot (subplan targetlist) index to column numbers */
	List	   *planSlotAttributeNumbers;

	/* number of bits reserved for file row numbers in ctids */
	int			fileRowNumberBits;

	/* during updates, we keep track of updated ctids */
	bool		requiresUpdateTracking;
	RelationUpdateTrackingState updateTrackingState;

	/* for update row movement if subplan result rel */
	struct PgLakeModifyState *aux_fmstate;	/* foreign-insert state, if
											 * created */
}			PgLakeModifyState;

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as a Boolean node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as a Boolean node) */
	FdwPathPrivateHasLimit
};

/* Struct for extra information passed to estimate_path_cost_size() */
typedef struct
{
	PathTarget *target;
	bool		has_final_sort;
	bool		has_limit;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
}			PgLakePathExtraData;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation
{
	AttrNumber	cur_attno;		/* attribute number being processed, or 0 */
	Relation	rel;			/* foreign table being processed, or NULL */
	ForeignScanState *fsstate;	/* plan node being processed, or NULL */
} ConversionLocation;

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
	Expr	   *current;		/* current expr, or NULL if not yet found */
	List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * ResultFileIndex is an HTAB entry for mapping a file name to an index.
 */
typedef struct ResultFileIndex
{
	char		path[MAXPGPATH];
	int			index;
}			ResultFileIndex;

/*
 * FindResultRelationScanStateContext is passed down via plan_tree_walker
 * in FindResultRelationScanState.
 */
typedef struct FindResultRelationScanStateContext
{
	/* the OID of the result relation we are looking for */
	Oid			resultRelationId;

	/* the lake scan we found */
	PgLakeScanState *lakeScan;
}			FindResultRelationScanStateContext;


PgLakeModifyValidityCheckHookType PgLakeModifyValidityCheckHook = NULL;


/*
* This is an overly simplified heuristic. We hard code the number of
* rows that we expect to be returned by the top-level node of the
* foreign scan plan. This is used to estimate the cost of the foreign
* scan plan.
*/
#define ESTIMATED_ROW_COUNT 1000

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(pg_lake_table_handler);


/*
 * FDW callback routines
 */
static void postgresGetForeignRelSize(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
static void postgresGetForeignPaths(PlannerInfo *root,
									RelOptInfo *baserel,
									Oid foreigntableid);
static ForeignScan *postgresGetForeignPlan(PlannerInfo *root,
										   RelOptInfo *foreignrel,
										   Oid foreigntableid,
										   ForeignPath *best_path,
										   List *tlist,
										   List *scan_clauses,
										   Plan *outer_plan);
static void postgresBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *postgresIterateForeignScan(ForeignScanState *node);
static void postgresReScanForeignScan(ForeignScanState *node);
static void postgresEndForeignScan(ForeignScanState *node);
static void postgresAddForeignUpdateTargets(PlannerInfo *root,
											Index rtindex,
											RangeTblEntry *target_rte,
											Relation target_relation);
static void AddDeleteReturningTargetTableVars(PlannerInfo *root,
											  Index rtindex,
											  Relation targetRelation,
											  List *returningVars);
static void AddAllTargetTableVars(PlannerInfo *root,
								  Index rtindex,
								  Relation targetRelation);
static List *postgresPlanForeignModify(PlannerInfo *root,
									   ModifyTable *plan,
									   Index resultRelation,
									   int subplan_index);
static void postgresBeginForeignModify(ModifyTableState *mtstate,
									   ResultRelInfo *resultRelInfo,
									   List *fdw_private,
									   int subplan_index,
									   int eflags);
static TupleTableSlot *postgresExecForeignInsert(EState *estate,
												 ResultRelInfo *resultRelInfo,
												 TupleTableSlot *slot,
												 TupleTableSlot *planSlot);
static TupleTableSlot *postgresExecForeignUpdate(EState *estate,
												 ResultRelInfo *resultRelInfo,
												 TupleTableSlot *slot,
												 TupleTableSlot *planSlot);
static TupleTableSlot *postgresExecForeignDelete(EState *estate,
												 ResultRelInfo *resultRelInfo,
												 TupleTableSlot *slot,
												 TupleTableSlot *planSlot);
static void DeleteSingleRow(PgLakeModifyState * fmstate,
							ItemPointer ctid);
static void StoreDeleteReturningSlot(TupleTableSlot *planSlot,
									 TupleTableSlot *returningSlot,
									 List *planSlotAttributeNumbers);
static void postgresEndForeignModify(EState *estate,
									 ResultRelInfo *resultRelInfo);
static void postgresBeginForeignInsert(ModifyTableState *mtstate,
									   ResultRelInfo *resultRelInfo);
static void postgresEndForeignInsert(EState *estate,
									 ResultRelInfo *resultRelInfo);
static int	postgresIsForeignRelUpdatable(Relation rel);
static void postgresExplainForeignScan(ForeignScanState *node,
									   ExplainState *es);
static void postgresExecForeignTruncate(List *rels,
										DropBehavior behavior,
										bool restart_seqs);
static void postgresGetForeignJoinPaths(PlannerInfo *root,
										RelOptInfo *joinrel,
										RelOptInfo *outerrel,
										RelOptInfo *innerrel,
										JoinType jointype,
										JoinPathExtraData *extra);
static bool postgresRecheckForeignScan(ForeignScanState *node,
									   TupleTableSlot *slot);
static void postgresGetForeignUpperPaths(PlannerInfo *root,
										 UpperRelationKind stage,
										 RelOptInfo *input_rel,
										 RelOptInfo *output_rel,
										 void *extra);

/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
									RelOptInfo *foreignrel,
									List *param_join_conds,
									List *pathkeys,
									PgLakePathExtraData * fpextra,
									double *p_rows, int *p_width,
									int *p_disabled_nodes,
									Cost *p_startup_cost, Cost *p_total_cost);
static bool ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
									  EquivalenceClass *ec, EquivalenceMember *em,
									  void *arg);
static void send_prepared_statement(ForeignScanState *node);
static void fetch_more_data(ForeignScanState *node);
static PgLakeModifyState * create_foreign_modify(Relation rel,
												 Index resultRangeTableIndex,
												 ModifyTableState *mtstate);
static PgLakeScanState * FindResultRelationScanState(PlanState *planState,
													 Oid relationId);
static bool FindResultRelationScanStateWalker(PlanState *planState,
											  FindResultRelationScanStateContext * context);
static void prepare_query_params(PlanState *node,
								 List *fdw_exprs,
								 int numParams,
								 FmgrInfo **param_flinfo,
								 List **param_exprs,
								 const char ***param_values);
static void process_query_params(ExprContext *econtext,
								 FmgrInfo *param_flinfo,
								 List *param_exprs,
								 const char **param_values);
static HeapTuple make_tuple_from_result_row(PGresult *res,
											int row,
											Relation rel,
											AttInMetadata *attinmeta,
											List *retrieved_attrs,
											ForeignScanState *fsstate,
											MemoryContext temp_context);
static void conversion_error_callback(void *arg);
static bool foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
							JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel,
							JoinPathExtraData *extra);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
								Node *havingQual);
static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
											  RelOptInfo *rel);
static List *get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel);
static void add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
											Path *epq_path,
											List *restrictlist);
static void add_foreign_grouping_paths(PlannerInfo *root,
									   RelOptInfo *input_rel,
									   RelOptInfo *grouped_rel,
									   GroupPathExtraData *extra);
static void add_foreign_ordered_paths(PlannerInfo *root,
									  RelOptInfo *input_rel,
									  RelOptInfo *ordered_rel);
static void add_foreign_final_paths(PlannerInfo *root,
									RelOptInfo *input_rel,
									RelOptInfo *final_rel,
									FinalPathExtraData *extra);
static void apply_server_options(PgLakeRelationInfo * fpinfo);
static void apply_table_options(PgLakeRelationInfo * fpinfo);
static void merge_fdw_options(PgLakeRelationInfo * fpinfo,
							  const PgLakeRelationInfo * fpinfo_o,
							  const PgLakeRelationInfo * fpinfo_i);

static void WriteInsertRecord(PgLakeModifyState * modifyState, TupleTableSlot *slot);
static void PrepareDeletionSlot(PgLakeFileModifyState * fileModifyState,
								uint64 fileRowNumber,
								TupleTableSlot *deleteSlot);
static void WriteDeleteRecord(PgLakeFileModifyState * fileModifyState,
							  TupleTableSlot *deleteSlot);
static TupleDesc CreateRowIdTupleDesc(void);
static ItemPointer RowIdRecordStringToItemPointer(char *recordString,
												  PgLakeScanState * scanState);
static uint64 ExtractFileIndexFromItemPointer(ItemPointer ctid,
											  int fileRowNumberBits);
static uint64 ExtractFileRowNumberFromItemPointer(ItemPointer ctid,
												  int fileRowNumberBits);
static void FinishForeignModify(PgLakeModifyState * modifyState);

static void ErrorIfSystemColumnUsed(Oid relationId, AttrNumber attnum);
static bool AdjustUniqueRelationIdentifiersViaAlias(Node *node, void *context);
static bool IsSimpleDelete(ModifyTableState *mtstate, Relation resultRel);
static void ShutdownPgLakeScan(ForeignScanState *node);


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
pg_lake_table_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = postgresGetForeignRelSize;
	routine->GetForeignPaths = postgresGetForeignPaths;
	routine->GetForeignPlan = postgresGetForeignPlan;
	routine->BeginForeignScan = postgresBeginForeignScan;
	routine->IterateForeignScan = postgresIterateForeignScan;
	routine->ReScanForeignScan = postgresReScanForeignScan;
	routine->EndForeignScan = postgresEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = postgresAddForeignUpdateTargets;
	routine->PlanForeignModify = postgresPlanForeignModify;
	routine->BeginForeignModify = postgresBeginForeignModify;
	routine->ExecForeignInsert = postgresExecForeignInsert;
	routine->ExecForeignBatchInsert = NULL;
	routine->GetForeignModifyBatchSize = NULL;
	routine->ExecForeignUpdate = postgresExecForeignUpdate;
	routine->ExecForeignDelete = postgresExecForeignDelete;
	routine->EndForeignModify = postgresEndForeignModify;
	routine->BeginForeignInsert = postgresBeginForeignInsert;
	routine->EndForeignInsert = postgresEndForeignInsert;
	routine->IsForeignRelUpdatable = postgresIsForeignRelUpdatable;
	routine->PlanDirectModify = NULL;
	routine->BeginDirectModify = NULL;
	routine->IterateDirectModify = NULL;
	routine->EndDirectModify = NULL;

	/* Function for EvalPlanQual rechecks */
	routine->RecheckForeignScan = postgresRecheckForeignScan;
	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = postgresExplainForeignScan;
	routine->ExplainForeignModify = NULL;
	routine->ExplainDirectModify = NULL;

	/* Support function for TRUNCATE */
	routine->ExecForeignTruncate = postgresExecForeignTruncate;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = NULL;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = NULL;

	/* Support functions for join push-down */
	routine->GetForeignJoinPaths = postgresGetForeignJoinPaths;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = postgresGetForeignUpperPaths;

	/* Support functions for asynchronous execution */
	routine->IsForeignPathAsyncCapable = NULL;
	routine->ForeignAsyncRequest = NULL;
	routine->ForeignAsyncConfigureWait = NULL;
	routine->ForeignAsyncNotify = NULL;

	routine->ShutdownForeignScan = ShutdownPgLakeScan;

	PG_RETURN_POINTER(routine);
}


/*
 * postgresGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
postgresGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid)
{
	PgLakeRelationInfo *fpinfo;
	ListCell   *lc;

	/*
	 * We use PgLakeRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (PgLakeRelationInfo *) palloc0(sizeof(PgLakeRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table settings of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = NIL;

	apply_server_options(fpinfo);
	apply_table_options(fpinfo);

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Check for referenced system columns; we do not allow users direct
	 * access to any system columns, but internally use for mapping rows for
	 * update/delete. See also additional checks in the update/delete
	 * RETURNING clause.
	 */
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	bool		isUpdateDelete = rte->rellockmode == RowExclusiveLock;
	int			attributeNumber;

	/* Now, are any system columns requested from rel? */
	for (attributeNumber = FirstLowInvalidHeapAttributeNumber + 1; attributeNumber < 0; attributeNumber++)
	{
		if (bms_is_member(attributeNumber - FirstLowInvalidHeapAttributeNumber, fpinfo->attrs_used))
		{
			/* tableoid always allowed, ctid if update or delete */
			if (!((attributeNumber == TableOidAttributeNumber) ||
				  (attributeNumber == SelfItemPointerAttributeNumber && isUpdateDelete)))
				break;
		}
	}

	if (attributeNumber != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("System column \"%s\" is not supported for "
						"pg_lake table  \"%s\"",
						get_attname(rte->relid, attributeNumber, false),
						get_rel_name(rte->relid))));
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		/*
		 * Get cost/size estimates with help of remote server.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->disabled_nodes,
								&fpinfo->startup_cost, &fpinfo->total_cost);

		/* Report estimated baserel size to planner. */
		baserel->rows = fpinfo->rows;
		baserel->reltarget->width = fpinfo->width;
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have
		 * reltuples < 0, meaning "unknown".  We can't do much if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->tuples < 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->disabled_nodes,
								&fpinfo->startup_cost, &fpinfo->total_cost);
	}

	if (root->rowMarks && root->parse->commandType == CMD_SELECT)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("SELECT .. FOR UPDATE is not yet supported "
							   "on pg_lake tables")));

	/*
	 * fpinfo->relation_name gets the numeric rangetable index of the foreign
	 * table RTE.  (If this query gets EXPLAIN'd, we'll convert that to a
	 * human-readable string at that time.)
	 */
	fpinfo->relation_name = psprintf("%u", baserel->relid);

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	fpinfo->hidden_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;
}


/*
 * ErrorIfSystemColumnUsed checks if any system column is used in the query
 * for the given relation, given a list of Vars.
 *
 * We allow tableoid, since that's generated by PostgreSQL.
 *
 * We also check all attributes if checkVarno is 0
 */
static void
ErrorIfSystemColumnUsed(Oid relationId, AttrNumber attributeNumber)
{
	if (attributeNumber < 0 &&
		attributeNumber != TableOidAttributeNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("System column \"%s\" is not supported for "
						"pg_lake table  \"%s\"",
						get_attname(relationId, attributeNumber, false),
						get_rel_name(relationId))));
	}
}


/*
 * get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 */
			if (!is_foreign_pathkey(root, rel, pathkey))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	/*
	 * Even if we're not using remote estimates, having the remote side do the
	 * sort generally won't be any worse than doing it locally, and it might
	 * be much better if the remote side can generate data in the right order
	 * without needing a sort at all.  However, what we're going to do next is
	 * try to generate pathkeys that seem promising for possible merge joins,
	 * and that's more speculative.  A wrong choice might hurt quite a bit, so
	 * bail out if we can't use remote estimates.
	 */
	if (!fpinfo->use_remote_estimate)
		return useful_pathkeys_list;

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceClass *cur_ec = lfirst(lc);
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		/* Can't push down the sort if the EC's opfamily is not shippable. */
		if (!is_shippable(linitial_oid(cur_ec->ec_opfamilies),
						  OperatorFamilyRelationId, NULL))
			continue;

		/* If no pushable expression for this rel, skip it. */
		if (find_em_for_rel(root, cur_ec, rel) == NULL)
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);
		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

/*
 * postgresGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
postgresGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) baserel->fdw_private;
	ForeignPath *path;
	List	   *ppi_list;
	ListCell   *lc;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 *
	 * Although this path uses no join clauses, it could still have required
	 * parameterization due to LATERAL refs in its tlist.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
#if PG_VERSION_NUM >= 180000
								   fpinfo->disabled_nodes,
#endif
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
#if PG_VERSION_NUM >= 170000
								   NIL, /* no fdw_restrictinfo list */
#endif
								   NIL);	/* no fdw_private list */
	add_path(baserel, (Path *) path);

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL, NIL);

	/*
	 * If we're not using remote estimates, stop here.  We have no way to
	 * estimate whether any join clauses would be worth sending across, so
	 * don't bother building parameterized paths.
	 */
	if (!fpinfo->use_remote_estimate)
		return;

	/*
	 * Thumb through all join clauses for the rel to identify which outer
	 * relations could supply one or more safe-to-send-to-remote join clauses.
	 * We'll build a parameterized path for each such outer relation.
	 *
	 * It's convenient to manage this by representing each candidate outer
	 * relation by the ParamPathInfo node for it.  We can then use the
	 * ppi_clauses list in the ParamPathInfo node directly as a list of the
	 * interesting join clauses for that rel.  This takes care of the
	 * possibility that there are multiple safe join clauses for such a rel,
	 * and also ensures that we account for unsafe join clauses that we'll
	 * still have to enforce locally (since the parameterized-path machinery
	 * insists that we handle all movable clauses).
	 */
	ppi_list = NIL;
	foreach(lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Relids		required_outer;
		ParamPathInfo *param_info;

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, baserel))
			continue;

		/* See if it is safe to send to remote */
		if (!is_foreign_expr(root, baserel, rinfo->clause))
			continue;

		/* Calculate required outer rels for the resulting path */
		required_outer = bms_union(rinfo->clause_relids,
								   baserel->lateral_relids);
		/* We do not want the foreign rel itself listed in required_outer */
		required_outer = bms_del_member(required_outer, baserel->relid);

		/*
		 * required_outer probably can't be empty here, but if it were, we
		 * couldn't make a parameterized path.
		 */
		if (bms_is_empty(required_outer))
			continue;

		/* Get the ParamPathInfo */
		param_info = get_baserel_parampathinfo(root, baserel,
											   required_outer);
		Assert(param_info != NULL);

		/*
		 * Add it to list unless we already have it.  Testing pointer equality
		 * is OK since get_baserel_parampathinfo won't make duplicates.
		 */
		ppi_list = list_append_unique_ptr(ppi_list, param_info);
	}

	/*
	 * The above scan examined only "generic" join clauses, not those that
	 * were absorbed into EquivalenceClauses.  See if we can make anything out
	 * of EquivalenceClauses.
	 */
	if (baserel->has_eclass_joins)
	{
		/*
		 * We repeatedly scan the eclass list looking for column references
		 * (or expressions) belonging to the foreign rel.  Each time we find
		 * one, we generate a list of equivalence joinclauses for it, and then
		 * see if any are safe to send to the remote.  Repeat till there are
		 * no more candidate EC members.
		 */
		ec_member_foreign_arg arg;

		arg.already_used = NIL;
		for (;;)
		{
			List	   *clauses;

			/* Make clauses, skipping any that join to lateral_referencers */
			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root,
															 baserel,
															 ec_member_matches_foreign,
															 (void *) &arg,
															 baserel->lateral_referencers);

			/* Done if there are no more expressions in the foreign rel */
			if (arg.current == NULL)
			{
				Assert(clauses == NIL);
				break;
			}

			/* Scan the extracted join clauses */
			foreach(lc, clauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
				Relids		required_outer;
				ParamPathInfo *param_info;

				/* Check if clause can be moved to this rel */
				if (!join_clause_is_movable_to(rinfo, baserel))
					continue;

				/* See if it is safe to send to remote */
				if (!is_foreign_expr(root, baserel, rinfo->clause))
					continue;

				/* Calculate required outer rels for the resulting path */
				required_outer = bms_union(rinfo->clause_relids,
										   baserel->lateral_relids);
				required_outer = bms_del_member(required_outer, baserel->relid);
				if (bms_is_empty(required_outer))
					continue;

				/* Get the ParamPathInfo */
				param_info = get_baserel_parampathinfo(root, baserel,
													   required_outer);
				Assert(param_info != NULL);

				/* Add it to list unless we already have it */
				ppi_list = list_append_unique_ptr(ppi_list, param_info);
			}

			/* Try again, now ignoring the expression we found this time */
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}

	/*
	 * Now build a path for each useful outer relation.
	 */
	foreach(lc, ppi_list)
	{
		ParamPathInfo *param_info = (ParamPathInfo *) lfirst(lc);
		double		rows;
		int			width;
		int			disabled_nodes;
		Cost		startup_cost;
		Cost		total_cost;

		/* Get a cost estimate from the remote */
		estimate_path_cost_size(root, baserel,
								param_info->ppi_clauses, NIL, NULL,
								&rows, &width,
								&disabled_nodes,
								&startup_cost, &total_cost);

		/*
		 * ppi_rows currently won't get looked at by anything, but still we
		 * may as well ensure that it matches our idea of the rowcount.
		 */
		param_info->ppi_rows = rows;

		/* Make the path */
		path = create_foreignscan_path(root, baserel,
									   NULL,	/* default pathtarget */
									   rows,
#if PG_VERSION_NUM >= 180000
									   disabled_nodes,
#endif
									   startup_cost,
									   total_cost,
									   NIL, /* no pathkeys */
									   param_info->ppi_req_outer,
									   NULL,
#if PG_VERSION_NUM >= 170000
									   NIL, /* no fdw_restrictinfo list */
#endif
									   NIL);	/* no fdw_private list */
		add_path(baserel, (Path *) path);
	}
}

/*
 * postgresGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
postgresGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *foreignrel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *fdw_scan_tlist = NIL;
	List	   *fdw_recheck_quals = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	bool		has_final_sort = false;
	bool		has_limit = false;
	ListCell   *lc;

	/*
	 * Get FDW private data created by postgresGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_final_sort = boolVal(list_nth(best_path->fdw_private,
										  FdwPathPrivateHasFinalSort));
		has_limit = boolVal(list_nth(best_path->fdw_private,
									 FdwPathPrivateHasLimit));
	}

	if (IS_SIMPLE_REL(foreignrel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = foreignrel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by postgresGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	}
	else
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot.  Also, remove the local conditions
		 * from outer plan's quals, lest they be evaluated twice, once by the
		 * local plan and once by the scan.
		 */
		if (outer_plan)
		{
			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			/*
			 * First, update the plan's qual list if possible.  In some cases
			 * the quals might be enforced below the topmost plan level, in
			 * which case we'll fail to remove them; it's not worth working
			 * harder than this.
			 */
			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}

			/*
			 * Now fix the subplan's tlist --- this might result in inserting
			 * a Result node atop the plan tree.
			 */
			outer_plan = change_plan_targetlist(outer_plan, fdw_scan_tlist,
												best_path->path.parallel_safe);
		}
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
							remote_exprs, best_path->path.pathkeys,
							has_final_sort, has_limit, false,
							&retrieved_attrs, &params_list);

	Query	   *fdwQueryTree = ParseQuery(sql.data, params_list);

	/*
	 * pg_lake tables are identified by their unique relation identifiers,
	 * however the deparse above loses that information. So, we need to
	 * re-associate the unique relation identifiers with the RTEs in the
	 * QueryTree.
	 *
	 * To do that, we use the alias name of the RTE, which is set to
	 * REL_ALIAS_PREFIX.%d, where %d is the unique relation identifier. We
	 * extract the unique relation identifier from the alias name and set it
	 * back in the RTE.
	 */
	AdjustUniqueRelationIdentifiersViaAlias((Node *) fdwQueryTree, NULL);

	/*
	 * Rewrite any unsupported expression to one that DuckDB understands.
	 */
	fdwQueryTree = RewriteQueryTreeForPGDuck(fdwQueryTree);

	/*
	 * Override all the RTE_RELATIONs that are pg_lake tables with
	 * read_table('table_name') function calls.
	 *
	 * In the execution phase, the read_table() function will replaced with
	 * the actual function call (e.g., read_parquet(file_paths),
	 * read_csv(file_paths), etc.
	 *
	 * We have this multi-stage separation for two reasons: 1. The parameters
	 * to read_parquet()/read_csv() etc might not be known during the planning
	 * for all use-cases such as when querying Iceberg tables. In that case,
	 * the set of files to query is determined by whatever the last
	 * transaction on the table did, so we can't reliability know it here in
	 * the planner yet, in case of a prepared statement.
	 *
	 * 2. There are some tiny differences in the function call syntax between
	 * Postgres and the execution engines, such as DuckDB. We want to be able
	 * to flexibly change the function call syntax anyway. Hence, we cannot
	 * apply all the transformations at this stage, on the QueryTree. So, we
	 * defer some parts to execution phase.
	 */
	List	   *rteList =
		ReplacePgLakeTableWithReadTableFunc((Node *) fdwQueryTree);
	char	   *pgDuckSQLTemplate = PreparePGDuckSQLTemplate(fdwQueryTree);

	/* override the query */
	resetStringInfo(&sql);
	appendStringInfoString(&sql, pgDuckSQLTemplate);

	/* Remember remote_exprs for possible use by postgresPlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	List	   *restrictionList = GetCurrentRelationRestrictions();

	fdw_private = list_make4(makeString(sql.data),
							 retrieved_attrs, rteList,
							 restrictionList);
	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name));

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);
}


/*
* AdjustUniqueRelationIdentifiersViaAlias adjusts the unique relation
* identifiers for the RTEs in the QueryTree.
*/
static bool
AdjustUniqueRelationIdentifiersViaAlias(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node,
								 AdjustUniqueRelationIdentifiersViaAlias,
								 context,
								 QTW_EXAMINE_RTES_BEFORE);
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_RELATION)
		{
			Alias	   *alias = rte->alias;

			if (alias != NULL && alias->aliasname != NULL)
			{
				char	   *aliasName = alias->aliasname;

				/* extract the unique identifier from REL_ALIAS_PREFIX.%d */
				int			uniqueRelationIdentifier = atoi(aliasName + 1);

				SetUniqueRelationIdentifier(rte, uniqueRelationIdentifier);
			}
			else
			{
				elog(ERROR, "Alias name is NULL for RTE with relation name %s",
					 get_rel_name(rte->relid));
			}
		}

		/* query_tree_walker descends into RTEs */
		return false;
	}

	return expression_tree_walker(node,
								  AdjustUniqueRelationIdentifiersViaAlias,
								  context);

}

/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc
get_tupdesc_for_join_scan_tuples(ForeignScanState *node)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	TupleDesc	tupdesc;

	/*
	 * The core code has already set up a scan tuple slot based on
	 * fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough,
	 * but there's one case where it isn't.  If we have any whole-row row
	 * identifier Vars, they may have vartype RECORD, and we need to replace
	 * that with the associated table's actual composite type.  This ensures
	 * that when we read those ROW() expression values from the remote server,
	 * we can convert them to a composite type the local server knows.
	 */
	tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var		   *var;
		RangeTblEntry *rte;
		Oid			reltype;

		/* Nothing to do if it's not a generic RECORD attribute */
		if (att->atttypid != RECORDOID || att->atttypmod >= 0)
			continue;

		/*
		 * If we can't identify the referenced table, do nothing.  This'll
		 * likely lead to failure later, but perhaps we can muddle through.
		 */
		var = (Var *) list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
									i)->expr;
		if (!IsA(var, Var) || var->varattno != 0)
			continue;
		rte = list_nth(estate->es_range_table, var->varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;
		reltype = get_rel_type_id(rte->relid);
		if (!OidIsValid(reltype))
			continue;
		att->atttypid = reltype;
		/* shouldn't need to change anything else */
	}
	return tupdesc;
}

/*
 * postgresBeginForeignScan
 *		Initiate an executor scan of a foreign pg_lake table.
 */
static void
postgresBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	PgLakeScanState *fsstate;

	/* copyParamList forces to fetch dynamic parameters */
	ParamListInfo paramListInfo = copyParamList(estate->es_param_list_info);

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (PgLakeScanState *) palloc0(sizeof(PgLakeScanState));
	node->fdw_state = (void *) fsstate;

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->conn = GetPGDuckConnection();

	/* Assign a unique ID for my cursor */
	fsstate->prepared_statement_sent = false;

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSelectSql));

	List	   *rteList =
		list_nth(fsplan->fdw_private, FdwScanPrivateRteList);

	int			rtindex;

	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_base_relids, -1);
	RangeTblEntry *rte = exec_rt_fetch(rtindex, estate);

	/*
	 * There are a few different ways we could detect whether we're doing a
	 * scan (SELECT) for an UPDATE/DELETE, but checking the lock mode seems to
	 * be the simplest.
	 */
	bool		isUpdateDelete = rte->rellockmode == RowExclusiveLock;

	/* result relation ID for an update/delete */
	fsstate->resultRelationId = isUpdateDelete ? rte->relid : InvalidOid;

	/*
	 * Preceding updates may generate new files that we need to be able to see
	 * if we're doing an update, because they may contain some of the rows
	 * that are supposed to be in our snapshot.
	 *
	 * Hence we take a global update/delete lock on the table, but we do not
	 * lock the table itself because we can allow inserts to go through.
	 */
	if (isUpdateDelete)
		LockTableForUpdate(fsstate->resultRelationId);

	List	   *restrictionList =
		list_nth(fsplan->fdw_private, FdwScanPrivateRestrictions);

	/*
	 * In the foreign scan path, child tables are handled by append nodes.
	 */
	bool		includeChildren = false;

	/* find the files to scan for each relation */
	fsstate->scanSnapshot =
		CreatePgLakeScanSnapshot(rteList, restrictionList, paramListInfo,
								 includeChildren, fsstate->resultRelationId);

	/*
	 * We do some extra bookkeeping for scans that are part of an
	 * update/delete to interpret the row identifier (filename,
	 * file_row_number) records.
	 */
	if (isUpdateDelete)
	{
		/* find the files belonging to the result relation */
		PgLakeTableScan *resultTableScan =
			GetTableScanByRelationId(fsstate->scanSnapshot, fsstate->resultRelationId);

		/*
		 * build a file name -> index map for the result relation (based on
		 * snapshot order) to be able to compactly reference files in the
		 * t_self field of the TupleTableSlots that are passed from the
		 * current scan to ExecForeignUpdate/Delete.
		 */
		HASHCTL		ctl;

		ctl.keysize = MAXPGPATH;
		ctl.entrysize = sizeof(ResultFileIndex);
		ctl.hcxt = CurrentMemoryContext;

		fsstate->resultFileIndexes =
			hash_create("result file indexes", 512, &ctl, HASH_STRINGS | HASH_ELEM | HASH_CONTEXT);

		ListCell   *fileScanCell = NULL;

		foreach(fileScanCell, resultTableScan->fileScans)
		{
			PgLakeFileScan *fileScan = lfirst(fileScanCell);

			/*
			 * We use the base URL without query arguments as the identifier
			 * in the hash. When read_parquet returns the file name our file
			 * system may have injected additional query arguments, which
			 * would cause it to not match.
			 */
			char	   *baseUrl = StripFromChar(fileScan->path, '?');

			bool		isFound = false;
			ResultFileIndex *fileIndex =
				hash_search(fsstate->resultFileIndexes, baseUrl, HASH_ENTER, &isFound);

			fileIndex->index = foreach_current_index(fileScanCell);

			/*
			 * keep track of number of rows we skip (read: delete without
			 * scanning)
			 */
			if (fileScan->allRowsMatch)
			{
				fsstate->skippableRows += fileScan->rowCount - fileScan->deletedRowCount;
				fsstate->skippableDataFiles++;
			}
		}

		/*
		 * set up parsing information for row location (filename,
		 * file_row_number) records
		 */
		TupleDesc	rowLocationTupleDesc = CreateRowIdTupleDesc();

		BlessTupleDesc(rowLocationTupleDesc);

		fsstate->rowLocationTypeMod = rowLocationTupleDesc->tdtypmod;

		fmgr_info(F_RECORD_IN, &fsstate->recordInFunction);

		/* calculate the number of bits used needed the file index */
		int			resultFileCount = list_length(resultTableScan->fileScans);

		/*
		 * we add an extra 0.5 to avoid rounding errors, since log2 returns
		 * double
		 */
		int			fileIndexBits = (int) (log2(resultFileCount) + 1 + 0.5);

		fsstate->fileRowNumberBits = 48 - fileIndexBits;
	}

	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwScanPrivateRetrievedAttrs);

	/*
	 * We used to check for system columns here, but these checks are now done
	 * in postgresGetForeignRelSize() prior to ever invoking
	 * postgresBeginForeignScan.
	 */

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "pg_lake_table tuple data",
											   ALLOCSET_DEFAULT_SIZES);
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "pg_lake_table temporary data",
											  ALLOCSET_SMALL_SIZES);

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		fsstate->rel = node->ss.ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	}
	else
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = get_tupdesc_for_join_scan_tuples(node);
	}

	fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	fsstate->numParams = list_length(fsplan->fdw_exprs);
	if (fsstate->numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 fsstate->numParams,
							 &fsstate->param_flinfo,
							 &fsstate->param_exprs,
							 &fsstate->param_values);

}


/*
 * postgresIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
postgresIterateForeignScan(ForeignScanState *node)
{
	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * In sync mode, if this is the first call after Begin or ReScan, we need
	 * to create the cursor on the remote side.
	 */
	if (!fsstate->prepared_statement_sent)
		send_prepared_statement(node);

	/*
	 * Get some more tuples, if we've run out.
	 */
	if (fsstate->next_tuple >= fsstate->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (!fsstate->eof_reached)
			fetch_more_data(node);
		/* If we didn't get any tuples, must be end of data. */
		if (fsstate->next_tuple >= fsstate->num_tuples)
			return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
	ExecStoreHeapTuple(fsstate->tuples[fsstate->next_tuple++],
					   slot,
					   false);

	return slot;
}

/*
 * postgresReScanForeignScan
 *		Restart the scan.
 */
static void
postgresReScanForeignScan(ForeignScanState *node)
{
	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->prepared_statement_sent)
		return;

	/* we are done with the previous scan, now get ready for the next scan */
	PGresult   *res = WaitForLastResult(fsstate->conn);

	/*
	 * We might get back more results than the executor requires (e.g. when
	 * LIMIT in a subplan is not pushed down), simply discard the result.
	 */
	if (res != NULL)
		PQclear(res);

	/* forces a fresh result set */
	send_prepared_statement(node);
}

/*
 * postgresEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
postgresEndForeignScan(ForeignScanState *node)
{
	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Release remote connection */
	ReleasePGDuckConnection(fsstate->conn);
	fsstate->conn = NULL;

	/* MemoryContexts will be deleted automatically. */
}


/*
 * postgresAddForeignUpdateTargets
 *		Add resjunk column(s) needed for update/delete on a foreign table
 */
static void
postgresAddForeignUpdateTargets(PlannerInfo *root,
								Index rtindex,
								RangeTblEntry *targetRte,
								Relation targetRelation)
{

	/*
	 * Using the ctid attribute number here will cause ctid to be included in
	 * the target list.
	 *
	 * While the theory of AddForeignUpdateTargets is that you can add junk
	 * columns that uniquely identify rows. In practice they must be columns
	 * that exist in the table. Adding a non-existent attribute number will
	 * immediately trigger an assert failure in add_vars_to_targetlist.
	 *
	 * Luckily, we can can overload ctid column, which loses meaning in the
	 * context of foreign data, and this is an established practice since
	 * postgres_fdw does it too for the remote ctid.
	 *
	 * When constructing the remote query, we return the file name and row
	 * number as a record field named ctid.
	 *
	 * We add a (resjunk) Var for that field here, such that the value will
	 * show up in the planSlot in ExecuteForeignUpdate/Delete.
	 */
	Var		   *rowLocationVar = makeVar(rtindex,
										 SelfItemPointerAttributeNumber,
										 TIDOID,
										 -1,
										 InvalidOid,
										 0);

	add_row_identity_var(root, rowLocationVar, rtindex, "ctid");

	/*
	 * Get all the column references in the RETURNING clause.
	 */
	List	   *returningList = root->parse->returningList;
	List	   *returningVars = pull_vars_of_level((Node *) returningList, 0);

#if PG_VERSION_NUM >= 180000
	ListCell   *varCell = NULL;
	List	   *copyOfReturningVars = NIL;

	foreach(varCell, returningVars)
	{
		Var		   *returningVar = lfirst(varCell);

		/*
		 * We are pushing down the returning clause, so we need to set the
		 * varreturningtype to VAR_RETURNING_DEFAULT. Otherwise, we'd end up
		 * with a VAR_RETURNING_NEW or VAR_RETURNING_OLD on the foreign scan,
		 * which is wrong.
		 */
		Var		   *copyOfVar = copyObject(returningVar);

		copyOfVar->varreturningtype = VAR_RETURNING_DEFAULT;
		copyOfReturningVars = lappend(copyOfReturningVars, copyOfVar);
	}

	if (root->parse->commandType == CMD_DELETE)
		AddDeleteReturningTargetTableVars(root, rtindex, targetRelation, copyOfReturningVars);
#else
	if (root->parse->commandType == CMD_DELETE)
		AddDeleteReturningTargetTableVars(root, rtindex, targetRelation, returningVars);
#endif
}


/*
 * postgresPlanForeignModify
 *
 * We use this hook to verify that an INSERT statement with a RETURNING clause
 * does not have a system column.
 *
 */
static List *
postgresPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index)
{
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	List	   *returningList;

	if (plan->returningLists)
	{
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

		ListCell   *lc;

		foreach(lc, returningList)
		{
			TargetEntry *target = castNode(TargetEntry, lfirst(lc));

			if (IsA(target->expr, Var))
			{
				Var		   *var = castNode(Var, target->expr);

				ErrorIfSystemColumnUsed(rte->relid, var->varattno);
			}
		}
	}

	/* we don't care about the state, only using this as a hook */
	return NIL;
}


/*
 * The FDW implementation expects ExecForeignDelete to populate the returned
 * slot with values that are used in RETURNING. We only write to a delete file
 * and perform no additional reads in ExecForeignDelete. Hence, we request the
 * columns that are used in RETURNING from the initial scan. They will be passed
 * to ExecForeignDelete via planSlot.
 */
static void
AddDeleteReturningTargetTableVars(PlannerInfo *root,
								  Index rtindex,
								  Relation targetRelation,
								  List *returningVars)
{
	TupleDesc	tupleDesc = RelationGetDescr(targetRelation);

	/*
	 * If there is a wholerow reference (varattno 0), we request all the
	 * columns in the table.
	 */
	ListCell   *varCell = NULL;

	foreach(varCell, returningVars)
	{
		Var		   *returningVar = lfirst(varCell);

		if (returningVar->varno == root->parse->resultRelation &&
			returningVar->varattno == 0)
		{
			AddAllTargetTableVars(root, rtindex, targetRelation);

			/*
			 * If we already added all the columns there is no more work to
			 * do. Multiple uses of the same column will be handled by
			 * PostgreSQL.
			 */
			return;
		}
	}

	/*
	 * If there is no wholerow reference, we only request columns that are
	 * used in RETURNING. Given columnar storage, this can greatly speed up
	 * the initial scan.
	 */
	foreach(varCell, returningVars)
	{
		Var		   *returningVar = lfirst(varCell);

		/*
		 * We only care about Vars that point to the result relation, since
		 * others are handled by PostgreSQL.
		 */
		if (returningVar->varno != root->parse->resultRelation)
			continue;

#if PG_VERSION_NUM >= 180000
		if (returningVar->varreturningtype == VAR_RETURNING_NEW ||
			returningVar->varreturningtype == VAR_RETURNING_OLD)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DELETE RETURNING clause with NEW or OLD columns is not supported for foreign tables"),
					 errhint("Use a regular column reference instead.")));
#endif

		/*
		 * Update the varno (and importantly, varnosyn) to reference the
		 * partition. We'll use the varnosyn in create_foreign_modify.
		 */
		Var		   *newVar = copyObject(returningVar);

		newVar->varno = rtindex;
		newVar->varnosyn = rtindex;

		Form_pg_attribute attribute =
			TupleDescAttr(tupleDesc, newVar->varattno - 1);

		char	   *columnName = NameStr(attribute->attname);

		add_row_identity_var(root, newVar, rtindex, columnName);
	}
}


/*
 * AddAllTargetTableVars adds a Var for every column in the target table,
 * which is used when requesting a wholerow Var.
 */
static void
AddAllTargetTableVars(PlannerInfo *root,
					  Index rtindex,
					  Relation targetRelation)
{
	TupleDesc	relationDesc = RelationGetDescr(targetRelation);

	for (int columnIndex = 0; columnIndex < relationDesc->natts; columnIndex++)
	{
		Form_pg_attribute attribute = TupleDescAttr(relationDesc, columnIndex);

		if (attribute->attisdropped)
			continue;

		Var		   *newVar = makeVar(rtindex,
									 columnIndex + 1,
									 attribute->atttypid,
									 attribute->atttypmod,
									 attribute->attcollation,
									 0);

		/*
		 * We use varnosyn in create_foreign_modify because it is not affected
		 * by subplans.
		 */
		newVar->varnosyn = rtindex;

		char	   *columnName = NameStr(attribute->attname);

		add_row_identity_var(root, newVar, rtindex, columnName);
	}
}


/*
 * postgresBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
postgresBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
	PgLakeModifyState *fmstate;

	/*
	 * For simple DELETE (no join, no scan output), we can skip files that are
	 * known to fully match the WHERE clause in the underlying ForeignScan.
	 *
	 * We want this logic to be visible in the EXPLAIN case as well.
	 */
	if (IsSimpleDelete(mtstate, resultRelInfo->ri_RelationDesc))
	{
		ForeignScanState *fsstate = (ForeignScanState *) mtstate->ps.lefttree;
		PgLakeScanState *scanState = (PgLakeScanState *) fsstate->fdw_state;

		/*
		 * Setting skipFullMatchFiles ultimately affects the query generation
		 * in send_prepared_statement and postgresExplainForeignScan.
		 */
		scanState->skipFullMatchFiles = true;
	}

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Construct an execution state. */
	fmstate = create_foreign_modify(resultRelInfo->ri_RelationDesc,
									resultRelInfo->ri_RangeTableIndex,
									mtstate);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * postgresExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
postgresExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	PgLakeModifyState *fmstate = (PgLakeModifyState *) resultRelInfo->ri_FdwState;

	/*
	 * Constraint checks are skipped by PostgreSQL itself, since it assumes
	 * them to be unenforceable as data can change underneath.
	 */
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, slot, estate);

	WriteInsertRecord(fmstate, slot);

	return slot;
}

/*
 * postgresExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
postgresExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	PgLakeModifyState *fmstate = (PgLakeModifyState *) resultRelInfo->ri_FdwState;
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;

	/* look up the ctid value */
	bool		isNull = false;
	Datum		ctidDatum = ExecGetJunkAttribute(planSlot, fmstate->ctidAttno, &isNull);

	if (isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("ctid is NULL")));

	/* get the ctid value produced by RowIdRecordStringToItemPointer */
	ItemPointer ctid = (ItemPointer) DatumGetPointer(ctidDatum);

	/*
	 * If the row was already updated, we skip subsequent updates in the same
	 * command to match the behaviour of PostgreSQL.
	 */
	if (fmstate->requiresUpdateTracking &&
		!IsFirstUpdateOfTuple(&fmstate->updateTrackingState, ctid))
	{
		return NULL;
	}

	/*
	 * Constraint checks are skipped by PostgreSQL itself, since it assumes
	 * them to be unenforceable as data can change underneath.
	 */
	if (resultRelation->rd_att->constr)
		ExecConstraints(resultRelInfo, slot, estate);

	/*
	 * Also check the tuple against the partition constraint, since PostgreSQL
	 * does not expect to have to do this for foreign relations. It also does
	 * not handle cross-partition updates.
	 */
	if (resultRelation->rd_rel->relispartition &&
		!ExecPartitionCheck(resultRelInfo, slot, estate, false))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cross-partition updates are not supported on "
							   "pg_lake tables")));
	}

	/* UPDATE = DELETE + INSERT */

	/* write a delete record for the file name and row number to a delete file */
	DeleteSingleRow(fmstate, ctid);

	/* insert the new values into an insert file */
	WriteInsertRecord(fmstate, slot);

	return slot;
}


/*
 * postgresExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
postgresExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	PgLakeModifyState *fmstate = (PgLakeModifyState *) resultRelInfo->ri_FdwState;

	/* look up the ctid value */
	bool		isNull = false;
	Datum		ctidDatum = ExecGetJunkAttribute(planSlot, fmstate->ctidAttno, &isNull);

	if (isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("ctid is NULL")));

	/* get the ctid value produced by RowIdRecordStringToItemPointer */
	ItemPointer ctid = (ItemPointer) DatumGetPointer(ctidDatum);

	/* write a delete record for the file name and row number to a delete file */
	DeleteSingleRow(fmstate, ctid);

	/*
	 * In case of RETURNING we requested columns to be read via
	 * AddForeignUpdateTargets. Populate the slot here.
	 */
	StoreDeleteReturningSlot(planSlot, slot, fmstate->planSlotAttributeNumbers);

	return slot;
}


/*
 * DeleteSingleRow deletes a single row specified by the ctid column in planSlot.
 */
static void
DeleteSingleRow(PgLakeModifyState * fmstate, ItemPointer ctid)
{
	/* deleteSlot can be NULL for WHERE false, but we should not get here */
	Assert(fmstate->deleteSlot != NULL);

	/* extract the file index from the CTID */
	uint64		fileIndex =
		ExtractFileIndexFromItemPointer(ctid, fmstate->fileRowNumberBits);

	if (fileIndex >= list_length(fmstate->fileModifyStates))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("invalid file index " UINT64_FORMAT, fileIndex)));

	/* extract the file row number from the CTID */
	uint64		fileRowNumber =
		ExtractFileRowNumberFromItemPointer(ctid, fmstate->fileRowNumberBits);

	/* discover the source file */
	PgLakeFileModifyState *fileModifyState =
		list_nth(fmstate->fileModifyStates, fileIndex);

	if (fileModifyState->deleteDest == NULL)
		/* not tracking deletions for this file, which is fully deleted */
		return;

	/* construct a (filename,position,...) delete record */
	PrepareDeletionSlot(fileModifyState, fileRowNumber, fmstate->deleteSlot);

	if (fileModifyState->modifiedRowCount == 0)
	{
		DestReceiver *deleteDest = fileModifyState->deleteDest;

		/* first row for this file */
		deleteDest->rStartup(deleteDest,
							 fmstate->operation,
							 fmstate->deleteSlot->tts_tupleDescriptor);
	}

	/* write the delete record to a delete file */
	WriteDeleteRecord(fileModifyState, fmstate->deleteSlot);
}


/*
 * StoreDeleteReturningSlot sets the values for DELETE .. RETURNING in
 * returningSlot by taking them from planSlot.
 */
static void
StoreDeleteReturningSlot(TupleTableSlot *planSlot,
						 TupleTableSlot *returningSlot,
						 List *planSlotAttributeNumbers)
{
	TupleDesc	planSlotTupleDesc = planSlot->tts_tupleDescriptor;
	TupleDesc	relationTupleDesc = returningSlot->tts_tupleDescriptor;
	Datum	   *values = (Datum *) palloc0(relationTupleDesc->natts * sizeof(Datum));
	bool	   *nulls = (bool *) palloc(relationTupleDesc->natts * sizeof(bool));

	if (planSlotTupleDesc->natts == 0)
		/* no RETURNING */
		return;

	if (relationTupleDesc->natts == 0)
		/* 0 column table */
		return;

	/*
	 * make sure the tuple is fully deconstructed, since we'll use all the
	 * values
	 */
	slot_getallattrs(planSlot);

	/* we return not-requested values as NULL */
	memset(nulls, true, relationTupleDesc->natts * sizeof(bool));

	ItemPointer ctid = NULL;

	for (int attributeIndex = 0; attributeIndex < planSlotTupleDesc->natts; attributeIndex++)
	{
		/* use the column mapping from create_foreign_modify */
		int			columnNumber = list_nth_int(planSlotAttributeNumbers, attributeIndex);

		if (columnNumber == SelfItemPointerAttributeNumber)
		{
			/* for RETURNING ctid, use the synthetic value */
			ctid = DatumGetItemPointer(planSlot->tts_values[attributeIndex]);
			continue;
		}

		/* skip system columns */
		if (columnNumber <= 0)
			continue;

		if (columnNumber > relationTupleDesc->natts)
			elog(ERROR, "invalid column number %d", columnNumber);

		/* copy value from planSlot to slot */
		values[columnNumber - 1] = planSlot->tts_values[attributeIndex];
		nulls[columnNumber - 1] = planSlot->tts_isnull[attributeIndex];
	}

	HeapTuple	returningTuple =
		heap_form_tuple(returningSlot->tts_tupleDescriptor, values, nulls);

	if (ctid)
		returningTuple->t_self = returningTuple->t_data->t_ctid = *ctid;

	bool		shouldFree = true;

	ExecForceStoreHeapTuple(returningTuple, returningSlot, shouldFree);
}


/*
 * postgresEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
postgresEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	PgLakeModifyState *fmstate = (PgLakeModifyState *) resultRelInfo->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	/* drop the update tracking table */
	if (fmstate->operation == CMD_UPDATE || fmstate->operation == CMD_DELETE)
		FinishRelationUpdateTracking(&fmstate->updateTrackingState);

	FinishForeignModify(fmstate);
}

/*
 * postgresBeginForeignInsert
 *		Begin an insert operation on a foreign table
 */
static void
postgresBeginForeignInsert(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo)
{
	PgLakeModifyState *fmstate;
	ModifyTable *plan = castNode(ModifyTable, mtstate->ps.plan);
	Relation	rel = resultRelInfo->ri_RelationDesc;

	/*
	 * If the foreign table we are about to insert routed rows into is also an
	 * UPDATE subplan result rel that will be updated later, proceeding with
	 * the INSERT will result in the later UPDATE incorrectly modifying those
	 * routed rows, so prevent the INSERT --- it would be nice if we could
	 * handle this case; but for now, throw an error for safety.
	 */
	if (plan && plan->operation == CMD_UPDATE &&
		(resultRelInfo->ri_usesFdwDirectModify ||
		 resultRelInfo->ri_FdwState))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route tuples into foreign table to be updated \"%s\"",
						RelationGetRelationName(rel))));

	/* Construct an execution state. */
	fmstate = create_foreign_modify(resultRelInfo->ri_RelationDesc,
									resultRelInfo->ri_RangeTableIndex,
									mtstate);

	/*
	 * If the given resultRelInfo already has PgLakeModifyState set, it means
	 * the foreign table is an UPDATE subplan result rel; in which case, store
	 * the resulting state into the aux_fmstate of the PgLakeModifyState.
	 */
	if (resultRelInfo->ri_FdwState)
	{
		Assert(plan && plan->operation == CMD_UPDATE);
		Assert(resultRelInfo->ri_usesFdwDirectModify == false);
		((PgLakeModifyState *) resultRelInfo->ri_FdwState)->aux_fmstate = fmstate;
	}
	else
		resultRelInfo->ri_FdwState = fmstate;
}

/*
 * postgresEndForeignInsert
 *		Finish an insert operation on a foreign table
 */
static void
postgresEndForeignInsert(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	PgLakeModifyState *fmstate = (PgLakeModifyState *) resultRelInfo->ri_FdwState;

	Assert(fmstate != NULL);

	/*
	 * If the fmstate has aux_fmstate set, get the aux_fmstate (see
	 * postgresBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
		fmstate = fmstate->aux_fmstate;

	FinishForeignModify(fmstate);
}


/*
 * postgresIsForeignRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
postgresIsForeignRelUpdatable(Relation rel)
{
	Oid			relationId = RelationGetRelid(rel);

	/*
	 * By default, all pg_lake foreign tables are assumed not writable. This
	 * can be overridden by a per-table setting.
	 */
	if (!IsWritablePgLakeTable(relationId) && !IsWritableIcebergTable(relationId))
		return 0;

	int			writeFlags = (1 << CMD_INSERT);

	/*
	 * We currently only support update/delete on Parquet, because we cannot
	 * get the necessary file and row information from read_csv/read_json.
	 */
	CopyDataFormat format = GetForeignTableFormat(relationId);

	if (format == DATA_FORMAT_PARQUET)
		writeFlags |= (1 << CMD_UPDATE) | (1 << CMD_DELETE);

	return writeFlags;
}


/*
 * WriteInsertRecord writes the tuple held by the given slot to the insert
 * destination.
 */
static void
WriteInsertRecord(PgLakeModifyState * modifyState, TupleTableSlot *slot)
{
	DestReceiver *insertDest = modifyState->insertDest;

	if (modifyState->insertedRowCount == 0)
	{
		/* incoming inserts have the tuple descriptor of the table */
		insertDest->rStartup(insertDest,
							 CMD_INSERT,
							 RelationGetDescr(modifyState->rel));
	}

	insertDest->receiveSlot(slot, insertDest);
	modifyState->insertedRowCount++;
}


/*
 * PrepareDeletionSlot puts a deletion record for the given CTID in the
 * deleteSlot.
 */
static void
PrepareDeletionSlot(PgLakeFileModifyState * fileModifyState,
					uint64 fileRowNumber,
					TupleTableSlot *deleteSlot)
{
	Datum		datums[] = {
		fileModifyState->pathDatum,
		UInt64GetDatum(fileRowNumber),
		0
	};

	bool		nulls[] = {
		false,
		false,
		true
	};

	MinimalTuple deleteTuple = heap_form_minimal_tuple(deleteSlot->tts_tupleDescriptor,
													   datums,
													   nulls
#if PG_VERSION_NUM >= 180000
													   ,0	/* extra */
#endif
		);

	bool		shouldFree = true;

	ExecStoreMinimalTuple(deleteTuple, deleteSlot, shouldFree);
}

/*
 * WriteDeleteRecord writes a (data_file_url,position,NULL) record
 * to a the delete destination that corresponds to the file being
 * modified.
 */
static void
WriteDeleteRecord(PgLakeFileModifyState * fileModifyState,
				  TupleTableSlot *deleteSlot)
{
	DestReceiver *deleteDest = fileModifyState->deleteDest;

	if (deleteDest == NULL)
		/* all rows are deleted, we do not write delete records */
		return;

	deleteDest->receiveSlot(deleteSlot, deleteDest);
	fileModifyState->modifiedRowCount++;

	if (fileModifyState->modifiedRowCount == fileModifyState->liveRowCount)
	{
		/*
		 * All rows are deleted, we do not actually need a delete file. Delete
		 * it now to free up disk space.
		 *
		 * We'll just pass on the modified row count.
		 */
		fileModifyState->deleteDest->rShutdown(fileModifyState->deleteDest);
		fileModifyState->deleteDest->rDestroy(fileModifyState->deleteDest);
		fileModifyState->deleteDest = NULL;

		DeleteLocalFile(fileModifyState->deleteFile);
		fileModifyState->deleteFile = NULL;
	}
}


/*
 * CreateRowIdTupleDesc returns a TupleDescriptor for interpreting
 * row IDs returned by read_parquet.
 */
static TupleDesc
CreateRowIdTupleDesc(void)
{
	int			columnCount = 2;
	TupleDesc	tupleDescriptor = CreateTemplateTupleDesc(columnCount);

	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "filename",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "file_row_number",
					   INT8OID, -1, 0);

	return tupleDescriptor;
}


/*
 * RecordStringToItemPointer converts a row type string of the form
 * (filename text, file_row_number int) into a 48-bit CTID.
 */
static ItemPointer
RowIdRecordStringToItemPointer(char *recordString,
							   PgLakeScanState * scanState)
{
	int			recordTypeMod = scanState->rowLocationTypeMod;
	HTAB	   *resultFileIndexes = scanState->resultFileIndexes;
	int			fileRowNumberBits = scanState->fileRowNumberBits;

	Datum		recordDatum = FunctionCall3(&scanState->recordInFunction,
											CStringGetDatum(recordString),
											RECORDOID, recordTypeMod);
	HeapTupleHeader fileIndexTuple = DatumGetHeapTupleHeader(recordDatum);

	bool		isNull = false;
	Datum		fileNameDatum = GetAttributeByNum(fileIndexTuple, 1, &isNull);
	Datum		fileRowNumberDatum = GetAttributeByNum(fileIndexTuple, 2, &isNull);

	char	   *fileName = TextDatumGetCString(fileNameDatum);
	uint64		fileRowNumber = (uint64) DatumGetUInt64(fileRowNumberDatum);

	bool		isFound = false;
	ResultFileIndex *resultFileIndex =
		hash_search(resultFileIndexes, fileName, HASH_FIND, &isFound);

	if (!isFound)
		elog(ERROR, "remote query result did not return expected ctid values");

	if ((fileRowNumber >> fileRowNumberBits) != 0)
		elog(ERROR, "file too large to be updated");

	uint64		ctidInt = ((uint64) resultFileIndex->index << fileRowNumberBits) | fileRowNumber;
	ItemPointer ctid = UInt64ToItemPointer(ctidInt);

	return ctid;
}


/*
 * ExtractFileIndexFromItemPointer extracts the file index from the
 * 48-bit CTID.
 *
 * The last fileRowNumberBits bits are used for the file row number
 * and the remaining bits for the file index.
 */
uint64
ExtractFileIndexFromItemPointer(ItemPointer ctid, int fileRowNumberBits)
{
	uint64		ctidInt = ItemPointerToUInt64(ctid);

	/* the leading bits contain the file row number */
	return ctidInt >> fileRowNumberBits;
}

/*
 * ExtractFileRowNumberFromItemPointer extracts the file row number from the
 * 48-bit CTID.
 *
 * The last fileRowNumberBits bits are used for the file row number
 * and the remaining bits for the file index.
 */
uint64
ExtractFileRowNumberFromItemPointer(ItemPointer ctid, int fileRowNumberBits)
{
	uint64		ctidInt = ItemPointerToUInt64(ctid);

	/* construct a bitmask for the last fileRowNumberBits bits */
	uint64		fileRowNumberMask = ((uint64) 1 << fileRowNumberBits) - 1;

	/* the trailing bits contain the file row number */
	return ctidInt & fileRowNumberMask;
}


/*
 * FinishForeignModify finalizes a write to pg_lake table.
 */
static void
FinishForeignModify(PgLakeModifyState * fmstate)
{
	ListCell   *fileModifyCell = NULL;
	List	   *modifications = NIL;

	foreach(fileModifyCell, fmstate->fileModifyStates)
	{
		PgLakeFileModifyState *fileModifyState = lfirst(fileModifyCell);

		if (fileModifyState->modifiedRowCount == 0)
			continue;

		/*
		 * Finalize the intermediate CSV file into which we wrote our deletes.
		 */
		if (fileModifyState->deleteDest != NULL)
			fileModifyState->deleteDest->rShutdown(fileModifyState->deleteDest);

		DataFileModification *modification = palloc0(sizeof(DataFileModification));

		modification->type = ADD_DELETION_FILE_FROM_CSV;
		modification->sourcePath = TextDatumGetCString(fileModifyState->pathDatum);
		modification->sourceRowCount = fileModifyState->sourceRowCount;
		modification->liveRowCount = fileModifyState->liveRowCount;
		modification->deleteFile = fileModifyState->deleteFile;
		modification->deletedRowCount = fileModifyState->modifiedRowCount;

		modifications = lappend(modifications, modification);
	}

	if (fmstate->insertedRowCount > 0)
	{
		/*
		 * Finalize the intermediate CSV file into which we wrote our inserts.
		 */
		fmstate->insertDest->rShutdown(fmstate->insertDest);

		bool		partitionedTable =
			GetIcebergTablePartitionByOption(RelationGetRelid(fmstate->rel)) != NULL;

		List	   *insertModifications =
			partitionedTable ? GetPartitionedDestReceiverModifications(fmstate->insertDest) :
			GetMultiDataFileDestReceiverModifications(fmstate->insertDest);

		modifications = list_concat(modifications, insertModifications);
	}

	ApplyDataFileModifications(fmstate->rel, modifications);
}


/*
 * postgresRecheckForeignScan
 *		Execute a local join execution plan for a foreign join
 */
static bool
postgresRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
	Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
	PlanState  *outerPlan = outerPlanState(node);
	TupleTableSlot *result;

	/* For base foreign relations, it suffices to set fdw_recheck_quals */
	if (scanrelid > 0)
		return true;

	Assert(outerPlan != NULL);

	/* Execute a local join execution plan */
	result = ExecProcNode(outerPlan);
	if (TupIsNull(result))
		return false;

	/* Store result in the given slot */
	ExecCopySlot(slot, result);

	return true;
}


/*
 * postgresExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
postgresExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *plan = castNode(ForeignScan, node->ss.ps.plan);
	List	   *fdw_private = plan->fdw_private;

	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;
	int			numParams = fsstate->numParams;
	const char **paramValues = fsstate->param_values;

	/*
	 * Identify foreign scans that are really joins or upper relations.  The
	 * input looks something like "(1) LEFT JOIN (2)", and we must replace the
	 * digit string(s), which are RT indexes, with the correct relation names.
	 * We do that here, not when the plan is created, because we can't know
	 * what aliases ruleutils.c will assign at plan creation time.
	 */
	if (list_length(fdw_private) > FdwScanPrivateRelations)
	{
		StringInfo	relations;
		char	   *rawrelations;
		char	   *ptr;
		int			minrti,
					rtoffset;

		rawrelations = strVal(list_nth(fdw_private, FdwScanPrivateRelations));

		/*
		 * A difficulty with using a string representation of RT indexes is
		 * that setrefs.c won't update the string when flattening the
		 * rangetable.  To find out what rtoffset was applied, identify the
		 * minimum RT index appearing in the string and compare it to the
		 * minimum member of plan->fs_base_relids.  (We expect all the relids
		 * in the join will have been offset by the same amount; the Asserts
		 * below should catch it if that ever changes.)
		 */
		minrti = INT_MAX;
		ptr = rawrelations;
		while (*ptr)
		{
			if (isdigit((unsigned char) *ptr))
			{
				int			rti = strtol(ptr, &ptr, 10);

				if (rti < minrti)
					minrti = rti;
			}
			else
				ptr++;
		}
		rtoffset = bms_next_member(plan->fs_base_relids, -1) - minrti;

		/* Now we can translate the string */
		relations = makeStringInfo();
		ptr = rawrelations;
		while (*ptr)
		{
			if (isdigit((unsigned char) *ptr))
			{
				int			rti = strtol(ptr, &ptr, 10);
				RangeTblEntry *rte;
				char	   *relname;
				char	   *refname;

				rti += rtoffset;
				Assert(bms_is_member(rti, plan->fs_base_relids));
				rte = rt_fetch(rti, es->rtable);
				Assert(rte->rtekind == RTE_RELATION);
				/* This logic should agree with explain.c's ExplainTargetRel */
				relname = get_rel_name(rte->relid);
				if (es->verbose)
				{
					char	   *namespace;

					namespace = get_namespace_name_or_temp(get_rel_namespace(rte->relid));
					appendStringInfo(relations, "%s.%s",
									 quote_identifier(namespace),
									 quote_identifier(relname));
				}
				else
					appendStringInfoString(relations,
										   quote_identifier(relname));
				refname = (char *) list_nth(es->rtable_names, rti - 1);
				if (refname == NULL)
					refname = rte->eref->aliasname;
				if (strcmp(refname, relname) != 0)
					appendStringInfo(relations, " %s",
									 quote_identifier(refname));
			}
			else
				appendStringInfoChar(relations, *ptr++);
		}
		ExplainPropertyText("Relations", relations->data, es);
	}

	char	   *sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));

	ExplainPropertyText("Engine", "DuckDB", es);

	if (es->verbose)
	{
		int			dataFileScans = 0;
		int			deleteFileScans = 0;

		SnapshotFilesScanned(fsstate->scanSnapshot, &dataFileScans, &deleteFileScans);

		if (fsstate->skipFullMatchFiles)
			dataFileScans -= fsstate->skippableDataFiles;

		char	   *dataFileScansString = psprintf("%d", dataFileScans);
		char	   *deleteFileScansString = psprintf("%d", deleteFileScans);

		ExplainPropertyText("Data Files Scanned", dataFileScansString, es);
		ExplainPropertyText("Deletion Files Scanned", deleteFileScansString, es);

		if (fsstate->skipFullMatchFiles)
		{
			char	   *dataFilesSkippedString = psprintf(UINT64_FORMAT, fsstate->skippableDataFiles);

			ExplainPropertyText("Data Files Skipped", dataFilesSkippedString, es);
		}
	}

	/*
	 * Add remote query without read_parquet calls, when VERBOSE option is
	 * specified.
	 */
	if (es->verbose)
	{
		char	   *explainQuery =
			ReplaceReadTableFunctionCalls(sql, fsstate->scanSnapshot, EXPLAIN_REQUESTED);

		ExplainPropertyText("Vectorized SQL", explainQuery, es);
	}

	int			scanFlags = 0;

	if (fsstate->skipFullMatchFiles)
		scanFlags |= SKIP_FULL_MATCH_FILES;

	char	   *fullQuery = ReplaceReadTableFunctionCalls(fsstate->query,
														  fsstate->scanSnapshot,
														  scanFlags);

	/*
	 * Add EXPLAIN output for the actual query with read_parquet calls.
	 */
	ExplainPGDuckQuery(fullQuery, numParams, paramValues, es);
}


/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 * fpextra specifies additional post-scan/join-processing steps such as the
 * final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						PgLakePathExtraData * fpextra,
						double *p_rows, int *p_width,
						int *p_disabled_nodes,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) foreignrel->fdw_private;

	/*
	 * We hard code the expected row count from the top-level foreign scan
	 * node. See comments on ESTIMATED_ROW_COUNT for the reasoning.
	 */
	double		rows = ESTIMATED_ROW_COUNT;
	double		retrieved_rows = rows;
	int			width = fpinfo->width;
	int			disabled_nodes = 0;
	Cost		startup_cost = fpinfo->startup_cost;
	Cost		total_cost = fpinfo->total_cost;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	/*
	 * We hard code fpinfo->use_remote_estimate=true so that we imitate that
	 * the remote server provides the estimates. However, we also hard code
	 * the expected row count from the top-level foreign scan node.
	 *
	 * We do that because the cost calculations in postgres_fdw's
	 * estimate_path_cost_size() when fpinfo->use_remote_estimate=false are
	 * overly complex for what we have now.
	 *
	 * So, we imiate that the remote server provides the estimates and we hard
	 * code the expected row count from the top-level foreign scan node.
	 */
	if (fpinfo->use_remote_estimate)
	{
		List	   *remote_param_join_conds;
		List	   *local_param_join_conds;
		Selectivity local_sel;
		QualCost	local_cost;
		List	   *fdw_scan_tlist = NIL;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		classifyConditions(root, foreignrel, param_join_conds,
						   &remote_param_join_conds, &local_param_join_conds);

		/* Build the list of columns to be fetched from the foreign server. */
		if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
			fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
		else
			fdw_scan_tlist = NIL;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel = clauselist_selectivity(root,
										   local_param_join_conds,
										   foreignrel->relid,
										   JOIN_INNER,
										   NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;

		/*
		 * Add in tlist eval cost for each output row.  In case of an
		 * aggregate, some of the tlist expressions such as grouping
		 * expressions will be evaluated remotely, so adjust the costs.
		 */
		startup_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.per_tuple * rows;
		if (IS_UPPER_REL(foreignrel))
		{
			QualCost	tlist_cost;

			cost_qual_eval(&tlist_cost, fdw_scan_tlist, root);
			startup_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.per_tuple * rows;
		}
	}
	else
	{
		/*
		 * postgres_fdw's estimate_path_cost_size() contains a comprehensive
		 * logic for estimating the cost and size of a foreign scan when the
		 * table or the server is not configured to use remote estimates.
		 *
		 * In this fork, we have removed the logic and always rely on the
		 * remote server to provide the estimates.
		 */
		Assert(false);
	}

	/*
	 * If this includes the final sort step, we favor the sort to be pushed
	 * down, hence decreasing the costs such that the postgres planner picks
	 * this path.
	 *
	 * Our approach is arbitrary and we might need to revisit this in the
	 * future.
	 */
	if (fpextra && fpextra->has_final_sort)
	{
		startup_cost *= 0.01;
		total_cost *= 0.01;
	}

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL)
	{
		fpinfo->retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/*
	 * If we have LIMIT, we should prefer performing the restriction remotely
	 * rather than locally, as the former avoids extra row fetches from the
	 * remote that the latter might cause. Postgres_fdw has a lot more
	 * comprehensive logic for calculating this. However, we have removed that
	 * logic and incorporated a simple approach to favor the remote LIMITs.
	 *
	 * Our approach is arbitrary and we might need to revisit this in the
	 * future.
	 */
	if (fpextra && fpextra->has_limit &&
		fpextra->limit_tuples > 0)
	{
		Assert(fpinfo->rows > 0);
		total_cost *= 0.01;
	}

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_disabled_nodes = disabled_nodes;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}


/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
	Expr	   *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}

/**
 * Sends a prepared statement to the foreign server for execution.
 *
 * This function constructs an array of query parameter values in text format and sends
 * the prepared statement to the foreign server using PQsendQueryParams. It also sets
 * the single row mode for the connection and initializes the state variables for the
 * foreign scan.
 */
static void
send_prepared_statement(ForeignScanState *node)
{
	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int			numParams = fsstate->numParams;
	const char **values = fsstate->param_values;
	int			scanFlags = 0;

	if (fsstate->skipFullMatchFiles)
		scanFlags |= SKIP_FULL_MATCH_FILES;

	/*
	 * "Magic" part where we replace the tables in the query with the calls to
	 * read the current set of files.
	 *
	 * We pass in the snapshot to make sure we still get the same files.
	 */
	char	   *query = ReplaceReadTableFunctionCalls(fsstate->query,
													  fsstate->scanSnapshot,
													  scanFlags);

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the per query context, so as not to cause a memory leak
	 * over repeated scans.
	 */
	if (numParams > 0)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

		process_query_params(econtext,
							 fsstate->param_flinfo,
							 fsstate->param_exprs,
							 values);

		MemoryContextSwitchTo(oldcontext);
	}

	/* if sending fails, throws error */
	SendQueryWithParams(fsstate->conn, query, numParams, values);

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->prepared_statement_sent = true;
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->eof_reached = false;
}

/*
 * Fetch some more rows from the connections's prepared statement.
 */
static void
fetch_more_data(ForeignScanState *node)
{
	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;
	PGresult   *volatile res = NULL;
	MemoryContext oldcontext;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	fsstate->tuples = NULL;
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	/* PGresult must be released before leaving this function. */
	PG_TRY();
	{
		int			numrows;
		int			i;

		res = WaitForResult(fsstate->conn);
		if (res == NULL)
		{
			fsstate->eof_reached = true;
			numrows = 0;
		}
		else
		{
			numrows = PQntuples(res);

			/* one way of SingleRowMode to indicate we are at the end */
			if (PQresultStatus(res) == PGRES_TUPLES_OK && numrows == 0)
				fsstate->eof_reached = true;
			else if (PQresultStatus(res) != PGRES_SINGLE_TUPLE)
			{
				ThrowIfPGDuckResultHasError(fsstate->conn, res);
			}
		}

		fsstate->tuples = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
		fsstate->num_tuples = numrows;
		fsstate->next_tuple = 0;

		for (i = 0; i < numrows; i++)
		{
			Assert(IsA(node->ss.ps.plan, ForeignScan));

			fsstate->tuples[i] =
				make_tuple_from_result_row(res, i,
										   fsstate->rel,
										   fsstate->attinmeta,
										   fsstate->retrieved_attrs,
										   node,
										   fsstate->temp_cxt);
		}
	}
	PG_FINALLY();
	{
		PQclear(res);
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

/*
 * create_foreign_modify
 *		Construct an execution state of a foreign insert/update/delete
 *		operation
 */
static PgLakeModifyState *
create_foreign_modify(Relation rel,
					  Index resultRangeTableIndex,
					  ModifyTableState *mtstate)
{
	CmdType		operation = mtstate->operation;
	Oid			relationId = RelationGetRelid(rel);

	/* extra checks */
	if (PgLakeModifyValidityCheckHook)
		PgLakeModifyValidityCheckHook(relationId);

	PgLakeModifyState *fmstate;

	/* Begin constructing PgLakeModifyState. */
	fmstate = (PgLakeModifyState *) palloc0(sizeof(PgLakeModifyState));
	fmstate->rel = rel;
	fmstate->operation = operation;

	/* Initialize auxiliary state */
	fmstate->aux_fmstate = NULL;

	/* Find the underlying foreign table format */
	CopyDataFormat foreignTableFormat = GetForeignTableFormat(RelationGetRelid(rel));

	/* pg_lake_table additions */
	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		const char *partitionBy = GetIcebergTablePartitionByOption(relationId);

		/*
		 * We ingesting data into a partitioned table, and new data files are
		 * always created with the largest/current spec ID. If the table is
		 * not partitioned, we still use the largest/current spec ID (which is
		 * 0), as the table may evolve into a partitioned table in the future.
		 */
		int			specId = GetCurrentSpecId(relationId);

		if (partitionBy != NULL)
		{
			fmstate->insertDest =
				CreatePartitionedDestReceiver(relationId, foreignTableFormat, specId);
		}
		else
		{
			fmstate->insertDest =
				CreateMultiDataFileDestReceiver(relationId,
												foreignTableFormat,
												MaxWriteTempFileSizeMB,
												specId,
												0);
		}
	}

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		/* Find the ctid resjunk column in the subplan's result */
		Plan	   *subPlan = outerPlanState(mtstate)->plan;

		fmstate->ctidAttno = ExecFindJunkAttributeInTlist(subPlan->targetlist, "ctid");
		if (!AttributeNumberIsValid(fmstate->ctidAttno))
			elog(ERROR, "could not find junk ctid column");

		/* retrieve the file scan states from the ForeignScanState */
		PgLakeScanState *scanState = FindResultRelationScanState(&mtstate->ps,
																 relationId);

		if (scanState != NULL)
		{
			/* the scan should be on the current relation */
			Assert(scanState->resultRelationId == relationId);

			/* format options for deletion files */
			bool		includeHeader = true;
			List	   *copyOptions = InternalCSVOptions(includeHeader);

			/* construct a tuple table slot for position deletes */
			TupleDesc	deleteTupleDesc = CreatePositionDeleteTupleDesc();

			fmstate->deleteSlot = MakeSingleTupleTableSlot(deleteTupleDesc,
														   &TTSOpsMinimalTuple);

			/* find the files belonging to the relation being updated */
			PgLakeScanSnapshot *snapshot = scanState->scanSnapshot;
			PgLakeTableScan *resultTableScan = GetTableScanByRelationId(snapshot, relationId);

			if (resultTableScan == NULL)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("could not find result relation in snapshot")));

			List	   *fileModifyStates = NIL;

			/*
			 * For each source file, we currently keep separate delete files.
			 * This gives us some flexibility in terms of how to assemble the
			 * final data files.
			 */
			ListCell   *fileScanCell = NULL;

			foreach(fileScanCell, resultTableScan->fileScans)
			{
				PgLakeFileScan *fileScan = lfirst(fileScanCell);

				if (fileScan->rowCount < 0)
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("deleting from a file with unknown row count "
										   "is not supported")));

				char	   *baseUrl = StripFromChar(fileScan->path, '?');

				PgLakeFileModifyState *fileModifyState = palloc0(sizeof(PgLakeFileModifyState));

				fileModifyState->pathDatum = CStringGetTextDatum(baseUrl);
				fileModifyState->sourceRowCount = fileScan->rowCount;
				fileModifyState->liveRowCount = fileScan->rowCount - fileScan->deletedRowCount;

				if (fileScan->allRowsMatch && scanState->skipFullMatchFiles)
				{
					/*
					 * All rows will be deleted. We skip writing a deletion
					 * file and set the modifiedRowCount to the number of
					 * remaining rows in the data file.
					 */
					fileModifyState->modifiedRowCount = fileModifyState->liveRowCount;
				}
				else
				{
					/*
					 * We create a DestReceiver for a temporary file upfront.
					 * This does not create the file yet, but we want it to be
					 * in a long-lived memory context.
					 */
					char	   *tempFileName = GenerateTempFileName("lake_table_delete", true);

					fileModifyState->deleteFile = tempFileName;
					fileModifyState->deleteDest = CreateCSVDestReceiver(tempFileName, copyOptions, foreignTableFormat);
				}

				fileModifyStates = lappend(fileModifyStates, fileModifyState);
			}

			/*
			 * fileModifyStates preserves the order of
			 * resultTableScan->fileScans, which is very important because we
			 * use it to interpret the file index part of ctids, which allows
			 * us to know the source file name and the corresponding deletion
			 * file.
			 */
			fmstate->fileModifyStates = fileModifyStates;

			/* number of bits reserved for file row numbers in ctids */
			fmstate->fileRowNumberBits = scanState->fileRowNumberBits;

			/*
			 * If our subplan is not a foreign scan (of the relation being
			 * updated), then it may produce duplicates and we need
			 * deduplication.
			 */
			fmstate->requiresUpdateTracking =
				operation == CMD_UPDATE && !IsA(subPlan, ForeignScan);
		}

		/*
		 * Create an update tracking table. We currently do this even when
		 * update tracking is not required as a convenient (read: lazy) way to
		 * keep global state in a way that also factors in things like rolling
		 * back to savepoint.
		 */
		if (!BeginRelationUpdateTracking(&fmstate->updateTrackingState, relationId))
		{
			/*
			 * Apparently the tracking table already exists because another
			 * update on the same table is active. We get into murky waters.
			 *
			 * We could theoretically skip individual row updates using the
			 * update tracking, but for now we prefer to error out upfront.
			 */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake table %s cannot be "
								   "%s by multiple operations in a single "
								   "command",
								   get_rel_name(relationId),
								   operation == CMD_UPDATE ? "updated" : "deleted")));
		}

		if (operation == CMD_DELETE)
		{
			/*
			 * In AddForeignUpdateTargets we added ctid and the columns from
			 * RETURNING. In ExecForeignDelete we get those in a
			 * TupleTableSlot that matches the target list of the subplan.
			 * We'll need to write those to a TupleTableSlot that matches the
			 * result relation.
			 *
			 * We prepare a list that maps the planSlot indices to result
			 * relation column numbers, and also find the position of the
			 * ctid.
			 *
			 * There may be certain columns we don't care about (e.g. wholerow
			 * in case of joins), for which we set columnNumber to 0.
			 */
			List	   *planSlotAttributeNumbers = NIL;

			ListCell   *entryCell = NULL;

			foreach(entryCell, subPlan->targetlist)
			{
				TargetEntry *targetEntry = lfirst(entryCell);
				int			columnNumber = 0;

				if (targetEntry->resjunk && IsA(targetEntry->expr, Var))
				{
					Var		   *returningVar = (Var *) targetEntry->expr;

					/*
					 * We either extract the RETURNING Var directly from the
					 * ForeignScan, in which case varnosyn would be set to the
					 * result relation by AddForeignUpdateTargets. In case
					 * there are intermediate subplans (e.g. Append node in
					 * case of a partitioned table, or a join), we extract it
					 * from the subplan.
					 *
					 * varattnosyn gives us the original column number of the
					 * table.
					 */
					if (returningVar->varno == OUTER_VAR ||
						returningVar->varnosyn == resultRangeTableIndex)
						columnNumber = returningVar->varattnosyn;
				}

				planSlotAttributeNumbers = lappend_int(planSlotAttributeNumbers,
													   columnNumber);
			}

			fmstate->planSlotAttributeNumbers = planSlotAttributeNumbers;
		}
	}

	return fmstate;
}


/*
 * FindResultRelationScanState finds the scan on the result relation
 * from which we obtain ctids. We need the list of files scanned to be able
 * to interpret the file index part of the ctid.
 */
static PgLakeScanState *
FindResultRelationScanState(PlanState *planState, Oid resultRelationId)
{
	FindResultRelationScanStateContext context = {
		.resultRelationId = resultRelationId,
		.lakeScan = NULL
	};

	FindResultRelationScanStateWalker(planState, &context);

	return context.lakeScan;
}


/*
 * FindResultRelationScanStateWalker finds the scan on the result relation
 * from which we obtain ctids. We need the list of files scanned to be able
 * to interpret the file index part of the ctid.
 */
static bool
FindResultRelationScanStateWalker(PlanState *planState,
								  FindResultRelationScanStateContext * context)
{
	if (planState == NULL)
		return false;

	if (IsA(planState, ForeignScanState))
	{
		ForeignScanState *fsstate = (ForeignScanState *) planState;

		if (fsstate->fdwroutine->BeginForeignModify != postgresBeginForeignModify)
			/* other type of FDW */
			return false;

		PgLakeScanState *lakeScan = (PgLakeScanState *) fsstate->fdw_state;

		if (lakeScan->resultRelationId == InvalidOid)
			/* not a result relation scan */
			return false;

		if (lakeScan->resultRelationId != context->resultRelationId)
			/* result relation scan on another partition */
			return false;

		context->lakeScan = lakeScan;
		return true;
	}

	return planstate_tree_walker(planState, FindResultRelationScanStateWalker, context);
}


/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	/*
	 * In addition force restrictive search_path, in case there are any
	 * regproc or similar constants to be printed.
	 */
	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
					 List *fdw_exprs,
					 int numParams,
					 FmgrInfo **param_flinfo,
					 List **param_exprs,
					 const char ***param_values)
{
	int			i;
	ListCell   *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * numParams);

	i = 0;
	foreach(lc, fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require pg_lake_table to know more than is desirable
	 * about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values in text format.
 */
static void
process_query_params(ExprContext *econtext,
					 FmgrInfo *param_flinfo,
					 List *param_exprs,
					 const char **param_values)
{
	int			nestlevel;
	int			i;
	ListCell   *lc;

	nestlevel = set_transmission_modes();

	i = 0;
	foreach(lc, param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 *
		 * PGDuckSerialize() takes an output function and information about
		 * the type and potentially calls its own output function for arrays
		 * or record types to handle the details of converting to a
		 * PGDuck-compatible format.
		 */
		if (isNull)
			param_values[i] = NULL;
		else

			param_values[i] = PGDuckSerialize(&param_flinfo[i], exprType((Node *) expr_state->expr), expr_value);
		i++;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * Check if reltarget is safe enough to push down semi-join.  Reltarget is not
 * safe, if it contains references to inner rel relids, which do not belong to
 * outer rel.
 */
static bool
semijoin_target_ok(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel, RelOptInfo *innerrel)
{
	List	   *vars;
	ListCell   *lc;
	bool		ok = true;

	Assert(joinrel->reltarget);

	vars = pull_var_clause((Node *) joinrel->reltarget->exprs, PVC_INCLUDE_PLACEHOLDERS);

	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		if (!IsA(var, Var))
			continue;

		if (bms_is_member(var->varno, innerrel->relids))
		{
			/*
			 * The planner can create semi-join, which refers to inner rel
			 * vars in its target list. However, we deparse semi-join as an
			 * exists() subquery, so can't handle references to inner rel in
			 * the target list.
			 */
			ok = false;
			break;
		}
	}
	return ok;
}

/*
 * Assess whether the join between inner and outer relations can be pushed down
 * to the foreign server. As a side effect, save information we obtain in this
 * function to PgLakeRelationInfo passed in.
 */
static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
				RelOptInfo *outerrel, RelOptInfo *innerrel,
				JoinPathExtraData *extra)
{
	PgLakeRelationInfo *fpinfo;
	PgLakeRelationInfo *fpinfo_o;
	PgLakeRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;

	/*
	 * For pg_lake_table, we found that pushing down joins in update/delete
	 * causes excessive complexity due to fundamental differences in planning
	 * between joins and base relations in the FDW APIs, and the added
	 * complexity of obtaining ctid and wholerow values and wiring them
	 * through all the stages of planning, execution, and pushdown.
	 *
	 * Neither the planner, nor DuckDB is very cooperative at any stage. For
	 * instance, deparse.c will generate CASE statements that cannot be
	 * processed by DuckDB due the use of a CASE (r1.*)::text expression and a
	 * NULL::record constant.
	 *
	 * The handling of ctid in a join for update/delete is also very different
	 * because it is no longer a resjunk column with varattno -1, but is wired
	 * through joins and subqueries. That also causes issues in the deparser
	 * in part due to our __lake_read_table trick, because ctid is normally an
	 * implicit join result, but when __lake_read_table also returns ctid it
	 * should be listed as part of join alias column names.
	 *
	 * These are just some examples of the problems you'll encounter if you go
	 * down the path of making joins work. Probably better to look at direct
	 * modify first.
	 *
	 * Returning false here avoids a join path being generated and therefore
	 * avoids all of the downstream problems, at the cost of much slower
	 * UPDATE .. FROM and DELETE .. USING statements.
	 */
	if (root->parse->commandType == CMD_UPDATE ||
		root->parse->commandType == CMD_DELETE)
		return false;

	/*
	 * We support pushing down INNER, LEFT, RIGHT, FULL OUTER and SEMI joins.
	 * Constructing queries representing ANTI joins is hard, hence not
	 * considered right now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT && jointype != JOIN_FULL &&
		jointype != JOIN_SEMI)
		return false;

	/*
	 * We can't push down semi-join if its reltarget is not safe
	 */
	if ((jointype == JOIN_SEMI) && !semijoin_target_ok(root, joinrel, outerrel, innerrel))
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	fpinfo = (PgLakeRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (PgLakeRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (PgLakeRelationInfo *) innerrel->fdw_private;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * Merge FDW options.  We might be tempted to do this after we have deemed
	 * the foreign join to be OK.  But we must do this beforehand so that we
	 * know which quals can be evaluated on the foreign server, which might
	 * depend on shippable_extensions.
	 */
	fpinfo->server = fpinfo_o->server;
	merge_fdw_options(fpinfo, fpinfo_o, fpinfo_i);

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		is_remote_clause = is_foreign_expr(root, joinrel,
													   rinfo->clause);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * deparseExplicitTargetList() isn't smart enough to handle anything other
	 * than a Var.  In particular, if there's some PlaceHolderVar that would
	 * need to be evaluated within this join tree (because there's an upper
	 * reference to a quantity that may go to NULL as a result of an outer
	 * join), then we can't try to push the join down because we'll fail when
	 * we get to deparseExplicitTargetList().  However, a PlaceHolderVar that
	 * needs to be evaluated *at the top* of this join tree is OK, because we
	 * can do that locally after fetching the results from the remote side.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids		relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
			joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/*
	 * By default, both the input relations are not required to be deparsed as
	 * subqueries, but there might be some relations covered by the input
	 * relations that are required to be deparsed as subqueries, so save the
	 * relids of those relations for later use by the deparser.
	 */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	Assert(bms_is_subset(fpinfo_o->lower_subquery_rels, outerrel->relids));
	Assert(bms_is_subset(fpinfo_i->lower_subquery_rels, innerrel->relids));
	fpinfo->lower_subquery_rels = bms_union(fpinfo_o->lower_subquery_rels,
											fpinfo_i->lower_subquery_rels);
	fpinfo->hidden_subquery_rels = bms_union(fpinfo_o->hidden_subquery_rels,
											 fpinfo_i->hidden_subquery_rels);

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from
	 * the outer side are added to remote_conds since those can be evaluated
	 * after the join is evaluated. The clauses from inner side are added to
	 * the joinclauses, since they need to be evaluated while constructing the
	 * join.
	 *
	 * For SEMI-JOIN clauses from inner relation can not be added to
	 * remote_conds, but should be treated as join clauses (as they are
	 * deparsed to EXISTS subquery, where inner relation can be referred). A
	 * list of relation ids, which can't be referred to from higher levels, is
	 * preserved as a hidden_subquery_rels list.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   fpinfo_i->remote_conds);
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   fpinfo_o->remote_conds);
			break;

		case JOIN_LEFT:

			/*
			 * When semi-join is involved in the inner or outer part of the
			 * left join, it's deparsed as a subquery, and we can't refer to
			 * its vars on the upper level.
			 */
			if (bms_is_empty(fpinfo_i->hidden_subquery_rels))
				fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
												  fpinfo_i->remote_conds);
			if (bms_is_empty(fpinfo_o->hidden_subquery_rels))
				fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
												   fpinfo_o->remote_conds);
			break;

		case JOIN_RIGHT:

			/*
			 * When semi-join is involved in the inner or outer part of the
			 * right join, it's deparsed as a subquery, and we can't refer to
			 * its vars on the upper level.
			 */
			if (bms_is_empty(fpinfo_o->hidden_subquery_rels))
				fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
												  fpinfo_o->remote_conds);
			if (bms_is_empty(fpinfo_i->hidden_subquery_rels))
				fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
												   fpinfo_i->remote_conds);
			break;

		case JOIN_SEMI:
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
											  fpinfo_i->remote_conds);
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
											  fpinfo->remote_conds);
			fpinfo->remote_conds = list_copy(fpinfo_o->remote_conds);
			fpinfo->hidden_subquery_rels = bms_union(fpinfo->hidden_subquery_rels,
													 innerrel->relids);
			break;

		case JOIN_FULL:

			/*
			 * In this case, if any of the input relations has conditions, we
			 * need to deparse that relation as a subquery so that the
			 * conditions can be evaluated before the join.  Remember it in
			 * the fpinfo of this relation so that the deparser can take
			 * appropriate action.  Also, save the relids of base relations
			 * covered by that relation for later use by the deparser.
			 */
			if (fpinfo_o->remote_conds)
			{
				fpinfo->make_outerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels,
									outerrel->relids);
			}
			if (fpinfo_i->remote_conds)
			{
				fpinfo->make_innerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels,
									innerrel->relids);
			}
			break;

		default:
			/* Should not happen, we have just checked this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/*
	 * For an inner join, all restrictions can be treated alike. Treating the
	 * pushed down conditions as join conditions allows a top level full outer
	 * join to be deparsed without requiring subqueries.
	 */
	if (jointype == JOIN_INNER)
	{
		Assert(!fpinfo->joinclauses);
		fpinfo->joinclauses = fpinfo->remote_conds;
		fpinfo->remote_conds = NIL;
	}
	else if (jointype == JOIN_LEFT || jointype == JOIN_RIGHT || jointype == JOIN_FULL)
	{
		/*
		 * Conditions, generated from semi-joins, should be evaluated before
		 * LEFT/RIGHT/FULL join.
		 */
		if (!bms_is_empty(fpinfo_o->hidden_subquery_rels))
		{
			fpinfo->make_outerrel_subquery = true;
			fpinfo->lower_subquery_rels = bms_add_members(fpinfo->lower_subquery_rels, outerrel->relids);
		}

		if (!bms_is_empty(fpinfo_i->hidden_subquery_rels))
		{
			fpinfo->make_innerrel_subquery = true;
			fpinfo->lower_subquery_rels = bms_add_members(fpinfo->lower_subquery_rels, innerrel->relids);
		}
	}

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation names mustn't include any digits, or it'll confuse
	 * postgresExplainForeignScan.
	 */
	fpinfo->relation_name = psprintf("(%s) %s JOIN (%s)",
									 fpinfo_o->relation_name,
									 get_jointype_name(fpinfo->jointype),
									 fpinfo_i->relation_name);

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	Assert(fpinfo->relation_index == 0);	/* shouldn't be set yet */
	fpinfo->relation_index =
		list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
								Path *epq_path, List *restrictlist)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/*
	 * Before creating sorted paths, arrange for the passed-in EPQ path, if
	 * any, to return columns needed by the parent ForeignScan node so that
	 * they will propagate up through Sort nodes injected below, if necessary.
	 */
	if (epq_path != NULL && useful_pathkeys_list != NIL)
	{
		PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) rel->fdw_private;
		PathTarget *target = copy_pathtarget(epq_path->pathtarget);

		/* Include columns required for evaluating PHVs in the tlist. */
		add_new_columns_to_pathtarget(target,
									  pull_var_clause((Node *) target->exprs,
													  PVC_RECURSE_PLACEHOLDERS));

		/* Include columns required for evaluating the local conditions. */
		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			add_new_columns_to_pathtarget(target,
										  pull_var_clause((Node *) rinfo->clause,
														  PVC_RECURSE_PLACEHOLDERS));
		}

		/*
		 * If we have added any new columns, adjust the tlist of the EPQ path.
		 *
		 * Note: the plan created using this path will only be used to execute
		 * EPQ checks, where accuracy of the plan cost and width estimates
		 * would not be important, so we do not do set_pathtarget_cost_width()
		 * for the new pathtarget here.  See also postgresGetForeignPlan().
		 */
		if (list_length(target->exprs) > list_length(epq_path->pathtarget->exprs))
		{
			/* The EPQ path is a join path, so it is projection-capable. */
			Assert(is_projection_capable_path(epq_path));

			/*
			 * Use create_projection_path() here, so as to avoid modifying it
			 * in place.
			 */
			epq_path = (Path *) create_projection_path(root,
													   rel,
													   epq_path,
													   target);
		}
	}

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		double		rows;
		int			width;
		int			disabled_nodes;
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		estimate_path_cost_size(root, rel, NIL, useful_pathkeys, NULL,
								&rows, &width, &disabled_nodes,
								&startup_cost, &total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (IS_SIMPLE_REL(rel))
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rows,
#if PG_VERSION_NUM >= 180000
											 disabled_nodes,
#endif
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
#if PG_VERSION_NUM >= 170000
											 NIL,	/* no fdw_restrictinfo
													 * list */
#endif
											 NIL));
		else
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rows,
#if PG_VERSION_NUM >= 180000
											  disabled_nodes,
#endif
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
#if PG_VERSION_NUM >= 170000
											  restrictlist,
#endif
											  NIL));
	}
}

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(PgLakeRelationInfo * fpinfo)
{
	/* we always assume the estimates are fetched from remote server */
	fpinfo->use_remote_estimate = true;

	ListCell   *lc;

	foreach(lc, fpinfo->server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "fdw_startup_cost") == 0)
			(void) parse_real(defGetString(def), &fpinfo->fdw_startup_cost, 0,
							  NULL);
		else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
			(void) parse_real(defGetString(def), &fpinfo->fdw_tuple_cost, 0,
							  NULL);
	}
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(PgLakeRelationInfo * fpinfo)
{
	/* we always assume the estimates are fetched from remote server */
	fpinfo->use_remote_estimate = true;
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(PgLakeRelationInfo * fpinfo,
				  const PgLakeRelationInfo * fpinfo_o,
				  const PgLakeRelationInfo * fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;

	/* Merge the table level options from either side of the join. */
	if (fpinfo_i)
	{
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
			fpinfo_i->use_remote_estimate;
	}
}

/*
 * postgresExecForeignTruncate
 *		Truncate one or more foreign tables
 */
static void
postgresExecForeignTruncate(List *relations,
							DropBehavior behavior,
							bool restartSeqs)
{
	ListCell   *relationCell;

	foreach(relationCell, relations)
	{
		Relation	relation = lfirst(relationCell);
		Oid			relationId = RelationGetRelid(relation);

		ErrorIfReadOnlyIcebergTable(relationId);

		/* extra checks */
		if (PgLakeModifyValidityCheckHook)
			PgLakeModifyValidityCheckHook(relationId);

		RemoveAllDataFilesFromTable(relationId);
	}
}


/*
 * postgresGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
postgresGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	PgLakeRelationInfo *fpinfo;
	ForeignPath *joinpath;
	double		rows;
	int			width;
	int			disabled_nodes;
	Cost		startup_cost;
	Cost		total_cost;
	Path	   *epq_path;		/* Path to create plan to be executed when
								 * EvalPlanQual gets triggered. */

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/*
	 * This code does not work for joins with lateral references, since those
	 * must have parameterized paths, which we don't generate yet.
	 */
	if (!bms_is_empty(joinrel->lateral_relids))
		return;

	/*
	 * Create unfinished PgLakeRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe. Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (PgLakeRelationInfo *) palloc0(sizeof(PgLakeRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;

	/*
	 * If there is a possibility that EvalPlanQual will be executed, we need
	 * to be able to reconstruct the row using scans of the base relations.
	 * GetExistingLocalJoinPath will find a suitable path for this purpose in
	 * the path list of the joinrel, if one exists.  We must be careful to
	 * call it before adding any ForeignPath, since the ForeignPath might
	 * dominate the only suitable local path available.  We also do it before
	 * calling foreign_join_ok(), since that function updates fpinfo and marks
	 * it as pushable if the join is found to be pushable.
	 */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE ||
		root->rowMarks)
	{
		epq_path = GetExistingLocalJoinPath(joinrel);
		if (!epq_path)
		{
			elog(DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
			return;
		}
	}
	else
		epq_path = NULL;

	if (!foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
	{
		/* Free path required for EPQ if we copied one; we don't need it now */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path. The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 * The local conditions are applied after the join has been computed on
	 * the remote side like quals in WHERE clause, so pass jointype as
	 * JOIN_INNER.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);
	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * If we are going to estimate costs locally, estimate the join clause
	 * selectivity here while we have special join info.
	 */
	if (!fpinfo->use_remote_estimate)
		fpinfo->joinclause_sel = clauselist_selectivity(root, fpinfo->joinclauses,
														0, fpinfo->jointype,
														extra->sjinfo);

	/* Estimate costs for bare join relation */
	estimate_path_cost_size(root, joinrel, NIL, NIL, NULL,
							&rows, &width, &disabled_nodes,
							&startup_cost, &total_cost);
	/* Now update this information in the joinrel */
	joinrel->rows = rows;
	joinrel->reltarget->width = width;
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->disabled_nodes = disabled_nodes;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,	/* default pathtarget */
										rows,
#if PG_VERSION_NUM >= 180000
										disabled_nodes,
#endif
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										epq_path,
#if PG_VERSION_NUM >= 170000
										extra->restrictlist,
#endif
										NIL);	/* no fdw_private */

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *) joinpath);

	/* Consider pathkeys for the join relation */
	add_paths_with_pathkeys_for_rel(root, joinrel, epq_path,
									extra->restrictlist);

	/* XXX Consider parameterized paths for the join relation */
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgLakeRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
					Node *havingQual)
{
	Query	   *query = root->parse;
	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) grouped_rel->fdw_private;
	PathTarget *grouping_target = grouped_rel->reltarget;
	PgLakeRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (PgLakeRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/*
		 * Check whether this expression is part of GROUP BY clause.  Note we
		 * check the whole GROUP BY clause not just processed_groupClause,
		 * because we will ship all of it, cf. appendGroupByClause.
		 */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Can we ship it
			 * as-is to the foreign server?
			 */
			if (is_foreign_expr(root, grouped_rel, expr) &&
				!is_foreign_param(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				List	   *aggvars;

				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach(l, aggvars)
				{
					Expr	   *aggref = (Expr *) lfirst(l);

					if (IsA(aggref, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(aggref));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
			if (is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation name mustn't include any digits, or it'll confuse
	 * postgresExplainForeignScan.
	 */
	fpinfo->relation_name = psprintf("Aggregate on (%s)",
									 ofpinfo->relation_name);

	return true;
}

/*
 * postgresGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
postgresGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel,
							 void *extra)
{
	PgLakeRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((PgLakeRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG &&
		 stage != UPPERREL_ORDERED &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (PgLakeRelationInfo *) palloc0(sizeof(PgLakeRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			add_foreign_grouping_paths(root, input_rel, output_rel,
									   (GroupPathExtraData *) extra);
			break;
		case UPPERREL_ORDERED:
			add_foreign_ordered_paths(root, input_rel, output_rel);
			break;
		case UPPERREL_FINAL:
			add_foreign_final_paths(root, input_rel, output_rel,
									(FinalPathExtraData *) extra);
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	PgLakeRelationInfo *ifpinfo = input_rel->fdw_private;
	PgLakeRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	int			width;
	int			disabled_nodes;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  (Currently we create just a single
	 * path here, but in future it would be possible that we build more paths
	 * such as pre-sorted paths as in postgresGetForeignPaths and
	 * postgresGetForeignJoinPaths.)  The best we can do for these conditions
	 * is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/* Estimate the cost of push down */
	estimate_path_cost_size(root, grouped_rel, NIL, NIL, NULL,
							&rows, &width, &disabled_nodes,
							&startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->disabled_nodes = disabled_nodes;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
#if PG_VERSION_NUM >= 180000
										  disabled_nodes,
#endif
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
#if PG_VERSION_NUM >= 170000
										  NIL,	/* no fdw_restrictinfo list */
#endif
										  NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
						  RelOptInfo *ordered_rel)
{
	Query	   *parse = root->parse;
	PgLakeRelationInfo *ifpinfo = input_rel->fdw_private;
	PgLakeRelationInfo *fpinfo = ordered_rel->fdw_private;
	PgLakePathExtraData *fpextra;
	double		rows;
	int			width;
	int			disabled_nodes;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *ordered_path;
	ListCell   *lc;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * If the input_rel is a base or join relation, we would already have
	 * considered pushing down the final sort to the remote server when
	 * creating pre-sorted foreign paths for that relation, because the
	 * query_pathkeys is set to the root->sort_pathkeys in that case (see
	 * standard_qp_callback()).
	 */
	if (input_rel->reloptkind == RELOPT_BASEREL ||
		input_rel->reloptkind == RELOPT_JOINREL)
	{
		Assert(root->query_pathkeys == root->sort_pathkeys);

		/* Safe to push down if the query_pathkeys is safe to push down */
		fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;

		return;
	}

	/* The input_rel should be a grouping relation */
	Assert(input_rel->reloptkind == RELOPT_UPPER_REL &&
		   ifpinfo->stage == UPPERREL_GROUP_AGG);

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying grouping relation to perform the final sort remotely,
	 * which is stored into the fdw_private list of the resulting path.
	 */

	/* Assess if it is safe to push down the final sort */
	foreach(lc, root->sort_pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;

		/*
		 * is_foreign_expr would detect volatile expressions as well, but
		 * checking ec_has_volatile here saves some cycles.
		 */
		if (pathkey_ec->ec_has_volatile)
			return;

		/*
		 * Can't push down the sort if pathkey's opfamily is not shippable.
		 */
		if (!is_shippable(pathkey->pk_opfamily, OperatorFamilyRelationId,
						  NULL))
			return;

		/*
		 * The EC must contain a shippable EM that is computed in input_rel's
		 * reltarget, else we can't push down the sort.
		 */
		if (find_em_for_rel_target(root,
								   pathkey_ec,
								   input_rel) == NULL)
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct PgLakePathExtraData */
	fpextra = (PgLakePathExtraData *) palloc0(sizeof(PgLakePathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_ORDERED];
	fpextra->has_final_sort = true;

	/* Estimate the costs of performing the final sort remotely */
	estimate_path_cost_size(root, input_rel, NIL, root->sort_pathkeys, fpextra,
							&rows, &width, &disabled_nodes,
							&startup_cost, &total_cost);

	/*
	 * Build the fdw_private list that will be used by postgresGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeBoolean(true), makeBoolean(false));

	/* Create foreign ordering path */
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
#if PG_VERSION_NUM >= 180000
											 disabled_nodes,
#endif
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
#if PG_VERSION_NUM >= 170000
											 NIL,	/* no fdw_restrictinfo
													 * list */
#endif
											 fdw_private);

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *) ordered_path);
}

/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *final_rel,
						FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	PgLakeRelationInfo *ifpinfo = (PgLakeRelationInfo *) input_rel->fdw_private;
	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List	   *pathkeys = NIL;
	PgLakePathExtraData *fpextra;
	double		rows;
	int			width;
	int			disabled_nodes = 0;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell   *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
				 IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
#if PG_VERSION_NUM >= 180000
													   disabled_nodes,
#endif
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
#if PG_VERSION_NUM >= 170000
													   NIL, /* no fdw_restrictinfo
															 * list */
#endif
													   NULL);	/* no fdw_private */

				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = (PgLakeRelationInfo *) input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/* DuckDB doesn't have this feature, so cannot pushdown */
	if (parse->limitOption == LIMIT_OPTION_WITH_TIES)
		return;

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) ||
		!is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct PgLakePathExtraData */
	fpextra = (PgLakePathExtraData *) palloc0(sizeof(PgLakePathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_FINAL];
	fpextra->has_final_sort = has_final_sort;
	fpextra->has_limit = extra->limit_needed;
	fpextra->limit_tuples = extra->limit_tuples;
	fpextra->count_est = extra->count_est;
	fpextra->offset_est = extra->offset_est;

	/*
	 * Estimate the costs of performing the final sort and the LIMIT
	 * restriction remotely.  If has_final_sort is false, we wouldn't need to
	 * execute EXPLAIN anymore if use_remote_estimate, since the costs can be
	 * roughly estimated using the costs we already have for the underlying
	 * relation, in the same way as when use_remote_estimate is false.  Since
	 * it's pretty expensive to execute EXPLAIN, force use_remote_estimate to
	 * false in that case.
	 */
	estimate_path_cost_size(root, input_rel, NIL, pathkeys, fpextra,
							&rows, &width, &disabled_nodes,
							&startup_cost, &total_cost);

	/*
	 * Build the fdw_private list that will be used by postgresGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeBoolean(has_final_sort),
							 makeBoolean(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
#if PG_VERSION_NUM >= 180000
										   disabled_nodes,
#endif
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
#if PG_VERSION_NUM >= 170000
										   NIL, /* no fdw_restrictinfo list */
#endif
										   fdw_private);

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}


/*
 * Create a tuple from the specified row of the PGresult.
 *
 * rel is the local representation of the foreign table, attinmeta is
 * conversion data for the rel's tupdesc, and retrieved_attrs is an
 * integer list of the table column numbers present in the PGresult.
 * fsstate is the ForeignScan plan node's execution state.
 * temp_context is a working context that can be reset after each tuple.
 *
 * Note: either rel or fsstate, but not both, can be NULL.  rel is NULL
 * if we're processing a remote join, while fsstate is NULL in a non-query
 * context such as ANALYZE, or if we're processing a non-scan query node.
 */
static HeapTuple
make_tuple_from_result_row(PGresult *res,
						   int row,
						   Relation rel,
						   AttInMetadata *attinmeta,
						   List *retrieved_attrs,
						   ForeignScanState *fsstate,
						   MemoryContext temp_context)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ItemPointer ctid = NULL;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell   *lc;
	int			j;

	Assert(row < PQntuples(res));

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else
	{
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
	errpos.cur_attno = 0;
	errpos.rel = rel;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *) &errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * i indexes columns in the relation, j indexes columns in the PGresult.
	 */
	j = 0;
	foreach(lc, retrieved_attrs)
	{
		int			i = lfirst_int(lc);
		char	   *valstr;

		/* fetch next column's textual value */
		if (PQgetisnull(res, row, j))
			valstr = NULL;
		else
			valstr = PQgetvalue(res, row, j);

		/*
		 * convert value to internal representation
		 *
		 * Note: we ignore system columns other than ctid and oid in result
		 */
		errpos.cur_attno = i;
		if (i > 0)
		{
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			nulls[i - 1] = (valstr == NULL);
			/* Apply the input function even to nulls, to support domains */
			values[i - 1] = InputFunctionCall(&attinmeta->attinfuncs[i - 1],
											  valstr,
											  attinmeta->attioparams[i - 1],
											  attinmeta->atttypmods[i - 1]);
		}
		else if (i == SelfItemPointerAttributeNumber)
		{
			/* ctid is a (filename,file_row_number) record */

			if (valstr == NULL)
				elog(ERROR, "unexpected NULL ctid");

			PgLakeScanState *scanState = fsstate->fdw_state;

			/*
			 * The TupleTableSlot during an update/delete will be based on
			 * columns from the table. That leaves little room to store our
			 * record. We therefore convert it to ItemPointer, which contains
			 * an index of the file in fileModifyStates and the row number.
			 *
			 * We store the ItemPointer in the t_self field below, which we'll
			 * be able to read in ExecForeignUpdate/Delete (similar to what
			 * postgres_fdw does).
			 */
			ctid = RowIdRecordStringToItemPointer(valstr, scanState);
		}

		errpos.cur_attno = 0;

		j++;
	}

	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

	/*
	 * Check we got the expected number of columns.  Note: j == 0 and
	 * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
	 */
	if (j > 0 && j != PQnfields(res))
		elog(ERROR, "remote query result does not match the foreign table");

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 *
 * Note that this function mustn't do any catalog lookups, since we are in
 * an already-failed transaction.  Fortunately, we can get the needed info
 * from the relation or the query's rangetable instead.
 */
static void
conversion_error_callback(void *arg)
{
	ConversionLocation *errpos = (ConversionLocation *) arg;
	Relation	rel = errpos->rel;
	ForeignScanState *fsstate = errpos->fsstate;
	const char *attname = NULL;
	const char *relname = NULL;
	bool		is_wholerow = false;

	/*
	 * If we're in a scan node, always use aliases from the rangetable, for
	 * consistency between the simple-relation and remote-join cases.  Look at
	 * the relation's tupdesc only if we're not in a scan node.
	 */
	if (fsstate)
	{
		/* ForeignScan case */
		ForeignScan *fsplan = castNode(ForeignScan, fsstate->ss.ps.plan);
		int			varno = 0;
		AttrNumber	colno = 0;

		if (fsplan->scan.scanrelid > 0)
		{
			/* error occurred in a scan against a foreign table */
			varno = fsplan->scan.scanrelid;
			colno = errpos->cur_attno;
		}
		else
		{
			/* error occurred in a scan against a foreign join */
			TargetEntry *tle;

			tle = list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
								errpos->cur_attno - 1);

			/*
			 * Target list can have Vars and expressions.  For Vars, we can
			 * get some information, however for expressions we can't.  Thus
			 * for expressions, just show generic context message.
			 */
			if (IsA(tle->expr, Var))
			{
				Var		   *var = (Var *) tle->expr;

				varno = var->varno;
				colno = var->varattno;
			}
		}

		if (varno > 0)
		{
			EState	   *estate = fsstate->ss.ps.state;
			RangeTblEntry *rte = exec_rt_fetch(varno, estate);

			relname = rte->eref->aliasname;

			if (colno == 0)
				is_wholerow = true;
			else if (colno > 0 && colno <= list_length(rte->eref->colnames))
				attname = strVal(list_nth(rte->eref->colnames, colno - 1));
			else if (colno == SelfItemPointerAttributeNumber)
				attname = "ctid";
		}
	}
	else if (rel)
	{
		/* Non-ForeignScan case (we should always have a rel here) */
		TupleDesc	tupdesc = RelationGetDescr(rel);

		relname = RelationGetRelationName(rel);
		if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc,
												   errpos->cur_attno - 1);

			attname = NameStr(attr->attname);
		}
		else if (errpos->cur_attno == SelfItemPointerAttributeNumber)
			attname = "ctid";
	}

	if (relname && is_wholerow)
		errcontext("whole-row reference to foreign table \"%s\"", relname);
	else if (relname && attname)
		errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
	else
		errcontext("processing expression at position %d in select list",
				   errpos->cur_attno);
}

/*
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc;

	PgLakeRelationInfo *fpinfo = (PgLakeRelationInfo *) rel->fdw_private;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids) &&
			bms_is_empty(bms_intersect(em->em_relids, fpinfo->hidden_subquery_rels)) &&
			is_foreign_expr(root, rel, em->em_expr))
			return em;
	}

	return NULL;
}

/*
 * Find an EquivalenceClass member that is to be computed as a sort column
 * in the given rel's reltarget, and is shippable.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
find_em_for_rel_target(PlannerInfo *root, EquivalenceClass *ec,
					   RelOptInfo *rel)
{
	PathTarget *target = rel->reltarget;
	ListCell   *lc1;
	int			i;

	i = 0;
	foreach(lc1, target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc1);
		Index		sgref = get_pathtarget_sortgroupref(target, i);
		ListCell   *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
										  root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr	   *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (!equal(em_expr, expr))
				continue;

			/* Check that expression (including relabels!) is shippable */
			if (is_foreign_expr(root, rel, em->em_expr))
				return em;
		}

		i++;
	}

	return NULL;
}


/*
 * IsSimpleDelete determines whether the given ModifyTableState corresponds to
 * a simple DELETE FROM writable_table WHERE .. command, which can use full scan
 * pruning.
 */
static bool
IsSimpleDelete(ModifyTableState *mtstate, Relation resultRel)
{
	if (mtstate->operation != CMD_DELETE)
		return false;

	if (resultRel->rd_rel->relhastriggers)
		/* triggers require a scan */
		return false;

	if (!IsA(mtstate->ps.lefttree, ForeignScanState))
		/* DELETE with USING may not be prunable */
		return false;

	Plan	   *subPlan = outerPlanState(mtstate)->plan;

	/*
	 * Target list length of 1 implies to RETURNING, since there is always a
	 * ctid column (and RETURNING ctid is not supported).
	 */
	if (list_length(subPlan->targetlist) > 1)
		return false;

	ForeignScanState *fsstate = (ForeignScanState *) mtstate->ps.lefttree;
	PgLakeScanState *scanState = (PgLakeScanState *) fsstate->fdw_state;

	if (scanState == NULL)
		return false;

	return true;
}


/*
 * ShutdownPgLakeScan is called at the end of ExecutorRun, which
 * allows it to adjust es_processed. We need that to deal with files
 * that we skip during DELETE because they fully match the WHERE.
 *
 * This is a bit of a hack, but since we might never get called during
 * ExecutorRun the only alternative would be to set up ExecutorStart
 * and ExecutorRun hooks, so this seems a bit cleaner.
 */
static void
ShutdownPgLakeScan(ForeignScanState *node)
{
	PgLakeScanState *fsstate = (PgLakeScanState *) node->fdw_state;

	if (!fsstate->skipFullMatchFiles)
		return;

	EState	   *estate = node->ss.ps.state;

	estate->es_processed += fsstate->skippableRows;
}

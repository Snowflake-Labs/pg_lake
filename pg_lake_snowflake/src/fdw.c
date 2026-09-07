/*
 * Copyright 2026 Snowflake Inc.
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
 * fdw.c
 *
 * The foreign data wrapper callbacks: planning a scan of a Snowflake table,
 * running it, and the two kinds of work that are worth moving into the
 * warehouse rather than doing here, which are grouping with aggregation and
 * LIMIT.
 *
 * A scan is one statement and its result set is read once, so the startup cost
 * of a path is deliberately high: the planner should prefer a plan that sends
 * one statement with as much work in it as possible over a plan that scans the
 * table repeatedly.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif

#include "pg_lake_snowflake/deparse.h"
#include "pg_lake_snowflake/import_schema.h"
#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"
#include "pg_lake_snowflake/sql_api.h"
#include "pg_lake_snowflake/type_map.h"

/* the elements of the fdw_private list of a ForeignScan plan */
enum SnowflakeScanPrivateIndex
{
	SNOWFLAKE_SCAN_PRIVATE_FRAGMENTS,
	SNOWFLAKE_SCAN_PRIVATE_RETRIEVED_ATTRS,
	SNOWFLAKE_SCAN_PRIVATE_STATEMENT_TEXT
};

/* SnowflakeScanState is the executor state of one Snowflake scan */
typedef struct SnowflakeScanState
{
	List	   *sqlFragments;
	List	   *retrievedAttrs;
	char	   *statementText;

	List	   *paramExprStates;
	List	   *paramTypes;

	SnowflakeConnection *connection;
	SnowflakeStatement *statement;

	SnowflakeTypeConversion *conversions;
	int			columnCount;

	/* holds the values of the row the scan is currently returning */
	MemoryContext rowContext;
}			SnowflakeScanState;

PG_FUNCTION_INFO_V1(pg_lake_snowflake_handler);

static void SnowflakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
									   Oid foreignTableId);
static void SnowflakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
									 Oid foreignTableId);
static ForeignScan *SnowflakeGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignRel,
											Oid foreignTableId, ForeignPath *bestPath,
											List *tlist, List *scanClauses,
											Plan *outerPlan);
static void SnowflakeBeginForeignScan(ForeignScanState *scanState, int executorFlags);
static TupleTableSlot *SnowflakeIterateForeignScan(ForeignScanState *scanState);
static void SnowflakeReScanForeignScan(ForeignScanState *scanState);
static void SnowflakeEndForeignScan(ForeignScanState *scanState);
static void SnowflakeExplainForeignScan(ForeignScanState *scanState, ExplainState *es);
static bool SnowflakeAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
										 BlockNumber *totalPages);
static void SnowflakeGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
										  RelOptInfo *inputRel, RelOptInfo *outputRel,
										  void *extra);

static void AddForeignGroupingPaths(PlannerInfo *root, RelOptInfo *inputRel,
									RelOptInfo *groupedRel, GroupPathExtraData *extra);
static void AddForeignFinalPaths(PlannerInfo *root, RelOptInfo *inputRel,
								 RelOptInfo *finalRel, FinalPathExtraData *extra);
static void MarkRelationAsForeign(RelOptInfo *rel, RelOptInfo *inputRel);
static bool GroupingIsShippable(PlannerInfo *root, RelOptInfo *groupedRel,
								SnowflakeRelationInfo * relationInfo,
								GroupPathExtraData *extra);
static SnowflakeRelationInfo * CopyRelationInfo(SnowflakeRelationInfo * relationInfo);
static void CostSnowflakeScan(SnowflakeRelationInfo * relationInfo);
static Oid	RelationUserId(RelOptInfo *baserel);
static void ExecuteScanStatement(ForeignScanState *scanState);
static int	AcquireSampleRows(Relation relation, int elevel, HeapTuple *rows,
							  int targetRowCount, double *totalRows,
							  double *totalDeadRows);
static ForeignPath *CreateScanPath(PlannerInfo *root, RelOptInfo *baserel,
								   SnowflakeRelationInfo * relationInfo);
static ForeignPath *CreateUpperPath(PlannerInfo *root, RelOptInfo *rel,
									PathTarget *target,
									SnowflakeRelationInfo * relationInfo);


/*
 * pg_lake_snowflake_handler returns the callbacks of the Snowflake foreign data
 * wrapper.
 */
Datum
pg_lake_snowflake_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = SnowflakeGetForeignRelSize;
	routine->GetForeignPaths = SnowflakeGetForeignPaths;
	routine->GetForeignPlan = SnowflakeGetForeignPlan;
	routine->BeginForeignScan = SnowflakeBeginForeignScan;
	routine->IterateForeignScan = SnowflakeIterateForeignScan;
	routine->ReScanForeignScan = SnowflakeReScanForeignScan;
	routine->EndForeignScan = SnowflakeEndForeignScan;
	routine->ExplainForeignScan = SnowflakeExplainForeignScan;
	routine->AnalyzeForeignTable = SnowflakeAnalyzeForeignTable;
	routine->GetForeignUpperPaths = SnowflakeGetForeignUpperPaths;
	routine->ImportForeignSchema = SnowflakeImportForeignSchema;

	PG_RETURN_POINTER(routine);
}


/*
 * SnowflakeGetForeignRelSize decides which conditions Snowflake will evaluate
 * and how many rows the scan is expected to return.
 */
static void
SnowflakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	SnowflakeRelationInfo *relationInfo = palloc0(sizeof(SnowflakeRelationInfo));
	ListCell   *restrictionCell = NULL;

	baserel->fdw_private = relationInfo;

	relationInfo->pushdownSafe = true;
	relationInfo->scanRel = baserel;
	relationInfo->relationId = foreignTableId;
	relationInfo->connection = GetSnowflakeConnection(baserel->serverid,
													  RelationUserId(baserel));
	relationInfo->table = GetSnowflakeTable(foreignTableId, relationInfo->connection);

	foreach(restrictionCell, baserel->baserestrictinfo)
	{
		RestrictInfo *restriction = (RestrictInfo *) lfirst(restrictionCell);

		if (SnowflakeIsShippableExpression(restriction->clause, relationInfo))
			relationInfo->remoteConds = lappend(relationInfo->remoteConds, restriction);
		else
			relationInfo->localConds = lappend(relationInfo->localConds, restriction);
	}

	/* the columns the query reads, plus those a local condition needs */
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &relationInfo->attrsUsed);

	foreach(restrictionCell, relationInfo->localConds)
	{
		RestrictInfo *restriction = (RestrictInfo *) lfirst(restrictionCell);

		pull_varattnos((Node *) restriction->clause, baserel->relid,
					   &relationInfo->attrsUsed);
	}

	/*
	 * A remote table has no size in the catalog until it is analyzed, so the
	 * row_estimate option wins, then whatever ANALYZE left behind, then the
	 * setting.
	 */
	if (relationInfo->table->rowEstimate >= 0)
		baserel->tuples = relationInfo->table->rowEstimate;
	else if (baserel->tuples <= 0)
		baserel->tuples = SnowflakeDefaultRowEstimate;

	set_baserel_size_estimates(root, baserel);

	/*
	 * The rows the statement returns are the ones that survive the conditions
	 * Snowflake evaluates, which is what the scan is costed on.
	 */
	if (relationInfo->localConds == NIL)
		relationInfo->rows = baserel->rows;
	else
	{
		List	   *remoteClauses = extract_actual_clauses(relationInfo->remoteConds,
														   false);
		Selectivity remoteSelectivity =
			clauselist_selectivity(root, remoteClauses, baserel->relid, JOIN_INNER,
								   NULL);

		relationInfo->rows = clamp_row_est(baserel->tuples * remoteSelectivity);
	}

	relationInfo->width = baserel->reltarget->width;

	CostSnowflakeScan(relationInfo);
}


/*
 * RelationUserId returns the user whose credentials a scan of a relation uses,
 * which is the view owner when the query reads through a view. The planner
 * leaves the field invalid when that is the current user.
 */
static Oid
RelationUserId(RelOptInfo *baserel)
{
	return OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
}


/*
 * CostSnowflakeScan fills in the cost of running a statement and reading its
 * result.
 */
static void
CostSnowflakeScan(SnowflakeRelationInfo * relationInfo)
{
	/*
	 * The number of rows the statement produces is part of the startup cost,
	 * not only of the total: Snowflake finishes a statement before any of its
	 * result is available, so a Limit on top of the scan saves the local work
	 * of forming tuples but none of the remote work of producing them.
	 * Costing it this way is what makes a pushed-down LIMIT or aggregate look
	 * cheaper than the same query with the rows thrown away here.
	 */
	relationInfo->startupCost = SNOWFLAKE_STARTUP_COST +
		relationInfo->rows * SNOWFLAKE_PER_TUPLE_COST;
	relationInfo->totalCost = relationInfo->startupCost +
		relationInfo->rows * cpu_tuple_cost;
}


/*
 * SnowflakeGetForeignPaths adds the one way there is to scan a Snowflake table.
 */
static void
SnowflakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	SnowflakeRelationInfo *relationInfo =
		(SnowflakeRelationInfo *) baserel->fdw_private;

	add_path(baserel, (Path *) CreateScanPath(root, baserel, relationInfo));
}


/*
 * CreateScanPath creates the path for a base relation scan.
 */
static ForeignPath *
CreateScanPath(PlannerInfo *root, RelOptInfo *baserel,
			   SnowflakeRelationInfo * relationInfo)
{
#if PG_VERSION_NUM >= 180000
	return create_foreignscan_path(root, baserel, NULL, relationInfo->rows, 0,
								   relationInfo->startupCost, relationInfo->totalCost,
								   NIL, NULL, NULL, NIL, NIL);
#elif PG_VERSION_NUM >= 170000
	return create_foreignscan_path(root, baserel, NULL, relationInfo->rows,
								   relationInfo->startupCost, relationInfo->totalCost,
								   NIL, NULL, NULL, NIL, NIL);
#else
	return create_foreignscan_path(root, baserel, NULL, relationInfo->rows,
								   relationInfo->startupCost, relationInfo->totalCost,
								   NIL, NULL, NULL, NIL);
#endif
}


/*
 * CreateUpperPath creates the path for a relation whose work happens remotely on
 * top of a scan, which is a grouped relation or one that limits.
 */
static ForeignPath *
CreateUpperPath(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
				SnowflakeRelationInfo * relationInfo)
{
#if PG_VERSION_NUM >= 180000
	return create_foreign_upper_path(root, rel, target, relationInfo->rows, 0,
									 relationInfo->startupCost, relationInfo->totalCost,
									 NIL, NULL, NIL, NIL);
#elif PG_VERSION_NUM >= 170000
	return create_foreign_upper_path(root, rel, target, relationInfo->rows,
									 relationInfo->startupCost, relationInfo->totalCost,
									 NIL, NULL, NIL, NIL);
#else
	return create_foreign_upper_path(root, rel, target, relationInfo->rows,
									 relationInfo->startupCost, relationInfo->totalCost,
									 NIL, NULL, NIL);
#endif
}


/*
 * SnowflakeGetForeignPlan builds the ForeignScan plan node, which carries the
 * statement to run and which result column belongs to which attribute.
 */
static ForeignScan *
SnowflakeGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignRel, Oid foreignTableId,
						ForeignPath *bestPath, List *tlist, List *scanClauses,
						Plan *outerPlan)
{
	SnowflakeRelationInfo *relationInfo =
		(SnowflakeRelationInfo *) foreignRel->fdw_private;
	List	   *localExprs = NIL;
	List	   *fdwScanTlist = NIL;
	Index		scanRelId = 0;

	if (IS_SIMPLE_REL(foreignRel))
	{
		ListCell   *clauseCell = NULL;

		scanRelId = foreignRel->relid;

		/*
		 * Conditions that Snowflake evaluates are left out of the plan, and
		 * everything else stays as a filter on the scan.
		 */
		foreach(clauseCell, scanClauses)
		{
			RestrictInfo *restriction = (RestrictInfo *) lfirst(clauseCell);

			if (restriction->pseudoconstant)
				continue;

			if (list_member_ptr(relationInfo->remoteConds, restriction))
				continue;

			localExprs = lappend(localExprs, restriction->clause);
		}
	}
	else
	{
		/*
		 * An upper relation has no Postgres relation to scan, so the result
		 * columns are described by a target list, and the conditions that
		 * could not be pushed into HAVING become a filter.
		 */
		fdwScanTlist = relationInfo->scanTlist;
		localExprs = relationInfo->havingLocalQuals;
	}

	SnowflakeDeparsedQuery query = {0};

	SnowflakeDeparseSelect(root, relationInfo, &query);

	List	   *fdwPrivate = list_make3(query.sqlFragments,
										query.retrievedAttrs,
										makeString(SnowflakeDeparsedQueryText(query.sqlFragments)));

	return make_foreignscan(tlist, localExprs, scanRelId, query.paramExprs, fdwPrivate,
							fdwScanTlist, NIL, outerPlan);
}


/*
 * SnowflakeBeginForeignScan prepares the executor state of a scan. The statement
 * itself is not submitted until the first row is asked for, so that a plan which
 * never runs never pays for a warehouse.
 */
static void
SnowflakeBeginForeignScan(ForeignScanState *scanState, int executorFlags)
{
	ForeignScan *foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	EState	   *executorState = scanState->ss.ps.state;
	SnowflakeScanState *state = palloc0(sizeof(SnowflakeScanState));

	scanState->fdw_state = state;

	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	state->sqlFragments = (List *) list_nth(foreignScan->fdw_private,
											SNOWFLAKE_SCAN_PRIVATE_FRAGMENTS);
	state->retrievedAttrs = (List *) list_nth(foreignScan->fdw_private,
											  SNOWFLAKE_SCAN_PRIVATE_RETRIEVED_ATTRS);
	state->statementText = strVal(list_nth(foreignScan->fdw_private,
										   SNOWFLAKE_SCAN_PRIVATE_STATEMENT_TEXT));

	/*
	 * The plan node records the server and the user to scan as, which holds
	 * for a grouped or limited relation that has no Postgres relation of its
	 * own.
	 */
	Oid			userId = OidIsValid(foreignScan->checkAsUser) ?
		foreignScan->checkAsUser : GetUserId();

	state->connection = GetSnowflakeConnection(foreignScan->fs_server, userId);

	ListCell   *paramCell = NULL;

	foreach(paramCell, foreignScan->fdw_exprs)
		state->paramTypes = lappend_oid(state->paramTypes,
										exprType((Node *) lfirst(paramCell)));

	state->paramExprStates = ExecInitExprList(foreignScan->fdw_exprs,
											  (PlanState *) scanState);

	state->rowContext = AllocSetContextCreate(executorState->es_query_cxt,
											  "pg_lake_snowflake row",
											  ALLOCSET_SMALL_SIZES);
}


/*
 * ExecuteScanStatement submits the statement of a scan, with the current values
 * of its parameters, and prepares the conversion of its result columns.
 */
static void
ExecuteScanStatement(ForeignScanState *scanState)
{
	SnowflakeScanState *state = (SnowflakeScanState *) scanState->fdw_state;
	ExprContext *expressionContext = scanState->ss.ps.ps_ExprContext;
	TupleDesc	tupleDescriptor = scanState->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	List	   *paramLiterals = NIL;
	ListCell   *paramStateCell = NULL;
	ListCell   *paramTypeCell = NULL;

	forboth(paramStateCell, state->paramExprStates, paramTypeCell, state->paramTypes)
	{
		ExprState  *paramState = (ExprState *) lfirst(paramStateCell);
		Oid			paramType = lfirst_oid(paramTypeCell);
		bool		isNull = false;
		Datum		paramValue = ExecEvalExpr(paramState, expressionContext, &isNull);

		paramLiterals = lappend(paramLiterals,
								isNull ? pstrdup("NULL") :
								SnowflakeFormatLiteral(paramValue, paramType));
	}

	char	   *sql = SnowflakeBuildStatement(state->sqlFragments, paramLiterals);

	state->statement = SnowflakeExecute(state->connection, sql);

	int			retrievedColumnCount = list_length(state->retrievedAttrs);

	if (state->statement->columnCount < retrievedColumnCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION),
				 errmsg("Snowflake returned %d columns where %d were expected",
						state->statement->columnCount, retrievedColumnCount)));
	}

	/*
	 * The type each result column is converted to is the one the attached
	 * column is declared as, which is not necessarily the one the Snowflake
	 * type maps to.
	 *
	 * This runs inside a scan callback, whose memory context the executor
	 * resets between rows, so the conversions live in the context of the
	 * query instead.
	 */
	MemoryContext previousContext =
		MemoryContextSwitchTo(scanState->ss.ps.state->es_query_cxt);

	state->columnCount = retrievedColumnCount;
	state->conversions = palloc0(sizeof(SnowflakeTypeConversion) *
								 Max(retrievedColumnCount, 1));

	MemoryContextSwitchTo(previousContext);

	ListCell   *attributeCell = NULL;
	int			columnIndex = 0;

	foreach(attributeCell, state->retrievedAttrs)
	{
		AttrNumber	attributeNumber = (AttrNumber) lfirst_int(attributeCell);
		Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
													attributeNumber - 1);

		SnowflakeInitTypeConversion(&state->conversions[columnIndex],
									attribute->atttypid, attribute->atttypmod);
		columnIndex++;
	}
}


/*
 * SnowflakeIterateForeignScan returns the next row of the result.
 */
static TupleTableSlot *
SnowflakeIterateForeignScan(ForeignScanState *scanState)
{
	SnowflakeScanState *state = (SnowflakeScanState *) scanState->fdw_state;
	TupleTableSlot *slot = scanState->ss.ss_ScanTupleSlot;

	if (state->statement == NULL)
		ExecuteScanStatement(scanState);

	ExecClearTuple(slot);

	if (state->columnCount == 0 && state->statement->columnCount == 0)
		return slot;

	if (!SnowflakeStatementNextRow(state->statement))
		return slot;

	/*
	 * The values of a row live until the next row is asked for, which is as
	 * long as the executor may look at them.
	 */
	MemoryContextReset(state->rowContext);

	MemoryContext previousContext = MemoryContextSwitchTo(state->rowContext);

	memset(slot->tts_values, 0,
		   slot->tts_tupleDescriptor->natts * sizeof(Datum));
	memset(slot->tts_isnull, true,
		   slot->tts_tupleDescriptor->natts * sizeof(bool));

	ListCell   *attributeCell = NULL;
	int			columnIndex = 0;

	foreach(attributeCell, state->retrievedAttrs)
	{
		AttrNumber	attributeNumber = (AttrNumber) lfirst_int(attributeCell);
		char	   *value = SnowflakeStatementGetValue(state->statement, columnIndex);

		if (value != NULL)
		{
			slot->tts_values[attributeNumber - 1] =
				SnowflakeValueToDatum(value, &state->statement->columns[columnIndex],
									  &state->conversions[columnIndex]);
			slot->tts_isnull[attributeNumber - 1] = false;
		}

		columnIndex++;
	}

	MemoryContextSwitchTo(previousContext);

	ExecStoreVirtualTuple(slot);

	return slot;
}


/*
 * SnowflakeReScanForeignScan runs the statement again, which re-evaluates its
 * parameters.
 */
static void
SnowflakeReScanForeignScan(ForeignScanState *scanState)
{
	SnowflakeScanState *state = (SnowflakeScanState *) scanState->fdw_state;

	if (state->statement != NULL)
	{
		SnowflakeStatementClose(state->statement);
		state->statement = NULL;
	}
}


/*
 * SnowflakeEndForeignScan releases the statement of a scan.
 */
static void
SnowflakeEndForeignScan(ForeignScanState *scanState)
{
	SnowflakeScanState *state = (SnowflakeScanState *) scanState->fdw_state;

	if (state == NULL)
		return;

	if (state->statement != NULL)
	{
		SnowflakeStatementClose(state->statement);
		state->statement = NULL;
	}
}


/*
 * SnowflakeExplainForeignScan shows the statement the scan sends, which is the
 * only way to see what was pushed down.
 */
static void
SnowflakeExplainForeignScan(ForeignScanState *scanState, ExplainState *es)
{
	ForeignScan *foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	char	   *statementText = strVal(list_nth(foreignScan->fdw_private,
												SNOWFLAKE_SCAN_PRIVATE_STATEMENT_TEXT));

	ExplainPropertyText("Snowflake SQL", statementText, es);
}


/*
 * SnowflakeAnalyzeForeignTable makes ANALYZE work on a Snowflake table, so that
 * the planner has a row count and column statistics rather than a guess.
 */
static bool
SnowflakeAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
							 BlockNumber *totalPages)
{
	*func = AcquireSampleRows;

	/*
	 * There are no pages to report. ANALYZE only uses this to decide that
	 * there is something to do, and the row count comes from the sampling
	 * function.
	 */
	*totalPages = 1;

	return true;
}


/*
 * AcquireSampleRows reads a sample of a Snowflake table for ANALYZE, using
 * Snowflake's own row sampling rather than reading the whole table.
 */
static int
AcquireSampleRows(Relation relation, int elevel, HeapTuple *rows, int targetRowCount,
				  double *totalRows, double *totalDeadRows)
{
	Oid			relationId = RelationGetRelid(relation);
	SnowflakeConnection *connection = GetSnowflakeConnectionForRelation(relationId);
	SnowflakeTable *table = GetSnowflakeTable(relationId, connection);
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	StringInfo	columnList = makeStringInfo();
	int			liveColumnCount = 0;
	AttrNumber *sampledAttributes = palloc0(sizeof(AttrNumber) *
											Max(tupleDescriptor->natts, 1));

	for (AttrNumber attributeNumber = 1;
		 attributeNumber <= tupleDescriptor->natts;
		 attributeNumber++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
													attributeNumber - 1);

		if (attribute->attisdropped)
			continue;

		if (liveColumnCount > 0)
			appendStringInfoString(columnList, ", ");

		appendStringInfoString(columnList,
							   SnowflakeColumnName(relationId, attributeNumber));
		sampledAttributes[liveColumnCount] = attributeNumber;
		liveColumnCount++;
	}

	*totalDeadRows = 0;
	*totalRows = 0;

	char	   *countStatement = psprintf("SELECT COUNT(*) FROM %s",
										  SnowflakeQualifiedTableName(table));
	char	   *rowCount = SnowflakeExecuteScalar(connection, countStatement);

	if (rowCount != NULL)
		*totalRows = strtod(rowCount, NULL);

	if (liveColumnCount == 0 || targetRowCount <= 0 || *totalRows == 0)
		return 0;

	/*
	 * Row sampling asks Snowflake for a number of rows rather than a
	 * fraction, which is what ANALYZE wants and what keeps the statement
	 * cheap on a large table.
	 */
	char	   *sampleStatement = psprintf("SELECT %s FROM %s SAMPLE ROW (%d ROWS)",
										   columnList->data,
										   SnowflakeQualifiedTableName(table),
										   targetRowCount);
	SnowflakeStatement *statement = SnowflakeExecute(connection, sampleStatement);
	int			sampledRowCount = 0;

	PG_TRY();
	{
		SnowflakeTypeConversion *conversions =
			palloc0(sizeof(SnowflakeTypeConversion) * liveColumnCount);
		Datum	   *values = palloc0(sizeof(Datum) * tupleDescriptor->natts);
		bool	   *nulls = palloc0(sizeof(bool) * tupleDescriptor->natts);

		for (int columnIndex = 0; columnIndex < liveColumnCount; columnIndex++)
		{
			Form_pg_attribute attribute =
				TupleDescAttr(tupleDescriptor, sampledAttributes[columnIndex] - 1);

			SnowflakeInitTypeConversion(&conversions[columnIndex], attribute->atttypid,
										attribute->atttypmod);
		}

		while (sampledRowCount < targetRowCount &&
			   SnowflakeStatementNextRow(statement))
		{
			CHECK_FOR_INTERRUPTS();

			memset(values, 0, sizeof(Datum) * tupleDescriptor->natts);
			memset(nulls, true, sizeof(bool) * tupleDescriptor->natts);

			for (int columnIndex = 0; columnIndex < liveColumnCount; columnIndex++)
			{
				char	   *value = SnowflakeStatementGetValue(statement, columnIndex);
				AttrNumber	attributeNumber = sampledAttributes[columnIndex];

				if (value == NULL)
					continue;

				values[attributeNumber - 1] =
					SnowflakeValueToDatum(value, &statement->columns[columnIndex],
										  &conversions[columnIndex]);
				nulls[attributeNumber - 1] = false;
			}

			rows[sampledRowCount++] = heap_form_tuple(tupleDescriptor, values, nulls);
		}
	}
	PG_FINALLY();
	{
		SnowflakeStatementClose(statement);
	}
	PG_END_TRY();

	ereport(elevel,
			(errmsg("\"%s\": scanned %d of " INT64_FORMAT " rows",
					RelationGetRelationName(relation), sampledRowCount,
					(int64) *totalRows)));

	return sampledRowCount;
}


/*
 * SnowflakeGetForeignUpperPaths considers doing the work above a scan remotely.
 */
static void
SnowflakeGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							  RelOptInfo *inputRel, RelOptInfo *outputRel, void *extra)
{
	SnowflakeRelationInfo *inputRelationInfo =
		(SnowflakeRelationInfo *) inputRel->fdw_private;

	/* nothing to add on top of a relation we are not scanning remotely */
	if (inputRelationInfo == NULL || !inputRelationInfo->pushdownSafe)
		return;

	/* only the first path of a stage is ours to add */
	if (outputRel->fdw_private != NULL)
		return;

	if (stage == UPPERREL_GROUP_AGG)
		AddForeignGroupingPaths(root, inputRel, outputRel, (GroupPathExtraData *) extra);
	else if (stage == UPPERREL_FINAL)
		AddForeignFinalPaths(root, inputRel, outputRel, (FinalPathExtraData *) extra);
}



/*
 * MarkRelationAsForeign records on an upper relation which server its work runs
 * on, which is what puts the server into the plan node and lets the planner
 * treat the relation as one this wrapper can plan.
 */
static void
MarkRelationAsForeign(RelOptInfo *rel, RelOptInfo *inputRel)
{
	rel->serverid = inputRel->serverid;
	rel->userid = inputRel->userid;
	rel->useridiscurrent = inputRel->useridiscurrent;
	rel->fdwroutine = inputRel->fdwroutine;
}

/*
 * AddForeignGroupingPaths adds a path that lets Snowflake do the grouping and
 * the aggregation.
 */
static void
AddForeignGroupingPaths(PlannerInfo *root, RelOptInfo *inputRel,
						RelOptInfo *groupedRel, GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	SnowflakeRelationInfo *inputRelationInfo =
		(SnowflakeRelationInfo *) inputRel->fdw_private;

	if (!SnowflakeEnableAggregatePushdown ||
		!inputRelationInfo->connection->enableAggregatePushdown)
		return;

	/* only a plain GROUP BY, and only when there is something to aggregate */
	if (!parse->hasAggs || parse->groupingSets != NIL)
		return;

	if (extra->patype != PARTITIONWISE_AGGREGATE_NONE)
		return;

	/*
	 * A condition that Snowflake cannot evaluate has to filter the rows
	 * before they are aggregated, which cannot be arranged if the aggregation
	 * happens remotely.
	 */
	if (inputRelationInfo->localConds != NIL)
		return;

	SnowflakeRelationInfo *relationInfo = CopyRelationInfo(inputRelationInfo);

	relationInfo->isGrouped = true;

	if (!GroupingIsShippable(root, groupedRel, relationInfo, extra))
		return;

	/* the number of groups is what the statement will return */
	if (relationInfo->groupExprs == NIL)
		relationInfo->rows = 1;
	else
	{
		relationInfo->rows = estimate_num_groups(root, relationInfo->groupExprs,
												 inputRelationInfo->rows, NULL, NULL);
	}

	CostSnowflakeScan(relationInfo);

	groupedRel->fdw_private = relationInfo;
	MarkRelationAsForeign(groupedRel, inputRel);

	add_path(groupedRel, (Path *) CreateUpperPath(root, groupedRel,
												  groupedRel->reltarget, relationInfo));
}


/*
 * GroupingIsShippable decides whether the grouping and aggregation of the query
 * can be evaluated by Snowflake, and builds the target list of the statement
 * that would do it.
 *
 * The target list starts with the grouping expressions, in the order of the
 * GROUP BY, followed by the aggregates, so that the GROUP BY can refer to them
 * by ordinal.
 */
static bool
GroupingIsShippable(PlannerInfo *root, RelOptInfo *groupedRel,
					SnowflakeRelationInfo * relationInfo, GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	ListCell   *groupCell = NULL;
	List	   *scanTlist = NIL;

	foreach(groupCell, parse->groupClause)
	{
		SortGroupClause *groupClause = (SortGroupClause *) lfirst(groupCell);
		Expr	   *groupExpr = (Expr *) get_sortgroupclause_expr(groupClause,
																  root->processed_tlist);

		if (!SnowflakeIsShippableExpression(groupExpr, relationInfo))
			return false;

		relationInfo->groupExprs = lappend(relationInfo->groupExprs, groupExpr);
		scanTlist = add_to_flat_tlist(scanTlist, list_make1(groupExpr));
	}

	/*
	 * Everything the query needs above the aggregation has to be either an
	 * aggregate Snowflake can compute or an expression over the grouping
	 * columns. A Var that is neither is one Postgres allows through a
	 * functional dependency on a primary key, which Snowflake would reject.
	 */
	List	   *neededExprs = list_copy(groupedRel->reltarget->exprs);

	if (extra->havingQual != NULL)
		neededExprs = lappend(neededExprs, extra->havingQual);

	List	   *aggregatesAndVars = pull_var_clause((Node *) neededExprs,
													PVC_INCLUDE_AGGREGATES |
													PVC_RECURSE_PLACEHOLDERS);
	ListCell   *exprCell = NULL;

	foreach(exprCell, aggregatesAndVars)
	{
		Expr	   *expr = (Expr *) lfirst(exprCell);

		if (IsA(expr, Aggref))
		{
			if (!SnowflakeIsShippableExpression(expr, relationInfo))
				return false;

			scanTlist = add_to_flat_tlist(scanTlist, list_make1(expr));
		}
		else if (!list_member(relationInfo->groupExprs, expr))
		{
			return false;
		}
	}

	/* HAVING conditions that Snowflake cannot evaluate are applied locally */
	if (extra->havingQual != NULL)
	{
		ListCell   *havingCell = NULL;

		foreach(havingCell, (List *) extra->havingQual)
		{
			Expr	   *havingClause = (Expr *) lfirst(havingCell);

			if (SnowflakeIsShippableExpression(havingClause, relationInfo))
			{
				relationInfo->havingQuals = lappend(relationInfo->havingQuals,
													havingClause);
			}
			else
			{
				relationInfo->havingLocalQuals =
					lappend(relationInfo->havingLocalQuals, havingClause);
			}
		}
	}

	if (scanTlist == NIL)
		return false;

	relationInfo->scanTlist = scanTlist;

	return true;
}


/*
 * AddForeignFinalPaths adds a path that lets Snowflake apply the LIMIT, so that
 * an exploratory query does not move a whole table.
 */
static void
AddForeignFinalPaths(PlannerInfo *root, RelOptInfo *inputRel, RelOptInfo *finalRel,
					 FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	SnowflakeRelationInfo *inputRelationInfo =
		(SnowflakeRelationInfo *) inputRel->fdw_private;

	if (!extra->limit_needed)
		return;

	/*
	 * Anything between the scan and the limit decides which rows the limit
	 * keeps, so the limit can only travel when there is nothing in between.
	 */
	if (parse->commandType != CMD_SELECT || parse->rowMarks != NIL ||
		parse->distinctClause != NIL || parse->sortClause != NIL ||
		parse->hasWindowFuncs || parse->hasDistinctOn ||
		parse->setOperations != NULL)
		return;

	if (parse->limitOption != LIMIT_OPTION_COUNT)
		return;

	if (inputRelationInfo->localConds != NIL ||
		inputRelationInfo->havingLocalQuals != NIL)
		return;

	/*
	 * The count has to be known now. A parameterised LIMIT would have to be
	 * spliced into the statement like a condition parameter, which is not
	 * worth it for a clause the local Limit node applies correctly anyway.
	 */
	if (parse->limitCount == NULL || !IsA(parse->limitCount, Const) ||
		((Const *) parse->limitCount)->constisnull)
		return;

	int64		limitCount = DatumGetInt64(((Const *) parse->limitCount)->constvalue);
	int64		limitOffset = 0;

	if (parse->limitOffset != NULL)
	{
		if (!IsA(parse->limitOffset, Const) ||
			((Const *) parse->limitOffset)->constisnull)
			return;

		limitOffset = DatumGetInt64(((Const *) parse->limitOffset)->constvalue);
	}

	if (limitCount < 0 || limitOffset < 0)
		return;

	SnowflakeRelationInfo *relationInfo = CopyRelationInfo(inputRelationInfo);

	relationInfo->hasLimit = true;
	relationInfo->limitCount = limitCount;
	relationInfo->limitOffset = limitOffset;
	relationInfo->rows = Min(inputRelationInfo->rows, (double) limitCount);

	/*
	 * An upper relation returns its columns through a target list. For a
	 * grouped relation that list already exists; for a base relation it is
	 * the columns the query reads.
	 */
	if (relationInfo->scanTlist == NIL)
	{
		List	   *columnVars = pull_var_clause((Node *) inputRel->reltarget->exprs,
												 PVC_RECURSE_PLACEHOLDERS);

		if (columnVars == NIL)
			return;

		relationInfo->scanTlist = add_to_flat_tlist(NIL, columnVars);
	}

	CostSnowflakeScan(relationInfo);

	finalRel->fdw_private = relationInfo;
	MarkRelationAsForeign(finalRel, inputRel);

	PathTarget *target = root->upper_targets[UPPERREL_FINAL] != NULL ?
		root->upper_targets[UPPERREL_FINAL] : inputRel->reltarget;

	add_path(finalRel, (Path *) CreateUpperPath(root, finalRel, target, relationInfo));
}


/*
 * CopyRelationInfo copies the planner state of a relation, so that a stage on
 * top of it can add to what the stage below decided.
 */
static SnowflakeRelationInfo *
CopyRelationInfo(SnowflakeRelationInfo * relationInfo)
{
	SnowflakeRelationInfo *copy = palloc0(sizeof(SnowflakeRelationInfo));

	*copy = *relationInfo;

	copy->remoteConds = list_copy(relationInfo->remoteConds);
	copy->localConds = list_copy(relationInfo->localConds);
	copy->scanTlist = list_copy(relationInfo->scanTlist);
	copy->groupExprs = list_copy(relationInfo->groupExprs);
	copy->havingQuals = list_copy(relationInfo->havingQuals);
	copy->havingLocalQuals = list_copy(relationInfo->havingLocalQuals);

	return copy;
}

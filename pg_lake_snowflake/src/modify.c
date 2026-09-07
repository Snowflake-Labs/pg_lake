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
 * modify.c
 *
 * Writing to a Snowflake table.
 *
 * Rows arrive here one slot at a time, so an INSERT and a COPY become batched
 * INSERT statements: one request carrying as many VALUES rows as the batch size
 * and the statement size allow. That is the difference between a load that runs
 * at the speed of the warehouse and one that pays a round trip per row.
 *
 * An UPDATE or a DELETE cannot work that way. Postgres modifies rows it has
 * already read, identified by a row identifier the wrapper is supposed to carry
 * along, and a Snowflake table exposes nothing that identifies a row: no ctid,
 * no rowid, and a primary key only on a hybrid table. So the only correct shape
 * is the one Snowflake evaluates in full, and this file pushes the whole
 * statement down or refuses it with the reason. Refusing is deliberate: the
 * alternative would be to match rows by value, which silently modifies the wrong
 * number of rows as soon as two of them are equal.
 *
 * None of this is transactional with respect to the Postgres transaction. The
 * SQL API has no session that spans requests, so every statement commits on its
 * own and a later ROLLBACK cannot undo it.
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/optimizer.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif

#include "pg_lake_snowflake/deparse.h"
#include "pg_lake_snowflake/modify.h"
#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"
#include "pg_lake_snowflake/sql_api.h"
#include "pg_lake_snowflake/type_map.h"

/* the elements of the fdw_private list of an insert */
enum SnowflakeModifyPrivateIndex
{
	SNOWFLAKE_MODIFY_PRIVATE_TARGET_ATTRS
};

/* the elements of the fdw_private list of a direct UPDATE or DELETE */
enum SnowflakeDirectModifyPrivateIndex
{
	SNOWFLAKE_DIRECT_PRIVATE_FRAGMENTS,
	SNOWFLAKE_DIRECT_PRIVATE_SETS_TAG,
	SNOWFLAKE_DIRECT_PRIVATE_STATEMENT_TEXT
};

/*
 * How large a generated INSERT statement may get before the rows of a batch are
 * split across statements. Well below the limit Snowflake puts on statement
 * text, because a batch is sized in rows and a row can be wide.
 */
#define SNOWFLAKE_MAX_STATEMENT_BYTES (512 * 1024)

/* SnowflakeModifyState is the state of an insert into one table */
typedef struct SnowflakeModifyState
{
	SnowflakeConnection *connection;
	SnowflakeTable *table;

	/* "INSERT INTO db.schema.table (col, col) VALUES " */
	char	   *insertPrefix;

	int			columnCount;
	AttrNumber *attributeNumbers;
	Oid		   *columnTypes;

	int			batchSize;
}			SnowflakeModifyState;

/* SnowflakeDirectModifyState is the state of one pushed-down UPDATE or DELETE */
typedef struct SnowflakeDirectModifyState
{
	List	   *sqlFragments;
	char	   *statementText;
	bool		setsCommandTag;

	List	   *paramExprStates;
	List	   *paramTypes;

	SnowflakeConnection *connection;
	bool		executed;
}			SnowflakeDirectModifyState;

static SnowflakeModifyState * CreateModifyState(ResultRelInfo *resultRelInfo,
												List *targetAttrs);
static List *InsertTargetAttributes(Relation relation, Bitmapset *insertedColumns);
static char *BuildInsertPrefix(SnowflakeModifyState * state, Relation relation);
static int	InsertSlots(SnowflakeModifyState * state, TupleTableSlot **slots,
						int slotCount);
static void AppendSlotValues(SnowflakeModifyState * state, StringInfo statement,
							 TupleTableSlot *slot);
static void ErrorIfUnsupportedModify(ModifyTable *plan, Relation relation);
static ForeignScan *FindModifyTableSubplan(ModifyTable *plan, Index resultRelation,
										   int subplanIndex);
static void ExecuteDirectModify(ForeignScanState *scanState);
static int	TableBatchSize(Relation relation);


/*
 * SnowflakeIsForeignRelUpdatable reports which statements a Snowflake foreign
 * table accepts, which is all of them unless the server or the table says it is
 * read-only.
 */
int
SnowflakeIsForeignRelUpdatable(Relation relation)
{
	if (!SnowflakeTableIsUpdatable(RelationGetRelid(relation)))
		return 0;

	return (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE);
}


/*
 * SnowflakePlanForeignModify plans an INSERT.
 *
 * Reaching it with an UPDATE or a DELETE means SnowflakePlanDirectModify already
 * refused to push the statement down, so this is where that is reported.
 */
List *
SnowflakePlanForeignModify(PlannerInfo *root, ModifyTable *plan,
						   Index resultRelation, int subplanIndex)
{
	RangeTblEntry *rangeTableEntry = planner_rt_fetch(resultRelation, root);
	Relation	relation = table_open(rangeTableEntry->relid, NoLock);
	List	   *targetAttrs = NIL;

	PG_TRY();
	{
		ErrorIfUnsupportedModify(plan, relation);

		RTEPermissionInfo *permissionInfo =
			getRTEPermissionInfo(root->parse->rteperminfos, rangeTableEntry);

		targetAttrs = InsertTargetAttributes(relation, permissionInfo->insertedCols);
	}
	PG_FINALLY();
	{
		table_close(relation, NoLock);
	}
	PG_END_TRY();

	return list_make1(targetAttrs);
}


/*
 * ErrorIfUnsupportedModify reports the statements this wrapper cannot carry out,
 * with what to do instead.
 */
static void
ErrorIfUnsupportedModify(ModifyTable *plan, Relation relation)
{
	const char *relationName = RelationGetRelationName(relation);

	/*
	 * Postgres reports this itself for an INSERT, but an UPDATE or a DELETE
	 * only reaches its own check after planning, which is where this is.
	 */
	if (!SnowflakeTableIsUpdatable(RelationGetRelid(relation)))
	{
		const char *operationName = plan->operation == CMD_INSERT ? "inserts" :
			plan->operation == CMD_UPDATE ? "updates" : "deletes";

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign table \"%s\" does not allow %s", relationName,
						operationName)));
	}

	if (plan->operation == CMD_UPDATE || plan->operation == CMD_DELETE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot %s the Snowflake table \"%s\" one row at a time",
						plan->operation == CMD_UPDATE ? "update" : "delete",
						relationName),
				 errdetail("A Snowflake table has no row identifier, so the whole "
						   "statement has to be one Snowflake can evaluate: its "
						   "conditions and its assignments must refer only to "
						   "\"%s\" and be of a kind that is pushed down.",
						   relationName),
				 errhint("EXPLAIN the statement to see what stayed local, or read "
						 "\"What runs in Snowflake\" in the pg_lake documentation.")));
	}

	if (plan->returningLists != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("RETURNING is not supported on the Snowflake table \"%s\"",
						relationName),
				 errdetail("Snowflake answers a data modification with a row count "
						   "rather than with the rows it changed.")));
	}

	if (plan->onConflictAction != ONCONFLICT_NONE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ON CONFLICT is not supported on the Snowflake table \"%s\"",
						relationName)));
	}
}


/*
 * InsertTargetAttributes returns the columns an INSERT sends.
 *
 * A column the statement did not mention is left out, so that the default or the
 * sequence Snowflake has for it applies. One that Postgres has a default for is
 * included, because then the value in the slot is that default and leaving it out
 * would quietly ignore it.
 */
static List *
InsertTargetAttributes(Relation relation, Bitmapset *insertedColumns)
{
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	List	   *targetAttrs = NIL;
	List	   *allAttrs = NIL;

	for (AttrNumber attributeNumber = 1;
		 attributeNumber <= tupleDescriptor->natts;
		 attributeNumber++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
													attributeNumber - 1);

		if (attribute->attisdropped || attribute->attgenerated != '\0')
			continue;

		allAttrs = lappend_int(allAttrs, attributeNumber);

		bool		wasMentioned =
			bms_is_member(attributeNumber - FirstLowInvalidHeapAttributeNumber,
						  insertedColumns);

		if (wasMentioned || attribute->atthasdef)
			targetAttrs = lappend_int(targetAttrs, attributeNumber);
	}

	/* INSERT ... DEFAULT VALUES mentions nothing at all */
	if (targetAttrs == NIL)
		return allAttrs;

	return targetAttrs;
}


/*
 * SnowflakeBeginForeignModify prepares an insert.
 */
void
SnowflakeBeginForeignModify(ModifyTableState *modifyTableState,
							ResultRelInfo *resultRelInfo, List *fdwPrivate,
							int subplanIndex, int executorFlags)
{
	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	List	   *targetAttrs = (List *) list_nth(fdwPrivate,
												SNOWFLAKE_MODIFY_PRIVATE_TARGET_ATTRS);

	resultRelInfo->ri_FdwState = CreateModifyState(resultRelInfo, targetAttrs);
}


/*
 * SnowflakeBeginForeignInsert prepares an insert that does not come from a plan
 * of its own, which is COPY FROM and a row routed into this table.
 *
 * Nothing here knows which columns the statement mentioned, so all of them are
 * sent and a Snowflake default does not apply.
 */
void
SnowflakeBeginForeignInsert(ModifyTableState *modifyTableState,
							ResultRelInfo *resultRelInfo)
{
	Relation	relation = resultRelInfo->ri_RelationDesc;
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	List	   *targetAttrs = NIL;

	if (modifyTableState != NULL && modifyTableState->operation != CMD_INSERT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route rows into the Snowflake table \"%s\" as part "
						"of an UPDATE", RelationGetRelationName(relation))));
	}

	for (AttrNumber attributeNumber = 1;
		 attributeNumber <= tupleDescriptor->natts;
		 attributeNumber++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
													attributeNumber - 1);

		if (attribute->attisdropped || attribute->attgenerated != '\0')
			continue;

		targetAttrs = lappend_int(targetAttrs, attributeNumber);
	}

	resultRelInfo->ri_FdwState = CreateModifyState(resultRelInfo, targetAttrs);
}


/*
 * CreateModifyState resolves everything an insert needs once rather than per row.
 */
static SnowflakeModifyState *
CreateModifyState(ResultRelInfo *resultRelInfo, List *targetAttrs)
{
	Relation	relation = resultRelInfo->ri_RelationDesc;
	Oid			relationId = RelationGetRelid(relation);
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	SnowflakeModifyState *state = palloc0(sizeof(SnowflakeModifyState));
	ListCell   *attributeCell = NULL;
	int			columnIndex = 0;

	state->connection = GetSnowflakeConnectionForRelation(relationId);
	state->table = GetSnowflakeTable(relationId, state->connection);
	state->columnCount = list_length(targetAttrs);
	state->attributeNumbers = palloc0(sizeof(AttrNumber) * Max(state->columnCount, 1));
	state->columnTypes = palloc0(sizeof(Oid) * Max(state->columnCount, 1));
	state->batchSize = TableBatchSize(relation);

	foreach(attributeCell, targetAttrs)
	{
		AttrNumber	attributeNumber = (AttrNumber) lfirst_int(attributeCell);
		Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
													attributeNumber - 1);

		/*
		 * A column whose type cannot be written as a Snowflake literal is
		 * worth reporting now, by name, rather than on whichever row first
		 * has a value in it.
		 */
		if (!SnowflakeTypeIsWritable(attribute->atttypid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot write column \"%s\" of the Snowflake table "
							"\"%s\"", NameStr(attribute->attname),
							RelationGetRelationName(relation)),
					 errdetail("There is no Snowflake literal for type %s.",
							   format_type_be(attribute->atttypid))));
		}

		state->attributeNumbers[columnIndex] = attributeNumber;
		state->columnTypes[columnIndex] = attribute->atttypid;
		columnIndex++;
	}

	if (state->columnCount == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot insert into the Snowflake table \"%s\" without "
						"columns", RelationGetRelationName(relation))));
	}

	state->insertPrefix = BuildInsertPrefix(state, relation);

	return state;
}


/*
 * BuildInsertPrefix builds everything of an INSERT that does not depend on the
 * rows, which is all of it up to the VALUES.
 */
static char *
BuildInsertPrefix(SnowflakeModifyState * state, Relation relation)
{
	StringInfo	prefix = makeStringInfo();
	Oid			relationId = RelationGetRelid(relation);
	bool		needsSelect = false;

	appendStringInfo(prefix, "INSERT INTO %s (",
					 SnowflakeQualifiedTableName(state->table));

	for (int columnIndex = 0; columnIndex < state->columnCount; columnIndex++)
	{
		if (columnIndex > 0)
			appendStringInfoString(prefix, ", ");

		appendStringInfoString(prefix,
							   SnowflakeColumnName(relationId,
												   state->attributeNumbers[columnIndex]));

		if (SnowflakeTypeNeedsValuesExpression(state->columnTypes[columnIndex]))
			needsSelect = true;
	}

	appendStringInfoChar(prefix, ')');

	/*
	 * Snowflake accepts only constants in a VALUES clause, so a column that
	 * has to be built with a function call is converted in a SELECT over the
	 * same rows instead. The rows are appended by the caller either way.
	 */
	if (!needsSelect)
	{
		appendStringInfoString(prefix, " VALUES ");

		return prefix->data;
	}

	appendStringInfoString(prefix, " SELECT ");

	for (int columnIndex = 0; columnIndex < state->columnCount; columnIndex++)
	{
		if (columnIndex > 0)
			appendStringInfoString(prefix, ", ");

		appendStringInfoString(prefix,
							   SnowflakeValuesColumnExpression(state->columnTypes[columnIndex],
															   columnIndex + 1));
	}

	appendStringInfoString(prefix, " FROM VALUES ");

	return prefix->data;
}


/*
 * TableBatchSize returns how many rows one INSERT may carry, from the batch_size
 * option of the table or of its server.
 */
static int
TableBatchSize(Relation relation)
{
	int			batchSize = SnowflakeDefaultBatchSize;
	char	   *tableOption = SnowflakeTableOption(RelationGetRelid(relation),
												   "batch_size");

	if (tableOption != NULL)
		(void) parse_int(tableOption, &batchSize, 0, NULL);

	return Max(batchSize, 1);
}


/*
 * SnowflakeExecForeignInsert inserts one row.
 */
TupleTableSlot *
SnowflakeExecForeignInsert(EState *executorState, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	SnowflakeModifyState *state =
		(SnowflakeModifyState *) resultRelInfo->ri_FdwState;

	(void) InsertSlots(state, &slot, 1);

	return slot;
}


/*
 * SnowflakeExecForeignBatchInsert inserts a batch of rows, which is one
 * statement unless the rows are wide enough to need more than one.
 */
TupleTableSlot **
SnowflakeExecForeignBatchInsert(EState *executorState, ResultRelInfo *resultRelInfo,
								TupleTableSlot **slots, TupleTableSlot **planSlots,
								int *slotCount)
{
	SnowflakeModifyState *state =
		(SnowflakeModifyState *) resultRelInfo->ri_FdwState;

	*slotCount = InsertSlots(state, slots, *slotCount);

	return slots;
}


/*
 * InsertSlots sends the given rows and returns how many were inserted.
 *
 * The rows of one call are split across statements when the text would grow past
 * what Snowflake accepts, which keeps a wide table from failing where a narrow
 * one succeeds with the same batch size.
 */
static int
InsertSlots(SnowflakeModifyState * state, TupleTableSlot **slots, int slotCount)
{
	MemoryContext statementContext = AllocSetContextCreate(CurrentMemoryContext,
														   "pg_lake_snowflake insert",
														   ALLOCSET_DEFAULT_SIZES);
	MemoryContext previousContext = MemoryContextSwitchTo(statementContext);
	StringInfo	statement = makeStringInfo();
	int			rowsInStatement = 0;

	SnowflakeWarnIfWriteIsNotTransactional();

	appendStringInfoString(statement, state->insertPrefix);

	for (int slotIndex = 0; slotIndex < slotCount; slotIndex++)
	{
		if (rowsInStatement > 0)
		{
			if (statement->len >= SNOWFLAKE_MAX_STATEMENT_BYTES)
			{
				SnowflakeStatementClose(SnowflakeExecute(state->connection,
														 statement->data));

				resetStringInfo(statement);
				appendStringInfoString(statement, state->insertPrefix);
				rowsInStatement = 0;
			}
			else
			{
				appendStringInfoString(statement, ", ");
			}
		}

		AppendSlotValues(state, statement, slots[slotIndex]);
		rowsInStatement++;
	}

	if (rowsInStatement > 0)
		SnowflakeStatementClose(SnowflakeExecute(state->connection, statement->data));

	MemoryContextSwitchTo(previousContext);
	MemoryContextDelete(statementContext);

	return slotCount;
}


/*
 * AppendSlotValues writes one row of an INSERT.
 */
static void
AppendSlotValues(SnowflakeModifyState * state, StringInfo statement,
				 TupleTableSlot *slot)
{
	appendStringInfoChar(statement, '(');

	for (int columnIndex = 0; columnIndex < state->columnCount; columnIndex++)
	{
		bool		isNull = false;
		Datum		value = slot_getattr(slot, state->attributeNumbers[columnIndex],
										 &isNull);

		if (columnIndex > 0)
			appendStringInfoString(statement, ", ");

		if (isNull)
		{
			/*
			 * A column that is converted in the SELECT list needs a type even
			 * when it is NULL in every row of the batch: Snowflake resolves
			 * the conversion against the type of the VALUES column, and an
			 * untyped NULL leaves it nothing to resolve against.
			 */
			if (SnowflakeTypeNeedsValuesExpression(state->columnTypes[columnIndex]))
				appendStringInfoString(statement, "NULL::VARCHAR");
			else
				appendStringInfoString(statement, "NULL");
		}
		else
		{
			appendStringInfoString(statement,
								   SnowflakeFormatValuesLiteral(value,
																state->columnTypes[columnIndex]));
		}
	}

	appendStringInfoChar(statement, ')');
}


/*
 * SnowflakeGetForeignModifyBatchSize returns how many rows the executor may hand
 * over at once.
 */
int
SnowflakeGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
	SnowflakeModifyState *state =
		(SnowflakeModifyState *) resultRelInfo->ri_FdwState;

	/* nothing was prepared, which is the EXPLAIN-only case */
	if (state == NULL)
		return 1;

	/*
	 * A row trigger or a RETURNING projection has to see one row at a time,
	 * and batching would hide the rows from it.
	 */
	if (resultRelInfo->ri_projectReturning != NULL)
		return 1;

	if (resultRelInfo->ri_TrigDesc != NULL &&
		(resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
		 resultRelInfo->ri_TrigDesc->trig_insert_new_table))
		return 1;

	return state->batchSize;
}


/*
 * SnowflakeEndForeignModify releases the state of an insert.
 */
void
SnowflakeEndForeignModify(EState *executorState, ResultRelInfo *resultRelInfo)
{
	resultRelInfo->ri_FdwState = NULL;
}


/*
 * SnowflakeEndForeignInsert releases the state of a COPY or a routed insert.
 */
void
SnowflakeEndForeignInsert(EState *executorState, ResultRelInfo *resultRelInfo)
{
	resultRelInfo->ri_FdwState = NULL;
}


/*
 * SnowflakeExecForeignUpdate and SnowflakeExecForeignDelete exist so that a path
 * that reaches them fails with the reason rather than with a null pointer. The
 * plan-time check in SnowflakePlanForeignModify is what normally reports it.
 */
TupleTableSlot *
SnowflakeExecForeignUpdate(EState *executorState, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot update the Snowflake table \"%s\" one row at a time",
					RelationGetRelationName(resultRelInfo->ri_RelationDesc))));

	return NULL;
}


TupleTableSlot *
SnowflakeExecForeignDelete(EState *executorState, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot delete from the Snowflake table \"%s\" one row at a "
					"time",
					RelationGetRelationName(resultRelInfo->ri_RelationDesc))));

	return NULL;
}


/*
 * SnowflakePlanDirectModify turns an UPDATE or a DELETE into one statement that
 * Snowflake evaluates, when everything the statement does can travel.
 */
bool
SnowflakePlanDirectModify(PlannerInfo *root, ModifyTable *plan,
						  Index resultRelation, int subplanIndex)
{
	CmdType		operation = plan->operation;

	if (operation != CMD_UPDATE && operation != CMD_DELETE)
		return false;

	/* Snowflake answers a modification with a count, never with rows */
	if (plan->returningLists != NIL)
		return false;

	ForeignScan *foreignScan = FindModifyTableSubplan(plan, resultRelation,
													  subplanIndex);

	if (foreignScan == NULL)
		return false;

	/*
	 * A condition that stayed behind as a filter decides which rows are
	 * modified, so the statement cannot be pushed down whole.
	 */
	if (foreignScan->scan.plan.qual != NIL)
		return false;

	/* only a plain scan of the target table itself */
	if (foreignScan->scan.scanrelid != resultRelation)
		return false;

	RelOptInfo *foreignRel = root->simple_rel_array[resultRelation];

	if (foreignRel == NULL || foreignRel->fdw_private == NULL)
		return false;

	SnowflakeRelationInfo *relationInfo =
		(SnowflakeRelationInfo *) foreignRel->fdw_private;

	if (!relationInfo->pushdownSafe || relationInfo->localConds != NIL)
		return false;

	if (!SnowflakeTableIsUpdatable(relationInfo->relationId))
		return false;

	SnowflakeDeparsedQuery query = {0};

	if (operation == CMD_UPDATE)
	{
		List	   *setExpressions = NIL;
		List	   *targetAttrs = NIL;
		ListCell   *expressionCell = NULL;
		ListCell   *attributeCell = NULL;

		get_translated_update_targetlist(root, resultRelation, &setExpressions,
										 &targetAttrs);

		forboth(expressionCell, setExpressions, attributeCell, targetAttrs)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(expressionCell);
			AttrNumber	attributeNumber = (AttrNumber) lfirst_int(attributeCell);

			if (attributeNumber <= InvalidAttrNumber)
				return false;

			if (!SnowflakeIsShippableAssignment(targetEntry->expr, relationInfo))
				return false;
		}

		SnowflakeDeparseDirectUpdate(root, relationInfo, targetAttrs, setExpressions,
									 &query);
	}
	else
	{
		SnowflakeDeparseDirectDelete(root, relationInfo, &query);
	}

	/*
	 * Rewrite the scan into the modification.
	 *
	 * The target list of the scan is left alone even though the modification
	 * returns no rows through it: an UPDATE of a foreign table carries a
	 * whole-row junk column that the executor looks for by name, and clearing
	 * the list takes it away.
	 */
	foreignScan->operation = operation;
	foreignScan->resultRelation = resultRelation;
	foreignScan->fdw_exprs = query.paramExprs;
	foreignScan->fdw_private =
		list_make3(query.sqlFragments,
				   makeBoolean(plan->canSetTag),
				   makeString(SnowflakeDeparsedQueryText(query.sqlFragments)));

	return true;
}


/*
 * FindModifyTableSubplan returns the ForeignScan that reads the table being
 * modified, or NULL when the plan reads it in any way that is more involved than
 * that. Anything else means local work decides which rows are affected.
 */
static ForeignScan *
FindModifyTableSubplan(ModifyTable *plan, Index resultRelation, int subplanIndex)
{
	Plan	   *subplan = outerPlan(plan);

	if (subplan == NULL)
		return NULL;

	/* one target table means one subplan, which has to be the scan itself */
	if (list_length(plan->resultRelations) != 1 || subplanIndex != 0)
		return NULL;

	if (!IsA(subplan, ForeignScan))
		return NULL;

	ForeignScan *foreignScan = (ForeignScan *) subplan;

	if (!bms_is_member(resultRelation, foreignScan->fs_base_relids))
		return NULL;

	return foreignScan;
}


/*
 * SnowflakeBeginDirectModify prepares a pushed-down UPDATE or DELETE.
 */
void
SnowflakeBeginDirectModify(ForeignScanState *scanState, int executorFlags)
{
	ForeignScan *foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	SnowflakeDirectModifyState *state = palloc0(sizeof(SnowflakeDirectModifyState));

	scanState->fdw_state = state;

	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	state->sqlFragments = (List *) list_nth(foreignScan->fdw_private,
											SNOWFLAKE_DIRECT_PRIVATE_FRAGMENTS);
	state->setsCommandTag = boolVal(list_nth(foreignScan->fdw_private,
											 SNOWFLAKE_DIRECT_PRIVATE_SETS_TAG));
	state->statementText = strVal(list_nth(foreignScan->fdw_private,
										   SNOWFLAKE_DIRECT_PRIVATE_STATEMENT_TEXT));

	Oid			userId = OidIsValid(foreignScan->checkAsUser) ?
		foreignScan->checkAsUser : GetUserId();

	state->connection = GetSnowflakeConnection(foreignScan->fs_server, userId);

	ListCell   *paramCell = NULL;

	foreach(paramCell, foreignScan->fdw_exprs)
		state->paramTypes = lappend_oid(state->paramTypes,
										exprType((Node *) lfirst(paramCell)));

	state->paramExprStates = ExecInitExprList(foreignScan->fdw_exprs,
											  (PlanState *) scanState);
}


/*
 * SnowflakeIterateDirectModify runs the statement once and reports how many rows
 * it changed. It returns no rows, which is why an empty slot ends the scan.
 */
TupleTableSlot *
SnowflakeIterateDirectModify(ForeignScanState *scanState)
{
	SnowflakeDirectModifyState *state =
		(SnowflakeDirectModifyState *) scanState->fdw_state;

	if (!state->executed)
		ExecuteDirectModify(scanState);

	return ExecClearTuple(scanState->ss.ss_ScanTupleSlot);
}


/*
 * ExecuteDirectModify sends the statement with the current values of its
 * parameters.
 */
static void
ExecuteDirectModify(ForeignScanState *scanState)
{
	SnowflakeDirectModifyState *state =
		(SnowflakeDirectModifyState *) scanState->fdw_state;
	EState	   *executorState = scanState->ss.ps.state;
	ExprContext *expressionContext = scanState->ss.ps.ps_ExprContext;
	List	   *paramLiterals = NIL;
	ListCell   *paramStateCell = NULL;
	ListCell   *paramTypeCell = NULL;

	SnowflakeWarnIfWriteIsNotTransactional();

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
	SnowflakeStatement *statement = SnowflakeExecute(state->connection, sql);

	state->executed = true;

	if (state->setsCommandTag)
		executorState->es_processed += statement->affectedRowCount;

	SnowflakeStatementClose(statement);
}


/*
 * SnowflakeEndDirectModify releases the state of a pushed-down modification.
 */
void
SnowflakeEndDirectModify(ForeignScanState *scanState)
{
	scanState->fdw_state = NULL;
}


/*
 * SnowflakeExplainDirectModify shows the statement Snowflake will run.
 */
void
SnowflakeExplainDirectModify(ForeignScanState *scanState, ExplainState *es)
{
	ForeignScan *foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	char	   *statementText = strVal(list_nth(foreignScan->fdw_private,
												SNOWFLAKE_DIRECT_PRIVATE_STATEMENT_TEXT));

	ExplainPropertyText("Snowflake SQL", statementText, es);
}


/*
 * SnowflakeExecForeignTruncate empties Snowflake tables.
 *
 * One statement per table, because Snowflake truncates one table at a time. The
 * drop behaviour is not passed on: there is nothing in Snowflake for it to mean,
 * and a hybrid table with dependents refuses the truncation on its own.
 */
void
SnowflakeExecForeignTruncate(List *relations, DropBehavior behavior,
							 bool restartSequences)
{
	ListCell   *relationCell = NULL;

	if (restartSequences)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TRUNCATE with RESTART IDENTITY is not supported on a "
						"Snowflake table"),
				 errdetail("Snowflake does not expose the state of a column's "
						   "sequence, so restarting it cannot be carried out.")));
	}

	SnowflakeWarnIfWriteIsNotTransactional();

	foreach(relationCell, relations)
	{
		Relation	relation = (Relation) lfirst(relationCell);
		Oid			relationId = RelationGetRelid(relation);
		SnowflakeConnection *connection = GetSnowflakeConnectionForRelation(relationId);
		SnowflakeTable *table = GetSnowflakeTable(relationId, connection);

		if (!SnowflakeTableIsUpdatable(relationId))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("foreign table \"%s\" does not allow truncates",
							RelationGetRelationName(relation))));
		}

		char	   *sql = psprintf("TRUNCATE TABLE %s",
								   SnowflakeQualifiedTableName(table));

		SnowflakeStatementClose(SnowflakeExecute(connection, sql));
	}
}

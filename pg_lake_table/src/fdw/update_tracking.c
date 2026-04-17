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
 * When an update involves a join, ExecForeignUpdate may be invoked multiple
 * times, even though the row should only be updated once and have a single
 * RETURNING slot.
 *
 * Other FDWs deal with this by repeating a single-row update on the remote
 * table, usually in a way where the second update is a noop.
 *
 * It's required that ExecForeignUpdate immediately knows whether the update
 * is a noop, because when the update has a RETURNING it needs to decide whether
 * to return a slot. The slot is also used for other operations like triggers.
 *
 * Since we do not have a remote transactional store in which we can efficiently
 * do our bookkeeping on updated rows, we instead do it in a temporary table of
 * the form:
 * CREATE TEMP TABLE lake_update_<relid> (rowid tid not null primary key);
 *
 * For every updated row, IsFirstUpdateOfTuple checks whether the table contains the
 * (synthetic) tuple identifier and if not inserts it. If the tuple identifier
 * was already in the table, ExecForeignUpdate can stop processing, since the
 * first update is perfectly valid.
 *
 * The temporary table should not be reused across commands, since the synthetic
 * tuple identifiers are only valid for a single command.
 *
 * Temporary tables have significant overhead for storing tuple identifiers
 * due to the tuple headers, but since the number of identifiers we need to track
 * can be quite large, it's important to be able to spill to disk. A temporary
 * table is a reasonably straight-forward way of doing that and also have
 * reasonably efficient lookups.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "pg_lake/fdw/update_tracking.h"
#include "pg_lake/util/item_pointer_utils.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/itemptr.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"


static RangeVar *GetUpdateTableRangeVar(Oid relationId);
static Oid	CreateUpdateTrackingTable(RangeVar *updateTableName);

/*
 * CreatePgLakeUpdateTable creates a temporary table to track which
 * row IDs have been updated by the current command.
 *
 * If the temporary table already exists, it means another update on the table
 * is active and we return false.
 */
bool
BeginRelationUpdateTracking(RelationUpdateTrackingState *state, Oid relationId)
{
	RangeVar   *updateTableName = GetUpdateTableRangeVar(relationId);

	/* check whether the update tracking table already exists */
	bool		missingOk = true;
	Oid			existingTempId = RangeVarGetRelid(updateTableName, NoLock, missingOk);

	if (existingTempId != InvalidOid)
		return false;

	Oid			updateRelationId = CreateUpdateTrackingTable(updateTableName);

	/*
	 * Open the update tracking table. ExecInitResultRelation will also open
	 * the table, but it also verifies that we already have a lock.
	 */
	state->updateRel = table_open(updateRelationId, RowExclusiveLock);

	/* create a slot for insertion */
	state->rowLocationSlot = table_slot_create(state->updateRel, NULL);

	/* set up executor state for insertion */
	state->estate = CreateExecutorState();
	state->updateRelInfo = makeNode(ResultRelInfo);
	state->perTupleContext = AllocSetContextCreate(state->estate->es_query_cxt,
												   "pg_lake_table update tracking",
												   ALLOCSET_SMALL_SIZES);

	/* open temp table for insertion */
	RangeTblEntry *rte = makeNode(RangeTblEntry);

	rte->rtekind = RTE_RELATION;
	rte->relid = updateRelationId;
	rte->relkind = RELKIND_RELATION;
	rte->rellockmode = RowExclusiveLock;

	ExecInitRangeTable(state->estate,
					   list_make1(rte),
					   NIL
#if PG_VERSION_NUM >= 180000
					   ,bms_make_singleton(1)	/* unpruned_relids */
#endif
		);
	ExecInitResultRelation(state->estate, state->updateRelInfo, 1);

	/* needed to initialize ExecCheckIndexConstraints */
	bool		speculative = true;

	ExecOpenIndices(state->updateRelInfo, speculative);

	return true;
}


/*
 * GetUpdateTableRangeVar returns a temporary table RangeVar for a
 * update table.
 */
static RangeVar *
GetUpdateTableRangeVar(Oid relationId)
{
	char	   *schemaName = NULL;
	char	   *tableName = psprintf("lake_update_%d", relationId);
	RangeVar   *relation = makeRangeVar(schemaName, tableName, -1);

	relation->relpersistence = RELPERSISTENCE_TEMP;

	return relation;
}


/*
 * CreateUpdateTrackingTable creates a temporary table used for tracking
 * updated ctids.
 */
static Oid
CreateUpdateTrackingTable(RangeVar *updateTableName)
{
	/* CREATE TEMP TABLE lake_update_<relationId> */
	CreateStmt *createTempTable = makeNode(CreateStmt);

	createTempTable->relation = updateTableName;

	/*
	 * We use the heap access method to as we need primary key support. We do
	 * not want table_default_access_method to be used.
	 */
	createTempTable->accessMethod = "heap";

	/* (rowid bigint not null) */
	ColumnDef  *column = makeColumnDef("rowid", TIDOID, -1, InvalidOid);

	column->is_not_null = true;
	createTempTable->tableElts = list_make1(column);

	ObjectAddress updateTableAddress =
		DefineRelation(createTempTable, RELKIND_RELATION, InvalidOid, NULL, "");

	/*
	 * ALTER TABLE lake_update_<relationId> ADD CONSTRAINT
	 * lake_update_pk_<relationId> PRIMARY KEY (rowid);
	 */
	IndexStmt  *createPrimaryKey = makeNode(IndexStmt);

	createPrimaryKey->idxname = psprintf("%s_pk", updateTableName->relname);
	createPrimaryKey->relation = updateTableName;
	createPrimaryKey->accessMethod = "btree";
	createPrimaryKey->primary = true;
	createPrimaryKey->unique = true;
	createPrimaryKey->isconstraint = true;

	IndexElem  *indexColumn = makeNode(IndexElem);

	indexColumn->name = "rowid";
	createPrimaryKey->indexParams = list_make1(indexColumn);

	DefineIndex(updateTableAddress.objectId,
				createPrimaryKey,
				 /* indexRelationId */ InvalidOid,
				 /* parentIndexId */ InvalidOid,
				 /* parentConstraintId */ InvalidOid,
				 /* total_parts */ -1,
				 /* is_alter_table */ false,
				 /* check_rights */ false,
				 /* check_not_in_use */ false,
				 /* skip_builds */ false,
				 /* quiet */ true);

	CommandCounterIncrement();

	return updateTableAddress.objectId;
}


/*
 * IsFirstUpdateOfTuple checks whether a row ID exists in an update table, and
 * inserts it if it does not.
 *
 * We use low-level C function to minimize overhead because the number of rows
 * updated can potentially be very large.
 *
 * Using a temporary table is not particularly efficient, since row headers and
 * index entries are way bigger than the actual values, but it scales well and
 * spills to disk.
 */
bool
IsFirstUpdateOfTuple(RelationUpdateTrackingState *state, ItemPointer rowLocation)
{
	MemoryContext oldContext = MemoryContextSwitchTo(state->perTupleContext);

	/* make sure we see previously written tuples */
	PushActiveSnapshot(GetTransactionSnapshot());

	/* set the row ID in the TupleTableSlot */
	TupleTableSlot *rowLocationSlot = state->rowLocationSlot;

	ExecClearTuple(rowLocationSlot);

	rowLocationSlot->tts_values[0] = ItemPointerGetDatum(rowLocation);
	rowLocationSlot->tts_isnull[0] = false;
	ExecStoreVirtualTuple(rowLocationSlot);


	/* check whether the new row ID would conflict */
	ItemPointerData conflictTid;
	bool		isNew = ExecCheckIndexConstraints(state->updateRelInfo,
												  rowLocationSlot,
												  state->estate,
												  &conflictTid,
#if PG_VERSION_NUM >= 180000
												  &rowLocationSlot->tts_tid,	/* tupleid */
#endif
												  NIL);

	if (isNew)
		/* row ID is not yet in the table, insert it now */
		ExecSimpleRelationInsert(state->updateRelInfo, state->estate, rowLocationSlot);

	PopActiveSnapshot();

	/*
	 * Make sure we do not leak memory, since all our state is written to the
	 * temp table we only care to remember whether the tuple was found.
	 */
	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(state->perTupleContext);

	return isNew;
}


/*
 * FinishRelationUpdateTracking drops the previously created temp table
 * to track which rows have been updated by the current command.
 */
void
FinishRelationUpdateTracking(RelationUpdateTrackingState *state)
{
	char	   *tableName = get_rel_name(RelationGetRelid(state->updateRel));

	/* close the temp table for writes */
	table_close(state->updateRel, NoLock);
	ExecCloseResultRelations(state->estate);
	ExecCloseRangeTableRelations(state->estate);
	FreeExecutorState(state->estate);
	ExecDropSingleTupleTableSlot(state->rowLocationSlot);

	/*
	 * RemoveRelation wants name lists, we don't need to quality our temp
	 * table
	 */
	List	   *nameList = list_make1(makeString(tableName));

	/* drop the temp table */
	DropStmt   *dropStmt = makeNode(DropStmt);

	dropStmt->objects = list_make1(nameList);
	dropStmt->removeType = OBJECT_TABLE;

	RemoveRelations(dropStmt);
}

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
#include "access/xact.h"
#include "executor/executor.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"


static void IcebergXactCallback(XactEvent event, void *arg);
static void RecordTableWritesExecutorStart(QueryDesc *queryDesc, int eflags);

static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;


void
IcebergRegisterCallbacks(void)
{
	RegisterXactCallback(IcebergXactCallback, NULL);
	RegisterSubXactCallback(IcebergMetadataSubXactCallback, NULL);

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = RecordTableWritesExecutorStart;
}


/*
 * RecordTableWritesExecutorStart records the result relations of a
 * statement, so a transaction that the REST catalog commits cannot also write
 * to tables that commit through PostgreSQL.
 */
static void
RecordTableWritesExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;

	if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
	{
#if PG_VERSION_NUM >= 190000
		int			rangeTableIndex = -1;

		while ((rangeTableIndex = bms_next_member(plannedStmt->resultRelationRelids,
												  rangeTableIndex)) >= 0)
		{
#else
		ListCell   *resultRelationCell = NULL;

		foreach(resultRelationCell, plannedStmt->resultRelations)
		{
			Index		rangeTableIndex = lfirst_int(resultRelationCell);
#endif
			RangeTblEntry *rangeTableEntry = rt_fetch(rangeTableIndex, plannedStmt->rtable);

			RecordTableWriteInTx(rangeTableEntry->relid);
		}
	}

	if (PreviousExecutorStartHook != NULL)
		PreviousExecutorStartHook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}


static void
IcebergXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:

			{
				/*
				 * Postgres does not expect a snapshot to be left active at
				 * this point in the transaction. We temporarily push one, and
				 * remove in the non-error cases. If there are errors,
				 * Postgres handles Pop'ing the snapshot later on, so we don't
				 * need to worry about.
				 */
				Assert(!ActiveSnapshotSet());
				PushActiveSnapshot(GetTransactionSnapshot());

				ConsumeTrackedIcebergMetadataChanges(false);

				/*
				 * When the REST catalog commits the transaction, send the
				 * changes now, while a rejection can still abort it.
				 */
				CommitRestCatalogRequestsBeforeCommit();

				PopActiveSnapshot();

				break;
			}

		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:
			{
				ResetTrackedIcebergMetadataOperation();
				ResetRestCatalogRequests();
				ResetPostgresTableWrites();
				break;
			}
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:
			{
				if (HasAnyTrackedIcebergMetadataChanges())
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot prepare a transaction that has Iceberg metadata changes")));
				ResetPostgresTableWrites();
				break;
			}

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			{
				PostAllRestCatalogRequests();
				ResetRestCatalogRequests();
				ResetPostgresTableWrites();
				break;
			}
	}
}

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
#include "catalog/dependency.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/rel_utils.h"


static void IcebergXactCallback(XactEvent event, void *arg);
static void RecordPostgresTableWritesExecutorStart(QueryDesc *queryDesc, int eflags);

static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;


void
IcebergRegisterCallbacks(void)
{
	RegisterXactCallback(IcebergXactCallback, NULL);
	RegisterSubXactCallback(IcebergMetadataSubXactCallback, NULL);

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = RecordPostgresTableWritesExecutorStart;
}


/*
 * IsPostgresTableWrite returns whether a write to the given relation commits
 * through PostgreSQL, as opposed to through the REST catalog.
 */
bool
IsPostgresTableWrite(Oid relationId)
{
	if (IsPgLakeIcebergForeignTableById(relationId) &&
		GetIcebergCatalogType(relationId) == REST_CATALOG_READ_WRITE)
		return false;

	/* temporary tables do not outlive the session */
	if (get_rel_persistence(relationId) == RELPERSISTENCE_TEMP)
		return false;

	/* bookkeeping of pg_lake itself, such as the data files of a table */
	if (OidIsValid(getExtensionOfObject(RelationRelationId, relationId)))
		return false;

	return true;
}


/*
 * RecordPostgresTableWritesExecutorStart records the result relations of a
 * statement that commit through PostgreSQL, so a transaction that the REST
 * catalog commits cannot also write to them.
 */
static void
RecordPostgresTableWritesExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;

	if ((RestCatalogCommitMode == REST_CATALOG_COMMIT_MODE_CATALOG ||
		 XactCommitsInRestCatalog()) &&
		!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
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

			if (IsPostgresTableWrite(rangeTableEntry->relid))
				RecordPostgresTableWriteInTx(rangeTableEntry->relid);
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

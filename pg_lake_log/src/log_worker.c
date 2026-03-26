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
 * Background worker for pg_lake_log.
 *
 * Periodically drains the shared-memory ring buffer and writes the captured
 * log entries to an Iceberg table.  All entries in a single drain cycle are
 * inserted via InsertBatchToIceberg, which issues one multi-row INSERT per
 * sub-batch so that they all land in the same Parquet file and commit as a
 * single Iceberg snapshot.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/backend_status.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "pg_extension_base/base_workers.h"
#include "pg_lake_log/log_buffer.h"
#include "pg_lake_log/log_flush.h"
#include "pg_lake_log/log_worker.h"

#define WORKER_NAME  "pg_lake_log flush worker"
#define LIBRARY_NAME "pg_lake_log"

/* Forward declaration */
static void FlushBatch(LogEntry *entries, int count);


/*
 * StartLogFlushWorker registers the background worker during _PG_init.
 */
void
StartLogFlushWorker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 10;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;

	strlcpy(worker.bgw_library_name, LIBRARY_NAME, sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_name, WORKER_NAME, sizeof(worker.bgw_name));
	strlcpy(worker.bgw_function_name, "LogFlushWorkerMain",
			sizeof(worker.bgw_function_name));

	RegisterBackgroundWorker(&worker);
}


/*
 * LogFlushWorkerMain is the entry point for the background worker.
 */
void
LogFlushWorkerMain(Datum arg)
{
	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	pgstat_report_appname(WORKER_NAME);

	ereport(LOG, (errmsg(WORKER_NAME ": started")));

	BackgroundWorkerInitializeConnection(PgLakeLogDatabase, NULL, 0);

	LogEntry   *batch = (LogEntry *)
		MemoryContextAlloc(TopMemoryContext,
						   sizeof(LogEntry) * PgLakeLogBatchSize);

	while (true)
	{
		int			drained;

		CHECK_FOR_INTERRUPTS();

		if (ReloadRequested)
		{
			ReloadRequested = false;
			ProcessConfigFile(PGC_SIGHUP);

			pfree(batch);
			batch = (LogEntry *)
				MemoryContextAlloc(TopMemoryContext,
								   sizeof(LogEntry) * PgLakeLogBatchSize);
		}

		if (!PgLakeLogEnabled)
		{
			LightSleep(PgLakeLogFlushIntervalMs);
			continue;
		}

		drained = LogBufferDrain(batch, PgLakeLogBatchSize);

		if (drained > 0)
		{
			pgstat_report_activity(STATE_RUNNING, NULL);

			MemoryContext savedContext = CurrentMemoryContext;

			PG_TRY();
			{
				FlushBatch(batch, drained);
			}
			PG_CATCH();
			{
				MemoryContextSwitchTo(savedContext);
				ErrorData  *edata = CopyErrorData();

				FlushErrorState();

				/*
				 * Downgrade to WARNING so the worker stays alive.  Discard
				 * the batch rather than re-queuing to avoid an infinite retry
				 * loop on persistent errors (e.g. table does not exist).
				 */
				ereport(WARNING,
						(errmsg(WORKER_NAME ": error flushing %d entries: %s",
								drained, edata->message)));
				FreeErrorData(edata);
			}
			PG_END_TRY();

			pgstat_report_activity(STATE_IDLE, NULL);
		}

		LightSleep(PgLakeLogFlushIntervalMs);
	}
}


/*
 * FlushBatch wraps InsertBatchToIceberg in a transaction.
 *
 * Reads the set of target tables from lake_log.log_tables and writes the
 * batch to each one.
 */
static void
FlushBatch(LogEntry *entries, int count)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();

	PG_TRY();
	{
		PushActiveSnapshot(GetTransactionSnapshot());

		List	   *tableOids = GetLogTableOids();
		ListCell   *lc;

		foreach(lc, tableOids)
			InsertBatchToIceberg(entries, count, lfirst_oid(lc));

		if (ActiveSnapshotSet())
			PopActiveSnapshot();

		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		AbortCurrentTransaction();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

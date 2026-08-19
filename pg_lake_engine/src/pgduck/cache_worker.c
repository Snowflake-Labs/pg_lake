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
#include "miscadmin.h"

#include "access/xact.h"
#include "libpq/libpq-be.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/backend_status.h"
#include "utils/memutils.h"
#include "pg_lake/pgduck/cache_worker.h"
#include "pg_lake/pgduck/client.h"

#include "pg_extension_base/base_workers.h"

#define BACKGROUND_WORKER_NAME "pg_lake cache worker"
#define LIBRARY_NAME "pg_lake_engine"


typedef enum ManageCacheResult
{
	MANAGE_CACHE_SUCCESS,
	MANAGE_CACHE_ERROR,
	MANAGE_CACHE_DISABLED
}			ManageCacheResult;


/* background worker entry point */
PGDLLEXPORT void PgLakeCacheWorkerMain(Datum arg);

/* main logic */
static ManageCacheResult ManageCache(void);


/* flags set by signal handlers */
volatile sig_atomic_t ReloadRequested = false;
volatile sig_atomic_t TerminationRequested = false;

/* pg_lake_engine.enable_cache_manager setting */
bool		EnableCacheManager = true;

/* pg_lake_engine.max_cache_size_mb setting */
int			MaxCacheSizeMB = MAX_CACHE_SIZE_MB_DEFAULT;

/* pg_lake_engine.cache_manager_interval setting */
int			CacheManagerIntervalMs = CACHE_MANAGER_INTERVAL_MS_DEFAULT;

/*
 * StartPGDuckCacheWorker registers a server-level background worker
 * that periodically calls pg_lake_manage_cache on pgduck.
 */
void
StartPGDuckCacheWorker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* seconds */
	worker.bgw_restart_time = 5;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;

	/* make sure we change this if we change the library name */
	strlcpy(worker.bgw_library_name, LIBRARY_NAME,
			sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_name, BACKGROUND_WORKER_NAME,
			sizeof(worker.bgw_name));
	strlcpy(worker.bgw_function_name, "PgLakeCacheWorkerMain",
			sizeof(worker.bgw_function_name));

	RegisterBackgroundWorker(&worker);
}


void
PgLakeCacheWorkerMain(Datum arg)
{
	/* set up signal handlers */
	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);

	/* allow signals */
	BackgroundWorkerUnblockSignals();

	/* connect only to shared catalogs */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/* report application_name in pg_stat_activity */
	pgstat_report_appname(BACKGROUND_WORKER_NAME);

	/* set up a memory context that resets every iteration */
	MemoryContext loopContext = AllocSetContextCreate(CurrentMemoryContext,
													  BACKGROUND_WORKER_NAME,
													  ALLOCSET_DEFAULT_MINSIZE,
													  ALLOCSET_DEFAULT_INITSIZE,
													  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(loopContext);

	ereport(LOG, (errmsg(BACKGROUND_WORKER_NAME " started")));

	bool		isCachingEnabled = true;

	while (true)
	{
		if (EnableCacheManager)
		{
			pgstat_report_activity(STATE_RUNNING, NULL);

			/* run cache management */
			ManageCacheResult result = ManageCache();

			pgstat_report_activity(STATE_IDLE, NULL);

			if (result == MANAGE_CACHE_DISABLED)
			{
				/* emit a log message once, when caching is disabled */
				if (isCachingEnabled)
				{
					ereport(WARNING, (errmsg(BACKGROUND_WORKER_NAME ": file cache is "
											 "currently disabled")));
				}

				isCachingEnabled = false;
			}
			else
			{
				if (!isCachingEnabled)
				{
					ereport(LOG, (errmsg(BACKGROUND_WORKER_NAME ": file cache is "
										 "re-enabled")));
				}

				isCachingEnabled = true;
			}
		}

		/* clean up any allocated memory */
		MemoryContextReset(loopContext);

		/*
		 * If caching is enabled, we manage cache every 10 seconds to quickly
		 * react when the user starts running queries.
		 *
		 * Otherwise, we wait longer, maybe an operator comes in and fixes it.
		 */
		int			delay = CacheManagerIntervalMs;

		if (isCachingEnabled)
			delay = Max(delay, 60000);

		LightSleep(delay);
	}
}


/*
 * ManageCache manages the cache by calling into pg_lake_manage_cache and
 * returns whether it was successful.
 */
static ManageCacheResult
ManageCache(void)
{
	MemoryContext savedContext = CurrentMemoryContext;
	StringInfoData manageCacheCommand;

	initStringInfo(&manageCacheCommand);

	int64		maxCacheSizeBytes = ((int64) MaxCacheSizeMB) * 1024 * 1024;

	Assert(maxCacheSizeBytes >= 0);

	appendStringInfo(&manageCacheCommand,
					 "CALL pg_lake_manage_cache(" INT64_FORMAT ")",
					 maxCacheSizeBytes);

	volatile	ManageCacheResult manageCacheResult;

	/*
	 * We start a transaction primarily to go through the transaction
	 * callbacks in client.c, which will send a cancellation in case of an
	 * error (incl. process death, SIGINT).
	 */
	StartTransactionCommand();

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result =
		ExecuteQueryOnPGDuckConnection(pgDuckConn, manageCacheCommand.data);

	PG_TRY();
	{
		/*
		 * Use ThrowIfPGDuckResultHasError to avoid PQclear on error, since we
		 * already call it below.
		 */
		ThrowIfPGDuckResultHasError(pgDuckConn, result);

		int			rowCount = PQntuples(result);
		int			columnCount = PQnfields(result);

		if (columnCount < 3)
			ereport(ERROR, (errmsg("unexpected column count %d", columnCount)));

		for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			char	   *url = PQgetvalue(result, rowIndex, 0);
			char	   *fileSizeStr = PQgetvalue(result, rowIndex, 1);
			char	   *action = PQgetvalue(result, rowIndex, 2);

			if (strcmp(action, "added") == 0)
			{
				ereport(LOG, (errmsg(BACKGROUND_WORKER_NAME ": "
									 "added %s (%s bytes) to cache",
									 url, fileSizeStr)));
			}
			else if (strcmp(action, "removed") == 0)
			{
				ereport(LOG, (errmsg(BACKGROUND_WORKER_NAME ": "
									 "removed %s (%s bytes) from cache",
									 url, fileSizeStr)));
			}
			else
			{
				/*
				 * We do not log skipped actions, since they might cause
				 * excessive noise if a large file is read frequently
				 */
			}
		}

		manageCacheResult = MANAGE_CACHE_SUCCESS;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		if (strstr(edata->message, "caching is currently disabled") != NULL)
		{
			manageCacheResult = MANAGE_CACHE_DISABLED;
		}
		else
		{
			manageCacheResult = MANAGE_CACHE_ERROR;

			/* rethrow as WARNING */
			edata->elevel = WARNING;
			ThrowErrorData(edata);
		}
	}
	PG_END_TRY();

	PQclear(result);
	ReleasePGDuckConnection(pgDuckConn);
	CommitTransactionCommand();
	MemoryContextSwitchTo(savedContext);

	return manageCacheResult;
}

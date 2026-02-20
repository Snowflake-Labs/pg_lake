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
 * job_scheduler.c
 *
 * A job scheduler that uses attached workers to execute SQL commands
 * from a job queue table. A base worker polls job_scheduler.jobs for
 * pending jobs and launches attached workers to run them, up to a
 * configurable concurrency limit.
 *
 * The main loop follows the pg_cron pattern: the scheduler runs outside
 * any transaction, opens short-lived transactions only to read/write
 * the jobs table, and maintains running-job state in a persistent
 * memory context. A loop context is reset each iteration to prevent
 * memory leaks.
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "pg_extension_base/attached_worker.h"
#include "pg_extension_base/base_workers.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_job_scheduler/job_scheduler.h"

#define GUC_STANDARD 0
#define JOB_SCHEDULER_SCHEMA "job_scheduler"
#define JOB_SCHEDULER_SLEEP_MS 1000

/* columns returned by the claim query */
#define CLAIM_COL_JOB_ID	1
#define CLAIM_COL_COMMAND	2
#define CLAIM_COL_DATABASE	3
#define CLAIM_COL_USERNAME	4

/* columns returned by the list query */
#define LIST_COL_COUNT		10

PG_MODULE_MAGIC;

/* GUC variable */
int			JobSchedulerMaxWorkers = 4;

/* function declarations */
void		_PG_init(void);

/* in-memory state for a running job */
typedef struct RunningJob
{
	int64		jobId;			/* hash key */
	AttachedWorker *worker;
	char	   *lastCommandTag;
}			RunningJob;

/* info about a finished job, collected during scan for deferred processing */
typedef struct FinishedJob
{
	int64		jobId;
	AttachedWorker *worker;
	char	   *lastCommandTag;
	char	   *errorMessage;
}			FinishedJob;

/* claimed job info copied out of SPI context */
typedef struct ClaimedJob
{
	int64		jobId;
	char	   *command;
	char	   *databaseName;
	char	   *userName;
}			ClaimedJob;


PG_FUNCTION_INFO_V1(pg_job_scheduler_main);
PG_FUNCTION_INFO_V1(pg_job_scheduler_submit_job);
PG_FUNCTION_INFO_V1(pg_job_scheduler_list_jobs);


/*
 * _PG_init is the entry-point for pg_job_scheduler which is called on
 * postmaster start-up when pg_job_scheduler is in shared_preload_libraries.
 */
void
_PG_init(void)
{
	DefineCustomIntVariable(
							 "pg_job_scheduler.max_workers",
							 gettext_noop("Maximum number of concurrent job scheduler workers"),
							 NULL,
							 &JobSchedulerMaxWorkers,
							 4,
							 1,
							 64,
							 PGC_SIGHUP,
							 GUC_STANDARD,
							 NULL, NULL, NULL);
}


/*
 * ResetOrphanedJobs marks any jobs left in 'running' state as 'pending'.
 * This handles the case where the scheduler crashed while jobs were in flight.
 */
static void
ResetOrphanedJobs(void)
{
	START_TRANSACTION();
	{
		SPI_connect();

		SPI_execute("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
					"SET status = 'pending', started_at = NULL "
					"WHERE status = 'running'",
					false, 0);

		if (SPI_processed > 0)
			elog(LOG, "job scheduler: reset %lu orphaned job(s) to pending",
				 (unsigned long) SPI_processed);

		SPI_finish();
	}
	END_TRANSACTION();
}


/*
 * ClaimPendingJobs claims up to maxJobs pending jobs by atomically
 * setting their status to 'running'. Returns a list of ClaimedJob
 * structs allocated in CurrentMemoryContext.
 */
static List *
ClaimPendingJobs(int maxJobs, MemoryContext resultContext)
{
	List	   *claimedJobs = NIL;

	if (maxJobs <= 0)
		return NIL;

	START_TRANSACTION();
	{
		SPI_connect();

		DECLARE_SPI_ARGS(1);

		SPI_ARG_DATUM(1, INT4OID, Int32GetDatum(maxJobs));
		SPI_EXECUTE("SELECT job_id, command, database_name, user_name "
					"FROM " JOB_SCHEDULER_SCHEMA ".jobs "
					"WHERE status = 'pending' "
					"ORDER BY job_id "
					"FOR UPDATE SKIP LOCKED "
					"LIMIT $1",
					false);

		for (uint64 i = 0; i < SPI_processed; i++)
		{
			bool		isNull = false;
			int64		jobId = DatumGetInt64(GET_SPI_DATUM(i, CLAIM_COL_JOB_ID, &isNull));
			char	   *command = TextDatumGetCString(GET_SPI_DATUM(i, CLAIM_COL_COMMAND, &isNull));
			char	   *dbName = TextDatumGetCString(GET_SPI_DATUM(i, CLAIM_COL_DATABASE, &isNull));
			char	   *userName = TextDatumGetCString(GET_SPI_DATUM(i, CLAIM_COL_USERNAME, &isNull));

			MemoryContext spiContext = MemoryContextSwitchTo(resultContext);

			ClaimedJob *job = palloc(sizeof(ClaimedJob));

			job->jobId = jobId;
			job->command = pstrdup(command);
			job->databaseName = pstrdup(dbName);
			job->userName = pstrdup(userName);
			claimedJobs = lappend(claimedJobs, job);

			MemoryContextSwitchTo(spiContext);
		}

		/* mark claimed jobs as running */
		if (SPI_processed > 0)
		{
			DECLARE_SPI_ARGS(1);

			ListCell   *lc;

			foreach(lc, claimedJobs)
			{
				ClaimedJob *job = (ClaimedJob *) lfirst(lc);

				SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(job->jobId));
				SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
							"SET status = 'running', started_at = now() "
							"WHERE job_id = $1",
							false);
			}
		}

		SPI_finish();
	}
	END_TRANSACTION();

	return claimedJobs;
}


/*
 * MarkJobCompleted updates a job's status to 'completed' with the
 * given command tag as result.
 */
static void
MarkJobCompleted(int64 jobId, char *commandTag)
{
	START_TRANSACTION();
	{
		SPI_connect();

		DECLARE_SPI_ARGS(2);

		SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(jobId));
		SPI_ARG_VALUE(2, TEXTOID, commandTag, commandTag == NULL);

		SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
					"SET status = 'completed', completed_at = now(), result = $2 "
					"WHERE job_id = $1",
					false);

		SPI_finish();
	}
	END_TRANSACTION();
}


/*
 * MarkJobFailed updates a job's status to 'failed' with the given
 * error message.
 */
static void
MarkJobFailed(int64 jobId, char *errorMessage)
{
	START_TRANSACTION();
	{
		SPI_connect();

		DECLARE_SPI_ARGS(2);

		SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(jobId));
		SPI_ARG_VALUE(2, TEXTOID, errorMessage, errorMessage == NULL);

		SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
					"SET status = 'failed', completed_at = now(), "
					"error_message = $2 "
					"WHERE job_id = $1",
					false);

		SPI_finish();
	}
	END_TRANSACTION();
}


/*
 * DrainWorkerMessages reads all available messages from an attached
 * worker without blocking. Captures the last command tag seen and
 * any error that is thrown.
 *
 * Returns true if an error was caught (stored in *errorMessage).
 */
static bool
DrainWorkerMessages(RunningJob *job, MemoryContext persistentContext,
					char **errorMessage)
{
	*errorMessage = NULL;

	PG_TRY();
	{
		for (;;)
		{
			char	   *tag = ReadFromAttachedWorker(job->worker, false);

			if (tag == NULL)
				break;

			MemoryContext oldContext = MemoryContextSwitchTo(persistentContext);

			if (job->lastCommandTag != NULL)
				pfree(job->lastCommandTag);
			job->lastCommandTag = pstrdup(tag);

			MemoryContextSwitchTo(oldContext);
		}
	}
	PG_CATCH();
	{
		MemoryContext oldContext = MemoryContextSwitchTo(persistentContext);
		ErrorData  *edata = CopyErrorData();

		*errorMessage = pstrdup(edata->message);

		MemoryContextSwitchTo(oldContext);
		FreeErrorData(edata);
		FlushErrorState();

		return true;
	}
	PG_END_TRY();

	return false;
}


/*
 * pg_job_scheduler_main is the base worker entry point
 * for the job scheduler.
 */
Datum
pg_job_scheduler_main(PG_FUNCTION_ARGS)
{
	int32		workerId = PG_GETARG_INT32(0);

	elog(LOG, "job scheduler started (worker %d, max workers %d)",
		 workerId, JobSchedulerMaxWorkers);

	/* persistent context for the running jobs hash and worker state */
	MemoryContext schedulerContext = AllocSetContextCreate(CurrentMemoryContext,
														  "job scheduler context",
														  ALLOCSET_DEFAULT_MINSIZE,
														  ALLOCSET_DEFAULT_INITSIZE,
														  ALLOCSET_DEFAULT_MAXSIZE);

	/* per-iteration context that gets reset each loop */
	MemoryContext loopContext = AllocSetContextCreate(CurrentMemoryContext,
													 "job scheduler loop context",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

	/* create running jobs hash in the persistent context */
	HASHCTL		hashInfo;

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(int64);
	hashInfo.entrysize = sizeof(RunningJob);
	hashInfo.hash = tag_hash;
	hashInfo.hcxt = schedulerContext;

	HTAB	   *runningJobs = hash_create("job scheduler running jobs", 32,
										 &hashInfo,
										 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/* crash recovery: reset any orphaned running jobs */
	ResetOrphanedJobs();

	MemoryContextSwitchTo(loopContext);

	while (!TerminationRequested)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Step 1: poll running workers for completion.
		 *
		 * We must not start transactions during the hash scan because
		 * CommitTransactionCommand terminates active hash scans. So we
		 * collect finished jobs during the scan and process them after.
		 */
		HASH_SEQ_STATUS hashStatus;
		RunningJob *entry;
		List	   *finishedJobs = NIL;

		hash_seq_init(&hashStatus, runningJobs);

		while ((entry = (RunningJob *) hash_seq_search(&hashStatus)) != NULL)
		{
			char	   *errorMessage = NULL;
			bool		hadError = DrainWorkerMessages(entry, schedulerContext,
													   &errorMessage);

			if (hadError || !IsAttachedWorkerRunning(entry->worker))
			{
				if (!hadError)
				{
					/* worker finished normally — do a final drain */
					DrainWorkerMessages(entry, schedulerContext, &errorMessage);
				}

				/* save info for deferred processing */
				MemoryContext oldCtx = MemoryContextSwitchTo(schedulerContext);
				FinishedJob *fj = palloc(sizeof(FinishedJob));

				fj->jobId = entry->jobId;
				fj->worker = entry->worker;
				fj->lastCommandTag = entry->lastCommandTag;
				fj->errorMessage = errorMessage;
				finishedJobs = lappend(finishedJobs, fj);
				MemoryContextSwitchTo(oldCtx);
			}
		}

		/* process finished jobs now that the hash scan is complete */
		{
			ListCell   *lc;

			foreach(lc, finishedJobs)
			{
				FinishedJob *fj = (FinishedJob *) lfirst(lc);

				EndAttachedWorker(fj->worker);

				if (fj->errorMessage != NULL)
				{
					MarkJobFailed(fj->jobId, fj->errorMessage);
					pfree(fj->errorMessage);
				}
				else
				{
					MarkJobCompleted(fj->jobId, fj->lastCommandTag);
				}

				if (fj->lastCommandTag != NULL)
					pfree(fj->lastCommandTag);

				hash_search(runningJobs, &fj->jobId, HASH_REMOVE, NULL);
				pfree(fj);
			}
		}

		/* Step 2: claim pending jobs if we have available slots */
		int			runningCount = hash_get_num_entries(runningJobs);
		int			availableSlots = JobSchedulerMaxWorkers - runningCount;

		if (availableSlots > 0)
		{
			List	   *claimed = ClaimPendingJobs(availableSlots, loopContext);
			ListCell   *lc;

			/* Step 3: launch attached workers outside the transaction */
			foreach(lc, claimed)
			{
				ClaimedJob *job = (ClaimedJob *) lfirst(lc);
				bool		found;

				MemoryContextSwitchTo(schedulerContext);
				RunningJob *runEntry = hash_search(runningJobs, &job->jobId,
												   HASH_ENTER, &found);

				runEntry->lastCommandTag = NULL;

				PG_TRY();
				{
					runEntry->worker = StartAttachedWorkerInDatabase(
																	 job->command,
																	 job->databaseName,
																	 job->userName);
				}
				PG_CATCH();
				{
					ErrorData  *edata;

					MemoryContextSwitchTo(schedulerContext);
					edata = CopyErrorData();
					FlushErrorState();

					hash_search(runningJobs, &job->jobId, HASH_REMOVE, NULL);

					elog(LOG, "job scheduler: failed to launch worker for job %ld: %s",
						 (long) job->jobId, edata->message);

					MarkJobFailed(job->jobId, edata->message);
					FreeErrorData(edata);
				}
				PG_END_TRY();

				MemoryContextSwitchTo(loopContext);
			}
		}

		/* Step 4: sleep, wake on signals */
		LightSleep(JOB_SCHEDULER_SLEEP_MS);

		MemoryContextReset(loopContext);
	}

	/* clean shutdown: terminate any still-running workers */
	{
		HASH_SEQ_STATUS hashStatus;
		RunningJob *entry;

		hash_seq_init(&hashStatus, runningJobs);

		while ((entry = (RunningJob *) hash_seq_search(&hashStatus)) != NULL)
		{
			EndAttachedWorker(entry->worker);
			MarkJobFailed(entry->jobId, "job scheduler shutting down");
		}
	}

	elog(LOG, "job scheduler shutting down");

	PG_RETURN_VOID();
}


/*
 * pg_job_scheduler_submit_job inserts a new job into the queue and
 * returns the job_id.
 */
Datum
pg_job_scheduler_submit_job(PG_FUNCTION_ARGS)
{
	char	   *command = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *databaseName = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *userName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	SPI_START();

	DECLARE_SPI_ARGS(3);

	SPI_ARG_DATUM(1, TEXTOID, CStringGetTextDatum(command));
	SPI_ARG_DATUM(2, TEXTOID, CStringGetTextDatum(databaseName));
	SPI_ARG_DATUM(3, TEXTOID, CStringGetTextDatum(userName));

	SPI_EXECUTE("INSERT INTO " JOB_SCHEDULER_SCHEMA ".jobs "
				"(command, database_name, user_name) "
				"VALUES ($1, $2, $3) "
				"RETURNING job_id",
				false);

	if (SPI_processed != 1)
		ereport(ERROR, (errmsg("failed to insert job")));

	bool		isNull = false;
	int64		jobId = DatumGetInt64(GET_SPI_DATUM(0, 1, &isNull));

	SPI_END();

	PG_RETURN_INT64(jobId);
}


/*
 * pg_job_scheduler_list_jobs returns all jobs from the queue table.
 */
Datum
pg_job_scheduler_list_jobs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	SPI_START();

	SPI_execute("SELECT job_id, command, database_name, user_name, status, "
				"created_at, started_at, completed_at, result, error_message "
				"FROM " JOB_SCHEDULER_SCHEMA ".jobs "
				"ORDER BY job_id",
				true, 0);

	for (uint64 i = 0; i < SPI_processed; i++)
	{
		Datum		values[LIST_COL_COUNT];
		bool		nulls[LIST_COL_COUNT];

		for (int col = 0; col < LIST_COL_COUNT; col++)
		{
			values[col] = GET_SPI_DATUM(i, col + 1, &nulls[col]);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	SPI_END();

	PG_RETURN_VOID();
}

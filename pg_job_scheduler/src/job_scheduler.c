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
 * A job scheduler that uses attached workers to execute SQL commands from a
 * job queue. Two tables back it: job_scheduler.jobs holds the definitions
 * (what to run and when to run it next) and job_scheduler.job_runs holds one
 * row per execution.
 *
 * A base worker polls for jobs whose next_run_at has fallen due and launches
 * attached workers to run them, up to a configurable concurrency limit. A job
 * with no schedule runs once; a job with schedule_interval or schedule_cron
 * has its next_run_at advanced when a run starts, so it recurs.
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
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "pg_extension_base/attached_worker.h"
#include "pg_extension_base/base_workers.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_job_scheduler/cron_schedule.h"
#include "pg_job_scheduler/job_scheduler.h"

#define GUC_STANDARD 0

/*
 * How long the main loop sleeps between passes, when nothing wakes it sooner.
 *
 * A running job does wake us: we are the receiver of its attached worker's
 * message queue, and shm_mq sets the receiver's latch when the sender writes,
 * so LightSleep returns as soon as a worker reports anything. That is what
 * keeps throughput up -- a backlog of quick jobs drains at a couple of hundred
 * a second with only four workers, rather than four per pass.
 *
 * What does not wake us is a job being *submitted*: nothing signals a base
 * worker, since pg_extension_base exports no way for a backend to reach one
 * (MyBaseWorkerId is only set inside the worker itself). So this interval is
 * the upper bound on how long a job submitted to run now waits before it
 * starts, measured at about a second. That is the reason the sleep is a flat
 * poll rather than derived from the earliest next_run_at, tempting though the
 * latter is when the next job is hours away: sleeping to the deadline needs a
 * way to be woken by a submit first.
 */
#define JOB_SCHEDULER_SLEEP_MS 1000

/* default pg_job_scheduler.run_retention: keep a week of run history */
#define DEFAULT_RUN_RETENTION_SEC (7 * 24 * 60 * 60)

/*
 * How many expired runs one pass may delete. Retention runs on every pass
 * rather than on a timer of its own, which is affordable because the scan is an
 * ordered index scan that finds nothing the moment there is nothing to delete.
 * The cap is what keeps a first sweep over a long-neglected history from doing
 * it all in a single transaction; it spreads over passes instead.
 */
#define JOB_SCHEDULER_RETENTION_BATCH 1000

/* columns returned by the claim query */
#define CLAIM_COL_JOB_ID	1
#define CLAIM_COL_COMMAND	2
#define CLAIM_COL_DATABASE	3
#define CLAIM_COL_USERNAME	4
#define CLAIM_COL_INTERVAL	5
#define CLAIM_COL_CRON		6
#define CLAIM_COL_ATOMIC	7

/* columns returned by the list queries */
#define LIST_JOBS_COL_COUNT 12
#define LIST_JOB_RUNS_COL_COUNT 10

PG_MODULE_MAGIC;

/* GUC variables */
int			JobSchedulerMaxWorkers = 4;
int			JobSchedulerRunRetentionSec = DEFAULT_RUN_RETENTION_SEC;

/* function declarations */
void		_PG_init(void);

/* in-memory state for a running job */
typedef struct RunningJob
{
	int64		jobId;			/* hash key */
	int64		runId;
	AttachedWorker *worker;
	char	   *lastCommandTag;
}			RunningJob;

/* info about a finished run, collected during scan for deferred processing */
typedef struct FinishedRun
{
	int64		jobId;
	int64		runId;
	AttachedWorker *worker;
	char	   *lastCommandTag;
	char	   *errorMessage;
}			FinishedRun;

/* claimed job info copied out of SPI context */
typedef struct ClaimedJob
{
	int64		jobId;
	int64		runId;
	char	   *command;
	char	   *databaseName;
	char	   *userName;

	/* whether the job records its own outcome; see run_job.c */
	bool		atomic;

	/* when this job should run again, computed before any of the writes */
	TimestampTz nextRunAt;
	bool		nextRunIsNull;
}			ClaimedJob;


PG_FUNCTION_INFO_V1(pg_job_scheduler_main);
PG_FUNCTION_INFO_V1(pg_job_scheduler_submit_job);
PG_FUNCTION_INFO_V1(pg_job_scheduler_list_jobs);
PG_FUNCTION_INFO_V1(pg_job_scheduler_list_job_runs);


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

	DefineCustomIntVariable(
							"pg_job_scheduler.run_retention",
							gettext_noop("How long to keep the record of a finished job run"),
							gettext_noop("Runs that completed longer ago than this are deleted. "
										 "A negative value keeps every run forever, which leaves "
										 "job_scheduler.job_runs to grow without bound."),
							&JobSchedulerRunRetentionSec,
							DEFAULT_RUN_RETENTION_SEC,
							-1,
							INT32_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	InitializeJobSchedulerIdCache();
}


/*
 * ResetOrphanedRuns cleans up after a scheduler that died with runs in
 * flight. Their attached workers went down with it, so the runs are marked
 * failed, and any one-shot job whose only run was orphaned is made due again
 * rather than being silently lost.
 */
static void
ResetOrphanedRuns(void)
{
	START_TRANSACTION();
	{
		SPI_connect();

		SPI_execute("UPDATE " JOB_SCHEDULER_SCHEMA ".job_runs "
					"SET status = 'failed', completed_at = now(), "
					"error_message = 'the job scheduler restarted while this "
					"run was in progress' "
					"WHERE status = 'running'",
					false, 0);

		if (SPI_processed > 0)
			elog(LOG, "job scheduler: failed %lu orphaned run(s)",
				 (unsigned long) SPI_processed);

		/*
		 * A one-shot job clears next_run_at when it is claimed and only moves
		 * off 'active' when its run finishes, so an active one-shot with no
		 * next_run_at is exactly one whose run we just orphaned.
		 *
		 * Retrying it is only safe when the job was atomic. Such a job
		 * deletes its own definition in the same transaction as its work, so
		 * a definition that is still here proves the work did not commit, and
		 * running it again cannot duplicate anything. That is what makes an
		 * atomic one-shot run exactly once.
		 */
		SPI_execute("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
					"SET next_run_at = now() "
					"WHERE status = 'active' AND next_run_at IS NULL "
					"AND schedule_interval IS NULL AND schedule_cron IS NULL "
					"AND atomic",
					false, 0);

		/*
		 * A non-atomic one-shot gets no such proof: its command may well have
		 * committed before we died, with nothing to say so. Retrying could
		 * run it twice, so it is failed instead. That is the weaker bargain
		 * such a job accepted by opting out.
		 */
		SPI_execute("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
					"SET status = 'failed' "
					"WHERE status = 'active' AND next_run_at IS NULL "
					"AND schedule_interval IS NULL AND schedule_cron IS NULL "
					"AND NOT atomic",
					false, 0);

		SPI_finish();
	}
	END_TRANSACTION();
}


/*
 * PurgeExpiredRuns deletes run history that has aged past
 * pg_job_scheduler.run_retention, oldest first and at most
 * JOB_SCHEDULER_RETENTION_BATCH rows per call.
 *
 * Only finished runs are eligible: a run still in flight has a NULL
 * completed_at, which no "completed_at < cutoff" scan will ever match, so an
 * in-flight run cannot be deleted out from under its worker however long it
 * has been going.
 */
static void
PurgeExpiredRuns(void)
{
	if (JobSchedulerRunRetentionSec < 0)
		return;

	TimestampTz cutoff = GetCurrentTimestamp() -
		(int64) JobSchedulerRunRetentionSec * USECS_PER_SEC;

	START_TRANSACTION();
	{
		SPI_connect();

		DECLARE_SPI_ARGS(2);

		SPI_ARG_DATUM(1, TIMESTAMPTZOID, TimestampTzGetDatum(cutoff));
		SPI_ARG_DATUM(2, INT4OID, Int32GetDatum(JOB_SCHEDULER_RETENTION_BATCH));

		SPI_EXECUTE("WITH expired AS ("
					"  SELECT run_id FROM " JOB_SCHEDULER_SCHEMA ".job_runs "
					"  WHERE completed_at < $1 "
					"  ORDER BY completed_at "
					"  LIMIT $2) "
					"DELETE FROM " JOB_SCHEDULER_SCHEMA ".job_runs "
					"WHERE run_id IN (SELECT run_id FROM expired)",
					false);

		if (SPI_processed > 0)
			elog(DEBUG1, "job scheduler: deleted %lu expired run(s)",
				 (unsigned long) SPI_processed);

		SPI_finish();
	}
	END_TRANSACTION();
}


/*
 * NextRunAfter computes when a job should next run, given its schedule and the
 * time the current run is starting. A job with no schedule reports NULL
 * through *isNull and never runs again.
 *
 * The calculation lives here rather than in the claim SQL so that an
 * unevaluatable schedule fails one job instead of aborting the whole claim
 * transaction. Neither the cron code nor interval arithmetic touches the
 * database, so the caller can catch an error from this and carry on.
 */
static TimestampTz
NextRunAfter(Datum scheduleInterval, bool hasInterval, char *scheduleCron,
			 TimestampTz startTime, bool *isNull)
{
	*isNull = false;

	if (hasInterval)
		return DatumGetTimestampTz(
								   DirectFunctionCall2(timestamptz_pl_interval,
													   TimestampTzGetDatum(startTime),
													   scheduleInterval));

	if (scheduleCron != NULL)
	{
		CronSchedule schedule;

		ParseCronSchedule(scheduleCron, &schedule);

		return CronScheduleNextRun(&schedule, startTime);
	}

	*isNull = true;
	return 0;
}


/*
 * ClaimDueJobs finds up to maxJobs jobs whose next_run_at has fallen due, opens
 * a run for each, and advances each job's next_run_at. It returns the claimed
 * jobs as ClaimedJob structs allocated in resultContext.
 *
 * All of that happens in one transaction, while FOR UPDATE SKIP LOCKED still
 * holds the rows. Splitting the claim across two transactions would let two
 * schedulers -- briefly possible while a base worker is being relaunched --
 * each select the same job and each open a run for it, since neither would see
 * the other's uncommitted run row.
 *
 * Jobs whose schedule could not be evaluated are not claimed; their ids are
 * appended to *unschedulableJobIds for the caller to fail outside this
 * transaction, so one unusable schedule cannot stall the rest of the queue.
 */
static List *
ClaimDueJobs(int maxJobs, MemoryContext resultContext,
			 List **unschedulableJobIds)
{
	List	   *claimedJobs = NIL;

	*unschedulableJobIds = NIL;

	if (maxJobs <= 0)
		return NIL;

	START_TRANSACTION();
	{
		SPI_connect();

		{
			DECLARE_SPI_ARGS(1);

			SPI_ARG_DATUM(1, INT4OID, Int32GetDatum(maxJobs));

			/*
			 * The NOT EXISTS keeps a recurring job from overlapping itself
			 * when a run outlives its own interval: the next run is skipped
			 * rather than started alongside the one still going.
			 */
			SPI_EXECUTE("SELECT job_id, command, database_name, user_name, "
						"schedule_interval, schedule_cron, atomic "
						"FROM " JOB_SCHEDULER_SCHEMA ".jobs "
						"WHERE status = 'active' AND next_run_at <= now() "
						"AND NOT EXISTS ("
						"  SELECT 1 FROM " JOB_SCHEDULER_SCHEMA ".job_runs "
						"  WHERE job_id = jobs.job_id AND status = 'running') "
						"ORDER BY next_run_at "
						"FOR UPDATE SKIP LOCKED "
						"LIMIT $1",
						false);
		}

		uint64		dueCount = SPI_processed;
		TimestampTz startTime = GetCurrentTimestamp();

		/*
		 * Copy every due row out before writing anything. Each SPI_EXECUTE
		 * below replaces SPI_tuptable, so reading the claim columns after the
		 * first insert would read them out of that insert's result instead.
		 */
		for (uint64 rowIndex = 0; rowIndex < dueCount; rowIndex++)
		{
			bool		isNull = false;
			int64		jobId = DatumGetInt64(GET_SPI_DATUM(rowIndex, CLAIM_COL_JOB_ID, &isNull));
			char	   *command = TextDatumGetCString(GET_SPI_DATUM(rowIndex, CLAIM_COL_COMMAND, &isNull));
			char	   *dbName = TextDatumGetCString(GET_SPI_DATUM(rowIndex, CLAIM_COL_DATABASE, &isNull));
			char	   *userName = TextDatumGetCString(GET_SPI_DATUM(rowIndex, CLAIM_COL_USERNAME, &isNull));
			bool		atomic = DatumGetBool(GET_SPI_DATUM(rowIndex, CLAIM_COL_ATOMIC, &isNull));

			bool		intervalIsNull = false;
			Datum		scheduleInterval = GET_SPI_DATUM(rowIndex, CLAIM_COL_INTERVAL,
														 &intervalIsNull);

			bool		cronIsNull = false;
			Datum		cronDatum = GET_SPI_DATUM(rowIndex, CLAIM_COL_CRON, &cronIsNull);
			char	   *scheduleCron = cronIsNull ? NULL : TextDatumGetCString(cronDatum);

			/*
			 * next_run_at advances from now() rather than from the old
			 * next_run_at, so a scheduler that was down for a while does not
			 * come back to a backlog of missed runs to catch up on.
			 *
			 * This is computed here, while the schedule columns are still to
			 * hand and before any writes, precisely so that a schedule we
			 * cannot evaluate costs one job rather than the whole claim
			 * transaction.
			 */
			TimestampTz nextRunAt = 0;
			bool		nextRunIsNull = false;
			bool		scheduleFailed = false;

			PG_TRY();
			{
				nextRunAt = NextRunAfter(scheduleInterval, !intervalIsNull,
										 scheduleCron, startTime, &nextRunIsNull);
			}
			PG_CATCH();
			{
				FlushErrorState();
				scheduleFailed = true;
			}
			PG_END_TRY();

			MemoryContext spiContext = MemoryContextSwitchTo(resultContext);

			if (scheduleFailed)
			{
				*unschedulableJobIds = lappend_int(*unschedulableJobIds, (int) jobId);
			}
			else
			{
				ClaimedJob *job = palloc0(sizeof(ClaimedJob));

				job->jobId = jobId;
				job->command = pstrdup(command);
				job->databaseName = pstrdup(dbName);
				job->userName = pstrdup(userName);
				job->atomic = atomic;
				job->nextRunAt = nextRunAt;
				job->nextRunIsNull = nextRunIsNull;
				claimedJobs = lappend(claimedJobs, job);
			}

			MemoryContextSwitchTo(spiContext);
		}

		/* now open a run for each and advance its schedule */
		ListCell   *claimedCell;

		foreach(claimedCell, claimedJobs)
		{
			ClaimedJob *job = (ClaimedJob *) lfirst(claimedCell);

			{
				DECLARE_SPI_ARGS(4);

				SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(job->jobId));
				SPI_ARG_DATUM(2, TEXTOID, CStringGetTextDatum(job->command));
				SPI_ARG_DATUM(3, TEXTOID, CStringGetTextDatum(job->databaseName));
				SPI_ARG_DATUM(4, TEXTOID, CStringGetTextDatum(job->userName));

				/*
				 * The run carries its own copy of what it ran, because a
				 * one-shot job deletes its definition when it succeeds.
				 */
				SPI_EXECUTE("INSERT INTO " JOB_SCHEDULER_SCHEMA ".job_runs "
							"(job_id, command, database_name, user_name) "
							"VALUES ($1, $2, $3, $4) RETURNING run_id",
							false);

				if (SPI_processed != 1)
					ereport(ERROR, (errmsg("failed to open a run for job %ld",
										   (long) job->jobId)));

				bool		runIdIsNull = false;

				job->runId = DatumGetInt64(GET_SPI_DATUM(0, 1, &runIdIsNull));
			}

			{
				DECLARE_SPI_ARGS(2);

				SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(job->jobId));

				if (job->nextRunIsNull)
				{
					SPI_ARG_NULL(2, TIMESTAMPTZOID);
				}
				else
				{
					SPI_ARG_DATUM(2, TIMESTAMPTZOID, TimestampTzGetDatum(job->nextRunAt));
				}

				SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
							"SET next_run_at = $2 WHERE job_id = $1",
							false);
			}
		}

		SPI_finish();
	}
	END_TRANSACTION();

	return claimedJobs;
}


/*
 * FailJobDefinition marks a job itself as failed, for the case where the job
 * could not be started at all. Without this the job would stay due forever
 * and the scheduler would retry it on every iteration.
 */
static void
FailJobDefinition(int64 jobId)
{
	START_TRANSACTION();
	{
		SPI_connect();

		DECLARE_SPI_ARGS(1);

		SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(jobId));
		SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
					"SET status = 'failed', next_run_at = NULL "
					"WHERE job_id = $1",
					false);

		SPI_finish();
	}
	END_TRANSACTION();
}


/*
 * FinishRun records the outcome of one run, and for a one-shot job also
 * closes out the definition. A recurring job stays 'active' whatever its
 * individual runs do, so there is always somewhere for the next run to go.
 *
 * An atomic job has already recorded its own success inside its transaction,
 * and deleted its definition if it was a one-shot. Both statements below are
 * written so that they are no-ops in that case rather than contradicting what
 * already committed: the run is only updated while it is still 'running', and
 * the definition is only updated if the row is still there.
 */
static void
FinishRun(int64 runId, int64 jobId, bool succeeded, char *commandTag,
		  char *errorMessage)
{
	START_TRANSACTION();
	{
		SPI_connect();

		{
			DECLARE_SPI_ARGS(4);

			SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(runId));
			SPI_ARG_DATUM(2, BOOLOID, BoolGetDatum(succeeded));
			SPI_ARG_VALUE(3, TEXTOID, commandTag, commandTag == NULL);
			SPI_ARG_VALUE(4, TEXTOID, errorMessage, errorMessage == NULL);

			SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".job_runs "
						"SET status = CASE WHEN $2 THEN 'succeeded' ELSE 'failed' END, "
						"completed_at = now(), result = $3, error_message = $4 "
						"WHERE run_id = $1 AND status = 'running'",
						false);
		}

		{
			DECLARE_SPI_ARGS(2);

			SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(jobId));
			SPI_ARG_DATUM(2, BOOLOID, BoolGetDatum(succeeded));

			SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".jobs "
						"SET status = CASE WHEN $2 THEN 'completed' ELSE 'failed' END "
						"WHERE job_id = $1 "
						"AND schedule_interval IS NULL AND schedule_cron IS NULL",
						false);
		}

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
DrainWorkerMessages(RunningJob * job, MemoryContext persistentContext,
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

	/* crash recovery: close out runs left behind by a dead scheduler */
	ResetOrphanedRuns();

	MemoryContextSwitchTo(loopContext);

	while (!TerminationRequested)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Step 1: poll running workers for completion.
		 *
		 * We must not start transactions during the hash scan because
		 * CommitTransactionCommand terminates active hash scans. So we
		 * collect finished runs during the scan and process them after.
		 */
		HASH_SEQ_STATUS hashStatus;
		RunningJob *entry;
		List	   *finishedRuns = NIL;

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
				MemoryContext oldContext = MemoryContextSwitchTo(schedulerContext);
				FinishedRun *finished = palloc(sizeof(FinishedRun));

				finished->jobId = entry->jobId;
				finished->runId = entry->runId;
				finished->worker = entry->worker;
				finished->lastCommandTag = entry->lastCommandTag;
				finished->errorMessage = errorMessage;
				finishedRuns = lappend(finishedRuns, finished);
				MemoryContextSwitchTo(oldContext);
			}
		}

		/* process finished runs now that the hash scan is complete */
		{
			ListCell   *finishedCell;

			foreach(finishedCell, finishedRuns)
			{
				FinishedRun *finished = (FinishedRun *) lfirst(finishedCell);

				EndAttachedWorker(finished->worker);

				FinishRun(finished->runId, finished->jobId,
						  finished->errorMessage == NULL,
						  finished->lastCommandTag,
						  finished->errorMessage);

				if (finished->errorMessage != NULL)
					pfree(finished->errorMessage);

				if (finished->lastCommandTag != NULL)
					pfree(finished->lastCommandTag);

				hash_search(runningJobs, &finished->jobId, HASH_REMOVE, NULL);
				pfree(finished);
			}
		}

		/* Step 2: claim due jobs if we have available slots */
		int			runningCount = hash_get_num_entries(runningJobs);
		int			availableSlots = JobSchedulerMaxWorkers - runningCount;
		List	   *unschedulableJobIds = NIL;

		if (availableSlots > 0)
		{
			List	   *claimedJobs = ClaimDueJobs(availableSlots, loopContext,
												   &unschedulableJobIds);
			ListCell   *claimedCell;
			ListCell   *unschedulableCell;

			/* a schedule we cannot evaluate would otherwise stay due forever */
			foreach(unschedulableCell, unschedulableJobIds)
			{
				int64		jobId = (int64) lfirst_int(unschedulableCell);

				elog(LOG, "job scheduler: job %ld has a schedule that cannot be "
					 "evaluated; marking it failed", (long) jobId);

				FailJobDefinition(jobId);
			}

			/* Step 3: launch attached workers outside the transaction */
			foreach(claimedCell, claimedJobs)
			{
				ClaimedJob *job = (ClaimedJob *) lfirst(claimedCell);
				bool		found;

				MemoryContextSwitchTo(schedulerContext);
				RunningJob *runEntry = hash_search(runningJobs, &job->jobId,
												   HASH_ENTER, &found);

				runEntry->runId = job->runId;
				runEntry->lastCommandTag = NULL;

				/*
				 * An atomic job runs through run_job() so that its command
				 * and the record of it commit together; see run_job.c.
				 * Anything else runs its command directly and has its outcome
				 * recorded afterwards by FinishRun.
				 */
				char	   *workerCommand = job->atomic
					? psprintf("SELECT %s.run_job(" INT64_FORMAT ", " INT64_FORMAT ")",
							   JOB_SCHEDULER_SCHEMA, job->jobId, job->runId)
					: job->command;

				PG_TRY();
				{
					runEntry->worker = StartAttachedWorkerInDatabase(
																	 workerCommand,
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

					FinishRun(job->runId, job->jobId, false, NULL, edata->message);
					FreeErrorData(edata);
				}
				PG_END_TRY();

				MemoryContextSwitchTo(loopContext);
			}
		}

		/* Step 4: age out run history that has outlived its retention */
		PurgeExpiredRuns();

		/* Step 5: sleep, wake on signals */
		MemoryContextReset(loopContext);

		LightSleep(JOB_SCHEDULER_SLEEP_MS);
	}

	/* clean shutdown: terminate any still-running workers */
	{
		HASH_SEQ_STATUS hashStatus;
		RunningJob *entry;

		hash_seq_init(&hashStatus, runningJobs);

		while ((entry = (RunningJob *) hash_seq_search(&hashStatus)) != NULL)
		{
			EndAttachedWorker(entry->worker);
			FinishRun(entry->runId, entry->jobId, false, NULL,
					  "job scheduler shutting down");
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
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("command cannot be null")));

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("database name and user name cannot be null")));

	if (PG_ARGISNULL(5))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("atomic cannot be null")));

	SPI_START();

	DECLARE_SPI_ARGS(6);

	SPI_ARG_DATUM(1, TEXTOID, PG_GETARG_DATUM(0));
	SPI_ARG_DATUM(2, TEXTOID, PG_GETARG_DATUM(1));
	SPI_ARG_DATUM(3, TEXTOID, PG_GETARG_DATUM(2));

	if (PG_ARGISNULL(3))
	{
		SPI_ARG_NULL(4, INTERVALOID);
	}
	else
	{
		SPI_ARG_DATUM(4, INTERVALOID, PG_GETARG_DATUM(3));
	}

	if (PG_ARGISNULL(4))
	{
		SPI_ARG_NULL(5, TEXTOID);
	}
	else
	{
		SPI_ARG_DATUM(5, TEXTOID, PG_GETARG_DATUM(4));
	}

	SPI_ARG_DATUM(6, BOOLOID, PG_GETARG_DATUM(5));

	/*
	 * A cron job starts at its first matching time, which also validates the
	 * expression here rather than leaving behind a job that can never be
	 * claimed. Everything else is due immediately.
	 */
	SPI_EXECUTE("INSERT INTO " JOB_SCHEDULER_SCHEMA ".jobs "
				"(command, database_name, user_name, schedule_interval, "
				" schedule_cron, atomic, next_run_at) "
				"VALUES ($1, $2, $3, $4, $5, $6, "
				"        CASE WHEN $5 IS NOT NULL "
				"               THEN " JOB_SCHEDULER_SCHEMA ".next_cron_run($5, now()) "
				"             ELSE now() END) "
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
 * pg_job_scheduler_list_jobs returns all job definitions.
 */
Datum
pg_job_scheduler_list_jobs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	SPI_START();

	/*
	 * last_run_status and last_run_at are derived rather than stored, so they
	 * cannot drift from job_runs. They exist because status answers a
	 * different question: for a recurring job it stays 'active' however its
	 * runs go, so it is the wrong column to look at to find out whether the
	 * job is working.
	 */
	SPI_execute("SELECT jobs.job_id, jobs.command, jobs.database_name, "
				"jobs.user_name, jobs.schedule_interval, jobs.schedule_cron, "
				"jobs.atomic, jobs.status, last_run.status, last_run.started_at, "
				"jobs.next_run_at, jobs.created_at "
				"FROM " JOB_SCHEDULER_SCHEMA ".jobs "
				"LEFT JOIN LATERAL ("
				"  SELECT status, started_at FROM " JOB_SCHEDULER_SCHEMA ".job_runs "
				"  WHERE job_runs.job_id = jobs.job_id "
				"  ORDER BY run_id DESC LIMIT 1) AS last_run ON true "
				"ORDER BY jobs.job_id",
				true, 0);

	for (uint64 rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		Datum		values[LIST_JOBS_COL_COUNT];
		bool		nulls[LIST_JOBS_COL_COUNT];

		for (int column = 0; column < LIST_JOBS_COL_COUNT; column++)
		{
			values[column] = GET_SPI_DATUM(rowIndex, column + 1, &nulls[column]);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	SPI_END();

	PG_RETURN_VOID();
}


/*
 * pg_job_scheduler_list_job_runs returns the run history, optionally
 * restricted to a single job.
 */
Datum
pg_job_scheduler_list_job_runs(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	SPI_START();

	DECLARE_SPI_ARGS(1);

	if (PG_ARGISNULL(0))
	{
		SPI_ARG_NULL(1, INT8OID);
	}
	else
	{
		SPI_ARG_DATUM(1, INT8OID, PG_GETARG_DATUM(0));
	}

	SPI_EXECUTE("SELECT run_id, job_id, command, database_name, user_name, "
				"status, started_at, completed_at, result, error_message "
				"FROM " JOB_SCHEDULER_SCHEMA ".job_runs "
				"WHERE $1 IS NULL OR job_id = $1 "
				"ORDER BY run_id",
				true);

	for (uint64 rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		Datum		values[LIST_JOB_RUNS_COL_COUNT];
		bool		nulls[LIST_JOB_RUNS_COL_COUNT];

		for (int column = 0; column < LIST_JOB_RUNS_COL_COUNT; column++)
		{
			values[column] = GET_SPI_DATUM(rowIndex, column + 1, &nulls[column]);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	SPI_END();

	PG_RETURN_VOID();
}

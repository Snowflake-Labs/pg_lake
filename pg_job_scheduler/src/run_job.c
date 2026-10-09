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
 * run_job.c
 *
 * job_scheduler.run_job() runs a claimed job's command and records the outcome
 * in the same transaction, which is what lets a one-shot job run exactly once.
 *
 * The attached worker calls this as a single statement, so everything below
 * happens inside the one transaction the worker opened. Either the command's
 * effects, the run's 'succeeded' row and the deletion of the definition all
 * commit, or none of them do. A crash therefore cannot leave work committed
 * with no record of it, and the surviving definition is proof the work did not
 * happen, so the scheduler can safely run it again.
 *
 * The command runs as the job's user_name, which is the identity the worker
 * connected with and therefore the identity calling this function. Only the
 * bookkeeping is elevated, and only to the extension owner, because the job's
 * user has no privileges on our tables. The command string is never executed
 * with elevated privileges, and is never taken from the caller: it is read from
 * the job row named by the arguments.
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "executor/spi.h"
#include "tcop/cmdtag.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_job_scheduler/job_scheduler.h"

/* columns returned by the authorization query */
#define JOB_COL_COMMAND		1
#define JOB_COL_USER_NAME	2
#define JOB_COL_IS_ONE_SHOT 3

CachedExtensionIds *PgJobScheduler = NULL;

PG_FUNCTION_INFO_V1(pg_job_scheduler_run_job);


/*
 * InitializeJobSchedulerIdCache sets up extension ID caching for
 * pg_job_scheduler, so that ExtensionOwnerId() can be resolved cheaply.
 */
void
InitializeJobSchedulerIdCache(void)
{
	PgJobScheduler = CreateExtensionIdsCache(PG_JOB_SCHEDULER_NAME, NULL, NULL);
}


/*
 * AuthorizeRun checks that the given run really is one the scheduler opened
 * for the given job, that the job is meant to run atomically, and that the
 * caller is the job's own user. On success it returns the command to run and
 * reports through *isOneShot whether the definition should be deleted when it
 * completes.
 *
 * This runs as the extension owner, since the caller cannot read our tables.
 */
static char *
AuthorizeRun(int64 jobId, int64 runId, bool *isOneShot)
{
	char	   *command = NULL;
	char	   *userName = NULL;

	/* SPI_END frees the SPI context, so results are copied back into this one */
	MemoryContext callerContext = CurrentMemoryContext;

	SPI_START_EXTENSION_OWNER(PgJobScheduler);

	DECLARE_SPI_ARGS(2);

	SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(jobId));
	SPI_ARG_DATUM(2, INT8OID, Int64GetDatum(runId));

	/*
	 * Joining the run to the job is the authorization: a run that is not
	 * 'running', or belongs to a different job, is not one we were asked to
	 * perform. Together with the user check below this leaves a caller who
	 * invents arguments able to do nothing.
	 */
	SPI_EXECUTE("SELECT jobs.command, jobs.user_name, "
				"jobs.schedule_interval IS NULL AND jobs.schedule_cron IS NULL "
				"FROM " JOB_SCHEDULER_SCHEMA ".jobs "
				"JOIN " JOB_SCHEDULER_SCHEMA ".job_runs "
				"  ON job_runs.job_id = jobs.job_id "
				"WHERE jobs.job_id = $1 AND job_runs.run_id = $2 "
				"AND job_runs.status = 'running' AND jobs.atomic",
				true);

	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no atomic job %ld with a run %ld in progress",
						(long) jobId, (long) runId),
				 errhint("job_scheduler.run_job is called by the job scheduler "
						 "for a run it has opened; it is not meant to be "
						 "called directly.")));

	bool		isNull = false;

	command = TextDatumGetCString(GET_SPI_DATUM(0, JOB_COL_COMMAND, &isNull));
	userName = TextDatumGetCString(GET_SPI_DATUM(0, JOB_COL_USER_NAME, &isNull));
	*isOneShot = DatumGetBool(GET_SPI_DATUM(0, JOB_COL_IS_ONE_SHOT, &isNull));

	/*
	 * Copy out before SPI_END, which frees the SPI context. GetUserId() below
	 * is the caller's own identity: this function is not SECURITY DEFINER,
	 * and the elevation above is scoped to the query.
	 */
	MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

	command = pstrdup(command);
	userName = pstrdup(userName);

	MemoryContextSwitchTo(spiContext);

	SPI_END();

	/*
	 * Refuse to run one user's command as another. Without this, any user
	 * able to call run_job could execute somebody else's job body as
	 * themselves, which at best confuses the audit trail and at worst runs a
	 * command the job's owner wrote against the caller's own privileges.
	 */
	Oid			jobUserId = get_role_oid(userName, true);

	if (jobUserId != GetUserId())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("job %ld runs as \"%s\", not as \"%s\"",
						(long) jobId, userName, GetUserNameFromId(GetUserId(), false))));

	return command;
}


/*
 * CommandTagForCommand returns the command tag the given SQL string would
 * report, as a client would see it: the tag of the last statement, with a row
 * count where that tag carries one.
 *
 * The worker's own CommandComplete now describes the run_job() call rather than
 * the job's command, so the tag stored in job_runs.result has to be built here.
 */
static char *
CommandTagForCommand(const char *command, uint64 rowCount)
{
	List	   *parseTreeList = pg_parse_query(command);

	if (parseTreeList == NIL)
		return NULL;

	RawStmt    *lastStatement = (RawStmt *) llast(parseTreeList);
	QueryCompletion queryCompletion;

	SetQueryCompletion(&queryCompletion, CreateCommandTag(lastStatement->stmt),
					   rowCount);

	char	   *tagBuffer = palloc0(COMPLETION_TAG_BUFSIZE);

	BuildQueryCompletionString(tagBuffer, &queryCompletion, false);

	return tagBuffer;
}


/*
 * RecordRunSucceeded marks the run as succeeded and, for a one-shot job,
 * deletes the definition. Runs as the extension owner.
 *
 * Deleting the definition is what makes the guarantee: after this commits
 * there is no job left to claim, and before it commits there is no trace of
 * the work at all.
 */
static void
RecordRunSucceeded(int64 jobId, int64 runId, bool isOneShot, char *commandTag)
{
	SPI_START_EXTENSION_OWNER(PgJobScheduler);

	{
		DECLARE_SPI_ARGS(2);

		SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(runId));
		SPI_ARG_VALUE(2, TEXTOID, commandTag, commandTag == NULL);

		SPI_EXECUTE("UPDATE " JOB_SCHEDULER_SCHEMA ".job_runs "
					"SET status = 'succeeded', completed_at = now(), result = $2 "
					"WHERE run_id = $1",
					false);
	}

	if (isOneShot)
	{
		DECLARE_SPI_ARGS(1);

		SPI_ARG_DATUM(1, INT8OID, Int64GetDatum(jobId));

		SPI_EXECUTE("DELETE FROM " JOB_SCHEDULER_SCHEMA ".jobs "
					"WHERE job_id = $1",
					false);
	}

	SPI_END();
}


/*
 * pg_job_scheduler_run_job runs a claimed job and records that it ran, both in
 * the caller's transaction.
 */
Datum
pg_job_scheduler_run_job(PG_FUNCTION_ARGS)
{
	int64		jobId = PG_GETARG_INT64(0);
	int64		runId = PG_GETARG_INT64(1);
	bool		isOneShot = false;

	char	   *command = AuthorizeRun(jobId, runId, &isOneShot);

	/*
	 * Run the command as the caller, with no elevation and no restricted
	 * context: this is the job body, and it should behave as though the user
	 * had typed it. A statement that cannot run inside a transaction block
	 * fails here, which is the whole reason a job can opt out with atomic =
	 * false.
	 */
	SPI_connect();

	int			spiStatus = SPI_execute(command, false, 0);

	if (spiStatus < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("job command failed with SPI status %d", spiStatus)));

	uint64		rowCount = SPI_processed;

	SPI_finish();

	char	   *commandTag = CommandTagForCommand(command, rowCount);

	RecordRunSucceeded(jobId, runId, isOneShot, commandTag);

	PG_RETURN_VOID();
}

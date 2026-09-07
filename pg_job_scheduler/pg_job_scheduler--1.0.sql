CREATE SCHEMA job_scheduler;

/*
 * A job runs as its own user_name, and that user has to be able to call
 * job_scheduler.run_job, so the schema itself is reachable by anyone. Nothing
 * in it is granted by default: the tables hold command text, which can carry
 * anything the submitter put there, and every function below is revoked from
 * public except run_job, which authorizes its caller itself.
 */
GRANT USAGE ON SCHEMA job_scheduler TO public;

/*
 * A job definition: what to run, and when to run it next.
 *
 * A job is either a one-shot (both schedule columns NULL) or recurring, in
 * which case exactly one of schedule_interval / schedule_cron says how often.
 */
CREATE TABLE job_scheduler.jobs (
	job_id bigserial
		CONSTRAINT jobs_pk PRIMARY KEY,

	command text NOT NULL,
	database_name text NOT NULL DEFAULT current_database(),
	user_name text NOT NULL DEFAULT current_user,

	/* fixed delay between runs, measured from the start of each run */
	schedule_interval interval,

	/* 5-field cron expression, evaluated in the server's TimeZone */
	schedule_cron text,

	/*
	 * Whether to run the command and record its outcome in one transaction, so
	 * that a crash can never leave work committed but unrecorded. This is what
	 * makes a one-shot job run exactly once: the definition is deleted in the
	 * same transaction as the work, so a job that is still here provably has
	 * not run, and retrying it is safe.
	 *
	 * The cost is that the command must be able to run inside a transaction
	 * block. VACUUM, CREATE DATABASE, CREATE INDEX CONCURRENTLY and friends
	 * cannot, and neither can anything doing its own transaction control, so
	 * such a job needs atomic = false and gets the weaker guarantee: its
	 * outcome is recorded separately afterwards, and a crash in between means
	 * it is never retried rather than possibly run twice.
	 */
	atomic boolean NOT NULL DEFAULT true,

	/*
	 * Lifecycle of the definition: whether the scheduler should still consider
	 * this job at all. This is *not* the outcome of any run.
	 *
	 * For a recurring job it stays 'active' no matter how its runs go -- every
	 * run failing does not make the job 'failed', because the schedule is still
	 * live and the next run is still due. So this is the wrong column to read to
	 * find out whether a recurring job is actually working; read the last run's
	 * status for that, which job_scheduler.list_jobs() reports as
	 * last_run_status alongside last_run_at.
	 *
	 * 'completed' and 'failed' are only reachable for a one-shot job, and
	 * 'completed' only for a non-atomic one: an atomic one-shot deletes its
	 * definition when it succeeds, so success is the absence of a row.
	 */
	status text NOT NULL DEFAULT 'active'
		CONSTRAINT valid_status CHECK (status IN ('active', 'paused', 'completed', 'failed')),

	/*
	 * When the scheduler should start the next run. NULL means never again,
	 * which is where a one-shot job lands once it has been claimed. A job is
	 * due when status = 'active' AND next_run_at <= now(), so due-ness is
	 * derived rather than stored.
	 */
	next_run_at timestamptz,

	created_at timestamptz NOT NULL DEFAULT now(),

	CONSTRAINT one_schedule CHECK (num_nonnulls(schedule_interval, schedule_cron) <= 1),
	CONSTRAINT positive_interval CHECK (schedule_interval IS NULL
										OR schedule_interval > interval '0')
);

/* the scheduler's claim query looks for due active jobs */
CREATE INDEX jobs_next_run_at_idx ON job_scheduler.jobs (next_run_at)
	WHERE status = 'active';

ALTER TABLE job_scheduler.jobs REPLICA IDENTITY FULL;


/*
 * One row per execution of a job. Recurring jobs accumulate one row per run,
 * so this is the high-churn table of the two.
 *
 * There is deliberately no foreign key to job_scheduler.jobs. A one-shot job
 * deletes its own definition when it succeeds, which is what makes it run
 * exactly once, so the run is the durable record and has to outlive the
 * definition. It therefore carries its own copy of what it ran.
 */
CREATE TABLE job_scheduler.job_runs (
	run_id bigserial
		CONSTRAINT job_runs_pk PRIMARY KEY,

	/* the job this ran for; the definition may since have been deleted */
	job_id bigint NOT NULL,

	/* what ran, copied from the definition so this row stands alone */
	command text NOT NULL,
	database_name text NOT NULL,
	user_name text NOT NULL,

	/* outcome of this single execution */
	status text NOT NULL DEFAULT 'running'
		CONSTRAINT valid_status CHECK (status IN ('running', 'succeeded', 'failed')),

	started_at timestamptz NOT NULL DEFAULT now(),
	completed_at timestamptz,

	/* command tag of the last statement, e.g. 'SELECT 1' */
	result text,
	error_message text
);

/* list_job_runs filters on this */
CREATE INDEX job_runs_job_id_idx ON job_scheduler.job_runs (job_id);

/*
 * The claim query asks "does this job already have a run in flight" on every
 * pass, once a second. Without an index restricted to those runs, that becomes
 * a sequential scan of the whole history looking for the handful of 'running'
 * rows: measured at 78 ms per pass against 500k runs, and growing with every
 * run ever recorded. With it the same query is 0.1 ms, and the index stays tiny
 * because at most pg_job_scheduler.max_workers rows are ever in it (16 kB
 * against 8 MB for job_runs_job_id_idx over the same history).
 */
CREATE INDEX job_runs_running_idx ON job_scheduler.job_runs (job_id)
	WHERE status = 'running';

ALTER TABLE job_scheduler.job_runs REPLICA IDENTITY FULL;

/* run history churns fast enough to want its own analyze thresholds */
ALTER TABLE job_scheduler.job_runs SET (
	autovacuum_analyze_scale_factor = 0.05,
	autovacuum_analyze_threshold = 500
);


/*
 * next_cron_run returns the first time a cron expression matches strictly
 * after from_time. The scheduler uses it to advance next_run_at, and
 * submit_job uses it to validate an expression at insert time.
 */
CREATE FUNCTION job_scheduler.next_cron_run(schedule text, from_time timestamptz DEFAULT now())
 RETURNS timestamptz
 LANGUAGE c STRICT STABLE
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_next_cron_run$function$;

COMMENT ON FUNCTION job_scheduler.next_cron_run(text, timestamptz)
 IS 'compute the next time a cron expression matches after from_time';

REVOKE ALL ON FUNCTION job_scheduler.next_cron_run(text, timestamptz) FROM public;

/*
 * run_job executes a job's command and records the outcome in the caller's
 * transaction, so that the two cannot disagree. The job scheduler's attached
 * worker calls this instead of the command itself when the job is atomic; it
 * is not meant to be called by hand, and it authorizes its caller rather than
 * trusting its arguments.
 */
CREATE FUNCTION job_scheduler.run_job(job_id bigint, run_id bigint)
 RETURNS void
 LANGUAGE c STRICT
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_run_job$function$;

COMMENT ON FUNCTION job_scheduler.run_job(bigint, bigint)
 IS 'run a claimed job and record its outcome in the same transaction';

/*
 * Callable by anyone, because a job runs as its own user_name and that user is
 * the one that has to call this. It is safe because it takes no command: it
 * reads the command from the job it was given, and refuses unless the caller
 * is that job's user_name and the run is one the scheduler has just opened.
 */
REVOKE ALL ON FUNCTION job_scheduler.run_job(bigint, bigint) FROM public;
GRANT EXECUTE ON FUNCTION job_scheduler.run_job(bigint, bigint) TO public;

/* submit a job to the queue */
CREATE FUNCTION job_scheduler.submit_job(command text,
										 database_name text DEFAULT current_database(),
										 user_name text DEFAULT current_user,
										 schedule_interval interval DEFAULT NULL,
										 schedule_cron text DEFAULT NULL,
										 atomic boolean DEFAULT true)
 RETURNS bigint
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_submit_job$function$;

COMMENT ON FUNCTION job_scheduler.submit_job(text, text, text, interval, text, boolean)
 IS 'submit a job to the job scheduler queue';

REVOKE ALL ON FUNCTION job_scheduler.submit_job(text, text, text, interval, text, boolean)
 FROM public;

/* list all job definitions */
CREATE FUNCTION job_scheduler.list_jobs(
	OUT job_id bigint, OUT command text, OUT database_name text,
	OUT user_name text, OUT schedule_interval interval, OUT schedule_cron text,
	OUT atomic boolean, OUT status text,
	OUT last_run_status text, OUT last_run_at timestamptz,
	OUT next_run_at timestamptz, OUT created_at timestamptz)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_list_jobs$function$;

COMMENT ON FUNCTION job_scheduler.list_jobs()
 IS 'list all job definitions in the job scheduler queue';

REVOKE ALL ON FUNCTION job_scheduler.list_jobs() FROM public;

/* list job runs, optionally for a single job */
CREATE FUNCTION job_scheduler.list_job_runs(
	for_job_id bigint DEFAULT NULL,
	OUT run_id bigint, OUT job_id bigint, OUT command text,
	OUT database_name text, OUT user_name text, OUT status text,
	OUT started_at timestamptz, OUT completed_at timestamptz,
	OUT result text, OUT error_message text)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_list_job_runs$function$;

COMMENT ON FUNCTION job_scheduler.list_job_runs(bigint)
 IS 'list job runs, most recent last, optionally filtered to one job';

REVOKE ALL ON FUNCTION job_scheduler.list_job_runs(bigint) FROM public;

/* job scheduler base worker entry point */
CREATE FUNCTION job_scheduler.main(internal)
 RETURNS internal
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_main$function$;

COMMENT ON FUNCTION job_scheduler.main(internal)
 IS 'main entry point for the job scheduler base worker';

REVOKE ALL ON FUNCTION job_scheduler.main(internal) FROM public;

/* register the job scheduler base worker */
SELECT extension_base.register_worker(
	'pg_job_scheduler',
	'job_scheduler.main');

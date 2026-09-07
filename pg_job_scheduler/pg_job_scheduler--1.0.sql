CREATE SCHEMA job_scheduler;

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
	 * Lifecycle of the definition, not the outcome of any one run: whether the
	 * scheduler should still consider this job at all. 'completed' and 'failed'
	 * are only reachable for a one-shot job, since a recurring job is never
	 * done. Per-run outcomes live in job_scheduler.job_runs.status.
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

GRANT SELECT ON job_scheduler.jobs TO public;


/*
 * One row per execution of a job. Recurring jobs accumulate one row per run,
 * so this is the high-churn table of the two.
 */
CREATE TABLE job_scheduler.job_runs (
	run_id bigserial
		CONSTRAINT job_runs_pk PRIMARY KEY,

	job_id bigint NOT NULL,

	/* outcome of this single execution */
	status text NOT NULL DEFAULT 'running'
		CONSTRAINT valid_status CHECK (status IN ('running', 'succeeded', 'failed')),

	started_at timestamptz NOT NULL DEFAULT now(),
	completed_at timestamptz,

	/* command tag of the last statement, e.g. 'SELECT 1' */
	result text,
	error_message text,

	/* a run has no meaning without its job */
	CONSTRAINT job_id_fk FOREIGN KEY (job_id)
		REFERENCES job_scheduler.jobs (job_id) ON DELETE CASCADE
);

/* without this, deleting a job rescans the whole history to cascade */
CREATE INDEX job_runs_job_id_idx ON job_scheduler.job_runs (job_id);

ALTER TABLE job_scheduler.job_runs REPLICA IDENTITY FULL;

/* run history churns fast enough to want its own analyze thresholds */
ALTER TABLE job_scheduler.job_runs SET (
	autovacuum_analyze_scale_factor = 0.05,
	autovacuum_analyze_threshold = 500
);

GRANT SELECT ON job_scheduler.job_runs TO public;


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

/* submit a job to the queue */
CREATE FUNCTION job_scheduler.submit_job(command text,
										 database_name text DEFAULT current_database(),
										 user_name text DEFAULT current_user,
										 schedule_interval interval DEFAULT NULL,
										 schedule_cron text DEFAULT NULL)
 RETURNS bigint
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_submit_job$function$;

COMMENT ON FUNCTION job_scheduler.submit_job(text, text, text, interval, text)
 IS 'submit a job to the job scheduler queue';

REVOKE ALL ON FUNCTION job_scheduler.submit_job(text, text, text, interval, text) FROM public;

/* list all job definitions */
CREATE FUNCTION job_scheduler.list_jobs(
	OUT job_id bigint, OUT command text, OUT database_name text,
	OUT user_name text, OUT schedule_interval interval, OUT schedule_cron text,
	OUT status text, OUT next_run_at timestamptz, OUT created_at timestamptz)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_list_jobs$function$;

COMMENT ON FUNCTION job_scheduler.list_jobs()
 IS 'list all job definitions in the job scheduler queue';

REVOKE ALL ON FUNCTION job_scheduler.list_jobs() FROM public;

/* list job runs, optionally for a single job */
CREATE FUNCTION job_scheduler.list_job_runs(
	for_job_id bigint DEFAULT NULL,
	OUT run_id bigint, OUT job_id bigint, OUT status text,
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

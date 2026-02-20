CREATE SCHEMA job_scheduler;

/* job queue table */
CREATE TABLE job_scheduler.jobs (
	job_id bigserial PRIMARY KEY,
	command text NOT NULL,
	database_name text NOT NULL DEFAULT current_database(),
	user_name text NOT NULL DEFAULT current_user,
	status text NOT NULL DEFAULT 'pending'
		CONSTRAINT valid_status CHECK (status IN ('pending', 'running', 'completed', 'failed')),
	created_at timestamptz NOT NULL DEFAULT now(),
	started_at timestamptz,
	completed_at timestamptz,
	result text,
	error_message text
);

/* submit a job to the queue */
CREATE FUNCTION job_scheduler.submit_job(command text, database_name text DEFAULT current_database(), user_name text DEFAULT current_user)
 RETURNS bigint
 LANGUAGE c STRICT
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_submit_job$function$;

COMMENT ON FUNCTION job_scheduler.submit_job(text, text, text)
 IS 'submit a job to the job scheduler queue';

/* list all jobs */
CREATE FUNCTION job_scheduler.list_jobs(
	OUT job_id bigint, OUT command text, OUT database_name text,
	OUT user_name text, OUT status text, OUT created_at timestamptz,
	OUT started_at timestamptz, OUT completed_at timestamptz,
	OUT result text, OUT error_message text)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_job_scheduler_list_jobs$function$;

COMMENT ON FUNCTION job_scheduler.list_jobs()
 IS 'list all jobs in the job scheduler queue';

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

-- Upgrade script for pg_extension_base from 3.4 to 3.5

-- Add the failure_count column to list_base_workers so the exponential restart
-- backoff is observable. Changing the OUT parameters changes the result type,
-- which CREATE OR REPLACE cannot do, so drop and recreate the function.
DROP FUNCTION extension_base.list_base_workers();

CREATE FUNCTION extension_base.list_base_workers(OUT database_id oid, OUT worker_id int, OUT extension_id oid, OUT pid int, OUT needs_restart bool, OUT failure_count int)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_base_workers$function$;

COMMENT ON FUNCTION extension_base.list_base_workers()
 IS 'list all base worker states';

REVOKE ALL ON FUNCTION extension_base.list_base_workers() FROM public;

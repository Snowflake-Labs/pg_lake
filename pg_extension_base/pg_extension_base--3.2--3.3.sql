-- Upgrade script for pg_extension_base from 3.2 to 3.3

/* run a command in a worker and return the query results */
CREATE FUNCTION extension_base.run_attached_returning(command text, dbname text DEFAULT current_database())
 RETURNS SETOF record
 LANGUAGE c STRICT
AS 'MODULE_PATHNAME', $function$pg_extension_base_run_attached_worker_returning$function$;

COMMENT ON FUNCTION extension_base.run_attached_returning(text,text)
 IS 'run a command in a separate attached worker and return the query results';

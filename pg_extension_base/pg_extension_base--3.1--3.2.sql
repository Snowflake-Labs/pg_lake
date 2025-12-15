/* remove a worker from another extension, by id */
CREATE FUNCTION extension_base.deregister_worker(worker_id int)
 RETURNS void
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_deregister_worker$function$;

COMMENT ON FUNCTION extension_base.deregister_worker(int)
 IS 'deregister a base worker';

REVOKE ALL ON FUNCTION extension_base.deregister_worker(int) FROM public;

CREATE SCHEMA extension_base_oneshot;

CREATE FUNCTION extension_base_oneshot.main_worker(internal)
 RETURNS internal
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_test_oneshot_main_worker$function$;
COMMENT ON FUNCTION extension_base_oneshot.main_worker(internal)
    IS 'one-shot entry point for pg_extension_base_test_oneshot';

SELECT extension_base.register_worker('pg_extension_base_test_oneshot_main_worker', 'extension_base_oneshot.main_worker');

CREATE SCHEMA extension_base;

/* list all the extensions that are preloaded */
CREATE FUNCTION extension_base.list_preload_libraries(OUT extension_name text, OUT library_name text)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_preload_libraries$function$;

COMMENT ON FUNCTION extension_base.list_preload_libraries()
 IS 'list all libraries that are preloaded by pg_extension_base';

/* run a command in a worker in the current database as the current user */
CREATE FUNCTION extension_base.run_attached(command text, dbname text DEFAULT current_database(), OUT command_id int, OUT command_tag text)
 RETURNS SETOF record
 LANGUAGE c STRICT
AS 'MODULE_PATHNAME', $function$pg_extension_base_run_attached_worker$function$;

COMMENT ON FUNCTION extension_base.run_attached(text,text)
 IS 'run a command in the a separate attached worker in a database';

 CREATE TABLE extension_base.workers (
	worker_id serial not null
		CONSTRAINT worker_id_unique UNIQUE,

	worker_name text not null
		CONSTRAINT workers_pk PRIMARY KEY
		CONSTRAINT name_length CHECK (char_length(worker_name) <= 255),

	/* we use names here because pg_upgrade does not preserve OIDs */
	extension_name name not null,
	entry_point_schema name not null,
	entry_point_function name not null
);

/* add a worker from another extension */
CREATE FUNCTION extension_base.register_worker(worker_name text, entry_point regproc)
 RETURNS bigint
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_register_worker$function$;

COMMENT ON FUNCTION extension_base.register_worker(text, regproc)
 IS 'register a base worker';

REVOKE ALL ON FUNCTION extension_base.register_worker(text, regproc) FROM public;


/* list all the base workers */
CREATE FUNCTION extension_base.list_base_workers(OUT database_id oid, OUT worker_id int, OUT extension_id oid, OUT pid int, OUT needs_restart bool)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_base_workers$function$;

COMMENT ON FUNCTION extension_base.list_base_workers()
 IS 'list all base worker states';

REVOKE ALL ON FUNCTION extension_base.list_base_workers() FROM public;


/* list all the database starters */
CREATE FUNCTION extension_base.list_database_starters(OUT database_id oid, OUT pid int, OUT needs_restart bool)
 RETURNS SETOF record
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_list_database_starters$function$;

COMMENT ON FUNCTION extension_base.list_database_starters()
 IS 'list all database starter states';

/* remove a worker from another extension */
CREATE FUNCTION extension_base.deregister_worker(worker_name text)
 RETURNS void
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_extension_base_deregister_worker$function$;

COMMENT ON FUNCTION extension_base.deregister_worker(text)
 IS 'deregister a base worker';

REVOKE ALL ON FUNCTION extension_base.deregister_worker(text) FROM public;

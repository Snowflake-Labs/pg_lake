-- Cleanup things that now belong to pg_lake_replication.
--
-- If we do not need a migration path forward, we can just drop these objects
-- here and not care about existing tables in a replication set.

DROP VIEW lake_iceberg.stat_subscription;
DROP VIEW lake_iceberg.stat_subscription_rel;

DROP TABLE crunchy_lake_analytics.iceberg_subscription;
DROP TABLE crunchy_lake_analytics.iceberg_subscription_rel;

DROP FUNCTION crunchy_lake_analytics.append_query_result(table_name regclass, user_id oid, query text);
DROP FUNCTION crunchy_lake_analytics.iceberg_unflushed_insert_count(table_name regclass);
DROP FUNCTION crunchy_lake_analytics.iceberg_unflushed_delete_count(table_name regclass);
DROP FUNCTION __crunchy_writes.replication_apply_trigger();
DROP FUNCTION __crunchy_writes.replication_truncate_trigger();

-- rename roles
DO LANGUAGE plpgsql $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'crunchy_lake_read') THEN
		ALTER ROLE crunchy_lake_read RENAME TO lake_read;
	END IF;
	IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'crunchy_lake_write') THEN
		ALTER ROLE crunchy_lake_write RENAME TO lake_write;
	END IF;
	IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'crunchy_lake_read_write') THEN
		ALTER ROLE crunchy_lake_read_write RENAME TO lake_read_write;
	END IF;
END;
$$;

-- new test function to check if object is created by pg_lake
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.is_object_created_by_lake(regclass,oid)
RETURNS bool
LANGUAGE C
IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $$is_object_created_by_lake$$;
GRANT EXECUTE ON FUNCTION __lake__internal__nsp__.is_object_created_by_lake(regclass,oid) TO public;

-- register back with the correct name
UPDATE extension_base.workers SET entry_point_schema = 'lake_iceberg' WHERE worker_name = 'iceberg vacuum worker';
UPDATE extension_base.workers SET extension_name = 'pg_lake_table' WHERE extension_name = 'crunchy_lake_analytics';

-- rename objects with crunchy reference
ALTER FUNCTION crunchy_lake_analytics_handler RENAME TO pg_lake_table_handler;
ALTER FUNCTION crunchy_lake_analytics_validator RENAME TO pg_lake_table_validator;
ALTER FOREIGN DATA WRAPPER crunchy_lake_analytics RENAME TO pg_lake_table;
ALTER SERVER crunchy_lake_analytics RENAME TO pg_lake;
ALTER SCHEMA crunchy_file_cache RENAME TO lake_file_cache;
ALTER SCHEMA crunchy_lake_analytics RENAME TO lake_table;
ALTER SCHEMA crunchy_lake RENAME TO lake_file;
ALTER FUNCTION lake_iceberg.crunchy_iceberg_am_handler RENAME TO pg_lake_iceberg_am_handler;
ALTER FUNCTION crunchy_iceberg_validator RENAME TO pg_lake_iceberg_validator;
ALTER FOREIGN DATA WRAPPER crunchy_iceberg RENAME TO pg_lake_iceberg;
ALTER SERVER crunchy_iceberg RENAME TO pg_lake_iceberg;
ALTER SCHEMA __crunchy_writes RENAME TO __pg_lake_table_writes;
ALTER FUNCTION lake_file.list_files RENAME TO list;
ALTER FUNCTION lake_file.file_size RENAME TO size;
ALTER FUNCTION lake_file.file_exists RENAME TO exists;
ALTER FUNCTION lake_file.file_preview RENAME TO preview;
ALTER FUNCTION lake_file.delete_file RENAME TO delete;
ALTER TABLE lake_table.data_files RENAME TO files;
ALTER SEQUENCE lake_table.data_files_id_seq RENAME TO files_id_seq;

-- recreate access method (no rename command for access methods)
DROP ACCESS METHOD crunchy_iceberg;
CREATE ACCESS METHOD pg_lake_iceberg TYPE TABLE HANDLER lake_iceberg.pg_lake_iceberg_am_handler;
COMMENT ON ACCESS METHOD pg_lake_iceberg IS 'pg_lake_iceberg table access method, a placeholder for internal usage';
COMMENT ON ACCESS METHOD iceberg IS 'iceberg table access method, alias for pg_lake_iceberg table access method';

-- recreate all C udfs to replace their library name and internal function name
CREATE OR REPLACE FUNCTION pg_lake_table_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_lake_table_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_lake_iceberg_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION lake_file_cache.add(path text, refresh bool default false)
 RETURNS bigint
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_add_file_to_cache$function$;

CREATE OR REPLACE FUNCTION lake_file_cache.remove(path text)
 RETURNS bool
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_remove_file_from_cache$function$;

CREATE OR REPLACE FUNCTION lake_file_cache.list(OUT path text, OUT file_size bigint, OUT last_access_time timestamp)
 RETURNS SETOF record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_list_files_in_cache$function$;

CREATE OR REPLACE FUNCTION lake_file.list(url_wildcard text,
										OUT path text,
										OUT file_size bigint,
										OUT last_modified_time timestamptz,
										OUT etag text)
RETURNS SETOF record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_list_files$function$;

CREATE OR REPLACE FUNCTION lake_file.size(path text)
 RETURNS bigint
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_file_size$function$;

CREATE OR REPLACE FUNCTION lake_file.exists(path text)
 RETURNS boolean
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_file_exists$function$;

/* file_preview() returns a structured description of the source file */
CREATE OR REPLACE FUNCTION lake_file.preview(url text, format text DEFAULT NULL, compression text DEFAULT NULL, out column_name text, out column_type text)
 RETURNS SETOF record
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $function$pg_lake_file_preview$function$;

CREATE OR REPLACE FUNCTION lake_iceberg.pg_lake_iceberg_am_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_iceberg_am_handler$function$;

CREATE OR REPLACE FUNCTION lake_file.delete(url text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_delete_file$function$;

CREATE OR REPLACE FUNCTION lake_iceberg.table_size(table_name regclass)
 RETURNS bigint
 LANGUAGE C
 PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_table_size$function$;

CREATE OR REPLACE PROCEDURE lake_table.finish_postgres_recovery()
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_finish_postgres_recovery$function$;

CREATE OR REPLACE PROCEDURE lake_table.finish_postgres_recovery_in_db()
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_finish_postgres_recovery_in_db$function$;

CREATE OR REPLACE FUNCTION lake_iceberg.vacuum(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'pg_lake_iceberg_vacuum'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION __lake__internal__nsp__.is_object_created_by_lake(regclass,oid)
RETURNS bool
LANGUAGE C
IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $$is_object_created_by_lake$$;

-- Upgrade script for pg_lake_engine from 3.3 to 3.4

-- resolve_metadata rows are deferred-drop entries: "path" is a table's
-- metadata.json, which VACUUM resolves into the exact referenced files to
-- delete, moving the object-store walk off the DROP path.
ALTER TABLE lake_engine.deletion_queue
    ADD COLUMN resolve_metadata bool NOT NULL DEFAULT false;

-- Postgres-semantics wrappers for uuid_extract_timestamp / uuid_extract_version
-- pushdown; queries are rewritten to these and evaluated by the duckdb_pglake
-- implementations, so the local definitions are dummies.
CREATE FUNCTION __lake__internal__nsp__.uuid_extract_timestamp_pg(uuid)
 RETURNS timestamp with time zone
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.uuid_extract_version_pg(uuid)
 RETURNS smallint
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

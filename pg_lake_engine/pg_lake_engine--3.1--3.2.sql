-- Upgrade script for pg_lake_engine from 3.1 to 3.2

CREATE FUNCTION __lake__internal__nsp__.uuid_extract_timestamp_pg(uuid, integer)
 RETURNS timestamp with time zone
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

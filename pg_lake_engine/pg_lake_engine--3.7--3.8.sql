-- Upgrade script for pg_lake_engine from 3.7 to 3.8

-- Postgres-semantics wrappers for encode(bytea, 'base64') and
-- decode(text, 'base64'|'hex') pushdown; queries are rewritten to these and
-- evaluated by the duckdb_pglake implementations, so the local definitions
-- are dummies.
CREATE FUNCTION __lake__internal__nsp__.base64_encode_pg(bytea)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.base64_decode_pg(text)
 RETURNS bytea
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.hex_decode_pg(text)
 RETURNS bytea
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.to_base64(bytea)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.from_base64(text)
 RETURNS bytea
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.to_hex(bytea)
 RETURNS text
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.acosh_pg(double precision)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.atanh_pg(double precision)
 RETURNS double precision
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

CREATE FUNCTION __lake__internal__nsp__.from_hex(text)
 RETURNS bytea
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;

-- Register map types, will be used for parsing DuckDB maps for COPY .. (return_stats)
-- we prefer to create in the extension script to avoid concurrent attempts to create
-- the same map, which may throw errors 
SELECT map_type.create('TEXT','TEXT');
SELECT map_type.create('TEXT','map_type.key_text_val_text');

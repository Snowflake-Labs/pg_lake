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

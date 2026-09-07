/* pg_lake_snowflake--3.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake_snowflake" to load this file. \quit

CREATE FUNCTION pg_lake_snowflake_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pg_lake_snowflake_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

/*
 * Unlike the pre-defined pg_lake servers, every Snowflake account needs its own
 * server (account URL, warehouse, role) and its own user mappings, so we only
 * create the wrapper here.
 */
CREATE FOREIGN DATA WRAPPER snowflake
  HANDLER pg_lake_snowflake_handler
  VALIDATOR pg_lake_snowflake_validator;

GRANT USAGE ON FOREIGN DATA WRAPPER snowflake TO lake_write;

CREATE SCHEMA lake_snowflake;
GRANT USAGE ON SCHEMA lake_snowflake TO lake_read;

/*
 * lake_snowflake.execute runs a single statement on the account behind
 * server_name and returns the first column of the first row, which is what
 * SHOW / DESCRIBE / DDL statements answer with. The caller needs USAGE on the
 * foreign server, and the statement runs with the credentials of the user
 * mapping that applies to the caller.
 */
CREATE FUNCTION lake_snowflake.execute(server_name text, statement text)
 RETURNS text
 LANGUAGE C
 VOLATILE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_snowflake_execute$function$;

COMMENT ON FUNCTION lake_snowflake.execute(text,text) IS 'run a statement on a Snowflake account and return the first column of the first row';
REVOKE ALL ON FUNCTION lake_snowflake.execute(text,text) FROM public;
GRANT EXECUTE ON FUNCTION lake_snowflake.execute(text,text) TO lake_read;

/*
 * lake_snowflake.test_connection is the first thing to run after CREATE SERVER
 * and CREATE USER MAPPING: it proves that the URL, the credentials, the
 * warehouse and the role all work together.
 */
CREATE FUNCTION lake_snowflake.test_connection(server_name text)
 RETURNS text
 LANGUAGE SQL
 VOLATILE STRICT
AS $function$
	SELECT lake_snowflake.execute(server_name, $$
		SELECT 'version ' || CURRENT_VERSION() ||
		       ', account ' || CURRENT_ACCOUNT() ||
		       ', warehouse ' || COALESCE(CURRENT_WAREHOUSE(), '<none>') ||
		       ', role ' || COALESCE(CURRENT_ROLE(), '<none>') ||
		       ', database ' || COALESCE(CURRENT_DATABASE(), '<none>') ||
		       ', schema ' || COALESCE(CURRENT_SCHEMA(), '<none>')
	$$);
$function$;

COMMENT ON FUNCTION lake_snowflake.test_connection(text) IS 'check that a Snowflake foreign server, its credentials and its warehouse work';
REVOKE ALL ON FUNCTION lake_snowflake.test_connection(text) FROM public;
GRANT EXECUTE ON FUNCTION lake_snowflake.test_connection(text) TO lake_read;

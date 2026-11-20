-- Upgrade script for pg_lake_table from 3.7 to 3.8

-- Tables served by an external query engine, a server that speaks the
-- PostgreSQL wire protocol and runs DuckDB SQL (docs/external-query-engines.md).
-- No role gets USAGE on the wrapper, so only superusers create its servers.
CREATE FUNCTION pg_lake_remote_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_lake_remote
  HANDLER pg_lake_table_handler
  VALIDATOR pg_lake_remote_validator;

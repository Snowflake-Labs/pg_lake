-- Upgrade script for pg_lake_table from 3.3 to 3.4

-- DuckLake access methods
CREATE FUNCTION pg_lake_ducklake_am_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME';

CREATE ACCESS METHOD pg_lake_ducklake TYPE TABLE HANDLER pg_lake_ducklake_am_handler;
COMMENT ON ACCESS METHOD pg_lake_ducklake IS 'pg_lake_ducklake table access method';

CREATE ACCESS METHOD ducklake TYPE TABLE HANDLER pg_lake_ducklake_am_handler;
COMMENT ON ACCESS METHOD ducklake IS 'ducklake table access method, alias for pg_lake_ducklake';

/*
 * DuckLake support - similar to iceberg but using DuckLake specification.
 * Uses the same handler as pg_lake_table but has its own validator
 * for DuckLake-specific options.
 */
CREATE FUNCTION pg_lake_ducklake_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_lake_ducklake
  HANDLER pg_lake_table_handler
  VALIDATOR pg_lake_ducklake_validator;

CREATE SERVER pg_lake_ducklake
  FOREIGN DATA WRAPPER pg_lake_ducklake;

GRANT USAGE ON FOREIGN SERVER pg_lake_ducklake TO lake_read_write;

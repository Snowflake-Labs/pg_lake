-- pg_lake_table_test_data_file_hook
--
-- Loading the .so via LOAD or `requires`-cascade installs an _PG_init that
-- registers PgLakeAddDataFileHook to always return true.  That causes
-- pg_lake_table to insert added file IDs into the per-transaction temp
-- table inside SPI_START_EXTENSION_OWNER, which is the codepath this
-- extension exists to exercise.
--
-- The extension also exposes run_iceberg_dml_under_extension_owner so
-- tests can drive an UPDATE/DELETE on an Iceberg foreign table from
-- inside SPI_START_EXTENSION_OWNER, exercising the temp-table create in
-- CreateUpdateTrackingTable.

CREATE FUNCTION run_iceberg_dml_under_extension_owner(query text)
RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'run_iceberg_dml_under_extension_owner';

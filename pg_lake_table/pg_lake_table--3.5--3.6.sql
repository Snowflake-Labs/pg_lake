-- Upgrade script for pg_lake_table from 3.5 to 3.6

-- Both functions only read the catalogs, but they were VOLATILE, so every call
-- took a fresh snapshot: a DROP TABLE committing during the object store catalog
-- export made them return NULL for a table the export query could still see.
CREATE OR REPLACE FUNCTION lake_table.get_table_schema(p_table regclass)
RETURNS text AS $$
BEGIN
  RETURN (
    SELECT n.nspname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_table
  );
END;
$$ LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION lake_table.get_table_name(p_table regclass)
RETURNS text AS $$
BEGIN
  RETURN (
    SELECT c.relname
    FROM pg_class c
    WHERE c.oid = p_table
  );
END;
$$ LANGUAGE plpgsql STABLE;


-- Export the object store catalog independently of autovacuum. The worker
-- restarts after a delay while export is disabled, so it only occupies a
-- process slot when the catalog is enabled.
CREATE FUNCTION lake_iceberg.catalog_export(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'pg_lake_catalog_export_worker'
LANGUAGE C STRICT;

SELECT extension_base.register_worker('pg_lake catalog export worker', 'lake_iceberg.catalog_export');

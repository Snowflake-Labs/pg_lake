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


-- Dedicated catalog export worker, decoupled from the autovacuum cycle.
--
-- The autovacuum worker serializes catalog.json export behind its per-table
-- vacuum stages (compaction, deletion-queue drain, orphan cleanup), so a long
-- vacuum pass can delay the export for minutes.  This worker runs its own
-- tight loop that checks for invalidations every second and pushes a fresh
-- catalog.json whenever one is needed, regardless of what the vacuum worker
-- is doing.
--
-- The worker is NOT registered here.  The autovacuum worker registers it
-- dynamically when it discovers object-store-catalog tables, so databases
-- without such tables never pay for a second worker.

CREATE FUNCTION lake_iceberg.catalog_export(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'pg_lake_catalog_export_worker'
LANGUAGE C STRICT;

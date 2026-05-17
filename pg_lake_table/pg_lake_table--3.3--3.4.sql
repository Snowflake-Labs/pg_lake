-- Upgrade script for pg_lake_table from 3.3 to 3.4

-- Sibling INSTEAD OF trigger on pg_catalog.iceberg_tables that handles
-- the *internal*-catalog half (rows where catalog_name matches the
-- current database). pg_lake_iceberg owns the external-catalog half via
-- its own trigger; both fire on each iceberg_tables write and each
-- no-ops on the half it doesn't own.
--
-- The trigger lives here in pg_lake_table because INSERT and DELETE
-- need to run DDL (CREATE / DROP FOREIGN TABLE), which is pg_lake_table's
-- surface area. UPDATE drives the existing
-- sync_iceberg_metadata_from_external_write code path.
CREATE FUNCTION lake_table.internal_catalog_modification() RETURNS trigger
    AS 'MODULE_PATHNAME', 'internal_catalog_modification'
    LANGUAGE C;

CREATE TRIGGER internal_catalog_modifications_trg
INSTEAD OF INSERT OR UPDATE OR DELETE ON pg_catalog.iceberg_tables
FOR EACH ROW EXECUTE FUNCTION lake_table.internal_catalog_modification();

-- Upgrade script for pg_lake_table from 3.3 to 3.4

-- Sync function for external writes to Iceberg tables.
-- Called by the iceberg_tables INSTEAD OF trigger when an external client
-- updates metadata_location for a table in the current database catalog.
CREATE FUNCTION lake_table.sync_iceberg_metadata_from_external_write(regclass)
    RETURNS void AS 'MODULE_PATHNAME', 'sync_iceberg_metadata_from_external_write'
    LANGUAGE C STRICT;

-- Upgrade script for pg_lake_iceberg from 3.2 to 3.3

-- Set REPLICA IDENTITY FULL for catalog tables without primary keys
-- This is required for logical replication when using 'FOR ALL TABLES' publications
ALTER TABLE lake_iceberg.namespace_properties REPLICA IDENTITY FULL;
ALTER TABLE lake_iceberg.tables_internal REPLICA IDENTITY FULL;
ALTER TABLE lake_iceberg.tables_external REPLICA IDENTITY FULL;

GRANT SELECT ON lake_iceberg.tables TO public;
GRANT SELECT ON lake_iceberg.tables_internal TO public;
GRANT SELECT ON lake_iceberg.tables_external TO public;

CREATE OR REPLACE VIEW pg_catalog.iceberg_tables AS
	SELECT catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location
	FROM lake_iceberg.tables
    WHERE metadata_location IS NOT NULL;

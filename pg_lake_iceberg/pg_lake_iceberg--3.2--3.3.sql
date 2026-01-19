-- Upgrade script for pg_lake_iceberg from 3.2 to 3.3

-- Set REPLICA IDENTITY FULL for catalog tables without primary keys
-- This is required for logical replication when using 'FOR ALL TABLES' publications
ALTER TABLE lake_iceberg.namespace_properties REPLICA IDENTITY FULL;
ALTER TABLE lake_iceberg.tables_internal REPLICA IDENTITY FULL;
ALTER TABLE lake_iceberg.tables_external REPLICA IDENTITY FULL;

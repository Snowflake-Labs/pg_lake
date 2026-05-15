-- Upgrade script for pg_lake_table from 3.2 to 3.3

-- Set REPLICA IDENTITY FULL for catalog tables without primary keys
-- This is required for logical replication when using 'FOR ALL TABLES' publications
ALTER TABLE lake_table.files REPLICA IDENTITY FULL;
ALTER TABLE lake_table.deletion_file_map REPLICA IDENTITY FULL;
ALTER TABLE lake_table.field_id_mappings REPLICA IDENTITY FULL;
ALTER TABLE lake_table.data_file_column_stats REPLICA IDENTITY FULL;
ALTER TABLE lake_table.partition_specs REPLICA IDENTITY FULL;
ALTER TABLE lake_table.partition_fields REPLICA IDENTITY FULL;
ALTER TABLE lake_table.data_file_partition_values REPLICA IDENTITY FULL;

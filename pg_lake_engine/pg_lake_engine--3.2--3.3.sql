-- Upgrade script for pg_lake_engine from 3.2 to 3.3

-- Set REPLICA IDENTITY FULL for catalog tables without primary keys
-- This is required for logical replication when using 'FOR ALL TABLES' publications
ALTER TABLE lake_engine.deletion_queue REPLICA IDENTITY FULL;
ALTER TABLE lake_engine.in_progress_files REPLICA IDENTITY FULL;

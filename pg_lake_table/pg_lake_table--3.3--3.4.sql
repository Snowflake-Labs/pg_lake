-- Upgrade script for pg_lake_table from 3.3 to 3.4

-- Per-leaf surface->storage type divergence for compatibility_mode (e.g.
-- 'snowflake'): field_pg_type stays the PostgreSQL surface type (e.g. uuid),
-- while field_storage_pg_type records the Iceberg/Parquet storage type (e.g.
-- text/string) when it differs. NULL means storage == surface (the common
-- case), so existing rows keep identical behavior after upgrade.
ALTER TABLE lake_table.field_id_mappings
    ADD COLUMN field_storage_pg_type regtype,
    ADD COLUMN field_storage_pg_typemod int;

-- Tighten autovacuum_analyze on the commit-time diff catalogs so the planner's
-- reltuples stays close enough to reality to keep the diff query off the
-- nested-loop cliff. Complements pg_lake_table.commit_time_analyze_threshold.
ALTER TABLE lake_table.files SET (
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold    = 500
);
ALTER TABLE lake_table.data_file_partition_values SET (
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold    = 500
);
ALTER TABLE lake_table.data_file_column_stats SET (
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold    = 500
);

-- Sync function for external writes to Iceberg tables.
-- Called by the iceberg_tables INSTEAD OF trigger when an external client
-- updates metadata_location for a table in the current database catalog.
CREATE FUNCTION lake_table.sync_iceberg_metadata_from_external_write(regclass)
    RETURNS void AS 'MODULE_PATHNAME', 'sync_iceberg_metadata_from_external_write'
    LANGUAGE C STRICT;

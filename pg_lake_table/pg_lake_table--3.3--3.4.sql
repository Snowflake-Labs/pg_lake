-- Upgrade script for pg_lake_table from 3.3 to 3.4

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

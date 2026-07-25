-- Upgrade script for pg_lake_table from 3.4 to 3.5

-- Foreign-key cascades run once per deleted lake_table.files row. Without
-- indexes matching the referencing columns, bulk removal (especially
-- TRUNCATE) repeatedly scans the full child catalogs.
CREATE INDEX deletion_file_map_table_name_path_idx
ON lake_table.deletion_file_map (table_name, path);

CREATE INDEX data_file_column_stats_table_path_idx
ON lake_table.data_file_column_stats (table_name, path);

CREATE INDEX data_file_partition_values_id_idx
ON lake_table.data_file_partition_values (id);

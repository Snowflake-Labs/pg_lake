-- Upgrade script for pg_lake_table from 3.7 to 3.8

-- Number of NULL values of the column in the data file. NULL means unknown,
-- which is the case for every file written before this column existed.
ALTER TABLE lake_table.data_file_column_stats ADD COLUMN null_count bigint;

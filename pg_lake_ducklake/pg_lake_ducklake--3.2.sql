/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

-- DuckLake metadata schema
-- Implements DuckLake specification v0.3 for full DuckDB interoperability

CREATE SCHEMA lake_ducklake;
GRANT USAGE ON SCHEMA lake_ducklake TO public;

-- ============================================================================
-- Core Metadata Tables
-- ============================================================================

-- Key-value metadata store
CREATE TABLE lake_ducklake.metadata (
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    scope VARCHAR,
    scope_id BIGINT
);

-- Snapshot tracking - each data modification creates a new snapshot
CREATE TABLE lake_ducklake.snapshot (
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version BIGINT NOT NULL,
    next_catalog_id BIGINT NOT NULL,
    next_file_id BIGINT NOT NULL
);

-- Snapshot change tracking for audit/CDC
CREATE TABLE lake_ducklake.snapshot_changes (
    snapshot_id BIGINT PRIMARY KEY REFERENCES lake_ducklake.snapshot(snapshot_id),
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);

-- ============================================================================
-- Schema Definition Tables
-- ============================================================================

-- Schema (namespace) definitions
CREATE TABLE lake_ducklake.schema (
    schema_id BIGINT PRIMARY KEY,
    schema_uuid UUID NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_name VARCHAR NOT NULL,
    path VARCHAR,
    path_is_relative BOOLEAN DEFAULT true
);

-- Table definitions
CREATE TABLE lake_ducklake.table (
    table_id BIGINT PRIMARY KEY,
    table_uuid UUID NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_id BIGINT NOT NULL REFERENCES lake_ducklake.schema(schema_id) ON DELETE CASCADE,
    table_name VARCHAR NOT NULL,
    path VARCHAR,
    path_is_relative BOOLEAN DEFAULT true
);

-- Column definitions with schema evolution support
CREATE TABLE lake_ducklake.column (
    column_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    column_order BIGINT NOT NULL,
    column_name VARCHAR NOT NULL,
    column_type VARCHAR NOT NULL,
    initial_default VARCHAR,
    default_value VARCHAR,
    nulls_allowed BOOLEAN DEFAULT true,
    parent_column BIGINT,
    PRIMARY KEY (column_id, begin_snapshot)
);

-- View definitions
CREATE TABLE lake_ducklake.view (
    view_id BIGINT PRIMARY KEY,
    view_uuid UUID NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_id BIGINT NOT NULL REFERENCES lake_ducklake.schema(schema_id),
    view_name VARCHAR NOT NULL,
    dialect VARCHAR,
    sql VARCHAR NOT NULL,
    column_aliases VARCHAR
);

-- ============================================================================
-- Data File Tables
-- ============================================================================

-- Data files (Parquet files containing table data)
CREATE TABLE lake_ducklake.data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    file_order BIGINT,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN DEFAULT true,
    file_format VARCHAR DEFAULT 'parquet',
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT,
    row_id_start BIGINT,
    partition_id BIGINT,
    encryption_key VARCHAR,
    partial_file_info VARCHAR,
    mapping_id BIGINT
);

-- Delete files (track deleted rows)
CREATE TABLE lake_ducklake.delete_file (
    delete_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    data_file_id BIGINT REFERENCES lake_ducklake.data_file(data_file_id) ON DELETE CASCADE,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN DEFAULT true,
    format VARCHAR DEFAULT 'parquet',
    delete_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT,
    encryption_key VARCHAR
);

-- Files scheduled for deletion (cleanup queue)
CREATE TABLE lake_ducklake.files_scheduled_for_deletion (
    data_file_id BIGINT PRIMARY KEY,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN DEFAULT true,
    schedule_start TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Inlined data tables (small tables stored in metadata)
CREATE TABLE lake_ducklake.inlined_data_tables (
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    table_name VARCHAR NOT NULL,
    schema_version BIGINT NOT NULL,
    PRIMARY KEY (table_id, schema_version)
);

-- ============================================================================
-- Statistics Tables
-- ============================================================================

-- Table-level statistics
CREATE TABLE lake_ducklake.table_stats (
    table_id BIGINT PRIMARY KEY REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    record_count BIGINT,
    next_row_id BIGINT,
    file_size_bytes BIGINT
);

-- Table column statistics (aggregated across all files)
CREATE TABLE lake_ducklake.table_column_stats (
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    column_id BIGINT NOT NULL,
    contains_null BOOLEAN,
    contains_nan BOOLEAN,
    min_value VARCHAR,
    max_value VARCHAR,
    extra_stats VARCHAR,
    PRIMARY KEY (table_id, column_id)
);

-- Per-file column statistics
CREATE TABLE lake_ducklake.file_column_stats (
    data_file_id BIGINT NOT NULL REFERENCES lake_ducklake.data_file(data_file_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    column_id BIGINT NOT NULL,
    column_size_bytes BIGINT,
    value_count BIGINT,
    null_count BIGINT,
    min_value VARCHAR,
    max_value VARCHAR,
    contains_nan BOOLEAN,
    extra_stats VARCHAR,
    PRIMARY KEY (data_file_id, column_id)
);

-- ============================================================================
-- Partitioning Tables
-- ============================================================================

-- Partition info (partition spec per table)
CREATE TABLE lake_ducklake.partition_info (
    partition_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
);

-- Partition columns (which columns are used for partitioning)
CREATE TABLE lake_ducklake.partition_column (
    partition_id BIGINT NOT NULL REFERENCES lake_ducklake.partition_info(partition_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    partition_key_index BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    transform VARCHAR,
    PRIMARY KEY (partition_id, partition_key_index)
);

-- File partition values
CREATE TABLE lake_ducklake.file_partition_value (
    data_file_id BIGINT NOT NULL REFERENCES lake_ducklake.data_file(data_file_id) ON DELETE CASCADE,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    partition_key_index BIGINT NOT NULL,
    partition_value VARCHAR,
    PRIMARY KEY (data_file_id, partition_key_index)
);

-- ============================================================================
-- Column Mapping Tables
-- ============================================================================

-- Column mapping (for schema evolution)
CREATE TABLE lake_ducklake.column_mapping (
    mapping_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    type VARCHAR
);

-- Name mapping (field ID to name mapping)
CREATE TABLE lake_ducklake.name_mapping (
    mapping_id BIGINT NOT NULL REFERENCES lake_ducklake.column_mapping(mapping_id) ON DELETE CASCADE,
    column_id BIGINT NOT NULL,
    source_name VARCHAR,
    target_field_id BIGINT,
    parent_column BIGINT,
    is_partition BOOLEAN DEFAULT false,
    PRIMARY KEY (mapping_id, column_id)
);

-- ============================================================================
-- Tagging and Versioning Tables
-- ============================================================================

-- Object tags (for tables, schemas, etc.)
CREATE TABLE lake_ducklake.tag (
    object_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    key VARCHAR NOT NULL,
    value VARCHAR,
    PRIMARY KEY (object_id, key, begin_snapshot)
);

-- Column-specific tags
CREATE TABLE lake_ducklake.column_tag (
    table_id BIGINT NOT NULL REFERENCES lake_ducklake.table(table_id) ON DELETE CASCADE,
    column_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    key VARCHAR NOT NULL,
    value VARCHAR,
    PRIMARY KEY (table_id, column_id, key, begin_snapshot)
);

-- Schema versions (tracks schema evolution)
CREATE TABLE lake_ducklake.schema_versions (
    begin_snapshot BIGINT PRIMARY KEY,
    schema_version BIGINT NOT NULL
);

-- ============================================================================
-- Public Views (created in public schema for DuckDB compatibility)
-- ============================================================================

-- ducklake_table view - exposes table metadata (singular, following DuckDB pattern)
CREATE VIEW public.ducklake_table AS
SELECT
    table_id,
    table_uuid,
    begin_snapshot,
    end_snapshot,
    schema_id,
    table_name,
    path,
    path_is_relative
FROM lake_ducklake.table;

-- ducklake_schema view - exposes schema metadata
CREATE VIEW public.ducklake_schema AS
SELECT
    schema_id,
    schema_uuid,
    begin_snapshot,
    end_snapshot,
    schema_name,
    path,
    path_is_relative
FROM lake_ducklake.schema;

-- ducklake_column view - exposes column metadata
CREATE VIEW public.ducklake_column AS
SELECT * FROM lake_ducklake.column;

-- ducklake_view view - exposes view metadata
CREATE VIEW public.ducklake_view AS
SELECT * FROM lake_ducklake.view;

-- ducklake_snapshot view - exposes snapshot metadata
CREATE VIEW public.ducklake_snapshot AS
SELECT * FROM lake_ducklake.snapshot;

-- ducklake_data_file view - exposes data file metadata
CREATE VIEW public.ducklake_data_file AS
SELECT * FROM lake_ducklake.data_file;

-- Grant permissions
GRANT SELECT ON public.ducklake_table TO public;
GRANT SELECT ON public.ducklake_schema TO public;
GRANT SELECT ON public.ducklake_column TO public;
GRANT SELECT ON public.ducklake_view TO public;
GRANT SELECT ON public.ducklake_snapshot TO public;
GRANT SELECT ON public.ducklake_data_file TO public;

-- ============================================================================
-- INSTEAD OF Triggers for Writable Views
-- ============================================================================

-- ducklake_table INSERT trigger
CREATE FUNCTION lake_ducklake.ducklake_table_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.table (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
    VALUES (NEW.table_id, NEW.table_uuid, NEW.begin_snapshot, NEW.end_snapshot, NEW.schema_id, NEW.table_name, NEW.path, NEW.path_is_relative);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_insert_trigger
INSTEAD OF INSERT ON public.ducklake_table
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_insert();

-- ducklake_schema INSERT trigger
CREATE FUNCTION lake_ducklake.ducklake_schema_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)
    VALUES (NEW.schema_id, NEW.schema_uuid, NEW.begin_snapshot, NEW.end_snapshot, NEW.schema_name, NEW.path, NEW.path_is_relative);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_schema_insert_trigger
INSTEAD OF INSERT ON public.ducklake_schema
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_schema_insert();

-- ducklake_column INSERT trigger
CREATE FUNCTION lake_ducklake.ducklake_column_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.column (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column)
    VALUES (NEW.column_id, NEW.begin_snapshot, NEW.end_snapshot, NEW.table_id, NEW.column_order, NEW.column_name, NEW.column_type, NEW.initial_default, NEW.default_value, NEW.nulls_allowed, NEW.parent_column);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_insert_trigger
INSTEAD OF INSERT ON public.ducklake_column
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_insert();

-- ducklake_snapshot INSERT trigger
CREATE FUNCTION lake_ducklake.ducklake_snapshot_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
    VALUES (NEW.snapshot_id, NEW.snapshot_time, NEW.schema_version, NEW.next_catalog_id, NEW.next_file_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_snapshot_insert_trigger
INSTEAD OF INSERT ON public.ducklake_snapshot
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_snapshot_insert();

-- ducklake_data_file INSERT trigger
CREATE FUNCTION lake_ducklake.ducklake_data_file_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id)
    VALUES (NEW.data_file_id, NEW.table_id, NEW.begin_snapshot, NEW.end_snapshot, NEW.file_order, NEW.path, NEW.path_is_relative, NEW.file_format, NEW.record_count, NEW.file_size_bytes, NEW.footer_size, NEW.row_id_start, NEW.partition_id, NEW.encryption_key, NEW.partial_file_info, NEW.mapping_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_data_file_insert_trigger
INSTEAD OF INSERT ON public.ducklake_data_file
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_data_file_insert();

-- ============================================================================
-- SQL Functions
-- ============================================================================

-- Translate PostgreSQL type to DuckLake type
-- See https://ducklake.select/docs/stable/specification/data_types
CREATE FUNCTION lake_ducklake.pg_type_to_duckdb_type(pg_type TEXT)
RETURNS TEXT
LANGUAGE SQL IMMUTABLE
AS $$
    SELECT CASE
        -- Integer types (DuckLake uses int8, int16, int32, int64)
        WHEN pg_type IN ('smallint', 'int2') THEN 'int16'
        WHEN pg_type IN ('integer', 'int', 'int4') THEN 'int32'
        WHEN pg_type IN ('bigint', 'int8') THEN 'int64'

        -- Numeric types (DuckLake uses decimal(P,S) and float32/float64)
        WHEN pg_type = 'numeric' THEN 'decimal(38,9)'  -- Default for unbounded numeric
        WHEN pg_type LIKE 'numeric%' THEN REPLACE(pg_type, 'numeric', 'decimal')
        WHEN pg_type = 'decimal' THEN 'decimal(38,9)'  -- Default for unbounded decimal
        WHEN pg_type LIKE 'decimal%' THEN pg_type
        WHEN pg_type IN ('real', 'float4') THEN 'float32'
        WHEN pg_type IN ('double precision', 'float8') THEN 'float64'

        -- String types (DuckLake uses varchar)
        WHEN pg_type IN ('text', 'varchar', 'character varying') THEN 'varchar'
        WHEN pg_type LIKE 'character varying%' THEN 'varchar'
        WHEN pg_type LIKE 'varchar%' THEN 'varchar'
        WHEN pg_type LIKE 'char%' THEN 'varchar'
        WHEN pg_type = 'bpchar' THEN 'varchar'

        -- Boolean (DuckLake uses boolean)
        WHEN pg_type IN ('boolean', 'bool') THEN 'boolean'

        -- Date/time types (DuckLake uses date, timestamp, timestamptz, time, timetz)
        WHEN pg_type = 'date' THEN 'date'
        WHEN pg_type IN ('timestamp', 'timestamp without time zone') THEN 'timestamp'
        WHEN pg_type IN ('timestamptz', 'timestamp with time zone') THEN 'timestamptz'
        WHEN pg_type = 'time without time zone' THEN 'time'
        WHEN pg_type = 'time with time zone' THEN 'timetz'
        WHEN pg_type = 'time' THEN 'time'

        -- Binary (DuckLake uses blob)
        WHEN pg_type = 'bytea' THEN 'blob'

        -- UUID (DuckLake uses uuid)
        WHEN pg_type = 'uuid' THEN 'uuid'

        -- JSON (DuckLake uses json)
        WHEN pg_type IN ('json', 'jsonb') THEN 'json'

        -- Default: use as-is
        ELSE pg_type
    END;
$$;

-- Get snapshots for a table
CREATE FUNCTION lake_ducklake.snapshots(catalog_name TEXT)
RETURNS TABLE(
    snapshot_id BIGINT,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR
)
LANGUAGE SQL STABLE
AS $$
    SELECT
        s.snapshot_id,
        s.snapshot_time,
        s.schema_version,
        sc.changes_made,
        sc.author,
        sc.commit_message
    FROM lake_ducklake.snapshot s
    LEFT JOIN lake_ducklake.snapshot_changes sc ON s.snapshot_id = sc.snapshot_id
    ORDER BY s.snapshot_id DESC;
$$;

-- Get table info
CREATE FUNCTION lake_ducklake.table_info(p_table_id BIGINT)
RETURNS TABLE(
    table_name VARCHAR,
    table_uuid UUID,
    schema_name VARCHAR,
    record_count BIGINT,
    file_count BIGINT,
    file_size_bytes BIGINT
)
LANGUAGE SQL STABLE
AS $$
    SELECT
        t.table_name,
        t.table_uuid,
        s.schema_name,
        ts.record_count,
        (SELECT COUNT(*) FROM lake_ducklake.data_file df
         WHERE df.table_id = t.table_id AND df.end_snapshot IS NULL),
        ts.file_size_bytes
    FROM lake_ducklake.table t
    JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id
    LEFT JOIN lake_ducklake.table_stats ts ON t.table_id = ts.table_id
    WHERE t.table_id = p_table_id;
$$;

-- Get data files for a table at current or specific snapshot
CREATE FUNCTION lake_ducklake.data_files(p_table_id BIGINT, p_snapshot_id BIGINT DEFAULT NULL)
RETURNS TABLE(
    data_file_id BIGINT,
    path VARCHAR,
    file_format VARCHAR,
    record_count BIGINT,
    file_size_bytes BIGINT,
    row_id_start BIGINT
)
LANGUAGE SQL STABLE
AS $$
    SELECT
        df.data_file_id,
        df.path,
        df.file_format,
        df.record_count,
        df.file_size_bytes,
        df.row_id_start
    FROM lake_ducklake.data_file df
    WHERE df.table_id = p_table_id
      AND df.begin_snapshot <= COALESCE(p_snapshot_id,
            (SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot))
      AND (df.end_snapshot IS NULL
           OR df.end_snapshot > COALESCE(p_snapshot_id,
            (SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot)));
$$;

-- ============================================================================
-- Roles
-- ============================================================================

DO LANGUAGE plpgsql $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'ducklake_catalog') THEN
        CREATE ROLE ducklake_catalog;
    END IF;
    GRANT ALL ON ALL TABLES IN SCHEMA lake_ducklake TO ducklake_catalog;
END;
$$;

-- ============================================================================
-- Initialize with first snapshot
-- ============================================================================

INSERT INTO lake_ducklake.snapshot (snapshot_id, schema_version, next_catalog_id, next_file_id)
VALUES (0, 0, 1, 1);

INSERT INTO lake_ducklake.metadata (key, value, scope, scope_id)
VALUES ('ducklake_version', '0.3', NULL, NULL);

-- ============================================================================
-- Additional Public Views (remaining 16 tables)
-- ============================================================================

-- ducklake_column_mapping view
CREATE VIEW public.ducklake_column_mapping AS
SELECT * FROM lake_ducklake.column_mapping;

-- ducklake_name_mapping view
CREATE VIEW public.ducklake_name_mapping AS
SELECT * FROM lake_ducklake.name_mapping;

-- ducklake_column_tag view
CREATE VIEW public.ducklake_column_tag AS
SELECT * FROM lake_ducklake.column_tag;

-- ducklake_delete_file view
CREATE VIEW public.ducklake_delete_file AS
SELECT * FROM lake_ducklake.delete_file;

-- ducklake_file_column_stats view
CREATE VIEW public.ducklake_file_column_stats AS
SELECT * FROM lake_ducklake.file_column_stats;

-- ducklake_file_partition_value view
CREATE VIEW public.ducklake_file_partition_value AS
SELECT * FROM lake_ducklake.file_partition_value;

-- ducklake_files_scheduled_for_deletion view
CREATE VIEW public.ducklake_files_scheduled_for_deletion AS
SELECT * FROM lake_ducklake.files_scheduled_for_deletion;

-- ducklake_inlined_data_tables view
CREATE VIEW public.ducklake_inlined_data_tables AS
SELECT * FROM lake_ducklake.inlined_data_tables;

-- ducklake_metadata view
CREATE VIEW public.ducklake_metadata AS
SELECT * FROM lake_ducklake.metadata;

-- ducklake_partition_column view
CREATE VIEW public.ducklake_partition_column AS
SELECT * FROM lake_ducklake.partition_column;

-- ducklake_partition_info view
CREATE VIEW public.ducklake_partition_info AS
SELECT * FROM lake_ducklake.partition_info;

-- ducklake_schema_versions view
CREATE VIEW public.ducklake_schema_versions AS
SELECT * FROM lake_ducklake.schema_versions;

-- ducklake_snapshot_changes view
CREATE VIEW public.ducklake_snapshot_changes AS
SELECT * FROM lake_ducklake.snapshot_changes;

-- ducklake_table_column_stats view
CREATE VIEW public.ducklake_table_column_stats AS
SELECT * FROM lake_ducklake.table_column_stats;

-- ducklake_table_stats view
CREATE VIEW public.ducklake_table_stats AS
SELECT * FROM lake_ducklake.table_stats;

-- ducklake_tag view
CREATE VIEW public.ducklake_tag AS
SELECT * FROM lake_ducklake.tag;

-- Grant permissions
GRANT SELECT ON public.ducklake_column_mapping TO public;
GRANT SELECT ON public.ducklake_name_mapping TO public;
GRANT SELECT ON public.ducklake_column_tag TO public;
GRANT SELECT ON public.ducklake_delete_file TO public;
GRANT SELECT ON public.ducklake_file_column_stats TO public;
GRANT SELECT ON public.ducklake_file_partition_value TO public;
GRANT SELECT ON public.ducklake_files_scheduled_for_deletion TO public;
GRANT SELECT ON public.ducklake_inlined_data_tables TO public;
GRANT SELECT ON public.ducklake_metadata TO public;
GRANT SELECT ON public.ducklake_partition_column TO public;
GRANT SELECT ON public.ducklake_partition_info TO public;
GRANT SELECT ON public.ducklake_schema_versions TO public;
GRANT SELECT ON public.ducklake_snapshot_changes TO public;
GRANT SELECT ON public.ducklake_table_column_stats TO public;
GRANT SELECT ON public.ducklake_table_stats TO public;
GRANT SELECT ON public.ducklake_tag TO public;

-- ============================================================================
-- Convenience Views for Compatibility
-- ============================================================================

-- lake_ducklake.tables view - provides a more intuitive interface for querying tables
CREATE VIEW lake_ducklake.tables AS
SELECT
    t.table_name,
    s.schema_name as table_schema,
    t.path as location,
    t.table_id,
    t.table_uuid,
    t.begin_snapshot,
    t.end_snapshot
FROM lake_ducklake.table t
JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id
WHERE t.end_snapshot IS NULL;

GRANT SELECT ON lake_ducklake.tables TO public;

-- ============================================================================
-- DELETE and UPDATE Triggers for DuckLake Views
-- These triggers are required for UPDATE/DELETE operations from both
-- PostgreSQL and DuckDB
-- ============================================================================

-- Schema DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_schema_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.schema
    WHERE schema_id = OLD.schema_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_schema_delete_trigger
INSTEAD OF DELETE ON public.ducklake_schema
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_schema_delete();

-- Table DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.table
    WHERE table_id = OLD.table_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_delete_trigger
INSTEAD OF DELETE ON public.ducklake_table
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_delete();

-- Table UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.table
    SET
        table_uuid = NEW.table_uuid,
        begin_snapshot = NEW.begin_snapshot,
        end_snapshot = NEW.end_snapshot,
        schema_id = NEW.schema_id,
        table_name = NEW.table_name,
        path = NEW.path,
        path_is_relative = NEW.path_is_relative
    WHERE table_id = OLD.table_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_update_trigger
INSTEAD OF UPDATE ON public.ducklake_table
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_update();

-- Column DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.column
    WHERE column_id = OLD.column_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_delete_trigger
INSTEAD OF DELETE ON public.ducklake_column
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_delete();

-- Snapshot DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_snapshot_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.snapshot
    WHERE snapshot_id = OLD.snapshot_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_snapshot_delete_trigger
INSTEAD OF DELETE ON public.ducklake_snapshot
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_snapshot_delete();

-- Snapshot UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_snapshot_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.snapshot
    SET
        snapshot_time = NEW.snapshot_time,
        schema_version = NEW.schema_version,
        next_catalog_id = NEW.next_catalog_id,
        next_file_id = NEW.next_file_id
    WHERE snapshot_id = OLD.snapshot_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_snapshot_update_trigger
INSTEAD OF UPDATE ON public.ducklake_snapshot
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_snapshot_update();

-- Data file DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_data_file_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.data_file
    WHERE data_file_id = OLD.data_file_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_data_file_delete_trigger
INSTEAD OF DELETE ON public.ducklake_data_file
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_data_file_delete();

-- Data file UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_data_file_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.data_file
    SET
        table_id = NEW.table_id,
        begin_snapshot = NEW.begin_snapshot,
        end_snapshot = NEW.end_snapshot,
        file_order = NEW.file_order,
        path = NEW.path,
        path_is_relative = NEW.path_is_relative,
        file_format = NEW.file_format,
        record_count = NEW.record_count,
        file_size_bytes = NEW.file_size_bytes,
        footer_size = NEW.footer_size,
        row_id_start = NEW.row_id_start,
        partition_id = NEW.partition_id,
        encryption_key = NEW.encryption_key,
        partial_file_info = NEW.partial_file_info,
        mapping_id = NEW.mapping_id
    WHERE data_file_id = OLD.data_file_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_data_file_update_trigger
INSTEAD OF UPDATE ON public.ducklake_data_file
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_data_file_update();

-- Delete file DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_delete_file_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.delete_file
    WHERE delete_file_id = OLD.delete_file_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_delete_file_delete_trigger
INSTEAD OF DELETE ON public.ducklake_delete_file
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_delete_file_delete();

-- Table stats UPDATE and DELETE triggers  
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_stats_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.table_stats
    SET
        record_count = NEW.record_count,
        next_row_id = NEW.next_row_id,
        file_size_bytes = NEW.file_size_bytes
    WHERE table_id = OLD.table_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_stats_update_trigger
INSTEAD OF UPDATE ON public.ducklake_table_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_stats_update();

CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_stats_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.table_stats
    WHERE table_id = OLD.table_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_stats_delete_trigger
INSTEAD OF DELETE ON public.ducklake_table_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_stats_delete();

-- Metadata DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_metadata_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.metadata
    WHERE key = OLD.key AND scope = OLD.scope AND scope_id = OLD.scope_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_metadata_delete_trigger
INSTEAD OF DELETE ON public.ducklake_metadata
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_metadata_delete();


-- ============================================================================
-- Additional triggers for column_mapping, column_tag, delete_file
-- ============================================================================

-- column_mapping INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_mapping_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.column_mapping (mapping_id, table_id, type)
    VALUES (NEW.mapping_id, NEW.table_id, NEW.type);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_mapping_insert_trigger
INSTEAD OF INSERT ON public.ducklake_column_mapping
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_mapping_insert();

-- column_mapping DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_mapping_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.column_mapping
    WHERE mapping_id = OLD.mapping_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_mapping_delete_trigger
INSTEAD OF DELETE ON public.ducklake_column_mapping
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_mapping_delete();

-- column_tag INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_tag_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.column_tag (table_id, column_id, key, value, begin_snapshot, end_snapshot)
    VALUES (NEW.table_id, NEW.column_id, NEW.key, NEW.value, NEW.begin_snapshot, NEW.end_snapshot);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_tag_insert_trigger
INSTEAD OF INSERT ON public.ducklake_column_tag
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_tag_insert();

-- column_tag DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_tag_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.column_tag
    WHERE table_id = OLD.table_id AND column_id = OLD.column_id AND key = OLD.key AND begin_snapshot = OLD.begin_snapshot;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_tag_delete_trigger
INSTEAD OF DELETE ON public.ducklake_column_tag
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_tag_delete();

-- delete_file INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_delete_file_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.delete_file (
        delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id,
        path, path_is_relative, format, delete_count, file_size_bytes,
        footer_size, encryption_key
    )
    VALUES (
        NEW.delete_file_id, NEW.table_id, NEW.begin_snapshot, NEW.end_snapshot, NEW.data_file_id,
        NEW.path, NEW.path_is_relative, NEW.format, NEW.delete_count, NEW.file_size_bytes,
        NEW.footer_size, NEW.encryption_key
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_delete_file_insert_trigger
INSTEAD OF INSERT ON public.ducklake_delete_file
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_delete_file_insert();


-- ============================================================================
-- Triggers for file_column_stats, file_partition_value, files_scheduled_for_deletion
-- ============================================================================

-- file_column_stats INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_file_column_stats_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.file_column_stats (
        data_file_id, table_id, column_id, column_size_bytes, value_count,
        null_count, min_value, max_value, contains_nan, extra_stats
    )
    VALUES (
        NEW.data_file_id, NEW.table_id, NEW.column_id, NEW.column_size_bytes, NEW.value_count,
        NEW.null_count, NEW.min_value, NEW.max_value, NEW.contains_nan, NEW.extra_stats
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_file_column_stats_insert_trigger
INSTEAD OF INSERT ON public.ducklake_file_column_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_file_column_stats_insert();

-- file_column_stats DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_file_column_stats_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.file_column_stats
    WHERE data_file_id = OLD.data_file_id AND column_id = OLD.column_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_file_column_stats_delete_trigger
INSTEAD OF DELETE ON public.ducklake_file_column_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_file_column_stats_delete();

-- file_partition_value INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_file_partition_value_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.file_partition_value (data_file_id, partition_key_index, value)
    VALUES (NEW.data_file_id, NEW.partition_key_index, NEW.value);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_file_partition_value_insert_trigger
INSTEAD OF INSERT ON public.ducklake_file_partition_value
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_file_partition_value_insert();

-- file_partition_value DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_file_partition_value_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.file_partition_value
    WHERE data_file_id = OLD.data_file_id AND partition_key_index = OLD.partition_key_index;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_file_partition_value_delete_trigger
INSTEAD OF DELETE ON public.ducklake_file_partition_value
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_file_partition_value_delete();

-- files_scheduled_for_deletion INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_files_scheduled_for_deletion_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.files_scheduled_for_deletion (data_file_id, scheduled_at)
    VALUES (NEW.data_file_id, NEW.scheduled_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_files_scheduled_for_deletion_insert_trigger
INSTEAD OF INSERT ON public.ducklake_files_scheduled_for_deletion
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_files_scheduled_for_deletion_insert();

-- files_scheduled_for_deletion DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_files_scheduled_for_deletion_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.files_scheduled_for_deletion
    WHERE data_file_id = OLD.data_file_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_files_scheduled_for_deletion_delete_trigger
INSTEAD OF DELETE ON public.ducklake_files_scheduled_for_deletion
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_files_scheduled_for_deletion_delete();


-- ============================================================================
-- Triggers for inlined_data_tables, metadata, name_mapping
-- ============================================================================

-- inlined_data_tables INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_inlined_data_tables_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.inlined_data_tables (table_id, schema_version, data)
    VALUES (NEW.table_id, NEW.schema_version, NEW.data);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_inlined_data_tables_insert_trigger
INSTEAD OF INSERT ON public.ducklake_inlined_data_tables
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_inlined_data_tables_insert();

-- inlined_data_tables DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_inlined_data_tables_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.inlined_data_tables
    WHERE table_id = OLD.table_id AND schema_version = OLD.schema_version;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_inlined_data_tables_delete_trigger
INSTEAD OF DELETE ON public.ducklake_inlined_data_tables
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_inlined_data_tables_delete();

-- metadata INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_metadata_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.metadata (key, value, scope, scope_id)
    VALUES (NEW.key, NEW.value, NEW.scope, NEW.scope_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_metadata_insert_trigger
INSTEAD OF INSERT ON public.ducklake_metadata
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_metadata_insert();

-- name_mapping INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_name_mapping_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.name_mapping (
        mapping_id, column_id, source_name, target_field_id, parent_column, is_partition
    )
    VALUES (
        NEW.mapping_id, NEW.column_id, NEW.source_name, NEW.target_field_id,
        NEW.parent_column, NEW.is_partition
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_name_mapping_insert_trigger
INSTEAD OF INSERT ON public.ducklake_name_mapping
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_name_mapping_insert();

-- name_mapping DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_name_mapping_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.name_mapping
    WHERE mapping_id = OLD.mapping_id AND column_id = OLD.column_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_name_mapping_delete_trigger
INSTEAD OF DELETE ON public.ducklake_name_mapping
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_name_mapping_delete();


-- ============================================================================
-- Triggers for partition views and schema_versions
-- ============================================================================

-- partition_column INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_partition_column_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.partition_column (partition_id, partition_key_index, column_id, transform)
    VALUES (NEW.partition_id, NEW.partition_key_index, NEW.column_id, NEW.transform);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_partition_column_insert_trigger
INSTEAD OF INSERT ON public.ducklake_partition_column
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_partition_column_insert();

-- partition_column DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_partition_column_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.partition_column
    WHERE partition_id = OLD.partition_id AND partition_key_index = OLD.partition_key_index;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_partition_column_delete_trigger
INSTEAD OF DELETE ON public.ducklake_partition_column
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_partition_column_delete();

-- partition_info INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_partition_info_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.partition_info (partition_id, table_id, schema_version)
    VALUES (NEW.partition_id, NEW.table_id, NEW.schema_version);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_partition_info_insert_trigger
INSTEAD OF INSERT ON public.ducklake_partition_info
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_partition_info_insert();

-- partition_info DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_partition_info_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.partition_info
    WHERE partition_id = OLD.partition_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_partition_info_delete_trigger
INSTEAD OF DELETE ON public.ducklake_partition_info
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_partition_info_delete();

-- schema_versions INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_schema_versions_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.schema_versions (begin_snapshot, schema_version)
    VALUES (NEW.begin_snapshot, NEW.schema_version);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_schema_versions_insert_trigger
INSTEAD OF INSERT ON public.ducklake_schema_versions
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_schema_versions_insert();

-- schema_versions DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_schema_versions_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.schema_versions
    WHERE begin_snapshot = OLD.begin_snapshot;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_schema_versions_delete_trigger
INSTEAD OF DELETE ON public.ducklake_schema_versions
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_schema_versions_delete();


-- ============================================================================
-- Triggers for snapshot_changes, table_column_stats, tag, view
-- ============================================================================

-- snapshot_changes INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_snapshot_changes_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.snapshot_changes (snapshot_id, operations, author, description)
    VALUES (NEW.snapshot_id, NEW.operations, NEW.author, NEW.description);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_snapshot_changes_insert_trigger
INSTEAD OF INSERT ON public.ducklake_snapshot_changes
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_snapshot_changes_insert();

-- snapshot_changes DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_snapshot_changes_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.snapshot_changes
    WHERE snapshot_id = OLD.snapshot_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_snapshot_changes_delete_trigger
INSTEAD OF DELETE ON public.ducklake_snapshot_changes
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_snapshot_changes_delete();

-- table_column_stats INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_column_stats_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.table_column_stats (
        table_id, column_id, contains_null, contains_nan, min_value,
        max_value, extra_stats
    )
    VALUES (
        NEW.table_id, NEW.column_id, NEW.contains_null, NEW.contains_nan,
        NEW.min_value, NEW.max_value, NEW.extra_stats
    )
    ON CONFLICT (table_id, column_id) DO UPDATE SET
        contains_null = EXCLUDED.contains_null,
        contains_nan = EXCLUDED.contains_nan,
        min_value = EXCLUDED.min_value,
        max_value = EXCLUDED.max_value,
        extra_stats = EXCLUDED.extra_stats;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_column_stats_insert_trigger
INSTEAD OF INSERT ON public.ducklake_table_column_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_column_stats_insert();

-- table_column_stats DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_column_stats_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.table_column_stats
    WHERE table_id = OLD.table_id AND column_id = OLD.column_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_column_stats_delete_trigger
INSTEAD OF DELETE ON public.ducklake_table_column_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_column_stats_delete();

-- tag INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_tag_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.tag (object_id, key, value, begin_snapshot, end_snapshot)
    VALUES (NEW.object_id, NEW.key, NEW.value, NEW.begin_snapshot, NEW.end_snapshot);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_tag_insert_trigger
INSTEAD OF INSERT ON public.ducklake_tag
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_tag_insert();

-- tag DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_tag_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.tag
    WHERE object_id = OLD.object_id AND key = OLD.key AND begin_snapshot = OLD.begin_snapshot;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_tag_delete_trigger
INSTEAD OF DELETE ON public.ducklake_tag
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_tag_delete();

-- view INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_view_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.view (view_id, table_id, view_definition)
    VALUES (NEW.view_id, NEW.table_id, NEW.view_definition);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_view_insert_trigger
INSTEAD OF INSERT ON public.ducklake_view
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_view_insert();

-- view DELETE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_view_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM lake_ducklake.view
    WHERE view_id = OLD.view_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_view_delete_trigger
INSTEAD OF DELETE ON public.ducklake_view
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_view_delete();


-- ============================================================================
-- Missing table_stats INSERT trigger
-- ============================================================================

-- table_stats INSERT trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_stats_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO lake_ducklake.table_stats (
        table_id, record_count, next_row_id, file_size_bytes
    )
    VALUES (
        NEW.table_id, NEW.record_count, NEW.next_row_id, NEW.file_size_bytes
    )
    ON CONFLICT (table_id) DO UPDATE SET
        record_count = EXCLUDED.record_count,
        next_row_id = EXCLUDED.next_row_id,
        file_size_bytes = EXCLUDED.file_size_bytes;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_stats_insert_trigger
INSTEAD OF INSERT ON public.ducklake_table_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_stats_insert();


-- ============================================================================
-- UPDATE triggers for column, column_mapping, column_tag, delete_file
-- ============================================================================

-- column UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.column
    SET
        begin_snapshot = NEW.begin_snapshot,
        end_snapshot = NEW.end_snapshot,
        table_id = NEW.table_id,
        column_order = NEW.column_order,
        column_name = NEW.column_name,
        column_type = NEW.column_type,
        initial_default = NEW.initial_default,
        default_value = NEW.default_value,
        nulls_allowed = NEW.nulls_allowed,
        parent_column = NEW.parent_column
    WHERE column_id = OLD.column_id AND begin_snapshot = OLD.begin_snapshot;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_update_trigger
INSTEAD OF UPDATE ON public.ducklake_column
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_update();

-- column_mapping UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_mapping_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.column_mapping
    SET
        table_id = NEW.table_id,
        type = NEW.type
    WHERE mapping_id = OLD.mapping_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_mapping_update_trigger
INSTEAD OF UPDATE ON public.ducklake_column_mapping
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_mapping_update();

-- column_tag UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_column_tag_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.column_tag
    SET
        value = NEW.value,
        end_snapshot = NEW.end_snapshot
    WHERE table_id = OLD.table_id AND column_id = OLD.column_id 
      AND key = OLD.key AND begin_snapshot = OLD.begin_snapshot;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_column_tag_update_trigger
INSTEAD OF UPDATE ON public.ducklake_column_tag
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_column_tag_update();

-- delete_file UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_delete_file_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.delete_file
    SET
        table_id = NEW.table_id,
        begin_snapshot = NEW.begin_snapshot,
        end_snapshot = NEW.end_snapshot,
        data_file_id = NEW.data_file_id,
        path = NEW.path,
        path_is_relative = NEW.path_is_relative,
        format = NEW.format,
        delete_count = NEW.delete_count,
        file_size_bytes = NEW.file_size_bytes,
        footer_size = NEW.footer_size,
        encryption_key = NEW.encryption_key
    WHERE delete_file_id = OLD.delete_file_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_delete_file_update_trigger
INSTEAD OF UPDATE ON public.ducklake_delete_file
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_delete_file_update();


-- ============================================================================
-- UPDATE triggers for file stats and partition views
-- ============================================================================

-- file_column_stats UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_file_column_stats_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.file_column_stats
    SET
        table_id = NEW.table_id,
        column_size_bytes = NEW.column_size_bytes,
        value_count = NEW.value_count,
        null_count = NEW.null_count,
        min_value = NEW.min_value,
        max_value = NEW.max_value,
        contains_nan = NEW.contains_nan,
        extra_stats = NEW.extra_stats
    WHERE data_file_id = OLD.data_file_id AND column_id = OLD.column_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_file_column_stats_update_trigger
INSTEAD OF UPDATE ON public.ducklake_file_column_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_file_column_stats_update();

-- file_partition_value UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_file_partition_value_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.file_partition_value
    SET
        value = NEW.value
    WHERE data_file_id = OLD.data_file_id AND partition_key_index = OLD.partition_key_index;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_file_partition_value_update_trigger
INSTEAD OF UPDATE ON public.ducklake_file_partition_value
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_file_partition_value_update();

-- files_scheduled_for_deletion UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_files_scheduled_for_deletion_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.files_scheduled_for_deletion
    SET
        scheduled_at = NEW.scheduled_at
    WHERE data_file_id = OLD.data_file_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_files_scheduled_for_deletion_update_trigger
INSTEAD OF UPDATE ON public.ducklake_files_scheduled_for_deletion
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_files_scheduled_for_deletion_update();

-- partition_column UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_partition_column_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.partition_column
    SET
        column_id = NEW.column_id,
        transform = NEW.transform
    WHERE partition_id = OLD.partition_id AND partition_key_index = OLD.partition_key_index;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_partition_column_update_trigger
INSTEAD OF UPDATE ON public.ducklake_partition_column
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_partition_column_update();

-- partition_info UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_partition_info_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.partition_info
    SET
        table_id = NEW.table_id,
        schema_version = NEW.schema_version
    WHERE partition_id = OLD.partition_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_partition_info_update_trigger
INSTEAD OF UPDATE ON public.ducklake_partition_info
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_partition_info_update();


-- ============================================================================
-- UPDATE triggers for schema and metadata views
-- ============================================================================

-- inlined_data_tables UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_inlined_data_tables_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.inlined_data_tables
    SET
        data = NEW.data
    WHERE table_id = OLD.table_id AND schema_version = OLD.schema_version;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_inlined_data_tables_update_trigger
INSTEAD OF UPDATE ON public.ducklake_inlined_data_tables
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_inlined_data_tables_update();

-- metadata UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_metadata_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.metadata
    SET
        value = NEW.value
    WHERE key = OLD.key AND scope = OLD.scope AND scope_id = OLD.scope_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_metadata_update_trigger
INSTEAD OF UPDATE ON public.ducklake_metadata
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_metadata_update();

-- name_mapping UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_name_mapping_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.name_mapping
    SET
        source_name = NEW.source_name,
        target_field_id = NEW.target_field_id,
        parent_column = NEW.parent_column,
        is_partition = NEW.is_partition
    WHERE mapping_id = OLD.mapping_id AND column_id = OLD.column_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_name_mapping_update_trigger
INSTEAD OF UPDATE ON public.ducklake_name_mapping
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_name_mapping_update();

-- schema UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_schema_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.schema
    SET
        schema_uuid = NEW.schema_uuid,
        begin_snapshot = NEW.begin_snapshot,
        end_snapshot = NEW.end_snapshot,
        schema_name = NEW.schema_name,
        path = NEW.path,
        path_is_relative = NEW.path_is_relative
    WHERE schema_id = OLD.schema_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_schema_update_trigger
INSTEAD OF UPDATE ON public.ducklake_schema
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_schema_update();

-- schema_versions UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_schema_versions_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.schema_versions
    SET
        schema_version = NEW.schema_version
    WHERE begin_snapshot = OLD.begin_snapshot;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_schema_versions_update_trigger
INSTEAD OF UPDATE ON public.ducklake_schema_versions
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_schema_versions_update();


-- ============================================================================
-- UPDATE triggers for snapshot_changes, table_column_stats, tag, view
-- ============================================================================

-- snapshot_changes UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_snapshot_changes_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.snapshot_changes
    SET
        changes_made = NEW.changes_made,
        author = NEW.author,
        commit_message = NEW.commit_message,
        commit_extra_info = NEW.commit_extra_info
    WHERE snapshot_id = OLD.snapshot_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_snapshot_changes_update_trigger
INSTEAD OF UPDATE ON public.ducklake_snapshot_changes
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_snapshot_changes_update();

-- table_column_stats UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_table_column_stats_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.table_column_stats
    SET
        contains_null = NEW.contains_null,
        contains_nan = NEW.contains_nan,
        min_value = NEW.min_value,
        max_value = NEW.max_value,
        extra_stats = NEW.extra_stats
    WHERE table_id = OLD.table_id AND column_id = OLD.column_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_table_column_stats_update_trigger
INSTEAD OF UPDATE ON public.ducklake_table_column_stats
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_table_column_stats_update();

-- tag UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_tag_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.tag
    SET
        value = NEW.value,
        end_snapshot = NEW.end_snapshot
    WHERE object_id = OLD.object_id AND key = OLD.key AND begin_snapshot = OLD.begin_snapshot;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_tag_update_trigger
INSTEAD OF UPDATE ON public.ducklake_tag
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_tag_update();

-- view UPDATE trigger
CREATE OR REPLACE FUNCTION lake_ducklake.ducklake_view_update()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE lake_ducklake.view
    SET
        view_uuid = NEW.view_uuid,
        begin_snapshot = NEW.begin_snapshot,
        end_snapshot = NEW.end_snapshot,
        schema_id = NEW.schema_id,
        view_name = NEW.view_name,
        dialect = NEW.dialect,
        sql = NEW.sql,
        column_aliases = NEW.column_aliases
    WHERE view_id = OLD.view_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ducklake_view_update_trigger
INSTEAD OF UPDATE ON public.ducklake_view
FOR EACH ROW EXECUTE FUNCTION lake_ducklake.ducklake_view_update();


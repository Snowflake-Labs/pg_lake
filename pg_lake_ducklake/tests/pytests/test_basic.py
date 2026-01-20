"""
Basic tests for pg_lake_ducklake extension functionality.
"""

import pytest


def test_extension_loaded(pg_cursor):
    """
    Verify pg_lake_ducklake extension is loaded.
    """
    pg_cursor.execute("""
        SELECT extname, extversion
        FROM pg_extension
        WHERE extname = 'pg_lake_ducklake'
    """)
    result = pg_cursor.fetchone()
    assert result is not None, "pg_lake_ducklake extension not loaded"
    print(f"Extension version: {result[1]}")


def test_ducklake_schema_exists(pg_cursor):
    """
    Verify the lake_ducklake schema exists.
    """
    pg_cursor.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name = 'lake_ducklake'
    """)
    result = pg_cursor.fetchone()
    assert result is not None, "lake_ducklake schema not found"


def test_metadata_tables_exist(pg_cursor):
    """
    Test that all required DuckLake metadata tables exist.
    """
    required_tables = [
        'metadata', 'snapshot', 'snapshot_changes', 'schema', 'table',
        'column', 'view', 'data_file', 'delete_file', 'table_stats',
        'table_column_stats', 'file_column_stats', 'partition_info',
        'partition_column', 'file_partition_value', 'column_mapping',
        'name_mapping', 'tag', 'column_tag', 'schema_versions',
        'files_scheduled_for_deletion', 'inlined_data_tables'
    ]

    for table_name in required_tables:
        pg_cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
        """)
        count = pg_cursor.fetchone()[0]
        assert count == 1, f"Table lake_ducklake.{table_name} not found"


def test_initial_snapshot_exists(pg_cursor):
    """
    Test that the initial snapshot (snapshot_id=0) exists.
    """
    pg_cursor.execute("SELECT * FROM lake_ducklake.snapshot WHERE snapshot_id = 0")
    result = pg_cursor.fetchone()
    assert result is not None, "Initial snapshot not found"
    assert result[2] == 0, "Initial schema_version should be 0"  # schema_version
    assert result[3] >= 1, "next_catalog_id should be at least 1"  # next_catalog_id
    assert result[4] >= 1, "next_file_id should be at least 1"  # next_file_id


def test_ducklake_version_metadata(pg_cursor):
    """
    Test that the DuckLake version is recorded in metadata.
    """
    pg_cursor.execute("""
        SELECT value
        FROM lake_ducklake.metadata
        WHERE key = 'ducklake_version'
    """)
    result = pg_cursor.fetchone()
    assert result is not None, "ducklake_version metadata not found"
    assert result[0] == '0.3', f"Expected version 0.3, got {result[0]}"


def test_sql_functions_exist(pg_cursor):
    """
    Test that the SQL functions are created.
    """
    functions = [
        ('snapshots', 'text'),
        ('table_info', 'bigint'),
        ('data_files', 'bigint, bigint')
    ]

    for func_name, arg_types in functions:
        pg_cursor.execute(f"""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'lake_ducklake'
            AND p.proname = '{func_name}'
        """)
        count = pg_cursor.fetchone()[0]
        assert count > 0, f"Function lake_ducklake.{func_name} not found"


def test_views_exist(pg_cursor):
    """
    Test that the public views are created.
    """
    # Check lake_ducklake.tables exists
    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.views
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'tables'
    """)
    count = pg_cursor.fetchone()[0]
    assert count == 1, f"Expected lake_ducklake.tables view, found {count}"

    # Check public.ducklake_table exists
    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.views
        WHERE table_schema = 'public'
        AND table_name = 'ducklake_table'
    """)
    count = pg_cursor.fetchone()[0]
    assert count == 1, f"Expected public.ducklake_table view, found {count}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

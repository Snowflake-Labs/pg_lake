"""
Test DuckLake metadata compliance with spec v0.3.

Validates that all metadata tables match the DuckLake specification exactly.
"""

import pytest


def test_all_metadata_tables_exist(pg_cursor):
    """
    Verify all required metadata tables exist.
    """
    required_tables = [
        'metadata',
        'snapshot',
        'snapshot_changes',
        'schema',
        'table',
        'column',
        'view',
        'data_file',
        'delete_file',
        'files_scheduled_for_deletion',
        'inlined_data_tables',
        'table_stats',
        'table_column_stats',
        'file_column_stats',
        'partition_info',
        'partition_column',
        'file_partition_value',
        'column_mapping',
        'name_mapping',
        'tag',
        'column_tag',
        'schema_versions'
    ]

    for table_name in required_tables:
        pg_cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
        """)
        count = pg_cursor.fetchone()[0]
        assert count == 1, f"Required table lake_ducklake.{table_name} not found"


def test_snapshot_table_structure(pg_cursor):
    """
    Verify snapshot table matches spec.
    """
    pg_cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'snapshot'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()

    expected = [
        ('snapshot_id', 'bigint', 'NO'),
        ('snapshot_time', 'timestamp', 'NO'),
        ('schema_version', 'bigint', 'NO'),
        ('next_catalog_id', 'bigint', 'NO'),
        ('next_file_id', 'bigint', 'NO'),
    ]

    assert len(columns) >= len(expected), f"Missing columns in snapshot table: got {len(columns)}, expected at least {len(expected)}"

    for i, (col, exp) in enumerate(zip(columns, expected)):
        assert col[0] == exp[0], f"Column {i} name mismatch: {col[0]} != {exp[0]}"
        assert exp[1] in col[1].lower(), f"Type mismatch for {exp[0]}: {col[1]} doesn't contain {exp[1]}"


def test_table_metadata_structure(pg_cursor):
    """
    Verify table metadata structure.
    """
    pg_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'table'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()

    required_columns = ['table_id', 'table_uuid', 'table_name', 'schema_id', 'path', 'begin_snapshot', 'end_snapshot']

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert req_col in column_names, f"Required column {req_col} not found in lake_ducklake.table"


def test_data_file_structure(pg_cursor):
    """
    Verify data_file table structure.
    """
    pg_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'data_file'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()

    required_columns = [
        'data_file_id',
        'table_id',
        'begin_snapshot',
        'end_snapshot',
        'path',
        'record_count',
        'file_size_bytes'
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert req_col in column_names, f"Required column {req_col} not found in lake_ducklake.data_file"


def test_column_table_structure(pg_cursor):
    """
    Verify column metadata structure for schema evolution.
    """
    pg_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'column'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()

    required_columns = [
        'column_id',
        'begin_snapshot',
        'end_snapshot',
        'table_id',
        'column_order',
        'column_name',
        'column_type'
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert req_col in column_names, f"Required column {req_col} not found in lake_ducklake.column"


def test_primary_keys_exist(pg_cursor):
    """
    Verify critical tables have primary keys.
    """
    tables_with_pks = [
        'snapshot',
        'table',
        'data_file',
        'delete_file',
        'schema'
    ]

    for table_name in tables_with_pks:
        pg_cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
            AND constraint_type = 'PRIMARY KEY'
        """)
        count = pg_cursor.fetchone()[0]
        assert count > 0, f"Table lake_ducklake.{table_name} missing PRIMARY KEY"


def test_foreign_keys_exist(pg_cursor):
    """
    Verify referential integrity constraints.
    """
    # data_file should reference table
    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_schema = 'lake_ducklake'
        AND tc.table_name = 'data_file'
        AND tc.constraint_type = 'FOREIGN KEY'
        AND kcu.column_name = 'table_id'
    """)
    count = pg_cursor.fetchone()[0]
    assert count > 0, "data_file missing foreign key to table"


def test_schema_table_structure(pg_cursor):
    """
    Verify schema (namespace) table structure.
    """
    pg_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'schema'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()

    required_columns = ['schema_id', 'schema_uuid', 'schema_name', 'begin_snapshot', 'end_snapshot', 'path']

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert req_col in column_names, f"Required column {req_col} not found in lake_ducklake.schema"


def test_partition_tables_structure(pg_cursor):
    """
    Verify partitioning metadata tables.
    """
    # Check partition_info exists and has correct structure
    pg_cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'partition_info'
        ORDER BY ordinal_position
    """)
    columns = [col[0] for col in pg_cursor.fetchall()]
    assert 'partition_id' in columns
    assert 'table_id' in columns

    # Check partition_column exists
    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'partition_column'
    """)
    assert pg_cursor.fetchone()[0] == 1


def test_statistics_tables_exist(pg_cursor):
    """
    Verify statistics metadata tables.
    """
    stats_tables = ['table_stats', 'table_column_stats', 'file_column_stats']

    for table_name in stats_tables:
        pg_cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
        """)
        count = pg_cursor.fetchone()[0]
        assert count == 1, f"Statistics table lake_ducklake.{table_name} not found"


def test_delete_file_structure(pg_cursor):
    """
    Verify delete file tracking table.
    """
    pg_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'delete_file'
        ORDER BY ordinal_position
    """)
    columns = pg_cursor.fetchall()

    required_columns = [
        'delete_file_id',
        'table_id',
        'begin_snapshot',
        'end_snapshot',
        'data_file_id',
        'path',
        'delete_count',
        'file_size_bytes'
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert req_col in column_names, f"Required column {req_col} not found in lake_ducklake.delete_file"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

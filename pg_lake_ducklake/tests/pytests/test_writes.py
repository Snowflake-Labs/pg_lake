"""
Test write operations to DuckLake metadata tables.
"""

import pytest


def test_insert_snapshot(pg_cursor):
    """Test creating a new snapshot."""
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.snapshot
        (snapshot_id, schema_version, next_catalog_id, next_file_id)
        VALUES (100, 0, 2, 1)
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT * FROM lake_ducklake.snapshot WHERE snapshot_id = 100")
    result = pg_cursor.fetchone()
    assert result is not None
    assert result[2] == 0  # schema_version


def test_insert_schema(pg_cursor):
    """Test creating a schema entry."""
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name, path)
        VALUES (1, gen_random_uuid(), 0, 'test_schema', '/tmp/test')
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT schema_name FROM lake_ducklake.schema WHERE schema_id = 1")
    result = pg_cursor.fetchone()
    assert result[0] == 'test_schema'


def test_insert_table(pg_cursor):
    """Test creating a table entry."""
    # First create schema
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name)
        VALUES (2, gen_random_uuid(), 0, 'test_schema2')
    """)

    # Then create table
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name, path)
        VALUES (1, gen_random_uuid(), 0, 2, 'test_table', '/tmp/test_table')
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT table_name FROM lake_ducklake.table WHERE table_id = 1")
    result = pg_cursor.fetchone()
    assert result[0] == 'test_table'


def test_insert_data_file(pg_cursor):
    """Test adding a data file entry."""
    # Setup: schema and table
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name)
        VALUES (3, gen_random_uuid(), 0, 's3');

        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name, path)
        VALUES (2, gen_random_uuid(), 0, 3, 't1', '/tmp/t1');
    """)

    # Add data file
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.data_file
        (data_file_id, table_id, begin_snapshot, path, record_count, file_size_bytes)
        VALUES (1, 2, 0, '/tmp/t1/data1.parquet', 100, 1024)
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("""
        SELECT record_count, file_size_bytes
        FROM lake_ducklake.data_file
        WHERE data_file_id = 1
    """)
    result = pg_cursor.fetchone()
    assert result[0] == 100
    assert result[1] == 1024


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

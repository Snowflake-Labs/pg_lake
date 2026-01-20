"""
Test snapshot isolation and versioning.
"""

import pytest


def test_snapshot_sequence(pg_cursor):
    """Test creating multiple snapshots in sequence."""
    for i in range(1, 4):
        pg_cursor.execute(f"""
            INSERT INTO lake_ducklake.snapshot
            (snapshot_id, schema_version, next_catalog_id, next_file_id)
            VALUES ({i}, 0, {i+1}, 1)
        """)
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT COUNT(*) FROM lake_ducklake.snapshot WHERE snapshot_id > 0")
    count = pg_cursor.fetchone()[0]
    assert count == 3


def test_table_versioning(pg_cursor):
    """Test table begin_snapshot and end_snapshot."""
    # Create schema
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name)
        VALUES (10, gen_random_uuid(), 0, 's10')
    """)

    # Create table at snapshot 1
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name)
        VALUES (10, gen_random_uuid(), 1, 10, 't10')
    """)

    # Mark as dropped at snapshot 2
    pg_cursor.execute("""
        UPDATE lake_ducklake.table
        SET end_snapshot = 2
        WHERE table_id = 10
    """)
    pg_cursor.connection.commit()

    # Table should exist at snapshot 1
    pg_cursor.execute("""
        SELECT COUNT(*) FROM lake_ducklake.table
        WHERE table_id = 10
        AND begin_snapshot <= 1
        AND (end_snapshot IS NULL OR end_snapshot > 1)
    """)
    assert pg_cursor.fetchone()[0] == 1

    # Table should not exist at snapshot 2
    pg_cursor.execute("""
        SELECT COUNT(*) FROM lake_ducklake.table
        WHERE table_id = 10
        AND begin_snapshot <= 2
        AND (end_snapshot IS NULL OR end_snapshot > 2)
    """)
    assert pg_cursor.fetchone()[0] == 0


def test_data_file_lifecycle(pg_cursor):
    """Test data file creation and marking as deleted."""
    # Setup
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name)
        VALUES (11, gen_random_uuid(), 0, 's11');

        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name)
        VALUES (11, gen_random_uuid(), 0, 11, 't11');
    """)

    # Add file at snapshot 1
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.data_file
        (data_file_id, table_id, begin_snapshot, path, record_count, file_size_bytes)
        VALUES (10, 11, 1, '/f1.parquet', 100, 1000)
    """)

    # Mark deleted at snapshot 2
    pg_cursor.execute("""
        UPDATE lake_ducklake.data_file
        SET end_snapshot = 2
        WHERE data_file_id = 10
    """)
    pg_cursor.connection.commit()

    # File visible at snapshot 1
    pg_cursor.execute("""
        SELECT COUNT(*) FROM lake_ducklake.data_file
        WHERE data_file_id = 10 AND begin_snapshot <= 1
        AND (end_snapshot IS NULL OR end_snapshot > 1)
    """)
    assert pg_cursor.fetchone()[0] == 1

    # File not visible at snapshot 2
    pg_cursor.execute("""
        SELECT COUNT(*) FROM lake_ducklake.data_file
        WHERE data_file_id = 10 AND begin_snapshot <= 2
        AND (end_snapshot IS NULL OR end_snapshot > 2)
    """)
    assert pg_cursor.fetchone()[0] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

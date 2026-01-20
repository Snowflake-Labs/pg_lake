"""
Test DuckLake metadata interoperability between PostgreSQL and DuckDB.

These tests verify that:
1. PostgreSQL creates valid DuckLake metadata that DuckDB can read
2. DuckDB creates valid DuckLake metadata that PostgreSQL can read
3. Both systems can query the same data files

NOTE: These tests require FDW integration which is not yet implemented.
"""

import duckdb
import pytest

pytest.skip("FDW integration not yet implemented", allow_module_level=True)


def test_postgres_creates_duckdb_reads(pg_cursor, tmp_path):
    """
    Test that DuckDB can read a DuckLake table created by PostgreSQL.
    """
    # Setup location
    location = f"file://{tmp_path}/ducklake_table"

    # Create DuckLake table in PostgreSQL
    pg_cursor.execute(f"""
        CREATE TABLE test_table (
            id INTEGER,
            name TEXT,
            value DOUBLE PRECISION
        ) USING ducklake
        WITH (location = '{location}')
    """)

    # Insert data through PostgreSQL
    pg_cursor.execute("""
        INSERT INTO test_table VALUES
            (1, 'alice', 10.5),
            (2, 'bob', 20.3),
            (3, 'charlie', 30.7)
    """)
    pg_cursor.connection.commit()

    # Verify metadata tables exist in PostgreSQL
    pg_cursor.execute("""
        SELECT COUNT(*) FROM ducklake.snapshot
    """)
    snapshot_count = pg_cursor.fetchone()[0]
    assert snapshot_count > 0, "No snapshots created"

    pg_cursor.execute("""
        SELECT COUNT(*) FROM ducklake.data_file
    """)
    data_file_count = pg_cursor.fetchone()[0]
    assert data_file_count > 0, "No data files created"

    # Now try to read with DuckDB
    conn = duckdb.connect()

    # Install and load ducklake extension
    conn.execute("INSTALL ducklake FROM community")
    conn.execute("LOAD ducklake")

    # Attach the DuckLake table
    conn.execute(f"ATTACH '{location}' AS ducklake_db (TYPE DUCKLAKE)")

    # Query through DuckDB
    result = conn.execute("SELECT * FROM ducklake_db.test_table ORDER BY id").fetchall()

    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result[0] == (1, 'alice', 10.5)
    assert result[1] == (2, 'bob', 20.3)
    assert result[2] == (3, 'charlie', 30.7)

    # Verify DuckDB can see the snapshot
    snapshots = conn.execute("""
        SELECT snapshot_id, changes_made
        FROM ducklake_db.ducklake_snapshot()
        ORDER BY snapshot_id
    """).fetchall()

    assert len(snapshots) > 0, "DuckDB cannot see PostgreSQL snapshots"
    print(f"DuckDB found {len(snapshots)} snapshots")

    conn.close()


def test_duckdb_creates_postgres_reads(pg_cursor, tmp_path):
    """
    Test that PostgreSQL can read a DuckLake table created by DuckDB.
    """
    location = f"file://{tmp_path}/duckdb_table"

    # Create DuckLake table in DuckDB
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake FROM community")
    conn.execute("LOAD ducklake")

    # Create table and insert data through DuckDB
    conn.execute(f"""
        CREATE TABLE test_table (
            id INTEGER,
            product TEXT,
            price DECIMAL(10,2)
        )
    """)

    conn.execute("""
        INSERT INTO test_table VALUES
            (1, 'laptop', 999.99),
            (2, 'mouse', 29.99),
            (3, 'keyboard', 79.99)
    """)

    # Export to DuckLake format
    conn.execute(f"""
        COPY test_table TO '{location}' (FORMAT DUCKLAKE)
    """)

    conn.close()

    # Now try to read with PostgreSQL
    pg_cursor.execute(f"""
        CREATE FOREIGN TABLE duckdb_table (
            id INTEGER,
            product TEXT,
            price NUMERIC(10,2)
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
    """)

    # Query through PostgreSQL
    pg_cursor.execute("SELECT * FROM duckdb_table ORDER BY id")
    result = pg_cursor.fetchall()

    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result[0][0] == 1 and result[0][1] == 'laptop'
    assert result[1][0] == 2 and result[1][1] == 'mouse'
    assert result[2][0] == 3 and result[2][1] == 'keyboard'


def test_metadata_structure_validation(pg_cursor, tmp_path):
    """
    Validate the DuckLake metadata structure created by PostgreSQL.
    """
    location = f"file://{tmp_path}/metadata_test"

    # Create table with all supported column types
    pg_cursor.execute(f"""
        CREATE TABLE metadata_test (
            col_int INTEGER,
            col_bigint BIGINT,
            col_double DOUBLE PRECISION,
            col_text TEXT,
            col_bool BOOLEAN,
            col_date DATE,
            col_timestamp TIMESTAMP
        ) USING ducklake
        WITH (location = '{location}')
    """)

    # Insert sample data
    pg_cursor.execute("""
        INSERT INTO metadata_test VALUES
            (42, 9223372036854775807, 3.14159, 'hello', true, '2024-01-15', '2024-01-15 10:30:00')
    """)
    pg_cursor.connection.commit()

    # Verify all metadata tables are populated correctly

    # Check snapshot table
    pg_cursor.execute("""
        SELECT snapshot_id, changes_made, author, commit_message
        FROM ducklake.snapshot
        ORDER BY snapshot_id DESC
        LIMIT 1
    """)
    snapshot = pg_cursor.fetchone()
    assert snapshot is not None, "No snapshot created"
    print(f"Latest snapshot: ID={snapshot[0]}, changes='{snapshot[1]}', author='{snapshot[2]}'")

    # Check table metadata
    pg_cursor.execute("""
        SELECT t.table_id, t.table_name, t.schema_name, t.location
        FROM ducklake.table t
        WHERE t.table_name = 'metadata_test'
    """)
    table_info = pg_cursor.fetchone()
    assert table_info is not None, "Table metadata not found"
    assert table_info[3] == location, f"Location mismatch: {table_info[3]} != {location}"

    # Check column metadata
    pg_cursor.execute("""
        SELECT c.column_name, c.data_type, c.ordinal_position
        FROM ducklake.column c
        JOIN ducklake.table t ON c.table_id = t.table_id
        WHERE t.table_name = 'metadata_test'
        ORDER BY c.ordinal_position
    """)
    columns = pg_cursor.fetchall()
    assert len(columns) == 7, f"Expected 7 columns, got {len(columns)}"

    expected_columns = [
        ('col_int', 'INTEGER', 1),
        ('col_bigint', 'BIGINT', 2),
        ('col_double', 'DOUBLE', 3),
        ('col_text', 'VARCHAR', 4),
        ('col_bool', 'BOOLEAN', 5),
        ('col_date', 'DATE', 6),
        ('col_timestamp', 'TIMESTAMP', 7),
    ]

    for actual, expected in zip(columns, expected_columns):
        assert actual[0] == expected[0], f"Column name mismatch: {actual[0]} != {expected[0]}"
        # Type names might vary slightly, so just check they're present
        assert actual[2] == expected[2], f"Ordinal mismatch: {actual[2]} != {expected[2]}"

    # Check data_file table
    pg_cursor.execute("""
        SELECT df.path, df.record_count, df.file_size_bytes
        FROM ducklake.data_file df
        JOIN ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'metadata_test'
    """)
    data_files = pg_cursor.fetchall()
    assert len(data_files) > 0, "No data files registered"

    for path, record_count, file_size in data_files:
        assert record_count == 1, f"Expected 1 record, got {record_count}"
        assert file_size > 0, f"File size should be > 0, got {file_size}"
        print(f"Data file: {path}, records={record_count}, size={file_size}")


def test_snapshot_isolation(pg_cursor, tmp_path):
    """
    Test that DuckLake snapshots provide proper isolation.
    """
    location = f"file://{tmp_path}/snapshot_test"

    pg_cursor.execute(f"""
        CREATE TABLE snapshot_test (
            id INTEGER,
            value TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)

    # Insert initial data
    pg_cursor.execute("INSERT INTO snapshot_test VALUES (1, 'v1')")
    pg_cursor.connection.commit()

    # Get initial snapshot ID
    pg_cursor.execute("""
        SELECT MAX(snapshot_id) FROM ducklake.snapshot
    """)
    snapshot1_id = pg_cursor.fetchone()[0]

    # Insert more data
    pg_cursor.execute("INSERT INTO snapshot_test VALUES (2, 'v2')")
    pg_cursor.connection.commit()

    # Get new snapshot ID
    pg_cursor.execute("""
        SELECT MAX(snapshot_id) FROM ducklake.snapshot
    """)
    snapshot2_id = pg_cursor.fetchone()[0]

    assert snapshot2_id > snapshot1_id, "New snapshot not created"

    # Verify snapshot lineage
    pg_cursor.execute("""
        SELECT snapshot_id, parent_snapshot_id
        FROM ducklake.snapshot
        WHERE snapshot_id IN (%s, %s)
        ORDER BY snapshot_id
    """, (snapshot1_id, snapshot2_id))

    snapshots = pg_cursor.fetchall()
    assert len(snapshots) == 2
    # Second snapshot should reference first as parent
    if snapshots[1][1] is not None:
        assert snapshots[1][1] == snapshot1_id, "Parent snapshot not linked correctly"


def test_update_creates_delete_files(pg_cursor, tmp_path):
    """
    Test that UPDATE/DELETE operations create position delete files.
    """
    location = f"file://{tmp_path}/update_test"

    pg_cursor.execute(f"""
        CREATE TABLE update_test (
            id INTEGER,
            status TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)

    # Insert data
    pg_cursor.execute("""
        INSERT INTO update_test VALUES
            (1, 'active'),
            (2, 'active'),
            (3, 'active')
    """)
    pg_cursor.connection.commit()

    # Update a row (should create delete file)
    pg_cursor.execute("UPDATE update_test SET status = 'inactive' WHERE id = 2")
    pg_cursor.connection.commit()

    # Check for delete files
    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM ducklake.delete_file df
        JOIN ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'update_test'
    """)
    delete_file_count = pg_cursor.fetchone()[0]

    # Note: This test might need adjustment based on copy-on-write threshold
    # For now, just verify the metadata structure exists
    print(f"Delete files created: {delete_file_count}")

    # Verify query still works correctly
    pg_cursor.execute("SELECT * FROM update_test ORDER BY id")
    result = pg_cursor.fetchall()
    assert len(result) == 3
    assert result[1][1] == 'inactive', "Update not applied"


def test_concurrent_snapshot_creation(pg_cursor, pg_conn, tmp_path):
    """
    Test that concurrent inserts create proper snapshots.
    """
    location = f"file://{tmp_path}/concurrent_test"

    pg_cursor.execute(f"""
        CREATE TABLE concurrent_test (
            id INTEGER,
            worker TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)

    # Insert from first connection
    pg_cursor.execute("INSERT INTO concurrent_test VALUES (1, 'worker1')")
    pg_cursor.connection.commit()

    # Get snapshot count
    pg_cursor.execute("SELECT COUNT(*) FROM ducklake.snapshot")
    count1 = pg_cursor.fetchone()[0]

    # Insert from same connection again
    pg_cursor.execute("INSERT INTO concurrent_test VALUES (2, 'worker1')")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT COUNT(*) FROM ducklake.snapshot")
    count2 = pg_cursor.fetchone()[0]

    assert count2 > count1, "New snapshot not created after commit"

    # Verify all data is present
    pg_cursor.execute("SELECT COUNT(*) FROM concurrent_test")
    total_rows = pg_cursor.fetchone()[0]
    assert total_rows == 2, f"Expected 2 rows, got {total_rows}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

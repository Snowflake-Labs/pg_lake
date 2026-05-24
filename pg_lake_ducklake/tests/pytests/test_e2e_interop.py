"""
End-to-end DuckLake interoperability tests between PostgreSQL and DuckDB.

These tests verify that:
1. PostgreSQL can create DuckLake tables with real data
2. DuckDB can read PostgreSQL-created DuckLake tables
3. Metadata is compatible between both systems
"""

import pytest
from utils_pytest import TEST_BUCKET, server_params

# Check if duckdb is available
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_create_ducklake_table_s3(pg_cursor):
    """
    Test creating a DuckLake table with S3 location.
    Tables are created as foreign tables using the DuckLake FDW.
    """
    location = f"s3://{TEST_BUCKET}/test_table"

    pg_cursor.execute(f"""
        CREATE TABLE test_s3_table (
            id INTEGER,
            name TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.connection.commit()

    # Verify foreign table was created
    pg_cursor.execute("""
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_name = 'test_s3_table'
    """)
    result = pg_cursor.fetchone()
    assert result is not None, "Foreign table was not created"

    # Verify it's using the correct server
    pg_cursor.execute("""
        SELECT foreign_server_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_name = 'test_s3_table'
    """)
    server = pg_cursor.fetchone()
    assert server[0] == 'pg_lake_ducklake', f"Wrong server: {server[0]}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_metadata_structure_after_create(pg_cursor):
    """
    Test that creating a DuckLake table populates metadata tables.
    """
    location = f"s3://{TEST_BUCKET}/metadata_test"

    pg_cursor.execute(f"""
        CREATE TABLE metadata_test (
            id INTEGER,
            value TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("""
        SELECT table_name FROM lake_ducklake.tables
        WHERE table_name = 'metadata_test'
    """)
    rows = pg_cursor.fetchall()
    assert len(rows) == 1, f"expected metadata_test in lake_ducklake.tables, got {rows}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_insert_data_to_ducklake_table(pg_cursor, s3):
    """
    Test inserting data into a DuckLake table. After commit, the data
    file is registered in lake_ducklake.data_file with the correct row
    count and stats.
    """
    location = f"s3://{TEST_BUCKET}/insert_test"

    pg_cursor.execute(f"""
        CREATE TABLE insert_test (
            id INTEGER,
            name TEXT,
            value DOUBLE PRECISION
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("""
        INSERT INTO insert_test VALUES
            (1, 'alice', 10.5),
            (2, 'bob', 20.3)
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT COUNT(*) FROM insert_test")
    count = pg_cursor.fetchone()[0]
    assert count == 2, f"Expected 2 rows, got {count}"

    pg_cursor.execute(
        "SELECT COUNT(*), SUM(record_count) FROM lake_ducklake.data_file df "
        "JOIN lake_ducklake.\"table\" t USING (table_id) "
        "WHERE t.table_name = 'insert_test'"
    )
    data_files, total_records = pg_cursor.fetchone()
    assert data_files == 1
    assert total_records == 2


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_reads_postgres_metadata(pg_cursor, s3):
    """
    Test that DuckDB can read DuckLake metadata that PostgreSQL wrote
    via INSERT, and surface the rows over the lake.
    """
    location = f"s3://{TEST_BUCKET}/shared_table"
    pg_cursor.execute("DROP TABLE IF EXISTS shared_table")
    pg_cursor.execute(f"""
        CREATE TABLE shared_table (
            id INTEGER,
            name TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.execute("INSERT INTO shared_table VALUES (1, 'test'), (2, 'two')")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )

    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    rows = duck.execute(
        "SELECT id, name FROM dl.public.shared_table ORDER BY id"
    ).fetchall()
    assert rows == [(1, 'test'), (2, 'two')], rows


def test_manual_metadata_creation_for_duckdb(pg_cursor, tmp_path):
    """
    Test manually creating DuckLake metadata that DuckDB can read.

    This verifies our metadata schema matches DuckDB's expectations.
    """
    if not DUCKDB_AVAILABLE:
        pytest.skip("DuckDB not installed")

    # Manually populate metadata tables to simulate a complete DuckLake table
    pg_cursor.execute("""
        -- Create schema entry
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name, path)
        VALUES (100, gen_random_uuid(), 0, 'default', NULL);

        -- Create table entry
        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name, path)
        VALUES (100, gen_random_uuid(), 0, 100, 'manual_table', NULL);

        -- Create column entries
        INSERT INTO lake_ducklake.column
        (column_id, begin_snapshot, table_id, column_order, column_name, column_type)
        VALUES
        (1, 0, 100, 0, 'id', 'INTEGER'),
        (2, 0, 100, 1, 'name', 'VARCHAR');
    """)
    pg_cursor.connection.commit()

    # Verify metadata was created correctly
    pg_cursor.execute("""
        SELECT t.table_name, s.schema_name, COUNT(c.column_id) as col_count
        FROM lake_ducklake.table t
        JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id
        LEFT JOIN lake_ducklake.column c ON t.table_id = c.table_id
        WHERE t.table_id = 100
        GROUP BY t.table_name, s.schema_name
    """)
    result = pg_cursor.fetchone()
    assert result[0] == 'manual_table'
    assert result[1] == 'default'
    assert result[2] == 2, "Should have 2 columns"

    # Verify using the view
    pg_cursor.execute("""
        SELECT table_name
        FROM public.ducklake_table
        WHERE table_name = 'manual_table'
    """)
    view_result = pg_cursor.fetchone()
    assert view_result is not None, "Table should appear in ducklake_table view"


def test_snapshot_creation_workflow(pg_cursor):
    """
    Test the snapshot creation workflow for DuckLake tables.
    """
    # Create a snapshot for a table modification
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.snapshot
        (snapshot_id, schema_version, next_catalog_id, next_file_id)
        VALUES (200, 0, 101, 1)
    """)

    # Add snapshot changes metadata
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.snapshot_changes
        (snapshot_id, changes_made, author)
        VALUES (200, 'INSERT', 'test_user')
    """)
    pg_cursor.connection.commit()

    # Verify snapshot query works
    pg_cursor.execute("""
        SELECT * FROM lake_ducklake.snapshots('test_catalog')
        WHERE snapshot_id = 200
    """)
    result = pg_cursor.fetchone()
    assert result is not None
    assert result[0] == 200  # snapshot_id


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

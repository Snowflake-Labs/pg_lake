"""
Pytest configuration and fixtures for pg_lake_ducklake tests.
"""

import os
import pytest
import psycopg2
from pathlib import Path


@pytest.fixture(scope="session")
def pg_conn():
    """
    Create a PostgreSQL connection for the test session.
    """
    # Use environment variables or defaults
    dbname = os.environ.get("PGDATABASE", "postgres")
    user = os.environ.get("PGUSER", "postgres")
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port
    )
    conn.autocommit = False

    # Ensure extension is loaded
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_lake_ducklake CASCADE")
        conn.commit()

    yield conn
    conn.close()


@pytest.fixture
def pg_cursor(pg_conn):
    """
    Create a cursor for PostgreSQL queries.
    """
    cursor = pg_conn.cursor()
    yield cursor
    pg_conn.rollback()  # Rollback any uncommitted changes
    cursor.close()


@pytest.fixture
def tmp_path(tmp_path_factory):
    """
    Create a temporary directory for test data.
    """
    return tmp_path_factory.mktemp("ducklake_test")


@pytest.fixture(autouse=True)
def cleanup_test_tables(pg_cursor):
    """
    Clean up test tables and metadata after each test.
    """
    yield

    # Rollback any failed transactions
    try:
        pg_cursor.connection.rollback()
    except:
        pass

    # Clean up DuckLake metadata (keep only initial snapshot)
    try:
        pg_cursor.execute("DELETE FROM lake_ducklake.snapshot_changes WHERE snapshot_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.data_file WHERE data_file_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.column WHERE column_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.table WHERE table_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.schema WHERE schema_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.snapshot WHERE snapshot_id > 0")
        pg_cursor.connection.commit()
    except Exception as e:
        print(f"Warning: Could not clean metadata: {e}")
        pg_cursor.connection.rollback()

    # Drop any foreign tables from public schema
    try:
        pg_cursor.execute("""
            SELECT foreign_table_name
            FROM information_schema.foreign_tables
            WHERE foreign_table_schema = 'public'
            AND (foreign_table_name LIKE '%_test%' OR foreign_table_name LIKE 'test_%')
        """)

        foreign_tables = pg_cursor.fetchall()
        for table in foreign_tables:
            try:
                pg_cursor.execute(f"DROP FOREIGN TABLE IF EXISTS {table[0]} CASCADE")
            except Exception as e:
                print(f"Warning: Could not drop foreign table {table[0]}: {e}")

        pg_cursor.connection.commit()
    except Exception as e:
        print(f"Warning: Could not clean foreign tables: {e}")
        pg_cursor.connection.rollback()

    # Drop any test tables from public schema
    try:
        pg_cursor.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename LIKE '%_test%'
        """)

        tables = pg_cursor.fetchall()
        for table in tables:
            try:
                pg_cursor.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")
            except Exception as e:
                print(f"Warning: Could not drop table {table[0]}: {e}")

        pg_cursor.connection.commit()
    except Exception as e:
        print(f"Warning: Could not clean tables: {e}")
        pg_cursor.connection.rollback()


@pytest.fixture
def sample_ducklake_table(pg_cursor, tmp_path):
    """
    Create a sample DuckLake table for testing.
    Returns the table name and location.
    """
    table_name = "sample_test"
    location = f"file://{tmp_path}/sample_table"

    pg_cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            name TEXT,
            value DOUBLE PRECISION
        ) USING ducklake
        WITH (location = '{location}')
    """)

    # Insert sample data
    pg_cursor.execute(f"""
        INSERT INTO {table_name} VALUES
            (1, 'one', 1.1),
            (2, 'two', 2.2),
            (3, 'three', 3.3)
    """)
    pg_cursor.connection.commit()

    yield table_name, location

    # Cleanup
    pg_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_cursor.connection.commit()

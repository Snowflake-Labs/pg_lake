import pytest
from utils_pytest import *


def test_create_ducklake_table(s3, pg_conn, extension):
    """Test basic DuckLake table creation"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_create_table/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_basic(
            id BIGINT,
            name TEXT,
            value NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Verify table exists
    result = run_query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test_ducklake_basic'",
        pg_conn,
    )
    assert len(result) == 1

    # Verify table is registered in DuckLake catalog
    result = run_query(
        """
        SELECT table_name, table_schema, location
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_basic'
        """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0][0] == "test_ducklake_basic"
    assert result[0][1] == "public"
    assert location in result[0][2]  # location

    run_command("DROP FOREIGN TABLE test_ducklake_basic", pg_conn)
    pg_conn.rollback()


def test_create_ducklake_table_with_schema(s3, pg_conn, extension):
    """Test DuckLake table creation in custom schema"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_create_schema/"

    run_command("CREATE SCHEMA test_schema", pg_conn)

    run_command(
        f"""
        CREATE FOREIGN TABLE test_schema.test_ducklake_schema(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Verify table is registered with correct schema
    result = run_query(
        """
        SELECT table_name, table_schema
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_schema'
        """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0][1] == "test_schema"

    run_command("DROP SCHEMA test_schema CASCADE", pg_conn)
    pg_conn.rollback()


def test_create_ducklake_table_various_types(s3, pg_conn, extension):
    """Test DuckLake table with various data types"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_various_types/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_types(
            col_bigint BIGINT,
            col_int INT,
            col_smallint SMALLINT,
            col_text TEXT,
            col_varchar VARCHAR(100),
            col_numeric NUMERIC(10,2),
            col_float FLOAT,
            col_double DOUBLE PRECISION,
            col_boolean BOOLEAN,
            col_date DATE,
            col_timestamp TIMESTAMP
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Verify table structure
    result = run_query(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'test_ducklake_types'
        ORDER BY ordinal_position
        """,
        pg_conn,
    )
    assert len(result) == 11

    run_command("DROP FOREIGN TABLE test_ducklake_types", pg_conn)
    pg_conn.rollback()


def test_create_multiple_ducklake_tables(s3, pg_conn, extension):
    """Test creating multiple DuckLake tables"""
    location1 = f"s3://{TEST_BUCKET}/ducklake/test_multi1/"
    location2 = f"s3://{TEST_BUCKET}/ducklake/test_multi2/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_multi1(id BIGINT, name TEXT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location1}')
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_multi2(id BIGINT, value NUMERIC)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location2}')
        """,
        pg_conn,
    )

    # Verify both tables are registered
    result = run_query(
        """
        SELECT table_name
        FROM lake_ducklake.tables
        WHERE table_name IN ('test_ducklake_multi1', 'test_ducklake_multi2')
        ORDER BY table_name
        """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0][0] == "test_ducklake_multi1"
    assert result[1][0] == "test_ducklake_multi2"

    run_command("DROP FOREIGN TABLE test_ducklake_multi1, test_ducklake_multi2", pg_conn)
    pg_conn.rollback()


def test_create_ducklake_table_if_not_exists(s3, pg_conn, extension):
    """Test CREATE IF NOT EXISTS"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_if_not_exists/"

    run_command(
        f"""
        CREATE FOREIGN TABLE IF NOT EXISTS test_ducklake_exists(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Second create should not error
    run_command(
        f"""
        CREATE FOREIGN TABLE IF NOT EXISTS test_ducklake_exists(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Should only have one table registered
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_exists'
        """,
        pg_conn,
    )
    assert result[0][0] == 1

    run_command("DROP FOREIGN TABLE test_ducklake_exists", pg_conn)
    pg_conn.rollback()

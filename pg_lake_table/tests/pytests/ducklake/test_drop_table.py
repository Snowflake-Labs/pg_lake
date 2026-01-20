import pytest
from utils_pytest import *


def test_drop_empty_table(s3, pg_conn, extension):
    """Test DROP on empty DuckLake table"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_drop_empty/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_empty(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    table_id = run_query(
        """
        SELECT table_id
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_drop_empty'
        """,
        pg_conn,
    )[0][0]

    # Drop table
    run_command("DROP FOREIGN TABLE test_ducklake_drop_empty", pg_conn)
    pg_conn.commit()

    # Verify table is marked as ended
    result = run_query(
        f"""
        SELECT end_snapshot
        FROM lake_ducklake.table
        WHERE table_id = {table_id}
        """,
        pg_conn,
    )
    assert result[0][0] is not None

    # Verify table no longer exists in PostgreSQL
    result = run_query(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_ducklake_drop_empty'",
        pg_conn,
    )
    assert result[0][0] == 0


def test_drop_table_with_data(s3, pg_conn, extension):
    """Test DROP on DuckLake table with data"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_drop_with_data/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_data(
            id BIGINT,
            value NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data
    run_command(
        "INSERT INTO test_ducklake_drop_data SELECT i, i * 1.5 FROM generate_series(1, 100) i",
        pg_conn,
    )
    pg_conn.commit()

    # Get table ID before drop
    table_id = run_query(
        """
        SELECT table_id
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_drop_data'
        """,
        pg_conn,
    )[0][0]

    # Verify data files exist
    file_count = run_query(
        f"""
        SELECT COUNT(*)
        FROM lake_ducklake.data_file
        WHERE table_id = {table_id} AND end_snapshot IS NULL
        """,
        pg_conn,
    )[0][0]
    assert file_count > 0

    # Drop table
    run_command("DROP FOREIGN TABLE test_ducklake_drop_data", pg_conn)
    pg_conn.commit()

    # Verify table is marked as ended in metadata
    result = run_query(
        f"""
        SELECT end_snapshot
        FROM lake_ducklake.table
        WHERE table_id = {table_id}
        """,
        pg_conn,
    )
    assert result[0][0] is not None

    # Verify data files still exist (for snapshot history)
    result = run_query(
        f"""
        SELECT COUNT(*)
        FROM lake_ducklake.data_file
        WHERE table_id = {table_id}
        """,
        pg_conn,
    )
    assert result[0][0] == file_count


def test_drop_multiple_tables(s3, pg_conn, extension):
    """Test dropping multiple DuckLake tables at once"""
    location1 = f"s3://{TEST_BUCKET}/ducklake/test_drop_multi1/"
    location2 = f"s3://{TEST_BUCKET}/ducklake/test_drop_multi2/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_multi1(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location1}')
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_multi2(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location2}')
        """,
        pg_conn,
    )

    # Get table IDs
    table_ids = run_query(
        """
        SELECT table_id
        FROM lake_ducklake.tables
        WHERE table_name IN ('test_ducklake_drop_multi1', 'test_ducklake_drop_multi2')
        ORDER BY table_name
        """,
        pg_conn,
    )
    assert len(table_ids) == 2

    # Drop both tables
    run_command("DROP FOREIGN TABLE test_ducklake_drop_multi1, test_ducklake_drop_multi2", pg_conn)
    pg_conn.commit()

    # Verify both tables are marked as ended
    result = run_query(
        f"""
        SELECT COUNT(*)
        FROM lake_ducklake.table
        WHERE table_id IN ({table_ids[0][0]}, {table_ids[1][0]})
        AND end_snapshot IS NOT NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 2


def test_drop_if_exists(s3, pg_conn, extension):
    """Test DROP IF EXISTS"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_drop_if_exists/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_exists(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Drop existing table
    run_command("DROP FOREIGN TABLE IF EXISTS test_ducklake_drop_exists", pg_conn)
    pg_conn.commit()

    # Drop non-existent table (should not error)
    run_command("DROP FOREIGN TABLE IF EXISTS test_ducklake_drop_exists", pg_conn)
    pg_conn.commit()


def test_drop_cascade(s3, pg_conn, extension):
    """Test DROP CASCADE"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_drop_cascade/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_cascade(id BIGINT, name TEXT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Drop with CASCADE
    run_command("DROP FOREIGN TABLE test_ducklake_drop_cascade CASCADE", pg_conn)
    pg_conn.commit()

    # Verify table is gone
    result = run_query(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_ducklake_drop_cascade'",
        pg_conn,
    )
    assert result[0][0] == 0


def test_drop_rollback(s3, pg_conn, extension):
    """Test DROP rollback"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_drop_rollback/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_drop_rollback(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data
    run_command(
        "INSERT INTO test_ducklake_drop_rollback VALUES (1), (2), (3)",
        pg_conn,
    )
    pg_conn.commit()

    # Drop but rollback
    run_command("DROP FOREIGN TABLE test_ducklake_drop_rollback", pg_conn)
    pg_conn.rollback()

    # Verify table still exists
    result = run_query(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_ducklake_drop_rollback'",
        pg_conn,
    )
    assert result[0][0] == 1

    # Verify data is still there
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_drop_rollback",
        pg_conn,
    )
    assert result[0][0] == 3

    # Verify table is still active in metadata
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_drop_rollback'
        """,
        pg_conn,
    )
    assert result[0][0] == 1

    run_command("DROP FOREIGN TABLE test_ducklake_drop_rollback", pg_conn)
    pg_conn.rollback()


def test_drop_with_schema(s3, pg_conn, extension):
    """Test dropping table in custom schema"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_drop_schema/"

    run_command("CREATE SCHEMA test_drop_schema", pg_conn)

    run_command(
        f"""
        CREATE FOREIGN TABLE test_drop_schema.test_ducklake_table(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Get table ID
    table_id = run_query(
        """
        SELECT table_id
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_table'
        AND table_schema = 'test_drop_schema'
        """,
        pg_conn,
    )[0][0]

    # Drop table
    run_command("DROP FOREIGN TABLE test_drop_schema.test_ducklake_table", pg_conn)
    pg_conn.commit()

    # Verify table is marked as ended
    result = run_query(
        f"""
        SELECT end_snapshot
        FROM lake_ducklake.table
        WHERE table_id = {table_id}
        """,
        pg_conn,
    )
    assert result[0][0] is not None

    run_command("DROP SCHEMA test_drop_schema", pg_conn)
    pg_conn.rollback()


def test_drop_schema_cascade(s3, pg_conn, extension):
    """Test dropping schema with CASCADE drops DuckLake tables"""
    location1 = f"s3://{TEST_BUCKET}/ducklake/test_schema_cascade1/"
    location2 = f"s3://{TEST_BUCKET}/ducklake/test_schema_cascade2/"

    run_command("CREATE SCHEMA test_schema_cascade", pg_conn)

    run_command(
        f"""
        CREATE FOREIGN TABLE test_schema_cascade.table1(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location1}')
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_schema_cascade.table2(id BIGINT)
        SERVER pg_lake_ducklake
        OPTIONS (location '{location2}')
        """,
        pg_conn,
    )

    # Get table IDs
    table_ids = run_query(
        """
        SELECT table_id
        FROM lake_ducklake.tables
        WHERE table_schema = 'test_schema_cascade'
        ORDER BY table_name
        """,
        pg_conn,
    )
    assert len(table_ids) == 2

    # Drop schema with CASCADE
    run_command("DROP SCHEMA test_schema_cascade CASCADE", pg_conn)
    pg_conn.commit()

    # Verify both tables are marked as ended
    result = run_query(
        f"""
        SELECT COUNT(*)
        FROM lake_ducklake.table
        WHERE table_id IN ({table_ids[0][0]}, {table_ids[1][0]})
        AND end_snapshot IS NOT NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 2

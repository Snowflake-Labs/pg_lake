import pytest
from utils_pytest import *


def test_insert_basic(s3, pg_conn, extension):
    """Test basic INSERT into DuckLake table"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_insert_basic/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_insert(
            id BIGINT,
            name TEXT,
            value NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert single row
    run_command(
        "INSERT INTO test_ducklake_insert VALUES (1, 'Alice', 100.5)",
        pg_conn,
    )
    pg_conn.commit()

    # Verify data file was created
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_insert' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 1

    # Verify row count
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_insert",
        pg_conn,
    )
    assert result[0][0] == 1

    run_command("DROP FOREIGN TABLE test_ducklake_insert", pg_conn)
    pg_conn.rollback()


def test_insert_multiple_rows(s3, pg_conn, extension):
    """Test inserting multiple rows"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_insert_multiple/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_multi_insert(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert multiple rows
    run_command(
        """
        INSERT INTO test_ducklake_multi_insert VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie'),
        (4, 'David'),
        (5, 'Eve')
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Verify row count
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_multi_insert",
        pg_conn,
    )
    assert result[0][0] == 5

    # Verify data
    result = run_query(
        "SELECT id, name FROM test_ducklake_multi_insert ORDER BY id",
        pg_conn,
    )
    assert result[0] == [1, "Alice"]
    assert result[4] == [5, "Eve"]

    run_command("DROP FOREIGN TABLE test_ducklake_multi_insert", pg_conn)
    pg_conn.rollback()


def test_insert_from_select(s3, pg_conn, extension):
    """Test INSERT from SELECT"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_insert_select/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_insert_select(
            id BIGINT,
            value BIGINT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert from generate_series
    run_command(
        """
        INSERT INTO test_ducklake_insert_select
        SELECT i, i * 10 FROM generate_series(1, 100) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Verify row count
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_insert_select",
        pg_conn,
    )
    assert result[0][0] == 100

    # Verify sample data
    result = run_query(
        "SELECT id, value FROM test_ducklake_insert_select WHERE id = 50",
        pg_conn,
    )
    assert result[0] == [50, 500]

    run_command("DROP FOREIGN TABLE test_ducklake_insert_select", pg_conn)
    pg_conn.rollback()


def test_insert_multiple_batches(s3, pg_conn, extension):
    """Test multiple INSERT operations"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_insert_batches/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_batches(
            id BIGINT,
            batch_num INT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert first batch
    run_command(
        "INSERT INTO test_ducklake_batches SELECT i, 1 FROM generate_series(1, 50) i",
        pg_conn,
    )
    pg_conn.commit()

    # Insert second batch
    run_command(
        "INSERT INTO test_ducklake_batches SELECT i, 2 FROM generate_series(51, 100) i",
        pg_conn,
    )
    pg_conn.commit()

    # Verify total row count
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_batches",
        pg_conn,
    )
    assert result[0][0] == 100

    # Verify data files
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_batches' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 2

    run_command("DROP FOREIGN TABLE test_ducklake_batches", pg_conn)
    pg_conn.rollback()


def test_insert_null_values(s3, pg_conn, extension):
    """Test inserting NULL values"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_insert_nulls/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_nulls(
            id BIGINT,
            nullable_text TEXT,
            nullable_num NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert rows with NULLs
    run_command(
        """
        INSERT INTO test_ducklake_nulls VALUES
        (1, 'text', 10.5),
        (2, NULL, 20.5),
        (3, 'text', NULL),
        (4, NULL, NULL)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Verify NULL handling
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_nulls WHERE nullable_text IS NULL",
        pg_conn,
    )
    assert result[0][0] == 2

    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_nulls WHERE nullable_num IS NULL",
        pg_conn,
    )
    assert result[0][0] == 2

    run_command("DROP FOREIGN TABLE test_ducklake_nulls", pg_conn)
    pg_conn.rollback()


def test_insert_rollback(s3, pg_conn, extension):
    """Test INSERT rollback"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_insert_rollback/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_rollback(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert and commit
    run_command(
        "INSERT INTO test_ducklake_rollback VALUES (1, 'Alice')",
        pg_conn,
    )
    pg_conn.commit()

    # Insert and rollback
    run_command(
        "INSERT INTO test_ducklake_rollback VALUES (2, 'Bob')",
        pg_conn,
    )
    pg_conn.rollback()

    # Verify only first insert is visible
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_rollback",
        pg_conn,
    )
    assert result[0][0] == 1

    result = run_query(
        "SELECT name FROM test_ducklake_rollback",
        pg_conn,
    )
    assert result[0][0] == "Alice"

    run_command("DROP FOREIGN TABLE test_ducklake_rollback", pg_conn)
    pg_conn.rollback()

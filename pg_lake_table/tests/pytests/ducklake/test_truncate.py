import pytest
from utils_pytest import *


def test_truncate_empty_table(s3, pg_conn, extension):
    """Test TRUNCATE on empty DuckLake table"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_truncate_empty/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_truncate_empty(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Truncate empty table should succeed
    run_command("TRUNCATE test_ducklake_truncate_empty", pg_conn)
    pg_conn.commit()

    # Verify no data files
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_empty' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 0

    run_command("DROP FOREIGN TABLE test_ducklake_truncate_empty", pg_conn)
    pg_conn.rollback()


def test_truncate_with_data(s3, pg_conn, extension):
    """Test TRUNCATE removes all data"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_truncate_data/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_truncate_data(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data
    run_command(
        "INSERT INTO test_ducklake_truncate_data SELECT i, 'name_' || i FROM generate_series(1, 100) i",
        pg_conn,
    )
    pg_conn.commit()

    # Verify data exists
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_truncate_data",
        pg_conn,
    )
    assert result[0][0] == 100

    # Get snapshot before truncate
    snapshot_before = run_query(
        "SELECT snapshot_id FROM lake_ducklake.snapshot ORDER BY snapshot_id DESC LIMIT 1",
        pg_conn,
    )[0][0]

    # Truncate table
    run_command("TRUNCATE test_ducklake_truncate_data", pg_conn)
    pg_conn.commit()

    # Verify table is empty
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_truncate_data",
        pg_conn,
    )
    assert result[0][0] == 0

    # Verify data files are marked as ended
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_data'
        AND df.end_snapshot IS NOT NULL
        """,
        pg_conn,
    )
    assert result[0][0] > 0

    # Verify no active data files
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_data' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 0

    run_command("DROP FOREIGN TABLE test_ducklake_truncate_data", pg_conn)
    pg_conn.rollback()


def test_truncate_and_reinsert(s3, pg_conn, extension):
    """Test TRUNCATE followed by new INSERT"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_truncate_reinsert/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_truncate_reinsert(
            id BIGINT,
            value BIGINT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert initial data
    run_command(
        "INSERT INTO test_ducklake_truncate_reinsert SELECT i, i * 10 FROM generate_series(1, 50) i",
        pg_conn,
    )
    pg_conn.commit()

    # Truncate
    run_command("TRUNCATE test_ducklake_truncate_reinsert", pg_conn)
    pg_conn.commit()

    # Insert new data
    run_command(
        "INSERT INTO test_ducklake_truncate_reinsert SELECT i, i * 100 FROM generate_series(1, 30) i",
        pg_conn,
    )
    pg_conn.commit()

    # Verify new data
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_truncate_reinsert",
        pg_conn,
    )
    assert result[0][0] == 30

    result = run_query(
        "SELECT value FROM test_ducklake_truncate_reinsert WHERE id = 10",
        pg_conn,
    )
    assert result[0][0] == 1000  # New data: 10 * 100, not old data: 10 * 10

    run_command("DROP FOREIGN TABLE test_ducklake_truncate_reinsert", pg_conn)
    pg_conn.rollback()


def test_truncate_multiple_data_files(s3, pg_conn, extension):
    """Test TRUNCATE with multiple data files"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_truncate_multi_files/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_truncate_multi(
            id BIGINT,
            batch INT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert multiple batches to create multiple data files
    for batch in range(1, 6):
        run_command(
            f"INSERT INTO test_ducklake_truncate_multi SELECT i, {batch} FROM generate_series(1, 20) i",
            pg_conn,
        )
        pg_conn.commit()

    # Verify multiple data files exist
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_multi' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    file_count_before = result[0][0]
    assert file_count_before >= 5

    # Truncate
    run_command("TRUNCATE test_ducklake_truncate_multi", pg_conn)
    pg_conn.commit()

    # Verify all data files are marked as ended
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_multi' AND df.end_snapshot IS NOT NULL
        """,
        pg_conn,
    )
    assert result[0][0] == file_count_before

    # Verify no active data files
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_multi' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 0

    run_command("DROP FOREIGN TABLE test_ducklake_truncate_multi", pg_conn)
    pg_conn.rollback()


def test_truncate_rollback(s3, pg_conn, extension):
    """Test TRUNCATE rollback"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_truncate_rollback/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_truncate_rollback(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data
    run_command(
        "INSERT INTO test_ducklake_truncate_rollback VALUES (1, 'Alice'), (2, 'Bob')",
        pg_conn,
    )
    pg_conn.commit()

    # Truncate but rollback
    run_command("TRUNCATE test_ducklake_truncate_rollback", pg_conn)
    pg_conn.rollback()

    # Verify data is still there
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_truncate_rollback",
        pg_conn,
    )
    assert result[0][0] == 2

    # Verify data files are still active
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_truncate_rollback' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] > 0

    run_command("DROP FOREIGN TABLE test_ducklake_truncate_rollback", pg_conn)
    pg_conn.rollback()


def test_truncate_cascade(s3, pg_conn, extension):
    """Test TRUNCATE with CASCADE (should just succeed like regular TRUNCATE)"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_truncate_cascade/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_truncate_cascade(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data
    run_command(
        "INSERT INTO test_ducklake_truncate_cascade SELECT i, 'name' FROM generate_series(1, 10) i",
        pg_conn,
    )
    pg_conn.commit()

    # Truncate with CASCADE
    run_command("TRUNCATE test_ducklake_truncate_cascade CASCADE", pg_conn)
    pg_conn.commit()

    # Verify table is empty
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_truncate_cascade",
        pg_conn,
    )
    assert result[0][0] == 0

    run_command("DROP FOREIGN TABLE test_ducklake_truncate_cascade", pg_conn)
    pg_conn.rollback()

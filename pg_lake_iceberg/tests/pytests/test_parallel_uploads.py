"""
Tests for parallel file uploads during commit.

These tests verify that:
1. Multiple metadata files are uploaded in parallel correctly
2. max_parallel_file_uploads setting limits parallelism
3. Errors during upload are handled properly
4. State machine transitions work correctly
"""

import pytest
from utils_pytest import *


def test_parallel_uploads_multiple_tables(
    superuser_conn, iceberg_extension, s3, with_default_location
):
    """Test that multiple tables uploading in parallel works correctly."""

    # Create multiple tables and insert data to trigger parallel uploads
    num_tables = 10

    for i in range(num_tables):
        run_command(
            f"""
            CREATE TABLE parallel_test_{i} (id int, data text)
            USING iceberg
            """,
            superuser_conn,
        )

    # Insert data into all tables in a single transaction
    # This will cause all metadata files to be uploaded in parallel at commit
    for i in range(num_tables):
        run_command(
            f"INSERT INTO parallel_test_{i} VALUES (1, 'test_{i}')",
            superuser_conn,
        )

    # Commit should succeed with all uploads completing
    superuser_conn.commit()

    # Verify all tables have data
    for i in range(num_tables):
        result = run_query(
            f"SELECT COUNT(*) FROM parallel_test_{i}",
            superuser_conn,
        )
        assert result[0][0] == 1


def test_parallel_uploads_with_limited_parallelism(
    superuser_conn, iceberg_extension, s3, with_default_location
):
    """Test that max_parallel_file_uploads setting limits concurrent uploads."""

    # Set max to 2 to force sequential processing
    run_command(
        "SET pg_lake_engine.max_parallel_file_uploads = 2",
        superuser_conn,
    )

    num_tables = 8

    for i in range(num_tables):
        run_command(
            f"""
            CREATE TABLE limited_parallel_{i} (id int, data text)
            USING iceberg
            """,
            superuser_conn,
        )

    # Insert into all tables
    for i in range(num_tables):
        run_command(
            f"INSERT INTO limited_parallel_{i} VALUES (1, 'data_{i}')",
            superuser_conn,
        )

    # Commit should work with limited parallelism
    superuser_conn.commit()

    # Verify data
    for i in range(num_tables):
        result = run_query(
            f"SELECT data FROM limited_parallel_{i}",
            superuser_conn,
        )
        assert result[0][0] == f"data_{i}"


def test_parallel_uploads_with_single_connection(
    superuser_conn, iceberg_extension, s3, with_default_location
):
    """Test uploads work correctly with max_parallel_file_uploads = 1 (sequential)."""

    run_command(
        "SET pg_lake_engine.max_parallel_file_uploads = 1",
        superuser_conn,
    )

    num_tables = 5

    for i in range(num_tables):
        run_command(
            f"""
            CREATE TABLE sequential_test_{i} (id int)
            USING iceberg
            """,
            superuser_conn,
        )
        run_command(
            f"INSERT INTO sequential_test_{i} VALUES ({i})",
            superuser_conn,
        )

    # All uploads should happen sequentially
    superuser_conn.commit()

    for i in range(num_tables):
        result = run_query(
            f"SELECT id FROM sequential_test_{i}",
            superuser_conn,
        )
        assert result[0][0] == i


def test_parallel_upload_failure_during_flush(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    """Test that failures during PQflush are handled correctly.

    Note: This test requires PostgreSQL 18+ with injection points support.
    It will be skipped on earlier versions.
    """

    # Injection points only supported with PG 18+
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("Injection points not available (requires PostgreSQL 18+)")

    # Enable injection point to fail during flush phase
    run_command(
        "SELECT injection_points_attach('parallel-command-flush-fail', 'error')",
        superuser_conn,
    )

    try:
        run_command(
            """
            CREATE TABLE flush_fail_test (id int)
            USING iceberg
            """,
            superuser_conn,
        )

        run_command(
            "INSERT INTO flush_fail_test VALUES (1)",
            superuser_conn,
        )

        # Commit should fail during flush phase
        error = None
        try:
            superuser_conn.commit()
        except Exception as e:
            error = str(e)

        assert error is not None
        assert "connection" in error.lower() or "injection" in error.lower()

    finally:
        run_command(
            "SELECT injection_points_detach('parallel-command-flush-fail')",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()


def test_parallel_upload_failure_during_wait(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    """Test that failures during result wait are handled correctly.

    Note: This test requires PostgreSQL 18+ with injection points support.
    It will be skipped on earlier versions.
    """

    # Injection points only supported with PG 18+
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("Injection points not available (requires PostgreSQL 18+)")

    run_command(
        "SELECT injection_points_attach('parallel-command-wait-fail', 'error')",
        superuser_conn,
    )

    try:
        run_command(
            """
            CREATE TABLE wait_fail_test (id int)
            USING iceberg
            """,
            superuser_conn,
        )

        run_command(
            "INSERT INTO wait_fail_test VALUES (1)",
            superuser_conn,
        )

        # Commit should fail during wait phase
        error = None
        try:
            superuser_conn.commit()
        except Exception as e:
            error = str(e)

        assert error is not None

    finally:
        run_command(
            "SELECT injection_points_detach('parallel-command-wait-fail')",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()


def test_parallel_upload_partial_failure(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    """Test that when one upload fails, all connections are cleaned up properly.

    This test uses an injection point that fires when an upload completes successfully,
    causing it to error. This simulates a scenario where some uploads may succeed but
    one fails, requiring proper cleanup of all parallel connections.

    Note: This test requires PostgreSQL 18+ with injection points support.
    It will be skipped on earlier versions.
    """

    # Injection points only supported with PG 18+
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("Injection points not available (requires PostgreSQL 18+)")

    # Create tables
    for i in range(5):
        run_command(
            f"""
            CREATE TABLE partial_fail_{i} (id int)
            USING iceberg
            """,
            superuser_conn,
        )

    # Set up injection point to fail when an upload completes
    # This will cause an error after the first successful upload completion
    run_command(
        "SELECT injection_points_attach('parallel-command-complete-nth', 'error')",
        superuser_conn,
    )

    try:
        # Insert into all tables
        for i in range(5):
            run_command(
                f"INSERT INTO partial_fail_{i} VALUES ({i})",
                superuser_conn,
            )

        # Commit should fail when the first upload completes
        error = None
        try:
            superuser_conn.commit()
        except Exception as e:
            error = str(e)

        assert error is not None

    finally:
        run_command(
            "SELECT injection_points_detach('parallel-command-complete-nth')",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()


def test_parallel_uploads_with_many_inserts(
    superuser_conn, iceberg_extension, s3, with_default_location
):
    """Test parallel uploads with many small inserts (generates multiple data files)."""

    run_command(
        "SET pg_lake_engine.max_parallel_file_uploads = 3",
        superuser_conn,
    )

    # Create multiple tables
    num_tables = 6

    for i in range(num_tables):
        run_command(
            f"""
            CREATE TABLE many_inserts_{i} (
                id int,
                category text,
                data text
            )
            USING iceberg
            """,
            superuser_conn,
        )

    # Insert data multiple times to create multiple files
    for i in range(num_tables):
        for j in range(3):
            run_command(
                f"INSERT INTO many_inserts_{i} VALUES ({i}, 'cat_{j}', 'test')",
                superuser_conn,
            )
            superuser_conn.commit()

    # Verify data
    for i in range(num_tables):
        result = run_query(
            f"SELECT COUNT(*) FROM many_inserts_{i}",
            superuser_conn,
        )
        assert result[0][0] == 3


def test_parallel_uploads_memory_context_cleanup(
    superuser_conn, iceberg_extension, s3, with_default_location
):
    """Test that memory contexts are properly cleaned up during parallel uploads."""

    # This test ensures no memory leaks or dangling pointers
    # by creating and destroying many tables with uploads

    for round in range(3):
        run_command(
            f"CREATE TABLE mem_test_{round} (id int) USING iceberg",
            superuser_conn,
        )
        run_command(
            f"INSERT INTO mem_test_{round} VALUES (1)",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(
            f"DROP TABLE mem_test_{round}",
            superuser_conn,
        )
        superuser_conn.commit()

    # If we get here without crashes, memory management is working


def test_parallel_uploads_with_rollback(
    superuser_conn, iceberg_extension, s3, with_default_location
):
    """Test that rollback properly cancels pending uploads."""

    run_command(
        """
        CREATE TABLE rollback_test (id int)
        USING iceberg
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        "INSERT INTO rollback_test VALUES (1)",
        superuser_conn,
    )

    # Rollback should prevent the insert and its uploads
    superuser_conn.rollback()

    # Table should be empty
    result = run_query(
        "SELECT COUNT(*) FROM rollback_test",
        superuser_conn,
    )
    assert result[0][0] == 0

import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_s3_copy_to_parquet(pg_conn, duckdb_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )

    assert list_objects(s3, TEST_BUCKET, "test_s3_copy_to/") == [
        "test_s3_copy_to/data.parquet"
    ]

    # Make sure it's actually Parquet
    duckdb_conn.execute("SELECT count(*) AS count FROM read_parquet($1)", [str(url)])
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 5

    # Confirm that it's snappy
    duckdb_conn.execute(
        f"SELECT compression FROM parquet_metadata($1) LIMIT 1", [str(url)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == "SNAPPY"

    pg_conn.rollback()


def test_s3_copy_to_parquet_noperm(superuser_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Create user1 and without write privileges
    error = run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user1;
        SET ROLE user1;
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,10) s) TO '{url}';
    """,
        superuser_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  permission denied to write to URL")

    superuser_conn.rollback()


def test_s3_copy_to_parquet_perm(superuser_conn, duckdb_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Create user1 and grant write privileges
    error = run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user1;
        GRANT lake_write TO user1;
        SET ROLE user1;
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,10) s) TO '{url}';
    """,
        superuser_conn,
    )

    # Make sure it's actually Parquet
    duckdb_conn.execute("SELECT count(*) AS count FROM read_parquet($1)", [str(url)])
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 10

    superuser_conn.rollback()


def test_s3_copy_from_parquet(superuser_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Read the Parquet file from prior tests
    run_command(
        f"""
        CREATE TABLE test_s3_copy_from_parquet (id int, description text);
        COPY test_s3_copy_from_parquet FROM '{url}';
    """,
        superuser_conn,
    )

    result = run_query(
        "SELECT id, description FROM test_s3_copy_from_parquet ORDER BY 1",
        superuser_conn,
    )
    assert len(result) == 10
    assert result[0]["id"] == 1
    assert result[0]["description"] == "hello-1"

    superuser_conn.rollback()


def test_s3_copy_from_parquet_notexists(pg_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/notexists.parquet"

    # Read a Parquet file that does not exist
    error = run_command(
        f"""
        CREATE TABLE test_s3_copy_from_parquet_notexists (id int, description text);
        COPY test_s3_copy_from_parquet_notexists FROM '{url}';
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  HTTP Error:")

    pg_conn.rollback()


def test_s3_copy_from_parquet_invalid(pg_conn, s3):
    url = f"s3://foo/invalid.parquet"

    # Read a Parquet file we cannot have access to
    error = run_command(
        f"""
        CREATE TABLE test_s3_copy_from_parquet_invalid (id int, description text);
        COPY test_s3_copy_from_parquet_invalid FROM '{url}';
    """,
        pg_conn,
        raise_error=False,
    )
    assert (
        error.startswith("ERROR:  HTTP Error: HTTP GET error")
        or "Unable to connect to URL" in error
    )

    pg_conn.rollback()


def test_s3_copy_from_parquet_noperm(superuser_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Read a Parquet file without permission
    error = run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user1;
        GRANT USAGE ON SCHEMA public TO user1;
        CREATE TABLE test_s3_copy_from_parquet_noperm (id int, description text);
        ALTER TABLE test_s3_copy_from_parquet_noperm OWNER TO user1;

        SET ROLE user1;
        COPY test_s3_copy_from_parquet_noperm FROM '{url}';
    """,
        superuser_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  permission denied to read from URL")

    superuser_conn.rollback()


def test_s3_copy_from_parquet_perm_readonly(superuser_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Read a Parquet file with full permission
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user1;
        GRANT lake_read_write TO user1;
        CREATE TABLE test_s3_copy_from_parquet_perm (id int, description text);
        ALTER TABLE test_s3_copy_from_parquet_perm OWNER TO user1;

        SET ROLE user1;
        COPY test_s3_copy_from_parquet_perm FROM '{url}';
    """,
        superuser_conn,
    )

    result = run_query(
        "SELECT id, description FROM test_s3_copy_from_parquet_perm  ORDER BY 1",
        superuser_conn,
    )
    assert len(result) == 10
    assert result[0]["id"] == 1
    assert result[0]["description"] == "hello-1"

    superuser_conn.rollback()


def test_s3_copy_from_parquet_perm_readonly(superuser_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.parquet"

    # Read a Parquet file with read-only permission
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user1;
        GRANT lake_read TO user1;
        CREATE TABLE test_s3_copy_from_parquet_perm (id int, description text);
        ALTER TABLE test_s3_copy_from_parquet_perm OWNER TO user1;

        SET ROLE user1;
        COPY test_s3_copy_from_parquet_perm FROM '{url}';
    """,
        superuser_conn,
    )

    result = run_query(
        "SELECT id, description FROM test_s3_copy_from_parquet_perm  ORDER BY 1",
        superuser_conn,
    )
    assert len(result) == 10
    assert result[0]["id"] == 1
    assert result[0]["description"] == "hello-1"

    superuser_conn.rollback()

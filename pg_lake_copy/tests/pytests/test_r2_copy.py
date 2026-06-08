import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_r2_copy_to_parquet(pg_conn, duckdb_conn, r2):
    url = f"r2://{TEST_BUCKET_R2}/test_r2_copy_to/data.parquet"

    # Create a Parquet file in mock R2
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )

    assert list_objects(r2, TEST_BUCKET_R2, "test_r2_copy_to/") == [
        "test_r2_copy_to/data.parquet"
    ]

    # Make sure it's actually Parquet
    duckdb_conn.execute("SELECT count(*) AS count FROM read_parquet($1)", [str(url)])
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 5

    pg_conn.rollback()

    # Read the Parquet file from prior tests
    run_command(
        f"""
        CREATE TABLE test_r2_copy_from_parquet (id int, description text);
        COPY test_r2_copy_from_parquet FROM '{url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT id, description FROM test_r2_copy_from_parquet ORDER BY 1", pg_conn
    )
    assert len(result) == 5
    assert result[0]["id"] == 1
    assert result[0]["description"] == "hello-1"

    pg_conn.rollback()


def test_r2_copy_from_parquet_notexists(pg_conn, r2):
    url = f"r2://{TEST_BUCKET_R2}/test_r2_copy_to/notexists.parquet"

    # Read a Parquet file that does not exist
    error = run_command(
        f"""
        CREATE TABLE test_r2_copy_from_parquet_notexists (id int, description text);
        COPY test_r2_copy_from_parquet_notexists FROM '{url}';
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  HTTP Error: Unable to connect to URL ")

    pg_conn.rollback()


def test_r2_copy_from_parquet_invalid(pg_conn, r2):
    url = f"r2://foo/invalid.parquet"

    # Read a Parquet file we cannot have access to
    error = run_command(
        f"""
        CREATE TABLE test_r2_copy_from_parquet_invalid (id int, description text);
        COPY test_r2_copy_from_parquet_invalid FROM '{url}';
    """,
        pg_conn,
        raise_error=False,
    )
    assert (
        error.startswith("ERROR:  HTTP Error: HTTP GET error")
        or "Unable to connect to URL" in error
        or "Could not establish connection" in error
    )

    pg_conn.rollback()

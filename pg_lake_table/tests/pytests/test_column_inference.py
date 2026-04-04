import pytest
import psycopg2
import time
import duckdb
import math
from decimal import *
from utils_pytest import *


@pytest.fixture(scope="module")
def conn(s3, gcs, pg_conn, extension):
    yield pg_conn


def test_basic_infer(conn):
    url = f"s3://{TEST_BUCKET}/test_create_table_infer/data.parquet"
    wildcard_url = f"s3://{TEST_BUCKET}/test_create_table_infer/*.parquet"

    run_command(
        f"""
        COPY (SELECT 'hello' h, now() "N", 3.4::decimal(18,3) d, s FROM generate_series(1,10) s) TO '{url}';
        CREATE FOREIGN TABLE test_infer () SERVER pg_lake OPTIONS (path '{wildcard_url}');
    """,
        conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_infer'::regclass and attnum > 0 order by attnum
    """,
        conn,
    )
    assert len(result) == 4
    assert result == [
        ["h", "text"],
        ["n", "timestamp with time zone"],
        ["d", "numeric"],
        ["s", "integer"],
    ]

    result = run_query("SELECT count(n) FROM test_infer", conn)
    assert result[0]["count"] == 10

    conn.rollback()


def test_invalid_url(conn):
    url = f"s3://{TEST_BUCKET}/test_create_table_infer/nodata.parquet"

    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_invalid_url () SERVER pg_lake OPTIONS (path '{url}');
    """,
        conn,
        raise_error=False,
    )
    assert "NOT FOUND" in error.upper()

    conn.rollback()


def test_create_table_if_exists(conn):
    url = f"s3://{TEST_BUCKET}/test_create_table_if_table_exists/data.parquet"

    # Create the same table twice
    error = run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h FROM generate_series(1,10) s) TO '{url}';
        CREATE FOREIGN TABLE exists () SERVER pg_lake OPTIONS (path '{url}');
        CREATE FOREIGN TABLE exists () SERVER pg_lake OPTIONS (path '{url}');
    """,
        conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  relation "exists" already exists')

    conn.rollback()

    # IF NOT EXISTS should be ok
    error = run_command(
        f"""
        CREATE FOREIGN TABLE IF NOT EXISTS exists () SERVER pg_lake OPTIONS (path '{url}');
        CREATE FOREIGN TABLE IF NOT EXISTS exists () SERVER pg_lake OPTIONS (path '{url}');
    """,
        conn,
    )

    conn.rollback()


def test_create_table_unicode_column(conn):
    url = f"gs://{TEST_BUCKET_GCS}/test_create_table_unicode_column/data.p"

    # Create the same table twice
    run_command(
        f"""
        COPY (SELECT 'hello' AS "🙂😉👍" FROM generate_series(1,10) s) TO '{url}' WITH (format 'parquet');
        CREATE FOREIGN TABLE test_uni () SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
    """,
        conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_uni'::regclass and attnum > 0 order by attnum
    """,
        conn,
    )
    assert result == [["🙂😉👍", "text"]]

    conn.rollback()

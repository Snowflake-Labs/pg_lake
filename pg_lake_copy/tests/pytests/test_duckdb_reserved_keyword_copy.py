"""
Tests for correct identifier quoting during COPY TO / COPY FROM when table
columns are named after DuckDB reserved keywords.

DuckDB reserves several keywords (PIVOT, QUALIFY, LAMBDA, etc.) that
PostgreSQL does not.  These are legal unquoted column names in PostgreSQL but
require quoting in the SQL that pg_lake generates for pgduck_server.

See: https://github.com/Snowflake-Labs/pg_lake/issues/277
"""

import pytest
from utils_pytest import *


# DuckDB-only reserved keywords that are safe as unquoted PostgreSQL
# identifiers.
DUCKDB_ONLY_RESERVED = [
    "describe",
    "lambda",
    "pivot",
    "pivot_longer",
    "pivot_wider",
    "qualify",
    "show",
    "summarize",
    "unpivot",
]

# Smaller set used in multi-column tests for conciseness.
TEST_KEYWORDS = ["pivot", "qualify", "lambda", "show"]


def test_copy_to_parquet_with_reserved_keyword_columns(pg_conn, s3):
    """
    COPY ... TO parquet must succeed when the table has DuckDB-reserved
    keyword column names, and COPY ... FROM must read the file back correctly.
    """
    url = f"s3://{TEST_BUCKET}/test_kw_copy_parquet/kw_cols.parquet"

    try:
        run_command(
            """
            CREATE TABLE test_kw_copy_src (
                pivot    int,
                qualify  int,
                lambda   int,
                show     int
            );
            INSERT INTO test_kw_copy_src VALUES (1, 2, 3, 4), (10, 20, 30, 40);
            """,
            pg_conn,
        )

        run_command(
            f"COPY test_kw_copy_src TO '{url}' WITH (format 'parquet')",
            pg_conn,
        )

        run_command(
            f"""
            CREATE TABLE test_kw_copy_dst (
                pivot    int,
                qualify  int,
                lambda   int,
                show     int
            );
            COPY test_kw_copy_dst FROM '{url}' WITH (format 'parquet');
            """,
            pg_conn,
        )

        result = run_query(
            "SELECT pivot, qualify, lambda, show FROM test_kw_copy_dst ORDER BY pivot",
            pg_conn,
        )
        assert result == [[1, 2, 3, 4], [10, 20, 30, 40]]
    finally:
        pg_conn.rollback()


def test_copy_to_csv_with_reserved_keyword_columns(pg_conn, s3):
    """
    COPY ... TO CSV and back must work with DuckDB-reserved keyword columns.
    """
    url = f"s3://{TEST_BUCKET}/test_kw_copy_csv/kw_cols.csv"

    try:
        run_command(
            f"""
            CREATE TABLE test_kw_csv_src (pivot int, qualify int, lambda int, show int);
            INSERT INTO test_kw_csv_src VALUES (3, 6, 9, 12), (30, 60, 90, 120);
            COPY test_kw_csv_src TO '{url}' WITH (format 'csv', header on);

            CREATE TABLE test_kw_csv_dst (pivot int, qualify int, lambda int, show int);
            COPY test_kw_csv_dst FROM '{url}' WITH (format 'csv', header on);
            """,
            pg_conn,
        )

        result = run_query(
            "SELECT pivot, qualify, lambda, show FROM test_kw_csv_dst ORDER BY pivot",
            pg_conn,
        )
        assert result == [[3, 6, 9, 12], [30, 60, 90, 120]]
    finally:
        pg_conn.rollback()


def test_copy_to_json_with_reserved_keyword_columns(pg_conn, s3):
    """
    COPY ... TO JSON and back must work with DuckDB-reserved keyword columns.
    """
    url = f"s3://{TEST_BUCKET}/test_kw_copy_json/kw_cols.json"

    try:
        run_command(
            f"""
            CREATE TABLE test_kw_json_src (pivot int, qualify int, lambda int, show int);
            INSERT INTO test_kw_json_src VALUES (7, 14, 21, 28), (70, 140, 210, 280);
            COPY test_kw_json_src TO '{url}' WITH (format 'json');

            CREATE TABLE test_kw_json_dst (pivot int, qualify int, lambda int, show int);
            COPY test_kw_json_dst FROM '{url}' WITH (format 'json');
            """,
            pg_conn,
        )

        result = run_query(
            "SELECT pivot, qualify, lambda, show FROM test_kw_json_dst ORDER BY pivot",
            pg_conn,
        )
        assert result == [[7, 14, 21, 28], [70, 140, 210, 280]]
    finally:
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Parameterised round-trip tests — each keyword, each format
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keyword", DUCKDB_ONLY_RESERVED)
def test_copy_roundtrip_parquet(pg_conn, s3, keyword):
    """Each DuckDB-only reserved keyword survives a parquet round-trip."""
    url = f"s3://{TEST_BUCKET}/test_kw_roundtrip_parquet/{keyword}/data.parquet"

    try:
        run_command(
            f"""
            CREATE TABLE test_kw_pq_src_{keyword} ({keyword} int);
            INSERT INTO test_kw_pq_src_{keyword} VALUES (42);
            COPY test_kw_pq_src_{keyword} TO '{url}' WITH (format 'parquet');

            CREATE TABLE test_kw_pq_dst_{keyword} ({keyword} int);
            COPY test_kw_pq_dst_{keyword} FROM '{url}' WITH (format 'parquet');
            """,
            pg_conn,
        )

        result = run_query(f"SELECT {keyword} FROM test_kw_pq_dst_{keyword}", pg_conn)
        assert result == [[42]]
    finally:
        pg_conn.rollback()


@pytest.mark.parametrize("keyword", DUCKDB_ONLY_RESERVED)
def test_copy_roundtrip_csv(pg_conn, s3, keyword):
    """Each DuckDB-only reserved keyword survives a CSV round-trip."""
    url = f"s3://{TEST_BUCKET}/test_kw_roundtrip_csv/{keyword}/data.csv"

    try:
        run_command(
            f"""
            CREATE TABLE test_kw_csv_src_{keyword} ({keyword} int);
            INSERT INTO test_kw_csv_src_{keyword} VALUES (42);
            COPY test_kw_csv_src_{keyword} TO '{url}' WITH (format 'csv', header on);

            CREATE TABLE test_kw_csv_dst_{keyword} ({keyword} int);
            COPY test_kw_csv_dst_{keyword} FROM '{url}' WITH (format 'csv', header on);
            """,
            pg_conn,
        )

        result = run_query(f"SELECT {keyword} FROM test_kw_csv_dst_{keyword}", pg_conn)
        assert result == [[42]]
    finally:
        pg_conn.rollback()


@pytest.mark.parametrize("keyword", DUCKDB_ONLY_RESERVED)
def test_copy_roundtrip_json(pg_conn, s3, keyword):
    """Each DuckDB-only reserved keyword survives a JSON round-trip."""
    url = f"s3://{TEST_BUCKET}/test_kw_roundtrip_json/{keyword}/data.json"

    try:
        run_command(
            f"""
            CREATE TABLE test_kw_json_src_{keyword} ({keyword} int);
            INSERT INTO test_kw_json_src_{keyword} VALUES (42);
            COPY test_kw_json_src_{keyword} TO '{url}' WITH (format 'json');

            CREATE TABLE test_kw_json_dst_{keyword} ({keyword} int);
            COPY test_kw_json_dst_{keyword} FROM '{url}' WITH (format 'json');
            """,
            pg_conn,
        )

        result = run_query(f"SELECT {keyword} FROM test_kw_json_dst_{keyword}", pg_conn)
        assert result == [[42]]
    finally:
        pg_conn.rollback()

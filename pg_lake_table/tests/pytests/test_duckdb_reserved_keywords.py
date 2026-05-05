"""
Tests for correct quoting of DuckDB-reserved identifiers in pushdown SQL.

DuckDB reserves several keywords that PostgreSQL does not (PIVOT, QUALIFY,
LAMBDA, SUMMARIZE, DESCRIBE, SHOW, UNPIVOT, etc.).  PostgreSQL allows these
as unquoted column names, but when pg_lake generates SQL for pgduck_server
it must quote them.  These tests create tables with such column names and
verify that reads, writes, and filter pushdown all work correctly.

Each test that exercises filter pushdown also verifies via EXPLAIN that:
  - the column actually appears in the SQL sent to DuckDB (not filtered locally)
  - the column is double-quoted in that SQL (duckdb_quote_identifier worked)

See: https://github.com/Snowflake-Labs/pg_lake/issues/277
"""

import pytest
from utils_pytest import *

# All DuckDB-specific RESERVED_KEYWORD entries that are NOT reserved in
# PostgreSQL (i.e. usable as unquoted identifiers in PG DDL/DML).
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

# Representative subset used in multi-column tests.
TEST_KEYWORDS = ["pivot", "qualify", "lambda", "show"]


# ---------------------------------------------------------------------------
# Read-only foreign table tests — parquet
# ---------------------------------------------------------------------------


def test_read_parquet_with_reserved_keyword_columns(
    pg_conn, duckdb_conn, s3, extension
):
    """
    Parquet FDW: columns named after DuckDB reserved keywords must be
    selectable and filterable.  EXPLAIN confirms the WHERE clause is pushed
    down to DuckDB and the column is properly double-quoted in the remote SQL.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_read_parquet/data.parquet"

    run_command(
        f"""
        COPY (
            SELECT 1 AS pivot, 2 AS qualify, 3 AS lambda, 4 AS show
            UNION ALL
            SELECT 10, 20, 30, 40
        ) TO '{url}'
        """,
        duckdb_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_read (
            pivot int, qualify int, lambda int, show int
        ) SERVER pg_lake OPTIONS (path '{url}')
        """,
        pg_conn,
    )

    try:
        # Basic SELECT
        result = run_query(
            "SELECT pivot, qualify, lambda, show FROM test_duckdb_kw_read ORDER BY pivot",
            pg_conn,
        )
        assert result == [[1, 2, 3, 4], [10, 20, 30, 40]]

        # Confirm the SELECT list columns are double-quoted in the SQL sent to DuckDB
        assert_remote_query_contains_expression(
            "SELECT pivot, qualify FROM test_duckdb_kw_read ORDER BY pivot",
            '"pivot"',
            pg_conn,
        )
        assert_remote_query_contains_expression(
            "SELECT pivot, qualify FROM test_duckdb_kw_read ORDER BY pivot",
            '"qualify"',
            pg_conn,
        )

        # WHERE filter pushdown: confirm the filter column is double-quoted in
        # the remote SQL, proving deparse.c quotes it correctly
        filter_query = "SELECT pivot FROM test_duckdb_kw_read WHERE qualify > 5"
        result = run_query(filter_query, pg_conn)
        assert result == [[10]]
        assert_remote_query_contains_expression(filter_query, '"qualify"', pg_conn)
    finally:
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Read-only foreign table tests — CSV
# ---------------------------------------------------------------------------


def test_read_csv_with_reserved_keyword_columns(pg_conn, s3, extension):
    """
    CSV FDW: columns named after DuckDB reserved keywords must be selectable
    and filterable, with filter pushdown confirmed via EXPLAIN.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_read_csv/data.csv"

    run_command(
        f"""
        COPY (
            SELECT 1 AS pivot, 2 AS qualify
            UNION ALL
            SELECT 10, 20
        ) TO '{url}' WITH (format 'csv', header on)
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_csv (
            pivot int, qualify int
        ) SERVER pg_lake OPTIONS (path '{url}', format 'csv', header 'true')
        """,
        pg_conn,
    )

    try:
        result = run_query(
            "SELECT pivot, qualify FROM test_duckdb_kw_csv ORDER BY pivot",
            pg_conn,
        )
        assert result == [[1, 2], [10, 20]]

        filter_query = "SELECT qualify FROM test_duckdb_kw_csv WHERE pivot = 1"
        result = run_query(filter_query, pg_conn)
        assert result == [[2]]
        assert_remote_query_contains_expression(filter_query, '"pivot"', pg_conn)
    finally:
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Read-only foreign table tests — JSON
# ---------------------------------------------------------------------------


def test_read_json_with_reserved_keyword_columns(pg_conn, s3, extension):
    """
    JSON FDW: columns named after DuckDB reserved keywords must be selectable
    and filterable, with filter pushdown confirmed via EXPLAIN.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_read_json/data.json"

    run_command(
        f"""
        COPY (
            SELECT 1 AS pivot, 2 AS qualify
            UNION ALL
            SELECT 10, 20
        ) TO '{url}' WITH (format 'json')
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_json (
            pivot int, qualify int
        ) SERVER pg_lake OPTIONS (path '{url}', format 'json')
        """,
        pg_conn,
    )

    try:
        result = run_query(
            "SELECT pivot, qualify FROM test_duckdb_kw_json ORDER BY pivot",
            pg_conn,
        )
        assert result == [[1, 2], [10, 20]]

        filter_query = "SELECT qualify FROM test_duckdb_kw_json WHERE pivot = 10"
        result = run_query(filter_query, pg_conn)
        assert result == [[20]]
        assert_remote_query_contains_expression(filter_query, '"pivot"', pg_conn)
    finally:
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Writable parquet foreign table tests
# ---------------------------------------------------------------------------


def test_writable_parquet_with_reserved_keyword_columns(pg_conn, s3, extension):
    """
    Writable parquet FDW: INSERT, SELECT, and filter pushdown must work for
    DuckDB-reserved column names.  EXPLAIN confirms filter pushdown and
    proper column quoting in the remote SQL.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_writable_parquet/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_writable (
            pivot int, qualify int, lambda int, show int
        ) SERVER pg_lake OPTIONS (writable 'true', location '{url}', format 'parquet')
        """,
        pg_conn,
    )

    run_command(
        """
        INSERT INTO test_duckdb_kw_writable
        VALUES (1, 2, 3, 4), (10, 20, 30, 40), (100, 200, 300, 400)
        """,
        pg_conn,
    )

    try:
        result = run_query(
            "SELECT pivot, qualify, lambda, show FROM test_duckdb_kw_writable ORDER BY pivot",
            pg_conn,
        )
        assert result == [[1, 2, 3, 4], [10, 20, 30, 40], [100, 200, 300, 400]]

        filter_query = (
            "SELECT pivot, qualify FROM test_duckdb_kw_writable "
            "WHERE lambda > 10 ORDER BY pivot"
        )
        result = run_query(filter_query, pg_conn)
        assert result == [[10, 20], [100, 200]]
        assert_remote_query_contains_expression(filter_query, '"lambda"', pg_conn)
    finally:
        pg_conn.rollback()


# ---------------------------------------------------------------------------
# Iceberg table tests
# ---------------------------------------------------------------------------


def test_iceberg_table_with_reserved_keyword_columns(pg_conn, s3, extension):
    """
    Iceberg tables with DuckDB-reserved keyword column names must support
    CREATE, INSERT, SELECT, and filter pushdown.  EXPLAIN confirms the filter
    reaches DuckDB with the column properly quoted.
    """
    schema = "test_duckdb_kw_iceberg"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    col_defs = ", ".join(f"{kw} int" for kw in TEST_KEYWORDS)
    run_command(
        f"""
        CREATE TABLE {schema}.t ({col_defs})
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema}.t (pivot, qualify, lambda, show)
        VALUES (1, 2, 3, 4), (10, 20, 30, 40)
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT pivot, qualify, lambda, show FROM {schema}.t ORDER BY pivot",
        pg_conn,
    )
    assert result == [[1, 2, 3, 4], [10, 20, 30, 40]]

    filter_query = f"SELECT pivot FROM {schema}.t WHERE qualify > 5 ORDER BY pivot"
    result = run_query(filter_query, pg_conn)
    assert result == [[10]]

    # Confirm the filter reaches DuckDB with the column quoted
    assert_remote_query_contains_expression(filter_query, '"qualify"', pg_conn)

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_table_insert_select_all_reserved_keywords(pg_conn, s3, extension):
    """
    All DuckDB-only reserved keywords should work as iceberg column names.
    """
    schema = "test_duckdb_kw_iceberg_all"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    col_defs = ", ".join(f"{kw} int" for kw in DUCKDB_ONLY_RESERVED)
    kw_list = ", ".join(DUCKDB_ONLY_RESERVED)
    val_list = ", ".join(str(i + 1) for i in range(len(DUCKDB_ONLY_RESERVED)))

    run_command(
        f"""
        CREATE TABLE {schema}.t ({col_defs})
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {schema}.t ({kw_list}) VALUES ({val_list})",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT {kw_list} FROM {schema}.t", pg_conn)
    expected = [[i + 1 for i in range(len(DUCKDB_ONLY_RESERVED))]]
    assert result == expected

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Parameterised smoke test — each keyword individually, all three formats
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keyword", DUCKDB_ONLY_RESERVED)
def test_each_reserved_keyword_parquet(pg_conn, duckdb_conn, s3, extension, keyword):
    """
    Each DuckDB-only reserved keyword must work as a parquet column name.
    EXPLAIN confirms the SELECT and WHERE both reach DuckDB with the column
    properly double-quoted.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_param_parquet/{keyword}/data.parquet"

    run_command(
        f"COPY (SELECT 42 AS \"{keyword}\") TO '{url}'",
        duckdb_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_pq_{keyword} (
            {keyword} int
        ) SERVER pg_lake OPTIONS (path '{url}')
        """,
        pg_conn,
    )

    try:
        select_query = f"SELECT {keyword} FROM test_duckdb_kw_pq_{keyword}"
        result = run_query(select_query, pg_conn)
        assert result == [[42]]
        # Column must appear double-quoted in the SQL sent to DuckDB
        assert_remote_query_contains_expression(select_query, f'"{keyword}"', pg_conn)

        filter_query = (
            f"SELECT {keyword} FROM test_duckdb_kw_pq_{keyword} WHERE {keyword} = 42"
        )
        result = run_query(filter_query, pg_conn)
        assert result == [[42]]
        # WHERE clause column must also be double-quoted (exercises deparse.c)
        assert_remote_query_contains_expression(filter_query, f'"{keyword}"', pg_conn)

        result = run_query(
            f"SELECT {keyword} FROM test_duckdb_kw_pq_{keyword} WHERE {keyword} = 0",
            pg_conn,
        )
        assert result == []
    finally:
        pg_conn.rollback()


@pytest.mark.parametrize("keyword", DUCKDB_ONLY_RESERVED)
def test_each_reserved_keyword_csv(pg_conn, s3, extension, keyword):
    """
    Each DuckDB-only reserved keyword must work as a CSV column name with
    filter pushdown confirmed via EXPLAIN.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_param_csv/{keyword}/data.csv"

    run_command(
        f"COPY (SELECT 42 AS {keyword}) TO '{url}' WITH (format 'csv', header on)",
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_csv_{keyword} (
            {keyword} int
        ) SERVER pg_lake OPTIONS (path '{url}', format 'csv', header 'true')
        """,
        pg_conn,
    )

    try:
        select_query = f"SELECT {keyword} FROM test_duckdb_kw_csv_{keyword}"
        result = run_query(select_query, pg_conn)
        assert result == [[42]]
        assert_remote_query_contains_expression(select_query, f'"{keyword}"', pg_conn)

        filter_query = (
            f"SELECT {keyword} FROM test_duckdb_kw_csv_{keyword} WHERE {keyword} = 42"
        )
        result = run_query(filter_query, pg_conn)
        assert result == [[42]]
        assert_remote_query_contains_expression(filter_query, f'"{keyword}"', pg_conn)
    finally:
        pg_conn.rollback()


@pytest.mark.parametrize("keyword", DUCKDB_ONLY_RESERVED)
def test_each_reserved_keyword_json(pg_conn, s3, extension, keyword):
    """
    Each DuckDB-only reserved keyword must work as a JSON column name with
    filter pushdown confirmed via EXPLAIN.
    """
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_param_json/{keyword}/data.json"

    run_command(
        f"COPY (SELECT 42 AS {keyword}) TO '{url}' WITH (format 'json')",
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_duckdb_kw_json_{keyword} (
            {keyword} int
        ) SERVER pg_lake OPTIONS (path '{url}', format 'json')
        """,
        pg_conn,
    )

    try:
        select_query = f"SELECT {keyword} FROM test_duckdb_kw_json_{keyword}"
        result = run_query(select_query, pg_conn)
        assert result == [[42]]
        assert_remote_query_contains_expression(select_query, f'"{keyword}"', pg_conn)

        filter_query = (
            f"SELECT {keyword} FROM test_duckdb_kw_json_{keyword} WHERE {keyword} = 42"
        )
        result = run_query(filter_query, pg_conn)
        assert result == [[42]]
        assert_remote_query_contains_expression(filter_query, f'"{keyword}"', pg_conn)
    finally:
        pg_conn.rollback()

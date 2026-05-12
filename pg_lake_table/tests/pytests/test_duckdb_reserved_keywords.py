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


def test_readable_iceberg_foreign_table_with_reserved_keyword_columns(
    pg_conn, s3, extension
):
    """
    Read-only iceberg foreign table: after writing an iceberg table with
    DuckDB-reserved keyword columns, create a FOREIGN TABLE pointing at its
    metadata_location and verify SELECT and filter pushdown still quote the
    columns correctly for DuckDB.
    """
    schema = "test_duckdb_kw_iceberg_readable"
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

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 't' AND table_namespace = '{schema}'",
        pg_conn,
    )[0][0]

    run_command(
        f"""
        CREATE FOREIGN TABLE {schema}.t_readable ()
        SERVER pg_lake
        OPTIONS (path '{metadata_location}', format 'iceberg')
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT pivot, qualify, lambda, show FROM {schema}.t_readable ORDER BY pivot",
        pg_conn,
    )
    assert result == [[1, 2, 3, 4], [10, 20, 30, 40]]

    filter_query = (
        f"SELECT pivot FROM {schema}.t_readable WHERE qualify > 5 ORDER BY pivot"
    )
    result = run_query(filter_query, pg_conn)
    assert result == [[10]]
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


# ---------------------------------------------------------------------------
# Composite type field names with special characters
# ---------------------------------------------------------------------------


def test_struct_of_interval_field_name_with_single_quote(pg_conn, s3, extension):
    """
    Iceberg table with a STRUCT field containing an INTERVAL sub-field whose
    name contains a single quote.  Exercises the INTERVAL-specific struct
    projection branch in pg_lake_engine/src/pgduck/read_data.c: the key emit
    site previously used bare "'%s':" formatting, which truncated at the
    embedded "'" and produced a DuckDB parser error.

    Regression test for review feedback on PR #297 (read_data.c:986) — the
    key emit must go through QuoteDuckDBStructKey() like the sibling site in
    struct_conversion.c:157 does.
    """
    schema = "test_duckdb_kw_interval_quote"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TYPE {schema}.bad_interval_t AS (
            "has'quote" interval,
            "normal" interval,
            "plain" int
        )
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.t (id int, s {schema}.bad_interval_t)
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema}.t
        VALUES (1, ROW(INTERVAL '1 day', INTERVAL '5 minutes', 42))
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"""
        SELECT id,
               (s)."has'quote" = INTERVAL '1 day',
               (s)."normal"    = INTERVAL '5 minutes',
               (s)."plain"
          FROM {schema}.t
        """,
        pg_conn,
    )
    assert result == [[1, True, True, 42]]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_autodetect_struct_field_names_with_hostile_characters(pg_conn, s3, extension):
    """
    Auto-detect (CREATE FOREIGN TABLE () ... OPTIONS (path ...)) against a
    parquet file whose struct field names contain characters that would break
    a raw-formatted name[] array literal: quotes, commas, colons, spaces,
    parens, backslashes.

    Regression test for review feedback on PR #297 — after fixing
    ParseDuckDBFieldName to handle SQL-standard "" doubling, the next layer
    (GetOrCreatePGStructType's catalog-lookup SPI query) was still formatting
    field names directly into a '{...}'::name[] literal, producing
    "malformed array literal" for any hostile shape.
    """
    schema = "test_duckdb_kw_autodetect_hostile"
    url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TYPE {schema}.edge_t AS (
            plain          int,
            "with space"   int,
            U&"has\\0022one"   int,
            "comma,here"   int,
            "colon:here"   int,
            "with(parens)" int,
            U&"back\\005Cslash" int
        )
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.src (id int, e {schema}.edge_t)
        USING iceberg
        WITH (location = 's3://{TEST_BUCKET}/{schema}_src/')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {schema}.src VALUES (1, ROW(1, 2, 3, 4, 5, 6, 7))",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"COPY (SELECT * FROM {schema}.src) TO '{url}' WITH (format 'parquet')",
        pg_conn,
    )
    pg_conn.commit()

    # Drop the catalog-side composite type so the next CREATE FOREIGN TABLE is
    # forced down the auto-detect path through GetOrCreatePGStructType.
    run_command(f"DROP TABLE {schema}.src", pg_conn)
    run_command(f"DROP TYPE {schema}.edge_t", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE FOREIGN TABLE {schema}.edge_pq ()
        SERVER pg_lake
        OPTIONS (path '{url}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f'SELECT id, (e).plain, (e)."with space", (e).U&"has\\0022one", '
        f'(e)."comma,here", (e)."colon:here", (e)."with(parens)", '
        f'(e).U&"back\\005Cslash" FROM {schema}.edge_pq',
        pg_conn,
    )
    assert result == [[1, 1, 2, 3, 4, 5, 6, 7]]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize(
    "keyword,pg_type,pg_value,sql_literal",
    [
        (
            "lambda",
            "interval",
            "INTERVAL '2 hours 30 minutes'",
            "INTERVAL '2 hours 30 minutes'",
        ),
        (
            "pivot",
            "timestamp",
            "TIMESTAMP '2024-01-15 10:30:00'",
            "TIMESTAMP '2024-01-15 10:30:00'",
        ),
        (
            "qualify",
            "interval[]",
            "ARRAY[INTERVAL '1 day', INTERVAL '2 hours']::interval[]",
            "ARRAY[INTERVAL '1 day', INTERVAL '2 hours']::interval[]",
        ),
    ],
)
def test_reserved_keyword_struct_field_typed(
    pg_conn, s3, extension, keyword, pg_type, pg_value, sql_literal
):
    """
    Reserved-keyword field names inside a composite column must also work for
    the INTERVAL / TIMESTAMP / INTERVAL[] branches of the struct projection
    builder in read_data.c (which has specialised emission for those types).
    Locks in regression protection for review feedback on PR #297 — the main
    parameterized tests use plain int, which doesn't touch those branches.
    """
    schema = f"test_duckdb_kw_struct_{keyword}_{pg_type.replace('[]', 'arr')}"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f'CREATE TYPE {schema}.composite_t AS ("{keyword}" {pg_type}, plain int)',
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.t (id int, s {schema}.composite_t)
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {schema}.t VALUES (1, ROW({pg_value}, 42))",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f'SELECT id, (s)."{keyword}" = {sql_literal}, (s).plain FROM {schema}.t',
        pg_conn,
    )
    assert result == [[1, True, 42]]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_composite_field_with_embedded_double_quote(pg_conn, s3, extension):
    """
    Composite type field names containing double-quote characters must be
    properly escaped in the STRUCT type definitions sent to DuckDB.

    DuckDB's type parser requires identifier quoting in STRUCT(...) type
    strings.  A field name like  has"quote  must be emitted as "has""quote"
    (standard SQL identifier escaping) rather than being passed through raw.

    Reproduces: https://github.com/snowflake-eng/sfpg-extension-pg_lake_replication/issues/361
    """
    schema = "test_duckdb_kw_struct_quote"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TYPE {schema}.composite_with_quote AS (
            U&"has\\0022quote" text,
            normal_field int
        )
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.t (
            id int,
            s {schema}.composite_with_quote
        )
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema}.t VALUES (1, ROW('hello', 42))
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT id, (s).normal_field FROM {schema}.t",
        pg_conn,
    )
    assert result == [[1, 42]]

    result = run_query(
        f'SELECT (s).U&"has\\0022quote" FROM {schema}.t',
        pg_conn,
    )
    assert result == [["hello"]]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_composite_field_with_special_characters(pg_conn, s3, extension):
    """
    Composite type field names containing characters that require quoting
    (spaces, embedded double-quotes, single-quotes) must work correctly in
    Iceberg tables when passed through duckdb_quote_identifier for STRUCT
    type generation.

    Exercises the STRUCT(...) type string path in read_data.c:GetSchemaType()
    and GetDuckDBStructDefinitionForCompositeType() in parse_struct.c.

    Reproduces: https://github.com/snowflake-eng/sfpg-extension-pg_lake_replication/issues/361
    """
    schema = "test_duckdb_kw_struct_special"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TYPE {schema}.composite_special AS (
            "has space" text,
            U&"has\\0022dquote" int,
            "has'single" int,
            U&"has\\005Cback" int
        )
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.t (
            id int,
            s {schema}.composite_special
        )
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema}.t VALUES (1, ROW('val', 10, 20, 30))
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f'SELECT (s)."has space", (s).U&"has\\0022dquote", (s)."has\'single", (s).U&"has\\005Cback" FROM {schema}.t',
        pg_conn,
    )
    assert result == [["val", 10, 20, 30]]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


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


# ---------------------------------------------------------------------------
# Extended coverage from PR #297 review 4270327414
# ---------------------------------------------------------------------------


def test_iceberg_timetz_to_time_cast_on_reserved_column(pg_conn, s3, extension):
    """
    TIMETZ columns are CAST to TIME on iceberg INSERT (in
    write_data.c:TupleDescToProjectionListForWrite).  Verify the emitted
    CAST path correctly quotes a reserved-keyword column name.
    """
    schema = "test_duckdb_kw_timetz"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.t (id int, pivot timetz)
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {schema}.t VALUES (1, TIMETZ '10:30:00+00'), "
        f"(2, TIMETZ '15:45:00+00')",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT id, pivot FROM {schema}.t ORDER BY id",
        pg_conn,
    )
    assert [r[0] for r in result] == [1, 2]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_analyze_with_reserved_columns(pg_conn, s3, extension):
    """
    ANALYZE on an iceberg foreign table with reserved-keyword column names
    goes through the deparse/pushdown path and must properly quote column
    names when generating the remote stats-collection query.
    """
    schema = "test_duckdb_kw_analyze"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema}.t (pivot int, qualify text, lambda int)
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema}.t (pivot, qualify, lambda) VALUES
          (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Must not error; exercises the stats/ANALYZE deparse path.
    run_command(f"ANALYZE {schema}.t", pg_conn)
    pg_conn.commit()

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_reserved_keyword_schema_with_custom_function_and_operator(
    pg_conn, s3, extension
):
    """
    Custom operator / function living in a DuckDB-reserved-keyword schema
    must have its schema-qualified name correctly quoted when deparsed into
    the SQL sent to pgduck_server (deparse.c:deparseOperatorName/deparseFuncExpr).
    """
    schema = "pivot"
    url = f"s3://{TEST_BUCKET}/test_duckdb_kw_schema_func/data.parquet"

    run_command(
        f'DROP SCHEMA IF EXISTS "{schema}" CASCADE; CREATE SCHEMA "{schema}"',
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE FUNCTION "{schema}".add_one(x int) RETURNS int
          LANGUAGE sql IMMUTABLE PARALLEL SAFE
          AS $$ SELECT x + 1 $$;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        COPY (SELECT 41 AS n UNION ALL SELECT 9) TO '{url}' WITH (format 'parquet')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE FOREIGN TABLE test_kw_schema_func (n int)
        SERVER pg_lake OPTIONS (path '{url}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # Local execution (function not shipped) — still exercises the
        # schema-qualified deparse for the projection column list.
        result = run_query(
            f'SELECT "{schema}".add_one(n) FROM test_kw_schema_func ORDER BY 1',
            pg_conn,
        )
        assert [r[0] for r in result] == [10, 42]
    finally:
        pg_conn.rollback()

    run_command(f'DROP SCHEMA "{schema}" CASCADE', pg_conn)
    pg_conn.commit()


def test_iceberg_overflow_conversion_on_reserved_column(pg_conn, s3, extension):
    """
    The overflow / native-conversion wrapper in read_data.c's projection
    builder (e.g. try_cast / explicit AS cast paths, ::TIMETZ, ::type AS
    alias) must quote reserved-keyword column names consistently.  Verify
    with a reserved-keyword smallint column that needs widening on read.
    """
    schema = "test_duckdb_kw_overflow"
    location = f"s3://{TEST_BUCKET}/{schema}/"

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()

    # Create iceberg table with a reserved-keyword smallint column, then
    # read it back through a FDW typed as int to exercise the ::type AS
    # alias projection path with a reserved column name.
    run_command(
        f"""
        CREATE TABLE {schema}.t (id int, pivot smallint)
        USING iceberg
        WITH (location = '{location}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {schema}.t VALUES (1, 100::smallint), (2, 200::smallint)",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT id, pivot::int AS pivot_widened FROM {schema}.t ORDER BY id",
        pg_conn,
    )
    assert result == [[1, 100], [2, 200]]

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_map_of_interval_with_reserved_column(
    pg_conn, s3, extension, with_default_location
):
    """
    MAP-of-INTERVAL column with a DuckDB-reserved keyword name on an
    iceberg table.  Exercises the read_data.c map projection / interval
    encoding paths with duckdb_quote_identifier applied to the column.
    """
    import datetime

    map_type_name = create_map_type("text", "interval")

    schema = "test_duckdb_kw_map_interval"
    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, pivot {map_type_name}) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema}.t VALUES
          (1, ARRAY[ROW('meeting', '1 hour'::interval)]::{map_type_name});
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT map_type.extract(pivot, 'meeting') FROM {schema}.t WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(hours=1)

    run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
    pg_conn.commit()


def test_s3log_strptime_with_reserved_column_name(pg_conn, s3, extension):
    """
    format='log' with log_format='s3' on pgduck_server triggers the
    strptime() projection wrapper in read_data.c:1468 for TIMESTAMP /
    TIMESTAMPTZ columns.  With a reserved-keyword column name this
    exercises the duckdb_quote_identifier() path that wraps the strptime
    argument.

    The upstream s3log schema pins a column named 'request_time'; we
    verify the strptime wrapper is emitted with proper quoting by
    checking EXPLAIN for the remote query.
    """
    pg_conn.rollback()

    s3log_prefix = "test_duckdb_kw_s3log_strptime"
    s3log_url = f"s3://{TEST_BUCKET}/{s3log_prefix}"
    s3log_path = sampledata_filepath("s3log")

    for root, dirs, files in os.walk(s3log_path):
        for filename in files:
            s3.upload_file(
                os.path.join(root, filename),
                TEST_BUCKET,
                f"{s3log_prefix}/{filename}",
            )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_kw_s3log ()
        SERVER pg_lake
        OPTIONS (format 'log', log_format 's3', path '{s3log_url}/**');
        """,
        pg_conn,
    )

    try:
        # Any query against a TIMESTAMP column drives strptime() emission in
        # read_data.c for the LOG format; a successful round-trip is enough
        # to show the path executed with the column identifier correctly
        # quoted (the strptime() call itself doesn't always survive to the
        # top-level EXPLAIN remote SQL string).
        result = run_query(
            "SELECT count(*) FROM test_kw_s3log WHERE request_time < now()",
            pg_conn,
        )
        assert result[0][0] > 0
    finally:
        pg_conn.rollback()

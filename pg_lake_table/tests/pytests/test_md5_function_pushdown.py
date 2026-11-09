import pytest
from utils_pytest import *


EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
ZERO_BYTE_MD5 = "93b885adfe0da089cdf634904fd59f71"
FF_BYTE_MD5 = "00594fd4f42ba43fc1ca0427a0576295"
BINARY_MD5_FILTER = (
    f"(md5(col_bytea) = '{EMPTY_MD5}' "
    f"OR md5(col_bytea) = '{ZERO_BYTE_MD5}' "
    f"OR md5(col_bytea) = '{FF_BYTE_MD5}')"
)


@pytest.fixture(scope="module")
def create_md5_pushdown_tables(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/md5_function_pushdown/data.parquet"
    run_command(
        f"""
        COPY (
            SELECT NULL::bytea AS col_bytea, NULL::text AS col_text
            UNION ALL SELECT ''::bytea, ''::text
            UNION ALL SELECT 'abc'::bytea, 'abc'::text
            UNION ALL SELECT 'hello'::bytea, 'hello'::text
            UNION ALL SELECT '\\x00'::bytea, 'has-zero-byte'::text
            UNION ALL SELECT '\\xff'::bytea, 'ff'::text
            UNION ALL SELECT '\\x0001ff'::bytea, '0001ff'::text
        ) TO '{url}' WITH (FORMAT 'parquet');

        CREATE SCHEMA md5_pushdown;
        CREATE FOREIGN TABLE md5_pushdown.fdw_tbl (
            col_bytea bytea,
            col_text text
        ) SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE md5_pushdown.heap_tbl (
            col_bytea bytea,
            col_text text
        );
        COPY md5_pushdown.heap_tbl FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()
    yield
    run_command("DROP SCHEMA md5_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def test_md5_bytea_pushdown(create_md5_pushdown_tables, pg_conn):
    """Push down md5(bytea) and match Postgres on binary inputs.

    Covers NULL, empty bytes, ASCII, an embedded zero, and non-UTF-8
    sequences (ff, 0001ff). Result is lowercase hex text. Also checks
    an FDW predicate with full query pushdown off.
    """
    select_query = "SELECT md5(col_bytea) AS md5_val FROM {table}"
    pushed_query = select_query.format(table="md5_pushdown.fdw_tbl")
    assert_query_pushdownable(pushed_query, pg_conn)
    assert_remote_query_contains_expression(pushed_query, '"md5"(', pg_conn)

    # Compare the pushed query's own rows with Postgres. Wrapping both sides in
    # EXCEPT ALL would run md5() in Postgres on the foreign table side.
    pushed_rows = run_query(pushed_query, pg_conn)
    heap_rows = run_query(select_query.format(table="md5_pushdown.heap_tbl"), pg_conn)
    sort_key = lambda value: (value is None, value or "")
    assert sorted((row["md5_val"] for row in pushed_rows), key=sort_key) == sorted(
        (row["md5_val"] for row in heap_rows), key=sort_key
    )

    empty_rows = run_query(
        "SELECT md5(col_bytea) FROM md5_pushdown.fdw_tbl "
        "WHERE col_bytea = ''::bytea",
        pg_conn,
    )
    assert empty_rows[0]["md5"] == EMPTY_MD5

    run_command(
        "SET pg_lake_table.enable_full_query_pushdown TO false;",
        pg_conn,
    )
    try:
        predicate_query = (
            "SELECT col_bytea FROM md5_pushdown.fdw_tbl " f"WHERE {BINARY_MD5_FILTER}"
        )
        assert_query_not_pushdownable(predicate_query, pg_conn)
        assert_remote_query_contains_expression(predicate_query, '"md5"(', pg_conn)
        assert_table_contents_match(
            pg_conn,
            "(SELECT col_bytea FROM md5_pushdown.fdw_tbl "
            f"WHERE {BINARY_MD5_FILTER}) fdw",
            "(SELECT col_bytea FROM md5_pushdown.heap_tbl "
            f"WHERE {BINARY_MD5_FILTER}) heap",
        )
        matched = run_query(predicate_query, pg_conn)
        assert len(matched) == 3
    finally:
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)

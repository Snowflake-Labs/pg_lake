import pytest
from utils_pytest import *

# string_agg(text, text) is pushed down to pgduck_server / DuckDB only when the
# delimiter is a constant.  A NULL delimiter means "no separator" in PostgreSQL
# but makes DuckDB return NULL, so a NULL constant is sent to DuckDB as ''.  With that
# guarantee every other behavior (NULL/empty values, ORDER BY, DISTINCT, FILTER,
# GROUP BY) is identical between PostgreSQL and DuckDB.
#
# IMPORTANT: string_agg without an ORDER BY has an unspecified concatenation
# order, which legitimately differs between local (PostgreSQL) and pushed-down
# (DuckDB) execution.  Every parity case below therefore uses an ORDER BY inside
# the aggregate so the comparison against the heap table is deterministic.

# Cases that must be pushed down and must match the heap table.
positive_cases = [
    (
        "basic",
        "SELECT string_agg(v, ',' ORDER BY v) FROM {}",
    ),
    (
        "order_desc",
        "SELECT string_agg(v, ',' ORDER BY v DESC) FROM {}",
    ),
    (
        "order_sort_key_nulls_last",
        "SELECT string_agg(v, ',' ORDER BY sk NULLS LAST, v) FROM {}",
    ),
    (
        "order_sort_key_nulls_first",
        "SELECT string_agg(v, ',' ORDER BY sk ASC NULLS FIRST, v) FROM {}",
    ),
    (
        "distinct",
        "SELECT string_agg(DISTINCT v, ',' ORDER BY v) FROM {}",
    ),
    (
        "filter",
        "SELECT string_agg(v, ',' ORDER BY v) FILTER (WHERE v <> 'skip') FROM {}",
    ),
    (
        "empty_delimiter",
        "SELECT string_agg(v, '' ORDER BY v) FROM {}",
    ),
    (
        "unicode_delimiter",
        "SELECT string_agg(v, '★' ORDER BY v) FROM {}",
    ),
    (
        "grouped",
        "SELECT g, string_agg(v, ',' ORDER BY v) FROM {} GROUP BY g",
    ),
    (
        "folded_delimiter",
        "SELECT string_agg(v, upper(';') ORDER BY v) FROM {}",
    ),
    (
        "folded_delimiter_rewritten_function",
        "SELECT string_agg(v, substr(',;', 2, 1) ORDER BY v) FROM {}",
    ),
    (
        "folded_delimiter_padded",
        "SELECT string_agg(v, lpad('', 1, '|') ORDER BY v) FROM {}",
    ),
    (
        "null_delimiter",
        "SELECT string_agg(v, NULL::text ORDER BY v) FROM {}",
    ),
    (
        "null_delimiter_untyped",
        "SELECT string_agg(v, NULL ORDER BY v) FROM {}",
    ),
    # The planner folds these to a NULL constant before the FDW pushes down the
    # aggregate, so they reach DuckDB as '' too.  The full query pushdown checks
    # the unfolded query and leaves them to the FDW.
    (
        "null_delimiter_folded",
        "SELECT string_agg(v, NULLIF(',', ',') ORDER BY v) FROM {}",
    ),
    (
        "null_delimiter_folded_case",
        "SELECT string_agg(v, CASE WHEN false THEN ',' END ORDER BY v) FROM {}",
    ),
    (
        "null_delimiter_distinct",
        "SELECT string_agg(DISTINCT v, NULL::text ORDER BY v) FROM {}",
    ),
    (
        "null_delimiter_filter",
        "SELECT string_agg(v, NULL::text ORDER BY v) FILTER (WHERE v <> 'skip') FROM {}",
    ),
    (
        "null_delimiter_grouped",
        "SELECT g, string_agg(v, NULL::text ORDER BY v) FROM {} GROUP BY g",
    ),
    (
        "null_delimiter_no_rows",
        "SELECT string_agg(v, NULL::text ORDER BY v) FROM {} WHERE false",
    ),
]

# Cases that must NOT be pushed down (they would diverge from, or error on,
# DuckDB) but must still produce correct results by running locally.
negative_cases = [
    # Non-constant (per-row) delimiter: DuckDB rejects a non-constant separator.
    (
        "column_delimiter",
        "SELECT string_agg(v, d ORDER BY v) FROM {}",
    ),
    # bytea variant: DuckDB has no string_agg(BLOB, BLOB).
    (
        "bytea_variant",
        "SELECT encode(string_agg(v::bytea, ','::bytea ORDER BY v::bytea), 'hex') FROM {}",
    ),
]


@pytest.mark.parametrize(
    "test_id, query_template",
    positive_cases,
    ids=[case[0] for case in positive_cases],
)
def test_string_agg_pushdown(
    create_sagg_pushdown_table, pg_conn, test_id, query_template
):
    query = query_template.format("sagg_pushdown.tbl")

    assert_remote_query_contains_expression(query, "string_agg", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["sagg_pushdown.tbl"],
        ["sagg_pushdown.heap_tbl"],
    )


@pytest.mark.parametrize(
    "test_id, query_template",
    negative_cases,
    ids=[case[0] for case in negative_cases],
)
def test_string_agg_not_pushdown(
    create_sagg_pushdown_table, pg_conn, test_id, query_template
):
    query = query_template.format("sagg_pushdown.tbl")

    assert_remote_query_not_contains_expression(query, "string_agg", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["sagg_pushdown.tbl"],
        ["sagg_pushdown.heap_tbl"],
    )


# A NULL delimiter must reach DuckDB as '', i.e. produce the same remote SQL as
# an empty-string delimiter.
null_delimiter_cases = [
    ("null_typed", "NULL::text"),
    ("null_untyped", "NULL"),
]


@pytest.mark.parametrize(
    "test_id, delimiter",
    null_delimiter_cases,
    ids=[case[0] for case in null_delimiter_cases],
)
def test_string_agg_null_delimiter_sent_as_empty(
    create_sagg_pushdown_table, pg_conn, test_id, delimiter
):
    query_template = "SELECT string_agg(v, {} ORDER BY v) FROM sagg_pushdown.tbl"

    null_remote_sql = fetch_remote_sql_for_query(
        query_template.format(delimiter), pg_conn
    )
    empty_remote_sql = fetch_remote_sql_for_query(query_template.format("''"), pg_conn)

    assert null_remote_sql == empty_remote_sql


# A delimiter that the planner folds to NULL takes the FDW path, so its remote
# SQL differs from the full query pushdown of ''.  Check the delimiter instead.
folded_null_delimiter_cases = [
    ("null_folded", "NULLIF(',', ',')"),
    ("null_folded_case", "CASE WHEN false THEN ',' END"),
]


@pytest.mark.parametrize(
    "test_id, delimiter",
    folded_null_delimiter_cases,
    ids=[case[0] for case in folded_null_delimiter_cases],
)
def test_string_agg_folded_null_delimiter_sent_as_empty(
    create_sagg_pushdown_table, pg_conn, test_id, delimiter
):
    query = f"SELECT string_agg(v, {delimiter} ORDER BY v) FROM sagg_pushdown.tbl"

    assert_remote_query_contains_expression(query, "''::\"text\"", pg_conn)
    assert_remote_query_not_contains_expression(query, "NULL", pg_conn)


def fetch_remote_sql_for_query(query, pg_conn):
    explain = "EXPLAIN (ANALYZE, VERBOSE, format JSON) " + query
    return fetch_remote_sql(perform_query_on_cursor(explain, pg_conn)[0])


@pytest.fixture(scope="module")
def create_sagg_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/sagg_pushdown/data.parquet"
    run_command(
        f"""
            COPY (
                    SELECT 1 AS id, 1 AS g, 'a'    AS v, 3::int AS sk, ','::text AS d
                        UNION ALL
                    SELECT 2, 1, 'b',    1,    ';'
                        UNION ALL
                    SELECT 3, 1, NULL,   2,    ','
                        UNION ALL
                    SELECT 4, 1, '',     NULL, ','
                        UNION ALL
                    SELECT 5, 2, 'café', 1,    ','
                        UNION ALL
                    SELECT 6, 2, '日本語', 2,   ','
                        UNION ALL
                    SELECT 7, 3, 'skip', 1,    ','
                        UNION ALL
                    SELECT 8, 3, 'keep', 2,    ','
                        UNION ALL
                    SELECT 9, 4, '😀',   1,    ','
                        UNION ALL
                    SELECT 10, 4, '👍🏽', 2,    ','
                ) TO '{url}' WITH (FORMAT 'parquet');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
            CREATE SCHEMA sagg_pushdown;
            CREATE FOREIGN TABLE sagg_pushdown.tbl
            (
                id int,
                g int,
                v text,
                sk int,
                d text
            ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
            """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
            CREATE TABLE sagg_pushdown.heap_tbl
            AS SELECT * FROM sagg_pushdown.tbl;
            """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA sagg_pushdown CASCADE", pg_conn)
    pg_conn.commit()

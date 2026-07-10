import pytest
from utils_pytest import *

# string_agg(text, text) is pushed down to pgduck_server / DuckDB only when the
# delimiter is a non-NULL constant.  With that guarantee every other behavior
# (NULL/empty values, ORDER BY, DISTINCT, FILTER, GROUP BY) is identical between
# PostgreSQL and DuckDB.
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
]

# Cases that must NOT be pushed down (they would diverge from, or error on,
# DuckDB) but must still produce correct results by running locally.
negative_cases = [
    # NULL delimiter: DuckDB returns NULL, PostgreSQL concatenates.
    (
        "null_delimiter",
        "SELECT string_agg(v, NULL::text) FROM {}",
    ),
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
    create_string_agg_pushdown_table, pg_conn, test_id, query_template
):
    query = query_template.format("string_agg_pushdown.tbl")

    assert_remote_query_contains_expression(query, "string_agg", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["string_agg_pushdown.tbl"],
        ["string_agg_pushdown.heap_tbl"],
    )


@pytest.mark.parametrize(
    "test_id, query_template",
    negative_cases,
    ids=[case[0] for case in negative_cases],
)
def test_string_agg_not_pushdown(
    create_string_agg_pushdown_table, pg_conn, test_id, query_template
):
    query = query_template.format("string_agg_pushdown.tbl")

    assert_remote_query_not_contains_expression(query, "string_agg", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["string_agg_pushdown.tbl"],
        ["string_agg_pushdown.heap_tbl"],
    )


@pytest.fixture(scope="module")
def create_string_agg_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/string_agg_pushdown/data.parquet"
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
                ) TO '{url}' WITH (FORMAT 'parquet');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
            CREATE SCHEMA string_agg_pushdown;
            CREATE FOREIGN TABLE string_agg_pushdown.tbl
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
            CREATE TABLE string_agg_pushdown.heap_tbl
            AS SELECT * FROM string_agg_pushdown.tbl;
            """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA string_agg_pushdown CASCADE", pg_conn)
    pg_conn.commit()

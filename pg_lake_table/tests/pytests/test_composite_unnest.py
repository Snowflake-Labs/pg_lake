import pytest
import psycopg2
from utils_pytest import *


# unnest(..) over an array of a composite type, see issues/530.
# Each case: test id, query, whether we expect the query to be pushed down.
test_cases = [
    (
        "left_join_all_fields",
        """SELECT a.code, a.charges, a.is_waived, a.is_shiplify
           FROM composite_unnest.tbl t
           LEFT JOIN unnest(t.accessorials) a ON true
           WHERE pro_number = 34342342""",
        True,
    ),
    (
        "first_field_only",
        """SELECT a.code FROM composite_unnest.tbl t, unnest(t.accessorials) a""",
        True,
    ),
    (
        "qualification_and_aggregate",
        """SELECT count(*), max(a.charges)
           FROM composite_unnest.tbl t, unnest(t.accessorials) a
           WHERE a.is_waived""",
        True,
    ),
    (
        "join_qualification",
        """SELECT t.pro_number, a.charges
           FROM composite_unnest.tbl t
           LEFT JOIN unnest(t.accessorials) a ON a.code = 'LIMIT'""",
        True,
    ),
    (
        "column_alias_list",
        """SELECT b.c, b.w
           FROM composite_unnest.tbl t, unnest(t.accessorials) AS b(c, ch, w, s, n)""",
        True,
    ),
    (
        "nested_composite_field",
        """SELECT a.code, (a.nested).x, (a.nested).y
           FROM composite_unnest.tbl t, unnest(t.accessorials) a""",
        True,
    ),
    (
        "unnest_star",
        """SELECT t.pro_number, a.* FROM composite_unnest.tbl t, unnest(t.accessorials) a""",
        True,
    ),
    (
        "two_unnests",
        """SELECT a.code, b.charges
           FROM composite_unnest.tbl t,
                unnest(t.accessorials) a,
                unnest(t.accessorials) b""",
        True,
    ),
    (
        "in_cte",
        """WITH s AS (
             SELECT a.code, a.charges FROM composite_unnest.tbl t, unnest(t.accessorials) a
           )
           SELECT code, sum(charges) FROM s GROUP BY code""",
        True,
    ),
    (
        "in_subquery",
        """SELECT s.code FROM (
             SELECT a.code FROM composite_unnest.tbl t, unnest(t.accessorials) a
           ) s WHERE s.code LIKE 'LI%'""",
        True,
    ),
    (
        "correlated_reference",
        """SELECT t.pro_number, (SELECT a.charges WHERE a.is_waived)
           FROM composite_unnest.tbl t, unnest(t.accessorials) a""",
        True,
    ),
    (
        "scalar_array_unnest",
        """SELECT t.pro_number, x
           FROM composite_unnest.tbl t, unnest(array[1, 2]) x""",
        True,
    ),
    # DuckDB has no WITH ORDINALITY, so this one runs in PostgreSQL
    (
        "with_ordinality",
        """SELECT a.code, a.ord
           FROM composite_unnest.tbl t,
                unnest(t.accessorials) WITH ORDINALITY
                AS a(code, charges, is_waived, is_shiplify, nested, ord)""",
        False,
    ),
]


@pytest.mark.parametrize(
    "test_id, query, assert_pushdown",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_composite_unnest(
    create_composite_unnest_tables, pg_conn, test_id, query, assert_pushdown
):
    if assert_pushdown:
        assert_query_pushdownable(query, pg_conn)
    else:
        assert_query_not_pushdownable(query, pg_conn)

    assert_query_results_on_tables(
        query, pg_conn, ["composite_unnest.tbl"], ["composite_unnest.heap_tbl"]
    )


def test_composite_unnest_field_values(create_composite_unnest_tables, pg_conn):
    """The query from issues/530, which used to fail to bind in DuckDB."""

    query = """
    SELECT a.code, a.charges, a.is_waived, a.is_shiplify
    FROM composite_unnest.tbl t
    LEFT JOIN unnest(t.accessorials) a ON true
    WHERE pro_number = 34342342
    ORDER BY 1
    """

    # the fields are selected out of the single struct column DuckDB returns
    assert_remote_query_contains_expression(query, '."code"', pg_conn)

    assert run_query(query, pg_conn) == [
        ["LIFTPU", 25, False, False],
        ["LIMIT", 50, False, True],
    ]

    pg_conn.rollback()


@pytest.fixture(scope="module")
def create_composite_unnest_tables(pg_conn, s3, request, extension):
    url = f"s3://{TEST_BUCKET}/{request.node.name}/"

    run_command(
        f"""
        CREATE SCHEMA composite_unnest;
        SET search_path TO composite_unnest;

        CREATE TYPE nested AS (x int, y text);
        CREATE TYPE accessorial AS (
            code text,
            charges numeric,
            is_waived boolean,
            is_shiplify boolean,
            nested nested
        );

        CREATE FOREIGN TABLE tbl (pro_number bigint, accessorials accessorial[])
            SERVER pg_lake OPTIONS (location '{url}', writable 'true', format 'parquet');
        CREATE TABLE heap_tbl (pro_number bigint, accessorials accessorial[]);
        """,
        pg_conn,
    )

    rows = """
        VALUES (34342342, array[
                    row('LIFTPU', 25.00, false, false, row(1, 'a'))::accessorial,
                    row('LIMIT', 50.00, false, true, row(2, 'b'))::accessorial]),
               (1, NULL),
               (2, array[]::accessorial[])
    """

    run_command(f"INSERT INTO tbl {rows};", pg_conn)
    run_command(f"INSERT INTO heap_tbl {rows};", pg_conn)
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA composite_unnest CASCADE;", pg_conn)
    pg_conn.commit()

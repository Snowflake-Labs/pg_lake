import pytest
from utils_pytest import *

# An OR of join conditions is split into a UNION ALL with one join per arm,
# since DuckDB cannot use a hash join for the OR.
# Each case: test id, query, whether we expect the query to be rewritten.
test_cases = [
    (
        "two_tables",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id""",
        True,
    ),
    (
        "third_table_on_both_sides",
        """SELECT a.id, b.id, l.name FROM or_join.fa a
           JOIN or_join.fb b ON a.id = b.a_id
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = b.location_id""",
        True,
    ),
    (
        "where_clause",
        """SELECT a.id, l.name FROM or_join.fa a, or_join.fl l
           WHERE a.location_id = l.id OR a.id = l.alt_id""",
        True,
    ),
    (
        "three_arms",
        """SELECT b.id, l.name FROM or_join.fb b
           JOIN or_join.fl l ON l.id = b.location_id OR l.id = b.a_id OR l.alt_id = b.id""",
        True,
    ),
    (
        "arms_with_filters",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON (l.id = a.location_id AND a.kind = 'x')
                             OR (l.alt_id = a.id AND a.kind = 'y')""",
        True,
    ),
    (
        "other_join_conditions",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON (l.id = a.location_id OR l.id = a.alt_location_id)
                             AND a.kind = 'x'
           WHERE a.id > 10""",
        True,
    ),
    (
        "aggregate",
        """SELECT a.kind, count(*), max(l.name) FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id
           GROUP BY a.kind HAVING count(*) > 1""",
        True,
    ),
    (
        "two_ors",
        """SELECT a.id, b.id, l.name FROM or_join.fa a
           JOIN or_join.fb b ON a.id = b.a_id OR a.location_id = b.location_id
           JOIN or_join.fl l ON l.id = a.location_id OR l.alt_id = b.id""",
        True,
    ),
    (
        "outer_side_of_left_join",
        """SELECT a.id, l.name, b.id FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id
           LEFT JOIN or_join.fb b ON b.a_id = a.id""",
        True,
    ),
    (
        "subquery_in_target_list",
        """SELECT a.id, (SELECT count(*) FROM or_join.fb b WHERE b.location_id = l.id)
           FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id""",
        True,
    ),
    (
        "cte",
        """WITH c AS (SELECT * FROM or_join.fl WHERE alt_id > 0)
           SELECT a.id, c.name FROM or_join.fa a
           JOIN c ON c.id = a.location_id OR c.id = a.alt_location_id""",
        True,
    ),
    (
        "order_by_limit",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id
           ORDER BY a.id, l.name LIMIT 20""",
        True,
    ),
    (
        "distinct",
        """SELECT DISTINCT a.kind, l.name FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id""",
        True,
    ),
    (
        "window_function",
        """SELECT a.id, l.name, count(*) OVER (PARTITION BY a.kind)
           FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id""",
        True,
    ),
    (
        "correlated_subquery_on_two_tables",
        """SELECT a.id, (SELECT count(*) FROM or_join.fb b
                         WHERE b.a_id = a.id AND b.location_id = l.id)
           FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id""",
        True,
    ),
    # An outer join would NULL-extend a row once per branch, so the controls
    # below are left alone.
    (
        "left_join_condition",
        """SELECT a.id, l.name FROM or_join.fa a
           LEFT JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id""",
        False,
    ),
    (
        "inner_side_of_left_join",
        """SELECT b.id, a.id, l.name FROM or_join.fb b
           LEFT JOIN (or_join.fa a
                      JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id)
           ON b.a_id = a.id""",
        False,
    ),
    # The tables are already hash joined on another equality, so the OR is
    # only a filter and splitting it would run the join twice.
    (
        "already_joined_in_on_clause",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id
                             AND (l.alt_id = a.id OR l.alt_id = a.alt_location_id)""",
        False,
    ),
    (
        "already_joined_in_where_clause",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON l.id = a.location_id
           WHERE l.alt_id = a.id OR l.alt_id = a.alt_location_id""",
        False,
    ),
    # Each branch would run a MATERIALIZED CTE again
    (
        "materialized_cte",
        """WITH c AS MATERIALIZED (SELECT * FROM or_join.fl WHERE alt_id > 0)
           SELECT a.id, c.name FROM or_join.fa a
           JOIN c ON c.id = a.location_id OR c.id = a.alt_location_id""",
        False,
    ),
    # An arm without an equality would not be a hash join either
    (
        "arm_without_equality",
        """SELECT a.id, l.name FROM or_join.fa a
           JOIN or_join.fl l ON l.id < a.location_id OR l.id = a.alt_location_id""",
        False,
    ),
]


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


@pytest.mark.parametrize(
    "test_id, query, rewritten",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_or_join_rewrite(create_or_join_tables, pg_conn, test_id, query, rewritten):
    if rewritten:
        assert_remote_query_contains_expression(query, "UNION ALL", pg_conn)
    else:
        assert_remote_query_not_contains_expression(query, "UNION ALL", pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["or_join.fa", "or_join.fb", "or_join.fl"],
        ["or_join.ha", "or_join.hb", "or_join.hl"],
    )


def test_or_join_rewrite_join_pushdown(create_or_join_tables, pg_conn):
    """The FDW join pushdown path goes through the same rewrite."""

    run_command(
        "SET LOCAL pg_lake_table.enable_full_query_pushdown TO false",
        pg_conn,
    )

    query = """SELECT a.id, b.id, l.name FROM or_join.fa a
               JOIN or_join.fb b ON a.id = b.a_id
               JOIN or_join.fl l ON l.id = a.location_id OR l.id = b.location_id"""

    assert_remote_query_contains_expression(query, "UNION ALL", pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["or_join.fa", "or_join.fb", "or_join.fl"],
        ["or_join.ha", "or_join.hb", "or_join.hl"],
    )


def test_or_join_rewrite_disabled(create_or_join_tables, pg_conn):
    """The rewrite can be turned off, and the query still gives the same rows."""

    run_command("SET LOCAL pg_lake_engine.enable_or_join_rewrite TO off", pg_conn)

    query = """SELECT a.id, l.name FROM or_join.fa a
               JOIN or_join.fl l ON l.id = a.location_id OR l.id = a.alt_location_id"""

    assert_remote_query_not_contains_expression(query, "UNION ALL", pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["or_join.fa", "or_join.fl"],
        ["or_join.ha", "or_join.hl"],
    )


@pytest.mark.parametrize("full_query_pushdown", [True, False])
def test_or_join_rewrite_modifying_cte(
    create_or_join_tables, pg_conn, full_query_pushdown
):
    """A data-modifying CTE runs once, not once per branch of the rewrite."""

    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {full_query_pushdown}",
        pg_conn,
    )
    run_command("CREATE TEMP TABLE cte_log (n int) ON COMMIT DROP", pg_conn)

    query = """WITH ins AS (INSERT INTO cte_log VALUES (1) RETURNING n)
               SELECT a.id, l.name, (SELECT count(*) FROM ins)
               FROM or_join.{a} a
               JOIN or_join.{l} l ON l.id = a.location_id OR l.id = a.alt_location_id
               ORDER BY 1, 2"""

    fdw_result = run_query(query.format(a="fa", l="fl"), pg_conn)
    assert run_query("SELECT count(*) FROM cte_log", pg_conn)[0][0] == 1

    heap_result = run_query(query.format(a="ha", l="hl"), pg_conn)
    assert len(fdw_result) > 0
    assert fdw_result == heap_result


@pytest.fixture(scope="module")
def create_or_join_tables(pg_conn, s3, request, extension):
    url = f"s3://{TEST_BUCKET}/{request.node.name}/"

    run_command(
        f"""
        CREATE SCHEMA or_join;
        SET search_path TO or_join;

        CREATE FOREIGN TABLE fa (id int, location_id int, alt_location_id int, kind text)
            SERVER pg_lake
            OPTIONS (location '{url}a/', writable 'true', format 'parquet');
        CREATE FOREIGN TABLE fb (id int, a_id int, location_id int)
            SERVER pg_lake
            OPTIONS (location '{url}b/', writable 'true', format 'parquet');
        CREATE FOREIGN TABLE fl (id int, alt_id int, name text)
            SERVER pg_lake
            OPTIONS (location '{url}l/', writable 'true', format 'parquet');

        CREATE TABLE ha (id int, location_id int, alt_location_id int, kind text);
        CREATE TABLE hb (id int, a_id int, location_id int);
        CREATE TABLE hl (id int, alt_id int, name text);

        RESET search_path;
        """,
        pg_conn,
    )

    # NULLs, duplicate keys, and rows that match more than one arm
    rows = {
        "a": """SELECT g,
                       CASE WHEN g % 10 = 0 THEN NULL ELSE g % 50 END,
                       CASE WHEN g % 7 = 0 THEN g % 50 ELSE g % 30 END,
                       (ARRAY['x', 'y', NULL])[1 + g % 3]
                FROM generate_series(1, 500) g""",
        "b": """SELECT g, g % 400, CASE WHEN g % 9 = 0 THEN NULL ELSE g % 40 END
                FROM generate_series(1, 500) g""",
        "l": """SELECT CASE WHEN g % 13 = 0 THEN NULL ELSE g % 60 END, g % 25, 'name' || g
                FROM generate_series(1, 80) g""",
    }

    for table, select in rows.items():
        run_command(f"INSERT INTO or_join.f{table} {select};", pg_conn)
        run_command(f"INSERT INTO or_join.h{table} {select};", pg_conn)
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA or_join CASCADE;", pg_conn)
    pg_conn.commit()

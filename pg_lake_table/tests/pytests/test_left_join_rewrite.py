import pytest
from utils_pytest import *

# ON conditions of a LEFT JOIN that only reference the left side are folded
# into a CASE around the join key, since DuckDB cannot use a hash join for
# them otherwise.
# Each case: test id, query, whether we expect the query to be rewritten.
test_cases = [
    (
        "left_only_condition",
        """SELECT a.id, l.name FROM left_join.fa a
           LEFT JOIN left_join.fl l ON a.kind = 'x' AND a.location_id = l.id""",
        True,
    ),
    (
        "condition_never_true",
        """SELECT a.id, l.name FROM left_join.fa a
           LEFT JOIN left_join.fl l ON a.kind = 'nomatch' AND a.location_id = l.id""",
        True,
    ),
    (
        "left_key_on_the_right",
        """SELECT a.id, l.name FROM left_join.fa a
           LEFT JOIN left_join.fl l
           ON l.id = a.location_id AND a.id > 3 AND a.kind IS NOT NULL""",
        True,
    ),
    (
        "right_only_condition_kept",
        """SELECT a.id, l.name FROM left_join.fa a
           LEFT JOIN left_join.fl l
           ON a.kind = 'x' AND a.location_id = l.id AND l.alt_id > 3""",
        True,
    ),
    (
        "below_another_left_join",
        """SELECT b.id, a.id, l.name FROM left_join.fb b
           LEFT JOIN left_join.fa a ON b.a_id = a.id
           LEFT JOIN left_join.fl l ON a.kind = 'y' AND l.alt_id = a.id""",
        True,
    ),
    (
        "aggregate",
        """SELECT a.kind, count(l.id), max(l.name) FROM left_join.fa a
           LEFT JOIN left_join.fl l ON a.kind = 'x' AND a.location_id = l.id
           GROUP BY a.kind""",
        True,
    ),
    (
        "condition_on_other_relation",
        """SELECT a.id, b.id, l.name FROM left_join.fa a
           JOIN left_join.fb b ON a.id = b.a_id
           LEFT JOIN left_join.fl l ON b.id > 10 AND a.location_id = l.id""",
        True,
    ),
    (
        "right_join",
        """SELECT a.id, l.name FROM left_join.fl l
           RIGHT JOIN left_join.fa a ON a.kind = 'x' AND a.location_id = l.id""",
        True,
    ),
    (
        "right_join_inner_side_condition",
        """SELECT a.id, l.name FROM left_join.fa a
           RIGHT JOIN left_join.fl l ON a.kind = 'x' AND a.location_id = l.id""",
        False,
    ),
    # The controls below are left alone
    (
        "volatile_condition",
        """SELECT a.id, l.name FROM left_join.fa a
           LEFT JOIN left_join.fl l ON a.id + random() < 1000 AND a.location_id = l.id""",
        False,
    ),
    (
        "no_equality",
        """SELECT a.id, l.name FROM left_join.fa a
           LEFT JOIN left_join.fl l ON a.kind = 'x' AND l.id > 3""",
        False,
    ),
    (
        "inner_join",
        """SELECT a.id, l.name FROM left_join.fa a
           JOIN left_join.fl l ON a.kind = 'x' AND a.location_id = l.id""",
        False,
    ),
]


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


@pytest.mark.parametrize("full_query_pushdown", [True, False])
@pytest.mark.parametrize(
    "test_id, query, rewritten",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_left_join_rewrite(
    create_left_join_tables, pg_conn, test_id, query, rewritten, full_query_pushdown
):
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {full_query_pushdown}",
        pg_conn,
    )

    if rewritten:
        assert_remote_query_contains_expression(query, "CASE", pg_conn)
    else:
        assert_remote_query_not_contains_expression(query, "CASE", pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["left_join.fa", "left_join.fb", "left_join.fl"],
        ["left_join.ha", "left_join.hb", "left_join.hl"],
    )


def test_left_join_rewrite_disabled(create_left_join_tables, pg_conn):
    """The rewrite can be turned off, and the query still gives the same rows."""

    run_command("SET LOCAL pg_lake_engine.enable_left_join_rewrite TO off", pg_conn)

    query = """SELECT a.id, l.name FROM left_join.fa a
               LEFT JOIN left_join.fl l ON a.kind = 'x' AND a.location_id = l.id"""

    assert_remote_query_not_contains_expression(query, "CASE", pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["left_join.fa", "left_join.fl"],
        ["left_join.ha", "left_join.hl"],
    )


@pytest.fixture(scope="module")
def create_left_join_tables(pg_conn, s3, request, extension):
    url = f"s3://{TEST_BUCKET}/{request.node.name}/"

    run_command(
        f"""
        CREATE SCHEMA left_join;
        SET search_path TO left_join;

        CREATE FOREIGN TABLE fa (id int, location_id int, kind text)
            SERVER pg_lake
            OPTIONS (location '{url}a/', writable 'true', format 'parquet');
        CREATE FOREIGN TABLE fb (id int, a_id int)
            SERVER pg_lake
            OPTIONS (location '{url}b/', writable 'true', format 'parquet');
        CREATE FOREIGN TABLE fl (id int, alt_id int, name text)
            SERVER pg_lake
            OPTIONS (location '{url}l/', writable 'true', format 'parquet');

        CREATE TABLE ha (id int, location_id int, kind text);
        CREATE TABLE hb (id int, a_id int);
        CREATE TABLE hl (id int, alt_id int, name text);

        RESET search_path;
        """,
        pg_conn,
    )

    # NULLs and duplicate keys on both sides
    rows = {
        "a": """SELECT g,
                       CASE WHEN g % 10 = 0 THEN NULL ELSE g % 50 END,
                       (ARRAY['x', 'y', NULL])[1 + g % 3]
                FROM generate_series(1, 500) g""",
        "b": """SELECT g, CASE WHEN g % 9 = 0 THEN NULL ELSE g % 400 END
                FROM generate_series(1, 500) g""",
        "l": """SELECT CASE WHEN g % 13 = 0 THEN NULL ELSE g % 60 END, g % 25, 'name' || g
                FROM generate_series(1, 80) g""",
    }

    for table, select in rows.items():
        run_command(f"INSERT INTO left_join.f{table} {select};", pg_conn)
        run_command(f"INSERT INTO left_join.h{table} {select};", pg_conn)
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA left_join CASCADE;", pg_conn)
    pg_conn.commit()

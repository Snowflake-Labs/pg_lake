# in these tests we are NOT aiming to test Postgres'
# restrict info capabilities. So, we are not trying out
# with all sorts of data types or query structures
# instead, use some representative set of queries
# that we show PgLake can use restriction info

import pytest
from utils_pytest import *


def test_debug_print_plan_with_lake_table(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        CREATE SCHEMA test_debug_print_plan;
        CREATE TABLE test_debug_print_plan.test(i int) USING iceberg;
        SET LOCAL debug_print_plan TO ON;
        """,
        pg_conn,
    )

    queries = [
        "SELECT count(*) FROM test_debug_print_plan.test",
        "SELECT count(*) FROM test_debug_print_plan.test WHERE i = 1",
    ]

    expected_plan_nodes = {
        "on": "Custom Scan (Query Pushdown)",
        "off": "Foreign Scan",
    }

    for full_pushdown, expected_plan_node in expected_plan_nodes.items():
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {full_pushdown}",
            pg_conn,
        )

        for query in queries:
            explain = run_query(f"EXPLAIN {query}", pg_conn)
            assert expected_plan_node in str(explain)
            assert run_query(query, pg_conn) == [[0]]

    pg_conn.rollback()


def test_restrictions_on_table(pg_conn, s3, extension, with_default_location):

    run_command("CREATE SCHEMA test_restrictions;", pg_conn)
    run_command("CREATE TYPE test_restrictions.t1 AS (a int, b int);", pg_conn)

    run_command(
        "CREATE TABLE test_restrictions.test(bool_col bool, int_col int, text_col text, composite_col test_restrictions.t1) USING iceberg;",
        pg_conn,
    )

    # we print the restrict into with DEBUG2
    run_command("SET client_min_messages TO DEBUG2;", pg_conn)

    filter_and_restrict_info = [
        # simple cases, single filter per column
        ("bool_col IS TRUE", "WHERE (bool_col IS TRUE)"),
        ("int_col = 5", "WHERE (int_col = 5)"),
        ("text_col IS NULL", "WHERE (text_col IS NULL)"),
        (
            "composite_col = '(1,1)'::test_restrictions.t1",
            "WHERE (composite_col = '(1,1)'::test_restrictions.t1)",
        ),
        # multiple filters
        (
            "bool_col IS FALSE AND int_col > 10 OR text_col = upper('test')",
            "WHERE (((bool_col IS FALSE) AND (int_col > 10)) OR (text_col = 'TEST'::text))",
        ),
        # same column multiple filters
        (
            "(int_col >= 0 AND int_col <= 100) OR (int_col >= 500 AND int_col <= 1000) ",
            "(((int_col >= 0) AND (int_col <= 100)) OR ((int_col >= 500) AND (int_col <= 1000)))",
        ),
        # where True cases, note WHERE false behaves slightly differently for fdw, it skips the execution altogether
        ("true and int_col=3", "WHERE (int_col = 3)"),
        ("true or int_col=3", "WHERE true"),
        ("false or int_col=3", "WHERE (int_col = 3)"),
    ]

    for full_pushdown in ["on", "off"]:
        run_command(
            f"set local pg_lake_table.enable_full_query_pushdown TO {full_pushdown}",
            pg_conn,
        )
        for query_filter in filter_and_restrict_info:

            # Clear any existing notices
            pg_conn.notices.clear()

            command = "SELECT * FROM test_restrictions.test WHERE " + str(
                query_filter[0]
            )
            run_command(command, pg_conn)

            assert any(
                "Restrictions for relation:" in str(line) and query_filter[1] in line
                for line in pg_conn.notices
            )

    pg_conn.rollback()


def test_restrictions_on_join(pg_conn, s3, extension, with_default_location):

    run_command("CREATE SCHEMA test_restrictions;", pg_conn)

    run_command(
        """
        SET search_path TO test_restrictions;
        CREATE TABLE t1(bool_col bool, int_col int, text_col text, bigint_col bigint) USING iceberg;
        CREATE TABLE t2(bool_col bool, int_col int, text_col text, bigint_col bigint) USING iceberg;
        CREATE TABLE t3(bool_col bool, int_col int, text_col text, bigint_col bigint, unrelated_col int) USING iceberg;

        CREATE TABLE t4_local(bool_col bool, int_col int, text_col text, bigint_col bigint);

        SET LOCAL geqo_threshold TO 2;
        SET LOCAL geqo_pool_size TO 1000;
        SET LOCAL geqo_generations TO 1000;
        """,
        pg_conn,
    )

    # we print the restrict into with DEBUG2
    run_command("SET client_min_messages TO DEBUG2;", pg_conn)

    # the second query also involves a join of a heap table, but we can still collect all the filters on lake tables
    queries = [
        "SELECT * FROM t1 JOIN t2 USING (int_col, text_col, bigint_col) JOIN t3 USING (text_col, int_col, bigint_col) WHERE t1.int_col = 5 AND t3.text_col ='test' and t1.bigint_col=1 and t3.unrelated_col=15",
        "SELECT * FROM t1 JOIN t2 USING (int_col, text_col, bigint_col) JOIN t3 USING (text_col, int_col, bigint_col) JOIN t4_local USING (text_col, int_col, bigint_col) WHERE t1.int_col = 5 AND t3.text_col ='test' and t3.unrelated_col=15 and t1.bigint_col=1",
    ]

    expected_filter_for_t1 = [
        "FROM t1",
        """WHERE ((int_col = 5) AND (text_col = 'test'::text) AND (bigint_col = 1))""",
    ]

    expected_filter_for_t2 = [
        "FROM t2",
        """WHERE ((int_col = 5) AND (text_col = 'test'::text) AND (bigint_col = 1))""",
    ]

    expected_filter_for_t3 = [
        "FROM t3",
        """WHERE ((int_col = 5) AND (text_col = 'test'::text) AND (bigint_col = 1) AND (unrelated_col = 15))""",
    ]

    for full_pushdown in ["on", "off"]:
        run_command(
            f"set local pg_lake_table.enable_full_query_pushdown TO {full_pushdown}",
            pg_conn,
        )

        for query in queries:
            # Clear any existing notices
            pg_conn.notices.clear()

            run_command(query, pg_conn)

            found_all = all(
                any(
                    all(
                        expected_filter in str(notice)
                        for expected_filter in expected_filter_group
                    )
                    for notice in pg_conn.notices
                )
                for expected_filter_group in [
                    expected_filter_for_t1,
                    expected_filter_for_t2,
                    expected_filter_for_t3,
                ]
            )

            assert found_all

    pg_conn.rollback()


def test_restrictions_on_subqueries(pg_conn, s3, extension, with_default_location):

    run_command("CREATE SCHEMA test_restrictions;", pg_conn)

    run_command(
        """
        SET search_path TO test_restrictions;
        CREATE TABLE t1(bool_col bool, int_col int, text_col text) USING iceberg;
        """,
        pg_conn,
    )

    # we print the restrict into with DEBUG2
    run_command("SET client_min_messages TO DEBUG2;", pg_conn)

    # all queries have the same filters on the table
    expected_filters = """WHERE ((bool_col IS NOT NULL) AND (int_col = 1) AND (text_col = 'test'::text))"""

    queries = [
        "WITH cte_1 AS (SELECT * FROM t1 WHERE int_col = 1 AND text_col = 'test' and bool_col IS NOT NULL) SELECT * FROM cte_1",
        "WITH cte_1 AS (SELECT * FROM t1 ) SELECT * FROM cte_1 WHERE int_col = 1 AND text_col = 'test' and bool_col IS NOT NULL",
        "WITH cte_1 AS MATERIALIZED (SELECT * FROM t1 WHERE int_col = 1 AND text_col = 'test' and bool_col IS NOT NULL) SELECT * FROM cte_1",
        "SELECT * FROM (SELECT * FROM t1 WHERE int_col = 1 AND text_col = 'test' and bool_col IS NOT NULL) as foo",
        "SELECT * FROM (SELECT *,random() FROM t1 WHERE int_col = 1 AND text_col = 'test' and bool_col IS NOT NULL) as foo",
        "SELECT * FROM t1 JOIN LATERAL (SELECT *, random() FROM t1 AS t1_sub WHERE int_col = 1 AND text_col = 'test' AND bool_col IS NOT NULL) AS foo ON t1.int_col = foo.int_col AND t1.text_col = foo.text_col AND t1.bool_col = foo.bool_col;",
    ]

    for full_pushdown in ["on", "off"]:

        for query in queries:

            # Clear any existing notices
            pg_conn.notices.clear()

            run_command(
                f"set local pg_lake_table.enable_full_query_pushdown TO {full_pushdown}",
                pg_conn,
            )

            run_command(query, pg_conn)
            found_all = all(
                any(expected_filter in str(notice) for notice in pg_conn.notices)
                for expected_filter in [expected_filters]
            )

            assert found_all

    pg_conn.rollback()


def test_restrictions_on_params(pg_conn, s3, extension, with_default_location):

    run_command("CREATE SCHEMA test_restrictions_on_params;", pg_conn)

    run_command(
        """
        SET search_path TO test_restrictions_on_params;
        CREATE TABLE t1(int_col int, text_col text) USING iceberg;
        """,
        pg_conn,
    )

    # we print the restrict into with DEBUG2
    run_command("SET client_min_messages TO DEBUG2;", pg_conn)

    for full_pushdown in ["on", "off"]:

        run_command(
            f"set local pg_lake_table.enable_full_query_pushdown TO {full_pushdown}",
            pg_conn,
        )

        run_command(
            f"PREPARE p_{full_pushdown} (int, text) AS SELECT * FROM test_restrictions_on_params.t1 WHERE int_col = $1 and text_col = $2",
            pg_conn,
        )

        for i in range(0, 10):

            # Clear any existing notices
            pg_conn.notices.clear()

            run_command(f"EXECUTE p_{full_pushdown}({i}, {i}::text)", pg_conn)

            expected_filter = f"WHERE ((int_col = {i}) AND (text_col = '{i}'::text))"
            assert any(
                "Restrictions for relation:" in str(line) and expected_filter in line
                for line in pg_conn.notices
            )

    pg_conn.rollback()

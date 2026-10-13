import pytest
import subprocess

from utils_pytest import *


def test_dbt_materialized_table(installcheck, s3, superuser_conn, extension):
    if installcheck:
        return

    # check if materialized table can be recreated multiple times
    dbt_run_helper(["my_first_dbt_model"], repeat=3)

    # check if the materialized table exists in iceberg catalog
    query = "SELECT COUNT(*) = 1 FROM lake_iceberg.tables WHERE table_name = 'my_first_dbt_model'"
    result = run_query(query, superuser_conn)
    assert result[0][0] == True

    # check table rows
    query = "SELECT id FROM my_first_dbt_model ORDER BY id"
    result = run_query(query, superuser_conn)
    assert result == [[1], [2]]

    # drop tables
    run_command("DROP TABLE my_first_dbt_model", superuser_conn)
    superuser_conn.commit()


def test_dbt_materialized_incremental(installcheck, s3, superuser_conn, extension):
    if installcheck:
        return

    # check if incremental table can be recreated multiple times
    dbt_run_helper(["my_first_dbt_model", "my_second_dbt_model"], repeat=3)

    # check if the incremental table exists in iceberg catalog
    query = "SELECT COUNT(*) = 1 FROM lake_iceberg.tables WHERE table_name = 'my_second_dbt_model'"
    result = run_query(query, superuser_conn)
    assert result[0][0] == True

    # check table rows
    result = run_query("SELECT id FROM my_second_dbt_model ORDER BY id", superuser_conn)
    assert result == [[1], [2]]

    # insert into materialized table. for dbt to see the new rows, we commit the transaction
    run_command(
        "INSERT INTO my_first_dbt_model VALUES (3, now()), (4, now())", superuser_conn
    )
    superuser_conn.commit()

    # ingest new rows into incremental table
    dbt_run_helper(["my_second_dbt_model"])

    # check table rows
    result = run_query("SELECT id FROM my_second_dbt_model ORDER BY id", superuser_conn)
    assert result == [[1], [2], [3], [4]]

    # drop tables
    run_command("DROP TABLE my_first_dbt_model, my_second_dbt_model", superuser_conn)
    superuser_conn.commit()


@pytest.mark.parametrize("application_name", ["dbt", "pg_lake_test"])
@pytest.mark.parametrize(
    "predicate, expected",
    [
        pytest.param("IN (1, 2)", [[1, 1], [1, 2]], id="in"),
        pytest.param("NOT IN (1, 2)", [[1, 3]], id="not_in"),
        pytest.param("= ANY (ARRAY[1, 2])", [[1, 1], [1, 2]], id="any"),
        pytest.param("<> ALL (ARRAY[1, 2])", [[1, 3]], id="all"),
    ],
)
def test_dbt_query_context_nested_scalar(
    pg_conn, application_name, predicate, expected
):
    # Visiting the scalar subquery must not replace the outer query's range table.
    query = f"""
        SELECT (SELECT 1) AS scalar, a
        FROM (VALUES (1), (2), (3), (NULL::integer)) AS x(a)
        WHERE a {predicate}
        ORDER BY a
    """
    run_command(f"SET LOCAL application_name = '{application_name}'", pg_conn)
    plan = run_query(f"EXPLAIN (FORMAT JSON) {query}", pg_conn)
    assert "Plan" in plan[0][0][0]
    assert run_query(query, pg_conn) == expected
    pg_conn.rollback()


@pytest.mark.parametrize("application_name", ["dbt", "pg_lake_test"])
def test_dbt_query_context_correlated_var(pg_conn, application_name):
    # x is the second outer RTE, but the nested query has only one RTE.
    # Its correlated Var must not be looked up in the nested range table.
    query = """
        SELECT x.a,
               (SELECT y.b FROM (VALUES (10)) AS y(b) WHERE x.a IN (1, 2))
        FROM (VALUES (0)) AS first_rte(n), (VALUES (1), (2), (3)) AS x(a)
        ORDER BY x.a
    """
    run_command(f"SET LOCAL application_name = '{application_name}'", pg_conn)
    plan = run_query(f"EXPLAIN (FORMAT JSON) {query}", pg_conn)
    assert "Plan" in plan[0][0][0]
    assert run_query(query, pg_conn) == [[1, 10], [2, 10], [3, None]]
    pg_conn.rollback()


def test_dbt_query_context_non_catalog_var(pg_conn):
    # Only pg_class.relkind should have 'f' added, not ordinary char columns.
    query = """
        SELECT (SELECT 1) AS scalar, kind
        FROM (VALUES ('r'::"char"), ('p'::"char"), ('f'::"char")) AS x(kind)
        WHERE kind IN ('r'::"char", 'p'::"char")
        ORDER BY kind
    """
    run_command("SET LOCAL application_name = 'dbt'", pg_conn)
    assert run_query(query, pg_conn) == [[1, "p"], [1, "r"]]
    pg_conn.rollback()


@pytest.mark.parametrize(
    "application_name, expected",
    [
        ("dbt", [[1, "foreign_table"], [1, "heap_table"]]),
        ("pg_lake_test", [[1, "heap_table"]]),
    ],
)
def test_dbt_query_context_foreign_table_discovery(
    superuser_conn, application_name, expected
):
    # A handlerless FDW is sufficient: this only exercises catalog discovery.
    run_command(
        """
        CREATE SCHEMA test_dbt_query_context;
        CREATE FOREIGN DATA WRAPPER dbt_query_context_fdw;
        CREATE SERVER dbt_query_context_server
            FOREIGN DATA WRAPPER dbt_query_context_fdw;
        CREATE FOREIGN TABLE test_dbt_query_context.foreign_table (a integer)
            SERVER dbt_query_context_server;
        CREATE TABLE test_dbt_query_context.heap_table (a integer) USING heap;
        """,
        superuser_conn,
    )
    run_command(f"SET LOCAL application_name = '{application_name}'", superuser_conn)
    assert (
        run_query(
            """
            SELECT (SELECT 1), tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'test_dbt_query_context'
            ORDER BY tablename
            """,
            superuser_conn,
        )
        == expected
    )
    # Keep the catalog filter in the outer query, after visiting SELECT 1.
    # A bounds check alone would avoid the crash but fail to add foreign tables.
    assert (
        run_query(
            """
            SELECT (SELECT 1), c.relname
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'test_dbt_query_context'
              AND c.relkind = ANY (ARRAY['r'::"char", 'p'::"char"])
            ORDER BY c.relname
            """,
            superuser_conn,
        )
        == expected
    )
    superuser_conn.rollback()


def dbt_run_helper(models, repeat=1):
    try:
        for _ in range(repeat):
            subprocess.run(
                [
                    "dbt",
                    "run",
                    "--project-dir",
                    "tests/dbt_pglake_tests",
                    "--profiles-dir",
                    "tests/dbt_pglake_tests",
                    "--log-path",
                    "tests/dbt_pglake_tests/logs",
                    "--select",
                    " ".join(models),
                ],
                check=True,
            )
    except subprocess.CalledProcessError as e:
        print("dbt run command failed with return code:", e.returncode)
        raise e

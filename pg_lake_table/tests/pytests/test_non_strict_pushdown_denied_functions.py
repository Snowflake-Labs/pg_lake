import pytest
from utils_pytest import *


@pytest.mark.parametrize(
    "expression",
    [
        "current_setting('search_path')",
        "current_setting('search_path', true)",
        "current_database()",
        "current_schema()",
        "current_schemas(false)",
        "current_query()",
        "version()",
    ],
)
def test_non_strict_pushdown_denied_functions(
    s3, superuser_conn, extension, with_default_location, expression
):
    """
    Built-ins that collide with DuckDB functions reading query engine state
    must not be pushed down, even with strict pushdown disabled.
    """
    run_command(
        """
        CREATE TABLE test_denied_functions USING iceberg AS SELECT 1 AS a;
        SET LOCAL pg_lake_table.enable_strict_pushdown TO off;
        """,
        superuser_conn,
    )

    query = f"SELECT a, {expression} FROM test_denied_functions"
    remote_name = expression.split("(")[0]
    assert_remote_query_not_contains_expression(query, remote_name, superuser_conn)

    # evaluated locally, so we get the PostgreSQL value
    result = run_query(query, superuser_conn)
    expected = run_query(f"SELECT 1, {expression}", superuser_conn)
    if expression != "current_query()":
        assert result == expected

    superuser_conn.rollback()


def test_strict_pushdown_requires_superuser(superuser_conn, extension):
    run_command(
        """
        CREATE ROLE strict_pushdown_user;
        SET ROLE strict_pushdown_user;
        """,
        superuser_conn,
    )

    error = run_command(
        "SET pg_lake_table.enable_strict_pushdown TO off",
        superuser_conn,
        raise_error=False,
    )
    assert "permission denied" in str(error)

    superuser_conn.rollback()

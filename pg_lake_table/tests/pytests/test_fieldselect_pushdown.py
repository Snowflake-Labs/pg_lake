import pytest
import psycopg2
import duckdb
from decimal import *
from utils_pytest import *

# Using pytest's parametrize decorator to specify different test cases for operator expressions
# Each tuple in the list represents a specific test case with the SQL operator expression and
# the expected expression to assert, followed by a comment indicating the test procedure name.
import pytest

test_cases = [
    # Field access pushdown
    ("access_col_first", "WHERE (point).x = 1", 'WHERE (("point")."x" = 1)'),
    ("access_col_second", "WHERE (point).y = 2", 'WHERE (("point")."y" = 2)'),
    (
        "multi_col",
        "WHERE (point).x = 1 AND (point).y = 2",
        'WHERE ((("point")."x" = 1) AND (("point")."y" = 2))',
    ),
    # Note that commutators are used when accessing a FieldSelect; this is
    # semantically the same, as well as serving as validation of #445.
    ("ineq_col_first", "WHERE (point).x > 0", 'WHERE (0 < ("point")."x")'),
    ("ineq_col_second", "WHERE (point).y <= 10", 'WHERE (10 >= ("point")."y")'),
    ("ineq_col_fs_order", "WHERE 5 < (point).y", 'WHERE (5 < ("point")."y")'),
    # Nested access
    (
        "nested_int",
        "WHERE ((intfoo).bar).baz = 1",
        'WHERE (("intfoo")."bar"."baz" = 1)',
    ),
    (
        "nested_text",
        "WHERE ((foo).bar).baz = 'bat'",
        'WHERE (("foo")."bar"."baz" = \'bat\'::"text")',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, expression, expected_expression",
    test_cases,
    ids=[test_cases[0] for test_cases in test_cases],
)
def test_fieldselect_pushdown(
    create_fieldselect_pushdown_table, pg_conn, test_id, expression, expected_expression
):
    query = "SELECT * FROM fieldselect_pushdown.tbl " + expression
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)


def test_select_star_decomposition(create_fieldselect_pushdown_table, pg_conn):
    query = "SELECT ((foo).bar).* FROM fieldselect_pushdown.tbl"
    res = run_query(query, pg_conn)
    assert res[0]["baz"] == "bat"


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_fieldselect_pushdown_table(duckdb_conn, pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA fieldselect_pushdown;
	            """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
        SELECT {{ 'x': 1, 'y': 2 }} point, {{ 'bar': {{ 'baz': 'bat' }} }} foo,
            {{ 'bar': {{ 'baz': 1 }} }} intfoo
				) TO '{url}'
		""",
        duckdb_conn,
    )

    run_command(
        """
	            CREATE FOREIGN TABLE fieldselect_pushdown.tbl
	            () SERVER pg_lake OPTIONS (format 'parquet', path '{}');
	            """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA fieldselect_pushdown CASCADE", pg_conn)
    pg_conn.commit()

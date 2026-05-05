import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

# including an ID as part of each parameter set
# where id is oprcode for the given operator
# this id shows up in the pytest output
test_cases = [
    (
        "byteaeq",
        "WHERE col_bytea = '\\xdeadbeef'",
        'WHERE ("col_bytea" = (\'\\xde\\xad\\xbe\\xef\'::"text")::"bytea")',
    ),
    (
        "byteane",
        "WHERE col_bytea <> '\\xdeadbeef'",
        'WHERE ("col_bytea" <> (\'\\xde\\xad\\xbe\\xef\'::"text")::"bytea")',
    ),
    (
        "bytea_lt",
        "WHERE col_bytea < '\\xaaaa'",
        'WHERE ("col_bytea" < (\'\\xaa\\xaa\'::"text")::"bytea")',
    ),
    (
        "bytea_le",
        "WHERE col_bytea <= '\\x0000'",
        'WHERE ("col_bytea" <= (\'\\x00\\x00\'::"text")::"bytea")',
    ),
    (
        "bytea_gt",
        "WHERE col_bytea > '\\x0000'",
        'WHERE ("col_bytea" > (\'\\x00\\x00\'::"text")::"bytea")',
    ),
    (
        "bytea_ge",
        "WHERE col_bytea >= '\\xaaaa'",
        'WHERE ("col_bytea" >= (\'\\xaa\\xaa\'::"text")::"bytea")',
    ),
    ("byteacat", "WHERE col_bytea || '\\x0000' = '\\xdeadbeef0000'", "||"),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_bytea_comparison_operator_pushdown(
    create_test_bytea_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM test_bytea_comparison_operator_pushdown.tbl "
        + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_bytea_comparison_operator_pushdown.tbl"],
        ["test_bytea_comparison_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_bytea_comparison_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_test_bytea_comparison_operator_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::bytea as col_bytea
						UNION ALL
                    SELECT ''::bytea as col_bytea
					 	UNION ALL
                    SELECT '\\xdeadbeef'::bytea as col_bytea
                        UNION ALL
                    SELECT '\\x0000'::bytea as col_bytea
				) TO '{url}' WITH (FORMAT 'parquet');

	            CREATE SCHEMA test_bytea_comparison_operator_pushdown;
	            CREATE FOREIGN TABLE test_bytea_comparison_operator_pushdown.tbl
	            (
	            	col_bytea bytea
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{url}');

                CREATE TABLE test_bytea_comparison_operator_pushdown.heap_tbl
                AS SELECT * FROM test_bytea_comparison_operator_pushdown.tbl;
	""",
        pg_conn,
    )

    pg_conn.commit()

    yield
    pg_conn.rollback()

    run_command("DROP SCHEMA test_bytea_comparison_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

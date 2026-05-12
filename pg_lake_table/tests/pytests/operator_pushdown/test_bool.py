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
    ("bool", "WHERE col_int4::boolean", 'WHERE ("col_int4")::boolean'),
    ("bool", "WHERE bool(col_int4)", 'WHERE ("col_int4")::boolean'),
    ("booleq", "WHERE col_bool = true", 'WHERE ("col_bool" = true)'),
    ("boolge", "WHERE col_bool >= true", 'WHERE ("col_bool" >= '),
    ("boolgt", "WHERE col_bool > col_bool2", 'WHERE ("col_bool" > '),
    ("boolle", "WHERE col_bool <= true", 'WHERE ("col_bool" <= '),
    ("boollt", "WHERE col_bool < col_bool2", 'WHERE ("col_bool" < '),
    ("boolne", "WHERE col_bool != true", 'WHERE ("col_bool" <> true)'),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_int_comparison_operator_pushdown(
    create_test_int_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM test_int_comparison_operator_pushdown.tbl " + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_int_comparison_operator_pushdown.tbl"],
        ["test_int_comparison_operator_pushdown.heap_tbl"],
    )


agg_test_cases = [
    ("bool_and", "bool_and(col_bool)", '"bool_and"("col_bool")'),
    ("bool_or", "bool_or(col_bool)", '"bool_or"("col_bool")'),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, agg_expression, expected_expression",
    agg_test_cases,
    ids=[test_case[0] for test_case in agg_test_cases],
)
def test_bool_agg_comparison_operator_pushdown(
    create_test_int_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    agg_expression,
    expected_expression,
):
    query = (
        "SELECT " + agg_expression + " FROM test_int_comparison_operator_pushdown.tbl "
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_int_comparison_operator_pushdown.tbl"],
        ["test_int_comparison_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_int_comparison_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_test_int_comparison_operator_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::int2 as col_int2, NULL::int4 as col_int4, NULL::bigint as col_int8, NULL::bool as col_bool, NULL::bool as col_bool2
						UNION ALL
					SELECT 1::int2 as col_int2, 1::int4 as col_int4, 1::bigint as col_int8, 1::bool, 0::bool
					 	UNION ALL
					SELECT -1::int2 as col_int2, -1::int4 as col_int4, -1::bigint as col_int8, 1::bool, 0::bool
					 	UNION ALL
					SELECT 100::int2 as col_int2, 100::int4 as col_int4, 100::bigint as col_int8, 0::bool, 1::bool
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA test_int_comparison_operator_pushdown;
	            CREATE FOREIGN TABLE test_int_comparison_operator_pushdown.tbl
	            (
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
					col_bool boolean,
					col_bool2 boolean
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE TABLE test_int_comparison_operator_pushdown.heap_tbl
				(
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
					col_bool boolean,
					col_bool2 boolean
	            );
	            COPY test_int_comparison_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_int_comparison_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

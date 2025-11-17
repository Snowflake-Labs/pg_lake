import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

# Using pytest's parametrize decorator to specify different test cases for operator expressions
# Each tuple in the list represents a specific test case with the SQL operator expression and
# the expected expression to assert, followed by a comment indicating the test procedure name.
import pytest

# Time operators
test_cases = [
    (
        "time",
        'WHERE col_time = "time"(col_timestamp)',
        "WHERE (col_time = (col_timestamp)::time without time zone)",
    ),
    (
        "time",
        'WHERE col_time = "time"(col_timetz)',
        "WHERE (col_time = (col_timetz)::time without time zone)",
    ),
    (
        "time_eq",
        "WHERE col_time = '12:00:00'::time",
        "WHERE (col_time = '12:00:00'::time without time zone)",
    ),
    (
        "time_ne",
        "WHERE col_time <> '13:00:00'::time",
        "WHERE (col_time <> '13:00:00'::time without time zone)",
    ),
    (
        "time_lt",
        "WHERE col_time < '14:00:00'::time",
        "WHERE (col_time < '14:00:00'::time without time zone)",
    ),
    (
        "time_le",
        "WHERE col_time <= '15:00:00'::time",
        "WHERE (col_time <= '15:00:00'::time without time zone)",
    ),
    (
        "time_gt",
        "WHERE col_time > '16:00:00'::time",
        "WHERE (col_time > '16:00:00'::time without time zone)",
    ),
    (
        "time_ge",
        "WHERE col_time >= '17:00:00'::time",
        "WHERE (col_time >= '17:00:00'::time without time zone)",
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_cases[0] for test_cases in test_cases],
)
def test_time_operator_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM time_operator_pushdown.tbl " + operator_expression
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["time_operator_pushdown.tbl"],
        ["time_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA time_operator_pushdown;
	            CREATE TYPE time_operator_pushdown.mood AS ENUM ('sad', 'ok', 'happy');

	            """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT  NULL::interval as interval_col, NULL::date AS col_date, NULL::timestamp AS col_timestamp,  NULL::timestamptz AS col_timestamptz,  NULL::timetz AS col_timetz,  NULL::time AS col_time
						UNION ALL
					SELECT '1 day', '2024-01-01', '2024-01-01 12:00:00', '2024-01-01 12:00:00+00', '12:00:00+00', '12:00:00'
					 	UNION ALL
					SELECT  '2 days', '2023-01-01', '2023-01-01 13:00:00', '2023-01-01 13:00:00+00',  '13:00:00+01', '13:00:00'
						UNION ALL
					SELECT '962 days', '2022-12-31', '2022-12-31 23:59:59','2024-01-01 00:00:00+00'::timestamp,  '23:59:59+02', '23:59:59'
					 	UNION ALL
					SELECT  '2 days', '2023-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00+00',  '13:00:00+01', '13:00:00'

				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE FOREIGN TABLE time_operator_pushdown.tbl
	            (
                    col_interval interval,
        			col_date date,
					col_timestamp timestamp,
					col_timestamptz timestamptz,
					col_timetz time with time zone,
					col_time time
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
	            CREATE TABLE time_operator_pushdown.heap_tbl
				(
                    col_interval interval,
        			col_date date,
					col_timestamp timestamp,
					col_timestamptz timestamptz,
					col_timetz time with time zone,
					col_time time
	            );
	            COPY time_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA time_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

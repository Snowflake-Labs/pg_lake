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

# Interval operators
test_cases = [
    (
        "interval_eq",
        "WHERE col_interval = INTERVAL '1 day'",
        "WHERE (\"col_interval\" = '1 day'::interval)",
    ),
    (
        "interval_ne",
        "WHERE col_interval <> INTERVAL '2 days'",
        "WHERE (\"col_interval\" <> '2 days'::interval)",
    ),
    (
        "interval_lt",
        "WHERE col_interval < INTERVAL '3 days'",
        "WHERE (\"col_interval\" < '3 days'::interval)",
    ),
    (
        "interval_le",
        "WHERE col_interval <= INTERVAL '4 days'",
        "WHERE (\"col_interval\" <= '4 days'::interval)",
    ),
    (
        "interval_gt",
        "WHERE col_interval > INTERVAL '1 hour'",
        "WHERE (\"col_interval\" > '01:00:00'::interval)",
    ),
    (
        "interval_ge",
        "WHERE col_interval >= INTERVAL '2 hours'",
        "WHERE (\"col_interval\" >= '02:00:00'::interval)",
    ),
    (
        "interval_pl",
        "WHERE col_interval + INTERVAL '1 day' = INTERVAL '2 days'",
        "WHERE ((\"col_interval\" + '1 day'::interval) = '2 days'::interval)",
    ),
    (
        "interval_mi",
        "WHERE col_interval - INTERVAL '1 day' = INTERVAL '1 day'",
        "WHERE ((\"col_interval\" - '1 day'::interval) = '1 day'::interval)",
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
    extension,
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

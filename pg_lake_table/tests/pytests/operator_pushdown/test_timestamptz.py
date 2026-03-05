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

# timestamptz operators, do not print the whole expected expression as that depends on the timezone of the server
test_cases = [
    (
        "timestamptz",
        "WHERE timestamptz(col_timestamp) = col_timestamptz",
        "WHERE ((col_timestamp)::timestamp with time zone = ",
    ),
    (
        "timestamptz",
        "WHERE timestamptz(col_date) = '2024-01-01 00:00:00'",
        "WHERE ((col_date)::timestamp with time zone = ",
    ),
    (
        "timestamptz_eq",
        "WHERE col_timestamptz = col_timestamptz",
        "WHERE (col_timestamptz ",
    ),
    (
        "timestamptz_ne",
        "WHERE col_timestamptz <> '2023-01-01 13:00:00+00'::timestamptz",
        "WHERE (col_timestamptz <> ",
    ),
    (
        "timestamptz_lt",
        "WHERE col_timestamptz < '2024-01-01 12:00:00'::timestamptz",
        "WHERE (col_timestamptz < ",
    ),
    (
        "timestamptz_le",
        "WHERE col_timestamptz <= '2023-01-01 23:59:59'::timestamptz",
        "WHERE (col_timestamptz <= ",
    ),
    (
        "timestamptz_gt",
        "WHERE col_timestamptz > '2023-01-01 13:00:00'::timestamptz",
        "WHERE (col_timestamptz > ",
    ),
    (
        "timestamptz_ge",
        "WHERE col_timestamptz >= '2022-12-31 23:59:59'::timestamptz",
        "WHERE (col_timestamptz >= ",
    ),
    (
        "timestamptz_mi",
        "WHERE col_timestamptz - '2022-12-30'::timestamptz >= INTERVAL '1 day'",
        "WHERE ((col_timestamptz - ('",
    ),
    (
        "timestamptz_eq_date",
        "WHERE col_timestamptz = '2024-01-01'::date",
        "WHERE (col_timestamptz = ",
    ),
    (
        "timestamptz_ge_date",
        "WHERE col_timestamptz >= '2024-01-01'::date",
        "WHERE (col_timestamptz >= ",
    ),
    (
        "timestamptz_gt_date",
        "WHERE col_timestamptz > '2024-01-01'::date",
        "WHERE (col_timestamptz > ",
    ),
    (
        "timestamptz_le_date",
        "WHERE col_timestamptz <= '2024-01-01'::date",
        "WHERE (col_timestamptz <= ",
    ),
    (
        "timestamptz_lt_date",
        "WHERE col_timestamptz < '2024-01-01'::date",
        "WHERE (col_timestamptz < ",
    ),
    (
        "timestamptz_ne_date",
        "WHERE col_timestamptz <> '2024-01-01'::date",
        "WHERE (col_timestamptz <> ",
    ),
    (
        "timestamptz_eq_timestamp",
        "WHERE col_timestamptz = '2024-01-01'::timestamp",
        "WHERE (col_timestamptz = ",
    ),
    (
        "timestamptz_ge_timestamp",
        "WHERE col_timestamptz >= '2024-01-01'::timestamp",
        "WHERE (col_timestamptz >= ",
    ),
    (
        "timestamptz_gt_timestamp",
        "WHERE col_timestamptz > '2024-01-01'::timestamp",
        "WHERE (col_timestamptz > ",
    ),
    (
        "timestamptz_le_timestamp",
        "WHERE col_timestamptz <= '2024-01-01'::timestamp",
        "WHERE (col_timestamptz <= ",
    ),
    (
        "timestamptz_lt_timestamp",
        "WHERE col_timestamptz < '2024-01-01'::timestamp",
        "WHERE (col_timestamptz < ",
    ),
    (
        "timestamptz_ne_timestamp",
        "WHERE col_timestamptz <> '2024-01-01'::timestamp",
        "WHERE (col_timestamptz <> ",
    ),
    (
        "timestamptz_pl_interval",
        "WHERE col_timestamptz + INTERVAL '1 day' = '2024-01-02 12:00:00'::timestamptz",
        "WHERE ((col_timestamptz + '1 day'::interval) = ",
    ),
    (
        "timestamptz_mi_interval",
        "WHERE col_timestamptz - INTERVAL '1 day' >= '2022-12-30'::timestamptz",
        "WHERE ((col_timestamptz - '1 day'::interval)",
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

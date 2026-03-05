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

# Date operators
test_cases = [
    (
        "date",
        "WHERE col_date = date(col_timestamp)",
        "WHERE (col_date = (col_timestamp)::date)",
    ),
    (
        "date",
        "WHERE col_date = date(col_timestamptz)",
        "WHERE (col_date = (col_timestamptz)::date)",
    ),
    (
        "date_eq",
        "WHERE col_date = '2024-01-01'",
        "WHERE (col_date = ('2024-01-01'::text)::date)",
    ),
    (
        "date_ne",
        "WHERE col_date <> '2023-01-01'",
        "WHERE (col_date <> ('2023-01-01'::text)::date)",
    ),
    (
        "date_lt",
        "WHERE col_date < '2024-01-01'",
        "WHERE (col_date < ('2024-01-01'::text)::date)",
    ),
    (
        "date_le",
        "WHERE col_date <= '2022-12-31'",
        "WHERE (col_date <= ('2022-12-31'::text)::date)",
    ),
    (
        "date_gt",
        "WHERE col_date > '2023-01-01'",
        "WHERE (col_date > ('2023-01-01'::text)::date)",
    ),
    (
        "date_ge",
        "WHERE col_date >= '2022-12-31'",
        "WHERE (col_date >= ('2022-12-31'::text)::date)",
    ),
    (
        "date_eq_timestamp",
        "WHERE col_date = '2024-01-01 00:00:00'::timestamp without time zone",
        "WHERE (col_date = ",
    ),
    (
        "date_ge_timestamp",
        "WHERE col_date >= '2023-01-01 12:00:00'::timestamp without time zone",
        "WHERE (col_date >= ",
    ),
    (
        "date_gt_timestamp",
        "WHERE col_date > '2023-01-01 12:00:00'::timestamp without time zone",
        "WHERE (col_date > ",
    ),
    (
        "date_le_timestamp",
        "WHERE col_date <= '2025-01-01 12:00:00'::timestamp without time zone",
        "WHERE (col_date <= ",
    ),
    (
        "date_lt_timestamp",
        "WHERE col_date < '2025-01-01 12:00:00'::timestamp without time zone",
        "WHERE (col_date < ",
    ),
    (
        "date_ne_timestamp",
        "WHERE col_date != '2021-01-01 12:00:00'::timestamp without time zone",
        "WHERE (col_date <> ",
    ),
    (
        "date_eq_timestamptz",
        "WHERE col_date = '2024-01-01 00:00:00'::timestamptz",
        "WHERE (col_date = ",
    ),
    (
        "date_ge_timestamptz",
        "WHERE col_date >= '2023-01-01 12:00:00'::timestamptz",
        "WHERE (col_date >= ",
    ),
    (
        "date_gt_timestamptz",
        "WHERE col_date > '2023-01-01 12:00:00'::timestamptz",
        "WHERE (col_date > ",
    ),
    (
        "date_le_timestamptz",
        "WHERE col_date <= '2025-01-01 12:00:00'::timestamptz",
        "WHERE (col_date <= ",
    ),
    (
        "date_lt_timestamptz",
        "WHERE col_date < '2025-01-01 12:00:00'::timestamptz",
        "WHERE (col_date < ",
    ),
    (
        "date_ne_timestamptz",
        "WHERE col_date != '2021-01-01 12:00:00'::timestamptz",
        "WHERE (col_date <> ",
    ),
    (
        "date_pli",
        "WHERE col_date + 1 = '2024-01-02'::date",
        "WHERE ((col_date + 1) = ('2024-01-02'::text)::date)",
    ),
    (
        "date_mii",
        "WHERE col_date - 1 = '2023-12-31'::date",
        "WHERE ((col_date - 1) = ('2023-12-31'::text)::date)",
    ),
    (
        "date_pl_interval",
        "WHERE col_date + interval '1 day' = '2024-01-02'::date",
        "WHERE ((col_date + '1 day'::interval) = ('2024-01-02'::text)::date)",
    ),
    (
        "date_mi_interval",
        "WHERE col_date - interval '1 day' = '2023-12-31'::date",
        "WHERE ((col_date - '1 day'::interval) = ('2023-12-31'::text)::date)",
    ),
    (
        "datetime_pl",
        "WHERE col_date + '03:04'::time = '2024-01-01 03:04:00'",
        "WHERE ((col_date + ",
    ),
    (
        "datetimetz_pl",
        "WHERE col_date + '23:59+00'::timetz = '2024-01-01 23:59:00+00'",
        "WHERE ((col_date +",
    ),
    (
        "date_mi",
        "WHERE col_date - '2023-12-31' = 1",
        "WHERE ((col_date - ('2023-12-31'::text)::date) = 1)",
    ),
    (
        "date cast",
        "WHERE col_timestamp::date = '2024-01-01'::date",
        "WHERE ((col_timestamp)::date = ('2024-01-01'::text)::date)",
    ),
    (
        "date cast",
        "WHERE col_timestamptz::date = '2024-01-01'::date",
        "WHERE ((col_timestamptz)::date = ('2024-01-01'::text)::date)",
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

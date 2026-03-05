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

# timestamp operators
test_cases = [
    (
        "timestamp",
        'WHERE "timestamp"(col_timestamptz) = col_timestamptz',
        "WHERE ((col_timestamptz)::timestamp without time zone = ",
    ),
    (
        "timestamp",
        "WHERE \"timestamp\"(col_date) = '2024-01-01 00:00:00'",
        "WHERE ((col_date)::timestamp without time zone = ",
    ),
    (
        "timestamp_eq",
        "WHERE col_timestamp = '2024-01-01 12:00:00'::timestamp",
        "WHERE (col_timestamp = ('2024-01-01 12:00:00'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_ne",
        "WHERE col_timestamp <> '2023-01-01 13:00:00'::timestamp",
        "WHERE (col_timestamp <> ('2023-01-01 13:00:00'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_lt",
        "WHERE col_timestamp < '2024-01-01 12:00:00'::timestamp",
        "WHERE (col_timestamp < ('2024-01-01 12:00:00'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_le",
        "WHERE col_timestamp <= '2022-12-31 23:59:59'::timestamp",
        "WHERE (col_timestamp <= ('2022-12-31 23:59:59'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_gt",
        "WHERE col_timestamp > '2023-01-01 13:00:00'::timestamp",
        "WHERE (col_timestamp > ('2023-01-01 13:00:00'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_ge",
        "WHERE col_timestamp >= '2022-12-31 23:59:59'::timestamp",
        "WHERE (col_timestamp >= ('2022-12-31 23:59:59'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_mi",
        "WHERE col_timestamp - '2022-12-30'::timestamp >= INTERVAL '1 day'",
        "WHERE ((col_timestamp - ('",
    ),
    (
        "timestamp_eq_date",
        "WHERE col_timestamp = '2024-01-01'::date",
        "WHERE (col_timestamp = ('2024-01-01'::text)::date)",
    ),
    (
        "timestamp_ge_date",
        "WHERE col_timestamp >= '2024-01-01'::date",
        "WHERE (col_timestamp >= ('2024-01-01'::text)::date)",
    ),
    (
        "timestamp_gt_date",
        "WHERE col_timestamp > '2024-01-01'::date",
        "WHERE (col_timestamp > ('2024-01-01'::text)::date)",
    ),
    (
        "timestamp_le_date",
        "WHERE col_timestamp <= '2024-01-01'::date",
        "WHERE (col_timestamp <= ('2024-01-01'::text)::date)",
    ),
    (
        "timestamp_lt_date",
        "WHERE col_timestamp < '2024-01-01'::date",
        "WHERE (col_timestamp < ('2024-01-01'::text)::date)",
    ),
    (
        "timestamp_ne_date",
        "WHERE col_timestamp <> '2024-01-01'::date",
        "WHERE (col_timestamp <> ('2024-01-01'::text)::date)",
    ),
    (
        "timestamp_eq_timestamptz",
        "WHERE col_timestamp = '2024-01-01 00:00:00'::timestamptz",
        "WHERE (col_timestamp = ",
    ),
    (
        "timestamp_ge_timestamptz",
        "WHERE col_timestamp >= '2023-01-01 12:00:00'::timestamptz",
        "WHERE (col_timestamp >= ",
    ),
    (
        "timestamp_gt_timestamptz",
        "WHERE col_timestamp > '2023-01-01 12:00:00'::timestamptz",
        "WHERE (col_timestamp > ",
    ),
    (
        "timestamp_le_timestamptz",
        "WHERE col_timestamp <= '2025-01-01 12:00:00'::timestamptz",
        "WHERE (col_timestamp <= ",
    ),
    (
        "timestamp_lt_timestamptz",
        "WHERE col_timestamp < '2025-01-01 12:00:00'::timestamptz",
        "WHERE (col_timestamp < ",
    ),
    (
        "timestamp_ne_timestamptz",
        "WHERE col_timestamp != '2021-01-01 12:00:00'::timestamptz",
        "WHERE (col_timestamp <> ",
    ),
    (
        "timestamp_pl_interval",
        "WHERE col_timestamp + INTERVAL '1 day' = '2024-01-02 12:00:00'::timestamp",
        "WHERE ((col_timestamp + '1 day'::interval) = ('2024-01-02 12:00:00'::text)::timestamp without time zone)",
    ),
    (
        "timestamp_mi_interval",
        "WHERE col_timestamp - INTERVAL '1 day' >= '2022-12-30'::timestamp",
        "WHERE ((col_timestamp - '1 day'::interval)",
    ),
    # todo: move below tests
    # Push down now
    ("now", "WHERE col_timestamp < now()", "WHERE (col_timestamp "),
    ("now tz", "WHERE col_timestamptz < now()", "WHERE (col_timestamptz "),
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


def test_now(create_operator_pushdown_table, pg_conn):
    url = f"s3://{TEST_BUCKET}/test_now/data.parquet"

    run_command(
        f"""
        copy (select now() AS col from generate_series(1,10) s) to '{url}';
        create foreign table now_table ()
        server pg_lake options (path '{url}');
    """,
        pg_conn,
    )

    # now() should return the same result
    result = run_query("select count(*) from now_table where col = now()", pg_conn)
    assert result[0]["count"] == 10

    pg_conn.rollback()


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

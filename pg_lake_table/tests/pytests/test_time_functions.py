import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

extract_cases = [
    ("years", "col_date", "= 2019"),
    ("hour", "col_interval", "= 4"),
    ("minutes", "col_timetz", "= 56"),
    ("hour", "col_time", "= 23"),
    ("century", "col_timestamptz", "= 21"),
    ("second", "col_timestamp", "= 0"),
    ("day", "col_timestamp", "= 31"),
    ("decade", "col_timestamp", "= 201"),
    ("dow", "col_timestamp", "= 2"),
    ("doy", "col_timestamp", "= 1"),
    ("epoch", "col_timestamp", "= 1577836620"),
    ("isodow", "col_timestamp", "= 2"),
    ("isoyear", "col_timestamp", "= 2022"),
    ("microseconds", "col_timestamp", "= 0"),
    ("millennium", "col_timestamp", "= 3"),
    ("milliseconds", "col_timestamp", "= 0"),
    ("milliseconds", "col_timestamp", "= 0"),
    ("quarter", "col_timestamp", "= 1"),
    ("week", "col_timestamptz", "= 1"),
    # result depends on machine time zone
    ("timezone", "col_timestamptz", " is not null"),
    ("timezone_hour", "col_timestamptz", " is not null"),
    ("timezone_minute", "col_timestamptz", " is not null"),
    # multiple rewrites in a single clause
    ("year", "col_timestamp + col_interval", "> extract(years from col_timestamp)"),
]

date_trunc_cases = [
    ("year", "col_date", "= '2019-01-01'"),
    ("day", "col_interval", "= interval '3 days'"),
    ("minute", "col_timestamptz", "= '2019-12-31 23:58:00+0'"),
    ("hour", "col_timestamptz", "= '2019-12-31 23:00:00+0'"),
    ("century", "col_timestamp", "= '2001-01-01'"),
    ("second", "col_timestamp", "= '2019-12-31 23:57:00'"),
    ("day", "col_timestamp", "= '2019-12-31 00:00:00'"),
    ("decade", "col_timestamp", "= '2010-01-01 00:00:00'"),
    ("microseconds", "col_timestamp", "= '2019-12-31 23:57:00'"),
    ("millennium", "col_date", "= '2001-01-01'"),
    ("milliseconds", "col_timestamp", "= '2019-12-31 23:57:00'"),
    ("quarter", "col_timestamp", "= '2019-10-01 00:00:00'"),
    ("week", "col_timestamp", "= '2019-12-30'"),
]

# Define date_bin test cases
test_cases = [
    (
        "date_bin(interval,timestamp,timestamp)",
        "WHERE date_bin(interval '5 minutes', col_timestamp, '2023-01-01 00:00:00') = '2023-01-01 00:05:00'",
        "time_bucket",
    ),
    (
        "date_bin(interval,timestamptz,timestamptz",
        "WHERE date_bin(interval '5 minutes', col_timestamptz, '2023-01-01 00:00:00+00') = '2023-01-01 00:05:00+00'",
        "time_bucket",
    ),
]

# Convert templates to (test ID, WHERE clause, expected pushdown phrase)
test_cases += [
    (
        f"extract({ex[0]} from {ex[1]})",
        f"WHERE extract({ex[0]} from {ex[1]}) {ex[2]}",
        "date_part",
    )
    for ex in extract_cases
]
test_cases += [
    (
        f"date_part('{ex[0]}', {ex[1]})",
        f"WHERE date_part('{ex[0]}', {ex[1]}) {ex[2]}",
        "date_part",
    )
    for ex in extract_cases
]
test_cases += [
    (
        f"date_trunc('{ex[0]}', {ex[1]})",
        f"WHERE date_trunc('{ex[0]}', {ex[1]}) {ex[2]}",
        "date_trunc",
    )
    for ex in date_trunc_cases
]

test_cases += [
    (
        "age(timestamp,timestamp)",
        "WHERE age(col_timestamp, '2019-01-01'::timestamp) IS NOT NULL",
        "age",
    ),
    (
        "isfinite(date)",
        "WHERE isfinite(col_date)",
        "isfinite",
    ),
    (
        "isfinite(timestamp)",
        "WHERE isfinite(col_timestamp)",
        "isfinite",
    ),
    (
        "isfinite(timestamptz)",
        "WHERE isfinite(col_timestamptz)",
        "isfinite",
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_time_functions_pushdown(
    create_time_function_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT col_date FROM time_function_pushdown.tbl " + operator_expression
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["time_function_pushdown.tbl"],
        ["time_function_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_time_function_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_time_function_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::date col_date, NULL::interval col_interval, NULL::time col_time, NULL::timetz col_timetz, NULL::timestamp col_timestamp, NULL::timestamptz col_timestamptz
						UNION ALL
					SELECT '2019-12-31'::date, interval '3 days 4 hours', '23:55'::time, '23:56'::timetz, '2019-12-31 23:57:00'::timestamp, '2019-12-31 23:58:00+0'::timestamptz
					 	UNION ALL
					SELECT '2023-01-01'::date, interval '5 days', '00:05'::time, '00:06'::timetz, '2023-01-01 00:07:00'::timestamp, '2023-01-01 00:08:00+0'::timestamptz
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA time_function_pushdown;
	            CREATE FOREIGN TABLE time_function_pushdown.tbl
	            (
	            	col_date date,
	            	col_interval interval,
	            	col_time time,
	            	col_timetz timetz,
	            	col_timestamp timestamp,
	            	col_timestamptz timestamptz
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
	            CREATE TABLE time_function_pushdown.heap_tbl
				(
                    LIKE time_function_pushdown.tbl
	            );
	            COPY time_function_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA time_function_pushdown CASCADE", pg_conn)
    pg_conn.commit()

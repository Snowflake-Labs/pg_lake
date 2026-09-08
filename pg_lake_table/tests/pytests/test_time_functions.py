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


def _isfinite_tables():
    return ["isfinite_fn.tbl"], ["isfinite_fn.heap_tbl"]


@pytest.fixture(scope="module")
def create_isfinite_table(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/create_isfinite_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::date AS col_date,
					       NULL::timestamp AS col_timestamp,
					       NULL::timestamptz AS col_timestamptz
					UNION ALL
					SELECT '2024-01-01'::date,
					       '2024-01-01 12:00:00'::timestamp,
					       '2024-01-01 12:00:00+00'::timestamptz
					UNION ALL
					SELECT '1970-01-01'::date,
					       '1970-01-01 00:00:00'::timestamp,
					       '1970-01-01 00:00:00+00'::timestamptz
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
	            CREATE SCHEMA isfinite_fn;
	            CREATE FOREIGN TABLE isfinite_fn.tbl
	            (
	            	col_date date,
	            	col_timestamp timestamp,
	            	col_timestamptz timestamptz
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
	            """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
	            CREATE TABLE isfinite_fn.heap_tbl (LIKE isfinite_fn.tbl);
	            COPY isfinite_fn.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA isfinite_fn CASCADE", pg_conn)
    pg_conn.commit()


isfinite_match_queries = [
    (
        "col_date",
        "SELECT isfinite(col_date) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "col_timestamp",
        "SELECT isfinite(col_timestamp) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "col_timestamptz",
        "SELECT isfinite(col_timestamptz) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "not_col_date",
        "SELECT NOT isfinite(col_date) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "not_col_timestamp",
        "SELECT NOT isfinite(col_timestamp) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "not_col_timestamptz",
        "SELECT NOT isfinite(col_timestamptz) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "where_col_date",
        "SELECT col_date FROM isfinite_fn.tbl WHERE isfinite(col_date)",
        "isfinite",
    ),
    (
        "where_col_timestamp",
        "SELECT col_timestamp FROM isfinite_fn.tbl WHERE isfinite(col_timestamp)",
        "isfinite",
    ),
    (
        "where_col_timestamptz",
        "SELECT col_timestamptz FROM isfinite_fn.tbl WHERE isfinite(col_timestamptz)",
        "isfinite",
    ),
    (
        "null_date",
        "SELECT isfinite(NULL::date) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "null_timestamp",
        "SELECT isfinite(NULL::timestamp) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "null_timestamptz",
        "SELECT isfinite(NULL::timestamptz) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "inf_date",
        "SELECT isfinite('infinity'::date) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "neg_inf_date",
        "SELECT isfinite('-infinity'::date) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "inf_timestamp",
        "SELECT isfinite('infinity'::timestamp) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "neg_inf_timestamp",
        "SELECT isfinite('-infinity'::timestamp) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "inf_timestamptz",
        "SELECT isfinite('infinity'::timestamptz) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "neg_inf_timestamptz",
        "SELECT isfinite('-infinity'::timestamptz) FROM isfinite_fn.tbl",
        "isfinite",
    ),
    (
        "in_range_timestamp",
        "SELECT isfinite('294247-01-10 04:00:54.775806'::timestamp) FROM isfinite_fn.tbl",
        "isfinite",
    ),
]


@pytest.mark.parametrize(
    "test_id, query, expected_expression",
    isfinite_match_queries,
    ids=[c[0] for c in isfinite_match_queries],
)
def test_isfinite_pushdown_matches_heap(
    create_isfinite_table, pg_conn, test_id, query, expected_expression
):
    assert_query_pushdownable(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    fdw, heap = _isfinite_tables()
    assert_query_results_on_tables(query, pg_conn, fdw, heap)


isfinite_range_overflow_queries = [
    (
        "pg_max_timestamp",
        "SELECT isfinite('294276-12-31 23:59:59'::timestamp) FROM isfinite_fn.tbl",
    ),
    (
        "pg_max_timestamptz",
        "SELECT isfinite('294276-12-31 23:59:59+00'::timestamptz) FROM isfinite_fn.tbl",
    ),
]


@pytest.mark.parametrize(
    "test_id, query",
    isfinite_range_overflow_queries,
    ids=[c[0] for c in isfinite_range_overflow_queries],
)
def test_isfinite_out_of_duckdb_range_errors_on_fdw(
    create_isfinite_table, pg_conn, test_id, query
):
    # Postgres accepts a wider finite timestamp range than DuckDB.
    with pytest.raises(Exception, match="out of range"):
        run_query(query, pg_conn)
    pg_conn.rollback()
    heap_query = query.replace("isfinite_fn.tbl", "isfinite_fn.heap_tbl")
    run_query(heap_query, pg_conn)
    pg_conn.rollback()

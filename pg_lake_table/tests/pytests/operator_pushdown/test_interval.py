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
    (
        "interval_um",
        "WHERE - col_interval = INTERVAL '-1 day'",
        "WHERE ((- \"col_interval\") = '-1 days'::interval)",
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


@pytest.fixture(scope="module")
def create_interval_um_table(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/create_interval_um_table/data.parquet"
    run_command(
        f"""
        COPY (
            SELECT 1 AS id, NULL::interval AS col_interval
            UNION ALL SELECT 2, INTERVAL '0 seconds'
            UNION ALL SELECT 3, INTERVAL '1 second'
            UNION ALL SELECT 4, INTERVAL '1 minute'
            UNION ALL SELECT 5, INTERVAL '1 hour'
            UNION ALL SELECT 6, INTERVAL '1 day'
            UNION ALL SELECT 7, INTERVAL '1 month'
            UNION ALL SELECT 8, INTERVAL '1 year'
            UNION ALL SELECT 9, INTERVAL '3 days 4 hours'
            UNION ALL SELECT 10, INTERVAL '1 month 2 days 03:04:05'
        ) TO '{url}' WITH (FORMAT 'parquet');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE SCHEMA interval_um;
        CREATE FOREIGN TABLE interval_um.tbl (id int, col_interval interval)
        SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE interval_um.heap_tbl (LIKE interval_um.tbl);
        COPY interval_um.heap_tbl FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA interval_um CASCADE", pg_conn)
    pg_conn.commit()


def _um_tables():
    return ["interval_um.tbl"], ["interval_um.heap_tbl"]


interval_um_queries = [
    (
        "project",
        "SELECT id, - col_interval FROM interval_um.tbl",
        '(- "col_interval")',
    ),
    (
        "double_negation",
        "SELECT id, - (- col_interval) FROM interval_um.tbl",
        '(- (- "col_interval"))',
    ),
    (
        "is_null",
        "SELECT id FROM interval_um.tbl WHERE - col_interval IS NULL",
        '(- "col_interval")',
    ),
    (
        "eq_zero",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = INTERVAL '0 seconds'",
        '(- "col_interval")',
    ),
    (
        "eq_neg_day",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = INTERVAL '-1 day'",
        '(- "col_interval")',
    ),
    (
        "eq_neg_month",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = INTERVAL '-1 month'",
        '(- "col_interval")',
    ),
    (
        "eq_neg_year",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = INTERVAL '-1 year'",
        '(- "col_interval")',
    ),
    (
        "eq_neg_second",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = INTERVAL '-1 second'",
        '(- "col_interval")',
    ),
    (
        "eq_neg_mixed_hms",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = INTERVAL '-3 days -04:00:00'",
        '(- "col_interval")',
    ),
    (
        "filter_on_negated_column",
        "SELECT id FROM interval_um.tbl WHERE - col_interval < INTERVAL '0 seconds'",
        '(- "col_interval")',
    ),
]


@pytest.mark.parametrize(
    "test_id, query, expected_expression",
    interval_um_queries,
    ids=[c[0] for c in interval_um_queries],
)
def test_interval_um_pushdown_matches_heap(
    create_interval_um_table, pg_conn, test_id, query, expected_expression
):
    assert_query_pushdownable(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    fdw, heap = _um_tables()
    assert_query_results_on_tables(query, pg_conn, fdw, heap)


def test_interval_um_infinite_const_not_pushed(create_interval_um_table, pg_conn):
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("infinity intervals require PostgreSQL 17+")

    query = (
        "SELECT id FROM interval_um.tbl "
        "WHERE - col_interval IS DISTINCT FROM INTERVAL 'infinity'"
    )
    assert_query_not_pushdownable(query, pg_conn)
    fdw, heap = _um_tables()
    assert_query_results_on_tables(query, pg_conn, fdw, heap)


def test_interval_um_neg_infinite_const_not_pushed(create_interval_um_table, pg_conn):
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("infinity intervals require PostgreSQL 17+")

    query = (
        "SELECT id FROM interval_um.tbl "
        "WHERE - col_interval IS DISTINCT FROM INTERVAL '-infinity'"
    )
    assert_query_not_pushdownable(query, pg_conn)
    fdw, heap = _um_tables()
    assert_query_results_on_tables(query, pg_conn, fdw, heap)


interval_const_deparse_errors = [
    (
        "eq_mixed_sign",
        "SELECT id FROM interval_um.tbl WHERE col_interval = '-1 mons +2 days'::interval",
    ),
    (
        "eq_mixed_sign_years",
        "SELECT id FROM interval_um.tbl WHERE col_interval = '-1 years +2 days -03:04:05'::interval",
    ),
    (
        "eq_wide_hour",
        "SELECT id FROM interval_um.tbl WHERE col_interval = '2562047788:00:54.775807'::interval",
    ),
    (
        "pl_mixed_sign",
        "SELECT id FROM interval_um.tbl WHERE col_interval + '-1 mons +2 days'::interval = INTERVAL '1 day'",
    ),
    (
        "mi_mixed_sign",
        "SELECT id FROM interval_um.tbl WHERE col_interval - '-1 mons +2 days'::interval = INTERVAL '1 day'",
    ),
    (
        "um_mixed_sign",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = '-1 mons +2 days'::interval",
    ),
    (
        "um_wide_hour",
        "SELECT id FROM interval_um.tbl WHERE - col_interval = '2562047788:00:54.775807'::interval",
    ),
]


@pytest.mark.parametrize(
    "test_id, query",
    interval_const_deparse_errors,
    ids=[c[0] for c in interval_const_deparse_errors],
)
def test_interval_const_deparse_errors_on_fdw(
    create_interval_um_table, pg_conn, test_id, query
):
    with pytest.raises(Exception, match="Could not convert string"):
        run_query(query, pg_conn)
    pg_conn.rollback()
    heap_query = query.replace("interval_um.tbl", "interval_um.heap_tbl")
    run_query(heap_query, pg_conn)
    pg_conn.rollback()


def test_interval_const_deparse_ok_without_plus(create_interval_um_table, pg_conn):
    query = (
        "SELECT id FROM interval_um.tbl "
        "WHERE col_interval <> '1 mon -2 days'::interval"
    )
    assert_query_pushdownable(query, pg_conn)
    fdw, heap = _um_tables()
    assert_query_results_on_tables(query, pg_conn, fdw, heap)


# Parquet cannot store a negative interval, so the fixture above only has
# non-negative values. Iceberg can, so negating an already-negative interval
# and negating mixed-sign fields are only reachable through an iceberg table.
interval_um_iceberg_rows = """
    (1, NULL),
    (2, INTERVAL '-1 day'),
    (3, INTERVAL '1 mon -2 days'),
    (4, INTERVAL '-1 year -6 months'),
    (5, INTERVAL '-3 days -04:00:00'),
    (6, INTERVAL '1 mon -2 days -03:04:05')
"""


@pytest.fixture(scope="module")
def create_interval_um_iceberg_table(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/create_interval_um_iceberg_table"
    run_command(
        f"""
        CREATE SCHEMA interval_um_iceberg;
        CREATE TABLE interval_um_iceberg.tbl (id int, col_interval interval)
        USING iceberg WITH (location = '{url}');
        CREATE TABLE interval_um_iceberg.heap_tbl (id int, col_interval interval);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO interval_um_iceberg.tbl VALUES {interval_um_iceberg_rows};
        INSERT INTO interval_um_iceberg.heap_tbl VALUES {interval_um_iceberg_rows};
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA interval_um_iceberg CASCADE", pg_conn)
    pg_conn.commit()


def _um_iceberg_tables():
    return ["interval_um_iceberg.tbl"], ["interval_um_iceberg.heap_tbl"]


interval_um_iceberg_queries = [
    (
        "project_negative_and_mixed_sign",
        "SELECT id, - col_interval FROM interval_um_iceberg.tbl",
        '(- "col_interval")',
    ),
    (
        "double_negation",
        "SELECT id, - (- col_interval) FROM interval_um_iceberg.tbl",
        '(- (- "col_interval"))',
    ),
    (
        "negate_already_negative",
        "SELECT id FROM interval_um_iceberg.tbl WHERE - col_interval = INTERVAL '1 day'",
        '(- "col_interval")',
    ),
    (
        "negate_already_negative_ym",
        "SELECT id FROM interval_um_iceberg.tbl WHERE - col_interval = INTERVAL '1 year 6 months'",
        '(- "col_interval")',
    ),
    (
        "negate_already_negative_mixed_hms",
        "SELECT id FROM interval_um_iceberg.tbl WHERE - col_interval = INTERVAL '3 days 04:00:00'",
        '(- "col_interval")',
    ),
    (
        "negated_column_is_positive",
        "SELECT id FROM interval_um_iceberg.tbl WHERE - col_interval > INTERVAL '0 seconds'",
        '(- "col_interval")',
    ),
]


@pytest.mark.parametrize(
    "test_id, query, expected_expression",
    interval_um_iceberg_queries,
    ids=[c[0] for c in interval_um_iceberg_queries],
)
def test_interval_um_pushdown_matches_heap_on_iceberg(
    create_interval_um_iceberg_table, pg_conn, test_id, query, expected_expression
):
    assert_query_pushdownable(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    fdw, heap = _um_iceberg_tables()
    assert_query_results_on_tables(query, pg_conn, fdw, heap)

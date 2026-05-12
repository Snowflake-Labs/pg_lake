import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

# for some equality tests, easier to have a constant value for comparison
hard_coded_uuid = "61370351-4216-6996-9448-172193487445"

test_cases = [
    (
        "uuid_eq",
        f"WHERE col_uuid = '{hard_coded_uuid}'",
        '("col_uuid" = \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_eq",
        f"WHERE col_text::uuid = '{hard_coded_uuid}'::uuid",
        '(("col_text")::"uuid" = \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_eq",
        f"WHERE col_varchar::uuid = '{hard_coded_uuid}'::uuid",
        '(("col_varchar")::"uuid" = \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_ne",
        f"WHERE col_uuid != '{hard_coded_uuid}'",
        '("col_uuid" <> \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_ne",
        f"WHERE col_text::uuid <> '{hard_coded_uuid}'::uuid",
        '(("col_text")::"uuid" <> \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_ne",
        f"WHERE col_varchar::uuid != '{hard_coded_uuid}'::uuid",
        '(("col_varchar")::"uuid" <> \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_lt",
        f"WHERE col_uuid < '{hard_coded_uuid}'",
        '("col_uuid" < \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_lt",
        f"WHERE col_text::uuid < '{hard_coded_uuid}'::uuid",
        '(("col_text")::"uuid" < \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_lt",
        f"WHERE col_varchar::uuid < '{hard_coded_uuid}'::uuid",
        '(("col_varchar")::"uuid" < \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_le",
        f"WHERE col_uuid <= '{hard_coded_uuid}'",
        '("col_uuid" <= \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_le",
        f"WHERE col_text::uuid <= '{hard_coded_uuid}'::uuid",
        '(("col_text")::"uuid" <= \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_le",
        f"WHERE col_varchar::uuid <= '{hard_coded_uuid}'::uuid",
        '(("col_varchar")::"uuid" <= \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_gt",
        f"WHERE col_uuid > '{hard_coded_uuid}'",
        '("col_uuid" > \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_gt",
        f"WHERE col_text::uuid > '{hard_coded_uuid}'::uuid",
        '(("col_text")::"uuid" > \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_gt",
        f"WHERE col_varchar::uuid > '{hard_coded_uuid}'::uuid",
        '(("col_varchar")::"uuid" > \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_ge",
        f"WHERE col_uuid >= '{hard_coded_uuid}'",
        '("col_uuid" >= \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_ge",
        f"WHERE col_text::uuid >= '{hard_coded_uuid}'::uuid",
        '(("col_text")::"uuid" >= \'' + hard_coded_uuid + '\'::"uuid")',
    ),
    (
        "uuid_ge",
        f"WHERE col_varchar::uuid >= '{hard_coded_uuid}'::uuid",
        '(("col_varchar")::"uuid" >= \'' + hard_coded_uuid + '\'::"uuid")',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_uuid_comparison_operator_pushdown(
    create_test_uuid_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM test_uuid_comparison_operator_pushdown.tbl "
        + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_uuid_comparison_operator_pushdown.tbl"],
        ["test_uuid_comparison_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_uuid_comparison_operator_pushdown_table(pg_conn, s3, extension):

    location = (
        f"s3://{TEST_BUCKET}/create_test_uuid_comparison_operator_pushdown_table/"
    )
    # Create a table with 2 columns on the fdw
    run_command(
        f"""
	            CREATE SCHEMA test_uuid_comparison_operator_pushdown;
	            CREATE TABLE test_uuid_comparison_operator_pushdown.tbl
	            (
                    col_uuid uuid,
                    col_text text,
                    col_varchar varchar
	            ) USING iceberg WITH (location = '{location}');

                -- first, insert the hard coded value, then generate some random
                INSERT INTO test_uuid_comparison_operator_pushdown.tbl VALUES ('{hard_coded_uuid}'::uuid, '{hard_coded_uuid}'::text, '{hard_coded_uuid}'::varchar);

                INSERT INTO test_uuid_comparison_operator_pushdown.tbl SELECT gen_random_uuid(), gen_random_uuid()::text, gen_random_uuid()::varchar FROM generate_series(0,100);
	            """,
        pg_conn,
    )

    pg_conn.commit()

    run_command(
        """
	            CREATE TABLE test_uuid_comparison_operator_pushdown.heap_tbl
				(
					col_uuid uuid,
					col_text text,
					col_varchar varchar
	            );
	            INSERT INTO test_uuid_comparison_operator_pushdown.heap_tbl 
                    SELECT * FROM test_uuid_comparison_operator_pushdown.tbl;
	            """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_uuid_comparison_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

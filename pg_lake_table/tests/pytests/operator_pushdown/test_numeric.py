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
    # numeric operator tests of the form (test ID, where clause, expected remote SQL phrase)
    ("numeric_eq", "WHERE col_unbounded_numeric = 111.11", "= 111.11"),
    ("numeric_ne", "WHERE col_unbounded_numeric <> 2.0", "<> 2.0"),
    ("numeric_lt", "WHERE col_unbounded_numeric < 13.0", "< 13.0"),
    ("numeric_gt", "WHERE col_unbounded_numeric > 4.0", "> 4.0"),
    ("numeric_le", "WHERE col_unbounded_numeric <= 10.0", "<= 10.0"),
    ("numeric_ge", "WHERE col_unbounded_numeric >= 6.0", ">= 6.0"),
    ("numeric_pl", "WHERE col_unbounded_numeric + 1.0 = 8.0", "+ 1.0"),
    ("numeric_mi", "WHERE col_unbounded_numeric - 1.0 = 6.0", "- 1.0"),
    ("numeric_mul", "WHERE col_unbounded_numeric * 2.0 = 14.0", "* 2.0"),
    ("numeric_div", "WHERE col_unbounded_numeric / 2.0 = 3.5", "/ 2.0"),
    ("numeric_mod", "WHERE col_unbounded_numeric % 2 = 1", "%"),
    ("numeric_pow", "WHERE col_unbounded_numeric ^ 2 = 49", "^"),
    # numeric operator tests of the form (test ID, where clause, expected remote SQL phrase)
    ("numeric_eq", "WHERE col_large_numeric::numeric = 111.11", "= 111.11"),
    ("numeric_ne", "WHERE col_large_numeric::numeric <> 2.0", "<> 2.0"),
    ("numeric_lt", "WHERE col_large_numeric::numeric < 13.0", "< 13.0"),
    ("numeric_gt", "WHERE col_large_numeric::numeric > 4.0", "> 4.0"),
    ("numeric_le", "WHERE col_large_numeric::numeric <= 10.0", "<= 10.0"),
    ("numeric_ge", "WHERE col_large_numeric::numeric >= 6.0", ">= 6.0"),
    ("numeric_pl", "WHERE col_large_numeric::numeric + 1.0 = 8.0", "+ 1.0"),
    ("numeric_mi", "WHERE col_large_numeric::numeric - 1.0 = 6.0", "- 1.0"),
    ("numeric_mul", "WHERE col_large_numeric::numeric * 2.0 = 14.0", "* 2.0"),
    ("numeric_div", "WHERE col_large_numeric::numeric / 2.0 = 3.5", "/ 2.0"),
    ("numeric_mod", "WHERE col_large_numeric::numeric % 2 = 1", "%"),
    ("numeric_pow", "WHERE col_large_numeric::numeric ^ 2 = 49", "^"),
    # decimal operator tests of the form (test ID, where clause, expected remote SQL phrase)
    ("decimal_eq", "WHERE col_small_numeric = 111.11", "= 111.11"),
    ("decimal_ne", "WHERE col_small_numeric <> 2.0", "<> 2.0"),
    ("decimal_lt", "WHERE col_small_numeric < 13.0", "< 13.0"),
    ("decimal_gt", "WHERE col_small_numeric > 4.0", "> 4.0"),
    ("decimal_le", "WHERE col_small_numeric <= 10.0", "<= 10.0"),
    ("decimal_ge", "WHERE col_small_numeric >= 6.0", ">= 6.0"),
    ("decimal_pl", "WHERE col_small_numeric + 1.0 = 8.0", "+ 1.0"),
    ("decimal_mi", "WHERE col_small_numeric - 1.0 = 6.0", "- 1.0"),
    ("decimal_mul", "WHERE col_small_numeric * 2.0 = 14.0", "* 2.0"),
    ("decimal_div", "WHERE col_small_numeric / 2.0 = 3.5", "/ 2.0"),
    ("decimal_mod", "WHERE col_small_numeric % 2 = 1", "%"),
    ("decimal_pow", "WHERE col_small_numeric ^ 2 = 49", "^"),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_numeric_operator_pushdown(
    create_numeric_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):

    query = (
        "SELECT * FROM numeric_operator_pushdown.numeric_table " + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["numeric_operator_pushdown.numeric_table"],
        ["numeric_operator_pushdown.heap_numeric_table"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_numeric_operator_pushdown_table(pg_conn, s3, extension):

    numeric_table_url = (
        f"s3://{TEST_BUCKET}/create_numeric_operator_pushdown_table/numeric.parquet"
    )

    run_command(
        f"""
        COPY (SELECT col_unbounded_numeric::numeric AS col_unbounded_numeric,
                     col_large_numeric::decimal(39,2) AS col_large_numeric,
                     col_small_numeric::decimal(5,2) AS col_small_numeric FROM (
             SELECT NULL as col_unbounded_numeric, NULL as col_large_numeric, NULL as col_small_numeric
                UNION ALL
             SELECT 111.11 as col_unbounded_numeric, 111.11 as col_large_numeric, 111.11 as col_small_numeric
                UNION ALL
             SELECT 7 as col_unbounded_numeric, 7 as col_large_numeric, 7 as col_small_numeric) u
        ) TO '{numeric_table_url}' WITH (FORMAT 'parquet');
	""",
        pg_conn,
    )

    # Create foreign and heap tables for the files
    run_command(
        f"""
	   CREATE SCHEMA numeric_operator_pushdown;

	   CREATE FOREIGN TABLE numeric_operator_pushdown.numeric_table  ()
	   SERVER pg_lake OPTIONS (format 'parquet', path '{numeric_table_url}');

	   CREATE TABLE numeric_operator_pushdown.heap_numeric_table (
           LIKE numeric_operator_pushdown.numeric_table
	   ) WITH (load_from = '{numeric_table_url}');
    """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA numeric_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

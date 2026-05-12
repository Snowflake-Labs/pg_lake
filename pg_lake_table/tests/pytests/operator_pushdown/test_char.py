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
    (
        "char_lt",
        "WHERE col_char < 'a'::char",
        "WHERE (\"col_char\" < 'a'::character(1)",
    ),
    (
        "char_le",
        "WHERE col_char <= 'b'::char",
        "WHERE (\"col_char\" <= 'b'::character(1))",
    ),
    (
        "char_gt",
        "WHERE col_char > 'c'::char",
        "WHERE (\"col_char\" > 'c'::character(1))",
    ),
    (
        "char_ge",
        "WHERE col_char >= 'd'::char",
        "WHERE (\"col_char\" >= 'd'::character(1))",
    ),
    (
        "chareq",
        "WHERE col_char = 'e'::char",
        "WHERE (\"col_char\" = 'e'::character(1))",
    ),
    (
        "charne",
        "WHERE col_char <> 'f'::char",
        "WHERE (\"col_char\" <> 'f'::character(1))",
    ),
    ("charlike", "WHERE col_char LIKE '%e%'", "~~"),
    ("charnlike", "WHERE col_char NOT LIKE '%q%'", "!~~"),
    ("chariclike", "WHERE col_char ILIKE '%E%'", "~~*"),
    ("charicnlike", "WHERE col_char NOT ILIKE '%Q%'", "!~~"),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_text_comparison_operator_pushdown(
    create_test_text_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM test_text_comparison_operator_pushdown.tbl "
        + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_text_comparison_operator_pushdown.tbl"],
        ["test_text_comparison_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_text_comparison_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_test_text_comparison_operator_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::text as col_text, NULL::name as col_name, NULL::varchar as col_varchar, NULL::char
						UNION ALL
                    SELECT ''::text as col_text, ''::name as col_int4, ''::varchar as col_varchar, ''::char as col_char
					 	UNION ALL
                    SELECT 'test'::text as col_text, 'test'::name as col_int4, 'test'::varchar as col_varchar, 's'::char
                        UNION ALL
                    SELECT 'moo'::text as col_text, 'moo'::name as col_int4, 'moo'::varchar as col_varchar, 'e'::char
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA test_text_comparison_operator_pushdown;
	            CREATE FOREIGN TABLE test_text_comparison_operator_pushdown.tbl
	            (
	            	col_text text,
	            	col_name name,
	            	col_varchar varchar,
                    col_char char
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
	            CREATE TABLE test_text_comparison_operator_pushdown.heap_tbl
				(
                    col_text text,
                    col_name name,
                    col_varchar varchar,
                    col_char char
	            );
	            COPY test_text_comparison_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_text_comparison_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

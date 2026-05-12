import pytest
import psycopg2
import array
import duckdb
import math
import json
from decimal import *
from utils_pytest import *

test_cases = [
    # Int array operators
    (
        "int_array_eq",
        "WHERE col_int_array = array[13,1]",
        'WHERE ("col_int_array" = ARRAY[13, 1])',
    ),
    (
        "int_array_in",
        "WHERE col_int_array IN (array[4,5], array[13,1])",
        'WHERE (("col_int_array" = ARRAY[4, 5]) OR ("col_int_array" = ARRAY[13, 1]))',
    ),
    (
        "int_array_lt",
        "WHERE col_int_array < array[4,5]",
        'WHERE ("col_int_array" < ARRAY[4, 5])',
    ),
    (
        "int_array_le",
        "WHERE col_int_array <= array[4,5]",
        'WHERE ("col_int_array" <= ARRAY[4, 5])',
    ),
    (
        "int_array_gt",
        "WHERE col_int_array > array[4,5]",
        'WHERE ("col_int_array" > ARRAY[4, 5])',
    ),
    (
        "int_array_ge",
        "WHERE col_int_array >= array[]::int[]",
        'WHERE ("col_int_array" >= ARRAY[]::integer[])',
    ),
    (
        "int_array_overlaps",
        "WHERE col_int_array && array[4,5]",
        'WHERE ("col_int_array" && ARRAY[4, 5])',
    ),
    (
        "int_array_concat",
        "WHERE col_int_array || array[4] = array[13,1,4]",
        'WHERE (("col_int_array" || ARRAY[4]) = ARRAY[13, 1, 4])',
    ),
    (
        "int_array_append",
        "WHERE col_int_array || 4 = array[4]",
        'WHERE ("array_append"("col_int_array", 4) = ARRAY[4])',
    ),
    # Text array operators
    (
        "text_array_eq",
        "WHERE col_text_array = array['hello', 'world']",
        'WHERE ("col_text_array" = ARRAY[\'hello\'::"text", \'world\'::"text"])',
    ),
    (
        "text_array_in",
        "WHERE col_text_array IN (array[]::text[], array['hello', 'world'])",
        'WHERE (("col_text_array" = ARRAY[]::"text"[]) OR ("col_text_array" = ARRAY[\'hello\'::"text", \'world\'::"text"]))',
    ),
    (
        "text_array_lt",
        "WHERE col_text_array < array['bye']",
        'WHERE ("col_text_array" < ARRAY[\'bye\'::"text"])',
    ),
    (
        "text_array_le",
        "WHERE col_text_array <= array['bye']",
        'WHERE ("col_text_array" <= ARRAY[\'bye\'::"text"])',
    ),
    (
        "text_array_gt",
        "WHERE col_text_array > array['hello']",
        'WHERE ("col_text_array" > ARRAY[\'hello\'::"text"])',
    ),
    (
        "text_array_ge",
        "WHERE col_text_array >= array[]::text[]",
        'WHERE ("col_text_array" >= ARRAY[]::"text"[])',
    ),
    (
        "text_array_overlaps",
        "WHERE col_text_array && array['earth','hello']",
        'WHERE ("col_text_array" && ARRAY[\'earth\'::"text", \'hello\'::"text"])',
    ),
    (
        "text_array_concat",
        "WHERE col_text_array || array['earth'] = array['hello', 'world', 'earth']",
        'WHERE (("col_text_array" || ARRAY[\'earth\'::"text"]) = ARRAY[\'hello\'::"text", \'world\'::"text", \'earth\'::"text"])',
    ),
    (
        "text_array_append",
        "WHERE col_text_array || ''::text = array['hello', 'world', '']",
        'WHERE ("array_append"("col_text_array", \'\'::"text") = ARRAY[\'hello\'::"text", \'world\'::"text", \'\'::"text"])',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_cases[0] for test_cases in test_cases],
)
def test_array_operator_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM array_operator_pushdown.tbl " + operator_expression
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_operator_pushdown.tbl"],
        ["array_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA array_operator_pushdown;

	            """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT  NULL::int[] as int_array, NULL::text[] as text_array
						UNION ALL
					SELECT  array[3,4] as int_array, array['hello', 'world'] as text_array
						UNION ALL
					SELECT  array[13,1] as int_array, array['', 'zoo'] as text_array
						UNION ALL
					SELECT  array[]::int[] as int_array, array[]::text[] as text_array

				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE FOREIGN TABLE array_operator_pushdown.tbl
	            (
					col_int_array int[],
					col_text_array text[]
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
	            CREATE TABLE array_operator_pushdown.heap_tbl
				(
					col_int_array int[],
					col_text_array text[]
	            );
	            COPY array_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA array_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

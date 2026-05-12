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

test_agg_cases = [
    # count works on "any" column type, so test with as many types as possible
    ("count(*)", "count(*)", '"count"(*)'),
    ("count(int2)", "count(col_int2)", '"count"("col_int2")'),
    ("count(int4)", "count(col_int4)", '"count"("col_int4")'),
    ("count(int8)", "count(col_int8)", '"count"("col_int8")'),
    ("count(float)", "count(col_float)", '"count"("col_float")'),
    ("count(double precision)", "count(col_double)", '"count"("col_double")'),
    ("count(text)", "count(col_text)", '"count"("col_text")'),
    ("count(varchar)", "count(col_varchar)", '"count"("col_varchar")'),
    (
        "count(DISTINCT int2)",
        "count(DISTINCT col_int2)",
        '"count"(DISTINCT "col_int2")',
    ),
    (
        "count(DISTINCT int4)",
        "count(DISTINCT col_int4)",
        '"count"(DISTINCT "col_int4")',
    ),
    (
        "count(DISTINCT int8)",
        "count(DISTINCT col_int8)",
        '"count"(DISTINCT "col_int8")',
    ),
    (
        "count(DISTINCT float)",
        "count(DISTINCT col_float)",
        '"count"(DISTINCT "col_float")',
    ),
    (
        "count(DISTINCT double precision)",
        "count(DISTINCT col_double)",
        '"count"(DISTINCT "col_double")',
    ),
    (
        "count(DISTINCT text)",
        "count(DISTINCT col_text)",
        '"count"(DISTINCT "col_text")',
    ),
    (
        "count(DISTINCT col)",
        "count(DISTINCT col_varchar)",
        '"count"(DISTINCT "col_varchar")',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, agg_expression, expected_expression",
    test_agg_cases,
    ids=[test_agg_cases[0] for test_agg_cases in test_agg_cases],
)
def test_aggregate_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    agg_expression,
    expected_expression,
):
    query = "SELECT " + agg_expression + " FROM count_aggregate_pushdown.tbl "
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["count_aggregate_pushdown.tbl"],
        ["count_aggregate_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::smallint as c1, NULL::int as c2 , NULL::bigint as c3, NULL::float as c4, NULL::double precision as c5, NULL::text as c6, NULL::varchar as c7, NULL::interval as c8, NULL::real as c9, NULL::real as c10
						UNION ALL
					SELECT 1, 1, 1, 1.1, 1.1, '', '', '1 day', 1.1, 1.1
					 	UNION ALL
					SELECT -1, -1, -100, -1.1, -1.1, 'test', 'test', '2 days', 2.2, 2.2

				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA count_aggregate_pushdown;
	            CREATE FOREIGN TABLE count_aggregate_pushdown.tbl
	            (
	            	col_int2 smallint,
	            	col_int4 int,
	            	col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_text text,
                    col_varchar varchar,
                    col_interval interval,
                    col_real real,
                    col_numeric numeric
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
	            CREATE TABLE count_aggregate_pushdown.heap_tbl
				(
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_text text,
                    col_varchar varchar,
                    col_interval interval,
                    col_real real,
                    col_numeric numeric
	            );
	            COPY count_aggregate_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA count_aggregate_pushdown CASCADE", pg_conn)
    pg_conn.commit()

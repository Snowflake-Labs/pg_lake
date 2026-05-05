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

# query that this test is based on
# SELECT proname, array_to_string(ARRAY(SELECT unnest(proargtypes)::regtype), ', ') AS argtypes FROM pg_proc WHERE proname = 'avg';
# proname |     argtypes
# ---------+------------------
# avg     | bigint
# avg     | smallint
# avg     | integer
# avg     | real
# avg     | double precision
# avg     | interval
# avg     | numeric
# (7 rows)
# But pgduck_server does NOT support avg(interval)
# ```

test_agg_cases = [
    ("avg(col_int2)", "avg(col_int2)", '"avg"("col_int2")'),
    ("avg(col_int4)", "avg(col_int4)", '"avg"("col_int4")'),
    ("avg(col_int8)", "avg(col_int8)", '"avg"("col_int8")'),
    ("avg(col_float)", "avg(col_float)", '"avg"("col_float")'),
    ("avg(double precision)", "avg(col_double)", '"avg"("col_double")'),
    ("avg(numeric)", "avg(col_numeric)", '"avg"("col_numeric")'),
    ("avg(numeric_3_1)", "avg(col_numeric_1)", '"avg"("col_numeric_1")'),
    ("avg(real)", "avg(col_real)", '"avg"("col_real")'),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, agg_expression, expected_expression",
    test_agg_cases,
    ids=[ids_list[0] for ids_list in test_agg_cases],
)
def test_aggregate_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    agg_expression,
    expected_expression,
):
    query = "SELECT " + agg_expression + " FROM avg_aggregate_pushdown.tbl "
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["avg_aggregate_pushdown.tbl"],
        ["avg_aggregate_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::smallint as c1, NULL::int as c2 , NULL::bigint as c3, NULL::float as c4, NULL::double precision as c5, NULL::interval as c8, NULL::real as c9, NULL::real as c10,  NULL::real as c11
						UNION ALL
					SELECT 1, 1, 1, 1.1, 1.1, '1 day', 1.1, 1.1, 1.1
					 	UNION ALL
					SELECT -1, -1, -100, -1.1, -1.1, '2 days', 2.2, 2.2, 2.2
						UNION ALL
					SELECT 1561, 223123, -100123, -111.111111, -12222.1222222, '962 days', 21231.2123123, 4534652.2456456, 4.2

				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA avg_aggregate_pushdown;
	            CREATE FOREIGN TABLE avg_aggregate_pushdown.tbl
	            (
	            	col_int2 smallint,
	            	col_int4 int,
	            	col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_interval interval,
                    col_real real,
                    col_numeric numeric,
					col_numeric_1 NUMERIC(3, 1)
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
	            CREATE TABLE avg_aggregate_pushdown.heap_tbl
				(
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_interval interval,
                    col_real real,
                    col_numeric numeric,
                    col_numeric_1 NUMERIC(3, 1)
	            );
	            COPY avg_aggregate_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA avg_aggregate_pushdown CASCADE", pg_conn)
    pg_conn.commit()

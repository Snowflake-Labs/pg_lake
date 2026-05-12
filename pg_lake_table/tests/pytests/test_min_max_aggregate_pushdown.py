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
# SELECT proname, array_to_string(ARRAY(SELECT unnest(proargtypes)::regtype), ', ') AS argtypes FROM pg_proc WHERE proname = 'min';
# proname |          argtypes
# ---------+-----------------------------
# min     | bigint
# min     | smallint
# min     | integer
# min     | text
# min     | oid
# min     | tid
# min     | real
# min     | double precision
# min     | money
# min     | inet
# min     | character
# min     | date
# min     | time without time zone
# min     | timestamp without time zone
# min     | timestamp with time zone
# min     | interval
# min     | time with time zone
# min     | numeric
# min     | anyarray
# min     | pg_lsn
# min     | anyenum
# min     | xid8
# (22 rows)
# ```

test_agg_cases = [
    # duckdb does not support data types:
    # 	oid, tid, money, pg_lsn, xid8
    # duckdb does not support inet and enums as base type
    # so currently not supported for pushdown
    ("min(col_int2)", "min(col_int2)", '"min"("col_int2")'),
    ("min(col_int4)", "min(col_int4)", '"min"("col_int4")'),
    ("min(col_int8)", "min(col_int8)", '"min"("col_int8")'),
    ("min(col_float)", "min(col_float)", '"min"("col_float")'),
    ("min(double precision)", "min(col_double)", '"min"("col_double")'),
    ("min(numeric)", "min(col_numeric)", '"min"("col_numeric")'),
    ("min(numeric_3_1)", "min(col_numeric_1)", '"min"("col_numeric_1")'),
    ("min(real)", "min(col_real)", '"min"("col_real")'),
    ("min(text)", "min(col_text)", '"min"("col_text")'),
    ("min(timetz)", "min(col_timetz)", '"min"("col_timetz")'),
    ("min(date)", "min(col_date)", '"min"("col_date")'),
    ("min(timestamp)", "min(col_timestamp)", '"min"("col_timestamp")'),
    ("min(timestamptz)", "min(col_timestamptz)", '"min"("col_timestamptz")'),
    ("min(time)", "min(col_time)", '"min"("col_time")'),
    ("min(int_array)", "min(col_int_array)", '"min"("col_int_array")'),
    ("min(character)", "min(col_char)", '"min"("col_char")'),
    ("max(col_int2)", "max(col_int2)", '"max"("col_int2")'),
    ("max(col_int4)", "max(col_int4)", '"max"("col_int4")'),
    ("max(col_int8)", "max(col_int8)", '"max"("col_int8")'),
    ("max(col_float)", "max(col_float)", '"max"("col_float")'),
    ("max(double precision)", "max(col_double)", '"max"("col_double")'),
    ("max(numeric)", "max(col_numeric)", '"max"("col_numeric")'),
    ("max(numeric_3_1)", "max(col_numeric_1)", '"max"("col_numeric_1")'),
    ("max(real)", "max(col_real)", '"max"("col_real")'),
    ("max(text)", "max(col_text)", '"max"("col_text")'),
    ("max(timetz)", "max(col_timetz)", '"max"("col_timetz")'),
    ("max(date)", "max(col_date)", '"max"("col_date")'),
    ("max(timestamp)", "max(col_timestamp)", '"max"("col_timestamp")'),
    ("max(timestamptz)", "max(col_timestamptz)", '"max"("col_timestamptz")'),
    ("max(time)", "max(col_time)", '"max"("col_time")'),
    ("max(int_array)", "max(col_int_array)", '"max"("col_int_array")'),
    ("max(character)", "max(col_char)", '"max"("col_char")'),
    # There is only one type of any_value(anyelement), just just try a few types
    ("any_value(int2)", "any_value(col_int2)", '"any_value"("col_int2")'),
    (
        "any_value(int_array)",
        "any_value(col_int_array)",
        '"any_value"("col_int_array")',
    ),
    ("any_value(character)", "any_value(col_char)", '"any_value"("col_char")'),
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
    query = "SELECT " + agg_expression + " FROM min_aggregate_pushdown.tbl "
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["min_aggregate_pushdown.tbl"],
        ["min_aggregate_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA min_aggregate_pushdown;
	            CREATE TYPE min_aggregate_pushdown.mood AS ENUM ('sad', 'ok', 'happy');

	            """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::smallint as c1, NULL::int as c2 , NULL::bigint as c3, NULL::float as c4, NULL::double precision as c5, NULL::interval as c8, NULL::real as c9, NULL::real as c10,  NULL::real as c11, NULL::text as c12, NULL::varchar(100) as c13, NULL::char as c14, NULL::date AS col_date,
       NULL::timestamp AS col_timestamp,  NULL::timestamptz AS col_timestamptz,  NULL::timetz AS col_timetz,  NULL::time AS col_time, NULL::min_aggregate_pushdown.mood, NULL::int[3] as int_arr
						UNION ALL
					SELECT 1, 1, 1, 1.1, 1.1, '1 day', 1.1, 1.1, 1.1, '','','p', '2024-01-01', '2024-01-01 12:00:00', '2024-01-01 12:00:00+00', '12:00:00+00', '12:00:00', 'sad', ARRAY[0,0,0]
					 	UNION ALL
					SELECT -1, -1, -100, -1.1, -1.1, '2 days', 2.2, 2.2, 2.2, 'text', 'varchar', 'c', '2023-01-01', '2023-01-01 13:00:00', '2023-01-01 13:00:00+00',  '13:00:00+01', '13:00:00', 'happy', ARRAY[0,0,0]
						UNION ALL
					SELECT 1561, 223123, -100123, -111.111111, -12222.1222222, '962 days', 21231.2123123, 4534652.2456456, 4.2, 'text2', 'varchar2', 's', '2022-12-31', '2022-12-31 23:59:59', '2022-12-31 23:59:59+00',  '23:59:59+02', '23:59:59', 'ok', ARRAY[0,0,0]

				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE FOREIGN TABLE min_aggregate_pushdown.tbl
	            (
	            	col_int2 smallint,
	            	col_int4 int,
	            	col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_interval interval,
                    col_real real,
                    col_numeric numeric,
					col_numeric_1 NUMERIC(3, 1),
					col_text text,
					col_varchar varchar,
					col_char char,
					col_date date,
					col_timestamp timestamp,
					col_timestamptz timestamptz,
					col_timetz time with time zone,
					col_time time,
					col_enum min_aggregate_pushdown.mood,
					col_int_array int[3]
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
	            CREATE TABLE min_aggregate_pushdown.heap_tbl
				(
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_interval interval,
                    col_real real,
                    col_numeric numeric,
                    col_numeric_1 NUMERIC(3, 1),
					col_text text,
					col_varchar varchar,
					col_char char,
					col_date date,
					col_timestamp timestamp,
					col_timestamptz timestamptz,
					col_timetz time with time zone,
					col_time time,
					col_enum min_aggregate_pushdown.mood,
					col_int_array int[3]
	            );
	            COPY min_aggregate_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA min_aggregate_pushdown CASCADE", pg_conn)
    pg_conn.commit()

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

# query that this test is based on for all the functions stddev, stddev_pop, stddev_samp, variance, var_pop, var_samp
# SELECT proname, array_to_string(ARRAY(SELECT unnest(proargtypes)::regtype), ', ') AS argtypes FROM pg_proc WHERE proname = 'stddev';
# proname |     argtypes
# ---------+------------------
# stddev  | bigint
# stddev  | smallint
# stddev  | integer
# stddev  | real
# stddev  | double precision
# stddev  | numeric
# (6 rows)
#
# also, the followings for corr, covar_pop, covar_samp, regr_avgx, regr_avgy, regr_count, regr_intercept, regr_r2, regr_slope, regr_sxx, regr_sxy, regr_syy
# SELECT proname, array_to_string(ARRAY(SELECT unnest(proargtypes)::regtype), ', ') AS argtypes FROM pg_proc WHERE proname = 'corr';
# proname |              argtypes
# ---------+------------------------------------
# corr    | double precision, double precision
# (1 row)

#


test_agg_cases = [
    ("stddev(col_int2)", "stddev(col_int2)", '"stddev"("col_int2")', True),
    ("stddev(col_int4)", "stddev(col_int4)", '"stddev"("col_int4")', True),
    ("stddev(col_int8)", "stddev(col_int8)", '"stddev"("col_int8")', True),
    ("stddev(col_float)", "stddev(col_float)", '"stddev"("col_float")', True),
    ("stddev(double precision)", "stddev(col_double)", '"stddev"("col_double")', True),
    ("stddev(numeric)", "stddev(col_numeric)", '"stddev"("col_numeric")', True),
    ("stddev(numeric_3_1)", "stddev(col_numeric_1)", '"stddev"("col_numeric_1")', True),
    ("stddev(real)", "stddev(col_real)", '"stddev"("col_real")', True),
    ("stddev_pop(col_int2)", "stddev_pop(col_int2)", '"stddev_pop"("col_int2")', True),
    ("stddev_pop(col_int4)", "stddev_pop(col_int4)", '"stddev_pop"("col_int4")', True),
    ("stddev_pop(col_int8)", "stddev_pop(col_int8)", '"stddev_pop"("col_int8")', True),
    (
        "stddev_pop(col_float)",
        "stddev_pop(col_float)",
        '"stddev_pop"("col_float")',
        True,
    ),
    (
        "stddev_pop(double precision)",
        "stddev_pop(col_double)",
        '"stddev_pop"("col_double")',
        True,
    ),
    (
        "stddev_pop(numeric)",
        "stddev_pop(col_numeric)",
        '"stddev_pop"("col_numeric")',
        True,
    ),
    (
        "stddev_pop(numeric_3_1)",
        "stddev_pop(col_numeric_1)",
        '"stddev_pop"("col_numeric_1")',
        True,
    ),
    ("stddev_pop(real)", "stddev_pop(col_real)", '"stddev_pop"("col_real")', True),
    (
        "stddev_samp(col_int2)",
        "stddev_samp(col_int2)",
        '"stddev_samp"("col_int2")',
        True,
    ),
    (
        "stddev_samp(col_int4)",
        "stddev_samp(col_int4)",
        '"stddev_samp"("col_int4")',
        True,
    ),
    (
        "stddev_samp(col_int8)",
        "stddev_samp(col_int8)",
        '"stddev_samp"("col_int8")',
        True,
    ),
    (
        "stddev_samp(col_float)",
        "stddev_samp(col_float)",
        '"stddev_samp"("col_float")',
        True,
    ),
    (
        "stddev_samp(double precision)",
        "stddev_samp(col_double)",
        '"stddev_samp"("col_double")',
        True,
    ),
    (
        "stddev_samp(numeric)",
        "stddev_samp(col_numeric)",
        '"stddev_samp"("col_numeric")',
        True,
    ),
    (
        "stddev_samp(numeric_3_1)",
        "stddev_samp(col_numeric_1)",
        '"stddev_samp"("col_numeric_1")',
        True,
    ),
    ("stddev_samp(real)", "stddev_samp(col_real)", '"stddev_samp"("col_real")', True),
    ("variance(col_int2)", "variance(col_int2)", '"variance"("col_int2")', True),
    ("variance(col_int4)", "variance(col_int4)", '"variance"("col_int4")', True),
    ("variance(col_int8)", "variance(col_int8)", '"variance"("col_int8")', True),
    ("variance(col_float)", "variance(col_float)", '"variance"("col_float")', True),
    (
        "variance(double precision)",
        "variance(col_double)",
        '"variance"("col_double")',
        True,
    ),
    ("variance(numeric)", "variance(col_numeric)", '"variance"("col_numeric")', True),
    (
        "variance(numeric_3_1)",
        "variance(col_numeric_1)",
        '"variance"("col_numeric_1")',
        True,
    ),
    ("variance(real)", "variance(col_real)", '"variance"("col_real")', True),
    ("var_pop(col_int2)", "var_pop(col_int2)", '"var_pop"("col_int2")', True),
    ("var_pop(col_int4)", "var_pop(col_int4)", '"var_pop"("col_int4")', True),
    ("var_pop(col_int8)", "var_pop(col_int8)", '"var_pop"("col_int8")', True),
    ("var_pop(col_float)", "var_pop(col_float)", '"var_pop"("col_float")', True),
    (
        "var_pop(double precision)",
        "var_pop(col_double)",
        '"var_pop"("col_double")',
        True,
    ),
    ("var_pop(numeric)", "var_pop(col_numeric)", '"var_pop"("col_numeric")', True),
    (
        "var_pop(numeric_3_1)",
        "var_pop(col_numeric_1)",
        '"var_pop"("col_numeric_1")',
        True,
    ),
    ("var_pop(real)", "var_pop(col_real)", '"var_pop"("col_real")', True),
    ("var_samp(col_int2)", "var_samp(col_int2)", '"var_samp"("col_int2")', True),
    ("var_samp(col_int4)", "var_samp(col_int4)", '"var_samp"("col_int4")', True),
    ("var_samp(col_int8)", "var_samp(col_int8)", '"var_samp"("col_int8")', True),
    ("var_samp(col_float)", "var_samp(col_float)", '"var_samp"("col_float")', True),
    (
        "var_samp(double precision)",
        "var_samp(col_double)",
        '"var_samp"("col_double")',
        True,
    ),
    ("var_samp(numeric)", "var_samp(col_numeric)", '"var_samp"("col_numeric")', True),
    (
        "var_samp(numeric_3_1)",
        "var_samp(col_numeric_1)",
        '"var_samp"("col_numeric_1")',
        True,
    ),
    ("var_samp(real)", "var_samp(col_real)", '"var_samp"("col_real")', True),
    (
        "corr(double precision)",
        "corr(col_double, col_double)",
        '"corr"("col_double", "col_double")',
        True,
    ),
    (
        "covar_pop(double precision)",
        "covar_pop(col_double, col_double)",
        '"covar_pop"("col_double", "col_double")',
        True,
    ),
    (
        "covar_samp(double precision)",
        "covar_samp(col_double, col_double)",
        '"covar_samp"("col_double", "col_double")',
        True,
    ),
    (
        "regr_avgx(double precision)",
        "regr_avgx(col_double, col_double)",
        '"regr_avgx"("col_double", "col_double")',
        True,
    ),
    (
        "regr_avgy(double precision)",
        "regr_avgy(col_double, col_double)",
        '"regr_avgy"("col_double", "col_double")',
        True,
    ),
    (
        "regr_count(double precision)",
        "regr_count(col_double, col_double)",
        '"regr_count"("col_double", "col_double")',
        True,
    ),
    (
        "regr_intercept(double precision)",
        "regr_intercept(col_double, col_double)",
        '"regr_intercept"("col_double", "col_double")',
        True,
    ),
    (
        "regr_r2(double precision)",
        "regr_r2(col_double, col_double)",
        '"regr_r2"("col_double", "col_double")',
        True,
    ),
    (
        "regr_slope(double precision)",
        "regr_slope(col_double, col_double)",
        '"regr_slope"("col_double", "col_double")',
        True,
    ),
    (
        "regr_sxx(double precision)",
        "regr_sxx(col_double, col_double)",
        '"regr_sxx"("col_double", "col_double")',
        True,
    ),
    ("mode() int2", "mode() within group (order by col_int2)", '"mode"()', True),
    ("mode() int4", "mode() within group (order by col_int4)", '"mode"()', True),
    ("mode() int8", "mode() within group (order by col_int8)", '"mode"()', True),
    ("mode() float", "mode() within group (order by col_float)", '"mode"()', True),
    ("mode() double", "mode() within group (order by col_double)", '"mode"()', True),
    (
        "mode() interval",
        "mode() within group (order by col_interval)",
        '"mode"()',
        True,
    ),
    ("mode() real", "mode() within group (order by col_real)", '"mode"()', True),
    ("mode() numeric", "mode() within group (order by col_numeric)", '"mode"()', True),
    (
        "mode() numeric(3,1)",
        "mode() within group (order by col_numeric_1)",
        '"mode"()',
        True,
    ),
    (
        "percentile_cont(double precision) int2",
        "percentile_cont(0.5) within group (order by col_int2)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) int4",
        "percentile_cont(0.5) within group (order by col_int4)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) int8",
        "percentile_cont(0.5) within group (order by col_int8)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) float",
        "percentile_cont(0.5) within group (order by col_float)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) double",
        "percentile_cont(0.5) within group (order by col_double)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) interval",
        "percentile_cont(0.5) within group (order by col_interval)",
        '"percentile_cont"(',
        False,
    ),
    (
        "percentile_cont(double precision) real",
        "percentile_cont(0.5) within group (order by col_real)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) numeric",
        "percentile_cont(0.5) within group (order by col_numeric)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) numeric(3,1)",
        "percentile_cont(0.5) within group (order by col_numeric_1)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision) expression",
        "percentile_cont(0.5) within group (order by col_int2 * col_numeric)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) int2",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_int2)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) int4",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_int4)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) int8",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_int8)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) float",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_float)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) double",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_double)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) interval",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_interval)",
        '"percentile_cont"(',
        False,
    ),
    (
        "percentile_cont(double precision[]) real",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_real)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) numeric",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_numeric)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_cont(double precision[]) numeric(3,1)",
        "percentile_cont(array[0.5,0.9,0.99]) within group (order by col_numeric_1)",
        '"percentile_cont"(',
        True,
    ),
    (
        "percentile_disc(double precision) int2",
        "percentile_disc(0.5) within group (order by col_int2)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) int4",
        "percentile_disc(0.5) within group (order by col_int4)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) int8",
        "percentile_disc(0.5) within group (order by col_int8)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) float",
        "percentile_disc(0.5) within group (order by col_float)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) double",
        "percentile_disc(0.5) within group (order by col_double)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) interval",
        "percentile_disc(0.5) within group (order by col_interval)",
        '"percentile_disc"(',
        False,
    ),
    (
        "percentile_disc(double precision) real",
        "percentile_disc(0.5) within group (order by col_real)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) numeric",
        "percentile_disc(0.5) within group (order by col_numeric)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) numeric(3,1)",
        "percentile_disc(0.5) within group (order by col_numeric_1)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision) expression",
        "percentile_disc(0.5) within group (order by col_int2 + col_int4)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) int2",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_int2)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) int4",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_int4)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) int8",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_int8)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) float",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_float)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) double",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_double)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) interval",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_interval)",
        '"percentile_disc"(',
        False,
    ),
    (
        "percentile_disc(double precision[]) real",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_real)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) numeric",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_numeric)",
        '"percentile_disc"(',
        True,
    ),
    (
        "percentile_disc(double precision[]) numeric(3,1)",
        "percentile_disc(array[0.5,0.9,0.99]) within group (order by col_numeric_1)",
        '"percentile_disc"(',
        True,
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, agg_expression, expected_expression, expect_pushdown",
    test_agg_cases,
    ids=[ids_list[0] for ids_list in test_agg_cases],
)
def test_aggregate_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    agg_expression,
    expected_expression,
    expect_pushdown,
):
    query = "SELECT " + agg_expression + " FROM stddev_aggregate_pushdowntbl "

    if expect_pushdown:
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    tolerance = 50 if "var" in agg_expression and "real" in agg_expression else 0.15
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["stddev_aggregate_pushdowntbl"],
        ["stddev_aggregate_pushdownheap_tbl"],
        tolerance=tolerance,
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_operator_pushdown_table/data.parquet"
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
	            CREATE SCHEMA sum_aggregate_pushdown;
	            CREATE FOREIGN TABLE stddev_aggregate_pushdowntbl
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
	            CREATE TABLE stddev_aggregate_pushdownheap_tbl
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
	            COPY stddev_aggregate_pushdownheap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA sum_aggregate_pushdown CASCADE", pg_conn)
    pg_conn.commit()

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
    ("abs(col_int2)", "WHERE abs(col_int2) > 0", "abs(col_int2)", True),
    ("abs(col_int4)", "WHERE abs(col_int4) > 0", "abs(col_int4)", True),
    ("abs(col_int8)", "WHERE abs(col_int8) > 0", "abs(col_int8)", True),
    ("abs(col_float)", "WHERE abs(col_float) > 0", "abs(col_float)", True),
    ("abs(double precision)", "WHERE abs(col_double) > 0", "abs(col_double)", True),
    ("abs(numeric)", "WHERE abs(col_numeric) > 0", "abs(col_numeric)", True),
    ("abs(numeric_3_1)", "WHERE abs(col_numeric_1) > 0", "abs(col_numeric_1)", True),
    ("abs(real)", "WHERE abs(col_real) > 0", "abs(col_real)", True),
    ("cbrt", "WHERE cbrt(col_double) > 0", "cbrt(", True),
    ("ceil(double)", "WHERE ceil(col_double) > 1", "ceil(", True),
    ("ceil(numeric)", "WHERE ceil(col_numeric) > 1", "ceil(", True),
    ("ceiling(double)", "WHERE ceiling(col_double) > 1", "ceiling(", True),
    ("ceiling(numeric)", "WHERE ceiling(col_numeric) > 1", "ceiling(", True),
    ("degrees", "WHERE degrees(col_double) > 1", "degrees(", True),
    ("div", "WHERE div(col_numeric,2) = 0", "fdiv(", True),
    # ("exp", "WHERE exp(col_double) > 1", "exp(", True),
    # ("exp", "WHERE exp(col_numeric) > 1", "exp(", True),
    ("floor(double)", "WHERE floor(col_double) > 0", "floor(", True),
    ("floor(numeric)", "WHERE floor(col_numeric) > 0", "floor(", True),
    ("log(double)", "WHERE col_double > 0 and log(col_double) > 0", "log(", True),
    ("log(numeric)", "WHERE col_numeric > 0 and log(col_numeric) > 0", "log(", True),
    ("log10(double)", "WHERE col_double > 0 and log10(col_double) > 0", "log10(", True),
    (
        "log10(numeric)",
        "WHERE col_numeric > 0 and log10(col_numeric) > 0",
        "log10(",
        True,
    ),
    ("ln(double)", "WHERE col_double > 0 and ln(col_double) > 0", "ln(", True),
    ("ln(numeric)", "WHERE col_numeric > 0 and ln(col_numeric) > 0", "ln(", True),
    ("mod", "WHERE mod(col_numeric, 3) > 0", "fmod(", True),
    ("pi", "WHERE col_numeric * pi() > 0", "pi(", True),
    ("power(double)", "WHERE power(col_double, 2) > 0", "power(", True),
    ("power(numeric)", "WHERE power(col_numeric, 2) > 0", "power(", True),
    ("radians", "WHERE radians(col_double) > 0", "radians(", True),
    ("round(col_int2)", "WHERE round(col_int2) > 0", "round(", True),
    ("round(col_int4)", "WHERE round(col_int4) > 0", "round(", True),
    ("round(col_int8)", "WHERE round(col_int8) > 0", "round(", True),
    ("round(col_float)", "WHERE round(col_float) > 0", "round(", True),
    ("round(double precision)", "WHERE round(col_double) > 0", "round(", True),
    ("round(numeric)", "WHERE round(col_numeric) > 0", "round(", True),
    ("round(numeric_3_1)", "WHERE round(col_numeric_1) > 0", "round(", True),
    ("round(real)", "WHERE round(col_real) > 0", "round(", True),
    ("round(col_int2)", "WHERE round(col_int2, 5) > 0", "round(", True),
    ("round(col_int4)", "WHERE round(col_int4, -5) > 0", "round(", True),
    ("round(col_int8)", "WHERE round(col_int8, 5) > 0", "round(", True),
    ("round(numeric)", "WHERE round(col_numeric, 5) > 0", "round(", True),
    ("round(numeric_3_1)", "WHERE round(col_numeric_1, -1) <= 0", "round(", True),
    ("round(real)", "WHERE round(col_real::numeric, 10) > 0", "round(", True),
    ("round(col_float)", "WHERE round(col_float::numeric, 5) > 0", "round(", True),
    (
        "round(double precision)",
        "WHERE round(col_double::numeric, 5) > 0",
        "round(",
        True,
    ),
    ("sqrt(double)", "WHERE col_double > 0 and sqrt(col_double) > 0", "sqrt(", True),
    ("sqrt(numeric)", "WHERE col_double > 0 and sqrt(col_numeric) > 0", "sqrt(", True),
    ("trunc(double)", "WHERE trunc(col_double) = 1.0", "trunc(", True),
    ("trunc(numeric)", "WHERE trunc(col_numeric) = 1.0", "trunc(", True),
    # Trigonometry operators, input must be in the range [-1,1]
    (
        "acos",
        "WHERE abs(col_double)<1 and abs(acos(col_double) - 0.988432) < 0.001",
        "acos(",
        True,
    ),
    (
        "acosd",
        "WHERE abs(col_double)<1 and abs(acosd(col_double) - 56.63298) < 0.001",
        "degrees(acos(",
        True,
    ),
    (
        "asin",
        "WHERE abs(col_double)<1 and abs(asin(col_double) - 0.582364) < 0.001",
        "asin(",
        True,
    ),
    (
        "asind",
        "WHERE abs(col_double)<1 and abs(asind(col_double) - 33.36701) < 0.001",
        "degrees(asin(",
        True,
    ),
    (
        "atan",
        "WHERE abs(atan(col_double) - 0.5028432) < 0.001",
        "atan(",
        True,
    ),
    (
        "atand",
        "WHERE abs(atand(col_double) - 28.81079) < 0.001",
        "degrees(atan(",
        True,
    ),
    (
        "atan2",
        "WHERE abs(atan2(col_double, 1) - 0.5028432) < 0.001",
        "atan2(",
        True,
    ),
    (
        "atan2d",
        "WHERE abs(atan2d(col_double, 1) - 28.810793) < 0.001",
        "degrees(atan2(",
        True,
    ),
    ("cos", "WHERE abs(cos(col_double) - 0.8525245) < 0.001", "cos(", True),
    (
        "cosd",
        "WHERE abs(cosd(col_double) - 0.999953) < 0.001",
        "cos(radians(",
        True,
    ),
    ("sin", "WHERE abs(sin(col_double) - 0.522687) < 0.001", "sin(", True),
    (
        "sind",
        "WHERE abs(sind(col_double) - 0.00959) < 0.001",
        "sin(radians(",
        True,
    ),
    ("tan", "WHERE abs(tan(col_double) - 0.61310) < 0.001", "tan(", True),
    (
        "tand",
        "WHERE abs(tand(col_double) - 0.00959) < 0.001",
        "tan(radians(",
        True,
    ),
    (
        "asinh",
        "WHERE col_double > 1 and abs(asinh(col_double) - 0.9503469) < 0.001",
        "asinh(",
        True,
    ),
    (
        "acosh",
        "WHERE col_double > 1 and abs(acosh(col_double) - 0.4435682) < 0.001",
        "acosh_pg(",
        True,
    ),
    (
        "atanh",
        "WHERE abs(col_double)<1 and abs(atanh(col_double) - 0.6183813) < 0.001",
        "atanh_pg(",
        True,
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, func_expression, expected_expression, expect_pushdown",
    test_agg_cases,
    ids=[ids_list[0] for ids_list in test_agg_cases],
)
def test_math_pushdown(
    create_math_pushdown_table,
    pg_conn,
    test_id,
    func_expression,
    expected_expression,
    expect_pushdown,
):
    query = "SELECT * FROM math_f_pushdowntbl " + func_expression

    if expect_pushdown:
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    assert_query_results_on_tables(
        query, pg_conn, ["math_f_pushdowntbl"], ["math_f_pushdown_heap"]
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_math_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_math_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::smallint as c1, NULL::int as c2 , NULL::bigint as c3, NULL::float as c4, NULL::double precision as c5, NULL::real as c9, NULL::real as c10,  NULL::real as c11
						UNION ALL
					SELECT 1, 1, 1, 1.1, 1.1,  1.1, 1.1, 1.1
					 	UNION ALL
					SELECT -1, -1, -100, -1.1, -0.1,  2.2, 2.2, 2.2
						UNION ALL
					SELECT -1, -1, -100, -1.1, 0.55,  2.2, 2.2, 2.2
						UNION ALL
					SELECT 1561, 223123, -100123, -111.111111, -12222.1222222, 21231.2123123, 4534652.2456456, 4.2
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA math_f_pushdown;
	            CREATE FOREIGN TABLE math_f_pushdowntbl
	            (
	            	col_int2 smallint,
	            	col_int4 int,
	            	col_int8 bigint,
                    col_float float,
                    col_double double precision,
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
	            CREATE TABLE math_f_pushdown_heap
				(
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
                    col_float float,
                    col_double double precision,
                    col_real real,
                    col_numeric numeric,
                    col_numeric_1 NUMERIC(3, 1)
	            );
	            COPY math_f_pushdown_heap FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA math_f_pushdown CASCADE", pg_conn)
    pg_conn.commit()

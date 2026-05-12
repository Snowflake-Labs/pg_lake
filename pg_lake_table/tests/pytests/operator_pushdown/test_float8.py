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
        "float8",
        "WHERE float8(col_float4) <= col_float8",
        'WHERE (("col_float4")::double precision <= "col_float8")',
    ),
    (
        "float8",
        "WHERE col_float8 <= float8(1::int2)",
        'WHERE ("col_float8" <= ((1)::smallint)::double precision)',
    ),
    (
        "float8",
        "WHERE col_float8 <= float8(1::int4)",
        'WHERE ("col_float8" <= (1)::double precision)',
    ),
    (
        "float8",
        "WHERE col_float8 <= float8(1::int8)",
        'WHERE ("col_float8" <= ((1)::bigint)::double precision',
    ),
    (
        "float8",
        "WHERE col_float8 <= float8(1::numeric)",
        'WHERE ("col_float8" <= ((1)::numeric)::double precision)',
    ),
    (
        "float8abs",
        "WHERE @ col_float8 > 0",
        'WHERE ((@ "col_float8") > (0)::double precision)',
    ),
    (
        "float8um",
        "WHERE - col_float8 < 0",
        'WHERE ((- "col_float8") < (0)::double precision)',
    ),
    (
        "float8up",
        "WHERE + col_float8 > 0",
        'WHERE ((+ "col_float8") > (0)::double precision)',
    ),
    (
        "float8sqrt",
        "WHERE col_float8 > 0 and |/ col_float8 > 1",
        '"sqrt"("col_float8") > (1)::double precision',
    ),
    (
        "float8cbrt",
        "WHERE col_float8 > 0 and ||/ col_float8 > 1",
        '"cbrt"("col_float8") > (1)::double precision',
    ),
    (
        "float8eq",
        "WHERE col_float8 = 111.1111::float8",
        'WHERE ("col_float8" = (111.1111)::double precision)',
    ),
    (
        "float8ne",
        "WHERE col_float8 <> 2.0::float8",
        'WHERE ("col_float8" <> (2.0)::double precision)',
    ),
    (
        "float8lt",
        "WHERE col_float8 < 3.0::float8",
        'WHERE ("col_float8" < (3.0)::double precision)',
    ),
    (
        "float8gt",
        "WHERE col_float8 > 4.0::float8",
        'WHERE ("col_float8" > (4.0)::double precision)',
    ),
    (
        "float8le",
        "WHERE col_float8 <= 5.0::float8",
        'WHERE ("col_float8" <= (5.0)::double precision)',
    ),
    (
        "float8ge",
        "WHERE col_float8 >= 6.0::float8",
        'WHERE ("col_float8" >= (6.0)::double precision)',
    ),
    (
        "float8pl",
        "WHERE (col_float8 + 1.0::float8) = 112.1111::float8",
        'WHERE (("col_float8" + (1.0)::double precision) = (112.1111)::double precision)',
    ),
    (
        "float8mi",
        "WHERE (col_float8 - 1.0::float8) = 110.1111::float8",
        'WHERE (("col_float8" - (1.0)::double precision) = (110.1111)::double precision)',
    ),
    (
        "float8mul",
        "WHERE (col_float8 * 2.0::float8) = 222.2222::float8",
        'WHERE (("col_float8" * (2.0)::double precision) = (222.2222)::double precision)',
    ),
    (
        "float8div",
        "WHERE (col_float8 / 2.0::float4) > 0::float8",
        'WHERE (("col_float8" / (2.0)::real) > (0)::double precision)',
    ),
    (
        "float8pow",
        "WHERE (col_float8 ^ 2) > 2.0::float8",
        'WHERE (("col_float8" ^ (2)::double precision) > (2.0)::double precision)',
    ),
    (
        "float84eq",
        "WHERE col_float8 = col_float4",
        'WHERE ("col_float8" = "col_float4")',
    ),
    (
        "float84ne",
        "WHERE col_float8 <> col_float4",
        'WHERE ("col_float8" <> "col_float4")',
    ),
    (
        "float84lt",
        "WHERE col_float8 < col_float4",
        'WHERE ("col_float8" < "col_float4")',
    ),
    (
        "float84gt",
        "WHERE col_float8 > col_float4",
        'WHERE ("col_float8" > "col_float4")',
    ),
    (
        "float84le",
        "WHERE col_float8 <= col_float4",
        'WHERE ("col_float8" <= "col_float4")',
    ),
    (
        "float84ge",
        "WHERE col_float8 >= col_float4",
        'WHERE ("col_float8" >= "col_float4")',
    ),
    (
        "float84pl",
        "WHERE (col_float8 + col_float4) > 0",
        'WHERE (("col_float8" + "col_float4") > (0)::double precision)',
    ),
    (
        "float84mi",
        "WHERE (col_float8 - col_float4) = 0::float8",
        'WHERE (("col_float8" - "col_float4") = (0)::double precision)',
    ),
    (
        "float84mul",
        "WHERE (col_float8 * col_float4) IS NULL",
        'WHERE (("col_float8" * "col_float4") IS NULL)',
    ),
    (
        "float84div",
        "WHERE (col_float8 / 2.0::float4) > 1::float4",
        'WHERE (("col_float8" / (2.0)::real) > (1)::real)',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_float_comparison_operator_pushdown(
    create_maxtest_float_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM maxtest_float_comparison_operator_pushdown.tbl "
        + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["maxtest_float_comparison_operator_pushdown.tbl"],
        ["maxtest_float_comparison_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_maxtest_float_comparison_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_maxtest_float_comparison_operator_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::float as col_float4, NULL::float8 as col_float8
						UNION ALL
					SELECT 111.1111::float4 as col_float4, 111.1111::float8 as col_float8
					 	UNION ALL
					SELECT -222.22222::float4 as col_float4, -222.22222::float8 as col_float8
                        UNION ALL
                    SELECT 500::float4 as col_float4, 500::float8 as col_float8
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA maxtest_float_comparison_operator_pushdown;
	            CREATE FOREIGN TABLE maxtest_float_comparison_operator_pushdown.tbl
	            (
	            	col_float4 float4,
	            	col_float8 float8
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
	            CREATE TABLE maxtest_float_comparison_operator_pushdown.heap_tbl
				(
                    col_float4 float4,
                    col_float8 float8
	            );
	            COPY maxtest_float_comparison_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command(
        "DROP SCHEMA maxtest_float_comparison_operator_pushdown CASCADE", pg_conn
    )
    pg_conn.commit()

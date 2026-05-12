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
    ("int8", "WHERE int8(col_int2) = 1", 'WHERE (("col_int2")::bigint = 1)'),
    ("int8", "WHERE int8(col_int4) = 1", 'WHERE (("col_int4")::bigint = 1)'),
    (
        "int8",
        "WHERE col_int8 = int8(1.0::real)",
        'WHERE ("col_int8" = ((1.0)::real)::bigint)',
    ),
    (
        "int8",
        "WHERE col_int8 = int8(1.0::double precision)",
        'WHERE ("col_int8" = ((1.0)::double precision)::bigint)',
    ),
    (
        "int8",
        "WHERE col_int8 = int8(1.0::numeric)",
        'WHERE ("col_int8" = (1.0)::bigint)',
    ),
    ("int8abs", "WHERE @ col_int8 > 0", 'WHERE ((@ "col_int8") > 0)'),
    ("int8um", "WHERE - col_int8 < 0", 'WHERE ((- "col_int8") < 0)'),
    ("int8up", "WHERE + col_int8 > 0", 'WHERE ((+ "col_int8") > 0)'),
    ("int8not", "WHERE ~ col_int8 < 0", 'WHERE ((~ "col_int8") < 0)'),
    ("int84eq", "WHERE ((1)::bigint = col_int4)", 'WHERE ((1)::bigint = "col_int4")'),
    ("int84ne", "WHERE ((1)::bigint <> col_int4)", 'WHERE ((1)::bigint <> "col_int4")'),
    (
        "int84ne_alt_syntax",
        "WHERE ((1)::bigint != col_int4)",
        'WHERE ((1)::bigint <> "col_int4")',
    ),
    ("int84lt", "WHERE ((1)::bigint < col_int4)", 'WHERE ((1)::bigint < "col_int4")'),
    ("int84gt", "WHERE ((1)::bigint > col_int4)", 'WHERE ((1)::bigint > "col_int4")'),
    ("int84le", "WHERE ((1)::bigint <= col_int4)", 'WHERE ((1)::bigint <= "col_int4")'),
    ("int84ge", "WHERE ((1)::bigint >= col_int4)", 'WHERE ((1)::bigint >= "col_int4")'),
    (
        "int84pl",
        "WHERE 1::bigint + col_int4 > 5::bigint;",
        'WHERE (((1)::bigint + "col_int4") > (5)::bigint)',
    ),
    (
        "int84mi",
        "WHERE 1000::bigint - col_int4 > 5::bigint;",
        'WHERE (((1000)::bigint - "col_int4") > (5)::bigint)',
    ),
    (
        "int84mul",
        "WHERE 1::bigint * col_int4 > 5::bigint;",
        'WHERE (((1)::bigint * "col_int4") > (5)::bigint)',
    ),
    (
        "int84div",
        "WHERE 10000::bigint / col_int4 > 5::bigint;",
        'WHERE ("divide"((10000)::bigint, "col_int4") > (5)::bigint)',
    ),
    ("int82eq", "WHERE ((1)::bigint = col_int2)", 'WHERE ((1)::bigint = "col_int2")'),
    ("int82ne", "WHERE ((1)::bigint <> col_int2)", 'WHERE ((1)::bigint <> "col_int2")'),
    (
        "int82ne_alt_syntax",
        "WHERE ((1)::bigint != col_int2)",
        'WHERE ((1)::bigint <> "col_int2")',
    ),
    ("int82lt", "WHERE ((1)::bigint < col_int2)", 'WHERE ((1)::bigint < "col_int2")'),
    ("int82gt", "WHERE ((1)::bigint > col_int2)", 'WHERE ((1)::bigint > "col_int2")'),
    ("int82le", "WHERE ((1)::bigint <= col_int2)", 'WHERE ((1)::bigint <= "col_int2")'),
    ("int82ge", "WHERE ((1)::bigint >= col_int2)", 'WHERE ((1)::bigint >= "col_int2")'),
    (
        "int82pl",
        "WHERE 1::bigint + col_int2 > 5::bigint;",
        'WHERE (((1)::bigint + "col_int2") > (5)::bigint)',
    ),
    (
        "int82mi",
        "WHERE 1000::bigint - col_int2 > 5::bigint;",
        'WHERE (((1000)::bigint - "col_int2") > (5)::bigint)',
    ),
    (
        "int82mul",
        "WHERE 1::bigint * col_int2 > 5::bigint;",
        'WHERE (((1)::bigint * "col_int2") > (5)::bigint)',
    ),
    (
        "int82div",
        "WHERE 100000::bigint / col_int2 > 5::bigint;",
        'WHERE ("divide"((100000)::bigint, "col_int2") > (5)::bigint)',
    ),
    ("int8eq", "WHERE col_int8 = 1::bigint", 'WHERE ("col_int8" = (1)::bigint)'),
    ("int8ne", "WHERE col_int8 <> 1::bigint", 'WHERE ("col_int8" <> (1)::bigint)'),
    ("int8lt", "WHERE col_int8 < 1::bigint", 'WHERE ("col_int8" < (1)::bigint)'),
    ("int8gt", "WHERE col_int8 > 1::bigint", 'WHERE ("col_int8" > (1)::bigint)'),
    ("int8le", "WHERE col_int8 <= 1::bigint", 'WHERE ("col_int8" <= (1)::bigint)'),
    ("int8ge", "WHERE col_int8 >= 1::bigint", 'WHERE ("col_int8" >= (1)::bigint)'),
    (
        "int8pl",
        "WHERE (col_int8 + 1::bigint) > 10::bigint",
        'WHERE (("col_int8" + (1)::bigint) > (10)::bigint)',
    ),
    (
        "int8mi",
        "WHERE (col_int8 - 1::bigint) = 0::bigint",
        'WHERE (("col_int8" - (1)::bigint) = (0)::bigint)',
    ),
    (
        "int8mul",
        "WHERE (col_int8 * 1::bigint) > 25::bigint",
        'WHERE (("col_int8" * (1)::bigint) > (25)::bigint)',
    ),
    (
        "int8div",
        "WHERE (col_int8 / 1::bigint) = (1)::bigint",
        'WHERE ("divide"("col_int8", (1)::bigint) = (1)::bigint)',
    ),
    ("int8mod", "WHERE (col_int8 % 2) = 1", 'WHERE (("col_int8" % (2)::bigint) = 1)'),
    ("int8and", "WHERE col_int8 & 1 = 1", 'WHERE (("col_int8" & (1)::bigint) = 1)'),
    ("int8or", "WHERE col_int8 | 1 = 1", 'WHERE (("col_int8" | (1)::bigint) = 1)'),
    ("int8xor", "WHERE col_int8 # 1 = 0", 'WHERE ("xor"("col_int8", (1)::bigint) = 0)'),
    (
        "int8shl",
        "WHERE col_int8 > 0 and col_int8 << 1 = 2",
        'WHERE (("col_int8" > 0) AND (("col_int8" << 1) = 2))',
    ),
    (
        "int8shr",
        "WHERE col_int8 > 0 and col_int8 >> 1 = 0",
        'WHERE (("col_int8" > 0) AND (("col_int8" >> 1) = 0))',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_int_comparison_operator_pushdown(
    create_test_int_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM test_int_comparison_operator_pushdown.tbl " + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_int_comparison_operator_pushdown.tbl"],
        ["test_int_comparison_operator_pushdown.heap_tbl"],
    )


agg_test_cases = [
    ("bool_and", "bool_and(col_bool)", '"bool_and"("col_bool")'),
    ("bool_or", "bool_or(col_bool)", '"bool_or"("col_bool")'),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, agg_expression, expected_expression",
    agg_test_cases,
    ids=[test_case[0] for test_case in agg_test_cases],
)
def test_int_agg_comparison_operator_pushdown(
    create_test_int_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    agg_expression,
    expected_expression,
):
    query = (
        "SELECT " + agg_expression + " FROM test_int_comparison_operator_pushdown.tbl "
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_int_comparison_operator_pushdown.tbl"],
        ["test_int_comparison_operator_pushdown.heap_tbl"],
    )


# Trivially verify that our division by int gets rewritten to a "divide"() statement
def test_integer_division_pushdown(
    pg_conn, create_test_int_comparison_operator_pushdown_table
):
    assert_remote_query_contains_expression(
        "SELECT COUNT(*)/3 FROM test_int_comparison_operator_pushdown.tbl",
        '"divide"(',
        pg_conn,
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_int_comparison_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_test_int_comparison_operator_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::int2 as col_int2, NULL::int4 as col_int4, NULL::bigint as col_int8, NULL::bool as col_bool, NULL::bool as col_bool2
						UNION ALL
					SELECT 1::int2 as col_int2, 1::int4 as col_int4, 1::bigint as col_int8, 1::bool, 0::bool
					 	UNION ALL
					SELECT -1::int2 as col_int2, -1::int4 as col_int4, -1::bigint as col_int8, 1::bool, 0::bool
					 	UNION ALL
					SELECT 100::int2 as col_int2, 100::int4 as col_int4, 100::bigint as col_int8, 0::bool, 1::bool
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA test_int_comparison_operator_pushdown;
	            CREATE FOREIGN TABLE test_int_comparison_operator_pushdown.tbl
	            (
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
					col_bool boolean,
					col_bool2 boolean
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
	            CREATE TABLE test_int_comparison_operator_pushdown.heap_tbl
				(
					col_int2 smallint,
					col_int4 int,
					col_int8 bigint,
					col_bool boolean,
					col_bool2 boolean
	            );
	            COPY test_int_comparison_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_int_comparison_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()

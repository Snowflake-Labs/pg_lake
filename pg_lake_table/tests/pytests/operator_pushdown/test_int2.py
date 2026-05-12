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
    ("int2", "WHERE int2(col_int4) = 1", 'WHERE (("col_int4")::smallint = 1)'),
    ("int2", "WHERE int2(col_int8) = 1", 'WHERE (("col_int8")::smallint = 1)'),
    (
        "int2",
        "WHERE col_int2 = int2(1.0::real)",
        'WHERE ("col_int2" = ((1.0)::real)::smallint)',
    ),
    (
        "int2",
        "WHERE col_int2 = int2(1.0::double precision)",
        'WHERE ("col_int2" = ((1.0)::double precision)::smallint)',
    ),
    (
        "int2",
        "WHERE col_int2 = int2(1.0::numeric)",
        'WHERE ("col_int2" = (1.0)::smallint)',
    ),
    ("int2abs", "WHERE @ col_int2 > 0", 'WHERE ((@ "col_int2") > 0)'),
    ("int2um", "WHERE - col_int2 < 0", 'WHERE ((- "col_int2") < 0)'),
    ("int2up", "WHERE + col_int2 > 0", 'WHERE ((+ "col_int2") > 0)'),
    ("int2not", "WHERE ~ col_int2 < 0", 'WHERE ((~ "col_int2") < 0)'),
    ("int28eq", "WHERE (col_int2 = (1)::bigint)", 'WHERE ("col_int2" = (1)::bigint)'),
    ("int28ne", "WHERE (col_int2 <> (1)::bigint)", 'WHERE ("col_int2" <> (1)::bigint)'),
    (
        "int28ne_alt_syntax",
        "WHERE (col_int2 != (1)::bigint)",
        'WHERE ("col_int2" <> (1)::bigint)',
    ),
    ("int28lt", "WHERE (col_int2 < (1)::bigint)", 'WHERE ("col_int2" < (1)::bigint)'),
    ("int28gt", "WHERE (col_int2 > (1)::bigint)", 'WHERE ("col_int2" > (1)::bigint)'),
    ("int28le", "WHERE (col_int2 <= (1)::bigint)", 'WHERE ("col_int2" <= (1)::bigint)'),
    ("int28ge", "WHERE (col_int2 >= (1)::bigint)", 'WHERE ("col_int2" >= (1)::bigint)'),
    (
        "int28pl",
        "WHERE col_int2 + (1)::bigint > 5",
        'WHERE (("col_int2" + (1)::bigint) > 5)',
    ),
    (
        "int28mi",
        "WHERE col_int2 - (1)::bigint > 5",
        'WHERE (("col_int2" - (1)::bigint) > 5)',
    ),
    (
        "int28mul",
        "WHERE col_int2 * (1)::bigint > 5",
        'WHERE (("col_int2" * (1)::bigint) > 5)',
    ),
    (
        "int28div",
        "WHERE col_int2 / (1)::bigint > 5",
        'WHERE ("divide"("col_int2", (1)::bigint) > 5)',
    ),
    ("int24eq", "WHERE (col_int2 = 1)", 'WHERE ("col_int2" = 1'),
    ("int24ne", "WHERE (col_int2 <> 1)", 'WHERE ("col_int2" <> 1'),
    ("int24ne_alt_syntax", "WHERE (col_int2 != 1)", 'WHERE ("col_int2" <> 1'),
    ("int24lt", "WHERE (col_int2 < 1)", 'WHERE ("col_int2" < 1)'),
    ("int24gt", "WHERE (col_int2 > 1)", 'WHERE ("col_int2" > 1)'),
    ("int24le", "WHERE (col_int2 <= 1)", 'WHERE ("col_int2" <= 1)'),
    ("int24ge", "WHERE (col_int2 >= 1)", 'WHERE ("col_int2" >= 1)'),
    ("int24pl", "WHERE col_int2 + 1 >= 1", 'WHERE (("col_int2" + 1) >= 1)'),
    ("int24mi", "WHERE col_int2 - 1 >= 1", 'WHERE (("col_int2" - 1) >= 1)'),
    ("int24mul", "WHERE col_int2 * 1 >= 1", 'WHERE (("col_int2" * 1) >= 1)'),
    ("int24div", "WHERE col_int2 / 1 >= 1", 'WHERE ("divide"("col_int2", 1) >= 1)'),
    ("int2eq", "WHERE col_int2 = 1", 'WHERE ("col_int2" = 1)'),
    ("int2ne", "WHERE col_int2 <> 1", 'WHERE ("col_int2" <> 1)'),
    ("int2lt", "WHERE col_int2 < 1", 'WHERE ("col_int2" < 1)'),
    ("int2gt", "WHERE col_int2 > 1", 'WHERE ("col_int2" > 1)'),
    ("int2le", "WHERE col_int2 <= 1", 'WHERE ("col_int2" <= 1)'),
    ("int2ge", "WHERE col_int2 >= 1", 'WHERE ("col_int2" >= 1)'),
    ("int2pl", "WHERE (col_int2 + 1) > 10", 'WHERE (("col_int2" + 1) > 10)'),
    ("int2mi", "WHERE (col_int2 - 1) = 0", 'WHERE (("col_int2" - 1) = 0)'),
    ("int2mul", "WHERE (col_int2 * 1) > 25", 'WHERE (("col_int2" * 1) > 25)'),
    ("int2div", "WHERE (col_int2 / 1) = 1", 'WHERE ("divide"("col_int2", 1) = 1)'),
    (
        "int2mod",
        "WHERE (col_int2 % 2::smallint) = 1",
        'WHERE (("col_int2" % (2)::smallint) = 1)',
    ),
    (
        "int2and",
        "WHERE col_int2 & 1::smallint = 1",
        'WHERE (("col_int2" & (1)::smallint) = 1)',
    ),
    (
        "int2or",
        "WHERE col_int2 | 1::smallint = 1",
        'WHERE (("col_int2" | (1)::smallint) = 1)',
    ),
    (
        "int2xor",
        "WHERE col_int2 # 1::smallint = 0",
        'WHERE ("xor"("col_int2", (1)::smallint) = 0)',
    ),
    (
        "int2shl",
        "WHERE col_int2 > 0 and col_int2 << 1 = 2",
        'WHERE (("col_int2" > 0) AND (("col_int2" << 1) = 2))',
    ),
    (
        "int2shr",
        "WHERE col_int2 > 0 and col_int2 >> 1 = 0",
        'WHERE (("col_int2" > 0) AND (("col_int2" >> 1) = 0))',
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
def test_bool_agg_comparison_operator_pushdown(
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

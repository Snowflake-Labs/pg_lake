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
    ("int4", "WHERE int4(col_int2) = 1", 'WHERE (("col_int2")::integer = 1)'),
    ("int4", "WHERE int4(col_int8) = 1", 'WHERE (("col_int8")::integer = 1)'),
    (
        "int4",
        "WHERE col_int4 = int4(1.0::real)",
        'WHERE ("col_int4" = ((1.0)::real)::integer)',
    ),
    (
        "int4",
        "WHERE col_int4 = int4(1.0::double precision)",
        'WHERE ("col_int4" = ((1.0)::double precision)::integer)',
    ),
    (
        "int4",
        "WHERE col_int4 = int4(1.0::numeric)",
        'WHERE ("col_int4" = (1.0)::integer)',
    ),
    ("int4abs", "WHERE @ col_int4 > 0", 'WHERE ((@ "col_int4") > 0)'),
    ("int4um", "WHERE - col_int4 < 0", 'WHERE ((- "col_int4") < 0)'),
    ("int4up", "WHERE + col_int4 > 0", 'WHERE ((+ "col_int4") > 0)'),
    ("int4not", "WHERE ~ col_int4 < 0", 'WHERE ((~ "col_int4") < 0)'),
    ("int48eq", "WHERE col_int4 = (1)::bigint", 'WHERE ("col_int4" = (1)::bigint)'),
    ("int48ne", "WHERE col_int4 <> (1)::bigint", 'WHERE ("col_int4" <> (1)::bigint)'),
    (
        "int48ne_alt_syntax",
        "WHERE col_int4 != (1)::bigint",
        'WHERE ("col_int4" <> (1)::bigint)',
    ),
    ("int48lt", "WHERE col_int4 < (1)::bigint", 'WHERE ("col_int4" < (1)::bigint)'),
    ("int48gt", "WHERE col_int4 > (1)::bigint", 'WHERE ("col_int4" > (1)::bigint)'),
    ("int48le", "WHERE col_int4 <= (1)::bigint", 'WHERE ("col_int4" <= (1)::bigint)'),
    ("int48ge", "WHERE col_int4 >= (1)::bigint", 'WHERE ("col_int4" >= (1)::bigint)'),
    (
        "int48pl",
        "WHERE ((col_int4 + (1)::bigint)) > (5)::bigint",
        'WHERE (("col_int4" + (1)::bigint) > (5)::bigint)',
    ),
    (
        "int48mi",
        "WHERE ((col_int4 - (1)::bigint)) > (5)::bigint",
        'WHERE (("col_int4" - (1)::bigint) > (5)::bigint)',
    ),
    (
        "int48mul",
        "WHERE ((col_int4 * (1)::bigint)) > (5)::bigint",
        'WHERE (("col_int4" * (1)::bigint) > (5)::bigint)',
    ),
    (
        "int48div",
        "WHERE ((col_int4 / (1)::bigint)) > (5)::bigint",
        'WHERE ("divide"("col_int4", (1)::bigint) > (5)::bigint)',
    ),
    (
        "int42eq",
        "WHERE (col_int4 = (1)::smallint)",
        'WHERE ("col_int4" = (1)::smallint)',
    ),
    (
        "int42ne",
        "WHERE (col_int4 <> (1)::smallint)",
        'WHERE ("col_int4" <> (1)::smallint)',
    ),
    (
        "int42ne_alt_syntax",
        "WHERE (col_int4 != (1)::smallint)",
        'WHERE ("col_int4" <> (1)::smallint)',
    ),
    (
        "int42lt",
        "WHERE (col_int4 < (1)::smallint)",
        'WHERE ("col_int4" < (1)::smallint)',
    ),
    (
        "int42gt",
        "WHERE (col_int4 > (1)::smallint)",
        'WHERE ("col_int4" > (1)::smallint)',
    ),
    (
        "int42le",
        "WHERE (col_int4 <= (1)::smallint)",
        'WHERE ("col_int4" <= (1)::smallint)',
    ),
    (
        "int42ge",
        "WHERE (col_int4 >= (1)::smallint)",
        'WHERE ("col_int4" >= (1)::smallint)',
    ),
    (
        "int42pl",
        "WHERE ((col_int4 + (1)::smallint) >= (5)::smallint)",
        'WHERE (("col_int4" + (1)::smallint) >= (5)::smallint)',
    ),
    (
        "int42mi",
        "WHERE ((col_int4 - (1)::smallint) >= (5)::smallint)",
        'WHERE (("col_int4" - (1)::smallint) >= (5)::smallint)',
    ),
    (
        "int42mul",
        "WHERE ((col_int4 * (1)::smallint) >= (5)::smallint)",
        'WHERE (("col_int4" * (1)::smallint) >= (5)::smallint)',
    ),
    (
        "int42div",
        "WHERE ((col_int4 / (1)::smallint) >= (5)::smallint)",
        'WHERE ("divide"("col_int4", (1)::smallint) >= (5)::smallint)',
    ),
    ("int4eq", "WHERE col_int4 = 1", 'WHERE ("col_int4" = 1)'),
    ("int4ne", "WHERE col_int4 <> 1", 'WHERE ("col_int4" <> 1)'),
    ("int4lt", "WHERE col_int4 < 1", 'WHERE ("col_int4" < 1)'),
    ("int4gt", "WHERE col_int4 > 1", 'WHERE ("col_int4" > 1)'),
    ("int4le", "WHERE col_int4 <= 1", 'WHERE ("col_int4" <= 1)'),
    ("int4ge", "WHERE col_int4 >= 1", 'WHERE ("col_int4" >= 1)'),
    ("int4pl", "WHERE (col_int4 + 1) <= 10", 'WHERE (("col_int4" + 1) <= 10)'),
    ("int4mi", "WHERE (col_int4 - 1) <= 0", 'WHERE (("col_int4" - 1) <= 0)'),
    ("int4mul", "WHERE (col_int4 * 1) <= 25", 'WHERE (("col_int4" * 1) <= 25)'),
    ("int4div", "WHERE (col_int4 / 1) <= 1", 'WHERE ("divide"("col_int4", 1) <= 1)'),
    ("int4mod", "WHERE (col_int4 % 2) = 1", 'WHERE (("col_int4" % 2) = 1)'),
    ("int4and", "WHERE col_int4 & 1 = 1", 'WHERE (("col_int4" & 1) = 1)'),
    ("int4or", "WHERE col_int4 | 1 = 1", 'WHERE (("col_int4" | 1) = 1)'),
    ("int4xor", "WHERE col_int4 # 1 = 0", 'WHERE ("xor"("col_int4", 1) = 0)'),
    (
        "int4shl",
        "WHERE col_int4 > 0 and col_int4 << 1 = 2",
        'WHERE (("col_int4" > 0) AND (("col_int4" << 1) = 2))',
    ),
    (
        "int4shr",
        "WHERE col_int4 > 0 and col_int4 >> 1 = 0",
        'WHERE (("col_int4" > 0) AND (("col_int4" >> 1) = 0))',
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
    extension,
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
    extension,
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

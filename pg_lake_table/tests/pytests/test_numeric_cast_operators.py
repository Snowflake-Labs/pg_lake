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
        "float8_int2",
        "WHERE col_float8 = 500::int2",
        '("col_float8" = ((500)::smallint)::double precision)',
    ),
    (
        "float8_int4",
        "WHERE col_float8 = 500::int4",
        '("col_float8" = (500)::double precision)',
    ),
    (
        "float8_int8",
        "WHERE col_float8 = 500::int8",
        '("col_float8" = ((500)::bigint)::double precision)',
    ),
    ("float8_float4", "WHERE col_float8 = 500::float4", '("col_float8" = (500)::real)'),
    (
        "float8_numeric",
        "WHERE col_float8 = 500::numeric",
        '("col_float8" = ((500)::numeric)::double precision)',
    ),
    # Downcasting in effect due to PostgreSQL adding float8 casts, while DuckDB prefers float4 casts
    (
        "float4_int2",
        "WHERE col_float4 >= -500::int2",
        '("col_float4" >= ((- (500)::smallint))::double precision)',
    ),
    ("float4_int4", "WHERE col_float4 = 500::int4", "(\"col_float4\" = '500'::real)"),
    (
        "float4_int8",
        "WHERE col_float4 < 50000000000000123::int8",
        "(\"col_float4\" < '5e+16'::real)",
    ),
    (
        "float4_float8",
        "WHERE col_float4 = 500::float8",
        "(\"col_float4\" = '500'::real)",
    ),
    (
        "float4_numeric",
        "WHERE col_float4 <> -500.342::numeric",
        '("col_float4" <> ((- 500.342))::double precision)',
    ),
    (
        "int8_bit",
        "WHERE col_int8 = int8(B'100000'::bit)",
        'WHERE ("col_int8" = (1)::bigint)',
    ),
    (
        "int8_double",
        "WHERE col_int8 = 500::double precision",
        'WHERE (("col_int8")::double precision = (500)::double precision)',
    ),
    (
        "int8_float4",
        "WHERE col_int8 = 500::float4",
        'WHERE (("col_int8")::double precision = (500)::real)',
    ),
    ("int8_integer", "WHERE col_int8 = 500::integer", 'WHERE ("col_int8" = 500)'),
    (
        "int8_numeric",
        "WHERE col_int8 = 500::numeric",
        'WHERE (("col_int8")::numeric = (500)::numeric)',
    ),
    (
        "int8_smallint",
        "WHERE col_int8 = 500::smallint",
        'WHERE ("col_int8" = (500)::smallint)',
    ),
    (
        "int8_real",
        "WHERE col_int8 = 500::real",
        '(("col_int8")::double precision = (500)::real)',
    ),
    ("int4_bit", "WHERE col_int4 = int4(B'100000'::bit)", 'WHERE ("col_int4" = 1)'),
    (
        "int4_double",
        "WHERE col_int4 = 500::double precision",
        'WHERE (("col_int4")::double precision = (500)::double precision)',
    ),
    (
        "int4_float4",
        "WHERE col_int4 = 500::float4",
        'WHERE (("col_int4")::double precision = (500)::real)',
    ),
    (
        "int4_bigint",
        "WHERE col_int4 = 500::bigint",
        'WHERE ("col_int4" = (500)::bigint)',
    ),
    (
        "int4_numeric",
        "WHERE col_int4 = 500::numeric",
        'WHERE (("col_int4")::numeric = (500)::numeric)',
    ),
    (
        "int4_smallint",
        "WHERE col_int4 = 500::smallint",
        'WHERE ("col_int4" = (500)::smallint)',
    ),
    (
        "int4_real",
        "WHERE col_int4 = 500::real",
        'WHERE (("col_int4")::double precision = (500)::real)',
    ),
    ("int2_bit", "WHERE col_int2 = int4(B'100000'::bit)", 'WHERE ("col_int2" = 1)'),
    (
        "int2_double",
        "WHERE col_int2 = 500::double precision",
        'WHERE (("col_int2")::double precision = (500)::double precision)',
    ),
    (
        "int2_float4",
        "WHERE col_int2 = 500::float4",
        'WHERE (("col_int2")::double precision = (500)::real)',
    ),
    ("int2_integer", "WHERE col_int2 = 500::integer", 'WHERE ("col_int2" = 500)'),
    (
        "int2_numeric",
        "WHERE col_int2 = 500::numeric",
        'WHERE (("col_int2")::numeric = (500)::numeric)',
    ),
    (
        "int2_bigint",
        "WHERE col_int2 = 500::smallint",
        'WHERE ("col_int2" = (500)::smallint)',
    ),
    (
        "int2_real",
        "WHERE col_int2 = 500::real",
        'WHERE (("col_int2")::double precision = (500)::real)',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_float_comparison_operator_pushdown(
    create_maxtest_datatypes_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM maxtest_datatypes.tbl " + operator_expression
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["maxtest_datatypes.tbl"], ["maxtest_datatypes.heap_tbl"]
    )


# Create the table on both Postgres
@pytest.fixture(scope="module")
def create_maxtest_datatypes_table(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/create_maxtest_datatypes_table/data.parquet"
    # Generate and copy data for all specified types into a Parquet file
    run_command(
        f"""
            COPY (
                    SELECT
                        NULL::int2 as col_int2,
                        NULL::int4 as col_int4,
                        NULL::int8 as col_int8,
                        NULL::float4 as col_float4,
                        NULL::float8 as col_float8,
                        NULL::numeric as col_numeric,
                        NULL::real as col_real
                    UNION ALL
                    SELECT
                        1::int2,
                        1::int4,
                        1::int8,
                        111.1111::float4,
                        111.1111::float8,
                        123.456::numeric,
                        255.75::real
                    UNION ALL
                    SELECT
                        -1::int2,
                        -100::int4,
                        -10000::int8,
                        -222.22222::float4,
                        -222.22222::float8,
                        -123.456::numeric,
                        -255.75::real
                    UNION ALL
                    SELECT
                        500::int2,
                        500::int4,
                        500::int8,
                        500::float4,
                        500::float8,
                        9999999.999::numeric,
                        12345.6789::real
            ) TO '{url}' WITH (FORMAT 'parquet');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a schema and a foreign table with the specified columns
    run_command(
        """
                CREATE SCHEMA maxtest_datatypes;
                CREATE FOREIGN TABLE maxtest_datatypes.tbl
                (
                    col_int2 int2,
                    col_int4 int4,
                    col_int8 int8,
                    col_float4 float4,
                    col_float8 float8,
                    col_numeric numeric,
                    col_real real
                ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
                """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    # Create a heap table and copy data from the Parquet file
    run_command(
        """
                CREATE TABLE maxtest_datatypes.heap_tbl
                (
                    col_int2 int2,
                    col_int4 int4,
                    col_int8 int8,
                    col_float4 float4,
                    col_float8 float8,
                    col_numeric numeric,
                    col_real real
                );
                COPY maxtest_datatypes.heap_tbl FROM '{}';
                """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    yield

    # Clean up: Drop the schema and all contained tables
    run_command("DROP SCHEMA maxtest_datatypes CASCADE", pg_conn)
    pg_conn.commit()

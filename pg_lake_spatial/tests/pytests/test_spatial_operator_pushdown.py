import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *


test_cases = [
    (
        "geometry_eq",
        "WHERE col_point = ST_GeomFromText('POINT(4 4)')",
        "WHERE (col_point = st_geomfromtext('POINT(4 4)'::text))",
    ),
    ("<->", "WHERE col_point <-> ST_GeomFromText('POINT(4 4)') > 1", "st_distance"),
    ("&&", "WHERE col_point && 'POINT(4 4)'::geometry", "st_intersects_extent"),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_spatial_operator_pushdown(
    create_test_spatial_operator_pushdown_table,
    user_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM test_spatial_operator_pushdown.tbl " + operator_expression

    assert_remote_query_contains_expression(query, expected_expression, user_conn)
    assert_query_results_on_tables(
        query,
        user_conn,
        ["test_spatial_operator_pushdown.tbl"],
        ["test_spatial_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_spatial_operator_pushdown_table(
    user_conn, s3, spatial_analytics_extension, pg_lake_extension
):
    url = f"s3://{TEST_BUCKET}/create_test_spatial_operator_pushdown_table/data.parquet"

    run_command(
        f"""
        COPY ( SELECT * FROM (VALUES
          (NULL, NULL::geometry),
          ('', 'POINT(0 0)'::geometry),
          ('pi', 'POINT(3.14 6.28)'::geometry),
          ('four', 'POINT(4 4)'::geometry)
        ) AS res (col_text, col_geometry)) TO '{url}' WITH (FORMAT 'parquet');

        CREATE SCHEMA test_spatial_operator_pushdown;
        CREATE FOREIGN TABLE test_spatial_operator_pushdown.tbl
        (
            col_text text,
            col_point geometry
        ) SERVER pg_lake OPTIONS (format 'parquet', path '{url}');

        CREATE TABLE test_spatial_operator_pushdown.heap_tbl
        AS SELECT * FROM test_spatial_operator_pushdown.tbl
    """,
        user_conn,
    )

    user_conn.commit()

    yield
    user_conn.rollback()

    run_command("DROP SCHEMA test_spatial_operator_pushdown CASCADE", user_conn)
    user_conn.commit()

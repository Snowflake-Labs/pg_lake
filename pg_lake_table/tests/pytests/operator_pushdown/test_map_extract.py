import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *


# only one check so parameterization is overkill, but use the same skeleton
def test_map_extract_operator_pushdown_where(pg_conn, map_extract_setup):

    operator_expression = "WHERE map_col->'foo' = 1"
    expected_expression = 'WHERE (("map_extract"("map_col", \'foo\'::"text"))[1] = 1)'
    query = "SELECT * FROM map_extract_pushdown.tbl " + operator_expression

    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["map_extract_pushdown.tbl"], ["map_extract_pushdown.heap_tbl"]
    )

    pg_conn.rollback()


def test_map_extract_operator_pushdown_target(pg_conn, map_extract_setup):

    expected_expression = 'SELECT ("map_extract"("map_col", \'foo\'::"text"))[1]'
    query = "SELECT map_col->'foo' FROM map_extract_pushdown.tbl"

    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["map_extract_pushdown.tbl"], ["map_extract_pushdown.heap_tbl"]
    )

    pg_conn.rollback()


def test_map_extract_function_pushdown_where(pg_conn, map_extract_setup):

    operator_expression = "WHERE map_type.extract(map_col, 'foo') = 1"
    expected_expression = 'WHERE (("map_extract"("map_col", \'foo\'::"text"))[1] = 1)'
    query = "SELECT * FROM map_extract_pushdown.tbl " + operator_expression

    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["map_extract_pushdown.tbl"], ["map_extract_pushdown.heap_tbl"]
    )

    pg_conn.rollback()


def test_map_extract_function_pushdown_target(pg_conn, map_extract_setup):

    expected_expression = 'SELECT ("map_extract"("map_col", \'foo\'::"text"))[1]'
    query = "SELECT map_type.extract(map_col, 'foo') FROM map_extract_pushdown.tbl"

    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["map_extract_pushdown.tbl"], ["map_extract_pushdown.heap_tbl"]
    )

    pg_conn.rollback()


# create the table on both Postgres
@pytest.fixture(scope="module")
def map_extract_setup(superuser_conn, duckdb_conn, request, extension, app_user):

    run_command(
        f"""
	            CREATE SCHEMA map_extract_pushdown;
                    GRANT USAGE ON SCHEMA map_extract_pushdown TO {app_user};
	            """,
        superuser_conn,
    )
    superuser_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT  NULL::MAP(TEXT,INT) AS map_col, NULL::text AS text_col
						UNION ALL
					SELECT MAP {{ 'foo': 1, 'bar': 2 }}, 'baz'
					 	UNION ALL
					SELECT MAP {{ 'fee': 3, 'fi': NULL, 'foo': 1234 }}, 'fum'
				) TO '{url}';
		""",
        duckdb_conn,
    )

    # Create a table with 2 columns on the fdw
    create_map_type("text", "int")
    run_command(
        f"""
	            CREATE FOREIGN TABLE map_extract_pushdown.tbl
	            (
                        map_col map_type.key_text_val_int,
                        text_col text
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
                    GRANT SELECT ON map_extract_pushdown.tbl TO {app_user};
	            """,
        superuser_conn,
    )

    superuser_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        f"""
	            CREATE TABLE map_extract_pushdown.heap_tbl
                    (
                        map_col map_type.key_text_val_int,
                        text_col text
                    );
	            COPY map_extract_pushdown.heap_tbl FROM '{url}';
                    GRANT SELECT ON map_extract_pushdown.heap_tbl TO {app_user};
	            """,
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    run_command("DROP SCHEMA map_extract_pushdown CASCADE", superuser_conn)
    superuser_conn.commit()

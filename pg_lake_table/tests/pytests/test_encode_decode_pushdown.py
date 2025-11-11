import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *


@pytest.mark.parametrize(
    "format,pushdown",
    [
        ("base64", True),
        ("hex", True),
        ("escape", False),
    ],
)
def test_encode_decode_pushdown(
    s3,
    pg_conn,
    extension,
    with_default_location,
    format,
    pushdown,
):
    run_command(
        f"""
            CREATE TABLE test_encode_decode_function_pushdown_heap(
                col_bytea bytea
            );

            INSERT INTO test_encode_decode_function_pushdown_heap VALUES (NULL),
            ('\\x0102'), ('\\x0a0b0c0d0e0f'), ('\\xffeeffee'), ('\\x1234567890abcdef'),
            ('abcd'), ('\\012\\345\\067');

            CREATE TABLE test_encode_decode_function_pushdown USING iceberg AS SELECT * FROM test_encode_decode_function_pushdown_heap;
        """,
        pg_conn,
    )

    query = f"SELECT decode(encode(col_bytea, '{format}'), '{format}') FROM test_encode_decode_function_pushdown ORDER BY col_bytea"

    if pushdown:
        assert_remote_query_contains_expression(
            query, f"from_{format}(to_{format}(col_bytea))", pg_conn
        )
    else:
        assert_remote_query_not_contains_expression(
            query, f"from_{format}(to_{format}(col_bytea))", pg_conn
        )

    heap_result = run_query(query, pg_conn)
    fdw_result = run_query(query, pg_conn)

    assert len(heap_result) > 0
    assert heap_result == fdw_result

    pg_conn.rollback()

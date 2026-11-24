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
    "format,encoded,round_trip",
    [
        (
            "base64",
            '"to_base64"("col_bytea")',
            '"from_base64"("to_base64"("col_bytea"))',
        ),
        (
            "hex",
            '"lower"("to_hex"("col_bytea"))',
            '"from_hex"("lower"("to_hex"("col_bytea")))',
        ),
        ("escape", None, None),
    ],
)
def test_encode_decode_pushdown(
    s3,
    pg_conn,
    extension,
    with_default_location,
    format,
    encoded,
    round_trip,
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

    table = "test_encode_decode_function_pushdown"
    for query, expression in [
        (f"SELECT encode(col_bytea, '{format}') FROM {{}} ORDER BY col_bytea", encoded),
        (
            f"SELECT decode(encode(col_bytea, '{format}'), '{format}') FROM {{}} ORDER BY col_bytea",
            round_trip,
        ),
    ]:
        if expression is not None:
            assert_remote_query_contains_expression(
                query.format(table), expression, pg_conn
            )
        else:
            assert_remote_query_not_contains_expression(
                query.format(table), '"encode"', pg_conn
            )

        heap_result = run_query(query.format(f"{table}_heap"), pg_conn)

        assert len(heap_result) > 0
        assert run_query(query.format(table), pg_conn) == heap_result

    pg_conn.rollback()

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
    "format,text,encoded,decoded",
    [
        (
            "base64",
            # Postgres breaks base64 after every 76 characters, and its
            # decode() skips spaces, tabs and line breaks
            "replace(encode(col_bytea, 'base64'), E'\\n', E' \\t\\r\\n')",
            '"base64_encode_pg"("col_bytea")',
            '"base64_decode_pg"(',
        ),
        (
            "hex",
            "regexp_replace(encode(col_bytea, 'hex'), '(..)', '\\1 ', 'g')",
            '"lower"("to_hex"("col_bytea"))',
            '"hex_decode_pg"(',
        ),
        ("escape", "encode(col_bytea, 'escape')", None, None),
    ],
)
def test_encode_decode_pushdown(
    s3,
    pg_conn,
    extension,
    with_default_location,
    format,
    text,
    encoded,
    decoded,
):
    run_command(
        f"""
            CREATE TABLE test_encode_decode_function_pushdown_heap(
                col_bytea bytea, col_text text
            );

            INSERT INTO test_encode_decode_function_pushdown_heap (col_bytea) VALUES (NULL),
            ('\\x0102'), ('\\x0a0b0c0d0e0f'), ('\\xffeeffee'), ('\\x1234567890abcdef'),
            ('abcd'), ('\\012\\345\\067');

            -- every length up to 300 bytes, for each line break and padding
            INSERT INTO test_encode_decode_function_pushdown_heap (col_bytea)
            SELECT substring(decode(repeat(md5('pg_lake'), 20), 'hex') FOR n)
            FROM generate_series(0, 300) n;

            UPDATE test_encode_decode_function_pushdown_heap SET col_text = {text};

            CREATE TABLE test_encode_decode_function_pushdown USING iceberg AS SELECT * FROM test_encode_decode_function_pushdown_heap;
        """,
        pg_conn,
    )

    table = "test_encode_decode_function_pushdown"
    for query, expression in [
        (f"SELECT encode(col_bytea, '{format}') FROM {{}} ORDER BY col_bytea", encoded),
        (
            f"SELECT decode(encode(col_bytea, '{format}'), '{format}') FROM {{}} ORDER BY col_bytea",
            decoded,
        ),
        (f"SELECT decode(col_text, '{format}') FROM {{}} ORDER BY col_bytea", decoded),
    ]:
        if expression is not None:
            assert_remote_query_contains_expression(
                query.format(table), expression, pg_conn
            )
        else:
            assert_remote_query_not_contains_expression(
                query.format(table), '"encode"', pg_conn
            )
            assert_remote_query_not_contains_expression(
                query.format(table), '"decode"', pg_conn
            )

        heap_result = run_query(query.format(f"{table}_heap"), pg_conn)

        assert len(heap_result) > 0
        assert run_query(query.format(table), pg_conn) == heap_result

    pg_conn.rollback()


@pytest.mark.parametrize(
    "format,value",
    [
        # whitespace with part of a group read
        ("base64", "YW Jj"),
        ("base64", "Y\\tW\\nJ\\rj"),
        ("base64", "YQ= ="),
        ("base64", "YW\\nI="),
        # padding Postgres accepts in the middle
        ("base64", "ab=c"),
        ("base64", "YQ==YQ=="),
        ("hex", " 61 \\t62\\n"),
    ],
)
def test_decode_whitespace_and_padding_pushdown(
    s3, pg_conn, extension, with_default_location, format, value
):
    """Pushed down, decode() reads what Postgres reads, wherever the whitespace
    and padding fall."""
    run_command(
        f"""
            CREATE TABLE test_decode_stored_text (col_text text) USING iceberg;
            INSERT INTO test_decode_stored_text VALUES (E'{value}');
        """,
        pg_conn,
    )

    query = f"SELECT decode(col_text, '{format}') FROM test_decode_stored_text"
    explain = run_query(f"EXPLAIN (VERBOSE, FORMAT JSON) {query}", pg_conn)
    assert f'"{format}_decode_pg"(' in fetch_remote_sql(explain)

    expected = run_query(f"SELECT decode(E'{value}', '{format}')", pg_conn)
    assert run_query(query, pg_conn) == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "format,value,error",
    [
        ("base64", "a=bc", 'unexpected "=" while decoding base64 sequence'),
        ("base64", "ab%c", 'invalid symbol "%" found while decoding base64 sequence'),
        ("base64", "abcde", "invalid base64 end sequence"),
        ("hex", "abc", "invalid hexadecimal data: odd number of digits"),
        ("hex", "zz", 'invalid hexadecimal digit: "z"'),
        ("hex", "a b", 'invalid hexadecimal digit: " "'),
        ("hex", "aé", 'invalid hexadecimal digit: "é"'),
    ],
)
def test_decode_invalid_input_pushdown(
    s3, pg_conn, extension, with_default_location, format, value, error
):
    """Pushed down, decode() refuses what Postgres refuses, with its message."""
    heap_error = run_query(
        f"SELECT decode('{value}', '{format}')", pg_conn, raise_error=False
    )
    assert error in str(heap_error)
    pg_conn.rollback()

    run_command(
        f"""
            CREATE TABLE test_decode_invalid_input (col_text text) USING iceberg;
            INSERT INTO test_decode_invalid_input VALUES ('{value}');
        """,
        pg_conn,
    )

    query = f"SELECT decode(col_text, '{format}') FROM test_decode_invalid_input"
    explain = run_query(f"EXPLAIN (VERBOSE, FORMAT JSON) {query}", pg_conn)
    assert f'"{format}_decode_pg"(' in fetch_remote_sql(explain)

    pushed_down_error = run_query(query, pg_conn, raise_error=False)
    assert error in str(pushed_down_error)

    pg_conn.rollback()

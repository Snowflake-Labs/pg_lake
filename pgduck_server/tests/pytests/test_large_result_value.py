import io

import psycopg2
import pytest
from utils_pytest import *

# Result values and rows of 1 GB or more used to make enlargeStringInfo()
# call exit(), taking down pgduck_server for every client. They must now fail
# only the query that produced them.

ONE_GB = 1024 * 1024 * 1024
HALF_GB_PLUS = 600 * 1000 * 1000


def _assert_query_fails(query, transmit=False, binary_types=None):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    try:
        cur = conn.cursor()
        with pytest.raises(psycopg2.Error):
            if binary_types is not None:
                cur.copy_expert(
                    f"transmit binary ({binary_types}) " + query, io.BytesIO()
                )
            elif transmit:
                cur.copy_expert("transmit " + query, io.StringIO())
            else:
                cur.execute(query)
                cur.fetchall()
    finally:
        conn.close()

    # the server must still accept new connections
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


@pytest.mark.parametrize(
    "query",
    [
        # a single value that is too large
        f"SELECT repeat('a', {ONE_GB})",
        # a row whose values together are too large
        f"SELECT repeat('a', {HALF_GB_PLUS}), repeat('b', {HALF_GB_PLUS})",
        # nested values that are too large once combined
        f"SELECT [repeat('a', {HALF_GB_PLUS}), repeat('b', {HALF_GB_PLUS})]",
        # record quoting doubles the size of every quote
        f"SELECT {{'a': repeat('\"', {HALF_GB_PLUS})}}",
    ],
)
def test_large_result_value(pgduck_server, query):
    _assert_query_fails(query)


def test_large_result_value_transmit(pgduck_server):
    # CSV escaping doubles the size of every quote
    _assert_query_fails(f"SELECT repeat('\"', {HALF_GB_PLUS})", transmit=True)


@pytest.mark.parametrize(
    "query, binary_types",
    [
        # 25 is text, 17 is bytea
        (f"SELECT repeat('a', {ONE_GB})", "25"),
        (f"SELECT repeat('a'::blob, {ONE_GB})", "17"),
        (f"SELECT repeat('a', {HALF_GB_PLUS}), repeat('b', {HALF_GB_PLUS})", "25,25"),
    ],
)
def test_large_result_value_transmit_binary(pgduck_server, query, binary_types):
    _assert_query_fails(query, binary_types=binary_types)


# Largest value the per-value check admits in a DataRow (2-byte column count,
# 4-byte length word, terminating NUL); the NULL marker after it must not exit().
MAX_DATAROW_VALUE = 0x3FFFFFFF - 2 - 4 - 1


def test_large_result_row_trailing_null(pgduck_server):
    _assert_query_fails(f"SELECT repeat('a', {MAX_DATAROW_VALUE}), NULL::text")


def test_large_error_message(pgduck_server):
    # the cast error echoes the whole input value
    _assert_query_fails("SELECT repeat('a', 1100000000)::int")


def test_large_result_value_sqlstate(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    try:
        cur = conn.cursor()
        with pytest.raises(psycopg2.Error) as excinfo:
            cur.execute(f"SELECT repeat('a', {ONE_GB})")
            cur.fetchall()
        assert excinfo.value.pgcode == "54000"
        assert "exceeds the maximum message size" in str(excinfo.value)
    finally:
        conn.close()

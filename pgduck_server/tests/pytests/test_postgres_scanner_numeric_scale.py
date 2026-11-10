"""
Verify postgres_scan reads a numeric with a negative scale (PostgreSQL 15+).

numeric(5,-2) holds integers below 10^7 rounded to hundreds. The upstream
scanner decodes the typmod with unsigned arithmetic, so the negative scale
wrapped around and the column was typed DECIMAL(5,254): every value came back
as a number near 1e-248 and turned into 0 once cast to anything usable. The
patch types it DECIMAL(width - scale, 0), or DOUBLE past 38 digits.
"""

from decimal import Decimal

import pytest

from utils_pytest import *


def _connstr():
    return (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )


def _scan(table, schema="public"):
    return f"postgres_scan('{_connstr()}', '{schema}', '{table}')"


@pytest.fixture(scope="module")
def pg_numeric_scale_tables(postgres):
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        "DROP TABLE IF EXISTS scanner_numeric_scale_tbl;"
        "DROP TYPE IF EXISTS scanner_numeric_scale_comp;"
        "DROP DOMAIN IF EXISTS scanner_numeric_scale_dom;"
        "CREATE DOMAIN scanner_numeric_scale_dom AS numeric(4,-3);"
        "CREATE TYPE scanner_numeric_scale_comp AS (n numeric(5,-2));"
        "CREATE TABLE scanner_numeric_scale_tbl ("
        "  id int PRIMARY KEY,"
        "  neg numeric(5,-2),"
        "  wide numeric(36,-2),"
        "  past38 numeric(37,-2),"
        "  small numeric(3,5),"
        "  arr numeric(5,-2)[],"
        "  dom scanner_numeric_scale_dom,"
        "  comp scanner_numeric_scale_comp"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_numeric_scale_tbl VALUES "
        "(1, -9999900, repeat('9', 36)::numeric * 100, 12345600, 0.00123,"
        "    ARRAY[149, -9999900]::numeric(5,-2)[], 9999000, ROW(1234500)),"
        "(2, 0, 0, 0, 0, '{}', 0, ROW(0)),"
        "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    )

    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "DROP TABLE IF EXISTS scanner_numeric_scale_tbl;"
        "DROP TYPE IF EXISTS scanner_numeric_scale_comp;"
        "DROP DOMAIN IF EXISTS scanner_numeric_scale_dom;"
    )
    cur.close()
    conn.close()


def test_negative_scale_types(pg_numeric_scale_tables, pgduck_conn):
    scan = _scan("scanner_numeric_scale_tbl")
    rows = perform_query_on_cursor(
        "SELECT typeof(neg), typeof(wide), typeof(past38), typeof(small), "
        f"typeof(arr), typeof(dom) FROM {scan} WHERE id = 1",
        pgduck_conn,
    )
    assert rows == [
        (
            "DECIMAL(7,0)",
            "DECIMAL(38,0)",
            "DOUBLE",
            "DECIMAL(3,5)",
            "DECIMAL(7,0)[]",
            "DECIMAL(7,0)",
        )
    ]


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_negative_scale_values(pg_numeric_scale_tables, pgduck_conn, use_text_protocol):
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        scan = _scan("scanner_numeric_scale_tbl")
        rows = perform_query_on_cursor(
            "SELECT neg::text, wide::text, past38, arr::text, "
            f"dom::text, comp.n::text FROM {scan} ORDER BY id",
            pgduck_conn,
        )
        assert rows == [
            (
                "-9999900",
                "9" * 36 + "00",
                12345600.0,
                "[100, -9999900]",
                "9999000",
                "1234500",
            ),
            ("0", "0", 0.0, "[]", "0", "0"),
            (None, None, None, None, None, None),
        ]
    finally:
        if use_text_protocol:
            perform_query_on_cursor("RESET pg_use_text_protocol", pgduck_conn)

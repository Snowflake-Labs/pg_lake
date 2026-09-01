"""Wire-protocol coverage for the pgduck_server type map."""

import struct

import pytest
from utils_pytest import *


def test_startup_gucs(pgduck_conn):
    """The two globals duckdb_global_init sets must actually be in effect."""
    rows = run_query(
        "SELECT current_setting('enable_geoparquet_conversion') AS geo, "
        "current_setting('geometry_always_xy') AS xy",
        pgduck_conn,
    )
    # current_setting returns DuckDB BOOLEAN, which psycopg2 maps to bool
    assert rows[0]["geo"] is False
    assert rows[0]["xy"] is False


def test_geometry_column_sent_as_hex_wkb(pgduck_conn):
    """GEOMETRY got a first-class type id in 1.5; it must serialize, not error."""
    rows = run_query("SELECT ST_Point(3, 4) AS g", pgduck_conn)
    val = rows[0]["g"]
    # GEOMETRY is declared BYTEAOID (type_conversion.c) but geometry_to_text
    # writes bare hex with no "\x" prefix, so libpq's bytea decoder yields the
    # ASCII hex characters rather than the WKB bytes. That is the intended
    # contract: pg_lake casts the column to geometry, and PostGIS parses hex
    # (E)WKB. A generic client has to un-hex it itself, as here.
    hex_wkb = bytes(val).decode("ascii")
    assert bytes.fromhex(hex_wkb) == struct.pack("<BIdd", 1, 1, 3.0, 4.0)


@pytest.mark.parametrize(
    "expr,typename",
    [
        ("'101'::BIT", "BIT"),
        ("union_value(a := 1)", "UNION"),
        # VARIANT is 41, one past the `duckType <= DUCKDB_TYPE_GEOMETRY` bound,
        # so it has no TypeInfo row at all. It has to be caught by the bounds
        # check and named from duck_type_error_name's switch. This is the case
        # that breaks first if a later bump adds a type and moves the bound.
        ("'{\"a\": 1}'::VARIANT", "VARIANT"),
    ],
)
def test_unsupported_type_names_type_and_column(pgduck_conn, expr, typename):
    """These have a TypeInfo row with a NULL to_text, so the old code called
    through a NULL function pointer. Now it must be a clean error that names
    the type and the column, and the server must stay up."""
    error = run_command(f"SELECT {expr} AS mycol", pgduck_conn, raise_error=False)
    assert error is not None, f"{expr} should not have succeeded"
    assert typename in error, error
    assert "mycol" in error, error
    pgduck_conn.rollback()

    # server survived and still answers
    assert run_query("SELECT 42 AS answer", pgduck_conn)[0]["answer"] == 42


def test_geometry_typemap_survives_cast(pgduck_conn):
    """A GEOMETRY produced through the spatial functions round-trips as WKB."""
    rows = run_query(
        "SELECT ST_AsText(ST_GeomFromWKB(ST_AsWKB(ST_Point(1, 2)))) AS wkt", pgduck_conn
    )
    assert rows[0]["wkt"] == "POINT (1 2)"

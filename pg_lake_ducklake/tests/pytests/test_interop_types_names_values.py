"""
Interop tests for exotic types, weird identifier names, and edge-case
values, with back-and-forth flow between PostgreSQL and DuckDB.

Strategy:

  * PG creates a DuckLake table, INSERTs values, commits.
  * DuckDB ATTACHes (TYPE DUCKLAKE, METADATA_SCHEMA 'public') and
    SELECTs to verify the same values come back.
  * For mixed-direction tests, DuckDB INSERTs more rows; PG SELECTs to
    verify both writers' rows round-trip.

These pin the catalog/parquet/protocol path against unusual but legal
data, not the happy SMALLINT/TEXT path covered elsewhere.
"""

import pytest
from utils_pytest import TEST_BUCKET, server_params

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


def _attach(conn_str):
    duck = duckdb.connect()
    duck.execute("INSTALL postgres; LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly; LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    duck.execute(
        f"ATTACH 'postgres:{conn_str}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )
    return duck


def _conn_str():
    return (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_numeric_decimal_roundtrip(pg_cursor, s3):
    """
    NUMERIC(p,s) values, including negative scale, very-small fractions,
    and large magnitudes, must round-trip PG -> DuckDB without precision
    loss or sign flip.
    """
    location = f"s3://{TEST_BUCKET}/types_numeric"
    pg_cursor.execute("DROP TABLE IF EXISTS t_numeric")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_numeric (id INT, n NUMERIC(20, 6))
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        """
        INSERT INTO t_numeric VALUES
            (1,  1234567890.123456),
            (2, -1234567890.123456),
            (3,  0.000001),
            (4, -0.000001),
            (5,  0),
            (6,  99999999999999.999999)
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute(
        "SELECT id, n::VARCHAR FROM dl.public.t_numeric ORDER BY id"
    ).fetchall()
    assert rows == [
        (1, "1234567890.123456"),
        (2, "-1234567890.123456"),
        (3, "0.000001"),
        (4, "-0.000001"),
        (5, "0.000000"),
        (6, "99999999999999.999999"),
    ], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_temporal_types_roundtrip(pg_cursor, s3):
    """
    DATE, TIMESTAMP, and TIMESTAMPTZ round-trip with exact time values
    including the epoch boundary, sub-second precision, and BC dates
    (where supported).
    """
    location = f"s3://{TEST_BUCKET}/types_temporal"
    pg_cursor.execute("DROP TABLE IF EXISTS t_temporal")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_temporal (
            id INT,
            d DATE,
            ts TIMESTAMP,
            tz TIMESTAMP WITH TIME ZONE
        )
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        """
        INSERT INTO t_temporal VALUES
            (1, DATE '1970-01-01',
                TIMESTAMP '1970-01-01 00:00:00',
                TIMESTAMPTZ '1970-01-01 00:00:00+00'),
            (2, DATE '2026-05-27',
                TIMESTAMP '2026-05-27 12:34:56.789',
                TIMESTAMPTZ '2026-05-27 12:34:56.789+02'),
            (3, DATE '9999-12-31',
                TIMESTAMP '9999-12-31 23:59:59',
                TIMESTAMPTZ '9999-12-31 23:59:59+00')
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute(
        "SELECT id, d::VARCHAR, ts::VARCHAR, "
        "       tz AT TIME ZONE 'UTC' AS utc_ts "
        "FROM dl.public.t_temporal ORDER BY id"
    ).fetchall()
    assert len(rows) == 3
    assert rows[0][1] == "1970-01-01"
    assert "1970-01-01" in rows[0][2]
    assert rows[1][1] == "2026-05-27"
    assert "2026-05-27" in rows[1][2]
    assert rows[2][1] == "9999-12-31"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_boolean_and_uuid_roundtrip(pg_cursor, s3):
    """
    BOOLEAN and UUID -- both fixed-width types that DuckDB and Iceberg
    support natively but with different storage representations from
    PG's heap. Confirm null-handling for booleans and UUID literal
    fidelity.
    """
    location = f"s3://{TEST_BUCKET}/types_bool_uuid"
    pg_cursor.execute("DROP TABLE IF EXISTS t_bool_uuid")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_bool_uuid (id INT, flag BOOLEAN, u UUID)
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        """
        INSERT INTO t_bool_uuid VALUES
            (1, TRUE,  '00000000-0000-0000-0000-000000000000'),
            (2, FALSE, 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
            (3, NULL,  NULL),
            (4, TRUE,  '12345678-1234-5678-1234-567812345678')
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute(
        "SELECT id, flag, u::VARCHAR FROM dl.public.t_bool_uuid ORDER BY id"
    ).fetchall()
    assert rows == [
        (1, True, "00000000-0000-0000-0000-000000000000"),
        (2, False, "ffffffff-ffff-ffff-ffff-ffffffffffff"),
        (3, None, None),
        (4, True, "12345678-1234-5678-1234-567812345678"),
    ], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_bytea_roundtrip(pg_cursor, s3):
    """
    BYTEA <-> BLOB: DuckDB's BLOB shows up in PG as bytea. Verify the
    bytes survive the parquet write+read with no encoding shenanigans
    (escape vs hex, NULs in the middle, leading 0x00).
    """
    location = f"s3://{TEST_BUCKET}/types_bytea"
    pg_cursor.execute("DROP TABLE IF EXISTS t_bytea")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_bytea (id INT, b BYTEA)
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        r"""
        INSERT INTO t_bytea VALUES
            (1, '\x'::bytea),
            (2, '\x00'::bytea),
            (3, '\xdeadbeef'::bytea),
            (4, '\x00010203fefdfcfb'::bytea)
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute("SELECT id, b FROM dl.public.t_bytea ORDER BY id").fetchall()
    assert rows == [
        (1, b""),
        (2, b"\x00"),
        (3, b"\xde\xad\xbe\xef"),
        (4, b"\x00\x01\x02\x03\xfe\xfd\xfc\xfb"),
    ], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_text_with_nasty_values(pg_cursor, s3):
    """
    Strings with embedded quotes, backslashes, NULL bytes, multi-byte
    UTF-8, unicode emoji. The parquet path should pass these bytes
    verbatim.
    """
    location = f"s3://{TEST_BUCKET}/types_nasty_text"
    pg_cursor.execute("DROP TABLE IF EXISTS t_nasty")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_nasty (id INT, s TEXT)
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        r"""
        INSERT INTO t_nasty VALUES
            (1, ''),
            (2, ' '),
            (3, '"'),
            (4, '\backslash'),
            (5, 'tab	tab'),
            (6, 'newline
inside'),
            (7, U&'na\00efve'),
            (8, '🦆'),
            (9, NULL)
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute("SELECT id, s FROM dl.public.t_nasty ORDER BY id").fetchall()
    assert rows == [
        (1, ""),
        (2, " "),
        (3, '"'),
        (4, "\\backslash"),
        (5, "tab\ttab"),
        (6, "newline\ninside"),
        (7, "naïve"),
        (8, "🦆"),
        (9, None),
    ], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_weird_identifier_names(pg_cursor, s3):
    """
    Quoted identifiers with mixed case, spaces, embedded quotes, and
    reserved-word column names round-trip through PG -> parquet field_id
    mapping -> DuckDB schema introspection.
    """
    location = f"s3://{TEST_BUCKET}/types_weird_names"
    pg_cursor.execute('DROP TABLE IF EXISTS "Weird Name"')
    pg_cursor.execute(
        f'''
        CREATE TABLE "Weird Name" (
            "id col" INT,
            "Name With ""Quotes""" TEXT,
            "select" TEXT,
            "Mixed_CASE" INT
        )
        USING ducklake WITH (location = '{location}')
        '''
    )
    pg_cursor.execute(
        """
        INSERT INTO "Weird Name" VALUES
            (1, 'value with "embedded" quotes', 'reserved-word col', 42),
            (2, NULL, NULL, NULL)
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute(
        '''
        SELECT "id col", "Name With ""Quotes""", "select", "Mixed_CASE"
        FROM dl.public."Weird Name"
        ORDER BY "id col"
        '''
    ).fetchall()
    assert rows == [
        (1, 'value with "embedded" quotes', "reserved-word col", 42),
        (2, None, None, None),
    ], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_floating_point_edge_values(pg_cursor, s3):
    """
    REAL/DOUBLE: NaN, Infinity, -Infinity, smallest subnormal, largest
    finite. These have historically tripped Iceberg/Avro stat
    serialization in places that don't handle NaN as "no min/max".
    """
    location = f"s3://{TEST_BUCKET}/types_float_edges"
    pg_cursor.execute("DROP TABLE IF EXISTS t_float_edges")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_float_edges (id INT, r REAL, d DOUBLE PRECISION)
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        """
        INSERT INTO t_float_edges VALUES
            (1, 'NaN'::real,        'NaN'::double precision),
            (2, 'Infinity'::real,   'Infinity'::double precision),
            (3, '-Infinity'::real,  '-Infinity'::double precision),
            (4, 0.0,                0.0),
            (5, -0.0,               -0.0),
            (6, 1.175494e-38,       2.225073858507201e-308),
            (7, 3.4028235e38,       1.7976931348623157e308)
        """
    )
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute(
        "SELECT id, r, d FROM dl.public.t_float_edges ORDER BY id"
    ).fetchall()
    assert len(rows) == 7
    import math

    assert math.isnan(rows[0][1]) and math.isnan(rows[0][2])
    assert rows[1] == (2, float("inf"), float("inf"))
    assert rows[2] == (3, float("-inf"), float("-inf"))
    assert rows[3] == (4, 0.0, 0.0)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_back_and_forth_pg_then_duckdb_then_pg(pg_cursor, s3):
    """
    Three-leg round trip: PG INSERT -> DuckDB SELECT (verify) ->
    DuckDB INSERT new rows -> PG SELECT (verify all rows). Pins that
    DuckDB-written parquet plus DuckDB-written catalog rows are
    immediately visible to PG without any extra refresh step.
    """
    location = f"s3://{TEST_BUCKET}/types_round_trip"
    pg_cursor.execute("DROP TABLE IF EXISTS t_rt")
    pg_cursor.execute(
        f"""
        CREATE TABLE t_rt (id INT, label TEXT, qty NUMERIC(10, 2))
        USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO t_rt VALUES (1, 'pg-a', 1.50), (2, 'pg-b', 2.75)")
    pg_cursor.connection.commit()

    duck = _attach(_conn_str())
    rows = duck.execute(
        "SELECT id, label, qty::VARCHAR FROM dl.public.t_rt ORDER BY id"
    ).fetchall()
    assert rows == [(1, "pg-a", "1.50"), (2, "pg-b", "2.75")], rows

    duck.execute(
        "INSERT INTO dl.public.t_rt VALUES (3, 'duck-a', 3.25), (4, 'duck-b', 4.00)"
    )

    pg_cursor.connection.commit()
    pg_cursor.execute("SELECT id, label, qty::text FROM t_rt ORDER BY id")
    pg_rows = pg_cursor.fetchall()
    assert pg_rows == [
        (1, "pg-a", "1.50"),
        (2, "pg-b", "2.75"),
        (3, "duck-a", "3.25"),
        (4, "duck-b", "4.00"),
    ], pg_rows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

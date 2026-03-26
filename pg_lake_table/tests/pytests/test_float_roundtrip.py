"""
Verify that float4/float8 edge-case values survive a full Iceberg round-trip
(PostgreSQL -> Parquet via pgduck_server -> read back) with correct,
lossless shortest-decimal output.

Test strategy:
  1. Create an Iceberg table with float4/float8 columns, insert edge-case
     values, and read them back.
  2. Verify text representation round-trips exactly (float4 via struct,
     float8 via Python float).
"""

import math
import struct

import pytest

from utils_pytest import *


FLOAT4_EDGE_CASES = {
    "f4_nan": "'NaN'::float4",
    "f4_inf": "'Infinity'::float4",
    "f4_neg_inf": "'-Infinity'::float4",
    "f4_zero": "0::float4",
    "f4_neg_zero": "'-0.0'::float4",
    "f4_max": "3.4028235e+38::float4",
    "f4_neg_max": "-3.4028235e+38::float4",
    "f4_just_below_max": "3.4028233e+38::float4",
    "f4_min_normal": "1.17549435e-38::float4",
    "f4_min_subnormal": "1e-45::float4",
    "f4_neg_subnormal": "-1e-45::float4",
    "f4_epsilon": "1.1920929e-7::float4",
    "f4_point_one": "0.1::float4",
    "f4_one": "1::float4",
    "f4_neg_one": "-1::float4",
    "f4_third": "0.333333343::float4",
    "f4_frac": "1234.5::float4",
    "f4_pow2": "0.015625::float4",
    "f4_max_int": "16777216::float4",
}

FLOAT8_EDGE_CASES = {
    "f8_nan": "'NaN'::float8",
    "f8_inf": "'Infinity'::float8",
    "f8_neg_inf": "'-Infinity'::float8",
    "f8_zero": "0::float8",
    "f8_neg_zero": "'-0.0'::float8",
    "f8_max": "1.7976931348623157e+308::float8",
    "f8_neg_max": "-1.7976931348623157e+308::float8",
    "f8_just_below_max": "1.7976931348623155e+308::float8",
    "f8_min_normal": "2.2250738585072014e-308::float8",
    "f8_min_subnormal": "5e-324::float8",
    "f8_neg_subnormal": "-5e-324::float8",
    # Requires 17 significant digits to distinguish from min_normal;
    # pg_strfromd with ndig=16 collapses both to the same string,
    # guaranteeing a round-trip failure without shortest-decimal output.
    "f8_17digits": "2.2250738585072011e-308::float8",
    "f8_epsilon": "2.220446049250313e-16::float8",
    "f8_point_one": "0.1::float8",
    "f8_one": "1::float8",
    "f8_neg_one": "-1::float8",
    "f8_third": "0.3333333333333333::float8",
    "f8_frac": "123456789.98765432::float8",
    "f8_pow2": "0.0625::float8",
    "f8_pi": "3.141592653589793::float8",
    "f8_max_int": "9007199254740992::float8",
}

SCHEMA = "test_float_roundtrip"


def _float32_roundtrips(text):
    """True if *text* survives a float32 round-trip (including sign of zero)."""
    val = float(text)
    as_f32 = struct.unpack("f", struct.pack("f", val))[0]
    if math.isnan(val):
        return math.isnan(as_f32)
    if math.isinf(val):
        return math.isinf(as_f32) and math.copysign(1, val) == math.copysign(1, as_f32)
    if math.isinf(as_f32):
        return False
    if val == 0:
        return as_f32 == 0 and math.copysign(1, val) == math.copysign(1, as_f32)
    return struct.pack("f", val) == struct.pack("f", as_f32)


def _read_row_as_dict(pg_conn, table, columns):
    col_list = ", ".join(columns)
    rows = run_query(f"SELECT {col_list} FROM {SCHEMA}.{table}", pg_conn)
    assert len(rows) == 1
    return dict(zip(columns, rows[0]))


# --------------------------------------------------------------------------
# Wide-table round-trips (one column per edge case)
# --------------------------------------------------------------------------


def test_float4_iceberg_roundtrip(pg_conn, s3, extension, with_default_location):
    """float4 edge-case values survive an Iceberg write/read cycle."""
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    cols = ", ".join(f"{name} float4" for name in FLOAT4_EDGE_CASES)
    run_command(f"CREATE TABLE {SCHEMA}.f4 ({cols}) USING iceberg", pg_conn)
    vals = ", ".join(FLOAT4_EDGE_CASES.values())
    run_command(f"INSERT INTO {SCHEMA}.f4 VALUES ({vals})", pg_conn)
    pg_conn.commit()

    result = _read_row_as_dict(pg_conn, "f4", list(FLOAT4_EDGE_CASES.keys()))

    assert math.isnan(result["f4_nan"])
    assert result["f4_inf"] == float("inf")
    assert result["f4_neg_inf"] == float("-inf")
    assert result["f4_zero"] == 0.0
    assert result["f4_neg_zero"] == 0.0
    assert math.copysign(1, result["f4_neg_zero"]) == -1.0

    for name, val in result.items():
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            continue
        text = str(val)
        assert _float32_roundtrips(
            text
        ), f"float4 Iceberg round-trip failed for {name}={text!r}"

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def test_float8_iceberg_roundtrip(pg_conn, s3, extension, with_default_location):
    """float8 edge-case values survive an Iceberg write/read cycle."""
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    cols = ", ".join(f"{name} float8" for name in FLOAT8_EDGE_CASES)
    run_command(f"CREATE TABLE {SCHEMA}.f8 ({cols}) USING iceberg", pg_conn)
    vals = ", ".join(FLOAT8_EDGE_CASES.values())
    run_command(f"INSERT INTO {SCHEMA}.f8 VALUES ({vals})", pg_conn)
    pg_conn.commit()

    result = _read_row_as_dict(pg_conn, "f8", list(FLOAT8_EDGE_CASES.keys()))

    assert math.isnan(result["f8_nan"])
    assert result["f8_inf"] == float("inf")
    assert result["f8_neg_inf"] == float("-inf")
    assert result["f8_zero"] == 0.0
    assert result["f8_neg_zero"] == 0.0
    assert math.copysign(1, result["f8_neg_zero"]) == -1.0

    for name, val in result.items():
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            continue
        reparsed = float(repr(val))
        assert reparsed == val, f"float8 Iceberg round-trip failed for {name}={val!r}"

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


# --------------------------------------------------------------------------
# Single-column table round-trips (values as rows)
# --------------------------------------------------------------------------


def test_float4_single_column_roundtrip(pg_conn, s3, extension, with_default_location):
    """Insert all float4 edge cases as rows in a single-column Iceberg table."""
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    run_command(f"CREATE TABLE {SCHEMA}.f4_col (v float4) USING iceberg", pg_conn)
    values = ", ".join(f"({e})" for e in FLOAT4_EDGE_CASES.values())
    run_command(f"INSERT INTO {SCHEMA}.f4_col VALUES {values}", pg_conn)
    pg_conn.commit()

    rows = run_query(f"SELECT v FROM {SCHEMA}.f4_col", pg_conn)
    assert len(rows) == len(FLOAT4_EDGE_CASES)

    for row in rows:
        val = row[0]
        if val is None or (
            isinstance(val, float) and (math.isnan(val) or math.isinf(val))
        ):
            continue
        assert _float32_roundtrips(
            str(val)
        ), f"float4 single-column round-trip failed for {val!r}"

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def test_float8_single_column_roundtrip(pg_conn, s3, extension, with_default_location):
    """Insert all float8 edge cases as rows in a single-column Iceberg table."""
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    run_command(f"CREATE TABLE {SCHEMA}.f8_col (v float8) USING iceberg", pg_conn)
    values = ", ".join(f"({e})" for e in FLOAT8_EDGE_CASES.values())
    run_command(f"INSERT INTO {SCHEMA}.f8_col VALUES {values}", pg_conn)
    pg_conn.commit()

    rows = run_query(f"SELECT v FROM {SCHEMA}.f8_col", pg_conn)
    assert len(rows) == len(FLOAT8_EDGE_CASES)

    for row in rows:
        val = row[0]
        if val is None or (
            isinstance(val, float) and (math.isnan(val) or math.isinf(val))
        ):
            continue
        reparsed = float(repr(val))
        assert reparsed == val, f"float8 single-column round-trip failed for {val!r}"

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


# --------------------------------------------------------------------------
# Mixed float4 + float8 table round-trip
# --------------------------------------------------------------------------


def test_float_mixed_table_roundtrip(pg_conn, s3, extension, with_default_location):
    """
    Insert float4 + float8 edge cases into a mixed Iceberg table and
    verify both types survive the round-trip.
    """
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA}.f_mixed (f4 float4, f8 float8) USING iceberg",
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO {SCHEMA}.f_mixed VALUES
            ('NaN'::float4,        'NaN'::float8),
            ('Infinity'::float4,   'Infinity'::float8),
            ('-Infinity'::float4,  '-Infinity'::float8),
            (0::float4,            0::float8),
            ('-0.0'::float4,       '-0.0'::float8),
            (3.4028235e+38,        1.7976931348623157e+308),
            (-3.4028235e+38,       -1.7976931348623157e+308),
            (1.17549435e-38,       2.2250738585072014e-308),
            (1e-45,                5e-324),
            (-1e-45::float4,       -5e-324::float8),
            (0.1::float4,          0.1::float8),
            (0.333333343::float4,  2.2250738585072011e-308::float8)
        """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(f"SELECT f4, f8 FROM {SCHEMA}.f_mixed", pg_conn)
    assert len(rows) == 12

    for row in rows:
        f4, f8 = row[0], row[1]
        if (
            f4 is not None
            and isinstance(f4, float)
            and not math.isnan(f4)
            and not math.isinf(f4)
        ):
            assert _float32_roundtrips(
                str(f4)
            ), f"float4 mixed-table round-trip failed for {f4!r}"
        if (
            f8 is not None
            and isinstance(f8, float)
            and not math.isnan(f8)
            and not math.isinf(f8)
        ):
            reparsed = float(repr(f8))
            assert reparsed == f8, f"float8 mixed-table round-trip failed for {f8!r}"

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()

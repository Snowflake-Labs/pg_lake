"""Data file pruning on the column bounds in Iceberg manifests.

Tables pg_lake did not write itself are pruned on the lower and upper bounds of
their manifest entries. Most cases here build the manifest by hand, so a bound
can be exactly what another writer records: one-sided, truncated, malformed, out
of the Postgres range, from before a type promotion, or NaN-blind. Two read
tables written by pg_lake and by Spark.

A file wrongly skipped loses rows, so every case asserts on the rows returned as
well as on the files scanned.
"""

import struct
import uuid
from datetime import date, datetime, time as daytime, timezone
from decimal import Decimal

import pytest
from helpers.spark import create_spark_catalog_database, spark_session
from test_iceberg_equality_delete import DeleteTable, attach, delete_table, int_bound
from utils_pytest import fetch_data_files_used, run_command, run_query


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


def long_bound(value):
    return struct.pack("<q", value)


def float_bound(value):
    return struct.pack("<f", value)


def double_bound(value):
    return struct.pack("<d", value)


def decimal_bound(unscaled, length=16):
    """A decimal bound: the unscaled value as big-endian two's complement."""
    return unscaled.to_bytes(length, "big", signed=True)


def scanned_files(pg_conn, where, table="equality_test"):
    """(data files scanned, rows returned) for a filter, with and without pushdown.

    The rows are checked against a read behind OFFSET 0, which keeps the filter
    out of the scan, so a wrongly pruned file fails here even when the expected
    count in the test is wrong too.
    """
    results = set()
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}", pg_conn
        )
        plan = run_query(
            f"EXPLAIN (analyze, verbose, format json) SELECT * FROM {table} WHERE {where}",
            pg_conn,
        )
        count = run_query(f"SELECT count(*) FROM {table} WHERE {where}", pg_conn)[0][0]
        unpruned = run_query(
            f"SELECT count(*) FROM (SELECT * FROM {table} OFFSET 0) t WHERE {where}",
            pg_conn,
        )[0][0]
        assert count == unpruned, f"{where}: {count} rows, {unpruned} unpruned"
        results.add((fetch_data_files_used(plan), count))
    assert len(results) == 1
    return results.pop()


def one_column_table(tmp_path, s3, iceberg_type, rows, lower, upper, nan_count=None):
    table = DeleteTable(tmp_path, s3, fields=[(1, "id", "int"), (2, "v", iceberg_type)])
    table.add(
        rows,
        lower_bounds={2: lower},
        upper_bounds={2: upper},
        nan_value_counts=None if nan_count is None else {2: nan_count},
    )
    return table


@pytest.mark.parametrize(
    "bounds,where,expected",
    [
        # Only one bound, as when a writer cannot increment a truncated string
        # upper bound. Rare enough that the column is simply not pruned on.
        ("lower", "id < 10", ("1", 0)),
        ("lower", "id > 15", ("1", 1)),
        ("upper", "id > 20", ("1", 0)),
        ("upper", "id < 15", ("1", 1)),
        # both: the file holds 10 and 20
        ("both", "id > 20", ("0", 0)),
        ("both", "id < 10", ("0", 0)),
        ("both", "id = 15", ("1", 0)),
        ("both", "id BETWEEN 11 AND 19", ("1", 0)),
        ("both", "id IN (10, 20)", ("1", 2)),
        ("both", "id IS NULL", ("1", 0)),
    ],
)
def test_one_sided_bounds(
    s3, pg_conn, extension, delete_table, bounds, where, expected
):
    """A column is pruned on only when the manifest has both of its bounds."""
    delete_table.add(
        [(10, "x", "row", 1), (20, "y", "row", 2)],
        lower_bounds={1: int_bound(10)} if bounds in ("lower", "both") else None,
        upper_bounds={1: int_bound(20)} if bounds in ("upper", "both") else None,
    )
    attach(pg_conn, delete_table)
    assert scanned_files(pg_conn, where) == expected


@pytest.mark.parametrize(
    "lower,upper",
    [
        # 3 bytes cannot be an int
        (b"\x0a\x00\x00", int_bound(20)),
        (int_bound(10), b"\x14\x00\x00\x00\x00"),
        (b"", b""),
    ],
)
def test_malformed_bounds_are_ignored(
    s3, pg_conn, extension, delete_table, lower, upper
):
    """A bound we cannot decode makes the column unprunable, never an error."""
    delete_table.add(
        [(10, "x", "row", 1), (20, "y", "row", 2)],
        lower_bounds={1: lower},
        upper_bounds={1: upper},
    )
    attach(pg_conn, delete_table)
    assert scanned_files(pg_conn, "id < 5") == ("1", 0)
    assert scanned_files(pg_conn, "id > 25") == ("1", 0)
    assert scanned_files(pg_conn, "id = 20") == ("1", 1)


@pytest.mark.parametrize(
    "iceberg_type,length",
    [
        ("boolean", 2),
        ("float", 8),
        ("double", 2),
        ("long", 2),
        ("date", 8),
        ("time", 4),
        ("timestamp", 4),
        ("timestamptz", 4),
        ("uuid", 15),
        ("decimal(9,2)", 17),
    ],
)
def test_wrong_bound_length_per_type(
    tmp_path, s3, pg_conn, extension, iceberg_type, length
):
    """Every fixed-width type rejects a bound of the wrong length."""
    values = {
        "boolean": True,
        "float": 1.5,
        "double": 1.5,
        "long": 7,
        "date": date(2026, 1, 1),
        "time": daytime(12, 0),
        "timestamp": datetime(2026, 1, 1),
        "timestamptz": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "uuid": uuid.UUID(int=1).bytes,
        "decimal(9,2)": Decimal("1.50"),
    }
    bad = b"\x01" * length
    table = one_column_table(
        tmp_path, s3, iceberg_type, [(1, values[iceberg_type])], bad, bad, 0
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v IS NOT NULL") == ("1", 1)
    assert scanned_files(pg_conn, "id = 1") == ("1", 1)


@pytest.mark.parametrize(
    "iceberg_type,value,lower,upper",
    [
        # Iceberg timestamps reach further back than Postgres ones; the earliest
        # wrap around to a valid far-future timestamp when moved to the Postgres
        # epoch, which made the lower bound larger than the file's values.
        (
            "timestamp",
            datetime(2026, 1, 1),
            long_bound(-(2**63)),
            long_bound(2**63 - 1),
        ),
        (
            "timestamp",
            datetime(2026, 1, 1),
            long_bound(-(2**62)),
            long_bound(2**62),
        ),
        ("date", date(2026, 1, 1), int_bound(-(2**31)), int_bound(2**31 - 1)),
        # a time of day is at most 24:00
        ("time", daytime(12, 0), long_bound(-1), long_bound(86_400_000_001)),
    ],
)
def test_out_of_range_bounds(
    tmp_path, s3, pg_conn, extension, iceberg_type, value, lower, upper
):
    """A bound Postgres cannot represent is skipped instead of failing the query."""
    table = one_column_table(tmp_path, s3, iceberg_type, [(1, value)], lower, upper)
    attach(pg_conn, table)
    assert scanned_files(pg_conn, f"v = '{value}'") == ("1", 1)
    assert scanned_files(pg_conn, f"v < '{value}'") == ("1", 0)
    assert scanned_files(pg_conn, f"v > '{value}'") == ("1", 0)


@pytest.mark.parametrize(
    "iceberg_type,lower,upper,where,expected",
    [
        # 16 bytes of 0x01 do not fit decimal(9,2): unprunable, not an error
        ("decimal(9,2)", b"\x01" * 16, b"\x01" * 16, "v > 5", ("1", 0)),
        ("decimal(9,2)", b"\x01" * 16, b"\x01" * 16, "v = 1.50", ("1", 1)),
        ("decimal(9,2)", decimal_bound(1), b"\x01" * 16, "v < 0", ("1", 0)),
        # all 0xFF is -0.01, sign-extended from any length
        ("decimal(9,2)", b"\xff" * 16, decimal_bound(200), "v < -1", ("0", 0)),
        ("decimal(9,2)", b"\xff", decimal_bound(200), "v > 2", ("0", 0)),
        ("decimal(9,2)", b"\xff", decimal_bound(200), "v = 1.50", ("1", 1)),
        # minimal-length negative bound: 0x80 is -1.28
        ("decimal(9,2)", b"\x80", b"\x00\x96", "v < -1.28", ("0", 0)),
        ("decimal(9,2)", b"\x80", b"\x00\x96", "v > 1.50", ("0", 0)),
        ("decimal(9,2)", b"\x80", b"\x00\x96", "v = 1.50", ("1", 1)),
        # the extremes of decimal(38,2)
        (
            "decimal(38,2)",
            decimal_bound(-(10**38 - 1)),
            decimal_bound(10**38 - 1),
            "v > CAST(1e30 AS numeric(38, 2))",
            ("1", 0),
        ),
        (
            "decimal(38,2)",
            decimal_bound(-(10**38 - 1)),
            decimal_bound(10**38 - 1),
            "v = 1.50",
            ("1", 1),
        ),
        ("decimal(38,2)", decimal_bound(-150), decimal_bound(150), "v > 2", ("0", 0)),
        # past the precision of decimal(38,2)
        (
            "decimal(38,2)",
            decimal_bound(-(10**38)),
            decimal_bound(150),
            "v < 0",
            ("1", 0),
        ),
    ],
)
def test_decimal_bounds(
    tmp_path, s3, pg_conn, extension, iceberg_type, lower, upper, where, expected
):
    """
    Decimal bounds are two's complement of any length up to 16 bytes. One that
    does not fit the precision of the column is skipped instead of failing the
    query.
    """
    table = one_column_table(
        tmp_path, s3, iceberg_type, [(1, Decimal("1.50"))], lower, upper
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, where) == expected


@pytest.mark.parametrize("nan_count", [None, 1])
def test_nan_disables_float_pruning(tmp_path, s3, pg_conn, extension, nan_count):
    """
    Iceberg bounds leave out NaN, while Postgres orders NaN above every value.
    Unless the writer records that a file has no NaN, its float bounds say
    nothing about it.
    """
    table = one_column_table(
        tmp_path,
        s3,
        "double",
        [(1, 1.0), (2, float("nan"))],
        double_bound(1.0),
        double_bound(1.0),
        nan_count,
    )
    attach(pg_conn, table)
    # Only the file count for filters NaN satisfies: the reader also filters
    # row groups on the Parquet statistics, which leave out NaN as well, and
    # that is a separate path from manifest pruning.
    assert fetch_files(pg_conn, "v > 5") == "1"
    assert fetch_files(pg_conn, "v = 'NaN'") == "1"
    assert fetch_files(pg_conn, "v NOT IN (1)") == "1"
    assert scanned_files(pg_conn, "v < 0") == ("1", 0)


def fetch_files(pg_conn, where, table="equality_test"):
    plan = run_query(
        f"EXPLAIN (analyze, verbose, format json) SELECT * FROM {table} WHERE {where}",
        pg_conn,
    )
    return fetch_data_files_used(plan)


@pytest.mark.parametrize(
    "iceberg_type,bound", [("double", double_bound), ("float", float_bound)]
)
def test_float_bounds_without_nan(
    tmp_path, s3, pg_conn, extension, iceberg_type, bound
):
    table = one_column_table(
        tmp_path, s3, iceberg_type, [(1, 1.0), (2, 2.0)], bound(1.0), bound(2.0), 0
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v > 5") == ("0", 0)
    assert scanned_files(pg_conn, "v < 0") == ("0", 0)
    assert scanned_files(pg_conn, "v = 'NaN'") == ("0", 0)
    assert scanned_files(pg_conn, "v = 2") == ("1", 1)


def test_float_infinities_and_signed_zero(tmp_path, s3, pg_conn, extension):
    (tmp_path / "inf").mkdir()
    (tmp_path / "zero").mkdir()
    infinite = one_column_table(
        tmp_path / "inf",
        s3,
        "double",
        [(1, float("-inf")), (2, float("inf"))],
        double_bound(float("-inf")),
        double_bound(float("inf")),
        0,
    )
    attach(pg_conn, infinite)
    assert scanned_files(pg_conn, "v = 'Infinity'") == ("1", 1)
    assert scanned_files(pg_conn, "v = '-Infinity'") == ("1", 1)
    assert scanned_files(pg_conn, "v > 1e12") == ("1", 1)
    pg_conn.rollback()

    # Iceberg orders -0.0 before 0.0; Postgres treats them as equal
    zero = one_column_table(
        tmp_path / "zero",
        s3,
        "double",
        [(1, -0.0)],
        double_bound(-0.0),
        double_bound(-0.0),
        0,
    )
    attach(pg_conn, zero)
    assert scanned_files(pg_conn, "v = 0") == ("1", 1)
    assert scanned_files(pg_conn, "v = '-0'") == ("1", 1)
    assert scanned_files(pg_conn, "v > 0") == ("0", 0)
    assert scanned_files(pg_conn, "v < 0") == ("0", 0)


@pytest.mark.parametrize("extra_float_digits", [-15, 0, 3])
@pytest.mark.parametrize(
    "iceberg_type,value,bound",
    [
        ("double", 0.1234567890123, double_bound),
        # a float4 that needs all 9 significant digits
        ("float", struct.unpack("<f", struct.pack("<f", 0.123456789))[0], float_bound),
    ],
)
def test_float_bounds_are_exact(
    tmp_path, s3, pg_conn, extension, extra_float_digits, iceberg_type, value, bound
):
    """
    Bounds are compared as text internally. They must keep every digit, or a
    rounded bound would exclude the value it was taken from.
    """
    table = one_column_table(
        tmp_path, s3, iceberg_type, [(1, value)], bound(value), bound(value), 0
    )
    attach(pg_conn, table)
    run_command(f"SET LOCAL extra_float_digits = {extra_float_digits}", pg_conn)
    cast = "real" if iceberg_type == "float" else "double precision"
    # Only the pruning decision: with extra_float_digits below zero, full
    # pushdown of this filter loses the row with pruning off as well.
    assert fetch_files(pg_conn, f"v = '{value!r}'::{cast}") == "1"


def test_boolean_bounds(tmp_path, s3, pg_conn, extension):
    table = one_column_table(
        tmp_path, s3, "boolean", [(1, False), (2, False)], b"\x00", b"\x00"
    )
    attach(pg_conn, table)
    # Postgres simplifies v = true to v, which the bound constraint v = false
    # does not refute, so boolean filters read the file; they must still be
    # right.
    assert scanned_files(pg_conn, "v = true") == ("1", 0)
    assert scanned_files(pg_conn, "v = false") == ("1", 2)
    assert scanned_files(pg_conn, "v IS NULL") == ("1", 0)


def test_time_bounds(tmp_path, s3, pg_conn, extension):
    micros = lambda t: ((t.hour * 60 + t.minute) * 60 + t.second) * 1_000_000
    low, high = daytime(9, 0), daytime(17, 30)
    table = one_column_table(
        tmp_path,
        s3,
        "time",
        [(1, low), (2, high)],
        long_bound(micros(low)),
        long_bound(micros(high)),
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v < '08:00'") == ("0", 0)
    assert scanned_files(pg_conn, "v > '18:00'") == ("0", 0)
    assert scanned_files(pg_conn, "v = '17:30'") == ("1", 1)
    assert scanned_files(pg_conn, "v BETWEEN '10:00' AND '11:00'") == ("1", 0)


def test_truncated_string_bounds(tmp_path, s3, pg_conn, extension):
    """
    Writers truncate string bounds: the lower bound to a prefix, the upper bound
    to a prefix with its last character incremented. Both still enclose the
    values.
    """
    table = one_column_table(
        tmp_path,
        s3,
        "string",
        [(1, "abcdefghijklmnopqrstu"), (2, "abczzzzzzzzzzzzzzzzzz")],
        b"abc",
        b"abd",
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v = 'abcdefghijklmnopqrstu'") == ("1", 1)
    assert scanned_files(pg_conn, "v > 'abczz'") == ("1", 1)
    assert scanned_files(pg_conn, "v < 'abc'") == ("0", 0)
    assert scanned_files(pg_conn, "v > 'abd'") == ("0", 0)
    assert scanned_files(pg_conn, "v LIKE 'abc%'") == ("1", 2)


def test_invalid_utf8_string_bound(tmp_path, s3, pg_conn, extension):
    """A string bound that is not valid in the database encoding is skipped."""
    table = one_column_table(
        tmp_path, s3, "string", [(1, "a"), (2, "b")], b"a", b"b\xff"
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v < 'a'") == ("1", 0)
    assert scanned_files(pg_conn, "v > 'c'") == ("1", 0)
    assert scanned_files(pg_conn, "v = 'b'") == ("1", 1)


def test_multibyte_string_bounds(tmp_path, s3, pg_conn, extension):
    """
    Iceberg orders string bounds by UTF-8 bytes, so 'z' < 'é'. Pruning compares
    them in the column's collation, which agrees only when the collation
    orders by bytes as well, as the C.UTF-8 test cluster does. Under a
    linguistic collation, 'é' < 'z', and a filter on 'é' is refuted by the
    bounds ['z', 'é'] of a file that has it.
    """
    if not run_query("SELECT 'é' > 'z'", pg_conn)[0][0]:
        pytest.skip("the default collation does not order strings by bytes")

    table = one_column_table(
        tmp_path, s3, "string", [(1, "z"), (2, "é")], b"z", "é".encode()
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v = 'é'") == ("1", 1)
    assert scanned_files(pg_conn, "v = 'z'") == ("1", 1)
    # 'ä' sorts between 'z' and 'é' by bytes
    assert scanned_files(pg_conn, "v = 'ä'") == ("1", 0)
    assert scanned_files(pg_conn, "v < 'z'") == ("0", 0)
    assert scanned_files(pg_conn, "v > 'é'") == ("0", 0)


def test_string_bound_with_nul_byte(tmp_path, s3, pg_conn, extension):
    """Postgres text cannot hold NUL; cutting the bound there would shrink it."""
    table = one_column_table(
        tmp_path, s3, "string", [(1, "a"), (2, "b")], b"a", b"\x00b"
    )
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v = 'b'") == ("1", 1)


@pytest.mark.parametrize(
    "iceberg_type,value,lower,upper,where",
    [
        (
            "uuid",
            uuid.UUID(int=5).bytes,
            uuid.UUID(int=5).bytes,
            uuid.UUID(int=5).bytes,
            "v > '00000000-0000-0000-0000-000000000009'",
        ),
        ("binary", b"\x05", b"\x05", b"\x05", "v > '\\x09'::bytea"),
    ],
)
def test_uuid_and_bytea_are_never_pruned(
    tmp_path, s3, pg_conn, extension, iceberg_type, value, lower, upper, where
):
    """Pruning on these types is not enabled; their bounds must be ignored."""
    table = one_column_table(tmp_path, s3, iceberg_type, [(1, value)], lower, upper)
    attach(pg_conn, table)
    assert scanned_files(pg_conn, where) == ("1", 0)


def test_int_to_long_promotion(tmp_path, s3, pg_conn, extension):
    """A file written before int -> long keeps 4-byte bounds."""
    table = DeleteTable(tmp_path, s3, fields=[(1, "id", "int"), (2, "v", "int")])
    table.add(
        [(1, 10), (2, 20)],
        lower_bounds={2: int_bound(10)},
        upper_bounds={2: int_bound(20)},
    )
    table.fields = [(1, "id", "int"), (2, "v", "long")]
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v > 20") == ("0", 0)
    assert scanned_files(pg_conn, "v = 20") == ("1", 1)
    assert scanned_files(pg_conn, "v > 5000000000") == ("0", 0)


def test_float_to_double_promotion(tmp_path, s3, pg_conn, extension):
    """A file written before float -> double keeps 4-byte bounds."""
    table = DeleteTable(tmp_path, s3, fields=[(1, "id", "int"), (2, "v", "float")])
    table.add(
        [(1, 1.5), (2, 2.5)],
        lower_bounds={2: float_bound(1.5)},
        upper_bounds={2: float_bound(2.5)},
        nan_value_counts={2: 0},
    )
    table.fields = [(1, "id", "int"), (2, "v", "double")]
    attach(pg_conn, table)
    assert scanned_files(pg_conn, "v > 3") == ("0", 0)
    assert scanned_files(pg_conn, "v = 2.5") == ("1", 1)


@pytest.mark.parametrize("column_type", ["double precision", "real"])
def test_nan_in_pg_lake_table_metadata(
    pg_conn, extension, with_default_location, column_type
):
    """
    pg_lake's own files read through their Iceberg metadata: a file with a NaN
    must not be pruned on the min and max of its other values. A file without
    one is still pruned, on the NaN count of zero pg_lake writes for it.
    """
    schema = f"nan_pruning_{column_type.split()[0]}"
    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, f {column_type}) USING iceberg;
        INSERT INTO {schema}.t VALUES (1, 1.0), (2, 'NaN');
        INSERT INTO {schema}.t VALUES (3, 1.0), (4, 2.0);
        """,
        pg_conn,
    )
    pg_conn.commit()
    try:
        metadata = run_query(
            f"SELECT metadata_location FROM iceberg_tables "
            f"WHERE table_namespace = '{schema}' AND table_name = 't'",
            pg_conn,
        )[0][0]
        run_command(
            f"CREATE FOREIGN TABLE {schema}.t_external () SERVER pg_lake "
            f"OPTIONS (path '{metadata}', format 'iceberg')",
            pg_conn,
        )
        table = f"{schema}.t_external"
        # only the file with the NaN can match
        assert scanned_files(pg_conn, "f > 5", table) == ("1", 1)
        assert scanned_files(pg_conn, "f = 'NaN'", table) == ("1", 1)
        # neither file can be pruned: [1, 2] holds values other than 1 and 2
        assert scanned_files(pg_conn, "f NOT IN (1, 2)", table) == ("2", 1)
        # 2 is only in the file without NaN, and the other cannot be pruned
        assert scanned_files(pg_conn, "f = 2", table) == ("2", 1)
        assert scanned_files(pg_conn, "f < 0", table) == ("1", 0)
    finally:
        pg_conn.rollback()
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_spark_written_bounds(installcheck, spark_session, s3, pg_conn, extension):
    """
    Spark's bounds make the same pruning decisions as reading every file: one
    file per INSERT, each with its own ranges, a NaN in one of them, and
    strings past the 16-character bound truncation.
    """
    if installcheck:
        pytest.skip("requires the Spark integration fixture")

    namespace = f"manifest_pruning_{uuid.uuid4().hex}"
    name = f"{namespace}.t"
    spark = spark_session
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, big BIGINT, f DOUBLE, r FLOAT, "
            "s STRING, b BOOLEAN, d DATE, ts TIMESTAMP_NTZ, n DECIMAL(10,2)) "
            "USING iceberg TBLPROPERTIES ('format-version'='2')"
        )
        long_a = "a" * 30
        long_m = "m" * 30
        for values in (
            f"(1, 10, 1.5, 1.5, '{long_a}1', true, DATE'2020-01-01', "
            "TIMESTAMP_NTZ'2020-01-01 00:00:00', 1.25),"
            f"(2, 20, 2.5, 2.5, '{long_a}2', true, DATE'2020-01-02', "
            "TIMESTAMP_NTZ'2020-01-02 00:00:00', 2.25)",
            f"(101, 1000, double('NaN'), float('NaN'), '{long_m}1', false, "
            "DATE'2021-01-01', TIMESTAMP_NTZ'2021-01-01 00:00:00', 101.25),"
            f"(102, 2000, 100.0, 100.0, '{long_m}2', false, DATE'2021-01-02', "
            "TIMESTAMP_NTZ'2021-01-02 00:00:00', 102.25)",
            "(201, -5, double('-Infinity'), float('Infinity'), 'z', NULL, "
            "DATE'1900-01-01', TIMESTAMP_NTZ'1900-01-01 00:00:00', -1.00),"
            "(202, NULL, -0.0, NULL, NULL, true, NULL, NULL, NULL)",
        ):
            spark.sql(f"INSERT INTO {name} VALUES {values}")

        jvm = spark._jvm
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, name
        )
        metadata = table.operations().current().metadataFileLocation()
        run_command(
            f"CREATE FOREIGN TABLE spark_bounds () SERVER pg_lake "
            f"OPTIONS (path '{metadata}', format 'iceberg')",
            pg_conn,
        )

        assert int(fetch_files(pg_conn, "true", "spark_bounds")) >= 3
        assert fetch_files(pg_conn, "id > 1000", "spark_bounds") == "0"

        for where in (
            "id < 3",
            "id BETWEEN 50 AND 150",
            "big > 1500",
            "big < 0",
            "f < 0",
            "f = 2.5",
            "f = '-Infinity'",
            "f = 0",
            "r = 'Infinity'",
            "r < 2",
            f"s = '{long_a}2'",
            "s > 'n'",
            "s < 'b'",
            "s LIKE 'mmm%'",
            "b = false",
            "b IS NULL",
            "d < '2000-01-01'",
            "d = '2021-01-02'",
            "ts > '2020-06-01'",
            "ts IS NULL",
            "n > 100",
            "n = 2.25",
        ):
            scanned_files(pg_conn, where, "spark_bounds")

        # The NaN rows' file must be read for filters NaN satisfies in
        # Postgres; asserted on files, see test_nan_disables_float_pruning.
        for where in ("f > 1000", "f = 'NaN'", "r > 1000", "r = 'NaN'"):
            where = f"id BETWEEN 100 AND 199 AND {where}"
            assert int(fetch_files(pg_conn, where, "spark_bounds")) >= 1, where
    finally:
        pg_conn.rollback()
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP NAMESPACE {namespace}")

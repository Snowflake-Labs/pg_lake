"""
Tests for the binary transmit format, which COPY .. FROM a data lake file
into a regular table uses to receive rows from pgduck_server.

The binary format should produce exactly the same values as the CSV format,
so most tests load the same file twice, once with each format, and compare
the text representation of every row.
"""

import datetime
import decimal

import pyarrow
import pytest
import psycopg2
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
from utils_pytest import *

BINARY_NOTICE = "receiving rows in binary format"


@pytest.fixture(autouse=True)
def rollback_after_test(superuser_conn):
    yield
    superuser_conn.rollback()


def copy_from_file(conn, table, path, binary, columns="", options="format 'parquet'"):
    """Run COPY table FROM path and return whether the binary format was used."""
    run_command(
        f"SET pg_lake_copy.enable_binary_transmit TO {'on' if binary else 'off'}", conn
    )
    run_command("SET client_min_messages TO debug1", conn)
    del conn.notices[:]

    # on error the transaction is aborted, and the caller rolls back
    run_command(f"COPY {table} {columns} FROM '{path}' WITH ({options})", conn)
    run_command("RESET client_min_messages", conn)

    return any(BINARY_NOTICE in notice for notice in conn.notices)


def rows_as_text(conn, table):
    return [
        row[0] for row in run_query(f"SELECT t::text FROM {table} t ORDER BY id", conn)
    ]


def assert_same_load(
    conn, table_ddl, path, expect_binary=True, columns="", options="format 'parquet'"
):
    """Load path into two copies of a table, with and without binary transmit."""
    run_command(table_ddl.format(name="via_binary"), conn)
    run_command(table_ddl.format(name="via_csv"), conn)

    used_binary = copy_from_file(conn, "via_binary", path, True, columns, options)
    used_csv = copy_from_file(conn, "via_csv", path, False, columns, options)

    assert used_binary == expect_binary
    assert not used_csv

    binary_rows = rows_as_text(conn, "via_binary")
    csv_rows = rows_as_text(conn, "via_csv")

    assert len(binary_rows) > 0
    assert binary_rows == csv_rows

    conn.rollback()
    return binary_rows


def write_parquet(duckdb_conn, path, query):
    duckdb_conn.execute(f"COPY ({query}) TO '{path}' (FORMAT 'parquet')")


def test_binary_transmit_scalar_types(superuser_conn, duckdb_conn, tmp_path):
    path = tmp_path / "scalars.parquet"

    write_parquet(
        duckdb_conn,
        path,
        """
        SELECT * FROM (VALUES
          (1, true, (-128)::tinyint, (-32768)::smallint, (-2147483648)::integer,
           255::utinyint, 65535::usmallint, 4294967295::uinteger,
           1.5::float, 1.25::double,
           'hello'::varchar, ''::varchar,
           '\\x00\\x01\\xff'::blob,
           '2024-02-29'::date, '2024-02-29 12:34:56.789012'::timestamp,
           '2024-02-29 12:34:56.789012+00'::timestamptz,
           '12:34:56.789012'::time,
           'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid),
          (2, false, 127::tinyint, 32767::smallint, 2147483647::integer,
           0::utinyint, 0::usmallint, 0::uinteger,
           'nan'::float, 'inf'::double,
           'quotes " and, commas' || chr(10) || 'newline', '\\N',
           ''::blob,
           'infinity'::date, 'infinity'::timestamp, 'infinity'::timestamptz,
           '00:00:00'::time,
           '00000000-0000-0000-0000-000000000000'::uuid),
          (3, NULL, 0::tinyint, 0::smallint, 0::integer,
           NULL, NULL, NULL,
           '-inf'::float, '-0.0'::double,
           'ünïcødé ✓ 日本', NULL,
           NULL,
           '-infinity'::date, '-infinity'::timestamp, '-infinity'::timestamptz,
           '24:00:00'::time,
           'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid),
          (4, true, NULL, NULL, NULL,
           1::utinyint, 1::usmallint, 1::uinteger,
           1e-30::float, 1.7976931348623157e308::double,
           'x', 'y',
           'abc'::blob,
           '0001-01-01'::date, '1900-01-01 00:00:00'::timestamp,
           '1999-12-31 23:59:59.999999+00'::timestamptz,
           '23:59:59.999999'::time,
           NULL::uuid),
          (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
           NULL, NULL, NULL, NULL, NULL, NULL)
        ) AS t(id, c_bool, c_tinyint, c_smallint, c_int, c_utinyint, c_usmallint,
               c_uint, c_float, c_double, c_varchar, c_varchar2, c_blob, c_date,
               c_timestamp, c_timestamptz, c_time, c_uuid)
        """,
    )

    rows = assert_same_load(
        superuser_conn,
        """
        CREATE TEMP TABLE {name} (
          id int, c_bool bool, c_tinyint int2, c_smallint int4, c_int int8,
          c_utinyint int2, c_usmallint int4, c_uint int8,
          c_float float4, c_double float8, c_varchar text, c_varchar2 varchar(20),
          c_blob bytea, c_date date, c_timestamp timestamp, c_timestamptz timestamptz,
          c_time time, c_uuid uuid
        )
        """,
        path,
    )
    assert len(rows) == 5


def test_binary_transmit_numeric(superuser_conn, duckdb_conn, tmp_path):
    path = tmp_path / "numerics.parquet"

    write_parquet(
        duckdb_conn,
        path,
        """
        SELECT * FROM (VALUES
          (1, 12.34::decimal(4,2), 123456789012.345678::decimal(18,6),
           12345678901234567890.123456789012345678::decimal(38,18),
           (-0.0001)::decimal(5,4), 99999999999999999999999999999999999999::decimal(38,0),
           18446744073709551615::ubigint, 1.005::decimal(6,3), (-128)::tinyint),
          (2, (-99.99)::decimal(4,2), (-0.000001)::decimal(18,6),
           (-99999999999999999999.999999999999999999)::decimal(38,18),
           0::decimal(5,4), (-99999999999999999999999999999999999999)::decimal(38,0),
           0::ubigint, (-1.005)::decimal(6,3), 0::tinyint),
          (3, 0.00::decimal(4,2), 10000::decimal(18,6),
           0.000000000000000001::decimal(38,18),
           1.0000::decimal(5,4), 0::decimal(38,0),
           10000::ubigint, 99.995::decimal(6,3), 127::tinyint),
          (4, NULL, 100000000::decimal(18,6), 1e19::decimal(38,18), NULL,
           10000000000000000::decimal(38,0), NULL, 0.5::decimal(6,3), NULL)
        ) AS t(id, d_small, d_bigint, d_huge, d_frac, h, ub, d_round, ti)
        """,
    )

    # numeric(5,2) checks that the typmod rounds identically in both paths
    assert_same_load(
        superuser_conn,
        """
        CREATE TEMP TABLE {name} (
          id int, d_small numeric, d_bigint numeric, d_huge numeric,
          d_frac numeric(10,6), h numeric, ub numeric, d_round numeric(5,2),
          ti numeric
        )
        """,
        path,
    )


def test_binary_transmit_text_variants(superuser_conn, duckdb_conn, tmp_path):
    path = tmp_path / "texts.parquet"

    write_parquet(
        duckdb_conn,
        path,
        """
        SELECT * FROM (VALUES
          (1, 'ab', 'abc', '{"a": [1, 2, {"b": null}]}', '{"b": 2, "a": 1}',
           'before' || chr(0) || 'after'),
          (2, 'a  ', '', '[]', '"string"', chr(0) || 'starts with nul'),
          (3, NULL, NULL, NULL, NULL, NULL)
        ) AS t(id, c_bpchar, c_varchar, c_json, c_jsonb, c_nul)
        """,
    )

    rows = assert_same_load(
        superuser_conn,
        """
        CREATE TEMP TABLE {name} (
          id int, c_bpchar char(5), c_varchar varchar(3), c_json json,
          c_jsonb jsonb, c_nul text
        )
        """,
        path,
    )

    # the text path ends strings at NUL bytes, binary should do the same
    assert rows[0].endswith(",before)")


@pytest.mark.parametrize(
    "value, column_type",
    [
        ("[1, 2, 3]", "int[]"),
        ("INTERVAL 1 DAY", "interval"),
        # a narrowing conversion is left to the int4 input function
        ("1000::bigint", "int4"),
        # widening a float to float8 differs from the text round trip
        ("1.1::float", "float8"),
        # a timestamp is interpreted in the session time zone
        ("'2024-01-01 10:00:00'::timestamp", "timestamptz"),
        ("'2024-01-01 10:00:00.123456789'::timestamp_ns", "timestamp"),
        ("'abc'::varchar", "name"),
    ],
)
def test_binary_transmit_fallback_types(
    superuser_conn, duckdb_conn, tmp_path, value, column_type
):
    """Columns without an equivalent binary writer make the whole COPY use CSV."""
    path = tmp_path / "fallback.parquet"

    write_parquet(duckdb_conn, path, f"SELECT 1 AS id, {value} AS v")

    rows = assert_same_load(
        superuser_conn,
        f"CREATE TEMP TABLE {{name}} (id int, v {column_type})",
        path,
        expect_binary=False,
    )
    assert len(rows) == 1


def test_binary_transmit_narrowing_error(superuser_conn, duckdb_conn, tmp_path):
    path = tmp_path / "narrowing.parquet"

    write_parquet(duckdb_conn, path, "SELECT 10000000000::bigint AS v")

    run_command("CREATE TEMP TABLE narrowing (v int4)", superuser_conn)

    with pytest.raises(psycopg2.errors.NumericValueOutOfRange):
        copy_from_file(superuser_conn, "narrowing", path, True)

    superuser_conn.rollback()


def test_binary_transmit_column_list(superuser_conn, duckdb_conn, tmp_path):
    path = tmp_path / "columns.parquet"

    write_parquet(
        duckdb_conn,
        path,
        "SELECT i AS id, 'v' || i AS b, i * 2 AS a FROM range(1, 1001) r(i)",
    )

    run_command(
        "CREATE TEMP TABLE with_defaults (id bigint, a bigint, b text, c text DEFAULT 'def', g bigint GENERATED ALWAYS AS (a + 1) STORED)",
        superuser_conn,
    )

    used_binary = copy_from_file(
        superuser_conn, "with_defaults", path, True, "(id, b, a)"
    )
    assert used_binary

    rows = run_query(
        "SELECT count(*), sum(a), min(b), max(c), sum(g) FROM with_defaults",
        superuser_conn,
    )
    assert rows[0] == [1000, 1001000, "v1", "def", 1002000]
    superuser_conn.rollback()


def test_binary_transmit_domain(superuser_conn, duckdb_conn, tmp_path):
    path = tmp_path / "domain.parquet"

    write_parquet(duckdb_conn, path, "SELECT * FROM (VALUES (1), (-1)) t(v)")

    run_command(
        "CREATE DOMAIN positive_int AS int CHECK (VALUE > 0); CREATE TEMP TABLE dom (v positive_int)",
        superuser_conn,
    )

    with pytest.raises(psycopg2.errors.CheckViolation):
        copy_from_file(superuser_conn, "dom", path, True)
    superuser_conn.rollback()


def test_binary_transmit_out_of_range(superuser_conn, duckdb_conn, tmp_path):
    """Values outside the PostgreSQL range fail in both formats."""
    for value, column_type in [
        ("'5874898-01-01'::date", "date"),
        ("'-5000-01-01'::date", "date"),
        ("'5000-01-01 (BC)'::timestamp", "timestamp"),
    ]:
        path = tmp_path / "out_of_range.parquet"
        write_parquet(duckdb_conn, path, f"SELECT {value} AS v")

        for binary in [True, False]:
            run_command(f"CREATE TEMP TABLE oor (v {column_type})", superuser_conn)

            with pytest.raises(psycopg2.errors.DatetimeFieldOverflow):
                copy_from_file(superuser_conn, "oor", path, binary)

            superuser_conn.rollback()


def test_binary_transmit_client_encoding(superuser_conn, duckdb_conn, tmp_path):
    """Binary receive functions depend on client_encoding, so only use UTF-8."""
    path = tmp_path / "encoding.parquet"

    write_parquet(duckdb_conn, path, "SELECT 1 AS id, 'é' AS v")

    run_command("CREATE TEMP TABLE enc (id int, v text)", superuser_conn)
    run_command("SET client_encoding TO 'LATIN1'", superuser_conn)
    used_binary = copy_from_file(superuser_conn, "enc", path, True)
    superuser_conn.rollback()
    run_command("RESET client_encoding", superuser_conn)

    assert not used_binary

    rows = assert_same_load(
        superuser_conn, "CREATE TEMP TABLE {name} (id int, v text)", path
    )
    assert rows == ["(1,é)"]


def test_binary_transmit_large(superuser_conn, duckdb_conn, tmp_path):
    """Rows span many CopyData messages."""
    path = tmp_path / "large.parquet"

    write_parquet(
        duckdb_conn,
        path,
        """
        SELECT i AS id, repeat('x', (i % 100)::int) AS s, (i / 7)::decimal(18,2) AS d,
               '2020-01-01'::timestamp + i * INTERVAL 1 SECOND AS ts
        FROM range(0, 200000) r(i)
        """,
    )

    run_command(
        "CREATE TEMP TABLE big_binary (id bigint, s text, d numeric, ts timestamp)",
        superuser_conn,
    )
    run_command(
        "CREATE TEMP TABLE big_csv (id bigint, s text, d numeric, ts timestamp)",
        superuser_conn,
    )

    assert copy_from_file(superuser_conn, "big_binary", path, True)
    assert not copy_from_file(superuser_conn, "big_csv", path, False)

    rows = run_query(
        """
        SELECT count(*) FROM (
          (SELECT * FROM big_binary EXCEPT ALL SELECT * FROM big_csv)
          UNION ALL
          (SELECT * FROM big_csv EXCEPT ALL SELECT * FROM big_binary)
        ) diff
        """,
        superuser_conn,
    )
    assert rows[0][0] == 0

    rows = run_query("SELECT count(*) FROM big_binary", superuser_conn)
    assert rows[0][0] == 200000
    superuser_conn.rollback()


def test_binary_transmit_iceberg_source(superuser_conn, iceberg_catalog):
    """COPY from an Iceberg table into a heap table uses binary."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "amount", DecimalType(12, 3), required=False),
        NestedField(4, "created", TimestamptzType(), required=False),
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.binary_transmit_source",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/binary_transmit_source",
    )

    iceberg_table.append(
        pyarrow.Table.from_pylist(
            [
                {
                    "id": i,
                    "name": f"name {i}" if i % 10 else None,
                    "amount": decimal.Decimal(i) / 8,
                    "created": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.timezone.utc
                    )
                    + datetime.timedelta(minutes=i),
                }
                for i in range(1000)
            ],
            schema=iceberg_table.schema().as_arrow(),
        )
    )

    rows = assert_same_load(
        superuser_conn,
        "CREATE TEMP TABLE {name} (id bigint, name text, amount numeric(12,3), created timestamptz)",
        iceberg_table.metadata_location,
        options="format 'iceberg'",
    )
    assert len(rows) == 1000

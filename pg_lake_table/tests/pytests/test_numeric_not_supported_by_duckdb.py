import pytest
import decimal
from utils_pytest import *


def test_numerics_with_negative_scale(pg_conn, extension, s3, with_default_location):
    run_command(
        f"""
    CREATE TABLE test_numeric (
        a numeric(12, -6),
        b numeric(12, -6)[]
    ) USING iceberg;

    INSERT INTO test_numeric VALUES (
        123456789012345678.12321,
        ARRAY[null, 123456789012345678.12321]
    );
    """,
        pg_conn,
    )

    res = run_query("SELECT * FROM test_numeric", pg_conn)

    assert res == [
        [
            Decimal("123456789012000000"),
            [None, Decimal("123456789012000000")],
        ]
    ]


def test_numerics_with_negative_scale_exceeds_max_precision(
    pg_conn, extension, s3, with_default_location
):
    run_command(
        f"""
    CREATE TABLE test_numeric (
        a numeric(33, -6)
    ) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type from information_schema.columns where table_name = 'test_numeric' and column_name = 'a'",
        pg_conn,
    )
    assert result == [["double precision"]]

    pg_conn.rollback()


def test_numerics_with_larger_scale_than_precision(
    pg_conn, extension, s3, with_default_location
):
    run_command(
        f"""
    CREATE TABLE test_numeric (
        a numeric(3, 6),
        b numeric(3, 6)[]
    ) USING iceberg;

    INSERT INTO test_numeric VALUES (0.0001, ARRAY[null, 0.0001]);
    """,
        pg_conn,
    )

    res = run_query("SELECT * FROM test_numeric", pg_conn)

    assert res == [[Decimal("0.000100"), [None, Decimal("0.000100")]]]


def test_numerics_with_larger_scale_than_precision_exceeds_max_precision(
    pg_conn, extension, s3, with_default_location
):
    error = run_command(
        f"""
    CREATE TABLE test_numeric (
        a numeric(3, 50)[]
    ) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )

    assert "scale > 38 are not supported" in error

    pg_conn.rollback()

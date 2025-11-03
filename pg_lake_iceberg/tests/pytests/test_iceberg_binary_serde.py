import pytest
import json
import random
import string
import sys
from utils_pytest import *


def test_pg_lake_serde_numeric(
    installcheck,
    superuser_conn,
    iceberg_extension,
    create_helper_functions,
):
    # random(numeric, numeric) does not exist before pg17
    if get_pg_version_num(superuser_conn) < 170000:
        return

    result = run_query(
        f"SELECT lake_iceberg.serde_value('NaN'::numeric, 'decimal(38,0)');",
        superuser_conn,
    )[0][0]
    assert result is None

    result = run_query(
        f"SELECT lake_iceberg.serde_value('+inf'::numeric, 'decimal(38,0)');",
        superuser_conn,
    )[0][0]
    assert result is None

    result = run_query(
        f"SELECT lake_iceberg.serde_value('-inf'::numeric, 'decimal(38,0)');",
        superuser_conn,
    )[0][0]
    assert result is None

    # numerics with Precision >= Scale (Iceberg (and pglake tables) does not allow otherwise)
    numerics = [
        ("5.56", 10, 2),
        ("0.0010", 5, 4),
        ("0.001", 5, 4),
        ("128", 3, 0),
        ("12345678901234567890123456789012345678", 38, 0),
        ("12345678901234567890.123456789012345678", 38, 18),
        ("-5.56", 10, 2),
        ("-0.0010", 5, 4),
        ("-0.001", 5, 4),
        ("-128", 3, 0),
        ("-12345678901234567890123456789012345678", 38, 0),
        ("-12345678901234567890.123456789012345678", 38, 18),
    ]

    for num, precision, scale in numerics:
        assert precision >= scale, "Precision must be greater than or equal to scale"

        pg_query = f"SELECT lake_iceberg.serde_value('{num}'::numeric({precision}, {scale}), 'decimal({precision}, {scale})');"
        result = run_query(pg_query, superuser_conn)
        assert result[0][0] == Decimal(num)

    # test with random numerics
    for _ in range(1_000):
        random_precision = random.randint(1, 38)
        random_scale = random.randint(0, random_precision)

        assert (
            random_precision >= random_scale
        ), "Precision must be greater than or equal to scale"

        min_numeric = (
            f"-{'9' * (random_precision - random_scale)}"
            if random_precision > random_scale
            else f"-0.{'9' * random_scale}"
        )
        max_numeric = (
            f"{'9' * (random_precision - random_scale)}"
            if random_precision > random_scale
            else f"0.{'9' * random_scale}"
        )

        pg_query = f"SELECT lake_iceberg.serde_value(random({min_numeric}::numeric({random_precision},{random_scale}), {max_numeric}::numeric({random_precision},{random_scale})), 'decimal({random_precision}, {random_scale})');"
        result = run_query(pg_query, superuser_conn)
        assert result[0][0] == Decimal(result[0][0]), pg_query

    superuser_conn.rollback()


def test_pg_lake_serde_floating(
    superuser_conn,
    iceberg_extension,
    create_helper_functions,
):
    result = run_query(
        f"SELECT lake_iceberg.serde_value('NaN'::float4, 'float');", superuser_conn
    )[0][0]
    assert result is None

    result = run_query(
        f"SELECT lake_iceberg.serde_value('NaN'::float8, 'double');", superuser_conn
    )[0][0]
    assert result is None

    # check infinity values
    inf_values = [
        ("float", "float4", "-inf"),
        ("float", "float4", "inf"),
        ("double", "float8", "-inf"),
        ("double", "float8", "inf"),
    ]

    for iceberg_type, pg_type, value in inf_values:
        pg_query = (
            f"SELECT lake_iceberg.serde_value('{value}'::{pg_type}, '{iceberg_type}');"
        )
        result = run_query(pg_query, superuser_conn)
        assert result[0][0] is None

    values = [
        ("double", "float8", 1.7976931348623157e308),
        ("double", "float8", -1.7976931348623157e308),
        ("double", "float8", pow(2, 64)),
        ("double", "float8", -pow(2, 64)),
        ("float", "float4", 3.4028235e38),
        ("float", "float4", -3.4028235e38),
    ]

    for iceberg_type, pg_type, value in values:
        pg_query = (
            f"SELECT lake_iceberg.serde_value({value}::{pg_type}, '{iceberg_type}');"
        )
        result = run_query(pg_query, superuser_conn)
        assert result[0][0] == value

    # test with random floats
    for _ in range(1_000):
        iceberg_type, pg_type = random.choice(
            [("float", "float4"), ("double", "float8")]
        )
        random_value = random.uniform(3.4e-38, 1.7e38)

        if pg_type == "float4":
            random_value = float(f"{random_value:.5e}")

        pg_query = f"SELECT lake_iceberg.serde_value({random_value}::{pg_type}, '{iceberg_type}');"
        result = run_query(pg_query, superuser_conn)
        assert result[0][0] == random_value, pg_query

    superuser_conn.rollback()


def test_pg_lake_serde(
    superuser_conn,
    iceberg_extension,
    create_helper_functions,
):
    # some of the values are output differently in python but preserves the same value
    values = [
        # native types
        ("string", "text", "hello\\0world"),
        ("uuid", "uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        # not native supported types (serialized as text)
        ("string", "smallint", "12"),
        ("string", "inet", "192.168.1.5"),
        ("string", "interval", "1155 days, 0:02:01"),
        ("string", "point", "(1.2,3.4)"),
        ("string", "json", "{}"),
        ("string", "json", '[true, "two", [1, 2, 3], {"key": 4}]'),
        ("string", "json", '{"key": null}'),
        ("string", "jsonb", "[]"),
        ("string", "jsonb", '{"key": "value"}'),
        ("string", "jsonb", '{"outer_key": {"inner_key": "value"} }'),
        ("string", "jsonb", '["a", "b", "c"]'),
        ("string", "jsonb", '{"emoji": "😊", "cyrillic": "Привет"}'),
        ("string", "jsonb", '{"key": "value with \\n newline and \\"quotes\\""}'),
    ]

    for iceberg_type, pg_type, value in values:
        pg_query = (
            f"SELECT lake_iceberg.serde_value('{value}'::{pg_type}, '{iceberg_type}');"
        )
        result = run_query(pg_query, superuser_conn)

        if pg_type == "json" or pg_type == "jsonb":
            assert result[0][0] == json.loads(value)
        else:
            assert str(result[0][0]) == value

    superuser_conn.rollback()


def test_pg_lake_serde_int(
    superuser_conn,
    iceberg_extension,
    create_helper_functions,
):
    # test with random integers
    for _ in range(1_000):
        iceberg_type, pg_type = random.choice(
            [("integer", "integer"), ("long", "bigint")]
        )

        if pg_type == "integer":
            random_value = random.randint(pow(-2, 31), pow(2, 31) - 1)
        else:
            random_value = random.randint(pow(-2, 63), pow(2, 63) - 1)

        pg_query = f"SELECT lake_iceberg.serde_value({random_value}::{pg_type}, '{iceberg_type}');"
        result = run_query(pg_query, superuser_conn)
        assert result[0][0] == random_value, pg_query

    superuser_conn.rollback()


def test_pg_lake_serde_temporal(
    superuser_conn,
    iceberg_extension,
    create_helper_functions,
):
    values = [
        ("date", "date", "1970-01-01"),
        ("date", "date", "0001-01-01"),
        ("date", "date", "9999-12-31"),
        ("date", "date", "2000-02-29"),
        ("date", "date", "1900-02-28"),
        ("date", "date", "0001-12-31"),
        ("time", "time", "00:00:00"),
        ("time", "time", "23:59:59.999999"),
        ("time", "time", "12:00:00"),
        ("time", "time", "12:00:00"),
        ("timestamp", "timestamp", "1970-01-01 00:00:00"),
        ("timestamp", "timestamp", "2000-02-29 23:59:59.999999"),
        ("timestamp", "timestamp", "9999-12-31 23:59:59"),
        ("timestamp", "timestamp", "0001-01-01 00:00:00"),
        ("timestamp", "timestamp", "1970-01-01 13:15:30"),
    ]

    for iceberg_type, pg_type, value in values:
        pg_query = (
            f"SELECT lake_iceberg.serde_value('{value}'::{pg_type}, '{iceberg_type}');"
        )
        result = run_query(pg_query, superuser_conn)
        assert str(result[0][0]) == value

    timestamptz_values = [
        (
            "timestamptz",
            "timestamptz",
            "1970-01-01 00:00:00+00",
            "1970-01-01 00:00:00+00:00",
        ),
        (
            "timestamptz",
            "timestamptz",
            "2020-02-29 23:59:59.999999+05:30",
            "2020-02-29 18:29:59.999999+00:00",
        ),
        (
            "timestamptz",
            "timestamptz",
            "1990-01-01 03:30:00-07",
            "1990-01-01 10:30:00+00:00",
        ),
        (
            "timestamptz",
            "timestamptz",
            "2038-01-19 03:14:07+00",
            "2038-01-19 03:14:07+00:00",
        ),
        (
            "timestamptz",
            "timestamptz",
            "9998-12-31 23:59:59.999999-12",
            "9999-01-01 11:59:59.999999+00:00",
        ),
        (
            "timestamptz",
            "timestamptz",
            "0002-01-01 00:00:00+14",
            "0001-12-31 10:00:00+00:00",
        ),
    ]

    for iceberg_type, pg_type, value, expected in timestamptz_values:
        pg_query = (
            f"SELECT lake_iceberg.serde_value('{value}'::{pg_type}, '{iceberg_type}');"
        )
        result = run_query(pg_query, superuser_conn)
        assert str(result[0][0]) == expected

    superuser_conn.rollback()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, s3, iceberg_extension):

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.serde_value(value anyelement, iceberg_scalar_type text)
        RETURNS anyelement
        LANGUAGE C STRICT
        AS 'pg_lake_iceberg', $$pg_lake_serde_value$$;
""",
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    superuser_conn.rollback()

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION IF EXISTS lake_iceberg.serde_value
""",
        superuser_conn,
    )

    superuser_conn.commit()

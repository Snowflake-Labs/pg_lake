import json
import pytest
from utils_pytest import *


def test_bytea_domain(extension, pg_conn, s3, with_default_location):
    run_command(
        f"""
        create domain bbb as bytea;
        create table test (x int, b bbb default '\\x0000') using iceberg;
        insert into test values (1);
    """,
        pg_conn,
    )

    result = run_query(
        f"""
        select b from test
    """,
        pg_conn,
    )
    assert bytes(result[0]["b"]) == b"\x00\x00"

    pg_conn.rollback()


def test_copy_from_domain(extension, pg_conn, s3, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_copy_from_domain/input.parquet"

    run_command(
        f"""
        copy (select 9 as x, -1 as y) to '{url}';

        create domain positive as int check (value > 0);

        create table test_copy_from_domain (x int, y positive default 2) using iceberg;
    """,
        pg_conn,
    )

    # Domain should be checked
    error = run_command(
        f"""
        copy test_copy_from_domain from '{url}'
    """,
        pg_conn,
        raise_error=False,
    )
    assert "value for domain positive violates check constraint" in error

    pg_conn.rollback()


def test_domain_iceberg_field_type(extension, pg_conn, s3, with_default_location):
    """Domain over a scalar type must produce the base type in Iceberg metadata, not string."""
    run_command(
        """
        create domain year_int as integer check (value >= 1 and value <= 9999);
        create domain small_float as double precision check (value > 0);
        create table domain_types (
            y year_int,
            d small_float,
            t text
        ) using iceberg;
        insert into domain_types values (2024, 3.14, 'hello');
    """,
        pg_conn,
    )

    results = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'domain_types'",
        pg_conn,
    )
    assert len(results) == 1
    metadata_path = results[0][0]

    data = read_s3_operations(s3, metadata_path)
    parsed = json.loads(data)
    fields = {f["name"]: f["type"] for f in parsed["schemas"][0]["fields"]}

    assert fields["y"] == "int", f"domain over integer should be 'int', got {fields['y']!r}"
    assert fields["d"] == "double", f"domain over double precision should be 'double', got {fields['d']!r}"
    assert fields["t"] == "string"

    result = run_query("SELECT y, d, t FROM domain_types", pg_conn)
    assert result[0]["y"] == 2024
    assert abs(result[0]["d"] - 3.14) < 0.001
    assert result[0]["t"] == "hello"

    pg_conn.rollback()

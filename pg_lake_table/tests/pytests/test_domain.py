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
        create schema test_domain_field_type;
        set search_path to test_domain_field_type;
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
    pg_conn.commit()

    results = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables"
        " WHERE table_name = 'domain_types' AND table_namespace = 'test_domain_field_type'",
        pg_conn,
    )
    assert len(results) == 1
    metadata_path = results[0][0]

    data = read_s3_operations(s3, metadata_path)
    parsed = json.loads(data)
    fields = {f["name"]: f["type"] for f in parsed["schemas"][0]["fields"]}

    assert (
        fields["y"] == "int"
    ), f"domain over integer should be 'int', got {fields['y']!r}"
    assert (
        fields["d"] == "double"
    ), f"domain over double precision should be 'double', got {fields['d']!r}"
    assert fields["t"] == "string"

    result = run_query("SELECT y, d, t FROM domain_types", pg_conn)
    assert result[0]["y"] == 2024
    assert abs(result[0]["d"] - 3.14) < 0.001
    assert result[0]["t"] == "hello"

    pg_conn.rollback()
    run_command("drop schema if exists test_domain_field_type cascade;", pg_conn)
    pg_conn.commit()


def test_domain_over_array(extension, pg_conn, s3, with_default_location):
    """A domain over an array type is an array itself and must be written as a list.

    Domains can be stacked arbitrarily deep, and getBaseType() walks the whole
    stack, so nesting resolves to the same list type.
    """
    run_command(
        """
        create schema test_domain_over_array;
        set search_path to test_domain_over_array;
        create domain int_list as int[];
        create domain time_list as timetz[];
        create domain positive as int check (value > 0);
        -- domains can be stacked, both above and below the array type
        create domain nested_int_list as int_list;
        create domain still_positive as positive;
        create table domain_arrays (
            i int_list,
            t time_list,
            p positive[],
            n nested_int_list,
            s still_positive[]
        ) using iceberg;
        insert into domain_arrays values (
            array[1, 2]::int_list,
            array['09:00:00+04'::timetz]::time_list,
            array[3, 4]::positive[],
            array[5, 6]::nested_int_list,
            array[7, 8]::still_positive[]
        );
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT i, t::text AS t, p::text AS p, n, s::text AS s FROM domain_arrays",
        pg_conn,
    )
    assert result[0]["i"] == [1, 2]
    assert result[0]["p"] == "{3,4}"
    # timetz is stored UTC-normalized because Iceberg has no timetz type
    assert result[0]["t"] == "{05:00:00+00}"
    assert result[0]["n"] == [5, 6]
    assert result[0]["s"] == "{7,8}"

    results = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables"
        " WHERE table_name = 'domain_arrays'"
        " AND table_namespace = 'test_domain_over_array'",
        pg_conn,
    )
    parsed = json.loads(read_s3_operations(s3, results[0][0]))
    fields = {f["name"]: f["type"] for f in parsed["schemas"][0]["fields"]}
    assert fields["i"]["element"] == "int"
    assert fields["t"]["element"] == "time"
    assert fields["p"]["element"] == "int"
    assert fields["n"]["element"] == "int"
    assert fields["s"]["element"] == "int"

    pg_conn.rollback()
    run_command("drop schema if exists test_domain_over_array cascade;", pg_conn)
    pg_conn.commit()

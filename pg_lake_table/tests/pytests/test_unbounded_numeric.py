import pytest
from utils_pytest import *

TEST_NUMERIC_URL = f"s3://{TEST_BUCKET}/numeric.parquet"


def test_small_numeric_pushdown(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/large_numeric.parquet"

    run_command(
        f"""
        copy (select 12 as id, 123.12::numeric(5,2) as a)
        to '{uri}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        create foreign table small_numeric (id int, a numeric(5,2))
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    expected_expression = "sum(a)"
    query = "select sum(a) from small_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = "abs(a)"
    query = "select abs(a) from small_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = "a + a"
    query = "select a + a from small_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_large_numeric_pushdown(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/large_numeric.parquet"

    run_command(
        f"""
        copy (select 12 as id, 123456789012345678901234567890123456789.123456789012345::numeric(54,15) as a)
        to '{uri}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        create foreign table large_numeric (id int, a numeric(54,15))
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    run_command("SAVEPOINT sp1", pg_conn)
    error = run_query("select sum(a) from large_numeric", pg_conn, raise_error=False)
    assert (
        "No function matches the given name and argument types 'sum(VARCHAR)'" in error
    )
    run_command("ROLLBACK TO SAVEPOINT sp1", pg_conn)

    run_command("SAVEPOINT sp1", pg_conn)
    error = run_query("select abs(a) from large_numeric", pg_conn, raise_error=False)
    assert (
        "No function matches the given name and argument types 'abs(VARCHAR)'" in error
    )
    run_command("ROLLBACK TO SAVEPOINT sp1", pg_conn)

    error = run_query("select a + a from large_numeric", pg_conn, raise_error=False)
    assert (
        "No function matches the given name and argument types '+(VARCHAR, VARCHAR)'"
        in error
    )

    pg_conn.rollback()


def test_unbounded_numeric_pushdown(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/unbounded_numeric.parquet"

    run_command(
        f"""
        copy (select 12 as id, 123.12::numeric as a)
        to '{uri}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        create foreign table unbounded_numeric (id int, a numeric)
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    expected_expression = "sum(a)"
    query = "select sum(a) from unbounded_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = "abs(a)"
    query = "select abs(a) from unbounded_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = "a + a"
    query = "select a + a from unbounded_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_copy_to_unbounded_numeric_with_default(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/unbounded_numeric.parquet"

    error = run_command(
        f"""
        copy (select random()::numeric)
        to '{uri}'
    """,
        pg_conn,
        raise_error=False,
    )

    assert (
        "unbounded numeric type exceeds max allowed digits 9 after decimal point"
        in error
    )

    pg_conn.rollback()


def test_copy_to_exceeds_unbounded_numeric_max_integral_digits(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/numeric_exceeds_max_integral.parquet"

    invalid_numeric = "123456789012345678901234567890.56"

    error = run_command(
        f"""
        copy (select 12 as id, {invalid_numeric}::numeric as a)
        to '{uri}'
    """,
        pg_conn,
        raise_error=False,
    )

    assert "numeric type exceeds max allowed digits 29 before decimal point" in error

    pg_conn.rollback()


def test_copy_to_exceeds_unbounded_numeric_max_decimal_digits(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/numeric_exceeds_max_decimal.parquet"

    invalid_numeric = "23.1234567890123"

    error = run_command(
        f"""
        copy (select 12 as id, {invalid_numeric}::numeric as a)
        to '{uri}'
    """,
        pg_conn,
        raise_error=False,
    )

    assert "numeric type exceeds max allowed digits 9 after decimal point" in error

    pg_conn.rollback()


def test_regular_table_load_from(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create table regular_table () with (load_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'regular_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    pg_conn.rollback()


def test_regular_table_definition_from(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create table regular_table () with (definition_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'regular_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    pg_conn.rollback()


def test_regular_table_explicit(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create table regular_table (numeric_small numeric(5,3), numeric_large numeric(39,10), numeric_unbounded numeric) with (load_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'regular_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 39, 10], ["numeric", 5, 3], ["numeric", None, None]]

    pg_conn.rollback()


def test_pg_lake_table_with_no_cols(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create foreign table pg_lake_table_with_no_cols () server pg_lake options (path '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'pg_lake_table_with_no_cols' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT numeric_large, numeric_small, numeric_unbounded FROM pg_lake_table_with_no_cols {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [["12.2230000000", Decimal("12.223"), Decimal("12.22300000")]]

    pg_conn.rollback()


def test_pg_lake_table_explicit(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create foreign table pg_lake_table_explicit (numeric_small numeric(5,3), numeric_large numeric(39,10), numeric_unbounded numeric)
         server pg_lake options (path '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'pg_lake_table_explicit' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 39, 10], ["numeric", 5, 3], ["numeric", None, None]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT numeric_large, numeric_small, numeric_unbounded FROM pg_lake_table_explicit {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [Decimal("12.2230000000"), Decimal("12.223"), Decimal("12.22300000")]
    ]

    pg_conn.rollback()


def test_writable_pg_lake_table(s3, pg_conn, extension, copy_numeric_to_file):
    run_command("set pg_lake_table.unbounded_numeric_default_precision = 10", pg_conn)
    run_command("set pg_lake_table.unbounded_numeric_default_scale = 5", pg_conn)

    run_command(
        f"""
        create foreign table writable_pg_lake_table (numeric_small numeric(5,3), numeric_large numeric(39,10), numeric_unbounded numeric)
         server pg_lake options (location '{TEST_NUMERIC_URL}', format 'parquet', writable 'true');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'writable_pg_lake_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 39, 10], ["numeric", 5, 3], ["numeric", None, None]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT * FROM writable_pg_lake_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_table_load_from(s3, pg_conn, extension, copy_numeric_to_file):
    location = f"s3://{TEST_BUCKET}/iceberg_table"

    run_command(
        f"""
        create table iceberg_table () using pg_lake_iceberg with (load_from = '{TEST_NUMERIC_URL}', location = '{location}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT * FROM iceberg_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_table_definition_from(
    s3, pg_conn, extension, copy_numeric_to_file, with_default_location
):
    run_command(
        f"""
        create table iceberg_table () using pg_lake_iceberg with (definition_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT * FROM iceberg_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_table_explicit(s3, pg_conn, extension, with_default_location):
    # lets try with different defaults
    run_command("set pg_lake_table.unbounded_numeric_default_precision = 10", pg_conn)
    run_command("set pg_lake_table.unbounded_numeric_default_scale = 5", pg_conn)

    run_command(
        f"""
        create table iceberg_table (numeric_small numeric(5,3), numeric_unbounded numeric) using pg_lake_iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 5, 3], ["numeric", None, None]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT * FROM iceberg_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_create_as_select(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        create table iceberg_table using pg_lake_iceberg
         as select 12.223::numeric(5,3) as numeric_small,
                   12.223::numeric as numeric_unbounded;
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 5, 3], ["numeric", None, None]]

    expected_expression = "WHERE (abs(numeric_unbounded) > (1)::numeric)"
    query = f"SELECT numeric_small, numeric_unbounded FROM iceberg_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [[Decimal("12.223"), Decimal("12.22300000")]]

    pg_conn.rollback()


@pytest.fixture(scope="module")
def copy_numeric_to_file(pg_conn, s3, request):
    run_command(
        f"""copy (select 12.223::numeric(5,3) as numeric_small,
                                 12.223::numeric(39, 10) as numeric_large,
                                 12.223::numeric as numeric_unbounded)
                    to '{TEST_NUMERIC_URL}'
                """,
        pg_conn,
    )

import pytest
from utils_pytest import *


def test_unbounded_numeric_converted_to_double(
    s3, pg_conn, extension, with_default_location
):
    """Unbounded numeric columns become float8 on Iceberg tables (GUC default on)."""
    run_command(
        """
        CREATE TABLE test_num_double (a numeric, b numeric(10,2)) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_double' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["a", "double precision"], ["b", "numeric"]]

    pg_conn.rollback()


def test_precision_above_38_converted_to_double(
    s3, pg_conn, extension, with_default_location
):
    """Bounded numeric with precision > 38 becomes float8 on Iceberg tables."""
    run_command(
        """
        CREATE TABLE test_num_double (a numeric(50,10)) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_double' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["a", "double precision"]]

    pg_conn.rollback()


def test_guc_off_unbounded_numeric_errors(
    s3, pg_conn, extension, with_default_location
):
    """With GUC disabled, unbounded numeric errors."""
    run_command("SET pg_lake_iceberg.unsupported_numeric_as_double TO off", pg_conn)

    error = run_command(
        """
        CREATE TABLE test_num_keep (a numeric, b numeric(10,2)) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )

    assert "unbounded numeric types are not supported" in error

    pg_conn.rollback()


def test_guc_off_precision_above_38_errors(
    s3, pg_conn, extension, with_default_location
):
    """With GUC disabled, precision > 38 still errors (old behaviour)."""
    run_command("SET pg_lake_iceberg.unsupported_numeric_as_double TO off", pg_conn)

    error = run_command(
        """
        CREATE TABLE test_num_err (a numeric(50,10)) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )

    assert "precision > 38 are not supported" in error

    pg_conn.rollback()


def test_insert_and_read_converted_unbounded_numeric(
    s3, pg_conn, extension, with_default_location
):
    """Insert values into a converted unbounded numeric column and read them back."""
    run_command(
        """
        CREATE TABLE test_num_insert (id int, val numeric) USING iceberg;
        INSERT INTO test_num_insert VALUES (1, 3.14), (2, -999.5), (3, 0);
    """,
        pg_conn,
    )

    result = run_query("SELECT id, val FROM test_num_insert ORDER BY id", pg_conn)
    assert result == [[1, 3.14], [2, -999.5], [3, 0.0]]

    pg_conn.rollback()


def test_insert_and_read_converted_large_precision_numeric(
    s3, pg_conn, extension, with_default_location
):
    """Insert values into a converted large-precision numeric column and read them back."""
    run_command(
        """
        CREATE TABLE test_num_large (id int, val numeric(50,10)) USING iceberg;
        INSERT INTO test_num_large VALUES (1, 12345.6789), (2, -0.001);
    """,
        pg_conn,
    )

    result = run_query("SELECT id, val FROM test_num_large ORDER BY id", pg_conn)
    assert result == [[1, 12345.6789], [2, -0.001]]

    pg_conn.rollback()


def test_iceberg_metadata_shows_double_for_converted_column(
    s3, pg_conn, extension, with_default_location
):
    """Iceberg metadata should show 'double' type for converted numeric columns."""
    location = f"s3://{TEST_BUCKET}/test_num_metadata"
    run_command(
        f"""
        CREATE FOREIGN TABLE test_num_meta (a numeric, b numeric(50,5))
            SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    import json

    results = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'test_num_meta'",
        pg_conn,
    )
    metadata_path = results[0][0]
    data = read_s3_operations(s3, metadata_path)
    parsed = json.loads(data)
    fields = parsed["schemas"][0]["fields"]

    assert fields[0]["name"] == "a"
    assert fields[0]["type"] == "double"
    assert fields[1]["name"] == "b"
    assert fields[1]["type"] == "double"

    run_command("DROP FOREIGN TABLE test_num_meta", pg_conn)
    pg_conn.commit()


def test_pushdown_works_for_converted_column(
    s3, pg_conn, extension, with_default_location
):
    """Filter pushdown should work for columns converted from numeric to float8."""
    run_command(
        """
        CREATE TABLE test_num_push (id int, val numeric) USING iceberg;
        INSERT INTO test_num_push VALUES (1, 10.0), (2, 20.0), (3, 30.0);
    """,
        pg_conn,
    )

    query = "SELECT id, val FROM test_num_push WHERE val > 15"
    result = run_query(query, pg_conn)
    assert len(result) == 2

    assert_remote_query_contains_expression(query, "val", pg_conn)

    pg_conn.rollback()


def test_alter_table_add_unsupported_numeric_column(
    s3, pg_conn, extension, with_default_location
):
    """ALTER TABLE ADD COLUMN with unsupported numeric should also convert."""
    run_command(
        """
        CREATE TABLE test_num_alter (id int) USING iceberg;
        ALTER TABLE test_num_alter ADD COLUMN val numeric;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_alter' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["id", "integer"], ["val", "double precision"]]

    pg_conn.rollback()


def test_bounded_numeric_within_limits_not_converted(
    s3, pg_conn, extension, with_default_location
):
    """Bounded numeric with precision <= 38 should remain numeric."""
    run_command(
        """
        CREATE TABLE test_num_ok (a numeric(38,10), b numeric(20,5)) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_ok' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["a", "numeric"], ["b", "numeric"]]

    pg_conn.rollback()

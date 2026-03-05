import pytest
from utils_pytest import *


def test_special_numeric(s3, pg_conn, extension, with_default_location):
    """Default error mode: NaN, Inf, -Inf are rejected for iceberg numeric columns."""

    run_command(
        """
                CREATE SCHEMA test_special_numeric;

                CREATE TABLE test_special_numeric.unbounded(b numeric) USING iceberg
                    WITH (out_of_range_values = 'error');
                CREATE TABLE test_special_numeric.bounded(b numeric(20,10)) USING iceberg
                    WITH (out_of_range_values = 'error');

        """,
        pg_conn,
    )
    pg_conn.commit()

    values = [
        "Inf",
        "+Inf",
        "Infinity",
        "-Infinity",
        "   -inf  ",
        "nan",
        "Nan",
        " +Infinity ",
    ]
    tables = ["test_special_numeric.unbounded", "test_special_numeric.bounded"]

    for table in tables:
        for value in values:

            err = run_command(
                f"INSERT INTO {table} VALUES ('{value}')", pg_conn, raise_error=False
            )
            assert (
                "NaN is not allowed for numeric type" in str(err)
                or "Infinity values are not allowed for numeric type" in str(err)
                or "cannot hold an infinite value" in str(err)
            )

            pg_conn.rollback()

    run_command(
        """
                DROP SCHEMA test_special_numeric CASCADE;
                """,
        pg_conn,
    )
    pg_conn.commit()


def test_special_numeric_array_error(s3, pg_conn, extension, with_default_location):
    """Default error mode: NaN, Inf, -Inf in numeric arrays are rejected."""

    run_command(
        "CREATE TABLE test_arr_err (b numeric[]) USING iceberg"
        " WITH (out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    special_values = ["NaN", "Inf", "-Inf"]
    for value in special_values:
        err = run_command(
            f"INSERT INTO test_arr_err VALUES (ARRAY['{value}'::numeric]);",
            pg_conn,
            raise_error=False,
        )
        assert "NaN is not allowed for numeric type" in str(
            err
        ) or "Infinity values are not allowed for numeric type" in str(err)
        pg_conn.rollback()

    run_command("DROP TABLE test_arr_err;", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize(
    "col_def,value,expected",
    [
        # NaN → NULL for unbounded numeric
        ("numeric", "NaN", None),
        # +Inf → max DECIMAL(38,9) for unbounded numeric
        ("numeric", "Inf", "99999999999999999999999999999.999999999"),
        # -Inf → min DECIMAL(38,9) for unbounded numeric
        ("numeric", "-Inf", "-99999999999999999999999999999.999999999"),
        # NaN → NULL for bounded numeric (PG allows NaN for bounded)
        ("numeric(20,10)", "NaN", None),
    ],
)
def test_special_numeric_clamp(
    s3, pg_conn, extension, with_default_location, col_def, value, expected
):
    """Clamp mode: NaN → NULL, Inf → max/min for iceberg numeric columns."""
    run_command(
        f"CREATE TABLE test_clamp_numeric (b {col_def}) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO test_clamp_numeric VALUES ('{value}');",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT b::text FROM test_clamp_numeric;",
        pg_conn,
    )
    assert result[0][0] == expected

    run_command("DROP TABLE test_clamp_numeric;", pg_conn)
    pg_conn.commit()


def test_special_numeric_array_clamp(s3, pg_conn, extension, with_default_location):
    """Clamp mode for numeric arrays: NaN → NULL, Inf → max, -Inf → min."""
    run_command(
        "CREATE TABLE test_clamp_arr (b numeric[]) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_clamp_arr VALUES "
        "(ARRAY['NaN'::numeric, 'Inf'::numeric, '-Inf'::numeric]);",
        pg_conn,
    )
    pg_conn.commit()

    # Verify each array element individually
    result = run_query(
        "SELECT b[1]::text, b[2]::text, b[3]::text FROM test_clamp_arr;",
        pg_conn,
    )
    # NaN → NULL
    assert result[0][0] is None
    # Inf → max DECIMAL(38,9)
    assert result[0][1] == "99999999999999999999999999999.999999999"
    # -Inf → min DECIMAL(38,9)
    assert result[0][2] == "-99999999999999999999999999999.999999999"

    run_command("DROP TABLE test_clamp_arr;", pg_conn)
    pg_conn.commit()

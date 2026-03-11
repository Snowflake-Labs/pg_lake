import pytest
from utils_pytest import *


def test_special_numeric_bounded(s3, pg_conn, extension, with_default_location):
    run_command(
        """
                CREATE SCHEMA test_special_numeric;
                CREATE TABLE test_special_numeric.bounded(b numeric(20,10)) USING iceberg;
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

    for value in values:
        err = run_command(
            f"INSERT INTO test_special_numeric.bounded VALUES ('{value}')",
            pg_conn,
            raise_error=False,
        )
        assert (
            "Special numeric values" in str(err)
            and "are not allowed for numeric type" in str(err)
        ) or "cannot hold an infinite value" in str(err)

        pg_conn.rollback()

    run_command(
        """
                DROP SCHEMA test_special_numeric CASCADE;
                """,
        pg_conn,
    )
    pg_conn.commit()


def test_special_numeric_unbounded_as_double(
    s3, pg_conn, extension, with_default_location
):
    """With the GUC on (default), unbounded numeric becomes float8 which accepts NaN/Inf."""
    run_command(
        """
                CREATE SCHEMA test_special_numeric;
                CREATE TABLE test_special_numeric.unbounded(b numeric) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_special_numeric.unbounded VALUES ('Inf'), ('NaN'), ('-Infinity')",
        pg_conn,
    )

    result = run_query(
        "SELECT b FROM test_special_numeric.unbounded ORDER BY b", pg_conn
    )
    import math

    assert len(result) == 3
    assert result[0][0] == float("-inf")
    assert result[1][0] == float("inf")
    assert math.isnan(result[2][0])

    run_command(
        """
                DROP SCHEMA test_special_numeric CASCADE;
                """,
        pg_conn,
    )
    pg_conn.commit()

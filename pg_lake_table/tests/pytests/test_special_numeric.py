import pytest
import math
from utils_pytest import *


def test_special_numeric_bounded(s3, pg_conn, extension, with_default_location):
    """Bounded numeric (precision <= 38) becomes DECIMAL in DuckDB, which rejects NaN."""
    schema = "test_special_numeric_bounded"

    run_command(
        f"""
                CREATE SCHEMA {schema};
                CREATE TABLE {schema}.bounded_err(b numeric(20,10)) USING iceberg
                    WITH (out_of_range_values = 'error');
                CREATE TABLE {schema}.bounded_clamp(b numeric(20,10)) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        nan_values = ["nan", "Nan", "NaN"]

        for value in nan_values:
            err = run_command(
                f"INSERT INTO {schema}.bounded_err VALUES ('{value}')",
                pg_conn,
                raise_error=False,
            )
            assert "NaN is not supported for Iceberg decimal" in str(err)
            pg_conn.rollback()

        for value in nan_values:
            run_command(
                f"INSERT INTO {schema}.bounded_clamp VALUES ('{value}')",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(f"SELECT b FROM {schema}.bounded_clamp ORDER BY b", pg_conn)
        assert len(result) == len(nan_values)
        for row in result:
            assert row[0] is None

    finally:
        pg_conn.rollback()
        run_command(
            f"DROP SCHEMA {schema} CASCADE;",
            pg_conn,
        )
        pg_conn.commit()


def test_special_numeric_unbounded_as_double(
    s3, pg_conn, extension, with_default_location
):
    """With the GUC on (default), unbounded numeric becomes float8 which accepts NaN/Inf."""
    schema = "test_special_numeric_unbounded"

    run_command(
        f"""
                CREATE SCHEMA {schema};
                CREATE TABLE {schema}.unbounded(b numeric) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command(
            f"INSERT INTO {schema}.unbounded VALUES ('Inf'), ('NaN'), ('-Infinity')",
            pg_conn,
        )

        result = run_query(f"SELECT b FROM {schema}.unbounded ORDER BY b", pg_conn)

        assert len(result) == 3
        assert result[0][0] == float("-inf")
        assert result[1][0] == float("inf")
        assert math.isnan(result[2][0])
    finally:
        pg_conn.rollback()
        run_command(
            f"DROP SCHEMA {schema} CASCADE;",
            pg_conn,
        )
        pg_conn.commit()

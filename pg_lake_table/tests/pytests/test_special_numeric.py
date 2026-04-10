import pytest
import math
from decimal import Decimal
from utils_pytest import *


def test_special_numeric_bounded(s3, pg_conn, extension, with_default_location):
    """Bounded numeric (precision <= 38) becomes DECIMAL in DuckDB, which rejects NaN."""
    schema = "test_special_numeric_bounded"

    run_command(
        f"""
                CREATE SCHEMA {schema};
                CREATE TABLE {schema}.bounded_err(b numeric(20,10)) USING iceberg
                    WITH (out_of_range_values = 'error');
                CREATE TABLE {schema}.bounded_clamp(b numeric(20,10)) USING iceberg WITH (out_of_range_values = 'clamp');
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


def test_special_values_across_float_and_numeric_types(
    s3, pg_conn, extension, with_default_location
):
    """±Inf, NaN, and NULL across float4, float8, and three numeric flavors.

    Column types and their Iceberg storage:

      f4            float4         → REAL   (IEEE 754 float)
      f8            float8         → DOUBLE (IEEE 754 double)
      num_unbounded numeric        → DOUBLE (unsupported numeric → double)
      num_large     numeric(50,2)  → DOUBLE (precision > 38 → double)
      num_bounded   numeric(10,2)  → DECIMAL(10,2)

    PostgreSQL rejects ±Infinity for any bounded numeric (including
    large-precision), so Infinity rows use NULL for num_large and
    num_bounded.  Bounded DECIMAL cannot represent NaN; with
    out_of_range_values = 'clamp' it is replaced with NULL.
    Large numeric(50,2) maps to DOUBLE, so NaN is preserved there.

    +----+------+------+--------+-------+---------+
    | id | f4   | f8   | num_u  | num_l | num_b   |
    +----+------+------+--------+-------+---------+
    | 1  | NaN  | NaN  | NaN    | NaN   | NaN→NULL|
    | 2  | +Inf | +Inf | +Inf   | NULL  | NULL    |
    | 3  | -Inf | -Inf | -Inf   | NULL  | NULL    |
    | 4  | NULL | NULL | NULL   | NULL  | NULL    |
    | 5  | 1.5  | 1.5  | 1.5    | 1.50  | 1.50    |
    +----+------+------+--------+-------+---------+
    """
    schema = "test_special_all_types"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (
            id int,
            f4 float4,
            f8 float8,
            num_unbounded numeric,
            num_large numeric(50, 2),
            num_bounded numeric(10, 2)
        ) USING iceberg WITH (out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command(
            f"""INSERT INTO {schema}.t VALUES
                (1, 'NaN'::float4,      'NaN'::float8,      'NaN'::numeric,   'NaN'::numeric(50,2),   'NaN'::numeric(10,2)),
                (2, 'Infinity'::float4,  'Infinity'::float8,  'Infinity'::numeric, NULL, NULL),
                (3, '-Infinity'::float4, '-Infinity'::float8, '-Infinity'::numeric, NULL, NULL),
                (4, NULL,                NULL,                 NULL,               NULL,                 NULL),
                (5, 1.5::float4,         1.5::float8,         1.5::numeric,       1.50::numeric(50,2),  1.50::numeric(10,2))""",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT id, f4, f8, num_unbounded, num_large, num_bounded "
            f"FROM {schema}.t ORDER BY id",
            pg_conn,
        )
        assert len(rows) == 5
        r = {row[0]: row for row in rows}

        # --- Row 1: NaN ---
        assert math.isnan(r[1][1]), f"f4 NaN failed: {r[1][1]}"
        assert math.isnan(r[1][2]), f"f8 NaN failed: {r[1][2]}"
        assert math.isnan(r[1][3]), f"num_unbounded NaN failed: {r[1][3]}"
        assert math.isnan(r[1][4]), f"num_large NaN failed: {r[1][4]}"
        assert r[1][5] is None, f"num_bounded NaN should be clamped to NULL: {r[1][5]}"

        # --- Row 2: +Infinity ---
        assert r[2][1] == float("inf"), f"f4 +Inf failed: {r[2][1]}"
        assert r[2][2] == float("inf"), f"f8 +Inf failed: {r[2][2]}"
        assert r[2][3] == float("inf"), f"num_unbounded +Inf failed: {r[2][3]}"
        assert (
            r[2][4] is None
        ), "num_large +Inf row should be NULL (PG rejects Inf for bounded numeric)"
        assert r[2][5] is None, "num_bounded +Inf row should be NULL"

        # --- Row 3: -Infinity ---
        assert r[3][1] == float("-inf"), f"f4 -Inf failed: {r[3][1]}"
        assert r[3][2] == float("-inf"), f"f8 -Inf failed: {r[3][2]}"
        assert r[3][3] == float("-inf"), f"num_unbounded -Inf failed: {r[3][3]}"
        assert (
            r[3][4] is None
        ), "num_large -Inf row should be NULL (PG rejects Inf for bounded numeric)"
        assert r[3][5] is None, "num_bounded -Inf row should be NULL"

        # --- Row 4: NULL ---
        for col in range(1, 6):
            assert r[4][col] is None, f"column {col} NULL failed: {r[4][col]}"

        # --- Row 5: normal value 1.5 ---
        assert r[5][1] == pytest.approx(1.5)
        assert r[5][2] == pytest.approx(1.5)
        assert r[5][3] == pytest.approx(1.5)
        assert r[5][4] == pytest.approx(1.5)
        assert r[5][5] == Decimal("1.50")

    finally:
        pg_conn.rollback()
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()

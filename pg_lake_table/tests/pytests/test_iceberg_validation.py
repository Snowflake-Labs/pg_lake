import re
import pytest
from utils_pytest import *


# =====================================================================
# Non-pushdown path: scalar temporal clamping (INSERT VALUES)
# =====================================================================


@pytest.mark.parametrize(
    "col_type,value,expected_clamped",
    [
        ("date", "infinity", "9999-12-31"),
        ("date", "-infinity", "4713-01-01 BC"),
        ("timestamp", "infinity", "9999-12-31 23:59:59.999999"),
        ("timestamp", "-infinity", "0001-01-01 00:00:00"),
        ("timestamptz", "infinity", "9999-12-31 23:59:59.999999+00"),
        ("timestamptz", "-infinity", "0001-01-01 00:00:00+00"),
    ],
)
def test_scalar_temporal_clamp(
    pg_conn, extension, s3, with_default_location, col_type, value, expected_clamped
):
    """Scalar infinity temporal values are clamped to Iceberg boundaries."""
    schema = f"test_sc_{col_type}_{value.replace('-', 'm')}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TABLE target (col {col_type}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"INSERT INTO target VALUES ('{value}'::{col_type});",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query("SELECT col::text FROM target;", pg_conn)
        assert normalize_bc(result)[0][0] == expected_clamped
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize(
    "col_type,value,expected_err",
    [
        ("date", "infinity", "date out of range"),
        ("date", "-infinity", "date out of range"),
        ("timestamp", "infinity", "timestamp out of range"),
        ("timestamp", "-infinity", "timestamp out of range"),
        ("timestamptz", "infinity", "timestamptz out of range"),
        ("timestamptz", "-infinity", "timestamptz out of range"),
    ],
)
def test_scalar_temporal_error(
    pg_conn, extension, s3, with_default_location, col_type, value, expected_err
):
    """Scalar infinity temporal values are rejected in error mode."""
    schema = f"test_se_{col_type}_{value.replace('-', 'm')}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TABLE target (col {col_type}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            f"INSERT INTO target VALUES ('{value}'::{col_type});",
            pg_conn,
            raise_error=False,
        )
        assert expected_err in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Non-pushdown path: scalar numeric NaN (INSERT VALUES)
# =====================================================================


def test_scalar_numeric_nan_clamp(pg_conn, extension, s3, with_default_location):
    """Scalar NaN in bounded numeric is clamped to NULL."""
    schema = "test_snnc"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TABLE target (col numeric(10,2)) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command("INSERT INTO target VALUES ('NaN'::numeric(10,2));", pg_conn)
        pg_conn.commit()

        result = run_query("SELECT col FROM target;", pg_conn)
        assert result[0][0] is None
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_scalar_numeric_nan_error(pg_conn, extension, s3, with_default_location):
    """Scalar NaN in bounded numeric raises error in error mode."""
    schema = "test_snne"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TABLE target (col numeric(10,2)) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES ('NaN'::numeric(10,2));",
            pg_conn,
            raise_error=False,
        )
        assert "NaN is not supported for Iceberg decimal" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Pushdown path: scalar temporal clamping
#
# Uses Parquet files as source so infinity values survive until the
# pushdown SQL wrapper on INSERT SELECT.
# =====================================================================


@pytest.mark.parametrize(
    "col_type,value,expected_clamped",
    [
        ("date", "infinity", "9999-12-31"),
        ("date", "-infinity", "4713-01-01 BC"),
        ("timestamp", "infinity", "9999-12-31 23:59:59.999999"),
        ("timestamp", "-infinity", "0001-01-01 00:00:00"),
        ("timestamptz", "infinity", "9999-12-31 23:59:59.999999+00"),
        ("timestamptz", "-infinity", "0001-01-01 00:00:00+00"),
    ],
)
def test_scalar_temporal_clamp_pushdown(
    pg_conn, extension, s3, with_default_location, col_type, value, expected_clamped
):
    """Scalar infinity temporal values are clamped via pushdown SQL wrapper."""
    schema = f"test_scp_{col_type}_{value.replace('-', 'm')}"
    parquet_url = (
        f"s3://{TEST_BUCKET}/test_scp_{col_type}_{value.replace('-', 'm')}/data.parquet"
    )

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"COPY (SELECT '{value}'::{col_type} AS col) TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {col_type}) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {col_type}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        result = run_query("SELECT col::text FROM target;", pg_conn)
        assert normalize_bc(result)[0][0] == expected_clamped
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize(
    "col_type,value,expected_err",
    [
        ("date", "infinity", "date out of range"),
        ("date", "-infinity", "date out of range"),
        ("timestamp", "infinity", "timestamp out of range"),
        ("timestamp", "-infinity", "timestamp out of range"),
        ("timestamptz", "infinity", "timestamptz out of range"),
        ("timestamptz", "-infinity", "timestamptz out of range"),
    ],
)
def test_scalar_temporal_error_pushdown(
    pg_conn, extension, s3, with_default_location, col_type, value, expected_err
):
    """Scalar infinity temporal values are rejected via pushdown SQL wrapper."""
    schema = f"test_sep_{col_type}_{value.replace('-', 'm')}"
    parquet_url = (
        f"s3://{TEST_BUCKET}/test_sep_{col_type}_{value.replace('-', 'm')}/data.parquet"
    )

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"COPY (SELECT '{value}'::{col_type} AS col) TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {col_type}) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {col_type}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        with pytest.raises(Exception, match=expected_err):
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Non-pushdown path: temporal clamping in nested types (INSERT VALUES)
# =====================================================================


def test_temporal_clamp_in_array(pg_conn, extension, s3, with_default_location):
    """Infinity dates inside an array are clamped to Iceberg boundaries."""
    schema = "test_tc_arr"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TABLE target (col date[]) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ARRAY['infinity'::date, '2024-01-01'::date, '-infinity'::date]);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT col[1]::text, col[2]::text, col[3]::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [["9999-12-31", "2024-01-01", "4713-01-01 BC"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_error_in_array(pg_conn, extension, s3, with_default_location):
    """Infinity dates inside an array are rejected in error mode."""
    schema = "test_te_arr"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TABLE target (col date[]) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES "
            "(ARRAY['infinity'::date, '2024-01-01'::date]);",
            pg_conn,
            raise_error=False,
        )
        assert "date out of range" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_clamp_in_composite(pg_conn, extension, s3, with_default_location):
    """Infinity date inside a composite field is clamped to Iceberg boundary."""
    schema = "test_tc_comp"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TYPE event_tc AS (id int, happened_at date);", pg_conn)
        run_command("CREATE TABLE target (col event_tc) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, 'infinity'::date)::event_tc), "
            "(ROW(2, '-infinity'::date)::event_tc), "
            "(ROW(3, '2024-06-15'::date)::event_tc);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT (col).id, (col).happened_at::text FROM target ORDER BY (col).id;",
            pg_conn,
        )
        assert normalize_bc(result) == [
            [1, "9999-12-31"],
            [2, "4713-01-01 BC"],
            [3, "2024-06-15"],
        ]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_error_in_composite(pg_conn, extension, s3, with_default_location):
    """Infinity date inside a composite field raises error in error mode."""
    schema = "test_te_comp"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TYPE event_te AS (id int, happened_at date);", pg_conn)
        run_command(
            "CREATE TABLE target (col event_te) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES " "(ROW(1, 'infinity'::date)::event_te);",
            pg_conn,
            raise_error=False,
        )
        assert "date out of range" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_clamp_in_map(pg_conn, extension, s3, with_default_location):
    """Infinity date inside a map value is clamped to Iceberg boundary."""
    schema = "test_tc_map"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("text", "date")
        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"INSERT INTO target VALUES "
            f"(ARRAY[('k1', 'infinity'::date), ('k2', '2024-01-01'::date)]::{map_type_name});",
            pg_conn,
        )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col[1]).key, (col[1]).val::text, "
            "(col[2]).key, (col[2]).val::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [["k1", "9999-12-31", "k2", "2024-01-01"]]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_error_in_map(pg_conn, extension, s3, with_default_location):
    """Infinity date inside a map value raises error in error mode."""
    schema = "test_te_map"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("text", "date")
        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            f"INSERT INTO target VALUES "
            f"(ARRAY[('k1', 'infinity'::date)]::{map_type_name});",
            pg_conn,
            raise_error=False,
        )
        assert "date out of range" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Non-pushdown path: numeric NaN in nested types (INSERT VALUES)
#
# Numeric columns block INSERT SELECT pushdown, so NaN validation is
# always exercised through the non-pushdown datum-level path.
# =====================================================================


def test_numeric_nan_clamp_in_array(pg_conn, extension, s3, with_default_location):
    """NaN inside a numeric array is clamped to NULL."""
    schema = "test_nn_arr"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TABLE target (col numeric(10,2)[]) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ARRAY['NaN'::numeric(10,2), 1.50::numeric(10,2), 'NaN'::numeric(10,2)]);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT col[1], col[2], col[3] FROM target;",
            pg_conn,
        )
        assert result[0][0] is None
        assert float(result[0][1]) == 1.5
        assert result[0][2] is None
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_numeric_nan_error_in_array(pg_conn, extension, s3, with_default_location):
    """NaN inside a numeric array raises error in error mode."""
    schema = "test_ne_arr"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TABLE target (col numeric(10,2)[]) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES "
            "(ARRAY['NaN'::numeric(10,2), 1.50::numeric(10,2)]);",
            pg_conn,
            raise_error=False,
        )
        assert "NaN is not supported for Iceberg decimal" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_numeric_nan_clamp_in_composite(pg_conn, extension, s3, with_default_location):
    """NaN inside a composite numeric field is clamped to NULL."""
    schema = "test_nn_comp"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE measurement_nc AS (id int, val numeric(10,2));",
            pg_conn,
        )
        run_command(
            "CREATE TABLE target (col measurement_nc) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, 'NaN'::numeric(10,2))::measurement_nc), "
            "(ROW(2, 3.14::numeric(10,2))::measurement_nc);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT (col).id, (col).val FROM target ORDER BY (col).id;",
            pg_conn,
        )
        assert result[0][0] == 1
        assert result[0][1] is None
        assert result[1][0] == 2
        assert float(result[1][1]) == pytest.approx(3.14)
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_numeric_nan_error_in_composite(pg_conn, extension, s3, with_default_location):
    """NaN inside a composite numeric field raises error in error mode."""
    schema = "test_ne_comp"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE measurement_ne AS (id int, val numeric(10,2));",
            pg_conn,
        )
        run_command(
            "CREATE TABLE target (col measurement_ne) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, 'NaN'::numeric(10,2))::measurement_ne);",
            pg_conn,
            raise_error=False,
        )
        assert "NaN is not supported for Iceberg decimal" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_numeric_nan_clamp_in_map(pg_conn, extension, s3, with_default_location):
    """NaN inside a map value's bounded numeric field is clamped to NULL.

    map_type.create() strips numeric precision, so we wrap bounded numeric
    in a composite to preserve it through the map type creation.
    """
    schema = "test_nn_map"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE num_val AS (amount numeric(10,2));",
            pg_conn,
        )
        pg_conn.commit()

        map_type_name = create_map_type("int", f"{schema}.num_val")
        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"INSERT INTO target VALUES "
            f"(ARRAY[(1, ROW('NaN'::numeric(10,2))::num_val), "
            f"(2, ROW(3.14::numeric(10,2))::num_val)]::{map_type_name});",
            pg_conn,
        )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col[1]).key, ((col[1]).val).amount, "
            "(col[2]).key, ((col[2]).val).amount FROM target;",
            pg_conn,
        )
        assert result[0][0] == 1
        assert result[0][1] is None
        assert result[0][2] == 2
        assert float(result[0][3]) == pytest.approx(3.14)
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_numeric_nan_error_in_map(pg_conn, extension, s3, with_default_location):
    """NaN inside a map value's bounded numeric field raises error in error mode."""
    schema = "test_ne_map"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE num_val_ne AS (amount numeric(10,2));",
            pg_conn,
        )
        pg_conn.commit()

        map_type_name = create_map_type("int", f"{schema}.num_val_ne")
        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            f"INSERT INTO target VALUES "
            f"(ARRAY[(1, ROW('NaN'::numeric(10,2))::num_val_ne)]::{map_type_name});",
            pg_conn,
            raise_error=False,
        )
        assert "NaN is not supported for Iceberg decimal" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Deeply nested validation: composite → numeric array
# =====================================================================


def test_deeply_nested_numeric_nan_clamp(pg_conn, extension, s3, with_default_location):
    """NaN inside a numeric array within a composite is clamped.

    Exercises recursive datum validation: composite → array → numeric(10,2).
    """
    schema = "test_deep_clamp"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE measurements AS (id int, readings numeric(10,2)[]);",
            pg_conn,
        )
        run_command(
            "CREATE TABLE target (col measurements) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, ARRAY['NaN'::numeric(10,2), 2.50, 'NaN'::numeric(10,2), 7.00])::measurements), "
            "(ROW(2, ARRAY[1.11, 2.22, 3.33])::measurements);",
            pg_conn,
        )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col).id, (col).readings FROM target ORDER BY (col).id;",
            pg_conn,
        )
        assert result[0][0] == 1
        readings_1 = result[0][1]
        assert readings_1[0] is None
        assert float(readings_1[1]) == pytest.approx(2.50)
        assert readings_1[2] is None
        assert float(readings_1[3]) == pytest.approx(7.00)

        assert result[1][0] == 2
        readings_2 = result[1][1]
        assert float(readings_2[0]) == pytest.approx(1.11)
        assert float(readings_2[1]) == pytest.approx(2.22)
        assert float(readings_2[2]) == pytest.approx(3.33)
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_deeply_nested_numeric_nan_error(pg_conn, extension, s3, with_default_location):
    """NaN inside a numeric array within a composite raises error.

    Exercises recursive datum validation in error mode:
    composite → array → numeric(10,2).
    """
    schema = "test_deep_error"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE measurements_e AS (id int, readings numeric(10,2)[]);",
            pg_conn,
        )
        run_command(
            "CREATE TABLE target (col measurements_e) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, ARRAY['NaN'::numeric(10,2), 2.50])::measurements_e);",
            pg_conn,
            raise_error=False,
        )
        assert "NaN is not supported for Iceberg decimal" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Pushdown path: temporal clamping in nested types
#
# Uses Parquet files as source (no Iceberg validation) so that infinity
# values survive until they reach the pushdown SQL wrapper on the
# INSERT SELECT into the target Iceberg table.
# =====================================================================


def test_temporal_clamp_in_array_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """Infinity dates inside an array are clamped via pushdown SQL wrapper."""
    schema = "test_tc_arr_pd"
    parquet_url = f"s3://{TEST_BUCKET}/test_tc_arr_pd/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"COPY (SELECT ARRAY['infinity'::date, '2024-01-01'::date, "
            f"'-infinity'::date] AS col) TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col date[]) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            "CREATE TABLE target (col date[]) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        result = run_query(
            "SELECT col[1]::text, col[2]::text, col[3]::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [["9999-12-31", "2024-01-01", "4713-01-01 BC"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_error_in_array_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """Infinity dates inside an array raise errors via pushdown SQL wrapper."""
    schema = "test_te_arr_pd"
    parquet_url = f"s3://{TEST_BUCKET}/test_te_arr_pd/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"COPY (SELECT ARRAY['infinity'::date, '2024-01-01'::date] AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col date[]) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            "CREATE TABLE target (col date[]) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        with pytest.raises(Exception, match="date out of range"):
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_clamp_in_composite_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """Infinity date inside a composite is clamped via pushdown SQL wrapper."""
    schema = "test_tc_comp_pd"
    parquet_url = f"s3://{TEST_BUCKET}/test_tc_comp_pd/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TYPE event_tc_pd AS (id int, happened_at date);",
            pg_conn,
        )

        run_command(
            f"COPY (SELECT ROW(1, 'infinity'::date)::event_tc_pd AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col event_tc_pd) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            "CREATE TABLE target (col event_tc_pd) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        result = run_query(
            "SELECT (col).id, (col).happened_at::text FROM target;",
            pg_conn,
        )
        assert result == [[1, "9999-12-31"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_error_in_composite_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """Infinity date inside a composite raises error via pushdown SQL wrapper."""
    schema = "test_te_comp_pd"
    parquet_url = f"s3://{TEST_BUCKET}/test_te_comp_pd/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TYPE event_te_pd AS (id int, happened_at date);",
            pg_conn,
        )

        run_command(
            f"COPY (SELECT ROW(1, 'infinity'::date)::event_te_pd AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col event_te_pd) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            "CREATE TABLE target (col event_te_pd) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        with pytest.raises(Exception, match="date out of range"):
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_clamp_in_map_pushdown(pg_conn, extension, s3, with_default_location):
    """Infinity date inside a map value is clamped via pushdown SQL wrapper."""
    schema = "test_tc_map_pd"
    parquet_url = f"s3://{TEST_BUCKET}/test_tc_map_pd/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("text", "date")

        run_command(
            f"COPY (SELECT ARRAY[('k1', 'infinity'::date), "
            f"('k2', '2024-01-01'::date)]::{map_type_name} AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {map_type_name}) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col[1]).key, (col[1]).val::text, "
            "(col[2]).key, (col[2]).val::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [["k1", "9999-12-31", "k2", "2024-01-01"]]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_error_in_map_pushdown(pg_conn, extension, s3, with_default_location):
    """Infinity date inside a map value raises error via pushdown SQL wrapper."""
    schema = "test_te_map_pd"
    parquet_url = f"s3://{TEST_BUCKET}/test_te_map_pd/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("text", "date")

        run_command(
            f"COPY (SELECT ARRAY[('k1', 'infinity'::date)]::{map_type_name} AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {map_type_name}) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        with pytest.raises(Exception, match="date out of range"):
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# EXPLAIN verification: nested temporal wrapping patterns
#
# Verifies the pushdown SQL wrapper generates the correct DuckDB
# expressions for nested types (list_transform for arrays,
# struct_pack for composites, map_from_entries for maps).
# =====================================================================


@pytest.mark.parametrize(
    "col_type,expected_pattern",
    [
        ("date[]", r"list_transform.*WHEN"),
        ("timestamp[]", r"list_transform.*WHEN"),
        ("timestamptz[]", r"list_transform.*WHEN"),
    ],
)
def test_explain_shows_list_transform_for_temporal_array(
    pg_conn, extension, s3, with_default_location, col_type, expected_pattern
):
    """EXPLAIN output for temporal array columns shows list_transform wrapping."""
    schema = "test_explain_lt"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TABLE explain_target (col {col_type}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        base_type = col_type.rstrip("[]")
        result = run_query(
            "EXPLAIN (VERBOSE) INSERT INTO explain_target"
            f" SELECT ARRAY['2024-01-01'::{base_type}] AS col;",
            pg_conn,
        )
        explain_text = "\n".join(line[0] for line in result)

        assert re.search(
            expected_pattern, explain_text, re.DOTALL
        ), f"Expected list_transform pattern not found in EXPLAIN:\n{explain_text}"
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_explain_shows_struct_pack_for_temporal_composite(
    pg_conn, extension, s3, with_default_location
):
    """EXPLAIN output for composite with temporal field shows struct_pack wrapping."""
    schema = "test_explain_sp"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TYPE event_explain AS (id int, happened_at date);",
            pg_conn,
        )
        run_command(
            "CREATE TABLE explain_target (col event_explain) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "CREATE TABLE explain_source (col event_explain) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "EXPLAIN (VERBOSE) INSERT INTO explain_target"
            " SELECT * FROM explain_source;",
            pg_conn,
        )
        explain_text = "\n".join(line[0] for line in result)

        assert re.search(
            r"struct_pack.*WHEN", explain_text, re.DOTALL
        ), f"Expected struct_pack pattern not found in EXPLAIN:\n{explain_text}"
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_explain_shows_map_from_entries_for_temporal_map(
    pg_conn, extension, s3, with_default_location
):
    """EXPLAIN output for map with temporal value shows map_from_entries wrapping."""
    schema = "test_explain_mfe"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("text", "date")
        run_command(
            f"CREATE TABLE explain_target (col {map_type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"CREATE TABLE explain_source (col {map_type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "EXPLAIN (VERBOSE) INSERT INTO explain_target"
            " SELECT * FROM explain_source;",
            pg_conn,
        )
        explain_text = "\n".join(line[0] for line in result)

        assert re.search(
            r"map_from_entries.*WHEN", explain_text, re.DOTALL
        ), f"Expected map_from_entries pattern not found in EXPLAIN:\n{explain_text}"
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()

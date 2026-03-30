import re
import pytest
from utils_pytest import *


# =====================================================================
# Shared parametrize data for temporal boundary values
# =====================================================================

TEMPORAL_CLAMP_PARAMS = [
    ("date", "2024-01-01", "9999-12-31", "4713-01-01 BC"),
    (
        "timestamp",
        "2024-01-01 12:00:00",
        "9999-12-31 23:59:59.999999",
        "0001-01-01 00:00:00",
    ),
    (
        "timestamptz",
        "2024-01-01 12:00:00+00",
        "9999-12-31 23:59:59.999999+00",
        "0001-01-01 00:00:00+00",
    ),
]

TEMPORAL_ERROR_PARAMS = [
    ("date", "date out of range"),
    ("timestamp", "timestamp out of range"),
    ("timestamptz", "timestamptz out of range"),
]


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

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
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

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
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
# Non-pushdown path: temporal clamping/error in nested types
#
# Parametrized by temporal type (date, timestamp, timestamptz) to
# verify correct boundary literals inside each container kind.
# =====================================================================


@pytest.mark.parametrize(
    "col_type,normal_val,clamped_inf,clamped_neg_inf", TEMPORAL_CLAMP_PARAMS
)
def test_temporal_clamp_in_array(
    pg_conn,
    extension,
    s3,
    with_default_location,
    col_type,
    normal_val,
    clamped_inf,
    clamped_neg_inf,
):
    """Infinity values inside a temporal array are clamped to Iceberg boundaries."""
    schema = f"test_tc_arr_{col_type}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(f"CREATE TABLE target (col {col_type}[]) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            f"INSERT INTO target VALUES "
            f"(ARRAY['infinity'::{col_type}, '{normal_val}'::{col_type}, "
            f"'-infinity'::{col_type}]);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT col[1]::text, col[2]::text, col[3]::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [[clamped_inf, normal_val, clamped_neg_inf]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("col_type,expected_err", TEMPORAL_ERROR_PARAMS)
def test_temporal_error_in_array(
    pg_conn, extension, s3, with_default_location, col_type, expected_err
):
    """Infinity values inside a temporal array are rejected in error mode."""
    schema = f"test_te_arr_{col_type}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TABLE target (col {col_type}[]) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            f"INSERT INTO target VALUES "
            f"(ARRAY['infinity'::{col_type}, '2024-01-01'::{col_type}]);",
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


@pytest.mark.parametrize(
    "col_type,normal_val,clamped_inf,clamped_neg_inf",
    [
        ("date", "2024-06-15", "9999-12-31", "4713-01-01 BC"),
        (
            "timestamptz",
            "2024-06-15 12:00:00+00",
            "9999-12-31 23:59:59.999999+00",
            "0001-01-01 00:00:00+00",
        ),
    ],
)
def test_temporal_clamp_in_composite(
    pg_conn,
    extension,
    s3,
    with_default_location,
    col_type,
    normal_val,
    clamped_inf,
    clamped_neg_inf,
):
    """Infinity value inside a composite temporal field is clamped."""
    schema = f"test_tc_comp_{col_type}"
    type_name = f"event_tc_{col_type}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TYPE {type_name} AS (id int, happened_at {col_type});",
            pg_conn,
        )
        run_command(f"CREATE TABLE target (col {type_name}) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            f"INSERT INTO target VALUES "
            f"(ROW(1, 'infinity'::{col_type})::{type_name}), "
            f"(ROW(2, '-infinity'::{col_type})::{type_name}), "
            f"(ROW(3, '{normal_val}'::{col_type})::{type_name});",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT (col).id, (col).happened_at::text FROM target ORDER BY (col).id;",
            pg_conn,
        )
        assert normalize_bc(result) == [
            [1, clamped_inf],
            [2, clamped_neg_inf],
            [3, normal_val],
        ]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize(
    "col_type,expected_err",
    [
        ("date", "date out of range"),
        ("timestamptz", "timestamptz out of range"),
    ],
)
def test_temporal_error_in_composite(
    pg_conn, extension, s3, with_default_location, col_type, expected_err
):
    """Infinity value inside a composite temporal field raises error."""
    schema = f"test_te_comp_{col_type}"
    type_name = f"event_te_{col_type}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TYPE {type_name} AS (id int, happened_at {col_type});",
            pg_conn,
        )
        run_command(
            f"CREATE TABLE target (col {type_name}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            f"INSERT INTO target VALUES "
            f"(ROW(1, 'infinity'::{col_type})::{type_name});",
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

    Exercises recursive datum validation: composite -> array -> numeric(10,2).
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
    composite -> array -> numeric(10,2).
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
# Pushdown path: temporal clamping/error in nested types
#
# Uses Parquet files as source (no Iceberg validation) so that infinity
# values survive until they reach the pushdown SQL wrapper on the
# INSERT SELECT into the target Iceberg table.
#
# Parametrized by temporal type like the non-pushdown counterparts.
# =====================================================================


@pytest.mark.parametrize(
    "col_type,normal_val,clamped_inf,clamped_neg_inf", TEMPORAL_CLAMP_PARAMS
)
def test_temporal_clamp_in_array_pushdown(
    pg_conn,
    extension,
    s3,
    with_default_location,
    col_type,
    normal_val,
    clamped_inf,
    clamped_neg_inf,
):
    """Infinity values inside a temporal array are clamped via pushdown."""
    schema = f"test_tc_arr_pd_{col_type}"
    parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"COPY (SELECT ARRAY['infinity'::{col_type}, '{normal_val}'::{col_type}, "
            f"'-infinity'::{col_type}] AS col) TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {col_type}[]) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {col_type}[]) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        result = run_query(
            "SELECT col[1]::text, col[2]::text, col[3]::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [[clamped_inf, normal_val, clamped_neg_inf]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("col_type,expected_err", TEMPORAL_ERROR_PARAMS)
def test_temporal_error_in_array_pushdown(
    pg_conn, extension, s3, with_default_location, col_type, expected_err
):
    """Infinity values inside a temporal array raise errors via pushdown."""
    schema = f"test_te_arr_pd_{col_type}"
    parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"COPY (SELECT ARRAY['infinity'::{col_type}, "
            f"'2024-01-01'::{col_type}] AS col) TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {col_type}[]) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {col_type}[]) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
        with pytest.raises(Exception, match=expected_err):
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize(
    "col_type,clamped_inf",
    [
        ("date", "9999-12-31"),
        ("timestamptz", "9999-12-31 23:59:59.999999+00"),
    ],
)
def test_temporal_clamp_in_composite_pushdown(
    pg_conn, extension, s3, with_default_location, col_type, clamped_inf
):
    """Infinity value inside a composite is clamped via pushdown SQL wrapper."""
    schema = f"test_tc_comp_pd_{col_type}"
    parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
    type_name = f"event_tc_pd_{col_type}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TYPE {type_name} AS (id int, happened_at {col_type});",
            pg_conn,
        )

        run_command(
            f"COPY (SELECT ROW(1, 'infinity'::{col_type})::{type_name} AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {type_name}) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        result = run_query(
            "SELECT (col).id, (col).happened_at::text FROM target;",
            pg_conn,
        )
        assert result == [[1, clamped_inf]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize(
    "col_type,expected_err",
    [
        ("date", "date out of range"),
        ("timestamptz", "timestamptz out of range"),
    ],
)
def test_temporal_error_in_composite_pushdown(
    pg_conn, extension, s3, with_default_location, col_type, expected_err
):
    """Infinity value inside a composite raises error via pushdown."""
    schema = f"test_te_comp_pd_{col_type}"
    parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
    type_name = f"event_te_pd_{col_type}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TYPE {type_name} AS (id int, happened_at {col_type});",
            pg_conn,
        )

        run_command(
            f"COPY (SELECT ROW(1, 'infinity'::{col_type})::{type_name} AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col {type_name}) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command(
            f"CREATE TABLE target (col {type_name}) USING iceberg"
            f" WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
        with pytest.raises(Exception, match=expected_err):
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

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
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

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
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
# Cross-container nesting: array of composites — composite(int, date)[]
#
# Exercises struct_pack inside list_transform in the pushdown SQL
# wrapper, producing nested lambda variables (_x0).  In the datum
# path it tests array -> composite recursion.
# =====================================================================


@pytest.mark.parametrize("policy", ["clamp", "error"])
def test_temporal_in_array_of_composites(
    pg_conn, extension, s3, with_default_location, policy
):
    """Infinity date in composite(int,date)[] is clamped or raises error."""
    schema = f"test_t_arr_comp_{policy}"
    type_name = f"event_ac_{policy}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(f"CREATE TYPE {type_name} AS (id int, happened_at date);", pg_conn)
        options = " WITH (out_of_range_values = 'error')" if policy == "error" else ""
        run_command(
            f"CREATE TABLE target (col {type_name}[])" f" USING iceberg{options};",
            pg_conn,
        )
        pg_conn.commit()

        insert_sql = (
            f"INSERT INTO target VALUES "
            f"(ARRAY[ROW(1, 'infinity'::date)::{type_name}, "
            f"ROW(2, '2024-06-15'::date)::{type_name}, "
            f"ROW(3, '-infinity'::date)::{type_name}]);"
        )

        if policy == "error":
            err = run_command(insert_sql, pg_conn, raise_error=False)
            assert "date out of range" in str(err)
            pg_conn.rollback()
        else:
            run_command(insert_sql, pg_conn)
            pg_conn.commit()

            result = run_query(
                "SELECT (col[1]).id, (col[1]).happened_at::text, "
                "(col[2]).id, (col[2]).happened_at::text, "
                "(col[3]).id, (col[3]).happened_at::text FROM target;",
                pg_conn,
            )
            assert normalize_bc(result) == [
                [1, "9999-12-31", 2, "2024-06-15", 3, "4713-01-01 BC"]
            ]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_temporal_clamp_in_array_of_composites_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """Infinity dates in array of composites are clamped via pushdown."""
    schema = "test_tc_arr_comp_pd"
    parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TYPE event_ac_pd AS (id int, happened_at date);", pg_conn)

        run_command(
            f"COPY (SELECT ARRAY[ROW(1, 'infinity'::date)::event_ac_pd, "
            f"ROW(2, '2024-06-15'::date)::event_ac_pd] AS col) "
            f"TO '{parquet_url}';",
            pg_conn,
        )

        run_command(
            f"CREATE FOREIGN TABLE source (col event_ac_pd[]) "
            f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
            pg_conn,
        )

        run_command("CREATE TABLE target (col event_ac_pd[]) USING iceberg;", pg_conn)
        pg_conn.commit()

        assert_query_pushdownable("INSERT INTO target SELECT * FROM source;", pg_conn)
        run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        pg_conn.commit()

        result = run_query(
            "SELECT (col[1]).id, (col[1]).happened_at::text, "
            "(col[2]).id, (col[2]).happened_at::text FROM target;",
            pg_conn,
        )
        assert result == [[1, "9999-12-31", 2, "2024-06-15"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Cross-container nesting: composite containing a map
# =====================================================================


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_in_composite_with_map(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Infinity date inside a map within a composite is clamped (composite -> map)."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_comp_map{suffix}"
    type_name = f"comp_with_map{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("text", "date")
        run_command(
            f"CREATE TYPE {type_name} AS (id int, tags {map_type_name});",
            pg_conn,
        )

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT ROW(1, ARRAY[('k1', 'infinity'::date), "
                f"('k2', '2024-01-01'::date)]::{map_type_name})::{type_name} AS col) "
                f"TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (col {type_name}) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(f"CREATE TABLE target (col {type_name}) USING iceberg;", pg_conn)
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ROW(1, ARRAY[('k1', 'infinity'::date), "
                f"('k2', '2024-01-01'::date)]::{map_type_name})::{type_name});",
                pg_conn,
            )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col).id, ((col).tags[1]).key, ((col).tags[1]).val::text, "
            "((col).tags[2]).key, ((col).tags[2]).val::text FROM target;",
            pg_conn,
        )
        assert normalize_bc(result) == [[1, "k1", "9999-12-31", "k2", "2024-01-01"]]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Cross-container nesting: map with composite value
# =====================================================================


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_in_map_with_composite_value(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Infinity date inside a composite map value is clamped (map -> composite)."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_map_comp{suffix}"
    comp_type = f"event_for_map{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(f"CREATE TYPE {comp_type} AS (id int, happened_at date);", pg_conn)
        pg_conn.commit()

        map_type_name = create_map_type("text", f"{schema}.{comp_type}")

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT ARRAY[('k1', ROW(1, 'infinity'::date)::{comp_type}), "
                f"('k2', ROW(2, '2024-06-15'::date)::{comp_type})]::{map_type_name} "
                f"AS col) TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (col {map_type_name}) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg;", pg_conn
        )
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ARRAY[('k1', ROW(1, 'infinity'::date)::{comp_type}), "
                f"('k2', ROW(2, '2024-06-15'::date)::{comp_type})]::{map_type_name});",
                pg_conn,
            )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col[1]).key, ((col[1]).val).id, ((col[1]).val).happened_at::text, "
            "(col[2]).key, ((col[2]).val).id, ((col[2]).val).happened_at::text "
            "FROM target;",
            pg_conn,
        )
        assert result == [["k1", 1, "9999-12-31", "k2", 2, "2024-06-15"]]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Map with temporal key
#
# Exercises the key validation branch in AppendIcebergValidationExpression
# and the key-side recursion in IcebergErrorOrClampNestedDatum.
# =====================================================================


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_in_map_temporal_key(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Infinity date used as a map key is clamped to Iceberg boundary."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_mapk{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        map_type_name = create_map_type("date", "text")

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT ARRAY[('infinity'::date, 'future'), "
                f"('2024-01-01'::date, 'normal')]::{map_type_name} AS col) "
                f"TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (col {map_type_name}) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(
            f"CREATE TABLE target (col {map_type_name}) USING iceberg;", pg_conn
        )
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ARRAY[('infinity'::date, 'future'), "
                f"('2024-01-01'::date, 'normal')]::{map_type_name});",
                pg_conn,
            )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col[1]).key::text, (col[1]).val, "
            "(col[2]).key::text, (col[2]).val FROM target;",
            pg_conn,
        )
        assert result == [["9999-12-31", "future", "2024-01-01", "normal"]]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Domain over temporal type
#
# Exercises the domain-unwrap branch in TypeNeedsIcebergValidation and
# IcebergErrorOrClampNestedDatum for non-map domain types.
# Domains block INSERT SELECT pushdown, so only the datum-level path
# is exercised.
# =====================================================================


def test_temporal_clamp_domain_over_date(pg_conn, extension, s3, with_default_location):
    """Infinity value in a domain-over-date column is clamped."""
    schema = "test_tc_domain"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE DOMAIN date_alias AS date;", pg_conn)
        run_command("CREATE TABLE target (col date_alias) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "('infinity'::date), ('-infinity'::date), ('2024-06-15'::date);",
            pg_conn,
        )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query("SELECT col::text FROM target;", pg_conn)
        actual = set(r[0] for r in normalize_bc(result))
        assert actual == {"4713-01-01 BC", "2024-06-15", "9999-12-31"}
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Composite type reuse across tables
# =====================================================================


def test_composite_type_reuse_validation(pg_conn, extension, s3, with_default_location):
    """Two tables sharing one composite type both clamp correctly."""
    schema = "test_comp_reuse"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TYPE shared_event AS (id int, happened_at date);", pg_conn)
        run_command("CREATE TABLE target_a (col shared_event) USING iceberg;", pg_conn)
        run_command("CREATE TABLE target_b (col shared_event) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            "INSERT INTO target_a VALUES " "(ROW(1, 'infinity'::date)::shared_event);",
            pg_conn,
        )
        run_command(
            "INSERT INTO target_b VALUES " "(ROW(2, '-infinity'::date)::shared_event);",
            pg_conn,
        )
        pg_conn.commit()

        result_a = run_query(
            "SELECT (col).id, (col).happened_at::text FROM target_a;", pg_conn
        )
        result_b = run_query(
            "SELECT (col).id, (col).happened_at::text FROM target_b;", pg_conn
        )
        assert result_a == [[1, "9999-12-31"]]
        assert normalize_bc(result_b) == [[2, "4713-01-01 BC"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Field name quoting in struct_pack
#
# The pushdown wrapper uses quote_identifier for composite field names.
# A field named with a SQL reserved word ("order") exercises this path.
# =====================================================================


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_composite_reserved_field_name(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Composite with a reserved-word field name is clamped correctly."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_quoted{suffix}"
    type_name = f"quoted_comp{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(f'CREATE TYPE {type_name} AS (id int, "order" date);', pg_conn)

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT ROW(1, 'infinity'::date)::{type_name} AS col) "
                f"TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (col {type_name}) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(f"CREATE TABLE target (col {type_name}) USING iceberg;", pg_conn)
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ROW(1, 'infinity'::date)::{type_name}), "
                f"(ROW(2, '2024-03-01'::date)::{type_name});",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            f'SELECT (col).id, (col)."order"::text FROM target ORDER BY (col).id;',
            pg_conn,
        )
        if use_pushdown:
            assert result == [[1, "9999-12-31"]]
        else:
            assert result == [[1, "9999-12-31"], [2, "2024-03-01"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Empty arrays and NULL container columns
# =====================================================================


def test_empty_temporal_array_clamp(pg_conn, extension, s3, with_default_location):
    """An empty date array passes through validation without error."""
    schema = "test_empty_arr"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TABLE target (col date[]) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES (ARRAY[]::date[]);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query("SELECT col FROM target;", pg_conn)
        assert result[0][0] == []
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_null_container_columns_passthrough(
    pg_conn, extension, s3, with_default_location
):
    """NULL values in temporal-containing container columns pass through."""
    schema = "test_null_cont"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TYPE event_null AS (id int, happened_at date);", pg_conn)
        run_command(
            "CREATE TABLE target ("
            "arr_col date[], "
            "comp_col event_null, "
            "scalar_col date"
            ") USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES (NULL, NULL, NULL);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "SELECT arr_col IS NULL, comp_col IS NULL, scalar_col IS NULL "
            "FROM target;",
            pg_conn,
        )
        assert result == [[True, True, True]]
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


def test_explain_array_of_composites_shows_nested_wrapping(
    pg_conn, extension, s3, with_default_location
):
    """EXPLAIN for composite(int,date)[] shows list_transform wrapping struct_pack."""
    schema = "test_explain_ac"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TYPE event_explain_ac AS (id int, happened_at date);",
            pg_conn,
        )
        run_command(
            "CREATE TABLE explain_target (col event_explain_ac[]) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "CREATE TABLE explain_source (col event_explain_ac[]) USING iceberg;",
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
            r"list_transform.*struct_pack.*WHEN", explain_text, re.DOTALL
        ), f"Expected list_transform+struct_pack pattern not found in EXPLAIN:\n{explain_text}"
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# Quoted identifier coverage: column names, type names, field names
#
# The pushdown SQL wrapper uses quote_identifier() for column names
# and composite field names in the generated DuckDB SQL.  These tests
# exercise identifiers that require quoting: SQL reserved words
# (e.g. "select", "from") and mixed-case names (e.g. "EventDate").
# =====================================================================


@pytest.mark.parametrize(
    "col_name_quoted,name_tag",
    [
        ('"select"', "rw"),
        ('"EventDate"', "mc"),
    ],
    ids=["reserved_word", "mixed_case"],
)
@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_quoted_column_name(
    pg_conn,
    extension,
    s3,
    with_default_location,
    use_pushdown,
    col_name_quoted,
    name_tag,
):
    """Temporal column whose name requires quoting is clamped correctly."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_qcol_{name_tag}{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f"CREATE TABLE target (id int, {col_name_quoted} date) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT 1 AS id, 'infinity'::date AS {col_name_quoted}) "
                f"TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (id int, {col_name_quoted} date) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                "INSERT INTO target VALUES "
                "(1, 'infinity'::date), (2, '2024-06-15'::date);",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            f"SELECT id, {col_name_quoted}::text FROM target ORDER BY id;",
            pg_conn,
        )
        if use_pushdown:
            assert result == [[1, "9999-12-31"]]
        else:
            assert result == [[1, "9999-12-31"], [2, "2024-06-15"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_quoted_column_with_array(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Temporal array in column named with reserved word is clamped."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_qcolarr{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            'CREATE TABLE target ("from" date[]) USING iceberg;',
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"""COPY (SELECT ARRAY['infinity'::date, '2024-01-01'::date] AS "from") TO '{parquet_url}';""",
                pg_conn,
            )
            run_command(
                f"""CREATE FOREIGN TABLE source ("from" date[]) """
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                "INSERT INTO target VALUES "
                "(ARRAY['infinity'::date, '2024-01-01'::date]);",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            'SELECT "from"[1]::text, "from"[2]::text FROM target;',
            pg_conn,
        )
        assert result == [["9999-12-31", "2024-01-01"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_composite_mixed_case_fields(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Composite with mixed-case field names is clamped via struct_pack."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_mcfld{suffix}"
    type_name = f"event_mc{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f'CREATE TYPE {type_name} AS ("RecordId" int, "EventDate" date);',
            pg_conn,
        )

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT ROW(1, 'infinity'::date)::{type_name} AS col) "
                f"TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (col {type_name}) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(
            f"CREATE TABLE target (col {type_name}) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ROW(1, 'infinity'::date)::{type_name}), "
                f"(ROW(2, '2024-03-01'::date)::{type_name});",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            'SELECT (col)."RecordId", (col)."EventDate"::text '
            'FROM target ORDER BY (col)."RecordId";',
            pg_conn,
        )
        if use_pushdown:
            assert result == [[1, "9999-12-31"]]
        else:
            assert result == [[1, "9999-12-31"], [2, "2024-03-01"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_multiple_quoted_columns(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Multiple temporal columns with quoted names are all clamped."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_mqcol{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TABLE target ("
            'id int, "from" date, "Table" timestamptz'
            ") USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"""COPY (SELECT 1 AS id, 'infinity'::date AS "from", """
                f"""'infinity'::timestamptz AS "Table") TO '{parquet_url}';""",
                pg_conn,
            )
            run_command(
                f"""CREATE FOREIGN TABLE source (id int, "from" date, "Table" timestamptz) """
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                "INSERT INTO target VALUES "
                "(1, 'infinity'::date, 'infinity'::timestamptz);",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            'SELECT id, "from"::text, "Table"::text FROM target;',
            pg_conn,
        )
        assert result == [[1, "9999-12-31", "9999-12-31 23:59:59.999999+00"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_quoted_column_with_quoted_composite_fields(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Column and composite field names both requiring quoting are handled."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_qcolqf{suffix}"
    type_name = f"comp_qq{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f'CREATE TYPE {type_name} AS ("group" int, "order" date);',
            pg_conn,
        )

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"""COPY (SELECT ROW(1, 'infinity'::date)::{type_name} AS "Data") TO '{parquet_url}';""",
                pg_conn,
            )
            run_command(
                f"""CREATE FOREIGN TABLE source ("Data" {type_name}) """
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(
            f'CREATE TABLE target ("Data" {type_name}) USING iceberg;',
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ROW(1, 'infinity'::date)::{type_name});",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            'SELECT ("Data")."group", ("Data")."order"::text FROM target;',
            pg_conn,
        )
        assert result == [[1, "9999-12-31"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_clamp_array_of_composites_quoted_fields(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Array of composites with mixed-case field names is clamped."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_tc_acqf{suffix}"
    type_name = f"event_acq{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            f'CREATE TYPE {type_name} AS ("Id" int, "OccurredAt" date);',
            pg_conn,
        )

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"COPY (SELECT ARRAY["
                f"ROW(1, 'infinity'::date)::{type_name}, "
                f"ROW(2, '2024-06-15'::date)::{type_name}] AS col) "
                f"TO '{parquet_url}';",
                pg_conn,
            )
            run_command(
                f"CREATE FOREIGN TABLE source (col {type_name}[]) "
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )

        run_command(
            f"CREATE TABLE target (col {type_name}[]) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(
                f"INSERT INTO target VALUES "
                f"(ARRAY[ROW(1, 'infinity'::date)::{type_name}, "
                f"ROW(2, '2024-06-15'::date)::{type_name}]);",
                pg_conn,
            )
        pg_conn.commit()

        result = run_query(
            'SELECT (col[1])."Id", (col[1])."OccurredAt"::text, '
            '(col[2])."Id", (col[2])."OccurredAt"::text FROM target;',
            pg_conn,
        )
        assert result == [[1, "9999-12-31", 2, "2024-06-15"]]
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
def test_temporal_error_quoted_column_name(
    pg_conn, extension, s3, with_default_location, use_pushdown
):
    """Error mode with quoted column name raises error correctly."""
    suffix = "_pd" if use_pushdown else ""
    schema = f"test_te_qcol{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            'CREATE TABLE target (id int, "from" date) USING iceberg'
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            run_command(
                f"""COPY (SELECT 1 AS id, 'infinity'::date AS "from") TO '{parquet_url}';""",
                pg_conn,
            )
            run_command(
                f"""CREATE FOREIGN TABLE source (id int, "from" date) """
                f"SERVER pg_lake OPTIONS (path '{parquet_url}');",
                pg_conn,
            )
            pg_conn.commit()

            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            with pytest.raises(Exception, match="date out of range"):
                run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
            pg_conn.rollback()
        else:
            err = run_command(
                "INSERT INTO target VALUES (1, 'infinity'::date);",
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
# Multidimensional array validation
#
# Multidimensional arrays are clamped to NULL (clamp mode) or rejected
# (error mode).  Parametrized across array shapes and both the
# non-pushdown (INSERT VALUES) and pushdown (INSERT SELECT from foreign
# table) paths.
# =====================================================================

ARRAY_CLAMP_PARAMS = [
    pytest.param("int[]", "ARRAY[1,2,3]", None, [1, 2, 3], id="1d_int"),
    pytest.param("int[]", "ARRAY[1, NULL, 3]", None, [1, None, 3], id="1d_int_null"),
    pytest.param("int[]", "ARRAY[]::int[]", None, [], id="empty"),
    pytest.param(
        "int[]",
        "ARRAY[ARRAY[1,2,3], ARRAY[4,5,6]]",
        "[[1,2,3],[4,5,6]]",
        None,
        id="2d_int",
    ),
    pytest.param(
        "text[]",
        "ARRAY[ARRAY['a','b'], ARRAY['c','d']]",
        "[['a','b'],['c','d']]",
        None,
        id="2d_text",
    ),
    pytest.param(
        "int[]",
        (
            "ARRAY[ARRAY[ARRAY[1,2,3], ARRAY[4,5,6]], "
            "ARRAY[ARRAY[7,8,9], ARRAY[10,11,12]]]"
        ),
        "[[[1,2,3],[4,5,6]],[[7,8,9],[10,11,12]]]",
        None,
        id="3d_int",
    ),
    pytest.param(
        "int[]",
        "ARRAY[ARRAY[1, NULL, 3], ARRAY[NULL, 5, 6]]",
        "[[1, NULL, 3],[NULL, 5, 6]]",
        None,
        id="2d_int_nulls",
    ),
    pytest.param(
        "int[]",
        None,
        "[[]::INTEGER[], []::INTEGER[]]",
        None,
        id="empty_nested",
    ),
]


@pytest.mark.parametrize("use_pushdown", [False, True], ids=["values", "pushdown"])
@pytest.mark.parametrize(
    "col_type,pg_insert_expr,duckdb_copy_expr,expected",
    ARRAY_CLAMP_PARAMS,
)
def test_array_multidim_clamp(
    pg_conn,
    pgduck_conn,
    extension,
    s3,
    with_default_location,
    use_pushdown,
    col_type,
    pg_insert_expr,
    duckdb_copy_expr,
    expected,
):
    """Multidimensional arrays become NULL in clamp mode; 1D arrays pass through."""
    if not use_pushdown and pg_insert_expr is None:
        pytest.skip("no non-pushdown INSERT expression for this case")

    suffix = "_pd" if use_pushdown else ""
    schema = f"test_arr_clamp{suffix}"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        if use_pushdown:
            parquet_url = f"s3://{TEST_BUCKET}/{schema}/data.parquet"
            if duckdb_copy_expr is not None:
                run_command(
                    f"COPY (SELECT {duckdb_copy_expr} AS col) "
                    f"TO '{parquet_url}' (FORMAT PARQUET)",
                    pgduck_conn,
                )
                pgduck_conn.commit()
            else:
                run_command(
                    f"COPY (SELECT {pg_insert_expr} AS col) " f"TO '{parquet_url}';",
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

        if use_pushdown:
            assert_query_pushdownable(
                "INSERT INTO target SELECT * FROM source;", pg_conn
            )
            run_command("INSERT INTO target SELECT * FROM source;", pg_conn)
        else:
            run_command(f"INSERT INTO target VALUES ({pg_insert_expr});", pg_conn)
        pg_conn.commit()

        result = run_query("SELECT col FROM target;", pg_conn)
        assert result[0]["col"] == expected
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_multidim_array_error(pg_conn, extension, s3, with_default_location):
    """Multidimensional arrays are rejected in error mode."""
    schema = "test_md_arr_err"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TABLE target (col int[]) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES (ARRAY[ARRAY[1,2,3]]);",
            pg_conn,
            raise_error=False,
        )
        assert "multidimensional arrays are not supported" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_multidim_array_in_composite_clamp(
    pg_conn, extension, s3, with_default_location
):
    """Multidimensional array inside a composite field becomes NULL."""
    schema = "test_md_comp_clamp"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE md_arr_comp AS (id int, vals int[]);",
            pg_conn,
        )
        run_command(
            "CREATE TABLE target (col md_arr_comp) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, ARRAY[ARRAY[10,20], ARRAY[30,40]])::md_arr_comp);",
            pg_conn,
        )
        pg_conn.commit()

        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)
        result = run_query(
            "SELECT (col).id, (col).vals FROM target;",
            pg_conn,
        )
        assert result[0][0] == 1
        assert result[0][1] is None
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_multidim_array_in_composite_error(
    pg_conn, extension, s3, with_default_location
):
    """Multidimensional array inside a composite raises error in error mode."""
    schema = "test_md_comp_err"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TYPE md_arr_comp_e AS (id int, vals int[]);",
            pg_conn,
        )
        run_command(
            "CREATE TABLE target (col md_arr_comp_e) USING iceberg"
            " WITH (out_of_range_values = 'error');",
            pg_conn,
        )
        pg_conn.commit()

        err = run_command(
            "INSERT INTO target VALUES "
            "(ROW(1, ARRAY[ARRAY[10,20]])::md_arr_comp_e);",
            pg_conn,
            raise_error=False,
        )
        assert "multidimensional arrays are not supported" in str(err)
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_multidim_temporal_array_clamp(pg_conn, extension, s3, with_default_location):
    """Multidimensional date array becomes NULL (not flattened+clamped)."""
    schema = "test_md_temp_arr"

    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command("CREATE TABLE target (col date[]) USING iceberg;", pg_conn)
        pg_conn.commit()

        run_command(
            "INSERT INTO target VALUES "
            "(ARRAY[ARRAY['2024-01-01'::date, 'infinity'::date], "
            "ARRAY['-infinity'::date, '2024-06-15'::date]]);",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query("SELECT col FROM target;", pg_conn)
        assert result[0]["col"] is None
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


# =====================================================================
# EXPLAIN verification: pg_nullify_nested_list in query wrapper
# =====================================================================


def test_explain_shows_pg_nullify_nested_list_for_plain_array(
    pg_conn, extension, s3, with_default_location
):
    """EXPLAIN output for int[] column shows pg_nullify_nested_list wrapping."""
    schema = "test_explain_prnl"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)

    try:
        run_command(
            "CREATE TABLE explain_source (col int[]) USING iceberg;",
            pg_conn,
        )
        run_command(
            "CREATE TABLE explain_target (col int[]) USING iceberg;",
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
            r"pg_nullify_nested_list", explain_text
        ), f"Expected pg_nullify_nested_list not found in EXPLAIN:\n{explain_text}"
    finally:
        pg_conn.rollback()
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()


def test_explain_shows_pg_nullify_nested_list_in_list_transform(
    pg_conn, extension, s3, with_default_location
):
    """EXPLAIN for date[] shows pg_nullify_nested_list inside list_transform."""
    schema = "test_explain_prnl_lt"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    run_command(f"SET search_path TO {schema};", pg_conn)
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    try:
        run_command(
            "CREATE TABLE explain_source (col date[]) USING iceberg;",
            pg_conn,
        )
        run_command(
            "CREATE TABLE explain_target (col date[]) USING iceberg;",
            pg_conn,
        )
        pg_conn.commit()

        result = run_query(
            "EXPLAIN (VERBOSE) INSERT INTO explain_target"
            " SELECT * FROM explain_source;",
            pg_conn,
        )
        explain_text = "\n".join(line[0] for line in result)

        assert re.search(r"list_transform\(pg_nullify_nested_list", explain_text), (
            f"Expected list_transform(pg_nullify_nested_list pattern "
            f"not found in EXPLAIN:\n{explain_text}"
        )
    finally:
        pg_conn.rollback()
        run_command("RESET TIME ZONE;", pg_conn)
        run_command("RESET search_path;", pg_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
        pg_conn.commit()

"""
Verify postgres_scanner behavior when pgduck_server scans PostgreSQL tables
via postgres_scan().

Scenarios covered:
- Composite types read as structs with correct field values
- Bounded numeric(p,s) NaN clamped to NULL (binary & text protocol)
- Unbounded / large-precision numerics converted to double (NaN and ±Inf preserved)
- Special values (±Inf, NaN, NULL) across float4, float8, and numeric flavors
  (binary & text protocol)
- Multidimensional array values in int[] column clamped to NULL
"""

import pytest
from utils_pytest import *


def _connstr():
    return (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )


def _scan(table, schema="public"):
    return f"postgres_scan('{_connstr()}', '{schema}', '{table}')"


@pytest.fixture(scope="module")
def pg_tables(postgres):
    """Create test tables in PostgreSQL."""
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # -- composite type -------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_composite_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_comp_type CASCADE")
    cur.execute("CREATE TYPE scanner_comp_type AS (id int, name text)")
    cur.execute("CREATE TABLE scanner_composite_tbl (c scanner_comp_type)")
    cur.execute(
        "INSERT INTO scanner_composite_tbl VALUES "
        "(ROW(42, 'hello')::scanner_comp_type), "
        "(ROW(99, 'world')::scanner_comp_type), "
        "(NULL)"
    )

    # -- same-name composite type in two schemas --------------------------
    cur.execute("CREATE SCHEMA IF NOT EXISTS scanner_schema_a")
    cur.execute("CREATE SCHEMA IF NOT EXISTS scanner_schema_b")
    cur.execute("DROP TABLE IF EXISTS scanner_schema_a.comp_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_schema_b.comp_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_schema_a.shared_type CASCADE")
    cur.execute("DROP TYPE IF EXISTS scanner_schema_b.shared_type CASCADE")
    cur.execute("CREATE TYPE scanner_schema_a.shared_type AS (id int, name text)")
    cur.execute(
        "CREATE TYPE scanner_schema_b.shared_type AS " "(id int, name text, extra int)"
    )
    cur.execute(
        "CREATE TABLE scanner_schema_a.comp_tbl " "(c scanner_schema_a.shared_type)"
    )
    cur.execute(
        "CREATE TABLE scanner_schema_b.comp_tbl " "(c scanner_schema_b.shared_type)"
    )
    cur.execute(
        "INSERT INTO scanner_schema_a.comp_tbl VALUES "
        "(ROW(1, 'alpha')::scanner_schema_a.shared_type)"
    )
    cur.execute(
        "INSERT INTO scanner_schema_b.comp_tbl VALUES "
        "(ROW(2, 'beta', 42)::scanner_schema_b.shared_type)"
    )

    # -- composite with special characters in identifiers -----------------
    cur.execute('CREATE SCHEMA IF NOT EXISTS "scan""er\'s schema!"')
    cur.execute('DROP TABLE IF EXISTS "scan""er\'s schema!"."comp ""tbl"')
    cur.execute('DROP TYPE IF EXISTS "scan""er\'s schema!"."my ""type" CASCADE')
    cur.execute(
        'CREATE TYPE "scan""er\'s schema!"."my ""type" AS ('
        '  "id col" int,'
        '  "na""me" text,'
        '  "val;use" int'
        ")"
    )
    cur.execute(
        'CREATE TABLE "scan""er\'s schema!"."comp ""tbl" ('
        '  c "scan""er\'s schema!"."my ""type"'
        ")"
    )
    cur.execute(
        'INSERT INTO "scan""er\'s schema!"."comp ""tbl" VALUES '
        "(ROW(7, 'hi \"there', 99))"
    )

    # -- composite containing array field ---------------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_comp_with_array_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_comp_with_array CASCADE")
    cur.execute("CREATE TYPE scanner_comp_with_array AS (id int, tags text[])")
    cur.execute(
        "CREATE TABLE scanner_comp_with_array_tbl " "(c scanner_comp_with_array)"
    )
    cur.execute(
        "INSERT INTO scanner_comp_with_array_tbl VALUES "
        "(ROW(1, ARRAY['a', 'b'])::scanner_comp_with_array), "
        "(ROW(2, ARRAY['x'])::scanner_comp_with_array), "
        "(NULL)"
    )

    # -- array of composite type ------------------------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_array_of_comp_tbl")
    cur.execute("CREATE TABLE scanner_array_of_comp_tbl " "(items scanner_comp_type[])")
    cur.execute(
        "INSERT INTO scanner_array_of_comp_tbl VALUES "
        "(ARRAY[ROW(1, 'one'), ROW(2, 'two')]::scanner_comp_type[]), "
        "(ARRAY[ROW(3, 'three')]::scanner_comp_type[]), "
        "(NULL)"
    )

    # -- enum type -------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_enum_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_color CASCADE")
    cur.execute("CREATE TYPE scanner_color AS ENUM ('red', 'green', 'blue')")
    cur.execute("CREATE TABLE scanner_enum_tbl (c scanner_color)")
    cur.execute(
        "INSERT INTO scanner_enum_tbl VALUES " "('red'), ('blue'), ('green'), (NULL)"
    )

    # -- bounded numeric (NaN) ------------------------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_bounded_numeric_tbl")
    cur.execute("CREATE TABLE scanner_bounded_numeric_tbl (v numeric(10,2))")
    cur.execute(
        "INSERT INTO scanner_bounded_numeric_tbl VALUES "
        "(123.45), ('NaN'::numeric), (NULL), (0.00)"
    )

    # -- unbounded / large-precision numerics ---------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_unbounded_numeric_tbl")
    cur.execute(
        "CREATE TABLE scanner_unbounded_numeric_tbl ("
        "  unbounded numeric,"
        "  large_precision numeric(40,2)"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_unbounded_numeric_tbl VALUES "
        "(123.456, 999.99), "
        "('NaN'::numeric, 'NaN'::numeric), "
        "(NULL, NULL), "
        "(1e30, 1e30), "
        "('Infinity'::numeric, NULL), "
        "('-Infinity'::numeric, NULL)"
    )

    # -- special values across float and numeric types -------------------
    cur.execute("DROP TABLE IF EXISTS scanner_special_values_tbl")
    cur.execute(
        "CREATE TABLE scanner_special_values_tbl ("
        "  id int PRIMARY KEY,"
        "  f4 float4,"
        "  f8 float8,"
        "  num_unbounded numeric,"
        "  num_large numeric(50, 2),"
        "  num_bounded numeric(10, 2)"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_special_values_tbl VALUES "
        "(1, 'NaN'::float4, 'NaN'::float8, 'NaN'::numeric, 'NaN'::numeric, 'NaN'::numeric),"
        "(2, 'Infinity'::float4, 'Infinity'::float8, 'Infinity'::numeric, NULL, NULL),"
        "(3, '-Infinity'::float4, '-Infinity'::float8, '-Infinity'::numeric, NULL, NULL),"
        "(4, NULL, NULL, NULL, NULL, NULL),"
        "(5, 1.5::float4, 1.5::float8, 1.5::numeric, 1.50::numeric(50,2), 1.50::numeric(10,2))"
    )

    # -- multidimensional arrays ----------------------------------------
    cur.execute("DROP TABLE IF EXISTS scanner_multidim_array_tbl")
    cur.execute("CREATE TABLE scanner_multidim_array_tbl (a int[])")
    cur.execute(
        "INSERT INTO scanner_multidim_array_tbl VALUES "
        "(ARRAY[1, 2, 3]), "
        "(ARRAY[ARRAY[1, 2], ARRAY[3, 4]]), "
        "(NULL), "
        "(ARRAY[10, 20])"
    )

    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_composite_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_comp_type CASCADE")
    cur.execute("DROP TABLE IF EXISTS scanner_schema_a.comp_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_schema_b.comp_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_schema_a.shared_type CASCADE")
    cur.execute("DROP TYPE IF EXISTS scanner_schema_b.shared_type CASCADE")
    cur.execute("DROP SCHEMA IF EXISTS scanner_schema_a")
    cur.execute("DROP SCHEMA IF EXISTS scanner_schema_b")
    cur.execute('DROP TABLE IF EXISTS "scan""er\'s schema!"."comp ""tbl"')
    cur.execute('DROP TYPE IF EXISTS "scan""er\'s schema!"."my ""type" CASCADE')
    cur.execute('DROP SCHEMA IF EXISTS "scan""er\'s schema!"')
    cur.execute("DROP TABLE IF EXISTS scanner_array_of_comp_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_comp_with_array_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_comp_with_array CASCADE")
    cur.execute("DROP TABLE IF EXISTS scanner_enum_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_color CASCADE")
    cur.execute("DROP TABLE IF EXISTS scanner_bounded_numeric_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_unbounded_numeric_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_special_values_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_multidim_array_tbl")
    cur.close()
    conn.close()


# -------------------------------------------------------------------
# Composite types
# -------------------------------------------------------------------


def test_composite_type(pg_tables, pgduck_conn):
    """Composite type columns are readable as structs; NULL rows preserved."""
    scan = _scan("scanner_composite_tbl")
    rows = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'name') "
        f"FROM {scan} ORDER BY struct_extract(c, 'id') NULLS LAST",
        pgduck_conn,
    )
    assert rows == [("42", "hello"), ("99", "world"), (None, None)]


def test_composite_same_name_different_schemas(pg_tables, pgduck_conn):
    """Same type name in two schemas with different fields resolves correctly."""
    scan_a = _scan("comp_tbl", schema="scanner_schema_a")
    rows_a = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'name') " f"FROM {scan_a}",
        pgduck_conn,
    )
    assert rows_a == [("1", "alpha")]

    scan_b = _scan("comp_tbl", schema="scanner_schema_b")
    rows_b = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'name'), "
        f"struct_extract(c, 'extra') "
        f"FROM {scan_b}",
        pgduck_conn,
    )
    assert rows_b == [("2", "beta", "42")]


def test_composite_special_characters(pg_tables, pgduck_conn):
    """Composite type with quotes and special chars in schema, type, and fields."""
    scan = _scan('comp "tbl', schema="scan\"er''s schema!")
    rows = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id col'), "
        f"""struct_extract(c, 'na"me'), """
        f"struct_extract(c, 'val;use') "
        f"FROM {scan}",
        pgduck_conn,
    )
    assert rows == [("7", 'hi "there', "99")]


def test_composite_with_array_field(pg_tables, pgduck_conn):
    """Composite type containing an array field is readable."""
    scan = _scan("scanner_comp_with_array_tbl")
    rows = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'tags') "
        f"FROM {scan} ORDER BY struct_extract(c, 'id') NULLS LAST",
        pgduck_conn,
    )
    assert rows == [("1", "{a,b}"), ("2", "{x}"), (None, None)]


def test_array_of_composite(pg_tables, pgduck_conn):
    """Array of composite type column is readable."""
    scan = _scan("scanner_array_of_comp_tbl")
    rows = perform_query_on_cursor(
        f"SELECT items FROM {scan} " f"ORDER BY len(items) NULLS LAST",
        pgduck_conn,
    )
    assert rows == [
        ('{"(3,three)"}',),
        ('{"(1,one)","(2,two)"}',),
        (None,),
    ]


# -------------------------------------------------------------------
# Enum types
# -------------------------------------------------------------------


def test_enum_type(pg_tables, pgduck_conn):
    """Enum type columns are readable as VARCHAR; NULL preserved."""
    scan = _scan("scanner_enum_tbl")
    rows = perform_query_on_cursor(
        f"SELECT c FROM {scan} ORDER BY c NULLS LAST",
        pgduck_conn,
    )
    assert rows == [("blue",), ("green",), ("red",), (None,)]


# -------------------------------------------------------------------
# Bounded numeric – NaN → NULL
# -------------------------------------------------------------------


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_bounded_numeric_nan_to_null(pg_tables, pgduck_conn, use_text_protocol):
    """NaN in numeric(10,2) is scanned as NULL (DuckDB DECIMAL can't hold NaN)."""
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        scan = _scan("scanner_bounded_numeric_tbl")
        rows = perform_query_on_cursor(
            f"SELECT v FROM {scan} ORDER BY v NULLS LAST",
            pgduck_conn,
        )
        # Source: 123.45, NaN, NULL, 0.00
        # After:  0.00, 123.45, NULL (NaN→NULL), NULL (original)
        assert rows == [("0.00",), ("123.45",), (None,), (None,)]
    finally:
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


# -------------------------------------------------------------------
# Unbounded / large-precision numerics → double
# -------------------------------------------------------------------


def test_unbounded_numeric_as_double(pg_tables, pgduck_conn):
    """Unbounded numeric maps to DOUBLE; NaN and ±Infinity preserved, NULL preserved."""
    scan = _scan("scanner_unbounded_numeric_tbl")
    rows = perform_query_on_cursor(
        f"SELECT typeof(unbounded), "
        f"  CASE WHEN unbounded IS NULL THEN 'null' "
        f"       WHEN isnan(unbounded) THEN 'nan' "
        f"       WHEN isinf(unbounded) AND unbounded > 0 THEN '+inf' "
        f"       WHEN isinf(unbounded) AND unbounded < 0 THEN '-inf' "
        f"       ELSE 'value' END "
        f"FROM {scan} ORDER BY unbounded NULLS LAST",
        pgduck_conn,
    )
    assert rows == [
        ("DOUBLE", "-inf"),  # -Infinity preserved
        ("DOUBLE", "value"),  # 123.456
        ("DOUBLE", "value"),  # 1e30
        ("DOUBLE", "+inf"),  # +Infinity preserved
        ("DOUBLE", "nan"),  # NaN preserved as double NaN
        ("DOUBLE", "null"),  # original NULL
    ]


def test_large_precision_numeric_as_double(pg_tables, pgduck_conn):
    """numeric(40,2) (precision > 38) maps to DOUBLE; NaN preserved, NULL preserved.

    PostgreSQL rejects ±Infinity for bounded numeric types (including
    large-precision), so the Infinity rows in the source table have NULL
    for this column.
    """
    scan = _scan("scanner_unbounded_numeric_tbl")
    rows = perform_query_on_cursor(
        f"SELECT typeof(large_precision), "
        f"  CASE WHEN large_precision IS NULL THEN 'null' "
        f"       WHEN isnan(large_precision) THEN 'nan' "
        f"       ELSE 'value' END "
        f"FROM {scan} "
        f"WHERE large_precision IS NOT NULL "
        f"ORDER BY large_precision NULLS LAST",
        pgduck_conn,
    )
    assert rows == [
        ("DOUBLE", "value"),  # 999.99
        ("DOUBLE", "value"),  # 1e30
        ("DOUBLE", "nan"),  # NaN preserved as double NaN
    ]


# -------------------------------------------------------------------
# Special values (±Inf, NaN, NULL) across float & numeric types
# -------------------------------------------------------------------


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_special_values_across_types(pg_tables, pgduck_conn, use_text_protocol):
    """±Inf, NaN, and NULL across float4, float8, and three numeric flavors.

    Column types and their DuckDB mapping via postgres_scan:

      f4            float4         → FLOAT
      f8            float8         → DOUBLE
      num_unbounded numeric        → DOUBLE (NUMERIC_AS_DOUBLE)
      num_large     numeric(50,2)  → DOUBLE (precision > 38)
      num_bounded   numeric(10,2)  → DECIMAL(10,2)

    PostgreSQL rejects ±Infinity for bounded numeric types, so Infinity
    rows use NULL for num_large and num_bounded.  NaN is accepted by all
    PostgreSQL numeric types, but DuckDB DECIMAL cannot represent NaN, so
    bounded numeric NaN is scanned as NULL.
    """
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        scan = _scan("scanner_special_values_tbl")

        def classify(col):
            return (
                f"CASE WHEN {col} IS NULL THEN 'null' "
                f"WHEN isnan({col}) THEN 'nan' "
                f"WHEN isinf({col}) AND {col} > 0 THEN '+inf' "
                f"WHEN isinf({col}) AND {col} < 0 THEN '-inf' "
                f"ELSE 'value' END"
            )

        rows = perform_query_on_cursor(
            f"SELECT id, "
            f"  {classify('f4')}, {classify('f8')}, "
            f"  {classify('num_unbounded')}, {classify('num_large')}, "
            f"  CASE WHEN num_bounded IS NULL THEN 'null' ELSE 'value' END "
            f"FROM {scan} ORDER BY id",
            pgduck_conn,
        )

        assert rows == [
            ("1", "nan", "nan", "nan", "nan", "null"),  # NaN (bounded → NULL)
            (
                "2",
                "+inf",
                "+inf",
                "+inf",
                "null",
                "null",
            ),  # +Inf (large/bounded NULL in PG)
            (
                "3",
                "-inf",
                "-inf",
                "-inf",
                "null",
                "null",
            ),  # -Inf (large/bounded NULL in PG)
            ("4", "null", "null", "null", "null", "null"),  # NULL
            ("5", "value", "value", "value", "value", "value"),  # 1.5
        ]
    finally:
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


# -------------------------------------------------------------------
# Multidimensional arrays → NULL
# -------------------------------------------------------------------


def test_multidim_array_to_null(pg_tables, pgduck_conn):
    """Multidimensional array values in int[] column are read as NULL."""
    scan = _scan("scanner_multidim_array_tbl")
    rows = perform_query_on_cursor(
        f"SELECT a FROM {scan} ORDER BY a[1] NULLS LAST",
        pgduck_conn,
    )
    # Source: [1,2,3], [[1,2],[3,4]], NULL, [10,20]
    # After:  [1,2,3], [10,20], NULL (multidim→NULL), NULL (original)
    assert rows == [
        ("{1,2,3}",),
        ("{10,20}",),
        (None,),  # multidim→NULL
        (None,),  # original NULL
    ]

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
- Wide rows do not blow past the Parquet row group size limit (binary & text
  protocol), which requires bounding the scanner's output chunks by size
- Mixed row widths, the worst case for that limit, stay within 2x of it
- Arrays of very short strings, where the cap is charged 8x fewer bytes than the
  writer counts, and where the 16MB default is what still holds the row group
  size the writer was asked for
"""

from decimal import Decimal

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

    # -- wide rows (large strings) ---------------------------------------
    # 256 rows of ~64KB each.  The payload is md5 hex derived from the row and
    # block index, so it is deterministic yet incompressible - the bytes on the
    # wire, in the scanner's output chunk, and in the Parquet row group are all
    # within a few percent of each other.
    cur.execute("DROP TABLE IF EXISTS scanner_wide_rows_tbl")
    cur.execute(
        "CREATE TABLE scanner_wide_rows_tbl AS "
        "SELECT i AS id, ("
        "  SELECT string_agg(md5(i::text || ':' || j::text), '')"
        "  FROM generate_series(1, 2048) j"
        ") AS payload "
        "FROM generate_series(1, 256) i"
    )

    # -- mixed row widths, which is where the overshoot is worst ----------
    # 8192 rows of ~1KB followed by 256 rows of ~64KB.  The narrow rows fill
    # whole 2048-row chunks that walk the writer's buffer up to just under the
    # row group limit, and the first wide chunk then lands on top of it - so
    # this is the shape that realizes the worst case of limit + one chunk.  A
    # table of uniformly wide rows does not: its chunks divide evenly into the
    # limit, so the buffer is always empty when one arrives.
    cur.execute("DROP TABLE IF EXISTS scanner_mixed_rows_tbl")
    cur.execute(
        "CREATE TABLE scanner_mixed_rows_tbl AS "
        "SELECT i AS id, ("
        "  SELECT substr(string_agg(md5(i::text || ':' || j::text), ''), 1, 1000)"
        "  FROM generate_series(1, 32) j"
        ") AS payload "
        "FROM generate_series(1, 8192) i"
    )
    cur.execute(
        "INSERT INTO scanner_mixed_rows_tbl "
        "SELECT 100000 + i, ("
        "  SELECT string_agg(md5(i::text || ':' || j::text), '')"
        "  FROM generate_series(1, 2048) j"
        ") "
        "FROM generate_series(1, 256) i"
    )

    # -- arrays of very short strings, the widest unit gap there is --------
    # 1200 rows x 65536 single-character elements.  Over the text protocol an
    # element costs 2 bytes on the wire, which is what the cap is charged, while
    # DuckDB holds a 16-byte string_t for it - so these rows read ~128KB each on
    # the wire and occupy ~1MB each in the writer's buffer.  Storage is small
    # (~10MB): the array is a single repeated value, so it TOASTs down well.
    cur.execute("DROP TABLE IF EXISTS scanner_small_str_array_tbl")
    cur.execute(
        "CREATE TABLE scanner_small_str_array_tbl AS "
        "SELECT i AS id, tags "
        "FROM generate_series(1, 1200) i, "
        "     (SELECT array_agg(chr(97 + (j % 26))) AS tags "
        "        FROM generate_series(1, 65536) j) elems"
    )

    # -- wide payload nested inside a composite ---------------------------
    # 256 rows carrying ~16KB of bytea and ~32KB of text[] per row, all of it
    # below the top level.  Byte accounting that only looked at a column's own
    # inline bytes would score these rows near zero and hand the writer 2048-row
    # chunks again, so this is the case that proves nested payload is counted.
    cur.execute("DROP TABLE IF EXISTS scanner_wide_nested_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_wide_comp_type CASCADE")
    cur.execute(
        "CREATE TYPE scanner_wide_comp_type AS (id int, blob bytea, tags text[])"
    )
    cur.execute(
        "CREATE TABLE scanner_wide_nested_tbl AS "
        "SELECT ROW("
        "  i,"
        "  decode(("
        "    SELECT string_agg(md5(i::text || ':' || j::text), '')"
        "    FROM generate_series(1, 1024) j"
        "  ), 'hex'),"
        "  ARRAY["
        "    (SELECT string_agg(md5(i::text || ':a:' || j::text), '')"
        "     FROM generate_series(1, 512) j),"
        "    (SELECT string_agg(md5(i::text || ':b:' || j::text), '')"
        "     FROM generate_series(1, 512) j)"
        "  ]"
        ")::scanner_wide_comp_type AS c "
        "FROM generate_series(1, 256) i"
    )

    # -- narrow rows, for the no-regression leg of the chunk size test ----
    cur.execute("DROP TABLE IF EXISTS scanner_narrow_rows_tbl")
    cur.execute(
        "CREATE TABLE scanner_narrow_rows_tbl AS "
        "SELECT i AS id, i * 2 AS v FROM generate_series(1, 20000) i"
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
    cur.execute("DROP TABLE IF EXISTS scanner_wide_rows_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_mixed_rows_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_small_str_array_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_wide_nested_tbl")
    cur.execute("DROP TYPE IF EXISTS scanner_wide_comp_type CASCADE")
    cur.execute("DROP TABLE IF EXISTS scanner_narrow_rows_tbl")
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
    assert rows == [(42, "hello"), (99, "world"), (None, None)]


def test_composite_same_name_different_schemas(pg_tables, pgduck_conn):
    """Same type name in two schemas with different fields resolves correctly."""
    scan_a = _scan("comp_tbl", schema="scanner_schema_a")
    rows_a = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'name') " f"FROM {scan_a}",
        pgduck_conn,
    )
    assert rows_a == [(1, "alpha")]

    scan_b = _scan("comp_tbl", schema="scanner_schema_b")
    rows_b = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'name'), "
        f"struct_extract(c, 'extra') "
        f"FROM {scan_b}",
        pgduck_conn,
    )
    assert rows_b == [(2, "beta", 42)]


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
    assert rows == [(7, 'hi "there', 99)]


def test_composite_with_array_field(pg_tables, pgduck_conn):
    """Composite type containing an array field is readable."""
    scan = _scan("scanner_comp_with_array_tbl")
    rows = perform_query_on_cursor(
        f"SELECT struct_extract(c, 'id'), struct_extract(c, 'tags') "
        f"FROM {scan} ORDER BY struct_extract(c, 'id') NULLS LAST",
        pgduck_conn,
    )
    assert rows == [(1, "{a,b}"), (2, "{x}"), (None, None)]


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
        assert rows == [(Decimal("0.00"),), (Decimal("123.45"),), (None,), (None,)]
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
            (1, "nan", "nan", "nan", "nan", "null"),  # NaN (bounded → NULL)
            (
                2,
                "+inf",
                "+inf",
                "+inf",
                "null",
                "null",
            ),  # +Inf (large/bounded NULL in PG)
            (
                3,
                "-inf",
                "-inf",
                "-inf",
                "null",
                "null",
            ),  # -Inf (large/bounded NULL in PG)
            (4, "null", "null", "null", "null", "null"),  # NULL
            (5, "value", "value", "value", "value", "value"),  # 1.5
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


# -------------------------------------------------------------------
# Row group size limit with wide rows
# -------------------------------------------------------------------

# The Parquet writer can only close a row group on a chunk boundary, so the
# scanner's chunk size is the granularity of the row group size limit.  These
# numbers keep one uncapped chunk (256 rows x ~64KB = ~16MB) several times
# larger than the row group limit, which is what makes the overshoot visible.
_RG_LIMIT_BYTES = 4 * 1024 * 1024
_CHUNK_CAP_BYTES = 1024 * 1024

# The mixed-width table is written at a limit its ~1KB rows divide into unevenly,
# which is what walks the writer's buffer close to the limit before the first
# wide chunk arrives.  The cap is set equal to the limit here because that is the
# loosest a cap can be and still bound anything: it is the setting the 2x figure
# belongs to.  The default cap is a fraction of the row group size pg_lake asks
# for, so production sits below this.
_MIXED_RG_LIMIT_BYTES = 8 * 1024 * 1024
# A chunk is closed once it has *reached* the cap, so it overruns by at most the
# last row - the widest row in the mixed table is ~64KB.
_MIXED_MAX_ROW_BYTES = 128 * 1024

# The two tests above run at 1/16 and 1/32 scale.  The one below runs at the row
# group size pg_lake actually asks for, because what it measures is the ratio
# between the unit the cap is charged in (Postgres bytes) and the unit the writer
# thresholds on (DuckDB's in-memory size) - and it is that ratio, against the real
# row group size, that decides what the default cap has to be.
_SMALL_STR_RG_LIMIT_BYTES = 128 * 1024 * 1024
_DEFAULT_CHUNK_CAP_BYTES = 16 * 1024 * 1024
# DuckDB holds one string_t per array element no matter how short the element is,
# so this table's in-memory footprint follows from its row count.  Deriving it
# that way is necessary because parquet_metadata() cannot show it:
# row_group_bytes is Parquet-encoded, and 26 distinct single-character values
# dictionary-encode to ~4% of the in-memory size.
_STRING_T_BYTES = 16
_SMALL_STR_ELEMS_PER_ROW = 65536
_SMALL_STR_ROW_MEM_BYTES = _SMALL_STR_ELEMS_PER_ROW * _STRING_T_BYTES


def _write_parquet(pgduck_conn, path, table, chunk_cap_bytes, rg_limit_bytes):
    """Scan a table into Parquet, then report its row group sizes.

    A chunk_cap_bytes of None leaves pg_max_chunk_size_bytes at its default,
    which is how the scanner runs in production.
    """
    if chunk_cap_bytes is not None:
        run_command(f"SET pg_max_chunk_size_bytes = {chunk_cap_bytes}", pgduck_conn)
    try:
        run_command(
            f"COPY (SELECT * FROM {_scan(table)}) "
            f"TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE_BYTES {rg_limit_bytes})",
            pgduck_conn,
        )
    finally:
        if chunk_cap_bytes is not None:
            run_command("RESET pg_max_chunk_size_bytes", pgduck_conn)

    rows = run_query(
        f"SELECT count(DISTINCT row_group_id), max(row_group_bytes), "
        f"       sum(row_group_num_rows) FILTER (WHERE path_in_schema = 'id') "
        f"FROM parquet_metadata('{path}')",
        pgduck_conn,
    )
    return rows[0][0], rows[0][1], rows[0][2]


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_wide_rows_respect_row_group_size_limit(
    pg_tables, pgduck_conn, tmp_path, use_text_protocol
):
    """Bounding scanner chunks by size keeps wide rows inside the row group limit.

    postgres_scanner fills an output chunk up to STANDARD_VECTOR_SIZE (2048)
    rows regardless of how wide those rows are, and the Parquet writer only
    evaluates row_group_size_bytes between chunks.  A table with large strings
    therefore produced a single row group many times the requested size - the
    uncapped leg below demonstrates that.  pg_max_chunk_size_bytes caps the
    chunk payload so the writer gets to flush on time.
    """
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        # Uncapped (0 disables the bound): the old behavior.  All 256 rows land
        # in one chunk, so the writer never gets a chance to flush and emits a
        # single row group several times over the limit.
        groups, largest, num_rows = _write_parquet(
            pgduck_conn,
            tmp_path / "uncapped.parquet",
            "scanner_wide_rows_tbl",
            0,
            _RG_LIMIT_BYTES,
        )
        assert num_rows == 256
        assert groups == 1
        assert largest > 3 * _RG_LIMIT_BYTES
        uncapped_largest = largest

        # Capped: chunks are bounded at ~1MB, so the writer flushes once it has
        # accumulated the 4MB limit and the overshoot is at most one chunk.
        groups, largest, num_rows = _write_parquet(
            pgduck_conn,
            tmp_path / "capped.parquet",
            "scanner_wide_rows_tbl",
            _CHUNK_CAP_BYTES,
            _RG_LIMIT_BYTES,
        )
        assert num_rows == 256
        assert groups >= 3
        assert largest <= _RG_LIMIT_BYTES + 2 * _CHUNK_CAP_BYTES
        assert largest < uncapped_largest / 2
    finally:
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_mixed_row_widths_stay_within_twice_the_row_group_limit(
    pg_tables, pgduck_conn, tmp_path, use_text_protocol
):
    """Cap == row group limit bounds a row group at 2x the limit, not more.

    A cap equal to the limit is the loosest setting that still bounds anything,
    and it cannot do better than 2x: the writer only tests its threshold after
    appending a whole chunk, so a buffer sitting just under the limit plus one
    full chunk is the worst case.  Mixed row widths are what reach it.  Run here
    at 1/16 scale; the shipped default cap is well under the row group size
    pg_lake asks for, so production is inside this.
    """
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    # This table has enough heap pages to split into two scan tasks, and each
    # writer thread buffers its own row group - so the row group layout is only
    # deterministic single-threaded.  The bound under test is per buffer and so
    # does not depend on how many of them there are.
    run_command("SET threads = 1", pgduck_conn)
    try:
        # Uncapped, for contrast: a single row group holding the whole table.
        groups, uncapped_largest, num_rows = _write_parquet(
            pgduck_conn,
            tmp_path / "mixed_uncapped.parquet",
            "scanner_mixed_rows_tbl",
            0,
            _MIXED_RG_LIMIT_BYTES,
        )
        assert num_rows == 8448
        assert groups == 1
        assert uncapped_largest > 2 * _MIXED_RG_LIMIT_BYTES

        groups, largest, num_rows = _write_parquet(
            pgduck_conn,
            tmp_path / "mixed_capped.parquet",
            "scanner_mixed_rows_tbl",
            _MIXED_RG_LIMIT_BYTES,
            _MIXED_RG_LIMIT_BYTES,
        )
        assert num_rows == 8448
        assert groups >= 2
        assert largest <= 2 * _MIXED_RG_LIMIT_BYTES + _MIXED_MAX_ROW_BYTES
        assert largest < uncapped_largest
        # Guard against the assertion above passing for the wrong reason: this
        # table is meant to overshoot the limit, just boundedly.  If it stopped
        # doing so, the 2x bound would no longer be under test here.
        assert largest > _MIXED_RG_LIMIT_BYTES
    finally:
        run_command("RESET threads", pgduck_conn)
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


def _largest_row_group_mem_bytes(pgduck_conn, path):
    """Report the in-memory size of the largest row group in a Parquet file.

    Only meaningful for scanner_small_str_array_tbl, whose rows are all the same
    width.  Counts string_t's alone, so it under-reports what the writer charges
    by ~0.8%: the list offsets and the per-element validity bits add to it
    (measured 1,082,171,904 where this returns 1,073,741,824).  Under-reporting
    is the safe direction for the assertions below.
    """
    rows = run_query(
        f"SELECT max(row_group_num_rows) FROM parquet_metadata('{path}')",
        pgduck_conn,
    )
    return rows[0][0] * _SMALL_STR_ROW_MEM_BYTES


def test_default_cap_holds_small_string_arrays_to_the_row_group_limit(
    pg_tables, pgduck_conn, tmp_path
):
    """Arrays of one-character strings are why the default cap is not 128MB.

    The cap is charged Postgres bytes; the writer thresholds on DuckDB's
    in-memory size.  For a one-character element those are 2 bytes and 16 bytes,
    so a cap set equal to the 128MB row group size pg_lake asks for lets a chunk
    arrive in the writer's buffer as ~1GB, and the very first chunk closes a row
    group 8x over.  The bound works, it is just charged in the smaller unit -
    which is what the 16MB default leaves room for, bringing the same table and
    the same row group size back to ~1x.

    Text protocol is what makes 8x reachable: 2 bytes is the cheapest an element
    can be spelled there.  Binary spends 4 bytes on a length prefix per element
    and so tops out near 4x on this table.

    The writer's own accounting, read out of its flush log, puts the two legs at
    1,082,171,904 bytes (8.06x) and 135,307,776 bytes (1.01x).
    """
    perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    # Each writer thread buffers its own row group, so the layout is only
    # deterministic single-threaded.  The bound under test is per buffer.
    run_command("SET threads = 1", pgduck_conn)
    try:
        # A cap equal to the row group size, which is what the two tests above
        # run at and what holds them to 2x.
        groups, _, num_rows = _write_parquet(
            pgduck_conn,
            tmp_path / "small_str_cap_at_limit.parquet",
            "scanner_small_str_array_tbl",
            _SMALL_STR_RG_LIMIT_BYTES,
            _SMALL_STR_RG_LIMIT_BYTES,
        )
        assert num_rows == 1200
        # Guard against passing for the wrong reason: the cap does bind here, at
        # 1024 of the 1200 rows.  With the bound disabled the whole table would
        # be one chunk and so one row group, which would clear the assertion
        # below without the cap having done anything.
        assert groups >= 2
        at_limit_mem = _largest_row_group_mem_bytes(
            pgduck_conn, tmp_path / "small_str_cap_at_limit.parquet"
        )
        # A chunk is only closed once it has reached the cap, so at 2 wire bytes
        # per 16-byte element this is 8x by construction rather than by luck.
        assert at_limit_mem >= 8 * _SMALL_STR_RG_LIMIT_BYTES

        # Same table, same row group size, cap left at the shipped default.
        assert (
            run_query(
                "SELECT current_setting('pg_max_chunk_size_bytes')::BIGINT",
                pgduck_conn,
            )[0][0]
            == _DEFAULT_CHUNK_CAP_BYTES
        )
        groups, _, num_rows = _write_parquet(
            pgduck_conn,
            tmp_path / "small_str_default_cap.parquet",
            "scanner_small_str_array_tbl",
            None,
            _SMALL_STR_RG_LIMIT_BYTES,
        )
        assert num_rows == 1200
        assert groups >= 8
        default_mem = _largest_row_group_mem_bytes(
            pgduck_conn, tmp_path / "small_str_default_cap.parquet"
        )
        # 16MB of wire bytes is ~128MB in the buffer, so a chunk now crosses the
        # row group size on its own and the group closes at ~1x rather than at
        # the 2x a cap equal to the row group size would allow.
        assert default_mem <= 2 * _SMALL_STR_RG_LIMIT_BYTES
        assert default_mem < at_limit_mem / 5
    finally:
        run_command("RESET threads", pgduck_conn)
        perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


def _chunk_row_counts(pgduck_conn, path, table, chunk_cap_bytes):
    """Report the row counts of the chunks the scanner produced for a table.

    A 1-byte row group limit makes the writer close a row group after every
    chunk, so the Parquet footer reads back the chunk sizes directly.
    """
    _write_parquet(pgduck_conn, path, table, chunk_cap_bytes, 1)
    # The row count comes from the data rather than from row_group_num_rows,
    # which is reported per leaf column and so would need a schema-specific
    # filter for a struct column.
    rows = run_query(
        f"SELECT (SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{path}')), "
        f"       (SELECT max(row_group_num_rows) FROM parquet_metadata('{path}')), "
        f"       (SELECT count(*) FROM read_parquet('{path}'))",
        pgduck_conn,
    )
    return rows[0][0], rows[0][1], rows[0][2]


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_chunk_cap_counts_payload_nested_in_composite(
    pg_tables, pgduck_conn, tmp_path, use_text_protocol
):
    """Bytes inside a composite's bytea and text[] fields count toward the cap.

    The rows here carry ~48KB each, none of it at the top level.  The binary
    reader accounts for a whole row of wire bytes so nesting is covered by
    construction, and the TEXT reader counts each column's full text form, which
    contains the nested values.  Neither is obvious from the call site, hence
    this test: if either stopped seeing nested payload, the capped leg would
    produce 2048-row chunks like the uncapped one.
    """
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        groups, max_rows, num_rows = _chunk_row_counts(
            pgduck_conn,
            tmp_path / "nested_uncapped.parquet",
            "scanner_wide_nested_tbl",
            0,
        )
        assert num_rows == 256
        assert groups == 1
        assert max_rows == 256  # one chunk, no size bound

        # ~48KB per row against a 1MB cap is ~21 rows per chunk over the binary
        # protocol and fewer over TEXT, where bytea arrives as \x hex at twice
        # the size.  The bound is loose because the two protocols legitimately
        # count different numbers of bytes for the same row.
        groups, max_rows, num_rows = _chunk_row_counts(
            pgduck_conn,
            tmp_path / "nested_capped.parquet",
            "scanner_wide_nested_tbl",
            _CHUNK_CAP_BYTES,
        )
        assert num_rows == 256
        assert groups >= 4
        assert max_rows <= 64
    finally:
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_default_chunk_cap_keeps_full_chunks_for_narrow_rows(
    pg_tables, pgduck_conn, tmp_path, use_text_protocol
):
    """The default cap must not shrink chunks for ordinary row widths.

    16MB over 2048 rows is 8KB per row, so anything narrower still fills a
    whole chunk and the cap costs nothing.  Writing with a 1-byte row group
    limit turns every chunk into its own row group, which is how the chunk
    sizes the scanner produced are read back out of the Parquet footer.
    """
    path = tmp_path / "narrow.parquet"
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        run_command(
            f"COPY (SELECT * FROM {_scan('scanner_narrow_rows_tbl')}) "
            f"TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE_BYTES 1)",
            pgduck_conn,
        )
    finally:
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)

    rows = run_query(
        f"SELECT max(row_group_num_rows), "
        f"       sum(row_group_num_rows) FILTER (WHERE path_in_schema = 'id') "
        f"FROM parquet_metadata('{path}')",
        pgduck_conn,
    )
    assert rows[0][0] == 2048  # STANDARD_VECTOR_SIZE, i.e. not size-limited
    assert rows[0][1] == 20000


def test_default_chunk_cap_value(pgduck_conn):
    """pg_max_chunk_size_bytes defaults to the 16MB the patch installs.

    That is an eighth of pg_lake's default row group size, which leaves room for
    the cap being charged in Postgres bytes while the writer thresholds on its
    own in-memory size - up to 8x more for arrays of very short strings.
    """
    rows = run_query(
        "SELECT current_setting('pg_max_chunk_size_bytes')::BIGINT", pgduck_conn
    )
    assert rows[0][0] == 16 * 1024 * 1024

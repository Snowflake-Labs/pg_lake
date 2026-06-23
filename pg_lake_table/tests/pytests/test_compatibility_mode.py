"""
Tests for the per-table iceberg option ``compatibility_mode = 'snowflake'``.

Snowflake cannot store a UUID inside a structured / semi-structured type. This
option makes a nested uuid (inside an array, map, or composite) be stored
PHYSICALLY as Iceberg ``string`` while leaving the Postgres column type exactly
as declared.  A top-level uuid column stays a native Iceberg ``uuid``.

Unlike the type-rewrite approach, the user-facing schema never changes, so the
decisive invariants this suite checks are:

  - the Postgres column type is UNCHANGED (still uuid / uuid[] / the named
    composite) -- nothing the user sees is rewritten;
  - the Iceberg metadata schema AND the physical Parquet data files carry no
    uuid at any nested depth (only a top-level uuid column stays uuid);
  - values round-trip exactly through the string storage;
  - DEFAULT / GENERATED columns are accepted (they have no analogue restriction
    here because the column type is not rewritten);
  - both write paths convert: row-by-row INSERT .. VALUES (CSV ingest) and
    pushed-down INSERT .. SELECT;
  - a table without the option still stores native uuid (no regression).
"""

import json

import pytest
from utils_pytest import *


# ---------------------------------------------------------------------------
# introspection helpers
# ---------------------------------------------------------------------------


def _col_format_type(pg_conn, table, column):
    """format_type() of a top-level column."""
    return run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        f"WHERE attrelid = '{table}'::regclass AND attname = '{column}'",
        pg_conn,
    )[0][0]


def _composite_attrs_of_column(pg_conn, table, column):
    """(attname, format_type) of the composite type backing a column."""
    col_type_oid = run_query(
        "SELECT atttypid FROM pg_attribute "
        f"WHERE attrelid = '{table}'::regclass AND attname = '{column}'",
        pg_conn,
    )[0][0]
    return run_query(
        f"""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.oid = {col_type_oid} AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        pg_conn,
    )


def _iceberg_fields(s3, pg_conn, table):
    """Parse the iceberg metadata and return the top-level schema fields."""
    metadata_path = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table}'",
        pg_conn,
    )[0][0]
    data = read_s3_operations(s3, metadata_path)
    return json.loads(data)["schemas"][0]["fields"]


def _iceberg_field_type(s3, pg_conn, table, column):
    """The iceberg "type" node of a named top-level column."""
    return next(
        f["type"] for f in _iceberg_fields(s3, pg_conn, table) if f["name"] == column
    )


def _uuid_depths(itype, depth):
    """Nesting depths at which a uuid logical type appears (0 == top-level)."""
    if isinstance(itype, str):
        return [depth] if itype == "uuid" else []
    kind = itype["type"]
    if kind == "list":
        return _uuid_depths(itype["element"], depth + 1)
    if kind == "map":
        return _uuid_depths(itype["key"], depth + 1) + _uuid_depths(
            itype["value"], depth + 1
        )
    if kind == "struct":
        out = []
        for f in itype["fields"]:
            out += _uuid_depths(f["type"], depth + 1)
        return out
    return []


def _assert_no_nested_uuid(s3, pg_conn, table):
    """No uuid logical type at depth > 0 anywhere in the iceberg schema."""
    for f in _iceberg_fields(s3, pg_conn, table):
        depths = _uuid_depths(f["type"], 0)
        assert all(
            d == 0 for d in depths
        ), f"field {f['name']} has a nested uuid (depths {depths}) in {table}"


def _data_file_paths(conn, table):
    """The committed DATA files (parquet) backing an iceberg table."""
    metadata_path = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table}'",
        conn,
    )[0][0]
    rows = run_query(
        f"SELECT file_path FROM lake_iceberg.files('{metadata_path}') "
        "WHERE content = 'DATA'",
        conn,
    )
    return [r[0] for r in rows]


def _parquet_uuid_leaves(duck, file_path):
    """
    Parquet leaves serialized as a uuid.  A nested uuid stored as string MUST be
    a BYTE_ARRAY; if it shows up here as a UUID logical type / FIXED_LEN_BYTE_ARRAY
    the data file diverges from the iceberg schema.
    """
    rows = duck.execute(
        "SELECT name, type::varchar, logical_type::varchar "
        f"FROM parquet_schema('{file_path}')"
    ).fetchall()
    return [
        (name, ptype, ltype)
        for (name, ptype, ltype) in rows
        if (ltype and "UUID" in ltype.upper()) or ptype == "FIXED_LEN_BYTE_ARRAY"
    ]


def _assert_data_files_uuid_free(conn, table, allowed_top_level=()):
    """The physical Parquet data files must carry string, never uuid, for every
    nested leaf. A top-level uuid column legitimately stays uuid; pass its
    parquet leaf name(s) in allowed_top_level to exempt it."""
    paths = _data_file_paths(conn, table)
    assert paths, f"no data files written for {table}"
    duck = create_duckdb_conn()
    try:
        for path in paths:
            offenders = [
                o
                for o in _parquet_uuid_leaves(duck, path)
                if o[0] not in allowed_top_level
            ]
            assert (
                not offenders
            ), f"{table} data file {path} serialized uuid leaves: {offenders}"
    finally:
        duck.close()


# ---------------------------------------------------------------------------
# option validation
# ---------------------------------------------------------------------------


def test_option_accepted_and_auto_is_noop(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        "CREATE TABLE compat_ok (id int) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    run_command(
        "CREATE TABLE compat_auto (id int, us uuid[]) USING iceberg "
        "WITH (compatibility_mode = 'auto');",
        pg_conn,
    )
    # 'auto' applies no divergence; the column type is unchanged either way.
    assert _col_format_type(pg_conn, "compat_auto", "us") == "uuid[]"
    pg_conn.rollback()


def test_option_invalid_value_rejected(s3, pg_conn, extension, with_default_location):
    error = run_command(
        "CREATE TABLE compat_bad (id int) USING iceberg "
        "WITH (compatibility_mode = 'redshift');",
        pg_conn,
        raise_error=False,
    )
    assert "invalid compatibility_mode option" in error
    pg_conn.rollback()


def test_option_rejected_on_non_iceberg(s3, pg_conn, extension):
    location = f"s3://{TEST_BUCKET}/compat_nope.csv"
    error = run_command(
        f"CREATE FOREIGN TABLE compat_csv (id int) SERVER pg_lake "
        f"OPTIONS (format 'csv', path '{location}', compatibility_mode 'snowflake');",
        pg_conn,
        raise_error=False,
    )
    assert "compatibility_mode" in error
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# Postgres column type is never rewritten
# ---------------------------------------------------------------------------


def test_postgres_types_unchanged(s3, pg_conn, extension, with_default_location):
    """The user-facing schema is identical to a plain table: top-level uuid,
    uuid[], and the named composite are all preserved (the whole point vs a
    type rewrite)."""
    run_command(
        """
        CREATE TYPE compat_acct AS (acct_id uuid, note text);
        CREATE TABLE compat_types (
            id   uuid DEFAULT gen_random_uuid(),
            tags uuid[],
            meta compat_acct
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    assert _col_format_type(pg_conn, "compat_types", "id") == "uuid"
    assert _col_format_type(pg_conn, "compat_types", "tags") == "uuid[]"
    # the named composite is preserved (not rebuilt into a generated type)
    assert _col_format_type(pg_conn, "compat_types", "meta") == "compat_acct"
    assert _composite_attrs_of_column(pg_conn, "compat_types", "meta") == [
        ["acct_id", "uuid"],
        ["note", "text"],
    ]
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# storage divergence + round-trip (INSERT .. VALUES / CSV ingest path)
# ---------------------------------------------------------------------------


def test_nested_uuid_array_stored_as_string(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        "CREATE TABLE compat_arr (id int, tags uuid[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    run_command(
        """
        INSERT INTO compat_arr VALUES
          (1, ARRAY['11111111-1111-1111-1111-111111111111'::uuid,
                    '22222222-2222-2222-2222-222222222222'::uuid]),
          (2, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # storage: no nested uuid in iceberg metadata nor in the parquet data files
    _assert_no_nested_uuid(s3, pg_conn, "compat_arr")
    _assert_data_files_uuid_free(pg_conn, "compat_arr")

    # column type unchanged and values round-trip exactly
    assert _col_format_type(pg_conn, "compat_arr", "tags") == "uuid[]"
    assert run_query(
        "SELECT tags[1]::text, tags[2]::text FROM compat_arr WHERE id = 1", pg_conn
    ) == [
        [
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
        ]
    ]
    assert run_query("SELECT tags FROM compat_arr WHERE id = 2", pg_conn) == [[None]]

    run_command("DROP TABLE compat_arr;", pg_conn)
    pg_conn.commit()


def test_nested_uuid_composite_stored_as_string(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        """
        CREATE TYPE compat_c1 AS (cid uuid, note text);
        CREATE TABLE compat_comp (id int, c compat_c1) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        INSERT INTO compat_comp VALUES
          (1, ROW('33333333-3333-3333-3333-333333333333'::uuid, 'hi')::compat_c1);
        """,
        pg_conn,
    )
    pg_conn.commit()

    _assert_no_nested_uuid(s3, pg_conn, "compat_comp")
    _assert_data_files_uuid_free(pg_conn, "compat_comp")

    # the named composite is preserved and its uuid field still reads as uuid
    assert _col_format_type(pg_conn, "compat_comp", "c") == "compat_c1"
    assert run_query(
        "SELECT (c).cid::text, (c).note FROM compat_comp WHERE id = 1", pg_conn
    ) == [["33333333-3333-3333-3333-333333333333", "hi"]]

    run_command("DROP TABLE compat_comp; DROP TYPE compat_c1;", pg_conn)
    pg_conn.commit()


def test_deep_nesting_uuid_array_in_composite(
    s3, pg_conn, extension, with_default_location
):
    """uuid[] inside a composite (array-in-struct) is still uuid-free in storage."""
    run_command(
        """
        CREATE TYPE compat_c2 AS (cids uuid[], note text);
        CREATE TABLE compat_deep (id int, c compat_c2) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        INSERT INTO compat_deep VALUES
          (1, ROW(ARRAY['44444444-4444-4444-4444-444444444444'::uuid], 'x')::compat_c2);
        """,
        pg_conn,
    )
    pg_conn.commit()

    _assert_no_nested_uuid(s3, pg_conn, "compat_deep")
    _assert_data_files_uuid_free(pg_conn, "compat_deep")
    assert _col_format_type(pg_conn, "compat_deep", "c") == "compat_c2"
    assert run_query(
        "SELECT (c).cids[1]::text FROM compat_deep WHERE id = 1", pg_conn
    ) == [["44444444-4444-4444-4444-444444444444"]]

    run_command("DROP TABLE compat_deep; DROP TYPE compat_c2;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# top-level uuid is NOT diverged (stays native iceberg uuid)
# ---------------------------------------------------------------------------


def test_top_level_uuid_stays_native(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE compat_top (id int, u uuid) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    run_command(
        "INSERT INTO compat_top VALUES "
        "(1, '55555555-5555-5555-5555-555555555555'::uuid);",
        pg_conn,
    )
    pg_conn.commit()

    # the top-level uuid column is left as a native iceberg uuid
    assert _iceberg_field_type(s3, pg_conn, "compat_top", "u") == "uuid"
    assert _col_format_type(pg_conn, "compat_top", "u") == "uuid"
    assert run_query("SELECT u::text FROM compat_top", pg_conn) == [
        ["55555555-5555-5555-5555-555555555555"]
    ]

    run_command("DROP TABLE compat_top;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# pushed-down INSERT .. SELECT write path
# ---------------------------------------------------------------------------


def test_insert_select_pushdown_path(s3, pg_conn, extension, with_default_location):
    """A pushed-down INSERT .. SELECT (all-iceberg) takes the native-type walker
    path; it must also store nested uuid as string."""
    run_command(
        """
        CREATE TABLE compat_src (tags uuid[]) USING iceberg;
        INSERT INTO compat_src VALUES
          (ARRAY['66666666-6666-6666-6666-666666666666'::uuid]);
        CREATE TABLE compat_dst (tags uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()
    run_command("INSERT INTO compat_dst SELECT tags FROM compat_src;", pg_conn)
    pg_conn.commit()

    _assert_no_nested_uuid(s3, pg_conn, "compat_dst")
    _assert_data_files_uuid_free(pg_conn, "compat_dst")
    assert run_query("SELECT tags[1]::text FROM compat_dst", pg_conn) == [
        ["66666666-6666-6666-6666-666666666666"]
    ]

    run_command("DROP TABLE compat_dst; DROP TABLE compat_src;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# ALTER ADD COLUMN + DEFAULT clause
# ---------------------------------------------------------------------------


def test_alter_add_nested_uuid_column(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE compat_alter (id int) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    run_command("ALTER TABLE compat_alter ADD COLUMN tags uuid[];", pg_conn)
    run_command(
        "INSERT INTO compat_alter VALUES "
        "(1, ARRAY['77777777-7777-7777-7777-777777777777'::uuid]);",
        pg_conn,
    )
    pg_conn.commit()

    _assert_no_nested_uuid(s3, pg_conn, "compat_alter")
    _assert_data_files_uuid_free(pg_conn, "compat_alter")
    assert _col_format_type(pg_conn, "compat_alter", "tags") == "uuid[]"

    run_command("DROP TABLE compat_alter;", pg_conn)
    pg_conn.commit()


def test_default_clause_accepted_with_nested_uuid(
    s3, pg_conn, extension, with_default_location
):
    """A column carrying DEFAULT is accepted even alongside a nested-uuid column
    (a type rewrite would have to reject it; this approach does not)."""
    run_command(
        """
        CREATE TABLE compat_def (
            id   uuid DEFAULT gen_random_uuid(),
            tags uuid[]
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        INSERT INTO compat_def (tags) VALUES
          (ARRAY['88888888-8888-8888-8888-888888888888'::uuid]);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert run_query(
        "SELECT count(*) FROM compat_def WHERE id IS NOT NULL", pg_conn
    ) == [[1]]
    # tags is stored as string; the top-level id uuid is legitimately native
    _assert_data_files_uuid_free(pg_conn, "compat_def", allowed_top_level={"id"})
    # top-level uuid id is still native uuid
    assert _iceberg_field_type(s3, pg_conn, "compat_def", "id") == "uuid"

    run_command("DROP TABLE compat_def;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# regression: without the option, nested uuid is stored natively
# ---------------------------------------------------------------------------


def test_non_compat_keeps_native_uuid(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE compat_none (id int, tags uuid[]) USING iceberg;",
        pg_conn,
    )
    run_command(
        "INSERT INTO compat_none VALUES "
        "(1, ARRAY['99999999-9999-9999-9999-999999999999'::uuid]);",
        pg_conn,
    )
    pg_conn.commit()

    # the element IS a uuid at depth 1 in the iceberg metadata
    tags_type = _iceberg_field_type(s3, pg_conn, "compat_none", "tags")
    assert _uuid_depths(tags_type, 0) == [1]

    run_command("DROP TABLE compat_none;", pg_conn)
    pg_conn.commit()

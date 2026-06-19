"""
Tests for the per-table iceberg option `compatibility_mode = 'snowflake'`.

Snowflake cannot store a UUID inside a semi-structured or structured type, so
when the option is set pg_lake rewrites any uuid that appears nested inside an
array or composite to text.  A top-level uuid column is left native.

This suite OWNS the exhaustive conversion matrix (snowflake_cdc only does a
representative end-to-end check):
  - option validation
  - top-level uuid stays uuid; nested uuid -> text at every depth
  - multiple converting columns in one table
  - dropped-column lifecycle with data written on both sides of the drop
  - CREATE and ALTER ADD COLUMN
  - composition with numeric->double, timetz, and out_of_range clamping
"""

import json

import pytest
from utils_pytest import *


# ---------------------------------------------------------------------------
# helpers
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
        "SELECT metadata_location FROM iceberg_tables " f"WHERE table_name = '{table}'",
        pg_conn,
    )[0][0]
    data = read_s3_operations(s3, metadata_path)
    return json.loads(data)["schemas"][0]["fields"]


def _uuid_depths(itype, depth):
    """
    Collect the nesting depths at which a uuid logical type appears in an
    iceberg field "type" (depth 0 == top-level column type).
    """
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
    """The decisive invariant: no uuid logical type at depth > 0 anywhere."""
    fields = _iceberg_fields(s3, pg_conn, table)
    for f in fields:
        depths = _uuid_depths(f["type"], 0)
        assert all(
            d == 0 for d in depths
        ), f"field {f['name']} has a nested uuid (depths {depths}) in {table}"


# ---------------------------------------------------------------------------
# option validation
# ---------------------------------------------------------------------------


def test_option_accepted_on_iceberg(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE compat_ok (id int) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.rollback()


def test_option_auto_is_default_noop(s3, pg_conn, extension, with_default_location):
    """'auto' is accepted and, like the unset default, applies no rewrites."""
    run_command(
        "CREATE TABLE compat_auto (id int, us uuid[]) USING iceberg "
        "WITH (compatibility_mode = 'auto');",
        pg_conn,
    )
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
    """compatibility_mode is an iceberg-only option."""
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
# top-level vs nested uuid
# ---------------------------------------------------------------------------


def test_top_level_uuid_stays_uuid(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE compat_top (id int, u uuid) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    assert _col_format_type(pg_conn, "compat_top", "u") == "uuid"
    pg_conn.rollback()


def test_uuid_array_converted_to_text(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE compat_arr (id int, us uuid[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    assert _col_format_type(pg_conn, "compat_arr", "us") == "text[]"
    pg_conn.rollback()


def test_composite_uuid_field_converted(s3, pg_conn, extension, with_default_location):
    run_command(
        """
        CREATE TYPE compat_c1 AS (cid uuid, note text);
        CREATE TABLE compat_comp (id int, c compat_c1) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    # the composite is rebuilt (its oid differs from the user's type)
    assert _col_format_type(pg_conn, "compat_comp", "c") != "compat_c1"
    assert _composite_attrs_of_column(pg_conn, "compat_comp", "c") == [
        ["cid", "text"],
        ["note", "text"],
    ]
    pg_conn.rollback()


def test_composite_uuid_array_field_converted(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        """
        CREATE TYPE compat_c2 AS (cids uuid[], note text);
        CREATE TABLE compat_comp2 (id int, c compat_c2) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    assert _composite_attrs_of_column(pg_conn, "compat_comp2", "c") == [
        ["cids", "text[]"],
        ["note", "text"],
    ]
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# maps (a map is a domain over an array of key/value structs, so a nested uuid
# must stay a MAP with the uuid leaf rewritten, not collapse to array<struct>)
# ---------------------------------------------------------------------------


def _column_type_changed(pg_conn, table, column, original_type):
    """True if the column's type oid differs from original_type (a rebuild)."""
    return run_query(
        f"SELECT atttypid <> '{original_type}'::regtype FROM pg_attribute "
        f"WHERE attrelid = '{table}'::regclass AND attname = '{column}'",
        pg_conn,
    )[0][0]


def test_map_uuid_value_converted(s3, pg_conn, extension, with_default_location):
    """A nested uuid map value becomes text while the column stays a MAP."""
    map_type = create_map_type("text", "uuid")
    run_command(
        f"CREATE TABLE compat_map_val (id int, m {map_type}) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    try:
        assert _column_type_changed(pg_conn, "compat_map_val", "m", map_type)
        converted = _col_format_type(pg_conn, "compat_map_val", "m")

        fields = _iceberg_fields(s3, pg_conn, "compat_map_val")
        m_field = next(f for f in fields if f["name"] == "m")
        assert m_field["type"]["type"] == "map"
        assert m_field["type"]["key"] == "string"
        assert m_field["type"]["value"] == "string"
        _assert_no_nested_uuid(s3, pg_conn, "compat_map_val")

        # data round-trips through the rewritten text value
        u = "a0974028-d682-4796-b142-b96580b1c103"
        run_command(
            f"INSERT INTO compat_map_val VALUES (1, ARRAY[('k', '{u}')]::{converted});",
            pg_conn,
        )
        pg_conn.commit()
        rows = run_query("SELECT id, m FROM compat_map_val ORDER BY id", pg_conn)
        assert rows[0][0] == 1
        assert rows[0][1] is not None
    finally:
        run_command("DROP TABLE compat_map_val", pg_conn)
        pg_conn.commit()


def test_map_uuid_key_converted(s3, pg_conn, extension, with_default_location):
    """A nested uuid map key becomes text while the column stays a MAP."""
    map_type = create_map_type("uuid", "text")
    run_command(
        f"CREATE TABLE compat_map_key (id int, m {map_type}) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    try:
        assert _column_type_changed(pg_conn, "compat_map_key", "m", map_type)
        fields = _iceberg_fields(s3, pg_conn, "compat_map_key")
        m_field = next(f for f in fields if f["name"] == "m")
        assert m_field["type"]["type"] == "map"
        assert m_field["type"]["key"] == "string"
        _assert_no_nested_uuid(s3, pg_conn, "compat_map_key")
    finally:
        run_command("DROP TABLE compat_map_key", pg_conn)
        pg_conn.commit()


def test_uuid_free_map_not_rebuilt(s3, pg_conn, extension, with_default_location):
    """Regression guard: a map with no uuid must NOT be rebuilt."""
    map_type = create_map_type("int", "text")
    run_command(
        f"CREATE TABLE compat_map_clean (id int, m {map_type}) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    assert not _column_type_changed(pg_conn, "compat_map_clean", "m", map_type)
    pg_conn.rollback()


def test_uuid_free_composite_not_rebuilt(s3, pg_conn, extension, with_default_location):
    """Regression guard: a composite with no uuid must NOT be rebuilt."""
    run_command(
        """
        CREATE TYPE compat_clean AS (a int, b text);
        CREATE TABLE compat_clean_tbl (id int, c compat_clean) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    assert _col_format_type(pg_conn, "compat_clean_tbl", "c") == "compat_clean"
    pg_conn.rollback()


def test_deeply_nested_uuid_all_converted(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        """
        CREATE TYPE leaf AS (k uuid, ks uuid[], label text);
        CREATE TYPE mid  AS (one leaf, many leaf[], mid_id uuid);
        CREATE TABLE compat_deep (
            pk       bigint,
            top_id   uuid,
            flat_ids uuid[],
            m        mid,
            ms       mid[]
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        assert _col_format_type(pg_conn, "compat_deep", "top_id") == "uuid"
        assert _col_format_type(pg_conn, "compat_deep", "flat_ids") == "text[]"
        # the decisive invariant on the real iceberg schema
        _assert_no_nested_uuid(s3, pg_conn, "compat_deep")
    finally:
        run_command("DROP TABLE compat_deep", pg_conn)
        pg_conn.commit()


def test_multiple_converting_columns(s3, pg_conn, extension, with_default_location):
    """Don't assume one: many uuid[] and many composite columns all convert."""
    run_command(
        """
        CREATE TYPE m_a AS (cid uuid, note text);
        CREATE TYPE m_b AS (cids uuid[], n int);
        CREATE TABLE compat_multi (
            id      int,
            u1      uuid[],
            u2      uuid[],
            ca      m_a,
            cb      m_b,
            ca2     m_a,        -- same composite reused
            top_id  uuid
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        assert _col_format_type(pg_conn, "compat_multi", "u1") == "text[]"
        assert _col_format_type(pg_conn, "compat_multi", "u2") == "text[]"
        assert _col_format_type(pg_conn, "compat_multi", "top_id") == "uuid"
        assert _composite_attrs_of_column(pg_conn, "compat_multi", "ca") == [
            ["cid", "text"],
            ["note", "text"],
        ]
        assert _composite_attrs_of_column(pg_conn, "compat_multi", "ca2") == [
            ["cid", "text"],
            ["note", "text"],
        ]
        assert _composite_attrs_of_column(pg_conn, "compat_multi", "cb") == [
            ["cids", "text[]"],
            ["n", "integer"],
        ]
        _assert_no_nested_uuid(s3, pg_conn, "compat_multi")
    finally:
        run_command("DROP TABLE compat_multi", pg_conn)
        pg_conn.commit()


# ---------------------------------------------------------------------------
# ALTER TABLE ADD COLUMN
# ---------------------------------------------------------------------------


def test_alter_add_nested_uuid_converted(s3, pg_conn, extension, with_default_location):
    run_command(
        """
        CREATE TABLE compat_alter (id int) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        ALTER TABLE compat_alter ADD COLUMN us uuid[];
        ALTER TABLE compat_alter ADD COLUMN top_id uuid;
        """,
        pg_conn,
    )
    assert _col_format_type(pg_conn, "compat_alter", "us") == "text[]"
    assert _col_format_type(pg_conn, "compat_alter", "top_id") == "uuid"
    pg_conn.rollback()


def test_alter_add_without_option_keeps_uuid(
    s3, pg_conn, extension, with_default_location
):
    """A table created without the option does not convert on ALTER."""
    run_command(
        """
        CREATE TABLE compat_noopt (id int) USING iceberg;
        ALTER TABLE compat_noopt ADD COLUMN us uuid[];
        """,
        pg_conn,
    )
    assert _col_format_type(pg_conn, "compat_noopt", "us") == "uuid[]"
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# dropped-column lifecycle (data written on both sides of the drop)
# ---------------------------------------------------------------------------


def test_dropped_uuid_array_column_lifecycle(
    s3, pg_conn, extension, with_default_location
):
    u1 = "11111111-1111-1111-1111-111111111111"
    u2 = "22222222-2222-2222-2222-222222222222"

    run_command(
        """
        CREATE TABLE compat_drop (pk int, top_id uuid, keep_ids uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()
    # batch 1
    run_command(
        f"INSERT INTO compat_drop VALUES (1, '{u1}', ARRAY['{u1}','{u2}']::uuid[]);",
        pg_conn,
    )
    # add a nested-uuid column, write batch 2 populating it
    run_command("ALTER TABLE compat_drop ADD COLUMN extra uuid[];", pg_conn)
    assert _col_format_type(pg_conn, "compat_drop", "extra") == "text[]"
    run_command(
        f"INSERT INTO compat_drop VALUES (2, '{u2}', ARRAY['{u2}']::uuid[], ARRAY['{u1}']::uuid[]);",
        pg_conn,
    )
    # drop it, write batch 3
    run_command("ALTER TABLE compat_drop DROP COLUMN extra;", pg_conn)
    run_command(
        f"INSERT INTO compat_drop VALUES (3, '{u1}', ARRAY['{u1}']::uuid[]);",
        pg_conn,
    )
    pg_conn.commit()

    try:
        # final query: all three batches survive, conversions intact
        rows = run_query(
            "SELECT pk, top_id, keep_ids FROM compat_drop ORDER BY pk", pg_conn
        )
        assert [r[0] for r in rows] == [1, 2, 3]
        assert rows[0][1] == u1
        assert rows[0][2] == [u1, u2]
        assert rows[1][2] == [u2]
        assert rows[2][2] == [u1]
        # the dropped slot must not have re-appeared / left a uuid behind
        _assert_no_nested_uuid(s3, pg_conn, "compat_drop")
    finally:
        run_command("DROP TABLE compat_drop", pg_conn)
        pg_conn.commit()


def test_dropped_composite_attribute(s3, pg_conn, extension, with_default_location):
    """A composite whose uuid[] attribute was dropped must skip that attr."""
    run_command(
        """
        CREATE TYPE drop_attr AS (ids uuid[], note text);
        ALTER TYPE drop_attr DROP ATTRIBUTE ids;
        CREATE TABLE compat_drop_attr (id int, c drop_attr) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    # only the remaining (uuid-free) attribute survives; nothing rebuilt to uuid
    assert _composite_attrs_of_column(pg_conn, "compat_drop_attr", "c") == [
        ["note", "text"]
    ]
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# value round-trip
# ---------------------------------------------------------------------------


def test_nested_uuid_value_roundtrip(s3, pg_conn, extension, with_default_location):
    u1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    u2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    run_command(
        """
        CREATE TYPE rt_comp AS (cid uuid, note text);
        CREATE TABLE compat_rt (id int, top_id uuid, us uuid[], c rt_comp) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO compat_rt VALUES
            (1, '{u1}', ARRAY['{u1}','{u2}']::uuid[], ROW('{u2}', 'hi'));
        """,
        pg_conn,
    )
    rows = run_query("SELECT top_id, us, (c).cid, (c).note FROM compat_rt", pg_conn)
    assert rows[0][0] == u1
    assert rows[0][1] == [u1, u2]  # text[] preserving the canonical strings
    assert rows[0][2] == u2
    assert rows[0][3] == "hi"
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# iceberg metadata logical types
# ---------------------------------------------------------------------------


def test_iceberg_metadata_logical_types(s3, pg_conn, extension, with_default_location):
    location = f"s3://{TEST_BUCKET}/compat_meta"
    run_command(
        f"""
        CREATE TYPE meta_comp AS (cid uuid, note text);
        CREATE FOREIGN TABLE compat_meta (
            top_id uuid,
            us     uuid[],
            c      meta_comp
        ) SERVER pg_lake_iceberg
          OPTIONS (location '{location}', compatibility_mode 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        fields = _iceberg_fields(s3, pg_conn, "compat_meta")
        top = next(f for f in fields if f["name"] == "top_id")
        us = next(f for f in fields if f["name"] == "us")
        c = next(f for f in fields if f["name"] == "c")

        assert top["type"] == "uuid"  # top-level stays uuid
        assert us["type"]["type"] == "list"
        assert us["type"]["element"] == "string"
        assert c["type"]["type"] == "struct"
        cid = next(f for f in c["type"]["fields"] if f["name"] == "cid")
        assert cid["type"] == "string"

        _assert_no_nested_uuid(s3, pg_conn, "compat_meta")
    finally:
        run_command("DROP FOREIGN TABLE compat_meta", pg_conn)
        pg_conn.commit()


# ---------------------------------------------------------------------------
# composition: uuid + numeric + timetz + out_of_range clamp on one table
# ---------------------------------------------------------------------------


def test_combined_all_conversions(s3, pg_conn, extension, with_default_location):
    """
    A single composite carrying every conversion class at once proves the
    passes compose: nested uuid -> text AND unsupported numeric -> double on
    the same rebuilt composite, with timetz/clamping still applied at write.
    """
    run_command(
        """
        CREATE TYPE kitchen AS (
            cid       uuid,
            cids      uuid[],
            c_unb     numeric,
            c_large   numeric(50,2),
            c_bounded numeric(10,2),
            c_ttz     timetz,
            c_ttz_arr timetz[],
            c_label   text
        );
        CREATE TABLE compat_kitchen (
            pk          bigint,
            top_id      uuid,
            top_unb     numeric,
            top_bounded numeric(10,2),
            top_ttz     timetz,
            k           kitchen,
            ks          kitchen[]
        ) USING iceberg
          WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # top-level: uuid stays, unbounded numeric -> double, bounded numeric and
        # timetz are kept as-is (not rebuilt, so the typmod survives intact)
        assert _col_format_type(pg_conn, "compat_kitchen", "top_id") == "uuid"
        assert (
            _col_format_type(pg_conn, "compat_kitchen", "top_unb") == "double precision"
        )
        assert (
            _col_format_type(pg_conn, "compat_kitchen", "top_bounded")
            == "numeric(10,2)"
        )
        assert (
            _col_format_type(pg_conn, "compat_kitchen", "top_ttz")
            == "time with time zone"
        )

        # the rebuilt composite carries BOTH uuid->text AND numeric->double on the
        # same struct, with the bounded numeric / timetz fields still present.
        # (Bounded numeric typmod is dropped by the composite-rebuild round-trip;
        # we only assert it stays a numeric, which is orthogonal to this feature.)
        attrs = dict(
            (name, ftype)
            for name, ftype in _composite_attrs_of_column(
                pg_conn, "compat_kitchen", "k"
            )
        )
        assert attrs["cid"] == "text"
        assert attrs["cids"] == "text[]"
        assert attrs["c_unb"] == "double precision"
        assert attrs["c_large"] == "double precision"
        assert attrs["c_bounded"].startswith("numeric")
        assert attrs["c_ttz"] == "time with time zone"
        assert attrs["c_ttz_arr"] == "time with time zone[]"
        assert attrs["c_label"] == "text"

        # no uuid survives nested anywhere in the iceberg schema
        _assert_no_nested_uuid(s3, pg_conn, "compat_kitchen")

        # values: timetz normalized to UTC; uuid canonical text in the composite;
        # out_of_range clamp applies at write on the top-level bounded numeric.
        uid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        run_command(
            f"""
            INSERT INTO compat_kitchen (pk, top_id, top_unb, top_bounded, top_ttz, k) VALUES
              (1, '{uid}', 1.5, 'NaN'::numeric(10,2), '12:30:00+04'::timetz,
               ROW('{uid}', ARRAY['{uid}']::uuid[], 1.5, 2.5,
                   3.25, '01:00:00+02'::timetz,
                   ARRAY['00:00:00+00']::timetz[], 'lbl'));
            """,
            pg_conn,
        )
        row = run_query(
            """
            SELECT top_id, top_bounded, top_ttz, (k).cid, (k).cids
            FROM compat_kitchen WHERE pk = 1
            """,
            pg_conn,
        )[0]
        assert row[0] == uid
        assert row[1] is None  # NaN clamped to NULL on the bounded decimal
        # timetz normalized to UTC: 12:30+04 -> 08:30+00
        assert str(row[2]) == "08:30:00+00:00"
        assert row[3] == uid  # composite uuid is text now, value preserved
        assert row[4] == [uid]
    finally:
        run_command("DROP TABLE compat_kitchen", pg_conn)
        pg_conn.commit()

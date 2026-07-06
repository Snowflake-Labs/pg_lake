"""
Per-leaf Snowflake size caps *inside containers* (array / composite / map) for
Iceberg tables declared with ``compatibility_mode = 'snowflake'``.

These are the cases the aggregate OBJECT/ARRAY/VARIANT cap does NOT cover: a
single string/binary leaf over its own per-leaf cap (16 MiB STRING, 8 MiB
BINARY) while the whole container stays far under the 128 MiB nested cap.
Snowflake maps a pg array/composite to a *typed* ARRAY(VARCHAR) /
OBJECT(... VARCHAR ...), and each VARCHAR leaf carries the same physical value
limit a top-level string does -- a single oversize leaf breaks materialization
into a native Snowflake table even though the container as a whole is small.

Both write halves must descend into the container and clamp the leaf:

  - per-tuple FDW path   -> INSERT ... VALUES (...)
  - SQL pushdown wrapper -> INSERT INTO snowflake_t SELECT FROM other_lake_t
    (list_transform for arrays, struct_pack for composites)

Fixtures use a single element/field just over the cap, so the container total
is ~16 MiB -- three orders of magnitude under the nested cap -- which is what
makes the per-leaf clamp, not the aggregate check, the thing under test.

These tests live in their own module (separate from test_iceberg_size_clamping)
so they run on a fresh module-scoped backend: the suite's ``pg_conn`` is
module-scoped and a long-lived backend accumulates syscache-invalidation
callbacks across every table/type it touches, so a single file with too many
heavy table-creating tests can exhaust PostgreSQL's fixed callback list.
"""

import pytest
from utils_pytest import *


STRING_CAP = 16 * 1024 * 1024
BINARY_CAP = 8 * 1024 * 1024


def _just_over(cap, padding=128):
    """Return a byte count a little over the cap."""
    return cap + padding


# ---------------------------------------------------------------------------
# text[] -- element over the STRING cap
# ---------------------------------------------------------------------------


def test_text_array_element_over_cap_clamps_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """A text[] element > 16 MiB is truncated to the STRING cap under 'clamp',
    even though the array as a whole is far under the nested cap."""
    run_command(
        "CREATE SCHEMA test_size_arr_clamp_pt;"
        "SET search_path TO test_size_arr_clamp_pt;"
        "CREATE TABLE t (id int, v text[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "x" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, ARRAY[%s, 'small']);", (payload,))
    pg_conn.commit()

    result = run_query(
        "SELECT id, array_length(v, 1), octet_length(v[1]), octet_length(v[2]) FROM t;",
        pg_conn,
    )
    assert [[r[0], r[1], r[2], r[3]] for r in result] == [[1, 2, STRING_CAP, 5]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_arr_clamp_pt CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_array_element_over_cap_errors_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """A text[] element > 16 MiB raises under 'error', naming the column."""
    run_command(
        "CREATE SCHEMA test_size_arr_err_pt;"
        "SET search_path TO test_size_arr_err_pt;"
        "CREATE TABLE t (id int, v text[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "x" * _just_over(STRING_CAP)
    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute("INSERT INTO t VALUES (1, ARRAY[%s]);", (payload,))
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_arr_err_pt CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_array_element_over_cap_clamps_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The pushdown wrapper clamps an oversize text[] element on INSERT..SELECT."""
    run_command(
        "CREATE SCHEMA test_size_arr_clamp_pd;"
        "SET search_path TO test_size_arr_clamp_pd;"
        "CREATE TABLE src (id int, v text[]) USING iceberg;"
        "CREATE TABLE dst (id int, v text[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "p" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (1, ARRAY[%s, 'small']);", (payload,))
    pg_conn.commit()

    run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.commit()

    result = run_query(
        "SELECT id, array_length(v, 1), octet_length(v[1]), octet_length(v[2]) FROM dst;",
        pg_conn,
    )
    assert [[r[0], r[1], r[2], r[3]] for r in result] == [[1, 2, STRING_CAP, 5]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_arr_clamp_pd CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_array_element_over_cap_errors_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The pushdown wrapper raises on an oversize text[] element under 'error'."""
    run_command(
        "CREATE SCHEMA test_size_arr_err_pd;"
        "SET search_path TO test_size_arr_err_pd;"
        "CREATE TABLE src (id int, v text[]) USING iceberg;"
        "CREATE TABLE dst (id int, v text[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "q" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (1, ARRAY[%s]);", (payload,))
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_arr_err_pd CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# composite -- text field over the STRING cap
# ---------------------------------------------------------------------------


def test_composite_text_field_over_cap_clamps_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """A composite's text field > 16 MiB is truncated to the STRING cap under
    'clamp'; the other fields survive and the struct is rebuilt."""
    run_command(
        "CREATE SCHEMA test_size_comp_clamp_pt;"
        "SET search_path TO test_size_comp_clamp_pt;"
        "CREATE TYPE rec AS (a int, b text);"
        "CREATE TABLE t (id int, r rec) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "x" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, ROW(7, %s)::rec);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, (r).a, octet_length((r).b) FROM t;", pg_conn)
    assert [[r[0], r[1], r[2]] for r in result] == [[1, 7, STRING_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_comp_clamp_pt CASCADE;", pg_conn)
    pg_conn.commit()


def test_composite_text_field_over_cap_errors_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """A composite's text field > 16 MiB raises under 'error', naming the column."""
    run_command(
        "CREATE SCHEMA test_size_comp_err_pt;"
        "SET search_path TO test_size_comp_err_pt;"
        "CREATE TYPE rec AS (a int, b text);"
        "CREATE TABLE t (id int, r rec) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "x" * _just_over(STRING_CAP)
    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute("INSERT INTO t VALUES (1, ROW(7, %s)::rec);", (payload,))
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"r"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_comp_err_pt CASCADE;", pg_conn)
    pg_conn.commit()


def test_composite_text_field_over_cap_clamps_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The pushdown struct_pack rewrite clamps an oversize composite text field."""
    run_command(
        "CREATE SCHEMA test_size_comp_clamp_pd;"
        "SET search_path TO test_size_comp_clamp_pd;"
        "CREATE TYPE rec AS (a int, b text);"
        "CREATE TABLE src (id int, r rec) USING iceberg;"
        "CREATE TABLE dst (id int, r rec) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "p" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (1, ROW(7, %s)::rec);", (payload,))
    pg_conn.commit()

    run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.commit()

    result = run_query("SELECT id, (r).a, octet_length((r).b) FROM dst;", pg_conn)
    assert [[r[0], r[1], r[2]] for r in result] == [[1, 7, STRING_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_comp_clamp_pd CASCADE;", pg_conn)
    pg_conn.commit()


def test_composite_text_field_over_cap_errors_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The pushdown rewrite raises on an oversize composite text field under 'error'."""
    run_command(
        "CREATE SCHEMA test_size_comp_err_pd;"
        "SET search_path TO test_size_comp_err_pd;"
        "CREATE TYPE rec AS (a int, b text);"
        "CREATE TABLE src (id int, r rec) USING iceberg;"
        "CREATE TABLE dst (id int, r rec) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "q" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (1, ROW(7, %s)::rec);", (payload,))
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"r"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_comp_err_pd CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# bytea[] -- element over the BINARY cap; nested array-of-composite
# ---------------------------------------------------------------------------


def test_bytea_array_element_over_cap_clamps_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """A bytea[] element > 8 MiB is byte-truncated to the BINARY cap under 'clamp'."""
    run_command(
        "CREATE SCHEMA test_size_bytea_arr_pt;"
        "SET search_path TO test_size_bytea_arr_pt;"
        "CREATE TABLE t (id int, v bytea[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = b"\x00" * _just_over(BINARY_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, ARRAY[%s]::bytea[]);", (payload,))
    pg_conn.commit()

    result = run_query(
        "SELECT id, array_length(v, 1), octet_length(v[1]) FROM t;", pg_conn
    )
    assert [[r[0], r[1], r[2]] for r in result] == [[1, 1, BINARY_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_bytea_arr_pt CASCADE;", pg_conn)
    pg_conn.commit()


def test_nested_array_of_composite_clamps_both_paths(
    pg_conn, extension, s3, with_default_location
):
    """An oversize text leaf nested two levels deep (array of composite) is
    clamped on both the per-tuple walk and the pushdown list_transform +
    struct_pack rewrite."""
    run_command(
        "CREATE SCHEMA test_size_nested;"
        "SET search_path TO test_size_nested;"
        "CREATE TYPE rec2 AS (a int, b text);"
        "CREATE TABLE src (id int, arr rec2[]) USING iceberg;"
        "CREATE TABLE dst (id int, arr rec2[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "n" * _just_over(STRING_CAP)
    # id=1 lands via the per-tuple path (INSERT VALUES straight into dst).
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO dst VALUES (1, ARRAY[ROW(3, %s)::rec2]);", (payload,))
    pg_conn.commit()
    # id=2 lands via the pushdown path (INSERT .. SELECT from a lake source).
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (2, ARRAY[ROW(4, %s)::rec2]);", (payload,))
    pg_conn.commit()
    run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.commit()

    result = run_query(
        "SELECT id, (arr[1]).a, octet_length((arr[1]).b) FROM dst ORDER BY id;",
        pg_conn,
    )
    assert [[r[0], r[1], r[2]] for r in result] == [
        [1, 3, STRING_CAP],
        [2, 4, STRING_CAP],
    ]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_nested CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# jsonb leaf inside a container: a structured-string leaf can't be truncated,
# so an oversize leaf is raised (error) or NULLed (clamp) in place, while the
# container and its other leaves survive.  Mirrors the top-level jsonb rule.
#
# Note: clamp mode is covered for a jsonb *field of a composite* but not for a
# jsonb *array element*.  NULLing an oversize element would leave a NULL inside
# a jsonb[], and pg_lake cannot write a NULL element in a jsonb[] at all (a
# pre-existing limitation, independent of size clamping -- the CSV staging path
# fails to convert it back to JSON[]).  So for jsonb arrays only the error path,
# which raises before producing that value, is exercised here.
# ---------------------------------------------------------------------------


def test_jsonb_array_element_over_cap_errors_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """An oversize jsonb[] element raises under 'error', naming the column."""
    run_command(
        "CREATE SCHEMA test_size_jsonb_arr_err;"
        "SET search_path TO test_size_jsonb_arr_err;"
        "CREATE TABLE t (id int, v jsonb[]) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    big = json.dumps("x" * _just_over(STRING_CAP))
    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute("INSERT INTO t VALUES (1, ARRAY[%s]::jsonb[]);", (big,))
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_jsonb_arr_err CASCADE;", pg_conn)
    pg_conn.commit()


def test_jsonb_composite_field_over_cap_nulls_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """An oversize jsonb field inside a composite is NULLed under 'clamp'; the
    struct is rebuilt and the sibling field survives."""
    run_command(
        "CREATE SCHEMA test_size_jsonb_comp_pt;"
        "SET search_path TO test_size_jsonb_comp_pt;"
        "CREATE TYPE jrec AS (a int, b jsonb);"
        "CREATE TABLE t (id int, r jrec) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    big = json.dumps("x" * _just_over(STRING_CAP))
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, ROW(7, %s)::jrec);", (big,))
    pg_conn.commit()

    result = run_query("SELECT id, (r).a, (r).b IS NULL FROM t;", pg_conn)
    assert [[r[0], r[1], r[2]] for r in result] == [[1, 7, True]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_jsonb_comp_pt CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Pass-through fidelity: under compatibility_mode='snowflake' every container
# column with a clampable leaf is rebuilt on write (per-tuple deform/reform,
# pushdown list_transform/struct_pack) even when nothing is oversize.  A
# round-trip of only-small values must come back byte-identical -- this guards
# the rebuild against dropping/reordering elements or fields.
# ---------------------------------------------------------------------------


def test_small_container_roundtrips_unchanged_under_snowflake(
    pg_conn, extension, s3, with_default_location
):
    """Small-valued array and composite columns survive the snowflake-mode
    rebuild unchanged on both the per-tuple and pushdown write paths."""
    run_command(
        "CREATE SCHEMA test_size_passthrough;"
        "SET search_path TO test_size_passthrough;"
        "CREATE TYPE rec AS (a int, b text);"
        "CREATE TABLE src (id int, v text[], r rec) USING iceberg;"
        "CREATE TABLE dst (id int, v text[], r rec) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    # id=1 via the per-tuple path (INSERT VALUES straight into dst).
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dst VALUES (1, ARRAY['a', 'bb', 'ccc'], ROW(7, 'hello')::rec);"
        )
    pg_conn.commit()
    # id=2 via the pushdown path (INSERT .. SELECT from a lake source).
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO src VALUES (2, ARRAY['d', 'ee'], ROW(9, 'world')::rec);"
        )
    pg_conn.commit()
    run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.commit()

    result = run_query("SELECT id, v, (r).a, (r).b FROM dst ORDER BY id;", pg_conn)
    assert [[r[0], r[1], r[2], r[3]] for r in result] == [
        [1, ["a", "bb", "ccc"], 7, "hello"],
        [2, ["d", "ee"], 9, "world"],
    ]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_passthrough CASCADE;", pg_conn)
    pg_conn.commit()

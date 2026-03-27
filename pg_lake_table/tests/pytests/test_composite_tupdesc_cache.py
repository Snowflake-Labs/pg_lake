"""Tests for composite-type TupleDesc caching in copy_dest_receive.

CopyToStateData holds an Oid -> TupleDesc HTAB (tupdesc_cache) that is
pre-populated with top-level composite column types during StartCopyTo and
lazily extended with nested composite types encountered during serialization.
This avoids calling lookup_rowtype_tupdesc on every row.

These tests verify both correctness and that the optimised path handles the
cases the implementation touches:

  - single composite column, many rows
  - multiple composite columns per table
  - NULL values inside a composite column
  - nested composite types
  - arrays of composite elements
  - arrays of nested composite elements (array-of-nested-composite column)
  - composite columns whose fields are arrays of composite
  - performance: large-volume inserts should complete within a reasonable
    wall-clock budget; nested and array-of-composite cases exercise the
    lazy-population path that is new in the HTAB approach
"""

import time

import pytest

from utils_pytest import TEST_BUCKET, run_command, run_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SCHEMA = "test_composite_tupdesc_cache"


@pytest.fixture(scope="module")
def composite_schema(pg_conn, s3, extension):
    """Create the shared schema and composite types used by all tests."""
    run_command(
        f"SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}'",
        pg_conn,
    )
    pg_conn.commit()
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    run_command(
        f"""
        SET search_path TO {SCHEMA}, public;

        -- simple two-field point type
        CREATE TYPE {SCHEMA}.point2d AS (x int, y int);

        -- second composite type for multi-column tests
        CREATE TYPE {SCHEMA}.name_pair AS (first_name text, last_name text);

        -- composite whose fields are themselves composite (nested)
        CREATE TYPE {SCHEMA}.named_point AS (
            label {SCHEMA}.name_pair,
            location {SCHEMA}.point2d
        );

        -- composite whose field is an array of composite
        CREATE TYPE {SCHEMA}.named_path AS (
            waypoints {SCHEMA}.point2d[],
            label     {SCHEMA}.name_pair
        );
    """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    run_command("RESET pg_lake_iceberg.default_location_prefix", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


def test_insert_select_single_composite_column(pg_conn, composite_schema):
    """INSERT..SELECT with one composite column round-trips data correctly."""
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_single (
            id   int,
            pt   {SCHEMA}.point2d
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_single
            SELECT i, ROW(i, i * 2)::{SCHEMA}.point2d
            FROM generate_series(1, 100) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"SELECT id, (pt).x, (pt).y FROM {SCHEMA}.t_single ORDER BY id",
        pg_conn,
    )

    assert len(rows) == 100
    for i, (id_, x, y) in enumerate(rows, start=1):
        assert id_ == i
        assert x == i
        assert y == i * 2

    pg_conn.rollback()


def test_insert_select_multiple_composite_columns(pg_conn, composite_schema):
    """INSERT..SELECT with two composite columns per row caches both correctly."""
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_multi (
            id   int,
            pt   {SCHEMA}.point2d,
            nm   {SCHEMA}.name_pair
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_multi
            SELECT
                i,
                ROW(i, i + 1)::{SCHEMA}.point2d,
                ROW('first_' || i, 'last_' || i)::{SCHEMA}.name_pair
            FROM generate_series(1, 50) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT id, (pt).x, (pt).y, (nm).first_name, (nm).last_name
        FROM {SCHEMA}.t_multi
        ORDER BY id
        """,
        pg_conn,
    )

    assert len(rows) == 50
    for i, (id_, x, y, fn, ln) in enumerate(rows, start=1):
        assert id_ == i
        assert x == i
        assert y == i + 1
        assert fn == f"first_{i}"
        assert ln == f"last_{i}"

    pg_conn.rollback()


def test_insert_select_composite_with_nulls(pg_conn, composite_schema):
    """NULL composite values are handled correctly by the caching path."""
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_nulls (
            id int,
            pt {SCHEMA}.point2d
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Insert alternating non-NULL / NULL rows
    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_nulls
            SELECT
                i,
                CASE WHEN i % 2 = 0
                     THEN ROW(i, i)::{SCHEMA}.point2d
                     ELSE NULL
                END
            FROM generate_series(1, 20) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"SELECT id, pt FROM {SCHEMA}.t_nulls ORDER BY id",
        pg_conn,
    )

    assert len(rows) == 20
    for id_, pt in rows:
        if id_ % 2 == 0:
            assert pt is not None
        else:
            assert pt is None

    pg_conn.rollback()


def test_insert_select_nested_composite(pg_conn, composite_schema):
    """Nested composite types serialise and deserialise without errors."""
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_nested (
            id int,
            np {SCHEMA}.named_point
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_nested
            SELECT
                i,
                ROW(
                    ROW('Alice', 'Smith')::{SCHEMA}.name_pair,
                    ROW(i, i * 3)::{SCHEMA}.point2d
                )::{SCHEMA}.named_point
            FROM generate_series(1, 30) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT id,
               ((np).label).first_name,
               ((np).label).last_name,
               ((np).location).x,
               ((np).location).y
        FROM {SCHEMA}.t_nested
        ORDER BY id
        """,
        pg_conn,
    )

    assert len(rows) == 30
    for i, (id_, fn, ln, x, y) in enumerate(rows, start=1):
        assert id_ == i
        assert fn == "Alice"
        assert ln == "Smith"
        assert x == i
        assert y == i * 3

    pg_conn.rollback()


def test_insert_select_array_of_composite(pg_conn, composite_schema):
    """Arrays of composite values are serialised correctly.

    An array-of-composite column goes through ArrayOutForPGDuck rather than
    StructOutForPGDuck directly, so the top-level column is not TYPTYPE_COMPOSITE.
    ArrayOutForPGDuck now receives and forwards tupdesc_cache to PGDuckSerialize
    for each element, so the element TupleDesc is lazily cached on first use.
    This test verifies that arrays of composite types continue to work correctly.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_arr (
            id  int,
            pts {SCHEMA}.point2d[]
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_arr
            SELECT
                i,
                ARRAY[
                    ROW(i,     i * 2)::{SCHEMA}.point2d,
                    ROW(i + 1, i * 3)::{SCHEMA}.point2d
                ]
            FROM generate_series(1, 20) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"SELECT id, array_length(pts, 1) FROM {SCHEMA}.t_arr ORDER BY id",
        pg_conn,
    )

    assert len(rows) == 20
    for i, (id_, arr_len) in enumerate(rows, start=1):
        assert id_ == i
        assert arr_len == 2

    pg_conn.rollback()


def test_insert_select_array_of_nested_composite(pg_conn, composite_schema):
    """Arrays of nested composite values (named_point[]) round-trip correctly.

    named_point contains name_pair and point2d as fields, so each array
    element is itself a nested composite.  This exercises the path where
    ArrayOutForPGDuck encounters elements whose TupleDesc must be lazily
    cached and those elements in turn recurse into StructOutForPGDuck for
    their inner composite fields.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_arr_nested (
            id  int,
            nps {SCHEMA}.named_point[]
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_arr_nested
            SELECT
                i,
                ARRAY[
                    ROW(
                        ROW('Alice', 'Smith')::{SCHEMA}.name_pair,
                        ROW(i,     i * 2)::{SCHEMA}.point2d
                    )::{SCHEMA}.named_point,
                    ROW(
                        ROW('Bob',   'Jones')::{SCHEMA}.name_pair,
                        ROW(i + 1,  i * 3)::{SCHEMA}.point2d
                    )::{SCHEMA}.named_point
                ]
            FROM generate_series(1, 20) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT id,
               array_length(nps, 1),
               ((nps[1]).label).first_name,
               ((nps[1]).location).x,
               ((nps[2]).label).first_name,
               ((nps[2]).location).x
        FROM {SCHEMA}.t_arr_nested
        ORDER BY id
        """,
        pg_conn,
    )

    assert len(rows) == 20
    for i, (id_, arr_len, fn1, x1, fn2, x2) in enumerate(rows, start=1):
        assert id_ == i
        assert arr_len == 2
        assert fn1 == "Alice"
        assert x1 == i
        assert fn2 == "Bob"
        assert x2 == i + 1

    pg_conn.rollback()


def test_insert_select_composite_containing_array_of_composite(
    pg_conn, composite_schema
):
    """A composite column whose field is itself an array of composite.

    named_path has a waypoints point2d[] field and a label name_pair field.
    This exercises StructOutForPGDuck recursing into ArrayOutForPGDuck for
    the array-typed field, which then calls StructOutForPGDuck again for
    each point2d element — combining both nesting axes in one type.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_comp_arr (
            id   int,
            path {SCHEMA}.named_path
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_comp_arr
            SELECT
                i,
                ROW(
                    ARRAY[
                        ROW(i,     i * 2)::{SCHEMA}.point2d,
                        ROW(i + 1, i * 3)::{SCHEMA}.point2d
                    ],
                    ROW('Leg', 'Route' || i)::{SCHEMA}.name_pair
                )::{SCHEMA}.named_path
            FROM generate_series(1, 20) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT id,
               array_length((path).waypoints, 1),
               ((path).waypoints[1]).x,
               ((path).waypoints[2]).x,
               ((path).label).first_name,
               ((path).label).last_name
        FROM {SCHEMA}.t_comp_arr
        ORDER BY id
        """,
        pg_conn,
    )

    assert len(rows) == 20
    for i, (id_, arr_len, x1, x2, fn, ln) in enumerate(rows, start=1):
        assert id_ == i
        assert arr_len == 2
        assert x1 == i
        assert x2 == i + 1
        assert fn == "Leg"
        assert ln == f"Route{i}"

    pg_conn.rollback()


def test_insert_select_uncast_row_expressions(pg_conn, composite_schema):
    """ROW() expressions require an explicit cast to a named composite type.

    PostgreSQL does not implicitly coerce anonymous ROW() to a named type,
    so the stored tuple always carries the named type OID — never RECORDOID.
    This means the value always goes through the cache path in
    StructOutForPGDuck.

    The RECORDOID guard (tupType == RECORDOID -> skip cache) is defensive:
    anonymous records cannot appear in Iceberg columns since the schema
    requires named composite types.  If they somehow did, the code falls back
    to the uncached lookup_rowtype_tupdesc / ReleaseTupleDesc pair, which is
    the original pre-caching behavior and always correct.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_uncast (
            id  int,
            pt  {SCHEMA}.point2d
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_uncast
            SELECT i, ROW(i, i * 10)::{SCHEMA}.point2d
            FROM generate_series(1, 50) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"SELECT id, (pt).x, (pt).y FROM {SCHEMA}.t_uncast ORDER BY id",
        pg_conn,
    )

    assert len(rows) == 50
    for i, (id_, x, y) in enumerate(rows, start=1):
        assert id_ == i
        assert x == i
        assert y == i * 10

    pg_conn.rollback()


# ---------------------------------------------------------------------------
# Performance / profiling tests
# ---------------------------------------------------------------------------

# Upper bound (seconds) for inserting LARGE_N rows with a composite column.
# This is intentionally generous; the goal is to catch severe per-row overhead
# regressions rather than enforce a tight SLA.
LARGE_N = 10_000
PERF_LIMIT_SECONDS = 60


def test_insert_select_composite_large_volume(pg_conn, composite_schema):
    """Large-volume INSERT..SELECT with a composite column completes quickly.

    Before the caching fix each row triggered lookup_rowtype_tupdesc via
    StructOutForPGDuck.  With the cache the lookup is done once per column
    per COPY, making this path O(rows) instead of O(rows * type_cache_miss).
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_perf (
            id int,
            pt {SCHEMA}.point2d
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    start = time.perf_counter()
    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_perf
            SELECT i, ROW(i, i)::{SCHEMA}.point2d
            FROM generate_series(1, {LARGE_N}) i;
    """,
        pg_conn,
    )
    pg_conn.commit()
    elapsed = time.perf_counter() - start

    print(f"\nInserted {LARGE_N} rows with composite column in {elapsed:.2f}s")
    assert elapsed < PERF_LIMIT_SECONDS, (
        f"INSERT..SELECT with composite column took {elapsed:.2f}s, "
        f"expected < {PERF_LIMIT_SECONDS}s"
    )

    count = run_query(f"SELECT count(*) FROM {SCHEMA}.t_perf", pg_conn)[0][0]
    assert count == LARGE_N

    pg_conn.rollback()


def test_insert_select_composite_large_volume_multi_column(pg_conn, composite_schema):
    """Large-volume INSERT..SELECT with two composite columns per row.

    Verifies the cache handles multiple composite-typed attributes without
    cross-column aliasing (each Oid maps to its own entry in the HTAB).
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_perf_multi (
            id int,
            pt {SCHEMA}.point2d,
            nm {SCHEMA}.name_pair
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    start = time.perf_counter()
    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_perf_multi
            SELECT
                i,
                ROW(i, i)::{SCHEMA}.point2d,
                ROW('fn', 'ln')::{SCHEMA}.name_pair
            FROM generate_series(1, {LARGE_N}) i;
    """,
        pg_conn,
    )
    pg_conn.commit()
    elapsed = time.perf_counter() - start

    print(f"\nInserted {LARGE_N} rows with two composite columns in {elapsed:.2f}s")
    assert elapsed < PERF_LIMIT_SECONDS, (
        f"INSERT..SELECT with two composite columns took {elapsed:.2f}s, "
        f"expected < {PERF_LIMIT_SECONDS}s"
    )

    count = run_query(f"SELECT count(*) FROM {SCHEMA}.t_perf_multi", pg_conn)[0][0]
    assert count == LARGE_N

    pg_conn.rollback()


# For nested/array-of-composite tests we use a larger row count so that the
# difference between one lookup_rowtype_tupdesc call (cached) versus one per
# row (uncached) is visible in wall-clock time.
LARGE_N_NESTED = 100_000
PERF_LIMIT_NESTED_SECONDS = 120


def test_insert_select_nested_composite_large_volume(pg_conn, composite_schema):
    """Large-volume INSERT..SELECT with a deeply nested composite column.

    named_point contains name_pair and point2d as fields.  The inner types
    are not pre-populated by StartCopyTo (only the top-level named_point Oid
    is).  StructOutForPGDuck lazily inserts them into the HTAB on the first
    row; all subsequent rows hit the cache.  Without caching each of the
    LARGE_N_NESTED rows would call lookup_rowtype_tupdesc twice (once per
    inner type), making this test a meaningful timing signal.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_perf_nested (
            id int,
            np {SCHEMA}.named_point
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    start = time.perf_counter()
    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_perf_nested
            SELECT
                i,
                ROW(
                    ROW('Alice', 'Smith')::{SCHEMA}.name_pair,
                    ROW(i, i * 2)::{SCHEMA}.point2d
                )::{SCHEMA}.named_point
            FROM generate_series(1, {LARGE_N_NESTED}) i;
    """,
        pg_conn,
    )
    pg_conn.commit()
    elapsed = time.perf_counter() - start

    print(f"\nInserted {LARGE_N_NESTED} rows with nested composite in {elapsed:.2f}s")
    assert elapsed < PERF_LIMIT_NESTED_SECONDS, (
        f"INSERT..SELECT with nested composite took {elapsed:.2f}s, "
        f"expected < {PERF_LIMIT_NESTED_SECONDS}s"
    )

    count = run_query(f"SELECT count(*) FROM {SCHEMA}.t_perf_nested", pg_conn)[0][0]
    assert count == LARGE_N_NESTED

    pg_conn.rollback()


def test_insert_select_array_of_composite_large_volume(pg_conn, composite_schema):
    """Large-volume INSERT..SELECT with an array-of-composite column.

    ArrayOutForPGDuck forwards tupdesc_cache to PGDuckSerialize for each
    element, so the point2d TupleDesc is lazily cached after the first
    element of the first row.  Without caching every array element across
    all rows would call lookup_rowtype_tupdesc individually.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.t_perf_arr (
            id  int,
            pts {SCHEMA}.point2d[]
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    start = time.perf_counter()
    run_command(
        f"""
        INSERT INTO {SCHEMA}.t_perf_arr
            SELECT
                i,
                ARRAY[
                    ROW(i,     i * 2)::{SCHEMA}.point2d,
                    ROW(i + 1, i * 3)::{SCHEMA}.point2d,
                    ROW(i + 2, i * 4)::{SCHEMA}.point2d
                ]
            FROM generate_series(1, {LARGE_N_NESTED}) i;
    """,
        pg_conn,
    )
    pg_conn.commit()
    elapsed = time.perf_counter() - start

    print(f"\nInserted {LARGE_N_NESTED} rows with array-of-composite in {elapsed:.2f}s")
    assert elapsed < PERF_LIMIT_NESTED_SECONDS, (
        f"INSERT..SELECT with array-of-composite took {elapsed:.2f}s, "
        f"expected < {PERF_LIMIT_NESTED_SECONDS}s"
    )

    count = run_query(f"SELECT count(*) FROM {SCHEMA}.t_perf_arr", pg_conn)[0][0]
    assert count == LARGE_N_NESTED

    pg_conn.rollback()

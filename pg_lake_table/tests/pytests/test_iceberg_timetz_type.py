from utils_pytest import *
import json
import pytest
from datetime import time, timezone


def test_iceberg_timetz_as_utc_time(
    pg_conn, s3, extension, create_helper_functions, with_default_location
):
    """timetz columns are stored as Iceberg 'time' type, normalized to UTC."""

    schema_name = "test_iceberg_timetz"
    table_name = "timetz_table"
    location = f"s3://{TEST_BUCKET}/{schema_name}"

    run_command(f"CREATE SCHEMA {schema_name};", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema_name}.{table_name} (
            id INTEGER,
            t TIMETZ
        ) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # 1. Verify that the Iceberg metadata uses "time" (not "timetz")
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables "
        f"WHERE table_name = '{table_name}' AND table_namespace = '{schema_name}'",
        pg_conn,
    )
    assert len(results) == 1
    metadata_path = results[0][0]

    data = read_s3_operations(s3, metadata_path)
    parsed_data = json.loads(data)
    fields = parsed_data["schemas"][0]["fields"]

    time_field = next(f for f in fields if f["name"] == "t")
    assert (
        time_field["type"] == "time"
    ), f"Expected Iceberg type 'time' for timetz column, got '{time_field['type']}'"

    # 2. Insert rows with different timezone offsets
    run_command(
        f"""
        INSERT INTO {schema_name}.{table_name} VALUES
            (1, '12:30:00+00'),
            (2, '12:30:00+04'),
            (3, '23:30:00-02'),
            (4, '01:30:00+04'),
            (5, '00:00:00+00'),
            (6, '12:00:00.123456+00');
        """,
        pg_conn,
    )

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )

    utc = timezone.utc
    expected = [
        [1, time(12, 30, 0, tzinfo=utc)],  # 12:30:00+00 → 12:30:00+00
        [2, time(8, 30, 0, tzinfo=utc)],  # 12:30:00+04 → 08:30:00+00
        [3, time(1, 30, 0, tzinfo=utc)],  # 23:30:00-02 → 01:30:00+00
        [4, time(21, 30, 0, tzinfo=utc)],  # 01:30:00+04 → 21:30:00+00
        [5, time(0, 0, 0, tzinfo=utc)],  # 00:00:00+00 → 00:00:00+00
        [
            6,
            time(12, 0, 0, 123456, tzinfo=utc),
        ],  # 12:00:00.123456+00 → 12:00:00.123456+00
    ]

    assert results == expected

    # 3. COPY to csv file
    csv_path = f"s3://{TEST_BUCKET}/{schema_name}/timetz.csv"
    run_command(
        f"COPY (SELECT * FROM {schema_name}.{table_name}) TO '{csv_path}' WITH (header);",
        pg_conn,
    )

    run_command(f"TRUNCATE TABLE {schema_name}.{table_name};", pg_conn)

    # 4. INSERT..SELECT from another iceberg table
    run_command(
        f"CREATE TABLE {schema_name}.{table_name}_csv (id INTEGER, t TIMETZ) USING iceberg WITH (load_from = '{csv_path}');",
        pg_conn,
    )

    run_command(
        f"CREATE TABLE {schema_name}.{table_name}_copy (id INTEGER, t TIMETZ) USING iceberg;",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {schema_name}.{table_name}_copy SELECT * FROM {schema_name}.{table_name}_csv",
        pg_conn,
    )
    explain_results = run_query(
        f"EXPLAIN ANALYZE INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{table_name}_copy",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(
        explain_results
    ), "INSERT..SELECT with TIMETZ column should be pushed down"

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )
    assert results == expected

    run_command(f"TRUNCATE TABLE {schema_name}.{table_name};", pg_conn)

    # 5. INSERT..SELECT from a pg_lake table
    run_command(
        f"CREATE FOREIGN TABLE {schema_name}.{table_name}_pg_lake (id INTEGER, t TIMETZ) SERVER pg_lake OPTIONS (format 'csv', header 'true', path '{csv_path}');",
        pg_conn,
    )

    explain_results = run_query(
        f"EXPLAIN ANALYZE INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{table_name}_pg_lake",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(
        explain_results
    ), "INSERT..SELECT with TIMETZ column should be pushed down"

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )
    assert results == expected

    run_command(f"TRUNCATE TABLE {schema_name}.{table_name};", pg_conn)

    # 6. COPY FROM and verify data + pushdown
    run_command(
        f"COPY {schema_name}.{table_name} FROM '{csv_path}' WITH (header);",
        pg_conn,
    )

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )
    assert results == expected

    result = run_query(
        "SELECT pg_lake_last_copy_pushed_down_test() pushed_down",
        pg_conn,
    )
    assert result[0][
        "pushed_down"
    ], "COPY FROM with TIMETZ into Iceberg should be pushed down"

    # 7. timetz[] array round-trip: elements must come back at UTC (+00)
    array_table = f"{table_name}_array"
    run_command(
        f"""
        CREATE TABLE {schema_name}.{array_table} (
            id INTEGER,
            ts TIMETZ[]
        ) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema_name}.{array_table} VALUES
            (1, ARRAY['12:30:00+00'::timetz, '12:30:00+04'::timetz]),
            (2, ARRAY['23:30:00-02'::timetz, '01:30:00+04'::timetz]),
            (3, ARRAY['00:00:00+00'::timetz, '12:00:00.123456+00'::timetz]);
        """,
        pg_conn,
    )

    results = run_query(
        f"SELECT id, ts FROM {schema_name}.{array_table} ORDER BY id",
        pg_conn,
    )

    expected_arrays = [
        [1, [time(12, 30, 0, tzinfo=utc), time(8, 30, 0, tzinfo=utc)]],
        [2, [time(1, 30, 0, tzinfo=utc), time(21, 30, 0, tzinfo=utc)]],
        [3, [time(0, 0, 0, tzinfo=utc), time(12, 0, 0, 123456, tzinfo=utc)]],
    ]

    assert (
        results == expected_arrays
    ), f"timetz[] round-trip failed: {results} != {expected_arrays}"

    # 8. DROP TABLES
    run_command(f"DROP SCHEMA {schema_name} CASCADE", pg_conn)
    pg_conn.commit()


def _get_explain_text(query, pg_conn):
    """Return EXPLAIN (VERBOSE) output for ``query`` as a single string."""
    result = run_query("EXPLAIN (VERBOSE) " + query, pg_conn)
    return "\n".join(line[0] for line in result)


def _assert_timetz_utc_cast_in_explain(query, pg_conn):
    """
    Assert that ``query`` is pushed down AND the Vectorized SQL in EXPLAIN
    contains the UTC-normalizing cast that
    IcebergWrapQueryWithNativeTypeConversion emits for TIMETZ columns.

    The wrapper emits
    ``CAST(CAST((<expr>) AS TIMETZ) AT TIME ZONE 'UTC' AS TIME)`` for
    every TIMETZ leaf reachable from the target tuple descriptor (the
    inner ``::TIMETZ`` is defensive: it keeps the outer ``AT TIME ZONE
    'UTC'`` well-typed even when the source expression is already plain
    TIME, which happens for TIMETZ fields read back from inside an
    Iceberg composite).  The outer substring is enough to detect that
    the wrapper fired -- without it, DuckDB's implicit
    ``CAST(TIMETZ AS TIME)`` silently drops the offset.
    """
    explain = _get_explain_text(query, pg_conn)
    assert "Custom Scan (Query Pushdown)" in explain, (
        "Expected Query Pushdown for: " + query + "\n" + explain
    )
    assert "AT TIME ZONE 'UTC' AS TIME" in explain, (
        "Expected TIMETZ UTC-normalizing cast in EXPLAIN output:\n" + explain
    )


def test_timetz_insert_select_from_heap(pg_conn, s3, with_default_location):
    """
    INSERT INTO iceberg_table SELECT ... FROM heap_table is NOT pushed
    down (heap sources aren't DuckDB-shippable, so the plan is a regular
    Postgres "Insert on dest" + "Seq Scan on source").  It instead runs
    row-by-row through the FDW, which serializes tuples to CSV via
    csv_writer.c -- and that path already UTC-normalizes TIMETZ values
    via TimeTzOutForPGDuck in serialize.c before handing them to DuckDB.
    This test locks in that behavior so a future refactor cannot silently
    reintroduce the TIMETZ corruption on the non-pushdown path.
    The pushdown paths (iceberg->iceberg, pg_lake CSV foreign table,
    COPY FROM) are exercised by test_iceberg_timetz_as_utc_time and the
    nested-type tests below.
    """
    utc = timezone.utc

    run_command(
        """
        CREATE SCHEMA test_timetz_ins_sel;
        CREATE TABLE test_timetz_ins_sel.source (id int, t timetz) USING heap;
        INSERT INTO test_timetz_ins_sel.source VALUES
            (1, '12:30:00+00'),
            (2, '12:30:00+04'),
            (3, '23:30:00-02'),
            (4, '23:59:59.999+05:30'),
            (5, NULL);

        CREATE TABLE test_timetz_ins_sel.dest (id int, t timetz) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_timetz_ins_sel.dest
            SELECT id, t FROM test_timetz_ins_sel.source;
        """,
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(
        "SELECT id, t FROM test_timetz_ins_sel.dest ORDER BY id",
        pg_conn,
    )
    assert results == [
        [1, time(12, 30, 0, tzinfo=utc)],
        [2, time(8, 30, 0, tzinfo=utc)],
        [3, time(1, 30, 0, tzinfo=utc)],
        [4, time(18, 29, 59, 999000, tzinfo=utc)],
        [5, None],
    ]

    # also cover the timetz[] round-trip through the same CSV path
    run_command(
        """
        CREATE TABLE test_timetz_ins_sel.arr_source (id int, ts timetz[]) USING heap;
        INSERT INTO test_timetz_ins_sel.arr_source VALUES
            (1, ARRAY['12:30:00+00'::timetz, '12:30:00+04'::timetz]),
            (2, ARRAY['23:30:00-02'::timetz, '23:59:59.999+05:30'::timetz]),
            (3, NULL);

        CREATE TABLE test_timetz_ins_sel.arr_dest (id int, ts timetz[]) USING iceberg;

        INSERT INTO test_timetz_ins_sel.arr_dest
            SELECT id, ts FROM test_timetz_ins_sel.arr_source;
        """,
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(
        "SELECT id, ts FROM test_timetz_ins_sel.arr_dest ORDER BY id",
        pg_conn,
    )
    assert results == [
        [1, [time(12, 30, 0, tzinfo=utc), time(8, 30, 0, tzinfo=utc)]],
        [2, [time(1, 30, 0, tzinfo=utc), time(18, 29, 59, 999000, tzinfo=utc)]],
        [3, None],
    ]

    run_command("DROP SCHEMA test_timetz_ins_sel CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_select_timetz_in_composite_pushdown(
    pg_conn, s3, with_default_location
):
    """
    INSERT..SELECT on an Iceberg table whose column is a composite type
    containing a scalar timetz and a timetz[] field is pushed down, and
    the Vectorized SQL wraps both fields with
    CAST((..) AT TIME ZONE 'UTC' AS TIME).  Uses an Iceberg source (which
    guarantees pushdown eligibility for composite columns, since the
    source column type is the same shippable composite).  Value round-trip
    is checked as well.  Modelled after test_insert_select_interval_pushdown
    in test_insert_select_pushdown.py.
    """
    utc = timezone.utc

    run_command(
        """
        CREATE SCHEMA test_timetz_comp_pd;
        SET search_path TO test_timetz_comp_pd;

        CREATE TYPE day_slot AS (
            label    text,
            at_time  timetz,
            extras   timetz[]
        );
        CREATE TABLE src (id int, slot day_slot) USING iceberg;
        CREATE TABLE tgt (id int, slot day_slot) USING iceberg;

        INSERT INTO src VALUES
            (1, ROW('morning',
                    '08:00:00+04'::timetz,
                    ARRAY['12:30:00+04'::timetz, '23:30:00-02'::timetz]
                   )::day_slot),
            (2, ROW('evening',
                    '23:59:59.999+05:30'::timetz,
                    ARRAY['00:00:00+00'::timetz]
                   )::day_slot),
            (3, ROW('nulls', NULL, NULL)::day_slot),
            (4, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # EXPLAIN shows the struct_pack wrapper with the UTC cast on the
    # inner timetz field, and list_transform on the timetz[] field.
    _assert_timetz_utc_cast_in_explain(
        "INSERT INTO tgt SELECT * FROM src", pg_conn
    )
    explain = _get_explain_text("INSERT INTO tgt SELECT * FROM src", pg_conn)
    assert "struct_pack" in explain, (
        "Expected struct_pack wrapping for composite timetz field:\n" + explain
    )
    assert "list_transform" in explain, (
        "Expected list_transform wrapping for timetz[] field:\n" + explain
    )

    run_command("INSERT INTO tgt SELECT * FROM src", pg_conn)
    pg_conn.commit()

    result = run_query(
        "SELECT id, (slot).label, (slot).at_time, (slot).extras "
        "FROM tgt ORDER BY id",
        pg_conn,
    )
    assert len(result) == 4
    assert result[0] == [
        1,
        "morning",
        time(4, 0, 0, tzinfo=utc),
        [time(8, 30, 0, tzinfo=utc), time(1, 30, 0, tzinfo=utc)],
    ]
    assert result[1] == [
        2,
        "evening",
        time(18, 29, 59, 999000, tzinfo=utc),
        [time(0, 0, 0, tzinfo=utc)],
    ]
    assert result[2] == [3, "nulls", None, None]
    assert result[3] == [4, None, None, None]

    run_command("DROP SCHEMA test_timetz_comp_pd CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_select_timetz_in_map_pushdown(pg_conn, s3, with_default_location):
    """
    INSERT..SELECT with a map<text, timetz> column is pushed down and the
    Vectorized SQL rewrites the map entries through
    map_from_entries(list_transform(map_entries(..), .. -> struct_pack(
        key := k, value := CAST((v) AT TIME ZONE 'UTC' AS TIME))))
    so that every value is UTC-normalized before hitting Iceberg.
    Modelled after test_insert_select_interval_in_map_pushdown.
    """
    utc = timezone.utc
    map_typename = create_map_type("text", "timetz")

    run_command(
        f"""
        CREATE SCHEMA test_timetz_map_pd;
        SET search_path TO test_timetz_map_pd;

        CREATE TABLE src (id int, m {map_typename}) USING iceberg;
        CREATE TABLE tgt (id int, m {map_typename}) USING iceberg;

        INSERT INTO src VALUES
            (1, ARRAY[ROW('standup',  '09:00:00+04'::timetz),
                      ROW('retro',    '23:30:00-02'::timetz),
                      ROW('midnight', '00:00:00+00'::timetz)]::{map_typename}),
            (2, ARRAY[ROW('half_hr',  '23:59:59.999+05:30'::timetz)]::{map_typename}),
            (3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    explain = _get_explain_text("INSERT INTO tgt SELECT * FROM src", pg_conn)
    assert "Custom Scan (Query Pushdown)" in explain, explain
    assert "map_from_entries" in explain, (
        "Expected map_from_entries wrapping for map<text, timetz>:\n" + explain
    )
    assert "AT TIME ZONE 'UTC' AS TIME" in explain, (
        "Expected UTC-normalizing cast on map values:\n" + explain
    )

    run_command("INSERT INTO tgt SELECT * FROM src", pg_conn)
    pg_conn.commit()

    # Non-UTC values come back UTC-normalized regardless of which key we look up
    result = run_query(
        "SELECT map_type.extract(m, 'standup') FROM tgt WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == time(5, 0, 0, tzinfo=utc)

    result = run_query(
        "SELECT map_type.extract(m, 'retro') FROM tgt WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == time(1, 30, 0, tzinfo=utc)

    result = run_query(
        "SELECT map_type.extract(m, 'midnight') FROM tgt WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == time(0, 0, 0, tzinfo=utc)

    result = run_query(
        "SELECT map_type.extract(m, 'half_hr') FROM tgt WHERE id = 2",
        pg_conn,
    )
    assert result[0][0] == time(18, 29, 59, 999000, tzinfo=utc)

    result = run_query("SELECT m FROM tgt WHERE id = 3", pg_conn)
    assert result[0][0] is None

    run_command("DROP SCHEMA test_timetz_map_pd CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_select_timetz_deeply_nested_pushdown(
    pg_conn, s3, with_default_location
):
    """
    Stress-test the recursive type traversal in
    AppendNativeConversionExpression by stacking every supported
    container around timetz:

        outer composite { title text,
                          start_at timetz,            -- level 1
                          repeats  timetz[],          -- array of timetz
                          events   inner_event[] }    -- array of composite
        inner composite { label    text,
                          at_time  timetz,            -- timetz at level 3
                          extras   timetz[] }         -- array at level 4

    plus a sibling map<text, timetz> column on the table.  Every TIMETZ
    reachable from a top-level column must be rewritten to
    CAST((..) AT TIME ZONE 'UTC' AS TIME); the EXPLAIN output therefore
    must contain several copies of that cast wrapped inside struct_pack,
    list_transform and map_from_entries calls.
    """
    utc = timezone.utc
    map_typename = create_map_type("text", "timetz")

    run_command(
        f"""
        CREATE SCHEMA test_timetz_nested_pd;
        SET search_path TO test_timetz_nested_pd;

        CREATE TYPE inner_event AS (
            label   text,
            at_time timetz,
            extras  timetz[]
        );
        CREATE TYPE outer_session AS (
            title    text,
            start_at timetz,
            repeats  timetz[],
            events   inner_event[]
        );

        CREATE TABLE src (
            id  int,
            s   outer_session,
            by_name {map_typename}
        ) USING iceberg;

        CREATE TABLE tgt (
            id  int,
            s   outer_session,
            by_name {map_typename}
        ) USING iceberg;

        INSERT INTO src VALUES
            (1,
             ROW(
                 'Conference',
                 '09:00:00+04'::timetz,
                 ARRAY['13:00:00+04'::timetz, '23:30:00-02'::timetz],
                 ARRAY[
                     ROW('keynote',
                         '09:30:00+04'::timetz,
                         ARRAY['09:45:00+04'::timetz,
                               '23:59:59.999+05:30'::timetz])::inner_event,
                     ROW('panel',
                         '00:00:00+00'::timetz,
                         ARRAY['00:10:00+00'::timetz])::inner_event
                 ]
             )::outer_session,
             ARRAY[ROW('open',  '08:00:00+04'::timetz),
                   ROW('close', '23:00:00-02'::timetz)]::{map_typename}),
            (2,
             ROW('Nulls inside',
                 NULL,
                 NULL,
                 ARRAY[
                     ROW('only_label', NULL, NULL)::inner_event
                 ])::outer_session,
             NULL),
            (3, NULL, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    explain = _get_explain_text("INSERT INTO tgt SELECT * FROM src", pg_conn)
    assert "Custom Scan (Query Pushdown)" in explain, explain

    # Evidence that the recursive wrapping traversed every container level:
    #   struct_pack  -- outer + inner composite rewrite
    #   list_transform  -- timetz[] and inner_event[]
    #   map_from_entries  -- the map<text, timetz> sibling column
    assert "struct_pack" in explain, explain
    assert "list_transform" in explain, explain
    assert "map_from_entries" in explain, explain
    # And every TIMETZ-bearing leaf must have produced the UTC-normalizing
    # cast.  We have at least five such leaves in the target tuple desc:
    #   s.start_at, s.repeats[], s.events[].at_time, s.events[].extras[],
    #   by_name.<value>
    cast_count = explain.count("AT TIME ZONE 'UTC' AS TIME")
    assert cast_count >= 5, (
        f"Expected at least 5 UTC-normalizing casts in the Vectorized "
        f"SQL (one per reachable TIMETZ leaf), but found {cast_count}:\n"
        + explain
    )

    run_command("INSERT INTO tgt SELECT * FROM src", pg_conn)
    pg_conn.commit()

    # Row 1: every TIMETZ in the deeply nested structure must have been
    # rewritten to UTC on the way in.
    result = run_query(
        """
        SELECT id,
               (s).title,
               (s).start_at,
               (s).repeats,
               ((s).events)[1].label,
               ((s).events)[1].at_time,
               ((s).events)[1].extras,
               ((s).events)[2].label,
               ((s).events)[2].at_time,
               ((s).events)[2].extras
        FROM tgt WHERE id = 1
        """,
        pg_conn,
    )
    row = result[0]
    assert row[0] == 1
    assert row[1] == "Conference"
    assert row[2] == time(5, 0, 0, tzinfo=utc)
    assert row[3] == [time(9, 0, 0, tzinfo=utc), time(1, 30, 0, tzinfo=utc)]
    assert row[4] == "keynote"
    assert row[5] == time(5, 30, 0, tzinfo=utc)
    assert row[6] == [
        time(5, 45, 0, tzinfo=utc),
        time(18, 29, 59, 999000, tzinfo=utc),
    ]
    assert row[7] == "panel"
    assert row[8] == time(0, 0, 0, tzinfo=utc)
    assert row[9] == [time(0, 10, 0, tzinfo=utc)]

    # Map values on row 1 are UTC-normalized too
    result = run_query(
        "SELECT map_type.extract(by_name, 'open'),"
        "       map_type.extract(by_name, 'close') "
        "FROM tgt WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == time(4, 0, 0, tzinfo=utc)
    assert result[0][1] == time(1, 0, 0, tzinfo=utc)

    # Row 2: NULL leaves inside the composite survive the rewrite. We
    # deliberately avoid asserting exact semantics for composite elements
    # with all-NULL fields (PG and DuckDB disagree on when such rows
    # compare IS NULL), but we do verify that every directly-accessible
    # TIMETZ leaf we inserted as NULL is still NULL on read-back, and
    # the map column round-trips as NULL.
    result = run_query(
        """
        SELECT id,
               (s).title,
               (s).start_at,
               (s).repeats,
               ((s).events)[1].label,
               ((s).events)[1].at_time,
               ((s).events)[1].extras,
               by_name
        FROM tgt WHERE id = 2
        """,
        pg_conn,
    )
    row = result[0]
    assert row == [2, "Nulls inside", None, None, "only_label", None, None, None]

    # Row 3: both the outer composite and the map are NULL end-to-end.
    result = run_query(
        "SELECT id, s, by_name FROM tgt WHERE id = 3",
        pg_conn,
    )
    assert result[0] == [3, None, None]

    run_command("DROP SCHEMA test_timetz_nested_pd CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_select_timetz_quoted_identifiers_pushdown(
    pg_conn, s3, with_default_location
):
    """
    AppendTimeTzUtcCast wraps an arbitrary DuckDB *expression* (not just
    a bare column name) in ``CAST((<expr>) AT TIME ZONE 'UTC' AS TIME)``.
    Verify that every expression shape we can feed into it is safe when
    the underlying identifiers need quoting:

      * top-level column whose name is an SQL reserved keyword
        (``"order"``),
      * top-level column whose name needs quoting for whitespace /
        mixed case (``"Mixed CS"``),
      * composite field whose name is an SQL reserved keyword
        (``"time"``),
      * composite field whose name needs quoting for whitespace /
        mixed case (``"At Time"``),
      * mixed-case composite field carrying a timetz[] (``"UTC"``), so
        the array lambda path also sees a quoted field.

    If ``quote_identifier`` were dropped anywhere along the composite
    recursion, the wrapped query would become malformed DuckDB SQL and
    either fail to parse or bind to the wrong column -- so both the
    EXPLAIN-level check (quoted names survive in the Vectorized SQL)
    and the value round-trip protect against a regression here.
    """
    utc = timezone.utc

    run_command(
        """
        CREATE SCHEMA test_timetz_quoted_pd;
        SET search_path TO test_timetz_quoted_pd;

        CREATE TYPE ev AS (
            "time"    timetz,
            "At Time" timetz,
            "UTC"     timetz[]
        );

        CREATE TABLE src (
            id         int,
            "order"    timetz,
            "Mixed CS" timetz,
            e          ev
        ) USING iceberg;

        CREATE TABLE tgt (
            id         int,
            "order"    timetz,
            "Mixed CS" timetz,
            e          ev
        ) USING iceberg;

        INSERT INTO src VALUES
            (1,
             '08:00:00+04'::timetz,
             '23:30:00-02'::timetz,
             ROW(
                 '09:00:00+04'::timetz,
                 '23:59:59.999+05:30'::timetz,
                 ARRAY['12:30:00+04'::timetz,
                       '00:00:00+00'::timetz]
             )::ev);
        """,
        pg_conn,
    )
    pg_conn.commit()

    _assert_timetz_utc_cast_in_explain(
        "INSERT INTO tgt SELECT * FROM src", pg_conn
    )

    # Every quoted identifier must round-trip into the Vectorized SQL
    # exactly (quote_identifier is applied at each level of the wrap).
    explain = _get_explain_text("INSERT INTO tgt SELECT * FROM src", pg_conn)
    for quoted in ['"order"', '"Mixed CS"', '"time"', '"At Time"', '"UTC"']:
        assert quoted in explain, (
            f"Expected quoted identifier {quoted} to survive the wrap:\n"
            + explain
        )

    # And the composite-field recursion must have emitted a struct_pack
    # (otherwise the TIMETZ leaves inside ``e`` would have been left as
    # raw TIMETZ for DuckDB to implicitly cast -- the exact bug).
    assert "struct_pack" in explain, explain
    assert "list_transform" in explain, explain

    run_command("INSERT INTO tgt SELECT * FROM src", pg_conn)
    pg_conn.commit()

    result = run_query(
        """
        SELECT id,
               "order",
               "Mixed CS",
               (e)."time",
               (e)."At Time",
               (e)."UTC"
        FROM tgt
        WHERE id = 1
        """,
        pg_conn,
    )
    assert result[0] == [
        1,
        time(4, 0, 0, tzinfo=utc),        # 08:00:00+04   -> 04:00:00Z
        time(1, 30, 0, tzinfo=utc),       # 23:30:00-02   -> 01:30:00Z
        time(5, 0, 0, tzinfo=utc),        # 09:00:00+04   -> 05:00:00Z
        time(18, 29, 59, 999000, tzinfo=utc),  # 23:59:59.999+05:30 -> 18:29:59.999Z
        [time(8, 30, 0, tzinfo=utc),      # 12:30:00+04   -> 08:30:00Z
         time(0, 0, 0, tzinfo=utc)],      # 00:00:00+00   -> 00:00:00Z
    ]

    run_command("DROP SCHEMA test_timetz_quoted_pd CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, app_user):
    run_command(
        f"GRANT SELECT ON lake_iceberg.tables TO {app_user};",
        superuser_conn,
    )
    run_command(
        """
        CREATE OR REPLACE FUNCTION pg_lake_last_copy_pushed_down_test()
          RETURNS bool
          LANGUAGE C
        AS 'pg_lake_copy', $function$pg_lake_last_copy_pushed_down_test$function$;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        DROP FUNCTION IF EXISTS pg_lake_last_copy_pushed_down_test;
        REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

import pytest
import psycopg2
import datetime
from utils_pytest import *


def test_interval(pg_conn, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_interval;
        CREATE TABLE test_interval.test (i interval) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval.test VALUES ('1 day'), ('2 hours'), ('1 year 3 months'), (NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT i FROM test_interval.test ORDER BY i",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(hours=2)
    assert result[1][0] == datetime.timedelta(days=1)
    assert result[2][0] is not None  # 1 year 3 months
    assert result[3][0] is None

    # also test interval[] via ALTER TABLE ADD COLUMN
    run_command(
        """
        ALTER TABLE test_interval.test ADD COLUMN j interval[];
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval.test VALUES ('5 days', ARRAY['1 hour'::interval, '30 minutes'::interval]);
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT j FROM test_interval.test WHERE j IS NOT NULL",
        pg_conn,
    )
    assert len(result) == 1
    assert len(result[0][0]) == 2

    run_command("DROP SCHEMA test_interval CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_edge_cases(pg_conn, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_interval_edge;
        CREATE TABLE test_interval_edge.test (id int, i interval) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval_edge.test VALUES
            (1, '-1 day'),
            (2, '-2 hours -30 minutes'),
            (3, '-1 year -6 months'),
            (4, '1 month -5 days'),
            (5, '-3 days 12 hours'),
            (6, '2 years 3 months 10 days 4 hours 5 minutes 6.789012 seconds'),
            (7, '0.000001 seconds'),
            (8, '999 years'),
            (9, '999999999 days'),
            (10, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, i FROM test_interval_edge.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 10

    # negative day-only
    assert result[0][1] == datetime.timedelta(days=-1)

    # negative time-only
    assert result[1][1] == datetime.timedelta(hours=-2, minutes=-30)

    # negative months - psycopg2 cannot represent as timedelta, just check round-trip
    assert result[2][1] is not None

    # mixed sign: 1 month -5 days
    assert result[3][1] is not None

    # mixed sign: -3 days 12 hours
    assert result[4][1] == datetime.timedelta(days=-3, hours=12)

    # all components
    assert result[5][1] is not None

    # microsecond precision
    assert result[6][1] == datetime.timedelta(microseconds=1)

    # large year value
    assert result[7][1] is not None

    # large day value
    assert result[8][1] == datetime.timedelta(days=999999999)

    # NULL
    assert result[9][1] is None

    # verify round-trip by re-reading with explicit casts
    result = run_query(
        """
        SELECT
            extract(epoch from i)
        FROM test_interval_edge.test
        WHERE id = 6
        """,
        pg_conn,
    )
    # 2y3m = 27 months, 10d, 4h5m6.789012s => epoch depends on month interpretation
    assert result[0][0] is not None

    run_command("DROP SCHEMA test_interval_edge CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_array_edge_cases(pg_conn, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_interval_arr_edge;
        CREATE TABLE test_interval_arr_edge.test (id int, intervals interval[]) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval_arr_edge.test VALUES
            (1, ARRAY['1 day'::interval, '-2 hours'::interval, '1 year 6 months'::interval]),
            (2, ARRAY[NULL::interval, '30 minutes'::interval]),
            (3, ARRAY['0 seconds'::interval]),
            (4, NULL),
            (5, ARRAY[]::interval[]),
            (6, ARRAY['-1 year -6 months'::interval, '2 days 3 hours 4.567 seconds'::interval]);
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, intervals FROM test_interval_arr_edge.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 6

    # mixed positive/negative intervals in array
    assert len(result[0][1]) == 3
    assert result[0][1][0] == datetime.timedelta(days=1)
    assert result[0][1][1] == datetime.timedelta(hours=-2)

    # NULL element in array
    assert len(result[1][1]) == 2
    assert result[1][1][0] is None
    assert result[1][1][1] == datetime.timedelta(minutes=30)

    # single zero element
    assert result[2][1] == [datetime.timedelta(0)]

    # NULL array
    assert result[3][1] is None

    # empty array
    assert result[4][1] == []

    # negative month-based and mixed components
    assert len(result[5][1]) == 2

    run_command("DROP SCHEMA test_interval_arr_edge CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_in_composite(pg_conn, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_interval_comp;
        CREATE TYPE test_interval_comp.event_duration AS (
            name text,
            duration interval
        );
        CREATE TABLE test_interval_comp.test (
            id int,
            event test_interval_comp.event_duration
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval_comp.test VALUES
            (1, ROW('meeting', '1 hour 30 minutes')::test_interval_comp.event_duration),
            (2, ROW('sprint', '14 days')::test_interval_comp.event_duration),
            (3, ROW('break', '-15 minutes')::test_interval_comp.event_duration),
            (4, ROW(NULL, '1 day')::test_interval_comp.event_duration),
            (5, ROW('vacation', NULL)::test_interval_comp.event_duration),
            (6, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, (event).name, (event).duration FROM test_interval_comp.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 6
    assert result[0][1] == "meeting"
    assert result[0][2] == datetime.timedelta(hours=1, minutes=30)
    assert result[1][1] == "sprint"
    assert result[1][2] == datetime.timedelta(days=14)
    assert result[2][1] == "break"
    assert result[2][2] == datetime.timedelta(minutes=-15)
    assert result[3][1] is None
    assert result[3][2] == datetime.timedelta(days=1)
    assert result[4][1] == "vacation"
    assert result[4][2] is None
    assert result[5][1] is None
    assert result[5][2] is None

    run_command("DROP SCHEMA test_interval_comp CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_update_delete(pg_conn, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_interval_ud;
        CREATE TABLE test_interval_ud.test (id int, i interval) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval_ud.test VALUES
            (1, '1 day'),
            (2, '2 hours'),
            (3, '3 months'),
            (4, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # update an interval value
    run_command(
        "UPDATE test_interval_ud.test SET i = '-5 days' WHERE id = 1",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT i FROM test_interval_ud.test WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(days=-5)

    # update to NULL
    run_command(
        "UPDATE test_interval_ud.test SET i = NULL WHERE id = 2",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT i FROM test_interval_ud.test WHERE id = 2",
        pg_conn,
    )
    assert result[0][0] is None

    # update from NULL to a value
    run_command(
        "UPDATE test_interval_ud.test SET i = '1 year 6 months' WHERE id = 4",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT i FROM test_interval_ud.test WHERE id = 4",
        pg_conn,
    )
    assert result[0][0] is not None  # 1 year 6 months (not representable as timedelta)

    # delete
    run_command(
        "DELETE FROM test_interval_ud.test WHERE id = 3",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, i FROM test_interval_ud.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 3
    assert result[0][0] == 1 and result[0][1] == datetime.timedelta(days=-5)
    assert result[1][0] == 2 and result[1][1] is None
    assert result[2][0] == 4 and result[2][1] is not None

    run_command("DROP SCHEMA test_interval_ud CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_vacuum(pg_conn, s3, with_default_location):
    """
    Verify that interval data survives VACUUM (compaction) and VACUUM FULL
    (merge-on-read rewrite). This exercises the interval-as-struct round-trip
    through the compaction code path.
    """
    run_command(
        """
        CREATE SCHEMA test_interval_vac;
        CREATE TABLE test_interval_vac.test (id int, i interval) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # multiple inserts to create multiple data files
    run_command(
        "INSERT INTO test_interval_vac.test VALUES (1, '1 day')",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        "INSERT INTO test_interval_vac.test VALUES (2, '3 months')",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        "INSERT INTO test_interval_vac.test VALUES (3, '-2 hours 30 minutes')",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        "INSERT INTO test_interval_vac.test VALUES (4, NULL)",
        pg_conn,
    )
    pg_conn.commit()

    # VACUUM compacts the multiple data files
    pg_conn.autocommit = True
    run_command("SET pg_lake_table.vacuum_compact_min_input_files TO 1", pg_conn)
    run_command("VACUUM test_interval_vac.test", pg_conn)
    pg_conn.autocommit = False

    # verify all interval values survived compaction
    result = run_query(
        "SELECT id, i FROM test_interval_vac.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 4
    assert result[0][1] == datetime.timedelta(days=1)
    assert result[1][1] is not None  # 3 months (not representable as timedelta)
    assert result[2][1] == datetime.timedelta(hours=-1, minutes=-30)
    assert result[3][1] is None

    # delete a row to create a position delete file, then VACUUM FULL to rewrite
    run_command(
        """
        SET pg_lake_table.copy_on_write_threshold TO 80;
        DELETE FROM test_interval_vac.test WHERE id = 2;
        RESET pg_lake_table.copy_on_write_threshold;
    """,
        pg_conn,
    )
    pg_conn.commit()

    pg_conn.autocommit = True
    run_command("VACUUM FULL test_interval_vac.test", pg_conn)
    pg_conn.autocommit = False

    # verify remaining rows after VACUUM FULL
    result = run_query(
        "SELECT id, i FROM test_interval_vac.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 3
    assert result[0][0] == 1
    assert result[0][1] == datetime.timedelta(days=1)
    assert result[1][0] == 3
    assert result[1][1] == datetime.timedelta(hours=-1, minutes=-30)
    assert result[2][0] == 4
    assert result[2][1] is None

    run_command("DROP SCHEMA test_interval_vac CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_infinity(pg_conn, s3, with_default_location):
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("infinity intervals require PostgreSQL 17+")

    run_command(
        """
        CREATE SCHEMA test_interval_inf;
        CREATE TABLE test_interval_inf.test (id int, i interval) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # infinity intervals are rejected (same as infinity dates/timestamps)
    with pytest.raises(psycopg2.errors.DatetimeFieldOverflow):
        run_command(
            "INSERT INTO test_interval_inf.test VALUES (1, 'infinity')",
            pg_conn,
        )
    pg_conn.rollback()

    with pytest.raises(psycopg2.errors.DatetimeFieldOverflow):
        run_command(
            "INSERT INTO test_interval_inf.test VALUES (2, '-infinity')",
            pg_conn,
        )
    pg_conn.rollback()

    # finite intervals still work
    run_command(
        "INSERT INTO test_interval_inf.test VALUES (3, '1 day')",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT i FROM test_interval_inf.test WHERE id = 3",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(days=1)

    run_command("DROP SCHEMA test_interval_inf CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_infinity_predicate(pg_conn, s3, with_default_location):
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("infinity intervals require PostgreSQL 17+")

    run_command(
        """
        CREATE SCHEMA test_interval_inf_pred;
        CREATE TABLE test_interval_inf_pred.test (id int, i interval) USING iceberg;
        INSERT INTO test_interval_inf_pred.test VALUES
            (1, '1 day'),
            (2, '2 hours'),
            (3, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Queries using infinity interval constants must not be pushed down to
    # DuckDB since DuckDB does not support infinity intervals. Without the
    # fix, DuckDB raises "Could not convert string 'infinity' to INTERVAL".
    result = run_query(
        "SELECT id FROM test_interval_inf_pred.test WHERE i < INTERVAL 'infinity' ORDER BY id",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[1][0] == 2

    result = run_query(
        "SELECT id FROM test_interval_inf_pred.test WHERE i = INTERVAL 'infinity' ORDER BY id",
        pg_conn,
    )
    assert len(result) == 0

    result = run_query(
        "SELECT id FROM test_interval_inf_pred.test WHERE i > INTERVAL '-infinity' ORDER BY id",
        pg_conn,
    )
    assert len(result) == 2

    run_command("DROP SCHEMA test_interval_inf_pred CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_ctas(pg_conn, s3, with_default_location):
    """
    CREATE TABLE ... USING iceberg AS SELECT <interval> exercises the
    INSERT..SELECT pushdown path. Intervals are stored as
    STRUCT(months, days, microseconds) in Iceberg, which requires the
    pushdown to be disabled and fall back to the row-by-row path.
    """
    run_command(
        """
        CREATE SCHEMA test_interval_ctas;
        CREATE TABLE test_interval_ctas.source (id int, i interval) USING heap;
        INSERT INTO test_interval_ctas.source VALUES
            (1, '1 day'),
            (2, '2 hours 30 minutes'),
            (3, '1 year 3 months'),
            (4, '-5 days 12 hours'),
            (5, '0.000001 seconds'),
            (6, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # CTAS from a constant expression (the exact failing case from the bug report)
    run_command(
        """
        CREATE TABLE test_interval_ctas.from_const USING iceberg
        AS SELECT INTERVAL '01:00' AS "One hour";
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        'SELECT "One hour" FROM test_interval_ctas.from_const',
        pg_conn,
    )
    assert len(result) == 1
    assert result[0][0] == datetime.timedelta(hours=1)

    # CTAS from an existing table
    run_command(
        """
        CREATE TABLE test_interval_ctas.from_table USING iceberg
        AS SELECT id, i FROM test_interval_ctas.source;
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, i FROM test_interval_ctas.from_table ORDER BY id",
        pg_conn,
    )
    assert len(result) == 6
    assert result[0][1] == datetime.timedelta(days=1)
    assert result[1][1] == datetime.timedelta(hours=2, minutes=30)
    assert result[2][1] is not None  # 1 year 3 months
    assert result[3][1] == datetime.timedelta(days=-5, hours=12)
    assert result[4][1] == datetime.timedelta(microseconds=1)
    assert result[5][1] is None

    # CTAS with interval array column
    run_command(
        """
        CREATE TABLE test_interval_ctas.from_array USING iceberg
        AS SELECT
            1 AS id,
            ARRAY['1 hour'::interval, '30 minutes'::interval] AS durations;
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT durations FROM test_interval_ctas.from_array",
        pg_conn,
    )
    assert len(result) == 1
    assert len(result[0][0]) == 2
    assert result[0][0][0] == datetime.timedelta(hours=1)
    assert result[0][0][1] == datetime.timedelta(minutes=30)

    run_command("DROP SCHEMA test_interval_ctas CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_insert_select(pg_conn, s3, with_default_location):
    """
    INSERT INTO iceberg_table SELECT ... exercises the same pushdown-disabled
    path for interval columns. Verifies the row-by-row fallback correctly
    serializes intervals as STRUCT(months, days, microseconds).
    """
    run_command(
        """
        CREATE SCHEMA test_interval_ins_sel;
        CREATE TABLE test_interval_ins_sel.source (id int, i interval) USING heap;
        INSERT INTO test_interval_ins_sel.source VALUES
            (1, '3 days'),
            (2, '-1 hour 45 minutes'),
            (3, '2 years'),
            (4, NULL);

        CREATE TABLE test_interval_ins_sel.dest (id int, i interval) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval_ins_sel.dest
        SELECT id, i FROM test_interval_ins_sel.source;
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, i FROM test_interval_ins_sel.dest ORDER BY id",
        pg_conn,
    )
    assert len(result) == 4
    assert result[0][1] == datetime.timedelta(days=3)
    assert result[1][1] == datetime.timedelta(minutes=-15)
    assert result[2][1] is not None  # 2 years
    assert result[3][1] is None

    # also test interval[] in INSERT..SELECT
    run_command(
        """
        CREATE TABLE test_interval_ins_sel.arr_source (id int, intervals interval[]) USING heap;
        INSERT INTO test_interval_ins_sel.arr_source VALUES
            (1, ARRAY['1 day'::interval, '2 hours'::interval]),
            (2, NULL);

        CREATE TABLE test_interval_ins_sel.arr_dest (id int, intervals interval[]) USING iceberg;

        INSERT INTO test_interval_ins_sel.arr_dest
        SELECT id, intervals FROM test_interval_ins_sel.arr_source;
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, intervals FROM test_interval_ins_sel.arr_dest ORDER BY id",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0][1] == [datetime.timedelta(days=1), datetime.timedelta(hours=2)]
    assert result[1][1] is None

    run_command("DROP SCHEMA test_interval_ins_sel CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_map(pg_conn, s3, with_default_location):
    """
    Verify that interval values work as map values in Iceberg tables.
    This exercises the interval-as-struct encoding within the pg_map type.
    """
    map_type_name = create_map_type("text", "interval")

    run_command(
        f"""
        CREATE SCHEMA test_interval_map;
        CREATE TABLE test_interval_map.test (
            id int,
            durations {map_type_name}
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO test_interval_map.test VALUES
            (1, ARRAY[ROW('meeting', '1 hour 30 minutes'::interval),
                       ROW('break', '15 minutes'::interval)]::{map_type_name}),
            (2, ARRAY[ROW('sprint', '14 days'::interval),
                       ROW('negative', '-2 hours'::interval)]::{map_type_name}),
            (3, ARRAY[ROW('month_based', '3 months'::interval)]::{map_type_name}),
            (4, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # verify round-trip via map_type.extract
    result = run_query(
        "SELECT map_type.extract(durations, 'meeting') FROM test_interval_map.test WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(hours=1, minutes=30)

    result = run_query(
        "SELECT map_type.extract(durations, 'break') FROM test_interval_map.test WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(minutes=15)

    result = run_query(
        "SELECT map_type.extract(durations, 'sprint') FROM test_interval_map.test WHERE id = 2",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(days=14)

    result = run_query(
        "SELECT map_type.extract(durations, 'negative') FROM test_interval_map.test WHERE id = 2",
        pg_conn,
    )
    assert result[0][0] == datetime.timedelta(hours=-2)

    # month-based interval (not representable as timedelta)
    result = run_query(
        "SELECT map_type.extract(durations, 'month_based') FROM test_interval_map.test WHERE id = 3",
        pg_conn,
    )
    assert result[0][0] is not None

    # NULL map
    result = run_query(
        "SELECT durations FROM test_interval_map.test WHERE id = 4",
        pg_conn,
    )
    assert result[0][0] is None

    # non-existent key returns NULL
    result = run_query(
        "SELECT map_type.extract(durations, 'nonexistent') FROM test_interval_map.test WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] is None

    run_command("DROP SCHEMA test_interval_map CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_array_in_composite(pg_conn, s3, with_default_location):
    """
    Verify that interval[] fields inside a composite type round-trip
    correctly through the Iceberg struct(months, days, microseconds)
    representation.
    """
    run_command(
        """
        CREATE SCHEMA test_interval_arr_comp;
        CREATE TYPE test_interval_arr_comp.schedule AS (
            name text,
            durations interval[],
            single_dur interval
        );
        CREATE TABLE test_interval_arr_comp.test (
            id int,
            sched test_interval_arr_comp.schedule
        ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_interval_arr_comp.test VALUES
            (1, ROW('daily', ARRAY['8 hours'::interval, '1 hour'::interval, '30 minutes'::interval],
                     '9 hours 30 minutes'::interval)::test_interval_arr_comp.schedule),
            (2, ROW('weekly', ARRAY['5 days'::interval, '-2 hours'::interval],
                     '4 days 22 hours'::interval)::test_interval_arr_comp.schedule),
            (3, ROW('empty', ARRAY[]::interval[], '0 seconds'::interval)::test_interval_arr_comp.schedule),
            (4, ROW('nulls', NULL, NULL)::test_interval_arr_comp.schedule),
            (5, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT id, (sched).name, (sched).durations, (sched).single_dur "
        "FROM test_interval_arr_comp.test ORDER BY id",
        pg_conn,
    )
    assert len(result) == 5

    # row 1: array of 3 intervals + scalar interval
    assert result[0][1] == "daily"
    assert len(result[0][2]) == 3
    assert result[0][2][0] == datetime.timedelta(hours=8)
    assert result[0][2][1] == datetime.timedelta(hours=1)
    assert result[0][2][2] == datetime.timedelta(minutes=30)
    assert result[0][3] == datetime.timedelta(hours=9, minutes=30)

    # row 2: negative interval in array
    assert result[1][1] == "weekly"
    assert result[1][2][0] == datetime.timedelta(days=5)
    assert result[1][2][1] == datetime.timedelta(hours=-2)
    assert result[1][3] == datetime.timedelta(days=4, hours=22)

    # row 3: empty array + zero interval
    assert result[2][2] == []
    assert result[2][3] == datetime.timedelta(0)

    # row 4: NULL array and NULL scalar inside struct
    assert result[3][1] == "nulls"
    assert result[3][2] is None
    assert result[3][3] is None

    # row 5: entire struct is NULL
    assert result[4][1] is None
    assert result[4][2] is None
    assert result[4][3] is None

    run_command("DROP SCHEMA test_interval_arr_comp CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_array_update(pg_conn, s3, with_default_location):
    """
    Verify UPDATE and DELETE on interval[] columns.
    """
    run_command(
        """
        CREATE SCHEMA test_interval_arr_ud;
        CREATE TABLE test_interval_arr_ud.test (id int, durations interval[]) USING iceberg;
        INSERT INTO test_interval_arr_ud.test VALUES
            (1, ARRAY['1 day'::interval, '2 hours'::interval]),
            (2, ARRAY['3 months'::interval]),
            (3, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # update: replace array
    run_command(
        "UPDATE test_interval_arr_ud.test "
        "SET durations = ARRAY['-5 days'::interval, '10 minutes'::interval] WHERE id = 1",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT durations FROM test_interval_arr_ud.test WHERE id = 1",
        pg_conn,
    )
    assert result[0][0] == [datetime.timedelta(days=-5), datetime.timedelta(minutes=10)]

    # update: set to NULL
    run_command(
        "UPDATE test_interval_arr_ud.test SET durations = NULL WHERE id = 2",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT durations FROM test_interval_arr_ud.test WHERE id = 2",
        pg_conn,
    )
    assert result[0][0] is None

    # update: NULL to value
    run_command(
        "UPDATE test_interval_arr_ud.test "
        "SET durations = ARRAY['1 year'::interval] WHERE id = 3",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT durations FROM test_interval_arr_ud.test WHERE id = 3",
        pg_conn,
    )
    assert len(result[0][0]) == 1
    assert result[0][0][0] is not None  # 1 year (not representable as timedelta)

    # delete
    run_command("DELETE FROM test_interval_arr_ud.test WHERE id = 2", pg_conn)
    pg_conn.commit()

    result = run_query(
        "SELECT id FROM test_interval_arr_ud.test ORDER BY id",
        pg_conn,
    )
    assert [r[0] for r in result] == [1, 3]

    run_command("DROP SCHEMA test_interval_arr_ud CASCADE", pg_conn)
    pg_conn.commit()


def test_interval_prepared_statement(pg_conn, s3, with_default_location):
    """
    Verify that prepared statements with interval parameters work against
    Iceberg tables. This exercises the PGDuckSerialize call in
    QueryPushdownBeginScan where parameters are serialized for DuckDB.
    """
    run_command(
        """
        CREATE SCHEMA test_interval_prep;
        CREATE TABLE test_interval_prep.test (id int, i interval) USING iceberg;
        INSERT INTO test_interval_prep.test VALUES
            (1, '1 day'),
            (2, '2 hours'),
            (3, '1 year 3 months'),
            (4, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # prepared statement with interval parameter in WHERE clause
    run_command(
        "PREPARE interval_lookup(interval) AS "
        "SELECT id FROM test_interval_prep.test WHERE i = $1",
        pg_conn,
    )

    # execute enough times to trigger generic plan (uses parameters)
    for _ in range(7):
        result = run_query("EXECUTE interval_lookup('1 day')", pg_conn)

    assert len(result) == 1
    assert result[0][0] == 1

    # test with a different value
    result = run_query("EXECUTE interval_lookup('2 hours')", pg_conn)
    assert len(result) == 1
    assert result[0][0] == 2

    # test with NULL parameter
    result = run_query("EXECUTE interval_lookup(NULL)", pg_conn)
    assert len(result) == 0

    run_command("DEALLOCATE interval_lookup", pg_conn)

    run_command("DROP SCHEMA test_interval_prep CASCADE", pg_conn)
    pg_conn.commit()

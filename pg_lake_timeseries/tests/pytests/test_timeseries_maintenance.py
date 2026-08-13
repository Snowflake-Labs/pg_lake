"""Tests for the maintenance of a tiered table (DESIGN.md section 9).

Maintenance is what moves data between the tiers: it extends the partition
frontier ahead of the writes, refreshes the Iceberg copy of partitions that are
over, seals the ones that aged out of the hot window -- the only thing that moves
the authority boundary -- and expires cold history.

Every property here is about the boundary staying honest: it never advances past
data that is not in Iceberg, it never moves backwards, and the relation returns
the same rows before and after any of it.
"""

import time

import pytest

from utils_pytest import *


# maintenance is driven explicitly, so a table is staged with a known shape:
# hour-long partitions, two hours of hot window, rows 20 minutes apart
PARTITION_INTERVAL = "1 hour"
HOT_RETENTION = "2 hours"

# hours of history stage() builds, the partitions of it that are over, the rows
# in each of those, and the one row in the hour that is still running
HOURS_BACK = 4
PAST_PARTITIONS = 4
ROWS_PER_PARTITION = 3
TOTAL_ROWS = PAST_PARTITIONS * ROWS_PER_PARTITION + 1

# with a two-hour hot window, this many of the staged partitions are sealable
AGED_OUT = 2


def create(tiered, name="metrics", **options):
    """A tiered table with the shape the assertions below assume."""
    options.setdefault("partition_interval", PARTITION_INTERVAL)
    options.setdefault("hot_retention", HOT_RETENTION)

    return tiered(name, **options)


def stage(tiered, pg_conn, name="metrics", hours_back=HOURS_BACK, **options):
    """A tiered table with hour-long partitions over the last hours_back hours.

    CREATE TABLE covers the hot window and the future, so the history below it is
    added here: a table with more history than its hot window is what sealing has
    work to do on.
    """
    create(tiered, name, **options)

    run_command(
        f"""DO $$
        DECLARE
            frontier    timestamptz;
            part_start  timestamptz;
        BEGIN
            SELECT min(h.part_start) INTO frontier
              FROM timeseries.heap_ranges('{name}'::regclass) h;

            part_start := date_trunc('hour', now())
                          - interval '{hours_back} hours';

            WHILE part_start < frontier LOOP
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF {name}'
                    ' FOR VALUES FROM (%L) TO (%L)',
                    '{name}_h' || to_char(part_start, 'DDHH24MI'),
                    part_start, part_start + interval '{PARTITION_INTERVAL}');

                part_start := part_start + interval '{PARTITION_INTERVAL}';
            END LOOP;
        END $$""",
        pg_conn,
    )
    pg_conn.commit()

    fill(pg_conn, name, hours_back=hours_back)


def fill(pg_conn, name="metrics", hours_back=2, hours_ahead=0):
    """Rows every 20 minutes over a window around the top of the current hour."""
    run_command(
        f"INSERT INTO {name}"
        " SELECT ts, 1, extract(epoch FROM ts)::float8"
        f" FROM generate_series(date_trunc('hour', now()) - interval '{hours_back} hours'"
        "                       + interval '10 minutes',"
        f"                      date_trunc('hour', now()) + interval '{hours_ahead} hours'"
        "                       + interval '10 minutes',"
        "                      interval '20 minutes') ts",
        pg_conn,
    )
    pg_conn.commit()


def count_in(conn, relation):
    return run_query(f"SELECT count(*) FROM {relation}", conn)[0][0]


def boundary(pg_conn, name="metrics"):
    return run_query(
        f"SELECT boundary FROM timeseries.tiered_table('{name}'::regclass)", pg_conn
    )[0][0]


def at(pg_conn, expression):
    """A timestamp computed by the server, to compare against one it stored."""
    return run_query(f"SELECT {expression}", pg_conn)[0][0]


def hot_window_start(pg_conn):
    return at(
        pg_conn,
        f"timeseries.partition_start(now() - interval '{HOT_RETENTION}',"
        f" interval '{PARTITION_INTERVAL}')",
    )


def heap_ranges(pg_conn, name="metrics"):
    return run_query(
        "SELECT partition, part_start, part_end FROM"
        f" timeseries.heap_ranges('{name}'::regclass) ORDER BY part_start",
        pg_conn,
    )


def synced_ranges(pg_conn, name="metrics"):
    return run_query(
        "SELECT part_start, part_end, synced_at, sealed_at FROM"
        f" timeseries.synced_ranges('{name}'::regclass) ORDER BY part_start",
        pg_conn,
    )


def test_add_partitions_extends_the_frontier(tiered, pg_conn):
    create(tiered, precreate_ahead=3)

    initial = heap_ranges(pg_conn)

    # CREATE TABLE already covered the hot window and the precreated future
    assert initial[0]["part_start"] == hot_window_start(pg_conn)
    assert initial[-1]["part_end"] > at(pg_conn, "now() + interval '3 hours'")

    # the ranges are contiguous, which is what lets sealing walk them
    for lower, upper in zip(initial, initial[1:]):
        assert lower["part_end"] == upper["part_start"]

    # so there is nothing to extend until the future asked for grows
    assert run_query("SELECT timeseries.add_partitions('metrics')", pg_conn)[0][0] == 0
    pg_conn.commit()

    run_command("ALTER TABLE metrics SET (precreate_ahead = 6)", pg_conn)
    pg_conn.commit()

    created = run_query("SELECT timeseries.add_partitions('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    ranges = heap_ranges(pg_conn)

    assert created == 3
    assert len(ranges) == len(initial) + created
    assert ranges[-1]["part_end"] > at(pg_conn, "now() + interval '6 hours'")

    for lower, upper in zip(ranges, ranges[1:]):
        assert lower["part_end"] == upper["part_start"]

    # a write beyond the frontier is an error rather than a silent misplacement
    error = run_command(
        "INSERT INTO metrics VALUES (now() + interval '60 days', 1, 1)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert "no partition of relation" in error


def test_add_partitions_starts_at_the_boundary(tiered, pg_conn):
    """Iceberg owns everything below the boundary; the heap must not go there.

    A widened hot window is the case where the two disagree: it asks for
    partitions over a range that was already handed over, and the boundary wins.
    """
    create(tiered, precreate_ahead=0)

    # seal everything that exists, then leave the heap with no partitions at all
    run_command("SELECT timeseries.seal('metrics', upto => now())", pg_conn)
    pg_conn.commit()

    for row in heap_ranges(pg_conn):
        run_command(f"DROP TABLE {row['partition']}", pg_conn)
    pg_conn.commit()

    assert heap_ranges(pg_conn) == []
    assert boundary(pg_conn) == at(pg_conn, "date_trunc('hour', now())")

    # the hot window now reaches well below the boundary
    run_command("ALTER TABLE metrics SET (hot_retention = '10 hours')", pg_conn)
    pg_conn.commit()

    run_command("SELECT timeseries.add_partitions('metrics')", pg_conn)
    pg_conn.commit()

    ranges = heap_ranges(pg_conn)
    widened = at(
        pg_conn,
        "timeseries.partition_start(now() - interval '10 hours', interval '1 hour')",
    )

    # the first partition begins at the boundary, not at the hot window
    assert ranges[0]["part_start"] == boundary(pg_conn)
    assert boundary(pg_conn) > widened


def test_sync_copies_a_completed_partition_once(tiered, pg_conn):
    stage(tiered, pg_conn)

    total = count_in(pg_conn, "metrics")
    before = boundary(pg_conn)

    synced = run_query("SELECT timeseries.sync('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    # the partitions that are over are copied; the current one is not
    assert total == TOTAL_ROWS
    assert synced == PAST_PARTITIONS
    assert count_in(pg_conn, "metrics_cold") == PAST_PARTITIONS * ROWS_PER_PARTITION

    # copying does not hand anything over: PostgreSQL is still authoritative
    assert boundary(pg_conn) == before
    assert count_in(pg_conn, "metrics") == total

    # and a second pass copies nothing, because nothing changed
    assert run_query("SELECT timeseries.sync('metrics')", pg_conn)[0][0] == 0
    pg_conn.commit()

    ranges = synced_ranges(pg_conn)

    assert len(ranges) == PAST_PARTITIONS
    assert all(row["sealed_at"] is None for row in ranges)


def test_sync_overwrites_the_range_it_copies(tiered, pg_conn):
    """A forced re-copy replaces the range rather than appending to it."""
    stage(tiered, pg_conn)

    run_command("SELECT timeseries.sync('metrics')", pg_conn)
    pg_conn.commit()

    copied = count_in(pg_conn, "metrics_cold")
    first = heap_ranges(pg_conn)[0]

    # change a row of an already-copied partition, then force the re-copy
    run_command(
        f"UPDATE metrics SET device = 7 WHERE ts >= '{first['part_start']}'"
        f" AND ts < '{first['part_end']}'",
        pg_conn,
    )
    resynced = run_query(
        f"SELECT timeseries.sync('metrics', only_start => '{first['part_start']}')",
        pg_conn,
    )[0][0]
    pg_conn.commit()

    assert resynced == 1
    assert count_in(pg_conn, "metrics_cold") == copied
    assert count_in(pg_conn, "metrics_cold WHERE device = 7") == ROWS_PER_PARTITION


def test_seal_advances_the_boundary_and_drops_the_partition(tiered, pg_conn):
    stage(tiered, pg_conn)

    total = count_in(pg_conn, "metrics")
    partitions = heap_ranges(pg_conn)

    # the default seals what aged out of the hot window
    sealed = run_query("SELECT timeseries.seal('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    assert sealed == AGED_OUT

    remaining = heap_ranges(pg_conn)

    # the sealed partitions are gone from the heap and their range is Iceberg's
    assert len(remaining) == len(partitions) - sealed
    assert remaining[0]["part_start"] == partitions[sealed]["part_start"]
    assert boundary(pg_conn) == partitions[sealed - 1]["part_end"]
    assert boundary(pg_conn) == hot_window_start(pg_conn)

    # no row was lost or duplicated by the move
    assert count_in(pg_conn, "metrics") == total
    assert count_in(pg_conn, "metrics_cold") == sealed * ROWS_PER_PARTITION
    assert (
        count_in(pg_conn, "metrics_cold")
        + count_in(pg_conn, "metrics WHERE ts >= '%s'" % boundary(pg_conn))
        == total
    )

    # the sealed ranges are recorded as owned by Iceberg
    ranges = synced_ranges(pg_conn)

    assert len(ranges) == sealed
    assert all(row["sealed_at"] is not None for row in ranges)


def test_seal_stops_at_a_gap(tiered, pg_conn):
    """Sealing walks upward from the boundary; a hole is refused, not skipped."""
    stage(tiered, pg_conn)

    run_command("SELECT timeseries.seal('metrics')", pg_conn)
    pg_conn.commit()

    after_first = boundary(pg_conn)

    # a hole right above the boundary: a partition dropped without being sealed
    run_command(f"DROP TABLE {heap_ranges(pg_conn)[0]['partition']}", pg_conn)
    pg_conn.commit()

    sealed = run_query("SELECT timeseries.seal('metrics', upto => now())", pg_conn)[0][
        0
    ]
    pg_conn.commit()

    # nothing was sealed, and the boundary did not move over the hole
    assert sealed == 0
    assert boundary(pg_conn) == after_first

    # the range above the hole is still PostgreSQL's, and still readable
    assert all(row["part_start"] >= after_first for row in heap_ranges(pg_conn))
    assert count_in(pg_conn, "metrics") > 0


def test_apply_retention_expires_cold_data(tiered, pg_conn):
    stage(tiered, pg_conn, cold_retention="3 hours")

    # hand everything that is over to Iceberg, so retention has something to expire
    sealed = run_query("SELECT timeseries.seal('metrics', upto => now())", pg_conn)[0][
        0
    ]
    pg_conn.commit()

    assert sealed == PAST_PARTITIONS

    sealed_rows = count_in(pg_conn, "metrics_cold")
    cutoff = at(
        pg_conn,
        "timeseries.partition_start(now() - interval '3 hours', interval '1 hour')",
    )

    assert count_in(pg_conn, f"metrics_cold WHERE ts < '{cutoff}'") > 0

    forgotten = run_query("SELECT timeseries.apply_retention('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    # everything below the cutoff is gone from Iceberg and from the metadata
    assert forgotten > 0
    assert count_in(pg_conn, f"metrics_cold WHERE ts < '{cutoff}'") == 0
    assert count_in(pg_conn, "metrics_cold") < sealed_rows
    assert all(row["part_start"] >= cutoff for row in synced_ranges(pg_conn))

    # the boundary is untouched: expiry removes history, it does not hand it back
    assert boundary(pg_conn) >= cutoff
    assert count_in(pg_conn, "metrics") == count_in(pg_conn, "metrics_cold") + count_in(
        pg_conn, "metrics WHERE ts >= '%s'" % boundary(pg_conn)
    )


def test_retention_does_not_expire_data_postgresql_still_owns(tiered, pg_conn):
    """cold_retention is bounded by the boundary, so it cannot eat hot data."""
    stage(tiered, pg_conn, cold_retention="1 minute")

    # the cold tier holds copies of ranges that are still PostgreSQL's
    run_command("SELECT timeseries.sync('metrics')", pg_conn)
    pg_conn.commit()

    total = count_in(pg_conn, "metrics")
    copied = count_in(pg_conn, "metrics_cold")

    assert copied > 0

    forgotten = run_query("SELECT timeseries.apply_retention('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    assert forgotten == 0
    assert count_in(pg_conn, "metrics_cold") == copied
    assert count_in(pg_conn, "metrics") == total


def test_maintain_runs_one_pass(tiered, pg_conn):
    stage(tiered, pg_conn)

    total = count_in(pg_conn, "metrics")

    run_command("SELECT timeseries.maintain('metrics')", pg_conn)
    pg_conn.commit()

    # what aged out is in Iceberg, what is hot is not, and the table is intact
    assert boundary(pg_conn) == hot_window_start(pg_conn)
    assert count_in(pg_conn, "metrics") == total
    assert count_in(pg_conn, "metrics_cold") > 0
    assert all(row["part_start"] >= boundary(pg_conn) for row in heap_ranges(pg_conn))

    # and a second pass has nothing left to do
    run_command("SELECT timeseries.maintain('metrics')", pg_conn)
    pg_conn.commit()

    assert boundary(pg_conn) == hot_window_start(pg_conn)
    assert count_in(pg_conn, "metrics") == total


def test_the_maintenance_worker_runs_a_pass(tiered, pg_conn):
    stage(tiered, pg_conn)

    total = count_in(pg_conn, "metrics")

    assert count_in(pg_conn, "metrics_cold") == 0

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_timeseries.enable TO on",
            "SELECT pg_reload_conf()",
        ]
    )

    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            pg_conn.commit()
            if any(row["sealed_at"] is not None for row in synced_ranges(pg_conn)):
                break
            time.sleep(0.5)

        pg_conn.commit()

        # the worker sealed on its own, and the table still reads as one
        assert any(row["sealed_at"] is not None for row in synced_ranges(pg_conn))
        assert count_in(pg_conn, "metrics_cold") > 0
        assert count_in(pg_conn, "metrics") == total
        assert boundary(pg_conn) == hot_window_start(pg_conn)
    finally:
        run_command_outside_tx(
            [
                "ALTER SYSTEM SET pg_lake_timeseries.enable TO off",
                "SELECT pg_reload_conf()",
            ]
        )


def test_maintenance_is_refused_to_a_non_owner(tiered, pg_conn, user_conn):
    create(tiered)

    run_command("GRANT ALL ON metrics TO test_application", pg_conn)
    run_command("GRANT ALL ON metrics_cold TO test_application", pg_conn)
    pg_conn.commit()

    for statement in (
        "SELECT timeseries.add_partitions('metrics')",
        "SELECT timeseries.sync('metrics')",
        "SELECT timeseries.seal('metrics')",
        "SELECT timeseries.apply_retention('metrics')",
        "SELECT timeseries.maintain('metrics')",
    ):
        error = run_command(statement, user_conn, raise_error=False)
        user_conn.rollback()

        assert error is not None
        assert "must be owner of" in error


def test_maintenance_is_refused_on_an_unregistered_table(timeseries_extension, pg_conn):
    run_command(
        "CREATE TABLE plain (ts timestamptz NOT NULL, device int, value float8)",
        pg_conn,
    )
    pg_conn.commit()

    try:
        for statement in (
            "SELECT timeseries.add_partitions('plain')",
            "SELECT timeseries.sync('plain')",
            "SELECT timeseries.seal('plain')",
            "SELECT timeseries.apply_retention('plain')",
            "SELECT timeseries.maintain('plain')",
        ):
            error = run_command(statement, pg_conn, raise_error=False)
            pg_conn.rollback()

            assert error is not None
            assert "is not a tiered table" in error
    finally:
        run_command("DROP TABLE plain", pg_conn)
        pg_conn.commit()

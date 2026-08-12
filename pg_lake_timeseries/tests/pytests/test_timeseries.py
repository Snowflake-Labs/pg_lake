"""End-to-end tests for pg_lake_timeseries.

The properties under test are the ones the design rests on (DESIGN.md, section
13): the authority boundary decides which tier owns a row, the Iceberg copy of
the hot window is invisible to queries through the view, mutations below the
boundary are merged on read and folded back by repair, and a boundary advance is
only ever the result of a completed seal.
"""

import time

import pytest
from utils_pytest import *


COLUMNS = "ts timestamptz, device int, value float8"

# marker of a scan executed by DuckDB in an EXPLAIN plan
DUCKDB = "Engine: DuckDB"


@pytest.fixture(scope="function")
def ts_table(request, pg_conn, timeseries_extension, test_s3_path):
    """Factory for time-series tables, dropped with their data afterwards."""
    created = []
    # a location per test: DROP TABLE leaves the Iceberg files behind, and
    # CREATE TABLE ... USING iceberg refuses a non-empty location
    prefix = f"{test_s3_path}/{request.node.name}"

    def _create(name, columns=COLUMNS, **options):
        options.setdefault("key_columns", "{ts,device}")
        options.setdefault("partition_interval", "1 day")
        options.setdefault("hot_retention", "3 days")
        options.setdefault("cold_location", f"{prefix}/{name}")

        args = ", ".join(f"{key} => '{value}'" for key, value in options.items())

        run_command(f"CREATE TABLE {name} ({columns})", pg_conn)
        run_command(f"SELECT timeseries.create_table('{name}', 'ts', {args})", pg_conn)
        pg_conn.commit()

        created.append(name)
        return name

    yield _create

    pg_conn.rollback()
    for name in reversed(created):
        run_command(
            f"SELECT timeseries.drop_table('{name}', drop_data => true)",
            pg_conn,
            raise_error=False,
        )
        pg_conn.commit()


def catalog_row(pg_conn, name):
    return run_query(
        f"SELECT * FROM timeseries.tables WHERE parent = '{name}'::regclass", pg_conn
    )[0]


def boundary(pg_conn, name):
    return run_query(f"SELECT timeseries.boundary('{name}')", pg_conn)[0][0]


def partition_states(pg_conn, name):
    rows = run_query(
        f"""
        SELECT state, count(*) FROM timeseries.partitions
         WHERE parent = '{name}'::regclass GROUP BY state
        """,
        pg_conn,
    )
    return {row["state"]: row["count"] for row in rows}


def count_in(pg_conn, relation):
    return run_query(f"SELECT count(*) FROM {relation}", pg_conn)[0][0]


def test_create_table_builds_both_tiers(ts_table, pg_conn):
    ts_table("metrics")

    row = catalog_row(pg_conn, "metrics")
    assert row["hot_table"] == "metrics_hot"
    assert row["cold_table"] == "metrics_cold"
    assert row["delta_table"] == "metrics_delta"
    assert row["key_columns"] == ["ts", "device"]

    # the boundary is the start of the hot window, aligned to a partition
    assert (
        boundary(pg_conn, "metrics")
        == run_query(
            "SELECT timeseries.partition_start(now() - interval '3 days', interval '1 day')",
            pg_conn,
        )[0][0]
    )

    # the whole hot window plus the pre-created future is covered by partitions
    states = partition_states(pg_conn, "metrics")
    assert states == {"hot": 11}, states

    # the view routes between the tiers and merges the delta over the cold one
    definition = run_query("SELECT pg_get_viewdef('metrics'::regclass)", pg_conn)[0][0]
    assert "metrics_hot" in definition
    assert "metrics_cold_scan" in definition
    assert "metrics_delta" in definition


def test_create_table_validations(pg_conn, timeseries_extension):
    run_command("CREATE TABLE bad (ts timestamptz, device int)", pg_conn)
    pg_conn.commit()

    def fails(call):
        error = run_command(call, pg_conn, raise_error=False)
        pg_conn.rollback()
        assert error is not None, call
        return error

    fails("SELECT timeseries.create_table('bad', 'device')")
    fails("SELECT timeseries.create_table('bad', 'nosuchcolumn')")
    fails(
        "SELECT timeseries.create_table('bad', 'ts', partition_interval => '1 month')"
    )
    fails(
        "SELECT timeseries.create_table('bad', 'ts', partition_interval => '1 day',"
        " hot_retention => '1 hour')"
    )
    fails("SELECT timeseries.create_table('bad', 'ts', key_columns => '{device}')")
    fails("SELECT timeseries.create_table('bad', 'ts', upsert => true)")
    fails(
        "SELECT timeseries.create_table('bad', 'ts', key_columns => '{ts,device}',"
        " cluster_columns => '{nosuchcolumn}')"
    )

    # a non-empty table is refused: conversion has to decide where rows belong
    run_command("INSERT INTO bad VALUES (now(), 1)", pg_conn)
    pg_conn.commit()
    fails("SELECT timeseries.create_table('bad', 'ts', key_columns => '{ts,device}')")

    run_command("DROP TABLE bad", pg_conn)
    pg_conn.commit()


def test_hot_writes_stay_in_postgres(ts_table, pg_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 48) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert count_in(pg_conn, "metrics") == 48
    assert count_in(pg_conn, "metrics_hot") == 48
    assert count_in(pg_conn, "metrics_cold") == 0
    assert count_in(pg_conn, "metrics_delta") == 0


def test_tier_routing_is_decided_at_plan_time(ts_table, pg_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 48) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    b = boundary(pg_conn, "metrics")

    # A query restricted to the hot side prunes the Iceberg partition of the
    # cold-scan wrapper, so no part of the query reaches DuckDB. Pruning happens
    # on the partition bound rather than by refuting the cold branch's own
    # predicate, which makes it independent of the shape of the query.
    hot_plan = str(
        run_query(f"EXPLAIN SELECT count(*) FROM metrics WHERE ts >= '{b}'", pg_conn)
    )
    assert "metrics_hot" in hot_plan, hot_plan
    assert DUCKDB not in hot_plan, hot_plan

    # ... and the other way around: no heap partition is scanned.
    cold_plan = str(
        run_query(f"EXPLAIN SELECT count(*) FROM metrics WHERE ts < '{b}'", pg_conn)
    )
    assert DUCKDB in cold_plan, cold_plan
    assert "metrics_hot" not in cold_plan, cold_plan

    # An unrestricted query keeps both branches.
    both = str(run_query("EXPLAIN SELECT count(*) FROM metrics", pg_conn))
    assert "metrics_hot" in both, both
    assert DUCKDB in both, both

    # a projection is pruned the same way
    projected = str(
        run_query(f"EXPLAIN SELECT sum(value) FROM metrics WHERE ts >= '{b}'", pg_conn)
    )
    assert DUCKDB not in projected, projected


def test_sync_copy_is_invisible_to_readers(ts_table, pg_conn):
    """The Iceberg copy of the hot window must never be double-counted."""
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 12) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert count_in(pg_conn, "metrics") == 12

    synced = run_query("SELECT timeseries.sync('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    assert synced >= 1
    assert count_in(pg_conn, "metrics_cold") == 12

    # the rows are now in both tiers, but only PostgreSQL is authoritative
    assert count_in(pg_conn, "metrics") == 12
    assert (
        boundary(pg_conn, "metrics")
        == run_query(
            "SELECT timeseries.partition_start(now() - interval '3 days', interval '1 day')",
            pg_conn,
        )[0][0]
    )

    # syncing again is a no-op: a partition is copied once until it is sealed
    assert run_query("SELECT timeseries.sync('metrics')", pg_conn)[0][0] == 0
    pg_conn.commit()


def test_seal_advances_the_boundary(ts_table, pg_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 12) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    before = boundary(pg_conn, "metrics")
    hot_partitions = count_in(
        pg_conn,
        "timeseries.partitions WHERE parent = 'metrics'::regclass AND state = 'hot'",
    )

    # seal everything up to the start of today, which covers the rows above
    sealed = run_query(
        "SELECT timeseries.seal('metrics', upto => date_trunc('day', now()))", pg_conn
    )[0][0]
    pg_conn.commit()

    assert sealed >= 1
    assert boundary(pg_conn, "metrics") > before

    states = partition_states(pg_conn, "metrics")
    assert states["cold_clean"] == sealed
    assert states["hot"] == hot_partitions - sealed

    # the rows survived the move, exactly once, and now live in Iceberg
    assert count_in(pg_conn, "metrics") == 12
    assert count_in(pg_conn, "metrics_hot") == 0
    assert count_in(pg_conn, "metrics_cold") == 12

    # the heap partitions of the sealed ranges are gone
    assert (
        count_in(
            pg_conn,
            "timeseries.partitions WHERE parent = 'metrics'::regclass"
            " AND state = 'cold_clean' AND hot_partition IS NOT NULL",
        )
        == 0
    )


def test_update_below_boundary_merges_and_repairs(ts_table, pg_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 12) s
        """,
        pg_conn,
    )
    run_command(
        "SELECT timeseries.seal('metrics', upto => date_trunc('day', now()))", pg_conn
    )
    pg_conn.commit()

    target = run_query("SELECT ts, device FROM metrics ORDER BY ts LIMIT 1", pg_conn)[0]

    run_command(
        f"""
        UPDATE metrics SET value = -1
         WHERE ts = '{target['ts']}' AND device = {target['device']}
        """,
        pg_conn,
    )
    pg_conn.commit()

    # the update went to the delta as a tombstone for the old version plus the
    # new version (an update may move a row to another key), and dirtied the
    # partition it landed in
    assert count_in(pg_conn, "metrics_delta") == 2
    assert partition_states(pg_conn, "metrics").get("cold_dirty") == 1

    # merge-on-read: the new value, once, and no row count change
    assert count_in(pg_conn, "metrics") == 12
    assert (
        run_query(
            f"SELECT value FROM metrics WHERE ts = '{target['ts']}'"
            f" AND device = {target['device']}",
            pg_conn,
        )[0][0]
        == -1
    )

    # repair rematerialises the partition and empties the delta
    repaired = run_query("SELECT timeseries.repair('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    assert repaired == 1
    assert count_in(pg_conn, "metrics_delta") == 0
    assert partition_states(pg_conn, "metrics").get("cold_dirty") is None
    assert count_in(pg_conn, "metrics") == 12
    assert (
        run_query(
            f"SELECT value FROM metrics_cold WHERE ts = '{target['ts']}'"
            f" AND device = {target['device']}",
            pg_conn,
        )[0][0]
        == -1
    )


def test_delete_below_boundary_tombstones(ts_table, pg_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 12) s
        """,
        pg_conn,
    )
    run_command(
        "SELECT timeseries.seal('metrics', upto => date_trunc('day', now()))", pg_conn
    )
    pg_conn.commit()

    target = run_query("SELECT ts, device FROM metrics ORDER BY ts LIMIT 1", pg_conn)[0]

    run_command(
        f"DELETE FROM metrics WHERE ts = '{target['ts']}' AND device = {target['device']}",
        pg_conn,
    )
    pg_conn.commit()

    assert count_in(pg_conn, "metrics") == 11
    assert count_in(pg_conn, "metrics_cold") == 12  # still there, masked

    run_query("SELECT timeseries.repair('metrics')", pg_conn)
    pg_conn.commit()

    assert count_in(pg_conn, "metrics") == 11
    assert count_in(pg_conn, "metrics_cold") == 11
    assert count_in(pg_conn, "metrics_delta") == 0


def test_insert_below_boundary_is_visible(ts_table, pg_conn):
    ts_table("metrics")

    # a range that was never hot in this installation
    run_command(
        "INSERT INTO metrics VALUES (now() - interval '30 days', 7, 42)", pg_conn
    )
    pg_conn.commit()

    assert count_in(pg_conn, "metrics_delta") == 1
    assert count_in(pg_conn, "metrics") == 1
    assert run_query("SELECT value FROM metrics", pg_conn)[0][0] == 42

    run_query("SELECT timeseries.repair('metrics')", pg_conn)
    pg_conn.commit()

    assert count_in(pg_conn, "metrics_cold") == 1
    assert count_in(pg_conn, "metrics_delta") == 0
    assert count_in(pg_conn, "metrics") == 1


def test_cross_tier_aggregate(ts_table, pg_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 12) s
        """,
        pg_conn,
    )
    run_command(
        "SELECT timeseries.seal('metrics', upto => date_trunc('day', now()))", pg_conn
    )
    run_command(
        """
        INSERT INTO metrics
        SELECT date_trunc('day', now()) + interval '1 minute' * s, s % 3, 100 + s
          FROM generate_series(1, 6) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert count_in(pg_conn, "metrics_cold") == 12
    # anchored past the boundary the seal just set, so these route to the hot
    # tier whatever time of day the test runs
    assert count_in(pg_conn, "metrics_hot") == 6

    total = run_query("SELECT count(*), sum(value) FROM metrics", pg_conn)[0]
    assert total[0] == 18
    assert total[1] == sum(range(1, 13)) + sum(100 + s for s in range(1, 7))

    # the cold branch is still executed by DuckDB, and each tier is read once
    plan = str(run_query("EXPLAIN SELECT count(*) FROM metrics", pg_conn))
    assert plan.count(DUCKDB) == 1, plan


def test_cross_tier_aggregate_in_one_vectorized_plan(ts_table, pg_conn):
    """With heap pushdown on, a spanning query is a single DuckDB plan.

    DESIGN.md section 13.5, option 2: pg_lake_table admits the hot tier and the
    delta into whole-query pushdown, reading them back over a loopback
    connection at the pinned snapshot of this transaction, so the aggregate runs
    once in DuckDB instead of once per branch in PostgreSQL.
    """
    ts_table("metrics")

    # every timestamp is anchored to midnight rather than to now(), so which
    # daily partition a row lands in — and with it the cold file count asserted
    # at the end — does not depend on the time of day the test runs
    run_command(
        """
        INSERT INTO metrics
        SELECT date_trunc('day', now()) - interval '1 day' * d
                                       + interval '1 hour' * s,
               s % 3, s
          FROM generate_series(1, 2) d, generate_series(1, 6) s
        """,
        pg_conn,
    )
    run_command(
        "SELECT timeseries.seal('metrics', upto => date_trunc('day', now()))", pg_conn
    )
    run_command(
        """
        INSERT INTO metrics
        SELECT date_trunc('day', now()) + interval '1 minute' * s, s % 3, 100 + s
          FROM generate_series(1, 6) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    # statistics on a partitioned hot tier leave relpages = -1 on the parent,
    # which is why the pushed-down scan has to name the partitions themselves
    run_command("ANALYZE metrics_hot", pg_conn)
    pg_conn.commit()

    query = "SELECT count(*), sum(value) FROM metrics"
    expected = tuple(run_query(query, pg_conn)[0])
    pg_conn.commit()
    assert expected[0] == 18

    # the hot tier and the delta are scanned by PostgreSQL, and only the cold
    # branch is vectorized
    plan = str(run_query("EXPLAIN " + query, pg_conn))
    pg_conn.commit()
    assert "Custom Scan (Query Pushdown)" not in plan, plan
    assert "Scan on metrics_hot_" in plan, plan
    assert "metrics_delta" in plan, plan

    # a query restricted to the hot window prunes the cold tier at plan time
    # (DESIGN.md section 18.3), so no cold file is opened for it
    boundary = run_query("SELECT date_trunc('day', now())::text", pg_conn)[0][0]
    hot_only = f"SELECT count(*) FROM metrics WHERE ts >= '{boundary}'::timestamptz"
    hot_plan = str(run_query("EXPLAIN (ANALYZE, VERBOSE) " + hot_only, pg_conn))
    pg_conn.commit()
    assert "Data Files Scanned" not in hot_plan, hot_plan

    run_command("SET pg_lake_table.enable_heap_query_pushdown TO on", pg_conn)
    pg_conn.commit()
    try:
        # now the whole view, both tiers and the delta, is one DuckDB plan
        plan = str(run_query("EXPLAIN " + query, pg_conn))
        pg_conn.commit()
        assert plan.count(DUCKDB) == 1, plan
        assert plan.count("Custom Scan (Query Pushdown)") == 1, plan
        assert "Heap Scan" not in plan, plan
        assert "Append" not in plan, plan

        assert tuple(run_query(query, pg_conn)[0]) == expected
        pg_conn.commit()

        # The hot-window query is pushed down as a whole too, and that costs the
        # plan-time tier elimination above: DuckDB gets the cold branch with a
        # contradictory ts range rather than the planner dropping it. The answer
        # is the same and the row groups are skipped, but the two cold files are
        # opened (DESIGN.md section 18.11).
        assert run_query(hot_only, pg_conn)[0][0] == 6
        pg_conn.commit()

        hot_plan = str(run_query("EXPLAIN (ANALYZE, VERBOSE) " + hot_only, pg_conn))
        pg_conn.commit()
        assert hot_plan.count("Custom Scan (Query Pushdown)") == 1, hot_plan
        assert "Data Files Scanned: 2" in hot_plan, hot_plan
    finally:
        run_command("RESET pg_lake_table.enable_heap_query_pushdown", pg_conn)
        pg_conn.commit()


def test_upsert_on_the_hot_tier(ts_table, pg_conn):
    ts_table("metrics", upsert="true")

    ts = run_query("SELECT date_trunc('hour', now())", pg_conn)[0][0]

    run_command(f"INSERT INTO metrics VALUES ('{ts}', 1, 1)", pg_conn)
    run_command(f"INSERT INTO metrics VALUES ('{ts}', 1, 2)", pg_conn)
    pg_conn.commit()

    assert count_in(pg_conn, "metrics") == 1
    assert run_query("SELECT value FROM metrics", pg_conn)[0][0] == 2


def test_keyless_table_rejects_mutations(ts_table, pg_conn):
    ts_table("events", key_columns="{}")

    run_command("INSERT INTO events VALUES (now(), 1, 1)", pg_conn)
    pg_conn.commit()

    assert count_in(pg_conn, "events") == 1

    error = run_command("UPDATE events SET value = 2", pg_conn, raise_error=False)
    pg_conn.rollback()
    assert "keyless" in str(error)

    error = run_command("DELETE FROM events", pg_conn, raise_error=False)
    pg_conn.rollback()
    assert "keyless" in str(error)


def test_add_partitions_extends_the_frontier(ts_table, pg_conn):
    ts_table("metrics", precreate_ahead="1")

    frontier = run_query(
        "SELECT max(part_end) FROM timeseries.partitions"
        " WHERE parent = 'metrics'::regclass",
        pg_conn,
    )[0][0]

    created = run_query(
        "SELECT timeseries.add_partitions('metrics', now() + interval '5 days')",
        pg_conn,
    )[0][0]
    pg_conn.commit()

    assert created >= 4
    assert (
        run_query(
            "SELECT max(part_end) FROM timeseries.partitions"
            " WHERE parent = 'metrics'::regclass",
            pg_conn,
        )[0][0]
        > frontier
    )

    # a write beyond the frontier is an error rather than a silent misplacement
    error = run_command(
        "INSERT INTO metrics VALUES (now() + interval '60 days', 1, 1)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert "partition" in str(error)


def test_cold_retention_expires_partitions(ts_table, pg_conn):
    ts_table("metrics", hot_retention="1 day", cold_retention="3 days")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' * s, 1, s FROM generate_series(1, 10) s
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("SELECT timeseries.repair('metrics')", pg_conn)
    pg_conn.commit()

    assert count_in(pg_conn, "metrics") == 10

    run_command("SELECT timeseries.apply_retention('metrics')", pg_conn)
    pg_conn.commit()

    # rows older than three days are gone, newer ones are untouched
    assert count_in(pg_conn, "metrics") == 3
    assert (
        count_in(
            pg_conn,
            "metrics WHERE ts < timeseries.partition_start("
            "now() - interval '3 days', interval '1 day')",
        )
        == 0
    )


def test_maintenance_worker_runs_a_pass(ts_table, pg_conn, superuser_conn):
    ts_table("metrics")

    run_command(
        """
        INSERT INTO metrics
        SELECT now() - interval '1 day' - interval '1 hour' * s, s % 3, s
          FROM generate_series(1, 12) s
        """,
        pg_conn,
    )
    pg_conn.commit()

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
            if count_in(pg_conn, "metrics_cold") == 12:
                break
            time.sleep(0.5)

        pg_conn.commit()
        assert count_in(pg_conn, "metrics_cold") == 12
        assert (
            count_in(
                pg_conn,
                "timeseries.partitions WHERE parent = 'metrics'::regclass"
                " AND synced_at IS NOT NULL",
            )
            >= 1
        )
    finally:
        run_command_outside_tx(
            [
                "ALTER SYSTEM SET pg_lake_timeseries.enable TO off",
                "SELECT pg_reload_conf()",
            ]
        )


def test_drop_table_keeps_the_data_by_default(ts_table, pg_conn):
    ts_table("metrics")

    run_command("INSERT INTO metrics VALUES (now(), 1, 1)", pg_conn)
    run_command("SELECT timeseries.drop_table('metrics')", pg_conn)
    pg_conn.commit()

    # the view and the registration are gone, the tiers are not
    assert run_query("SELECT to_regclass('metrics')", pg_conn)[0][0] is None
    assert count_in(pg_conn, "timeseries.tables") == 0
    assert count_in(pg_conn, "timeseries.partitions") == 0
    assert count_in(pg_conn, "metrics_hot") == 1

    run_command(
        "DROP TABLE metrics_hot, metrics_delta, metrics_cold; DROP SEQUENCE metrics_seq",
        pg_conn,
    )
    pg_conn.commit()

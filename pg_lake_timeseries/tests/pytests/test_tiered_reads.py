"""Tests for reading a tiered table (DESIGN.md section 7).

A registered relation is a partitioned heap that holds everything at or above the
authority boundary; the Iceberg tier holds what is below it. Neither table knows
about the other, so what makes them one table is the planner hook in
src/planner.c, and what is worth testing is the property it has to preserve:
every row is returned exactly once, whichever tier happens to hold it, and a
query that asks for one side of the boundary does not pay for the other.

The staging helper below seals a partition explicitly rather than waiting for
maintenance, so the boundary is at a known point in every test.
"""

import io

import psycopg2
import pytest

from utils_pytest import *


# heap partitions are named <table>_<yyyymmdd>t<hh><mm>, so this substring is in
# the plan of a query that reads any of them and in no other plan
HEAP_PARTITION = "metrics_20"

# rows staged per hour-long partition, and the partitions they land in
ROWS_PER_PARTITION = 3
COLD_ROWS = 3
HOT_ROWS = 4
TOTAL_ROWS = COLD_ROWS + HOT_ROWS


def stage(pg_conn, tiered, name="metrics", seal=True):
    """A tiered table with three hour-long partitions of rows around now().

    Sealing hands the oldest of them to Iceberg, which leaves the boundary at the
    start of the previous hour with rows on both sides of it.
    """
    tiered(name, partition_interval="1 hour", hot_retention="2 hours")

    # 20 minutes apart from two hours ago, so three per hour-long partition
    run_command(
        f"INSERT INTO {name}"
        " SELECT ts, 1, extract(epoch FROM ts)::float8"
        " FROM generate_series(date_trunc('hour', now()) - interval '2 hours'"
        "                       + interval '10 minutes',"
        "                      date_trunc('hour', now()) + interval '10 minutes',"
        "                      interval '20 minutes') ts",
        pg_conn,
    )
    pg_conn.commit()

    if not seal:
        return None

    sealed = run_query(
        f"SELECT timeseries.seal('{name}', upto => now() - interval '1 hour')", pg_conn
    )[0][0]
    pg_conn.commit()

    # the staging is only useful if exactly the oldest partition went cold
    assert sealed == 1
    assert cold_count(pg_conn, name) == COLD_ROWS

    return boundary(pg_conn, name)


def boundary(pg_conn, name="metrics"):
    return run_query(
        f"SELECT boundary FROM timeseries.tiered_table('{name}'::regclass)", pg_conn
    )[0][0]


def boundary_text(pg_conn, name="metrics"):
    """The boundary as PostgreSQL prints it, so -infinity survives the client."""
    return run_query(
        f"SELECT boundary::text FROM timeseries.tiered_table('{name}'::regclass)",
        pg_conn,
    )[0][0]


def explain(pg_conn, query):
    """The plan of a query as one string, with the tiers' own details in it."""
    rows = run_query(f"EXPLAIN (VERBOSE, COSTS OFF) {query}", pg_conn)
    return "\n".join(row[0] for row in rows)


def cold_count(pg_conn, name="metrics"):
    """Rows in the Iceberg tier, read from the tier itself."""
    return run_query(f"SELECT count(*) FROM {name}_cold", pg_conn)[0][0]


def heap_count(pg_conn, name="metrics"):
    """Rows in the PostgreSQL tier, read without the expansion."""
    run_command("SET pg_lake_timeseries.expand_tiered_tables TO off", pg_conn)
    try:
        return run_query(f"SELECT count(*) FROM {name}", pg_conn)[0][0]
    finally:
        run_command("RESET pg_lake_timeseries.expand_tiered_tables", pg_conn)


def test_a_row_below_the_boundary_is_read_from_iceberg(tiered, pg_conn):
    b = stage(pg_conn, tiered)

    # the heap no longer has the sealed rows
    assert heap_count(pg_conn) == HOT_ROWS
    assert cold_count(pg_conn) == COLD_ROWS

    # but the relation still returns all of them
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS
    assert (
        run_query(f"SELECT count(*) FROM metrics WHERE ts < '{b}'", pg_conn)[0][0]
        == COLD_ROWS
    )

    # and the aggregate over both tiers is the one over the rows inserted
    assert (
        run_query("SELECT sum(value), min(ts), max(ts) FROM metrics", pg_conn)[0]
        == run_query(
            "SELECT sum(extract(epoch FROM ts)::float8), min(ts), max(ts)"
            " FROM (SELECT ts FROM metrics_cold UNION ALL"
            "       SELECT ts FROM metrics WHERE ts >= '" + str(b) + "') u",
            pg_conn,
        )[0]
    )


def test_an_unsealed_table_reads_only_the_heap(tiered, pg_conn):
    """With the boundary at -infinity the cold tier owns nothing at all."""
    stage(pg_conn, tiered, seal=False)

    assert boundary_text(pg_conn) == "-infinity"

    # a row written straight into the cold tier is not part of the table
    run_command(
        "INSERT INTO metrics_cold VALUES (now() - interval '1 year', 9, 9.0)", pg_conn
    )
    pg_conn.commit()

    assert cold_count(pg_conn) == 1
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS
    assert heap_count(pg_conn) == TOTAL_ROWS

    # and the plan does not read it
    plan = explain(pg_conn, "SELECT count(*) FROM metrics")
    assert "metrics_cold" not in plan


def test_the_iceberg_copy_of_a_hot_partition_is_not_returned_twice(tiered, pg_conn):
    """sync() copies partitions that are still authoritative in PostgreSQL.

    Those rows exist in both tiers at once, which is exactly what the boundary
    predicates on the two branches have to resolve.
    """
    b = stage(pg_conn, tiered)

    synced = run_query("SELECT timeseries.sync('metrics')", pg_conn)[0][0]
    pg_conn.commit()

    # the partition that is over but still hot has been copied
    assert synced == 1
    assert cold_count(pg_conn) == COLD_ROWS + ROWS_PER_PARTITION

    # and the table still has each row once
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS
    assert (
        run_query("SELECT count(DISTINCT ts) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS
    )
    assert boundary(pg_conn) == b


def test_a_predicate_on_one_side_of_the_boundary_prunes_the_other(tiered, pg_conn):
    b = stage(pg_conn, tiered)

    hot_only = explain(pg_conn, f"SELECT count(*) FROM metrics WHERE ts >= '{b}'")
    assert HEAP_PARTITION in hot_only
    # PostgreSQL cannot exclude a foreign table from the plan, so the Iceberg
    # scan is still there -- but its predicate contradicts the boundary, and it
    # opens no file
    assert "metrics_cold" in hot_only
    assert "Data Files Scanned: 0" in hot_only

    cold_only = explain(pg_conn, f"SELECT count(*) FROM metrics WHERE ts < '{b}'")
    assert "metrics_cold" in cold_only
    assert "Data Files Scanned: 1" in cold_only
    # every heap partition begins at or above the boundary, so all are pruned
    assert HEAP_PARTITION not in cold_only

    # without a predicate both tiers are read
    both = explain(pg_conn, "SELECT count(*) FROM metrics")
    assert "metrics_cold" in both
    assert "Data Files Scanned: 1" in both
    assert HEAP_PARTITION in both


def test_the_expansion_can_be_turned_off(tiered, pg_conn):
    stage(pg_conn, tiered)

    run_command("SET pg_lake_timeseries.expand_tiered_tables TO off", pg_conn)
    try:
        assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == HOT_ROWS
        plan = explain(pg_conn, "SELECT count(*) FROM metrics")
        assert "metrics_cold" not in plan
    finally:
        run_command("RESET pg_lake_timeseries.expand_tiered_tables", pg_conn)

    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS


def test_the_expansion_reaches_every_level_of_a_query(tiered, pg_conn):
    stage(pg_conn, tiered)

    # a subquery, a CTE, a sublink and a self-join
    assert (
        run_query("SELECT count(*) FROM (SELECT * FROM metrics) s", pg_conn)[0][0]
        == TOTAL_ROWS
    )
    assert (
        run_query("WITH c AS (SELECT ts FROM metrics) SELECT count(*) FROM c", pg_conn)[
            0
        ][0]
        == TOTAL_ROWS
    )
    assert (
        run_query(
            "SELECT count(*) FROM metrics m"
            " WHERE EXISTS (SELECT 1 FROM metrics o WHERE o.ts = m.ts)",
            pg_conn,
        )[0][0]
        == TOTAL_ROWS
    )
    assert (
        run_query("SELECT count(*) FROM metrics a JOIN metrics b USING (ts)", pg_conn)[
            0
        ][0]
        == TOTAL_ROWS
    )
    # and a set operation, whose branches are queries of their own
    assert (
        run_query(
            "SELECT count(*) FROM ("
            "  SELECT ts FROM metrics UNION ALL SELECT ts FROM metrics) u",
            pg_conn,
        )[0][0]
        == 2 * TOTAL_ROWS
    )


def test_writes_are_unaffected(tiered, pg_conn):
    b = stage(pg_conn, tiered)

    # an ordinary insert is routed by PostgreSQL into a heap partition
    run_command("INSERT INTO metrics VALUES (now(), 2, 1.0)", pg_conn)
    pg_conn.commit()

    assert heap_count(pg_conn) == HOT_ROWS + 1
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS + 1

    # an update of the relation reaches the heap rows only
    run_command("UPDATE metrics SET device = 3", pg_conn)
    pg_conn.commit()

    assert (
        run_query("SELECT count(*) FROM metrics WHERE device = 3", pg_conn)[0][0]
        == HOT_ROWS + 1
    )
    assert cold_count(pg_conn) == COLD_ROWS

    # a delete below the boundary matches nothing rather than deleting cold rows
    run_command(f"DELETE FROM metrics WHERE ts < '{b}'", pg_conn)
    pg_conn.commit()

    assert cold_count(pg_conn) == COLD_ROWS
    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS + 1

    # and a write below the boundary finds no partition, which is an error
    error = run_command(
        f"INSERT INTO metrics VALUES ('{b}'::timestamptz - interval '1 hour', 4, 1.0)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert error is not None
    assert "no partition of relation" in error


def test_a_cached_plan_is_replanned_when_the_boundary_moves(tiered, pg_conn):
    """The boundary is planted in the plan as a constant, so it has to invalidate."""
    stage(pg_conn, tiered, seal=False)

    run_command("PREPARE staged AS SELECT count(*) FROM metrics", pg_conn)

    for _ in range(6):
        assert run_query("EXECUTE staged", pg_conn)[0][0] == TOTAL_ROWS
    pg_conn.commit()

    sealed = run_query(
        "SELECT timeseries.seal('metrics', upto => now() - interval '1 hour')", pg_conn
    )[0][0]
    pg_conn.commit()

    assert sealed == 1
    assert heap_count(pg_conn) == HOT_ROWS

    # the sealed rows now come from the tier the cached plan did not read
    assert run_query("EXECUTE staged", pg_conn)[0][0] == TOTAL_ROWS

    run_command("DEALLOCATE staged", pg_conn)
    pg_conn.commit()


def test_row_level_security_is_refused(tiered, pg_conn):
    stage(pg_conn, tiered)

    run_command("ALTER TABLE metrics ENABLE ROW LEVEL SECURITY", pg_conn)
    pg_conn.commit()

    error = run_command("SELECT count(*) FROM metrics", pg_conn, raise_error=False)
    pg_conn.rollback()

    assert error is not None
    assert "row-level security is not supported" in error

    # the heap is still readable on its own, and disabling it restores the table
    run_command("ALTER TABLE metrics DISABLE ROW LEVEL SECURITY", pg_conn)
    pg_conn.commit()

    assert run_query("SELECT count(*) FROM metrics", pg_conn)[0][0] == TOTAL_ROWS


def test_reading_the_table_needs_no_privileges_on_the_tiers(tiered, pg_conn, user_conn):
    """The tiers are read like the branches of a view: as the table's owner."""
    stage(pg_conn, tiered)

    run_command("GRANT SELECT ON metrics TO test_application", pg_conn)
    pg_conn.commit()

    assert run_query("SELECT count(*) FROM metrics", user_conn)[0][0] == TOTAL_ROWS

    # while the cold tier itself is not readable
    error = run_query("SELECT count(*) FROM metrics_cold", user_conn, raise_error=False)
    user_conn.rollback()
    assert error is not None
    assert "permission denied" in error

    # and a reader without a grant on the table gets nowhere
    run_command("REVOKE SELECT ON metrics FROM test_application", pg_conn)
    pg_conn.commit()

    error = run_query("SELECT count(*) FROM metrics", user_conn, raise_error=False)
    user_conn.rollback()
    assert error is not None
    assert "permission denied" in error


def test_a_query_over_both_tiers_can_be_pushed_down_whole(tiered, pg_conn):
    """With heap pushdown on, a query spanning both tiers becomes one DuckDB plan.

    DESIGN.md section 13: pg_lake_table admits heap relations into
    whole-query pushdown and reads them back over a loopback connection at the
    pinned snapshot of this transaction, so an aggregate over the union the hook
    built runs once in DuckDB instead of once per tier in PostgreSQL.
    """
    stage(pg_conn, tiered)

    # statistics on a partitioned heap leave relpages = -1 on the parent, which is
    # why the pushed-down scan has to name the partitions themselves
    run_command("ANALYZE metrics", pg_conn)
    pg_conn.commit()

    query = "SELECT count(*), sum(value) FROM metrics"
    expected = tuple(run_query(query, pg_conn)[0])

    assert expected[0] == TOTAL_ROWS

    # by default each tier is scanned by the executor that owns it
    plan = explain(pg_conn, query)
    assert "Custom Scan (Query Pushdown)" not in plan
    assert HEAP_PARTITION in plan

    run_command("SET pg_lake_table.enable_heap_query_pushdown TO on", pg_conn)
    try:
        pushed = explain(pg_conn, query)

        # one scan for the whole query, and no Append of the two branches
        assert pushed.count("Custom Scan (Query Pushdown)") == 1
        assert "Append" not in pushed

        # and it is the same answer
        assert tuple(run_query(query, pg_conn)[0]) == expected
    finally:
        run_command("RESET pg_lake_table.enable_heap_query_pushdown", pg_conn)
        pg_conn.commit()


def test_copy_to_goes_through_the_planner_or_not_at_all(tiered, pg_conn):
    """COPY is the one read path that skips the planner, and PostgreSQL refuses it.

    A registered relation is partitioned, and COPY <partitioned table> TO is not
    supported, so there is no way to read the table with the cold tier left out.
    """
    stage(pg_conn, tiered)

    def copy_out(command):
        buffer = io.StringIO()
        cursor = pg_conn.cursor()
        try:
            cursor.copy_expert(command, buffer)
        finally:
            cursor.close()
        return len(buffer.getvalue().splitlines())

    with pytest.raises(psycopg2.errors.WrongObjectType):
        copy_out("COPY metrics TO STDOUT")
    pg_conn.rollback()

    # the form that is planned reads the whole table
    assert copy_out("COPY (SELECT * FROM metrics) TO STDOUT") == TOTAL_ROWS

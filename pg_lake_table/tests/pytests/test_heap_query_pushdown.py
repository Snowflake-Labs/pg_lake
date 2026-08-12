import psycopg2
import pytest
from utils_pytest import *

# Tests for pg_lake_table.enable_heap_query_pushdown, which admits plain
# PostgreSQL tables into whole-query pushdown so that a query spanning a
# PostgreSQL tier and an Iceberg tier becomes a single vectorized DuckDB plan.

SPANNING_QUERY = """
SELECT count(*), sum(value)::bigint FROM (
    SELECT event_day, value FROM cold_events
    UNION ALL
    SELECT event_day, value FROM hot_events
) all_events
WHERE event_day >= '2024-01-05'
"""

JOIN_QUERY = """
SELECT count(*), sum(c.value)::bigint
FROM cold_events c JOIN hot_devices d USING (device_id)
WHERE d.region = 'eu'
"""


@pytest.fixture(scope="module")
def tiered_tables(extension, pg_conn, s3):
    location = f"s3://{TEST_BUCKET}/test_heap_query_pushdown/"

    run_command(
        f"""
        DROP TABLE IF EXISTS cold_events, hot_events, hot_devices;

        SET pg_lake_iceberg.default_location_prefix = '{location}';

        CREATE TABLE cold_events (
            event_day date,
            device_id int,
            value int
        ) USING iceberg;

        RESET pg_lake_iceberg.default_location_prefix;

        -- the cold tier: 10 days, ending before the hot tier starts
        INSERT INTO cold_events
        SELECT '2024-01-01'::date + (g % 10), g % 7, g
        FROM generate_series(1, 1000) g;

        -- the hot tier: plain PostgreSQL
        CREATE TABLE hot_events (
            event_day date,
            device_id int,
            value int
        );

        INSERT INTO hot_events
        SELECT '2024-01-11'::date + (g % 4), g % 7, g
        FROM generate_series(1, 100) g;

        CREATE TABLE hot_devices (
            device_id int primary key,
            region text
        );

        INSERT INTO hot_devices
        SELECT g, CASE WHEN g % 2 = 0 THEN 'eu' ELSE 'us' END
        FROM generate_series(0, 6) g;

        ANALYZE hot_events, hot_devices;
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP TABLE IF EXISTS cold_events, hot_events, hot_devices", pg_conn)
    pg_conn.commit()


def enable_heap_pushdown(conn):
    run_command("SET pg_lake_table.enable_heap_query_pushdown TO on", conn)
    conn.commit()


def disable_heap_pushdown(conn):
    run_command("RESET pg_lake_table.enable_heap_query_pushdown", conn)
    conn.commit()


def single_row(query, conn):
    result = run_query(query, conn)
    conn.commit()
    return tuple(result[0])


def test_heap_pushdown_is_off_by_default(tiered_tables, pg_conn):
    # the default has to stay off: the loopback connection is only guaranteed
    # to authenticate once an administrator has said so
    result = run_query("SHOW pg_lake_table.enable_heap_query_pushdown", pg_conn)
    pg_conn.commit()
    assert result[0][0] == "off"

    assert_query_not_pushdownable(SPANNING_QUERY, pg_conn)
    assert_query_not_pushdownable(JOIN_QUERY, pg_conn)
    pg_conn.commit()


def test_spanning_query_is_one_vectorized_plan(tiered_tables, pg_conn):
    # answer computed by PostgreSQL, with the lake side scanned through the FDW
    disable_heap_pushdown(pg_conn)
    expected = single_row(SPANNING_QUERY, pg_conn)
    # 600 of the 1000 cold rows fall on or after 2024-01-05, plus all 100 hot rows
    assert expected[0] == 700

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_pushdownable(SPANNING_QUERY, pg_conn)
        pg_conn.commit()

        # both tiers end up in the same query sent to pgduck_server
        assert_remote_query_contains_expression(SPANNING_QUERY, "cold_events", pg_conn)
        assert_remote_query_contains_expression(SPANNING_QUERY, "hot_events", pg_conn)
        pg_conn.commit()

        # ... in a single DuckDB plan, not an Append of two scans
        plan = str(run_query("EXPLAIN (VERBOSE) " + SPANNING_QUERY, pg_conn))
        pg_conn.commit()
        assert plan.count("Custom Scan (Query Pushdown)") == 1
        assert "Foreign Scan" not in plan

        assert single_row(SPANNING_QUERY, pg_conn) == expected
    finally:
        disable_heap_pushdown(pg_conn)


def test_join_across_tiers(tiered_tables, pg_conn):
    disable_heap_pushdown(pg_conn)
    expected = single_row(JOIN_QUERY, pg_conn)
    assert expected[0] > 0

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_pushdownable(JOIN_QUERY, pg_conn)
        pg_conn.commit()
        assert single_row(JOIN_QUERY, pg_conn) == expected

        # the join filter reaches the loopback connection, and the join itself
        # happens inside DuckDB
        assert_remote_query_contains_expression(JOIN_QUERY, "hot_devices", pg_conn)
        pg_conn.commit()
    finally:
        disable_heap_pushdown(pg_conn)


def test_heap_only_query_is_untouched(tiered_tables, pg_conn):
    # without a lake table there is nothing to gain, and we must not take over
    # ordinary PostgreSQL queries
    enable_heap_pushdown(pg_conn)
    try:
        assert_query_not_pushdownable(
            "SELECT count(*) FROM hot_events JOIN hot_devices USING (device_id)",
            pg_conn,
        )
        pg_conn.commit()
    finally:
        disable_heap_pushdown(pg_conn)


def test_snapshot_is_pinned(tiered_tables, pg_conn):
    enable_heap_pushdown(pg_conn)
    other_conn = open_pg_conn()

    try:
        # psycopg2 has emitted BEGIN by the time this runs, so this sets the
        # isolation level of the transaction we are about to read in
        run_command("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", pg_conn)
        before = tuple(run_query(SPANNING_QUERY, pg_conn)[0])

        # another session adds rows to the hot tier and commits
        run_command(
            """
            INSERT INTO hot_events
            SELECT '2024-01-12'::date, 3, 10 FROM generate_series(1, 7)
            """,
            other_conn,
        )
        other_conn.commit()

        # our transaction still sees the tier as it was when it started, even
        # though the heap side is read back over a separate connection
        after = tuple(run_query(SPANNING_QUERY, pg_conn)[0])
        assert after == before
        pg_conn.commit()

        # a new transaction sees them
        latest = tuple(run_query(SPANNING_QUERY, pg_conn)[0])
        pg_conn.commit()
        assert latest[0] == before[0] + 7
    finally:
        run_command(
            "DELETE FROM hot_events WHERE device_id = 3 AND value = 10", other_conn
        )
        other_conn.commit()
        other_conn.close()
        disable_heap_pushdown(pg_conn)


def test_writing_transaction_falls_back(tiered_tables, pg_conn):
    enable_heap_pushdown(pg_conn)
    try:
        expected = single_row(SPANNING_QUERY, pg_conn)

        # an exported snapshot hides the exporting transaction's own writes, so
        # once we have written we plan the query the ordinary way instead
        run_command(
            "INSERT INTO hot_events VALUES ('2024-01-12', 3, 5)",
            pg_conn,
        )

        assert_query_not_pushdownable(SPANNING_QUERY, pg_conn)

        # and the answer includes the row we just inserted
        result = tuple(run_query(SPANNING_QUERY, pg_conn)[0])
        assert result[0] == expected[0] + 1

        pg_conn.rollback()
        assert single_row(SPANNING_QUERY, pg_conn) == expected
    finally:
        pg_conn.rollback()
        disable_heap_pushdown(pg_conn)


def test_partitioned_hot_tier(tiered_tables, pg_conn):
    run_command(
        """
        DROP TABLE IF EXISTS hot_partitioned;

        CREATE TABLE hot_partitioned (
            event_day date,
            device_id int,
            value int
        ) PARTITION BY RANGE (event_day);

        CREATE TABLE hot_partitioned_1
            PARTITION OF hot_partitioned
            FOR VALUES FROM ('2024-01-11') TO ('2024-01-13');
        CREATE TABLE hot_partitioned_2
            PARTITION OF hot_partitioned
            FOR VALUES FROM ('2024-01-13') TO ('2024-01-15');

        INSERT INTO hot_partitioned
        SELECT '2024-01-11'::date + (g % 4), g % 7, g
        FROM generate_series(1, 100) g;

        -- relpages of the parent becomes -1, which is what makes the scan of
        -- the parent itself unusable
        ANALYZE hot_partitioned;
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = """
    SELECT count(*), sum(value)::bigint FROM (
        SELECT event_day, value FROM cold_events
        UNION ALL
        SELECT event_day, value FROM hot_partitioned
    ) all_events
    """

    enable_heap_pushdown(pg_conn)
    try:
        disable_heap_pushdown(pg_conn)
        expected = single_row(query, pg_conn)

        enable_heap_pushdown(pg_conn)
        assert_query_pushdownable(query, pg_conn)
        pg_conn.commit()

        # the scanner sizes its parallel ctid range scans from pg_class.relpages,
        # and a partitioned table has relpages = -1, which it reads as an
        # unsigned page count of 2^64-1 and never finishes handing out tasks for.
        # So the partitions have to be named individually, and this query has to
        # answer rather than run until the timeout.
        run_command("SET statement_timeout TO '60s'", pg_conn)
        pg_conn.commit()
        assert single_row(query, pg_conn) == expected

        # both partitions are read, exactly once each
        assert_remote_query_contains_expression(query, "hot_partitioned_1", pg_conn)
        assert_remote_query_contains_expression(query, "hot_partitioned_2", pg_conn)
        pg_conn.commit()
    finally:
        run_command("RESET statement_timeout", pg_conn)
        disable_heap_pushdown(pg_conn)
        run_command("DROP TABLE IF EXISTS hot_partitioned", pg_conn)
        pg_conn.commit()


def test_partitioned_hot_tier_without_partitions(tiered_tables, pg_conn):
    # a parent with no partitions still has to produce the column layout the
    # rest of the query expects
    run_command(
        """
        DROP TABLE IF EXISTS hot_empty;

        CREATE TABLE hot_empty (event_day date, device_id int, value int)
            PARTITION BY RANGE (event_day);
        ANALYZE hot_empty;
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = """
    SELECT count(*), sum(value)::bigint FROM (
        SELECT event_day, value FROM cold_events
        UNION ALL
        SELECT event_day, value FROM hot_empty
    ) all_events
    """

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_pushdownable(query, pg_conn)
        pg_conn.commit()
        assert single_row(query, pg_conn) == (1000, 500500)
    finally:
        disable_heap_pushdown(pg_conn)
        run_command("DROP TABLE IF EXISTS hot_empty", pg_conn)
        pg_conn.commit()


def test_temp_table_is_not_pushed_down(tiered_tables, pg_conn):
    # another backend cannot see our temporary tables
    run_command(
        """
        CREATE TEMP TABLE hot_temp (event_day date, device_id int, value int);
        INSERT INTO hot_temp VALUES ('2024-01-12', 3, 5);
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = """
    SELECT count(*) FROM (
        SELECT event_day FROM cold_events
        UNION ALL
        SELECT event_day FROM hot_temp
    ) all_events
    """

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_not_pushdownable(query, pg_conn)
        pg_conn.commit()
        assert single_row(query, pg_conn) == (1001,)
    finally:
        disable_heap_pushdown(pg_conn)
        run_command("DROP TABLE IF EXISTS hot_temp", pg_conn)
        pg_conn.commit()


def test_row_level_security_is_not_pushed_down(tiered_tables, pg_conn):
    # the loopback connection would not apply the policies
    run_command(
        """
        DROP TABLE IF EXISTS hot_secured;

        CREATE TABLE hot_secured (event_day date, device_id int, value int);
        INSERT INTO hot_secured VALUES ('2024-01-12', 3, 5), ('2024-01-12', 4, 6);
        ALTER TABLE hot_secured ENABLE ROW LEVEL SECURITY;
        CREATE POLICY hot_secured_policy ON hot_secured
            USING (device_id = 3);
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = """
    SELECT count(*) FROM (
        SELECT event_day FROM cold_events
        UNION ALL
        SELECT event_day FROM hot_secured
    ) all_events
    """

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_not_pushdownable(query, pg_conn)
        pg_conn.commit()
    finally:
        disable_heap_pushdown(pg_conn)
        run_command("DROP TABLE IF EXISTS hot_secured", pg_conn)
        pg_conn.commit()


def test_dropped_column_is_not_pushed_down(tiered_tables, pg_conn):
    # the scanner only returns live columns, so a dropped column in the middle
    # would shift the attribute numbers the deparsed query uses
    run_command(
        """
        DROP TABLE IF EXISTS hot_dropped;

        CREATE TABLE hot_dropped (event_day date, junk text, value int);
        INSERT INTO hot_dropped VALUES ('2024-01-12', 'x', 5);
        ALTER TABLE hot_dropped DROP COLUMN junk;
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = """
    SELECT count(*), sum(value)::bigint FROM (
        SELECT event_day, value FROM cold_events
        UNION ALL
        SELECT event_day, value FROM hot_dropped
    ) all_events
    """

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_not_pushdownable(query, pg_conn)
        pg_conn.commit()
        assert single_row(query, pg_conn)[0] == 1001
    finally:
        disable_heap_pushdown(pg_conn)
        run_command("DROP TABLE IF EXISTS hot_dropped", pg_conn)
        pg_conn.commit()


def test_inheritance_parent_is_not_pushed_down(tiered_tables, pg_conn):
    # legacy inheritance: the parent has storage of its own, and a remote scan
    # of the parent would miss the children
    run_command(
        """
        DROP TABLE IF EXISTS hot_child, hot_parent;

        CREATE TABLE hot_parent (event_day date, value int);
        CREATE TABLE hot_child () INHERITS (hot_parent);
        INSERT INTO hot_parent VALUES ('2024-01-12', 5);
        INSERT INTO hot_child VALUES ('2024-01-13', 6);
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = """
    SELECT count(*), sum(value)::bigint FROM (
        SELECT event_day, value FROM cold_events
        UNION ALL
        SELECT event_day, value FROM hot_parent
    ) all_events
    """

    enable_heap_pushdown(pg_conn)
    try:
        assert_query_not_pushdownable(query, pg_conn)
        pg_conn.commit()
        assert single_row(query, pg_conn)[0] == 1002
    finally:
        disable_heap_pushdown(pg_conn)
        run_command("DROP TABLE IF EXISTS hot_child, hot_parent", pg_conn)
        pg_conn.commit()

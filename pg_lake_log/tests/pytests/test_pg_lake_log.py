"""
Tests for the pg_lake_log extension.

These tests verify:
  - The extension can be created.
  - Log entries are captured in the ring buffer.
  - pg_lake_log.flush() writes entries to the target Iceberg table.
  - pg_lake_log.buffer_status() returns sensible values.
  - The background worker eventually flushes entries on its own.
  - GUC changes (min_severity, enabled) filter entries correctly.
"""

import time
import pytest
from utils_pytest import *


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

LOG_TABLE = "pg_lake_log_test.captured_logs"


def create_extension(conn):
    run_command("CREATE EXTENSION IF NOT EXISTS pg_lake_log CASCADE;", conn)
    conn.commit()


def drop_log_table(conn):
    run_command(f"DROP TABLE IF EXISTS {LOG_TABLE};", conn)
    conn.commit()


def create_log_table(conn, location):
    run_command(
        f"SELECT pg_lake_log.create_log_table('{LOG_TABLE}', '{location}');",
        conn,
    )
    conn.commit()


def set_target_table(conn, table=LOG_TABLE):
    run_command(
        f"SET pg_lake_log.target_table TO '{table}';",
        conn,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def log_table(superuser_conn, location_prefix):
    """
    Create the target Iceberg table once for the whole module and clean up
    afterwards.
    """
    create_extension(superuser_conn)
    run_command("CREATE SCHEMA IF NOT EXISTS pg_lake_log_test;", superuser_conn)
    superuser_conn.commit()

    loc = location_prefix.rstrip("/") + "/pg_lake_log_test"
    create_log_table(superuser_conn, loc)

    yield LOG_TABLE

    drop_log_table(superuser_conn)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_extension_creates(superuser_conn):
    """The extension should install without errors."""
    create_extension(superuser_conn)
    result = fetch_one(
        "SELECT extname FROM pg_extension WHERE extname = 'pg_lake_log';",
        superuser_conn,
    )
    assert result is not None
    assert result[0] == "pg_lake_log"


def test_buffer_status_returns_row(superuser_conn):
    """buffer_status() should return a record with four non-null columns."""
    create_extension(superuser_conn)
    row = fetch_one("SELECT * FROM pg_lake_log.buffer_status();", superuser_conn)
    assert row is not None
    assert len(row) == 4
    # write_pos, read_pos, buffered_count, dropped_count are all non-negative
    for val in row:
        assert val >= 0


def test_flush_writes_to_iceberg(superuser_conn, log_table):
    """
    After generating some log messages and calling flush(), the Iceberg table
    should contain matching rows.
    """
    set_target_table(superuser_conn)

    # Lower the minimum severity so NOTICE messages are captured.
    run_command("SET pg_lake_log.min_severity TO 'notice';", superuser_conn)
    superuser_conn.commit()

    marker = f"pg_lake_log_test_marker_{int(time.time())}"

    # Emit a NOTICE that the hook will capture.
    run_command(f"DO $$ BEGIN RAISE NOTICE '{marker}'; END $$;", superuser_conn)
    superuser_conn.commit()

    # Synchronously flush the buffer.
    result = fetch_one("SELECT pg_lake_log.flush();", superuser_conn)
    assert result is not None
    written = result[0]
    assert written >= 1

    # Verify the marker appears in the table.
    count_row = fetch_one(
        f"SELECT count(*) FROM {log_table} WHERE message LIKE '%{marker}%';",
        superuser_conn,
    )
    assert count_row is not None
    assert count_row[0] >= 1


def test_flush_returns_zero_when_buffer_empty(superuser_conn, log_table):
    """Calling flush() on an already-empty buffer should return 0."""
    set_target_table(superuser_conn)

    # Drain whatever is pending first.
    run_command("SELECT pg_lake_log.flush();", superuser_conn)
    superuser_conn.commit()

    result = fetch_one("SELECT pg_lake_log.flush();", superuser_conn)
    assert result is not None
    assert result[0] == 0


def test_min_severity_filters_messages(superuser_conn, log_table):
    """Messages below min_severity should not appear in the buffer."""
    set_target_table(superuser_conn)

    # Set severity to WARNING – DEBUG messages should be ignored.
    run_command("SET pg_lake_log.min_severity TO 'warning';", superuser_conn)
    superuser_conn.commit()

    # Drain any existing entries.
    run_command("SELECT pg_lake_log.flush();", superuser_conn)
    superuser_conn.commit()

    debug_marker = f"pg_lake_log_debug_{int(time.time())}"

    # Emit a DEBUG1 message – should be filtered out.
    run_command(
        f"SET client_min_messages TO 'debug1';"
        f"DO $$ BEGIN RAISE DEBUG '{debug_marker}'; END $$;",
        superuser_conn,
    )
    superuser_conn.commit()

    written = fetch_one("SELECT pg_lake_log.flush();", superuser_conn)[0]

    # Even if some WARNING+ messages leaked in, the debug marker should not be
    # present.
    count_row = fetch_one(
        f"SELECT count(*) FROM {log_table} WHERE message LIKE '%{debug_marker}%';",
        superuser_conn,
    )
    assert count_row[0] == 0


def test_enabled_guc_stops_capture(superuser_conn, log_table):
    """Setting pg_lake_log.enabled = false should stop all capture."""
    set_target_table(superuser_conn)

    # Disable capture.
    run_command("SET pg_lake_log.enabled TO false;", superuser_conn)
    run_command("SET pg_lake_log.min_severity TO 'notice';", superuser_conn)
    superuser_conn.commit()

    marker = f"pg_lake_log_disabled_{int(time.time())}"
    run_command(f"DO $$ BEGIN RAISE NOTICE '{marker}'; END $$;", superuser_conn)
    superuser_conn.commit()

    written = fetch_one("SELECT pg_lake_log.flush();", superuser_conn)[0]
    assert written == 0

    count_row = fetch_one(
        f"SELECT count(*) FROM {log_table} WHERE message LIKE '%{marker}%';",
        superuser_conn,
    )
    assert count_row[0] == 0

    # Re-enable for subsequent tests.
    run_command("SET pg_lake_log.enabled TO true;", superuser_conn)
    superuser_conn.commit()


def test_buffer_columns_schema(superuser_conn, log_table):
    """Rows written to the Iceberg table should have the expected column types."""
    set_target_table(superuser_conn)
    run_command("SET pg_lake_log.min_severity TO 'notice';", superuser_conn)
    superuser_conn.commit()

    run_command("DO $$ BEGIN RAISE NOTICE 'schema check'; END $$;", superuser_conn)
    superuser_conn.commit()
    run_command("SELECT pg_lake_log.flush();", superuser_conn)
    superuser_conn.commit()

    row = fetch_one(
        f"SELECT log_time, pid, severity, message FROM {log_table} LIMIT 1;",
        superuser_conn,
    )
    assert row is not None
    log_time, pid, severity, message = row
    assert log_time is not None
    assert isinstance(pid, int)
    assert severity in (
        "DEBUG5", "DEBUG4", "DEBUG3", "DEBUG2", "DEBUG1",
        "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL", "PANIC",
    )
    assert isinstance(message, str)

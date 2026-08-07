import pytest
import time
import psycopg2
import select
from utils_pytest import *


def test_connection_id_matches_stat_activity(s3, pgduck_conn):
    """
    pg_lake_connection_id returns the current session's connection
    identifier. That is the same value libpq reports as PQbackendPID, and the
    same value that shows up in the connection_id column of
    pg_lake_stat_activity while the session has an active query.
    """
    sleep_conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
        async_=1,
    )
    wait_for_connection(sleep_conn)

    # Read the id from the session itself first, so we have something to
    # compare the stat_activity row against. The sleep query below never
    # returns, so we cannot read it from there.
    sleep_cur = sleep_conn.cursor()
    sleep_cur.execute("SELECT pg_lake_connection_id()")
    wait_for_connection(sleep_conn)
    my_id = sleep_cur.fetchone()[0]

    # pgduck seeds the DuckDB-side connection id from the cancellation proc
    # id it sends in BackendKeyData, which is what libpq exposes here.
    assert my_id == sleep_conn.get_backend_pid()

    sleep_cur.execute("SELECT pg_lake_sleep(10)")

    try:
        observed_id = None
        while True:
            state = sleep_conn.poll()

            result = run_query(
                "SELECT connection_id FROM pg_lake_stat_activity() "
                "WHERE query LIKE '%pg_lake_sleep%' "
                "  AND query NOT LIKE '%stat_activity%'",
                pgduck_conn,
            )

            if len(result) == 1:
                observed_id = result[0][0]
                break

            if state == psycopg2.extensions.POLL_OK:
                pytest.fail("sleep query finished before stat_activity reported it")

            time.sleep(0.05)

        cancel_and_wait(sleep_conn, pgduck_conn)
    finally:
        try:
            sleep_conn.close()
        finally:
            pgduck_conn.rollback()

    assert observed_id == my_id


def test_query_progress_returns_active_query(s3, pgduck_conn):
    """
    pg_lake_query_progress(connection_id) returns one row carrying the
    executor's progress for the matching session. The numeric columns
    reflect whatever the executor has populated; for queries without a
    measurable scan they are -1 / 0 but the row still appears.
    """
    sleep_conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
        async_=1,
    )
    wait_for_connection(sleep_conn)

    sleep_query = "SELECT pg_lake_sleep(10)"
    sleep_cur = sleep_conn.cursor()
    sleep_cur.execute(sleep_query)

    try:
        sleep_connection_id = None
        progress_row = None
        while True:
            state = sleep_conn.poll()

            stat = run_query(
                "SELECT connection_id FROM pg_lake_stat_activity() "
                "WHERE query LIKE '%pg_lake_sleep%' "
                "  AND query NOT LIKE '%query_progress%'",
                pgduck_conn,
            )
            if len(stat) == 1:
                sleep_connection_id = stat[0][0]

                progress = run_query(
                    f"SELECT percentage, rows_processed, total_rows_to_process "
                    f"FROM pg_lake_query_progress({sleep_connection_id})",
                    pgduck_conn,
                )
                if len(progress) == 1:
                    progress_row = progress[0]
                    break

            if state == psycopg2.extensions.POLL_OK:
                pytest.fail("sleep query finished before progress was observed")

            time.sleep(0.05)

        cancel_and_wait(sleep_conn, pgduck_conn)
    finally:
        try:
            sleep_conn.close()
        finally:
            pgduck_conn.rollback()

    assert progress_row is not None
    percentage, rows_processed, total_rows = progress_row
    assert isinstance(percentage, float)
    assert rows_processed >= 0
    assert total_rows >= 0


def test_query_progress_unknown_id_returns_no_rows(s3, pgduck_conn):
    """
    pg_lake_query_progress(connection_id) returns no rows when no session
    matches the requested id. Connection ids come from the random int32 the
    server generates for the cancellation key, so rather than assuming a
    particular value is unused we pick one that is not currently reported by
    pg_lake_stat_activity. Both functions only consider sessions with an
    active query, so an id missing there cannot match here either.
    """
    active_ids = {
        row[0]
        for row in run_query(
            "SELECT connection_id FROM pg_lake_stat_activity()", pgduck_conn
        )
    }

    unused_id = -1
    while unused_id in active_ids:
        unused_id -= 1

    result = run_query(
        "SELECT percentage, rows_processed, total_rows_to_process "
        f"FROM pg_lake_query_progress({unused_id})",
        pgduck_conn,
    )
    assert result == []


def cancel_and_wait(sleep_conn, pgduck_conn):
    """
    Cancel the sleeping session and wait until the server stops reporting it.

    Closing the connection right after cancel() is not enough: the session can
    still be running its sleep when the next test starts, and a test that
    expects a single row in pg_lake_stat_activity then sees two. pg_lake_sleep
    checks for interrupts every 10ms, so the wait is short.
    """
    sleep_conn.cancel()

    for _ in range(600):
        remaining = run_query(
            "SELECT 1 FROM pg_lake_stat_activity() "
            "WHERE query LIKE '%pg_lake_sleep%' "
            "  AND query NOT LIKE '%stat_activity%'",
            pgduck_conn,
        )
        if not remaining:
            return

        time.sleep(0.05)

    pytest.fail("cancelled sleep session is still reported by pg_lake_stat_activity")


def wait_for_connection(conn):
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)

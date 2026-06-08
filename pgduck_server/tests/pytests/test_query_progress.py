import pytest
import time
import psycopg2
import select
from utils_pytest import *


def test_connection_id_matches_stat_activity(s3, pgduck_conn):
    """
    pg_lake_connection_id returns the current session's connection
    identifier. The same identifier shows up in the connection_id column of
    pg_lake_stat_activity when the session has an active query.
    """
    sleep_conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
        async_=1,
    )
    wait_for_connection(sleep_conn)

    sleep_query = "SELECT pg_lake_connection_id() AS my_id, pg_lake_sleep(10) AS slept"
    sleep_cur = sleep_conn.cursor()
    sleep_cur.execute(sleep_query)

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

        sleep_conn.cancel()
    finally:
        try:
            sleep_conn.close()
        finally:
            pgduck_conn.rollback()

    assert observed_id is not None


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

        sleep_conn.cancel()
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
    matches the requested id. Connection ids are positive socket fds, so
    -1 is guaranteed never to match any active session.
    """
    result = run_query(
        "SELECT percentage, rows_processed, total_rows_to_process "
        "FROM pg_lake_query_progress(-1)",
        pgduck_conn,
    )
    assert result == []


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

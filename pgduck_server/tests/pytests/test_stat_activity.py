import os
import pytest
import time
import psycopg2
import select
from utils_pytest import *


def test_stat_activity(s3, pgduck_conn):
    # Create an asynchronous connection for sleep
    sleep_conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
        async_=1,
    )

    wait_for_connection(sleep_conn)

    sleep_query = "SELECT pg_lake_sleep(10)"

    sleep_cur = sleep_conn.cursor()
    sleep_cur.execute(sleep_query)

    while True:
        state = sleep_conn.poll()

        result = run_query(
            "SELECT query FROM pg_lake_stat_activity() WHERE query NOT LIKE '%activity%'",
            pgduck_conn,
        )

        if len(result) == 1 and result[0][0] == sleep_query:
            break

        if state == psycopg2.extensions.POLL_OK:
            assert False
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [sleep_conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([sleep_conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)

        time.sleep(0.05)

    sleep_conn.cancel()
    sleep_conn.close()
    pgduck_conn.rollback()


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

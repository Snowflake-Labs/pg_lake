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


# Regression test for https://github.com/Snowflake-Labs/pg_lake/issues/148:
# running pg_lake_stat_activity() concurrently from several pgbench clients
# used to crash pgduck_server because StatActivityExec read another
# session's PgLakeQueryListener fields and called ClientContext::
# ExecutionIsFinished() across threads. Both reads now go through
# thread-safe paths, and the test fails if pgduck_server crashes (pgbench
# starts reporting connection errors and exits non-zero).
def test_stat_activity_concurrent_pgbench(s3, pgduck_server):
    file_path = "/tmp/stat_activity_concurrent.sql"
    with open(file_path, "w") as f:
        f.write("SELECT count(*) FROM pg_lake_stat_activity();\n")

    returncode, stdout, stderr = run_pgbench_command(
        [
            "-n",
            "-c",
            "4",
            "-j",
            "2",
            "-T",
            "5",
            "-f",
            file_path,
            "-h",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "-p",
            str(server_params.PGDUCK_PORT),
        ]
    )

    assert returncode == 0, (
        f"pgbench against pg_lake_stat_activity() failed; "
        f"stdout=\n{stdout}\nstderr=\n{stderr}"
    )
    assert "number of transactions actually processed" in stdout, stdout
    # All transactions must succeed; a crashed pgduck_server shows up as
    # "number of failed transactions" > 0 or a non-zero return code.
    assert "number of failed transactions: 0" in stdout, stdout

    # The server must still be reachable after the stress run.
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


# Companion repro for the second scenario described in issue #148: a
# single client looping over pg_lake_stat_activity() while a wider fleet
# of pgbench clients hammers pgduck_server with cheap queries. The
# stat_activity caller iterates ConnectionManager::GetConnectionList()
# while many sessions are racing through QueryBegin / QueryEnd, which
# was the original trigger okalaci observed.
def test_stat_activity_under_background_load(s3, pgduck_server):
    import subprocess

    background_sql = "/tmp/stat_activity_background.sql"
    foreground_sql = "/tmp/stat_activity_foreground.sql"
    with open(background_sql, "w") as f:
        f.write("SELECT 1;\n")
    with open(foreground_sql, "w") as f:
        f.write("SELECT count(*) FROM pg_lake_stat_activity();\n")

    background = subprocess.Popen(
        [
            "pgbench",
            "-n",
            "-c",
            "16",
            "-j",
            "4",
            "-T",
            "5",
            "-f",
            background_sql,
            "-h",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "-p",
            str(server_params.PGDUCK_PORT),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    returncode, stdout, stderr = run_pgbench_command(
        [
            "-n",
            "-c",
            "1",
            "-j",
            "1",
            "-T",
            "5",
            "-f",
            foreground_sql,
            "-h",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "-p",
            str(server_params.PGDUCK_PORT),
        ]
    )

    bg_stdout, bg_stderr = background.communicate()
    bg_returncode = background.returncode

    assert (
        returncode == 0
    ), f"foreground pgbench failed; stdout=\n{stdout}\nstderr=\n{stderr}"
    assert "number of failed transactions: 0" in stdout, stdout
    assert bg_returncode == 0, (
        f"background pgbench failed; stdout=\n{bg_stdout.decode()}\n"
        f"stderr=\n{bg_stderr.decode()}"
    )

    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

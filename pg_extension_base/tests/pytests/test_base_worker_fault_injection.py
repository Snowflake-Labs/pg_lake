import os
import psycopg2
import pytest
import time
from utils_pytest import *


# Long enough that the reclaim cannot fire by accident between two polls, short
# enough that the test does not wait out the 60s production default.
STARTUP_TIMEOUT_MS = 400

# Either recovery path may win the race below, so accept both messages: the
# postmaster publishes the child's pid before the child loads our library, so
# WaitForBackgroundWorkerStartup can report either BGWH_STOPPED (caught by the
# status check) or BGWH_STARTED (caught by the startup timeout).
RECOVERY_WARNINGS = ("appears to have never started", "did not start")


def wait_until(fn, timeout=15.0, interval=0.05):
    """Poll fn() until it returns a truthy value or `timeout` seconds elapse;
    return the last observed value.  Background workers act asynchronously, so a
    single fixed sleep before asserting their effect is racy under CI load."""
    value = fn()
    deadline = time.monotonic() + timeout
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = fn()
    return value


def log_end_offset():
    with open(f"{server_params.PG_DIR}/logfile", "r") as f:
        f.seek(0, os.SEEK_END)
        return f.tell()


def read_new_log(offset):
    with open(f"{server_params.PG_DIR}/logfile", "r") as f:
        f.seek(offset)
        return f.read()


def set_startup_timeout(conn, value):
    """Shrink worker_startup_timeout cluster-wide and reload, so the already
    running server starter picks it up on SIGHUP."""
    was_autocommit = conn.autocommit
    conn.rollback()
    conn.autocommit = True
    try:
        if value is None:
            run_command(
                "ALTER SYSTEM RESET pg_extension_base.worker_startup_timeout", conn
            )
        else:
            run_command(
                f"ALTER SYSTEM SET pg_extension_base.worker_startup_timeout = '{value}'",
                conn,
            )
        run_command("SELECT pg_reload_conf()", conn)
    finally:
        conn.autocommit = was_autocommit


def attach(conn, point):
    run_command(f"SELECT injection_points_attach('{point}', 'error')", conn)
    conn.commit()


def detach(conn, point):
    run_command(f"SELECT injection_points_detach('{point}')", conn, raise_error=False)
    conn.commit()


def database_starter_pid(conn, dbname):
    rows = run_query(
        f"""
        SELECT ds.pid
        FROM extension_base.list_database_starters() ds
        JOIN pg_database d ON d.oid = ds.database_id
        WHERE d.datname = '{dbname}'
        """,
        conn,
    )
    return rows[0]["pid"] if rows else None


def base_worker_pid(conn, extname):
    rows = run_query(
        f"""
        SELECT w.pid
        FROM extension_base.list_base_workers() w
        JOIN pg_extension e ON e.oid = w.extension_id
        WHERE e.extname = '{extname}'
        """,
        conn,
    )
    return rows[0]["pid"] if rows else None


def test_database_starter_recovers_from_launch_that_never_ran(
    superuser_conn, create_injection_extension
):
    """
    A database starter whose child dies before registering its exit handler used
    to leave list_database_starters() reporting pid=0 forever, with nothing in
    the log: that handler was the only thing that ever moved the entry out of
    WORKER_STARTING, so every later pass saw "already starting" and returned
    without doing anything.
    """
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    dbname = "fault_inject_db"
    point = "database-starter-before-exit-handler"

    # the introspection functions live in the extension's schema, so the
    # observing connection needs the extension too
    run_command("CREATE EXTENSION IF NOT EXISTS pg_extension_base", superuser_conn)
    superuser_conn.commit()

    set_startup_timeout(superuser_conn, f"{STARTUP_TIMEOUT_MS}ms")
    attach(superuser_conn, point)

    start_offset = log_end_offset()
    superuser_conn.autocommit = True
    try:
        run_command(f"CREATE DATABASE {dbname}", superuser_conn)

        # Give the new database the extension, so its starter has a reason to
        # exist and announces itself once it finally runs. A starter for a
        # database without pg_extension_base exits before logging anything.
        other_conn = psycopg2.connect(
            f"dbname={dbname} user={server_params.PG_USER} "
            f"password={server_params.PG_PASSWORD} port={server_params.PG_PORT} "
            f"host={server_params.PG_HOST}"
        )
        run_command("CREATE EXTENSION pg_extension_base", other_conn)
        other_conn.commit()
        other_conn.close()

        # confirm we reproduce the wedge: a launch was attempted for the new
        # database and the child never claimed a pid
        assert wait_until(
            lambda: f"starting pg base extension database starter in database {dbname}"
            in read_new_log(start_offset)
        ), "no launch was ever attempted for the new database"
        assert database_starter_pid(superuser_conn, dbname) == 0

        # let the child through, and watch it announce itself once the wedged
        # entry has been reclaimed and relaunched
        detach_offset = log_end_offset()
        superuser_conn.autocommit = False
        detach(superuser_conn, point)
        superuser_conn.autocommit = True

        assert wait_until(
            lambda: f"database starter for database {dbname} started"
            in read_new_log(detach_offset)
        ), "the wedged entry was never reclaimed and relaunched"

        log = read_new_log(start_offset)
        assert any(
            warning in log for warning in RECOVERY_WARNINGS
        ), "recovery happened without logging anything"
    finally:
        # a relaunched starter may still briefly hold a connection to it
        assert wait_until(
            lambda: run_command(
                f"DROP DATABASE IF EXISTS {dbname}", superuser_conn, raise_error=False
            )
            is None
        ), f"could not drop {dbname}"
        superuser_conn.autocommit = False
        detach(superuser_conn, point)
        set_startup_timeout(superuser_conn, None)


def test_base_worker_recovers_from_launch_that_never_ran(
    superuser_conn, create_injection_extension
):
    """
    Same failure mode for a base worker.  Note this one cannot be fixed inside
    StartBaseWorker: a database starter exits as soon as every launch it issued
    reported success, so once it is gone nothing would revisit the entry.  The
    server starter has to be the one that reclaims it.
    """
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    extname = "pg_extension_base_test_scheduler"
    point = "base-worker-before-exit-handler"

    set_startup_timeout(superuser_conn, f"{STARTUP_TIMEOUT_MS}ms")
    attach(superuser_conn, point)

    start_offset = log_end_offset()
    try:
        run_command(f"CREATE EXTENSION {extname} CASCADE", superuser_conn)
        superuser_conn.commit()

        # confirm we reproduce the wedge before checking that it recovers
        assert wait_until(
            lambda: base_worker_pid(superuser_conn, extname) == 0
        ), "the worker entry never reached pid=0"
        assert wait_until(
            lambda: "starting pg base extension worker" in read_new_log(start_offset)
        ), "no launch was ever attempted"

        detach(superuser_conn, point)

        assert wait_until(
            lambda: base_worker_pid(superuser_conn, extname)
        ), "the wedged entry was never reclaimed and relaunched"

        log = read_new_log(start_offset)
        assert any(
            warning in log for warning in RECOVERY_WARNINGS
        ), "recovery happened without logging anything"
    finally:
        detach(superuser_conn, point)
        run_command(f"DROP EXTENSION IF EXISTS {extname} CASCADE", superuser_conn)
        superuser_conn.commit()
        set_startup_timeout(superuser_conn, None)


def test_base_worker_recovers_when_postmaster_reports_no_child(
    superuser_conn, create_injection_extension
):
    """
    The other recovery path, and the one the incident that prompted all this
    actually took: the postmaster failed to fork the child ("could not fork
    background worker process: Cannot allocate memory") so
    WaitForBackgroundWorkerStartup reported it stopped, and the launcher used to
    throw that status away and leave the entry claiming a launch was in flight.

    A test cannot make fork() fail, so it kills the child and has the launcher
    report the status the failed fork would have produced. Deliberately leaves
    worker_startup_timeout at its default, so nothing but the status check can
    get the entry moving again.
    """
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("IS_INJECTION_POINT_ATTACHED requires PostgreSQL 18+")

    extname = "pg_extension_base_test_scheduler"
    child_point = "base-worker-before-exit-handler"
    status_point = "base-worker-launch-reported-stopped"

    attach(superuser_conn, child_point)
    attach(superuser_conn, status_point)

    start_offset = log_end_offset()
    try:
        run_command(f"CREATE EXTENSION {extname} CASCADE", superuser_conn)
        superuser_conn.commit()

        assert wait_until(
            lambda: "did not start" in read_new_log(start_offset)
        ), "the discarded startup status was never noticed"

        detach(superuser_conn, child_point)
        detach(superuser_conn, status_point)

        assert wait_until(
            lambda: base_worker_pid(superuser_conn, extname)
        ), "the entry was never retried after the launch was reported stopped"
    finally:
        detach(superuser_conn, child_point)
        detach(superuser_conn, status_point)
        run_command(f"DROP EXTENSION IF EXISTS {extname} CASCADE", superuser_conn)
        superuser_conn.commit()

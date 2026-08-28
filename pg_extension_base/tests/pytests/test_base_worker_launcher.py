import os
import pytest
import psycopg2
import select
import time
from utils_pytest import *


def wait_until_equal(fn, expected, timeout=5.0, interval=0.05):
    """Poll fn() until it equals `expected` or `timeout` seconds elapse; return
    the last observed value.  Background workers act asynchronously, so a single
    fixed sleep before asserting their effect is racy under CI load."""
    value = fn()
    deadline = time.monotonic() + timeout
    while value != expected and time.monotonic() < deadline:
        time.sleep(interval)
        value = fn()
    return value


def log_end_offset():
    with open(f"{server_params.PG_DIR}/logfile", "r") as f:
        f.seek(0, os.SEEK_END)
        return f.tell()


def read_new_log_lines(offset):
    with open(f"{server_params.PG_DIR}/logfile", "r") as f:
        f.seek(offset)
        return f.readlines()


def test_server_start(superuser_conn):
    result = get_pg_extension_workers(superuser_conn)
    assert result[0]["datname"] == None
    assert result[0]["application_name"] == "pg base extension server starter"


def test_create_drop_pg_extension_base_test_scheduler(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # drop the pg_extension_base_test_scheduler extension
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_create_deregister_worker(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister and abort (worker should restart)
    run_command(
        "SELECT extension_base.deregister_worker('pg_extension_base_test_scheduler_main_worker')",
        superuser_conn,
    )
    superuser_conn.rollback()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister and commit (worker gone)
    run_command(
        "SELECT extension_base.deregister_worker('pg_extension_base_test_scheduler_main_worker')",
        superuser_conn,
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_create_deregister_worker_id(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # get id from UDF
    worker_id = run_query(
        "SELECT worker_id FROM extension_base.workers WHERE worker_name = 'pg_extension_base_test_scheduler_main_worker'",
        superuser_conn,
    )[0][0]
    assert worker_id > 0

    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister by id and abort (worker should restart)
    run_command(
        f"SELECT extension_base.deregister_worker({worker_id})",
        superuser_conn,
    )
    superuser_conn.rollback()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister by id and commit (worker gone)
    run_command(
        f"SELECT extension_base.deregister_worker({worker_id})",
        superuser_conn,
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)


def test_deregister_worker_missing_ok_by_name(superuser_conn):
    # deregistering a nonexistent worker by name with missing_ok=true should not raise
    run_command(
        "SELECT extension_base.deregister_worker('nonexistent_worker_name', true)",
        superuser_conn,
    )
    superuser_conn.rollback()


def test_deregister_worker_missing_ok_by_id(superuser_conn):
    # deregistering a nonexistent worker by id with missing_ok=true should not raise
    run_command(
        "SELECT extension_base.deregister_worker(99999, true)",
        superuser_conn,
    )
    superuser_conn.rollback()


def test_deregister_worker_not_found_by_name_raises(superuser_conn):
    # deregistering a nonexistent worker by name without missing_ok should raise an error
    with pytest.raises(psycopg2.DatabaseError, match="could not find worker"):
        run_command(
            "SELECT extension_base.deregister_worker('nonexistent_worker_name')",
            superuser_conn,
        )
    superuser_conn.rollback()


def test_deregister_worker_not_found_by_id_raises(superuser_conn):
    # deregistering a nonexistent worker by id without missing_ok should raise an error
    with pytest.raises(psycopg2.DatabaseError, match="could not find worker"):
        run_command(
            "SELECT extension_base.deregister_worker(99999)",
            superuser_conn,
        )
    superuser_conn.rollback()


def test_deregister_worker_missing_ok_false_by_name_raises(superuser_conn):
    # deregistering a nonexistent worker with explicit missing_ok=false should raise
    with pytest.raises(psycopg2.DatabaseError, match="could not find worker"):
        run_command(
            "SELECT extension_base.deregister_worker('nonexistent_worker_name', false)",
            superuser_conn,
        )
    superuser_conn.rollback()


def test_deregister_worker_missing_ok_false_by_id_raises(superuser_conn):
    # deregistering a nonexistent worker by id with explicit missing_ok=false should raise
    with pytest.raises(psycopg2.DatabaseError, match="could not find worker"):
        run_command(
            "SELECT extension_base.deregister_worker(99999, false)",
            superuser_conn,
        )
    superuser_conn.rollback()


def test_create_drop_pg_extension_base(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # drop the pg_extension_base extension
    run_command("DROP EXTENSION pg_extension_base CASCADE", superuser_conn)
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_create_abort_pg_extension_base_test_scheduler(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )

    assert count_pg_extension_base_workers(superuser_conn) == 0

    # rollback does not result in base worker creation
    superuser_conn.rollback()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_drop_create_pg_extension_base_test_scheduler(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    time.sleep(0.1)

    # base worker is killed
    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )

    # transaction is not yet over, no base worker started
    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # cleanup
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_drop_create_pg_extension_base(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    run_command("DROP EXTENSION pg_extension_base CASCADE", superuser_conn)
    time.sleep(0.1)

    # base worker is killed
    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )

    # transaction is not yet over, no base worker started
    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # cleanup
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_create_drop_database(superuser_conn):
    superuser_conn.autocommit = True

    # create another database and then add the extension
    run_command("CREATE DATABASE other", superuser_conn)

    other_conn_str = f"dbname=other user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)

    run_command("CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)
    other_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    other_conn.close()

    run_command("DROP DATABASE other", superuser_conn)

    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.autocommit = False


def test_create_database_from_template(superuser_conn):
    superuser_conn.autocommit = True

    # Create the extension in template1
    template_conn_str = f"dbname=template1 user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    template_conn = psycopg2.connect(template_conn_str)
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", template_conn
    )
    template_conn.commit()
    template_conn.close()

    # create another database which already has the extension
    run_command("CREATE DATABASE other", superuser_conn)
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # drop the other database
    run_command("DROP DATABASE other", superuser_conn)
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 0

    # clean template1
    template_conn = psycopg2.connect(template_conn_str)
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", template_conn
    )
    template_conn.commit()
    template_conn.close()

    superuser_conn.autocommit = False


def test_failed_drop_database(superuser_conn):
    superuser_conn.autocommit = True

    # create another database and then add the extension
    run_command("CREATE DATABASE other", superuser_conn)

    # open a connection to other and keep it open
    other_conn_str = f"dbname=other user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)
    run_command("CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)
    other_conn.commit()

    # try to drop it from the original connection
    error = run_command("DROP DATABASE other", superuser_conn, raise_error=False)
    assert "being accessed by other user" in error

    time.sleep(0.1)

    # should still have the base worker
    assert count_pg_extension_base_workers(superuser_conn) == 1

    other_conn.close()

    # now actually drop it
    run_command("DROP DATABASE other", superuser_conn)

    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.autocommit = False


def count_other_backends_in_database(conn, datname, own_pid):
    query = f"""
        SELECT count(*) FROM pg_stat_activity
        WHERE datname = '{datname}' AND pid <> {own_pid}
    """
    return run_query(query, conn)[0]["count"]


def test_no_starter_connects_while_a_drop_holds_the_lock(superuser_conn):
    """A DROP that stops the workers of a database also blocks new database
    starters for it until it is done.

    Otherwise the server starter connects a database starter to a database that
    a concurrent DROP DATABASE is waiting to become idle. The starter then
    blocks on the same lock the DROP holds, and because it counts as a session
    in the database, the DROP fails with "database is being accessed by other
    users" once it stops waiting -- FORCE does not help, since the SIGTERM it
    sends only sets a flag that a starter waiting for a lock cannot act on.

    We hold the lock with an uncommitted DROP EXTENSION rather than with a
    DROP DATABASE, which cannot run inside a transaction block.
    """
    superuser_conn.autocommit = True

    run_command("CREATE DATABASE other", superuser_conn)

    other_conn_str = f"dbname=other user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)
    run_command("CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)
    other_conn.commit()

    assert (
        wait_until_equal(lambda: count_pg_extension_base_workers(superuser_conn), 1)
        == 1
    )

    other_pid = run_query("SELECT pg_backend_pid()", other_conn)[0]["pg_backend_pid"]

    # take the lock and mark the database starter for restart, without
    # committing, which is the state a DROP DATABASE is in while it waits
    run_command("DROP EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)

    # CREATE DATABASE wakes up the server starter, so it iterates over the
    # databases while we hold the lock rather than at its own pace
    run_command("CREATE DATABASE other_wakeup", superuser_conn)

    # no database starter may connect to other for as long as we hold the lock
    assert (
        wait_until_equal(
            lambda: count_other_backends_in_database(
                superuser_conn, "other", other_pid
            ),
            1,
            timeout=3.0,
        )
        == 0
    )

    # rolling back has to bring the worker back, so the back-off above may not
    # simply drop the pending restart
    other_conn.rollback()

    assert (
        wait_until_equal(lambda: count_pg_extension_base_workers(superuser_conn), 1)
        == 1
    )

    run_command("DROP EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)
    other_conn.commit()
    other_conn.close()

    run_command("DROP DATABASE other", superuser_conn)
    run_command("DROP DATABASE other_wakeup", superuser_conn)

    assert (
        wait_until_equal(lambda: count_pg_extension_base_workers(superuser_conn), 0)
        == 0
    )

    superuser_conn.autocommit = False


# The server starter logs the sleep it computed once per pass.
STARTER_SLEEP_LOG = "pg base extension server starter sleeping for"

# What we set worker_starter_sleep_time to, and the milliseconds an idle pass
# then has to report. Read as milliseconds the same value lands on the 100ms
# floor instead.
STARTER_SLEEP_SECONDS = 2
STARTER_SLEEP_MS = STARTER_SLEEP_SECONDS * 1000


def test_server_starter_sleeps_the_configured_seconds(superuser_conn):
    """worker_starter_sleep_time is a seconds GUC, so an idle server starter has
    to sleep 1000 times its value in milliseconds.

    Nothing else in the suite pins that conversion. The restart tests all rely on
    the wake signal, which keeps working whatever the poll interval is, so with
    the multiplication removed all 28 launcher tests still passed. Every value
    below the 100ms floor collapses onto the floor, which is close enough to the
    old busy poll that nothing noticed.

    The sleep the starter logs is the only place the interval is observable from
    outside, and an idle pass is the only state that reports the configured value:
    a pending restart deliberately pulls the sleep down to the floor.
    """
    superuser_conn.autocommit = True

    run_command("ALTER SYSTEM SET log_min_messages = debug2", superuser_conn)
    run_command(
        "ALTER SYSTEM SET pg_extension_base.worker_starter_sleep_time = "
        f"'{STARTER_SLEEP_SECONDS}s'",
        superuser_conn,
    )
    run_command("SELECT pg_reload_conf()", superuser_conn)

    try:
        offset = log_end_offset()

        # CREATE DATABASE signals the server starter, so it re-reads the GUC and
        # makes a pass now rather than at the end of the sleep it is already in
        run_command("CREATE DATABASE starter_sleep", superuser_conn)

        # the pass that launches the new database's starter may still report the
        # floor, so allow for a few passes before the idle one
        deadline = time.time() + 4 * STARTER_SLEEP_SECONDS
        sleeps = []

        while time.time() < deadline:
            sleeps = [
                line.split(STARTER_SLEEP_LOG)[1].strip()
                for line in read_new_log_lines(offset)
                if STARTER_SLEEP_LOG in line
            ]

            if f"{STARTER_SLEEP_MS} ms" in sleeps:
                break

            time.sleep(0.05)

        assert f"{STARTER_SLEEP_MS} ms" in sleeps, (
            f"no idle pass slept {STARTER_SLEEP_MS} ms for a "
            f"{STARTER_SLEEP_SECONDS}s worker_starter_sleep_time, only {sleeps}"
        )
    finally:
        run_command("DROP DATABASE IF EXISTS starter_sleep", superuser_conn)
        run_command(
            "ALTER SYSTEM RESET pg_extension_base.worker_starter_sleep_time",
            superuser_conn,
        )
        run_command("ALTER SYSTEM RESET log_min_messages", superuser_conn)
        run_command("SELECT pg_reload_conf()", superuser_conn)
        superuser_conn.autocommit = False


def test_drop_database_force_removes_a_starting_starter(
    superuser_conn, create_injection_extension
):
    # A starter that already connected but has not reached its main loop counts
    # as a session in the database and cannot act on a SIGTERM flag, so
    # DROP DATABASE ... WITH (FORCE) used to fail with "database is being
    # accessed by other users". It should terminate the starter instead.

    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    superuser_conn.rollback()
    superuser_conn.autocommit = True

    run_command(
        "SELECT injection_points_attach('database-starter-before-lock', 'wait')",
        superuser_conn,
    )

    own_pid = run_query("SELECT pg_backend_pid()", superuser_conn)[0]["pg_backend_pid"]

    try:
        # the server starter starts a database starter for every new database,
        # which then parks in the injection point
        run_command("CREATE DATABASE other", superuser_conn)

        assert (
            wait_until_equal(
                lambda: count_other_backends_in_database(
                    superuser_conn, "other", own_pid
                ),
                1,
            )
            == 1
        )

        run_command("DROP DATABASE other WITH (FORCE)", superuser_conn)
    finally:
        # release any starter still parked, so a failure above does not leave
        # one behind for the next test
        run_command(
            "SELECT injection_points_wakeup('database-starter-before-lock')",
            superuser_conn,
            raise_error=False,
        )
        run_command(
            "SELECT injection_points_detach('database-starter-before-lock')",
            superuser_conn,
            raise_error=False,
        )
        run_command(
            "DROP DATABASE IF EXISTS other WITH (FORCE)",
            superuser_conn,
            raise_error=False,
        )

    assert (
        wait_until_equal(lambda: count_pg_extension_base_workers(superuser_conn), 0)
        == 0
    )

    superuser_conn.autocommit = False


def test_oneshot_worker_completes(superuser_conn):
    run_command("LISTEN oneshot", superuser_conn)
    superuser_conn.commit()

    run_command(
        "CREATE EXTENSION pg_extension_base_test_oneshot CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.3)

    # if the extension ran, we got the the notify before we deregistered
    if select.select([superuser_conn], [], [], 5) == ([], [], []):
        pytest.fail("Timeout: Did not receive notification from extension")
    else:
        superuser_conn.poll()  # Pull the data from the socket into Python objects
        seen = False
        while superuser_conn.notifies:
            notify = superuser_conn.notifies.pop(0)
            assert notify.channel == "oneshot"
            assert notify.payload != "0"
            seen = True

        assert seen, "got our notification proving the worker ran"

    # Worker starts, calls DeregisterBaseWorkerSelf(), and exits -- count drops to 0
    assert count_pg_extension_base_workers(superuser_conn) == 0

    # Wait longer and verify the worker is not restarted
    time.sleep(0.3)
    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.rollback()
    run_command("DROP EXTENSION pg_extension_base_test_oneshot CASCADE", superuser_conn)
    superuser_conn.commit()


def test_oneshot_worker_catalog_row_removed(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_oneshot CASCADE", superuser_conn
    )
    superuser_conn.commit()

    # The oneshot worker starts asynchronously, runs to completion, and removes
    # its own catalog row.  Poll for the row to disappear rather than asserting
    # after a single fixed sleep, which is racy under CI load.
    def catalog_row_count():
        return run_query(
            "SELECT count(*) FROM extension_base.workers WHERE worker_name = 'pg_extension_base_test_oneshot_main_worker'",
            superuser_conn,
        )[0][0]

    worker_count = wait_until_equal(catalog_row_count, 0)
    assert worker_count == 0

    run_command("DROP EXTENSION pg_extension_base_test_oneshot CASCADE", superuser_conn)
    superuser_conn.commit()


def test_oneshot_worker_drop_database(superuser_conn):
    # A predecessor that failed mid-transaction can leave this shared connection
    # with an open transaction, which would make the autocommit switch below
    # raise "set_session cannot be used inside a transaction".  Roll back first
    # so the switch is always valid.
    superuser_conn.rollback()
    superuser_conn.autocommit = True

    run_command("CREATE DATABASE other_oneshot", superuser_conn)

    other_conn_str = f"dbname=other_oneshot user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)

    run_command("CREATE EXTENSION pg_extension_base_test_oneshot CASCADE", other_conn)
    other_conn.commit()
    time.sleep(0.1)

    other_conn.close()

    run_command("DROP DATABASE other_oneshot", superuser_conn)

    # DROP DATABASE terminates the worker asynchronously; poll for the count to
    # settle rather than asserting after a single fixed sleep.
    worker_count = wait_until_equal(
        lambda: count_pg_extension_base_workers(superuser_conn), 0
    )
    assert worker_count == 0

    superuser_conn.autocommit = False


def test_get_worker_pid_running(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    worker_id = run_query(
        "SELECT worker_id FROM extension_base.workers WHERE worker_name = 'pg_extension_base_test_scheduler_main_worker'",
        superuser_conn,
    )[0][0]

    pid = run_query(
        f"SELECT extension_base.get_worker_pid({worker_id})",
        superuser_conn,
    )[0][0]
    assert pid > 0

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_get_worker_pid_not_found(superuser_conn):
    pid = run_query(
        "SELECT extension_base.get_worker_pid(99999)",
        superuser_conn,
    )[
        0
    ][0]
    assert pid == 0
    superuser_conn.rollback()


def get_pg_extension_workers(conn):
    query = "SELECT datname, application_name FROM pg_stat_activity WHERE backend_type LIKE 'pg_base_extension server %' OR backend_type LIKE 'pg base extension%' ORDER BY application_name"
    result = run_query(query, conn)
    return result


def count_pg_extension_base_workers(conn):
    query = "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg base extension worker'"
    result = run_query(query, conn)
    return result[0]["count"]


def test_hibernate_worker_restart_delay(superuser_conn):
    """
    Test that workers can hibernate and restart after a delay.

    The hibernate test worker:
    - Runs for 5 seconds
    - Exits and requests restart in 5 seconds
    - Repeats

    This test monitors worker count over time to verify the cycling behavior.
    """
    run_command(
        "CREATE EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    # Give the database starter time to start the worker
    time.sleep(0.5)

    # Monitor worker count over 12 seconds
    # We expect to see transitions: 1 -> 0 -> 1
    # Worker runs for 5s, hibernates for 5s, runs again
    counts = []
    for _ in range(24):  # 12 seconds total, check every 0.5s
        counts.append(count_pg_extension_base_workers(superuser_conn))
        time.sleep(0.5)

    # Verify we saw the worker running at some point
    assert 1 in counts, f"Worker should have been running at some point: {counts}"

    # Verify we saw the worker NOT running at some point (hibernating)
    assert 0 in counts, f"Worker should have been hibernating at some point: {counts}"

    # Verify at least one transition from running to not running
    transitions = sum(1 for i in range(len(counts) - 1) if counts[i] != counts[i + 1])
    assert (
        transitions >= 1
    ), f"Expected at least one transition, got {transitions}: {counts}"

    # Clean up
    run_command(
        "DROP EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def hibernate_worker_failure_count(conn):
    """
    Return the restart-backoff failure count for the hibernate test worker, or
    None if it is not currently registered.
    """
    rows = run_query(
        """
        SELECT w.failure_count
        FROM extension_base.list_base_workers() w
        JOIN pg_extension e ON e.oid = w.extension_id
        WHERE e.extname = 'pg_extension_base_test_hibernate'
        """,
        conn,
    )
    return rows[0]["failure_count"] if rows else None


def set_backoff_gucs(
    conn, initial_ms, max_ms, healthy_ms, fail_after_ms, starter_sleep=None
):
    """
    Configure the restart-backoff GUCs and the hibernate worker's fault
    injection, then reload so freshly (re)started workers pick them up.

    fail_after lives in the hibernate test library, so LOAD it into this
    backend first to make the placeholder known to ALTER SYSTEM.

    starter_sleep overrides worker_starter_sleep_time, for tests that need
    restarts to happen without help from the starter poll.
    """
    # End any transaction a prior test left open; autocommit can't be toggled
    # while one is in progress.
    conn.rollback()
    conn.autocommit = True
    try:
        run_command("LOAD 'pg_extension_base_test_hibernate'", conn)
        run_command(
            f"ALTER SYSTEM SET pg_extension_base.worker_restart_backoff_initial = '{initial_ms}ms'",
            conn,
        )
        run_command(
            f"ALTER SYSTEM SET pg_extension_base.worker_restart_backoff_max = '{max_ms}ms'",
            conn,
        )
        run_command(
            f"ALTER SYSTEM SET pg_extension_base.worker_restart_healthy_time = '{healthy_ms}ms'",
            conn,
        )
        run_command(
            f"ALTER SYSTEM SET pg_extension_base_test_hibernate.fail_after = '{fail_after_ms}ms'",
            conn,
        )
        if starter_sleep is not None:
            run_command(
                f"ALTER SYSTEM SET pg_extension_base.worker_starter_sleep_time = '{starter_sleep}'",
                conn,
            )
        run_command("SELECT pg_reload_conf()", conn)
    finally:
        conn.autocommit = False


def reset_backoff_gucs(conn):
    conn.rollback()
    conn.autocommit = True
    try:
        run_command(
            "ALTER SYSTEM RESET pg_extension_base.worker_restart_backoff_initial", conn
        )
        run_command(
            "ALTER SYSTEM RESET pg_extension_base.worker_restart_backoff_max", conn
        )
        run_command(
            "ALTER SYSTEM RESET pg_extension_base.worker_restart_healthy_time", conn
        )
        run_command(
            "ALTER SYSTEM RESET pg_extension_base_test_hibernate.fail_after", conn
        )
        run_command(
            "ALTER SYSTEM RESET pg_extension_base.worker_starter_sleep_time", conn
        )
        run_command("SELECT pg_reload_conf()", conn)
    finally:
        conn.autocommit = False


def test_worker_restart_backoff_escalates(superuser_conn):
    """
    A worker that keeps failing at startup should have its failure count climb
    while the exponential backoff throttles the restart rate.

    fail_after=0 makes the worker error out immediately (so it never stays up
    long enough to be considered healthy), and healthy_time is large so the
    failure counter is never reset.
    """
    set_backoff_gucs(
        superuser_conn, initial_ms=100, max_ms=400, healthy_ms=30000, fail_after_ms=0
    )

    run_command(
        "CREATE EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    try:
        # Poll for up to ~5s until the failure count escalates.
        counts = []
        for _ in range(20):
            time.sleep(0.25)
            fc = hibernate_worker_failure_count(superuser_conn)
            if fc is not None:
                counts.append(fc)
            if counts and counts[-1] >= 3:
                break

        assert counts, "hibernate worker was never registered"

        # The counter escalated, proving repeated failures are being tracked.
        assert max(counts) >= 3, f"failure count did not escalate: {counts}"

        # With a >=100ms backoff that doubles up to 400ms, the worker restarts a
        # handful of times per second at most. Without any delay it would crash
        # loop hundreds of times in the same window, so a modest ceiling proves
        # the restart is actually being throttled.
        assert max(counts) < 100, f"restarts were not throttled: {counts}"
    finally:
        run_command(
            "DROP EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
        )
        superuser_conn.commit()
        reset_backoff_gucs(superuser_conn)


def test_worker_restart_does_not_wait_for_the_starter_poll(superuser_conn):
    """
    A failing worker comes back on its own backoff, not on the starter poll
    interval, because the starters are woken when a restart falls due.

    worker_starter_sleep_time is an hour here, far past the test window, so a
    restart that needed the next poll would never happen. The sibling test above
    cannot tell the two apart: at the default poll it would also catch a missing
    wake, but it would stop catching it the moment the default got shorter,
    which is how the missing wake stayed hidden in the first place.
    """
    set_backoff_gucs(
        superuser_conn,
        initial_ms=100,
        max_ms=400,
        healthy_ms=30000,
        fail_after_ms=0,
        starter_sleep="1h",
    )

    run_command(
        "CREATE EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    try:
        counts = []
        for _ in range(32):  # ~8s
            time.sleep(0.25)
            fc = hibernate_worker_failure_count(superuser_conn)
            if fc is not None:
                counts.append(fc)
            if counts and counts[-1] >= 3:
                break

        assert counts, "hibernate worker was never registered"
        assert max(counts) >= 3, f"failure count did not escalate: {counts}"
    finally:
        run_command(
            "DROP EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
        )
        superuser_conn.commit()
        reset_backoff_gucs(superuser_conn)


def test_worker_restart_backoff_resets_after_healthy_uptime(superuser_conn):
    """
    A worker that stays up longer than the healthy threshold before failing
    should not accumulate backoff: each failure is treated as fresh, so the
    failure count never climbs past 1.
    """
    # fail_after (600ms) > healthy_time (300ms): every run is "healthy".
    set_backoff_gucs(
        superuser_conn, initial_ms=100, max_ms=400, healthy_ms=300, fail_after_ms=600
    )

    run_command(
        "CREATE EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    try:
        counts = []
        for _ in range(12):  # ~4.8s, several fail/restart cycles
            time.sleep(0.4)
            fc = hibernate_worker_failure_count(superuser_conn)
            if fc is not None:
                counts.append(fc)

        assert counts, "hibernate worker was never registered"

        # We should have observed at least one failure (count reaching 1) but it
        # must never escalate, because the healthy-uptime reset fires each time.
        assert max(counts) <= 1, f"backoff did not reset after healthy uptime: {counts}"
        assert 1 in counts, f"worker never failed as expected: {counts}"
    finally:
        run_command(
            "DROP EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
        )
        superuser_conn.commit()
        reset_backoff_gucs(superuser_conn)


def test_database_starter_lock_is_outside_the_database_lock_space(superuser_conn):
    """
    The server starter takes the database starter lock while connected to no
    database, so the lock belongs to no database's advisory lock space.  A
    session listing the advisory locks of its own database has to find only the
    ones it took itself, because that is what PostgreSQL's own advisory_lock
    regression test asserts about the database it runs in.
    """
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()

    # deregister_worker takes the same lock and holds it until the transaction
    # ends, so there is no need to catch a starter in the act
    run_command(
        "SELECT extension_base.deregister_worker('pg_extension_base_test_scheduler_main_worker')",
        superuser_conn,
    )

    assert [[1, 0]] == run_query(
        """
        SELECT count(*),
               count(*) FILTER (
                   WHERE database = (SELECT oid FROM pg_database
                                     WHERE datname = current_database()))
        FROM pg_locks
        WHERE locktype = 'advisory' AND pid = pg_backend_pid()
        """,
        superuser_conn,
    )

    superuser_conn.rollback()

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()

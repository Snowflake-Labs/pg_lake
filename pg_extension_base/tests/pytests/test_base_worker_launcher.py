import pytest
import psycopg2
import time
from utils_pytest import *
import server_params


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


def test_wakeup_worker(superuser_conn):
    """
    Test that a stopped worker can be woken up.

    With no_restart mode enabled, the hibernate test worker:
    - Runs for 2 seconds
    - Stops (returns 0, no automatic restart)

    We verify:
    1. The worker starts and runs
    2. The worker stops (no longer running)
    3. After calling wakeup_worker, the worker restarts
    """
    # no_restart is PGC_SIGHUP, so we need ALTER SYSTEM + reload
    # ALTER SYSTEM cannot run inside a transaction block
    superuser_conn.rollback()
    superuser_conn.autocommit = True
    run_command(
        "ALTER SYSTEM SET pg_extension_base_test_hibernate.no_restart TO on",
        superuser_conn,
    )
    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.autocommit = False

    run_command(
        "CREATE EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    # Give the database starter time to start the worker
    time.sleep(0.5)

    # Worker should be running
    assert count_pg_extension_base_workers(superuser_conn) == 1

    # Wait for the worker to finish (runs for 2 seconds) and stop
    time.sleep(3)

    # Worker should be stopped (not running)
    assert count_pg_extension_base_workers(superuser_conn) == 0

    # Wake up the worker by name
    run_command(
        "SELECT extension_base.wakeup_worker('pg_extension_base_test_hibernate_main_worker')",
        superuser_conn,
    )
    superuser_conn.commit()

    # Give the database starter time to restart the worker
    time.sleep(1)

    # Worker should be running again
    assert count_pg_extension_base_workers(superuser_conn) == 1

    # Wait for the worker to finish and stop again
    time.sleep(3)

    # Worker should be stopped again
    assert count_pg_extension_base_workers(superuser_conn) == 0

    # Wake up using worker_id this time
    worker_id = run_query(
        "SELECT worker_id FROM extension_base.workers WHERE worker_name = 'pg_extension_base_test_hibernate_main_worker'",
        superuser_conn,
    )[0][0]
    run_command(
        f"SELECT extension_base.wakeup_worker({worker_id}::int)",
        superuser_conn,
    )
    superuser_conn.commit()

    time.sleep(1)

    # Worker should be running again
    assert count_pg_extension_base_workers(superuser_conn) == 1

    # Clean up
    run_command(
        "DROP EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    superuser_conn.autocommit = True
    run_command(
        "ALTER SYSTEM RESET pg_extension_base_test_hibernate.no_restart",
        superuser_conn,
    )
    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.autocommit = False

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_wakeup_delayed_worker(superuser_conn):
    """
    Test that wakeup_worker can wake up a worker that is in a delayed restart
    (restart after N milliseconds), causing it to restart immediately.
    """
    run_command(
        "CREATE EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    # Give the database starter time to start the worker
    time.sleep(0.5)

    # Worker should be running
    assert count_pg_extension_base_workers(superuser_conn) == 1

    # Wait for the worker to finish (runs for 5 seconds) and enter delayed restart
    time.sleep(6)

    # Worker should be in delayed restart (5 second delay, not yet elapsed)
    assert count_pg_extension_base_workers(superuser_conn) == 0

    # Wake it up early
    run_command(
        "SELECT extension_base.wakeup_worker('pg_extension_base_test_hibernate_main_worker')",
        superuser_conn,
    )
    superuser_conn.commit()

    # Give the database starter time to restart the worker
    time.sleep(1)

    # Worker should be running again (woken up early)
    assert count_pg_extension_base_workers(superuser_conn) == 1

    # Clean up
    run_command(
        "DROP EXTENSION pg_extension_base_test_hibernate CASCADE", superuser_conn
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0

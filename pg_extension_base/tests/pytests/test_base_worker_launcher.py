import pytest
import psycopg2
import select
import time
from utils_pytest import *


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
    time.sleep(0.1)

    # After completion the catalog row is gone -- no workers registered
    worker_count = run_query(
        "SELECT count(*) FROM extension_base.workers WHERE worker_name = 'pg_extension_base_test_oneshot_main_worker'",
        superuser_conn,
    )[0][0]
    assert worker_count == 0

    run_command("DROP EXTENSION pg_extension_base_test_oneshot CASCADE", superuser_conn)
    superuser_conn.commit()


def test_oneshot_worker_drop_database(superuser_conn):
    superuser_conn.autocommit = True

    run_command("CREATE DATABASE other_oneshot", superuser_conn)

    other_conn_str = f"dbname=other_oneshot user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)

    run_command("CREATE EXTENSION pg_extension_base_test_oneshot CASCADE", other_conn)
    other_conn.commit()
    time.sleep(0.1)

    other_conn.close()

    run_command("DROP DATABASE other_oneshot", superuser_conn)

    assert count_pg_extension_base_workers(superuser_conn) == 0

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
    )[0][0]
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

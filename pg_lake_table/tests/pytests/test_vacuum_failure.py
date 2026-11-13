import psycopg2
import pytest
import threading
import time
from utils_pytest import *


injection_point_names = [
    ("compact-files-before-snapshot", "error"),
    ("compact-files-after-snapshot", "error"),
    ("compact-files-before-try", "error"),
    ("compact-files-before-compact", "warn"),
    ("compact-files-after-compact", "warn"),
    ("compact-files-after-tx", "fatal"),
    ("expire-snapshots", "warn"),
    ("deletion-queue", "warn"),
    ("in-progress-files", "warn"),
]


@pytest.mark.parametrize("injection_point_name, expected_state", injection_point_names)
def test_vacuum_failure(
    pg_conn,
    superuser_conn,
    extension,
    s3,
    create_injection_extension,
    with_default_location,
    injection_point_name,
    expected_state,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    run_command(
        f"""
		CREATE SCHEMA \"{injection_point_name}\";
		SET search_path TO \"{injection_point_name}\";
		SELECT public.injection_points_attach('{injection_point_name}', 'error');

		CREATE TABLE t_iceberg(a int) USING iceberg;
		INSERT INTO t_iceberg VALUES (1);
		INSERT INTO t_iceberg VALUES (2);
		INSERT INTO t_iceberg VALUES (3);
		
		""",
        pg_conn,
    )

    pg_conn.commit()

    expected_error = f"error triggered for injection point {injection_point_name}"
    if expected_state == "error":
        error = run_command_outside_tx(
            [f'VACUUM "{injection_point_name}".t_iceberg'], raise_error=False
        )
        assert expected_error in error
    elif expected_state == "warn":
        pg_conn.autocommit = True

        # Clear any existing notices
        pg_conn.notices.clear()

        run_command(f'VACUUM "{injection_point_name}".t_iceberg', pg_conn)
        assert any(expected_error in line for line in pg_conn.notices)
        pg_conn.autocommit = False
    elif expected_state == "fatal":

        error = run_command_outside_tx(
            [f'VACUUM "{injection_point_name}".t_iceberg'], raise_error=False
        )

    else:
        assert False, "programming error"

    pg_conn.rollback()

    # sanity check afterwards
    run_command(f'INSERT INTO  "{injection_point_name}".t_iceberg VALUES(4)', pg_conn)
    res = run_query(
        f'SELECT count(*) FROM  "{injection_point_name}".t_iceberg', pg_conn
    )

    assert res[0][0] == 4

    run_command(
        f"""
		SELECT public.injection_points_detach('{injection_point_name}');

		DROP SCHEMA \"{injection_point_name}\" CASCADE """,
        pg_conn,
    )


def test_vacuum_table_dropped_between_in_progress_passes(
    pg_conn,
    superuser_conn,
    extension,
    s3,
    create_injection_extension,
    with_default_location,
):
    """
    Removing a table's in-progress files commits after each pass, so the
    table can be dropped before the next pass looks up its location.  That
    pass skips the table instead of failing on it.
    """
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("injection points 'wait' requires PostgreSQL 17+")

    run_command(
        """
        CREATE SCHEMA vacuum_dropped_in_progress;
        CREATE TABLE vacuum_dropped_in_progress.t (a int) USING iceberg
        WITH (autovacuum_enabled = 'False');
        INSERT INTO vacuum_dropped_in_progress.t VALUES (1);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # local to this session, so the autovacuum worker never waits on it
    vacuum_conn = open_pg_conn()
    vacuum_conn.autocommit = True
    run_command(
        """
        SELECT injection_points_set_local();
        SELECT injection_points_attach('in-progress-files', 'wait');
        """,
        vacuum_conn,
    )
    vacuum_pid = vacuum_conn.get_backend_pid()

    vacuum_errors = []

    def vacuum():
        try:
            run_command("VACUUM vacuum_dropped_in_progress.t", vacuum_conn)
        except psycopg2.Error as error:
            vacuum_errors.append(error)

    vacuum_thread = threading.Thread(target=vacuum)
    vacuum_thread.start()

    try:
        deadline = time.monotonic() + 60
        while True:
            wait_event = run_query(
                f"SELECT wait_event FROM pg_stat_activity WHERE pid = {vacuum_pid}",
                superuser_conn,
            )[0][0]
            superuser_conn.commit()
            if wait_event == "in-progress-files":
                break
            assert time.monotonic() < deadline, "VACUUM never reached the wait"
            time.sleep(0.1)

        # the first pass has not looked up the location yet
        run_command("SET lock_timeout = '10s'", pg_conn)
        run_command("DROP TABLE vacuum_dropped_in_progress.t", pg_conn)
        pg_conn.commit()
    finally:
        run_command(
            "SELECT injection_points_wakeup('in-progress-files')", superuser_conn
        )
        superuser_conn.commit()
        vacuum_thread.join(timeout=60)

    assert vacuum_errors == []
    assert not [n for n in vacuum_conn.notices if n.startswith("WARNING")]

    vacuum_conn.close()
    run_command("DROP SCHEMA vacuum_dropped_in_progress CASCADE", pg_conn)
    pg_conn.commit()


def test_vacuum_without_s3_access(
    s3_server, pg_conn, superuser_conn, pgduck_conn, extension, with_default_location
):
    run_command(
        f"""
        CREATE SCHEMA test_vacuum_without_s3_access;

        CREATE TABLE test_vacuum_without_s3_access.test_drop_iceberg USING iceberg
        WITH (autovacuum_enabled='False')
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
    INSERT INTO test_vacuum_without_s3_access.test_drop_iceberg
            SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(11, 20) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
    INSERT INTO test_vacuum_without_s3_access.test_drop_iceberg
            SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(21, 30) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # stop the boto server
    s3_server.stop()

    # now, INSERT fails
    result = run_command(
        "INSERT INTO test_vacuum_without_s3_access.test_drop_iceberg VALUES (1)",
        pg_conn,
        raise_error=False,
    )
    assert "Could not connect to server error" in result
    pg_conn.rollback()

    # first, show that there are already items in the deletion queue
    results = run_query(
        "SELECT * FROM lake_engine.deletion_queue WHERE table_name = 'test_vacuum_without_s3_access.test_drop_iceberg'::regclass",
        pg_conn,
    )

    assert len(results) > 0

    # now, call VACUUM for this table, and check the retry count
    test_retry_range = 2

    run_command(
        f"SET pg_lake_engine.vacuum_file_remove_max_retries TO {test_retry_range}",
        superuser_conn,
    )
    run_command("SET pg_lake_iceberg.max_snapshot_age TO '0';", superuser_conn)
    run_command(
        "SET pg_lake_engine.orphaned_file_retention_period TO '0s';",
        superuser_conn,
    )
    # a failed path is otherwise left alone until the retry interval elapses, so
    # the back-to-back VACUUMs below would claim nothing after the first one
    run_command(
        "SET pg_lake_engine.vacuum_file_remove_retry_interval TO 0;",
        superuser_conn,
    )

    superuser_conn.commit()
    for retry in range(1, test_retry_range + 1):
        superuser_conn.autocommit = True

        results = run_command(
            "SELECT * FROM lake_engine.deletion_queue WHERE table_name = 'test_vacuum_without_s3_access.test_drop_iceberg'::regclass",
            superuser_conn,
        )

        run_command(
            "VACUUM test_vacuum_without_s3_access.test_drop_iceberg", superuser_conn
        )

        results = run_query(
            "SELECT retry_count FROM lake_engine.deletion_queue WHERE table_name = 'test_vacuum_without_s3_access.test_drop_iceberg'::regclass",
            superuser_conn,
        )
        if retry < test_retry_range:
            assert len(results) > 0
            assert all(row[0] == retry for row in results)
        elif retry == test_retry_range:
            # we passed the retry count, so not removing entries anymore
            assert len(results) > 0
            assert all(row[0] == test_retry_range for row in results)

    # for the rest of the tests, re-start the boto
    s3_server.start()

    # now, show that we still have some entries in the deletion queue for this table
    # this is similar to the check above elif retry == test_retry_range
    # but we do it here as well for completeness
    results = run_query(
        "SELECT retry_count FROM lake_engine.deletion_queue WHERE table_name = 'test_vacuum_without_s3_access.test_drop_iceberg'::regclass",
        superuser_conn,
    )
    assert len(results) > 0
    assert all(row[0] == test_retry_range for row in results)

    # now, if we increment the retry count, we should be able to remove the entries
    run_command(
        f"SET pg_lake_engine.vacuum_file_remove_max_retries TO {test_retry_range + 1}",
        superuser_conn,
    )
    run_command(
        "VACUUM test_vacuum_without_s3_access.test_drop_iceberg", superuser_conn
    )

    results = run_query(
        "SELECT retry_count FROM lake_engine.deletion_queue WHERE table_name = 'test_vacuum_without_s3_access.test_drop_iceberg'::regclass",
        superuser_conn,
    )
    assert len(results) == 0

    superuser_conn.autocommit = False
    run_command("RESET pg_lake_engine.vacuum_file_remove_max_retries", superuser_conn)
    run_command("RESET pg_lake_iceberg.max_snapshot_age;", superuser_conn)
    run_command("RESET pg_lake_engine.orphaned_file_retention_period;", superuser_conn)
    run_command(
        "RESET pg_lake_engine.vacuum_file_remove_retry_interval;", superuser_conn
    )
    run_command("RESET client_min_messages;", superuser_conn)
    run_command("DROP SCHEMA test_vacuum_without_s3_access CASCADE;", superuser_conn)
    superuser_conn.commit()

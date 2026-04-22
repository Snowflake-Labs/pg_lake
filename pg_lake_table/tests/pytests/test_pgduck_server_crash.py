"""Regression test: pgduck_server crash during WaitForResult must not crash PostgreSQL.

Before the fix, WaitForResult called ReleasePGDuckConnection() before
re-throwing the error when PQconsumeInput() returned false (broken
connection).  The PG_FINALLY block in the FDW scan then called
ReleasePGDuckConnection() a second time on the same (already freed or
reused) hash entry, causing a double-free and SIGSEGV in the backend
— which crashed the whole PostgreSQL cluster.

The fix removes the premature ReleasePGDuckConnection call from
WaitForResult so connection lifetime is always managed exclusively by the
caller's PG_FINALLY block.

The test uses the 'pgduck-wait-for-result' injection point (in the
WaitForResult poll loop in client.c) to pause a backend after it has sent
a query to pgduck_server but before it calls PQconsumeInput().  The test
kills pgduck_server while the backend is paused, then wakes it.
PQconsumeInput() detects the broken connection and throws an ERROR.  With
the fix the error propagates cleanly; without the fix the backend
SIGSEGV-crashes the entire PostgreSQL instance.
"""

import threading
import time
from pathlib import Path

import pytest
from helpers.server import _pgduck_servers
from utils_pytest import *

INJECTION_POINT = "pgduck-wait-for-result"
SCHEMA = "test_pgduck_crash_nsp"


def _wait_for_backend_at_injection_point(superuser_conn, timeout_seconds=30):
    """Poll pg_stat_activity until a backend is paused at our injection point."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        superuser_conn.rollback()
        rows = run_query(
            f"""
            SELECT count(*) AS n FROM pg_stat_activity
            WHERE wait_event_type = 'InjectionPoint'
              AND wait_event = '{INJECTION_POINT}'
            """,
            superuser_conn,
        )
        if int(rows[0]["n"]) > 0:
            return True
        time.sleep(0.05)
    return False


def _kill_pgduck_server():
    """Kill pgduck_server via its PID file and remove stale socket files."""
    stop_process_via_pidfile(server_params.PGDUCK_PID_FILE, timeout=10)
    for suffix in ("", ".lock"):
        p = Path(
            f"{server_params.PGDUCK_UNIX_DOMAIN_PATH}"
            f"/.s.PGSQL.{server_params.PGDUCK_PORT}{suffix}"
        )
        p.unlink(missing_ok=True)


def _restart_pgduck_server():
    """Restart pgduck_server without handing ownership to the per-test cleanup.

    setup_pgduck_server() appends the new PgDuckServer instance to
    _pgduck_servers.  The function-scoped cleanup_test_servers autouse fixture
    kills every instance added during the current test — which would leave the
    server dead for all subsequent tests in the session.  Removing the instance
    from the list after creation transfers ownership back to the session, so the
    server stays alive until the session ends.
    """
    server = setup_pgduck_server()
    try:
        _pgduck_servers.remove(server)
    except ValueError:
        pass
    return server


def test_pgduck_crash_mid_query_no_sigsegv(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_injection_extension,
    pgduck_server,
):
    """pgduck_server crash during WaitForResult must not crash PostgreSQL.

    Regression for the double-free in WaitForResult: PQconsumeInput() failure
    used to cause WaitForResult to call ReleasePGDuckConnection() before
    throwing, then PG_FINALLY in the FDW scan called it again on the same
    entry -> double-free -> SIGSEGV -> whole PostgreSQL cluster crash.
    """
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("injection_points 'wait' requires PG17+")

    parquet_url = f"s3://{TEST_BUCKET}/test_pgduck_crash/data.parquet"

    # Write a small Parquet file so the FDW scan has something to read.
    run_command(
        f"COPY (SELECT s AS id FROM generate_series(1, 1000) s) TO '{parquet_url}'",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", pg_conn)
    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.t (id int)
        SERVER pg_lake OPTIONS (format 'parquet', path '{parquet_url}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Attach the injection point so the next FDW scan pauses inside WaitForResult.
    run_command(
        f"SELECT injection_points_attach('{INJECTION_POINT}', 'wait')",
        superuser_conn,
    )
    superuser_conn.commit()

    query_exception = []
    restarted = False

    def run_scan():
        # Open a dedicated connection; the module-scoped pg_conn must not be
        # used from a background thread.
        conn = open_pg_conn()
        try:
            run_query(f"SELECT * FROM {SCHEMA}.t", conn)
        except Exception as e:
            query_exception.append(e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    query_thread = threading.Thread(target=run_scan, daemon=True)
    try:
        query_thread.start()

        # Wait for the FDW backend to pause at the injection point inside
        # WaitForResult — at that moment the query has been sent to
        # pgduck_server but PQconsumeInput() has not yet been called.
        assert _wait_for_backend_at_injection_point(
            superuser_conn, timeout_seconds=30
        ), f"No backend reached injection point '{INJECTION_POINT}'"

        # Kill pgduck_server while the backend is paused.  This breaks the
        # socket; the next PQconsumeInput() call will return false.
        _kill_pgduck_server()

        # Wake the backend.  PQconsumeInput() will detect the broken
        # connection and throw an ERROR.  Without the fix this causes a
        # double-free SIGSEGV that crashes the whole PostgreSQL cluster.
        run_command(
            f"SELECT injection_points_wakeup('{INJECTION_POINT}')",
            superuser_conn,
        )
        superuser_conn.commit()

        # Detach so subsequent queries are not blocked.
        run_command(
            f"SELECT injection_points_detach('{INJECTION_POINT}')",
            superuser_conn,
        )
        superuser_conn.commit()

        query_thread.join(timeout=15)

        # The FDW query must have failed cleanly, not silently succeeded.
        assert (
            query_exception
        ), "Expected the FDW scan to fail after pgduck_server was killed"

        # PostgreSQL must still be alive.  A SIGSEGV in the FDW backend
        # would have crashed the cluster and this query would fail.
        result = run_query("SELECT 1 AS ok", superuser_conn)
        assert result[0]["ok"] == 1, "PostgreSQL cluster crashed after pgduck kill"

        # Restart pgduck_server so the rest of the test suite can continue.
        _restart_pgduck_server()
        restarted = True

        # Verify the FDW works again now that pgduck_server is back.
        rows = run_query(f"SELECT count(*) AS n FROM {SCHEMA}.t", pg_conn)
        pg_conn.commit()
        assert int(rows[0]["n"]) == 1000

    finally:
        # Always detach — harmless if already detached.
        try:
            run_command(
                f"SELECT injection_points_detach('{INJECTION_POINT}')",
                superuser_conn,
            )
            superuser_conn.commit()
        except Exception:
            superuser_conn.rollback()

        # If pgduck_server was killed but not yet restarted, restart it so
        # the rest of the test session is not affected.
        if not restarted:
            try:
                _restart_pgduck_server()
            except Exception:
                pass

        try:
            pg_conn.rollback()
            run_command(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", pg_conn)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()

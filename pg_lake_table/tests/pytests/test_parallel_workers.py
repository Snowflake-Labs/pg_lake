import threading
import time

import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def run_in_parallel(superuser_conn, extension):
    run_command(
        """
        CREATE OR REPLACE FUNCTION public.test_run_commands_in_parallel(
            commands text[], worker_count int, max_failures int DEFAULT -1)
        RETURNS void
        LANGUAGE C STRICT
        AS 'pg_lake_engine', $function$test_run_commands_in_parallel$function$;

        CREATE TABLE public.parallel_jobs (job int);
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        """
        DROP FUNCTION public.test_run_commands_in_parallel;
        DROP TABLE public.parallel_jobs;
        """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture
def parallel_jobs(superuser_conn, run_in_parallel):
    run_command("TRUNCATE public.parallel_jobs", superuser_conn)
    superuser_conn.commit()
    yield


def insert_jobs(job_count):
    commands = ",".join(
        f"$$INSERT INTO public.parallel_jobs VALUES ({job})$$"
        for job in range(job_count)
    )
    return f"ARRAY[{commands}]"


def committed_jobs(superuser_conn):
    result = run_query(
        "SELECT count(*) AS jobs FROM public.parallel_jobs", superuser_conn
    )
    superuser_conn.commit()
    return result[0]["jobs"]


def attach_injection_point(name, action, superuser_conn):
    run_command(f"SELECT injection_points_attach('{name}', '{action}')", superuser_conn)
    superuser_conn.commit()


def detach_injection_point(name, superuser_conn):
    run_command(
        f"SELECT injection_points_detach('{name}')",
        superuser_conn,
        raise_error=False,
    )
    superuser_conn.commit()


def test_run_in_parallel(superuser_conn, parallel_jobs):
    run_command(
        f"SELECT public.test_run_commands_in_parallel({insert_jobs(8)}, 3)",
        superuser_conn,
    )
    superuser_conn.commit()

    assert committed_jobs(superuser_conn) == 8


def test_out_of_slots_waited_out(
    superuser_conn, parallel_jobs, create_injection_extension
):
    # Refused launches are retried until slots free up, and every job then
    # runs once.
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("IS_INJECTION_POINT_ATTACHED requires PostgreSQL 18+")

    attach_injection_point("attached-worker-out-of-slots", "notice", superuser_conn)

    errors = []

    def run_jobs():
        conn = open_pg_conn()
        try:
            error = run_command(
                f"SELECT public.test_run_commands_in_parallel({insert_jobs(4)}, 2)",
                conn,
                raise_error=False,
            )
            errors.append(error)
            conn.commit()
        finally:
            conn.close()

    caller = threading.Thread(target=run_jobs)

    try:
        caller.start()
        time.sleep(2)
        assert caller.is_alive()
        assert committed_jobs(superuser_conn) == 0
    finally:
        detach_injection_point("attached-worker-out-of-slots", superuser_conn)
        caller.join(60)

    assert not caller.is_alive()
    assert errors == [None]
    assert committed_jobs(superuser_conn) == 4


def test_out_of_slots_past_max_failures(
    superuser_conn, parallel_jobs, create_injection_extension
):
    # A shortage that outlasts maxFailures is raised with its own SQLSTATE
    # and hint, not replaced by a generic error.
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("IS_INJECTION_POINT_ATTACHED requires PostgreSQL 18+")

    attach_injection_point("attached-worker-out-of-slots", "notice", superuser_conn)

    try:
        run_command("SET statement_timeout = '60s'", superuser_conn)

        with pytest.raises(psycopg2.Error) as raised:
            run_command(
                f"SELECT public.test_run_commands_in_parallel({insert_jobs(2)}, 2, 1)",
                superuser_conn,
            )
        superuser_conn.rollback()
    finally:
        detach_injection_point("attached-worker-out-of-slots", superuser_conn)

    assert raised.value.pgcode == "53400"
    assert "out of background worker slots" in raised.value.pgerror
    assert "max_worker_processes" in raised.value.diag.message_hint
    assert committed_jobs(superuser_conn) == 0


def test_launch_error_raised(superuser_conn, parallel_jobs, create_injection_extension):
    # A launch error that is not a shortage, such as a cancel during the
    # startup wait, is raised instead of being retried with no limit.
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    attach_injection_point(
        "attached-worker-before-startup-wait", "error", superuser_conn
    )

    try:
        # bound the wait so a regression fails the test instead of hanging
        run_command("SET statement_timeout = '30s'", superuser_conn)

        error = run_command(
            "SELECT public.test_run_commands_in_parallel(ARRAY[$$SELECT 1$$], 1)",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
    finally:
        detach_injection_point("attached-worker-before-startup-wait", superuser_conn)

    assert error is not None
    assert "attached-worker-before-startup-wait" in error

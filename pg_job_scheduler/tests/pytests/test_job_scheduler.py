import pytest
import psycopg2
import time
from utils_pytest import *
import server_params


def wait_for_job_status(conn, job_id, status, timeout=10):
    """Wait for a job to reach the given status, polling every 0.5 seconds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = run_query(
            f"SELECT status FROM job_scheduler.jobs WHERE job_id = {job_id}",
            conn,
        )
        if result and result[0]["status"] == status:
            return True
        conn.commit()
        time.sleep(0.5)
    return False


def wait_for_scheduler(conn, timeout=5):
    """Wait for the job scheduler base worker to appear in pg_stat_activity."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = run_query(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE backend_type = 'pg base extension worker'",
            conn,
        )
        if result[0]["count"] > 0:
            return True
        conn.commit()
        time.sleep(0.5)
    return False


def submit_job(conn, command, database_name=None, user_name=None):
    """Submit a job and return its job_id."""
    if database_name and user_name:
        result = run_query(
            f"SELECT job_scheduler.submit_job($cmd${command}$cmd$, "
            f"'{database_name}', '{user_name}')",
            conn,
        )
    else:
        result = run_query(
            f"SELECT job_scheduler.submit_job($cmd${command}$cmd$)",
            conn,
        )
    conn.commit()
    return result[0][0]


def get_job(conn, job_id):
    """Get a job's full row as a dict."""
    result = run_query(
        f"SELECT * FROM job_scheduler.list_jobs() WHERE job_id = {job_id}",
        conn,
    )
    conn.commit()
    return result[0] if result else None


def cleanup_jobs(conn):
    """Delete all jobs from the queue."""
    run_command("DELETE FROM job_scheduler.jobs", conn)
    conn.commit()


def test_submit_and_complete(superuser_conn, pg_job_scheduler):
    """Test that a simple job completes successfully."""
    cleanup_jobs(superuser_conn)

    wait_for_scheduler(superuser_conn)

    job_id = submit_job(superuser_conn, "SELECT 1")

    assert wait_for_job_status(superuser_conn, job_id, "completed")

    job = get_job(superuser_conn, job_id)
    assert job["status"] == "completed"
    assert job["result"] == "SELECT 1"
    assert job["error_message"] is None
    assert job["started_at"] is not None
    assert job["completed_at"] is not None


def test_write_job(superuser_conn, pg_job_scheduler):
    """Test that a job can write to a table."""
    cleanup_jobs(superuser_conn)

    run_command("CREATE TABLE IF NOT EXISTS test_job_write (x int)", superuser_conn)
    run_command("TRUNCATE test_job_write", superuser_conn)
    superuser_conn.commit()

    job_id = submit_job(superuser_conn, "INSERT INTO test_job_write VALUES (42)")

    assert wait_for_job_status(superuser_conn, job_id, "completed")

    result = run_query("SELECT x FROM test_job_write", superuser_conn)
    assert result[0]["x"] == 42

    run_command("DROP TABLE test_job_write", superuser_conn)
    superuser_conn.commit()


def test_failed_job(superuser_conn, pg_job_scheduler):
    """Test that a job with invalid SQL is marked as failed."""
    cleanup_jobs(superuser_conn)

    wait_for_scheduler(superuser_conn)

    job_id = submit_job(superuser_conn, "SELECT 1/0")

    assert wait_for_job_status(superuser_conn, job_id, "failed")

    job = get_job(superuser_conn, job_id)
    assert job["status"] == "failed"
    assert job["error_message"] is not None
    assert "division" in job["error_message"]


def test_multiple_concurrent_jobs(superuser_conn, pg_job_scheduler):
    """Test that multiple jobs run concurrently."""
    cleanup_jobs(superuser_conn)

    wait_for_scheduler(superuser_conn)

    job_ids = []
    for i in range(4):
        job_id = submit_job(superuser_conn, f"SELECT {i + 1}")
        job_ids.append(job_id)

    for job_id in job_ids:
        assert wait_for_job_status(superuser_conn, job_id, "completed")

    for i, job_id in enumerate(job_ids):
        job = get_job(superuser_conn, job_id)
        assert job["status"] == "completed"
        assert job["result"] == f"SELECT 1"


def test_jobs_arrive_while_running(superuser_conn, pg_job_scheduler):
    """Test that new jobs are picked up while existing jobs are still running."""
    cleanup_jobs(superuser_conn)

    wait_for_scheduler(superuser_conn)

    # submit a slow job
    slow_job_id = submit_job(superuser_conn, "SELECT pg_sleep(3)")

    # wait for it to start running
    assert wait_for_job_status(superuser_conn, slow_job_id, "running")

    # submit a fast job while the slow one is still running
    fast_job_id = submit_job(superuser_conn, "SELECT 1")

    # the fast job should complete while the slow one is still running
    assert wait_for_job_status(superuser_conn, fast_job_id, "completed")

    # now wait for the slow job to complete too
    assert wait_for_job_status(superuser_conn, slow_job_id, "completed", timeout=15)


def test_list_jobs(superuser_conn, pg_job_scheduler):
    """Test that list_jobs returns all jobs."""
    cleanup_jobs(superuser_conn)

    wait_for_scheduler(superuser_conn)

    job_id = submit_job(superuser_conn, "SELECT 1")
    assert wait_for_job_status(superuser_conn, job_id, "completed")

    result = run_query("SELECT * FROM job_scheduler.list_jobs()", superuser_conn)
    superuser_conn.commit()

    assert len(result) >= 1

    found = False
    for row in result:
        if row["job_id"] == job_id:
            found = True
            assert row["status"] == "completed"
            break
    assert found

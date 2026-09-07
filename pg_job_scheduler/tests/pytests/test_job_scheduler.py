import pytest
import psycopg2
import time
from utils_pytest import *


def wait_for(condition, timeout=15, interval=0.25):
    """Poll condition() until it returns a truthy value, or give up."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    return None


def wait_for_job_status(conn, job_id, status, timeout=15):
    """Wait for a job definition to reach the given status."""

    def check():
        conn.commit()
        return get_job(conn, job_id)["status"] == status

    return wait_for(check, timeout=timeout) is not None


def wait_for_run_count(conn, job_id, count, status=None, timeout=15):
    """Wait until a job has at least `count` runs, optionally counting only
    those in a given status, and return all of its runs."""

    def check():
        conn.commit()
        runs = get_runs(conn, job_id)
        matching = [run for run in runs if status is None or run["status"] == status]
        return runs if len(matching) >= count else None

    return wait_for(check, timeout=timeout)


def wait_for_scheduler(conn, timeout=15):
    """Wait for the job scheduler base worker to appear in pg_stat_activity."""

    def check():
        conn.commit()
        result = run_query(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE backend_type = 'pg base extension worker'",
            conn,
        )
        return result[0]["count"] > 0

    return wait_for(check, timeout=timeout) is not None


def submit_job(conn, command, schedule_interval=None, schedule_cron=None):
    """Submit a job and return its job_id."""
    interval_arg = (
        f"'{schedule_interval}'::interval" if schedule_interval else "NULL::interval"
    )
    cron_arg = f"$cron${schedule_cron}$cron$" if schedule_cron else "NULL::text"

    result = run_query(
        f"SELECT job_scheduler.submit_job("
        f"  command => $cmd${command}$cmd$, "
        f"  schedule_interval => {interval_arg}, "
        f"  schedule_cron => {cron_arg})",
        conn,
    )
    conn.commit()
    return result[0][0]


def get_job(conn, job_id):
    """Return a job definition row as a dict."""
    result = run_query(
        f"SELECT * FROM job_scheduler.list_jobs() WHERE job_id = {job_id}",
        conn,
    )
    conn.commit()
    return result[0] if result else None


def get_runs(conn, job_id=None):
    """Return the run history, oldest first, optionally for one job."""
    argument = str(job_id) if job_id is not None else "NULL"
    result = run_query(
        f"SELECT * FROM job_scheduler.list_job_runs({argument})",
        conn,
    )
    conn.commit()
    return result


def cleanup_jobs(conn):
    """Delete every job, which cascades to its runs."""
    run_command("DELETE FROM job_scheduler.jobs", conn)
    conn.commit()


@pytest.fixture(autouse=True)
def clean_queue(superuser_conn, pg_job_scheduler):
    """Start every test with an empty queue, so a recurring job left behind by
    one test cannot keep firing during the next."""
    cleanup_jobs(superuser_conn)
    wait_for_scheduler(superuser_conn)

    yield

    cleanup_jobs(superuser_conn)


def test_submit_and_complete(superuser_conn):
    """A job with no schedule runs once and its definition is then done."""
    job_id = submit_job(superuser_conn, "SELECT 1")

    assert wait_for_job_status(superuser_conn, job_id, "completed")

    job = get_job(superuser_conn, job_id)
    assert job["status"] == "completed"
    assert job["schedule_interval"] is None
    assert job["schedule_cron"] is None

    # a one-shot job is never due again
    assert job["next_run_at"] is None

    runs = get_runs(superuser_conn, job_id)
    assert len(runs) == 1
    assert runs[0]["status"] == "succeeded"
    assert runs[0]["result"] == "SELECT 1"
    assert runs[0]["error_message"] is None
    assert runs[0]["started_at"] is not None
    assert runs[0]["completed_at"] is not None


def test_write_job(superuser_conn):
    """Test that a job can write to a table."""
    run_command("CREATE TABLE IF NOT EXISTS test_job_write (x int)", superuser_conn)
    run_command("TRUNCATE test_job_write", superuser_conn)
    superuser_conn.commit()

    job_id = submit_job(superuser_conn, "INSERT INTO test_job_write VALUES (42)")

    assert wait_for_job_status(superuser_conn, job_id, "completed")

    result = run_query("SELECT x FROM test_job_write", superuser_conn)
    assert result[0]["x"] == 42

    run_command("DROP TABLE test_job_write", superuser_conn)
    superuser_conn.commit()


def test_failed_job(superuser_conn):
    """A one-shot job whose command errors fails both the run and the job."""
    job_id = submit_job(superuser_conn, "SELECT 1/0")

    assert wait_for_job_status(superuser_conn, job_id, "failed")

    runs = get_runs(superuser_conn, job_id)
    assert len(runs) == 1
    assert runs[0]["status"] == "failed"
    assert "division" in runs[0]["error_message"]


def test_multiple_concurrent_jobs(superuser_conn):
    """Test that multiple jobs run concurrently."""
    job_ids = [submit_job(superuser_conn, f"SELECT {i + 1}") for i in range(4)]

    for job_id in job_ids:
        assert wait_for_job_status(superuser_conn, job_id, "completed")

    for job_id in job_ids:
        runs = get_runs(superuser_conn, job_id)
        assert len(runs) == 1
        assert runs[0]["status"] == "succeeded"
        assert runs[0]["result"] == "SELECT 1"


def test_jobs_arrive_while_running(superuser_conn):
    """Test that new jobs are picked up while existing jobs are still running."""
    slow_job_id = submit_job(superuser_conn, "SELECT pg_sleep(3)")

    # wait for a run to be opened for it
    assert wait_for_run_count(superuser_conn, slow_job_id, 1)

    fast_job_id = submit_job(superuser_conn, "SELECT 1")

    # the fast job completes while the slow one is still going
    assert wait_for_job_status(superuser_conn, fast_job_id, "completed")

    assert wait_for_job_status(superuser_conn, slow_job_id, "completed", timeout=20)


def test_list_jobs(superuser_conn):
    """Test that list_jobs returns all job definitions."""
    job_id = submit_job(superuser_conn, "SELECT 1")
    assert wait_for_job_status(superuser_conn, job_id, "completed")

    result = run_query("SELECT * FROM job_scheduler.list_jobs()", superuser_conn)
    superuser_conn.commit()

    matching = [row for row in result if row["job_id"] == job_id]
    assert len(matching) == 1
    assert matching[0]["status"] == "completed"


def test_list_job_runs_filters_by_job(superuser_conn):
    """list_job_runs(job_id) returns only that job's runs; no argument returns
    every run."""
    first_id = submit_job(superuser_conn, "SELECT 1")
    second_id = submit_job(superuser_conn, "SELECT 2")

    assert wait_for_job_status(superuser_conn, first_id, "completed")
    assert wait_for_job_status(superuser_conn, second_id, "completed")

    first_runs = get_runs(superuser_conn, first_id)
    assert len(first_runs) == 1
    assert first_runs[0]["job_id"] == first_id

    all_runs = get_runs(superuser_conn)
    assert {run["job_id"] for run in all_runs} == {first_id, second_id}


def test_interval_job_recurs(superuser_conn):
    """An interval job keeps running, accumulating one row per run, and its
    definition stays active rather than completing."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="2 seconds")

    runs = wait_for_run_count(superuser_conn, job_id, 3, status="succeeded", timeout=30)
    assert runs is not None, "the interval job did not succeed three times"

    # nothing failed along the way
    assert all(run["status"] in ("succeeded", "running") for run in runs)
    assert all(run["result"] == "SELECT 1" for run in runs if run["result"])

    job = get_job(superuser_conn, job_id)
    assert job["status"] == "active"
    assert job["next_run_at"] is not None


def test_interval_job_advances_next_run(superuser_conn):
    """next_run_at moves forward as runs happen."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="2 seconds")

    assert wait_for_run_count(superuser_conn, job_id, 1)
    first_next_run = get_job(superuser_conn, job_id)["next_run_at"]

    assert wait_for_run_count(superuser_conn, job_id, 2, timeout=30)

    def advanced():
        superuser_conn.commit()
        return get_job(superuser_conn, job_id)["next_run_at"] > first_next_run

    assert wait_for(advanced, timeout=15), "next_run_at did not advance"


def test_interval_job_does_not_overlap_itself(superuser_conn):
    """A run that outlives its own interval does not get a second run started
    alongside it."""
    job_id = submit_job(
        superuser_conn, "SELECT pg_sleep(4)", schedule_interval="1 second"
    )

    assert wait_for_run_count(superuser_conn, job_id, 1)

    # sample repeatedly across the long run: never two running at once
    for _ in range(12):
        superuser_conn.commit()
        runs = get_runs(superuser_conn, job_id)
        running = [run for run in runs if run["status"] == "running"]
        assert len(running) <= 1, f"{len(running)} runs were in flight at once"
        time.sleep(0.5)


def test_cron_job_is_scheduled_not_run_immediately(superuser_conn):
    """A cron job waits for its first matching minute instead of firing on
    submit, and next_run_at lands on a five-minute boundary."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_cron="*/5 * * * *")

    job = get_job(superuser_conn, job_id)
    assert job["status"] == "active"
    assert job["schedule_cron"] == "*/5 * * * *"
    assert job["next_run_at"] is not None
    assert job["next_run_at"].minute % 5 == 0
    assert job["next_run_at"].second == 0

    # it is in the future, so nothing has run yet
    assert get_runs(superuser_conn, job_id) == []


def test_cron_job_runs_when_due(superuser_conn):
    """Backdating next_run_at makes the cron job due, and once it has run its
    next_run_at is back in the future on a five-minute boundary."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_cron="*/5 * * * *")

    run_command(
        f"UPDATE job_scheduler.jobs SET next_run_at = now() WHERE job_id = {job_id}",
        superuser_conn,
    )
    superuser_conn.commit()

    runs = wait_for_run_count(superuser_conn, job_id, 1)
    assert runs is not None, "the cron job did not run once it was due"
    assert runs[0]["result"] == "SELECT 1"

    job = get_job(superuser_conn, job_id)
    assert job["status"] == "active"
    assert job["next_run_at"].minute % 5 == 0

    result = run_query(
        f"SELECT next_run_at > now() AS in_future FROM job_scheduler.list_jobs() "
        f"WHERE job_id = {job_id}",
        superuser_conn,
    )
    superuser_conn.commit()
    assert result[0]["in_future"]


def test_submit_rejects_an_invalid_cron_expression(superuser_conn):
    """A bad expression is refused at submit time rather than leaving behind a
    job that can never be claimed."""
    with pytest.raises(psycopg2.Error):
        submit_job(superuser_conn, "SELECT 1", schedule_cron="not a cron expression")
    superuser_conn.rollback()

    assert run_query("SELECT * FROM job_scheduler.list_jobs()", superuser_conn) == []
    superuser_conn.commit()


def test_submit_rejects_two_schedules(superuser_conn):
    """A job is either interval-based or cron-based, never both."""
    with pytest.raises(psycopg2.Error):
        submit_job(
            superuser_conn,
            "SELECT 1",
            schedule_interval="5 minutes",
            schedule_cron="*/5 * * * *",
        )
    superuser_conn.rollback()


def test_submit_rejects_a_non_positive_interval(superuser_conn):
    """A zero or negative interval would make the job due forever."""
    for bad_interval in ["0 seconds", "-1 minute"]:
        with pytest.raises(psycopg2.Error):
            submit_job(superuser_conn, "SELECT 1", schedule_interval=bad_interval)
        superuser_conn.rollback()


def test_deleting_a_job_removes_its_runs(superuser_conn):
    """Run history is owned by the definition and cascades away with it."""
    job_id = submit_job(superuser_conn, "SELECT 1")
    assert wait_for_job_status(superuser_conn, job_id, "completed")
    assert len(get_runs(superuser_conn, job_id)) == 1

    run_command(
        f"DELETE FROM job_scheduler.jobs WHERE job_id = {job_id}", superuser_conn
    )
    superuser_conn.commit()

    assert get_runs(superuser_conn, job_id) == []


def test_paused_job_is_not_claimed(superuser_conn):
    """A definition that is not active is left alone even when it is due."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="1 second")

    run_command(
        f"UPDATE job_scheduler.jobs SET status = 'paused' WHERE job_id = {job_id}",
        superuser_conn,
    )
    superuser_conn.commit()

    # give the scheduler several passes to wrongly pick it up
    time.sleep(4)
    superuser_conn.commit()

    assert get_runs(superuser_conn, job_id) == []
    assert get_job(superuser_conn, job_id)["status"] == "paused"

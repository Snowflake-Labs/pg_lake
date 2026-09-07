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


def wait_for_run_status(conn, job_id, status, timeout=15):
    """Wait until the job's most recent run reaches the given status."""

    def check():
        conn.commit()
        runs = get_runs(conn, job_id)
        return runs[-1] if runs and runs[-1]["status"] == status else None

    return wait_for(check, timeout=timeout)


def wait_for_job_status(conn, job_id, status, timeout=15):
    """Wait for a job definition to reach the given status."""

    def check():
        conn.commit()
        job = get_job(conn, job_id)
        return job is not None and job["status"] == status

    return wait_for(check, timeout=timeout) is not None


def wait_for_job_gone(conn, job_id, timeout=15):
    """Wait for a job definition to be deleted."""

    def check():
        conn.commit()
        return get_job(conn, job_id) is None

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


def submit_job(conn, command, schedule_interval=None, schedule_cron=None, atomic=True):
    """Submit a job and return its job_id."""
    interval_arg = (
        f"'{schedule_interval}'::interval" if schedule_interval else "NULL::interval"
    )
    cron_arg = f"$cron${schedule_cron}$cron$" if schedule_cron else "NULL::text"

    result = run_query(
        f"SELECT job_scheduler.submit_job("
        f"  command => $cmd${command}$cmd$, "
        f"  schedule_interval => {interval_arg}, "
        f"  schedule_cron => {cron_arg}, "
        f"  atomic => {str(atomic).lower()})",
        conn,
    )
    conn.commit()
    return result[0][0]


def get_job(conn, job_id):
    """Return a job definition row as a dict, or None if it is gone."""
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
    """Delete every job and every run. Runs no longer cascade with their
    definition, so both tables need clearing."""
    run_command("DELETE FROM job_scheduler.jobs", conn)
    run_command("DELETE FROM job_scheduler.job_runs", conn)
    conn.commit()


def set_retention(seconds):
    """Set pg_job_scheduler.run_retention and make the worker pick it up.

    The GUC is PGC_SIGHUP, so a session SET would never reach the worker's own
    process, and ALTER SYSTEM cannot run inside a transaction block -- hence
    the autocommit connection. The value is a bare number of seconds, which is
    what GUC_UNIT_S means."""
    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_job_scheduler.run_retention = {seconds}",
            "SELECT pg_reload_conf()",
        ]
    )


def reset_retention():
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_job_scheduler.run_retention",
            "SELECT pg_reload_conf()",
        ]
    )


def insert_finished_run(conn, job_id, completed_ago):
    """Insert a run that finished `completed_ago` in the past, as retention
    would see it. Written directly rather than produced by the scheduler,
    because backdating is the whole point."""
    result = run_query(
        f"INSERT INTO job_scheduler.job_runs "
        f"(job_id, command, database_name, user_name, status, "
        f" started_at, completed_at) "
        f"VALUES ({job_id}, 'SELECT 1', current_database(), current_user, "
        f"        'succeeded', now() - interval '{completed_ago}', "
        f"        now() - interval '{completed_ago}') "
        f"RETURNING run_id",
        conn,
    )
    conn.commit()
    return result[0]["run_id"]


def run_exists(conn, run_id):
    result = run_query(
        f"SELECT count(*) FROM job_scheduler.job_runs WHERE run_id = {run_id}",
        conn,
    )
    conn.commit()
    return result[0]["count"] == 1


@pytest.fixture(autouse=True)
def clean_queue(superuser_conn, pg_job_scheduler):
    """Start every test with an empty queue, so a recurring job left behind by
    one test cannot keep firing during the next."""
    cleanup_jobs(superuser_conn)
    wait_for_scheduler(superuser_conn)

    yield

    cleanup_jobs(superuser_conn)


# ---------------------------------------------------------------------------
# atomic one-shot jobs: the definition is deleted in the job's own transaction
# ---------------------------------------------------------------------------


def test_atomic_one_shot_deletes_its_definition(superuser_conn):
    """A successful atomic one-shot leaves no definition behind. That deletion
    is what makes it exactly-once: a job that is still there provably has not
    run."""
    job_id = submit_job(superuser_conn, "SELECT 1")

    run = wait_for_run_status(superuser_conn, job_id, "succeeded")
    assert run is not None, "the job did not succeed"

    assert wait_for_job_gone(superuser_conn, job_id), "the definition survived"

    assert run["result"] == "SELECT 1"
    assert run["error_message"] is None
    assert run["completed_at"] is not None


def test_run_history_outlives_its_definition(superuser_conn):
    """The run is the durable record, so it carries its own copy of what ran
    and stays readable once the definition is gone."""
    job_id = submit_job(superuser_conn, "SELECT 42")

    assert wait_for_run_status(superuser_conn, job_id, "succeeded")
    assert wait_for_job_gone(superuser_conn, job_id)

    runs = get_runs(superuser_conn, job_id)
    assert len(runs) == 1
    assert runs[0]["command"] == "SELECT 42"
    assert runs[0]["database_name"] is not None
    assert runs[0]["user_name"] is not None


def test_write_job(superuser_conn):
    """Test that a job can write to a table."""
    run_command("CREATE TABLE IF NOT EXISTS test_job_write (x int)", superuser_conn)
    run_command("TRUNCATE test_job_write", superuser_conn)
    superuser_conn.commit()

    job_id = submit_job(superuser_conn, "INSERT INTO test_job_write VALUES (42)")

    run = wait_for_run_status(superuser_conn, job_id, "succeeded")
    assert run is not None
    assert run["result"] == "INSERT 0 1"

    result = run_query("SELECT x FROM test_job_write", superuser_conn)
    assert result[0]["x"] == 42

    run_command("DROP TABLE test_job_write", superuser_conn)
    superuser_conn.commit()


def test_failed_job_keeps_its_definition(superuser_conn):
    """A command that errors rolls its own transaction back, so the scheduler
    records the failure afterwards and the definition stays for inspection."""
    job_id = submit_job(superuser_conn, "SELECT 1/0")

    run = wait_for_run_status(superuser_conn, job_id, "failed")
    assert run is not None
    assert "division" in run["error_message"]

    assert wait_for_job_status(superuser_conn, job_id, "failed")


def test_multiple_concurrent_jobs(superuser_conn):
    """Test that multiple jobs run concurrently."""
    job_ids = [submit_job(superuser_conn, f"SELECT {i + 1}") for i in range(4)]

    for job_id in job_ids:
        assert wait_for_run_status(superuser_conn, job_id, "succeeded", timeout=20)
        assert wait_for_job_gone(superuser_conn, job_id)

    for job_id in job_ids:
        runs = get_runs(superuser_conn, job_id)
        assert len(runs) == 1
        assert runs[0]["result"] == "SELECT 1"


def test_jobs_arrive_while_running(superuser_conn):
    """Test that new jobs are picked up while existing jobs are still running."""
    slow_job_id = submit_job(superuser_conn, "SELECT pg_sleep(3)")

    assert wait_for_run_count(superuser_conn, slow_job_id, 1)

    fast_job_id = submit_job(superuser_conn, "SELECT 1")

    assert wait_for_run_status(superuser_conn, fast_job_id, "succeeded")

    assert wait_for_run_status(superuser_conn, slow_job_id, "succeeded", timeout=25)


# ---------------------------------------------------------------------------
# opting out of atomicity
# ---------------------------------------------------------------------------


def test_non_atomic_one_shot_completes_in_place(superuser_conn):
    """Without atomicity the scheduler records the outcome separately, so the
    definition survives and reaches 'completed' instead of being deleted."""
    job_id = submit_job(superuser_conn, "SELECT 1", atomic=False)

    assert wait_for_job_status(superuser_conn, job_id, "completed")

    job = get_job(superuser_conn, job_id)
    assert job["atomic"] is False
    assert job["next_run_at"] is None

    runs = get_runs(superuser_conn, job_id)
    assert len(runs) == 1
    assert runs[0]["status"] == "succeeded"
    assert runs[0]["result"] == "SELECT 1"


def test_atomic_job_refuses_a_non_transactional_command(superuser_conn):
    """VACUUM cannot run inside a transaction block, so it cannot be made
    atomic with its own bookkeeping. The job fails rather than quietly losing
    the guarantee.

    Running it through run_job() means Postgres reports this as "cannot be
    executed from a function" rather than the "transaction block" wording it
    uses for a plain BEGIN; either way it is the same restriction."""
    job_id = submit_job(superuser_conn, "VACUUM", atomic=True)

    run = wait_for_run_status(superuser_conn, job_id, "failed")
    assert run is not None
    assert "VACUUM cannot be executed" in run["error_message"]

    # it failed before doing anything, so the definition is still here
    assert wait_for_job_status(superuser_conn, job_id, "failed")


def test_non_atomic_job_can_run_a_non_transactional_command(superuser_conn):
    """The same VACUUM succeeds once the job opts out of atomicity, which is
    the reason the opt-out exists."""
    job_id = submit_job(superuser_conn, "VACUUM", atomic=False)

    run = wait_for_run_status(superuser_conn, job_id, "succeeded", timeout=25)
    assert run is not None, "VACUUM did not succeed as a non-atomic job"

    assert wait_for_job_status(superuser_conn, job_id, "completed")


# ---------------------------------------------------------------------------
# run_job authorization
# ---------------------------------------------------------------------------


def test_run_job_refuses_an_unknown_run(superuser_conn):
    """run_job is for a run the scheduler has opened, and it takes no command,
    so invented arguments achieve nothing."""
    with pytest.raises(psycopg2.Error) as error:
        run_query("SELECT job_scheduler.run_job(999999, 999999)", superuser_conn)
    superuser_conn.rollback()

    assert "run" in str(error.value)


def test_run_job_refuses_a_run_that_already_finished(superuser_conn):
    """Once a run is no longer 'running' it cannot be replayed."""
    job_id = submit_job(superuser_conn, "SELECT 1")
    run = wait_for_run_status(superuser_conn, job_id, "succeeded")
    assert run is not None

    with pytest.raises(psycopg2.Error):
        run_query(
            f"SELECT job_scheduler.run_job({job_id}, {run['run_id']})",
            superuser_conn,
        )
    superuser_conn.rollback()


def test_run_job_is_atomic_with_the_work(superuser_conn):
    """The guarantee itself: the command, the run's outcome and the deletion of
    the definition are one transaction, so rolling it back undoes all three.

    This is what makes a retry after a crash safe. The job is set up by hand
    with next_run_at NULL so the scheduler never claims it and the transaction
    below is the only thing touching it."""
    run_command("CREATE TABLE IF NOT EXISTS test_atomicity (x int)", superuser_conn)
    run_command("TRUNCATE test_atomicity", superuser_conn)

    job_id = run_query(
        "INSERT INTO job_scheduler.jobs (command, next_run_at) "
        "VALUES ('INSERT INTO test_atomicity VALUES (1)', NULL) "
        "RETURNING job_id",
        superuser_conn,
    )[0]["job_id"]

    run_id = run_query(
        f"INSERT INTO job_scheduler.job_runs "
        f"(job_id, command, database_name, user_name) "
        f"SELECT {job_id}, command, current_database(), current_user "
        f"FROM job_scheduler.jobs WHERE job_id = {job_id} "
        f"RETURNING run_id",
        superuser_conn,
    )[0]["run_id"]
    superuser_conn.commit()

    def observe():
        """Read the three effects without committing, since committing here
        would defeat the point of the test."""
        rows = run_query("SELECT count(*) FROM test_atomicity", superuser_conn)
        jobs = run_query(
            f"SELECT count(*) FROM job_scheduler.jobs WHERE job_id = {job_id}",
            superuser_conn,
        )
        runs = run_query(
            f"SELECT status FROM job_scheduler.job_runs WHERE run_id = {run_id}",
            superuser_conn,
        )
        return rows[0]["count"], jobs[0]["count"], runs[0]["status"]

    # inside the transaction, all three effects are visible
    run_query(f"SELECT job_scheduler.run_job({job_id}, {run_id})", superuser_conn)

    assert observe() == (1, 0, "succeeded")

    superuser_conn.rollback()

    # and none of them survive the rollback: the work is gone, the definition
    # is back, and the run is open again
    assert observe() == (0, 1, "running")

    superuser_conn.commit()

    run_command("DROP TABLE test_atomicity", superuser_conn)
    superuser_conn.commit()


def test_run_job_refuses_another_users_job(superuser_conn):
    """A job's body must not be runnable by anyone other than the user it was
    submitted to run as."""
    run_command(
        "DO $$ BEGIN "
        "  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jobsched_other') "
        "  THEN CREATE ROLE jobsched_other; END IF; "
        "END $$",
        superuser_conn,
    )
    superuser_conn.commit()

    # a slow job, so its run is still 'running' while we poke at it
    job_id = submit_job(superuser_conn, "SELECT pg_sleep(5)")
    runs = wait_for_run_count(superuser_conn, job_id, 1)
    assert runs is not None
    run_id = runs[0]["run_id"]

    with pytest.raises(psycopg2.Error) as error:
        run_command("SET ROLE jobsched_other", superuser_conn)
        run_query(f"SELECT job_scheduler.run_job({job_id}, {run_id})", superuser_conn)
    superuser_conn.rollback()

    assert "runs as" in str(error.value)

    assert wait_for_run_status(superuser_conn, job_id, "succeeded", timeout=25)


# ---------------------------------------------------------------------------
# recurring jobs
# ---------------------------------------------------------------------------


def test_interval_job_recurs(superuser_conn):
    """An interval job keeps running, accumulating one row per run, and its
    definition is never deleted however many times it succeeds."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="2 seconds")

    runs = wait_for_run_count(superuser_conn, job_id, 3, status="succeeded", timeout=30)
    assert runs is not None, "the interval job did not succeed three times"

    assert all(run["status"] in ("succeeded", "running") for run in runs)
    assert all(run["result"] == "SELECT 1" for run in runs if run["result"])

    job = get_job(superuser_conn, job_id)
    assert job is not None, "a recurring job must not be deleted"
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

    run = wait_for_run_status(superuser_conn, job_id, "succeeded")
    assert run is not None, "the cron job did not run once it was due"
    assert run["result"] == "SELECT 1"

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


def test_unschedulable_job_is_failed_not_retried_forever(superuser_conn):
    """A schedule that cannot be evaluated is failed once rather than retried on
    every pass. Only a direct UPDATE can get a job into this state, since
    submit_job validates the expression."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_cron="*/5 * * * *")

    run_command(
        f"UPDATE job_scheduler.jobs "
        f"SET schedule_cron = '0 0 30 2 *', next_run_at = now() "
        f"WHERE job_id = {job_id}",
        superuser_conn,
    )
    superuser_conn.commit()

    assert wait_for_job_status(superuser_conn, job_id, "failed")

    job = get_job(superuser_conn, job_id)
    assert job["next_run_at"] is None
    assert get_runs(superuser_conn, job_id) == []


def test_paused_job_is_not_claimed(superuser_conn):
    """A definition that is not active is left alone even when it is due."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="1 second")

    run_command(
        f"UPDATE job_scheduler.jobs SET status = 'paused' WHERE job_id = {job_id}",
        superuser_conn,
    )
    superuser_conn.commit()

    time.sleep(4)
    superuser_conn.commit()

    assert get_runs(superuser_conn, job_id) == []
    assert get_job(superuser_conn, job_id)["status"] == "paused"


# ---------------------------------------------------------------------------
# listing and submit validation
# ---------------------------------------------------------------------------


def test_list_jobs(superuser_conn):
    """list_jobs returns definitions; a recurring one stays listed."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="1 hour")

    result = run_query("SELECT * FROM job_scheduler.list_jobs()", superuser_conn)
    superuser_conn.commit()

    matching = [row for row in result if row["job_id"] == job_id]
    assert len(matching) == 1
    assert matching[0]["status"] == "active"
    assert matching[0]["atomic"] is True


def test_last_run_status_reports_outcomes_status_does_not(superuser_conn):
    """A recurring job stays 'active' however its runs go, so last_run_status is
    what tells you whether it is actually working."""
    job_id = submit_job(superuser_conn, "SELECT 1/0", schedule_interval="2 seconds")

    assert wait_for_run_count(superuser_conn, job_id, 1, status="failed", timeout=30)

    def reported_failure():
        superuser_conn.commit()
        return get_job(superuser_conn, job_id)["last_run_status"] == "failed"

    assert wait_for(reported_failure), "last_run_status did not report the failure"

    job = get_job(superuser_conn, job_id)

    # the schedule is still live, so the definition itself is not failed
    assert job["status"] == "active"
    assert job["last_run_status"] == "failed"
    assert job["last_run_at"] is not None


def test_last_run_status_is_null_before_the_first_run(superuser_conn):
    """A job that has not run yet has no last run to report."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_cron="*/5 * * * *")

    job = get_job(superuser_conn, job_id)
    assert job["last_run_status"] is None
    assert job["last_run_at"] is None


def test_list_job_runs_filters_by_job(superuser_conn):
    """list_job_runs(job_id) returns only that job's runs; no argument returns
    every run."""
    first_id = submit_job(superuser_conn, "SELECT 1")
    second_id = submit_job(superuser_conn, "SELECT 2")

    assert wait_for_run_status(superuser_conn, first_id, "succeeded")
    assert wait_for_run_status(superuser_conn, second_id, "succeeded")

    first_runs = get_runs(superuser_conn, first_id)
    assert len(first_runs) == 1
    assert first_runs[0]["job_id"] == first_id

    all_runs = get_runs(superuser_conn)
    assert {run["job_id"] for run in all_runs} == {first_id, second_id}


def test_deleting_a_job_leaves_its_runs(superuser_conn):
    """Run history is not owned by the definition any more, so it survives it."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="1 hour")
    assert wait_for_run_status(superuser_conn, job_id, "succeeded")

    run_command(
        f"DELETE FROM job_scheduler.jobs WHERE job_id = {job_id}", superuser_conn
    )
    superuser_conn.commit()

    assert len(get_runs(superuser_conn, job_id)) == 1


# ---------------------------------------------------------------------------
# retention
# ---------------------------------------------------------------------------


def test_retention_deletes_runs_past_their_age(superuser_conn):
    """Run history older than pg_job_scheduler.run_retention is swept away, and
    history inside the window is left alone."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="1 hour")

    old_run = insert_finished_run(superuser_conn, job_id, "2 hours")
    recent_run = insert_finished_run(superuser_conn, job_id, "10 seconds")

    try:
        set_retention(60)

        assert wait_for(
            lambda: not run_exists(superuser_conn, old_run), timeout=15
        ), "the expired run was not deleted"

        assert run_exists(
            superuser_conn, recent_run
        ), "a run inside the retention window must be kept"
    finally:
        reset_retention()


def test_retention_never_deletes_a_run_in_flight(superuser_conn):
    """A run still going has no completed_at, so no age-based sweep can reach
    it however long it has been running."""
    job_id = submit_job(superuser_conn, "SELECT pg_sleep(6)")

    runs = wait_for_run_count(superuser_conn, job_id, 1)
    assert runs is not None
    run_id = runs[0]["run_id"]

    # backdate its start well past any retention window
    run_command(
        f"UPDATE job_scheduler.job_runs "
        f"SET started_at = now() - interval '1 day' WHERE run_id = {run_id}",
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        set_retention(1)

        # give the sweep several passes to wrongly take it
        time.sleep(4)
        assert run_exists(superuser_conn, run_id), "an in-flight run was deleted"
        assert (
            run_query(
                f"SELECT status FROM job_scheduler.job_runs WHERE run_id = {run_id}",
                superuser_conn,
            )[0]["status"]
            == "running"
        )
        superuser_conn.commit()
    finally:
        reset_retention()

    assert wait_for_run_status(superuser_conn, job_id, "succeeded", timeout=25)


def test_retention_can_be_disabled(superuser_conn):
    """A negative retention keeps everything, for anyone who wants to manage
    the history themselves."""
    job_id = submit_job(superuser_conn, "SELECT 1", schedule_interval="1 hour")
    ancient_run = insert_finished_run(superuser_conn, job_id, "30 days")

    try:
        set_retention(-1)

        time.sleep(4)
        assert run_exists(
            superuser_conn, ancient_run
        ), "retention is disabled, so nothing should have been deleted"
    finally:
        reset_retention()


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

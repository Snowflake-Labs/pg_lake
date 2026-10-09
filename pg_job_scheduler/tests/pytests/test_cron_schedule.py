import pytest
from utils_pytest import *


# A fixed point to compute from, so every expectation below is a constant.
# 2026-03-15 is mid-month and mid-year, so no case accidentally relies on a
# rollover it did not mean to test.
BASE_TIME = "2026-03-15 10:30:45+00"


@pytest.fixture(scope="module")
def utc_conn(pg_job_scheduler):
    """A connection pinned to UTC, since next_cron_run works in the session's
    time zone and the expectations here are written as UTC literals."""
    conn = open_pg_conn()
    run_command("SET TIME ZONE 'UTC'", conn)
    conn.commit()

    yield conn

    conn.close()


def next_run(conn, schedule, from_time=BASE_TIME):
    """Return next_cron_run(schedule, from_time) rendered as UTC text."""
    result = run_query(
        f"SELECT job_scheduler.next_cron_run($sched${schedule}$sched$, "
        f"'{from_time}'::timestamptz)::text AS next_run",
        conn,
    )
    conn.commit()
    return result[0]["next_run"]


def next_run_dow(conn, schedule, from_time=BASE_TIME):
    """Return (next run as text, its day of week) with Sunday as 0."""
    result = run_query(
        f"SELECT job_scheduler.next_cron_run($sched${schedule}$sched$, "
        f"'{from_time}'::timestamptz)::text AS next_run, "
        f"extract(dow FROM job_scheduler.next_cron_run("
        f"  $sched${schedule}$sched$, '{from_time}'::timestamptz))::int AS dow",
        conn,
    )
    conn.commit()
    return result[0]["next_run"], result[0]["dow"]


def cron_error(conn, schedule, from_time=BASE_TIME):
    """Return the error message next_cron_run raises, or None if it succeeds."""
    try:
        run_query(
            f"SELECT job_scheduler.next_cron_run($sched${schedule}$sched$, "
            f"'{from_time}'::timestamptz)",
            conn,
        )
        conn.commit()
        return None
    except Exception as error:
        conn.rollback()
        return str(error)


def test_every_minute(utc_conn):
    """The result is the next whole minute, whatever the seconds were."""
    assert next_run(utc_conn, "* * * * *") == "2026-03-15 10:31:00+00"


def test_result_is_strictly_after_from_time(utc_conn):
    """Landing exactly on a matching minute still moves forward."""
    assert (
        next_run(utc_conn, "* * * * *", "2026-03-15 10:30:00+00")
        == "2026-03-15 10:31:00+00"
    )


def test_step_in_minutes(utc_conn):
    assert next_run(utc_conn, "*/15 * * * *") == "2026-03-15 10:45:00+00"


def test_step_rolls_into_the_next_hour(utc_conn):
    assert (
        next_run(utc_conn, "*/15 * * * *", "2026-03-15 10:46:00+00")
        == "2026-03-15 11:00:00+00"
    )


def test_minute_list(utc_conn):
    assert next_run(utc_conn, "0,15,30,45 * * * *") == "2026-03-15 10:45:00+00"


def test_range_with_step(utc_conn):
    """0-30/10 is 0, 10, 20, 30 — so from 10:30:45 the next is 11:00."""
    assert next_run(utc_conn, "0-30/10 * * * *") == "2026-03-15 11:00:00+00"


def test_range_without_step(utc_conn):
    assert next_run(utc_conn, "31-35 * * * *") == "2026-03-15 10:31:00+00"


def test_value_from_slash_runs_to_end_of_field(utc_conn):
    """40/5 means "from 40 to 59, every 5" the way Vixie cron reads it."""
    assert next_run(utc_conn, "40/5 * * * *") == "2026-03-15 10:40:00+00"


def test_specific_hour_and_minute_next_day(utc_conn):
    """03:00 has already passed today, so the answer is tomorrow."""
    assert next_run(utc_conn, "0 3 * * *") == "2026-03-16 03:00:00+00"


def test_specific_hour_later_today(utc_conn):
    assert next_run(utc_conn, "30 22 * * *") == "2026-03-15 22:30:00+00"


def test_day_of_month(utc_conn):
    assert next_run(utc_conn, "0 0 20 * *") == "2026-03-20 00:00:00+00"


def test_day_of_month_rolls_into_the_next_month(utc_conn):
    assert next_run(utc_conn, "0 0 5 * *") == "2026-04-05 00:00:00+00"


def test_month_and_day(utc_conn):
    assert next_run(utc_conn, "0 0 1 1 *") == "2027-01-01 00:00:00+00"


def test_hour_range_wrapping_midnight(utc_conn):
    """22-2 covers 22, 23, 0, 1, 2 — the next of those after 10:30 is 22:00."""
    assert next_run(utc_conn, "0 22-2 * * *") == "2026-03-15 22:00:00+00"


def test_wrapping_hour_range_continues_past_midnight(utc_conn):
    assert (
        next_run(utc_conn, "0 22-2 * * *", "2026-03-15 23:30:00+00")
        == "2026-03-16 00:00:00+00"
    )


def test_month_names(utc_conn):
    assert next_run(utc_conn, "0 0 1 JAN *") == "2027-01-01 00:00:00+00"


def test_month_names_are_case_insensitive(utc_conn):
    assert next_run(utc_conn, "0 0 1 jUl *") == "2026-07-01 00:00:00+00"


def test_month_name_range(utc_conn):
    assert next_run(utc_conn, "0 0 1 JUN-AUG *") == "2026-06-01 00:00:00+00"


def test_day_of_week_lands_on_that_weekday(utc_conn):
    """Monday is 1, and the answer must be within the coming week."""
    next_time, dow = next_run_dow(utc_conn, "0 12 * * 1")
    assert dow == 1
    assert next_time.endswith("12:00:00+00")
    assert next_time < "2026-03-22"


def test_day_of_week_name(utc_conn):
    next_time, dow = next_run_dow(utc_conn, "0 12 * * FRI")
    assert dow == 5
    assert next_time.endswith("12:00:00+00")


def test_day_of_week_seven_is_sunday(utc_conn):
    """Cron accepts both 0 and 7 for Sunday, and they must agree."""
    assert next_run(utc_conn, "0 12 * * 7") == next_run(utc_conn, "0 12 * * 0")

    _, dow = next_run_dow(utc_conn, "0 12 * * 7")
    assert dow == 0


def test_day_of_month_and_day_of_week_are_a_union(utc_conn):
    """Vixie cron fires when *either* restricted day field matches, so
    "0 0 13 * 5" means the 13th and every Friday. From 2026-03-15 the next
    Friday (the 20th) comes before the next 13th (2026-04-13)."""
    next_time, dow = next_run_dow(utc_conn, "0 0 13 * 5")
    assert dow == 5
    assert next_time == "2026-03-20 00:00:00+00"


def test_day_of_month_alone_ignores_weekday(utc_conn):
    """With day-of-week unrestricted, only the day of month is consulted."""
    assert next_run(utc_conn, "0 0 13 * *") == "2026-04-13 00:00:00+00"


def test_leap_day_skips_non_leap_years(utc_conn):
    """2027 and 2026 have no 29 February, so the answer is 2028."""
    assert next_run(utc_conn, "0 0 29 2 *") == "2028-02-29 00:00:00+00"


def test_macro_daily(utc_conn):
    assert next_run(utc_conn, "@daily") == "2026-03-16 00:00:00+00"


def test_macro_midnight_matches_daily(utc_conn):
    assert next_run(utc_conn, "@midnight") == next_run(utc_conn, "@daily")


def test_macro_hourly(utc_conn):
    assert next_run(utc_conn, "@hourly") == "2026-03-15 11:00:00+00"


def test_macro_monthly(utc_conn):
    assert next_run(utc_conn, "@monthly") == "2026-04-01 00:00:00+00"


def test_macro_yearly(utc_conn):
    assert next_run(utc_conn, "@yearly") == "2027-01-01 00:00:00+00"


def test_macro_weekly_is_sunday(utc_conn):
    _, dow = next_run_dow(utc_conn, "@weekly")
    assert dow == 0


def test_macro_is_case_insensitive(utc_conn):
    assert next_run(utc_conn, "@DAILY") == next_run(utc_conn, "@daily")


def test_surrounding_whitespace_is_tolerated(utc_conn):
    assert next_run(utc_conn, "  0   3   *  *  * ") == "2026-03-16 03:00:00+00"


def test_evaluation_follows_the_session_time_zone(utc_conn):
    """ "0 3 * * *" means 3am local, so shifting the session zone shifts the
    answer. In Asia/Tokyo (UTC+9) the next local 3am is 18:00 UTC."""
    run_command("SET TIME ZONE 'Asia/Tokyo'", utc_conn)
    utc_conn.commit()
    try:
        result = run_query(
            f"SELECT job_scheduler.next_cron_run('0 3 * * *', "
            f"'{BASE_TIME}'::timestamptz) AT TIME ZONE 'UTC' AS next_run",
            utc_conn,
        )
        utc_conn.commit()
        assert str(result[0]["next_run"]) == "2026-03-15 18:00:00"
    finally:
        run_command("SET TIME ZONE 'UTC'", utc_conn)
        utc_conn.commit()


@pytest.mark.parametrize(
    "schedule",
    [
        "* * * *",
        "* * * * * *",
        "",
        "abc",
        "60 * * * *",
        "* 24 * * *",
        "0 0 32 * *",
        "0 0 0 * *",
        "0 0 * 13 *",
        "0 0 * 0 *",
        "* * * * 8",
        "*/0 * * * *",
        "0-30/0 * * * *",
        "*/ * * * *",
        "1- * * * *",
        "0 0 * NOTAMONTH *",
        "0 0 * * NOTADAY",
        "@nonsense",
    ],
)
def test_invalid_expressions_are_rejected(utc_conn, schedule):
    error = cron_error(utc_conn, schedule)
    assert error is not None, f"{schedule!r} should have been rejected"


def test_invalid_expression_names_the_field(utc_conn):
    """The detail should say which field was wrong, so the user does not have
    to count fields to find it."""
    error = cron_error(utc_conn, "0 0 32 * *")
    assert "day of month" in error


def test_impossible_expression_is_rejected(utc_conn):
    """30 February parses fine but can never match, and must not spin."""
    error = cron_error(utc_conn, "0 0 30 2 *")
    assert "never matches" in error


def test_null_schedule_returns_null(utc_conn):
    """The function is STRICT, so a NULL schedule is NULL rather than an error."""
    result = run_query(
        "SELECT job_scheduler.next_cron_run(NULL, now()) IS NULL AS is_null",
        utc_conn,
    )
    utc_conn.commit()
    assert result[0]["is_null"]

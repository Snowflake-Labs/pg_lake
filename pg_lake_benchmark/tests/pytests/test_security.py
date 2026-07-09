"""
Security regression tests for PUBLIC SQL injection into the shared DuckDB
engine via lake_tpch.gen / lake_tpcds.gen `location` parameter.
"""

import pytest
from utils_pytest import *


# ── helpers ───────────────────────────────────────────────────────────────


def _make_no_role_conn(superuser_conn):
    """Return a fresh psycopg2 connection as a user with NO pg_lake roles."""
    no_role_user = "sec_nolake_user"
    run_command(
        f"""
        DROP ROLE IF EXISTS {no_role_user};
        CREATE USER {no_role_user};
        GRANT USAGE, CREATE ON SCHEMA public TO {no_role_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    conn = open_pg_conn(no_role_user)
    return conn, no_role_user


def _cleanup_no_role_user(superuser_conn, username):
    run_command(
        f"DROP OWNED BY {username} CASCADE; DROP ROLE IF EXISTS {username};",
        superuser_conn,
    )
    superuser_conn.commit()


# ── access control tests (CheckURLWriteAccess gate) ────────────────────────


def test_tpch_gen_requires_lake_write(superuser_conn, s3, pg_lake_benchmark_extension):
    """
    Regression test for: PUBLIC SQL injection via lake_tpch.gen `location`.

    lake_tpch.gen() was GRANT EXECUTE TO PUBLIC but never called
    CheckURLWriteAccess().  Any authenticated Postgres user — without any
    pg_lake role — could therefore reach PgDuckCopyBenchTablesToRemoteParquet
    and inject arbitrary DuckDB statements via the `location` argument.

    Fix: CheckURLWriteAccess() is now called at the entry point, requiring
    lake_write (or lake_read_write) before any DuckDB command is issued.
    """
    conn, username = _make_no_role_conn(superuser_conn)
    try:
        url = f"s3://{TEST_BUCKET}/sec_bench_nolake"
        error = run_command(
            f"""SELECT lake_tpch.gen(
                    location      => '{url}',
                    table_type    => 'pg_lake_iceberg',
                    scale_factor  => 0.0,
                    iteration_count => 1
                )""",
            conn,
            raise_error=False,
        )
        conn.rollback()

        assert error is not None, (
            "SECURITY REGRESSION: lake_tpch.gen() succeeded for a user without "
            "lake_write. This allows arbitrary DuckDB injection via the `location` "
            "parameter. Add CheckURLWriteAccess() before any "
            "DuckDB command is issued."
        )
        assert (
            "permission" in str(error).lower() or "lake_write" in str(error).lower()
        ), f"Expected a permission/lake_write error, got: {error}"
    finally:
        conn.close()
        _cleanup_no_role_user(superuser_conn, username)


def test_tpcds_gen_requires_lake_write(superuser_conn, s3, pg_lake_benchmark_extension):
    """Same access-control check for lake_tpcds.gen()."""
    conn, username = _make_no_role_conn(superuser_conn)
    try:
        url = f"s3://{TEST_BUCKET}/sec_bench_tpcds_nolake"
        error = run_command(
            f"""SELECT lake_tpcds.gen(
                    location     => '{url}',
                    table_type   => 'pg_lake_iceberg',
                    scale_factor => 0.0
                )""",
            conn,
            raise_error=False,
        )
        conn.rollback()

        assert (
            error is not None
        ), "SECURITY REGRESSION: lake_tpcds.gen() succeeded without lake_write."
        assert (
            "permission" in str(error).lower() or "lake_write" in str(error).lower()
        ), f"Expected permission error, got: {error}"
    finally:
        conn.close()
        _cleanup_no_role_user(superuser_conn, username)


# ── GRANT EXECUTE TO PUBLIC re-enable guard ───────────


def test_benchmark_gen_functions_require_lake_write(
    superuser_conn, pg_lake_benchmark_extension
):
    """
    Regression test for unbounded benchmark generation exposed to PUBLIC.

    The three benchmark gen functions were originally GRANT EXECUTE TO PUBLIC,
    so any authenticated user could exhaust the shared pgduck_server engine
    via unbounded scale_factor / iteration_count on the lake_tpch.gen* /
    lake_tpcds.gen entry points. The 3.3->3.4 upgrade REVOKEs FROM PUBLIC and
    GRANTs EXECUTE TO lake_write.

    This test asserts a user with no pg_lake roles cannot call any of the
    three gen functions, so accidental re-enable (re-adding GRANT TO PUBLIC,
    or losing the lake_write grant in a future upgrade script) is caught.
    """
    conn, username = _make_no_role_conn(superuser_conn)
    try:
        calls = [
            "SELECT lake_tpch.gen('s3://x/y', 'pg_lake_iceberg', 0.0, 1)",
            "SELECT lake_tpch.gen_partitioned('s3://x/y', 'pg_lake_iceberg', 0.0, 1)",
            "SELECT lake_tpcds.gen('s3://x/y', 'pg_lake_iceberg', 0.0)",
        ]
        for call in calls:
            error = run_command(call, conn, raise_error=False)
            conn.rollback()
            assert error is not None, (
                f"SECURITY REGRESSION: {call} succeeded for a "
                f"user without lake_write. Check that pg_lake_benchmark--3.3--3.4.sql "
                f"REVOKEs FROM PUBLIC and GRANTs EXECUTE TO lake_write."
            )
            assert (
                "permission denied" in str(error).lower()
            ), f"Expected 'permission denied' for {call}, got: {error}"
    finally:
        conn.close()
        _cleanup_no_role_user(superuser_conn, username)


# ── SQL injection prevention tests ────────────────────────────────────────


def test_tpch_gen_location_sqli_is_blocked(
    pg_conn, superuser_conn, s3, pg_lake_benchmark_extension, with_default_location
):
    """
    Even with lake_write, the `location` argument must not allow single-quote
    breakout.  The vulnerable code was:
        psprintf("COPY %s TO '%s/%s.parquet' ...", table, location, table)
    An attacker with lake_write could supply:
        s3://bucket/' (FORMAT 'parquet'); COPY (SELECT read_text('...')) TO 's3://x/y
    and run arbitrary DuckDB statements on the shared engine that owns the
    deployment's cloud-storage credentials.

    Fix: quote_literal_cstr() is now used for the path, so single quotes in
    `location` are escaped and cannot break out of the string literal.

    This test verifies that a crafted `location` containing a single quote
    causes a clean error (path not found / S3 error) rather than
    executing the injected second DuckDB statement.
    """
    # Inject payload: close the DuckDB string, then try to run a second COPY.
    # The injected COPY writes a sentinel file; if it executes it will produce
    # a filesystem error referencing "sec_injected_marker", which only appears
    # if the stacked statement was executed (i.e., injection succeeded).
    injection = (
        f"s3://{TEST_BUCKET}/bench/"
        "' (FORMAT 'parquet'); "
        "COPY (SELECT 'injected') TO '/tmp/sec_injected_marker'; "
        f"COPY lineitem TO 's3://{TEST_BUCKET}/bench/dummy"
    )

    error = run_command(
        f"""SELECT lake_tpch.gen(
                location      => $loc${injection}$loc$,
                table_type    => 'pg_lake_iceberg',
                scale_factor  => 0.0,
                iteration_count => 1
            )""",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # On vulnerable code: the stacked COPY runs and produces an error
    # referencing "/tmp/sec_injected_marker" (filesystem or permission error).
    # On fixed code: quote_literal_cstr escapes the single quote;
    # DuckDB tries to COPY to a path that literally contains the single quote,
    # which fails with an S3 / "unsupported character" error — not the marker.
    if error is not None:
        assert "sec_injected_marker" not in str(error), (
            "SECURITY REGRESSION: the SQL injection payload was executed — "
            "'/tmp/sec_injected_marker' appeared in the DuckDB error, indicating "
            "the stacked COPY statement ran. single-quote escaping in "
            "PgDuckCopyBenchTablesToRemoteParquet is not working. "
            "Use quote_literal_cstr() for the path."
        )

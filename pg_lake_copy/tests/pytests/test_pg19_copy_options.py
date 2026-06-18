"""PG19 COPY option behavior on pg_lake data-lake targets.

PostgreSQL 19 extended COPY with multi-line ``HEADER n`` skipping and
``ON_ERROR SET_NULL``, and tightened COPY FROM ... WHERE to disallow system
columns. pg_lake serves COPY to/from data-lake URLs through DuckDB and parses
the options itself, so these tests pin how pg_lake currently behaves for each
new option on a lake target: either it is enforced (system columns) or it is
cleanly rejected (a documented gap), never silently applied incorrectly.
"""

import pytest
from utils_pytest import *

PG19 = 190000


def _skip_pre_pg19(conn):
    if get_pg_version_num(conn) < PG19:
        pytest.skip("PG19 COPY option behavior")


def _write_lake_csv(conn, url, ddl, insert):
    run_command("DROP TABLE IF EXISTS copt_src", conn)
    run_command("DROP TABLE IF EXISTS copt_dst", conn)
    run_command(ddl, conn)
    run_command(insert, conn)
    run_command(f"COPY copt_src TO '{url}' WITH (format csv, header true)", conn)
    conn.commit()


def test_multiline_header_rejected_on_lake_copy(pg_conn):
    """PG19 ``HEADER n`` (n > 1, skip multiple header lines) is not yet
    implemented for pg_lake lake COPY; it must be rejected cleanly (not silently
    treated as a single header) for both COPY FROM and COPY TO. Documents the
    gap so a future implementation flips these assertions.
    """
    _skip_pre_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_pg19_copy_options/header.csv"
    _write_lake_csv(
        pg_conn,
        url,
        "CREATE TABLE copt_src (a int, b text)",
        "INSERT INTO copt_src VALUES (1,'x'),(2,'y')",
    )
    run_command("CREATE TABLE copt_dst (a int, b text)", pg_conn)
    pg_conn.commit()

    from_err = run_command(
        f"COPY copt_dst FROM '{url}' WITH (format csv, header 2)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert (
        from_err and "header" in from_err.lower()
    ), f"HEADER 2 on lake COPY FROM should be rejected cleanly, got: {from_err!r}"

    to_err = run_command(
        f"COPY copt_src TO '{url}' WITH (format csv, header 2)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert (
        to_err and "header" in to_err.lower()
    ), f"HEADER 2 on lake COPY TO should be rejected cleanly, got: {to_err!r}"


def test_on_error_set_null_rejected_on_lake_copy(pg_conn):
    """PG19 ``ON_ERROR SET_NULL`` (and ON_ERROR generally) is not implemented for
    pg_lake lake COPY FROM; it must be rejected cleanly rather than silently
    ignored, which would let malformed input through. Documents the gap.
    """
    _skip_pre_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_pg19_copy_options/on_error.csv"
    _write_lake_csv(
        pg_conn,
        url,
        "CREATE TABLE copt_src (a text, b text)",
        "INSERT INTO copt_src VALUES ('notanint','x'),('2','y')",
    )
    run_command("CREATE TABLE copt_dst (a int, b text)", pg_conn)
    pg_conn.commit()

    err = run_command(
        f"COPY copt_dst FROM '{url}' WITH (format csv, header true, on_error set_null)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert (
        err and "on_error" in err.lower()
    ), f"ON_ERROR SET_NULL on lake COPY FROM should be rejected cleanly, got: {err!r}"


def test_system_column_in_copy_from_where_rejected(pg_conn):
    """PG19 disallows system columns in COPY FROM ... WHERE. A COPY FROM a lake
    file whose WHERE references a system column must be rejected (incoming rows
    have no system columns), not silently evaluated."""
    _skip_pre_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_pg19_copy_options/syscol.csv"
    _write_lake_csv(
        pg_conn,
        url,
        "CREATE TABLE copt_src (a int, b text)",
        "INSERT INTO copt_src VALUES (1,'x'),(2,'y')",
    )
    run_command("CREATE TABLE copt_dst (a int, b text)", pg_conn)
    pg_conn.commit()

    err = run_command(
        f"COPY copt_dst FROM '{url}' WITH (format csv, header true) WHERE xmin <> 0",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert (
        err and "system column" in err.lower()
    ), f"system column in COPY FROM WHERE should be rejected, got: {err!r}"

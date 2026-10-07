"""
Tests that snapshot expiry skips reading manifest entries on append-only commits.

DeleteUnreferencedFiles walks the manifests of both expired and surviving
snapshots to find files the expired ones referenced and the surviving ones
do not.  On an append-only commit the surviving snapshot is a strict superset
of the expired one, so no data files can be unreferenced.  The enumeration
should still find unreferenced manifests and manifest lists, but skip reading
manifest entries (the O(data-files) cost).

The injection point on FetchDataFilesFromManifest lets us check both halves:
the read does not happen for an append-only commit (because expiry skips
data file enumeration), and it still happens once a commit actually removes
files.
"""

import pytest
from utils_pytest import *

FETCH_DATA_FILES_INJECTION_POINT = "expiry-fetch-data-files-from-manifest"


def attach_fetch_error(conn):
    run_command(
        f"SELECT injection_points_attach('{FETCH_DATA_FILES_INJECTION_POINT}', 'error')",
        conn,
    )


def detach_fetch_error(conn):
    run_command(
        f"SELECT injection_points_detach('{FETCH_DATA_FILES_INJECTION_POINT}')",
        conn,
    )


def commit_error(conn):
    """Commit and return the error string, or None when the commit succeeded."""
    try:
        conn.commit()
        return None
    except Exception as e:
        conn.rollback()
        return str(e)


def test_append_only_expiry_does_not_read_data_files(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    run_command(
        "CREATE TABLE expiry_skip_append (id int) USING iceberg",
        superuser_conn,
    )
    # max_snapshot_age = 0 so every commit expires the previous snapshot
    run_command(
        "ALTER FOREIGN TABLE expiry_skip_append OPTIONS (ADD max_snapshot_age '0')",
        superuser_conn,
    )
    run_command("INSERT INTO expiry_skip_append VALUES (1)", superuser_conn)
    superuser_conn.commit()

    # second commit triggers expiry of the first snapshot
    attach_fetch_error(superuser_conn)
    try:
        run_command("INSERT INTO expiry_skip_append VALUES (2)", superuser_conn)
        assert commit_error(superuser_conn) is None
    finally:
        detach_fetch_error(superuser_conn)

    assert (
        run_query("SELECT count(*) FROM expiry_skip_append", superuser_conn)[0][0] == 2
    )


def test_expiry_after_delete_reads_data_files(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    """Positive control: expiry after a commit that removed files still reads."""

    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    run_command(
        "CREATE TABLE expiry_skip_delete (id int) USING iceberg",
        superuser_conn,
    )
    run_command(
        "ALTER FOREIGN TABLE expiry_skip_delete OPTIONS (ADD max_snapshot_age '0')",
        superuser_conn,
    )
    run_command(
        "INSERT INTO expiry_skip_delete SELECT generate_series(1, 10)",
        superuser_conn,
    )
    superuser_conn.commit()

    # delete removes all rows, so the data file is removed
    attach_fetch_error(superuser_conn)
    try:
        run_command("DELETE FROM expiry_skip_delete", superuser_conn)
        error = commit_error(superuser_conn)
        assert error is not None
        assert FETCH_DATA_FILES_INJECTION_POINT in error
    finally:
        detach_fetch_error(superuser_conn)

    # the failed commit changed nothing, and the delete works without injection
    run_command("DELETE FROM expiry_skip_delete", superuser_conn)
    superuser_conn.commit()

    assert (
        run_query("SELECT count(*) FROM expiry_skip_delete", superuser_conn)[0][0] == 0
    )

"""
Tests that a commit only reads the manifests it has a reason to read.

RemoveDeletedManifestEntries walks the manifests of the current snapshot to
drop entries that earlier snapshots marked deleted. A manifest whose manifest
list entry reports no deleted entries has nothing to drop, so it should not be
read at all. The injection point in the read path lets us check both halves:
the read does not happen for an append-only commit, and it still happens once a
manifest actually holds deleted entries.
"""

import pytest
from utils_pytest import *

READ_INJECTION_POINT = "manifest-merge-read-manifest-entries"


def attach_read_error(conn):
    run_command(
        f"SELECT injection_points_attach('{READ_INJECTION_POINT}', 'error')",
        conn,
    )


def detach_read_error(conn):
    run_command(
        f"SELECT injection_points_detach('{READ_INJECTION_POINT}')",
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


def test_append_does_not_read_manifests_without_deleted_entries(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    run_command(
        "CREATE TABLE manifest_read_append (id int) USING iceberg",
        superuser_conn,
    )
    run_command("INSERT INTO manifest_read_append VALUES (1)", superuser_conn)
    superuser_conn.commit()

    # Nothing is marked deleted, so the next append has no manifest to rewrite.
    attach_read_error(superuser_conn)
    try:
        run_command("INSERT INTO manifest_read_append VALUES (2)", superuser_conn)
        assert commit_error(superuser_conn) is None
    finally:
        detach_read_error(superuser_conn)

    assert (
        run_query("SELECT count(*) FROM manifest_read_append", superuser_conn)[0][0]
        == 2
    )


def test_append_after_delete_reads_the_manifest_with_deleted_entries(
    superuser_conn,
    iceberg_extension,
    s3,
    with_default_location,
    create_injection_extension,
):
    """Positive control for the test above: the read still happens when needed."""

    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("Injection points not available (requires PostgreSQL 17+)")

    run_command(
        "CREATE TABLE manifest_read_delete (id int) USING iceberg",
        superuser_conn,
    )
    run_command(
        "INSERT INTO manifest_read_delete SELECT generate_series(1, 10)",
        superuser_conn,
    )
    superuser_conn.commit()

    # Leaves entries marked deleted in the snapshot's manifests.
    run_command("DELETE FROM manifest_read_delete WHERE id <= 5", superuser_conn)
    superuser_conn.commit()

    attach_read_error(superuser_conn)
    try:
        run_command("INSERT INTO manifest_read_delete VALUES (11)", superuser_conn)
        error = commit_error(superuser_conn)
        assert error is not None
        assert READ_INJECTION_POINT in error
    finally:
        detach_read_error(superuser_conn)

    # The failed commit changed nothing, and the same append works once the
    # read is allowed to happen.
    run_command("INSERT INTO manifest_read_delete VALUES (11)", superuser_conn)
    superuser_conn.commit()

    assert (
        run_query("SELECT count(*) FROM manifest_read_delete", superuser_conn)[0][0]
        == 6
    )

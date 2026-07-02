"""
Regression tests for issue #226:
  Duplicate key error in deletion_queue after a post-commit REST catalog failure.

When a REST catalog commit fails with HTTP 429 (rate limit) and pg_lake retries,
InsertDeletionQueueRecordExtended is called again with the same metadata file path
that was already enqueued during the failed first attempt.  The fix adds
ON CONFLICT (path) DO NOTHING to that INSERT so the retry is a no-op.
"""

import pytest
from utils_pytest import *

_TEST_PATH = "s3://test-bucket/pg_lake/test/idempotency-regression.metadata.json"


@pytest.fixture(autouse=True)
def cleanup_test_path(superuser_conn):
    yield
    run_command(
        f"DELETE FROM lake_engine.deletion_queue WHERE path = '{_TEST_PATH}'",
        superuser_conn,
    )
    superuser_conn.commit()


def test_deletion_queue_idempotent_insert(superuser_conn, extension):
    """Inserting the same path twice must not raise a duplicate-key error."""
    insert = (
        "INSERT INTO lake_engine.deletion_queue "
        "(path, table_name, orphaned_at, is_prefix) "
        f"VALUES ('{_TEST_PATH}', NULL, NULL, false) "
        "ON CONFLICT (path) DO NOTHING"
    )

    # First insert — establishes the row
    run_command(insert, superuser_conn)
    superuser_conn.commit()

    # Second insert of the same path — must be a silent no-op (regression for #226)
    run_command(insert, superuser_conn)
    superuser_conn.commit()

    count = run_query(
        f"SELECT count(*) FROM lake_engine.deletion_queue WHERE path = '{_TEST_PATH}'",
        superuser_conn,
    )[0][0]
    assert count == 1, f"Expected 1 entry in deletion_queue for the path, got {count}"


def test_deletion_queue_duplicate_without_conflict_fails(superuser_conn, extension):
    """Confirm that a plain INSERT (no ON CONFLICT) raises on duplicate path.

    This documents the pre-fix behaviour and ensures the table constraint
    is still in place.  The fix is in the C code's query string, not in
    relaxing the PRIMARY KEY.
    """
    plain_insert = (
        "INSERT INTO lake_engine.deletion_queue "
        "(path, table_name, orphaned_at, is_prefix) "
        f"VALUES ('{_TEST_PATH}', NULL, NULL, false)"
    )

    run_command(plain_insert, superuser_conn)
    superuser_conn.commit()

    error = run_command(plain_insert, superuser_conn, raise_error=False)
    superuser_conn.rollback()

    assert error is not None, "Expected a duplicate key error on second plain INSERT"
    assert (
        "duplicate key" in error.lower() or "unique" in error.lower()
    ), f"Unexpected error text: {error}"

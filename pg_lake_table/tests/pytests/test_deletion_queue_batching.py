from utils_pytest import *

# Prefix under the shared test bucket that no table owns, so the rows we queue
# below are drained by the dropped-table path of VACUUM.
TEST_PREFIX = "deletion_queue_batching"


def _queue_paths(superuser_conn, paths):
    """Queue the given object-store paths as plain, immediately eligible
    deletion-queue rows for a table that does not exist, which is what a
    dropped table leaves behind."""
    values = ",".join(f"('{path}', 0, NULL, false, false)" for path in paths)

    run_command(
        f"INSERT INTO lake_engine.deletion_queue "
        f"(path, table_name, orphaned_at, is_prefix, resolve_metadata) "
        f"VALUES {values}",
        superuser_conn,
    )
    superuser_conn.commit()


def _queued_rows(superuser_conn, prefix):
    return run_query(
        f"SELECT path, retry_count FROM lake_engine.deletion_queue "
        f"WHERE path LIKE '{prefix}%' ORDER BY path",
        superuser_conn,
    )


def _existing_files(superuser_conn, prefix):
    return run_query(
        f"SELECT count(*) FROM lake_file.list('{prefix}/**')", superuser_conn
    )[0][0]


def _vacuum_dropped_tables():
    run_command_outside_tx(
        [
            "SET pg_lake_engine.orphaned_file_retention_period = 0",
            "VACUUM (ICEBERG)",
        ]
    )


def test_deletion_queue_drains_multiple_batches(s3, superuser_conn, extension):
    """A queue longer than one deletion batch drains completely.

    The drain removes files FILE_DELETION_BATCH_SIZE (1000) at a time, so 2100
    files cross the batch boundary twice and end on a partial batch -- the
    shape most likely to drop or double-count paths if the batching is wrong.
    """
    prefix = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/multi"
    file_count = 2100

    paths = [f"{prefix}/f{i}.parquet" for i in range(file_count)]

    for path in paths:
        bucket, key = parse_s3_path(path)
        s3.put_object(Bucket=bucket, Key=key, Body=b"x")

    assert _existing_files(superuser_conn, prefix) == file_count
    superuser_conn.commit()

    _queue_paths(superuser_conn, paths)

    _vacuum_dropped_tables()

    # every file is gone from the object store, and every row from the queue
    assert _existing_files(superuser_conn, prefix) == 0
    assert _queued_rows(superuser_conn, prefix) == []
    superuser_conn.commit()


def test_deletion_queue_batch_isolates_failing_path(s3, superuser_conn, extension):
    """One unremovable path does not take its batch down with it.

    A batch is one request and its failure does not say which path was at
    fault, so a failed batch is retried per file. Without that, the healthy
    paths sharing the request would collect retry_count for a failure that was
    never theirs and eventually be abandoned at VacuumFileRemoveMaxRetries.
    """
    prefix = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/isolate"

    good_paths = [f"{prefix}/f{i}.parquet" for i in range(5)]

    for path in good_paths:
        bucket, key = parse_s3_path(path)
        s3.put_object(Bucket=bucket, Key=key, Body=b"x")

    # A path no filesystem can remove: an unknown scheme falls through to the
    # local filesystem, where removing something absent is an error rather than
    # the no-op it is on an object store. Keeping the failure local also keeps
    # the test off the network.
    bad_path = f"nosuchfs://{TEST_PREFIX}/f.parquet"

    _queue_paths(superuser_conn, good_paths + [bad_path])

    _vacuum_dropped_tables()

    # the healthy files were removed and their rows are gone
    assert _existing_files(superuser_conn, prefix) == 0
    assert _queued_rows(superuser_conn, prefix) == []

    # the failing path is the only row left, and it is the only one charged for
    # the failure
    remaining = _queued_rows(superuser_conn, f"nosuchfs://{TEST_PREFIX}")
    assert len(remaining) == 1
    assert remaining[0][0] == bad_path
    assert remaining[0][1] >= 1

    run_command(
        f"DELETE FROM lake_engine.deletion_queue WHERE path = '{bad_path}'",
        superuser_conn,
    )
    superuser_conn.commit()

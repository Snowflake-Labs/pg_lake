import os
import time

from utils_pytest import *

# Prefix under the shared test bucket that no table owns, so the rows we queue
# below are drained by the dropped-table path of VACUUM.
TEST_PREFIX = "deletion_queue_batching"


def _queue_paths(superuser_conn, paths, table=None):
    """Queue the given object-store paths as plain, immediately eligible
    deletion-queue rows.

    Without a table the rows belong to a table that does not exist, which is
    what a dropped table leaves behind, and only the dropped-table path of
    VACUUM drains them. With a table they are drained by that table's own
    vacuum, which is the only path the autovacuum worker takes.
    """
    table_name = f"'{table}'::pg_catalog.regclass::pg_catalog.oid" if table else "0"
    values = ",".join(f"('{path}', {table_name}, NULL, false, false)" for path in paths)

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


def test_autovacuum_removes_files_for_dropped_tables(
    s3, superuser_conn, extension, installcheck
):
    """The autovacuum worker drains what a dropped table left queued.

    Those rows point at an oid that is gone from pg_class, so no per-table pass
    claims them. Before, only a manual VACUUM (ICEBERG) removed them, and until
    someone ran one the files stayed and every drain paged through their rows.
    """
    if installcheck:
        return

    prefix = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/dropped"
    file_count = 3
    naptime_seconds = 2
    deadline_seconds = 60

    paths = [f"{prefix}/f{i}.parquet" for i in range(file_count)]

    for path in paths:
        bucket, key = parse_s3_path(path)
        s3.put_object(Bucket=bucket, Key=key, Body=b"x")

    assert _existing_files(superuser_conn, prefix) == file_count
    superuser_conn.commit()

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime TO '{naptime_seconds}s'",
            "SELECT pg_reload_conf()",
        ]
    )

    try:
        _queue_paths(superuser_conn, paths)

        remaining = file_count
        deadline = time.monotonic() + deadline_seconds

        while time.monotonic() < deadline:
            remaining = len(_queued_rows(superuser_conn, prefix))
            superuser_conn.commit()

            if remaining == 0:
                break

            time.sleep(0.5)

        assert (
            remaining == 0
        ), f"{remaining} of {file_count} dropped-table rows still queued after {deadline_seconds}s"

        assert _existing_files(superuser_conn, prefix) == 0
        superuser_conn.commit()
    finally:
        run_command_outside_tx(
            [
                "ALTER SYSTEM RESET pg_lake_iceberg.autovacuum_naptime",
                "SELECT pg_reload_conf()",
            ]
        )


def test_autovacuum_does_not_spin_on_unremovable_paths(
    s3, superuser_conn, extension, installcheck
):
    """A queue whose rows cannot be removed does not keep the worker busy.

    The catch-up pass is meant for a queue the worker is making progress on, so
    a pass has to charge its budget for files it removed rather than rows it
    claimed. A permanently failing path is claimed by every pass, so charging
    claims would take any pass with such a path to its limit, and the worker
    would then run file removal every second. That is not just wasted work: each
    of those passes increments retry_count, so the failing rows would pass
    vacuum_file_remove_max_retries in minutes instead of the day that default is
    sized for, and be abandoned.
    """
    if installcheck:
        return

    table = "test_deletion_queue_spin"
    prefix = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/spin"
    bad_prefix = f"nosuchfs://{TEST_PREFIX}/spin"
    good_count = 2
    bad_count = 3
    naptime_seconds = 30
    quiet_seconds = 10
    deadline_seconds = 90

    good_paths = [f"{prefix}/f{i}.parquet" for i in range(good_count)]
    bad_paths = [f"{bad_prefix}/f{i}.parquet" for i in range(bad_count)]

    for path in good_paths:
        bucket, key = parse_s3_path(path)
        s3.put_object(Bucket=bucket, Key=key, Body=b"x")

    run_command_outside_tx(
        [
            # low enough that one pass claims both kinds of row and reaches the
            # limit if failures are charged for
            f"ALTER SYSTEM SET pg_lake_table.max_file_removals_per_vacuum TO {good_count + bad_count - 1}",
            f"ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime TO '{naptime_seconds}s'",
            "SELECT pg_reload_conf()",
        ]
    )

    try:
        run_command(
            f"CREATE TABLE {table} (id int) USING pg_lake_iceberg "
            f"WITH (location = 's3://{TEST_BUCKET}/{TEST_PREFIX}/{table}/')",
            superuser_conn,
        )
        superuser_conn.commit()

        _queue_paths(superuser_conn, good_paths + bad_paths, table=table)

        # wait for the worker to run a cycle, which we see by the removable
        # files going away
        remaining = good_count
        deadline = time.monotonic() + deadline_seconds

        while time.monotonic() < deadline:
            remaining = len(_queued_rows(superuser_conn, prefix))
            superuser_conn.commit()

            if remaining == 0:
                break

            time.sleep(0.5)

        assert remaining == 0, (
            f"{remaining} of {good_count} removable rows still queued after "
            f"{deadline_seconds}s"
        )

        def max_retry_count():
            rows = _queued_rows(superuser_conn, bad_prefix)
            superuser_conn.commit()

            assert len(rows) == bad_count, (
                f"{len(rows)} of {bad_count} failing rows left in the queue, "
                f"so retry_count no longer says how often they were tried"
            )

            return max(row[1] for row in rows)

        before = max_retry_count()

        # the next cycle is a naptime away, so a worker that is not spinning
        # leaves the failing rows alone for the whole window
        time.sleep(quiet_seconds)

        after = max_retry_count()

        assert after - before <= 2, (
            f"retry_count on the failing rows went {before} -> {after} in "
            f"{quiet_seconds}s with a {naptime_seconds}s naptime, so the worker "
            f"kept re-running file removal on a queue it could not drain"
        )
    finally:
        run_command(f"DROP TABLE IF EXISTS {table}", superuser_conn)
        run_command(
            f"DELETE FROM lake_engine.deletion_queue WHERE path LIKE '{bad_prefix}%'",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command_outside_tx(
            [
                "ALTER SYSTEM RESET pg_lake_table.max_file_removals_per_vacuum",
                "ALTER SYSTEM RESET pg_lake_iceberg.autovacuum_naptime",
                "SELECT pg_reload_conf()",
            ]
        )


def test_autovacuum_drains_a_backlog_without_napping(
    s3, superuser_conn, extension, installcheck
):
    """A backlog larger than one vacuum's budget drains without a nap per file.

    A vacuum stops at pg_lake_table.max_file_removals_per_vacuum, so a longer
    queue takes several passes. If each of those passes first waits
    pg_lake_iceberg.autovacuum_naptime (10 minutes by default), a queue that
    grows faster than one naptime drains it never catches up. So a pass that
    stopped on its own limit with files still queued keeps going.

    Only the file removal stages keep going. The rest of the vacuum cycle stays
    on the naptime, so the log must show one cycle per naptime and not one per
    removed file. The naptime below is set high enough that both halves of that
    are measurable: draining one file per nap could not finish in time, and a
    cycle per naptime is far fewer cycles than there are files.
    """
    if installcheck:
        return

    logfile = f"{server_params.PG_DIR}/logfile"
    table = "test_deletion_queue_backlog"
    prefix = f"s3://{TEST_BUCKET}/{TEST_PREFIX}/backlog"
    file_count = 12
    naptime_seconds = 15
    deadline_seconds = 120

    paths = [f"{prefix}/f{i}.parquet" for i in range(file_count)]

    for path in paths:
        bucket, key = parse_s3_path(path)
        s3.put_object(Bucket=bucket, Key=key, Body=b"x")

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_table.max_file_removals_per_vacuum TO 1",
            f"ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime TO '{naptime_seconds}s'",
            "ALTER SYSTEM SET pg_lake_iceberg.log_autovacuum_min_duration TO 0",
            "SELECT pg_reload_conf()",
        ]
    )

    try:
        # the autovacuum worker only walks tables that still exist, so the
        # queued rows have to belong to one
        run_command(
            f"CREATE TABLE {table} (id int) USING pg_lake_iceberg "
            f"WITH (location = 's3://{TEST_BUCKET}/{TEST_PREFIX}/{table}/')",
            superuser_conn,
        )
        superuser_conn.commit()

        offset = os.path.getsize(logfile)

        _queue_paths(superuser_conn, paths, table=table)

        remaining = file_count
        start = time.monotonic()
        deadline = start + deadline_seconds

        while time.monotonic() < deadline:
            remaining = len(_queued_rows(superuser_conn, prefix))
            superuser_conn.commit()

            if remaining == 0:
                break

            time.sleep(0.5)

        elapsed = time.monotonic() - start

        assert remaining == 0, (
            f"{remaining} of {file_count} rows still queued after "
            f"{deadline_seconds}s, which is only "
            f"{deadline_seconds // naptime_seconds} files if every file waits "
            f"a nap of its own"
        )

        assert _existing_files(superuser_conn, prefix) == 0
        superuser_conn.commit()

        with open(logfile) as f:
            f.seek(offset)
            delta = f.read()

        cycles = [
            line
            for line in delta.splitlines()
            if "Vacuuming iceberg table" in line and table in line
        ]

        # the file removal passes ran back to back, but the rest of the cycle
        # kept the naptime, so the drain fits in about one cycle per naptime
        # however long it took. One per removed file means the whole cycle came
        # along for the ride.
        max_cycles = int(elapsed / naptime_seconds) + 2

        assert len(cycles) <= max_cycles, (
            f"{len(cycles)} full vacuum cycles while removing {file_count} "
            f"files in {elapsed:.1f}s, expected at most {max_cycles} at one per "
            f"{naptime_seconds}s naptime: " + "\n".join(cycles)
        )
    finally:
        run_command(f"DROP TABLE IF EXISTS {table}", superuser_conn)
        superuser_conn.commit()

        run_command_outside_tx(
            [
                "ALTER SYSTEM RESET pg_lake_table.max_file_removals_per_vacuum",
                "ALTER SYSTEM RESET pg_lake_iceberg.autovacuum_naptime",
                "ALTER SYSTEM RESET pg_lake_iceberg.log_autovacuum_min_duration",
                "SELECT pg_reload_conf()",
            ]
        )

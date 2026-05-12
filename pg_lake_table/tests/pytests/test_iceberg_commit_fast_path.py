"""Verify correctness of the commit-time iceberg fast/slow paths.

The pre-commit hook for iceberg writes has two implementations:

  - The fast path (TryFastPathDataFileOperations) uses an in-memory list of
    DATA_FILE_ADD ops collected during the transaction, skipping the
    catalog-vs-iceberg-metadata diff query + LoadColumnStatsForFiles SPI call.
  - The slow / diff path rebuilds the operation list from the catalog.

These tests don't probe which path was taken (that's an internal detail
visible only via DEBUG1 logs); they assert that BOTH paths produce a correct
iceberg table for the workloads that select each path. The fast path is
selected for append-only transactions (the common, hot case). The slow path
is selected on:

  - DELETE / UPDATE (writes a position-delete or update-rewrite path which
    emits non-DATA_FILE_ADD ops, flipping fastPathDisabled),
  - SAVEPOINT ... ROLLBACK TO (the subxact callback disables fast path),
  - ALTER TABLE (DDL flips fastPathDisabled).
"""

from utils_pytest import *


def test_iceberg_commit_path_pure_insert(
    installcheck,
    s3,
    with_default_location,
    extension,
    pg_conn,
):
    """Append-only INSERT should commit correctly via the fast path."""
    run_command(
        """
        CREATE SCHEMA test_commit_path_fast;
        CREATE TABLE test_commit_path_fast.t (a int, b text) USING iceberg
            WITH (PARTITION_BY = 'a');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_commit_path_fast.t
            SELECT g, 'v' || g::text FROM generate_series(1, 10) g;
        """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        "SELECT count(*) FROM lake_table.files "
        "WHERE table_name = 'test_commit_path_fast.t'::regclass;",
        pg_conn,
    )
    assert rows[0][0] == 10, "expected one data file per partition"


def test_iceberg_commit_path_insert_then_delete(
    installcheck,
    s3,
    with_default_location,
    extension,
    pg_conn,
):
    """INSERT then DELETE in the same transaction must take the slow path."""
    run_command(
        """
        CREATE SCHEMA test_commit_path_delete;
        CREATE TABLE test_commit_path_delete.t (a int, b text) USING iceberg
            WITH (PARTITION_BY = 'a');
        INSERT INTO test_commit_path_delete.t
            SELECT g, 'v' || g::text FROM generate_series(1, 6) g;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_commit_path_delete.t
            SELECT g, 'v' || g::text FROM generate_series(7, 10) g;
        DELETE FROM test_commit_path_delete.t WHERE a IN (1, 7);
        """,
        pg_conn,
    )
    pg_conn.commit()

    live_files = run_query(
        "SELECT count(*) FROM lake_table.files "
        "WHERE table_name = 'test_commit_path_delete.t'::regclass "
        "  AND content = 0;",
        pg_conn,
    )
    assert live_files[0][0] >= 8, "live data files survive INSERT+DELETE"


def test_iceberg_commit_path_subtx_rollback(
    installcheck,
    s3,
    with_default_location,
    extension,
    pg_conn,
):
    """A subtransaction rollback after an iceberg INSERT must disable the
    fast path for the rest of the outer transaction. The COMMIT after the
    aborted subtxn should still produce a correct table.
    """
    run_command(
        """
        CREATE SCHEMA test_commit_path_subtx;
        CREATE TABLE test_commit_path_subtx.t (a int, b text) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    cur = pg_conn.cursor()
    cur.execute("INSERT INTO test_commit_path_subtx.t VALUES (1, 'first');")
    cur.execute("SAVEPOINT sp;")
    cur.execute("INSERT INTO test_commit_path_subtx.t VALUES (2, 'second');")
    cur.execute("ROLLBACK TO SAVEPOINT sp;")
    cur.execute("INSERT INTO test_commit_path_subtx.t VALUES (3, 'third');")
    pg_conn.commit()

    file_count = run_query(
        "SELECT count(*) FROM lake_table.files "
        "WHERE table_name = 'test_commit_path_subtx.t'::regclass "
        "  AND content = 0;",
        pg_conn,
    )
    assert file_count[0][0] == 2, (
        "expected two surviving data files (one per committed INSERT), "
        "got %r" % file_count
    )


def test_iceberg_commit_path_alter_table_disables_fast_path(
    installcheck,
    s3,
    with_default_location,
    extension,
    pg_conn,
):
    """ALTER TABLE in the same transaction as an INSERT must force the slow
    path (TABLE_DDL flips fastPathDisabled). The resulting table should be
    correct.
    """
    run_command(
        """
        CREATE SCHEMA test_commit_path_alter;
        CREATE TABLE test_commit_path_alter.t (a int, b text) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_commit_path_alter.t VALUES (1, 'first');
        ALTER TABLE test_commit_path_alter.t ADD COLUMN c int;
        INSERT INTO test_commit_path_alter.t VALUES (2, 'second', 22);
        """,
        pg_conn,
    )
    pg_conn.commit()

    file_count = run_query(
        "SELECT count(*) FROM lake_table.files "
        "WHERE table_name = 'test_commit_path_alter.t'::regclass "
        "  AND content = 0;",
        pg_conn,
    )
    assert file_count[0][0] == 2, (
        "expected one data file per INSERT (got %r)" % file_count
    )

"""
Crash/abort tests using PostgreSQL injection points (PG 17+).

Injection points let us synchronously force an ERROR from inside C
code at a specific named site. Here we use the
'ducklake-after-snapshot-create' injection point in
DucklakeCreateSnapshot (catalog.c) to abort a PG-side ducklake DML
mid-transaction -- after the snapshot row is inserted via SPI but
before the user's transaction commits.

What we want to pin: every PG-side ducklake INSERT/UPDATE/DELETE
that aborts (for any reason -- injection here, network blip,
backend crash mid-statement) must roll back the lake_ducklake
catalog rows it wrote, so subsequent legitimate writers don't trip
over zombie snapshot_id PKs and visible row counts match the pre-
abort state.
"""

import pytest
from utils_pytest import (
    TEST_BUCKET,
    get_pg_version_num,
    run_command,
    run_query,
    server_params,  # noqa: F401
)


INJECTION_POINT = "ducklake-after-snapshot-create"


def _attach(pg_conn):
    run_command("SELECT public.injection_points_set_local()", pg_conn)
    pg_conn.commit()
    run_command(
        f"SELECT public.injection_points_attach('{INJECTION_POINT}', 'error')",
        pg_conn,
    )


def _detach(pg_conn):
    run_command(
        f"SELECT public.injection_points_detach('{INJECTION_POINT}')",
        pg_conn,
    )
    pg_conn.commit()


def test_ducklake_insert_aborted_at_snapshot_create_leaves_no_rows(
    pg_conn, s3, ducklake_extension, create_injection_extension
):
    """
    INSERT triggers DucklakeCreateSnapshot, which writes the snapshot
    row via SPI; the injection then ERRORs. The user's xact rolls
    back. After the failed INSERT:
      - SELECT count(*) returns 0 (no row visible -- transactional)
      - lake_ducklake.snapshot has only the previously-committed
        snapshots (the one we tried to insert was rolled back)
    """
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("injection points require PG 17+")

    run_command(
        f"""
        DROP TABLE IF EXISTS abort_insert;
        CREATE TABLE abort_insert (id INT, val TEXT)
            USING ducklake WITH (location = 's3://{TEST_BUCKET}/abort_insert')
        """,
        pg_conn,
    )
    pg_conn.commit()

    pre_max_snapshot = run_query(
        "SELECT max(snapshot_id) FROM lake_ducklake.snapshot", pg_conn
    )[0][0]

    _attach(pg_conn)

    err = run_command(
        "INSERT INTO abort_insert VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        pg_conn,
        raise_error=False,
    )
    assert INJECTION_POINT in err, f"expected injection error, got: {err}"

    pg_conn.rollback()
    _detach(pg_conn)

    rows = run_query("SELECT count(*) FROM abort_insert", pg_conn)
    assert rows[0][0] == 0, "INSERT must have been rolled back"

    post_max_snapshot = run_query(
        "SELECT max(snapshot_id) FROM lake_ducklake.snapshot", pg_conn
    )[0][0]
    assert post_max_snapshot == pre_max_snapshot, (
        f"snapshot row from aborted INSERT must roll back: "
        f"pre={pre_max_snapshot} post={post_max_snapshot}"
    )

    run_command("DROP TABLE abort_insert", pg_conn)
    pg_conn.commit()


def test_ducklake_update_aborted_preserves_pre_update_state(
    pg_conn, s3, ducklake_extension, create_injection_extension
):
    """
    Same shape as above but for UPDATE. UPDATE goes through the same
    DucklakeCreateSnapshot path. After the abort:
      - All original rows still visible with their original values
      - No zombie snapshot row from the failed UPDATE
    """
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("injection points require PG 17+")

    run_command(
        f"""
        DROP TABLE IF EXISTS abort_update;
        CREATE TABLE abort_update (id INT, val TEXT)
            USING ducklake WITH (location = 's3://{TEST_BUCKET}/abort_update')
        """,
        pg_conn,
    )
    run_command(
        "INSERT INTO abort_update SELECT i, 'orig' FROM generate_series(1, 25) i",
        pg_conn,
    )
    pg_conn.commit()

    pre_max_snapshot = run_query(
        "SELECT max(snapshot_id) FROM lake_ducklake.snapshot", pg_conn
    )[0][0]
    pre_count = run_query("SELECT count(*) FROM abort_update", pg_conn)[0][0]
    assert pre_count == 25

    _attach(pg_conn)

    err = run_command(
        "UPDATE abort_update SET val = 'updated' WHERE id <= 10",
        pg_conn,
        raise_error=False,
    )
    assert INJECTION_POINT in err, f"expected injection error, got: {err}"

    pg_conn.rollback()
    _detach(pg_conn)

    rows = run_query("SELECT count(*) FROM abort_update WHERE val = 'orig'", pg_conn)
    assert rows[0][0] == 25, "all rows must still be 'orig' after UPDATE rolled back"

    post_max_snapshot = run_query(
        "SELECT max(snapshot_id) FROM lake_ducklake.snapshot", pg_conn
    )[0][0]
    assert post_max_snapshot == pre_max_snapshot, (
        f"snapshot row from aborted UPDATE must roll back: "
        f"pre={pre_max_snapshot} post={post_max_snapshot}"
    )

    run_command("DROP TABLE abort_update", pg_conn)
    pg_conn.commit()


def test_ducklake_recovery_after_aborted_writer(
    pg_conn, s3, ducklake_extension, create_injection_extension
):
    """
    Most important invariant: a writer that aborted at the injection
    point must not poison the catalog -- the next legitimate writer
    must succeed without a snapshot_id PK collision. Easy to break if
    the rollback path leaves a zombie row or a stuck advisory lock.
    """
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("injection points require PG 17+")

    run_command(
        f"""
        DROP TABLE IF EXISTS abort_then_succeed;
        CREATE TABLE abort_then_succeed (id INT)
            USING ducklake WITH (location = 's3://{TEST_BUCKET}/abort_then_succeed')
        """,
        pg_conn,
    )
    pg_conn.commit()

    _attach(pg_conn)
    err = run_command(
        "INSERT INTO abort_then_succeed VALUES (1), (2)",
        pg_conn,
        raise_error=False,
    )
    assert INJECTION_POINT in err
    pg_conn.rollback()
    _detach(pg_conn)

    # Same backend, fresh transaction, same DML. Must succeed.
    run_command("INSERT INTO abort_then_succeed VALUES (10), (20), (30)", pg_conn)
    pg_conn.commit()

    rows = run_query("SELECT id FROM abort_then_succeed ORDER BY id", pg_conn)
    assert [r[0] for r in rows] == [10, 20, 30]

    run_command("DROP TABLE abort_then_succeed", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Schema revive (object_access hook in catalog.c:RevivePgLakeDucklakeSchemaIfHistorical)
# ---------------------------------------------------------------------------

REVIVE_INJECTION_POINT = "ducklake-after-revive-schema"


def _attach_revive(pg_conn):
    run_command("SELECT public.injection_points_set_local()", pg_conn)
    pg_conn.commit()
    run_command(
        f"SELECT public.injection_points_attach('{REVIVE_INJECTION_POINT}', 'error')",
        pg_conn,
    )


def _detach_revive(pg_conn):
    run_command(
        f"SELECT public.injection_points_detach('{REVIVE_INJECTION_POINT}')",
        pg_conn,
    )
    pg_conn.commit()


def test_ducklake_revive_aborted_leaves_historical_row_intact(
    pg_conn, s3, ducklake_extension, create_injection_extension
):
    """
    Re-CREATEing a previously-dropped ducklake schema goes through
    RevivePgLakeDucklakeSchemaIfHistorical, which inserts a fresh
    lake_ducklake.schema row carrying the original schema_id. If the
    surrounding CREATE SCHEMA aborts mid-revive (here forced by the
    injection), the inserted revive row must roll back -- otherwise
    the schema appears live in the catalog while pg_namespace says
    it doesn't exist.
    """
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("injection points require PG 17+")

    run_command("DROP SCHEMA IF EXISTS revive_abort_probe CASCADE", pg_conn)
    pg_conn.commit()

    run_command("CREATE SCHEMA revive_abort_probe", pg_conn)
    run_command(
        f"""
        CREATE TABLE revive_abort_probe.t (id INT)
            USING ducklake
            WITH (location = 's3://{TEST_BUCKET}/revive_abort_probe')
        """,
        pg_conn,
    )
    pg_conn.commit()

    pre_schema_id = run_query(
        "SELECT schema_id FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_abort_probe' AND end_snapshot IS NULL",
        pg_conn,
    )[0][0]

    run_command("DROP SCHEMA revive_abort_probe CASCADE", pg_conn)
    pg_conn.commit()

    # Sanity: the schema row is now end-snapshotted (the revive target).
    historical = run_query(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_abort_probe' AND end_snapshot IS NOT NULL",
        pg_conn,
    )[0][0]
    assert historical >= 1

    _attach_revive(pg_conn)

    err = run_command("CREATE SCHEMA revive_abort_probe", pg_conn, raise_error=False)
    assert REVIVE_INJECTION_POINT in err, f"expected revive injection, got: {err}"

    pg_conn.rollback()
    _detach_revive(pg_conn)

    # No live schema row -- the revive INSERT must have rolled back.
    live = run_query(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_abort_probe' AND end_snapshot IS NULL",
        pg_conn,
    )[0][0]
    assert live == 0, "aborted CREATE SCHEMA must not leave a live revived row"

    # The historical end-snapshotted row should still be there for a future
    # successful revive.
    still_historical = run_query(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_abort_probe' AND end_snapshot IS NOT NULL",
        pg_conn,
    )[0][0]
    assert still_historical >= 1

    # And a successful CREATE SCHEMA after the abort still revives the
    # original schema_id (proves the rollback didn't leave the catalog
    # locked up or otherwise wedged).
    run_command("CREATE SCHEMA revive_abort_probe", pg_conn)
    pg_conn.commit()

    post_schema_id = run_query(
        "SELECT schema_id FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_abort_probe' AND end_snapshot IS NULL",
        pg_conn,
    )[0][0]
    assert post_schema_id == pre_schema_id, (
        f"successful revive after abort must reuse original schema_id: "
        f"pre={pre_schema_id} post={post_schema_id}"
    )

    run_command("DROP SCHEMA revive_abort_probe CASCADE", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# After-apply-catalog-changes (data_files_catalog.c:ApplyDataFileCatalogChanges)
# ---------------------------------------------------------------------------

APPLY_INJECTION_POINT = "ducklake-after-apply-catalog-changes"


def _attach_apply(pg_conn):
    run_command("SELECT public.injection_points_set_local()", pg_conn)
    pg_conn.commit()
    run_command(
        f"SELECT public.injection_points_attach('{APPLY_INJECTION_POINT}', 'error')",
        pg_conn,
    )


def _detach_apply(pg_conn):
    run_command(
        f"SELECT public.injection_points_detach('{APPLY_INJECTION_POINT}')",
        pg_conn,
    )
    pg_conn.commit()


def test_ducklake_data_file_rows_rolled_back_on_late_abort(
    pg_conn, s3, ducklake_extension, create_injection_extension
):
    """
    Later checkpoint than ducklake-after-snapshot-create:
    ducklake-after-apply-catalog-changes fires after every data_file /
    delete_file row from the current statement is already inserted via
    SPI. An ERROR here exercises the longest-possible window between
    catalog mutation and xact commit. Required invariant: after
    rollback, lake_ducklake.data_file count equals the pre-INSERT
    count -- no zombie rows from the aborted statement.
    """
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("injection points require PG 17+")

    run_command(
        f"""
        DROP TABLE IF EXISTS late_abort;
        CREATE TABLE late_abort (id INT, val TEXT)
            USING ducklake WITH (location = 's3://{TEST_BUCKET}/late_abort')
        """,
        pg_conn,
    )
    pg_conn.commit()

    pre_data_files = run_query(
        "SELECT count(*) FROM lake_ducklake.data_file df "
        'JOIN lake_ducklake."table" t USING (table_id) '
        "WHERE t.table_name = 'late_abort'",
        pg_conn,
    )[0][0]

    _attach_apply(pg_conn)

    err = run_command(
        "INSERT INTO late_abort SELECT i, 'x' FROM generate_series(1, 50) i",
        pg_conn,
        raise_error=False,
    )
    assert APPLY_INJECTION_POINT in err, f"expected apply injection, got: {err}"

    pg_conn.rollback()
    _detach_apply(pg_conn)

    post_data_files = run_query(
        "SELECT count(*) FROM lake_ducklake.data_file df "
        'JOIN lake_ducklake."table" t USING (table_id) '
        "WHERE t.table_name = 'late_abort'",
        pg_conn,
    )[0][0]
    assert post_data_files == pre_data_files, (
        f"data_file rows from aborted INSERT must roll back: "
        f"pre={pre_data_files} post={post_data_files}"
    )

    rows = run_query("SELECT count(*) FROM late_abort", pg_conn)
    assert rows[0][0] == 0

    # Recovery: same backend, real INSERT must succeed.
    run_command("INSERT INTO late_abort VALUES (1, 'real')", pg_conn)
    pg_conn.commit()
    rows = run_query("SELECT id, val FROM late_abort", pg_conn)
    assert rows == [[1, "real"]]

    run_command("DROP TABLE late_abort", pg_conn)
    pg_conn.commit()

"""
Tests for the per-table iceberg option `autovacuum_compact_data_files`.

The option is autovacuum-scoped: when set to false, the pg_lake autovacuum
worker must skip data-file compaction but still run every other vacuum stage
(manifest merge, deletion-queue drain, orphan-file cleanup, field-id
backfill).  Manual `VACUUM (ICEBERG) tbl` always compacts, mirroring the
heap-level `autovacuum_enabled` storage parameter semantics.
"""

import time

import pytest
from utils_pytest import *


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def test_autovacuum_compact_data_files_validator_accepts_boolean(
    pg_conn, s3, extension, with_default_location
):
    """The validator must accept the standard boolean spellings."""
    pg_conn.autocommit = True

    for value in ("true", "false", "on", "off"):
        run_command(
            f"""
            CREATE TABLE t_validator_{value} (id int) USING iceberg
                WITH (autovacuum_compact_data_files = '{value}');
            DROP TABLE t_validator_{value};
            """,
            pg_conn,
        )

    pg_conn.autocommit = False


def test_autovacuum_compact_data_files_validator_rejects_garbage(
    pg_conn, s3, extension, with_default_location
):
    """The validator must reject non-boolean values."""
    pg_conn.autocommit = True

    err = run_command(
        """
        CREATE TABLE t_validator_bad (id int) USING iceberg
            WITH (autovacuum_compact_data_files = 'maybe');
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None, "expected an error for non-boolean value"
    assert "boolean" in err.lower(), f"unexpected error: {err}"

    pg_conn.autocommit = False


# ---------------------------------------------------------------------------
# Manual VACUUM ignores the option (autovac-only scope)
# ---------------------------------------------------------------------------


def test_manual_vacuum_compacts_regardless_of_option(
    pg_conn, s3, extension, with_default_location
):
    """Manual `VACUUM tbl` must compact data files even when
    `autovacuum_compact_data_files = false`, because the option is
    autovacuum-scoped (mirroring PG's `autovacuum_enabled` storage
    parameter)."""
    pg_conn.autocommit = True

    run_command(
        """
        CREATE TABLE t_manual (id int) USING iceberg
            WITH (
                autovacuum_enabled = false,
                autovacuum_compact_data_files = false
            );
        """,
        pg_conn,
    )

    for i in range(4):
        run_command(f"INSERT INTO t_manual VALUES ({i})", pg_conn)

    files_before = run_query(
        "SELECT count(*) FROM lake_iceberg.files("
        "  (SELECT metadata_location FROM iceberg_tables WHERE table_name='t_manual')"
        ") WHERE content = 'DATA'",
        pg_conn,
    )[0][0]
    assert files_before == 4

    run_command("SET pg_lake_table.vacuum_compact_min_input_files TO 1", pg_conn)
    run_command("VACUUM t_manual", pg_conn)
    run_command("RESET pg_lake_table.vacuum_compact_min_input_files", pg_conn)

    files_after = run_query(
        "SELECT count(*) FROM lake_iceberg.files("
        "  (SELECT metadata_location FROM iceberg_tables WHERE table_name='t_manual')"
        ") WHERE content = 'DATA'",
        pg_conn,
    )[0][0]
    assert (
        files_after == 1
    ), f"manual VACUUM must compact regardless of option (got {files_after} files)"

    run_command("DROP TABLE t_manual", pg_conn)
    pg_conn.autocommit = False


# ---------------------------------------------------------------------------
# Autovacuum honors the option
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def autovac_fast_settings(superuser_conn):
    """Make autovacuum run frequently with low thresholds, so we can observe
    its effect within a few seconds.  Disables write-time manifest merge so
    that the manifest-merge stage of autovacuum has work to do (used as a
    positive signal that autovac actually executed)."""
    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_table.vacuum_compact_min_input_files = 1;",
            "ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime TO '1s';",
            "ALTER SYSTEM SET pg_lake_iceberg.manifest_min_count_to_merge = 2;",
            "ALTER SYSTEM SET pg_lake_iceberg.enable_manifest_merge_on_write = off;",
            "SELECT pg_reload_conf()",
        ]
    )
    yield
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_table.vacuum_compact_min_input_files;",
            "ALTER SYSTEM RESET pg_lake_iceberg.autovacuum_naptime;",
            "ALTER SYSTEM RESET pg_lake_iceberg.manifest_min_count_to_merge;",
            "ALTER SYSTEM RESET pg_lake_iceberg.enable_manifest_merge_on_write;",
            "SELECT pg_reload_conf()",
        ]
    )


def _data_file_count(conn, table_name):
    return run_query(
        "SELECT count(*) FROM lake_iceberg.files("
        f"  (SELECT metadata_location FROM iceberg_tables WHERE table_name='{table_name}')"
        ") WHERE content = 'DATA'",
        conn,
    )[0][0]


def _manifest_count(conn, table_name):
    return run_query(
        "SELECT COUNT(DISTINCT manifest_path) FROM lake_iceberg.files("
        f"  (SELECT metadata_location FROM iceberg_tables WHERE table_name='{table_name}')"
        ")",
        conn,
    )[0][0]


def _snapshot_count(conn, table_name):
    return run_query(
        "SELECT count(*) FROM lake_iceberg.snapshots("
        f"  (SELECT metadata_location FROM iceberg_tables WHERE table_name='{table_name}')"
        ")",
        conn,
    )[0][0]


def _wait_until(predicate, timeout):
    """Poll `predicate` once per second until it returns true or `timeout`
    seconds have elapsed.  Returns silently either way -- the caller must
    follow up with explicit assertions to capture the failure mode."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(1)


def test_autovac_skips_compaction_when_option_false(
    superuser_conn, s3, extension, installcheck, autovac_fast_settings
):
    """With `autovacuum_compact_data_files = false`, the autovacuum worker
    must NOT compact data files, but MUST still run every other vacuum
    stage.  We verify two positive signals (manifest merge + snapshot
    expiry) so a regression in the option's scope is caught."""
    if installcheck:
        return

    dbname = "db_autovac_compact_off"
    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)", superuser_conn)
    run_command(f"CREATE DATABASE {dbname}", superuser_conn)
    try:
        conn = open_pg_conn_to_db(dbname)
        conn.autocommit = True
        run_command("CREATE EXTENSION pg_lake CASCADE", conn)

        location = f"s3://{TEST_BUCKET}/test_autovac_compact_off/"
        run_command(
            f"""
            CREATE TABLE t (id int) USING iceberg
                WITH (
                    location = '{location}',
                    autovacuum_compact_data_files = false
                );
            """,
            conn,
        )

        for i in range(5):
            run_command(f"INSERT INTO t VALUES ({i})", conn)

        # Initial state: 5 data files, multiple manifests (write-time merge is
        # disabled by the fixture), and a snapshot per insert.
        assert _data_file_count(conn, "t") == 5
        manifests_before = _manifest_count(conn, "t")
        assert (
            manifests_before >= 2
        ), f"expected >=2 manifests before autovac, got {manifests_before}"
        snapshots_before = _snapshot_count(conn, "t")
        assert (
            snapshots_before >= 2
        ), f"expected >=2 snapshots before autovac, got {snapshots_before}"

        # Force snapshot expiry to be eligible during the next autovac pass.
        # Setting the table option to 0 means writes auto-expire too, but we
        # are not writing any more from here on -- we only wait.
        run_command(
            "ALTER FOREIGN TABLE t OPTIONS (ADD max_snapshot_age '0')",
            conn,
        )

        # Poll until the strongest single signal (snapshot expiry collapsed
        # the table to 1 snapshot) lands, or we time out.  Naptime is 1s.
        _wait_until(lambda: _snapshot_count(conn, "t") == 1, timeout=10)

        # CRITICAL: data files must be unchanged (compaction was skipped).
        assert (
            _data_file_count(conn, "t") == 5
        ), "autovacuum_compact_data_files=false but autovac compacted data files"

        # POSITIVE SIGNAL #1: snapshot expiry ran during autovac (with
        # max_snapshot_age=0, all old snapshots collapse to the current head).
        assert _snapshot_count(conn, "t") == 1, (
            f"expected snapshot expiry to leave 1 snapshot, got "
            f"{_snapshot_count(conn, 't')} (was {snapshots_before})"
        )

        # POSITIVE SIGNAL #2: manifest merge ran during autovac.
        manifests_after = _manifest_count(conn, "t")
        assert manifests_after < manifests_before, (
            f"expected manifest merge to run during autovac "
            f"(manifests {manifests_before} -> still {manifests_after})"
        )

        conn.close()
    finally:
        superuser_conn.autocommit = True
        run_command(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)", superuser_conn)


def test_autovac_compacts_when_option_default(
    superuser_conn, s3, extension, installcheck, autovac_fast_settings
):
    """Control test: with the option absent (default true), autovac compacts
    as expected.  This pins the comparison for the previous test."""
    if installcheck:
        return

    dbname = "db_autovac_compact_on"
    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)", superuser_conn)
    run_command(f"CREATE DATABASE {dbname}", superuser_conn)
    try:
        conn = open_pg_conn_to_db(dbname)
        conn.autocommit = True
        run_command("CREATE EXTENSION pg_lake CASCADE", conn)

        location = f"s3://{TEST_BUCKET}/test_autovac_compact_on/"
        run_command(
            f"CREATE TABLE t (id int) USING iceberg WITH (location = '{location}');",
            conn,
        )

        for i in range(5):
            run_command(f"INSERT INTO t VALUES ({i})", conn)

        assert _data_file_count(conn, "t") == 5

        _wait_until(lambda: _data_file_count(conn, "t") == 1, timeout=10)

        assert (
            _data_file_count(conn, "t") == 1
        ), "default option should let autovac compact"

        conn.close()
    finally:
        superuser_conn.autocommit = True
        run_command(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)", superuser_conn)

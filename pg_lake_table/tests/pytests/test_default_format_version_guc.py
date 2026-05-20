"""Stage 8 (Iceberg v3 rollout): pin the contract between the
``pg_lake_iceberg.default_format_version`` GUC, the
``WITH (format_version = N)`` CREATE TABLE option, and the writer's hard
refusal to materialise a v3 metadata.json today.

The invariants pinned here are:

1. ``WITH (format_version = 2)`` works (today's behaviour) and the resolved
   value is persisted on the foreign-table OPTIONS so it survives subsequent
   GUC changes.
2. The serialised metadata.json reflects ``format-version: 2`` (sanity check
   that the option is actually wired through to the writer, not just stored).
3. ``WITH (format_version = 3)`` is rejected with a writer-side error.
4. ``WITH (format_version = N)`` for unsupported N (1, 4, 99, ...) is
   rejected by ``IcebergFormatVersionFromInt``.
5. With the GUC at ``v2`` (default), no WITH option => v2.
6. With the GUC flipped to ``v3``, no WITH option => same writer-side error
   (the GUC alone is enough to opt into v3, no need to pass it explicitly).
7. The persisted option is sticky: flipping the GUC to v3 AFTER a v2 CREATE
   does NOT change how the existing table is interpreted.

These tests do not require S3 / object storage; they exercise only the
local CREATE path, the foreign-table options, and the metadata.json the
writer drops as part of CREATE.
"""

import json
import tempfile

import psycopg2
import pytest

from utils_pytest import *

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _create_iceberg_table(pg_conn, table_name: str, with_clause: str = "") -> None:
    """Create a writable iceberg table, optionally with a WITH (...) clause.

    ``with_clause`` is appended verbatim ("" means no WITH clause); the
    caller is responsible for passing a syntactically valid expression.
    """
    if with_clause:
        ddl = f"CREATE TABLE {table_name} (a int, b int) USING pg_lake_iceberg {with_clause};"
    else:
        ddl = f"CREATE TABLE {table_name} (a int, b int) USING pg_lake_iceberg;"
    run_command(ddl, pg_conn)


def _drop_iceberg_table(pg_conn, table_name: str) -> None:
    run_command(f"DROP TABLE IF EXISTS {table_name};", pg_conn)
    pg_conn.commit()


def _read_format_version_from_options(pg_conn, table_name: str) -> int:
    """Return the ``format_version`` foreign-table option for ``table_name``.

    Asserts the option is present (this is the stickiness contract from
    Stage 8: every iceberg CREATE bakes the resolved version into OPTIONS).
    """
    rows = run_query(
        f"""
        SELECT (
            SELECT option_value
            FROM pg_options_to_table(ftoptions)
            WHERE option_name = 'format_version'
        )
        FROM pg_foreign_table
        WHERE ftrelid = '{table_name}'::regclass;
        """,
        pg_conn,
    )
    assert rows, f"foreign table {table_name} not found"
    raw = rows[0][0]
    assert raw is not None, (
        f"format_version is missing from {table_name}'s foreign-table options; "
        "Stage 8 requires it to be persisted at CREATE time"
    )
    return int(raw)


def _read_metadata_format_version(s3, pg_conn, table_name: str) -> int:
    metadata_location = run_query(
        f"""
        SELECT metadata_location
        FROM iceberg_tables
        WHERE table_name = '{table_name}';
        """,
        pg_conn,
    )[0][0]
    json_string = read_s3_operations(s3, metadata_location)
    return int(json.loads(json_string)["format-version"])


# --------------------------------------------------------------------------
# WITH (format_version = N) -- direct option path
# --------------------------------------------------------------------------


def test_with_format_version_2_creates_table_and_persists_option(
    pg_conn, extension, app_user, s3, with_default_location
):
    table = "test_v3_guc_with_v2"
    _drop_iceberg_table(pg_conn, table)
    _create_iceberg_table(pg_conn, table, "WITH (format_version = 2)")
    pg_conn.commit()

    assert _read_format_version_from_options(pg_conn, table) == 2
    assert _read_metadata_format_version(s3, pg_conn, table) == 2

    _drop_iceberg_table(pg_conn, table)


def test_with_format_version_3_rejected_by_writer_gate(
    pg_conn, extension, app_user, with_default_location
):
    table = "test_v3_guc_with_v3"
    _drop_iceberg_table(pg_conn, table)

    with pytest.raises(
        psycopg2.Error,
        match="writing Iceberg format-version 3 tables is not yet supported",
    ):
        _create_iceberg_table(pg_conn, table, "WITH (format_version = 3)")
    pg_conn.rollback()

    # The CREATE was rejected, so the table must not exist.
    rows = run_query(
        f"SELECT 1 FROM pg_class WHERE relname = '{table}';",
        pg_conn,
    )
    assert rows == []


@pytest.mark.parametrize("bad_version", [0, 1, 4, 99])
def test_with_format_version_unsupported_int_rejected(
    pg_conn, extension, app_user, with_default_location, bad_version
):
    table = f"test_v3_guc_with_bad_{bad_version}"
    _drop_iceberg_table(pg_conn, table)

    with pytest.raises(psycopg2.Error, match="unsupported iceberg format version"):
        _create_iceberg_table(pg_conn, table, f"WITH (format_version = {bad_version})")
    pg_conn.rollback()


# --------------------------------------------------------------------------
# pg_lake_iceberg.default_format_version GUC path
# --------------------------------------------------------------------------


def test_guc_default_v2_no_with_option_creates_v2_table(
    pg_conn, extension, app_user, s3, with_default_location
):
    """No WITH option + factory-default GUC ('v2') ⇒ v2 table."""
    table = "test_v3_guc_default_v2"
    _drop_iceberg_table(pg_conn, table)

    # Explicitly reset the GUC for the session to its default, so we don't
    # depend on a leaked SET from a previous test.
    run_command(
        "SET pg_lake_iceberg.default_format_version = 'v2';",
        pg_conn,
    )
    _create_iceberg_table(pg_conn, table)
    pg_conn.commit()

    assert _read_format_version_from_options(pg_conn, table) == 2
    assert _read_metadata_format_version(s3, pg_conn, table) == 2

    _drop_iceberg_table(pg_conn, table)


def test_guc_set_to_v3_blocks_create_without_with_option(
    pg_conn, extension, app_user, with_default_location
):
    """The GUC alone is enough to opt-in to v3 (and trip the writer gate)."""
    table = "test_v3_guc_set_v3_default"
    _drop_iceberg_table(pg_conn, table)

    run_command(
        "SET pg_lake_iceberg.default_format_version = 'v3';",
        pg_conn,
    )
    try:
        with pytest.raises(
            psycopg2.Error,
            match="writing Iceberg format-version 3 tables is not yet supported",
        ):
            _create_iceberg_table(pg_conn, table)
        pg_conn.rollback()
    finally:
        # Always restore the GUC for follow-on tests in this session.
        run_command(
            "RESET pg_lake_iceberg.default_format_version;",
            pg_conn,
        )
        pg_conn.commit()


def test_guc_set_to_v3_does_not_override_explicit_with_v2(
    pg_conn, extension, app_user, s3, with_default_location
):
    """The WITH option wins over the GUC -- v2 still creates fine under v3 GUC."""
    table = "test_v3_guc_v3_with_v2"
    _drop_iceberg_table(pg_conn, table)

    run_command(
        "SET pg_lake_iceberg.default_format_version = 'v3';",
        pg_conn,
    )
    try:
        _create_iceberg_table(pg_conn, table, "WITH (format_version = 2)")
        pg_conn.commit()
        assert _read_format_version_from_options(pg_conn, table) == 2
        assert _read_metadata_format_version(s3, pg_conn, table) == 2
    finally:
        run_command(
            "RESET pg_lake_iceberg.default_format_version;",
            pg_conn,
        )
        pg_conn.commit()

    _drop_iceberg_table(pg_conn, table)


def test_persisted_format_version_option_is_sticky(
    pg_conn, extension, app_user, s3, with_default_location
):
    """Stage 8 contract: once a table is created at v2, flipping the GUC to
    v3 must not change how that table is interpreted -- the version lives on
    the foreign-table OPTIONS, not in the GUC."""
    table = "test_v3_guc_sticky"
    _drop_iceberg_table(pg_conn, table)

    _create_iceberg_table(pg_conn, table)  # default GUC, v2
    pg_conn.commit()
    assert _read_format_version_from_options(pg_conn, table) == 2

    run_command(
        "SET pg_lake_iceberg.default_format_version = 'v3';",
        pg_conn,
    )
    try:
        assert _read_format_version_from_options(pg_conn, table) == 2
    finally:
        run_command(
            "RESET pg_lake_iceberg.default_format_version;",
            pg_conn,
        )
        pg_conn.commit()

    _drop_iceberg_table(pg_conn, table)


def test_guc_rejects_unknown_values(
    pg_conn, extension, app_user, with_default_location
):
    """The enum GUC must reject anything other than v2 / v3 -- the rest of
    pg_lake assumes IcebergDefaultFormatVersion is always a known enum."""
    for bad in ("v1", "v4", "experimental_v3", "3", "2"):
        with pytest.raises(psycopg2.Error):
            run_command(
                f"SET pg_lake_iceberg.default_format_version = '{bad}';",
                pg_conn,
            )
        pg_conn.rollback()

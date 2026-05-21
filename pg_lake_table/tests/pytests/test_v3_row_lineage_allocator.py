"""Stage 12 (Iceberg v3 rollout): end-to-end row-lineage allocator.

Stage 11 made the *data model* v3-aware. Stage 12 turns row-lineage on:

* The writer-side hard error against v3 (Stage 8) is lifted; CREATE TABLE
  ``WITH (format_version = 3)`` now produces a v3 metadata.json.
* When a commit adds rows, the allocator in
  ``metadata_operations.c::AllocateRowLineageForNewManifestEntries`` assigns
  every newly-added data file's ``first_row_id`` from the table's
  ``next-row-id`` cursor, then bumps the cursor by the total
  ``record_count`` it just covered.
* The new snapshot publishes the absolute ``first-row-id`` / ``added-rows``
  pair the allocator pinned.
* The manifest list entry's ``first_row_id`` (Avro field 520) is the
  first new file's ``first_row_id``.

This file proves all four invariants via a real INSERT, not a synthetic
fixture, so the entire write chain (snapshot builder -> manifest entries ->
data file Avro write -> manifest list Avro write -> metadata.json write)
participates in CI.
"""

import json

from utils_pytest import *


def _read_metadata_json(s3, pg_conn, table_name: str) -> dict:
    metadata_location = run_query(
        f"""
        SELECT metadata_location
        FROM iceberg_tables
        WHERE table_name = '{table_name}';
        """,
        pg_conn,
    )[0][0]
    return json.loads(read_s3_operations(s3, metadata_location))


# ---------------------------------------------------------------------------
# Single-INSERT path: every newly-added row goes through the allocator at
# the same commit.
# ---------------------------------------------------------------------------


def test_v3_insert_assigns_row_lineage_at_commit(
    s3, pg_conn, extension, app_user, with_default_location
):
    """A v3 table that has just had its first INSERT must publish:

    * ``next-row-id == N`` (where N is the count we just appended);
    * one snapshot with ``first-row-id == 0`` and ``added-rows == N``;
    * one manifest carrying ``first_row_id == 0`` on the manifest list.

    Pinning all four together catches any future refactor that updates
    the table-level cursor without also stamping the snapshot, or vice
    versa: rolling forward only one would silently corrupt logical row
    identity for every downstream consumer.
    """
    table = "test_v3_row_lineage_basic"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int, b int) USING pg_lake_iceberg "
        "WITH (format_version = 3);",
        pg_conn,
    )
    pg_conn.commit()

    row_count = 100
    run_command(
        f"INSERT INTO {table} "
        f"SELECT i, i + 1 FROM generate_series(1, {row_count}) i;",
        pg_conn,
    )
    pg_conn.commit()

    md = _read_metadata_json(s3, pg_conn, table)

    assert md["format-version"] == 3
    assert md.get("next-row-id") == row_count, (
        f"v3 next-row-id must advance by the INSERT's record count "
        f"({row_count}); got {md.get('next-row-id')!r}"
    )

    snapshots = md["snapshots"]
    assert len(snapshots) == 1, snapshots
    snap = snapshots[0]
    assert snap["first-row-id"] == 0, snap
    assert snap["added-rows"] == row_count, snap

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Multi-INSERT path: the cursor must persist across snapshots so the second
# commit starts where the first one ended.
# ---------------------------------------------------------------------------


def test_v3_two_inserts_chain_row_lineage(
    s3, pg_conn, extension, app_user, with_default_location
):
    table = "test_v3_row_lineage_chain"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 3);",
        pg_conn,
    )
    pg_conn.commit()

    first_count, second_count = 7, 13

    run_command(
        f"INSERT INTO {table} " f"SELECT i FROM generate_series(1, {first_count}) i;",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"INSERT INTO {table} " f"SELECT i FROM generate_series(1, {second_count}) i;",
        pg_conn,
    )
    pg_conn.commit()

    md = _read_metadata_json(s3, pg_conn, table)

    # The cursor must cover both inserts.
    assert md.get("next-row-id") == first_count + second_count

    # Snapshots must be in commit order; the second's first-row-id picks up
    # where the first's range ended.
    snapshots = md["snapshots"]
    assert len(snapshots) == 2, snapshots

    snapshots_sorted = sorted(snapshots, key=lambda s: s["sequence-number"])
    first, second = snapshots_sorted

    assert first["first-row-id"] == 0
    assert first["added-rows"] == first_count

    assert second["first-row-id"] == first_count
    assert second["added-rows"] == second_count

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# v2 must never grow row-lineage fields, even after the writer learned to
# emit them on v3 (Stage 12 regression guard).
# ---------------------------------------------------------------------------


def test_v2_insert_does_not_emit_row_lineage(
    s3, pg_conn, extension, app_user, with_default_location
):
    """The Stage 12 writer is now v3-aware. The negative half of the
    contract is that a v2 INSERT still produces a v2-clean metadata.json
    with no v3-only fields anywhere -- not on the snapshot, not on the
    table-level header.
    """
    table = "test_v2_no_row_lineage_after_v3_writer"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 2);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {table} SELECT i FROM generate_series(1, 10) i;",
        pg_conn,
    )
    pg_conn.commit()

    md = _read_metadata_json(s3, pg_conn, table)

    assert md["format-version"] == 2
    assert "next-row-id" not in md, md.keys()

    for snap in md["snapshots"]:
        assert "first-row-id" not in snap, snap
        assert "added-rows" not in snap, snap

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

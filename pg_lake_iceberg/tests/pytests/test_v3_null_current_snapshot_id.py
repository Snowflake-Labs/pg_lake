"""Stage 13 (Iceberg v3 rollout): null ``current-snapshot-id`` on the wire.

Across format versions the iceberg spec changes how an empty table
(no snapshot yet) is represented in ``metadata.json``::

  * v1 / v2: ``"current-snapshot-id": -1``   (sentinel integer, required)
  * v3     : ``"current-snapshot-id": null`` (or the field omitted)

Internally pg_lake uses ``-1`` uniformly as the "no current snapshot"
sentinel (see ``InitializeIcebergTableMetadata``). The serializer's job is
to translate that sentinel into the right *wire* shape for the table's
format version, and the parser's job is to accept all three on-disk
shapes (``-1``, ``null``, absent) without losing the sentinel.

The tests here exercise that contract via
``reserialize_iceberg_table_metadata`` (parse -> in-memory struct ->
serialize back) on every shape Spark/PyIceberg can hand us, plus the
positive case where a real snapshot id is present and must survive the
round-trip on both versions.
"""

import copy
import json
from pathlib import Path

import pytest
from utils_pytest import *

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _load_v2_template() -> dict:
    """Cheapest v2 fixture to mutate per-test. Picks the smallest file in
    ``iceberg_metadata_json_folder_path()`` so the JSON is fast to parse
    and the test stays focused on the one field we care about."""
    folder = iceberg_metadata_json_folder_path()
    candidates = sorted(
        Path(folder).glob("*.metadata.json"),
        key=lambda p: p.stat().st_size,
    )
    assert candidates, f"no v2 metadata.json fixtures under {folder}"
    with open(candidates[0]) as f:
        return json.load(f)


def _empty_v3_template() -> dict:
    """An empty v3 table's ``metadata.json`` -- no snapshots, no log, no
    refs. We start from a v2 fixture and strip every snapshot-shaped key
    because the read path validates them when they're present; here we
    only want to exercise ``current-snapshot-id``."""
    md = _load_v2_template()
    md["format-version"] = 3
    md.pop("snapshots", None)
    md.pop("snapshot-log", None)
    md.pop("refs", None)
    md.pop("metadata-log", None)
    return md


def _write(tmp_path, name, doc) -> str:
    target = tmp_path / name
    with open(target, "w") as f:
        json.dump(doc, f)
    return str(target)


def _roundtrip(conn, path) -> dict:
    rows = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')::json",
        conn,
    )
    return rows[0][0]


# ---------------------------------------------------------------------------
# v3 wire shape: null
# ---------------------------------------------------------------------------


def test_v3_null_current_snapshot_id_round_trips(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The canonical v3 empty-table shape (``null``) must survive the
    round-trip with the same null value -- not get coerced to ``-1`` and
    not get dropped from the document.
    """
    doc = _empty_v3_template()
    doc["current-snapshot-id"] = None
    out = _roundtrip(superuser_conn, _write(tmp_path, "v3_null.metadata.json", doc))

    assert out["format-version"] == 3
    assert "current-snapshot-id" in out, (
        "writer must always emit current-snapshot-id (even when null) so "
        "external readers do not have to distinguish absent from null"
    )
    assert out["current-snapshot-id"] is None, out["current-snapshot-id"]


def test_v3_absent_current_snapshot_id_normalized_to_null(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The spec also lets v3 omit ``current-snapshot-id`` entirely. pg_lake
    normalises the in-memory sentinel back to *one* canonical wire form
    on output (``null``) so downstream byte-compare consumers see a stable
    document regardless of which writer produced the input.
    """
    doc = _empty_v3_template()
    doc.pop("current-snapshot-id", None)
    out = _roundtrip(superuser_conn, _write(tmp_path, "v3_absent.metadata.json", doc))

    assert out["format-version"] == 3
    assert out.get("current-snapshot-id", "missing") is None, out


def test_v3_real_snapshot_id_passes_through(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """When the v3 table *does* have a current snapshot, the round-trip is
    a no-op on the field; the v3 null shape must not leak across the
    "I have a snapshot" boundary.
    """
    doc = _empty_v3_template()
    real_id = 7234092374823482374  # arbitrary 64-bit positive id
    doc["current-snapshot-id"] = real_id
    out = _roundtrip(superuser_conn, _write(tmp_path, "v3_real.metadata.json", doc))

    assert out["format-version"] == 3
    assert out["current-snapshot-id"] == real_id


# ---------------------------------------------------------------------------
# v2 wire shape: integer -1 must stay -1, never become null
# ---------------------------------------------------------------------------


def test_v2_no_snapshot_still_emits_minus_one(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The v3 writer change must not regress the v2 wire format: v2
    consumers (Spark <1.10, legacy java-iceberg) reject ``null`` here.
    """
    doc = _load_v2_template()
    doc["format-version"] = 2
    doc.pop("snapshots", None)
    doc.pop("snapshot-log", None)
    doc.pop("refs", None)
    doc.pop("metadata-log", None)
    doc["current-snapshot-id"] = -1
    out = _roundtrip(superuser_conn, _write(tmp_path, "v2_empty.metadata.json", doc))

    assert out["format-version"] == 2
    assert out["current-snapshot-id"] == -1, (
        "v2 metadata.json must keep the -1 sentinel on the wire; null is a "
        "v3-only spec shape"
    )


def test_v2_absent_current_snapshot_id_normalized_to_minus_one(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """Some legacy v1/v2 writers omit the field entirely when no
    snapshot exists. pg_lake's reader must accept that and the v2
    writer must always produce the canonical -1 on output.
    """
    doc = _load_v2_template()
    doc["format-version"] = 2
    doc.pop("snapshots", None)
    doc.pop("snapshot-log", None)
    doc.pop("refs", None)
    doc.pop("metadata-log", None)
    doc.pop("current-snapshot-id", None)
    out = _roundtrip(superuser_conn, _write(tmp_path, "v2_absent.metadata.json", doc))

    assert out["format-version"] == 2
    assert out["current-snapshot-id"] == -1


def test_v2_real_snapshot_id_passes_through(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The Stage 13 writer changes only the no-snapshot branch; verify
    that a real id on a v2 table is unaffected.
    """
    doc = _load_v2_template()
    real_id = 9183721234234234
    doc["current-snapshot-id"] = real_id
    out = _roundtrip(superuser_conn, _write(tmp_path, "v2_real.metadata.json", doc))

    assert out["format-version"] == 2
    assert out["current-snapshot-id"] == real_id

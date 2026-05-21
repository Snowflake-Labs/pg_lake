"""Stage 19 (Iceberg v3 rollout): sort-order ``source-ids`` is v3-only.

Iceberg v3 §sorting introduced multi-argument sort transforms (the same
shape as v3 partition transforms -- one transform consuming multiple
source columns). Sort-order fields gained a ``source-ids`` array in
parallel with the partition-spec change Stage 18 plumbed; this commit
threads the array through pg_lake's sort-order read / write chain with
the same v3-gated writer.

Stage 18 already proved the *partition-spec* side of this; Stage 19
mirrors it for sort orders, ending Iteration 3 of the v3 plan.
"""

import json
import uuid as _uuid
from pathlib import Path

from utils_pytest import *


def _make_metadata(
    tmp_path: Path,
    format_version: int,
    sort_fields: list,
) -> str:
    """Write a structurally-minimal metadata.json with the given sort
    fields and return the absolute path."""
    meta = {
        "format-version": format_version,
        "table-uuid": str(_uuid.uuid4()),
        "location": str(tmp_path),
        "last-sequence-number": 0,
        "last-updated-ms": 1700000000000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "x", "required": False, "type": "long"},
                    {"id": 2, "name": "y", "required": False, "type": "long"},
                ],
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 1,
        "sort-orders": [
            {"order-id": 1, "fields": sort_fields},
        ],
        "current-snapshot-id": -1,
    }
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))
    return str(path)


def _reserialize(superuser_conn, path: str) -> dict:
    """Round-trip the metadata through the C reader+writer chain."""
    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
        superuser_conn,
    )
    return json.loads(result[0][0])


# ---------------------------------------------------------------------------
# 1. v2 writer must NOT emit source-ids on sort-order fields.
# ---------------------------------------------------------------------------


def test_v2_sort_order_writer_drops_source_ids_even_when_input_carried_one(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v2 metadata.json that arrives with ``source-ids`` on a sort
    field is non-spec; pg_lake must not propagate it forward, otherwise
    a strict v2 reader downstream will reject the metadata."""
    path = _make_metadata(
        tmp_path,
        format_version=2,
        sort_fields=[
            {
                "transform": "identity",
                "source-id": 1,
                "source-ids": [1],
                "direction": "asc",
                "null-order": "nulls-first",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["sort-orders"][0]["fields"][0]
    assert "source-ids" not in out_field, out_field
    assert out_field["source-id"] == 1


def test_v2_sort_order_writer_drops_source_ids_when_input_had_none(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The common shape on disk -- a v2 sort field with no ``source-ids``
    -- must also round-trip without the field."""
    path = _make_metadata(
        tmp_path,
        format_version=2,
        sort_fields=[
            {
                "transform": "identity",
                "source-id": 2,
                "direction": "desc",
                "null-order": "nulls-last",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["sort-orders"][0]["fields"][0]
    assert "source-ids" not in out_field, out_field


# ---------------------------------------------------------------------------
# 2. v3 writer preserves source-ids on sort-order fields.
# ---------------------------------------------------------------------------


def test_v3_sort_order_writer_preserves_single_source_source_ids(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The simplest v3 sort field uses a single-source transform but
    records the source-id in the array form anyway -- v3 readers rely
    on ``source-ids`` as the load-bearing field, mirroring the
    partition-spec contract."""
    path = _make_metadata(
        tmp_path,
        format_version=3,
        sort_fields=[
            {
                "transform": "identity",
                "source-id": 1,
                "source-ids": [1],
                "direction": "asc",
                "null-order": "nulls-first",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["sort-orders"][0]["fields"][0]
    assert out_field["source-id"] == 1
    assert out_field["source-ids"] == [1]


def test_v3_sort_order_writer_preserves_multi_source_source_ids(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v3 multi-source sort transform -- the load-bearing case for the
    field -- must round-trip with the full id list. This pins the "model
    is plumbed end-to-end" contract for Stage 19."""
    path = _make_metadata(
        tmp_path,
        format_version=3,
        sort_fields=[
            {
                # The transform name is opaque to pg_lake's read / write
                # chain (no execution-side handling for multi-arg sort);
                # the structural plumbing for source-ids is what this
                # test pins.
                "transform": "identity",
                "source-id": 1,
                "source-ids": [1, 2],
                "direction": "asc",
                "null-order": "nulls-first",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["sort-orders"][0]["fields"][0]
    assert out_field["source-ids"] == [1, 2]


def test_v3_sort_order_without_source_ids_round_trips(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v3 metadata.json that omits ``source-ids`` on its sort fields
    must round-trip without inventing one -- the field is structurally
    optional, and our reader treats "absent" as length-0 (not "synthesise
    from source-id"). Pinning this regression-guards a future
    well-meaning refactor that "fixes" the v3 writer to always emit the
    field; v3 is the format where it's *allowed*, not required."""
    path = _make_metadata(
        tmp_path,
        format_version=3,
        sort_fields=[
            {
                "transform": "identity",
                "source-id": 5,
                "direction": "desc",
                "null-order": "nulls-last",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["sort-orders"][0]["fields"][0]
    assert "source-ids" not in out_field, out_field
    assert out_field["source-id"] == 5

"""Stage 18 (Iceberg v3 rollout): partition-spec ``source-ids`` is v3-only.

Iceberg v3 §partitioning introduced multi-argument partition transforms
(e.g. a hypothetical ``bucket(N, x, y)``) and carries their source field
ids in a new ``source-ids`` array on each ``partition-specs[].fields``
entry. The legacy single-source ``source-id`` integer is kept for
backwards compatibility; the new array is the v3-only addition.

Until this commit, pg_lake's writer always emitted ``source-ids`` -- even
when serialising a v2 metadata.json with a single-source transform -- by
populating ``source_ids_length=1`` at spec-generation time "to comply
with the reference implementation". That reading was too liberal: the
reference v2 writers (Spark, PyIceberg) do not emit the field at all.

Stage 18 gates the emission on the active
``CurrentMetadataWriterFormatVersion``: v2 metadata.json files round-trip
byte-identical to Spark, and v3 metadata.json files keep the array so
multi-source transforms can be plumbed through end-to-end in later
stages.
"""

import json
import uuid as _uuid
from pathlib import Path

from utils_pytest import *


def _make_metadata(
    tmp_path: Path,
    format_version: int,
    partition_fields: list,
) -> str:
    """Write a structurally-minimal metadata.json with the given partition
    fields and return the absolute path. The schema, sort-order, and
    bookkeeping fields are kept boilerplate; the only thing that varies
    between fixtures is the partition spec.
    """
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
        "partition-specs": [
            {"spec-id": 0, "fields": partition_fields},
        ],
        "last-partition-id": 1000 + len(partition_fields),
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": -1,
    }
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))
    return str(path)


def _reserialize(superuser_conn, path: str) -> dict:
    """Round-trip the metadata through the C reader+writer chain and
    return the decoded JSON dict."""
    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
        superuser_conn,
    )
    return json.loads(result[0][0])


# ---------------------------------------------------------------------------
# 1. v2 writer must NOT emit source-ids, even if the on-disk file had one.
# ---------------------------------------------------------------------------


def test_v2_writer_drops_source_ids_even_when_input_carried_one(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v2 metadata.json that arrives with a redundant single-element
    ``source-ids: [1]`` (some older pg_lake builds wrote this) must
    round-trip without the field -- otherwise we keep carrying a
    non-spec metadata.json forward, and external readers that strictly
    validate v2 against the spec will reject it.
    """
    path = _make_metadata(
        tmp_path,
        format_version=2,
        partition_fields=[
            {
                "source-id": 1,
                "source-ids": [1],
                "field-id": 1000,
                "name": "x_identity",
                "transform": "identity",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["partition-specs"][0]["fields"][0]
    assert "source-ids" not in out_field, out_field
    assert out_field["source-id"] == 1


def test_v2_writer_drops_source_ids_when_input_had_none(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The common shape on disk -- a v2 spec with no ``source-ids`` --
    must also round-trip without the field."""
    path = _make_metadata(
        tmp_path,
        format_version=2,
        partition_fields=[
            {
                "source-id": 2,
                "field-id": 1001,
                "name": "y_identity",
                "transform": "identity",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["partition-specs"][0]["fields"][0]
    assert "source-ids" not in out_field, out_field
    assert out_field["source-id"] == 2


# ---------------------------------------------------------------------------
# 2. v3 writer preserves source-ids and supports the multi-source case.
# ---------------------------------------------------------------------------


def test_v3_writer_preserves_single_source_source_ids(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """The simplest v3 partition spec uses a single-source transform but
    still records the source field id in the array form -- v3 readers
    rely on ``source-ids`` as the load-bearing field."""
    path = _make_metadata(
        tmp_path,
        format_version=3,
        partition_fields=[
            {
                "source-id": 1,
                "source-ids": [1],
                "field-id": 1000,
                "name": "x_identity",
                "transform": "identity",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["partition-specs"][0]["fields"][0]
    assert out_field["source-id"] == 1
    assert out_field["source-ids"] == [1]


def test_v3_writer_preserves_multi_source_source_ids(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v3 multi-source transform (the whole reason ``source-ids`` was
    added) must round-trip with the full id list. This is the "the model
    is plumbed end-to-end" assertion for Stage 18.
    """
    path = _make_metadata(
        tmp_path,
        format_version=3,
        partition_fields=[
            {
                "source-id": 1,
                "source-ids": [1, 2],
                "field-id": 1000,
                "name": "xy_bucket",
                # The transform name itself is opaque to pg_lake's read /
                # write chain; the structural plumbing for source-ids is
                # what this test pins.
                "transform": "bucket[16]",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    out_field = out["partition-specs"][0]["fields"][0]
    assert out_field["source-ids"] == [1, 2]


# ---------------------------------------------------------------------------
# 3. Regression: spec-generation populates source-ids in the in-memory model.
# ---------------------------------------------------------------------------


def test_spec_generation_in_memory_model_still_populates_source_ids(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """``IcebergSpecGenerationForPartitionTransforms`` in
    spec_generation.c always fills ``source_ids_length=1`` for every
    transform it builds (Stage 18 leaves the in-memory model alone). This
    test is the regression guard that the writer-side gate -- not a
    spec_generation.c change -- is what suppresses the v2 emission. The
    way we observe the in-memory model from Python is by round-tripping a
    v3 metadata.json: if spec_generation had been "cleaned up" to skip
    source_ids for v2, the v3 round-trip below would lose the array too.
    """
    path = _make_metadata(
        tmp_path,
        format_version=3,
        partition_fields=[
            {
                "source-id": 1,
                "source-ids": [1],
                "field-id": 1000,
                "name": "x_identity",
                "transform": "identity",
            }
        ],
    )
    out = _reserialize(superuser_conn, path)
    assert out["partition-specs"][0]["fields"][0]["source-ids"] == [1]

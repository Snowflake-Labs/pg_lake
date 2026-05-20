"""Stage 7 (Iceberg v3 rollout): feature-level read-error choke points.

After Stage 7, pg_lake reads v3 metadata.json structurally but errors with
a specific message when it encounters a v3-only feature it cannot yet
honour. This file pins every such error to a string so we notice the day
any code path silently widens.

The matrix here:

| feature                | error site                          | fixture                                        |
|------------------------|-------------------------------------|------------------------------------------------|
| deletion vectors       | read_manifest.c (ReadDataFileFromAvro) | v3_pos_dv from the corpus                   |
| encryption keys (table)| read_table_metadata.c                | synthetic v3 metadata.json                     |
| encryption key-id (snap)| read_table_metadata.c (per-snap)   | synthetic v3 metadata.json with snapshot.key-id|
| unknown type           | iceberg_field.c (type dispatch)      | synthetic v3 metadata.json with `unknown` col  |
| variant type           | iceberg_field.c                      | synthetic v3 metadata.json with `variant` col  |
| geometry / geography   | iceberg_field.c                      | synthetic v3 metadata.json with geo col        |
| timestamp_ns / timestamptz_ns | iceberg_field.c               | synthetic v3 metadata.json with ts_ns col      |

The synthetic-metadata tests construct a minimal valid v3 metadata.json on
disk and feed it to ``reserialize_iceberg_table_metadata``, so they don't
need PyIceberg / Spark / a real catalog at run-time.
"""

import json
import os
import tempfile
import uuid as _uuid
from pathlib import Path

import pytest

from utils_pytest import *
from helpers.iceberg import iceberg_v3_sample_table_folder_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_v3_metadata(tmp_path: Path, **overrides) -> str:
    """Return an absolute path to a freshly-written v3 metadata.json with the
    structurally-minimum required fields. Callers pass `overrides` to inject
    v3-only constructs (encryption-keys, snapshot.key-id, type names, ...).
    """
    meta = {
        "format-version": 3,
        "table-uuid": str(_uuid.uuid4()),
        "location": str(tmp_path),
        "last-sequence-number": 0,
        "last-updated-ms": 1700000000000,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": False, "type": "long"},
                ],
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": -1,
    }
    meta.update(overrides)
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))
    return str(path)


def _read_v3_metadata_should_error(superuser_conn, path: str, expected_match: str):
    """Run reserialize_iceberg_table_metadata and assert it errors with
    `expected_match` (case-insensitive substring of the Postgres error)."""
    with pytest.raises(Exception, match=expected_match):
        run_query(
            f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
            superuser_conn,
        )
    superuser_conn.rollback()


# ---------------------------------------------------------------------------
# 1. Deletion vectors: real fixture
# ---------------------------------------------------------------------------


def _v3_pos_dv_manifest_paths():
    """Return absolute paths to the m0 manifest avro files committed under
    ``sample_tables_v3/public/v3_pos_dv/metadata/*-m0.avro``. The generator
    writes both an append-only manifest and a DV manifest (one snapshot per
    manifest); only the DV-flavoured one carries the
    ``referenced_data_file`` field that read_manifest.c errors on.
    """
    root = (
        Path(iceberg_v3_sample_table_folder_path())
        / "public"
        / "v3_pos_dv"
        / "metadata"
    )
    return sorted(str(p) for p in root.glob("*-m0.avro"))


def test_v3_pos_dv_manifest_read_raises_dv_error(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    manifests = _v3_pos_dv_manifest_paths()
    assert manifests, "v3_pos_dv corpus is missing -- regenerate fixtures"

    # Read each m0 manifest in turn. At least one must surface the DV error;
    # an append-only manifest in the same table is structurally legal v3 and
    # should *not* error here.
    out_path = str(tmp_path / "out.avro")
    error_count = 0
    for manifest_path in manifests:
        try:
            run_query(
                "SELECT lake_iceberg.reserialize_iceberg_manifest("
                f"'{manifest_path}', '{out_path}', 2)",
                superuser_conn,
            )
            superuser_conn.commit()
        except Exception as exc:  # noqa: BLE001
            superuser_conn.rollback()
            assert "deletion vectors are not yet supported" in str(exc), str(exc)
            error_count += 1
    assert error_count >= 1, (
        "expected at least one v3_pos_dv manifest to trip the DV choke point; "
        "if zero, the fixture regressed and no longer contains a DV entry"
    )


# ---------------------------------------------------------------------------
# 2. Encryption: synthetic metadata.json
# ---------------------------------------------------------------------------


def test_v3_table_level_encryption_keys_error(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    path = _make_v3_metadata(
        tmp_path,
        **{
            "encryption-keys": [
                {"key-id": "k1", "encrypted-key-metadata": "AAAA"},
            ],
        },
    )
    _read_v3_metadata_should_error(
        superuser_conn,
        path,
        expected_match="encrypted Iceberg tables are not yet supported",
    )


def test_v3_empty_encryption_keys_does_not_error(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A literal empty encryption-keys array is structurally legal and
    semantically uninteresting; the reader must not refuse it."""
    path = _make_v3_metadata(tmp_path, **{"encryption-keys": []})
    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
        superuser_conn,
    )
    assert json.loads(result[0][0])["format-version"] == 3


def test_v3_snapshot_key_id_error(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    path = _make_v3_metadata(
        tmp_path,
        snapshots=[
            {
                "snapshot-id": 1,
                "sequence-number": 1,
                "timestamp-ms": 1700000000000,
                "manifest-list": str(tmp_path / "ml.avro"),
                "summary": {"operation": "append"},
                "schema-id": 0,
                "key-id": "k1",
            }
        ],
    )
    _read_v3_metadata_should_error(
        superuser_conn,
        path,
        expected_match="encrypted Iceberg snapshots are not yet supported",
    )


# ---------------------------------------------------------------------------
# 3. v3-only types
# ---------------------------------------------------------------------------


def _v3_metadata_with_type(tmp_path: Path, type_name: str) -> str:
    """Build a v3 metadata.json whose only column has the given Iceberg type."""
    return _make_v3_metadata(
        tmp_path,
        schemas=[
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "c", "required": False, "type": type_name},
                ],
            }
        ],
        **{"last-column-id": 1},
    )


@pytest.mark.parametrize(
    "iceberg_type,expected_match",
    [
        ("unknown", r"\"unknown\" is not yet supported"),
        ("variant", r"\"variant\" is not yet supported"),
        ("geometry", r"\"geometry\" is not yet supported"),
        ("geography", r"\"geography\" is not yet supported"),
        ("timestamp_ns", r"\"timestamp_ns\" is not yet supported"),
        ("timestamptz_ns", r"\"timestamptz_ns\" is not yet supported"),
    ],
)
def test_v3_only_type_errors_per_type(
    create_reserialize_helper_functions,
    superuser_conn,
    tmp_path,
    iceberg_type,
    expected_match,
):
    path = _v3_metadata_with_type(tmp_path, iceberg_type)
    _read_v3_metadata_should_error(
        superuser_conn,
        path,
        expected_match=expected_match,
    )

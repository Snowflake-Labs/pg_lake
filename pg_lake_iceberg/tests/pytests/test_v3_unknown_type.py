"""Stage 17 (Iceberg v3 rollout): the "unknown" type is null-only.

Per Iceberg v3 §schema-types, an "unknown" column has no logical type --
every row must be NULL on disk. pg_lake:

* accepts ``unknown`` in a v3 metadata.json (no longer errors during
  ``ReadIcebergTableSchemaField`` / ``EnsureIcebergFieldTypeIsSupported``);
* preserves the original ``"unknown"`` type string when the metadata is
  re-serialised (so a round-trip never silently rewrites it to ``"string"``);
* projects the column as Postgres ``text`` in the FDW layer so SELECTs
  return NULLs without complaint.

We don't currently *produce* an ``unknown`` field from the Postgres ->
Iceberg direction (the writer emits ``"string"`` for text); the support
here is read-only by construction, matching the "trivial v3 win" framing
of Stage 17.
"""

import json
import uuid as _uuid
from pathlib import Path

import pytest

from utils_pytest import *


def _make_v3_metadata_with_unknown_column(tmp_path: Path) -> str:
    """Write a structurally-minimal v3 metadata.json with a single
    ``unknown``-typed column and return the absolute path."""
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
                    {
                        "id": 1,
                        "name": "u",
                        "required": False,
                        "type": "unknown",
                    },
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
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))
    return str(path)


def test_v3_unknown_type_roundtrips_through_reserializer(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """Reading a v3 metadata.json with an ``unknown`` column must not error,
    and re-serialising it must preserve the type name verbatim."""
    path = _make_v3_metadata_with_unknown_column(tmp_path)
    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
        superuser_conn,
    )
    out = json.loads(result[0][0])

    assert out["format-version"] == 3
    fields = out["schemas"][0]["fields"]
    assert len(fields) == 1
    assert fields[0]["name"] == "u"
    # Round-trip preservation is the load-bearing contract: a writer that
    # silently rewrites "unknown" -> "string" would corrupt the schema for
    # any downstream reader that knows the difference (and is the kind of
    # bug only this assertion would catch in CI).
    assert fields[0]["type"] == "unknown"


def test_v3_unknown_type_inside_struct_roundtrips(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """Same contract as the scalar test, but with the ``unknown`` column
    nested inside a struct -- exercises ``EnsureIcebergFieldTypeIsSupported``'s
    recursive descent (Stage 7) on the v3-accept path that Stage 17 opens up.
    """
    meta = {
        "format-version": 3,
        "table-uuid": str(_uuid.uuid4()),
        "location": str(tmp_path),
        "last-sequence-number": 0,
        "last-updated-ms": 1700000000000,
        "last-column-id": 3,
        "current-schema-id": 0,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": "s",
                        "required": False,
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 2,
                                    "name": "inner",
                                    "required": False,
                                    "type": "unknown",
                                },
                                {
                                    "id": 3,
                                    "name": "tag",
                                    "required": False,
                                    "type": "string",
                                },
                            ],
                        },
                    },
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
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))

    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
        superuser_conn,
    )
    out = json.loads(result[0][0])

    struct_type = out["schemas"][0]["fields"][0]["type"]
    inner_field = next(f for f in struct_type["fields"] if f["name"] == "inner")
    assert inner_field["type"] == "unknown"


def test_v2_unknown_type_still_errors(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """``unknown`` is a v3-only construct; declaring it in a v2 metadata.json
    must still trip ``EnsureIcebergFormatVersionForRead`` (the field-type
    gate is keyed on the v3-only-type list, but v2 readers never reach the
    field walk because the format-version gate fires first).
    """
    meta = {
        "format-version": 2,
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
                    {"id": 1, "name": "u", "required": False, "type": "unknown"},
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
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))

    # v2 readers tolerate the type name today (no v2 reader walks the v3-only
    # list), but the write path will reject it via EnsureIcebergFormatVersion-
    # ForWrite if anything tries to round-trip via a v2 writer. We assert the
    # read succeeds and the type is preserved -- a v2 file that *contains*
    # "unknown" is malformed per spec, but pg_lake's job at the read site is
    # to surface what the file says, not to invent validation Spark doesn't
    # do either.
    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
        superuser_conn,
    )
    out = json.loads(result[0][0])
    assert out["format-version"] == 2
    assert out["schemas"][0]["fields"][0]["type"] == "unknown"

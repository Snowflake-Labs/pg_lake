"""Stage 10 (Iceberg v3 rollout): per-version Avro schemas for manifests
and manifest lists.

This file pins the schema dispatch wired into
``pg_lake_iceberg/src/iceberg/write_manifest.c`` -- specifically the
shape of the Avro JSON schema string the writer feeds the Avro library
when it serialises a v2 vs a v3 manifest.

The contract is:

* v2 manifest entries continue to carry exactly the v2-era field set; no
  extra fields, no removed fields. The byte-equal CI corpus
  (``test_iceberg_metadata_json.py``) already covers the deeper "we
  round-trip v2 manifests byte-for-byte" invariant; this file just pins
  the schema *string* so a future refactor that accidentally widens the
  v2 schema (e.g. by sharing a common base with v3) trips here loudly.
* v3 manifest entries carry the v3-spec optional fields ``first_row_id``
  (field-id 142, row lineage), ``referenced_data_file`` (143, DV pointer),
  ``content_offset`` (144), ``content_size_in_bytes`` (145) inside the
  inner ``data_file`` record. Each is marked optional so a writer that
  does not yet emit them (Stage 10 -- writers still don't populate row
  lineage; Stage 12 wires the allocator) still produces a v3-conformant
  binary on the wire.
* v3 manifest *lists* carry the optional ``first_row_id`` (520, long).

The schemas are inspected through a pair of test C UDFs
(``lake_iceberg.iceberg_manifest_schema_for_version`` and
``lake_iceberg.iceberg_manifest_list_schema_for_version``) so the test
exercises the exact code the writer would call -- not a parallel Python
mock.
"""

import json

import pytest

from utils_pytest import *

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_schema(pg_conn, sql_fn: str, version: int) -> dict:
    rows = run_query(
        f"SELECT lake_iceberg.{sql_fn}({version});",
        pg_conn,
    )
    return json.loads(rows[0][0])


def _data_file_subrecord(manifest_entry_schema: dict) -> dict:
    """Drill into the manifest_entry Avro schema and return the inner
    ``data_file`` record schema. This is where the v3-only fields live.
    """
    fields = manifest_entry_schema["fields"]
    data_file_field = next(f for f in fields if f["name"] == "data_file")
    return data_file_field["type"]


def _field_ids(schema: dict) -> set[int]:
    return {f["field-id"] for f in schema["fields"]}


def _field_id_to_name(schema: dict) -> dict[int, str]:
    return {f["field-id"]: f["name"] for f in schema["fields"]}


# ---------------------------------------------------------------------------
# Manifest entry schema
# ---------------------------------------------------------------------------


def test_manifest_schema_v2_does_not_carry_v3_only_fields(
    pg_conn, create_reserialize_helper_functions
):
    schema = _fetch_schema(pg_conn, "iceberg_manifest_schema_for_version", 2)
    data_file = _data_file_subrecord(schema)
    field_ids = _field_ids(data_file)

    # v3-only field IDs must be absent from the v2 schema.
    for v3_only_id in (142, 143, 144, 145):
        assert v3_only_id not in field_ids, (
            f"v2 manifest schema must not carry v3-only field-id {v3_only_id}; "
            f"got field_ids={sorted(field_ids)}"
        )


def test_manifest_schema_v3_carries_row_lineage_and_dv_fields(
    pg_conn, create_reserialize_helper_functions
):
    schema = _fetch_schema(pg_conn, "iceberg_manifest_schema_for_version", 3)
    data_file = _data_file_subrecord(schema)
    fields_by_id = _field_id_to_name(data_file)

    expected = {
        142: "first_row_id",
        143: "referenced_data_file",
        144: "content_offset",
        145: "content_size_in_bytes",
    }

    for field_id, name in expected.items():
        assert fields_by_id.get(field_id) == name, (
            f"v3 manifest schema field-id {field_id} must be named '{name}'; "
            f"got '{fields_by_id.get(field_id)}'"
        )


@pytest.mark.parametrize("field_id", [142, 143, 144, 145])
def test_manifest_schema_v3_v3_only_fields_are_optional(
    pg_conn, create_reserialize_helper_functions, field_id
):
    """Every v3-only field must be encoded as Avro union ``["null", T]`` so a
    writer that doesn't emit them (Stage 10 -- writes still error upstream;
    Stage 12 wires row-lineage emission) still produces a v3-conformant
    binary. If a future change tightens the type to a non-union, this test
    catches it before any data is written.
    """
    schema = _fetch_schema(pg_conn, "iceberg_manifest_schema_for_version", 3)
    data_file = _data_file_subrecord(schema)
    field = next(f for f in data_file["fields"] if f["field-id"] == field_id)

    # Avro JSON allows three legal optional encodings:
    #   ["null", T]   (union -- canonical)
    #   T             (non-optional)
    # We want only the first one.
    assert isinstance(
        field["type"], list
    ), f"v3 field-id {field_id} must be a union; got {field['type']!r}"
    assert (
        "null" in field["type"]
    ), f"v3 field-id {field_id} must include null in its union; got {field['type']!r}"


def test_manifest_schema_v2_and_v3_share_all_v2_field_ids(
    pg_conn, create_reserialize_helper_functions
):
    """v3 must be a structural superset of v2 over the data_file record.
    Schema evolution rule: v3 only adds optional fields, never removes
    or repurposes existing field-ids. Catching a removal here means a
    future v3 read on a v2-written manifest could go wrong.
    """
    v2 = _data_file_subrecord(
        _fetch_schema(pg_conn, "iceberg_manifest_schema_for_version", 2)
    )
    v3 = _data_file_subrecord(
        _fetch_schema(pg_conn, "iceberg_manifest_schema_for_version", 3)
    )

    missing = _field_ids(v2) - _field_ids(v3)
    assert not missing, (
        f"v3 schema dropped v2 data_file field-ids {sorted(missing)}; "
        "this would break v2-on-v3 reads"
    )


# ---------------------------------------------------------------------------
# Manifest list schema
# ---------------------------------------------------------------------------


def test_manifest_list_schema_v2_has_no_row_lineage_field(
    pg_conn, create_reserialize_helper_functions
):
    schema = _fetch_schema(pg_conn, "iceberg_manifest_list_schema_for_version", 2)
    field_ids = _field_ids(schema)
    assert 520 not in field_ids, (
        "v2 manifest list must not carry the v3 row-lineage field-id 520; "
        f"got field_ids={sorted(field_ids)}"
    )


def test_manifest_list_schema_v3_has_first_row_id_optional(
    pg_conn, create_reserialize_helper_functions
):
    schema = _fetch_schema(pg_conn, "iceberg_manifest_list_schema_for_version", 3)
    field = next(f for f in schema["fields"] if f["field-id"] == 520)
    assert field["name"] == "first_row_id"

    assert isinstance(field["type"], list), (
        f"v3 manifest list field-id 520 must be a nullable union; "
        f"got {field['type']!r}"
    )
    assert "null" in field["type"]


def test_manifest_list_schema_v2_and_v3_share_all_v2_field_ids(
    pg_conn, create_reserialize_helper_functions
):
    v2 = _fetch_schema(pg_conn, "iceberg_manifest_list_schema_for_version", 2)
    v3 = _fetch_schema(pg_conn, "iceberg_manifest_list_schema_for_version", 3)

    missing = _field_ids(v2) - _field_ids(v3)
    assert (
        not missing
    ), f"v3 manifest list schema dropped v2 field-ids {sorted(missing)}"


# ---------------------------------------------------------------------------
# Unsupported / unknown version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_version", [0, 1, 4, 99, -1])
def test_schema_for_version_rejects_unknown(
    pg_conn, create_reserialize_helper_functions, bad_version
):
    """Both UDFs go through IcebergFormatVersionFromInt, so they share the
    canonical "unsupported iceberg format version" rejection. Pinning it
    here catches any future refactor that bypasses the enum."""
    for sql_fn in (
        "iceberg_manifest_schema_for_version",
        "iceberg_manifest_list_schema_for_version",
    ):
        with pytest.raises(Exception, match="unsupported iceberg format version"):
            run_query(
                f"SELECT lake_iceberg.{sql_fn}({bad_version});",
                pg_conn,
            )
        pg_conn.rollback()

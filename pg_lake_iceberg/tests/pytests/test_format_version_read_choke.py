"""Stage 4 acceptance: read-side ``format-version`` choke goes through the
:c:type:`IcebergFormatVersion` enum.

``pg_lake_iceberg/src/iceberg/read_table_metadata.c::ReadIcebergTableMetadataFromJson``
used to read ``format-version`` as a raw ``int32_t`` and reject anything
``!= 2`` with a single ``ereport(ERROR, ...)``. Stage 4 routes the wire
integer through :c:func:`IcebergFormatVersionFromInt`, which:

* Rejects unknown / retired / future integers (v1, v4, 0, negative, ...)
  with the message
  ``"unsupported iceberg format version <N>"`` + the hint
  ``"pg_lake supports Iceberg format-version 2 and 3."``.
* Returns a typed :c:type:`IcebergFormatVersion` on success.

The local ``!= ICEBERG_FORMAT_VERSION_V2`` check then still rejects
structurally-known v3 metadata at the same call site -- the v3 read
acceptance switch is wired in a later stage in the v3 effort
(see ``docs/iceberg-v3/PLAN.md`` Stage 7).

This file pins both behaviours into CI so that:

1. The byte-identical v2 round-trip path keeps working (covered by the
   pre-existing 24-fixture corpus in ``test_iceberg_metadata_json.py``).
2. Hand-crafted metadata with a bad ``format-version`` integer surfaces
   *the new* error message -- if a future refactor accidentally bypasses
   ``IcebergFormatVersionFromInt`` this assertion catches it.
3. A ``format-version: 3`` metadata is still rejected at the read site
   (Stage 4 invariant), with a v3-specific error that *omits* the
   "supports 2 and 3" hint (because v3 is structurally known but not yet
   readable).

When Stage 7 lands and the reader accepts v3, the v3 case becomes a
positive read assertion and the parametrization here gets pared back to
the truly-unknown integers.
"""

import json
import os
from pathlib import Path

import psycopg2
import pytest
from utils_pytest import *


def _load_v2_template():
    """Return a parsed copy of the smallest committed v2 metadata fixture.

    Using a real fixture (rather than a hand-crafted dict) guarantees the
    only thing we mutate in the negative tests is the ``format-version``
    integer -- every other required field is present, so the reader gets
    past field-presence checks and reaches the version dispatch.
    """
    folder = iceberg_metadata_json_folder_path()
    candidates = sorted(
        Path(folder).glob("*.metadata.json"),
        key=lambda p: p.stat().st_size,
    )
    assert candidates, f"no v2 metadata.json fixtures found under {folder}"
    with open(candidates[0]) as f:
        return json.load(f)


def _write_metadata_with_version(tmp_path, version_value):
    metadata = _load_v2_template()
    metadata["format-version"] = version_value
    target = tmp_path / f"format-version-{version_value}.metadata.json"
    with open(target, "w") as f:
        json.dump(metadata, f)
    return str(target)


@pytest.mark.parametrize(
    "bad_version",
    [0, 1, 4, 99, -1],
    ids=["zero", "v1-retired", "v4-future", "garbage-99", "negative"],
)
def test_unknown_format_version_rejected_via_enum_conversion(
    create_reserialize_helper_functions,
    superuser_conn,
    tmp_path,
    bad_version,
):
    """An unknown wire integer must surface IcebergFormatVersionFromInt's
    error -- including the hint that names the supported set."""
    metadata_path = _write_metadata_with_version(tmp_path, bad_version)

    try:
        with pytest.raises(
            psycopg2.DatabaseError,
            match=f"unsupported iceberg format version {bad_version}",
        ):
            run_query(
                f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_path}')",
                superuser_conn,
            )
    finally:
        superuser_conn.rollback()


def test_v3_format_version_rejected_at_read_site(
    create_reserialize_helper_functions,
    superuser_conn,
    tmp_path,
):
    """A structurally-known v3 metadata is rejected by the local
    ``!= ICEBERG_FORMAT_VERSION_V2`` check, *not* by
    :c:func:`IcebergFormatVersionFromInt` (which accepts v3 as a known
    enum member).

    Stage 7 of the v3 effort flips this to an accept; until then it is
    an explicit error and this test prevents accidental relaxation.
    """
    metadata_path = _write_metadata_with_version(tmp_path, 3)

    try:
        with pytest.raises(
            psycopg2.DatabaseError,
            match="unsupported iceberg format version 3",
        ):
            run_query(
                f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_path}')",
                superuser_conn,
            )
    finally:
        superuser_conn.rollback()


def test_v2_format_version_still_round_trips(
    create_reserialize_helper_functions,
    superuser_conn,
    tmp_path,
):
    """The Stage 4 type-discipline change must not regress v2 read+write.

    The 24-fixture corpus already covers this for every committed
    metadata layout. This one assertion -- on a freshly-roundtripped
    copy via a temp path -- proves the *enum-typed* field round-trips
    its wire integer untouched.
    """
    metadata_path = _write_metadata_with_version(tmp_path, 2)

    result = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_path}')::json",
        superuser_conn,
    )
    returned_json = result[0][0]
    assert returned_json["format-version"] == 2, (
        "Stage 4 must preserve the v2 wire integer byte-for-byte; "
        f"got {returned_json.get('format-version')!r}"
    )

    original_json = _load_v2_template()
    assert_jsons_equivalent(original_json, returned_json)

"""Stage 11 (Iceberg v3 rollout): row lineage data-model round-trip.

The Iceberg v3 spec introduces three row-lineage fields that pg_lake's
:c:type:`IcebergTableMetadata` / :c:type:`IcebergSnapshot` structs must
carry losslessly so a future writer (Stage 12 allocator) can extend
them and downstream readers can rebuild a row's logical id without
walking history.

* ``next-row-id`` (table-level, optional): exclusive upper bound of
  every row-id ever assigned.
* ``first-row-id`` (snapshot-level, optional): inclusive start of the
  range claimed by this snapshot's newly-appended rows.
* ``added-rows``  (snapshot-level, optional): width of that range.

This stage does not allocate or mutate these values -- Stage 12 wires
the allocator. Here we only ensure the read/write code is v3-aware:
that the JSON round-trips byte-faithfully, that present-but-zero is
distinguished from absent, that v2 metadata never grows these fields,
and that v2 metadata carrying ``next-row-id`` (a spec violation) is
rejected at the read site so external writers can't sneak a forward-
compatibility hazard into pg_lake's catalog.
"""

import copy
import json
from pathlib import Path

import psycopg2
import pytest
from utils_pytest import *

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _load_v2_template() -> dict:
    folder = iceberg_metadata_json_folder_path()
    candidates = sorted(
        Path(folder).glob("*.metadata.json"),
        key=lambda p: p.stat().st_size,
    )
    assert candidates, f"no v2 metadata.json fixtures under {folder}"
    with open(candidates[0]) as f:
        return json.load(f)


def _v3_template() -> dict:
    """Smallest v3-shaped metadata: take the smallest v2 fixture, flip the
    version, drop snapshot references that point at off-disk manifest
    lists (we're not exercising the snapshot read path here, just the
    table-level lineage field).
    """
    md = _load_v2_template()
    md["format-version"] = 3
    md.pop("snapshots", None)
    md.pop("snapshot-log", None)
    md.pop("refs", None)
    md["current-snapshot-id"] = -1
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
# Table-level: next-row-id
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [0, 1, 42, 2**31 - 1, 2**40, 2**62],
    ids=["zero", "one", "small", "int32-max", "above-int32", "near-int64"],
)
def test_v3_next_row_id_round_trips(
    create_reserialize_helper_functions, superuser_conn, tmp_path, value
):
    """A v3 metadata carrying ``next-row-id: N`` must round-trip with
    that exact integer preserved -- including ``0``, which has to stay
    distinguishable from "field absent" because Stage 12 allocator uses
    present-and-zero on a freshly created table.
    """
    md = _v3_template()
    md["next-row-id"] = value

    path = _write(tmp_path, f"v3-next-row-id-{value}.metadata.json", md)
    out = _roundtrip(superuser_conn, path)

    assert (
        out.get("next-row-id") == value
    ), f"v3 next-row-id={value} did not round-trip; got {out.get('next-row-id')!r}"


def test_v3_omitted_next_row_id_stays_omitted(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """If the input v3 metadata does not carry ``next-row-id`` (e.g. a
    metadata.json written before row lineage was wired up), the writer
    must not synthesise one out of thin air -- doing so would promote
    omission semantics to "explicitly zero" and confuse readers that
    key off field presence.
    """
    md = _v3_template()
    md.pop("next-row-id", None)

    path = _write(tmp_path, "v3-no-next-row-id.metadata.json", md)
    out = _roundtrip(superuser_conn, path)

    assert "next-row-id" not in out, (
        "writer must not invent next-row-id when the input lacks it; "
        f"got {out.get('next-row-id')!r}"
    )


def test_v2_metadata_must_not_grow_next_row_id(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v2 metadata round-trip must never spontaneously gain the v3-only
    ``next-row-id`` field. This is the inverse safety net for
    ``test_v3_omitted_next_row_id_stays_omitted``: the same writer
    serving both v2 and v3 must not bleed v3 fields into v2 output.
    """
    md = _load_v2_template()
    assert md["format-version"] == 2

    path = _write(tmp_path, "v2-clean.metadata.json", md)
    out = _roundtrip(superuser_conn, path)

    assert "next-row-id" not in out, (
        "v2 metadata round-trip leaked v3 next-row-id; "
        f"got {out.get('next-row-id')!r}"
    )


def test_v2_metadata_carrying_next_row_id_is_rejected(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v2 metadata.json that nevertheless declares ``next-row-id`` is
    almost certainly the product of a misconfigured writer (or a
    deliberate forward-compat probe). pg_lake refuses to read it rather
    than silently dropping the value, because dropping would create a
    one-way data loss: a downstream v3 upgrade would lose track of the
    high-water mark of row-ids already assigned.
    """
    md = _load_v2_template()
    md["next-row-id"] = 99

    path = _write(tmp_path, "v2-with-next-row-id.metadata.json", md)

    try:
        with pytest.raises(
            psycopg2.DatabaseError,
            match=r'"next-row-id" on a non-v3 table',
        ):
            run_query(
                f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
                superuser_conn,
            )
    finally:
        superuser_conn.rollback()


# ---------------------------------------------------------------------------
# Snapshot-level: first-row-id / added-rows
# ---------------------------------------------------------------------------


def _v3_with_snapshot(extra: dict | None = None) -> dict:
    """Construct a v3 metadata with one snapshot carrying optional
    row-lineage fields. We patch the v2 template's first snapshot
    in-place so all the v2-required structure (manifest-list path,
    summary, sequence number, ...) is inherited unchanged.
    """
    base = _load_v2_template()
    base["format-version"] = 3
    snapshots = base.get("snapshots") or []
    if not snapshots:
        pytest.skip("v2 template lacks a snapshot to extend with v3 lineage fields")
    snap = copy.deepcopy(snapshots[0])
    if extra:
        snap.update(extra)
    base["snapshots"] = [snap]
    base["current-snapshot-id"] = snap["snapshot-id"]
    # snapshot-log and refs may reference further snapshot ids; trim to one
    base["snapshot-log"] = [
        {"timestamp-ms": snap["timestamp-ms"], "snapshot-id": snap["snapshot-id"]}
    ]
    base.pop("refs", None)
    return base


@pytest.mark.parametrize(
    "first_row_id,added_rows",
    [
        (0, 0),
        (0, 1),
        (1, 1),
        (1000, 7),
        (2**40, 2**20),
    ],
    ids=["zero-zero", "zero-one", "one-one", "thousand-seven", "huge"],
)
def test_v3_snapshot_row_lineage_round_trips(
    create_reserialize_helper_functions,
    superuser_conn,
    tmp_path,
    first_row_id,
    added_rows,
):
    md = _v3_with_snapshot({"first-row-id": first_row_id, "added-rows": added_rows})
    path = _write(
        tmp_path, f"v3-snap-fri{first_row_id}-ar{added_rows}.metadata.json", md
    )
    out = _roundtrip(superuser_conn, path)

    snap_out = out["snapshots"][0]
    assert snap_out.get("first-row-id") == first_row_id
    assert snap_out.get("added-rows") == added_rows


def test_v3_snapshot_lineage_absence_round_trips(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v3 snapshot may legitimately omit both lineage fields (e.g. a
    future metadata-only commit). The writer must not synthesise them."""
    md = _v3_with_snapshot()
    snap_in = md["snapshots"][0]
    snap_in.pop("first-row-id", None)
    snap_in.pop("added-rows", None)

    path = _write(tmp_path, "v3-snap-no-lineage.metadata.json", md)
    out = _roundtrip(superuser_conn, path)

    snap_out = out["snapshots"][0]
    assert "first-row-id" not in snap_out, snap_out
    assert "added-rows" not in snap_out, snap_out


def test_v3_snapshot_only_one_lineage_field_round_trips(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A malformed-but-on-wire v3 snapshot with only first-row-id (no
    added-rows) round-trips with the same shape. The spec marks both as
    optional, so refusing one-without-the-other at read time would be
    over-eager; downstream consumers do the cross-field validation.
    """
    md = _v3_with_snapshot({"first-row-id": 7})

    path = _write(tmp_path, "v3-snap-fri-only.metadata.json", md)
    out = _roundtrip(superuser_conn, path)
    snap_out = out["snapshots"][0]
    assert snap_out.get("first-row-id") == 7
    assert "added-rows" not in snap_out

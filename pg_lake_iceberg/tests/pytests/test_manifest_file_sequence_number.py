"""External-reader check for a manifest entry's file_sequence_number.

metadata_operations.c and write_manifest.c populate the field per data file
(it used to be hard-coded null/absent) and no longer write sequence_number into
it. Nothing else in the suite reads the field back, so this reads it two ways:
straight out of the Avro with fastavro, and through pyiceberg, which is the
reader an outside consumer would use.
"""

import io
import json

import fastavro
import pytest
from pyiceberg.table import StaticTable
from utils_pytest import *

TABLE = "manifest_file_seq"


def _avro(s3, uri):
    bucket, key = parse_s3_path(uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return list(fastavro.reader(io.BytesIO(body)))


@pytest.mark.location_prefix(f"s3://{TEST_BUCKET}/manifest_file_seq")
def test_file_sequence_number_is_written_per_file(
    pg_conn, extension, with_default_location, s3
):
    # three separate commits, so three snapshots with sequence numbers 1..3
    run_command(f"CREATE TABLE {TABLE} (id int) USING iceberg", pg_conn)
    pg_conn.commit()
    for i in (1, 2, 3):
        run_command(f"INSERT INTO {TABLE} VALUES ({i})", pg_conn)
        pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE}'",
        pg_conn,
    )[0][0]

    snapshots = run_query(
        f"SELECT sequence_number, snapshot_id, manifest_list "
        f"FROM lake_iceberg.snapshots('{metadata_location}') ORDER BY sequence_number",
        pg_conn,
    )
    assert [r["sequence_number"] for r in snapshots] == [1, 2, 3], snapshots
    seq_of_snapshot = {r["snapshot_id"]: r["sequence_number"] for r in snapshots}

    # ---- read 1: the raw Avro, via the newest snapshot's manifest list
    manifests = _avro(s3, snapshots[-1]["manifest_list"])
    assert manifests, "newest snapshot has no manifests"

    entries = []
    for m in manifests:
        for e in _avro(s3, m["manifest_path"]):
            entries.append((m, e))

    assert len(entries) == 3, [e["data_file"]["file_path"] for _, e in entries]

    for m, e in entries:
        fsn = e.get("file_sequence_number")
        assert fsn is not None, f"file_sequence_number absent: {e}"
        # each entry must carry the sequence number of the snapshot that added
        # the file, which is also what the manifest list records for it
        assert fsn == m["sequence_number"], (fsn, m["sequence_number"])
        if e.get("snapshot_id") is not None:
            assert fsn == seq_of_snapshot[e["snapshot_id"]], (fsn, e["snapshot_id"])

    # the three files must not all be stamped with the newest sequence number
    assert sorted(e.get("file_sequence_number") for _, e in entries) == [1, 2, 3]

    # ---- read 2: pyiceberg's own interpretation of the same manifests.
    # The SqlCatalog fixture only knows tables created through it, and pg_lake
    # registered this one itself, so point StaticTable at the metadata directly
    # rather than looking it up by name. Read the manifest entries rather than
    # inspect.entries(), which needs column_sizes that pg_lake does not write.
    static = StaticTable.from_metadata(
        metadata_location,
        properties={
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
        },
    )
    io = static.io
    snapshot = static.metadata.current_snapshot()
    pyi = [
        entry
        for manifest in snapshot.manifests(io)
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=False)
    ]
    assert sorted(e.sequence_number for e in pyi) == [1, 2, 3], pyi
    assert sorted(e.file_sequence_number for e in pyi) == [1, 2, 3], pyi

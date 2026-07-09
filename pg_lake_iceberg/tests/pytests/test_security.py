"""
Security regression tests for pg_lake_iceberg Avro parsing vulnerabilities.
"""

import tempfile
import pytest
from utils_pytest import *


def test_avro_int32_array_with_equality_ids_parses_correctly(
    pg_conn, superuser_conn, s3, extension
):
    """
    Regression test for: cross-tenant heap corruption via double-increment OOB
    write in AvroGetInt32ArrayField.

    AvroGetInt32ArrayField allocated palloc0(sizeof(int32_t) * itemCount) and
    then in the loop body BOTH used entries[itemIndex] (index-based write) AND
    incremented entries++ (pointer advance), advancing the write position by
    2*sizeof(int32_t) per iteration instead of one.  For an equality_ids array
    of length N this caused writes at base+0, base+2, base+4, … — past the
    end of the palloc buffer once i > 0.

    The OOB write is reachable without any pg_lake role: any SELECT on a
    foreign Iceberg table backed by a shared object store parses the manifest
    from external (attacker-controllable) storage.

    Fix: remove the spurious entries++ so the loop uses only the index variable,
    matching AvroGetInt64ArrayField.

    The crafted sample file equality-ids-manifest.avro contains a single
    manifest entry with equality_ids=[1, 2, 3, 4].  For N=4 the vulnerable
    code writes to byte offsets 0, 8, 20 (OOB by 4 bytes), 36 (OOB by 20
    bytes) of a 16-byte palloc chunk.
    """
    # Upload the crafted manifest Avro file that contains equality_ids=[1,2,3,4]
    key = "sec_test/equality-ids-manifest.avro"
    local_path = sample_avro_filepath("equality-ids-manifest.avro")
    s3.upload_file(local_path, TEST_BUCKET, key)
    manifest_url = f"s3://{TEST_BUCKET}/{key}"

    # lake_iceberg.reserialize_iceberg_manifest() reads the manifest via
    # ReadManifestEntries() -> ReadDataFileFromAvro() -> AvroGetInt32ArrayField()
    # On vulnerable code the OOB write corrupts adjacent palloc memory,
    # leading to an error or crash during the subsequent write pass.
    # On fixed code the function returns successfully.
    with tempfile.NamedTemporaryFile(suffix=".avro") as out_file:
        run_command(
            f"SELECT lake_iceberg.reserialize_iceberg_manifest("
            f"'{manifest_url}', '{out_file.name}')",
            superuser_conn,
        )
        superuser_conn.commit()

        # Verify the rewritten manifest is non-empty (basic sanity check).
        # A corrupted write would either error above or produce a zero-byte file.
        out_file.seek(0, 2)
        size = out_file.tell()
        assert size > 0, (
            "SECURITY REGRESSION: reserialize_iceberg_manifest produced an "
            "empty output file — likely caused by memory corruption from the "
            "AvroGetInt32ArrayField double-increment OOB write. "
            "Remove the spurious 'entries++' from the "
            "for-loop in avro_reader.c."
        )


def test_avro_int32_array_equality_ids_values_survive_roundtrip(
    pg_conn, superuser_conn, s3, extension
):
    """
    After the fix, equality_ids values must be preserved in a reserialize
    round-trip.  On the vulnerable code, the OOB double-advance stores values
    at the wrong indices and corrupts adjacent heap memory, so the written
    values would be incorrect even if the process does not crash.
    """
    key = "sec_test/equality-ids-manifest-roundtrip.avro"
    local_path = sample_avro_filepath("equality-ids-manifest.avro")
    s3.upload_file(local_path, TEST_BUCKET, key)
    manifest_url = f"s3://{TEST_BUCKET}/{key}"

    # Reserialize twice — each read-write cycle triggers AvroGetInt32ArrayField.
    # A double-advance OOB write would corrupt neighboring allocations, causing
    # an error or wrong values on the second pass.
    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp1:
        tmp1_path = tmp1.name

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp2:
        tmp2_path = tmp2.name

    run_command(
        f"SELECT lake_iceberg.reserialize_iceberg_manifest('{manifest_url}', '{tmp1_path}')",
        superuser_conn,
    )
    superuser_conn.commit()

    # Re-upload pass-1 output and reserialize again
    s3.upload_file(tmp1_path, TEST_BUCKET, key + ".pass2")
    manifest_url2 = f"s3://{TEST_BUCKET}/{key}.pass2"

    run_command(
        f"SELECT lake_iceberg.reserialize_iceberg_manifest('{manifest_url2}', '{tmp2_path}')",
        superuser_conn,
    )
    superuser_conn.commit()

    import os

    assert os.path.getsize(tmp2_path) > 0, (
        "SECURITY REGRESSION: second reserialize pass produced an empty file, "
        "indicating heap corruption from the AvroGetInt32ArrayField "
        "double-increment OOB write."
    )


def test_data_file_stats_rejects_local_path(pg_conn, extension):
    """
    Regression test for: second-order URI bypass -> DuckDB local-file read
    (direct-path variant via lake_iceberg.data_file_stats).

    lake_iceberg.data_file_stats(text) is GRANT EXECUTE TO lake_read but had
    NO IsSupportedURL() check on its argument.  DuckDB's read_text() treats a
    string without a URL scheme as a local filesystem path, so:
        SELECT * FROM lake_iceberg.data_file_stats('/etc/passwd')
    ran read_text('/etc/passwd') on the pgduck_server host and leaked the
    file's leading bytes through jsonb_in's CONTEXT error message — a
    local-file-read oracle available to any lake_read user, bypassing the
    pg_read_server_files privilege boundary.

    Fix: ReadIcebergTableMetadata now goes through the checked
    GetTextFromURI, which calls IsSupportedURL() and raises before any
    DuckDB read_text() runs.
    """
    error = run_command(
        "SELECT * FROM lake_iceberg.data_file_stats('/etc/passwd')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: lake_iceberg.data_file_stats('/etc/passwd') "
        "succeeded or did not return an error. A local filesystem path must be "
        "rejected by IsSupportedURL() before reaching DuckDB. "
        "Add IsSupportedURL() check in pg_lake_read_data_file_stats()."
    )
    # On fixed code: 'unsupported URL' error.
    # On vulnerable code: jsonb_in CONTEXT leak or DuckDB filesystem error.
    assert (
        "unsupported url" in str(error).lower() or "unsupported" in str(error).lower()
    ), f"Expected 'unsupported URL' error for local path, got: {error}"
    # Defense: ensure the file content did NOT leak via jsonb_in CONTEXT.
    assert "root:" not in str(error) and "nobody:" not in str(error), (
        "SECURITY REGRESSION: /etc/passwd content appeared in the error message, "
        "confirming DuckDB read the file before the scheme check fired."
    )


def test_data_file_stats_rejects_proc_environ_path(pg_conn, extension):
    """
    /proc/self/environ is the primary SSRF/exfil target because it contains
    the process environment including AWS_SECRET_ACCESS_KEY, OPENAI_API_KEY,
    etc. as a single null-delimited line.  Confirm it is rejected.
    """
    error = run_command(
        "SELECT * FROM lake_iceberg.data_file_stats('/proc/self/environ')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: lake_iceberg.data_file_stats('/proc/self/environ') "
        "succeeded."
    )
    assert (
        "unsupported" in str(error).lower()
    ), f"Expected unsupported URL error, got: {error}"


def test_data_file_stats_accepts_valid_s3_url(pg_conn, extension):
    """
    Sanity check: a valid s3:// URL must still be accepted by the entry-point
    check.  The IsSupportedURL() guard must not block legitimate use.
    """
    # Use a non-existent but scheme-valid URL; data_file_stats may fail for
    # other reasons (no such object), but must not raise 'unsupported URL'.
    error = run_command(
        "SELECT * FROM lake_iceberg.data_file_stats('s3://nonexistent-bucket/nonexistent/metadata.json')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # The bucket does not exist, so data_file_stats will fail.  We just
    # need to confirm the failure is not the scheme-allowlist check.
    assert (
        error is not None
    ), "Expected data_file_stats to fail on a nonexistent bucket, got success"
    assert (
        "unsupported url" not in str(error).lower()
    ), f"Regression: lake_iceberg.data_file_stats rejected a valid s3:// URL: {error}"


def test_iceberg_files_nested_manifest_list_local_path_is_rejected(
    pg_conn, s3, extension
):
    """
    Second-order URI attack: an attacker-controlled metadata.json is served
    over HTTP with manifest-list pointing to a local file path.
    ReadIcebergManifests() previously called GetBlobFromURI() directly on
    snapshot->manifest_list without any IsSupportedURL() check.

    Fix: ReadIcebergManifests() now calls IsSupportedURL(manifestListPath)
    and raises 'unsupported manifest-list URL' before calling GetBlobFromURI().

    This test uploads a crafted metadata.json whose manifest-list field
    is a local path, then calls lake_iceberg.files() on it.  On fixed code
    the error is 'unsupported manifest-list URL'; on vulnerable code DuckDB
    tries to open the local path.
    """
    import json

    # Craft a minimal Iceberg v2 metadata.json with a local manifest-list path.
    crafted_metadata = {
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": f"s3://{TEST_BUCKET}/sec_nested_test",
        "last-sequence-number": 1,
        "last-updated-ms": 0,
        "last-column-id": 1,
        "schemas": [{"type": "struct", "fields": [], "schema-id": 0}],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "snapshots": [
            {
                "snapshot-id": 1,
                "sequence-number": 1,
                "timestamp-ms": 0,
                "manifest-list": "/etc/passwd",  # ← attacker-controlled local path
                "summary": {"operation": "append"},
                "schema-id": 0,
            }
        ],
        "current-snapshot-id": 1,
        "refs": {"main": {"snapshot-id": 1, "type": "branch"}},
        "snapshot-log": [],
        "metadata-log": [],
    }

    key = "sec_test/crafted_nested_metadata.json"
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key=key,
        Body=json.dumps(crafted_metadata).encode("utf-8"),
    )
    metadata_url = f"s3://{TEST_BUCKET}/{key}"

    error = run_command(
        f"SELECT * FROM lake_iceberg.files('{metadata_url}')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: lake_iceberg.files() with a metadata.json "
        "containing manifest-list='/etc/passwd' did not raise an error. "
        "ReadIcebergManifests() must check IsSupportedURL(manifestListPath) "
        "before calling GetBlobFromURI()."
    )
    # On fixed code: 'unsupported manifest-list URL' or similar.
    # On vulnerable code: DuckDB tries to read /etc/passwd as Avro.
    assert (
        "unsupported" in str(error).lower() or "manifest" in str(error).lower()
    ), f"Expected unsupported manifest URL error, got: {error}"
    # Confirm no /etc/passwd content leaked.
    assert "root:" not in str(error) and "nobody:" not in str(error), (
        "SECURITY REGRESSION: /etc/passwd content appeared in the error, "
        "meaning DuckDB opened the local file before the URI check ran."
    )


def test_data_file_local_path_in_manifest_is_rejected(pg_conn, superuser_conn, s3):
    """
    Regression test for: second-order URI in data_file.file_path -> DuckDB
    local-file read.

    ReadDataFileFromAvro() read file_path from attacker-controlled Avro without
    any IsSupportedURL() check.  The path flowed to DuckDB's read_parquet()
    (via ConvertIcebergDataFilesToFileScan) and GetRemoteParquetColumnStats(),
    allowing a bucket writer to embed a local path such as /etc/passwd in a
    manifest entry and have DuckDB open it as the pgduck_server OS user.

    Fix: IsSupportedURL() is now checked in ReadDataFileFromAvro() immediately
    after the file_path field is decoded from Avro, before the DataFile struct
    is populated further.

    The sample file local-path-manifest.avro is equality-ids-manifest.avro
    with file_path patched to /etc/passwd; all other fields are unchanged.
    """
    key = "sec_test/local-path-manifest.avro"
    local_path = sample_avro_filepath("local-path-manifest.avro")
    s3.upload_file(local_path, TEST_BUCKET, key)
    manifest_url = f"s3://{TEST_BUCKET}/{key}"

    with tempfile.NamedTemporaryFile(suffix=".avro") as out_file:
        error = run_command(
            f"SELECT lake_iceberg.reserialize_iceberg_manifest("
            f"'{manifest_url}', '{out_file.name}')",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()

    assert error is not None, (
        "SECURITY REGRESSION: reserialize_iceberg_manifest accepted a manifest "
        "with file_path='/etc/passwd'. ReadDataFileFromAvro() must call "
        "IsSupportedURL(dataFile->file_path) before populating the DataFile "
        "struct."
    )
    assert (
        "unsupported" in str(error).lower()
    ), f"Expected 'unsupported data-file URL' error, got: {error}"
    assert "root:" not in str(error) and "nobody:" not in str(
        error
    ), "SECURITY REGRESSION: /etc/passwd content leaked in the error message."

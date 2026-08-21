"""Reading Iceberg v3 tables.

pg_lake writes v2 and will keep doing so.  What changed is that it no
longer refuses to *read* a table just because the catalog stamped it
v3: v3 metadata is a superset of v2, so a table that uses no v3-only
feature reads exactly like a v2 one, and catalogs have started moving
their default (Snowflake Horizon among them).

The v3-only features we cannot honor are rejected where they appear
instead of at the version check.  The one that matters for correctness
is deletion vectors: they arrive as position deletes carried in a Puffin
blob, and treating that path as Parquet would mean returning rows the
table considers deleted.

No tool available here writes v3 -- pyiceberg refuses outright and the
Spark runtime the suite pins predates v3 -- so these fixtures take a
real v2 table produced by pyiceberg and edit the metadata and manifests
into the v3 shapes under test.  That covers the version gate, the
manifest field the v3 writers drop, and the deletion-vector guard.  It
does not cover v3-only column types or a genuine Puffin blob.
"""

import io
import json

import fastavro
import pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField, StringType

from utils_pytest import *


# ---------------------------------------------------------------------------
# S3 / Avro helpers
# ---------------------------------------------------------------------------


def _get(s3, uri):
    bucket, key = parse_s3_path(uri)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def _put(s3, uri, body):
    bucket, key = parse_s3_path(uri)
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def _rewrite_manifest(s3, uri, schema_fn=None, record_fn=None):
    """Rewrite an Avro manifest in place, editing its schema and/or records.

    The Iceberg metadata carried in the Avro header (schema, partition
    spec, ...) is copied over, since the reader is entitled to rely on it.
    """
    reader = fastavro.reader(io.BytesIO(_get(s3, uri)))
    writer_schema = reader.writer_schema
    header = {k: v for k, v in reader.metadata.items() if not k.startswith("avro.")}
    records = list(reader)

    if schema_fn is not None:
        writer_schema = schema_fn(writer_schema)
    if record_fn is not None:
        records = [record_fn(r) for r in records]

    buf = io.BytesIO()
    fastavro.writer(buf, writer_schema, records, metadata=header)

    _put(s3, uri, buf.getvalue())


def _data_file_record(schema):
    """The data_file record inside a manifest_entry schema."""
    for field in schema["fields"]:
        if field["name"] != "data_file":
            continue
        field_type = field["type"]
        if isinstance(field_type, dict):
            return field_type
        return next(t for t in field_type if isinstance(t, dict))
    raise AssertionError("no data_file field in manifest schema")


def _manifest_paths(table):
    snapshot = table.current_snapshot()
    return [m.manifest_path for m in snapshot.manifests(table.io)]


# ---------------------------------------------------------------------------
# Fixture table
# ---------------------------------------------------------------------------


def _make_table(iceberg_catalog, name):
    identifier = f"public.{name}"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    # Unpartitioned on purpose: rewriting a manifest round-trips the Avro
    # schema, and partition field ids are the one thing that would not
    # survive it faithfully.
    schema = Schema(
        NestedField(1, "city", StringType(), required=False),
        NestedField(2, "lat", DoubleType(), required=False),
    )
    table = iceberg_catalog.create_table(
        identifier=identifier,
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/{name}",
    )
    table.append(
        pyarrow.Table.from_pylist(
            [
                {"city": "Amsterdam", "lat": 52.371807},
                {"city": "Istanbul", "lat": 41.091242},
            ],
        )
    )
    return table


def _data_files(conn, metadata_location, raise_error=True):
    # The connection is shared across the module and several of these reads
    # are expected to fail, so clear any failed transaction on both sides of
    # the query rather than leaving the next test to trip over it.
    conn.rollback()
    try:
        return run_query(
            f"SELECT path FROM lake_iceberg.data_file_stats('{metadata_location}')",
            conn,
            raise_error=raise_error,
        )
    finally:
        conn.rollback()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_v3_metadata_is_readable(
    superuser_conn, s3, iceberg_catalog, iceberg_extension, installcheck
):
    """A table stamped format-version 3 is read rather than refused."""
    if installcheck:
        return

    table = _make_table(iceberg_catalog, "v3_version_gate")

    metadata = json.loads(_get(s3, table.metadata_location))
    assert metadata["format-version"] == 2

    metadata["format-version"] = 3
    v3_location = table.metadata_location.replace(
        ".metadata.json", ".v3-test.metadata.json"
    )
    _put(s3, v3_location, json.dumps(metadata).encode())

    rows = _data_files(superuser_conn, v3_location)
    assert len(rows) > 0


def test_unsupported_format_version_still_refused(
    superuser_conn, s3, iceberg_catalog, iceberg_extension, installcheck
):
    """Accepting v3 did not turn the version gate off altogether."""
    if installcheck:
        return

    table = _make_table(iceberg_catalog, "v3_bad_version")

    metadata = json.loads(_get(s3, table.metadata_location))
    metadata["format-version"] = 4
    bad_location = table.metadata_location.replace(
        ".metadata.json", ".v4-test.metadata.json"
    )
    _put(s3, bad_location, json.dumps(metadata).encode())

    err = _data_files(superuser_conn, bad_location, raise_error=False)
    assert "unsupported iceberg format version 4" in str(err)


def test_manifest_without_sort_order_id_is_readable(
    superuser_conn, s3, iceberg_catalog, iceberg_extension, installcheck
):
    """An optional manifest field may be absent, not just null.

    v3 writers drop sort_order_id from the data_file record entirely.  The
    reader used to require the field to exist even though it is optional
    and already tracked with a has_ flag.
    """
    if installcheck:
        return

    table = _make_table(iceberg_catalog, "v3_no_sort_order")

    def strip_field(schema):
        record = _data_file_record(schema)
        record["fields"] = [f for f in record["fields"] if f["name"] != "sort_order_id"]
        return schema

    def strip_value(record):
        record["data_file"].pop("sort_order_id", None)
        return record

    for manifest in _manifest_paths(table):
        _rewrite_manifest(s3, manifest, schema_fn=strip_field, record_fn=strip_value)

    rows = _data_files(superuser_conn, table.metadata_location)
    assert len(rows) > 0


def test_deletion_vector_is_rejected(
    superuser_conn, s3, iceberg_catalog, iceberg_extension, installcheck
):
    """A Puffin deletion vector fails loudly instead of being read as Parquet.

    Silently ignoring deletes it cannot apply would let the scan return
    rows the table considers deleted, so this must be an error.
    """
    if installcheck:
        return

    table = _make_table(iceberg_catalog, "v3_deletion_vector")

    def as_deletion_vector(record):
        record["data_file"]["content"] = 1
        record["data_file"]["file_format"] = "PUFFIN"
        return record

    for manifest in _manifest_paths(table):
        _rewrite_manifest(s3, manifest, record_fn=as_deletion_vector)

    err = _data_files(superuser_conn, table.metadata_location, raise_error=False)
    assert "deletion vectors are not supported" in str(err)


def test_v3_column_default_is_rejected(
    superuser_conn, s3, iceberg_catalog, iceberg_extension, installcheck
):
    """A column default fails loudly rather than reading back as NULL.

    v3 lets a column be added with a default, and the rows written before
    it was added are supposed to read back as that default.  Nothing on
    the read path applies it, so those rows would come back NULL -- a
    wrong answer rather than a missing feature, which is why the whole
    table is refused instead.
    """
    if installcheck:
        return

    table = _make_table(iceberg_catalog, "v3_column_default")

    metadata = json.loads(_get(s3, table.metadata_location))
    metadata["format-version"] = 3
    for schema in metadata["schemas"]:
        if schema["schema-id"] == metadata["current-schema-id"]:
            schema["fields"][0]["initial-default"] = "unknown"

    default_location = table.metadata_location.replace(
        ".metadata.json", ".v3-default.metadata.json"
    )
    _put(s3, default_location, json.dumps(metadata).encode())

    err = _data_files(superuser_conn, default_location, raise_error=False)
    assert "column default values are not supported" in str(err)
    assert "city" in str(err)

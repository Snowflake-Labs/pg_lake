"""Behavioral coverage for externally produced Iceberg v2 row deletes.

Fastavro/Arrow construct small portable files, independently of pg_lake's
writers. Sequence numbers, field IDs and physical order are deliberately varied.
"""

import json
import math
import time
import uuid
from collections import Counter
from datetime import date, datetime, time as daytime, timedelta, timezone
from decimal import Decimal

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import IntegerType
from utils_pytest import (
    TEST_BUCKET,
    fetch_data_files_used,
    fetch_delete_files_used,
    run_command,
    run_query,
)


FIELDS = [
    (1, "id", "int"),
    (2, "code", "string"),
    (3, "payload", "string"),
    (4, "number", "long"),
]
ARROW_TYPES = {
    "int": pa.int32(),
    "long": pa.int64(),
    "string": pa.string(),
    "double": pa.float64(),
    "binary": pa.binary(),
    "date": pa.date32(),
    "float": pa.float32(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
    "time": pa.time64("us"),
    "decimal(9,2)": pa.decimal128(9, 2),
    "decimal(12,2)": pa.decimal128(12, 2),
    "uuid": pa.uuid(),
}


# Iceberg's fixed manifest field IDs; partition IDs come from each spec.
AVRO_IDS = {
    "status": 0,
    "snapshot_id": 1,
    "data_file": 2,
    "sequence_number": 3,
    "file_sequence_number": 4,
    "content": 134,
    "file_path": 100,
    "file_format": 101,
    "partition": 102,
    "record_count": 103,
    "file_size_in_bytes": 104,
    "equality_ids": 135,
    "lower_bounds": 125,
    "upper_bounds": 128,
    "null_value_counts": 110,
    "key": 126,
    "value": 127,
}
LIST_IDS = {
    "manifest_path": 500,
    "manifest_length": 501,
    "partition_spec_id": 502,
    "content": 517,
    "sequence_number": 515,
    "min_sequence_number": 516,
    "added_snapshot_id": 503,
    "added_files_count": 504,
    "existing_files_count": 505,
    "deleted_files_count": 506,
    "added_rows_count": 512,
    "existing_rows_count": 513,
    "deleted_rows_count": 514,
}


def avro_record(name, fields):
    ids = LIST_IDS if name == "manifest_file" else AVRO_IDS
    return {
        "type": "record",
        "name": name,
        "fields": [
            {"name": n, "type": t, **({"field-id": ids[n]} if n in ids else {})}
            for n, t in fields
        ],
    }


def avro_metric_map(name, value_type, key_id, value_id):
    record = avro_record(name, [("key", "int"), ("value", value_type)])
    for field, field_id in zip(record["fields"], (key_id, value_id)):
        field["field-id"] = field_id
    return {"type": "array", "logicalType": "map", "items": record}


class DeleteTable:
    def __init__(self, directory, s3, fields=None, specs=None):
        self.directory = directory
        self.s3 = s3
        self.prefix = f"equality-deletes/{uuid.uuid4()}"
        self.url = f"s3://{TEST_BUCKET}/{self.prefix}"
        self.fields = list(fields or FIELDS)
        # spec entries: source ID, partition ID, name, transform, Avro type
        self.specs = specs or {0: []}
        self.manifests = []
        self.histories = []
        self.files = []

    def file_url(self, path):
        return f"{self.url}/{path.name}"

    def merge_manifests(self):
        """Use ordinary multi-entry manifests for scale/lifetime coverage."""
        groups = {}
        for manifest in self.manifests:
            key = tuple(
                manifest[k] for k in ("partition_spec_id", "content", "sequence_number")
            )
            groups.setdefault(key, []).append(manifest)
        self.manifests = []
        for manifests in groups.values():
            records = []
            for manifest in manifests:
                path = self.directory / manifest["manifest_path"].split("/")[-1]
                with path.open("rb") as source:
                    reader = fastavro.reader(source)
                    schema = reader.writer_schema
                    records.extend(reader)
                path.unlink()
            merged = dict(manifests[0])
            path = self.directory / merged["manifest_path"].split("/")[-1]
            with path.open("wb") as output:
                fastavro.writer(output, schema, records)
            merged["manifest_length"] = path.stat().st_size
            for key in (
                "added_files_count",
                "existing_files_count",
                "deleted_files_count",
                "added_rows_count",
                "existing_rows_count",
                "deleted_rows_count",
            ):
                merged[key] = sum(manifest[key] for manifest in manifests)
            self.manifests.append(merged)

    def add(
        self,
        rows,
        content=0,
        sequence=1,
        manifest_sequence=None,
        status=1,
        equality_ids=None,
        spec=0,
        partition=None,
        physical_ids=None,
        physical_names=None,
        partition_order=None,
        partition_null_first=True,
        file_sequence=99,
        file_format="PARQUET",
        lower_bound=None,
        metrics=None,
        v1=False,
    ):
        index = len(self.files)
        path = self.directory / f"file-{index}.parquet"
        ids = (
            physical_ids
            if physical_ids is not None
            else (
                sorted(set(equality_ids or []))
                if content == 2
                else [f[0] for f in self.fields]
            )
        )
        if content == 1:
            rows = [
                (self.file_url(self.directory / row[0].split("/")[-1]), row[1])
                for row in rows
            ]
            schema = pa.schema(
                [
                    pa.field(
                        "file_path",
                        pa.string(),
                        metadata={b"PARQUET:field_id": b"2147483546"},
                    ),
                    pa.field(
                        "pos", pa.int64(), metadata={b"PARQUET:field_id": b"2147483545"}
                    ),
                ]
            )
        else:
            fields_by_id = {f[0]: f for f in self.fields}
            schema = pa.schema(
                [
                    pa.field(
                        (physical_names or {}).get(
                            i, fields_by_id.get(i, (i, f"unknown_{i}", "int"))[1]
                        ),
                        ARROW_TYPES[fields_by_id.get(i, (i, "", "int"))[2]],
                        metadata={b"PARQUET:field_id": str(i).encode()},
                    )
                    for i in ids
                ]
            )
        pq.write_table(
            pa.Table.from_pylist(
                [dict(zip(schema.names, row)) for row in rows], schema=schema
            ),
            path,
        )
        partition_fields = self.specs[spec]
        if partition_order is not None:
            partition_fields = [partition_fields[i] for i in partition_order]
        partition_schema = avro_record(
            "partition",
            [
                (f[2], ["null", f[4]] if partition_null_first else [f[4], "null"])
                for f in partition_fields
            ],
        )
        for avro_field, spec_field in zip(partition_schema["fields"], partition_fields):
            avro_field["field-id"] = spec_field[1]
        data_schema = avro_record(
            "data_file",
            [
                ("content", "int"),
                ("file_path", "string"),
                ("file_format", "string"),
                ("partition", partition_schema),
                ("record_count", "long"),
                ("file_size_in_bytes", "long"),
                (
                    "equality_ids",
                    ["null", {"type": "array", "items": "int", "element-id": 136}],
                ),
                (
                    "lower_bounds",
                    ["null", avro_metric_map("bound", "bytes", 126, 127)],
                ),
                (
                    "upper_bounds",
                    ["null", avro_metric_map("upper_bound", "bytes", 129, 130)],
                ),
                (
                    "null_value_counts",
                    ["null", avro_metric_map("null_count", "long", 121, 122)],
                ),
            ],
        )
        entry_schema = avro_record(
            "manifest_entry",
            [
                ("status", "int"),
                ("snapshot_id", ["null", "long"]),
                ("sequence_number", ["null", "long"]),
                ("file_sequence_number", ["null", "long"]),
                ("data_file", data_schema),
            ],
        )
        if v1:
            entry_schema["fields"] = [
                f
                for f in entry_schema["fields"]
                if f["name"] not in ("sequence_number", "file_sequence_number")
            ]
            data_schema["fields"] = [
                f for f in data_schema["fields"] if f["name"] != "content"
            ]
        metrics = dict(metrics or {})
        if lower_bound is not None:
            metrics.setdefault(
                "lower_bounds", {1: lower_bound.to_bytes(4, "little", signed=True)}
            )
        manifest_metrics = {}
        for name in ("lower_bounds", "upper_bounds", "null_value_counts"):
            values = metrics.get(name)
            if isinstance(values, dict):
                values = values.items()
            # Lists allow deliberately duplicated map keys in malformed metrics.
            manifest_metrics[name] = (
                None
                if values is None
                else [{"key": key, "value": value} for key, value in values]
            )
        data = dict(
            content=content,
            file_path=self.file_url(path),
            file_format=file_format,
            partition={f[2]: (partition or {}).get(f[1]) for f in partition_fields},
            record_count=len(rows),
            file_size_in_bytes=path.stat().st_size,
            equality_ids=equality_ids,
            **manifest_metrics,
        )
        manifest_path = self.directory / f"manifest-{index}.avro"
        with manifest_path.open("wb") as output:
            fastavro.writer(
                output,
                entry_schema,
                [
                    dict(
                        status=status,
                        snapshot_id=1,
                        sequence_number=sequence,
                        file_sequence_number=file_sequence,
                        data_file=data,
                    )
                ],
            )
        self.manifests.append(
            dict(
                manifest_path=self.file_url(manifest_path),
                manifest_length=manifest_path.stat().st_size,
                partition_spec_id=spec,
                content=0 if content == 0 else 1,
                sequence_number=(
                    manifest_sequence
                    if manifest_sequence is not None
                    else (sequence or 1)
                ),
                min_sequence_number=0,
                added_snapshot_id=1,
                added_files_count=1,
                existing_files_count=0,
                deleted_files_count=0,
                added_rows_count=len(rows),
                existing_rows_count=0,
                deleted_rows_count=0,
            )
        )
        self.files.append(path)
        return path

    def finish(self):
        list_path = self.directory / "manifest-list.avro"
        list_schema = (
            avro_record(
                "manifest_file",
                [
                    (
                        k,
                        (
                            "string"
                            if k == "manifest_path"
                            else (
                                "int"
                                if k
                                in (
                                    "partition_spec_id",
                                    "content",
                                    "added_files_count",
                                    "existing_files_count",
                                    "deleted_files_count",
                                )
                                else "long"
                            )
                        ),
                    )
                    for k in self.manifests[0]
                ],
            )
            if self.manifests
            else avro_record("manifest_file", [("manifest_path", "string")])
        )
        with list_path.open("wb") as output:
            fastavro.writer(output, list_schema, self.manifests)
        schema = dict(
            type="struct",
            **{"schema-id": 0},
            fields=[
                dict(id=i, name=n, type=t, required=False) for i, n, t in self.fields
            ],
        )
        metadata = {
            "format-version": 2,
            "table-uuid": str(uuid.uuid4()),
            "location": self.url,
            "last-sequence-number": max(
                [m["sequence_number"] for m in self.manifests] + [0]
            ),
            "last-updated-ms": 1,
            "last-column-id": max(f[0] for f in self.fields),
            "current-schema-id": 0,
            "schemas": [schema] + self.histories,
            "default-spec-id": max(self.specs),
            "partition-specs": [
                {
                    "spec-id": i,
                    "fields": [
                        {
                            "source-id": f[0],
                            "field-id": f[1],
                            "name": f[2],
                            "transform": f[3],
                        }
                        for f in fields
                    ],
                }
                for i, fields in self.specs.items()
            ],
            "last-partition-id": 1002,
            "current-snapshot-id": 1,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "sequence-number": max(
                        [m["sequence_number"] for m in self.manifests] + [0]
                    ),
                    "timestamp-ms": 1,
                    "manifest-list": self.file_url(list_path),
                    "schema-id": 0,
                }
            ],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
        }
        path = self.directory / "metadata.json"
        path.write_text(json.dumps(metadata))
        return path


@pytest.fixture
def delete_table(tmp_path, s3):
    return DeleteTable(tmp_path, s3)


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


def attach(pg_conn, table, columns=""):
    path = table.finish()
    for file in table.directory.iterdir():
        table.s3.upload_file(str(file), TEST_BUCKET, f"{table.prefix}/{file.name}")
    path = table.file_url(path)
    run_command(
        f"CREATE FOREIGN TABLE equality_test ({columns}) SERVER pg_lake OPTIONS (path '{path}', format 'iceberg')",
        pg_conn,
    )


def row_bag(rows):
    def value_key(value):
        if isinstance(value, memoryview):
            return bytes(value)
        if isinstance(value, float) and math.isnan(value):
            return ("float", "NaN")
        return value

    return Counter(tuple(value_key(value) for value in row) for row in rows)


def bag(pg_conn, query="SELECT * FROM equality_test"):
    return row_bag(run_query(query, pg_conn))


def assert_paths(pg_conn, expected, *, data_files=None, delete_files=None):
    results = []
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown = {enabled}", pg_conn
        )
        plan = run_query(
            "EXPLAIN (VERBOSE, FORMAT JSON) SELECT * FROM equality_test", pg_conn
        )
        plan_text = str(plan)
        if enabled == "on":
            assert "Custom Scan" in plan_text and "Query Pushdown" in plan_text
        else:
            assert "Foreign Scan" in plan_text and "Query Pushdown" not in plan_text
        if data_files is not None:
            assert int(fetch_data_files_used(plan)) == data_files
        if delete_files is not None:
            assert int(fetch_delete_files_used(plan)) == delete_files
        result = bag(pg_conn)
        assert result == row_bag(expected)
        results.append(result)
    assert results[0] == results[1]


@pytest.mark.parametrize("key,deleted", [(1, 2), (2, "b"), (4, 200)])
@pytest.mark.parametrize("validation", ["on", "off"])
def test_single_keys(s3, pg_conn, extension, delete_table, key, deleted, validation):
    assert run_query(
        "SHOW pg_lake_table.enable_equality_delete_validation", pg_conn
    ) == [["on"]]
    run_command(
        f"SET LOCAL pg_lake_table.enable_equality_delete_validation={validation}",
        pg_conn,
    )
    rows = [
        (1, "a", "duplicate", 100),
        (1, "a", "duplicate", 100),
        (2, "b", "gone", 200),
        (None, None, "null", None),
    ]
    delete_table.add(rows)
    delete_table.add(
        [(deleted,), (deleted,), (None,)], content=2, sequence=2, equality_ids=[key]
    )
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, rows[:2])
    pg_conn.rollback()


def test_sequence_and_deleted_entries(s3, pg_conn, extension, delete_table):
    expected = []
    for seq, file_seq in [(1, 30), (2, 1), (3, 1)]:
        row = (7, "same", f"sequence-{seq}", 7)
        delete_table.add([row], sequence=seq, file_sequence=file_seq, status=0)
        if seq >= 2:
            expected.append(row)
    delete_table.add([(7, "same", "inherited", 7)], sequence=None, manifest_sequence=2)
    expected.append((7, "same", "inherited", 7))
    delete_table.add([(7, "same", "deleted-entry", 7)], status=2)
    delete_table.add(
        [(7,)], content=2, sequence=None, manifest_sequence=2, equality_ids=[1]
    )
    delete_table.add([(7,)], content=2, sequence=4, status=2, equality_ids=[1])
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, expected)
    pg_conn.rollback()


def test_quoted_key_names(s3, pg_conn, extension, tmp_path):
    fields = [(1, "qualify", "int"), (2, 'key"name', "string")] + FIELDS[2:]
    table = DeleteTable(tmp_path, s3, fields=fields)
    table.add([(1, "x", "gone", 1), (2, "x", "keep", 2)])
    table.add(
        [(1, "x")],
        content=2,
        sequence=2,
        equality_ids=[2, 1],
        physical_names={1: "old", 2: "other"},
    )
    attach(pg_conn, table)
    assert_paths(pg_conn, [(2, "x", "keep", 2)])
    pg_conn.rollback()


def test_multiple_keys_and_projection(s3, pg_conn, extension, delete_table):
    rows = [
        (1, None, "a", 10),
        (None, "b", "b", 20),
        (None, None, "c", 30),
        (1, "b", "keep", 40),
        (1, "b", "keep", 40),
        (2, "c", "other", 50),
    ]
    # Physical names and order differ from the current Iceberg schema.
    delete_table.add(
        [tuple(row[i] for i in (2, 0, 3, 1)) for row in rows],
        physical_ids=[3, 1, 4, 2],
        physical_names={1: "old_id", 2: "old_code"},
    )
    delete_table.add(
        [(1, None, "ignored")],
        content=2,
        sequence=2,
        equality_ids=[2, 1],
        physical_ids=[1, 2, 3],
        physical_names={1: "renamed", 2: "different"},
    )
    delete_table.add(
        [(None, "b"), (None, None)], content=2, sequence=2, equality_ids=[1, 2]
    )
    delete_table.add([(2,)], content=2, sequence=2, equality_ids=[1])
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, rows[3:5])
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown = {enabled}", pg_conn
        )
        assert bag(pg_conn, "SELECT payload FROM equality_test") == Counter(
            [("keep",)] * 2
        )
        assert run_query("SELECT count(*) FROM equality_test", pg_conn)[0][0] == 2
        assert bag(
            pg_conn,
            "SELECT payload FROM equality_test WHERE id = 1 ORDER BY payload LIMIT 1",
        ) == Counter([("keep",)])
        assert bag(
            pg_conn,
            "SELECT a.payload FROM equality_test a JOIN equality_test b ON a.id=b.id",
        ) == Counter([("keep",)] * 4)
        run_command(
            "PREPARE equality_plan(int) AS SELECT payload FROM equality_test WHERE id=$1",
            pg_conn,
        )
        for i in range(6):
            assert bag(pg_conn, "EXECUTE equality_plan(1)") == Counter([("keep",)] * 2)
        run_command("DEALLOCATE equality_plan", pg_conn)
    pg_conn.rollback()


def test_position_overlap_and_same_commit(s3, pg_conn, extension, delete_table):
    rows = [(1, "a", "overlap", 1), (2, "b", "position", 2), (3, "c", "keep", 3)]
    path = delete_table.add(rows)
    new_path = delete_table.add([(1, "a", "new", 1)], sequence=2)
    delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    delete_table.add(
        [(str(path), 0), (str(path), 1), (str(new_path), 0)], content=1, sequence=2
    )
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, [rows[2]])
    run_command("SET LOCAL pg_lake_table.enable_full_query_pushdown=on", pg_conn)
    explain = run_query(
        "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) SELECT * FROM equality_test", pg_conn
    )
    assert int(fetch_delete_files_used(explain)) == 2
    pg_conn.rollback()


@pytest.mark.parametrize(
    "value,other,kind,avro_type",
    [
        (None, "", "string", "string"),
        ("", None, "string", "string"),
        (None, b"", "binary", "bytes"),
        (b"", None, "binary", "bytes"),
        (float("nan"), 0.0, "double", "double"),
        (-0.0, 1.0, "double", "double"),
        (12, 13, "date", {"type": "int", "logicalType": "date"}),
        (
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            "timestamp",
            {"type": "long", "logicalType": "timestamp-micros"},
        ),
        (
            daytime(12, 0),
            daytime(13, 0),
            "time",
            {"type": "long", "logicalType": "time-micros"},
        ),
        (
            Decimal("12.34"),
            Decimal("13.34"),
            "decimal(9,2)",
            {
                "type": "fixed",
                "name": "decimal_partition",
                "size": 4,
                "logicalType": "decimal",
                "precision": 9,
                "scale": 2,
            },
        ),
        (True, False, "boolean", "boolean"),
        (float("nan"), 0.0, "float", "float"),
    ],
)
def test_partition_identity(
    s3, pg_conn, extension, tmp_path, value, other, kind, avro_type
):
    table = DeleteTable(
        tmp_path,
        s3,
        fields=FIELDS + [(5, "p", kind)],
        specs={
            0: [],
            1: [(5, 1000, "p_identity", "identity", avro_type)],
            2: [(5, 1000, "new_name", "identity", avro_type)],
        },
    )
    expected = []
    for spec, partition, label in [
        (1, value, "deleted"),
        (1, other, "other"),
        (2, value, "other-spec"),
    ]:
        table.add(
            [(1, "x", label, 1, partition)], spec=spec, partition={1000: partition}
        )
        if label != "deleted":
            projected = (
                date(1970, 1, 1) + timedelta(days=partition)
                if kind == "date"
                else partition
            )
            expected.append((1, "x", label, 1, projected))
    table.add(
        [(1,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        spec=1,
        partition={1000: value},
    )
    attach(pg_conn, table)
    assert_paths(pg_conn, expected)
    pg_conn.rollback()


@pytest.mark.parametrize("kind", ["decimal(9,2)", "uuid"])
@pytest.mark.parametrize("with_delete", [False, True])
@pytest.mark.parametrize("qualified_name", [False, True])
@pytest.mark.parametrize("null_first", [True, False])
def test_named_partition_types(
    s3, pg_conn, extension, tmp_path, kind, with_delete, qualified_name, null_first
):
    definition = {"type": "fixed", "name": "partition_key"}
    reference = "partition_key"
    if qualified_name:
        definition["namespace"] = "test.partition"
        reference = "test.partition.partition_key"
    if kind == "uuid":
        definition.update(size=16, logicalType="uuid")
        first, second = uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes
    else:
        definition.update(size=4, logicalType="decimal", precision=9, scale=2)
        first, second = Decimal("12.34"), Decimal("56.78")

    table = DeleteTable(
        tmp_path,
        s3,
        fields=FIELDS + [(5, "first", kind), (6, "second", kind)],
        specs={
            0: [
                (5, 1000, "first", "identity", definition),
                (6, 1001, "second", "identity", reference),
            ]
        },
    )
    rows = [(1, "x", "first", 1, first, first), (1, "x", "second", 1, first, second)]
    for row in rows:
        table.add(
            [row],
            partition={1000: row[4], 1001: row[5]},
            partition_null_first=null_first,
        )
    if with_delete:
        table.add(
            [(1,)],
            content=2,
            sequence=2,
            equality_ids=[1],
            partition={1000: first, 1001: first},
            partition_null_first=null_first,
        )
    attach(pg_conn, table)
    expected = rows[1:] if with_delete else rows
    if kind == "uuid":
        expected = [
            (*row[:4], *(str(uuid.UUID(bytes=v)) for v in row[4:])) for row in expected
        ]
    assert_paths(pg_conn, expected)
    pg_conn.rollback()


def test_partition_spec_evolution(s3, pg_conn, extension, tmp_path):
    timestamp = datetime(2024, 1, 1, 12)
    day = (timestamp.date() - date(1970, 1, 1)).days
    table = DeleteTable(
        tmp_path,
        s3,
        fields=FIELDS + [(5, "ts", "timestamp")],
        specs={
            0: [(2, 1000, "category", "identity", "string")],
            1: [(5, 1001, "ts_day", "day", "int")],
            2: [],
        },
    )
    rows = [(1, "a", label, 1, timestamp) for label in ("A", "B", "C", "D")]
    table.add([rows[0]], spec=0, partition={1000: "a"})
    rows[1] = (1, "b", "B", 1, timestamp)
    table.add([rows[1]], spec=0, partition={1000: "b"})
    table.add([rows[2]], spec=1, sequence=2, partition={1001: day})
    table.add([rows[3]], spec=1, sequence=3, partition={1001: day})
    table.add(
        [(1,)], content=2, sequence=3, equality_ids=[1], spec=1, partition={1001: day}
    )
    attach(pg_conn, table)
    # Only C has both the matching spec/partition and an older sequence.
    assert_paths(pg_conn, [rows[0], rows[1], rows[3]])
    pg_conn.rollback()

    table.add([(1,)], content=2, sequence=4, equality_ids=[1], spec=2)
    attach(pg_conn, table)
    assert_paths(pg_conn, [])
    pg_conn.rollback()


@pytest.mark.parametrize(
    "transform,kind,avro_type,first,second,partition,other_partition",
    [
        (
            "year",
            "timestamp",
            "int",
            datetime(2024, 1, 1),
            datetime(2025, 1, 1),
            54,
            55,
        ),
        (
            "month",
            "timestamp",
            "int",
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            648,
            649,
        ),
        (
            "hour",
            "timestamp",
            "int",
            datetime(1970, 1, 1, 1),
            datetime(1970, 1, 1, 2),
            1,
            2,
        ),
        ("bucket[4]", "int", "int", 34, 0, None, None),
        ("truncate[2]", "string", "string", "abcd", "wxyz", "ab", "wx"),
    ],
)
def test_partition_transforms(
    s3,
    pg_conn,
    extension,
    tmp_path,
    transform,
    kind,
    avro_type,
    first,
    second,
    partition,
    other_partition,
):
    if transform == "bucket[4]":
        bucket = BucketTransform(4).transform(IntegerType())
        partition, other_partition = bucket(first), bucket(second)
        assert partition != other_partition
    table = DeleteTable(
        tmp_path,
        s3,
        fields=FIELDS + [(5, "p", kind)],
        specs={0: [(5, 1000, "part", transform, avro_type)]},
    )
    table.add([(1, "x", "gone", 1, first)], partition={1000: partition})
    survivor = (1, "x", "keep", 1, second)
    table.add([survivor], partition={1000: other_partition})
    table.add(
        [(1,)], content=2, sequence=2, equality_ids=[1], partition={1000: partition}
    )
    attach(pg_conn, table)
    assert_paths(pg_conn, [survivor])
    pg_conn.rollback()


@pytest.mark.parametrize("invalid", [None, "unknown_spec", "data_tuple"])
def test_empty_tuple_global_delete(s3, pg_conn, extension, tmp_path, invalid):
    partition_fields = [(2, 1000, "category", "identity", "string")]
    table = DeleteTable(tmp_path, s3, specs={0: partition_fields})
    table.add([(1, "a", "old a", 1)], partition={1000: "a"})
    table.add([(1, "b", "old b", 1)], partition={1000: "b"})
    survivor = (1, "a", "same commit", 1)
    table.add([survivor], sequence=2, partition={1000: "a"})

    # Model PartitionSpec.unpartitioned(): empty tuple, spec ID 0, while the
    # table's spec 0 is partitioned. Only the older rows should be deleted.
    table.specs[0] = []
    table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    if invalid == "unknown_spec":
        table.manifests[-1]["partition_spec_id"] = 999
    elif invalid == "data_tuple":
        table.add([(2, "a", "invalid data partition", 2)])
    table.specs[0] = partition_fields
    if invalid is None:
        attach(pg_conn, table)
        assert_paths(pg_conn, [survivor])
    else:
        with pytest.raises(Exception, match="invalid partition spec or tuple"):
            attach(pg_conn, table)
            bag(pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("field", ["data_file", "partition"])
@pytest.mark.parametrize("missing", [False, True])
def test_invalid_manifest_record_schema(
    s3, pg_conn, extension, delete_table, field, missing
):
    delete_table.add([(1, "x", "row", 1)])
    path = delete_table.directory / "manifest-0.avro"
    with path.open("rb") as source:
        reader = fastavro.reader(source)
        schema = reader.writer_schema
        records = list(reader)

    parent_schema, parent_record = schema, records[0]
    if field == "partition":
        parent_schema = next(
            f["type"] for f in schema["fields"] if f["name"] == "data_file"
        )
        parent_record = parent_record["data_file"]
    if missing:
        parent_schema["fields"] = [
            f for f in parent_schema["fields"] if f["name"] != field
        ]
        del parent_record[field]
    else:
        next(f for f in parent_schema["fields"] if f["name"] == field)[
            "type"
        ] = "string"
        parent_record[field] = "not a record"
    with path.open("wb") as output:
        fastavro.writer(output, schema, records)
    delete_table.manifests[0]["manifest_length"] = path.stat().st_size
    with pytest.raises(
        Exception, match="(missing Iceberg manifest record field|must be a record)"
    ):
        attach(pg_conn, delete_table)
        bag(pg_conn)
    pg_conn.rollback()
    # A malformed manifest must report an error without terminating the backend.
    assert run_query("SELECT 1", pg_conn) == [[1]]


@pytest.mark.parametrize("kind", ["float", "double"])
@pytest.mark.parametrize("delete_zero", [-0.0, 0.0], ids=["negative", "positive"])
def test_partition_signed_zero(s3, pg_conn, extension, tmp_path, kind, delete_zero):
    table = DeleteTable(
        tmp_path,
        s3,
        fields=FIELDS + [(5, "p", kind)],
        specs={1: [(5, 1000, "p", "identity", kind)]},
    )
    expected = []
    for value, label in [(-0.0, "negative"), (0.0, "positive")]:
        row = (1, "x", label, 1, value)
        table.add([row], spec=1, partition={1000: value})
        if math.copysign(1, value) != math.copysign(1, delete_zero):
            expected.append(row)
    table.add(
        [(1,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        spec=1,
        partition={1000: delete_zero},
    )
    attach(pg_conn, table)
    assert_paths(pg_conn, expected)
    pg_conn.rollback()


@pytest.mark.parametrize("data_date", [False, True])
@pytest.mark.parametrize("delete_date", [False, True])
def test_day_partition_encodings(
    s3, pg_conn, extension, tmp_path, data_date, delete_date
):
    def day_type(use_date):
        return {"type": "int", "logicalType": "date"} if use_date else "int"

    table = DeleteTable(
        tmp_path,
        s3,
        fields=FIELDS + [(5, "ts", "timestamp")],
        specs={1: [(5, 1000, "ts_day", "day", day_type(data_date))]},
    )
    first = datetime(2024, 1, 1, 12)
    second = first + timedelta(days=1)
    day = (first.date() - date(1970, 1, 1)).days
    table.add([(1, "x", "gone", 1, first)], spec=1, partition={1000: day})
    survivor = (1, "x", "keep", 1, second)
    table.add([survivor], spec=1, partition={1000: day + 1})
    table.specs[1] = [(5, 1000, "ts_day", "day", day_type(delete_date))]
    table.add(
        [(1,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        spec=1,
        partition={1000: day},
    )
    attach(pg_conn, table)
    assert_paths(pg_conn, [survivor])
    pg_conn.rollback()


@pytest.mark.parametrize("value", [None, Decimal("12.34")])
@pytest.mark.parametrize("encoding", ["bytes", "fixed"])
def test_decimal_partition_precision_promotion(
    s3, pg_conn, extension, tmp_path, value, encoding
):
    def partition_type(precision, size):
        return {
            "type": encoding,
            **(
                {"name": "decimal_partition", "size": size}
                if encoding == "fixed"
                else {}
            ),
            "logicalType": "decimal",
            "precision": precision,
            "scale": 2,
        }

    fields = FIELDS + [(5, "p", "decimal(9,2)")]
    table = DeleteTable(
        tmp_path,
        s3,
        fields=fields,
        specs={1: [(5, 1000, "p", "identity", partition_type(9, 4))]},
    )
    table.add([(1, "x", "gone", 1, value)], spec=1, partition={1000: value})
    survivor = (1, "x", "keep", 1, Decimal("56.78"))
    table.add([survivor], spec=1, partition={1000: survivor[-1]})
    table.histories.append(
        dict(
            type="struct",
            **{"schema-id": 1},
            fields=[dict(id=i, name=n, type=t, required=False) for i, n, t in fields],
        )
    )
    table.fields[-1] = (5, "p", "decimal(12,2)")
    table.specs[1] = [(5, 1000, "p", "identity", partition_type(12, 6))]
    table.add(
        [(1,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        spec=1,
        partition={1000: value},
    )
    table.merge_manifests()
    attach(pg_conn, table)
    assert_paths(pg_conn, [survivor])
    pg_conn.rollback()


@pytest.mark.parametrize("with_delete", [False, True])
def test_timetz_payload_conversion(s3, pg_conn, extension, tmp_path, with_delete):
    table = DeleteTable(tmp_path, s3, fields=[(1, "id", "int"), (2, "at_time", "time")])
    table.add([(1, daytime(8, 30)), (2, daytime(9, 30))])
    expected = [(1, daytime(8, 30, tzinfo=timezone.utc))]
    if with_delete:
        table.add([(2,)], content=2, sequence=2, equality_ids=[1])
    else:
        expected.append((2, daytime(9, 30, tzinfo=timezone.utc)))
    run_command("SET LOCAL TimeZone = 'Asia/Shanghai'", pg_conn)
    attach(pg_conn, table, columns="id int, at_time timetz")
    assert_paths(pg_conn, expected)
    pg_conn.rollback()


@pytest.mark.parametrize("void_type", [None, "string", "int"])
def test_global_delete_and_partition_field_order(
    s3, pg_conn, extension, tmp_path, void_type
):
    table = DeleteTable(
        tmp_path,
        s3,
        specs={
            0: [] if void_type is None else [(2, 1002, "removed", "void", void_type)],
            1: [
                (1, 1000, "a", "identity", "int"),
                (2, 1001, "b", "identity", "string"),
            ],
        },
    )
    table.add(
        [(1, "x", "partition-delete", 1)],
        spec=1,
        partition={1000: 1, 1001: "x"},
        partition_order=[1, 0],
    )
    table.add([(2, "x", "global-delete", 2)], spec=1, partition={1000: 2, 1001: "x"})
    table.add([(3, "x", "keep", 3)], spec=1, partition={1000: 3, 1001: "x"})
    table.add(
        [(1,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        spec=1,
        partition={1000: 1, 1001: "x"},
    )
    table.add([(2,)], content=2, sequence=2, equality_ids=[1], spec=0)
    attach(pg_conn, table)
    assert_paths(pg_conn, [(3, "x", "keep", 3)])
    pg_conn.rollback()


@pytest.mark.parametrize(
    "kind,avro_type,first,second",
    [("string", "string", "a", "b"), ("binary", "bytes", b"a", b"b")],
)
def test_partition_values_survive_manifest_records(
    s3, pg_conn, extension, tmp_path, kind, avro_type, first, second
):
    fields = FIELDS + [(5, "part", kind)]
    table = DeleteTable(
        tmp_path,
        s3,
        fields=fields,
        specs={0: [], 1: [(5, 1000, "part", "identity", avro_type)]},
    )
    table.add([(1, "x", "gone", 1, first)], spec=1, partition={1000: first})
    table.add([(1, "x", "keep", 1, second)], spec=1, partition={1000: second})
    table.add(
        [(1,)], content=2, sequence=2, equality_ids=[1], spec=1, partition={1000: first}
    )
    # Real manifests contain many entries: a later Avro record must not mutate
    # a partition value retained from an earlier record.
    table.merge_manifests()
    attach(pg_conn, table)
    assert_paths(pg_conn, [(1, "x", "keep", 1, second)])
    pg_conn.rollback()


@pytest.mark.parametrize(
    "options,error",
    [
        ({"equality_ids": []}, "no equality IDs"),
        ({"equality_ids": [1, 1]}, "duplicate equality field ID"),
        ({"equality_ids": [999]}, "unsupported equality field ID"),
        ({"physical_ids": [2]}, "missing or duplicated"),
        ({"sequence": None, "status": 0}, "missing data sequence"),
        ({"sequence": -1, "manifest_sequence": 2}, "invalid data sequence"),
        ({"sequence": 3, "manifest_sequence": 2}, "invalid data sequence"),
        ({"status": 99}, "invalid Iceberg manifest entry status"),
        ({"content": 99}, "invalid Iceberg data file content"),
        ({"file_format": "AVRO"}, "must use Parquet"),
    ],
)
def test_invalid_deletes(s3, pg_conn, extension, delete_table, options, error):
    delete_table.add([(1, "x", "keep", 1)])
    kwargs = dict(content=2, sequence=2, equality_ids=[1])
    kwargs.update(options)
    width = len(kwargs.get("physical_ids", sorted(set(kwargs["equality_ids"]))))
    delete_table.add(
        (
            [
                tuple(
                    "x" if i == 2 else 1
                    for i in kwargs.get(
                        "physical_ids", sorted(set(kwargs["equality_ids"]))
                    )
                )
            ]
            if width
            else []
        ),
        **kwargs,
    )
    with pytest.raises(Exception, match=error):
        attach(pg_conn, delete_table)
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("evolution", ["rename", "type", "missing"])
def test_schema_history(s3, pg_conn, extension, delete_table, evolution):
    delete_table.add([(1, "x", "gone", 1), (2, "y", "keep", 2)])
    delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    history = dict(
        type="struct",
        **{"schema-id": 1},
        fields=[dict(id=i, name=n, type=t, required=False) for i, n, t in FIELDS],
    )
    if evolution == "rename":
        history["fields"][0]["name"] = "old_id"
    elif evolution == "type":
        history["fields"][0]["type"] = "long"
    else:
        history["fields"] = history["fields"][1:]
    delete_table.histories.append(history)
    attach(pg_conn, delete_table)
    if evolution in ("rename", "missing"):
        assert_paths(pg_conn, [(2, "y", "keep", 2)])
    else:
        with pytest.raises(Exception, match="unsupported schema evolution"):
            run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("deletion", ["none", "empty", "all", "position", "v1"])
def test_empty_and_existing_paths(s3, pg_conn, extension, delete_table, deletion):
    rows = [(1, "x", "row", 1), (1, "x", "row", 1)]
    path = delete_table.add(
        rows, v1=deletion == "v1", status=0 if deletion == "v1" else 1
    )
    if deletion in ("empty", "all", "v1"):
        delete_table.add(
            [] if deletion == "empty" else [(1,)],
            content=2,
            sequence=2,
            equality_ids=[1],
        )
    elif deletion == "position":
        delete_table.add([(str(path), 0)], content=1, sequence=1)
    attach(pg_conn, delete_table)
    assert_paths(
        pg_conn,
        (
            []
            if deletion in ("all", "v1")
            else rows[:1] if deletion == "position" else rows
        ),
    )
    pg_conn.rollback()


def test_pruning_does_not_read_deletes(s3, pg_conn, extension, delete_table):
    delete_table.add([(10, "x", "row", 1)], lower_bound=10)
    path = delete_table.add([(10,)], content=2, sequence=2, equality_ids=[1])
    attach(pg_conn, delete_table)
    delete_table.s3.delete_object(
        Bucket=TEST_BUCKET, Key=f"{delete_table.prefix}/{path.name}"
    )
    # No retained data: even the delete footer is inaccessible and unnecessary.
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}", pg_conn
        )
        run_query("EXPLAIN SELECT * FROM equality_test WHERE id < 0", pg_conn)
        assert (
            run_query("SELECT count(*) FROM equality_test WHERE id < 0", pg_conn)[0][0]
            == 0
        )
    pg_conn.rollback()


def test_empty_snapshot(s3, pg_conn, extension, delete_table):
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, [])
    pg_conn.rollback()


@pytest.fixture
def scale_engine_threads(pgduck_conn):
    # Bound parallel requests to the local Moto server, consistently across
    # scales. This does not change parser/expression depth limits.
    previous = run_query("SELECT current_setting('threads')", pgduck_conn)[0][0]
    cache = run_query(
        "SELECT current_setting('enable_external_file_cache')", pgduck_conn
    )[0][0]
    run_command("SET GLOBAL threads=2", pgduck_conn)
    # Repeated group references still execute readers. Enable caching here
    # to avoid exhausting local test HTTP connections.
    run_command("SET GLOBAL enable_external_file_cache=true", pgduck_conn)
    yield
    run_command(f"SET GLOBAL threads={previous}", pgduck_conn)
    run_command(
        f"SET GLOBAL enable_external_file_cache={'true' if cache else 'false'}",
        pgduck_conn,
    )


@pytest.mark.parametrize("groups", [1, 32, 1024])
def test_scale_and_unique_file_statistics(
    s3, pg_conn, extension, tmp_path, groups, record_property, scale_engine_threads
):
    table = DeleteTable(
        tmp_path, s3, specs={0: [], 1: [(1, 1000, "part", "identity", "int")]}
    )
    for i in range(groups):
        table.add([(i, "x", "keep", i)], spec=1, partition={1000: i})
        table.add(
            [(-1,)],
            content=2,
            sequence=2,
            equality_ids=[1],
            spec=1,
            partition={1000: i},
        )
    # This file is referenced by every group, and must be counted just once.
    table.add([(-2,)], content=2, sequence=2, equality_ids=[1], spec=0)
    table.merge_manifests()
    attach(pg_conn, table)
    record_property("engine_threads", 2)
    record_property("external_file_cache", True)
    start = time.monotonic()
    plan = run_query("EXPLAIN (FORMAT JSON) SELECT * FROM equality_test", pg_conn)
    record_property("planning_seconds", time.monotonic() - start)
    record_property("explain_bytes", len(json.dumps(plan)))
    start = time.monotonic()
    assert bag(pg_conn) == row_bag([(i, "x", "keep", i) for i in range(groups)])
    record_property("execution_seconds", time.monotonic() - start)
    explain = run_query(
        "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) SELECT * FROM equality_test",
        pg_conn,
    )
    assert int(fetch_delete_files_used(explain)) == groups + 1
    assert int(fetch_data_files_used(explain)) == groups
    pg_conn.rollback()


def test_added_key_missing_in_old_data(s3, pg_conn, extension, delete_table):
    delete_table.add([("x", "old", 1)], physical_ids=[2, 3, 4])
    delete_table.add([(1, "x", "new", 1)], sequence=2)
    delete_table.add([(None,)], content=2, sequence=3, equality_ids=[1])
    delete_table.histories.append(
        dict(
            type="struct",
            **{"schema-id": 1},
            fields=[
                dict(id=i, name=n, type=t, required=False) for i, n, t in FIELDS[1:]
            ],
        )
    )
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, [(1, "x", "new", 1)])
    pg_conn.rollback()


def test_integer_key_promotion(s3, pg_conn, extension, tmp_path):
    fields = [(1, "id", "long")] + FIELDS[1:]
    table = DeleteTable(tmp_path, s3, fields=fields)
    path = table.add([(1, "x", "gone", 1), (2, "x", "keep", 2)])
    data = pq.read_table(path)
    promoted_schema = pa.schema(
        [data.schema.field(0).with_type(pa.int32())] + list(data.schema)[1:]
    )
    pq.write_table(data.cast(promoted_schema), path)
    table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    table.histories.append(
        dict(
            type="struct",
            **{"schema-id": 1},
            fields=[dict(id=i, name=n, type=t, required=False) for i, n, t in FIELDS],
        )
    )
    attach(pg_conn, table)
    assert_paths(pg_conn, [(2, "x", "keep", 2)])
    pg_conn.rollback()


def test_many_key_sets(s3, pg_conn, extension, tmp_path, record_property):
    fields = [(i, f"key_{i}", "int") for i in range(1, 33)] + [
        (33, "payload", "string")
    ]
    table = DeleteTable(tmp_path, s3, fields=fields)
    keep = (1,) * 32 + ("duplicate",)
    table.add([keep, keep, (2,) * 32 + ("gone",)])
    for i in range(1, 33):
        table.add([(2 if i == 1 else -1,)], content=2, sequence=2, equality_ids=[i])
    attach(pg_conn, table)
    assert_paths(pg_conn, [keep, keep])
    assert bag(pg_conn, "SELECT payload FROM equality_test") == Counter(
        [("duplicate",)] * 2
    )
    plan = run_query(
        "EXPLAIN (VERBOSE, FORMAT JSON) SELECT count(*) FROM equality_test", pg_conn
    )
    record_property("explain_bytes", len(json.dumps(plan)))
    assert int(fetch_delete_files_used(plan)) == 32
    pg_conn.rollback()


def test_foreign_scan_rescan(s3, pg_conn, extension, delete_table):
    delete_table.add([(1, "x", "keep", 1), (1, "x", "keep", 1), (2, "x", "gone", 2)])
    delete_table.add([(2,)], content=2, sequence=2, equality_ids=[1])
    attach(pg_conn, delete_table)
    run_command(
        "SET LOCAL pg_lake_table.enable_full_query_pushdown=off; SET LOCAL enable_material=off",
        pg_conn,
    )
    query = "SELECT t.payload FROM (VALUES(1),(1),(2)) v(i) CROSS JOIN LATERAL (SELECT payload FROM equality_test WHERE id=v.i OFFSET 0) t"
    assert bag(pg_conn, query) == Counter([("keep",)] * 4)
    plan = run_query("EXPLAIN (ANALYZE, FORMAT JSON) " + query, pg_conn)[0][0]

    def scans(node):
        if isinstance(node, dict):
            if node.get("Node Type") == "Foreign Scan":
                yield node
            for value in node.values():
                yield from scans(value)
        elif isinstance(node, list):
            for value in node:
                yield from scans(value)

    assert any(scan["Actual Loops"] > 1 for scan in scans(plan))
    pg_conn.rollback()


@pytest.mark.parametrize(
    "malformation,error",
    [
        ("spec", "invalid partition spec"),
        ("tuple", "invalid partition spec or tuple"),
        ("field", "missing partition field ID"),
        ("content", "invalid Iceberg manifest content"),
        ("manifest_sequence", "invalid Iceberg manifest sequence"),
        ("data_content", "manifest content does not match"),
    ],
)
def test_invalid_partition_and_manifest(
    s3, pg_conn, extension, tmp_path, malformation, error
):
    table = DeleteTable(
        tmp_path, s3, specs={0: [], 1: [(1, 1000, "part", "identity", "int")]}
    )
    table.add([(1, "x", "row", 1)], spec=1, partition={1000: 1})
    table.add(
        [(1,)], content=2, sequence=2, equality_ids=[1], spec=1, partition={1000: 1}
    )
    if malformation == "spec":
        table.manifests[0]["partition_spec_id"] = 999
    elif malformation == "tuple":
        table.specs[1] = []
    elif malformation == "field":
        table.specs[1][0] = (1, 1001, "part", "identity", "int")
    elif malformation == "content":
        table.manifests[0]["content"] = 99
    elif malformation == "manifest_sequence":
        table.manifests[0]["sequence_number"] = -1
    else:
        table.manifests[0]["content"] = 1
    with pytest.raises(Exception, match=error):
        attach(pg_conn, table)
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("pushdown", [True, False])
def test_footer_validation_setting(s3, pg_conn, extension, delete_table, pushdown):
    delete_table.add([(1, "x", "row", 1)])
    delete_table.add(
        [("x",)], content=2, sequence=2, equality_ids=[1], physical_ids=[2]
    )
    attach(pg_conn, delete_table)
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown={str(pushdown).lower()}",
        pg_conn,
    )

    # EXPLAIN skips footer validation; execution still rejects missing keys.
    run_query("EXPLAIN (VERBOSE) SELECT * FROM equality_test", pg_conn)
    run_command("SAVEPOINT validation_setting", pg_conn)
    with pytest.raises(Exception, match="missing or duplicated"):
        run_query("SELECT * FROM equality_test", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT validation_setting", pg_conn)

    run_command(
        "SET LOCAL pg_lake_table.enable_equality_delete_validation=off", pg_conn
    )
    run_query("EXPLAIN SELECT * FROM equality_test", pg_conn)
    assert bag(pg_conn) == row_bag([(1, "x", "row", 1)])

    # Re-enabling validation must reject malformed files on subsequent scans.
    run_command("SET LOCAL pg_lake_table.enable_equality_delete_validation=on", pg_conn)
    with pytest.raises(Exception, match="missing or duplicated"):
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("nested_key", [False, True])
@pytest.mark.parametrize("pushdown", [True, False])
def test_physical_key_depth(s3, pg_conn, extension, delete_table, nested_key, pushdown):
    rows = [(1, "x", "gone", 1), (2, "y", "keep", 2)]
    delete_table.add(rows)
    path = delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    nested = pa.field(
        "nested",
        pa.struct(
            [
                pa.field(
                    "child",
                    pa.int32(),
                    metadata={b"PARQUET:field_id": b"1" if nested_key else b"101"},
                )
            ]
        ),
        metadata={b"PARQUET:field_id": b"100"},
    )
    fields = [nested]
    row = {"nested": {"child": 1}}
    if not nested_key:
        # A key after an unrelated struct must still be seen at root depth.
        fields.append(pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}))
        row["id"] = 1
    pq.write_table(pa.Table.from_pylist([row], schema=pa.schema(fields)), path)
    attach(pg_conn, delete_table)
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown={str(pushdown).lower()}",
        pg_conn,
    )
    if nested_key:
        with pytest.raises(Exception, match="not a top-level scalar"):
            bag(pg_conn)
    else:
        assert bag(pg_conn) == row_bag(rows[1:])
    pg_conn.rollback()


def test_duplicate_physical_key(s3, pg_conn, extension, delete_table):
    delete_table.add([(1, "x", "row", 1)])
    path = delete_table.add(
        [(1, "x")], content=2, sequence=2, equality_ids=[1], physical_ids=[1, 2]
    )
    physical = pq.read_table(path)
    duplicate = pa.schema(
        [
            physical.schema.field(0),
            physical.schema.field(1).with_metadata({b"PARQUET:field_id": b"1"}),
        ]
    )
    pq.write_table(physical.cast(duplicate), path)
    attach(pg_conn, delete_table)
    with pytest.raises(Exception, match="missing or duplicated"):
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize(
    "field,error",
    [
        ("sequence_number", "missing sequence_number field in v2 manifest"),
        ("status", "missing or NULL required field"),
        ("content", "missing or NULL content field"),
    ],
)
def test_malformed_required_manifest_fields(
    s3, pg_conn, extension, delete_table, field, error
):
    delete_table.add([(1, "x", "row", 1)])
    delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    path = delete_table.directory / "manifest-0.avro"
    with path.open("rb") as source:
        reader = fastavro.reader(source)
        schema = reader.writer_schema
        record = next(reader)
    if field == "sequence_number":
        schema["fields"] = [f for f in schema["fields"] if f["name"] != field]
        record.pop(field)
    else:
        fields = (
            schema["fields"]
            if field == "status"
            else next(f["type"] for f in schema["fields"] if f["name"] == "data_file")[
                "fields"
            ]
        )
        next(f for f in fields if f["name"] == field)["type"] = ["null", "int"]
        target = record if field == "status" else record["data_file"]
        target[field] = None
    with path.open("wb") as output:
        fastavro.writer(output, schema, [record])
    delete_table.manifests[0]["manifest_length"] = path.stat().st_size
    with pytest.raises(Exception, match=error):
        attach(pg_conn, delete_table)
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("valid_files", [0, 1, 33])
@pytest.mark.parametrize("pushdown", [True, False])
def test_incompatible_physical_key(
    s3, pg_conn, extension, delete_table, valid_files, pushdown
):
    delete_table.add([(1, "x", "row", 1)])
    for _ in range(valid_files):
        delete_table.add([(2,)], content=2, sequence=2, equality_ids=[1])
    path = delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    physical = pq.read_table(path)
    pq.write_table(
        physical.cast(pa.schema([physical.schema.field(0).with_type(pa.string())])),
        path,
    )
    attach(pg_conn, delete_table)
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown={str(pushdown).lower()}",
        pg_conn,
    )
    with pytest.raises(Exception, match="incompatible Parquet type"):
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("kind", ["double", "nested", "dropped", "historical_nested"])
@pytest.mark.parametrize("validation", ["on", "off"])
def test_unsupported_key_schema(s3, pg_conn, extension, delete_table, kind, validation):
    run_command(
        f"SET LOCAL pg_lake_table.enable_equality_delete_validation={validation}",
        pg_conn,
    )
    delete_table.add([(1, "x", "row", 1)])
    delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    if kind == "double":
        delete_table.fields[0] = (1, "id", "double")
    elif kind == "nested":
        delete_table.fields[0] = (
            1,
            "id",
            dict(
                type="struct",
                fields=[dict(id=5, name="inner", type="int", required=False)],
            ),
        )
    elif kind == "dropped":
        delete_table.fields = FIELDS[1:]
    else:
        delete_table.histories.append(
            dict(
                type="struct",
                **{"schema-id": 1},
                fields=[
                    dict(
                        id=5,
                        name="outer",
                        type=dict(
                            type="struct",
                            fields=[dict(id=1, name="id", type="int", required=False)],
                        ),
                        required=False,
                    )
                ],
            )
        )
    with pytest.raises(
        Exception, match="unsupported (equality field|schema evolution)"
    ):
        attach(pg_conn, delete_table)
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


def test_inherited_projection(
    s3, pg_conn, extension, delete_table, with_default_location
):
    delete_table.add([(1, "x", "keep", 1), (1, "x", "keep", 1), (2, "x", "gone", 2)])
    delete_table.add([(2,)], content=2, sequence=2, equality_ids=[1])
    attach(pg_conn, delete_table)
    run_command(
        "CREATE TABLE equality_parent(payload text) USING iceberg; INSERT INTO equality_parent VALUES ('parent'); ALTER TABLE equality_test INHERIT equality_parent",
        pg_conn,
    )
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}", pg_conn
        )
        query = "SELECT payload FROM equality_parent"
        plan = str(run_query("EXPLAIN " + query, pg_conn))
        if enabled == "on":
            assert "Custom Scan (Query Pushdown)" in plan
        else:
            assert "Foreign Scan" in plan and "Query Pushdown" not in plan
        assert bag(pg_conn, query) == Counter([("keep",), ("keep",), ("parent",)])
        assert run_query("SELECT count(*) FROM equality_parent", pg_conn)[0][0] == 3
    pg_conn.rollback()


def integer_metrics(field_id, lower, upper, *, width=4, null_count=0):
    return {
        "lower_bounds": {field_id: lower.to_bytes(width, "little", signed=True)},
        "upper_bounds": {field_id: upper.to_bytes(width, "little", signed=True)},
        "null_value_counts": (None if null_count is None else {field_id: null_count}),
    }


@pytest.mark.parametrize(
    "delete_range,delete_files",
    [
        pytest.param((-30, -20), 0, id="below"),
        pytest.param((20, 30), 0, id="above"),
        pytest.param((-20, -10), 1, id="touch-lower"),
        pytest.param((10, 20), 1, id="touch-upper"),
        pytest.param((-5, 5), 1, id="overlap"),
    ],
)
def test_integer_bounds_pruning(
    s3, pg_conn, extension, delete_table, delete_range, delete_files
):
    rows = [(key, "x", "keep", 1) for key in (None, -10, 0, 0, 10)]
    delete_table.add(rows, metrics=integer_metrics(1, -10, 10, null_count=1))
    lower, upper = delete_range
    deleted = {lower, upper}
    if lower <= 0 <= upper:
        deleted.add(0)
    delete_table.add(
        [(key,) for key in sorted(deleted)],
        content=2,
        sequence=2,
        equality_ids=[1],
        metrics=integer_metrics(1, lower, upper),
    )
    attach(pg_conn, delete_table)
    # Bounds exclude NULLs; a NULL data key survives a non-NULL delete set.
    assert_paths(
        pg_conn,
        [row for row in rows if row[0] not in deleted],
        data_files=1,
        delete_files=delete_files,
    )


@pytest.mark.parametrize(
    "invalid",
    [
        "missing-data-lower",
        "missing-delete-upper",
        "bad-data-length",
        "bad-delete-length",
        "inverted-data",
        "inverted-delete",
        "duplicate-data-bound",
        "duplicate-delete-bound",
        "missing-delete-nulls",
        "negative-delete-nulls",
        "positive-delete-nulls",
        "duplicate-delete-nulls",
    ],
)
def test_integer_bounds_inconclusive_metrics(
    s3, pg_conn, extension, delete_table, invalid
):
    rows = [(10, "x", "keep", 1), (10, "x", "keep", 1), (12, "x", "keep", 1)]
    data_metrics = integer_metrics(1, 10, 12)
    delete_metrics = integer_metrics(1, 20, 20)
    if invalid == "missing-data-lower":
        data_metrics.pop("lower_bounds")
    elif invalid == "missing-delete-upper":
        delete_metrics.pop("upper_bounds")
    elif invalid == "bad-data-length":
        data_metrics["lower_bounds"][1] = b"\x0a"
    elif invalid == "bad-delete-length":
        delete_metrics["upper_bounds"][1] = b"\x14"
    elif invalid == "inverted-data":
        data_metrics = integer_metrics(1, 12, 10)
    elif invalid == "inverted-delete":
        delete_metrics = integer_metrics(1, 21, 20)
    elif invalid in ("duplicate-data-bound", "duplicate-delete-bound"):
        metrics = data_metrics if invalid == "duplicate-data-bound" else delete_metrics
        metrics["lower_bounds"] = list(metrics["lower_bounds"].items()) * 2
    elif invalid == "missing-delete-nulls":
        delete_metrics.pop("null_value_counts")
    elif invalid == "negative-delete-nulls":
        delete_metrics["null_value_counts"] = {1: -1}
    elif invalid == "positive-delete-nulls":
        delete_metrics["null_value_counts"] = {1: 1}
    else:
        delete_metrics["null_value_counts"] = [(1, 0), (1, 0)]
    delete_table.add(rows, metrics=data_metrics)
    delete_table.add(
        [(20,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        metrics=delete_metrics,
    )
    attach(pg_conn, delete_table)
    # Uncertain metadata keeps the delete reader, including duplicate metrics.
    assert_paths(pg_conn, rows, data_files=1, delete_files=1)


@pytest.mark.parametrize("null_count", [None, 1], ids=["unknown", "present"])
def test_integer_bounds_null_keys(s3, pg_conn, extension, delete_table, null_count):
    rows = [(None, "x", "gone", 1), (10, "x", "keep", 1)]
    delete_table.add(rows, metrics=integer_metrics(1, 10, 10, null_count=1))
    delete_table.add(
        [(None,), (20,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        metrics=integer_metrics(1, 20, 20, null_count=null_count),
    )
    attach(pg_conn, delete_table)
    # Disjoint non-NULL ranges do not prove that NULL-safe keys cannot match.
    assert_paths(pg_conn, rows[1:], data_files=1, delete_files=1)


@pytest.mark.parametrize("pushdown", [True, False])
def test_integer_bounds_reject_null_metric_value(
    s3, pg_conn, extension, delete_table, pushdown
):
    delete_table.add(
        [(None, "x", "gone", 1), (10, "x", "keep", 1)],
        metrics=integer_metrics(1, 10, 10, null_count=1),
    )
    delete_table.add(
        [(None,), (20,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        metrics=integer_metrics(1, 20, 20, null_count=1),
    )
    path = delete_table.directory / "manifest-1.avro"
    with path.open("rb") as source:
        reader = fastavro.reader(source)
        schema = reader.writer_schema
        records = list(reader)
    data_schema = next(f["type"] for f in schema["fields"] if f["name"] == "data_file")
    stats_schema = next(
        f["type"][1]["items"]
        for f in data_schema["fields"]
        if f["name"] == "null_value_counts"
    )
    next(f for f in stats_schema["fields"] if f["name"] == "value")["type"] = [
        "null",
        "long",
    ]
    records[0]["data_file"]["null_value_counts"][0]["value"] = None
    with path.open("wb") as output:
        fastavro.writer(output, schema, records)
    delete_table.manifests[1]["manifest_length"] = path.stat().st_size
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown={str(pushdown).lower()}",
        pg_conn,
    )
    # An invalid NULL metric must not become zero and hide a real NULL delete.
    with pytest.raises(
        Exception, match="Iceberg column metrics require non-null key and value"
    ):
        attach(pg_conn, delete_table)
        bag(pg_conn)
    pg_conn.rollback()
    assert run_query("SELECT 1", pg_conn) == [[1]]


@pytest.mark.parametrize(
    "data_value,delete_value,data_width,delete_width,mixed_width",
    [
        pytest.param(-10, 20, 4, 8, None, id="promoted-data"),
        pytest.param(-(2**40), 20, 8, 4, None, id="promoted-delete"),
        pytest.param(-(2**63), 2**63 - 1, 8, 8, None, id="long-extrema"),
        pytest.param(-(2**31), 2**31 - 1, 4, 4, None, id="int-extrema"),
        pytest.param(-10, 20, 8, 8, "data", id="mixed-data-width"),
        pytest.param(-10, 20, 8, 8, "delete", id="mixed-delete-width"),
    ],
)
def test_long_bounds_pruning(
    s3,
    pg_conn,
    extension,
    delete_table,
    data_value,
    delete_value,
    data_width,
    delete_width,
    mixed_width,
):
    data_metrics = integer_metrics(4, data_value, data_value, width=data_width)
    delete_metrics = integer_metrics(4, delete_value, delete_value, width=delete_width)
    if mixed_width:
        metrics = data_metrics if mixed_width == "data" else delete_metrics
        value = data_value if mixed_width == "data" else delete_value
        metrics["lower_bounds"][4] = value.to_bytes(4, "little", signed=True)
    rows = [(1, "x", "keep", data_value)]
    data_path = delete_table.add(rows, metrics=data_metrics)
    delete_path = delete_table.add(
        [(delete_value,)],
        content=2,
        sequence=2,
        equality_ids=[4],
        metrics=delete_metrics,
    )
    # Promoted files retain their original int Parquet field and int bounds.
    for path, width, field_index in (
        (data_path, data_width, 3),
        (delete_path, delete_width, 0),
    ):
        if width == 4:
            table = pq.read_table(path)
            fields = list(table.schema)
            fields[field_index] = fields[field_index].with_type(pa.int32())
            pq.write_table(table.cast(pa.schema(fields)), path)
    if data_width == 4 or delete_width == 4:
        delete_table.histories.append(
            dict(
                type="struct",
                **{"schema-id": 1},
                fields=[
                    dict(id=i, name=n, type="int" if i == 4 else t, required=False)
                    for i, n, t in FIELDS
                ],
            )
        )
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, rows, data_files=1, delete_files=int(mixed_width is not None))


@pytest.mark.parametrize("keys", ["added", "composite", "string"])
def test_bounds_pruning_key_scope(s3, pg_conn, extension, delete_table, keys):
    if keys == "added":
        delete_table.add(
            [("x", "old", 10)],
            physical_ids=[2, 3, 4],
            metrics=integer_metrics(4, 10, 10, width=8),
        )
        delete_table.histories.append(
            dict(
                type="struct",
                **{"schema-id": 1},
                fields=[
                    dict(id=i, name=n, type=t, required=False) for i, n, t in FIELDS[1:]
                ],
            )
        )
        deleted = [(20,)]
        equality_ids = [1]
        delete_metrics = integer_metrics(1, 20, 20)
        expected = [(None, "x", "old", 10)]
    elif keys == "composite":
        expected = [(1, "x", "keep", 10)] * 2
        delete_table.add(expected, metrics=integer_metrics(4, 10, 10, width=8))
        deleted = [("x", 20)]
        equality_ids = [2, 4]
        delete_metrics = integer_metrics(4, 20, 20, width=8)
    else:
        expected = [(1, "aaaa", "keep", 10)]
        delete_table.add(
            expected,
            metrics={"lower_bounds": {2: b"aaaa"}, "upper_bounds": {2: b"aaaa"}},
        )
        deleted = [("zzzz",)]
        equality_ids = [2]
        delete_metrics = {
            "lower_bounds": {2: b"zzzz"},
            "upper_bounds": {2: b"zzzz"},
            "null_value_counts": {2: 0},
        }
    delete_table.add(
        deleted,
        content=2,
        sequence=2,
        equality_ids=equality_ids,
        metrics=delete_metrics,
    )
    attach(pg_conn, delete_table)
    assert_paths(pg_conn, expected, data_files=1, delete_files=int(keys != "composite"))


def test_bounds_pruning_is_per_data_file(s3, pg_conn, extension, delete_table):
    low_rows = [(10, "x", "gone", 1), (11, "x", "keep", 1)]
    high_rows = [(20, "x", "gone", 1), (21, "x", "keep", 1)]
    low_path = delete_table.add(low_rows, metrics=integer_metrics(1, 10, 11))
    delete_table.add(high_rows, metrics=integer_metrics(1, 20, 21))
    delete_table.add(
        [(20,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        metrics=integer_metrics(1, 20, 20),
    )
    delete_table.add([(str(low_path), 0)], content=1, sequence=2)
    attach(pg_conn, delete_table)
    # Keep the equality delete in the overlapping group and position deletes in both.
    assert_paths(pg_conn, [low_rows[1], high_rows[1]], data_files=2, delete_files=2)


def test_disjoint_bounds_skip_unreadable_delete(s3, pg_conn, extension, delete_table):
    rows = [(10, "x", "keep", 1)]
    delete_table.add(rows, metrics=integer_metrics(1, 10, 10))
    path = delete_table.add(
        [(20,)],
        content=2,
        sequence=2,
        equality_ids=[1],
        metrics=integer_metrics(1, 20, 20),
    )
    attach(pg_conn, delete_table)
    delete_table.s3.delete_object(
        Bucket=TEST_BUCKET, Key=f"{delete_table.prefix}/{path.name}"
    )
    assert (
        run_query("SHOW pg_lake_table.enable_equality_delete_validation", pg_conn)[0][0]
        == "on"
    )
    # Retained data is readable; the pruned delete cannot even supply a footer.
    assert_paths(pg_conn, rows, data_files=1, delete_files=0)


@pytest.mark.parametrize("pushdown", [True, False])
@pytest.mark.parametrize("validation", ["on", "off"])
@pytest.mark.parametrize("inherited", [True, False])
def test_plain_explain_skips_delete_footers(
    s3,
    pg_conn,
    extension,
    delete_table,
    with_default_location,
    pushdown,
    validation,
    inherited,
):
    delete_table.add([(1, "x", "row", 1)])
    path = delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    attach(pg_conn, delete_table)
    relation = "equality_test"
    if inherited:
        run_command(
            "CREATE TABLE equality_parent (LIKE equality_test) USING iceberg;"
            "ALTER TABLE equality_test INHERIT equality_parent",
            pg_conn,
        )
        relation = "equality_parent"
    delete_table.s3.delete_object(
        Bucket=TEST_BUCKET, Key=f"{delete_table.prefix}/{path.name}"
    )
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown={str(pushdown).lower()};"
        f"SET LOCAL pg_lake_table.enable_equality_delete_validation={validation}",
        pg_conn,
    )
    for options in ("", "(VERBOSE)", "(VERBOSE, FORMAT JSON)"):
        plan = run_query(f"EXPLAIN {options} SELECT * FROM {relation}", pg_conn)
        if "FORMAT JSON" in options and not inherited:
            assert int(fetch_data_files_used(plan)) == 1
            assert int(fetch_delete_files_used(plan)) == 1

    # Both actual execution and EXPLAIN ANALYZE must read the retained delete.
    for prefix in ("", "EXPLAIN (ANALYZE, VERBOSE) "):
        run_command("SAVEPOINT unavailable_delete", pg_conn)
        with pytest.raises(Exception, match=path.name):
            run_query(f"{prefix}SELECT * FROM {relation}", pg_conn)
        run_command("ROLLBACK TO SAVEPOINT unavailable_delete", pg_conn)

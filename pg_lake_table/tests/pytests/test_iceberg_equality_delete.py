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
        file_sequence=99,
        file_format="PARQUET",
        lower_bound=None,
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
            "partition", [(f[2], ["null", f[4]]) for f in partition_fields]
        )
        for avro_field, spec_field in zip(partition_schema["fields"], partition_fields):
            avro_field["field-id"] = spec_field[1]
        bounds_schema = {
            "type": "array",
            "logicalType": "map",
            "items": avro_record("bound", [("key", "int"), ("value", "bytes")]),
        }
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
                ("lower_bounds", ["null", bounds_schema]),
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
        data = dict(
            content=content,
            file_path=self.file_url(path),
            file_format=file_format,
            partition={f[2]: (partition or {}).get(f[1]) for f in partition_fields},
            record_count=len(rows),
            file_size_in_bytes=path.stat().st_size,
            equality_ids=equality_ids,
            lower_bounds=(
                None
                if lower_bound is None
                else [
                    {"key": 1, "value": lower_bound.to_bytes(4, "little", signed=True)}
                ]
            ),
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


def assert_paths(pg_conn, expected):
    results = []
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown = {enabled}", pg_conn
        )
        plan = str(run_query("EXPLAIN SELECT * FROM equality_test", pg_conn))
        if enabled == "on":
            assert "Custom Scan (Query Pushdown)" in plan
        else:
            assert "Foreign Scan" in plan and "Query Pushdown" not in plan
        result = bag(pg_conn)
        assert result == row_bag(expected)
        results.append(result)
    assert results[0] == results[1]


@pytest.mark.parametrize("key,deleted", [(1, 2), (2, "b"), (4, 200)])
def test_single_keys(s3, pg_conn, extension, delete_table, key, deleted):
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
    # Production uses this cache by default. Repeated group references still
    # execute readers; caching avoids exhausting local test HTTP connections.
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


def test_incompatible_physical_key(s3, pg_conn, extension, delete_table):
    delete_table.add([(1, "x", "row", 1)])
    path = delete_table.add([(1,)], content=2, sequence=2, equality_ids=[1])
    physical = pq.read_table(path)
    pq.write_table(
        physical.cast(pa.schema([physical.schema.field(0).with_type(pa.string())])),
        path,
    )
    attach(pg_conn, delete_table)
    with pytest.raises(Exception, match="incompatible Parquet type"):
        run_query("SELECT * FROM equality_test", pg_conn)
    pg_conn.rollback()


@pytest.mark.parametrize("kind", ["double", "nested", "dropped", "historical_nested"])
def test_unsupported_key_schema(s3, pg_conn, extension, delete_table, kind):
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

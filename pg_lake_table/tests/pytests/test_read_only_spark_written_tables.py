"""Iceberg tables that Spark writes through a REST catalog read the same in
pg_lake, attached read-only, as they do in Spark itself."""

import uuid
from decimal import Decimal

import psycopg2
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import FixedType, LongType, NestedField, UUIDType
from pyspark.sql.functions import col
from helpers import server_params
from helpers.polaris import *
from helpers.spark import (
    create_spark_catalog_database,
    spark_rest_catalog,
    spark_session,
)
from utils_pytest import *


COLUMNS = (
    "id BIGINT, name STRING, category STRING, amount DECIMAL(12,2), ts TIMESTAMP_NTZ"
)
BASE_COLUMNS = ["id", "name", "category", "amount", "ts"]

PARTITIONINGS = {
    "unpartitioned": "",
    "identity": "PARTITIONED BY (category)",
    "bucket": "PARTITIONED BY (bucket(4, id))",
    "truncate": "PARTITIONED BY (truncate(6, name))",
    "year": "PARTITIONED BY (years(ts))",
    "month": "PARTITIONED BY (months(ts))",
    "day": "PARTITIONED BY (days(ts))",
    "hour": "PARTITIONED BY (hours(ts))",
}

DELETION_VECTORS_REFUSED = "deletion vectors are not supported"


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("partitioning", PARTITIONINGS.keys())
def test_partitioned_table(spark_table, format_version, partitioning):
    spark_table.create(PARTITIONINGS[partitioning], format_version=format_version)
    spark_table.insert(0, 500)
    spark_table.insert(500, 1000)
    spark_table.attach()

    spark_table.assert_same_rows()
    spark_table.assert_same_rows("category = 'c1' AND ts >= '2025-06-01'")

    # unpartitioned, only the column bounds of the first insert's files match
    where = PRUNING_FILTERS.get(partitioning, "id < 100")
    spark_table.assert_same_rows(where)
    assert spark_table.data_files_scanned(where) < spark_table.data_files_scanned()


PRUNING_FILTERS = {
    "identity": "category = 'c1'",
    "bucket": "id = 42",
    "truncate": "name = 'name_42'",
    "year": "ts < '2024-03-01'",
    "month": "ts < '2024-03-01'",
    "day": "ts < '2024-03-01'",
    "hour": "ts < '2024-03-01'",
}


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("mode", ["copy-on-write", "merge-on-read"])
@pytest.mark.parametrize("operation", ["delete", "update", "merge"])
@pytest.mark.parametrize("partitioning", ["unpartitioned", "identity"])
def test_row_level_change(spark_table, format_version, mode, operation, partitioning):
    spark_table.create(
        PARTITIONINGS[partitioning],
        format_version=format_version,
        properties={
            "write.delete.mode": mode,
            "write.update.mode": mode,
            "write.merge.mode": mode,
        },
    )
    spark_table.insert(0, 1000)
    spark_table.attach()

    spark_table.spark_sql(ROW_LEVEL_CHANGES[operation])

    if format_version == 3 and mode == "merge-on-read":
        spark_table.assert_refused(DELETION_VECTORS_REFUSED)
        return

    spark_table.assert_same_rows()
    if mode == "merge-on-read":
        assert spark_table.delete_files_scanned() > 0


ROW_LEVEL_CHANGES = {
    "delete": "DELETE FROM {table} WHERE id % 7 = 0",
    "update": """UPDATE {table} SET name = concat(name, '_updated'), amount = amount + 1
                 WHERE id % 3 = 0""",
    "merge": """MERGE INTO {table} t
                USING (SELECT id, concat('merged_', id) AS name FROM range(900, 1100)) s
                ON t.id = s.id
                WHEN MATCHED AND s.id % 2 = 0 THEN DELETE
                WHEN MATCHED THEN UPDATE SET t.name = s.name
                WHEN NOT MATCHED THEN INSERT (id, name, category, amount, ts)
                    VALUES (s.id, s.name, 'c9', 0, TIMESTAMP_NTZ'2030-01-01 00:00:00')""",
}


def test_overwritten_and_deleted_partitions(spark_table):
    spark_table.create(PARTITIONINGS["identity"])
    spark_table.insert(0, 1000)
    spark_table.attach()

    spark_table.spark_sql("DELETE FROM {table} WHERE category = 'c2'")
    spark_table.assert_same_rows()

    spark_table.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        spark_table.spark_sql(
            f"""INSERT OVERWRITE {{table}}
                SELECT id, concat('overwritten_', id), category, amount, ts
                FROM ({ROWS.format(start=0, end=100)}) WHERE category = 'c1'"""
        )
    finally:
        spark_table.spark.conf.unset("spark.sql.sources.partitionOverwriteMode")
    spark_table.assert_same_rows()

    spark_table.spark_sql(
        f"INSERT OVERWRITE {{table}} {ROWS.format(start=2000, end=2100)}"
    )
    spark_table.assert_same_rows()


def test_table_maintenance(spark_table):
    spark_table.create(
        PARTITIONINGS["bucket"],
        properties={
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
        },
    )
    for start in range(0, 1000, 100):
        spark_table.insert(start, start + 100)
    spark_table.attach()

    spark_table.spark_sql(ROW_LEVEL_CHANGES["delete"])
    spark_table.spark_sql(ROW_LEVEL_CHANGES["update"])
    spark_table.assert_same_rows()

    for procedure in [
        "rewrite_position_delete_files(table => '{name}', options => map('rewrite-all', 'true'))",
        """rewrite_data_files(table => '{name}',
               options => map('rewrite-all', 'true', 'remove-dangling-deletes', 'true'))""",
        "rewrite_manifests(table => '{name}')",
        "expire_snapshots(table => '{name}', older_than => TIMESTAMP '2100-01-01', retain_last => 1)",
    ]:
        spark_table.call(procedure)
        spark_table.assert_same_rows()

    assert spark_table.delete_files_scanned() == 0


def test_deletion_vectors_refused_until_compacted(spark_table):
    spark_table.create(
        PARTITIONINGS["identity"],
        format_version=3,
        properties={"write.delete.mode": "merge-on-read"},
    )
    spark_table.insert(0, 1000)
    spark_table.attach()

    spark_table.spark_sql(ROW_LEVEL_CHANGES["delete"])
    spark_table.assert_refused(DELETION_VECTORS_REFUSED)

    # the commit that applies the deletes lists the deletion vectors it
    # removes as deleted entries, which must not be refused
    spark_table.call(
        "rewrite_data_files(table => '{name}', options => map('delete-file-threshold', '1'))"
    )
    spark_table.assert_same_rows()


def test_position_deletes_kept_after_upgrade_to_v3(spark_table):
    spark_table.create(
        PARTITIONINGS["identity"],
        properties={"write.delete.mode": "merge-on-read"},
    )
    spark_table.insert(0, 1000)
    spark_table.attach()
    spark_table.spark_sql(ROW_LEVEL_CHANGES["delete"])

    spark_table.spark_sql(
        "ALTER TABLE {table} SET TBLPROPERTIES ('format-version' = '3')"
    )
    spark_table.assert_same_rows()
    assert spark_table.delete_files_scanned() > 0

    spark_table.spark_sql("DELETE FROM {table} WHERE id % 11 = 0")
    spark_table.assert_refused(DELETION_VECTORS_REFUSED)


EVOLVING_COLUMNS = """id BIGINT NOT NULL, i INT, f FLOAT, d DECIMAL(10,2), s STRING NOT NULL,
    st STRUCT<a: INT, b: STRING>, l ARRAY<STRUCT<x: INT>>, m MAP<STRING, STRUCT<y: INT>>"""

EVOLVING_ROWS = """SELECT id, CAST(id AS INT), CAST(id / 4 AS FLOAT),
                          CAST(id * 1.25 AS DECIMAL(10,2)), concat('s_', id),
                          named_struct('a', CAST(id AS INT), 'b', concat('b_', id)),
                          array(named_struct('x', CAST(id AS INT))),
                          map('k', named_struct('y', CAST(id AS INT)))
                   FROM range({start}, {end})"""

ST_A = ("st.a", "(st).a")
ST_B = ("st.b", "(st).b")
EVOLVING_PROJECTION = [
    "id",
    "i",
    "f",
    "d",
    "s",
    ST_A,
    ST_B,
    ("l[0].x", "(l[1]).x"),
    ("m['k'].y", "(map_type.extract(m, 'k')).y"),
]


def without(*columns):
    return [c for c in EVOLVING_PROJECTION if c not in columns]


class SchemaChange:
    def __init__(self, ddl, columns=EVOLVING_PROJECTION, fill=None, after_attach=None):
        self.ddl = ddl
        self.columns = columns
        self.fill = fill
        # the columns a table attached before the change still reads, or
        # None if the change asks for the table to be recreated
        self.after_attach = after_attach


SCHEMA_CHANGES = {
    "add_column": SchemaChange(
        ["ADD COLUMN extra STRING"],
        EVOLVING_PROJECTION + ["extra"],
        fill="extra = concat('e_', id)",
    ),
    "drop_column": SchemaChange(["DROP COLUMN f"], without("f")),
    "rename_column": SchemaChange(
        ["RENAME COLUMN s TO renamed"], without("s") + ["renamed"]
    ),
    "move_column": SchemaChange(
        ["ALTER COLUMN s FIRST"], after_attach=EVOLVING_PROJECTION
    ),
    "drop_and_add_column": SchemaChange(
        ["DROP COLUMN s", "ADD COLUMN s STRING"], fill="s = concat('new_', id)"
    ),
    "int_to_long": SchemaChange(["ALTER COLUMN i TYPE BIGINT"]),
    "float_to_double": SchemaChange(["ALTER COLUMN f TYPE DOUBLE"]),
    "widen_decimal": SchemaChange(["ALTER COLUMN d TYPE DECIMAL(20,2)"]),
    "drop_not_null": SchemaChange(["ALTER COLUMN s DROP NOT NULL"]),
    "add_struct_field": SchemaChange(
        ["ADD COLUMN st.c STRING"],
        EVOLVING_PROJECTION + [("st.c", "(st).c")],
        fill="st.c = concat('c_', id)",
    ),
    "rename_struct_field": SchemaChange(
        ["RENAME COLUMN st.a TO a2"], without(ST_A) + [("st.a2", "(st).a2")]
    ),
    "drop_struct_field": SchemaChange(["DROP COLUMN st.b"], without(ST_B)),
    "promote_struct_field": SchemaChange(["ALTER COLUMN st.a TYPE BIGINT"]),
    "add_list_element_field": SchemaChange(
        ["ADD COLUMN l.element.z STRING"],
        EVOLVING_PROJECTION + [("l[0].z", "(l[1]).z")],
    ),
    "add_map_value_field": SchemaChange(
        ["ADD COLUMN m.value.w STRING"],
        EVOLVING_PROJECTION + [("m['k'].w", "(map_type.extract(m, 'k')).w")],
    ),
}


@pytest.mark.parametrize("change", SCHEMA_CHANGES.keys())
def test_schema_change(spark_table, change):
    change = SCHEMA_CHANGES[change]
    spark_table.create(columns=EVOLVING_COLUMNS)
    spark_table.insert(0, 500, rows=EVOLVING_ROWS)
    spark_table.change_schema(change)
    spark_table.attach()

    spark_table.assert_same_rows(columns=change.columns)


@pytest.mark.parametrize("change", SCHEMA_CHANGES.keys())
def test_schema_change_after_attach(spark_table, change):
    change = SCHEMA_CHANGES[change]
    spark_table.create(columns=EVOLVING_COLUMNS)
    spark_table.insert(0, 500, rows=EVOLVING_ROWS)
    spark_table.attach()
    spark_table.change_schema(change)

    if change.after_attach is not None:
        spark_table.assert_same_rows(columns=change.after_attach)
        return

    spark_table.assert_refused("Please drop and recreate the table")
    spark_table.detach()
    spark_table.attach()
    spark_table.assert_same_rows(columns=change.columns)


TYPED_COLUMNS = """id BIGINT, b BOOLEAN, i INT, l BIGINT, f FLOAT, d DOUBLE,
    small_dec DECIMAL(9,2), big_dec DECIMAL(38,10), dt DATE, ts TIMESTAMP,
    ts_ntz TIMESTAMP_NTZ, s STRING, bin BINARY, arr ARRAY<INT>, mp MAP<STRING, INT>,
    deep STRUCT<x: ARRAY<STRUCT<y: INT>>, z: MAP<INT, STRING>>"""

TYPED_ROWS = """VALUES
    (1, true, -2147483648, -9223372036854775808, CAST('NaN' AS FLOAT),
     CAST('Infinity' AS DOUBLE), -9999999.99, -9999999999999999999999999999.9999999999,
     DATE'0001-01-01', TIMESTAMP'0001-01-01 00:00:00.000001',
     TIMESTAMP_NTZ'0001-01-01 00:00:00', '', X'', array(), map(),
     named_struct('x', array(), 'z', map())),
    (2, false, 2147483647, 9223372036854775807, CAST('-Infinity' AS FLOAT),
     CAST('-0.0' AS DOUBLE), 9999999.99, 9999999999999999999999999999.9999999999,
     DATE'9999-12-31', TIMESTAMP'9999-12-31 23:59:59.999999',
     TIMESTAMP_NTZ'9999-12-31 23:59:59.999999', 'ğüşçö 日本 🙂', X'00FF10',
     array(1, NULL, 3), map('a', 1, 'b', NULL),
     named_struct('x', array(named_struct('y', 7), NULL), 'z', map(1, 'one'))),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL),
    (4, true, 0, 0, 1.5, 2.25, 0.01, 0.0000000001, DATE'1970-01-01',
     TIMESTAMP'1969-12-31 23:59:59.999999', TIMESTAMP_NTZ'1970-01-01 00:00:00.000001',
     'a', X'61', array(NULL), map('a', NULL),
     named_struct('x', NULL, 'z', NULL))"""

TYPED_PROJECTION = [
    "id",
    "b",
    "i",
    "l",
    "f",
    "d",
    "small_dec",
    "big_dec",
    "dt",
    (
        "date_format(ts, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
        "to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')",
    ),
    (
        "date_format(ts_ntz, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
        "to_char(ts_ntz, 'YYYY-MM-DD HH24:MI:SS.US')",
    ),
    "s",
    ("lower(hex(bin))", "lower(encode(bin, 'hex'))"),
    "arr",
    ("mp['a']", "map_type.extract(mp, 'a')"),
    ("mp['b']", "map_type.extract(mp, 'b')"),
    ("deep.x[0].y", "((deep).x[1]).y"),
    ("deep.z[1]", "map_type.extract((deep).z, 1)"),
]


@pytest.mark.parametrize("format_version", [2, 3])
def test_types_and_edge_values(spark_table, format_version):
    spark_table.create(columns=TYPED_COLUMNS, format_version=format_version)
    spark_table.spark_sql(f"INSERT INTO {{table}} {TYPED_ROWS}")
    spark_table.attach()

    spark_table.assert_same_rows(columns=TYPED_PROJECTION)
    for where in ["i = 2147483647", "dt < '1970-01-01'", "s = 'ğüşçö 日本 🙂'", "b"]:
        spark_table.assert_same_rows(where, columns=["id"])


def test_uuid_and_fixed_columns(spark_table):
    catalog = create_iceberg_rest_catalog(spark_table.namespace)
    catalog.create_table(
        spark_table.name,
        schema=Schema(
            NestedField(1, "id", LongType()),
            NestedField(2, "u", UUIDType()),
            NestedField(3, "fx", FixedType(4)),
        ),
    )
    spark_table.spark_sql(
        """INSERT INTO {table} VALUES
               (1, '123e4567-e89b-12d3-a456-426614174000', X'DEADBEEF'),
               (2, NULL, NULL)"""
    )
    spark_table.attach()

    spark_table.assert_same_rows(
        columns=[
            "id",
            ("u", "u::text"),
            ("lower(hex(fx))", "lower(encode(fx, 'hex'))"),
        ]
    )
    spark_table.assert_same_rows(
        "u = '123e4567-e89b-12d3-a456-426614174000'", columns=["id"]
    )


@pytest.mark.parametrize("codec", ["gzip", "snappy", "zstd", "uncompressed"])
def test_manifest_compression(spark_table, codec):
    spark_table.create(
        PARTITIONINGS["identity"],
        properties={
            "write.manifest.compression-codec": codec,
            "write.delete.mode": "merge-on-read",
        },
    )
    spark_table.insert(0, 500)
    spark_table.spark_sql(ROW_LEVEL_CHANGES["delete"])
    avro_codec = {"gzip": "deflate", "zstd": "zstandard", "uncompressed": "null"}.get(
        codec, codec
    )
    for manifest in spark_table.spark_sql("SELECT path FROM {table}.manifests"):
        assert spark_table.avro_codec(manifest.path) == avro_codec
    spark_table.attach()

    if codec == "zstd":
        spark_table.assert_refused("File header contains an unknown codec")
        return

    spark_table.assert_same_rows()


@pytest.mark.parametrize("codec", ["zstd", "snappy", "gzip", "lz4", "uncompressed"])
def test_data_file_compression(spark_table, codec):
    spark_table.create(properties={"write.parquet.compression-codec": codec})
    spark_table.insert(0, 500)
    spark_table.attach()

    # Iceberg's lz4 is the deprecated Hadoop-framed codec, not LZ4_RAW
    if codec == "lz4":
        spark_table.assert_refused('Unsupported compression codec "LZ4"')
        return

    spark_table.assert_same_rows()


def test_partition_evolution(spark_table):
    spark_table.create(
        PARTITIONINGS["identity"],
        properties={
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
        },
    )
    spark_table.insert(0, 200)
    spark_table.spark_sql("ALTER TABLE {table} ADD PARTITION FIELD bucket(4, id) AS b")
    spark_table.insert(200, 400)
    spark_table.spark_sql(ROW_LEVEL_CHANGES["delete"])
    spark_table.spark_sql("ALTER TABLE {table} DROP PARTITION FIELD category")
    spark_table.insert(400, 600)
    spark_table.spark_sql(
        "ALTER TABLE {table} REPLACE PARTITION FIELD b WITH months(ts)"
    )
    spark_table.insert(600, 800)
    spark_table.spark_sql(ROW_LEVEL_CHANGES["update"])
    spark_table.spark_sql("ALTER TABLE {table} DROP PARTITION FIELD months(ts)")
    spark_table.insert(800, 1000)
    spark_table.attach()

    spark_table.assert_same_rows()
    for where in ["category = 'c1'", "id = 42", "ts < '2024-03-01'", "id >= 800"]:
        spark_table.assert_same_rows(where)


def test_branches_tags_and_statistics(spark_table):
    spark_table.create(PARTITIONINGS["identity"])
    spark_table.insert(0, 500)
    spark_table.spark_sql("ALTER TABLE {table} CREATE TAG first_insert")
    spark_table.spark_sql("ALTER TABLE {table} CREATE BRANCH audit")
    spark_table.spark_sql(
        f"INSERT INTO {{table}}.branch_audit {ROWS.format(start=5000, end=5100)}"
    )
    spark_table.spark_sql("DELETE FROM {table}.branch_audit WHERE id < 100")
    spark_table.insert(500, 1000)
    spark_table.call("compute_table_stats(table => '{name}')")
    spark_table.call("compute_partition_stats(table => '{name}')")
    spark_table.attach()

    spark_table.assert_same_rows()


def test_many_snapshots(spark_table):
    spark_table.create(
        PARTITIONINGS["bucket"],
        properties={"write.delete.mode": "merge-on-read"},
    )
    spark_table.attach()
    for start in range(0, 1000, 20):
        spark_table.insert(start, start + 20)
        if start % 200 == 0:
            spark_table.spark_sql(f"DELETE FROM {{table}} WHERE id = {start + 1}")
    spark_table.assert_same_rows()

    spark_table.call(
        "expire_snapshots(table => '{name}', older_than => TIMESTAMP '2100-01-01', retain_last => 5)"
    )
    spark_table.assert_same_rows()


ROWS = """SELECT id, concat('name_', id) AS name, concat('c', id % 5) AS category,
                 CAST(id * 1.25 AS DECIMAL(12,2)) AS amount,
                 TIMESTAMP_NTZ'2024-01-01 00:00:00'
                     + make_interval(0, 0, 0, 0, CAST(id % 40 AS INT) * 1000, 0, 0) AS ts
          FROM range({start}, {end})"""


def comparable(value):
    # Spark and psycopg2 give the same decimal different exponents
    if isinstance(value, Decimal):
        return value.normalize()
    return str(value)


class SparkTable:
    """A table that Spark writes through the Polaris catalog, and the
    read-only pg_lake table attached to it."""

    def __init__(self, spark, namespace, pg_conn, s3):
        self.spark = spark
        self.namespace = namespace
        self.pg_conn = pg_conn
        self.s3 = s3
        self.name = f"{namespace}.t"
        self.spark_name = f"{server_params.SPARK_REST_CATALOG}.{self.name}"
        self.pg_name = f'"{namespace}".t'

    def spark_sql(self, query):
        return self.spark.sql(query.format(table=self.spark_name)).collect()

    def call(self, procedure):
        self.spark.sql(
            f"CALL {server_params.SPARK_REST_CATALOG}.system."
            + procedure.format(name=self.name)
        ).collect()

    def create(self, partitioning="", format_version=2, properties={}, columns=COLUMNS):
        properties = {"format-version": str(format_version), **properties}
        tblproperties = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
        self.spark_sql(
            f"""CREATE TABLE {{table}} ({columns}) USING iceberg {partitioning}
                TBLPROPERTIES ({tblproperties})"""
        )

    def insert(self, start, end, rows=ROWS):
        self.spark_sql(f"INSERT INTO {{table}} {rows.format(start=start, end=end)}")

    def change_schema(self, change):
        """Apply the change, then append a copy of the rows under new ids,
        so the table has files written both before and after it."""
        for ddl in change.ddl:
            self.spark_sql(f"ALTER TABLE {{table}} {ddl}")
        self.spark.table(self.spark_name).withColumn("id", col("id") + 1000).writeTo(
            self.spark_name
        ).append()
        if change.fill is not None:
            self.spark_sql(f"UPDATE {{table}} SET {change.fill} WHERE id >= 1000")

    def attach(self):
        run_command(f'CREATE SCHEMA IF NOT EXISTS "{self.namespace}"', self.pg_conn)
        run_command(
            f"""CREATE TABLE {self.pg_name} () USING iceberg
                WITH (catalog='rest', read_only=True)""",
            self.pg_conn,
        )
        self.pg_conn.commit()

    def detach(self):
        run_command(f"DROP TABLE {self.pg_name}", self.pg_conn)
        self.pg_conn.commit()

    def assert_same_rows(self, where="true", columns=BASE_COLUMNS, pg_table=None):
        """Compare the rows Spark and pg_lake read. A column is either an
        expression both accept, or a (Spark, Postgres) pair of expressions.
        pg_table, by default the attached table, is the one Postgres reads."""
        spark_columns, pg_columns = zip(
            *[(c, c) if isinstance(c, str) else c for c in columns]
        )
        spark_rows = self.spark_sql(
            f"SELECT {', '.join(spark_columns)} FROM {{table}} WHERE {where} ORDER BY id"
        )
        pg_rows = run_query(
            f"SELECT {', '.join(pg_columns)} FROM {pg_table or self.pg_name} "
            f"WHERE {where} ORDER BY id",
            self.pg_conn,
        )
        self.pg_conn.commit()

        assert len(pg_rows) == len(spark_rows)
        for pg_row, spark_row in zip(pg_rows, spark_rows):
            assert [comparable(v) for v in pg_row] == [comparable(v) for v in spark_row]

    def assert_refused(self, message):
        with pytest.raises(psycopg2.Error, match=message):
            run_query(f"SELECT * FROM {self.pg_name}", self.pg_conn)
        self.pg_conn.rollback()

    def explain(self, where="true"):
        plan = run_query(
            f"""EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)
                SELECT * FROM {self.pg_name} WHERE {where}""",
            self.pg_conn,
        )
        self.pg_conn.commit()
        return plan

    def data_files_scanned(self, where="true"):
        return int(fetch_data_files_used(self.explain(where)))

    def delete_files_scanned(self):
        return int(fetch_delete_files_used(self.explain()))

    def avro_codec(self, path):
        bucket, key = path.removeprefix("s3://").split("/", 1)
        head = self.s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-8191")
        head = head["Body"].read()
        at = head.index(b"avro.codec") + len(b"avro.codec")
        # an Avro string is its zigzag-encoded length, then its bytes
        length = head[at] // 2
        return head[at + 1 : at + 1 + length].decode()


@pytest.fixture
def spark_table(
    installcheck,
    spark_rest_catalog,
    pg_conn,
    s3,
    set_polaris_gucs,
    with_default_location,
):
    if installcheck:
        pytest.skip("requires Spark and the Polaris fixture")

    namespace = f"spark_{uuid.uuid4().hex[:8]}"
    spark_rest_catalog.sql(
        f"CREATE NAMESPACE {server_params.SPARK_REST_CATALOG}.{namespace}"
    )
    table = SparkTable(spark_rest_catalog, namespace, pg_conn, s3)

    yield table

    pg_conn.rollback()
    run_command(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE', pg_conn)
    pg_conn.commit()
    table.spark_sql("DROP TABLE IF EXISTS {table} PURGE")
    spark_rest_catalog.sql(
        f"DROP NAMESPACE {server_params.SPARK_REST_CATALOG}.{namespace}"
    )

"""Iceberg tables written other ways than by Spark's Iceberg writer read the
same in pg_lake, attached read-only, as they do in Spark: a Hive table that
Spark migrates in place, and tables PyIceberg and DuckDB write through a REST
catalog. COPY FROM applies the deletes of a table Spark writes."""

import datetime
import json
import urllib.parse
import uuid
from pathlib import Path

import psycopg2
import pyarrow
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    FixedType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimeType,
    UUIDType,
)
from helpers import server_params
from helpers.polaris import *
from helpers.spark import (
    create_spark_catalog_database,
    spark_rest_catalog,
    spark_session,
)
from test_read_only_spark_written_tables import (
    DELETION_VECTORS_REFUSED,
    PARTITIONINGS,
    ROW_LEVEL_CHANGES,
    SparkTable,
    spark_table,
)
from utils_pytest import *


HIVE_COLUMNS = """id BIGINT, name STRING, amount DECIMAL(12,2), ts TIMESTAMP,
    tags ARRAY<STRING>, attrs MAP<STRING, INT>, info STRUCT<a: INT, b: STRING>,
    category STRING"""

HIVE_ROWS = """SELECT id, concat('name_', id) AS name,
                      CAST(id * 1.25 AS DECIMAL(12,2)) AS amount,
                      TIMESTAMP'2024-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0) AS ts,
                      array(concat('t', id), NULL) AS tags,
                      map('k', CAST(id AS INT)) AS attrs,
                      named_struct('a', CAST(id AS INT), 'b', concat('b_', id)) AS info,
                      concat('c', id % 3) AS category
               FROM range(0, 300)"""

HIVE_PROJECTION = [
    "id",
    "name",
    "amount",
    (
        "date_format(ts, 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
        "to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')",
    ),
    "tags",
    ("attrs['k']", "map_type.extract(attrs, 'k')"),
    ("info.a", "(info).a"),
    ("info.b", "(info).b"),
    "category",
]


@pytest.mark.parametrize(
    "parquet_settings",
    [
        {},
        {"spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS"},
        {"spark.sql.parquet.writeLegacyFormat": "true"},
    ],
    ids=["int96_timestamps", "micros_timestamps", "legacy_format"],
)
def test_migrated_hive_table(spark_table, tmp_path, parquet_settings):
    """Spark's plain Parquet writer puts a Hive table's partition values in
    the file paths only, and gives its files no field ids. Adding those files
    the way Spark's add_files and migrate procedures do leaves the partition
    column to be read from the manifests, and the other columns to be found
    by the name mapping."""
    spark = spark_table.spark
    spark_table.create(PARTITIONINGS["identity"], columns=HIVE_COLUMNS)

    for key, value in parquet_settings.items():
        spark.conf.set(key, value)
    try:
        spark.sql(HIVE_ROWS).write.partitionBy("category").parquet(
            f"file://{tmp_path}/hive"
        )
    finally:
        for key in parquet_settings:
            spark.conf.unset(key)

    table = iceberg_table(spark_table)
    files = upload_hive_files(spark_table.s3, tmp_path / "hive", table.location())
    add_files(spark_table, table, files)

    # written after the migration, with every column and field ids
    spark_table.spark_sql(
        f"INSERT INTO {{table}} {HIVE_ROWS.replace('range(0, 300)', 'range(300, 400)')}"
    )
    spark_table.attach()

    spark_table.assert_same_rows(columns=HIVE_PROJECTION)
    spark_table.assert_same_rows("category = 'c1'", columns=HIVE_PROJECTION)
    assert (
        spark_table.data_files_scanned("category = 'c1'")
        < spark_table.data_files_scanned()
    )


HIVE_PARTITION_COLUMNS = """id BIGINT, name STRING, p_str STRING, p_int INT,
    p_date DATE, p_bool BOOLEAN, p_dec DECIMAL(10,2)"""

HIVE_PARTITION_ROWS = """SELECT id, concat('name_', id) AS name,
                                CASE WHEN id % 7 = 0 THEN NULL ELSE concat('s', id % 2) END AS p_str,
                                CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id % 2 AS INT) END AS p_int,
                                DATE'2024-02-29' + CAST(id % 2 AS INT) AS p_date,
                                id % 2 = 0 AS p_bool,
                                CAST(1.25 + id % 2 AS DECIMAL(10,2)) AS p_dec
                         FROM range(0, 200)"""


def test_migrated_hive_table_partition_types(spark_table, tmp_path):
    spark = spark_table.spark
    spark_table.create(
        "PARTITIONED BY (p_str, p_int, p_date, p_bool, p_dec)",
        columns=HIVE_PARTITION_COLUMNS,
    )
    spark.sql(HIVE_PARTITION_ROWS).write.partitionBy(
        "p_str", "p_int", "p_date", "p_bool", "p_dec"
    ).parquet(f"file://{tmp_path}/hive")

    table = iceberg_table(spark_table)
    files = upload_hive_files(spark_table.s3, tmp_path / "hive", table.location())
    add_files(spark_table, table, files)
    spark_table.attach()

    columns = ["id", "name", "p_str", "p_int", "p_date", "p_bool", "p_dec"]
    spark_table.assert_same_rows(columns=columns)
    for where in [
        "p_str IS NULL",
        "p_int = 1",
        "p_date = '2024-03-01'",
        "p_bool",
        "p_dec > 2",
    ]:
        spark_table.assert_same_rows(where, columns=columns)


@pytest.mark.parametrize("format_version", [2, 3])
def test_copy_from_table_with_deletes(spark_table, format_version):
    spark_table.create(
        PARTITIONINGS["identity"],
        format_version=format_version,
        properties={"write.delete.mode": "merge-on-read"},
    )
    spark_table.insert(0, 1000)
    spark_table.spark_sql(ROW_LEVEL_CHANGES["delete"])

    copied = f'"{spark_table.namespace}".copied'
    run_command(f'CREATE SCHEMA "{spark_table.namespace}"', spark_table.pg_conn)
    run_command(
        f"""CREATE TABLE {copied}
              (id bigint, name text, category text, amount numeric(12,2), ts timestamp)""",
        spark_table.pg_conn,
    )
    spark_table.pg_conn.commit()

    copy = (
        f"COPY {copied} FROM '{metadata_location(spark_table)}' WITH (format 'iceberg')"
    )
    if format_version == 3:
        with pytest.raises(psycopg2.Error, match=DELETION_VECTORS_REFUSED):
            run_command(copy, spark_table.pg_conn)
        spark_table.pg_conn.rollback()

        spark_table.call(
            "rewrite_data_files(table => '{name}', options => map('delete-file-threshold', '1'))"
        )
        copy = f"COPY {copied} FROM '{metadata_location(spark_table)}' WITH (format 'iceberg')"

    run_command(copy, spark_table.pg_conn)
    spark_table.pg_conn.commit()

    spark_table.assert_same_rows(pg_table=copied)


def test_pyiceberg_partition_spec_changes(spark_table):
    catalog = create_iceberg_rest_catalog(spark_table.namespace)
    table = catalog.create_table(spark_table.name, schema=PYICEBERG_SCHEMA)

    table.append(pyiceberg_rows(table, 0, 100))
    with table.update_spec() as update:
        update.add_identity("category")
    table.append(pyiceberg_rows(table, 100, 200))
    # PyIceberg writes other transforms only with pyiceberg-core installed
    with table.update_spec() as update:
        update.add_identity("name")
    table.append(pyiceberg_rows(table, 200, 300))
    with table.update_spec() as update:
        update.remove_field("category")
    table.append(pyiceberg_rows(table, 300, 400))
    spark_table.attach()

    spark_table.assert_same_rows(columns=PYICEBERG_COLUMNS)
    for where in ["category = 'c1'", "name = 'name_242'", "name = 'name_342'"]:
        spark_table.assert_same_rows(where, columns=PYICEBERG_COLUMNS)
        assert spark_table.data_files_scanned(where) < spark_table.data_files_scanned()


def test_pyiceberg_deletes_and_overwrites(spark_table):
    catalog = create_iceberg_rest_catalog(spark_table.namespace)
    table = catalog.create_table(spark_table.name, schema=PYICEBERG_SCHEMA)
    with table.update_spec() as update:
        update.add_identity("category")

    table.append(pyiceberg_rows(table, 0, 300))
    table.delete("id < 50")
    table.delete("category == 'c2' and id >= 250")
    table.overwrite(
        pyiceberg_rows(table, 1000, 1010, category="c1"),
        overwrite_filter="category == 'c1'",
    )
    spark_table.attach()

    spark_table.assert_same_rows(columns=PYICEBERG_COLUMNS)


def test_pyiceberg_time_uuid_and_fixed_columns(spark_table):
    """Spark cannot read time columns, so compare with PyIceberg's scan."""
    catalog = create_iceberg_rest_catalog(spark_table.namespace)
    table = catalog.create_table(
        spark_table.name,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "t", TimeType(), required=False),
            NestedField(3, "u", UUIDType(), required=False),
            NestedField(4, "fx", FixedType(3), required=False),
        ),
    )
    values = [
        (1, datetime.time(0, 0), uuid.UUID(int=0), b"\x00\x00\x00"),
        (
            2,
            datetime.time(23, 59, 59, 999999),
            uuid.UUID(int=2**128 - 1),
            b"\xff\xfe\xfd",
        ),
        (3, datetime.time(12, 30, 0, 1), uuid.uuid4(), b"abc"),
        (4, None, None, None),
    ]
    table.append(
        pyarrow.Table.from_pylist(
            [
                {"id": i, "t": t, "u": u.bytes if u else None, "fx": fx}
                for i, t, u, fx in values
            ],
            table.schema().as_arrow(),
        )
    )
    spark_table.attach()

    rows = run_query(
        f"SELECT id, t, u, fx FROM {spark_table.pg_name} ORDER BY id",
        spark_table.pg_conn,
    )
    spark_table.pg_conn.commit()
    assert [
        [i, t, uuid.UUID(u) if u else None, bytes(fx) if fx else None]
        for i, t, u, fx in rows
    ] == [list(v) for v in values]
    assert run_query(
        f"SELECT id FROM {spark_table.pg_name} WHERE t > '12:00' ORDER BY id",
        spark_table.pg_conn,
    ) == [[2], [3]]
    spark_table.pg_conn.commit()


def test_duckdb_written_table(spark_table):
    duckdb_conn = create_duckdb_conn()
    try:
        attach_duckdb_to_polaris(duckdb_conn)
        name = f"polaris.{spark_table.namespace}.t"
        duckdb_conn.execute(
            f"""CREATE TABLE {name} (id BIGINT, name VARCHAR, category VARCHAR,
                                    amount DECIMAL(12,2), ts TIMESTAMP)"""
        )
        for start in (0, 500):
            duckdb_conn.execute(
                f"""INSERT INTO {name}
                    SELECT i, 'name_' || i, 'c' || (i % 5), (i * 1.25)::DECIMAL(12,2),
                           TIMESTAMP '2024-01-01' + to_hours((i % 40) * 1000)
                    FROM range({start}, {start + 500}) r(i)"""
            )
        duckdb_conn.execute(f"DELETE FROM {name} WHERE id % 7 = 0")
        duckdb_conn.execute(
            f"UPDATE {name} SET name = name || '_updated' WHERE id % 3 = 0"
        )
    finally:
        duckdb_conn.close()
    spark_table.attach()

    spark_table.assert_same_rows()
    spark_table.assert_same_rows("id < 100")


PYICEBERG_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "category", StringType(), required=False),
    NestedField(4, "ts", TimestampType(), required=False),
)
PYICEBERG_COLUMNS = ["id", "name", "category", "ts"]


def pyiceberg_rows(table, start, end, category=None):
    return pyarrow.Table.from_pylist(
        [
            {
                "id": i,
                "name": f"name_{i}",
                "category": category or f"c{i % 5}",
                "ts": datetime.datetime(2024, 1, 1) + datetime.timedelta(hours=i),
            }
            for i in range(start, end)
        ],
        table.schema().as_arrow(),
    )


def iceberg_table(spark_table):
    spark = spark_table.spark
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, spark_table.spark_name
    )


def metadata_location(spark_table):
    return iceberg_table(spark_table).operations().current().metadataFileLocation()


def upload_hive_files(s3, local_dir, location):
    """Upload the Parquet files under local_dir to the table's location,
    keeping their <column>=<value> directories, and return each file's path,
    size and partition values."""
    bucket, prefix = location.removeprefix("s3://").split("/", 1)
    files = []
    for path in sorted(local_dir.rglob("*.parquet")):
        relative = path.relative_to(local_dir)
        key = f"{prefix}/hive/{relative.as_posix()}"
        s3.upload_file(str(path), bucket, key)
        values = [
            (
                None
                if value == "__HIVE_DEFAULT_PARTITION__"
                else urllib.parse.unquote(value)
            )
            for value in (part.split("=", 1)[1] for part in relative.parent.parts)
        ]
        files.append((f"s3://{bucket}/{key}", path.stat().st_size, values))
    return files


def add_files(spark_table, table, files):
    """Append the files as Spark's add_files and migrate procedures do: take
    the partition from each file's directories and the metrics from its
    footer through the table's name mapping, which they also set."""
    spark = spark_table.spark
    iceberg = spark._jvm.org.apache.iceberg
    mapping = iceberg.mapping.MappingUtil.create(table.schema())
    table.updateProperties().set(
        "schema.name-mapping.default", iceberg.mapping.NameMappingParser.toJson(mapping)
    ).commit()

    metrics_config = iceberg.MetricsConfig.forTable(table)
    append = table.newAppend()
    for path, size, values in files:
        metrics = iceberg.parquet.ParquetUtil.fileMetrics(
            table.io().newInputFile(path), metrics_config, mapping
        )
        partition_values = spark._jvm.java.util.ArrayList()
        for value in values:
            partition_values.add(value)
        append.appendFile(
            iceberg.DataFiles.builder(table.spec())
            .withPath(path)
            .withFormat(iceberg.FileFormat.PARQUET)
            .withFileSizeInBytes(size)
            .withMetrics(metrics)
            .withPartitionValues(partition_values)
            .build()
        )
    append.commit()
    spark.sql(f"REFRESH TABLE {spark_table.spark_name}")


def attach_duckdb_to_polaris(duckdb_conn):
    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    uri = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog"
    duckdb_conn.execute("INSTALL iceberg")
    duckdb_conn.execute("LOAD iceberg")
    duckdb_conn.execute(
        f"""CREATE SECRET polaris_secret (
                TYPE iceberg,
                CLIENT_ID '{creds["credentials"]["clientId"]}',
                CLIENT_SECRET '{creds["credentials"]["clientSecret"]}',
                OAUTH2_SERVER_URI '{uri}/v1/oauth/tokens',
                OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')"""
    )
    duckdb_conn.execute(
        f"""ATTACH '{server_params.PG_DATABASE}' AS polaris (
                TYPE iceberg, SECRET polaris_secret, ENDPOINT '{uri}',
                ACCESS_DELEGATION_MODE 'none')"""
    )

"""Interoperate with the repository's Spark 3.5 / Apache Iceberg 1.4.3 fixture.

Spark writes the original data. Apache Iceberg's GenericAppenderFactory writes
Parquet equality/position deletes and new data in one RowDelta commit. Spark's
independent Iceberg reader validates the result before either pg_lake executor.
All objects use the test S3 service and are regenerated for every run.
"""

import uuid
from collections import Counter

import pytest
from helpers.spark import create_spark_catalog_database, spark_session
from utils_pytest import run_command, run_query


def test_spark_row_delta(installcheck, spark_session, s3, pg_conn, extension):
    if installcheck:
        pytest.skip("requires the Spark integration fixture")

    namespace = f"equality_interop_{uuid.uuid4().hex}"
    name = f"{namespace}.rows"
    spark = spark_session
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, payload STRING) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        )
        spark.sql(
            f"INSERT INTO {name} VALUES (1,'duplicate'),(1,'duplicate'),"
            "(2,'equality'),(3,'position'),(NULL,'null equality'),(4,'overlap')"
        )
        positions = spark.sql(
            f"SELECT _file, _pos FROM {name} WHERE id IN (3,4) ORDER BY _file, _pos"
        ).collect()

        jvm = spark._jvm
        iceberg = jvm.org.apache.iceberg
        table = iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
        schema = table.schema()
        names = jvm.java.util.ArrayList()
        names.add("id")
        keys = schema.select(names)
        ids = spark.sparkContext._gateway.new_array(jvm.int, 1)
        ids[0] = schema.findField("id").fieldId()
        factory = iceberg.data.GenericAppenderFactory(
            schema, table.spec(), ids, keys, None
        )

        def output(kind):
            path = f"{table.location()}/data/{uuid.uuid4()}-{kind}.parquet"
            return iceberg.encryption.EncryptedFiles.encryptedOutput(
                table.io().newOutputFile(path),
                iceberg.encryption.EncryptionKeyMetadata.empty(),
            )

        equality = factory.newEqDeleteWriter(
            output("equality"), iceberg.FileFormat.PARQUET, None
        )
        try:
            for key in (2, 4, None, 2):
                record = iceberg.data.GenericRecord.create(keys)
                record.setField("id", key)
                equality.write(record)
        finally:
            equality.close()

        position = factory.newPosDeleteWriter(
            output("position"), iceberg.FileFormat.PARQUET, None
        )
        try:
            for path, pos in positions:
                deletion = iceberg.deletes.PositionDelete.create()
                deletion.set(path, pos, None)
                position.write(deletion)
        finally:
            position.close()

        data = factory.newDataWriter(output("data"), iceberg.FileFormat.PARQUET, None)
        try:
            record = iceberg.data.GenericRecord.create(schema)
            record.setField("id", 2)
            record.setField("payload", "same commit")
            data.write(record)
        finally:
            data.close()

        table.newRowDelta().addRows(data.toDataFile()).addDeletes(
            equality.toDeleteFile()
        ).addDeletes(position.toDeleteFile()).commit()
        spark.sql(f"REFRESH TABLE {name}")
        expected = Counter([(1, "duplicate"), (1, "duplicate"), (2, "same commit")])
        assert (
            Counter(tuple(row) for row in spark.sql(f"SELECT * FROM {name}").collect())
            == expected
        )

        metadata = table.operations().current().metadataFileLocation()
        run_command(
            f"CREATE FOREIGN TABLE equality_interop () SERVER pg_lake "
            f"OPTIONS (path '{metadata}', format 'iceberg')",
            pg_conn,
        )
        for enabled in ("on", "off"):
            run_command(
                f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}",
                pg_conn,
            )
            plan = str(run_query("EXPLAIN SELECT * FROM equality_interop", pg_conn))
            if enabled == "on":
                assert "Custom Scan (Query Pushdown)" in plan
            else:
                assert "Foreign Scan" in plan and "Query Pushdown" not in plan
            assert (
                Counter(
                    tuple(row)
                    for row in run_query("SELECT * FROM equality_interop", pg_conn)
                )
                == expected
            )
        pg_conn.rollback()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP NAMESPACE {namespace}")

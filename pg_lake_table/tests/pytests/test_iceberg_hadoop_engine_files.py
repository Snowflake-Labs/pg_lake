"""Read Iceberg tables with files the way engines built on Hadoop write them.

Spark writes the tables through the repository's Spark 3.5 / Apache Iceberg
fixture. Its S3FileIO also takes the s3a:// scheme, so a table created at
an s3a:// location carries s3a:// paths in its metadata, manifests and position
delete files, like one written through Hadoop's S3A file system.
"""

import uuid
from collections import Counter

import pytest
from helpers import server_params
from helpers.spark import create_spark_catalog_database, spark_session
from utils_pytest import (
    TEST_BUCKET,
    catalog_log_lines,
    run_command,
    run_query,
    server_log_offset,
)


def test_s3a_paths_and_position_deletes(
    installcheck, spark_session, s3, pg_conn, extension
):
    if installcheck:
        pytest.skip("requires the Spark integration fixture")

    namespace = f"hadoop_files_{uuid.uuid4().hex}"
    name = f"{namespace}.rows"
    spark = spark_session
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, payload STRING) USING iceberg "
            f"LOCATION 's3a://{TEST_BUCKET}/{namespace}/rows' "
            "TBLPROPERTIES ('format-version'='2', "
            "'write.delete.mode'='merge-on-read')"
        )
        # One data file, so the delete below writes position deletes rather
        # than dropping whole files.
        spark.sql(
            f"INSERT INTO {name} SELECT /*+ REPARTITION(1) */ * FROM VALUES "
            "(1,'kept'),(2,'deleted'),(3,'kept'),(4,'deleted') AS t(id, payload)"
        )
        spark.sql(f"DELETE FROM {name} WHERE id IN (2, 4)")

        files = spark.sql(f"SELECT content, file_path FROM {name}.files").collect()
        assert {row.content for row in files} == {0, 1}
        assert all(row.file_path.startswith("s3a://") for row in files)

        metadata = metadata_location(spark, name)
        assert metadata.startswith("s3a://")

        expected = Counter([(1, "kept"), (3, "kept")])
        assert (
            Counter(tuple(row) for row in spark.sql(f"SELECT * FROM {name}").collect())
            == expected
        )
        # the s3a:// metadata location Spark reports is accepted as it is
        assert_pg_lake_rows(pg_conn, metadata, expected)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP NAMESPACE {namespace}")


@pytest.mark.parametrize("file_format", ["orc", "avro"])
def test_non_parquet_data_files_are_refused(
    installcheck, spark_session, s3, pg_conn, extension, file_format
):
    """DuckDB would fail on these as malformed Parquet, so pg_lake names the
    format instead."""
    if installcheck:
        pytest.skip("requires the Spark integration fixture")

    namespace = f"hadoop_files_{uuid.uuid4().hex}"
    name = f"{namespace}.rows"
    spark = spark_session
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, payload STRING) USING iceberg "
            f"TBLPROPERTIES ('format-version'='2', "
            f"'write.format.default'='{file_format}')"
        )
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")

        offset = server_log_offset()
        error = run_command(
            "CREATE FOREIGN TABLE non_parquet () SERVER pg_lake "
            f"OPTIONS (path '{metadata_location(spark, name)}', format 'iceberg')",
            pg_conn,
            raise_error=False,
        ) or run_query("SELECT * FROM non_parquet", pg_conn, raise_error=False)
        assert f"{file_format.upper()} files are not supported" in str(error)
        assert catalog_log_lines(offset) == [
            "a table uses Iceberg files in a format other than Parquet, "
            "which pg_lake does not support"
        ]
        pg_conn.rollback()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP NAMESPACE {namespace}")


def test_non_parquet_files_only_refused_when_read(
    installcheck, spark_session, s3, pg_conn, extension
):
    """A rewrite from ORC to Parquet leaves the ORC files in the current
    snapshot as deleted entries, and pruning can skip the ORC data files of
    a mixed-format table; neither is a file the read opens. Delete files are
    not pruned, so an ORC delete file is refused whenever the table is read."""
    if installcheck:
        pytest.skip("requires the Spark integration fixture")

    namespace = f"hadoop_files_{uuid.uuid4().hex}"
    name = f"{namespace}.rows"
    spark = spark_session
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT, part STRING) USING iceberg "
            "PARTITIONED BY (part) "
            "TBLPROPERTIES ('format-version'='2', 'write.format.default'='orc')"
        )
        spark.sql(f"INSERT INTO {name} VALUES (1, 'orc')")
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES ('write.format.default'='parquet')"
        )
        spark.sql(f"INSERT INTO {name} VALUES (2, 'parquet')")

        run_command(
            "CREATE FOREIGN TABLE mixed_formats () SERVER pg_lake "
            f"OPTIONS (path '{metadata_location(spark, name)}', format 'iceberg')",
            pg_conn,
        )
        assert run_query(
            "SELECT id FROM mixed_formats WHERE part = 'parquet'", pg_conn
        ) == [[2]]
        error = run_query("SELECT id FROM mixed_formats", pg_conn, raise_error=False)
        assert "ORC files are not supported" in str(error)
        pg_conn.rollback()

        spark.sql(
            f"CALL {server_params.SPARK_CATALOG}.system.rewrite_data_files("
            f"table => '{name}', options => map('rewrite-all', 'true'))"
        )
        files = spark.sql(
            f"SELECT data_file.file_format, status FROM {name}.entries"
        ).collect()
        assert {(row.file_format, row.status) for row in files} >= {("ORC", 2)}

        run_command(
            "CREATE FOREIGN TABLE rewritten () SERVER pg_lake "
            f"OPTIONS (path '{metadata_location(spark, name)}', format 'iceberg')",
            pg_conn,
        )
        assert run_query("SELECT id FROM rewritten ORDER BY id", pg_conn) == [
            [1],
            [2],
        ]
        pg_conn.rollback()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP NAMESPACE {namespace}")


def test_hadoop_scheme_path_only_for_iceberg(pg_conn, extension):
    error = run_command(
        "CREATE FOREIGN TABLE not_iceberg () SERVER pg_lake "
        f"OPTIONS (path 's3a://{TEST_BUCKET}/hadoop_files/data.parquet')",
        pg_conn,
        raise_error=False,
    )
    assert "unsupported URL" in str(error)
    assert "only in Iceberg tables" in str(error)
    pg_conn.rollback()


def metadata_location(spark, name):
    iceberg = spark._jvm.org.apache.iceberg
    table = iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    return table.operations().current().metadataFileLocation()


def assert_pg_lake_rows(pg_conn, metadata, expected):
    run_command(
        "CREATE FOREIGN TABLE hadoop_files () SERVER pg_lake "
        f"OPTIONS (path '{metadata}', format 'iceberg')",
        pg_conn,
    )
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}", pg_conn
        )
        assert (
            Counter(
                tuple(row) for row in run_query("SELECT * FROM hadoop_files", pg_conn)
            )
            == expected
        )
    pg_conn.rollback()

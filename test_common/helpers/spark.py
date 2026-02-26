"""
Spark-related test utilities and fixtures.

This module isolates all pyspark dependencies so that test files
that don't need Spark don't pull in the pyspark package.
"""

import os
import importlib.metadata

import pytest
from . import server_params

from utils_pytest import (
    run_query,
    run_command_outside_tx,
    compare_rows_as_string_or_float,
    compare_results_with_duckdb,
    TEST_BUCKET,
    TEST_AWS_REGION,
    TEST_AWS_ACCESS_KEY_ID,
    TEST_AWS_SECRET_ACCESS_KEY,
    MOTO_PORT,
)

from pyspark.errors import NumberFormatException
from pyspark.sql import SparkSession


def assert_query_result_on_spark_and_pg(
    installcheck, spark_session, pg_conn, spark_query, pg_query
):
    if installcheck:
        return

    # run query on spark table
    spark_result = spark_session.sql(spark_query).collect()

    # compare spark result with pg_lake result
    pg_lake_result = run_query(pg_query, pg_conn)

    assert (
        len(pg_lake_result) > 0
    ), "No rows returned, make sure at least one row returns"
    assert len(pg_lake_result) == len(
        spark_result
    ), "Result sets have different lengths"

    for row_pg_lake, row_spark in zip(pg_lake_result, spark_result):
        assert compare_rows_as_string_or_float(
            row_pg_lake, row_spark, 0.001
        ), f"Results do not match: {row_pg_lake} and {row_spark}"

    return pg_lake_result


def spark_register_table(
    installcheck, spark_session, table_name, table_namespace, metadata_location
):
    if installcheck:
        return

    table_name_str = (
        f"{table_namespace}.{table_name}" if table_namespace else table_name
    )

    # todo: remove try block. NumberFormatException is thrown even if the table is successfully registered
    try:
        spark_session.sql(
            f"""
            CALL {server_params.SPARK_CATALOG}.system.register_table(
                table => '{table_name_str}',
                metadata_file => '{metadata_location}'
            )
            """
        )
    except NumberFormatException:
        pass


def spark_unregister_table(installcheck, spark_session, table_name, table_namespace):
    if installcheck:
        return

    table_name_str = (
        f"{table_namespace}.{table_name}" if table_namespace else table_name
    )
    spark_session.sql(f"DROP TABLE {table_name_str}")


def compare_results_with_pyspark(
    installcheck,
    pg_conn,
    spark_session,
    table_name,
    table_namespace,
    metadata_location,
    query,
):
    if installcheck:
        return

    spark_register_table(
        installcheck, spark_session, table_name, table_namespace, metadata_location
    )

    assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, query, query
    )

    spark_unregister_table(installcheck, spark_session, table_name, table_namespace)


def compare_results_with_reference_iceberg_implementations(
    installcheck,
    pg_conn,
    duckdb_conn,
    spark_session,
    table_name,
    table_namespace,
    query,
    metadata_location=None,
):
    if metadata_location is not None:
        # already provided
        pass
    else:
        if table_namespace is not None:
            get_metadata_location_query = f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'"
            metadata_location = run_query(get_metadata_location_query, pg_conn)[0][0]
        else:
            get_metadata_location_query = f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'"
            locations = run_query(get_metadata_location_query, pg_conn)
            assert len(locations) == 1, "use a different table name"
            metadata_location = locations[0][0]

    if spark_session is not None:
        compare_results_with_pyspark(
            installcheck,
            pg_conn,
            spark_session,
            table_name,
            table_namespace,
            metadata_location,
            query,
        )

    if duckdb_conn is not None:
        compare_results_with_duckdb(
            pg_conn, duckdb_conn, table_name, table_namespace, metadata_location, query
        )


@pytest.fixture(scope="module")
def create_spark_catalog_database(installcheck, superuser_conn):
    if not installcheck:
        run_command_outside_tx(
            [f"CREATE DATABASE {server_params.SPARK_CATALOG_PG_DATABASE}"],
            superuser_conn,
        )

    yield

    if not installcheck:
        run_command_outside_tx(
            [f"DROP DATABASE {server_params.SPARK_CATALOG_PG_DATABASE} WITH (FORCE)"],
            superuser_conn,
        )


@pytest.fixture(scope="module")
def spark_session(installcheck, create_spark_catalog_database):
    spark_session = None

    if not installcheck:
        spark_version = ".".join(importlib.metadata.version("pyspark").split(".")[:2])
        # got these versions from pyiceberg tests
        # (https://github.com/apache/iceberg-python/blob/ff3a24981c59c2d1935cde02b72ff4249e4d8525/tests/conftest.py#L2224)
        scala_version = "2.12"
        iceberg_version = "1.4.3"

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version},"
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version} pyspark-shell"
        )
        os.environ["AWS_REGION"] = TEST_AWS_REGION
        os.environ["AWS_ACCESS_KEY_ID"] = TEST_AWS_ACCESS_KEY_ID
        os.environ["AWS_SECRET_ACCESS_KEY"] = TEST_AWS_SECRET_ACCESS_KEY

        # spark timestamp are alias of timestamp_ltz (it adjusts the time to the local timezone.
        # To compare Postgres timestamptz values with spark's timestamp,
        # we set system timezone to utc so soark uses ut for local timezone)
        prev_tz = os.environ.get("TZ")
        os.environ["TZ"] = "UTC"

        spark_session = (
            SparkSession.builder.appName("spark catalog test")
            .config(
                f"spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.driver.host", "localhost")
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.catalog-impl",
                "org.apache.iceberg.jdbc.JdbcCatalog",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.uri",
                f"jdbc:postgresql://localhost:{server_params.PG_PORT}/{server_params.SPARK_CATALOG_PG_DATABASE}",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.jdbc.user",
                server_params.PG_USER,
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.jdbc.password",
                server_params.PG_PASSWORD,
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.warehouse",
                f"s3://{TEST_BUCKET}/",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.s3.endpoint",
                f"http://localhost:{MOTO_PORT}",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.s3.path-style-access",
                "true",
            )
            .config(f"spark.driver.extraClassPath", os.getenv("JDBC_DRIVER_PATH"))
            .getOrCreate()
        )

        # we do not need to specify catalog name in queries or DDLs anymore
        spark_session.catalog.setCurrentCatalog(server_params.SPARK_CATALOG)

        # set system timezone to old one
        if prev_tz:
            os.environ["TZ"] = prev_tz

    yield spark_session

    if not installcheck:
        spark_session.stop()

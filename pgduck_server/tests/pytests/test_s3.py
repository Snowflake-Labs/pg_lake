import os
import pytest
from moto.server import ThreadedMotoServer
import boto3
from utils_pytest import *
import server_params
import psycopg2


def test_copy_to_parquet_s3(s3, pgduck_conn):
    perform_query(
        f"""
        COPY (SELECT * FROM generate_series(1,100))
        TO 's3://{TEST_BUCKET}/test_copy_to_parquet_s3/data.parquet';
    """,
        pgduck_conn,
    )

    assert list_objects(s3, TEST_BUCKET, "test_copy_to_parquet_s3/") == [
        "test_copy_to_parquet_s3/data.parquet"
    ]

    perform_query(
        f"""
        CREATE TABLE mytable (x int);
        COPY mytable
        FROM 's3://{TEST_BUCKET}/test_copy_to_parquet_s3/data.parquet';
    """,
        pgduck_conn,
    )

    results = perform_query_on_cursor("SELECT count(*) FROM mytable", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == "100"

    pgduck_conn.rollback()


def test_copy_to_parquet_gcs(gcs, pgduck_conn):
    perform_query(
        f"""
        COPY (SELECT * FROM generate_series(1,100))
        TO 'gs://{TEST_BUCKET_GCS}/test_copy_to_parquet_gcs/data.parquet';
    """,
        pgduck_conn,
    )

    assert list_objects(gcs, TEST_BUCKET_GCS, "test_copy_to_parquet_gcs/") == [
        "test_copy_to_parquet_gcs/data.parquet"
    ]

    perform_query(
        f"""
        CREATE TABLE mytable (x int);
        COPY mytable
        FROM 'gs://{TEST_BUCKET_GCS}/test_copy_to_parquet_gcs/data.parquet';
    """,
        pgduck_conn,
    )

    results = perform_query_on_cursor("SELECT count(*) FROM mytable", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == "100"

    pgduck_conn.rollback()


def test_copy_to_parquet_azure(azure, pgduck_conn):
    perform_query(
        f"""
        COPY (SELECT * FROM generate_series(1,100))
        TO 'az://{TEST_BUCKET}/test_copy_to_parquet_az/data.parquet';
    """,
        pgduck_conn,
    )

    blob_list = list(azure.list_blobs(name_starts_with="test_copy_to_parquet_az/"))
    assert len(blob_list) == 1
    assert blob_list[0].name == "test_copy_to_parquet_az/data.parquet"

    perform_query(
        f"""
        CREATE TABLE mytable (x int);
        COPY mytable
        FROM 'az://{TEST_BUCKET}/test_copy_to_parquet_az/data.parquet';
    """,
        pgduck_conn,
    )

    results = perform_query_on_cursor("SELECT count(*) FROM mytable", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == "100"

    pgduck_conn.rollback()


def test_copy_to_parquet_azure_long_prefix(azure, pgduck_conn):
    """Test that azure:// prefix (long form) works the same as az://"""
    perform_query(
        f"""
        COPY (SELECT * FROM generate_series(1,100))
        TO 'azure://{TEST_BUCKET}/test_copy_to_parquet_azure_long/data.parquet';
    """,
        pgduck_conn,
    )

    blob_list = list(
        azure.list_blobs(name_starts_with="test_copy_to_parquet_azure_long/")
    )
    assert len(blob_list) == 1
    assert blob_list[0].name == "test_copy_to_parquet_azure_long/data.parquet"

    perform_query(
        f"""
        CREATE TABLE mytable (x int);
        COPY mytable
        FROM 'azure://{TEST_BUCKET}/test_copy_to_parquet_azure_long/data.parquet';
    """,
        pgduck_conn,
    )

    results = perform_query_on_cursor("SELECT count(*) FROM mytable", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == "100"

    pgduck_conn.rollback()


def test_copy_from_non_existent(s3, pgduck_conn):
    perform_query("CREATE TABLE mytable (x int)", pgduck_conn)

    results = perform_query_on_cursor(
        f"""
        COPY mytable
        FROM 's3://{TEST_BUCKET}/test_copy_from_non_existent/notexisting.parquet';
    """,
        pgduck_conn,
    )

    assert results == None

    pgduck_conn.rollback()


def test_s3_express(pgduck_conn):
    result = run_query(
        "select pg_lake_test_add_s3_express('s3://test--use1-az4--x-s3/test.csv') test",
        pgduck_conn,
    )
    assert (
        result[0]["test"]
        == "s3://test--use1-az4--x-s3/test.csv?s3_region=us-east-1&s3_endpoint=s3express-use1-az4.us-east-1.amazonaws.com"
    )

    result = run_query(
        "select pg_lake_test_add_s3_express('s3://test--eun1-az4--x-s3/test.csv') test",
        pgduck_conn,
    )
    assert (
        result[0]["test"]
        == "s3://test--eun1-az4--x-s3/test.csv?s3_region=eu-north-1&s3_endpoint=s3express-eun1-az4.eu-north-1.amazonaws.com"
    )

    # unknown region
    error = run_query(
        "select pg_lake_test_add_s3_express('s3://test--nono-az4--x-s3/test.csv') test",
        pgduck_conn,
        raise_error=False,
    )
    assert "not an S3 express URL" in error

    pgduck_conn.rollback()

    # missing suffix
    error = run_query(
        "select pg_lake_test_add_s3_express('s3://test--nono-az4/test.csv') test",
        pgduck_conn,
        raise_error=False,
    )
    assert "not an S3 express URL" in error

    pgduck_conn.rollback()

    # only available for S3
    error = run_query(
        "select pg_lake_test_add_s3_express('gs://test--use1-az4--x-s3/test.csv') test",
        pgduck_conn,
        raise_error=False,
    )
    assert "not an S3 express URL" in error

    pgduck_conn.rollback()


# This test outputs a bad host name that seems like some sort of memory corruption at play
def test_s3_get_region_invalid(pgduck_conn):
    error = run_command(
        "select pg_lake_get_bucket_region('s3://.../abc/') test",
        pgduck_conn,
        raise_error=False,
    )
    assert "Could not establish connection error" in error

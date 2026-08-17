import os
import pytest
from moto.server import ThreadedMotoServer
import boto3
from utils_pytest import *
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
    assert results[0][0] == 100

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
    assert results[0][0] == 100

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
    assert results[0][0] == 100

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
    assert results[0][0] == 100

    pgduck_conn.rollback()


def test_copy_to_azure_multiple_appends(azure, pgduck_conn, tmp_path):
    """A file that does not fit in the write buffer is written as multiple appends,
    and each append tells Azure the offset it expects to be written at, so check that
    such a file still ends up with exactly the bytes we wrote."""
    key = "test_copy_to_azure_multiple_appends/data.csv"
    local_path = tmp_path / "data.csv"

    perform_query(
        f"""
        CREATE TABLE bigtable AS
        SELECT i, repeat('x', 100) padding FROM generate_series(1,20000) t(i);
        COPY bigtable TO '{local_path}';
        COPY bigtable TO 'az://{TEST_BUCKET}/{key}';
    """,
        pgduck_conn,
    )

    properties = azure.get_blob_client(key).get_blob_properties()

    # more than one append went into this blob
    assert properties.append_blob_committed_block_count > 1

    # and the blob is exactly as long as the same data written locally
    assert properties.size == os.path.getsize(local_path)

    perform_query(
        f"""
        CREATE TABLE roundtrip (i int, padding text);
        COPY roundtrip FROM 'az://{TEST_BUCKET}/{key}';
    """,
        pgduck_conn,
    )

    results = perform_query_on_cursor(
        "SELECT count(*), sum(i) FROM roundtrip", pgduck_conn
    )
    assert len(results) == 1
    assert results[0][0] == 20000
    assert int(results[0][1]) == 200010000

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
    assert (
        "Could not establish connection error" in error
        or "server closed the connection" in error
    )

    # The failed statement above aborts the transaction on this module-scoped
    # connection; roll back so the following tests start clean.
    pgduck_conn.rollback()


def test_pg_lake_remove_files_recursive(s3, pgduck_conn):
    """pg_lake_remove_files expands a recursive glob and deletes every matched
    object, recursing into sub-prefixes, while leaving objects outside the
    pattern untouched. It returns one row per deleted URL."""
    prefix = "test_remove_files/tbl"

    keys = [
        f"{prefix}/metadata/v1.metadata.json",
        f"{prefix}/metadata/snap.avro",
        f"{prefix}/data/a.parquet",
        f"{prefix}/data/nested/b.parquet",
    ]
    for key in keys:
        s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"x")

    # a sibling object outside the pattern must survive
    survivor = "test_remove_files/other/keep.parquet"
    s3.put_object(Bucket=TEST_BUCKET, Key=survivor, Body=b"x")

    results = perform_query_on_cursor(
        f"SELECT count(*) FROM pg_lake_remove_files('s3://{TEST_BUCKET}/{prefix}/**')",
        pgduck_conn,
    )
    assert results[0][0] == len(keys)

    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []
    assert list_objects(s3, TEST_BUCKET, survivor) == [survivor]

    # cleanup
    s3.delete_object(Bucket=TEST_BUCKET, Key=survivor)
    pgduck_conn.rollback()


def test_pg_lake_remove_files_batches_over_1000(s3, pgduck_conn):
    """More than 1000 matched objects must all be deleted: the DeleteObjects
    request caps at 1000 keys, so this crosses the batch boundary and exercises
    the multi-batch loop."""
    prefix = "test_remove_files_batch"
    count = 1001

    for i in range(count):
        s3.put_object(Bucket=TEST_BUCKET, Key=f"{prefix}/f{i}.parquet", Body=b"x")

    results = perform_query_on_cursor(
        f"SELECT count(*) FROM pg_lake_remove_files('s3://{TEST_BUCKET}/{prefix}/**')",
        pgduck_conn,
    )
    assert results[0][0] == count

    # nothing left under the prefix (list_objects_v2 caps a page at 1000, so a
    # non-empty first page would already prove leftovers)
    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    pgduck_conn.rollback()


def test_pg_lake_remove_files_xml_special_chars(s3, pgduck_conn):
    """S3 allows '&', '<' and '>' in object keys. Those have to be escaped in the
    DeleteObjects XML body: unescaped, the body is malformed XML and S3 rejects
    the whole batch, so a table whose location prefix contains one of them would
    never be cleaned up."""
    prefix = "test_remove_files_xml"

    keys = [
        f"{prefix}/a&b/data.parquet",
        f"{prefix}/c<d>e/data.parquet",
        f"{prefix}/plain/data.parquet",
    ]
    for key in keys:
        s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"x")

    results = perform_query_on_cursor(
        f"SELECT count(*) FROM pg_lake_remove_files('s3://{TEST_BUCKET}/{prefix}/**')",
        pgduck_conn,
    )
    assert results[0][0] == len(keys)

    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    pgduck_conn.rollback()


def test_pg_lake_remove_files_prepared_more_than_once(s3, pgduck_conn):
    """A prepared pg_lake_remove_files deletes on every execution, not only the
    first.

    DuckDB binds once per plan and reuses the bind data for every execution of
    it, so scan state kept there -- the "already produced my rows" flag, the scan
    offset, the list of deleted URLs -- survives into the next execution. For a
    deleting function that is worse than a wrong answer: the second EXECUTE would
    expand nothing, delete nothing and still report success to the caller, which
    for DeleteRemotePrefix reads as a completed cleanup. The state belongs in a
    GlobalTableFunctionState, which is created per execution."""
    prefix = "test_remove_files_prepared"

    cur = pgduck_conn.cursor()
    cur.execute(
        "PREPARE reused_remove AS "
        f"SELECT count(*) FROM pg_lake_remove_files('s3://{TEST_BUCKET}/{prefix}/**')"
    )
    cur.close()

    for execution in range(1, 4):
        key = f"{prefix}/round{execution}.parquet"
        s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"x")

        results = perform_query_on_cursor("EXECUTE reused_remove", pgduck_conn)
        assert (
            results[0][0] == 1
        ), f"execution {execution} of reused_remove deleted {results[0][0]} files"

        assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    pgduck_conn.rollback()


def test_pg_lake_remove_file_multiple_rows(s3, pgduck_conn):
    """pg_lake_remove_file is a scalar function, but DuckDB calls it with a whole
    vector at a time, so a multi-row statement deletes a set of files that share
    no common glob -- and the ones that live in the same bucket go out in a single
    DeleteObjects request rather than one request each."""
    keys = [
        "test_remove_file_rows/a/one.parquet",
        "test_remove_file_rows/b/two.parquet",
        "test_remove_file_rows/deep/nested/three.parquet",
        "test_remove_file_rows_elsewhere/four.parquet",
    ]
    for key in keys:
        s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"x")

    # a sibling object that no row names must survive
    survivor = "test_remove_file_rows/a/keep.parquet"
    s3.put_object(Bucket=TEST_BUCKET, Key=survivor, Body=b"x")

    values = ", ".join(f"('s3://{TEST_BUCKET}/{key}')" for key in keys)
    results = perform_query_on_cursor(
        f"SELECT count(*) FROM (VALUES {values}) v(f) WHERE pg_lake_remove_file(f)",
        pgduck_conn,
    )
    assert results[0][0] == len(keys)

    assert list_objects(s3, TEST_BUCKET, "test_remove_file_rows") == [survivor]

    # cleanup
    s3.delete_object(Bucket=TEST_BUCKET, Key=survivor)
    pgduck_conn.rollback()


def test_pg_lake_remove_file_rows_over_1000(s3, pgduck_conn):
    """A multi-row pg_lake_remove_file batches at 1000 keys per DeleteObjects
    request, and DuckDB's vector size is not a multiple of that, so the rows of a
    single vector can straddle a batch boundary.

    The call sits in WHERE rather than the select list because DuckDB prunes a
    projection that nothing reads, and count(*) reads none of it."""
    prefix = "test_remove_file_rows_batch"
    count = 2500

    for i in range(count):
        s3.put_object(Bucket=TEST_BUCKET, Key=f"{prefix}/f{i}.parquet", Body=b"x")

    results = perform_query_on_cursor(
        f"""
        SELECT count(*) FROM generate_series(0,{count - 1}) t(i)
        WHERE pg_lake_remove_file('s3://{TEST_BUCKET}/{prefix}/f' || i || '.parquet')
        """,
        pgduck_conn,
    )
    assert results[0][0] == count

    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    pgduck_conn.rollback()


def test_pg_lake_remove_file_already_gone(s3, pgduck_conn):
    """Removing a file that is not there is not an error -- a retry after a partly
    completed cleanup has to be able to finish -- and it must not stop the other
    files in the same vector from being deleted."""
    prefix = "test_remove_file_gone"

    present = f"{prefix}/present.parquet"
    s3.put_object(Bucket=TEST_BUCKET, Key=present, Body=b"x")

    values = ", ".join(
        f"('s3://{TEST_BUCKET}/{key}')"
        for key in [f"{prefix}/missing.parquet", present]
    )
    perform_query(
        f"SELECT pg_lake_remove_file(f) FROM (VALUES {values}) v(f)",
        pgduck_conn,
    )

    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    # and again, now that nothing is left
    perform_query(
        f"SELECT pg_lake_remove_file('s3://{TEST_BUCKET}/{present}')", pgduck_conn
    )

    pgduck_conn.rollback()

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


def test_copy_to_azure_multiple_blocks(azure, pgduck_conn, tmp_path):
    """A file that does not fit in one write block is staged as several blocks that are
    committed together, so check that such a file still ends up with exactly the bytes we
    wrote. The block size is lowered rather than the file grown, so the test stays small.
    """
    key = "test_copy_to_azure_multiple_blocks/data.csv"
    local_path = tmp_path / "data.csv"

    perform_query(
        f"""
        SET azure_write_block_size = 1048576;
        CREATE TABLE bigtable AS
        SELECT i, repeat('x', 100) padding FROM generate_series(1,20000) t(i);
        COPY bigtable TO '{local_path}';
        COPY bigtable TO 'az://{TEST_BUCKET}/{key}';
        SET azure_write_block_size = 0;
    """,
        pgduck_conn,
    )

    blob_client = azure.get_blob_client(key)

    # more than one block went into this blob, and none was left uncommitted
    committed, uncommitted = blob_client.get_block_list(block_list_type="all")
    assert len(committed) > 1
    assert uncommitted == []

    # and the blob is exactly as long as the same data written locally
    assert blob_client.get_blob_properties().size == os.path.getsize(local_path)

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


def test_s3_get_region_invalid(pgduck_conn):
    """A bucket whose host does not resolve reports the connection failure, with
    the URL it failed on."""
    error = run_command(
        "select pg_lake_get_bucket_region('s3://.../abc/') test",
        pgduck_conn,
        raise_error=False,
    )
    assert "Could not resolve hostname error for HTTP HEAD to 'https://" in error

    # The failed statement above aborts the transaction on this module-scoped
    # connection; roll back so the following tests start clean.
    pgduck_conn.rollback()


def test_denied_key_containing_301_keeps_its_own_error(
    enforcing_s3_server, pgduck_conn
):
    """A denied read reports the denial, on an object whose key happens to
    contain a substring that reads like a redirect status.

    RegionAwareS3FileSystem retries against a freshly probed region when a
    request looks like it went to the wrong one, and it used to decide that by
    searching the error text for "301". That text carries the URL of the object,
    and Iceberg names data files after a uuid, so about one key in a hundred
    turned an unrelated failure into a region probe -- whose own error then
    replaced the denial the caller was about to report.

    The second key is the control: it differs only in not containing "301"."""
    server = enforcing_s3_server
    prefix = "region_mismatch_classification"
    keys = [
        f"{prefix}/00000-0-3016a9de-0e0f-4a1b-9f0e-3b0d1e2f4a5b.parquet",
        f"{prefix}/00000-0-7c4a9de0-0e0f-4a1b-9f0e-3b0d1e2f4a5b.parquet",
    ]

    for key in keys:
        server.client().put_object(Bucket=server.bucket, Key=key, Body=b"denied")

    # allowed under a sibling prefix only, so every read below is denied
    access_key_id, secret_access_key = server.create_scoped_user(
        "region301_reader", [f"{prefix}_allowed"]
    )

    # DuckDB holds secrets in a transactional catalog set, and a denied read
    # aborts the transaction it ran in -- so the rollback that clears the abort
    # takes the secret with it. Re-assert it before each read; without it the
    # second read has no endpoint and goes to the real s3.amazonaws.com.
    create_secret = f"""
        CREATE OR REPLACE SECRET region301 (
            TYPE S3, KEY_ID '{access_key_id}', SECRET '{secret_access_key}',
            REGION '{server.region}', ENDPOINT '{server.endpoint}',
            SCOPE 's3://{server.bucket}/{prefix}',
            URL_STYLE 'path', USE_SSL false
        );
        """
    server.enforce()

    try:
        for key in keys:
            perform_query(create_secret, pgduck_conn)
            error = run_command(
                f"SELECT count(*) FROM read_parquet('s3://{server.bucket}/{key}')",
                pgduck_conn,
                raise_error=False,
            )
            pgduck_conn.rollback()

            assert error is not None, f"the read of {key} was not denied"
            assert "403" in error, f"expected a denial for {key}, got: {error}"
            # a read that lost the secret leaves for the real S3, which can deny
            # it too -- for having no credentials, which proves nothing here
            assert server.endpoint in error, f"{key} missed moto: {error}"
            assert key in error, f"{key}: the error is about another request: {error}"
    finally:
        server.relax()
        perform_query("DROP SECRET IF EXISTS region301", pgduck_conn)
        pgduck_conn.rollback()


def test_pg_lake_remove_file_glob_recursive(s3, pgduck_conn):
    """The glob + pg_lake_remove_file form that DeleteRemotePrefix uses deletes
    every object under a prefix, recursing into sub-prefixes, and leaves objects
    outside the pattern untouched.

    The call goes in WHERE rather than the select list because DuckDB prunes a
    projection that nothing reads, and count(*) reads none of it."""
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
        f"SELECT count(*) FROM glob('s3://{TEST_BUCKET}/{prefix}/**') "
        "WHERE pg_lake_remove_file(file)",
        pgduck_conn,
    )
    assert results[0][0] == len(keys)

    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []
    assert list_objects(s3, TEST_BUCKET, survivor) == [survivor]

    # cleanup
    s3.delete_object(Bucket=TEST_BUCKET, Key=survivor)
    pgduck_conn.rollback()


def test_pg_lake_remove_file_glob_batches_over_1000(s3, pgduck_conn):
    """More than 1000 matched objects must all be deleted: the DeleteObjects
    request caps at 1000 keys, so this crosses the batch boundary and exercises
    the multi-batch loop."""
    prefix = "test_remove_files_batch"
    count = 1001

    for i in range(count):
        s3.put_object(Bucket=TEST_BUCKET, Key=f"{prefix}/f{i}.parquet", Body=b"x")

    results = perform_query_on_cursor(
        f"SELECT count(*) FROM glob('s3://{TEST_BUCKET}/{prefix}/**') "
        "WHERE pg_lake_remove_file(file)",
        pgduck_conn,
    )
    assert results[0][0] == count

    # nothing left under the prefix (list_objects_v2 caps a page at 1000, so a
    # non-empty first page would already prove leftovers)
    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    pgduck_conn.rollback()


def test_pg_lake_remove_file_glob_xml_special_chars(s3, pgduck_conn):
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
        f"SELECT count(*) FROM glob('s3://{TEST_BUCKET}/{prefix}/**') "
        "WHERE pg_lake_remove_file(file)",
        pgduck_conn,
    )
    assert results[0][0] == len(keys)

    assert list_objects(s3, TEST_BUCKET, f"{prefix}/") == []

    pgduck_conn.rollback()


def test_pg_lake_remove_file_glob_empty(s3, pgduck_conn):
    """A prefix with nothing under it is not an error: dropping a table whose
    files are already gone, or that never wrote any, still has to succeed."""
    results = perform_query_on_cursor(
        f"SELECT count(*) FROM glob('s3://{TEST_BUCKET}/test_remove_files_empty/**') "
        "WHERE pg_lake_remove_file(file)",
        pgduck_conn,
    )
    assert results[0][0] == 0

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


def test_pg_lake_remove_file_azure(azure, pgduck_conn):
    """Only S3 has a batch delete API, so every other back end takes the fallback
    that removes one file at a time through the file system the ClientContext hands
    out. Nothing else covers that branch.

    That file system is an OpenerFileSystem: it pushes its own opener into each
    call and rejects one from the caller with "OpenerFileSystem cannot take an
    opener". Since that is an InternalException, getting it wrong does not fail the
    statement -- it terminates the server, and every later test with it."""
    prefix = "test_remove_file_azure"

    keys = [f"{prefix}/a.parquet", f"{prefix}/b.parquet"]
    for key in keys:
        azure.upload_blob(name=key, data=b"x")

    values = ", ".join(f"('az://{TEST_BUCKET}/{key}')" for key in keys)
    results = perform_query_on_cursor(
        f"SELECT count(*) FROM (VALUES {values}) v(f) WHERE pg_lake_remove_file(f)",
        pgduck_conn,
    )
    assert results[0][0] == len(keys)

    assert list(azure.list_blobs(name_starts_with=f"{prefix}/")) == []

    pgduck_conn.rollback()

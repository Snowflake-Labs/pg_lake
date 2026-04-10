import os
import pytest
import time
from utils_pytest import *

CACHE_FILE_PREFIX = "pgl-cache."


def test_pg_lake_cache_file(s3, gcs, azure, pgduck_conn):
    run_pg_lake_cache_file_test_for_protocol("s3", TEST_BUCKET, pgduck_conn, s3)
    run_pg_lake_cache_file_test_for_protocol("gs", TEST_BUCKET_GCS, pgduck_conn, gcs)
    run_pg_lake_cache_file_test_for_protocol("az", TEST_BUCKET, pgduck_conn, azure)
    run_pg_lake_cache_file_test_for_protocol(
        "http", f"localhost:5999/{TEST_BUCKET}", pgduck_conn, s3
    )


def run_pg_lake_cache_file_test_for_protocol(protocol, prefix, pgduck_conn, client):
    key = "test_pg_lake_cache_file/data.csv"
    url = f"{protocol}://{prefix}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/{protocol}/{prefix}/test_pg_lake_cache_file/{CACHE_FILE_PREFIX}data.csv"
    )
    upload_url = url

    if protocol == "http":
        # We use the S3 http endpoint for an S3 bucket, so upload to S3
        upload_url = f"s3://{TEST_BUCKET}/{key}"

    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,100)) TO '{upload_url}' WITH (header false);
    """,
        pgduck_conn,
    )

    uncached_size = pg_lake_file_size(url, pgduck_conn)

    if protocol == "http":
        # Make the S3 file public readable to be able to use HTTP endpoint
        client.put_object_acl(
            ACL="public-read", AccessControlPolicy={}, Bucket=TEST_BUCKET, Key=key
        )

    run_command(
        f"""
        CALL pg_lake_cache_file('{url}');
    """,
        pgduck_conn,
    )

    # Verify that the file was cached
    assert cached_path.exists()

    # Verify that sizes are all the same
    cached_size = pg_lake_file_size(url, pgduck_conn)
    local_size = local_file_size(cached_path)

    assert cached_size > 0
    assert cached_size == uncached_size == local_size

    results = run_query(
        f"SELECT file_size FROM pg_lake_list_cache() WHERE url = '{url}'", pgduck_conn
    )
    assert len(results) == 1
    assert results[0][0] == str(cached_path.stat().st_size)

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == "100"

    # Sneakily write something else to the cached file
    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,50)) TO '{cached_path}' WITH (header false);
    """,
        pgduck_conn,
    )

    # Verify that we are indeed reading from cache when using the URL
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == "50"

    # Can bypass cache using nocache prefix
    results = run_query(f"SELECT count(*) FROM 'nocache{url}'", pgduck_conn)
    assert results[0][0] == "100"

    # Calling pg_lake_cache_file without force does not change that
    run_command(
        f"""
        FROM pg_lake_cache_file('{url}');
    """,
        pgduck_conn,
    )

    # Verify that we are still from cache when using the URL
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == "50"

    # Calling pg_lake_cache_file with force will restore the real file
    run_command(
        f"""
        CALL pg_lake_cache_file('{url}', true);
    """,
        pgduck_conn,
    )

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == "100"

    # Remove the cached file
    results = run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    assert results[0][0] == "t"

    # Verify the file is gone
    assert not cached_path.exists()

    pgduck_conn.rollback()


def test_invalid_url(s3, pgduck_conn):
    url_notexists = f"s3://{TEST_BUCKET}/test_invalid_url/data.csv"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_invalid_url/{CACHE_FILE_PREFIX}data.csv"
    )

    # Trying to cache a non-existent URL throws an error
    error = run_command(
        f"CALL pg_lake_cache_file('{url_notexists}');", pgduck_conn, raise_error=False
    )
    assert "NOT FOUND" in error.upper()

    pgduck_conn.rollback()

    # Trying to cache a local file path is not allowed
    error = run_command(
        f"CALL pg_lake_cache_file('{cached_path}');", pgduck_conn, raise_error=False
    )
    assert "URL cannot be cached" in error

    pgduck_conn.rollback()

    # Trying to remove a non-existent URL just returns false
    results = run_query(f"CALL pg_lake_uncache_file('{url_notexists}');", pgduck_conn)
    assert results[0][0] == "f"

    pgduck_conn.rollback()

    # Trying to use wildcard results in an error
    url_wildcard = f"s3://{TEST_BUCKET}/test_invalid_url/*.csv"
    error = run_query(
        f"CALL pg_lake_cache_file('{url_wildcard}');", pgduck_conn, raise_error=False
    )
    assert "cannot cache paths with wildcard" in error

    pgduck_conn.rollback()

    error = run_query(
        f"CALL pg_lake_uncache_file('{url_wildcard}');", pgduck_conn, raise_error=False
    )
    assert "cannot cache paths with wildcard" in error

    pgduck_conn.rollback()


def test_pg_lake_manage_cache(s3, pgduck_conn):
    url1 = f"s3://{TEST_BUCKET}/test_pg_lake_manage_cache/data1.csv"
    cached_path1 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_manage_cache/{CACHE_FILE_PREFIX}data1.csv"
    )

    # Use a 200KB cache
    cache_size = 200000

    # Generate a file a ~150KB file
    results = run_query(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10000) as g(s)) TO '{url1}';
        SELECT * FROM pg_lake_manage_cache(0) WHERE url = '{url1}';
    """,
        pgduck_conn,
    )

    # Verify that the file is cached by writing it, and removed with pg_lake_manage_cache(0)
    assert len(results) == 1
    assert results[0][0] == str(url1)
    assert results[0][2] == "removed"
    assert not cached_path1.exists()

    # Manage cache before read
    results = run_query(f"CALL pg_lake_manage_cache({cache_size})", pgduck_conn)
    assert len(results) == 0

    # Verify that the file was not yet cached
    assert not cached_path1.exists()

    # Read the file
    run_query(f"SELECT count(*) FROM '{url1}'", pgduck_conn)

    # Verify that the file is skipped when it does not fit in cache
    results = run_query(f"CALL pg_lake_manage_cache(1000)", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == str(url1)
    assert results[0][2].startswith("skipped")
    assert not cached_path1.exists()

    # Read the file again
    run_query(f"SELECT count(*) FROM '{url1}'", pgduck_conn)

    # Verify that the file is cached when it fits in cache
    results = run_query(f"CALL pg_lake_manage_cache({cache_size})", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == str(url1)
    assert results[0][2] == "added"
    assert cached_path1.exists()

    # Generate another ~150KB file and make sure it is cached
    url2 = f"s3://{TEST_BUCKET}/test_pg_lake_manage_cache/data2.csv"
    cached_path2 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_manage_cache/{CACHE_FILE_PREFIX}data2.csv"
    )

    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10000) as g(s)) TO '{url2}';
    """,
        pgduck_conn,
    )

    # Manage the cache down to 200KB, so the first file is removed
    results = run_query(f"FROM pg_lake_manage_cache({cache_size})", pgduck_conn)
    print(results)
    # Verify that the original file was removed (remove always comes first) and the new one was added
    assert len(results) == 1
    assert results[0][0] == str(url1)
    assert results[0][2] == "removed"

    assert not cached_path1.exists()
    assert cached_path2.exists()

    # Read both files
    run_query(f"SELECT count(*) FROM '{url1}'", pgduck_conn)
    run_query(f"SELECT count(*) FROM '{url2}'", pgduck_conn)

    # Manage the cache down to 200KB
    results = run_query(f"FROM pg_lake_manage_cache({cache_size})", pgduck_conn)

    # Verify that url1 is skipped, because url2 is already cached
    assert len(results) == 1
    assert results[0][0] == str(url1)
    assert results[0][2].startswith("skipped")

    assert not cached_path1.exists()
    assert cached_path2.exists()

    # Wipe the cache
    results = run_query("CALL pg_lake_manage_cache(0)", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == str(url2)
    assert results[0][2] == "removed"

    pgduck_conn.rollback()


def test_pg_lake_manage_cache_invalid_url(s3, pgduck_conn):
    # Invalid URL should not get cached
    key = "test_pg_lake_manage_cache_invalid_url/data.csv"
    url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_manage_cache_invalid_url/{CACHE_FILE_PREFIX}data.csv"
    )
    cache_size = 200000

    # Read from non-existent URL
    error = run_command(f"SELECT count(*) FROM '{url}'", pgduck_conn, raise_error=False)
    assert "NOT FOUND" in error.upper()

    pgduck_conn.rollback()

    # Manage cache does not react to invalid read
    results = run_query(f"FROM pg_lake_manage_cache({cache_size})", pgduck_conn)
    assert len(results) == 0

    # Generate a file and read it
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10000) as g(s)) TO '{url}';
        SELECT count(*) FROM '{url}';
    """,
        pgduck_conn,
    )

    # remove the auto-cached file as the test relies
    # the local cache not having the file, then re-access
    # such that manage_cache can kick in
    run_command(
        f"""
     CALL pg_lake_manage_cache(0);
     SELECT count(*) FROM '{url}';
     """,
        pgduck_conn,
    )

    # Delete before managing the cache
    s3.delete_object(Bucket=TEST_BUCKET, Key=key)

    # Manage cache skips over the non-existent object
    results = run_query(f"FROM pg_lake_manage_cache({cache_size})", pgduck_conn)
    assert len(results) == 1
    assert results[0][0] == str(url)
    assert results[0][2] == "add failed"

    pgduck_conn.rollback()


# Confirm we clear the Parquet metadata cache
def test_parquet_metadata_cache_invalidation(s3, pgduck_conn):
    url = f"s3://{TEST_BUCKET}/test_parquet_metadata_cache_invalidation/data1.parquet"

    # Generate a file with 2 columns
    run_command(
        f"""
        COPY (SELECT 1 AS a, 2 AS b) TO '{url}'
    """,
        pgduck_conn,
    )

    # We expect 2 columns
    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 2

    # Cache the file explicitly
    run_command(
        f"""
        SELECT * FROM pg_lake_cache_file('{url}')
    """,
        pgduck_conn,
    )

    # We get 2 columns
    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 2

    # Replace the file with 3 columns
    run_command(
        f"""
        COPY (SELECT 1 AS a, 2 AS b, 3 AS c) TO '{url}'
    """,
        pgduck_conn,
    )

    # File is re-cached via copy, we get 3 columns
    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 3

    # Refresh the file explicitly
    run_command(
        f"""
        SELECT * FROM pg_lake_cache_file('{url}', true)
    """,
        pgduck_conn,
    )

    # We get 3 columns
    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 3

    # Replace the file with 4 columns
    run_command(
        f"""
        COPY (SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d) TO '{url}'
    """,
        pgduck_conn,
    )

    # Uncache the file explicitly
    run_command(
        f"""
        SELECT * FROM pg_lake_uncache_file('{url}')
    """,
        pgduck_conn,
    )

    # Now we get 4 columns
    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 4


# we can cache two different files concurrently
def test_concurrent_cache_uncache_different_files(s3, pgduck_conn):
    url_1 = f"s3://{TEST_BUCKET}/test_concurrent_cache_file/file_1.csv"
    path_1 = str(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_concurrent_cache_file/{CACHE_FILE_PREFIX}file_1.csv"
        )
    )
    stage_path_1 = path_1 + ".pgl-stage"

    url_2 = f"s3://{TEST_BUCKET}/test_concurrent_cache_file/file_2.csv"
    path_2 = str(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_concurrent_cache_file/{CACHE_FILE_PREFIX}file_2.csv"
        )
    )
    stage_path_2 = path_2 + ".pgl-stage"

    # Generate a file a ~150KB files
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,1000) as g(s)) TO '{url_1}';
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,1000) as g(s)) TO '{url_2}';
    """,
        pgduck_conn,
    )

    # first, remove auto-cached files
    run_command(
        f"""
        SELECT * FROM pg_lake_uncache_file('{url_1}');
        SELECT * FROM pg_lake_uncache_file('{url_2}');
    """,
        pgduck_conn,
    )

    # first, run the first pg_lake_cache_file
    # and wait until the stage file shows up
    t1 = thread_run_command(
        f"""
        CALL pg_lake_cache_file('{url_1}');
    """,
        pgduck_conn,
    )

    assert check_file_exist(stage_path_1), "the first file not staged as expected"

    # now, run the second pg_lake_cache_file
    # and assert both files are in the stage
    t2 = thread_run_command(
        f"""
        CALL pg_lake_cache_file('{url_2}');
    """,
        pgduck_conn,
    )
    assert check_file_exist(stage_path_1) and check_file_exist(
        stage_path_2
    ), "files are not staged concurrently"

    t1.join()
    t2.join()

    assert check_file_exist(path_1) and check_file_exist(
        path_2
    ), "files are not caches concurrently"

    # now, uncache both files concurrently
    t1 = thread_run_command(
        f"""
        CALL pg_lake_uncache_file('{url_1}');
    """,
        pgduck_conn,
    )

    t2 = thread_run_command(
        f"""
        CALL pg_lake_uncache_file('{url_2}');
    """,
        pgduck_conn,
    )

    t1.join()
    t2.join()

    assert not check_file_exist(path_1, timeout_seconds=0.01) and not check_file_exist(
        path_2, timeout_seconds=0.01
    ), "files are not removed concurrently"

    results = run_query(
        f"SELECT file_size FROM pg_lake_list_cache() WHERE url = '{url_1}'", pgduck_conn
    )
    assert len(results) == 0
    results = run_query(
        f"SELECT file_size FROM pg_lake_list_cache() WHERE url = '{url_2}'", pgduck_conn
    )
    assert len(results) == 0


# we cannot cache the same file concurrently
def test_concurrent_cache_same_file(s3, pgduck_conn):
    url_1 = f"s3://{TEST_BUCKET}/test_concurrent_cache_file/file_1.csv"
    path_1 = str(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_concurrent_cache_file/{CACHE_FILE_PREFIX}file_1.csv"
        )
    )
    stage_path_1 = path_1 + ".pgl-stage"

    # Generate a file a ~150KB files
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,1000) as g(s)) TO '{url_1}';
        SELECT count(*) FROM '{url_1}';
    """,
        pgduck_conn,
    )

    # first, run the first pg_lake_cache_file
    # and wait until the stage file shows up
    t1 = thread_run_command(
        f"""
        CALL pg_lake_cache_file('{url_1}', true);
    """,
        pgduck_conn,
    )

    assert check_file_exist(stage_path_1), "the first file not staged as expected"

    # now, run the second pg_lake_cache_file
    # and assert both files are in the stage
    run_command(
        f"""
        CALL pg_lake_cache_file('{url_1}', true);
    """,
        pgduck_conn,
    )
    assert check_file_exist(path_1), "the file is not cached concurrently"

    results = run_query(
        f"SELECT file_size FROM pg_lake_list_cache() WHERE url = '{url_1}'", pgduck_conn
    )
    assert len(results) == 1

    t1.join()


# we cannot cache the same file concurrently
def test_concurrent_cache_same_file_no_force(s3, pgduck_conn):
    url_1 = f"s3://{TEST_BUCKET}/test_concurrent_cache_same_file_no_force/file_1.csv"
    path_1 = str(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_concurrent_cache_same_file_no_force/{CACHE_FILE_PREFIX}file_1.csv"
        )
    )
    stage_path_1 = path_1 + ".pgl-stage"

    # Generate a file a ~150KB files and remove
    # from auto-generated cache file
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,1000) as g(s)) TO '{url_1}';
        CALL pg_lake_uncache_file('{url_1}');
    """,
        pgduck_conn,
    )

    # first, run the first pg_lake_cache_file
    # and wait until the stage file shows up
    t1 = thread_run_command(
        f"""
        CALL pg_lake_cache_file('{url_1}', true);
    """,
        pgduck_conn,
    )

    assert check_file_exist(stage_path_1), "the first file not staged as expected"

    # now, run the second pg_lake_cache_file
    # and assert both files are in the stage
    results = run_query(
        f"""
        CALL pg_lake_cache_file('{url_1}', false);
    """,
        pgduck_conn,
    )

    # ensure that this waited until the other pg_lake_cache_file
    # finished, then returned 0 bytes
    assert results[0][0] == "0"

    t1.join()


def test_copy_cache_results(s3, pgduck_conn):
    url1 = f"s3://{TEST_BUCKET}/test_copy_cache_results/data1.csv"
    cached_path1 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_copy_cache_results/{CACHE_FILE_PREFIX}data1.csv"
    )

    # Use a 200KB cache
    cache_size = 200000

    # Generate a file a ~150KB file
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10000) as g(s)) TO '{url1}';
    """,
        pgduck_conn,
    )

    # Verify that the file is cached by writing it, and removed with pg_lake_manage_cache(0)
    assert cached_path1.exists()


def test_cache_key_overlaps(pgduck_conn):
    """Test that we can cache files of the form "foo.parquet" and "foo.parquet/data_0.parquet"""

    url1 = f"s3://{TEST_BUCKET}/test_cache_key_overlaps/data.parquet"
    url2 = f"{url1}/data_0.parquet"

    cached_path1 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_cache_key_overlaps/{CACHE_FILE_PREFIX}data.parquet"
    )
    cached_path2 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_cache_key_overlaps/data.parquet/{CACHE_FILE_PREFIX}data_0.parquet"
    )

    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10) as g(s)) TO '{url1}';
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10) as g(s)) TO '{url2}';
    """,
        pgduck_conn,
    )

    run_command(
        f"""
        CALL pg_lake_cache_file('{url1}');
    """,
        pgduck_conn,
    )

    run_command(
        f"""
        CALL pg_lake_cache_file('{url2}');
    """,
        pgduck_conn,
    )

    assert cached_path1.exists()
    assert cached_path2.exists()


def test_cache_on_write_disabled(s3, pgduck_conn):
    url1 = f"s3://{TEST_BUCKET}/test_cache_on_write_disabled/data1.csv"
    cached_path1 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_cache_on_write_disabled/{CACHE_FILE_PREFIX}data1.csv"
    )

    cache_size = 0
    run_command(
        f"""
        SET GLOBAL pg_lake_cache_on_write_max_size TO '{cache_size}';
    """,
        pgduck_conn,
    )

    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10) as g(s)) TO '{url1}';
    """,
        pgduck_conn,
    )

    # Verify that the file is cached by writing it, and removed with pg_lake_manage_cache(0)
    assert not cached_path1.exists()

    # set back to 1GB
    cache_size = 1024 * 1024 * 1024
    run_command(
        f"""
        SET GLOBAL pg_lake_cache_on_write_max_size TO '{cache_size}';
    """,
        pgduck_conn,
    )


def test_cache_on_write_disabled_after_some_writes(s3, pgduck_conn):
    url1 = f"s3://{TEST_BUCKET}/test_cache_on_write_disabled_after_some_writes/data1.parquet"
    cached_path1 = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_cache_on_write_disabled_after_some_writes/{CACHE_FILE_PREFIX}data1.parquet"
    )

    # Duckdb's parquet writer always starts with 4096 bytes
    # in the first batch of write. So, allow the first batch
    # then make sure we do not cache afterwards
    cache_size = 5000
    run_command(
        f"""
        SET GLOBAL pg_lake_cache_on_write_max_size TO '{cache_size}';
    """,
        pgduck_conn,
    )

    # Generate a file a ~150KB file
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,1000) as g(s)) TO '{url1}';
    """,
        pgduck_conn,
    )

    # Verify that the file is cached by writing it, and removed with pg_lake_manage_cache(0)
    assert not cached_path1.exists()

    # set back to 1GB
    cache_size = 1024 * 1024 * 1024
    run_command(
        f"""
        SET GLOBAL pg_lake_cache_on_write_max_size TO '{cache_size}';
    """,
        pgduck_conn,
    )


# we cannot cache the same file concurrently
def test_copy_concurrently(s3, pgduck_conn):
    url_1 = f"s3://{TEST_BUCKET}/test_copy_concurrently/file_1.csv"
    path_1 = str(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_copy_concurrently/{CACHE_FILE_PREFIX}file_1.csv"
        )
    )
    stage_path_1 = path_1 + ".pgl-stage"

    # first, run the first pg_lake_cache_file
    # and wait until the stage file shows up
    t1 = thread_run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,10000) as g(s)) TO '{url_1}';
    """,
        pgduck_conn,
    )

    # copy into the same file will be blocked
    t2 = thread_run_command(
        f"""
        COPY (SELECT s, 'hello-'||s as h FROM generate_series(1,2000) as g(s)) TO '{url_1}';
    """,
        pgduck_conn,
    )

    t1.join()
    t2.join()

    assert check_file_exist(path_1), "final file is not showing up as expected"

    # Verify that we always have the final results by the second COPY
    results = run_query(f"SELECT count(*) FROM '{url_1}'", pgduck_conn)
    assert results[0][0] == "2000"


def test_pg_lake_remove_file(s3, pgduck_conn):
    run_test_pg_lake_remove_file("", s3, pgduck_conn)
    run_test_pg_lake_remove_file("?s3_region=us-east-1", s3, pgduck_conn)


def run_test_pg_lake_remove_file(query_arg, s3, pgduck_conn):
    key = "test_pg_lake_remove_file/data.parquet"
    url = f"s3://{TEST_BUCKET}/{key}{query_arg}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_remove_file/{CACHE_FILE_PREFIX}data.parquet"
    )

    run_command(
        f"""
        COPY (SELECT s AS s, s*2 d FROM generate_series(1,100) as g(s)) TO '{url}' (format 'parquet');
    """,
        pgduck_conn,
    )

    # Verify that the file was cached via write-through caching
    assert cached_path.exists()

    # Verify that we can read from the file
    results = run_query(f"SELECT sum(s) FROM '{url}'", pgduck_conn)
    assert results[0][0] == "5050"

    # Remove the file
    run_command(
        f"""
        SELECT pg_lake_remove_file('{url}');
    """,
        pgduck_conn,
    )

    # Verify that the file is no longer cached
    assert not cached_path.exists()

    # Verify that we can no longer read from the file
    error = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn, raise_error=False)
    assert "404" in error

    pgduck_conn.rollback()

    # Removing twice does not crash
    result = run_command(
        f"""
        SELECT pg_lake_remove_file('{url}');
    """,
        pgduck_conn,
        raise_error=False,
    )
    if result:
        assert "404" in result or "Not Found" in result
    pgduck_conn.rollback()


# Test that query arguments are included in the path
def test_http_query_args(s3, pgduck_conn):
    key = "test_http_query_args/data.parquet"
    url = f"http://localhost:5999/{TEST_BUCKET}/{key}"
    upload_url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = f"{server_params.PGDUCK_CACHE_DIR}/http/localhost:5999/{TEST_BUCKET}/test_http_query_args/{CACHE_FILE_PREFIX}data.parquet"

    # We use the S3 http endpoint for an S3 bucket, so upload to S3
    upload_url = f"s3://{TEST_BUCKET}/{key}"

    run_command(
        f"""
        COPY (SELECT 124 id, 'world' val) TO '{upload_url}';
    """,
        pgduck_conn,
    )

    # Make the S3 file public readable to be able to use HTTP endpoint
    s3.put_object_acl(
        ACL="public-read", AccessControlPolicy={}, Bucket=TEST_BUCKET, Key=key
    )

    # Cache 2 HTTP URLs separately
    run_command(
        f"""
        CALL pg_lake_cache_file('{url}');
        CALL pg_lake_cache_file('{url}?world=1');
    """,
        pgduck_conn,
    )

    # Check that there are 2 separate files
    assert Path(cached_path).exists()
    assert Path(cached_path + "?world=1").exists()

    # Overwrite the cached file for the original URL
    run_command(
        f"""
        COPY (SELECT 125 id, 'hello' val) TO '{cached_path}';
    """,
        pgduck_conn,
    )

    # Check that we get two different values
    results = run_query(f"SELECT val FROM '{url}'", pgduck_conn)
    assert results[0][0] == "hello"

    results = run_query(f"SELECT val FROM '{url}?world=1'", pgduck_conn)
    assert results[0][0] == "world"


def check_file_exist(file, timeout_seconds=3):
    end_time = time.time() + timeout_seconds  # Calculate when we should stop checking

    while time.time() < end_time:
        if not os.path.exists(file):
            time.sleep(0.001)  # Wait for 0.1 seconds before checking again
        else:
            return True
    return False  # Return False if not all files exist within the timeout period


def pg_lake_file_size(url, pgduck_conn):
    results = run_query(f"SELECT pg_lake_file_size('{url}') as file_size", pgduck_conn)
    return int(results[0]["file_size"])


def local_file_size(path):
    file_stats = os.stat(path)
    return file_stats.st_size

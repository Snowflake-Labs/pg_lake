import os
import pytest
from utils_pytest import *

CACHE_FILE_PREFIX = "pgl-cache."


# Similar to pgduck_server's test_caching, but via PG
def test_pg_lake_file_cache(s3, pg_conn, extension, superuser_conn):
    url = f"s3://{TEST_BUCKET}/test_pg_lake_file_cache/data.csv"
    url_cache_with_glob = (
        f"s3://{TEST_BUCKET}/test_pg_lake_file_cache/data2.csv?s3_region=us-west-1"
    )

    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_file_cache/{CACHE_FILE_PREFIX}data.csv"
    )
    cached_path_with_glob = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_file_cache/{CACHE_FILE_PREFIX}data2.csv"
    )

    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,100)) TO '{url_cache_with_glob}' WITH (header false);
        COPY (SELECT * FROM generate_series(1,100)) TO '{url}' WITH (header false);
    """,
        superuser_conn,
    )

    run_command(
        f"""
        SELECT lake_file_cache.add('{url}');
        CREATE FOREIGN TABLE test_cache () SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    # Verify that the file was cached
    assert cached_path.exists()

    results = run_query(
        f"SELECT file_size FROM lake_file_cache.list() WHERE path = '{url}'", pg_conn
    )
    assert len(results) == 1
    assert results[0][0] == cached_path.stat().st_size

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 100

    # Sneakily write something else to the cached file
    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,50)) TO '{cached_path}' WITH (header false);
    """,
        superuser_conn,
    )

    # Verify that we are indeed reading from cache when using the URL
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 50

    # Calling lake_file_cache.add without force does not change that
    results = run_query(f"SELECT lake_file_cache.add('{url}');", pg_conn)
    assert results[0][0] == 0

    # Verify that we are still from cache when using the URL
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 50

    # Calling lake_file_cache.add with force will restore the real file
    results = run_query(f"SELECT lake_file_cache.add('{url}', true);", pg_conn)
    assert results[0][0] == cached_path.stat().st_size

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 100

    # Remove the cached file
    results = run_query(f"SELECT lake_file_cache.remove('{url}');", pg_conn)
    assert results[0][0] == True

    # Verify the file is gone
    assert not cached_path.exists()

    pg_conn.rollback()

    # Using glob (in this case "?") in the URL (but not in the path) is allowed
    # we already have a test for path checks in test_invalid_url
    error = run_command(
        f"SELECT lake_file_cache.add('{url_cache_with_glob}');",
        pg_conn,
        raise_error=False,
    )
    assert results[0][0] == True

    # Verify that the file was cached
    assert cached_path_with_glob.exists()

    pg_conn.rollback()


def test_pg_lake_file_cache_override(s3, pg_conn, extension, superuser_conn, tmp_path):
    CSV_KEY = "test_pg_lake_file_cache_override/data.csv"
    url = f"s3://{TEST_BUCKET}/{CSV_KEY}"

    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_file_cache_override/{CACHE_FILE_PREFIX}data.csv"
    )

    # Missing characters in CSV
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        for i in range(0, 100):
            csv_file.write("1\n")

    s3.upload_file(local_csv_path, TEST_BUCKET, CSV_KEY)

    run_command(
        f"""
        SELECT lake_file_cache.add('{url}');
        CREATE FOREIGN TABLE test_cache () SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    # Verify that the file was cached
    assert cached_path.exists()

    results = run_query(
        f"SELECT file_size FROM lake_file_cache.list() WHERE path = '{url}'", pg_conn
    )
    assert len(results) == 1
    assert results[0][0] == cached_path.stat().st_size

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 100

    with open(local_csv_path, "w") as csv_file:
        for i in range(0, 50):
            csv_file.write("2\n")
    s3.upload_file(local_csv_path, TEST_BUCKET, CSV_KEY)

    # Verify that we still get the old value from the cache
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 100

    # now, remove the file from the cache
    results = run_query(f"SELECT lake_file_cache.remove('{url}');", pg_conn)
    assert results[0][0] > 0

    # Verify that now get the result on s3
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 50

    pg_conn.rollback()


def test_pg_lake_copy_twice(s3, azure, pg_conn, extension):
    internal_test_pg_lake_copy_twice("s3", pg_conn, extension)
    internal_test_pg_lake_copy_twice("az", pg_conn, extension)


def internal_test_pg_lake_copy_twice(prefix, pg_conn, extension):
    url = f"{prefix}://{TEST_BUCKET}/test_pg_lake_copy_twice/data.csv"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/{prefix}/{TEST_BUCKET}/test_pg_lake_copy_twice/{CACHE_FILE_PREFIX}data.csv"
    )

    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,100)) TO '{url}' WITH (header false);
        SELECT lake_file_cache.add('{url}');
        CREATE FOREIGN TABLE test_cache () SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    # Verify that the file was cached
    assert cached_path.exists()

    # Verify that we read all the rows
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 100

    # re-write data to the same URL, this time 50 rows
    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,50)) TO '{url}' WITH (header false);
    """,
        pg_conn,
    )

    # Verify that the cache invalidated and we read 50 rows
    results = run_query(f"SELECT count(*) FROM test_cache", pg_conn)
    assert results[0][0] == 50

    pg_conn.rollback()


def test_insert_cache_file(s3, superuser_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_insert_cache_file/"
    # We do not use the CACHE_FILE_PREFIX here, but add it later in this test
    cached_path = str(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_insert_cache_file/"
        )
    )

    for file_format in ("parquet", "csv", "json"):
        run_command(
            f"""
            CREATE FOREIGN TABLE test_cache (a int) SERVER pg_lake OPTIONS (writable 'true', format '{file_format}', location '{location}');
            INSERT INTO test_cache VALUES (1);
        """,
            superuser_conn,
        )

        # Verify that we read all the rows
        results = run_query(
            f"SELECT path FROM lake_table.files WHERE table_name::text = 'test_cache';",
            superuser_conn,
        )
        assert len(results) == 1, "should only have one entry for the table"
        assert len(results[0]) == 1, "should create a single file for the table"

        file_path = results[0][0]
        fragments = file_path[len(location) :].split("/")
        file_name = fragments.pop()
        if len(fragments) > 0:
            fragpath = "/".join(fragments) + "/"
        else:
            fragpath = ""
        assert Path(
            cached_path + "/" + fragpath + CACHE_FILE_PREFIX + file_name
        ).exists(), "inserted file is not cached"
        superuser_conn.rollback()


def test_invalid_url(s3, pg_conn, extension):
    url_notexists = f"s3://{TEST_BUCKET}/test_invalid_url/data.csv"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_invalid_url/{CACHE_FILE_PREFIX}data.csv"
    )

    # Trying to cache a non-existent URL throws an error
    error = run_command(
        f"SELECT lake_file_cache.add('{url_notexists}');", pg_conn, raise_error=False
    )
    assert "NOT FOUND" in error.upper()

    pg_conn.rollback()

    # Trying to cache a local file path is not allowed
    error = run_command(
        f"SELECT lake_file_cache.add('{cached_path}');", pg_conn, raise_error=False
    )
    assert "URL cannot be cached" in error

    pg_conn.rollback()

    # Trying to remove a non-existent URL just returns false
    results = run_query(f"SELECT lake_file_cache.remove('{url_notexists}');", pg_conn)
    assert results[0][0] == False

    pg_conn.rollback()

    # Trying to use nocache results in an error
    url_nocache = f"nocaches3://{TEST_BUCKET}/test_pg_lake_file_cache/data.csv"
    error = run_command(
        f"SELECT lake_file_cache.add('{url_nocache}');", pg_conn, raise_error=False
    )
    assert "URL cannot be cached" in error

    pg_conn.rollback()

    # Trying to use wildcard results in an error
    url_wildcard = f"s3://{TEST_BUCKET}/test_invalid_url/*.csv"
    error = run_query(
        f"SELECT lake_file_cache.add('{url_wildcard}');", pg_conn, raise_error=False
    )
    assert "cannot cache paths with wildcard" in error

    pg_conn.rollback()

    # Trying to use [] results in an error
    url_wildcard = f"s3://{TEST_BUCKET}/test_invalid_url/data[0-9].csv"
    error = run_query(
        f"SELECT lake_file_cache.add('{url_wildcard}');", pg_conn, raise_error=False
    )
    assert "cannot cache paths with wildcard" in error

    pg_conn.rollback()

    error = run_query(
        f"SELECT lake_file_cache.remove('{url_wildcard}');",
        pg_conn,
        raise_error=False,
    )
    assert "cannot cache paths with wildcard" in error

    pg_conn.rollback()


def test_pg_lake_file_cache_worker(s3, superuser_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_pg_lake_file_cache_worker/data.parquet"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_pg_lake_file_cache_worker/{CACHE_FILE_PREFIX}data.parquet"
    )

    # Confirm the presence of the cache worker
    results = run_query(
        "SELECT * FROM pg_stat_activity WHERE backend_type = 'pg_lake cache worker'",
        superuser_conn,
    )
    assert len(results) == 1

    # Generate and access a file and trigger caching
    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,100)) TO '{url}';
        CREATE FOREIGN TABLE test_cache () SERVER pg_lake OPTIONS (path '{url}');

        -- SIGHUP triggers cache worker
        SELECT pg_reload_conf();
    """,
        superuser_conn,
    )

    # Wait up 5 seconds for cache worker
    end_time = time.time() + 5

    while time.time() < end_time:
        if cached_path.exists():
            break

        time.sleep(0.05)

    # Should have been cached in the background
    assert cached_path.exists()

    superuser_conn.commit()

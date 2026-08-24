import os
import pytest
import socket
import threading
import time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from utils_pytest import *

CACHE_FILE_PREFIX = "pgl-cache."


def test_cache_file_owner_only_perms(s3, pgduck_conn):
    """Cache files and directories must be owner-only (0600 / 0700).

    pgduck_server sets umask(0077) at startup so DuckDB's mkdir(0755) and
    open(0666) end up masked to 0700 / 0600. Without that, any local user
    could read cached cloud-storage data without credentials.
    """
    key = "test_cache_file_owner_only_perms/data.csv"
    url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_cache_file_owner_only_perms/{CACHE_FILE_PREFIX}data.csv"
    )

    run_command(
        f"COPY (SELECT * FROM generate_series(1,10)) TO '{url}' WITH (header false);",
        pgduck_conn,
    )

    run_command(f"CALL pg_lake_cache_file('{url}');", pgduck_conn)
    assert cached_path.exists()

    file_mode = cached_path.stat().st_mode & 0o777
    assert file_mode == 0o600, f"cache file mode is {oct(file_mode)}, expected 0o600"

    parent_mode = cached_path.parent.stat().st_mode & 0o777
    assert (
        parent_mode == 0o700
    ), f"cache leaf dir mode is {oct(parent_mode)}, expected 0o700"

    cache_root_mode = Path(server_params.PGDUCK_CACHE_DIR).stat().st_mode & 0o777
    assert (
        cache_root_mode == 0o700
    ), f"cache root mode is {oct(cache_root_mode)}, expected 0o700"

    run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    pgduck_conn.rollback()


def test_cache_rejects_non_regular_file(s3, pgduck_conn, tmp_path):
    """A non-regular file (e.g. a symlink) at the cache path must be replaced.

    FileUtils::IsOwnedByCurrentUser uses lstat() and rejects anything that is
    not a regular file owned by the effective UID. Replacing the cached file
    with a symlink simulates a pre-planted entry; the next pg_lake_cache_file
    should re-download and overwrite the symlink with a real file.
    """
    key = "test_cache_rejects_non_regular_file/data.csv"
    url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_cache_rejects_non_regular_file/{CACHE_FILE_PREFIX}data.csv"
    )

    run_command(
        f"COPY (SELECT * FROM generate_series(1,10)) TO '{url}' WITH (header false);",
        pgduck_conn,
    )

    run_command(f"CALL pg_lake_cache_file('{url}');", pgduck_conn)
    assert cached_path.exists() and not cached_path.is_symlink()
    real_size = cached_path.stat().st_size

    # Replace the cached file with a symlink to a different file. The
    # IsOwnedByCurrentUser check uses lstat, so the symlink fails S_ISREG
    # and the cache treats the entry as missing.
    poisoned = tmp_path / "poisoned.csv"
    poisoned.write_text("attacker,content\n")
    cached_path.unlink()
    cached_path.symlink_to(poisoned)

    # Without force, the no-force path should still re-download because the
    # ownership check fails on the symlink.
    run_command(f"CALL pg_lake_cache_file('{url}');", pgduck_conn)

    assert cached_path.is_file() and not cached_path.is_symlink()
    assert cached_path.stat().st_size == real_size
    assert (cached_path.stat().st_mode & 0o777) == 0o600

    run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    pgduck_conn.rollback()


TRUNCATED_BODY_SIZE = 64 * 1024
TRUNCATED_BODY_SENT = 4 * 1024


@pytest.fixture(scope="module")
def flaky_http_server():
    """Truncate the first GET of the body, then serve it in full.

    This is the case that corrupts silently. A body that is *always* short ends
    up failing anyway, because HTTPUtil::SendRequest runs out of retries and
    throws -- so it proves nothing about the download path. Truncating only the
    first attempt lets the retry succeed, and a successful retry is what leaves
    the partial attempt and the complete one both in the output file.

    The handler keeps its own state so that a re-run does not inherit a counter.
    """

    state = {"gets": 0}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _headers(self):
            self.send_response(200)
            self.send_header("Content-Length", str(TRUNCATED_BODY_SIZE))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_HEAD(self):
            self._headers()

        def do_GET(self):
            state["gets"] += 1
            first = state["gets"] == 1
            self._headers()
            self.wfile.write(
                b"x" * (TRUNCATED_BODY_SENT if first else TRUNCATED_BODY_SIZE)
            )
            self.wfile.flush()
            if first:
                self.close_connection = True

        def log_message(self, *args):
            pass

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server = HTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield port, state

    server.shutdown()
    server.server_close()
    thread.join(timeout=10)


def test_retried_download_is_not_cached(flaky_http_server, pgduck_conn):
    """A download whose retry succeeded must still match the object.

    HTTPUtil::SendRequest retries a request error by re-running the content
    handler from the start of the body, and the handler appends to an output
    file that still holds whatever the previous attempt delivered. So a retry
    that succeeds leaves a partial copy followed by a complete one: longer than
    the object, with no exception, and reported as success.

    Nothing revalidates a finalized cache entry against object storage, so that
    entry would be served for every later read of the URL. For an Avro manifest
    the second copy begins exactly where the reader looks for the next block
    header, which is the shape that produces the corruption in #550.
    """
    port, state = flaky_http_server
    url = f"http://127.0.0.1:{port}/flaky.bin"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/http/127.0.0.1:{port}"
        f"/{CACHE_FILE_PREFIX}flaky.bin"
    )
    stage_path = Path(f"{cached_path}.pgl-stage")

    error = run_query(
        f"CALL pg_lake_cache_file('{url}');", pgduck_conn, raise_error=False
    )
    pgduck_conn.rollback()

    # Guards against the test passing for the wrong reason: the request has to
    # have been retried, otherwise this is just a plain failed download.
    assert state["gets"] >= 2, (
        f"the download was not retried ({state['gets']} GET(s)), so the "
        f"accumulation this test is about never happened"
    )

    cached_size = cached_path.stat().st_size if cached_path.exists() else None
    assert isinstance(error, str), (
        f"a retried download was cached as {cached_size} bytes for a "
        f"{TRUNCATED_BODY_SIZE}-byte object"
    )
    assert not cached_path.exists(), f"over-long download was published: {cached_path}"

    # The rejected download stays staged, which is deliberate: the sweep at the
    # top of ManageCache reclaims it. Nothing races us for it here -- this suite
    # talks to pgduck_server directly, so there is no cache worker running, and
    # unwinding out of CacheFile released the lock the sweep needs.
    assert stage_path.exists(), (
        f"expected the rejected download to stay staged for the sweep to "
        f"reclaim: {stage_path}"
    )

    # A budget far above the cache contents, so this only exercises the staging
    # sweep and does not evict anything another test cached.
    run_command("SELECT count(*) FROM pg_lake_manage_cache(21474836480);", pgduck_conn)
    pgduck_conn.rollback()

    assert (
        not stage_path.exists()
    ), f"sweep did not reclaim the staging file: {stage_path}"


def test_azure_download_is_length_checked(azure, pgduck_conn):
    """Azure goes through the same length check, and is not tripped up by it.

    The check lives in FileUtils::CopyFile rather than in the http/s3 download
    helper, because CopyFile dispatches on the file system name and only
    HTTPFileSystem and RegionAwareS3FileSystem use that helper. Azure registers
    as AzureBlobStorageFileSystem / AzureDfsStorageFileSystem, so it takes the
    generic Read/Write loop and would otherwise be left unverified.

    A truncated Azure body cannot be provoked here -- azurite serves what it is
    given -- so the truncation case is covered by the http test above, where the
    server is ours. What this pins down is that the generic branch is subject to
    the check and that a healthy transfer satisfies it, byte for byte, which is
    what a wrong expected-size would break.
    """
    key = "test_azure_download_is_length_checked/data.csv"
    url = f"az://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/az/{TEST_BUCKET}"
        f"/test_azure_download_is_length_checked/{CACHE_FILE_PREFIX}data.csv"
    )

    run_command(
        f"COPY (SELECT * FROM generate_series(1,5000)) TO '{url}' WITH (header false);",
        pgduck_conn,
    )

    remote_size = pg_lake_file_size(f"nocache{url}", pgduck_conn)

    # Writing the blob populated the cache on the way out, so drop that entry
    # first. Without this, pg_lake_cache_file finds the file already cached and
    # returns without copying anything, and the check would never be reached.
    run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    assert not cached_path.exists(), "cache entry survived the uncache"

    run_command(f"CALL pg_lake_cache_file('{url}');", pgduck_conn)

    assert cached_path.exists(), f"azure blob was not cached: {cached_path}"
    assert local_file_size(str(cached_path)) == remote_size, (
        f"cache entry is {local_file_size(str(cached_path))} bytes for a "
        f"{remote_size}-byte blob"
    )

    run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    pgduck_conn.rollback()


@pytest.mark.skipif(
    os.geteuid() == 0,
    reason="mode 0 does not deny root, so the cache open succeeds and this "
    "would pass with or without the fallback",
)
def test_unusable_cache_file_falls_back_to_remote(s3, pgduck_conn):
    """A cache entry that cannot be opened must not fail the read.

    OpenFile() decides to read from the cache with IsOwnedByCurrentUser() and
    then opens the file, and nothing holds the per-path cache lock across those
    two steps. So the entry can stop being usable in between -- cache management
    evicting it under pressure is the ordinary case, and the check only
    establishes that a regular file owned by us was there a moment ago. The open
    used to propagate, killing the statement with a bare
    "IO Error: Cannot open file <cache path>" for an object that is still
    perfectly readable in object storage.

    A cache file owned by us but with no read permission reaches the same open
    failure deterministically, without having to win a race: lstat() still
    reports a regular file owned by the effective UID, so the cache branch is
    taken, and only then does the open fail.
    """
    key = "test_unusable_cache_file_falls_back_to_remote/data.csv"
    url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_unusable_cache_file_falls_back_to_remote/{CACHE_FILE_PREFIX}data.csv"
    )

    run_command(
        f"COPY (SELECT * FROM generate_series(1,10)) TO '{url}' WITH (header false);",
        pgduck_conn,
    )
    run_command(f"CALL pg_lake_cache_file('{url}');", pgduck_conn)
    assert cached_path.is_file(), "file was not cached, nothing to exercise"

    remote_size = pg_lake_file_size(f"nocache{url}", pgduck_conn)
    original_mode = cached_path.stat().st_mode & 0o777

    cached_path.chmod(0o000)
    try:
        result = run_query(
            f"SELECT octet_length(content) AS size FROM read_blob('{url}')",
            pgduck_conn,
            raise_error=False,
        )
    finally:
        cached_path.chmod(original_mode)

    # run_query hands back the error string rather than rows when it fails.
    assert not isinstance(result, str), (
        f"an unusable cache entry failed the read instead of falling back to "
        f"object storage: {result}"
    )
    assert int(result[0]["size"]) == remote_size, (
        f"fell back but returned {result[0]['size']} bytes for a "
        f"{remote_size}-byte object"
    )

    run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    pgduck_conn.rollback()


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
    assert results[0][0] == cached_path.stat().st_size

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == 100

    # Sneakily write something else to the cached file
    run_command(
        f"""
        COPY (SELECT * FROM generate_series(1,50)) TO '{cached_path}' WITH (header false);
    """,
        pgduck_conn,
    )

    # Verify that we are indeed reading from cache when using the URL
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == 50

    # Can bypass cache using nocache prefix
    results = run_query(f"SELECT count(*) FROM 'nocache{url}'", pgduck_conn)
    assert results[0][0] == 100

    # Calling pg_lake_cache_file without force does not change that
    run_command(
        f"""
        FROM pg_lake_cache_file('{url}');
    """,
        pgduck_conn,
    )

    # Verify that we are still from cache when using the URL
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == 50

    # Calling pg_lake_cache_file with force will restore the real file
    run_command(
        f"""
        CALL pg_lake_cache_file('{url}', true);
    """,
        pgduck_conn,
    )

    # Verify that we go the result from S3
    results = run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    assert results[0][0] == 100

    # Remove the cached file
    results = run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    assert results[0][0] is True

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
    assert "NOT FOUND" in error

    pgduck_conn.rollback()

    # Trying to cache a local file path is not allowed
    error = run_command(
        f"CALL pg_lake_cache_file('{cached_path}');", pgduck_conn, raise_error=False
    )
    assert "URL cannot be cached" in error

    pgduck_conn.rollback()

    # Trying to remove a non-existent URL just returns false
    results = run_query(f"CALL pg_lake_uncache_file('{url_notexists}');", pgduck_conn)
    assert results[0][0] is False

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


def test_pg_lake_manage_cache_inode_pressure(s3, pgduck_conn):
    """Cache management must also evict when the cache file system is low on
    inodes, not only when the cache exceeds its byte budget.

    The cache mirrors the object store layout, so a workload with many small
    files can use up all the inodes of a file system with a fixed inode table
    (e.g. ext4) while using a tiny fraction of the byte budget, after which
    every write to the cache fails.

    The number of inodes to keep available is normally derived from the cache
    file system, but pg_lake_min_free_cache_inodes can name it, so that this
    test can ask for a floor that the cache itself can reach.
    """
    # Start from an empty cache, so the file under test is the oldest thing in
    # it and the inode counts below are about our own files
    run_query("CALL pg_lake_manage_cache(0)", pgduck_conn)

    url = f"s3://{TEST_BUCKET}/test_manage_cache_inodes/data.csv"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_manage_cache_inodes/{CACHE_FILE_PREFIX}data.csv"
    )

    # A byte budget the cache never comes close to
    cache_size = 20 * 1024 * 1024 * 1024

    run_command(
        f"COPY (SELECT s FROM generate_series(1,100) as g(s)) TO '{url}';",
        pgduck_conn,
    )
    assert cached_path.exists()

    # The cache directory exists now that it holds a file, so we can ask its
    # file system whether it has a fixed inode table at all
    if os.statvfs(server_params.PGDUCK_CACHE_DIR).f_files == 0:
        pytest.skip(
            "cache file system does not have a fixed inode table, so it does "
            f"not report inode counts: {server_params.PGDUCK_CACHE_DIR}"
        )

    # Cache management only evicts for inodes when its own files can get the
    # file system back above the floor, so pad the cache with files it can free.
    # The padding prefix sorts after the one under test, which stays the oldest
    # and is therefore evicted first.
    padding_count = 16

    for i in range(padding_count):
        padding_url = f"s3://{TEST_BUCKET}/test_manage_cache_inodes_zpad/pad{i}.csv"
        run_command(
            f"COPY (SELECT s FROM generate_series(1,10) as g(s)) TO '{padding_url}';",
            pgduck_conn,
        )

    # Without an inode floor, the file stays in the cache
    run_command("SET pg_lake_min_free_cache_inodes TO 0;", pgduck_conn)
    results = run_query(
        f"FROM pg_lake_manage_cache({cache_size}) WHERE url = '{url}'", pgduck_conn
    )
    assert len(results) == 0
    assert cached_path.exists()

    # A floor a few inodes above what is available, which the padding leaves
    # plenty of room to reach. The slack is there because other processes share
    # the file system, so the count can move between here and the call.
    stats = os.statvfs(server_params.PGDUCK_CACHE_DIR)
    reachable_free_inodes = stats.f_favail + padding_count // 2

    # Under inode pressure, the file is evicted even though it fits in the budget
    run_command(
        f"SET pg_lake_min_free_cache_inodes TO {reachable_free_inodes};", pgduck_conn
    )
    results = run_query(
        f"FROM pg_lake_manage_cache({cache_size}) WHERE url = '{url}'",
        pgduck_conn,
    )
    assert len(results) == 1
    assert results[0][2] == "removed"
    assert not cached_path.exists()

    # Directories hold inodes too, so empty cache directories are reclaimed
    assert not cached_path.parent.exists()

    # but pruning never goes past the cache directory itself
    assert Path(server_params.PGDUCK_CACHE_DIR).is_dir()

    # Reading the file makes it a cache candidate again
    run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)

    # A floor the cache cannot reach, which means something other than the cache
    # is using up the inodes
    stats = os.statvfs(server_params.PGDUCK_CACHE_DIR)
    unreachable_free_inodes = stats.f_files + stats.f_favail + 1

    padding_paths = list(
        Path(
            f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_manage_cache_inodes_zpad"
        ).glob(f"{CACHE_FILE_PREFIX}*")
    )
    assert padding_paths != []

    # We do not add files while we are low on inodes
    run_command(
        f"SET pg_lake_min_free_cache_inodes TO {unreachable_free_inodes};", pgduck_conn
    )
    results = run_query(
        f"FROM pg_lake_manage_cache({cache_size}) WHERE url = '{url}'",
        pgduck_conn,
    )
    assert len(results) == 1
    assert results[0][2] == "skipped (cache file system is low on inodes)"
    assert not cached_path.exists()

    # and we do not throw away a cache that cannot get us above the floor
    # anyway, since we can still read from it
    assert all(padding_path.exists() for padding_path in padding_paths)

    # Once there is no floor to meet, the file is cached
    run_query(f"SELECT count(*) FROM '{url}'", pgduck_conn)
    run_command("SET pg_lake_min_free_cache_inodes TO 0;", pgduck_conn)
    results = run_query(
        f"FROM pg_lake_manage_cache({cache_size}) WHERE url = '{url}'", pgduck_conn
    )
    assert len(results) == 1
    assert results[0][2] == "added"
    assert cached_path.exists()

    # Wipe the cache
    run_query("CALL pg_lake_manage_cache(0)", pgduck_conn)

    run_command("RESET pg_lake_min_free_cache_inodes;", pgduck_conn)

    pgduck_conn.rollback()


def test_min_free_cache_inodes_setting(pgduck_conn):
    """The default derives the floor from the cache file system, so that cache
    management is inode-aware without anybody configuring it.

    Anything other than AUTO or a non-negative number of inodes is rejected,
    rather than quietly turning inode management off. pgduck_server reports
    success for a SET it could not apply, so the rejection shows up as an
    aborted transaction and a setting that kept the value it had.
    """
    results = run_query(
        "SELECT current_setting('pg_lake_min_free_cache_inodes')", pgduck_conn
    )
    assert results[0][0] == "AUTO"

    for rejected in ("-2", "'not a number'"):
        run_command(f"SET pg_lake_min_free_cache_inodes TO {rejected};", pgduck_conn)
        pgduck_conn.rollback()

        results = run_query(
            "SELECT current_setting('pg_lake_min_free_cache_inodes')", pgduck_conn
        )
        assert results[0][0] == "AUTO"

    # A number of inodes is what an operator would put in the init file
    run_command("SET pg_lake_min_free_cache_inodes TO 100000;", pgduck_conn)

    results = run_query(
        "SELECT current_setting('pg_lake_min_free_cache_inodes')", pgduck_conn
    )
    assert results[0][0] == "100000"

    run_command("RESET pg_lake_min_free_cache_inodes;", pgduck_conn)

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
    assert "NOT FOUND" in error

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


def test_parquet_metadata_cache_invalidation_if_uncache_finds_no_local_file(
    s3, pgduck_conn
):
    url = (
        f"s3://{TEST_BUCKET}/"
        "test_parquet_metadata_cache_invalidation_if_uncache_finds_no_local_file/"
        "data1.parquet"
    )
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/"
        "test_parquet_metadata_cache_invalidation_if_uncache_finds_no_local_file/"
        f"{CACHE_FILE_PREFIX}data1.parquet"
    )

    run_command(
        f"""
        COPY (SELECT 1 AS a, 2 AS b) TO '{url}'
    """,
        pgduck_conn,
    )

    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 2

    run_command(
        f"""
        SELECT * FROM pg_lake_cache_file('{url}')
    """,
        pgduck_conn,
    )

    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 2

    run_command(
        f"""
        COPY (SELECT 1 AS a, 2 AS b, 3 AS c) TO '{url}'
    """,
        pgduck_conn,
    )

    cached_path.unlink()

    results = run_query(f"SELECT * FROM pg_lake_uncache_file('{url}')", pgduck_conn)
    assert results[0][0] is False

    results = run_query(f"SELECT * FROM '{url}'", pgduck_conn)
    assert len(results[0]) == 3


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
    assert results[0][0] == 0

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


def test_cache_on_write_success_leaves_no_stage_file(s3, pgduck_conn):
    """A successful write-through cache leaves the finalized pgl-cache.* file
    and no leftover .pgl-stage file.

    Parquet is finalized via FileSync() and CSV via Close(); both rename the
    staging file to its final name. This guards against the destructor
    over-deleting a file that was actually cached.
    """
    test_dir = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_cache_on_write_success_leaves_no_stage_file"
    )

    for ext in ("parquet", "csv"):
        url = (
            f"s3://{TEST_BUCKET}"
            f"/test_cache_on_write_success_leaves_no_stage_file/data.{ext}"
        )
        cached_path = test_dir / f"{CACHE_FILE_PREFIX}data.{ext}"

        run_command(
            f"COPY (SELECT s AS s, s * 2 AS d FROM generate_series(1, 1000) g(s)) "
            f"TO '{url}' (format '{ext}');",
            pgduck_conn,
        )

        assert cached_path.exists(), f"{ext}: write-through cache file is missing"

    # No staging files should be left anywhere under this test's cache subtree.
    stage_files = list(test_dir.rglob("*.pgl-stage")) if test_dir.exists() else []
    assert stage_files == [], f"leftover staging files: {stage_files}"


def test_cache_on_write_abort_removes_stage_file(s3, pgduck_conn):
    """A write-through-cached COPY that aborts mid-stream must not leave an
    orphaned .pgl-stage file behind.

    The COPY runs in a background thread; the main thread observes the
    .pgl-stage file appear while rows stream (proving the write reached the
    cache, so the cleanup check isn't vacuous). The SELECT calls error() on a
    specific row (row-dependent, lazily evaluated in CASE so earlier rows stage
    first) to force a runtime abort before finalization -- DuckDB's '/' is float
    division (1/0 -> Infinity), so error() is used instead. Once the COPY
    returns, the destructor must have removed the stage file (and no final file
    exists). Covers both the Parquet (FileSync) and CSV (Close) paths.
    """
    test_dir = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_cache_on_write_abort_removes_stage_file"
    )

    # A small limit left over from another test would disable write-through
    # caching, so pin it back to the 1GB default.
    run_command(
        "SET GLOBAL pg_lake_cache_on_write_max_size TO '1073741824';", pgduck_conn
    )

    for ext, extra in (("parquet", ", row_group_size 5000"), ("csv", "")):
        url = (
            f"s3://{TEST_BUCKET}"
            f"/test_cache_on_write_abort_removes_stage_file/data.{ext}"
        )
        cached_path = test_dir / f"{CACHE_FILE_PREFIX}data.{ext}"
        stage_path = test_dir / f"{CACHE_FILE_PREFIX}data.{ext}.pgl-stage"

        # Rows 1..199999 stream (and stage) fine; error() fires at g = 200000.
        result = {}

        def run_failing_copy():
            result["error"] = run_query(
                f"COPY (SELECT CASE WHEN g < 200000 THEN g "
                f"ELSE error('forced write-through abort for test') END AS x "
                f"FROM generate_series(1, 1000000) AS s(g)) "
                f"TO '{url}' (format '{ext}'{extra});",
                pgduck_conn,
                raise_error=False,
            )

        worker = threading.Thread(target=run_failing_copy)
        worker.start()

        # Catch the staging file while the COPY is still streaming. It lives for
        # the whole write, so polling reliably observes it.
        staged_during_write = check_file_exist(str(stage_path), timeout_seconds=30)

        worker.join()
        pgduck_conn.rollback()

        assert result["error"] is not None, f"{ext}: expected the COPY to fail"
        assert (
            staged_during_write
        ), f"{ext}: staging file never appeared -- write did not stream to cache"

        # After the abort neither the finalized file nor the staging file remain.
        assert not cached_path.exists(), f"{ext}: unexpected finalized cache file"
        assert not stage_path.exists(), f"{ext}: orphaned staging file not cleaned up"


def test_cache_on_write_truncates_orphaned_stage_file(s3, pgduck_conn):
    """Reusing a .pgl-stage file left by a crash must truncate it first.

    ~CachingFSFileHandle() removes the staging file on a caught abort, but a
    hard crash or kill skips the destructor entirely and leaves one on disk at a
    fully deterministic path. Opening that file with O_CREAT alone writes the new
    contents over its prefix and leaves whatever of the older, longer file
    extends past them; FileSync()/Close() then renames the hybrid in as a
    finalized cache entry. Nothing revalidates a finalized entry against object
    storage, so every later read of that URL gets the wrong bytes until it is
    evicted -- for an Avro manifest, a trailing fragment past the final sync
    marker is read as another block header.

    The orphan is written directly here because that is precisely the on-disk
    state a crash leaves behind, and the one state the destructor cannot cover.
    """
    test_dir = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_cache_on_write_truncates_orphaned_stage_file"
    )

    # A small limit left over from another test would disable write-through
    # caching, so pin it back to the 1GB default.
    run_command(
        "SET GLOBAL pg_lake_cache_on_write_max_size TO '1073741824';", pgduck_conn
    )

    # Far larger than the file about to be written, so a surviving tail is
    # unmistakable rather than a handful of bytes.
    filler = b"ORPHAN.." * (128 * 1024)

    for ext in ("parquet", "csv"):
        url = (
            f"s3://{TEST_BUCKET}"
            f"/test_cache_on_write_truncates_orphaned_stage_file/data.{ext}"
        )
        cached_path = test_dir / f"{CACHE_FILE_PREFIX}data.{ext}"
        stage_path = test_dir / f"{CACHE_FILE_PREFIX}data.{ext}.pgl-stage"

        test_dir.mkdir(parents=True, exist_ok=True)
        cached_path.unlink(missing_ok=True)
        stage_path.write_bytes(filler)

        run_command(
            f"COPY (SELECT s AS s, s * 2 AS d FROM generate_series(1, 100) g(s)) "
            f"TO '{url}' (format '{ext}');",
            pgduck_conn,
        )

        assert cached_path.exists(), f"{ext}: write-through cache file is missing"
        assert not stage_path.exists(), f"{ext}: staging file was not finalized"

        cached_bytes = cached_path.read_bytes()
        assert b"ORPHAN" not in cached_bytes, (
            f"{ext}: content of the orphaned staging file survived into the "
            f"finalized cache entry ({len(cached_bytes)} bytes cached)"
        )
        assert len(cached_bytes) == pg_lake_file_size(f"nocache{url}", pgduck_conn), (
            f"{ext}: finalized cache entry ({len(cached_bytes)} bytes) disagrees "
            f"with the object in storage"
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
    assert results[0][0] == 2000


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
    assert results[0][0] == Decimal("5050")

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

    # Removing twice does not give an error
    run_command(
        f"""
        SELECT pg_lake_remove_file('{url}');
    """,
        pgduck_conn,
    )


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

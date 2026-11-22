import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils_pytest import *

CACHE_FILE_PREFIX = "pgl-cache."

# 1000 bytes that are not valid UTF-8, so a wrong offset shows up in the hex
BLOB_CONTENT = bytes(range(256)) * 3 + bytes(range(232))


def read_blob_range(url, offset, length, pgduck_conn):
    result = run_query(
        f"SELECT hex(content) AS content, size "
        f"FROM pg_lake_read_blob_range('{url}', {offset}, {length})",
        pgduck_conn,
    )
    assert len(result) == 1
    return bytes.fromhex(result[0]["content"]), int(result[0]["size"])


def test_read_blob_range(s3, pgduck_conn):
    key = "test_read_blob_range/data.bin"
    url = f"s3://{TEST_BUCKET}/{key}"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=BLOB_CONTENT)

    # size describes the whole object, not the range
    assert read_blob_range(url, 100, 50, pgduck_conn) == (BLOB_CONTENT[100:150], 1000)
    assert read_blob_range(url, 0, 1000, pgduck_conn) == (BLOB_CONTENT, 1000)
    assert read_blob_range(url, 999, 1, pgduck_conn) == (BLOB_CONTENT[999:], 1000)
    assert read_blob_range(url, 100, 0, pgduck_conn) == (b"", 1000)

    # a range past the end returns the shorter tail, or nothing
    assert read_blob_range(url, 990, 50, pgduck_conn) == (BLOB_CONTENT[990:], 1000)
    assert read_blob_range(url, 1000, 10, pgduck_conn) == (b"", 1000)
    assert read_blob_range(url, 5000, 10, pgduck_conn) == (b"", 1000)

    result = run_query(
        f"SELECT filename, last_modified IS NOT NULL AS has_time "
        f"FROM pg_lake_read_blob_range('{url}', 0, 1)",
        pgduck_conn,
    )
    assert result[0]["filename"] == url
    assert result[0]["has_time"]

    for offset, length in [(-1, 10), (0, -1), ("NULL", 10), (0, "NULL")]:
        error = run_query(
            f"SELECT * FROM pg_lake_read_blob_range('{url}', {offset}, {length})",
            pgduck_conn,
            raise_error=False,
        )
        assert isinstance(error, str), f"range ({offset}, {length}) did not fail"
        pgduck_conn.rollback()

    error = run_query(
        f"SELECT * FROM pg_lake_read_blob_range('s3://{TEST_BUCKET}/test_read_blob_range/missing.bin', 0, 10)",
        pgduck_conn,
        raise_error=False,
    )
    assert isinstance(error, str), "reading a missing object did not fail"
    pgduck_conn.rollback()


def test_read_text_range(s3, pgduck_conn):
    key = "test_read_text_range/data.txt"
    url = f"s3://{TEST_BUCKET}/{key}"
    text = "header|wörd|footer"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=text.encode("utf-8"))

    result = run_query(
        f"SELECT content, size FROM pg_lake_read_text_range('{url}', 7, 5)",
        pgduck_conn,
    )
    assert result[0]["content"] == "wörd"
    assert int(result[0]["size"]) == len(text.encode("utf-8"))

    # ö is 2 bytes at offset 8, so a range ending at offset 9 splits it
    error = run_query(
        f"SELECT content FROM pg_lake_read_text_range('{url}', 7, 2)",
        pgduck_conn,
        raise_error=False,
    )
    assert isinstance(error, str), "a range that splits a character did not fail"
    assert "UTF-8" in error, error
    pgduck_conn.rollback()


def test_read_range_does_not_cache(s3, pgduck_conn):
    """A ranged read of an uncached object must not queue the whole object for
    caching, since the cache would download all of it to serve a small range."""
    key = "test_read_range_does_not_cache/data.bin"
    url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_read_range_does_not_cache/{CACHE_FILE_PREFIX}data.bin"
    )
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=BLOB_CONTENT)

    assert read_blob_range(url, 100, 50, pgduck_conn) == (BLOB_CONTENT[100:150], 1000)

    # A budget far above the cache contents, so this only adds candidates
    results = run_query(
        f"SELECT * FROM pg_lake_manage_cache(21474836480) WHERE url = '{url}'",
        pgduck_conn,
    )
    assert len(results) == 0, f"ranged read made the object a cache candidate"
    assert not cached_path.exists()

    # read_blob does queue it, which shows the check above can fail
    run_query(f"SELECT octet_length(content) FROM read_blob('{url}')", pgduck_conn)
    results = run_query(
        f"SELECT action FROM pg_lake_manage_cache(21474836480) WHERE url = '{url}'",
        pgduck_conn,
    )
    assert len(results) == 1 and results[0]["action"] == "added"
    assert cached_path.exists()

    run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
    pgduck_conn.rollback()


def test_read_range_uses_cache(s3, pgduck_conn):
    """A ranged read of a cached object is served from the cached copy."""
    key = "test_read_range_uses_cache/data.bin"
    url = f"s3://{TEST_BUCKET}/{key}"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}"
        f"/test_read_range_uses_cache/{CACHE_FILE_PREFIX}data.bin"
    )
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=BLOB_CONTENT)

    run_command(f"CALL pg_lake_cache_file('{url}');", pgduck_conn)
    assert cached_path.is_file(), "file was not cached, nothing to exercise"

    # make the cached copy differ from the object, to tell where a read went
    cached_content = bytes(reversed(BLOB_CONTENT))
    cached_path.write_bytes(cached_content)

    try:
        assert read_blob_range(url, 100, 50, pgduck_conn) == (
            cached_content[100:150],
            1000,
        )

        # the nocache prefix still reads the object itself
        assert read_blob_range(f"nocache{url}", 100, 50, pgduck_conn) == (
            BLOB_CONTENT[100:150],
            1000,
        )
    finally:
        run_query(f"CALL pg_lake_uncache_file('{url}');", pgduck_conn)
        pgduck_conn.rollback()


@pytest.fixture
def range_http_server():
    """Serve BLOB_CONTENT with range support and record each GET's Range."""

    ranges = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(BLOB_CONTENT)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            range_header = self.headers.get("Range")
            ranges.append(range_header)

            if range_header is None:
                body = BLOB_CONTENT
                self.send_response(200)
            else:
                first, last = range_header.removeprefix("bytes=").split("-")
                body = BLOB_CONTENT[int(first) : int(last) + 1]
                self.send_response(206)
                self.send_header(
                    "Content-Range",
                    f"bytes {first}-{int(first) + len(body) - 1}/{len(BLOB_CONTENT)}",
                )

            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server = HTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield port, ranges

    server.shutdown()
    server.server_close()
    thread.join(timeout=10)


def test_read_range_sends_one_range_get(range_http_server, pgduck_conn):
    port, ranges = range_http_server
    url = f"http://127.0.0.1:{port}/data.bin"

    assert read_blob_range(url, 100, 50, pgduck_conn) == (BLOB_CONTENT[100:150], 1000)
    assert ranges == ["bytes=100-149"]

    # the end of the request is clamped to the end of the object
    ranges.clear()
    assert read_blob_range(url, 990, 50, pgduck_conn) == (BLOB_CONTENT[990:], 1000)
    assert ranges == ["bytes=990-999"]

    # an empty range needs no GET at all
    ranges.clear()
    assert read_blob_range(url, 2000, 50, pgduck_conn) == (b"", 1000)
    assert ranges == []

    pgduck_conn.rollback()

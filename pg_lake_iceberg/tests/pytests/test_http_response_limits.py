"""
The HTTP client stops reading a response that exceeds its size limit and
fails the request. REST catalog responses also arrive after commit, where an
allocation error would turn into a PANIC.
"""

import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils_pytest import *

_CHUNK = b"x" * (1024 * 1024)


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        # /small or /large/<megabytes>
        megabytes = int(self.path.split("/")[-1]) if "large" in self.path else 0
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(max(megabytes * len(_CHUNK), 2)))
        self.end_headers()
        try:
            if megabytes == 0:
                self.wfile.write(b"ok")
            for _ in range(megabytes):
                self.wfile.write(_CHUNK)
        except (BrokenPipeError, ConnectionResetError):
            pass


@pytest.fixture(scope="module")
def http_server():
    port = _find_free_port()
    server = HTTPServer(("127.0.0.1", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


def test_small_response(superuser_conn, create_http_helper_functions, http_server):
    result = run_query(
        f"SELECT status, body FROM lake_iceberg.test_http_get('{http_server}/small')",
        superuser_conn,
    )
    assert result == [[200, "ok"]]
    superuser_conn.rollback()


def test_oversized_response_fails_request(
    superuser_conn, create_http_helper_functions, http_server
):
    # above the 256MB limit; the test helper raises curl's error, while the
    # REST catalog code reports it at the level it chooses
    error = run_query(
        f"SELECT status FROM lake_iceberg.test_http_get('{http_server}/large/300')",
        superuser_conn,
        raise_error=False,
    )
    assert "Failure writing output" in error
    superuser_conn.rollback()

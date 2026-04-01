"""
Test that the REST catalog retry logic replaces the Authorization header
with a freshly-fetched token when the server returns 419 (token expired).

A mock HTTP server is spun up in-process. It serves two roles:
  - Token endpoint (/api/catalog/v1/oauth/tokens): returns a unique token on
    every call.
  - All other paths: returns 419 on the first data request, then 200 only if
    the Authorization header carries a *different* token than the first request
    (proving the header was updated). If the token is stale, it keeps returning
    419, which will cause the test to fail.
"""

import json
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from utils_pytest import *


# ---------------------------------------------------------------------------
# Mock REST catalog server
# ---------------------------------------------------------------------------


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class():
    """
    Factory that returns a fresh handler class with its own isolated state,
    so concurrent test runs do not share mutable class variables.
    """

    class _Handler(BaseHTTPRequestHandler):
        tokens_issued = []
        data_request_auths = []
        token_was_refreshed = False

        def _handle(self):
            if "/oauth/tokens" in self.path:
                token = uuid.uuid4().hex
                _Handler.tokens_issued.append(token)
                body = json.dumps(
                    {
                        "access_token": token,
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body.encode())
                return

            auth = self.headers.get("Authorization", "<missing>")
            _Handler.data_request_auths.append(auth)
            seq = len(_Handler.data_request_auths)

            if seq == 1:
                self.send_response(419)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error": "token expired"}')
            elif seq == 2:
                same_as_first = auth == _Handler.data_request_auths[0]
                if same_as_first:
                    self.send_response(419)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"error": "still stale token"}')
                else:
                    _Handler.token_was_refreshed = True
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"namespace": ["myns"], "properties": {}}')
            else:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


@pytest.fixture(scope="function")
def mock_rest_catalog():
    """Start a mock REST catalog server on a free port and tear it down after."""
    port = _find_free_port()
    handler_class = _make_handler_class()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    yield port, handler_class

    httpd.shutdown()
    thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_token_refresh_on_419_retry(
    superuser_conn, iceberg_extension, installcheck, mock_rest_catalog
):
    """
    Verify that retrying after a 419 sends a refreshed token, not the stale one.

    The mock returns 419 on the first data request.  On retry it checks whether
    the Authorization header changed; if it did, it returns 200.  If the header
    did NOT change (the bug), the mock returns 419 again, exhausting retries.
    """
    if installcheck:
        return

    port, handler_class = mock_rest_catalog

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "SELECT pg_reload_conf()",
        ]
    )

    try:
        run_command(
            """
            CREATE OR REPLACE FUNCTION register_namespace_to_rest_catalog(TEXT,TEXT)
            RETURNS void
            LANGUAGE C VOLATILE STRICT
            AS 'pg_lake_iceberg', 'register_namespace_to_rest_catalog';
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(
            "SELECT register_namespace_to_rest_catalog('mycat', 'myns')",
            superuser_conn,
        )
        superuser_conn.commit()

        assert handler_class.token_was_refreshed, (
            "The Authorization header was NOT updated on retry — "
            "the stale token was reused. "
            f"tokens_issued={handler_class.tokens_issued}, "
            f"data_request_auths={handler_class.data_request_auths}"
        )
    finally:
        run_command(
            "DROP FUNCTION IF EXISTS register_namespace_to_rest_catalog(TEXT,TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command_outside_tx(
            [
                "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host",
                "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id",
                "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret",
                "SELECT pg_reload_conf()",
            ]
        )

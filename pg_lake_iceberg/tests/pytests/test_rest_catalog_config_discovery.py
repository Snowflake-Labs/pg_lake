"""
Verify that FetchRestCatalogConfigPrefix correctly auto-detects the catalog
prefix from the Iceberg REST /v1/config endpoint.

A mock HTTP server is spun up in-process.  It serves two roles:
  - Token endpoint (/v1/oauth/tokens): returns a canned Bearer token.
  - Config endpoint (/v1/config): returns a configurable JSON payload.

The test exercises:
  1. overrides.prefix takes priority over defaults.prefix.
  2. defaults.prefix is used when overrides is empty.
  3. NULL is returned when neither field is present.
  4. NULL is returned when the server returns a non-200 status.
"""

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from utils_pytest import *


# ---------------------------------------------------------------------------
# Mock REST catalog server
# ---------------------------------------------------------------------------

_FAKE_TOKEN = "fake-bearer-token-for-config-test"

_CONFIG_RESPONSES: dict = {}  # path suffix -> (status, body)


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class(responses: dict):
    """
    Factory so each test run gets an isolated handler with its own state.
    ``responses`` maps a path suffix to (status_code, body_str).
    """

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # silence server output during tests

        def _handle(self):
            if "/oauth/tokens" in self.path:
                body = json.dumps(
                    {
                        "access_token": _FAKE_TOKEN,
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body.encode())
                return

            for suffix, (status, body) in responses.items():
                if self.path.endswith(suffix):
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(body.encode())
                    return

            self.send_response(404)
            self.end_headers()

        def do_GET(self):
            self._handle()

        def do_POST(self):
            self._handle()

    return _Handler


def _start_mock_server(responses: dict):
    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), _make_handler_class(responses))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, port


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HELPER_FUNC_SQL = """
CREATE OR REPLACE FUNCTION lake_iceberg.fetch_rest_catalog_config_prefix_from_server(
    catalog_name text
)
RETURNS text
LANGUAGE C
VOLATILE STRICT
AS 'pg_lake_iceberg', 'fetch_rest_catalog_config_prefix_from_server';
"""


@pytest.fixture(scope="module")
def create_config_helper(superuser_conn):
    run_command(_HELPER_FUNC_SQL, superuser_conn)
    yield
    run_command(
        "DROP FUNCTION IF EXISTS "
        "lake_iceberg.fetch_rest_catalog_config_prefix_from_server(text)",
        superuser_conn,
    )


def _create_server(conn, server_name, endpoint):
    run_command(
        f"""
        CREATE SERVER {server_name} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (
                rest_endpoint '{endpoint}',
                location_prefix 's3://test-bucket/'
            )
        """,
        conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {server_name}
            OPTIONS (client_id 'test-id', client_secret 'test-secret')
        """,
        conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {server_name} TO PUBLIC",
        conn,
    )
    conn.commit()


def _drop_server(conn, server_name):
    try:
        run_command(
            f"DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER {server_name}", conn
        )
        run_command(f"DROP SERVER IF EXISTS {server_name} CASCADE", conn)
        conn.commit()
    except Exception:
        conn.rollback()


def _call_fetch(conn, server_name):
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT lake_iceberg.fetch_rest_catalog_config_prefix_from_server(%s)",
            (server_name,),
        )
        row = cur.fetchone()
        return row[0]
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_overrides_prefix_takes_priority(
    create_config_helper, superuser_conn, pg_conn, extension
):
    """overrides.prefix is returned when both overrides and defaults are present."""
    config_body = json.dumps(
        {
            "overrides": {"prefix": "override-wins"},
            "defaults": {"prefix": "default-loses"},
        }
    )
    httpd, port = _start_mock_server({"/v1/config": (200, config_body)})
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "cfg_disc_test_overrides"

    try:
        _create_server(superuser_conn, server_name, endpoint)
        result = _call_fetch(pg_conn, server_name)
        assert result == "override-wins", f"Expected 'override-wins', got {result!r}"
    finally:
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_defaults_prefix_used_when_overrides_empty(
    create_config_helper, superuser_conn, pg_conn, extension
):
    """defaults.prefix is returned when overrides carries no prefix."""
    config_body = json.dumps({"overrides": {}, "defaults": {"prefix": "from-defaults"}})
    httpd, port = _start_mock_server({"/v1/config": (200, config_body)})
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "cfg_disc_test_defaults"

    try:
        _create_server(superuser_conn, server_name, endpoint)
        result = _call_fetch(pg_conn, server_name)
        assert result == "from-defaults", f"Expected 'from-defaults', got {result!r}"
    finally:
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_no_prefix_returns_null(
    create_config_helper, superuser_conn, pg_conn, extension
):
    """NULL is returned when neither overrides nor defaults carry a prefix."""
    config_body = json.dumps({"overrides": {}, "defaults": {}})
    httpd, port = _start_mock_server({"/v1/config": (200, config_body)})
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "cfg_disc_test_no_prefix"

    try:
        _create_server(superuser_conn, server_name, endpoint)
        result = _call_fetch(pg_conn, server_name)
        assert result is None, f"Expected None, got {result!r}"
    finally:
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_non_200_returns_null(create_config_helper, superuser_conn, pg_conn, extension):
    """NULL is returned when /v1/config responds with a non-200 status."""
    httpd, port = _start_mock_server({"/v1/config": (503, '{"error": "unavailable"}')})
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "cfg_disc_test_non200"

    try:
        _create_server(superuser_conn, server_name, endpoint)
        result = _call_fetch(pg_conn, server_name)
        assert result is None, f"Expected None on 503, got {result!r}"
    finally:
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()

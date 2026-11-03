"""
Test that pg_lake names itself in the User-Agent of the requests it sends to a
REST catalog, including the token request, so a catalog can tell them apart
from other clients' requests.
"""

import json
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from utils_pytest import *


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class():
    class _Handler(BaseHTTPRequestHandler):
        user_agents = []

        def _handle(self):
            _Handler.user_agents.append(
                (self.path, self.headers.get("User-Agent", "<missing>"))
            )

            if "/oauth/tokens" in self.path:
                body = {
                    "access_token": uuid.uuid4().hex,
                    "token_type": "bearer",
                    "expires_in": 3600,
                }
            else:
                body = {"namespace": ["myns"], "properties": {}}

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        do_GET = _handle
        do_POST = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


@pytest.fixture(scope="function")
def mock_rest_catalog():
    port = _find_free_port()
    handler_class = _make_handler_class()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    yield port, handler_class

    httpd.shutdown()
    thread.join(timeout=5)


def test_requests_carry_pg_lake_user_agent(
    superuser_conn, iceberg_extension, installcheck, mock_rest_catalog
):
    if installcheck:
        return

    port, handler_class = mock_rest_catalog
    host = f"http://127.0.0.1:{port}/api/catalog"

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{host}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "SELECT pg_reload_conf()",
        ]
    )
    wait_for_reloaded_settings(
        [superuser_conn],
        {
            "pg_lake_iceberg.rest_catalog_host": host,
            "pg_lake_iceberg.rest_catalog_client_id": "test_id",
            "pg_lake_iceberg.rest_catalog_client_secret": "test_secret",
        },
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

        version = run_query(
            "SELECT default_version FROM pg_available_extensions "
            "WHERE name = 'pg_lake_iceberg'",
            superuser_conn,
        )[0][0]

        paths = [path for path, _ in handler_class.user_agents]
        assert any("/oauth/tokens" in path for path in paths), paths
        assert any("/namespaces/" in path for path in paths), paths
        assert {agent for _, agent in handler_class.user_agents} == {
            f"pg_lake/{version}"
        }
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

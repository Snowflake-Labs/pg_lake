"""
Test that changing a credential GUC of the built-in REST catalog flushes the
backend's token cache (#418), and that setting a GUC to its current value or
reloading the configuration does not.

A mock HTTP server is spun up in-process. It serves two roles:
  - Token endpoint (/api/catalog/v1/oauth/tokens): issues a fresh token on
    every call and remembers the Basic credentials that requested it.
  - All other paths: return 200 and record the Authorization header used.
"""

import base64
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
        # token -> "client_id:client_secret" that minted it
        tokens_issued = {}
        data_request_auths = []

        def _handle(self):
            if "/oauth/tokens" in self.path:
                basic = self.headers.get("Authorization", "")
                creds = ""
                if basic.startswith("Basic "):
                    creds = base64.b64decode(basic[len("Basic ") :]).decode()
                token = uuid.uuid4().hex
                _Handler.tokens_issued[token] = creds
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

            _Handler.data_request_auths.append(
                self.headers.get("Authorization", "<missing>")
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"namespace": ["myns"], "properties": {}}')

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
    port = _find_free_port()
    handler_class = _make_handler_class()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    yield port, handler_class

    httpd.shutdown()
    thread.join(timeout=5)


def _last_token_creds(handler_class):
    """Credentials behind the token on the most recent data request."""
    auth = handler_class.data_request_auths[-1]
    assert auth.startswith("Bearer "), auth
    return handler_class.tokens_issued[auth[len("Bearer ") :]]


def _register(conn):
    run_command("SELECT register_namespace_to_rest_catalog('mycat', 'myns')", conn)
    conn.commit()


def test_credential_guc_change_flushes_token_cache(
    superuser_conn, iceberg_extension, installcheck, mock_rest_catalog
):
    if installcheck:
        return

    port, handler_class = mock_rest_catalog

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}/api/catalog'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'id_a'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'secret_a'",
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

        # make sure this backend has picked up the reloaded settings
        run_command(
            "SET pg_lake_iceberg.rest_catalog_client_id TO 'id_a'", superuser_conn
        )
        run_command(
            "SET pg_lake_iceberg.rest_catalog_client_secret TO 'secret_a'",
            superuser_conn,
        )
        superuser_conn.commit()

        # warm the cache
        _register(superuser_conn)
        assert _last_token_creds(handler_class) == "id_a:secret_a"
        tokens_after_warm = len(handler_class.tokens_issued)

        # setting a GUC to its current value must keep the cached token
        run_command(
            "SET pg_lake_iceberg.rest_catalog_client_secret TO 'secret_a'",
            superuser_conn,
        )
        superuser_conn.commit()
        _register(superuser_conn)
        assert len(handler_class.tokens_issued) == tokens_after_warm, (
            "setting an unchanged GUC flushed the token cache: "
            f"{handler_class.tokens_issued}"
        )

        # a reload with nothing changed must keep the cached token too
        run_command("SELECT pg_reload_conf()", superuser_conn)
        superuser_conn.commit()
        _register(superuser_conn)
        assert len(handler_class.tokens_issued) == tokens_after_warm, (
            "pg_reload_conf() flushed the token cache: "
            f"{handler_class.tokens_issued}"
        )

        # rotating the secret must mint a token from the new credentials
        run_command(
            "SET pg_lake_iceberg.rest_catalog_client_secret TO 'secret_b'",
            superuser_conn,
        )
        superuser_conn.commit()
        _register(superuser_conn)
        assert _last_token_creds(handler_class) == "id_a:secret_b", (
            "the token minted from the previous secret was reused: "
            f"tokens_issued={handler_class.tokens_issued}, "
            f"data_request_auths={handler_class.data_request_auths}"
        )

        # and so must changing the scope
        tokens_before_scope = len(handler_class.tokens_issued)
        run_command(
            "SET pg_lake_iceberg.rest_catalog_scope TO 'PRINCIPAL_ROLE:other'",
            superuser_conn,
        )
        superuser_conn.commit()
        _register(superuser_conn)
        assert len(handler_class.tokens_issued) == tokens_before_scope + 1, (
            "changing rest_catalog_scope did not fetch a new token: "
            f"{handler_class.tokens_issued}"
        )
    finally:
        superuser_conn.rollback()
        run_command("RESET pg_lake_iceberg.rest_catalog_client_id", superuser_conn)
        run_command("RESET pg_lake_iceberg.rest_catalog_client_secret", superuser_conn)
        run_command("RESET pg_lake_iceberg.rest_catalog_scope", superuser_conn)
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

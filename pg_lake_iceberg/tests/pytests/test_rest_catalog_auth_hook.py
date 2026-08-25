"""
Tests for PgLakeRestCatalogAuthHook, the seam that lets another extension
supply REST catalog credentials for catalogs whose authentication pg_lake
has no built-in support for.

A mock HTTP server stands in for the catalog.  It records the
Authorization header of every non-token request and counts how often the
OAuth2 token endpoint was hit, which is what separates "the provider
supplied the credential" from "pg_lake fell back to its own grant".

Each test runs on its own connection because the credential cache is
backend-local, so a credential cached by one test would otherwise be
reused by the next.
"""

import json
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from utils_pytest import *


_HOOK_FNS = """
CREATE OR REPLACE FUNCTION install_test_rest_catalog_auth_hook(TEXT, INT, BOOL)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'install_test_rest_catalog_auth_hook';

CREATE OR REPLACE FUNCTION test_rest_catalog_auth_hook_calls()
RETURNS int LANGUAGE C VOLATILE
AS 'pg_lake_iceberg', 'test_rest_catalog_auth_hook_calls';

CREATE OR REPLACE FUNCTION remove_test_rest_catalog_auth_hook()
RETURNS void LANGUAGE C VOLATILE
AS 'pg_lake_iceberg', 'remove_test_rest_catalog_auth_hook';

CREATE OR REPLACE FUNCTION register_namespace_to_rest_catalog(TEXT,TEXT)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'register_namespace_to_rest_catalog';
"""


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class():
    """
    Factory returning a handler class with its own isolated state, so
    concurrent test runs do not share mutable class variables.
    """

    class _Handler(BaseHTTPRequestHandler):
        token_requests = 0
        data_request_auths = []

        def _handle(self):
            if "/oauth/tokens" in self.path:
                _Handler.token_requests += 1
                body = json.dumps(
                    {
                        "access_token": uuid.uuid4().hex,
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
def catalog_and_conn(postgres):
    """
    Point pg_lake at a mock catalog and hand back a fresh connection with
    the hook test shims declared.
    """
    port = _find_free_port()
    handler_class = _make_handler_class()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "SELECT pg_reload_conf()",
        ]
    )

    conn = open_pg_conn()
    run_command(_HOOK_FNS, conn)
    conn.commit()

    yield handler_class, conn

    conn.close()

    httpd.shutdown()
    thread.join(timeout=5)

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret",
            "SELECT pg_reload_conf()",
        ]
    )


def _install_hook(conn, authorization, expires_in, claims):
    run_command(
        f"SELECT install_test_rest_catalog_auth_hook("
        f"'{authorization}', {expires_in}, {claims})",
        conn,
    )
    conn.commit()


def _hook_calls(conn):
    result = run_query("SELECT test_rest_catalog_auth_hook_calls()", conn)
    conn.commit()
    return result[0][0]


def _touch_catalog(conn, namespace):
    run_command(
        f"SELECT register_namespace_to_rest_catalog('mycat', '{namespace}')", conn
    )
    conn.commit()


def test_provider_supplies_authorization_verbatim(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    The provider's value reaches the catalog unchanged, scheme included.

    The scheme here is deliberately not "Bearer": pg_lake must not assume
    one, so that a catalog authenticating some other way needs no change
    in pg_lake itself.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn

    _install_hook(conn, "Snowflake-WIF opaque-credential-123", 3600, "true")
    _touch_catalog(conn, "myns")

    assert handler_class.data_request_auths == [
        "Snowflake-WIF opaque-credential-123"
    ], "the provider's authorization did not reach the catalog verbatim"

    assert (
        handler_class.token_requests == 0
    ), "pg_lake ran its own OAuth2 grant even though a provider claimed the catalog"


def test_provider_credential_is_cached(
    iceberg_extension, installcheck, catalog_and_conn
):
    """A provider reporting a lifetime is consulted once, not per request."""
    if installcheck:
        return

    handler_class, conn = catalog_and_conn

    _install_hook(conn, "Bearer cacheable-credential", 3600, "true")
    _touch_catalog(conn, "ns_one")
    _touch_catalog(conn, "ns_two")

    assert len(handler_class.data_request_auths) == 2
    assert _hook_calls(conn) == 1, "a credential with a lifetime was re-fetched"


def test_uncacheable_credential_is_refetched(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    expiresIn = 0 means "do not cache", which is what a provider reading a
    credential rotated underneath it needs.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn

    _install_hook(conn, "Bearer rotating-credential", 0, "true")
    _touch_catalog(conn, "ns_one")
    _touch_catalog(conn, "ns_two")

    assert len(handler_class.data_request_auths) == 2
    assert _hook_calls(conn) == 2, "an uncacheable credential was cached anyway"


def test_declining_provider_falls_back_to_builtin_oauth2(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    A provider that declines a catalog leaves pg_lake's own OAuth2 grant in
    charge, so one provider can claim some servers and ignore others.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn

    _install_hook(conn, "Snowflake-WIF unused", 3600, "false")
    _touch_catalog(conn, "myns")

    assert _hook_calls(conn) == 1, "the provider was never consulted"
    assert (
        handler_class.token_requests == 1
    ), "pg_lake did not fall back to its own OAuth2 grant"
    assert handler_class.data_request_auths[0].startswith("Bearer ")

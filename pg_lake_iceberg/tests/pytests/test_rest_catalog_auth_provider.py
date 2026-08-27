"""
Tests for the credential provider seam, which lets another extension supply
REST catalog credentials for catalogs whose authentication pg_lake has no
built-in support for.  The provider is named by
pg_lake_iceberg.rest_catalog_auth_provider and resolved by name on first use.

A mock HTTP server stands in for the catalog.  It records the
Authorization header of every non-token request and counts how often the
OAuth2 token endpoint was hit, which is what separates "the provider
supplied the credential" from "pg_lake fell back to its own grant".  It
can also answer 401, which is how the refresh-on-rejection path is
exercised.

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
CREATE OR REPLACE FUNCTION set_test_rest_catalog_auth_response(TEXT, INT, BOOL)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'set_test_rest_catalog_auth_response';

CREATE OR REPLACE FUNCTION test_rest_catalog_auth_provider_calls()
RETURNS int LANGUAGE C VOLATILE
AS 'pg_lake_iceberg', 'test_rest_catalog_auth_provider_calls';

CREATE OR REPLACE FUNCTION test_rest_catalog_auth_provider_endpoints()
RETURNS text LANGUAGE C VOLATILE
AS 'pg_lake_iceberg', 'test_rest_catalog_auth_provider_endpoints';

CREATE OR REPLACE FUNCTION register_namespace_to_rest_catalog(TEXT,TEXT)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'register_namespace_to_rest_catalog';
"""

# The stub provider lives in pg_lake_iceberg itself, which is what a real
# provider extension would be named here instead.
_TEST_PROVIDER = "pg_lake_iceberg:test_rest_catalog_auth_provider"


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

        # Number of upcoming catalog requests to answer with 401, or -1 to
        # reject every one of them.
        reject_data_requests = 0

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

            if _Handler.reject_data_requests != 0:
                if _Handler.reject_data_requests > 0:
                    _Handler.reject_data_requests -= 1

                body = json.dumps(
                    {
                        "error": {
                            "message": "credential is not authorized",
                            "type": "NotAuthorizedException",
                            "code": 401,
                        }
                    }
                )
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body.encode())
                return

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
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_auth_provider TO '{_TEST_PROVIDER}'",
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
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_auth_provider",
            "SELECT pg_reload_conf()",
        ]
    )


def _install_hook(conn, authorization, expires_in, claims):
    run_command(
        f"SELECT set_test_rest_catalog_auth_response("
        f"'{authorization}', {expires_in}, {claims})",
        conn,
    )
    conn.commit()


def _hook_calls(conn):
    result = run_query("SELECT test_rest_catalog_auth_provider_calls()", conn)
    conn.commit()
    return result[0][0]


def _provider_endpoints(conn):
    result = run_query("SELECT test_rest_catalog_auth_provider_endpoints()", conn)
    conn.commit()
    return result[0][0].split("|")


def _touch_catalog(conn, namespace):
    run_command(
        f"SELECT register_namespace_to_rest_catalog('mycat', '{namespace}')", conn
    )
    conn.commit()


def test_the_provider_is_told_how_the_catalog_is_addressed(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    A provider mints credentials for a catalog it knows nothing else about, so
    pg_lake passes on how that catalog is addressed rather than leaving it to be
    reconstructed: the base URI as configured, mount path and all, plus an
    explicit oauth_endpoint when the deployment authenticates somewhere other
    than the catalog itself.  A provider left to reconstruct either one has to
    guess, and a wrong guess fails as a 404 that reads like a missing table.
    """
    handler_class, conn = catalog_and_conn

    # an uncacheable credential, so the provider is consulted on every request
    _install_hook(conn, "Bearer from-provider", 0, "true")

    _touch_catalog(conn, f"ns_{uuid.uuid4().hex[:8]}")
    base_uri, oauth_endpoint = _provider_endpoints(conn)

    assert base_uri.startswith("http://127.0.0.1:")
    assert oauth_endpoint == ""

    configured = "http://127.0.0.1:9/idp/oauth/tokens"

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_oauth_host_path TO "
            f"'{configured}'",
            "SELECT pg_reload_conf()",
        ]
    )
    try:
        # a fresh backend, so the reloaded setting is in force for the request
        # the provider is consulted for
        reloaded = open_pg_conn()
        try:
            _install_hook(reloaded, "Bearer from-provider", 0, "true")
            _touch_catalog(reloaded, f"ns_{uuid.uuid4().hex[:8]}")
            _, oauth_endpoint = _provider_endpoints(reloaded)

            assert oauth_endpoint == configured
        finally:
            reloaded.close()
    finally:
        run_command_outside_tx(
            [
                "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_oauth_host_path",
                "SELECT pg_reload_conf()",
            ]
        )


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


def test_malformed_provider_name_is_rejected(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    A provider name that is not "library:symbol" is reported when a catalog
    is contacted, since that is when the name is resolved.
    """
    if installcheck:
        return

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_auth_provider TO 'missing_symbol'",
            "SELECT pg_reload_conf()",
        ]
    )

    # A backend opened after the reload starts with the new setting, which
    # avoids racing the reload against the query below.
    conn = open_pg_conn()

    try:
        with pytest.raises(Exception, match="invalid value for parameter"):
            _touch_catalog(conn, "myns")
    finally:
        conn.close()


def test_catalog_401_refreshes_the_credential(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    A 401 re-fetches the credential and retries.

    A cached credential can lapse before the catalog sees it, and catalogs
    behind an OAuth2 token exchange report that as 401 rather than the
    non-standard 419 that Polaris uses.  Without this the request would
    fail even though a usable credential was one exchange away.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.reject_data_requests = 1

    _install_hook(conn, "Bearer cacheable-credential", 3600, "true")
    _touch_catalog(conn, "myns")

    assert len(handler_class.data_request_auths) == 2, "the 401 was not retried"
    assert _hook_calls(conn) == 2, "the 401 did not re-fetch the credential"


def test_persistent_401_is_reported_after_one_refresh(
    iceberg_extension, installcheck, catalog_and_conn
):
    """
    A credential that is genuinely unauthorized gets one refresh, not every
    retry slot, so the failure surfaces promptly instead of after three
    round trips.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.reject_data_requests = -1

    _install_hook(conn, "Bearer rejected-credential", 3600, "true")

    with pytest.raises(Exception, match="credential is not authorized"):
        _touch_catalog(conn, "myns")

    conn.rollback()

    assert (
        len(handler_class.data_request_auths) == 2
    ), "a persistent 401 consumed more than its one refresh"
    assert _hook_calls(conn) == 2

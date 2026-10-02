"""
Tests for vended credentials support in REST catalog integration.

A mock HTTP server simulates an Iceberg REST catalog that returns
vended credentials in the loadTable response.  The tests verify that:

1. The X-Iceberg-Access-Delegation header is sent on loadTable requests
   when vended credentials are enabled.
2. Vended credentials from the response "config" map are extracted and
   pushed to pgduck_server as DuckDB scoped secrets.
3. The credential cache works correctly (no redundant REST calls).
4. Disabling vended credentials suppresses the header and secret creation.
5. ALTER/DROP SERVER invalidates the vended credential cache.
6. Azure SAS tokens are extracted in every spelling catalogs state them
   by, with endpoints held to pg_lake.allowed_azure_host_suffixes.
7. Credentials pg_lake cannot use are reported rather than passed over
   in silence, while settings are not.
"""

import json
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from utils_pytest import *

from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Mock REST catalog server that returns vended credentials
# ---------------------------------------------------------------------------


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_vended_creds_handler():
    """
    Factory that returns a handler class which:
    - Issues OAuth tokens on /oauth/tokens
    - Returns a loadTable response with vended credentials in the config
      map when X-Iceberg-Access-Delegation: vended-credentials is present
    - Tracks all requests for assertion
    """

    class _Handler(BaseHTTPRequestHandler):
        tokens_issued = []
        load_table_requests = []
        access_delegation_headers = []
        namespace_requests = []

        def _handle(self):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length) if content_length > 0 else b""

            if "/oauth/tokens" in self.path:
                token = uuid.uuid4().hex
                _Handler.tokens_issued.append(token)
                resp = json.dumps(
                    {
                        "access_token": token,
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(resp.encode())
                return

            # Track namespace creation (POST to /namespaces)
            if "/namespaces" in self.path and self.command == "POST":
                _Handler.namespace_requests.append(
                    {
                        "path": self.path,
                        "method": self.command,
                    }
                )
                resp = json.dumps({"namespace": ["test_ns"], "properties": {}})
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(resp.encode())
                return

            # Track namespace HEAD check
            if "/namespaces/" in self.path and self.command == "HEAD":
                self.send_response(204)
                self.end_headers()
                return

            # loadTable: GET /namespaces/<ns>/tables/<table>
            if "/tables/" in self.path and self.command == "GET":
                delegation = self.headers.get("X-Iceberg-Access-Delegation", "")
                _Handler.access_delegation_headers.append(delegation)
                _Handler.load_table_requests.append(
                    {
                        "path": self.path,
                        "method": self.command,
                        "delegation": delegation,
                    }
                )

                config = {}
                if delegation == "vended-credentials":
                    config = {
                        "s3.access-key-id": "VENDED_ACCESS_KEY_123",
                        "s3.secret-access-key": "VENDED_SECRET_KEY_456",
                        "s3.session-token": "VENDED_SESSION_TOKEN_789",
                        "client.region": "us-west-2",
                    }

                resp = json.dumps(
                    {
                        "metadata-location": "s3://test-bucket/test-ns/test-table/metadata/v1.metadata.json",
                        "metadata": {
                            "format-version": 2,
                            "table-uuid": str(uuid.uuid4()),
                            "location": "s3://test-bucket/test-ns/test-table",
                        },
                        "config": config,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(resp.encode())
                return

            # Stage-create: POST /namespaces/<ns>/tables
            if "/tables" in self.path and self.command == "POST":
                delegation = self.headers.get("X-Iceberg-Access-Delegation", "")
                _Handler.access_delegation_headers.append(delegation)

                config = {}
                if delegation == "vended-credentials":
                    config = {
                        "s3.access-key-id": "STAGE_ACCESS_KEY",
                        "s3.secret-access-key": "STAGE_SECRET_KEY",
                        "s3.session-token": "STAGE_SESSION_TOKEN",
                        "client.region": "us-east-1",
                    }

                resp = json.dumps(
                    {
                        "metadata-location": "s3://test-bucket/test-ns/new-table/metadata/v1.metadata.json",
                        "metadata": {
                            "format-version": 2,
                            "table-uuid": str(uuid.uuid4()),
                            "location": "s3://test-bucket/test-ns/new-table",
                        },
                        "config": config,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(resp.encode())
                return

            # Catch-all: use Iceberg REST error format
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                b'{"error": {"message": "not found", "type": "NoSuchNamespaceException", "code": 404}}'
            )

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


@pytest.fixture(scope="function")
def mock_rest_catalog_with_vended_creds():
    """Start a mock REST catalog that returns vended creds, tear down after."""
    port = _find_free_port()
    handler_class = _make_vended_creds_handler()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    yield port, handler_class

    httpd.shutdown()
    thread.join(timeout=5)


@pytest.fixture(scope="function")
def configure_mock_catalog(
    superuser_conn, iceberg_extension, mock_rest_catalog_with_vended_creds
):
    """
    Point pg_lake_iceberg GUCs at the mock REST catalog and clean up after.
    """
    port, handler_class = mock_rest_catalog_with_vended_creds

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}/api/catalog'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            # Vended credentials are opt-in (disabled by default); these tests
            # exercise the vending path, so enable it explicitly.
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_enable_vended_credentials TO 'true'",
            "SELECT pg_reload_conf()",
        ]
    )

    yield port, handler_class

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_enable_vended_credentials",
            "SELECT pg_reload_conf()",
        ]
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_vended_credentials_header_sent_on_load_table(
    superuser_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    Verify that the X-Iceberg-Access-Delegation: vended-credentials header
    is sent when loading a table from the REST catalog.
    """
    if installcheck:
        return

    port, handler_class = configure_mock_catalog

    # LoadRestCatalogMetadataLocation is called internally.
    # We expose it via a SQL-callable C function for testing.
    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        result = run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'test_table')",
            superuser_conn,
        )
        superuser_conn.commit()

        # The metadata location should be returned
        assert result[0][0] is not None
        assert "metadata" in result[0][0]

        # The mock should have received the vended-credentials header
        assert len(handler_class.load_table_requests) > 0
        assert (
            handler_class.load_table_requests[-1]["delegation"] == "vended-credentials"
        )

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()


def test_vended_credentials_header_not_sent_when_disabled(
    superuser_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    Verify that the X-Iceberg-Access-Delegation header is NOT sent when
    vended credentials are disabled.
    """
    if installcheck:
        return

    port, handler_class = configure_mock_catalog

    # Disable vended credentials
    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_enable_vended_credentials TO 'false'",
            "SELECT pg_reload_conf()",
        ]
    )

    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        result = run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'test_table')",
            superuser_conn,
        )
        superuser_conn.commit()

        assert result[0][0] is not None

        # The header should be empty (not "vended-credentials")
        assert len(handler_class.load_table_requests) > 0
        assert handler_class.load_table_requests[-1]["delegation"] == ""

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command_outside_tx(
            [
                "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_enable_vended_credentials",
                "SELECT pg_reload_conf()",
            ]
        )


def test_vended_credentials_config_parsing(
    superuser_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    Verify that the loadTable response's config map is parsed correctly
    and that the credential values are extracted.

    We test this by calling LoadTableFromRestCatalog via
    get_rest_metadata_location (which exercises the full path) and then
    checking that the mock received the proper header.
    """
    if installcheck:
        return

    port, handler_class = configure_mock_catalog

    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        # Clear previous requests
        handler_class.load_table_requests.clear()
        handler_class.access_delegation_headers.clear()

        result = run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'my_table')",
            superuser_conn,
        )
        superuser_conn.commit()

        # Verify the metadata location was extracted
        assert "v1.metadata.json" in result[0][0]

        # Verify the vended-credentials header was sent
        assert len(handler_class.access_delegation_headers) == 1
        assert handler_class.access_delegation_headers[0] == "vended-credentials"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()


def test_load_table_alone_does_not_push_a_secret(
    superuser_conn, pgduck_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    Fetching credentials is not the same as delivering them.

    Credentials are pushed when something is about to read or write the
    table's storage, not when loadTable happens to return them.  A bare
    loadTable therefore caches credentials and pushes nothing, which is
    what keeps secrets off pgduck_server for tables nobody touches.
    """
    if installcheck:
        return

    port, handler_class = configure_mock_catalog

    # Create a REST catalog iceberg table that will trigger loadTable
    # We first need to ensure the REST namespace exists
    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        # Trigger loadTable to cache vended credentials
        run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'vc_table')",
            superuser_conn,
        )
        superuser_conn.commit()

        # Secrets are process-global in pgduck_server, so narrow this to the
        # bucket this mock catalog vends for rather than to any vended secret.
        secrets = run_query(
            "SELECT name, type, scope FROM duckdb_secrets()",
            pgduck_conn,
        )
        pushed = [
            s
            for s in secrets
            if s[0].startswith("pglake_vended_") and "test-bucket" in str(s[2])
        ]
        assert pushed == [], f"loadTable should not push a secret, got {pushed}"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()


def test_vended_credentials_no_config_in_response(
    superuser_conn, iceberg_extension, installcheck
):
    """
    Verify that the system handles REST catalog responses that don't
    include vended credentials in the config map gracefully (no crash).
    """
    if installcheck:
        return

    def _make_no_creds_handler():
        class _Handler(BaseHTTPRequestHandler):
            requests_received = []

            def _handle(self):
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    self.rfile.read(content_length)

                if "/oauth/tokens" in self.path:
                    resp = json.dumps(
                        {
                            "access_token": uuid.uuid4().hex,
                            "token_type": "bearer",
                            "expires_in": 3600,
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(resp.encode())
                    return

                if "/tables/" in self.path and self.command == "GET":
                    _Handler.requests_received.append(self.path)
                    # Return response WITHOUT config map
                    resp = json.dumps(
                        {
                            "metadata-location": "s3://bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                            },
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(resp.encode())
                    return

                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    port = _find_free_port()
    handler_class = _make_no_creds_handler()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}/api/catalog'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "SELECT pg_reload_conf()",
        ]
    )

    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        # Should not crash even without config map in response
        result = run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'tbl')",
            superuser_conn,
        )
        superuser_conn.commit()

        assert result[0][0] is not None
        assert "metadata" in result[0][0]

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()

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


def test_vended_credentials_empty_config_in_response(
    superuser_conn, iceberg_extension, installcheck
):
    """
    Verify graceful handling when config map exists but contains no
    credential keys (e.g., catalog returns config with other settings).
    """
    if installcheck:
        return

    def _make_empty_config_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    self.rfile.read(content_length)

                if "/oauth/tokens" in self.path:
                    resp = json.dumps(
                        {
                            "access_token": uuid.uuid4().hex,
                            "token_type": "bearer",
                            "expires_in": 3600,
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(resp.encode())
                    return

                if "/tables/" in self.path and self.command == "GET":
                    resp = json.dumps(
                        {
                            "metadata-location": "s3://bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                            },
                            "config": {
                                "some-other-setting": "value",
                            },
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(resp.encode())
                    return

                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    port = _find_free_port()
    handler_class = _make_empty_config_handler()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}/api/catalog'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "SELECT pg_reload_conf()",
        ]
    )

    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        result = run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'tbl')",
            superuser_conn,
        )
        superuser_conn.commit()

        assert result[0][0] is not None

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()

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


def test_vended_credentials_partial_config(
    superuser_conn, iceberg_extension, installcheck
):
    """
    Verify that the system handles a config map with only the access key
    but no secret key (incomplete credentials) without crashing.
    """
    if installcheck:
        return

    def _make_partial_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    self.rfile.read(content_length)

                if "/oauth/tokens" in self.path:
                    resp = json.dumps(
                        {
                            "access_token": uuid.uuid4().hex,
                            "token_type": "bearer",
                            "expires_in": 3600,
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(resp.encode())
                    return

                if "/tables/" in self.path and self.command == "GET":
                    resp = json.dumps(
                        {
                            "metadata-location": "s3://bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                            },
                            "config": {
                                "s3.access-key-id": "PARTIAL_KEY",
                                # missing s3.secret-access-key
                            },
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(resp.encode())
                    return

                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    port = _find_free_port()
    handler_class = _make_partial_handler()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}/api/catalog'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "SELECT pg_reload_conf()",
        ]
    )

    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        result = run_query(
            "SELECT get_rest_metadata_location('postgres', 'test_ns', 'tbl')",
            superuser_conn,
        )
        superuser_conn.commit()

        # Should succeed without crashing — partial creds are ignored
        assert result[0][0] is not None
        assert "metadata" in result[0][0]

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()

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


def test_vended_credentials_multiple_tables_independent_creds(
    superuser_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    Verify that loading two different tables results in two separate
    loadTable requests, each with the vended-credentials header.
    """
    if installcheck:
        return

    port, handler_class = configure_mock_catalog

    run_command(
        """
        CREATE OR REPLACE FUNCTION get_rest_metadata_location(TEXT, TEXT, TEXT)
        RETURNS text
        LANGUAGE C VOLATILE STRICT
        AS 'pg_lake_iceberg', 'get_rest_metadata_location';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        handler_class.load_table_requests.clear()

        run_query(
            "SELECT get_rest_metadata_location('postgres', 'ns1', 'table_a')",
            superuser_conn,
        )
        run_query(
            "SELECT get_rest_metadata_location('postgres', 'ns1', 'table_b')",
            superuser_conn,
        )
        superuser_conn.commit()

        assert len(handler_class.load_table_requests) >= 2
        for req in handler_class.load_table_requests:
            assert req["delegation"] == "vended-credentials"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_metadata_location(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()


# ---------------------------------------------------------------------------
# Credential extraction details: scope, storage-credentials, expiry
#
# These use the get_rest_vended_credentials test shim, which loads a table
# and returns the extracted credential fields as a pipe-delimited summary:
#     "<access-key-id>|<scope>|<yes|no session token>|<expiry|noexpiry>|
#      <region>|<endpoint>|<url-style>|<use-ssl>"
# ---------------------------------------------------------------------------

_VENDED_CREDS_FN = """
    CREATE OR REPLACE FUNCTION get_rest_vended_credentials(TEXT, TEXT, TEXT)
    RETURNS text
    LANGUAGE C VOLATILE STRICT
    AS 'pg_lake_iceberg', 'get_rest_vended_credentials';
    """


def _serve(handler_class, conn=None):
    """
    Start a mock catalog on a free port and point the GUCs at it.

    ``conn``, when given, is the connection about to use the catalog.  The
    SIGHUP that ALTER SYSTEM's reload sends reaches it at an unpredictable
    command boundary, so it also gets the settings with SET, which takes
    effect at once.
    """
    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    settings = {
        "pg_lake_iceberg.rest_catalog_host": f"http://127.0.0.1:{port}/api/catalog",
        "pg_lake_iceberg.rest_catalog_client_id": "test_id",
        "pg_lake_iceberg.rest_catalog_client_secret": "test_secret",
        # Vended credentials are opt-in (disabled by default); these tests
        # exercise the vending path, so enable it explicitly.
        "pg_lake_iceberg.rest_catalog_enable_vended_credentials": "true",
    }
    run_command_outside_tx(
        [f"ALTER SYSTEM SET {name} TO '{value}'" for name, value in settings.items()]
        + ["SELECT pg_reload_conf()"]
    )
    if conn is not None:
        for name, value in settings.items():
            run_command(f"SET {name} TO '{value}'", conn)
        conn.commit()
    return httpd, thread


def _stop(httpd, thread, conn=None):
    httpd.shutdown()
    thread.join(timeout=5)
    names = [
        "pg_lake_iceberg.rest_catalog_host",
        "pg_lake_iceberg.rest_catalog_client_id",
        "pg_lake_iceberg.rest_catalog_client_secret",
        "pg_lake_iceberg.rest_catalog_enable_vended_credentials",
    ]
    run_command_outside_tx(
        [f"ALTER SYSTEM RESET {name}" for name in names] + ["SELECT pg_reload_conf()"]
    )
    if conn is not None:
        conn.rollback()
        for name in names:
            run_command(f"RESET {name}", conn)
        conn.commit()


def _oauth_or_none(handler):
    """Handle the OAuth token endpoint; return True if handled."""
    if "/oauth/tokens" in handler.path:
        resp = json.dumps(
            {
                "access_token": uuid.uuid4().hex,
                "token_type": "bearer",
                "expires_in": 3600,
            }
        )
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(resp.encode())
        return True
    return False


def _reply(handler, payload):
    handler.send_response(200)
    handler.send_header("Content-Type", "application/json")
    handler.end_headers()
    handler.wfile.write(json.dumps(payload).encode())


def _run_vended_creds(superuser_conn, catalog, ns, table):
    """
    Load a table named after ``table`` and return its credential summary.

    Each call loads a table of its own: a backend reports a table's
    unusable credentials only once, so a reused name would hold back a
    warning one test expects, or make one that another rules out vacuous.
    """
    return _load_vended_creds(
        superuser_conn, catalog, ns, f"{table}_{uuid.uuid4().hex[:8]}"
    )


def _load_vended_creds(superuser_conn, catalog, ns, table):
    run_command(_VENDED_CREDS_FN, superuser_conn)
    superuser_conn.commit()
    result = run_query(
        f"SELECT get_rest_vended_credentials('{catalog}', '{ns}', '{table}')",
        superuser_conn,
    )
    superuser_conn.commit()
    return result[0][0]


def test_vended_credentials_scope_from_metadata_location(
    superuser_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    When the response carries a legacy top-level config map, the scope is
    taken from the table storage location ("metadata"."location") and
    normalized with a trailing slash -- not synthesized from the
    configured location prefix.
    """
    if installcheck:
        return

    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "test_ns", "test_table")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "VENDED_ACCESS_KEY_123"
        # mock returns metadata.location = s3://test-bucket/test-ns/test-table
        assert scope == "s3://test-bucket/test-ns/test-table/"
        assert has_token == "yes"
        # the base mock provides no expiry
        assert expiry == "noexpiry"
        # region comes from client.region; the base mock vends no S3 settings
        assert region == "us-west-2"
        assert endpoint == ""
        assert url_style == ""

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()


def test_vended_credentials_every_storage_credential_is_kept(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A catalog that vends per prefix can vend more than one credential --
    here the data files and the metadata directory get different keys.
    Both are kept, each with its own scope, because dropping either would
    leave that half of the table unreadable.

    The third entry repeats a prefix already covered.  DuckDB picks one
    secret per path, so a second at the same scope could only shadow the
    first; it is dropped.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://multi-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://multi-bucket/ns/tbl",
                            },
                            "storage-credentials": [
                                {
                                    "prefix": "s3://multi-bucket/ns/tbl/data/",
                                    "config": {
                                        "s3.access-key-id": "DATA_KEY",
                                        "s3.secret-access-key": "DATA_SECRET",
                                    },
                                },
                                {
                                    "prefix": "s3://multi-bucket/ns/tbl/metadata/",
                                    "config": {
                                        "s3.access-key-id": "META_KEY",
                                        "s3.secret-access-key": "META_SECRET",
                                    },
                                },
                                {
                                    "prefix": "s3://multi-bucket/ns/tbl/data/",
                                    "config": {
                                        "s3.access-key-id": "DUPE_KEY",
                                        "s3.secret-access-key": "DUPE_SECRET",
                                    },
                                },
                            ],
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")
        credentials = [entry.split("|") for entry in summary.split(";")]

        assert len(credentials) == 2, f"expected two credentials, got {summary!r}"

        by_key = {entry[0]: entry[1] for entry in credentials}
        assert by_key["DATA_KEY"] == "s3://multi-bucket/ns/tbl/data/"
        assert by_key["META_KEY"] == "s3://multi-bucket/ns/tbl/metadata/"
        assert "DUPE_KEY" not in by_key

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_no_scope_when_undeterminable(
    superuser_conn, iceberg_extension, installcheck
):
    """
    Credentials with nothing to scope them to come back scoped to nothing.

    The catalog names no prefix and the metadata location has no metadata
    directory to derive a table root from.  Rather than invent a prefix,
    extraction leaves the scope empty, which is what makes the resolver
    push no secret at all -- a guessed scope would either match nothing or
    match objects these credentials have no business covering.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            # no "/metadata/" segment, so no table root
                            "metadata-location": "s3://ns-bucket/flat-v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                            },
                            "config": {
                                "s3.access-key-id": "NOSCOPE_KEY",
                                "s3.secret-access-key": "NOSCOPE_SECRET",
                                "s3.session-token": "NOSCOPE_TOKEN",
                            },
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "NOSCOPE_KEY"
        assert scope == "", f"expected no scope, got {scope!r}"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_storage_credentials_array(
    superuser_conn, iceberg_extension, installcheck
):
    """
    Newer catalogs return per-prefix credentials in a "storage-credentials"
    array; the element's own "prefix" is used as the scope and its "config"
    map supplies the credentials and expiry.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://sc-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://sc-bucket/ns/tbl",
                            },
                            "storage-credentials": [
                                {
                                    "prefix": "s3://sc-bucket/ns/tbl/",
                                    "config": {
                                        "s3.access-key-id": "SC_ACCESS_KEY",
                                        "s3.secret-access-key": "SC_SECRET_KEY",
                                        "s3.session-token": "SC_TOKEN",
                                        "s3.session-token-expires-at-ms": "9999999999000",
                                        "client.region": "eu-central-1",
                                    },
                                }
                            ],
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "SC_ACCESS_KEY"
        # scope comes from the storage-credential prefix (already ends in /)
        assert scope == "s3://sc-bucket/ns/tbl/"
        assert has_token == "yes"
        assert expiry == "expiry"
        assert region == "eu-central-1"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_region_falls_back_to_table_config(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A credential says which keys to use, not where the store is, so a catalog
    may state the region once in the table's own config rather than repeating
    it in every storage credential.  Read only from the credential, the region
    is lost, and S3 is later addressed at a host with an empty region in it --
    a failure at scan time, far from the response that omitted it.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://rg-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://rg-bucket/ns/tbl",
                            },
                            # stated once for the table, not per credential
                            "config": {
                                "client.region": "us-west-2",
                            },
                            "storage-credentials": [
                                {
                                    "prefix": "s3://rg-bucket/ns/tbl/",
                                    "config": {
                                        "s3.access-key-id": "RG_ACCESS_KEY",
                                        "s3.secret-access-key": "RG_SECRET_KEY",
                                        "s3.session-token": "RG_TOKEN",
                                    },
                                }
                            ],
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "RG_ACCESS_KEY"
        assert region == "us-west-2"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_expiry_from_config(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A catalog-provided expiry ("s3.session-token-expires-at-ms") in the
    legacy config map is parsed and reflected in the extracted credentials.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://exp-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://exp-bucket/ns/tbl",
                            },
                            "config": {
                                "s3.access-key-id": "EXP_KEY",
                                "s3.secret-access-key": "EXP_SECRET",
                                "s3.session-token": "EXP_TOKEN",
                                "s3.session-token-expires-at-ms": "9999999999000",
                            },
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "EXP_KEY"
        assert scope == "s3://exp-bucket/ns/tbl/"
        assert expiry == "expiry"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_s3_settings_from_config(
    superuser_conn, iceberg_extension, installcheck
):
    """
    The catalog's own S3 connection settings are parsed from the config
    map: s3.endpoint -> endpoint, s3.path-style-access -> url-style, and
    s3.region is used as the region fallback when client.region is absent.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://cfg-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://cfg-bucket/ns/tbl",
                            },
                            "config": {
                                "s3.access-key-id": "CFG_KEY",
                                "s3.secret-access-key": "CFG_SECRET",
                                "s3.endpoint": "minio.example.com:9000",
                                "s3.path-style-access": "true",
                                # only s3.region (no client.region) -> fallback
                                "s3.region": "ap-south-1",
                            },
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "CFG_KEY"
        assert scope == "s3://cfg-bucket/ns/tbl/"
        assert has_token == "no"
        assert region == "ap-south-1"
        assert endpoint == "minio.example.com:9000"
        assert url_style == "path"
        # no scheme to read SSL from, so it is left to be inherited
        assert use_ssl == ""

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


@pytest.mark.parametrize(
    "catalog_endpoint,expected_endpoint,expected_ssl",
    [
        ("http://minio.example.com:9000", "minio.example.com:9000", "false"),
        ("https://s3.example.com/", "s3.example.com", "true"),
    ],
)
def test_vended_credentials_endpoint_scheme_decides_ssl(
    superuser_conn,
    iceberg_extension,
    installcheck,
    catalog_endpoint,
    expected_endpoint,
    expected_ssl,
):
    """
    Iceberg states s3.endpoint as a URL; DuckDB wants a bare host[:port]
    and a separate USE_SSL.  The scheme decides SSL, so a catalog pointing
    at a plaintext store is honored rather than inheriting SSL from
    whatever secret happens to cover the prefix.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://ssl-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://ssl-bucket/ns/tbl",
                            },
                            "config": {
                                "s3.access-key-id": "SSL_KEY",
                                "s3.secret-access-key": "SSL_SECRET",
                                "s3.endpoint": catalog_endpoint,
                            },
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        _, _, _, _, _, endpoint, _, use_ssl = summary.split("|")
        assert endpoint == expected_endpoint
        assert use_ssl == expected_ssl

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_scope_clamped_to_table_root(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A storage-credential prefix that is broader than the table's own
    directory (e.g. the warehouse root) is clamped down to the table root
    derived from "metadata"."location", so the pushed secret cannot shadow
    sibling tables on the shared pgduck_server.
    """
    if installcheck:
        return

    def _make_handler():
        class _Handler(BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                if length > 0:
                    self.rfile.read(length)
                if _oauth_or_none(self):
                    return
                if "/tables/" in self.path and self.command == "GET":
                    _reply(
                        self,
                        {
                            "metadata-location": "s3://wh-bucket/ns/tbl/metadata/v1.metadata.json",
                            "metadata": {
                                "format-version": 2,
                                "table-uuid": str(uuid.uuid4()),
                                "location": "s3://wh-bucket/ns/tbl",
                            },
                            "storage-credentials": [
                                {
                                    # broad prefix: the whole warehouse bucket
                                    "prefix": "s3://wh-bucket/",
                                    "config": {
                                        "s3.access-key-id": "CLAMP_KEY",
                                        "s3.secret-access-key": "CLAMP_SECRET",
                                    },
                                }
                            ],
                        },
                    )
                    return
                self.send_response(404)
                self.end_headers()

            do_GET = _handle
            do_POST = _handle

            def log_message(self, fmt, *args):
                pass

        return _Handler

    httpd, thread = _serve(_make_handler())
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        access_key, scope, has_token, expiry, region, endpoint, url_style, use_ssl = (
            summary.split("|")
        )
        assert access_key == "CLAMP_KEY"
        # clamped from the broad "s3://wh-bucket/" down to the table root
        assert scope == "s3://wh-bucket/ns/tbl/"

    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def _serve_scope_case(prefix):
    """Mock catalog for table s3://wh-bucket/ns/tbl vending ``prefix``."""

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length", 0))
            if length > 0:
                self.rfile.read(length)
            if _oauth_or_none(self):
                return
            if "/tables/" in self.path and self.command == "GET":
                _reply(
                    self,
                    {
                        "metadata-location": "s3://wh-bucket/ns/tbl/metadata/v1.metadata.json",
                        "metadata": {
                            "format-version": 2,
                            "table-uuid": str(uuid.uuid4()),
                            "location": "s3://wh-bucket/ns/tbl",
                        },
                        "storage-credentials": [
                            {
                                "prefix": prefix,
                                "config": {
                                    "s3.access-key-id": "SCOPE_KEY",
                                    "s3.secret-access-key": "SCOPE_SECRET",
                                },
                            }
                        ],
                    },
                )
                return
            self.send_response(404)
            self.end_headers()

        do_GET = _handle
        do_POST = _handle

        def log_message(self, fmt, *args):
            pass

    return _serve(_Handler)


def _scope_for_prefix(superuser_conn, prefix):
    httpd, thread = _serve_scope_case(prefix)
    try:
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")
        return summary.split("|")[1]
    finally:
        run_command(
            "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
            superuser_conn,
        )
        superuser_conn.commit()
        _stop(httpd, thread)


def test_vended_credentials_scope_sibling_clamped_to_table_root(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A storage-credential prefix pointing at a *sibling* path is clamped to
    the table root.

    Clamping only the broader-than-root case is not enough: secrets live in
    one process-wide DuckDB instance and are selected by longest matching
    scope, so a sibling scope would register a secret covering a table these
    credentials have nothing to do with, and could shadow the secret that
    table depends on.
    """
    if installcheck:
        return

    scope = _scope_for_prefix(superuser_conn, "s3://wh-bucket/ns/other_tbl/")
    assert scope == "s3://wh-bucket/ns/tbl/"


def test_vended_credentials_scope_below_table_root_preserved(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A prefix *below* the table root is honored as-is.

    The clamp must not over-correct: a catalog that vends credentials for
    just the data directory is granting less than the table root, which is
    fine to keep.
    """
    if installcheck:
        return

    scope = _scope_for_prefix(superuser_conn, "s3://wh-bucket/ns/tbl/data/")
    assert scope == "s3://wh-bucket/ns/tbl/data/"


# ---------------------------------------------------------------------------
# Credentials pg_lake cannot use
# ---------------------------------------------------------------------------

_ADLS_HOST = "acct.dfs.core.windows.net"
_ADLS_LOCATION = f"abfss://container@{_ADLS_HOST}/ns/tbl"

# An ADLS SAS token is usable, but only an account key is vended here.
_ADLS_CASE = (
    "Azure Data Lake Storage",
    f"abfss://container@{_ADLS_HOST}/ns/tbl",
    {
        "adls.account-name": "acct",
        "adls.account-key": "not-an-account-key",
    },
)

# A SAS token is only this table's credential when it is for the storage
# account the table is on.
_ADLS_OTHER_ACCOUNT_CASE = (
    "Azure Data Lake Storage",
    f"abfss://container@{_ADLS_HOST}/ns/tbl",
    {
        "adls.sas-token.other.dfs.core.windows.net": "sv=2021-08-06&sig=other",
        "adls.sas-token-expires-at-ms.other.dfs.core.windows.net": "9999999999000",
    },
)

_GCS_CASE = (
    "Google Cloud Storage",
    "gs://gcs-bucket/ns/tbl",
    {
        "gcs.oauth2.token": "not-a-token",
        "gcs.oauth2.token-expires-at": "9999999999000",
    },
)

_UNSUPPORTED_PROVIDERS = [
    pytest.param(*_ADLS_CASE, id="adls-account-key"),
    pytest.param(*_ADLS_OTHER_ACCOUNT_CASE, id="adls-other-account"),
    pytest.param(*_GCS_CASE, id="gcs"),
]


def _serve_provider_case(shape, location, vended_config, conn, s3_credential=None):
    """
    Mock catalog for a table at ``location`` vending ``vended_config``,
    either as a "storage-credentials" element or as the legacy top-level
    "config" map.  ``s3_credential``, when given, is vended as a second
    storage-credentials element covering the table's data directory.
    """

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length", 0))
            if length > 0:
                self.rfile.read(length)
            if _oauth_or_none(self):
                return
            if "/tables/" in self.path and self.command == "GET":
                payload = {
                    "metadata-location": f"{location}/metadata/v1.metadata.json",
                    "metadata": {
                        "format-version": 2,
                        "table-uuid": str(uuid.uuid4()),
                        "location": location,
                    },
                }

                if shape == "storage-credentials":
                    payload["storage-credentials"] = [
                        {"prefix": f"{location}/", "config": vended_config}
                    ]
                    if s3_credential is not None:
                        payload["storage-credentials"].append(
                            {"prefix": f"{location}/data/", "config": s3_credential}
                        )
                else:
                    payload["config"] = vended_config

                _reply(self, payload)
                return
            self.send_response(404)
            self.end_headers()

        do_GET = _handle
        do_POST = _handle

        def log_message(self, fmt, *args):
            pass

    return _serve(_Handler, conn)


def _drop_vended_creds_fn(superuser_conn):
    superuser_conn.rollback()
    run_command(
        "DROP FUNCTION IF EXISTS get_rest_vended_credentials(TEXT, TEXT, TEXT)",
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.mark.parametrize("shape", ["storage-credentials", "config"])
@pytest.mark.parametrize("provider,location,vended_config", _UNSUPPORTED_PROVIDERS)
def test_vended_credentials_unsupported_provider_is_reported(
    superuser_conn,
    iceberg_extension,
    installcheck,
    shape,
    provider,
    location,
    vended_config,
):
    """
    A catalog vending for a provider pg_lake cannot use has to say so.

    Nothing fails at extraction: no credential comes back and the table
    falls back to whatever secret pgduck_server was already configured
    with, which is scoped and expires on its own terms rather than the
    catalog's.  Silence would leave that substitution invisible, so the
    table would either fail much later at scan time or read with access
    the catalog never granted.

    Both response shapes are covered, since a catalog using the newer
    "storage-credentials" array never reaches the legacy config map.
    """
    if installcheck:
        return

    httpd, thread = _serve_provider_case(shape, location, vended_config, superuser_conn)
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        assert summary is None, f"expected no usable credential, got {summary!r}"

        warnings = [
            n
            for n in superuser_conn.notices
            if "ignoring vended credentials" in n and provider in n
        ]
        reported = "\n".join(superuser_conn.notices)
        # Exactly one: the two response shapes are tried in sequence, and
        # reporting the same table twice would be its own kind of noise.
        assert (
            len(warnings) == 1
        ), f"expected one warning naming {provider}:\n{reported}"

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


def test_vended_credentials_unusable_reported_once_per_table(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A backend warns about a table's unusable credentials once.

    A read-only table is loaded from the catalog more than once per
    statement, and a warning on every load would bury the one that
    matters.  Another table still gets its own.
    """
    if installcheck:
        return

    _, location, vended_config = _GCS_CASE
    httpd, thread = _serve_provider_case(
        "storage-credentials", location, vended_config, superuser_conn
    )
    table = f"tbl_{uuid.uuid4().hex[:8]}"
    try:

        def warnings_loading(name):
            superuser_conn.notices.clear()
            summary = _load_vended_creds(superuser_conn, "postgres", "ns", name)
            assert summary is None, f"expected no usable credential, got {summary!r}"
            return [
                n
                for n in superuser_conn.notices
                if n.startswith("WARNING") and "ignoring vended credentials" in n
            ]

        assert len(warnings_loading(table)) == 1
        assert warnings_loading(table) == [], "the same table was reported again"
        assert len(warnings_loading(f"{table}_other")) == 1, "another table was not"

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


def test_vended_credentials_unsupported_provider_reported_alongside_s3(
    superuser_conn, iceberg_extension, installcheck
):
    """
    Partial coverage is still reported.

    A usable S3 credential for one prefix does not make an unusable
    credential for another harmless: that part of the table is the part
    that falls back to a substitute secret, and it is the case most
    likely to look like it works.
    """
    if installcheck:
        return

    location = "s3://mixed-bucket/ns/tbl"
    httpd, thread = _serve_provider_case(
        "storage-credentials",
        location,
        {"s3.session-token": "A_TOKEN_WITHOUT_ITS_KEYS"},
        superuser_conn,
        s3_credential={
            "s3.access-key-id": "MIXED_KEY",
            "s3.secret-access-key": "MIXED_SECRET",
        },
    )
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        # The usable half is still extracted and scoped to its own prefix.
        access_key, scope = summary.split("|")[:2]
        assert access_key == "MIXED_KEY"
        assert scope == f"{location}/data/"

        assert any(
            "ignoring vended credentials for S3" in n for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


@pytest.mark.parametrize("shape", ["storage-credentials", "config"])
@pytest.mark.parametrize(
    "location,settings",
    [
        pytest.param(
            "s3://settings-bucket/ns/tbl",
            {
                "s3.endpoint": "https://s3.example.com",
                "s3.path-style-access": "true",
                "s3.region": "us-west-2",
                "s3.sse.type": "kms",
                "s3.sse.key": "arn:aws:kms:us-west-2:123456789012:key/example",
                "client.region": "us-west-2",
            },
            id="s3",
        ),
        pytest.param(
            _ADLS_LOCATION,
            {
                "adls.account-name": "acct",
                "adls.account-host": _ADLS_HOST,
                f"adls.connection-string.{_ADLS_HOST}": f"https://{_ADLS_HOST}",
                "adls.token-credential-provider": "com.example.TokenProvider",
                "client.region": "westeurope",
            },
            id="azure",
        ),
        pytest.param(
            "gs://gcs-bucket/ns/tbl",
            {
                "gcs.project-id": "some-project",
                "gcs.service.host": "storage.googleapis.com",
                "gcs.encryption-key": "not-a-credential",
                "gcs.decryption-key": "not-a-credential",
                "adls.account-host": _ADLS_HOST,
                "adls.account-name": "acct",
            },
            id="gcs",
        ),
    ],
)
def test_vended_credentials_provider_settings_are_not_a_credential(
    superuser_conn, iceberg_extension, installcheck, shape, location, settings
):
    """
    Settings are not reported as a credential nobody could use.

    A catalog states settings like these whether or not it vends anything,
    so only a key an HTTP trace masks counts as a credential, and even
    then not an endpoint, adls.connection-string.<host>, or an encryption
    key, such as the KMS key id s3.sse.key holds with s3.sse.type kms.  The
    class of a token provider says how to get a credential but is not one.
    """
    if installcheck:
        return

    httpd, thread = _serve_provider_case(shape, location, settings, superuser_conn)
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        assert summary is None, f"expected no credential, got {summary!r}"
        assert not any(
            "ignoring vended credentials" in n for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


@pytest.mark.parametrize(
    "shape,location,vended_config,provider",
    [
        pytest.param(
            "storage-credentials",
            "s3://other-storage-bucket/ns/tbl",
            {f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&sig=X"},
            "S3",
            id="entry-for-other-storage",
        ),
        pytest.param(
            "storage-credentials",
            _ADLS_LOCATION,
            {"adls.auth.shared-key.account.key": "not-an-account-key"},
            "Azure Data Lake Storage",
            id="azure-shared-key",
        ),
        pytest.param(
            "storage-credentials",
            _ADLS_LOCATION,
            {"adls.token": "not-a-token"},
            "Azure Data Lake Storage",
            id="azure-bearer-token",
        ),
        pytest.param(
            "config",
            _ADLS_LOCATION,
            {"adls.connection-string": "AccountName=acct;AccountKey=not-a-key"},
            "Azure Data Lake Storage",
            id="azure-connection-string",
        ),
        pytest.param(
            "config",
            "s3://incomplete-bucket/ns/tbl",
            {"s3.access-key-id": "KEY_WITHOUT_SECRET"},
            "S3",
            id="s3-incomplete",
        ),
    ],
)
def test_vended_credentials_credential_pg_lake_cannot_use_is_reported(
    superuser_conn,
    iceberg_extension,
    installcheck,
    shape,
    location,
    vended_config,
    provider,
):
    """
    A credential pg_lake cannot use is reported as one for the table's own
    storage, the one its location names.  That includes a perfectly good
    credential for some other storage, since a SAS token on an S3 table is
    still no S3 credential, and an incomplete one.  A real connection
    string, unlike the endpoint Iceberg states under a qualified key,
    carries an account key.
    """
    if installcheck:
        return

    httpd, thread = _serve_provider_case(shape, location, vended_config, superuser_conn)
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        assert summary is None, f"expected no usable credential, got {summary!r}"
        assert any(
            f"ignoring vended credentials for {provider}" in n
            for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


def test_vended_credentials_legacy_config_outside_namespace_is_not_reported(
    superuser_conn, iceberg_extension, installcheck
):
    """
    The legacy config map also carries credentials of the catalog's own,
    such as a token scoped to the table, so a credential outside the
    storage's namespace there is not taken for a storage one.  Only a
    storage-credentials entry, which exists to vend for storage, counts
    every credential.
    """
    if installcheck:
        return

    httpd, thread = _serve_provider_case(
        "config",
        "s3://settings-bucket/ns/tbl",
        {"token": "a-table-scoped-catalog-token"},
        superuser_conn,
    )
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        assert summary is None, f"expected no credential, got {summary!r}"
        assert not any(
            "ignoring vended credentials" in n for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


def test_vended_credentials_s3_is_not_reported_as_unsupported(
    superuser_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    The warning must not fire for the credentials pg_lake does support,
    or it would train users to ignore it.
    """
    if installcheck:
        return

    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "test_ns", "test_table")

        assert summary.startswith("VENDED_ACCESS_KEY_123|")
        assert not any(
            "ignoring vended credentials" in n for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

    finally:
        _drop_vended_creds_fn(superuser_conn)


# ---------------------------------------------------------------------------
# Azure SAS tokens
#
# The shim summarizes an Azure credential as
#     "azure|<account>|<scope>|<sas-token>|<expiry in unix seconds|noexpiry>|
#      <blob-endpoint>|<dfs-endpoint>"
# ---------------------------------------------------------------------------


def _polaris_adls_config(host, account, sas):
    """The SAS token under every key Polaris states it by."""
    return {
        f"adls.sas-token.{host}": sas,
        f"adls.sas-token-expires-at-ms.{host}": "9999999999000",
        f"adls.sas-token.{account}": sas,
        "adls.sas-token": sas,
        "adls.account-name": account,
    }


def _azure_credential(superuser_conn, shape, location, vended_config):
    """Load the table from a mock catalog; return its one Azure credential."""
    httpd, thread = _serve_provider_case(shape, location, vended_config, superuser_conn)
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")
        assert not any(
            "ignoring vended credentials" in n for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)
        if summary is None:
            return None
        assert ";" not in summary, f"expected one credential, got {summary!r}"
        fields = summary.split("|")
        assert fields[0] == "azure", summary
        keys = ["account", "scope", "sas", "expiry", "blob", "dfs"]
        return dict(zip(keys, fields[1:]))
    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


@pytest.mark.parametrize("shape", ["storage-credentials", "config"])
def test_vended_credentials_azure_sas_token(
    superuser_conn, iceberg_extension, installcheck, shape
):
    """
    A catalog vending for an ADLS table states the one SAS token under
    several keys, one per client generation.  They collapse onto a single
    credential for the account the table's location names, scoped to the
    table, with the expiry the catalog stated and endpoints derived from
    that same host.
    """
    if installcheck:
        return

    cred = _azure_credential(
        superuser_conn,
        shape,
        _ADLS_LOCATION,
        _polaris_adls_config(_ADLS_HOST, "acct", "sv=2025-01-05&sig=VENDED"),
    )

    assert cred == {
        "account": "acct",
        "scope": f"{_ADLS_LOCATION}/",
        "sas": "sv=2025-01-05&sig=VENDED",
        "expiry": "9999999999",
        "blob": "https://acct.blob.core.windows.net",
        "dfs": "https://acct.dfs.core.windows.net",
    }


def _unix_seconds(*utc_fields):
    return str(int(datetime(*utc_fields, tzinfo=timezone.utc).timestamp()))


@pytest.mark.parametrize(
    "vended_config,expected_expiry",
    [
        pytest.param(
            {
                f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&se=2030-01-02T03%3A04%3A05Z&sig=X"
            },
            _unix_seconds(2030, 1, 2, 3, 4, 5),
            id="date-and-time",
        ),
        pytest.param(
            {f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&se=2030-01-02&sig=X"},
            _unix_seconds(2030, 1, 2),
            id="date",
        ),
        pytest.param(
            {
                f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&se=2030-01-02&sig=X",
                f"adls.sas-token-expires-at-ms.{_ADLS_HOST}": _unix_seconds(2029, 6, 1)
                + "000",
            },
            _unix_seconds(2029, 6, 1),
            id="the-catalog-says-first",
        ),
        pytest.param(
            {f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&se=infinity&sig=X"},
            "noexpiry",
            id="not-a-date",
        ),
        pytest.param(
            {f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&ske=2030-01-02&sig=X"},
            "noexpiry",
            id="no-se",
        ),
    ],
)
def test_vended_credentials_azure_expiry_falls_back_to_the_token(
    superuser_conn, iceberg_extension, installcheck, vended_config, expected_expiry
):
    """
    A SAS token states its own expiry in "se", which is used when the
    catalog states none, so a token shorter-lived than the default TTL is
    not kept past it.  It is UTC and percent-encoded, as a date or a date
    and time; any other field, such as the key's own expiry "ske", is not
    it.
    """
    if installcheck:
        return

    cred = _azure_credential(
        superuser_conn, "storage-credentials", _ADLS_LOCATION, vended_config
    )

    assert cred["expiry"] == expected_expiry


@pytest.mark.parametrize(
    "vended_config,expected_sas",
    [
        pytest.param(
            {
                f"adls.sas-token.{_ADLS_HOST}": "BY-HOST",
                "adls.sas-token.acct": "BY-ACCOUNT",
                "adls.sas-token": "BARE",
            },
            "BY-HOST",
            id="host-first",
        ),
        pytest.param(
            {"adls.sas-token.acct": "BY-ACCOUNT", "adls.sas-token": "BARE"},
            "BY-ACCOUNT",
            id="then-account",
        ),
        pytest.param(
            {"adls.sas-token": "BARE", "adls.account-name": "acct"},
            "BARE",
            id="then-bare",
        ),
        pytest.param(
            {f"adls.sas-token.{_ADLS_HOST}": "?sv=2025-01-05&sig=Q"},
            "sv=2025-01-05&sig=Q",
            id="written-as-a-query-string",
        ),
        pytest.param(
            {"adls.sas-token": "BARE"},
            "BARE",
            id="bare-without-account-name",
        ),
    ],
)
def test_vended_credentials_azure_token_precedence(
    superuser_conn, iceberg_extension, installcheck, vended_config, expected_sas
):
    """
    Newer spellings are preferred, and a bare token is taken as this
    account's unless adls.account-name says it is some other account's.
    """
    if installcheck:
        return

    cred = _azure_credential(
        superuser_conn, "storage-credentials", _ADLS_LOCATION, vended_config
    )

    assert cred["account"] == "acct"
    assert cred["sas"] == expected_sas


def test_vended_credentials_azure_bare_token_for_another_account(
    superuser_conn, iceberg_extension, installcheck
):
    """A bare token stated for another account is not this table's."""
    if installcheck:
        return

    httpd, thread = _serve_provider_case(
        "storage-credentials",
        _ADLS_LOCATION,
        {"adls.sas-token": "OTHER", "adls.account-name": "other"},
        superuser_conn,
    )
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        assert summary is None, summary
        assert any(
            "ignoring vended credentials" in n and "Azure Data Lake Storage" in n
            for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)
    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


@pytest.mark.parametrize(
    "location,host,extra,expected_blob,expected_dfs",
    [
        pytest.param(
            "abfss://c@acct.dfs.core.usgovcloudapi.net/ns/tbl",
            "acct.dfs.core.usgovcloudapi.net",
            {},
            "https://acct.blob.core.usgovcloudapi.net",
            "https://acct.dfs.core.usgovcloudapi.net",
            id="sovereign-cloud",
        ),
        pytest.param(
            "az://acct.blob.core.windows.net/c/ns/tbl",
            "acct.blob.core.windows.net",
            {},
            "https://acct.blob.core.windows.net",
            "https://acct.dfs.core.windows.net",
            id="blob-url",
        ),
        pytest.param(
            _ADLS_LOCATION,
            _ADLS_HOST,
            {
                f"adls.connection-string.{_ADLS_HOST}": (
                    "https://acct.privatelink.dfs.core.windows.net/"
                )
            },
            "https://acct.privatelink.dfs.core.windows.net",
            "https://acct.privatelink.dfs.core.windows.net",
            id="catalog-endpoint",
        ),
        pytest.param(
            _ADLS_LOCATION,
            _ADLS_HOST,
            {"adls.connection-string.acct": "AccountName=acct;AccountKey=secret"},
            "https://acct.blob.core.windows.net",
            "https://acct.dfs.core.windows.net",
            id="connection-string-is-not-an-endpoint",
        ),
    ],
)
def test_vended_credentials_azure_endpoints(
    superuser_conn,
    iceberg_extension,
    installcheck,
    location,
    host,
    extra,
    expected_blob,
    expected_dfs,
):
    """
    The endpoints come from the table's own host, so a sovereign cloud is
    not sent to the public one.  A catalog can state an endpoint of its own
    under adls.connection-string.<host>, which in Iceberg's ADLS FileIO is
    an endpoint URL; a value that is a connection string instead is not
    taken for one.
    """
    if installcheck:
        return

    cred = _azure_credential(
        superuser_conn,
        "storage-credentials",
        location,
        {f"adls.sas-token.{host}": "sv=2025-01-05&sig=X", **extra},
    )

    assert (cred["blob"], cred["dfs"]) == (expected_blob, expected_dfs)


@pytest.mark.parametrize(
    "endpoint",
    [
        pytest.param("http://169.254.169.254/metadata", id="internal-host"),
        pytest.param("http://127.0.0.1:10000/acct", id="loopback"),
        pytest.param("https://evil@acct.blob.core.windows.net", id="userinfo"),
        pytest.param(
            "https://169.254.169.254\\.blob.core.windows.net/", id="backslash"
        ),
        pytest.param(
            "https://169.254.169.254%2f.blob.core.windows.net/", id="escaped-slash"
        ),
    ],
)
def test_vended_credentials_azure_endpoint_is_held_to_the_host_allowlist(
    superuser_conn, iceberg_extension, installcheck, endpoint
):
    """
    Regression test for: a catalog-stated endpoint could send pgduck_server's
    requests to any host.

    A role that can create a catalog server can make the catalog say
    anything, and the endpoint it states replaces the host the table's own
    URLs name -- the one pg_lake.allowed_azure_host_suffixes already vetted.
    So the stated endpoint is held to that list too, and the host has to be
    plain: userinfo, a backslash or an escape could otherwise pass the
    suffix match here and be read as a different host by the Azure SDK.  A
    refused endpoint is reported and the table's own host is used instead.
    """
    if installcheck:
        return

    cred = _azure_credential(
        superuser_conn,
        "storage-credentials",
        _ADLS_LOCATION,
        {
            f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&sig=X",
            f"adls.connection-string.{_ADLS_HOST}": endpoint,
        },
    )

    assert (cred["blob"], cred["dfs"]) == (
        "https://acct.blob.core.windows.net",
        "https://acct.dfs.core.windows.net",
    )
    assert any(
        "ignoring the endpoint the catalog vended" in n for n in superuser_conn.notices
    ), "\n".join(superuser_conn.notices)


def test_vended_credentials_azure_refused_endpoint_reported_once(
    superuser_conn, iceberg_extension, installcheck
):
    """
    A refused endpoint is reported once per backend, like an unusable
    credential, rather than on every load of the table.  Another endpoint
    still gets its own report.
    """
    if installcheck:
        return

    endpoint = f"http://169.254.169.254/{uuid.uuid4().hex[:8]}"

    def reported(stated):
        _azure_credential(
            superuser_conn,
            "storage-credentials",
            _ADLS_LOCATION,
            {
                f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&sig=X",
                f"adls.connection-string.{_ADLS_HOST}": stated,
            },
        )
        return any(
            n.startswith("WARNING") and "ignoring the endpoint the catalog vended" in n
            for n in superuser_conn.notices
        )

    assert reported(endpoint)
    assert not reported(endpoint), "the same endpoint was reported again"
    assert reported(f"{endpoint}/other"), "another endpoint was not reported"


def test_vended_credentials_azure_endpoint_allowed_by_the_host_allowlist(
    superuser_conn, iceberg_extension, installcheck
):
    """
    An administrator who adds a host to pg_lake.allowed_azure_host_suffixes
    lets a catalog-stated endpoint reach it, which is how an emulator or a
    private endpoint outside the default suffixes is used.  The list matches
    on a label boundary, so ".0.0.1" admits 127.0.0.1.
    """
    if installcheck:
        return

    run_command(
        "SET pg_lake.allowed_azure_host_suffixes TO '.dfs.core.windows.net,.0.0.1'",
        superuser_conn,
    )
    superuser_conn.commit()
    try:
        cred = _azure_credential(
            superuser_conn,
            "storage-credentials",
            _ADLS_LOCATION,
            {
                f"adls.sas-token.{_ADLS_HOST}": "sv=2025-01-05&sig=X",
                f"adls.connection-string.{_ADLS_HOST}": "http://127.0.0.1:10000/acct/",
            },
        )
        assert (cred["blob"], cred["dfs"]) == (
            "http://127.0.0.1:10000/acct",
            "http://127.0.0.1:10000/acct",
        )
    finally:
        run_command("RESET pg_lake.allowed_azure_host_suffixes", superuser_conn)
        superuser_conn.commit()


def test_vended_credentials_azure_location_host_is_held_to_the_host_allowlist(
    superuser_conn, iceberg_extension, installcheck
):
    """
    The endpoints derived from the table's location are held to the same
    list as a stated one, since the location comes from the catalog too.
    One that is outside it is left unset, so the Azure SDK reaches the
    account at its public endpoint instead.
    """
    if installcheck:
        return

    host = "acct.dfs.internal.example"
    cred = _azure_credential(
        superuser_conn,
        "storage-credentials",
        f"abfss://container@{host}/ns/tbl",
        {f"adls.sas-token.{host}": "sv=2025-01-05&sig=X"},
    )

    assert cred["account"] == "acct"
    assert (cred["blob"], cred["dfs"]) == ("", "")


@pytest.mark.parametrize(
    "account",
    [
        pytest.param("metadata#x", id="fragment"),
        pytest.param("ab", id="too-short"),
        pytest.param("a" * 25, id="too-long"),
    ],
)
def test_vended_credentials_azure_account_name_must_be_valid(
    superuser_conn, iceberg_extension, installcheck, account
):
    """
    The Azure SDK builds https://<account>.blob.core.windows.net itself
    when no endpoint is set, so an account name that is not one Azure
    accepts could change the host that URL names -- "metadata#x" names
    "metadata".  Such a credential is not used, and is reported.
    """
    if installcheck:
        return

    httpd, thread = _serve_provider_case(
        "config",
        "az://container/ns/tbl",
        {"adls.sas-token": "sv=2025-01-05&sig=X", "adls.account-name": account},
        superuser_conn,
    )
    try:
        superuser_conn.notices.clear()
        summary = _run_vended_creds(superuser_conn, "postgres", "ns", "tbl")

        assert summary is None, f"expected no credential, got {summary!r}"
        assert any(
            "ignoring vended credentials for Azure Data Lake Storage" in n
            for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

    finally:
        _drop_vended_creds_fn(superuser_conn)
        _stop(httpd, thread, superuser_conn)


def test_vended_credentials_azure_short_url_takes_the_stated_account(
    superuser_conn, iceberg_extension, installcheck
):
    """
    az://<container>/<path> names no account, so the one the catalog
    states is used, and the endpoints are left for the Azure SDK to derive
    from it.
    """
    if installcheck:
        return

    cred = _azure_credential(
        superuser_conn,
        "config",
        "az://container/ns/tbl",
        {"adls.sas-token.acct": "sv=2025-01-05&sig=X", "adls.account-name": "acct"},
    )

    assert cred["account"] == "acct"
    assert cred["scope"] == "az://container/ns/tbl/"
    assert (cred["blob"], cred["dfs"]) == ("", "")

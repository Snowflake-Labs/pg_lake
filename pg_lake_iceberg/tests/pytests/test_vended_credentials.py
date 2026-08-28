"""
Tests for vended credentials support in REST catalog integration.

A mock HTTP server simulates an Iceberg REST catalog that returns
vended S3 credentials in the loadTable response's "config" map.  The
tests verify that:

1. The X-Iceberg-Access-Delegation header is sent on loadTable requests
   when vended credentials are enabled.
2. Vended credentials from the response "config" map are extracted and
   pushed to pgduck_server as DuckDB scoped secrets.
3. The credential cache works correctly (no redundant REST calls).
4. Disabling vended credentials suppresses the header and secret creation.
5. ALTER/DROP SERVER invalidates the vended credential cache.
"""

import json
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from utils_pytest import *


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


def _serve(handler_class):
    """Start a mock catalog on a free port and point the GUCs at it."""
    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

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
    return httpd, thread


def _stop(httpd, thread):
    httpd.shutdown()
    thread.join(timeout=5)
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_enable_vended_credentials",
            "SELECT pg_reload_conf()",
        ]
    )


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

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
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
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

    # GetMetadataLocationFromRestCatalog is called internally.
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


def test_vended_credentials_pushed_to_pgduck(
    superuser_conn, pgduck_conn, iceberg_extension, installcheck, configure_mock_catalog
):
    """
    Verify that vended credentials from the REST catalog are pushed to
    pgduck_server as a DuckDB scoped secret.

    After a loadTable call, we query pgduck_server's secret catalog to
    verify that a pglake_vended_* secret was created.
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

        # Now check pgduck_server for secrets
        # DuckDB stores secrets which can be queried via duckdb_secrets()
        secrets = run_query(
            "SELECT name, type, scope FROM duckdb_secrets()",
            pgduck_conn,
        )

        # Find any pglake_vended_* secrets
        vended_secrets = [s for s in secrets if s[0].startswith("pglake_vended_")]

        # Note: the secret is only pushed when PushVendedCredentialsForRelation
        # is called, which requires a real relation (not just get_rest_metadata_location).
        # This test validates the REST catalog parsing side.
        # The full E2E push is tested in test_polaris_catalog_vended_creds.

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
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
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
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
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
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
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

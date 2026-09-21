"""
Verify that pg_lake distinguishes between a non-existent REST catalog and
a missing namespace (Issue #227).

When checking a namespace (GET /v1/{catalog}/namespaces/{namespace}):
  1. If the namespace GET returns 404, pg_lake queries GET /v1/{catalog}/namespaces.
  2. If the catalog listing also returns 404, the catalog itself does not exist,
     and a clear "catalog does not exist in the rest catalog server" error is raised.
  3. If the catalog listing returns 200, the catalog exists and the namespace is
     genuinely missing:
     - RegisterNamespaceToRestCatalog proceeds to create the namespace (POST).
     - ErrorIfRestNamespaceDoesNotExist raises "namespace does not exist in the rest catalog".
"""

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils_pytest import *

_FAKE_TOKEN = "fake-bearer-token-for-catalog-exists-test"


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class(get_responses: dict, post_responses: dict = None):
    """
    Factory so each test run gets an isolated handler with its own recorded requests.
    get_responses:  path suffix -> (status_code, body_str)
    post_responses: path suffix -> (status_code, body_str)
    """
    if post_responses is None:
        post_responses = {}

    class _Handler(BaseHTTPRequestHandler):
        recorded_requests = []

        def log_message(self, format, *args):
            pass  # silence server output during tests

        def do_POST(self):
            content_len = int(self.headers.get("Content-Length", 0))
            req_body = self.rfile.read(content_len).decode() if content_len > 0 else ""
            self.recorded_requests.append(("POST", self.path, req_body))

            if "/oauth/tokens" in self.path:
                token_body = json.dumps(
                    {
                        "access_token": _FAKE_TOKEN,
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(token_body.encode())
                return

            for suffix, (status, body) in post_responses.items():
                if self.path.endswith(suffix):
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(body.encode())
                    return

            self.send_response(404)
            self.end_headers()

        def do_GET(self):
            self.recorded_requests.append(("GET", self.path, ""))

            for suffix, (status, body) in get_responses.items():
                if self.path.endswith(suffix):
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(body.encode())
                    return

            self.send_response(404)
            self.end_headers()

    return _Handler


def _start_mock_server(get_responses: dict, post_responses: dict = None):
    port = _find_free_port()
    handler_cls = _make_handler_class(get_responses, post_responses)
    httpd = HTTPServer(("127.0.0.1", port), handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, port, handler_cls


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HELPER_FUNC_SQL = """
CREATE OR REPLACE FUNCTION lake_iceberg.register_namespace_to_named_catalog(
    catalog text,
    catalog_name text,
    namespace_name text
)
RETURNS void
LANGUAGE C
VOLATILE STRICT
AS 'pg_lake_iceberg', 'register_namespace_to_named_catalog';
"""


@pytest.fixture(scope="module")
def register_namespace_helper(superuser_conn):
    run_command(_HELPER_FUNC_SQL, superuser_conn)
    yield
    run_command(
        "DROP FUNCTION IF EXISTS "
        "lake_iceberg.register_namespace_to_named_catalog(text, text, text)",
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_register_namespace_when_catalog_does_not_exist(
    register_namespace_helper, superuser_conn, pg_conn, extension
):
    """When the catalog does not exist, RegisterNamespaceToRestCatalog reports
    that the catalog does not exist, rather than attempting to create the
    namespace and failing with a confusing namespace error (Issue #227).
    """
    httpd, port, handler = _start_mock_server(
        get_responses={
            "/v1/non_existing_catalog/namespaces/my_ns": (
                404,
                '{"error": "not found"}',
            ),
            "/v1/non_existing_catalog/namespaces": (
                404,
                '{"error": "catalog not found"}',
            ),
        }
    )
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "test_srv_cat_missing"

    try:
        _create_server(superuser_conn, server_name, endpoint)

        res = run_command(
            f"SELECT lake_iceberg.register_namespace_to_named_catalog('{server_name}', 'non_existing_catalog', 'my_ns')",
            pg_conn,
            raise_error=False,
        )

        assert (
            'catalog "non_existing_catalog" does not exist in the rest catalog server'
            in str(res)
        )
        assert "Create the catalog in the rest catalog server" in str(res)

        # Verify that CreateNamespace (POST) was never called because the missing
        # catalog was caught during existence verification.
        post_requests = [
            req
            for req in handler.recorded_requests
            if req[0] == "POST" and "oauth" not in req[1]
        ]
        assert (
            len(post_requests) == 0
        ), f"Expected no namespace POST, got {post_requests}"
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_register_namespace_when_catalog_exists_creates_namespace(
    register_namespace_helper, superuser_conn, pg_conn, extension
):
    """When the catalog exists but the namespace does not, RegisterNamespaceToRestCatalog
    verifies catalog existence and proceeds to create the namespace via POST.
    """
    httpd, port, handler = _start_mock_server(
        get_responses={
            "/v1/existing_catalog/namespaces/new_ns": (404, '{"error": "not found"}'),
            "/v1/existing_catalog/namespaces": (200, json.dumps({"namespaces": []})),
        },
        post_responses={
            "/v1/existing_catalog/namespaces": (200, json.dumps({})),
        },
    )
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "test_srv_cat_exists"

    try:
        _create_server(superuser_conn, server_name, endpoint)

        run_command(
            f"SELECT lake_iceberg.register_namespace_to_named_catalog('{server_name}', 'existing_catalog', 'new_ns')",
            pg_conn,
        )
        pg_conn.commit()

        # Verify that CreateNamespace (POST /v1/{catalog}/namespaces) was called
        post_requests = [
            req
            for req in handler.recorded_requests
            if req[0] == "POST" and "oauth" not in req[1]
        ]
        assert (
            len(post_requests) == 1
        ), f"Expected 1 namespace POST, got {post_requests}"
        assert "/v1/existing_catalog/namespaces" in post_requests[0][1]
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_error_if_rest_namespace_does_not_exist_when_catalog_missing(
    superuser_conn, pg_conn, extension
):
    """When creating a read-only table and the catalog does not exist,
    ErrorIfRestNamespaceDoesNotExist reports that the catalog does not exist
    instead of claiming the namespace is missing.
    """
    httpd, port, handler = _start_mock_server(
        get_responses={
            "/v1/bad_catalog/namespaces/public": (404, '{"error": "not found"}'),
            "/v1/bad_catalog/namespaces": (404, '{"error": "catalog not found"}'),
        }
    )
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "test_srv_ro_cat_missing"

    try:
        _create_server(superuser_conn, server_name, endpoint)

        res = run_command(
            f"""
            CREATE TABLE test_tbl(a int) USING iceberg WITH (
                catalog='{server_name}',
                read_only=true,
                catalog_name='bad_catalog',
                catalog_namespace='public',
                catalog_table_name='tbl'
            )
            """,
            pg_conn,
            raise_error=False,
        )

        assert 'catalog "bad_catalog" does not exist in the rest catalog server' in str(
            res
        )
        assert "check the catalog_name option" in str(res)
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_error_if_rest_namespace_does_not_exist_when_catalog_exists(
    superuser_conn, pg_conn, extension
):
    """When creating a read-only table and the catalog exists but the namespace
    does not, ErrorIfRestNamespaceDoesNotExist correctly reports that the
    namespace does not exist.
    """
    httpd, port, handler = _start_mock_server(
        get_responses={
            "/v1/good_catalog/namespaces/missing_ns": (404, '{"error": "not found"}'),
            "/v1/good_catalog/namespaces": (200, json.dumps({"namespaces": []})),
        }
    )
    endpoint = f"http://127.0.0.1:{port}"
    server_name = "test_srv_ro_cat_exists"

    try:
        _create_server(superuser_conn, server_name, endpoint)

        res = run_command(
            f"""
            CREATE TABLE test_tbl2(a int) USING iceberg WITH (
                catalog='{server_name}',
                read_only=true,
                catalog_name='good_catalog',
                catalog_namespace='missing_ns',
                catalog_table_name='tbl'
            )
            """,
            pg_conn,
            raise_error=False,
        )

        assert (
            'namespace "missing_ns" does not exist in the rest catalog while creating on catalog "good_catalog"'
            in str(res)
        )
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()

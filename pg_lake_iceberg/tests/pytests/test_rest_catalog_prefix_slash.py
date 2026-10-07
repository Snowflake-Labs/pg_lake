"""
Verify that a REST catalog prefix containing '/' is sent with literal slashes
(Issue #712).

Iceberg REST prefixes can span several path segments; Unity Catalog uses
"catalogs/<name>".  The reference client joins the prefix into the path as-is,
so pg_lake must request /v1/catalogs/unity/namespaces/... rather than
/v1/catalogs%2funity/namespaces/...  Other reserved characters inside a
segment are still percent-encoded.
"""

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils_pytest import *

_FAKE_TOKEN = "fake-bearer-token-for-prefix-slash-test"

_NOT_FOUND_BODY = json.dumps(
    {"error": {"message": "Not found", "type": "NotFoundException", "code": 404}}
)


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class(get_responses: dict):
    """
    get_responses: exact request path -> (status_code, body_str)
    Unmatched paths return 404, so an encoded '/' never matches.
    """

    class _Handler(BaseHTTPRequestHandler):
        recorded_requests = []

        def log_message(self, format, *args):
            pass

        def _reply(self, status, body):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body.encode())

        def do_POST(self):
            content_len = int(self.headers.get("Content-Length", 0))
            if content_len > 0:
                self.rfile.read(content_len)
            self.recorded_requests.append(("POST", self.path))

            if self.path.endswith("/oauth/tokens"):
                self._reply(
                    200,
                    json.dumps(
                        {
                            "access_token": _FAKE_TOKEN,
                            "token_type": "bearer",
                            "expires_in": 3600,
                        }
                    ),
                )
                return

            self._reply(404, _NOT_FOUND_BODY)

        def do_GET(self):
            self.recorded_requests.append(("GET", self.path))

            if self.path in get_responses:
                status, body = get_responses[self.path]
                self._reply(status, body)
                return

            self._reply(404, _NOT_FOUND_BODY)

    return _Handler


def _start_mock_server(get_responses: dict):
    port = _find_free_port()
    handler_cls = _make_handler_class(get_responses)
    httpd = HTTPServer(("127.0.0.1", port), handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, port, handler_cls


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
    run_command(f"GRANT USAGE ON FOREIGN SERVER {server_name} TO PUBLIC", conn)
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


def _catalog_paths(handler):
    return [path for method, path in handler.recorded_requests if "oauth" not in path]


def test_prefix_with_slash_is_not_encoded(superuser_conn, pg_conn, extension):
    """A multi-segment prefix reaches the namespace and table endpoints with
    literal slashes.  The table load returns 404, so CREATE TABLE fails, but
    only after requesting the correct paths.
    """
    httpd, port, handler = _start_mock_server(
        get_responses={
            "/v1/catalogs/unity/namespaces/iot": (
                200,
                json.dumps({"namespace": ["iot"], "properties": {}}),
            ),
        }
    )
    server_name = "test_srv_prefix_slash"

    try:
        _create_server(superuser_conn, server_name, f"http://127.0.0.1:{port}")

        run_command(
            f"""
            CREATE TABLE prefix_slash_tbl(a int) USING iceberg WITH (
                catalog='{server_name}',
                read_only=true,
                catalog_name='catalogs/unity',
                catalog_namespace='iot',
                catalog_table_name='sensor_readings'
            )
            """,
            pg_conn,
            raise_error=False,
        )

        paths = _catalog_paths(handler)
        assert "/v1/catalogs/unity/namespaces/iot" in paths, paths
        assert (
            "/v1/catalogs/unity/namespaces/iot/tables/sensor_readings" in paths
        ), paths
        assert not any("%2f" in p.lower() for p in paths), paths
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_prefix_with_slash_catalog_exists_check(superuser_conn, pg_conn, extension):
    """On a namespace 404, the catalog-existence check also keeps the slash, and
    the error names the prefix as configured.
    """
    httpd, port, handler = _start_mock_server(get_responses={})
    server_name = "test_srv_prefix_slash_missing"

    try:
        _create_server(superuser_conn, server_name, f"http://127.0.0.1:{port}")

        res = run_command(
            f"""
            CREATE TABLE prefix_slash_missing_tbl(a int) USING iceberg WITH (
                catalog='{server_name}',
                read_only=true,
                catalog_name='catalogs/unity',
                catalog_namespace='iot',
                catalog_table_name='sensor_readings'
            )
            """,
            pg_conn,
            raise_error=False,
        )

        paths = _catalog_paths(handler)
        assert "/v1/catalogs/unity/namespaces" in paths, paths
        assert not any("%2f" in p.lower() for p in paths), paths
        assert (
            'catalog "catalogs/unity" does not exist in the rest catalog server'
            in str(res)
        )
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()


def test_prefix_segments_still_encode_reserved_chars(
    superuser_conn, pg_conn, extension
):
    """Only '/' is kept; other reserved characters inside a prefix segment are
    still percent-encoded.
    """
    httpd, port, handler = _start_mock_server(get_responses={})
    server_name = "test_srv_prefix_reserved"

    try:
        _create_server(superuser_conn, server_name, f"http://127.0.0.1:{port}")

        run_command(
            f"""
            CREATE TABLE prefix_reserved_tbl(a int) USING iceberg WITH (
                catalog='{server_name}',
                read_only=true,
                catalog_name='catalogs/my cat',
                catalog_namespace='iot',
                catalog_table_name='sensor_readings'
            )
            """,
            pg_conn,
            raise_error=False,
        )

        paths = _catalog_paths(handler)
        assert "/v1/catalogs/my%20cat/namespaces/iot" in paths, paths
    finally:
        pg_conn.rollback()
        _drop_server(superuser_conn, server_name)
        httpd.shutdown()

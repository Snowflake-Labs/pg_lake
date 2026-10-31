"""
Tests for the "pg_lake_catalog: " lines pg_lake writes to the server log about
REST catalog activity: failed requests and authentication failures.

The lines are meant to be collectable where the rest of the server log is not,
so besides checking what they report, every test checks that nothing the user
or the catalog controls -- catalog and namespace names, the endpoint, the
catalog's error message -- made it into one.

A mock HTTP server stands in for the catalog, as in the credential provider
tests.  The lines go to the server log only, so they are read back from the
log file of the test server.
"""

import json
import os
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2
from utils_pytest import *


CATALOG_NAME = "logcat"
REMOTE_MESSAGE = "remote-detail-that-must-not-be-logged"

_HOOK_FNS = """
CREATE OR REPLACE FUNCTION set_test_rest_catalog_auth_provider(BOOL)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'set_test_rest_catalog_auth_provider';

CREATE OR REPLACE FUNCTION set_test_rest_catalog_auth_response(TEXT, INT, BOOL)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'set_test_rest_catalog_auth_response';

CREATE OR REPLACE FUNCTION register_namespace_to_rest_catalog(TEXT,TEXT)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'register_namespace_to_rest_catalog';
"""


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class():
    class _Handler(BaseHTTPRequestHandler):
        token_status = 200
        token_body = None

        # answer for GET .../namespaces/<name>
        namespace_status = 200
        namespace_body = None

        def _reply(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

        def _handle(self):
            if "/oauth/tokens" in self.path:
                body = _Handler.token_body or {
                    "access_token": uuid.uuid4().hex,
                    "token_type": "bearer",
                    "expires_in": 3600,
                }
                self._reply(_Handler.token_status, body)
            elif self.path.rstrip("/").endswith("/namespaces"):
                if self.command == "GET":
                    self._reply(200, {"namespaces": []})
                else:
                    self._reply(200, {"namespace": ["created"], "properties": {}})
            else:
                body = _Handler.namespace_body or {
                    "namespace": ["existing"],
                    "properties": {},
                }
                self._reply(_Handler.namespace_status, body)

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


def _set_catalog_gucs(host, extra=()):
    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{host}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            *extra,
            "SELECT pg_reload_conf()",
        ]
    )


def _reset_catalog_gucs():
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id",
            "ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret",
            "SELECT pg_reload_conf()",
        ]
    )


@pytest.fixture(scope="function")
def catalog_and_conn(postgres, iceberg_extension):
    """Point the built-in catalog at a mock and hand back a fresh connection."""
    port = _find_free_port()
    handler_class = _make_handler_class()
    handler_class.port = port
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    host = f"http://127.0.0.1:{port}"
    _set_catalog_gucs(host)

    conn = open_pg_conn()
    wait_for_reloaded_settings([conn], {"pg_lake_iceberg.rest_catalog_host": host})
    run_command(_HOOK_FNS, conn)
    conn.commit()

    yield handler_class, conn

    conn.close()

    httpd.shutdown()
    thread.join(timeout=5)

    _reset_catalog_gucs()


def _logfile():
    return f"{server_params.PG_DIR}/logfile"


def _log_offset():
    return os.path.getsize(_logfile())


def _catalog_lines(offset):
    with open(_logfile()) as f:
        f.seek(offset)
        delta = f.read()

    return [line for line in delta.splitlines() if "pg_lake_catalog: " in line]


def _parse(line):
    words = line.split("pg_lake_catalog: ", 1)[1].split()
    return words[0], dict(word.split("=", 1) for word in words[1:])


def _events(offset, event):
    return [
        fields
        for name, fields in (_parse(line) for line in _catalog_lines(offset))
        if name == event
    ]


def _assert_nothing_identifying(offset, *secrets):
    lines = _catalog_lines(offset)
    for line in lines:
        logged = line.split("pg_lake_catalog: ", 1)[1]
        for secret in secrets:
            assert secret not in logged, f"{secret!r} leaked into {line!r}"


def _register_namespace(conn, namespace):
    run_command(
        f"SELECT register_namespace_to_rest_catalog('{CATALOG_NAME}', '{namespace}')",
        conn,
    )
    conn.commit()


def test_failed_request_is_logged_by_class(installcheck, catalog_and_conn):
    """
    A catalog error is logged with its status and error type, and without the
    message the catalog sent, which can say anything.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_status = 403
    handler_class.namespace_body = {
        "error": {
            "message": REMOTE_MESSAGE,
            "type": "ForbiddenException",
            "code": 403,
        }
    }
    namespace = f"ns_{uuid.uuid4().hex[:8]}"

    offset = _log_offset()

    with pytest.raises(Exception, match="HTTP 403"):
        _register_namespace(conn, namespace)
    conn.rollback()

    failures = _events(offset, "request_failed")
    assert len(failures) == 1, _catalog_lines(offset)
    assert failures[0]["catalog"] == "rest_builtin"
    assert failures[0]["op"] == "get_namespace"
    assert failures[0]["status"] == "403"
    assert failures[0]["error_type"] == "ForbiddenException"
    assert failures[0]["attempts"] == "1"
    assert failures[0]["auth_refreshed"] == "false"

    _assert_nothing_identifying(
        offset,
        REMOTE_MESSAGE,
        namespace,
        CATALOG_NAME,
        "127.0.0.1",
        str(handler_class.port),
    )


def test_unknown_error_type_is_not_logged_verbatim(installcheck, catalog_and_conn):
    """An error type outside the known vocabulary is reported as "other"."""
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_status = 500
    handler_class.namespace_body = {
        "error": {"message": "boom", "type": REMOTE_MESSAGE, "code": 500}
    }

    offset = _log_offset()

    with pytest.raises(Exception, match="HTTP 500"):
        _register_namespace(conn, f"ns_{uuid.uuid4().hex[:8]}")
    conn.rollback()

    failures = _events(offset, "request_failed")
    assert [f["error_type"] for f in failures] == ["other"]
    _assert_nothing_identifying(offset, REMOTE_MESSAGE)


def test_not_found_is_not_logged_as_failure(installcheck, catalog_and_conn):
    """
    Probing for a namespace that does not exist yet is how a namespace gets
    created, so its 404 is not logged as a failure.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_status = 404
    handler_class.namespace_body = {
        "error": {
            "message": REMOTE_MESSAGE,
            "type": "NoSuchNamespaceException",
            "code": 404,
        }
    }
    namespace = f"ns_{uuid.uuid4().hex[:8]}"

    offset = _log_offset()

    _register_namespace(conn, namespace)

    assert _catalog_lines(offset) == []


def test_oauth2_failure_is_logged_as_auth_failed(installcheck, catalog_and_conn):
    """
    A rejected token request is logged both as the failed request it is and
    as the authentication failure it causes, with the OAuth2 error code but
    not the description the endpoint sent with it.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.token_status = 401
    handler_class.token_body = {
        "error": "invalid_client",
        "error_description": REMOTE_MESSAGE,
    }

    offset = _log_offset()

    with pytest.raises(Exception, match="token request failed"):
        _register_namespace(conn, "ns_auth")
    conn.rollback()

    failures = _events(offset, "request_failed")
    assert len(failures) == 1, _catalog_lines(offset)
    assert failures[0]["op"] == "token"
    assert failures[0]["status"] == "401"
    assert failures[0]["error_type"] == "invalid_client"

    auth_failures = _events(offset, "auth_failed")
    assert len(auth_failures) == 1, _catalog_lines(offset)
    assert auth_failures[0]["catalog"] == "rest_builtin"
    assert auth_failures[0]["source"] == "oauth2"
    assert len(auth_failures[0]["sqlstate"]) == 5

    _assert_nothing_identifying(offset, REMOTE_MESSAGE, "test_secret", "test_id")


def test_provider_failure_is_logged_as_auth_failed(installcheck, catalog_and_conn):
    """A credential provider that fails is reported as the source."""
    if installcheck:
        return

    handler_class, conn = catalog_and_conn

    run_command("SELECT set_test_rest_catalog_auth_response('', 3600, true)", conn)
    run_command("SELECT set_test_rest_catalog_auth_provider(true)", conn)
    conn.commit()

    offset = _log_offset()

    with pytest.raises(Exception, match="empty authorization"):
        _register_namespace(conn, "ns_provider")
    conn.rollback()

    auth_failures = _events(offset, "auth_failed")
    assert len(auth_failures) == 1, _catalog_lines(offset)
    assert auth_failures[0]["source"] == "provider"
    assert auth_failures[0]["sqlstate"] == "XX000"


def test_unreachable_catalog_is_logged_by_curl_class(
    installcheck, postgres, iceberg_extension
):
    """
    A request that never gets a response is logged with the class of the
    libcurl error rather than libcurl's message, which names the host.
    """
    if installcheck:
        return

    port = _find_free_port()
    host = f"http://127.0.0.1:{port}"
    _set_catalog_gucs(host)

    conn = open_pg_conn()
    try:
        wait_for_reloaded_settings([conn], {"pg_lake_iceberg.rest_catalog_host": host})
        run_command(_HOOK_FNS, conn)
        conn.commit()

        offset = _log_offset()

        with pytest.raises(Exception):
            _register_namespace(conn, "ns_unreachable")
        conn.rollback()

        failures = _events(offset, "request_failed")
        assert len(failures) == 1, _catalog_lines(offset)
        assert failures[0]["op"] == "token"
        assert failures[0]["status"] == "0"
        assert failures[0]["curl_error"] == "connect"

        _assert_nothing_identifying(offset, "127.0.0.1", str(port))
    finally:
        conn.close()
        _reset_catalog_gucs()


def test_catalog_events_can_be_disabled(installcheck, catalog_and_conn):
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_status = 403

    run_command("SET pg_lake_iceberg.log_catalog_events TO off", conn)
    conn.commit()

    offset = _log_offset()

    with pytest.raises(Exception, match="HTTP 403"):
        _register_namespace(conn, "ns_disabled")
    conn.rollback()

    assert _catalog_lines(offset) == []


def test_catalog_events_setting_is_superuser_only(postgres, iceberg_extension):
    conn = open_pg_conn()
    try:
        run_command("CREATE ROLE catalog_log_unprivileged LOGIN PASSWORD 'pw'", conn)
        conn.commit()
        run_command("SET ROLE catalog_log_unprivileged", conn)

        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            run_command("SET pg_lake_iceberg.log_catalog_events TO off", conn)
        conn.rollback()
    finally:
        conn.rollback()
        run_command("RESET ROLE", conn)
        run_command("DROP ROLE IF EXISTS catalog_log_unprivileged", conn)
        conn.commit()
        conn.close()

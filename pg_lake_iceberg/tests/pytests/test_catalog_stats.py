"""
Tests for lake_iceberg.catalog_stats(), the server-wide counters of REST
catalog requests, retries, authentication failures and vended credentials.

A mock HTTP server stands in for the catalog, as in the catalog log tests.
The counters cover the whole server since it started, so every test compares
them before and after, and reads them from a connection of its own, which
also checks that they are shared between backends.
"""

import json
import socket
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2
from utils_pytest import *


CATALOG_NAME = "statscat"

_TEST_FNS = """
CREATE OR REPLACE FUNCTION set_test_rest_catalog_auth_provider(BOOL)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'set_test_rest_catalog_auth_provider';

CREATE OR REPLACE FUNCTION set_test_rest_catalog_auth_response(TEXT, INT, BOOL)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'set_test_rest_catalog_auth_response';

CREATE OR REPLACE FUNCTION register_namespace_to_rest_catalog(TEXT,TEXT)
RETURNS void LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'register_namespace_to_rest_catalog';

CREATE OR REPLACE FUNCTION get_rest_vended_credentials(TEXT, TEXT, TEXT)
RETURNS text LANGUAGE C VOLATILE STRICT
AS 'pg_lake_iceberg', 'get_rest_vended_credentials';
"""

_S3_CREDENTIAL = {
    "s3.access-key-id": "STATS_ACCESS_KEY",
    "s3.secret-access-key": "STATS_SECRET_KEY",
    "s3.session-token": "STATS_SESSION_TOKEN",
    "client.region": "us-west-2",
}

_GCS_CREDENTIAL = {
    "gcs.oauth2.token": "not-a-token",
    "gcs.oauth2.token-expires-at": "9999999999000",
}

_OPERATIONS = {
    "config",
    "token",
    "list_namespaces",
    "get_namespace",
    "create_namespace",
    "load_table",
    "create_table",
    "publish_create_table",
    "publish_drop_table",
    "publish_commit",
}

_RESULTS = {
    "requests": {"ok", "not_found", "client_error", "server_error", "network_error"},
    "request_microseconds": {None},
    "retries": {"rate_limited", "unavailable", "auth_refresh"},
    "auth_failures": {None},
    "vended_credentials": {"vended", "none", "unusable"},
    "table_scans": {None},
}


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler_class():
    class _Handler(BaseHTTPRequestHandler):
        token_status = 200

        # statuses for successive GET .../namespaces/<name>, then 200
        namespace_statuses = []

        # where loadTable says the table is, and the (prefix below it,
        # config) of each storage credential it vends
        table_location = "s3://stats-bucket/ns/tbl"
        table_credentials = []

        def _reply(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

        def _handle(self):
            length = int(self.headers.get("Content-Length", 0))
            if length > 0:
                self.rfile.read(length)

            if "/oauth/tokens" in self.path:
                if _Handler.token_status != 200:
                    self._reply(_Handler.token_status, {"error": "invalid_client"})
                    return
                self._reply(
                    200,
                    {
                        "access_token": uuid.uuid4().hex,
                        "token_type": "bearer",
                        "expires_in": 3600,
                    },
                )
            elif "/tables/" in self.path:
                location = _Handler.table_location
                body = {
                    "metadata-location": f"{location}/metadata/v1.metadata.json",
                    "metadata": {
                        "format-version": 2,
                        "table-uuid": str(uuid.uuid4()),
                        "location": location,
                    },
                }
                if _Handler.table_credentials:
                    body["storage-credentials"] = [
                        {"prefix": f"{location}{prefix}", "config": config}
                        for prefix, config in _Handler.table_credentials
                    ]
                self._reply(200, body)
            elif self.path.rstrip("/").endswith("/namespaces"):
                if self.command == "GET":
                    self._reply(200, {"namespaces": []})
                else:
                    self._reply(200, {"namespace": ["created"], "properties": {}})
            else:
                status = 200
                if _Handler.namespace_statuses:
                    status = _Handler.namespace_statuses.pop(0)
                if status == 200:
                    self._reply(200, {"namespace": ["existing"], "properties": {}})
                else:
                    self._reply(
                        status,
                        {
                            "error": {
                                "message": "mock error",
                                "type": "MockException",
                                "code": status,
                            }
                        },
                    )

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


_CATALOG_SETTINGS = [
    "pg_lake_iceberg.rest_catalog_host",
    "pg_lake_iceberg.rest_catalog_client_id",
    "pg_lake_iceberg.rest_catalog_client_secret",
    "pg_lake_iceberg.rest_catalog_enable_vended_credentials",
]


def _set_catalog_gucs(host):
    values = [host, "test_id", "test_secret", "true"]
    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET {name} TO '{value}'"
            for name, value in zip(_CATALOG_SETTINGS, values)
        ]
        + ["SELECT pg_reload_conf()"]
    )


def _reset_catalog_gucs():
    run_command_outside_tx(
        [f"ALTER SYSTEM RESET {name}" for name in _CATALOG_SETTINGS]
        + ["SELECT pg_reload_conf()"]
    )


@pytest.fixture(scope="function")
def catalog_and_conn(postgres, iceberg_extension):
    """Point the built-in catalog at a mock and hand back a fresh connection."""
    port = _find_free_port()
    handler_class = _make_handler_class()
    httpd = HTTPServer(("127.0.0.1", port), handler_class)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    host = f"http://127.0.0.1:{port}"
    _set_catalog_gucs(host)

    conn = open_pg_conn()
    wait_for_reloaded_settings([conn], {"pg_lake_iceberg.rest_catalog_host": host})
    run_command(_TEST_FNS, conn)
    conn.commit()

    yield handler_class, conn

    conn.close()

    httpd.shutdown()
    thread.join(timeout=5)

    _reset_catalog_gucs()


def _counters():
    conn = open_pg_conn()
    try:
        rows = run_query(
            "SELECT metric, catalog, operation, result, value "
            "FROM lake_iceberg.catalog_stats()",
            conn,
        )
    finally:
        conn.close()
    return {tuple(row[:4]): row[4] for row in rows}


def _changes(before):
    after = _counters()
    return {
        key: value - before.get(key, 0)
        for key, value in after.items()
        if value != before.get(key, 0)
    }


def _register_namespace(conn, namespace):
    run_command(
        f"SELECT register_namespace_to_rest_catalog('{CATALOG_NAME}', '{namespace}')",
        conn,
    )
    conn.commit()


def _load_table(conn, table):
    result = run_query(
        f"SELECT get_rest_vended_credentials('{CATALOG_NAME}', 'ns', '{table}')",
        conn,
    )
    conn.commit()
    return result[0][0]


def test_requests_are_counted_by_outcome(installcheck, catalog_and_conn):
    """
    Each request is counted once under its operation and outcome, together
    with the time it took.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_statuses = [404]

    before = _counters()
    _register_namespace(conn, "ns_counted")
    changes = _changes(before)

    assert changes[("requests", "builtin", "get_namespace", "not_found")] == 1
    assert changes[("requests", "builtin", "list_namespaces", "ok")] == 1
    assert changes[("requests", "builtin", "create_namespace", "ok")] == 1
    for operation in ["get_namespace", "list_namespaces", "create_namespace"]:
        assert changes[("request_microseconds", "builtin", operation, None)] > 0


@pytest.mark.parametrize(
    "status, outcome",
    [(403, "client_error"), (500, "server_error")],
)
def test_failed_requests_are_counted(installcheck, catalog_and_conn, status, outcome):
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_statuses = [status]

    before = _counters()
    with pytest.raises(psycopg2.Error, match=f"HTTP {status}"):
        _register_namespace(conn, f"ns_{outcome}")
    conn.rollback()
    changes = _changes(before)

    assert changes[("requests", "builtin", "get_namespace", outcome)] == 1
    assert ("requests", "builtin", "get_namespace", "ok") not in changes


@pytest.mark.parametrize(
    "status, reason",
    [(429, "rate_limited"), (401, "auth_refresh")],
)
def test_retries_are_counted(installcheck, catalog_and_conn, status, reason):
    """
    A retried request counts each retry, and its final result once.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_statuses = [status]

    before = _counters()
    _register_namespace(conn, f"ns_retry_{reason}")
    changes = _changes(before)

    assert changes[("retries", "builtin", "get_namespace", reason)] == 1
    assert changes[("requests", "builtin", "get_namespace", "ok")] == 1
    assert not [
        key
        for key in changes
        if key[0] == "requests" and key[2] == "get_namespace" and key[3] != "ok"
    ]


def test_token_failure_is_counted(installcheck, catalog_and_conn):
    """
    A rejected token request counts as a failed request and as an
    authentication failure.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.token_status = 401

    before = _counters()
    with pytest.raises(psycopg2.Error, match="token request failed"):
        _register_namespace(conn, "ns_token")
    conn.rollback()
    changes = _changes(before)

    assert changes[("requests", "builtin", "token", "client_error")] == 1
    assert changes[("auth_failures", "builtin", None, None)] == 1


def test_provider_failure_is_counted_for_horizon(installcheck, catalog_and_conn):
    """
    The built-in catalog with horizon authentication is counted apart from
    the built-in catalog with any other.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    run_command("SET pg_lake_iceberg.rest_catalog_auth_type TO 'horizon'", conn)
    run_command("SELECT set_test_rest_catalog_auth_response('', 3600, true)", conn)
    run_command("SELECT set_test_rest_catalog_auth_provider(true)", conn)
    conn.commit()

    before = _counters()
    with pytest.raises(psycopg2.Error, match="empty authorization"):
        _register_namespace(conn, "ns_horizon")
    conn.rollback()
    changes = _changes(before)

    assert changes[("auth_failures", "horizon", None, None)] == 1
    assert ("auth_failures", "builtin", None, None) not in changes


def test_unreachable_catalog_is_counted(installcheck, postgres, iceberg_extension):
    if installcheck:
        return

    host = f"http://127.0.0.1:{_find_free_port()}"
    _set_catalog_gucs(host)

    conn = open_pg_conn()
    try:
        wait_for_reloaded_settings([conn], {"pg_lake_iceberg.rest_catalog_host": host})
        run_command(_TEST_FNS, conn)
        conn.commit()

        before = _counters()
        with pytest.raises(psycopg2.Error):
            _register_namespace(conn, "ns_unreachable")
        conn.rollback()
        changes = _changes(before)

        assert changes[("requests", "builtin", "token", "network_error")] == 1
        assert changes[("auth_failures", "builtin", None, None)] == 1
    finally:
        conn.close()
        _reset_catalog_gucs()


@pytest.mark.parametrize(
    "location, credentials, outcome",
    [
        pytest.param(
            "s3://stats-bucket/ns/tbl", [("/", _S3_CREDENTIAL)], "vended", id="s3"
        ),
        pytest.param("s3://stats-bucket/ns/tbl", [], "none", id="none"),
        pytest.param(
            "gs://stats-bucket/ns/tbl",
            [("/", _GCS_CREDENTIAL)],
            "unusable",
            id="gcs",
        ),
        pytest.param(
            "s3://stats-bucket/ns/tbl",
            [
                ("/", {"s3.session-token": "A_TOKEN_WITHOUT_ITS_KEYS"}),
                ("/data/", _S3_CREDENTIAL),
            ],
            "vended",
            id="unusable-and-s3",
        ),
    ],
)
def test_vended_credentials_are_counted(
    installcheck, catalog_and_conn, location, credentials, outcome
):
    """
    Every table load that asks for vended credentials counts one outcome,
    each time rather than once per table as the warnings are.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.table_location = location
    handler_class.table_credentials = credentials
    table = f"tbl_{uuid.uuid4().hex[:8]}"

    before = _counters()
    for _ in range(2):
        summary = _load_table(conn, table)
        assert (summary is not None) == (outcome == "vended")
    changes = _changes(before)

    assert changes[("vended_credentials", "builtin", None, outcome)] == 2
    assert [key for key in changes if key[0] == "vended_credentials"] == [
        ("vended_credentials", "builtin", None, outcome)
    ]
    assert changes[("requests", "builtin", "load_table", "ok")] == 2


def test_stats_use_a_fixed_vocabulary(installcheck, catalog_and_conn):
    """
    Like the catalog log lines, the counters name no catalog, namespace or
    table, only kinds of catalog and fixed words.
    """
    if installcheck:
        return

    handler_class, conn = catalog_and_conn
    handler_class.namespace_statuses = [404]
    _register_namespace(conn, "ns_vocabulary")

    counters = _counters()
    assert counters

    for (metric, catalog, operation, result), value in counters.items():
        assert metric in _RESULTS, metric
        assert result in _RESULTS[metric], (metric, result)
        assert catalog in {"horizon", "builtin", "user"}, catalog
        if metric in ("auth_failures", "vended_credentials", "table_scans"):
            assert operation is None
        else:
            assert operation in _OPERATIONS, operation
        assert value > 0


def test_stats_need_server_statistics_privilege(postgres, iceberg_extension):
    conn = open_pg_conn()
    try:
        run_command("CREATE ROLE catalog_stats_reader", conn)
        conn.commit()

        run_command("SET ROLE catalog_stats_reader", conn)
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            run_query("SELECT * FROM lake_iceberg.catalog_stats()", conn)
        conn.rollback()

        run_command("GRANT pg_read_all_stats TO catalog_stats_reader", conn)
        conn.commit()

        run_command("SET ROLE catalog_stats_reader", conn)
        run_query("SELECT * FROM lake_iceberg.catalog_stats()", conn)
    finally:
        conn.rollback()
        run_command("DROP ROLE IF EXISTS catalog_stats_reader", conn)
        conn.commit()
        conn.close()

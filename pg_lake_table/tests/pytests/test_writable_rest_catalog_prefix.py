"""
Writable REST catalog tables and their catalog prefix (#690).

A writable table resolves its REST prefix once, at CREATE TABLE: an explicit
table catalog_name, else the server's catalog_name, else /v1/config, else the
database name.  A prefix other than the database name is stored as the
table's catalog_name option and cannot be changed afterwards, so a later
ALTER SERVER never re-routes an existing table.

A mock REST catalog records every request path, so the tests can check which
prefix pg_lake sent each call under.
"""

import json
import re
import socket
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from utils_pytest import *


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _MockCatalog:
    """Just enough of the Iceberg REST API for a writable CREATE/DROP TABLE."""

    def __init__(self, config_status=400, config_body=None):
        self.config_status = config_status
        self.config_body = config_body or {
            "error": {"message": "Please specify a warehouse", "code": 400}
        }
        self.requests = []  # (method, path)
        self.bodies = []
        mock = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass

            def _reply(self, status, body=None):
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                if body is not None:
                    self.wfile.write(json.dumps(body).encode())

            def _handle(self):
                mock.requests.append((self.command, self.path))
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                mock.bodies.append(json.loads(body) if body else None)

                path = self.path.split("?", 1)[0]

                if path.endswith("/v1/config"):
                    return self._reply(mock.config_status, mock.config_body)
                if self.command == "DELETE":
                    return self._reply(204)
                if path.endswith("/transactions/commit"):
                    return self._reply(204)
                if self.command == "GET" and path.endswith("/namespaces"):
                    return self._reply(200, {"namespaces": []})
                m = re.search(r"/namespaces/([^/]+)$", path)
                if self.command == "GET" and m:
                    return self._reply(
                        200, {"namespace": [m.group(1)], "properties": {}}
                    )
                if self.command == "POST" and "/tables" in path:
                    # stage-create and the finish-create both expect 200
                    return self._reply(200, {})
                return self._reply(404, {"error": {"message": "not mocked"}})

            do_GET = _handle
            do_POST = _handle
            do_DELETE = _handle
            do_HEAD = _handle

        self.port = _find_free_port()
        self.httpd = HTTPServer(("127.0.0.1", self.port), _Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def prefixes(self):
        """The prefix of every /v1/{prefix}/... request, config excluded."""
        found = set()
        for _, path in self.requests:
            m = re.match(r"/v1/(?!config)([^?]+?)/(namespaces|transactions)", path)
            if m:
                found.add(m.group(1))
        return found

    def shutdown(self):
        self.httpd.shutdown()
        self.thread.join(timeout=5)
        self.httpd.server_close()


@pytest.fixture
def mock_catalog():
    created = []

    def make(**kwargs):
        mock = _MockCatalog(**kwargs)
        created.append(mock)
        return mock

    yield make
    for mock in created:
        mock.shutdown()


SCHEMA = "writable_prefix_sc"


def _create_server(conn, name, endpoint, catalog_name=None):
    catalog_name_opt = (
        f", catalog_name '{catalog_name}'" if catalog_name is not None else ""
    )
    run_command(
        f"""
        CREATE SERVER {name} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     rest_auth_type 'none',
                     enable_vended_credentials 'false',
                     location_prefix 's3://{TEST_BUCKET}'{catalog_name_opt})
        """,
        conn,
    )
    conn.commit()


def _table_option(conn, table, option):
    rows = run_query(
        f"""
        SELECT option_value FROM pg_options_to_table(
            (SELECT ftoptions FROM pg_foreign_table
             WHERE ftrelid = '{SCHEMA}.{table}'::regclass))
        WHERE option_name = '{option}'
        """,
        conn,
    )
    return rows[0][0] if rows else None


@pytest.fixture
def writable_schema(superuser_conn, s3, extension, mock_catalog):
    # Keep the mock alive until DROP TABLE's post-commit HTTP requests finish.
    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", superuser_conn)
    run_command(f"CREATE SCHEMA {SCHEMA}", superuser_conn)
    superuser_conn.commit()
    servers = []
    yield servers
    superuser_conn.rollback()
    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", superuser_conn)
    for name in servers:
        run_command(f"DROP SERVER IF EXISTS {name} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_server_catalog_name_is_used_and_stored(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    mock = mock_catalog()
    writable_schema.append("wp_server_catname")
    _create_server(superuser_conn, "wp_server_catname", mock.endpoint, "sales")

    before = len(mock.requests)
    err = run_command(
        f"CREATE TABLE {SCHEMA}.empty_prefix (a int) USING iceberg "
        "WITH (catalog='wp_server_catname', catalog_name='')",
        superuser_conn,
        raise_error=False,
    )
    assert "catalog_name must not be empty" in str(err), err
    assert mock.requests[before:] == []
    superuser_conn.rollback()

    run_command(
        f"CREATE TABLE {SCHEMA}.t (a int) USING iceberg "
        "WITH (catalog = 'wp_server_catname')",
        superuser_conn,
    )
    superuser_conn.commit()

    assert mock.prefixes() == {"sales"}, mock.requests
    assert _table_option(superuser_conn, "t", "catalog_name") == "sales"
    # the server's catalog_name wins, so /v1/config is not consulted
    assert not any(p.startswith("/v1/config") for _, p in mock.requests)


def test_advertised_prefix_is_used_and_stored(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    warehouse_id = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
    mock = mock_catalog(
        config_status=200, config_body={"defaults": {"prefix": warehouse_id}}
    )
    writable_schema.append("wp_discovery")
    _create_server(superuser_conn, "wp_discovery", mock.endpoint)

    run_command(
        f"CREATE TABLE {SCHEMA}.t (a int) USING iceberg "
        "WITH (catalog = 'wp_discovery')",
        superuser_conn,
    )
    superuser_conn.commit()

    assert mock.prefixes() == {warehouse_id}, mock.requests
    assert _table_option(superuser_conn, "t", "catalog_name") == warehouse_id


def test_falls_back_to_database_name_without_storing_it(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    """A catalog that rejects /v1/config keeps today's database-name prefix."""
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    mock = mock_catalog(config_status=400)
    writable_schema.append("wp_fallback")
    _create_server(superuser_conn, "wp_fallback", mock.endpoint)

    run_command(
        f"CREATE TABLE {SCHEMA}.t (a int) USING iceberg "
        "WITH (catalog = 'wp_fallback')",
        superuser_conn,
    )
    superuser_conn.commit()

    dbname = run_query("SELECT current_database()", superuser_conn)[0][0]
    assert mock.prefixes() == {dbname}, mock.requests
    assert _table_option(superuser_conn, "t", "catalog_name") is None


def test_alter_server_does_not_reroute_and_catalog_name_is_locked(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    mock = mock_catalog()
    writable_schema.append("wp_locked")
    _create_server(superuser_conn, "wp_locked", mock.endpoint, "first")

    run_command(
        f"CREATE TABLE {SCHEMA}.t (a int) USING iceberg "
        "WITH (catalog = 'wp_locked')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        "ALTER SERVER wp_locked OPTIONS (SET catalog_name 'second')", superuser_conn
    )
    superuser_conn.commit()
    assert _table_option(superuser_conn, "t", "catalog_name") == "first"

    for alter in (
        "SET catalog_name 'second'",
        "DROP catalog_name",
    ):
        err = run_command(
            f"ALTER FOREIGN TABLE {SCHEMA}.t OPTIONS ({alter})",
            superuser_conn,
            raise_error=False,
        )
        assert err is not None and 'Changing option "catalog_name"' in str(err), err
        superuser_conn.rollback()

    # pg_dump emits the stored prefix; accepting it at CREATE must not weaken
    # the ALTER guard or make restored tables inherit changed server defaults.
    run_command(
        f"CREATE TABLE {SCHEMA}.t2 (a int) USING iceberg "
        "WITH (catalog = 'wp_locked', catalog_name = 'first')",
        superuser_conn,
    )
    superuser_conn.commit()
    assert _table_option(superuser_conn, "t2", "catalog_name") == "first"


def test_mixed_prefixes_in_one_transaction_are_rejected(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    """
    Two tables on one server can carry different prefixes when the server's
    catalog_name changes between their CREATEs.  One transaction commits to a
    single transactions/commit URL, so touching both must fail before any
    request is sent.
    """
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    mock = mock_catalog()
    writable_schema.append("wp_mixed")
    _create_server(superuser_conn, "wp_mixed", mock.endpoint, "first")

    run_command(
        f"CREATE TABLE {SCHEMA}.t1 (a int) USING iceberg WITH (catalog = 'wp_mixed')",
        superuser_conn,
    )
    superuser_conn.commit()
    run_command(
        "ALTER SERVER wp_mixed OPTIONS (SET catalog_name 'second')", superuser_conn
    )
    superuser_conn.commit()
    run_command(
        f"CREATE TABLE {SCHEMA}.t2 (a int) USING iceberg WITH (catalog = 'wp_mixed')",
        superuser_conn,
    )
    superuser_conn.commit()
    assert _table_option(superuser_conn, "t2", "catalog_name") == "second"

    sent_before = len(mock.requests)
    err = run_command(
        f"DROP TABLE {SCHEMA}.t1, {SCHEMA}.t2", superuser_conn, raise_error=False
    )
    assert err is not None and "different REST catalog prefixes" in str(err), err
    superuser_conn.rollback()
    assert not any(
        m == "DELETE" for m, _ in mock.requests[sent_before:]
    ), mock.requests[sent_before:]

    # each table on its own is fine
    run_command(f"DROP TABLE {SCHEMA}.t1", superuser_conn)
    superuser_conn.commit()
    run_command(f"DROP TABLE {SCHEMA}.t2", superuser_conn)
    superuser_conn.commit()


def test_mixed_prefix_create_rejected_before_staging(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_early")
    _create_server(superuser_conn, "wp_early", mock.endpoint, "first")
    run_command(
        f"CREATE TABLE {SCHEMA}.t1 (a int) USING iceberg WITH (catalog='wp_early')",
        superuser_conn,
    )
    sent_before = len(mock.requests)
    err = run_command(
        f"CREATE TABLE {SCHEMA}.t2 (a int) USING iceberg "
        "WITH (catalog='wp_early', catalog_name='second')",
        superuser_conn,
        raise_error=False,
    )
    assert "different REST catalog prefixes" in str(err), err
    assert mock.requests[sent_before:] == []
    superuser_conn.rollback()


def test_dumped_prefix_can_be_replayed_after_server_change(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_dump")
    _create_server(superuser_conn, "wp_dump", mock.endpoint, "first")
    run_command(
        f"CREATE TABLE {SCHEMA}.dumped (a int) USING iceberg WITH (catalog='wp_dump')",
        superuser_conn,
    )
    superuser_conn.commit()
    dump = subprocess.check_output(
        [
            "pg_dump",
            "--dbname",
            superuser_conn.dsn,
            "--schema-only",
            "--table",
            f"{SCHEMA}.dumped",
        ],
        text=True,
    )
    ddl = re.search(r"CREATE FOREIGN TABLE .*?;", dump, re.DOTALL).group(0)
    assert "catalog_name 'first'" in ddl
    # Test catalog identity replay, not restoring existing data files.
    ddl, replaced = re.subn(r"location '([^']+)'", r"location '\1/replay'", ddl)
    assert replaced == 1
    run_command(f"DROP TABLE {SCHEMA}.dumped", superuser_conn)
    superuser_conn.commit()
    run_command(
        "ALTER SERVER wp_dump OPTIONS (SET catalog_name 'second')", superuser_conn
    )
    superuser_conn.commit()
    sent_before = len(mock.requests)
    run_command(ddl, superuser_conn)
    superuser_conn.commit()
    assert _table_option(superuser_conn, "dumped", "catalog_name") == "first"
    assert all("/second/" not in path for _, path in mock.requests[sent_before:])


def test_mixed_prefix_alter_rejected_at_statement_time(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_alter")
    _create_server(superuser_conn, "wp_alter", mock.endpoint, "first")
    for table, prefix in (("t1", "first"), ("t2", "second")):
        run_command(
            f"CREATE TABLE {SCHEMA}.{table} (a int) USING iceberg "
            f"WITH (catalog='wp_alter', catalog_name='{prefix}')",
            superuser_conn,
        )
        superuser_conn.commit()
    sent_before = len(mock.requests)
    run_command(f"ALTER TABLE {SCHEMA}.t1 ADD COLUMN b int", superuser_conn)
    err = run_command(
        f"ALTER TABLE {SCHEMA}.t2 ADD COLUMN b int", superuser_conn, raise_error=False
    )
    assert "different REST catalog prefixes" in str(err), err
    assert mock.requests[sent_before:] == []
    superuser_conn.rollback()
    for table in ("t1", "t2"):
        run_command(f"DROP TABLE {SCHEMA}.{table}", superuser_conn)
        superuser_conn.commit()

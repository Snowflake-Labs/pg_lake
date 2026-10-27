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
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import psycopg2
from utils_pytest import *


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _MockCatalog:
    """Just enough of the Iceberg REST API for a writable CREATE/DROP TABLE."""

    def __init__(
        self,
        config_status=400,
        config_body=None,
        stage_status=200,
        stage_body=None,
        honor_location=False,
        namespace_location=None,
        load_table=None,
    ):
        self.load_table = load_table
        self.stage_status = stage_status
        self.stage_body = stage_body
        self.honor_location = honor_location
        self.namespace_location = namespace_location
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
                    self.wfile.write(
                        (body if isinstance(body, str) else json.dumps(body)).encode()
                    )

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
                    properties = {}
                    if mock.namespace_location is not None:
                        properties["location"] = mock.namespace_location
                    return self._reply(
                        200, {"namespace": [m.group(1)], "properties": properties}
                    )
                if self.command == "POST" and "/tables" in path:
                    if path.endswith("/tables"):
                        assert mock.bodies[-1]["stage-create"] is True
                        if mock.stage_body is not None:
                            return self._reply(mock.stage_status, mock.stage_body)
                        requested = mock.bodies[-1].get("location")
                        return self._reply(
                            200,
                            {
                                "metadata": {
                                    "table-uuid": str(uuid.uuid4()),
                                    "location": (
                                        requested
                                        if mock.honor_location and requested
                                        else f"s3://{TEST_BUCKET}/staged-prefix/{uuid.uuid4()}"
                                    ),
                                }
                            },
                        )
                    return self._reply(200, {})
                if (
                    self.command == "GET"
                    and "/tables/" in path
                    and mock.load_table is not None
                ):
                    return self._reply(200, mock.load_table)
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


def test_savepoint_rollback_restores_prefix_and_requests(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_savepoint")
    _create_server(superuser_conn, "wp_savepoint", mock.endpoint, "first")
    run_command("SAVEPOINT before_first", superuser_conn)
    run_command(
        f"CREATE TABLE {SCHEMA}.rolled_back (a int) USING iceberg "
        "WITH (catalog='wp_savepoint')",
        superuser_conn,
    )
    run_command("ROLLBACK TO SAVEPOINT before_first", superuser_conn)
    sent_before = len(mock.requests)
    run_command(
        f"CREATE TABLE {SCHEMA}.kept (a int) USING iceberg "
        "WITH (catalog='wp_savepoint', catalog_name='second')",
        superuser_conn,
    )
    superuser_conn.commit()
    assert all("/first/" not in path for _, path in mock.requests[sent_before:])
    assert _table_option(superuser_conn, "kept", "catalog_name") == "second"


def test_released_savepoint_preserves_parent_binding(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_release")
    _create_server(superuser_conn, "wp_release", mock.endpoint, "first")
    run_command("SAVEPOINT parent", superuser_conn)
    run_command("SAVEPOINT child", superuser_conn)
    run_command(
        f"CREATE TABLE {SCHEMA}.t (a int) USING iceberg WITH (catalog='wp_release')",
        superuser_conn,
    )
    run_command("RELEASE SAVEPOINT child", superuser_conn)
    run_command("SAVEPOINT conflicting", superuser_conn)
    err = run_command(
        f"CREATE TABLE {SCHEMA}.bad (a int) USING iceberg "
        "WITH (catalog='wp_release', catalog_name='second')",
        superuser_conn,
        raise_error=False,
    )
    assert "different REST catalog prefixes" in str(err), err
    run_command("ROLLBACK TO SAVEPOINT conflicting", superuser_conn)
    run_command("ROLLBACK TO SAVEPOINT parent", superuser_conn)
    sent_before = len(mock.requests)
    run_command(
        f"CREATE TABLE {SCHEMA}.kept (a int) USING iceberg "
        "WITH (catalog='wp_release', catalog_name='second')",
        superuser_conn,
    )
    superuser_conn.commit()
    assert all("/first/" not in path for _, path in mock.requests[sent_before:])


def test_savepoint_rollback_preserves_parent_create(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_parent")
    _create_server(superuser_conn, "wp_parent", mock.endpoint, "first")
    run_command(
        f"CREATE TABLE {SCHEMA}.kept (a int) USING iceberg WITH (catalog='wp_parent')",
        superuser_conn,
    )
    run_command("SAVEPOINT child", superuser_conn)
    run_command(f"DROP TABLE {SCHEMA}.kept", superuser_conn)
    run_command(
        f"CREATE TABLE {SCHEMA}.discarded (a int) USING iceberg WITH (catalog='wp_parent')",
        superuser_conn,
    )
    run_command("ROLLBACK TO child", superuser_conn)
    sent_before = len(mock.requests)
    superuser_conn.commit()
    requests = mock.requests[sent_before:]
    assert ("POST", f"/v1/first/namespaces/{SCHEMA}/tables/kept") in requests
    assert not any(
        method == "DELETE" or "discarded" in path for method, path in requests
    )


def test_first_write_inside_fresh_session_savepoint(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog()
    writable_schema.append("wp_fresh")
    _create_server(superuser_conn, "wp_fresh", mock.endpoint, "first")
    conn = psycopg2.connect(superuser_conn.dsn)
    try:
        run_command("SAVEPOINT outer_savepoint", conn)
        run_command("SAVEPOINT inner_savepoint", conn)
        run_command(
            f"CREATE TABLE {SCHEMA}.discarded (a int) USING iceberg WITH (catalog='wp_fresh')",
            conn,
        )
        run_command("RELEASE inner_savepoint", conn)
        run_command("ROLLBACK TO outer_savepoint", conn)
        sent_before = len(mock.requests)
        run_command(
            f"CREATE TABLE {SCHEMA}.kept (a int) USING iceberg "
            "WITH (catalog='wp_fresh', catalog_name='second')",
            conn,
        )
        conn.commit()
        assert all("/first/" not in path for _, path in mock.requests[sent_before:])
    finally:
        conn.close()


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


@pytest.mark.parametrize(
    "status,body,message",
    [
        (200, {}, "must include a table UUID and location"),
        (422, "stage-create must be a boolean", "HTTP request failed (HTTP 422)"),
        (
            200,
            {
                "metadata": {
                    "table-uuid": str(uuid.uuid4()),
                    "location": "http://127.0.0.1:1/staged",
                }
            },
            'Iceberg file URL "http://127.0.0.1:1/staged" does not point to object storage',
        ),
        (
            200,
            {
                "metadata": {
                    "table-uuid": str(uuid.uuid4()),
                    "location": f"s3://{TEST_BUCKET}/staged?s3_endpoint=127.0.0.1:1",
                }
            },
            'query parameter "s3_endpoint" is not permitted',
        ),
    ],
)
def test_invalid_stage_response_aborts_create(
    installcheck, superuser_conn, writable_schema, mock_catalog, status, body, message
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog(stage_status=status, stage_body=body)
    writable_schema.append("wp_bad_stage")
    _create_server(superuser_conn, "wp_bad_stage", mock.endpoint, "first")
    err = run_command(
        f"CREATE TABLE {SCHEMA}.bad_stage (id int) USING iceberg WITH (catalog='wp_bad_stage')",
        superuser_conn,
        raise_error=False,
    )
    assert message in str(err), err
    superuser_conn.rollback()
    assert (
        run_query(f"SELECT to_regclass('{SCHEMA}.bad_stage')", superuser_conn)[0][0]
        is None
    )


def test_catalog_location_uuid_and_schema_used_for_create(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    table_uuid = str(uuid.uuid4())
    location = f"s3://{TEST_BUCKET}/catalog-chosen/{table_uuid}"
    mock = mock_catalog(
        stage_body={
            "metadata": {
                "table-uuid": table_uuid,
                "location": location,
            }
        }
    )
    writable_schema.append("wp_stage")
    _create_server(superuser_conn, "wp_stage", mock.endpoint, "first")
    run_command("ALTER SERVER wp_stage OPTIONS (DROP location_prefix)", superuser_conn)
    superuser_conn.commit()
    requested = f"s3://{TEST_BUCKET}/client-location"
    run_command(
        f"CREATE TABLE {SCHEMA}.staged (id int, labels text[]) USING iceberg "
        f"WITH (catalog='wp_stage', location='{requested}')",
        superuser_conn,
    )
    # The mock assigns its own location, so the user is told.
    assert any(
        "instead of the requested" in notice and requested in notice
        for notice in superuser_conn.notices
    )
    superuser_conn.commit()
    assert _table_option(superuser_conn, "staged", "location") == location
    stage = next(
        body
        for (_, path), body in zip(mock.requests, mock.bodies)
        if path.endswith("/tables")
    )
    assert stage["stage-create"] is True
    assert stage["location"] == requested
    assert [field["name"] for field in stage["schema"]["fields"]] == ["id", "labels"]
    assert stage["schema"]["fields"][1]["type"]["type"] == "list"
    final = next(
        body
        for (method, path), body in zip(mock.requests, mock.bodies)
        if method == "POST" and path.endswith("/tables/staged")
    )
    updates = {update["action"]: update for update in final["updates"]}
    assert updates["assign-uuid"]["uuid"] == table_uuid
    assert updates["set-location"]["location"] == location
    assert updates["upgrade-format-version"]["format-version"] == 2
    # Lakekeeper keeps the staged schema with its own field IDs; the schema
    # pg_lake adds must become current.
    assert updates["set-current-schema"]["schema-id"] == -1
    actions = [update["action"] for update in final["updates"]]
    assert actions.index("set-current-schema") == actions.index("add-schema") + 1


@pytest.mark.parametrize("honor", [True, False], ids=["honored", "replaced"])
def test_stage_requests_pg_lake_location(
    installcheck, superuser_conn, writable_schema, mock_catalog, honor
):
    """
    With a location_prefix, stage-create asks for pg_lake's per-relation
    location, so a re-created table cannot land on a dropped table's files
    (Polaris otherwise assigns <namespace>/<table>).  Whatever the catalog
    returns is the table's location.
    """
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog(honor_location=honor)
    writable_schema.append("wp_reqloc")
    _create_server(superuser_conn, "wp_reqloc", mock.endpoint, "first")
    superuser_conn.commit()
    run_command(
        f"CREATE TABLE {SCHEMA}.reqloc (id int) USING iceberg WITH (catalog='wp_reqloc')",
        superuser_conn,
    )
    superuser_conn.commit()

    relid = run_query(f"SELECT '{SCHEMA}.reqloc'::regclass::oid", superuser_conn)[0][0]
    expected = f"s3://{TEST_BUCKET}/{server_params.PG_DATABASE}/{SCHEMA}/reqloc/{relid}"
    stage = next(
        body
        for (_, path), body in zip(mock.requests, mock.bodies)
        if path.endswith("/tables")
    )
    assert stage["location"] == expected

    location = _table_option(superuser_conn, "reqloc", "location")
    if honor:
        assert location == expected
    else:
        assert location.startswith(f"s3://{TEST_BUCKET}/staged-prefix/")
    final = next(
        body
        for (method, path), body in zip(mock.requests, mock.bodies)
        if method == "POST" and path.endswith("/tables/reqloc")
    )
    updates = {update["action"]: update for update in final["updates"]}
    assert updates["set-location"]["location"] == location


@pytest.mark.parametrize(
    "namespace_location, under",
    [
        (f"s3://{TEST_BUCKET}/{server_params.PG_DATABASE}/{SCHEMA}", True),
        (f"s3://{TEST_BUCKET}/catalog-root/{SCHEMA}/", False),
    ],
    ids=["path-under-namespace", "path-outside-namespace"],
)
def test_requested_location_stays_under_namespace_location(
    installcheck,
    superuser_conn,
    writable_schema,
    mock_catalog,
    namespace_location,
    under,
):
    """
    A catalog may only accept tables under the namespace's location (Polaris
    names it after the raw schema name, so an encoded schema in pg_lake's
    path falls outside it).  When pg_lake's path is not under the reported
    namespace location, the same unique tail is requested under it instead.
    """
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog(honor_location=True, namespace_location=namespace_location)
    writable_schema.append("wp_nsloc")
    _create_server(superuser_conn, "wp_nsloc", mock.endpoint, "first")
    superuser_conn.commit()
    run_command(
        f"CREATE TABLE {SCHEMA}.nsloc (id int) USING iceberg WITH (catalog='wp_nsloc')",
        superuser_conn,
    )
    superuser_conn.commit()

    relid = run_query(f"SELECT '{SCHEMA}.nsloc'::regclass::oid", superuser_conn)[0][0]
    tail = f"{server_params.PG_DATABASE}/{SCHEMA}/nsloc/{relid}"
    expected = (
        f"s3://{TEST_BUCKET}/{tail}"
        if under
        else f"{namespace_location.rstrip('/')}/{tail}"
    )
    stage = next(
        body
        for (_, path), body in zip(mock.requests, mock.bodies)
        if path.endswith("/tables")
    )
    assert stage["location"] == expected
    assert _table_option(superuser_conn, "nsloc", "location") == expected


def test_explicit_location_is_requested_and_kept(
    installcheck, superuser_conn, writable_schema, mock_catalog
):
    """An explicit location is requested as given, as on main; a catalog that
    honors it keeps it, with no NOTICE."""
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    mock = mock_catalog(honor_location=True)
    writable_schema.append("wp_explicit")
    _create_server(superuser_conn, "wp_explicit", mock.endpoint, "first")
    superuser_conn.commit()
    requested = f"s3://{TEST_BUCKET}/explicit/{uuid.uuid4()}"
    del superuser_conn.notices[:]
    run_command(
        f"CREATE TABLE {SCHEMA}.explicit (id int) USING iceberg "
        f"WITH (catalog='wp_explicit', location='{requested}/')",
        superuser_conn,
    )
    superuser_conn.commit()
    assert not any("instead of the requested" in n for n in superuser_conn.notices)
    stage = next(
        body
        for (_, path), body in zip(mock.requests, mock.bodies)
        if path.endswith("/tables")
    )
    assert stage["location"] == requested
    assert _table_option(superuser_conn, "explicit", "location") == requested


def _load_table_response(table_uuid, location):
    """A loadTable answer for an empty table with one int column `id`."""
    return {
        "metadata-location": f"s3://{TEST_BUCKET}/lt/{table_uuid}/00000.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": location,
            "last-sequence-number": 0,
            "last-updated-ms": 1,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "type": "int", "required": False}
                    ],
                }
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
        },
    }


@pytest.mark.parametrize(
    "location,message",
    [
        (
            "http://127.0.0.1:1/loaded",
            'Iceberg URL "http://127.0.0.1:1/loaded" does not point to object storage',
        ),
        (
            f"s3://{TEST_BUCKET}/loaded?s3_endpoint=127.0.0.1:1",
            'query parameter "s3_endpoint" is not permitted',
        ),
        (
            "abfss://container@127.0.0.1:1/loaded",
            'Azure storage host "127.0.0.1:1" is not permitted',
        ),
    ],
)
def test_loaded_table_location_must_be_object_storage(
    installcheck, superuser_conn, writable_schema, mock_catalog, location, message
):
    """Writes to a writable REST table go under the location in the metadata
    the catalog loads, so a catalog must not steer them to another host."""
    if installcheck:
        pytest.skip("Requires an isolated test cluster")
    table_uuid = str(uuid.uuid4())
    mock = mock_catalog(load_table=_load_table_response(table_uuid, location))
    writable_schema.append("wp_bad_load")
    _create_server(superuser_conn, "wp_bad_load", mock.endpoint, "first")
    run_command(
        f"CREATE TABLE {SCHEMA}.bad_load (id int) USING iceberg WITH (catalog='wp_bad_load')",
        superuser_conn,
    )
    superuser_conn.commit()

    # Metadata changes are applied, and the manifests written, at commit.
    run_command(f"INSERT INTO {SCHEMA}.bad_load VALUES (1)", superuser_conn)
    with pytest.raises(psycopg2.Error) as err:
        superuser_conn.commit()
    assert message in str(err.value), err.value
    superuser_conn.rollback()

    mock.load_table = None
    run_command(f"DROP TABLE {SCHEMA}.bad_load", superuser_conn)
    superuser_conn.commit()

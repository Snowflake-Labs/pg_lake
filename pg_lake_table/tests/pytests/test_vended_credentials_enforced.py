"""Enforcement tests for REST-catalog vended credentials.

These tests prove the vended credential is genuinely *load-bearing* for a
data scan, which needs an S3 mock that can actually deny a request.  Moto
can: it implements IAM policy evaluation for S3 and the enforcement switch
can be flipped at runtime, so ``helpers/moto_iam_storage.py`` runs a moto
server that is permissive during setup and enforcing during the assertion.

The shape of every test is the same.  The pre-existing static secret can
read the table's *metadata* only; a successful data scan therefore has to
come from the credential the catalog vended.

  * ``test_moto_vended_credentials_required_for_scan``
        vending on -> the scan succeeds and a ``pglake_vended_*`` secret
        scoped to the table location is present on pgduck_server.

  * ``test_moto_scan_denied_without_vended_credentials``
        the catalog delegates nothing -> the data scan falls back to the
        metadata-only static secret and is denied.

  * ``test_moto_vended_credentials_wrong_scope_denied``
        the credential is labeled with a foreign prefix -> the scan is
        denied, and nothing is registered under that foreign scope where it
        would shadow the secret another table depends on.

  * ``test_moto_columnless_attach_does_not_read_storage``
        ``CREATE TABLE t ()`` learns its columns from the metadata the
        catalog inlines.  The static secret cannot reach the table at all,
        so an attach that goes to storage is denied.

  * ``test_moto_vended_secret_dropped_when_revoked``
        vending is turned off after a successful scan; the next scan must
        drop the secret it pushed, so the identical read is denied.

  * ``test_moto_two_principals_get_their_own_credentials``
        two roles on one backend are vended different credentials; serving
        the second role the first one's would let a denied scan through.

  * ``test_moto_vended_secret_dropped_with_read_only_table``
        a read-only table's secret goes away with the table.

  * ``test_moto_catalog_supplied_endpoint_still_reaches_the_store``
        a catalog that states its endpoint (with or without the addressing
        style) must still produce a usable secret.

  * ``test_moto_data_only_credential_serves_repeated_scans``
        a ``.../data/`` credential must not become the one DuckDB picks for
        the metadata beside it.

  * ``test_moto_unrelated_table_still_reads_through_the_static_secret``
        vending for one table leaves every other path alone.

Architecture recap (why the static/vended split works):
  - REST loadTable runs in PostgreSQL over HTTP (mock catalog here).
  - Iceberg metadata (metadata.json + manifest Avro) is read on
    pgduck_server using the pre-existing static S3 secret.
  - The parquet data scan runs on pgduck_server using the scoped
    ``pglake_vended_*`` secret (falling back to the static secret when no
    vended credential is cached).
  - The vended secret inherits ENDPOINT/URL_STYLE/USE_SSL from the static
    secret whose scope best-matches the vended scope, so pointing the
    static secret at the mock is what makes vended secrets reach it too.
"""

import base64
import json
import socket
import tempfile
import threading
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils_pytest import *

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.catalog.sql import SqlCatalog

    _HAVE_PYICEBERG = True
except Exception:  # pragma: no cover - depends on environment
    _HAVE_PYICEBERG = False

# Everything these tests read is read-only; the metadata-only principals
# additionally need ListBucket to resolve a prefix.
_READ_ACTIONS = ("s3:GetObject", "s3:ListBucket")


# ---------------------------------------------------------------------------
# Iceberg table materialization (via pyiceberg, with the full-access user)
# ---------------------------------------------------------------------------


def _materialize_iceberg_table(server, namespace, table, rows):
    warehouse = f"s3://{server.bucket}/wh"
    sqlite_dir = tempfile.mkdtemp(prefix="pgl_moto_cat_")
    catalog = SqlCatalog(
        "moto_materialize",
        **{
            "uri": f"sqlite:///{sqlite_dir}/catalog.db",
            "warehouse": warehouse,
            "s3.endpoint": server.endpoint_url,
            "s3.access-key-id": server.root_user,
            "s3.secret-access-key": server.root_password,
            "s3.region": server.region,
            "s3.path-style-access": "true",
        },
    )
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    schema = pa.schema([("id", pa.int64()), ("val", pa.string())])
    data = pa.table(
        {"id": list(range(rows)), "val": [str(i) for i in range(rows)]},
        schema=schema,
    )
    tbl = catalog.create_table(f"{namespace}.{table}", schema=schema)
    tbl.append(data)

    metadata_location = tbl.metadata_location
    key = metadata_location.split(f"s3://{server.bucket}/", 1)[1]
    key_prefix = key.split("/metadata/", 1)[0]
    table_location = f"s3://{server.bucket}/{key_prefix}"
    return metadata_location, table_location, key_prefix


# ---------------------------------------------------------------------------
# Mock Iceberg REST catalog
# ---------------------------------------------------------------------------


def _read_metadata_document(server, metadata_location):
    """Read a table's metadata.json with the full-access principal.

    The catalog has its own access to the store, so this stays valid once
    enforcement is on.
    """
    key = metadata_location.split(f"s3://{server.bucket}/", 1)[1]
    client = server.client(server.root_user, server.root_password)
    return json.loads(client.get_object(Bucket=server.bucket, Key=key)["Body"].read())


def _make_handler(tables, server):
    """Build a mock REST catalog handler.

    ``tables`` maps table-name -> dict with:
        metadata_location : str
        location          : str
        vended            : optional dict {"prefix": str, "config": dict}
        vended_by_client  : optional dict client_id -> {"prefix", "config"},
                            for catalogs that vend per principal

    Tokens carry the client_id that asked for them, which is how a
    loadTable knows which principal it is answering.

    loadTable inlines the table's real metadata document, as the Iceberg
    REST spec requires and as Polaris and Horizon both do.  It is what lets
    a columnless CREATE TABLE learn the schema without reaching for
    storage, so a stub here would hide whether that works.
    """

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length > 0 else b""

            if "/oauth/tokens" in self.path:
                form = urllib.parse.parse_qs(body.decode())
                client_id = (form.get("client_id") or [""])[0]
                auth = self.headers.get("Authorization", "")
                if not client_id and auth.lower().startswith("basic "):
                    decoded = base64.b64decode(auth.split(" ", 1)[1]).decode()
                    client_id = decoded.split(":", 1)[0]
                self._json(
                    {
                        "access_token": f"{client_id}.{uuid.uuid4().hex}",
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                )
                return

            if "/namespaces/" in self.path and self.command == "HEAD":
                self.send_response(204)
                self.end_headers()
                return

            if (
                "/namespaces/" in self.path
                and "/tables" not in self.path
                and self.command == "GET"
            ):
                ns = self.path.rstrip("/").split("/namespaces/", 1)[1].split("?")[0]
                self._json({"namespace": [ns], "properties": {}})
                return

            if "/tables/" in self.path and self.command == "GET":
                name = self.path.rstrip("/").split("/tables/", 1)[1].split("?")[0]
                info = tables.get(name)
                if info is None:
                    self._error()
                    return

                delegation = self.headers.get("X-Iceberg-Access-Delegation", "")
                resp = {
                    "metadata-location": info["metadata_location"],
                    "metadata": _read_metadata_document(
                        server, info["metadata_location"]
                    ),
                }

                vended = info.get("vended")
                by_client = info.get("vended_by_client")
                if by_client:
                    token = self.headers.get("Authorization", "").split(" ")[-1]
                    vended = by_client.get(token.split(".", 1)[0])

                if delegation == "vended-credentials" and vended:
                    resp["storage-credentials"] = [
                        {"prefix": vended["prefix"], "config": vended["config"]}
                    ]
                self._json(resp)
                return

            self._error()

        def _json(self, obj):
            body = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def _error(self):
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                b'{"error": {"message": "not found", '
                b'"type": "NoSuchTableException", "code": 404}}'
            )

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


_CATALOG_GUCS = (
    "pg_lake_iceberg.rest_catalog_host",
    "pg_lake_iceberg.rest_catalog_client_id",
    "pg_lake_iceberg.rest_catalog_client_secret",
    "pg_lake_iceberg.rest_catalog_enable_vended_credentials",
)


def _start_mock_catalog(tables, server):
    """Start the mock catalog, leaving it to the caller to point at it."""
    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), _make_handler(tables, server))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, port


def _serve_mock_catalog(tables, server, enable_vended, conn=None):
    """Start the mock catalog and point the REST GUCs at it.

    ``conn`` is the connection about to use the catalog.  ALTER SYSTEM alone
    is not enough for it: the SIGHUP it triggers is processed at an
    unpredictable command boundary, so the very next statement may still run
    against the previous test's (reset) settings.  A session-level SET on
    that connection takes effect immediately.
    """
    httpd, thread, port = _start_mock_catalog(tables, server)

    settings = {
        "pg_lake_iceberg.rest_catalog_host": f"http://127.0.0.1:{port}",
        "pg_lake_iceberg.rest_catalog_client_id": "test_id",
        "pg_lake_iceberg.rest_catalog_client_secret": "test_secret",
        "pg_lake_iceberg.rest_catalog_enable_vended_credentials": (
            "true" if enable_vended else "false"
        ),
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


def _stop_mock_catalog(httpd, thread, conn=None):
    httpd.shutdown()
    thread.join(timeout=5)
    run_command_outside_tx(
        [f"ALTER SYSTEM RESET {name}" for name in _CATALOG_GUCS]
        + ["SELECT pg_reload_conf()"]
    )
    if conn is not None:
        for name in _CATALOG_GUCS:
            run_command(f"RESET {name}", conn)
        conn.commit()


def _create_static_secret(pgduck_conn, server, access_key, secret_key):
    """The pre-existing static secret that points pgduck at this moto server.

    Scoped to the whole test bucket, so vended secrets (more specific
    per-table scope) inherit ENDPOINT / URL_STYLE / USE_SSL from it.
    """
    run_command(
        f"""
        CREATE OR REPLACE SECRET s3motoproto (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{server.endpoint}',
            SCOPE 's3://{server.bucket}',
            URL_STYLE 'path',
            USE_SSL false
        )
        """,
        pgduck_conn,
    )
    pgduck_conn.commit()


def _denied(text):
    if text is None:
        return False
    return "Access Denied" in text or "AccessDenied" in text or "HTTP 403" in text


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _reader(server, name, prefixes):
    """A read-only principal restricted to ``prefixes`` inside the bucket.

    Returns the ``(access_key_id, secret_access_key)`` moto generated for it.
    """
    return server.create_scoped_user(name, prefixes, actions=_READ_ACTIONS)


def _vended(location, credential, region, suffix="/"):
    """The storage-credentials entry a catalog returns for ``location``."""
    return {
        "prefix": location + suffix,
        "config": {
            "s3.access-key-id": credential[0],
            "s3.secret-access-key": credential[1],
            "client.region": region,
        },
    }


def _vended_secrets_for(pgduck_conn, location):
    rows = run_query(
        "SELECT name, scope FROM duckdb_secrets() WHERE name LIKE 'pglake_vended_%'",
        pgduck_conn,
    )
    pgduck_conn.commit()
    return [r for r in rows if r[1] and location in r[1]]


def test_moto_vended_credentials_required_for_scan(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_required"
    table = "vc_ok"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # The static credential can read *metadata* only; the vended credential
    # can read the whole table prefix (metadata + data).
    static = _reader(server, "meta_ok", [f"{prefix}/metadata"])
    data = _reader(server, "data_ok", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": _vended(location, data, server.region),
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 10

        # The vended secret must have been pushed to pgduck with the table's
        # storage prefix as its scope.
        assert _vended_secrets_for(
            pgduck_conn, location
        ), f"expected a pglake_vended_* secret scoped to {location}"

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_scan_denied_without_vended_credentials(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_denied"
    table = "vc_no"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)
    static = _reader(server, "meta_nv", [f"{prefix}/metadata"])

    # No "vended" entry -> the catalog returns no credentials even though the
    # delegation header is sent.
    tables = {
        table: {"metadata_location": meta_loc, "location": location, "vended": None}
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        # The metadata read (static secret) succeeds; the data scan falls back
        # to the metadata-only static secret and moto denies it.
        err = run_query(
            f"SELECT count(*) FROM {schema}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert _denied(err), f"expected an access-denied error, got: {err!r}"

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_vended_credentials_wrong_scope_denied(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """A credential labeled with a foreign prefix must not be trusted there.

    We clamp a scope that falls outside the table root back to the table
    root, so this secret *is* applied to this table's data path -- and then
    the store denies it, because the credential has no rights there.
    Clamping keeps a mislabeled credential from registering under the
    foreign scope, where it would shadow the secret that prefix depends on.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_scope"
    table = "vc_wrongscope"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)
    static = _reader(server, "meta_ws", [f"{prefix}/metadata"])

    # The vended credential belongs to a different table: it can read
    # wh/some-other-table and nothing else.  The catalog labels it with that
    # same foreign prefix.
    data = _reader(server, "data_ws", ["wh/some-other-table"])

    wrong_scope = f"s3://{server.bucket}/wh/some-other-table/"
    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {
                "prefix": wrong_scope,
                "config": {
                    "s3.access-key-id": data[0],
                    "s3.secret-access-key": data[1],
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        err = run_query(
            f"SELECT count(*) FROM {schema}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert _denied(err), f"expected an access-denied error, got: {err!r}"

        # Nothing may be registered under the foreign scope.  A secret there
        # would be selected for wh/some-other-table's own scans and shadow
        # whatever that table legitimately uses.
        shadowing = _vended_secrets_for(pgduck_conn, "some-other-table")
        assert not shadowing, f"secret registered under a foreign scope: {shadowing}"

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_columnless_attach_does_not_read_storage(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """Attaching a table without naming its columns must not touch storage.

    ``CREATE TABLE t ()`` is how an existing catalog table is normally
    attached; spelling out its columns is the workaround.  Inference used to
    read metadata.json off the store, which cannot work on a catalog that
    only vends: the relation does not exist yet, and credentials are
    resolved per relation, so nothing can have pushed a secret for it.

    Here the static credential is barred from the table entirely, so a
    CREATE that reaches for storage is denied.  Vending then covers the
    whole table root, which is what makes the scan afterwards work.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_attach"
    table = "vc_attach"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # Reaches somewhere else in the bucket, and nothing of this table.
    static = _reader(server, "static_at", ["elsewhere"])
    data = _reader(server, "data_at", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": _vended(location, data, server.region),
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()

        err = run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
            raise_error=False,
        )
        assert not _denied(err), f"columnless attach went to storage: {err!r}"
        assert err is None, f"columnless attach failed: {err!r}"
        superuser_conn.commit()

        columns = run_query(
            f"""SELECT attname, atttypid::regtype::text
                  FROM pg_attribute
                 WHERE attrelid = '{schema}.{table}'::regclass AND attnum > 0
                 ORDER BY attnum""",
            superuser_conn,
        )
        superuser_conn.commit()
        assert [tuple(c) for c in columns] == [("id", "bigint"), ("val", "text")]

        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 10

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_vended_secret_dropped_when_revoked(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """A stale credential cannot linger once the catalog stops vending it.

    With vending on, a scan pushes the data-capable secret and succeeds.
    Vending is then turned off; the next scan on the same backend resolves
    no credentials, *drops* the secret it previously pushed, and the data
    scan is denied.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_revoke"
    table = "vc_revoke"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)
    static = _reader(server, "meta_rv", [f"{prefix}/metadata"])
    data = _reader(server, "data_rv", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": _vended(location, data, server.region),
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        # Step 1: vending on -> scan succeeds and the secret is pushed.
        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 10
        assert _vended_secrets_for(
            pgduck_conn, location
        ), "expected a vended secret to be pushed while vending is on"

        # Step 2: revoke by turning vending off for this session.  A
        # session-level SET (PGC_SUSET) takes effect on the very next query in
        # this same backend -- deterministically, unlike an ALTER SYSTEM +
        # pg_reload_conf whose SIGHUP is only processed at the *following*
        # command boundary.
        run_command(
            "SET pg_lake_iceberg.rest_catalog_enable_vended_credentials = false",
            superuser_conn,
        )
        superuser_conn.commit()

        # The scan that succeeded above is now DENIED.  This is the
        # authoritative proof that the resolver dropped the secret it had
        # pushed: if the drop had not taken effect, the still-present
        # data-capable vended secret would have served this identical read.
        # (We deliberately assert on the scan result rather than on a peer
        # connection's duckdb_secrets() view, whose reflection of a drop made
        # on another pgduck connection is not deterministic.)
        err = run_query(
            f"SELECT count(*) FROM {schema}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert _denied(err), f"expected access-denied after revocation, got: {err!r}"

    finally:
        server.relax()
        run_command(
            "RESET pg_lake_iceberg.rest_catalog_enable_vended_credentials",
            superuser_conn,
        )
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_vended_secret_dropped_with_read_only_table(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """A read-only table's secret goes away with the table.

    A read-only table owns none of the files it reads, so its drop queues no
    deletes and nothing is left for the secret to authorize.  Leaving it
    behind would not be harmless: secrets outlive the backend that pushed
    them and DuckDB picks one by longest matching scope, so a leftover would
    keep answering for that prefix -- with credentials that expire -- for
    the rest of the server's life.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_ro_drop"
    table = "vc_ro_drop"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)
    static = _reader(server, "meta_rd", [f"{prefix}/metadata"])
    data = _reader(server, "data_rd", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": _vended(location, data, server.region),
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 10
        assert _vended_secrets_for(
            pgduck_conn, location
        ), "expected the scan to push a vended secret before the drop"

        run_command(f"DROP TABLE {schema}.{table}", superuser_conn)
        superuser_conn.commit()

        leftover = _vended_secrets_for(pgduck_conn, location)
        assert not leftover, f"vended secret outlived its read-only table: {leftover}"

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_two_principals_get_their_own_credentials(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """Two roles reading one table are each served their own credential.

    A catalog vends per principal, so a backend that changes role must not
    hand the second role what the first was given.  The two vended
    credentials here differ in what they can actually do -- one reads the
    table's data, the other only its metadata -- so reusing the first one is
    not a subtle difference: the second scan would succeed where the store
    must deny it.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_two_princ"
    table = "vc_two_princ"
    fdw_server = "mvm_two_princ_srv"
    role_a = "mvm_princ_a"
    role_b = "mvm_princ_b"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # The static secret reaches metadata only, so any successful data scan
    # below has to come from a vended credential.
    static = _reader(server, "meta_2p", [f"{prefix}/metadata"])
    data = _reader(server, "data_2p", [prefix])

    data_vended = _vended(location, data, server.region)
    meta_vended = _vended(location, static, server.region)

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended_by_client": {
                "princ_root": data_vended,
                "princ_a": data_vended,
                "princ_b": meta_vended,
            },
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread, port = _start_mock_catalog(tables, server)
    server.enforce()

    try:
        run_command(f"DROP SERVER IF EXISTS {fdw_server} CASCADE", superuser_conn)
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        run_command(
            f"""CREATE SERVER {fdw_server} TYPE 'rest'
                    FOREIGN DATA WRAPPER iceberg_catalog
                    OPTIONS (rest_endpoint 'http://127.0.0.1:{port}',
                             enable_vended_credentials 'true',
                             location_prefix 's3://{server.bucket}')""",
            superuser_conn,
        )
        for role, client_id in (
            (None, "princ_root"),
            (role_a, "princ_a"),
            (role_b, "princ_b"),
        ):
            target = "CURRENT_USER" if role is None else role
            if role is not None:
                run_command(f"DROP ROLE IF EXISTS {role}", superuser_conn)
                run_command(f"CREATE ROLE {role}", superuser_conn)
                run_command(f"GRANT lake_read TO {role}", superuser_conn)
            run_command(
                f"""CREATE USER MAPPING FOR {target} SERVER {fdw_server}
                        OPTIONS (client_id '{client_id}',
                                 client_secret '{client_id}_secret')""",
                superuser_conn,
            )
        superuser_conn.commit()

        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='{fdw_server}', read_only=True,
                      catalog_table_name='{table}')""",
            superuser_conn,
        )
        run_command(
            f"GRANT USAGE ON SCHEMA {schema} TO {role_a}, {role_b}", superuser_conn
        )
        run_command(
            f"GRANT SELECT ON {schema}.{table} TO {role_a}, {role_b}", superuser_conn
        )
        superuser_conn.commit()

        # The role whose principal is vended a data-capable credential.
        run_command(f"SET ROLE {role_a}", superuser_conn)
        rows = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert rows[0][0] == 10

        # Same backend, same table, different principal -- and this one is
        # vended a credential that cannot read data.  Reusing what was
        # resolved for the first role would let this through.
        run_command(f"SET ROLE {role_b}", superuser_conn)
        err = run_query(
            f"SELECT count(*) FROM {schema}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert _denied(err), (
            "the second role was served the first role's credential: its "
            f"scan should have been denied, got: {err!r}"
        )

    finally:
        server.relax()
        superuser_conn.rollback()
        run_command("RESET ROLE", superuser_conn)
        superuser_conn.commit()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"DROP SERVER IF EXISTS {fdw_server} CASCADE", superuser_conn)
        superuser_conn.commit()
        for role in (role_a, role_b):
            run_command(f"DROP OWNED BY {role}", superuser_conn)
            run_command(f"DROP ROLE IF EXISTS {role}", superuser_conn)
        superuser_conn.commit()
        httpd.shutdown()
        thread.join(timeout=5)


@pytest.mark.parametrize("vended_config_keys", ["endpoint_only", "endpoint_and_style"])
def test_moto_catalog_supplied_endpoint_still_reaches_the_store(
    superuser_conn,
    pgduck_conn,
    extension,
    installcheck,
    moto_enforcing_server,
    vended_config_keys,
):
    """A catalog that states its endpoint must still produce a usable secret.

    Stating an endpoint is what an S3-compatible deployment does; stating
    the addressing style as well is optional, and most catalogs leave it
    out.  Whatever the catalog does not say has to keep coming from the
    secret that already serves the prefix, because a vended secret with an
    endpoint but no URL_STYLE sends DuckDB to ``<bucket>.<host>``, which
    resolves nowhere.  Only a real scan shows this: the secret is created
    either way and looks right in ``duckdb_secrets()``.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_endpoint"
    table = f"vc_ep_{vended_config_keys}"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)
    static = _reader(server, f"meta_ep_{vended_config_keys}", [f"{prefix}/metadata"])
    data = _reader(server, f"data_ep_{vended_config_keys}", [prefix])

    vended = _vended(location, data, server.region)
    vended["config"]["s3.endpoint"] = server.endpoint_url
    if vended_config_keys == "endpoint_and_style":
        vended["config"]["s3.path-style-access"] = "true"

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": vended,
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 10

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_data_only_credential_serves_repeated_scans(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """A credential covering only the data files still serves every scan.

    Iceberg metadata lives under the table root, alongside the data, so a
    credential the catalog labels ``.../data/`` must not become the one
    DuckDB picks for the metadata beside it.  Scanning twice is the point:
    the first scan pushes the secret, and the second is the one that would
    read metadata through it if the scope were widened to the table root.
    Here the data credential cannot read metadata and the metadata
    credential cannot read data, so a query only succeeds while both are
    being applied to their own prefix.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_data_only"
    table = "vc_data_only"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)
    static = _reader(server, "meta_do", [f"{prefix}/metadata"])
    data = _reader(server, "data_do", [f"{prefix}/data"])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            # narrower than the table root, and preserved as such
            "vended": _vended(location, data, server.region, suffix="/data/"),
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        for attempt in ("first", "second"):
            result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
            superuser_conn.commit()
            assert result[0][0] == 10, f"{attempt} scan did not read the data files"

        scopes = [s[1] for s in _vended_secrets_for(pgduck_conn, location)]
        assert scopes, "expected a vended secret for this table"
        assert all(
            "/data/" in s for s in scopes
        ), f"data-only scope was widened to the table root: {scopes}"

    finally:
        server.relax()
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


def test_moto_unrelated_table_still_reads_through_the_static_secret(
    superuser_conn, pgduck_conn, extension, installcheck, moto_enforcing_server
):
    """Vending for one table leaves every other path alone.

    The vended secret is the most specific match for its own prefix, and for
    nothing else.  A plain parquet table sitting elsewhere in the same bucket
    has to keep reading through the static secret it always used --
    otherwise turning vending on for one Iceberg table would break unrelated
    tables across the whole deployment.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = moto_enforcing_server
    schema = "mvm_unrelated"
    table = "vc_unrelated"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # A plain parquet file outside the Iceberg table's prefix, written with
    # the full-access principal so its contents do not depend on this test's
    # scoped users.
    outside_key = "outside/plain/rows.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        pq.write_table(pa.table({"id": list(range(4))}), tmp.name)
        server.client(server.root_user, server.root_password).upload_file(
            tmp.name, server.bucket, outside_key
        )
    outside_url = f"s3://{server.bucket}/{outside_key}"

    # The static credential reads the Iceberg metadata and the unrelated
    # file; the vended one reads only the Iceberg table's own prefix.
    static = _reader(server, "meta_un", [f"{prefix}/metadata", "outside"])
    data = _reader(server, "data_un", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": _vended(location, data, server.region),
        }
    }

    _create_static_secret(pgduck_conn, server, static[0], static[1])
    httpd, thread = _serve_mock_catalog(
        tables, server, enable_vended=True, conn=superuser_conn
    )
    server.enforce()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
            superuser_conn,
        )
        run_command(
            f"""CREATE FOREIGN TABLE {schema}.plain ()
                SERVER pg_lake OPTIONS (path '{outside_url}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        assert (
            run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)[0][0]
            == 10
        )
        superuser_conn.commit()

        # The vended secret is now on the server; the unrelated path must
        # still be served by the static secret.
        result = run_query(f"SELECT count(*) FROM {schema}.plain", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 4, "unrelated parquet table stopped reading"

    finally:
        server.relax()
        run_command(f"DROP FOREIGN TABLE IF EXISTS {schema}.plain", superuser_conn)
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)

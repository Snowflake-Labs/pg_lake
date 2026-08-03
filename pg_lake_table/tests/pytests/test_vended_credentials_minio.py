"""Enforcement tests for REST-catalog vended credentials, backed by MinIO.

Unlike the Moto-backed tests (which cannot deny a request), these tests
use a real MinIO server with a real policy engine so we can prove that the
vended credential is genuinely *load-bearing* for a data scan:

  * ``test_minio_vended_credentials_required_for_scan``
        With vending enabled, the catalog vends a data-scoped credential;
        pg_lake pushes it to pgduck_server as a ``pglake_vended_*`` secret
        and the SELECT succeeds.  The pre-existing static secret is scoped
        to *metadata only*, so the scan can only succeed via the vended
        credential.

  * ``test_minio_scan_denied_without_vended_credentials``
        With vending disabled (catalog returns no credentials), the data
        scan falls back to the metadata-only static secret and MinIO
        denies it -- proving the static secret alone is insufficient.

  * ``test_minio_vended_credentials_wrong_scope_denied``
        The catalog vends a credential scoped to the *wrong* prefix, so
        DuckDB never applies it to the target table's data path and the
        scan is denied -- proving the scope string must be correct.

  * ``test_minio_vended_secret_dropped_when_revoked``
        With vending on, a scan pushes the data-capable secret and
        succeeds.  Vending is then turned off (the catalog stops
        delegating); the next scan on the same backend resolves no
        credentials, *drops* the secret it previously pushed, and the data
        scan is denied.  This proves the resolver's headline behavior: a
        stale credential cannot linger on the shared pgduck_server once the
        catalog stops vending it.

The whole module is skipped when MinIO (server binary + Python admin SDK)
is not available, mirroring the skip-if-absent pattern of the e2e suites.

Architecture recap (why the static/vended split works):
  - REST loadTable runs in PostgreSQL over HTTP (mock catalog here).
  - Iceberg metadata (metadata.json + manifest Avro) is read on
    pgduck_server using the pre-existing static S3 secret.
  - The parquet data scan runs on pgduck_server using the scoped
    ``pglake_vended_*`` secret (falling back to the static secret when no
    vended credential is cached).
  - The vended secret inherits ENDPOINT/URL_STYLE/USE_SSL from the static
    secret whose scope best-matches the vended scope, so pointing the
    static secret at MinIO is what makes vended secrets reach MinIO too.
"""

import json
import socket
import tempfile
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils_pytest import *

try:
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    _HAVE_PYICEBERG = True
except Exception as _exc:  # pragma: no cover - depends on environment
    _HAVE_PYICEBERG = False


# ---------------------------------------------------------------------------
# Iceberg table materialization on MinIO (via pyiceberg)
# ---------------------------------------------------------------------------


def _materialize_iceberg_table(server, namespace, table, rows):
    """Create a real Iceberg table with ``rows`` rows on MinIO.

    Returns ``(metadata_location, table_location, key_prefix)`` where
    ``key_prefix`` is the table's key prefix inside the bucket (e.g.
    ``wh/ns/tbl``).
    """
    warehouse = f"s3://{server.bucket}/wh"
    sqlite_dir = tempfile.mkdtemp(prefix="pgl_minio_cat_")
    catalog = SqlCatalog(
        "minio_materialize",
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
# Mock Iceberg REST catalog (data-driven by a table -> response map)
# ---------------------------------------------------------------------------


def _make_handler(tables):
    """Build a mock REST catalog handler.

    ``tables`` maps table-name -> dict with:
        metadata_location : str
        location          : str
        vended            : optional dict {"prefix": str, "config": dict}
    """

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length", 0))
            if length > 0:
                self.rfile.read(length)

            if "/oauth/tokens" in self.path:
                self._json(
                    {
                        "access_token": uuid.uuid4().hex,
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                )
                return

            if "/namespaces/" in self.path and self.command == "HEAD":
                self.send_response(204)
                self.end_headers()
                return

            # Namespace-exists check (GET .../namespaces/<ns>) done during
            # read-only CREATE TABLE: report the namespace as present.
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
                    "metadata": {
                        "format-version": 2,
                        "table-uuid": str(uuid.uuid4()),
                        "location": info["location"],
                    },
                }
                if delegation == "vended-credentials" and info.get("vended"):
                    resp["storage-credentials"] = [
                        {
                            "prefix": info["vended"]["prefix"],
                            "config": info["vended"]["config"],
                        }
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


def _serve_mock_catalog(tables, enable_vended):
    """Start the mock catalog and point the REST GUCs at it."""
    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), _make_handler(tables))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO 'http://127.0.0.1:{port}'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO 'test_id'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO 'test_secret'",
            "ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_enable_vended_credentials TO "
            + ("'true'" if enable_vended else "'false'"),
            "SELECT pg_reload_conf()",
        ]
    )
    return httpd, thread


def _stop_mock_catalog(httpd, thread):
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


def _create_static_minio_secret(pgduck_conn, server, access_key, secret_key):
    """Create the pre-existing static S3 secret that points pgduck at MinIO.

    Its scope covers the whole test bucket, so vended secrets (which have a
    more specific per-table scope) inherit MinIO's ENDPOINT/URL_STYLE/SSL
    from it.
    """
    run_command(
        f"""
        CREATE OR REPLACE SECRET s3minio (
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


@requires_minio
def test_minio_vended_credentials_required_for_scan(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_required"
    table = "vc_ok"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # Static credential can read *metadata* only; vended credential can read
    # the whole table prefix (metadata + data).
    server.create_scoped_user(
        "mv_meta_ok",
        "mv_meta_ok_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user("mv_data_ok", "mv_data_ok_secret", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {
                "prefix": location + "/",
                "config": {
                    "s3.access-key-id": "mv_data_ok",
                    "s3.secret-access-key": "mv_data_ok_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_ok", "mv_meta_ok_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True)

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
        secrets = run_query(
            "SELECT name, scope FROM duckdb_secrets() WHERE name LIKE 'pglake_vended_%'",
            pgduck_conn,
        )
        assert any(
            s[1] and location in s[1] for s in secrets
        ), f"expected a pglake_vended_* secret scoped to {location}, got {secrets}"

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread)


@requires_minio
def test_minio_scan_denied_without_vended_credentials(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_denied"
    table = "vc_novend"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    server.create_scoped_user(
        "mv_meta_nv",
        "mv_meta_nv_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )

    # No "vended" entry -> catalog returns no credentials even though the
    # delegation header is sent.
    tables = {
        table: {"metadata_location": meta_loc, "location": location, "vended": None}
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_nv", "mv_meta_nv_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True)

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

        # Metadata read (static secret) succeeds; the data scan falls back to
        # the metadata-only static secret and MinIO denies it.
        err = run_query(
            f"SELECT count(*) FROM {schema}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert _denied(err), f"expected an access-denied error, got: {err!r}"

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread)


@requires_minio
def test_minio_vended_secret_dropped_when_revoked(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_revoke"
    table = "vc_revoke"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # metadata-only static credential; data-capable vended credential.
    server.create_scoped_user(
        "mv_meta_rv",
        "mv_meta_rv_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user("mv_data_rv", "mv_data_rv_secret", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {
                "prefix": location + "/",
                "config": {
                    "s3.access-key-id": "mv_data_rv",
                    "s3.secret-access-key": "mv_data_rv_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_rv", "mv_meta_rv_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True)

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

        def _vended_secrets_for_this_table():
            rows = run_query(
                "SELECT name, scope FROM duckdb_secrets() "
                "WHERE name LIKE 'pglake_vended_%'",
                pgduck_conn,
            )
            return [r for r in rows if r[1] and location in r[1]]

        assert (
            _vended_secrets_for_this_table()
        ), "expected a vended secret to be pushed while vending is on"

        # Step 2: revoke by turning vending off for this session.  A
        # session-level SET (PGC_SUSET) takes effect on the very next query
        # in this same backend -- deterministically, unlike an ALTER SYSTEM
        # + pg_reload_conf whose SIGHUP is only processed at the *following*
        # command boundary.  The next scan then resolves no credentials and
        # must drop the secret it pushed above, leaving only the
        # metadata-only static secret so MinIO denies the data scan.
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
        # connection's duckdb_secrets() view, whose reflection of a drop
        # made on another pgduck connection is not deterministic.)
        err = run_query(
            f"SELECT count(*) FROM {schema}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert _denied(err), f"expected access-denied after revocation, got: {err!r}"

    finally:
        run_command(
            "RESET pg_lake_iceberg.rest_catalog_enable_vended_credentials",
            superuser_conn,
        )
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread)


@requires_minio
def test_minio_vended_credentials_wrong_scope_denied(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_scope"
    table = "vc_wrongscope"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    server.create_scoped_user(
        "mv_meta_ws",
        "mv_meta_ws_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    # A data-capable credential exists, but it will be vended under the WRONG
    # scope, so DuckDB never applies it to this table's data path.
    server.create_scoped_user("mv_data_ws", "mv_data_ws_secret", [prefix])

    wrong_scope = f"s3://{server.bucket}/wh/some-other-table/"
    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {
                "prefix": wrong_scope,
                "config": {
                    "s3.access-key-id": "mv_data_ws",
                    "s3.secret-access-key": "mv_data_ws_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_ws", "mv_meta_ws_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True)

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

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread)

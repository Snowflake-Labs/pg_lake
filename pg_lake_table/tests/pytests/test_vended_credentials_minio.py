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


def _make_handler(tables, calls=None):
    """Build a mock REST catalog handler.

    ``tables`` maps table-name -> dict with:
        metadata_location : str
        location          : str
        assigns_location  : optional bool.  When set, the catalog manages
                            its own storage: a staged create is answered
                            with ``location`` whatever the client proposed,
                            the way a Snowflake-managed volume does.
        vended            : optional dict {"prefix": str, "config": dict}
        vended_by_client  : optional dict client_id -> {"prefix", "config"},
                            for catalogs that vend per principal

    Tokens carry the client_id that asked for them, which is how a
    loadTable knows which principal it is answering.

    ``calls`` , when given, collects what the catalog was asked to do, so a
    test can assert on requests that have no visible answer -- a drop, say.
    """

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length > 0 else b""

            if "/oauth/tokens" in self.path:
                # The client id arrives either as a form field or as HTTP
                # basic auth, depending on how the client was configured.
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

                self._json(self._load_table_response(info, info["location"]))
                return

            # Staged create of a writable table.  A catalog that manages its
            # own storage answers with the location it assigned; one that
            # does not takes the location the client proposed.
            if self.path.rstrip("/").endswith("/tables") and self.command == "POST":
                request = json.loads(body or b"{}")
                info = tables.get(request.get("name"))
                if info is None:
                    self._error()
                    return

                location = (
                    info["location"]
                    if info.get("assigns_location")
                    else request.get("location", info["location"])
                )
                if calls is not None:
                    calls.setdefault("proposed_locations", []).append(
                        request.get("location")
                    )
                self._json(self._load_table_response(info, location))
                return

            # Finishing the staged create, and committing snapshots: the
            # catalog records them, and has nothing to say back.
            if self.command == "POST" and (
                "/tables/" in self.path or "/transactions/commit" in self.path
            ):
                self._json({})
                return

            if "/tables/" in self.path and self.command == "DELETE":
                if calls is not None:
                    calls.setdefault("drops", []).append(self.path)
                self.send_response(204)
                self.end_headers()
                return

            self._error()

        def _load_table_response(self, info, location):
            resp = {
                "metadata-location": info["metadata_location"],
                "metadata": {
                    "format-version": 2,
                    "table-uuid": str(uuid.uuid4()),
                    "location": location,
                },
            }

            vended = info.get("vended")
            by_client = info.get("vended_by_client")
            if by_client:
                token = self.headers.get("Authorization", "").split(" ")[-1]
                vended = by_client.get(token.split(".", 1)[0])

            delegation = self.headers.get("X-Iceberg-Access-Delegation", "")
            if delegation == "vended-credentials" and vended:
                resp["storage-credentials"] = [
                    {"prefix": vended["prefix"], "config": vended["config"]}
                ]

            return resp

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


def _start_mock_catalog(tables, calls=None):
    """Start the mock catalog, leaving it to the caller to point at it."""
    port = _find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), _make_handler(tables, calls))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, port


_CATALOG_GUCS = (
    "pg_lake_iceberg.rest_catalog_host",
    "pg_lake_iceberg.rest_catalog_client_id",
    "pg_lake_iceberg.rest_catalog_client_secret",
    "pg_lake_iceberg.rest_catalog_enable_vended_credentials",
)


def _serve_mock_catalog(tables, enable_vended, conn=None, calls=None):
    """Start the mock catalog and point the REST GUCs at it.

    ``conn`` is the connection about to use the catalog.  ALTER SYSTEM
    alone is not enough for it: the SIGHUP it triggers is processed at an
    unpredictable command boundary, so the very next statement may still
    run against the previous test's (reset) settings.  A session-level
    SET on that connection takes effect immediately.
    """
    httpd, thread, port = _start_mock_catalog(tables, calls)

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


def _bucket_keys(server):
    """Every object key currently in the test bucket."""
    paginator = server.client().get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=server.bucket)
        for obj in page.get("Contents", [])
    ]


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
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


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
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


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
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


@requires_minio
def test_minio_two_principals_get_their_own_credentials(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    """Two roles reading one table are each served their own credential.

    A catalog vends per principal, so a backend that changes role must
    not hand the second role what the first was given.  The two vended
    credentials here differ in what they can actually do -- one reads the
    table's data, the other only its metadata -- so reusing the first
    one is not a subtle difference: the second scan would succeed where
    MinIO must deny it.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_two_princ"
    table = "vc_two_princ"
    fdw_server = "mv_two_princ_srv"
    role_a = "mv_princ_a"
    role_b = "mv_princ_b"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # The static secret reaches metadata only, so any successful data scan
    # below has to come from a vended credential.
    server.create_scoped_user(
        "mv_meta_2p",
        "mv_meta_2p_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user("mv_data_2p", "mv_data_2p_secret", [prefix])

    data_config = {
        "s3.access-key-id": "mv_data_2p",
        "s3.secret-access-key": "mv_data_2p_secret",
        "client.region": server.region,
    }
    meta_config = {
        "s3.access-key-id": "mv_meta_2p",
        "s3.secret-access-key": "mv_meta_2p_secret",
        "client.region": server.region,
    }

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended_by_client": {
                "princ_root": {"prefix": location + "/", "config": data_config},
                "princ_a": {"prefix": location + "/", "config": data_config},
                "princ_b": {"prefix": location + "/", "config": meta_config},
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_2p", "mv_meta_2p_secret")
    httpd, thread, port = _start_mock_catalog(tables)

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


@requires_minio
def test_minio_vended_secret_dropped_with_read_only_table(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    """A read-only table's secret goes away with the table.

    A read-only table owns none of the files it reads, so its drop queues
    no deletes and nothing is left for the secret to authorize.  Leaving
    it behind would not be harmless: secrets outlive the backend that
    pushed them and DuckDB picks one by longest matching scope, so a
    leftover would keep answering for that prefix -- with credentials
    that expire -- for the rest of the server's life.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_ro_drop"
    table = "vc_ro_drop"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    server.create_scoped_user(
        "mv_meta_rd",
        "mv_meta_rd_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user("mv_data_rd", "mv_data_rd_secret", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {
                "prefix": location + "/",
                "config": {
                    "s3.access-key-id": "mv_data_rd",
                    "s3.secret-access-key": "mv_data_rd_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_rd", "mv_meta_rd_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

    def _vended_secrets_for_this_table():
        rows = run_query(
            "SELECT name, scope FROM duckdb_secrets() WHERE name LIKE 'pglake_vended_%'",
            pgduck_conn,
        )
        pgduck_conn.commit()
        return [r for r in rows if r[1] and location in r[1]]

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
        assert (
            _vended_secrets_for_this_table()
        ), "expected the scan to push a vended secret before the drop"

        run_command(f"DROP TABLE {schema}.{table}", superuser_conn)
        superuser_conn.commit()

        leftover = _vended_secrets_for_this_table()
        assert not leftover, f"vended secret outlived its read-only table: {leftover}"

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


@requires_minio
@pytest.mark.parametrize("vended_config_keys", ["endpoint_only", "endpoint_and_style"])
def test_minio_catalog_supplied_endpoint_still_reaches_the_store(
    superuser_conn,
    pgduck_conn,
    extension,
    installcheck,
    minio_server,
    vended_config_keys,
):
    """A catalog that states its endpoint must still produce a usable secret.

    Stating an endpoint is what an S3-compatible deployment does; stating
    the addressing style as well is optional, and most catalogs leave it
    out.  Whatever the catalog does not say has to keep coming from the
    secret that already serves the prefix, because a vended secret with
    an endpoint but no URL_STYLE sends DuckDB to ``<bucket>.<host>``,
    which resolves nowhere.  Only a real scan shows this: the secret is
    created either way and looks right in ``duckdb_secrets()``.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_endpoint"
    table = f"vc_ep_{vended_config_keys}"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    server.create_scoped_user(
        f"mv_meta_ep_{vended_config_keys}",
        "mv_meta_ep_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user(
        f"mv_data_ep_{vended_config_keys}", "mv_data_ep_secret", [prefix]
    )

    vended_config = {
        "s3.access-key-id": f"mv_data_ep_{vended_config_keys}",
        "s3.secret-access-key": "mv_data_ep_secret",
        "client.region": server.region,
        "s3.endpoint": server.endpoint_url,
    }
    if vended_config_keys == "endpoint_and_style":
        vended_config["s3.path-style-access"] = "true"

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {"prefix": location + "/", "config": vended_config},
        }
    }

    _create_static_minio_secret(
        pgduck_conn, server, f"mv_meta_ep_{vended_config_keys}", "mv_meta_ep_secret"
    )
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


@requires_minio
def test_minio_data_only_credential_serves_repeated_scans(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
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

    server = minio_server
    schema = "mv_data_only"
    table = "vc_data_only"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    server.create_scoped_user(
        "mv_meta_do",
        "mv_meta_do_secret",
        [f"{prefix}/metadata"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user(
        "mv_data_do",
        "mv_data_do_secret",
        [f"{prefix}/data"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            # narrower than the table root, and preserved as such
            "vended": {
                "prefix": location + "/data/",
                "config": {
                    "s3.access-key-id": "mv_data_do",
                    "s3.secret-access-key": "mv_data_do_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_do", "mv_meta_do_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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

        scopes = [
            s[1]
            for s in run_query(
                "SELECT name, scope FROM duckdb_secrets() "
                "WHERE name LIKE 'pglake_vended_%'",
                pgduck_conn,
            )
            if s[1] and location in s[1]
        ]
        pgduck_conn.commit()
        assert scopes, "expected a vended secret for this table"
        assert all(
            "/data/" in s for s in scopes
        ), f"data-only scope was widened to the table root: {scopes}"

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


@requires_minio
def test_minio_unrelated_table_still_reads_through_the_static_secret(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    """Vending for one table leaves every other path alone.

    The vended secret is the most specific match for its own prefix, and
    for nothing else.  A plain parquet table sitting elsewhere in the same
    bucket has to keep reading through the static secret it always used --
    otherwise turning vending on for one Iceberg table would break
    unrelated tables across the whole deployment.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_unrelated"
    table = "vc_unrelated"

    meta_loc, location, prefix = _materialize_iceberg_table(server, schema, table, 10)

    # A plain parquet file outside the Iceberg table's prefix, written with
    # root credentials so its contents do not depend on this test's users.
    outside_key = "outside/plain/rows.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        pq.write_table(pa.table({"id": list(range(4))}), tmp.name)
        server.client().upload_file(tmp.name, server.bucket, outside_key)
    outside_url = f"s3://{server.bucket}/{outside_key}"

    # The static credential reads the Iceberg metadata and the unrelated
    # file; the vended one reads only the Iceberg table's own prefix.
    server.create_scoped_user(
        "mv_meta_un",
        "mv_meta_un_secret",
        [f"{prefix}/metadata", "outside"],
        actions=("s3:GetObject", "s3:ListBucket"),
    )
    server.create_scoped_user("mv_data_un", "mv_data_un_secret", [prefix])

    tables = {
        table: {
            "metadata_location": meta_loc,
            "location": location,
            "vended": {
                "prefix": location + "/",
                "config": {
                    "s3.access-key-id": "mv_data_un",
                    "s3.secret-access-key": "mv_data_un_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(pgduck_conn, server, "mv_meta_un", "mv_meta_un_secret")
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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
        run_command(f"DROP FOREIGN TABLE IF EXISTS {schema}.plain", superuser_conn)
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


@requires_minio
def test_minio_managed_catalog_places_and_serves_a_writable_table(
    superuser_conn, pgduck_conn, extension, installcheck, minio_server
):
    """A catalog that owns its storage decides where a new table goes.

    pg_lake proposes a location built from its own prefix; this catalog
    overrules it, the way a Snowflake-managed external volume does, and
    answers with the location it assigned plus credentials that reach only
    there.  The table has to end up in that location: the credentials do
    not reach anywhere else, and the prefix pg_lake would have picked is
    barred to it here, so writing to the wrong one cannot silently pass.
    """
    if installcheck or not _HAVE_PYICEBERG:
        return

    server = minio_server
    schema = "mv_managed"
    table = "vc_managed"

    managed_prefix = f"managed/{schema}/{table}"
    managed_location = f"s3://{server.bucket}/{managed_prefix}"
    client_prefix = "clientside"

    # The static credential reaches only the prefix pg_lake would have
    # chosen; the vended one reaches only the location the catalog assigned.
    server.create_scoped_user("mv_mg_static", "mv_mg_static_secret", [client_prefix])
    server.create_scoped_user("mv_mg_vended", "mv_mg_vended_secret", [managed_prefix])

    calls = {}
    tables = {
        table: {
            "metadata_location": f"{managed_location}/metadata/v1.metadata.json",
            "location": managed_location,
            "assigns_location": True,
            "vended": {
                "prefix": managed_location + "/",
                "config": {
                    "s3.access-key-id": "mv_mg_vended",
                    "s3.secret-access-key": "mv_mg_vended_secret",
                    "client.region": server.region,
                },
            },
        }
    }

    _create_static_minio_secret(
        pgduck_conn, server, "mv_mg_static", "mv_mg_static_secret"
    )
    httpd, thread = _serve_mock_catalog(
        tables, enable_vended=True, conn=superuser_conn, calls=calls
    )

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        run_command(
            f"SET pg_lake_iceberg.default_location_prefix TO 's3://{server.bucket}/{client_prefix}'",
            superuser_conn,
        )
        superuser_conn.commit()

        # Create and fill in one transaction: the table's first snapshot is
        # then built from what we have in hand, with no metadata to fetch.
        run_command(
            f"""CREATE TABLE {schema}.{table} (id int, val text)
                USING iceberg WITH (catalog='rest')""",
            superuser_conn,
        )
        run_command(
            f"INSERT INTO {schema}.{table} SELECT i, 'v' || i FROM generate_series(1, 5) i",
            superuser_conn,
        )
        superuser_conn.commit()

        # We asked for nothing, so the catalog was free to place the table.
        proposed = calls.get("proposed_locations", [])
        assert proposed == [None], (
            f"a catalog places the tables it stores, so the create should name "
            f"no location, got {proposed}"
        )

        options = run_query(
            f"""SELECT ftoptions FROM pg_foreign_table
                WHERE ftrelid = '{schema}.{table}'::regclass""",
            superuser_conn,
        )[0][0]
        superuser_conn.commit()

        assert (
            f"location={managed_location}" in options
        ), f"expected the table to live where the catalog put it, got {options}"
        assert (
            "catalog_managed_location=true" in options
        ), f"expected the table to be marked catalog-managed, got {options}"

        # The data went where the catalog said, reachable only through the
        # credential it vended for that location.
        keys = _bucket_keys(server)
        assert any(
            k.startswith(f"{managed_prefix}/data/") for k in keys
        ), f"no data files under the catalog's location, got {keys}"
        assert not any(
            k.startswith(f"{client_prefix}/") for k in keys
        ), f"data files landed under the prefix pg_lake proposed, got {keys}"

        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 5

        # A catalog that places files does not necessarily remove them, so the
        # drop takes the table out of the catalog and leaves the files to us.
        run_command(f"DROP TABLE {schema}.{table}", superuser_conn)
        superuser_conn.commit()

        drops = calls.get("drops", [])
        assert len(drops) == 1, f"expected one drop request, got {drops}"
        assert not any("purgeRequested" in path for path in drops), (
            f"a catalog that places files does not promise to remove them, so "
            f"the drop must not rely on it, got {drops}"
        )

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command("RESET pg_lake_iceberg.default_location_prefix", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)


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
    # The vended credential belongs to a different table: it can read
    # wh/some-other-table and nothing else.  The catalog labels it with that
    # same foreign prefix.
    #
    # We clamp a scope that falls outside the table root back to the table
    # root, so this secret *is* applied to this table's data path -- and then
    # MinIO denies it, because the credential has no rights there.  Clamping
    # keeps a mislabeled credential from registering under the foreign scope,
    # where it would shadow the secret wh/some-other-table depends on.
    server.create_scoped_user(
        "mv_data_ws", "mv_data_ws_secret", ["wh/some-other-table"]
    )

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
    httpd, thread = _serve_mock_catalog(tables, enable_vended=True, conn=superuser_conn)

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
        registered = run_query(
            """
            SELECT name, scope
              FROM duckdb_secrets()
             WHERE name LIKE 'pglake_vended_%'
            """,
            pgduck_conn,
        )
        pgduck_conn.commit()
        shadowing = [r for r in registered if r[1] and "some-other-table" in r[1]]
        assert not shadowing, f"secret registered under a foreign scope: {shadowing}"

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _stop_mock_catalog(httpd, thread, conn=superuser_conn)

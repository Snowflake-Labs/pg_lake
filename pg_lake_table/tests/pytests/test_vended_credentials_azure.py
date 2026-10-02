"""Vended Azure SAS tokens, end to end.

A mock REST catalog vends a real SAS token, generated for the Azure
emulator, for an Iceberg table written there.  The static aztest
secret can read the whole emulator account, so a successful read proves
nothing on its own; what makes these load-bearing is that DuckDB selects
one secret per path by longest scope and never falls back, so a vended
SAS with a broken signature has to make the scan fail.

Cache-on-write is turned off while a table is written, so the scan reads
the emulator rather than a local copy of what was just written.
"""

import json
import socket
import threading
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils_pytest import *

from datetime import datetime, timedelta, timezone

import pyarrow as pa
from azure.storage.blob import ContainerSasPermissions, generate_container_sas
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO

_AZURITE = dict(part.split("=", 1) for part in AZURITE_CONNECTION_STRING.split(";"))
_ACCOUNT = _AZURITE["AccountName"]

_CATALOG_GUCS = (
    "pg_lake_iceberg.rest_catalog_host",
    "pg_lake_iceberg.rest_catalog_client_id",
    "pg_lake_iceberg.rest_catalog_client_secret",
    "pg_lake_iceberg.rest_catalog_enable_vended_credentials",
)

# The catalog states the emulator's endpoint, which a vended endpoint may
# only name once an administrator admits its host; the list matches on a
# label boundary, so ".0.0.1" admits 127.0.0.1.
_EMULATOR_HOST_SUFFIXES = ".dfs.core.windows.net,.blob.core.windows.net,.0.0.1"


def _container_sas(signature_suffix=""):
    sas = generate_container_sas(
        account_name=_ACCOUNT,
        container_name=TEST_BUCKET,
        account_key=_AZURITE["AccountKey"],
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    return sas + signature_suffix


# The two ways a table's location names its storage account: not at all,
# leaving it to adls.account-name, or in the host, as Polaris returns it.
_ACCOUNT_HOST = f"{_ACCOUNT}.dfs.core.windows.net"
_LOCATION_BASES = {
    "container": f"az://{TEST_BUCKET}",
    "abfss": f"abfss://{TEST_BUCKET}@{_ACCOUNT_HOST}",
}


def _vended_config(sas, form="container"):
    """What Polaris vends for a table on the emulator account."""
    expires_ms = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())

    # Iceberg's ADLS FileIO reads adls.connection-string.<host|account> as
    # the account's endpoint, which is how the emulator is reached.
    if form == "abfss":
        return {
            f"adls.sas-token.{_ACCOUNT_HOST}": sas,
            f"adls.sas-token-expires-at-ms.{_ACCOUNT_HOST}": str(expires_ms * 1000),
            f"adls.connection-string.{_ACCOUNT_HOST}": _AZURITE["BlobEndpoint"],
        }

    return {
        f"adls.sas-token.{_ACCOUNT}": sas,
        f"adls.sas-token-expires-at-ms.{_ACCOUNT}": str(expires_ms * 1000),
        "adls.sas-token": sas,
        "adls.account-name": _ACCOUNT,
        f"adls.connection-string.{_ACCOUNT}": _AZURITE["BlobEndpoint"],
    }


def _blob_name(location):
    """The blob a location names in the emulator's test container."""
    for base in _LOCATION_BASES.values():
        if location.startswith(base + "/"):
            return location[len(base) + 1 :]
    raise ValueError(f"{location} is not in the test container")


def _write_iceberg_table_with_pyiceberg(monkeypatch, rows):
    """
    Write an Iceberg table at an abfss:// location through PyIceberg; return
    its metadata location.  pg_lake writes abfss:// through the DataLake API,
    which the emulator does not have, while PyIceberg writes through the blob
    API, as Spark or PyIceberg would for a table a catalog serves.
    """
    parse_location = PyArrowFileIO.parse_location

    def parse_abfss_location(location, *args):
        # PyIceberg hands pyarrow "<container>@<host>/<path>" here, where
        # pyarrow takes "<container>/<path>".
        scheme, netloc, path = parse_location(location, *args)
        if scheme == "abfss":
            container = netloc.split("@", 1)[0]
            path = container + urllib.parse.urlparse(location).path
        return scheme, netloc, path

    monkeypatch.setattr(
        PyArrowFileIO, "parse_location", staticmethod(parse_abfss_location)
    )

    authority = urllib.parse.urlparse(_AZURITE["BlobEndpoint"]).netloc
    location = f"{_LOCATION_BASES['abfss']}/vended_azure/{uuid.uuid4().hex}"
    catalog = SqlCatalog(
        "vended_azure",
        uri="sqlite:///:memory:",
        warehouse=location,
        **{
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "adls.account-name": _ACCOUNT,
            "adls.account-key": _AZURITE["AccountKey"],
            "adls.blob-storage-authority": authority,
            "adls.dfs-storage-authority": authority,
            "adls.blob-storage-scheme": "http",
            "adls.dfs-storage-scheme": "http",
        },
    )
    catalog.create_namespace("ns")

    schema = pa.schema([("id", pa.int64()), ("val", pa.string())])
    table = catalog.create_table("ns.t", schema=schema, location=location)
    table.append(
        pa.table(
            {"id": list(range(rows)), "val": [str(i) for i in range(rows)]},
            schema=schema,
        )
    )
    return table.metadata_location


def _write_iceberg_table(
    superuser_conn, pgduck_conn, schema, table, rows, form="container", monkeypatch=None
):
    """Write an Iceberg table to the emulator; return its metadata location."""
    if form == "abfss":
        return _write_iceberg_table_with_pyiceberg(monkeypatch, rows)

    location = f"{_LOCATION_BASES[form]}/vended_azure/{uuid.uuid4().hex}"
    previous = run_query(
        "SELECT current_setting('pg_lake_cache_on_write_max_size')", pgduck_conn
    )[0][0]
    run_command("SET GLOBAL pg_lake_cache_on_write_max_size TO 0", pgduck_conn)
    pgduck_conn.commit()
    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        run_command(
            f"""CREATE FOREIGN TABLE {schema}.{table} (id int, val text)
                SERVER pg_lake_iceberg OPTIONS (location '{location}')""",
            superuser_conn,
        )
        run_command(
            f"INSERT INTO {schema}.{table} "
            f"SELECT i, i::text FROM generate_series(1, {rows}) i",
            superuser_conn,
        )
        superuser_conn.commit()
    finally:
        run_command(
            f"SET GLOBAL pg_lake_cache_on_write_max_size TO '{previous}'", pgduck_conn
        )
        pgduck_conn.commit()

    return run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_namespace = '{schema}' AND table_name = '{table}'",
        superuser_conn,
    )[0][0]


def _make_handler(tables, azure):
    """
    A catalog answering loadTable from ``tables`` (name -> metadata
    location and vended config), inlining the metadata document as the REST
    spec requires.
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

            if "/namespaces/" in self.path and "/tables" not in self.path:
                ns = self.path.rstrip("/").split("/namespaces/", 1)[1].split("?")[0]
                self._json({"namespace": [urllib.parse.unquote(ns)], "properties": {}})
                return

            if "/tables/" in self.path and self.command == "GET":
                name = self.path.rstrip("/").split("/tables/", 1)[1].split("?")[0]
                info = tables.get(name)
                if info is None:
                    self.send_response(404)
                    self.end_headers()
                    return

                key = _blob_name(info["metadata_location"])
                metadata = json.loads(azure.download_blob(key).readall())
                response = {
                    "metadata-location": info["metadata_location"],
                    "metadata": metadata,
                }
                if self.headers.get("X-Iceberg-Access-Delegation") == (
                    "vended-credentials"
                ):
                    response["storage-credentials"] = [
                        {"prefix": metadata["location"] + "/", "config": info["config"]}
                    ]
                self._json(response)
                return

            self.send_response(404)
            self.end_headers()

        def _json(self, payload):
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        do_GET = _handle
        do_POST = _handle
        do_HEAD = _handle

        def log_message(self, fmt, *args):
            pass

    return _Handler


class _MockCatalog:
    def __init__(self, tables, azure, conn):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        self.conn = conn
        self.httpd = HTTPServer(("127.0.0.1", port), _make_handler(tables, azure))
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

        settings = {
            "pg_lake_iceberg.rest_catalog_host": f"http://127.0.0.1:{port}",
            "pg_lake_iceberg.rest_catalog_client_id": "test_id",
            "pg_lake_iceberg.rest_catalog_client_secret": "test_secret",
            "pg_lake_iceberg.rest_catalog_enable_vended_credentials": "true",
        }
        run_command_outside_tx(
            [f"ALTER SYSTEM SET {k} TO '{v}'" for k, v in settings.items()]
            + ["SELECT pg_reload_conf()"]
        )
        # ALTER SYSTEM lands at an unpredictable command boundary; SET does not.
        for k, v in settings.items():
            run_command(f"SET {k} TO '{v}'", conn)
        run_command(
            f"SET pg_lake.allowed_azure_host_suffixes TO '{_EMULATOR_HOST_SUFFIXES}'",
            conn,
        )
        conn.commit()

    def stop(self):
        self.httpd.shutdown()
        self.thread.join(timeout=5)
        run_command_outside_tx(
            [f"ALTER SYSTEM RESET {k}" for k in _CATALOG_GUCS]
            + ["SELECT pg_reload_conf()"]
        )
        for k in _CATALOG_GUCS:
            run_command(f"RESET {k}", self.conn)
        run_command("RESET pg_lake.allowed_azure_host_suffixes", self.conn)
        self.conn.commit()


def _attach_rest_table(superuser_conn, schema, table):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
    run_command(
        f"""CREATE TABLE {schema}.{table} ()
            USING iceberg
            WITH (catalog='rest', read_only=True, catalog_table_name='{table}')""",
        superuser_conn,
    )
    superuser_conn.commit()


def _vended_azure_secrets(pgduck_conn):
    # A transaction still open on this connection sees the secrets as they
    # were when it began.
    pgduck_conn.rollback()
    return run_query(
        "SELECT name, type, scope FROM duckdb_secrets() "
        "WHERE name LIKE 'pglake_vended_%' AND type = 'azure'",
        pgduck_conn,
    )


def _drop_schemas(superuser_conn, *schemas):
    for schema in schemas:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
    superuser_conn.commit()


@pytest.mark.parametrize("form", list(_LOCATION_BASES))
def test_vended_azure_sas_reads_table(
    superuser_conn, pgduck_conn, extension, installcheck, azure, form, monkeypatch
):
    """The catalog's SAS token becomes a scoped Azure secret the scan reads with."""
    if installcheck:
        return

    source, attached, table = f"vaz_src_ok_{form}", f"vaz_rest_ok_{form}", "vaz_ok"
    catalog = None
    try:
        meta = _write_iceberg_table(
            superuser_conn, pgduck_conn, source, table, 10, form, monkeypatch
        )
        catalog = _MockCatalog(
            {
                table: {
                    "metadata_location": meta,
                    "config": _vended_config(_container_sas(), form),
                }
            },
            azure,
            superuser_conn,
        )
        _attach_rest_table(superuser_conn, attached, table)

        result = run_query(f"SELECT count(*) FROM {attached}.{table}", superuser_conn)
        superuser_conn.commit()
        assert result[0][0] == 10

        location = meta.split("/metadata/", 1)[0]
        secrets = _vended_azure_secrets(pgduck_conn)
        assert any(
            location in str(s["scope"]) for s in secrets
        ), f"expected an Azure secret scoped to {location}, got {secrets}"

    finally:
        superuser_conn.rollback()
        _drop_schemas(superuser_conn, attached, source)
        if catalog is not None:
            catalog.stop()


@pytest.mark.parametrize("form", list(_LOCATION_BASES))
def test_vended_azure_sas_is_the_credential_used(
    superuser_conn, pgduck_conn, extension, installcheck, azure, form, monkeypatch
):
    """
    The static secret could read this table too, so only a failure proves
    which secret the scan went through: a vended SAS with a broken signature
    has to be refused by the store.  That also proves the secret's scope
    matches the URLs the scan reads, whichever way the location names the
    account.
    """
    if installcheck:
        return

    source, attached, table = f"vaz_src_bad_{form}", f"vaz_rest_bad_{form}", "vaz_bad"
    catalog = None
    try:
        meta = _write_iceberg_table(
            superuser_conn, pgduck_conn, source, table, 10, form, monkeypatch
        )
        catalog = _MockCatalog(
            {
                table: {
                    "metadata_location": meta,
                    "config": _vended_config(_container_sas("tampered"), form),
                }
            },
            azure,
            superuser_conn,
        )
        _attach_rest_table(superuser_conn, attached, table)

        error = run_query(
            f"SELECT count(*) FROM {attached}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert "AuthorizationFailure" in str(error), error

    finally:
        superuser_conn.rollback()
        _drop_schemas(superuser_conn, attached, source)
        if catalog is not None:
            catalog.stop()


def test_vended_azure_sas_cannot_add_connection_string_fields(
    superuser_conn, pgduck_conn, extension, installcheck, azure
):
    """
    The SAS token is spliced into a connection string the Azure SDK splits
    at ';'.  One carrying ";BlobEndpoint=..." would send the token to a host
    of the catalog's choosing, so it is refused, and no secret is pushed.
    """
    if installcheck:
        return

    source, attached, table = "vaz_src_inj", "vaz_rest_inj", "vaz_inj"
    catalog = None
    try:
        meta = _write_iceberg_table(superuser_conn, pgduck_conn, source, table, 10)
        sas = _container_sas() + ";BlobEndpoint=http://127.0.0.1:1/elsewhere"
        catalog = _MockCatalog(
            {table: {"metadata_location": meta, "config": _vended_config(sas)}},
            azure,
            superuser_conn,
        )
        _attach_rest_table(superuser_conn, attached, table)

        superuser_conn.notices.clear()
        run_query(
            f"SELECT count(*) FROM {attached}.{table}",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()

        assert any(
            "could not resolve storage credentials" in n and '";"' in n
            for n in superuser_conn.notices
        ), "\n".join(superuser_conn.notices)

        location = meta.split("/metadata/", 1)[0]
        assert not any(
            location in str(s["scope"]) for s in _vended_azure_secrets(pgduck_conn)
        )

    finally:
        superuser_conn.rollback()
        _drop_schemas(superuser_conn, attached, source)
        if catalog is not None:
            catalog.stop()

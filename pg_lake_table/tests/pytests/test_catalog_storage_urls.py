"""
An Iceberg catalog, and the Iceberg files it leads to, can only send pg_lake
to object storage.

Otherwise whoever runs the catalog, or writes the files it leads to, picks a
host for the server to request whenever a table is attached or scanned, and
creating a catalog server takes only lake_write.  The catalog here answers
with URLs on a local web server that counts connections; nothing may connect
to it.
"""

import copy
import io
import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import fastavro
import psycopg2
import pytest
from utils_pytest import *

SCHEMA = "catalog_urls_sc"
WRITER = "catalog_urls_writer"
OBJECT_STORE_PREFIX = "catalog_urls_fromsf"


class _Server(ThreadingHTTPServer):
    """A local server that counts connections, whatever protocol they speak."""

    daemon_threads = True

    def __init__(self, handler, content):
        self.content = content
        self.connections = 0
        super().__init__(("127.0.0.1", 0), handler)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def host(self):
        return f"127.0.0.1:{self.server_address[1]}"

    def process_request(self, request, client_address):
        self.connections += 1
        super().process_request(request, client_address)

    def stop(self):
        self.shutdown()
        self.server_close()


class _FileHandler(BaseHTTPRequestHandler):
    """Serves the files in server.content, keyed by path."""

    def do_HEAD(self):
        self._serve(send_body=False)

    def do_GET(self):
        self._serve(send_body=True)

    def _serve(self, send_body):
        data = self.server.content.get(self.path)
        if data is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        start, end = 0, len(data) - 1
        byte_range = re.fullmatch(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
        if byte_range:
            start = int(byte_range.group(1))
            if byte_range.group(2):
                end = min(int(byte_range.group(2)), end)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        if send_body:
            self.wfile.write(data[start : end + 1])

    def log_message(self, fmt, *args):
        pass


class _CatalogHandler(BaseHTTPRequestHandler):
    """Has every namespace, and answers every loadTable with server.content."""

    def _handle(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        namespace = re.search(r"/namespaces/([^/?]+)$", self.path)
        if self.path.endswith("/oauth/tokens"):
            status, body = 200, {
                "access_token": "token",
                "token_type": "bearer",
                "expires_in": 3600,
            }
        elif self.path.startswith("/v1/config"):
            status, body = 200, {"defaults": {}, "overrides": {}}
        elif namespace:
            status, body = 200, {"namespace": [namespace.group(1)], "properties": {}}
        elif self.command == "GET" and "/tables/" in self.path:
            status, body = 200, self.server.content
        else:
            status, body = 404, {"error": {"message": "not mocked", "code": 404}}
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    do_GET = do_POST = do_HEAD = _handle

    def log_message(self, fmt, *args):
        pass


def _key(url):
    return url.removeprefix(f"s3://{TEST_BUCKET}/")


def _read(s3, url):
    return s3.get_object(Bucket=TEST_BUCKET, Key=_key(url))["Body"].read()


def _write(s3, url, data):
    s3.put_object(Bucket=TEST_BUCKET, Key=_key(url), Body=data)


def _rewrite_avro(data, rewrite):
    reader = fastavro.reader(io.BytesIO(data))
    records = [rewrite(record) for record in reader]
    out = io.BytesIO()
    fastavro.writer(
        out,
        json.loads(reader.metadata["avro.schema"]),
        records,
        codec=reader.codec,
        metadata={
            key: value
            for key, value in reader.metadata.items()
            if not key.startswith("avro.")
        },
    )
    return out.getvalue()


def _current_snapshot(metadata):
    return next(
        snapshot
        for snapshot in metadata["snapshots"]
        if snapshot["snapshot-id"] == metadata["current-snapshot-id"]
    )


def _copy_table(s3, metadata, level=None, url=None):
    """
    Copies the table to a new location in object storage, with the path at
    level (manifest_list, manifest or data_file) of the current snapshot
    replaced by url, and returns a catalog's loadTable answer for the copy.
    """
    base = f"s3://{TEST_BUCKET}/catalog_urls/{uuid.uuid4()}"
    metadata = copy.deepcopy(metadata)
    snapshot = _current_snapshot(metadata)

    def rewrite_entry(entry):
        if level == "data_file":
            entry["data_file"]["file_path"] = url
        return entry

    def rewrite_manifest(manifest_file):
        if level == "manifest":
            manifest_file["manifest_path"] = url
            return manifest_file
        manifest = _rewrite_avro(
            _read(s3, manifest_file["manifest_path"]), rewrite_entry
        )
        manifest_file["manifest_path"] = f"{base}/{uuid.uuid4()}-m0.avro"
        manifest_file["manifest_length"] = len(manifest)
        _write(s3, manifest_file["manifest_path"], manifest)
        return manifest_file

    if level == "manifest_list":
        snapshot["manifest-list"] = url
    else:
        manifest_list = _rewrite_avro(
            _read(s3, snapshot["manifest-list"]), rewrite_manifest
        )
        snapshot["manifest-list"] = f"{base}/snap-{uuid.uuid4()}.avro"
        _write(s3, snapshot["manifest-list"], manifest_list)

    metadata_location = f"{base}/00001-{uuid.uuid4()}.metadata.json"
    _write(s3, metadata_location, json.dumps(metadata).encode())
    return {"metadata-location": metadata_location, "metadata": metadata}


def _attach_and_scan(conn, load_table, columns="", role=None):
    """
    Creates a catalog server that answers loadTable with load_table, attaches a
    table through it, as role if given, and scans the table as the original
    user.  Returns the rows, or the error of the statement that failed.
    """
    catalog = _Server(_CatalogHandler, load_table)
    try:
        if role is not None:
            run_command(f"SET ROLE {role}", conn)
        run_command(
            f"""
            CREATE SERVER catalog_urls_rest TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint 'http://{catalog.host}',
                         enable_vended_credentials 'false');
            CREATE USER MAPPING FOR PUBLIC SERVER catalog_urls_rest
                OPTIONS (client_id 'client', client_secret 'secret');
            CREATE TABLE {SCHEMA}.t ({columns}) USING iceberg
                WITH (catalog = 'catalog_urls_rest', read_only = true,
                      catalog_table_name = 't');
            RESET ROLE;
            """,
            conn,
        )
        return run_query(f"SELECT * FROM {SCHEMA}.t ORDER BY 1", conn)
    except psycopg2.Error as error:
        return error.pgerror
    finally:
        conn.rollback()
        catalog.stop()


@pytest.fixture
def web_server():
    server = _Server(_FileHandler, {})
    yield server
    server.stop()


@pytest.fixture
def schema(superuser_conn, extension):
    run_command(f"CREATE SCHEMA {SCHEMA}", superuser_conn)
    superuser_conn.commit()
    yield
    superuser_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", superuser_conn)
    superuser_conn.commit()


@pytest.fixture
def writer(superuser_conn, schema):
    """A role with lake_write only, which can create tables in the schema."""
    run_command(
        f"""
        CREATE ROLE {WRITER};
        GRANT lake_write TO {WRITER};
        GRANT USAGE, CREATE ON SCHEMA {SCHEMA} TO {WRITER};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield WRITER
    superuser_conn.rollback()
    run_command(f"DROP OWNED BY {WRITER}; DROP ROLE {WRITER}", superuser_conn)
    superuser_conn.commit()


@pytest.fixture(scope="module")
def source_metadata(superuser_conn, s3, extension):
    """The metadata document of a three-row Iceberg table in object storage."""
    location = f"s3://{TEST_BUCKET}/catalog_urls/{uuid.uuid4()}"
    run_command(
        f"""
        CREATE TABLE catalog_urls_source (id bigint) USING iceberg
            WITH (location = '{location}');
        INSERT INTO catalog_urls_source VALUES (1), (2), (3);
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'catalog_urls_source'",
        superuser_conn,
    )[0][0]
    yield json.loads(_read(s3, metadata_location))
    run_command("DROP TABLE catalog_urls_source", superuser_conn)
    superuser_conn.commit()


@pytest.fixture
def object_store_catalog(superuser_conn):
    settings = {
        "object_store_catalog_location_prefix": f"s3://{TEST_BUCKET}",
        "internal_object_store_catalog_prefix": OBJECT_STORE_PREFIX,
        "external_object_store_catalog_prefix": OBJECT_STORE_PREFIX,
    }
    superuser_conn.autocommit = True
    for name, value in settings.items():
        run_command(
            f"ALTER SYSTEM SET pg_lake_iceberg.{name} = '{value}'", superuser_conn
        )
    run_command("SELECT pg_reload_conf(), pg_sleep(0.1)", superuser_conn)
    yield
    for name in settings:
        run_command(f"ALTER SYSTEM RESET pg_lake_iceberg.{name}", superuser_conn)
    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.autocommit = False


@pytest.mark.parametrize(
    "location, error",
    [
        (
            "http://{host}/00001.metadata.json",
            'Iceberg file URL "http://{host}/00001.metadata.json" does not point to object storage',
        ),
        (
            "https://{host}/00001.metadata.json",
            'Iceberg file URL "https://{host}/00001.metadata.json" does not point to object storage',
        ),
        (
            "s3://{bucket}/00001.metadata.json"
            "?s3_endpoint={host}&s3_use_ssl=false&s3_url_style=path",
            'query parameter "s3_endpoint" is not permitted',
        ),
        (
            "abfss://{bucket}@{host}/00001.metadata.json",
            'Azure storage host "{host}" is not permitted',
        ),
    ],
)
def test_catalog_metadata_location_must_be_object_storage(
    superuser_conn, writer, web_server, location, error
):
    """Attaching a table without columns reads the metadata document the
    catalog points to, which takes only lake_write."""
    location = location.format(host=web_server.host, bucket=TEST_BUCKET)
    result = _attach_and_scan(
        superuser_conn, {"metadata-location": location}, role=writer
    )
    assert error.format(host=web_server.host) in str(result), result
    assert web_server.connections == 0


@pytest.mark.parametrize("columns", ["", "id bigint"])
@pytest.mark.parametrize("level", ["manifest_list", "manifest", "data_file"])
def test_catalog_table_files_must_point_to_object_storage(
    superuser_conn, s3, schema, web_server, source_metadata, level, columns
):
    """Whoever runs the catalog, or writes the files it points to, picks the
    paths in those files that every scan of the table follows."""
    url = f"http://{web_server.host}/{level}"
    load_table = _copy_table(s3, source_metadata, level, url)
    result = _attach_and_scan(superuser_conn, load_table, columns)
    assert f'Iceberg file URL "{url}" does not point to object storage' in str(
        result
    ), result
    assert web_server.connections == 0


@pytest.mark.parametrize("columns", ["", "id bigint"])
def test_catalog_table_in_object_storage_is_readable(
    superuser_conn, s3, schema, source_metadata, columns
):
    load_table = _copy_table(s3, source_metadata)
    result = _attach_and_scan(superuser_conn, load_table, columns)
    assert not isinstance(result, str), result
    assert [row[0] for row in result] == [1, 2, 3]


def test_object_store_catalog_metadata_location_must_be_object_storage(
    superuser_conn, s3, writer, web_server, object_store_catalog
):
    location = f"http://{web_server.host}/00001.metadata.json"
    catalog = {
        "tables": [
            {"namespace": SCHEMA, "table-name": "t", "metadata-location": location}
        ]
    }
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key=f"{OBJECT_STORE_PREFIX}/catalog/catalog_urls/catalog.json",
        Body=json.dumps(catalog).encode(),
    )
    error = run_command(
        f"""
        SET ROLE {writer};
        CREATE TABLE {SCHEMA}.t () USING iceberg
            WITH (catalog = 'object_store', read_only = true,
                  catalog_name = 'catalog_urls', catalog_table_name = 't');
        """,
        superuser_conn,
        raise_error=False,
    )
    superuser_conn.rollback()
    assert f'Iceberg file URL "{location}" does not point to object storage' in str(
        error
    ), error
    assert web_server.connections == 0


def test_table_named_by_web_url_may_point_to_web_server(
    superuser_conn, s3, web_server, source_metadata
):
    """Naming a web URL takes lake_read, so the files it leads to may be on the
    web server too."""
    metadata = copy.deepcopy(source_metadata)
    snapshot = _current_snapshot(metadata)
    web_server.content["/snap.avro"] = _read(s3, snapshot["manifest-list"])
    snapshot["manifest-list"] = f"http://{web_server.host}/snap.avro"
    web_server.content["/00001.metadata.json"] = json.dumps(metadata).encode()
    try:
        run_command(
            f"""
            CREATE FOREIGN TABLE catalog_urls_web () SERVER pg_lake
                OPTIONS (path 'http://{web_server.host}/00001.metadata.json',
                         format 'iceberg')
            """,
            superuser_conn,
        )
        result = run_query(
            "SELECT id FROM catalog_urls_web ORDER BY id", superuser_conn
        )
    finally:
        superuser_conn.rollback()
    assert [row[0] for row in result] == [1, 2, 3]
    assert web_server.connections > 0


def test_table_with_web_manifest_list_can_be_dropped(
    superuser_conn, s3, with_default_location, web_server
):
    """A web manifest list in an old snapshot makes VACUUM and DROP TABLE warn
    instead of fail, and VACUUM still removes the dropped table's files."""
    run_command(
        "CREATE TABLE catalog_urls_drop (id bigint) USING iceberg", superuser_conn
    )
    for value in (1, 2):
        run_command(f"INSERT INTO catalog_urls_drop VALUES ({value})", superuser_conn)
        superuser_conn.commit()
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'catalog_urls_drop'",
        superuser_conn,
    )[0][0]
    metadata = json.loads(_read(s3, metadata_location))
    old_snapshot = next(
        snapshot
        for snapshot in metadata["snapshots"]
        if snapshot["snapshot-id"] != metadata["current-snapshot-id"]
    )
    url = f"http://{web_server.host}/snap.avro"
    old_snapshot["manifest-list"] = url
    location = metadata["location"].rstrip("/")
    # A new file, since pgduck_server caches the metadata files it writes.
    metadata_location = f"{location}/metadata/{uuid.uuid4()}.metadata.json"
    _write(s3, metadata_location, json.dumps(metadata).encode())
    run_command(
        f"UPDATE lake_iceberg.tables_internal "
        f"SET metadata_location = '{metadata_location}' "
        f"WHERE table_name = 'catalog_urls_drop'::regclass",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command_outside_tx(["VACUUM catalog_urls_drop"])

    superuser_conn.notices.clear()
    run_command("DROP TABLE catalog_urls_drop", superuser_conn)
    superuser_conn.commit()
    error = f'Iceberg file URL "{url}" does not point to object storage'
    assert any(
        error in notice for notice in superuser_conn.notices
    ), superuser_conn.notices

    run_command_outside_tx(
        [
            "SET pg_lake_engine.orphaned_file_retention_period = 0",
            "SET pg_lake_engine.vacuum_file_remove_retry_interval = 0",
            "VACUUM (ICEBERG)",
        ]
    )
    remaining = run_query(
        f"SELECT count(*) FROM lake_file.list('{location}/**')", superuser_conn
    )[0][0]
    assert remaining == 0
    assert web_server.connections == 0

import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

import pytest
from azure.storage.blob import BlobType
from utils_pytest import *


@pytest.mark.parametrize("blob_type", ["append", "block", "page", "missing"])
def test_delete_azure_append_blob(pgduck_conn, azure, blob_type):
    key = f"test_delete_azure_append_blob/{blob_type}.json"
    blob = azure.get_blob_client(key)
    if blob_type == "append":
        blob.create_append_blob()
        blob.append_block(b"legacy catalog")
    elif blob_type == "block":
        blob.upload_blob(b"block catalog", overwrite=True)
    elif blob_type == "page":
        blob.create_page_blob(size=512)

    properties = blob.get_blob_properties() if blob_type != "missing" else None
    command = f"SELECT pg_lake_delete_azure_append_blob('azure://{TEST_BUCKET}/{key}')"
    try:
        assert run_query(command, pgduck_conn) == [[blob_type == "append"]]
        if blob_type in ("append", "missing"):
            assert not blob.exists()
        else:
            assert blob.get_blob_properties().etag == properties.etag
        assert run_query(command, pgduck_conn) == [[False]]
    finally:
        if blob.exists():
            blob.delete_blob()


@pytest.mark.parametrize("scheme", ["az", "abfs", "abfss", "s3", "gs", "file"])
def test_delete_azure_append_blob_ignores_other_schemes(pgduck_conn, scheme):
    assert run_query(
        f"SELECT pg_lake_delete_azure_append_blob('{scheme}://unconfigured/catalog.json')",
        pgduck_conn,
    ) == [[False]]


def test_delete_azure_append_blob_preserves_leased_blob(pgduck_conn, azure):
    key = "test_delete_azure_append_blob/leased.json"
    blob = azure.get_blob_client(key)
    blob.create_append_blob()
    blob.append_block(b"leased catalog")
    lease = blob.acquire_lease(lease_duration=15)
    try:
        error = run_command(
            f"SELECT pg_lake_delete_azure_append_blob('azure://{TEST_BUCKET}/{key}')",
            pgduck_conn,
            raise_error=False,
        )
        assert "LeaseIdMissing" in str(error)
        pgduck_conn.rollback()
        assert blob.get_blob_properties().blob_type == BlobType.APPENDBLOB
        assert blob.download_blob().readall() == b"leased catalog"
    finally:
        lease.release()
        blob.delete_blob()


def test_delete_azure_append_blob_preserves_concurrent_write(pgduck_conn, azure):
    key = "test_delete_azure_append_blob/concurrent.json"
    blob = azure.get_blob_client(key)
    blob.create_append_blob()
    blob.append_block(b"original")
    original_etag = blob.get_blob_properties().etag
    delete_conditions = []

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            upstream = http.client.HTTPConnection("127.0.0.1", 10000, timeout=10)
            try:
                upstream.request(self.command, self.path, headers=dict(self.headers))
                response = upstream.getresponse()
                body = response.read()
                if self.command == "HEAD" and response.status == 200:
                    blob.append_block(b" concurrent update")
                if self.command == "DELETE":
                    delete_conditions.append(self.headers.get("If-Match"))
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in ("connection", "transfer-encoding"):
                        self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)
            finally:
                upstream.close()

        do_HEAD = forward
        do_DELETE = forward

    proxy = HTTPServer(("127.0.0.1", 0), Proxy)
    thread = threading.Thread(target=proxy.serve_forever)
    thread.start()
    connection_string = AZURITE_CONNECTION_STRING.replace(
        "127.0.0.1:10000", f"127.0.0.1:{proxy.server_port}"
    )
    try:
        run_command(
            "CREATE SECRET azure_delete_race (TYPE AZURE, "
            f"CONNECTION_STRING '{connection_string}', "
            f"SCOPE 'azure://{TEST_BUCKET}/{key}')",
            pgduck_conn,
        )
        error = run_command(
            f"SELECT pg_lake_delete_azure_append_blob('azure://{TEST_BUCKET}/{key}')",
            pgduck_conn,
            raise_error=False,
        )
        assert "ConditionNotMet" in str(error)
        pgduck_conn.rollback()
        assert delete_conditions == [original_etag]
        assert blob.download_blob().readall() == b"original concurrent update"
    finally:
        run_command("DROP SECRET IF EXISTS azure_delete_race", pgduck_conn)
        proxy.shutdown()
        thread.join(timeout=10)
        proxy.server_close()
        blob.delete_blob()

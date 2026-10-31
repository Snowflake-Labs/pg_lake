"""A failed REST commit must not queue the catalog's live metadata (#226).

Writable REST tables send their commit to the catalog only after the local
transaction commits.  When the catalog rejects it, the catalog keeps its
current metadata file, so that file must not be in the deletion queue, and
the next write to the table must not fail on a duplicate queue entry.

A proxy in front of Polaris forwards every request, and answers the
transactions/commit endpoint with 500 while it is told to.
"""

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from utils_pytest import *
from helpers.polaris import *

NAMESPACE = "test_rest_commit_failure"
TABLE = "tbl"


class _CommitFailingProxy:
    def __init__(self):
        self.fail_commits = False
        self.failed_commits = 0
        self.upstream = (
            f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        )

        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def _forward(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None

                if proxy.fail_commits and self.path.endswith("/transactions/commit"):
                    proxy.failed_commits += 1
                    payload = b'{"error":{"message":"injected","type":"X","code":500}}'
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                headers = {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower() not in ("host", "content-length")
                }
                resp = requests.request(
                    self.command,
                    proxy.upstream + self.path,
                    headers=headers,
                    data=body,
                    timeout=30,
                )
                self.send_response(resp.status_code)
                for k, v in resp.headers.items():
                    if k.lower() not in (
                        "content-length",
                        "transfer-encoding",
                        "connection",
                        "content-encoding",
                    ):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(resp.content)))
                self.end_headers()
                self.wfile.write(resp.content)

            do_GET = do_POST = do_DELETE = do_HEAD = do_PUT = _forward

            def log_message(self, *args):
                pass

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]

        self.httpd = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def _point_catalog_host(conns, host):
    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{host}'",
            "SELECT pg_reload_conf()",
        ]
    )
    wait_for_reloaded_settings(conns, {"pg_lake_iceberg.rest_catalog_host": host})


def _catalog_metadata_location():
    url = (
        f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        f"/api/catalog/v1/{server_params.PG_DATABASE}"
        f"/namespaces/{NAMESPACE}/tables/{TABLE}"
    )
    resp = requests.get(
        url, headers={"Authorization": f"Bearer {get_polaris_access_token()}"}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["metadata-location"]


def _queued_paths(conn):
    rows = run_query(
        f"""SELECT path FROM lake_engine.deletion_queue
            WHERE table_name = '{NAMESPACE}.{TABLE}'::regclass""",
        conn,
    )
    return [r[0] for r in rows]


def test_failed_rest_commit_does_not_queue_live_metadata(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    if installcheck:
        return

    run_command(f"CREATE SCHEMA {NAMESPACE}", pg_conn)
    run_command(
        f"CREATE TABLE {NAMESPACE}.{TABLE} (a int) USING iceberg WITH (catalog='rest')",
        pg_conn,
    )
    run_command(f"INSERT INTO {NAMESPACE}.{TABLE} VALUES (1)", pg_conn)
    pg_conn.commit()
    run_command(f"INSERT INTO {NAMESPACE}.{TABLE} VALUES (2)", pg_conn)
    pg_conn.commit()

    polaris_host = (
        f"{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog"
    )
    proxy = _CommitFailingProxy()
    try:
        _point_catalog_host(
            [pg_conn, superuser_conn], f"127.0.0.1:{proxy.port}/api/catalog"
        )

        live = _catalog_metadata_location()
        proxy.fail_commits = True

        # A rejected commit leaves our catalog ahead of the REST catalog,
        # which the heavy asserts report on the next write.  Recovering from
        # that is #94; here we only look at the deletion queue.
        run_command("SET pg_lake_engine.enable_heavy_asserts TO off", superuser_conn)
        superuser_conn.commit()

        # on main, the second of these failed on deletion_queue_pkey (#226)
        for value in (3, 4):
            run_command(
                f"INSERT INTO {NAMESPACE}.{TABLE} VALUES ({value})", superuser_conn
            )
            superuser_conn.commit()

        assert proxy.failed_commits == 2
        assert _catalog_metadata_location() == live
        assert live not in _queued_paths(superuser_conn)

        # once a commit goes through, the file it replaced may be queued
        proxy.fail_commits = False
        run_command(f"INSERT INTO {NAMESPACE}.{TABLE} VALUES (5)", superuser_conn)
        superuser_conn.commit()

        current = _catalog_metadata_location()
        assert current != live
        queued = _queued_paths(superuser_conn)
        assert current not in queued
        assert len(queued) == len(set(queued))
    finally:
        pg_conn.rollback()
        superuser_conn.rollback()
        run_command("RESET pg_lake_engine.enable_heavy_asserts", superuser_conn)
        superuser_conn.commit()
        _point_catalog_host([pg_conn, superuser_conn], polaris_host)
        proxy.stop()

    # the file the last write replaced is left for the drop to queue
    url = (
        f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        f"/api/catalog/v1/{server_params.PG_DATABASE}"
        f"/namespaces/{NAMESPACE}/tables/{TABLE}"
    )
    resp = requests.get(
        url, headers={"Authorization": f"Bearer {get_polaris_access_token()}"}
    )
    last_replaced = resp.json()["metadata"]["metadata-log"][-1]["metadata-file"]
    assert last_replaced not in _queued_paths(superuser_conn)

    run_command(f"DROP SCHEMA {NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()

    queued = run_query(
        f"SELECT 1 FROM lake_engine.deletion_queue WHERE path = '{last_replaced}'",
        superuser_conn,
    )
    assert queued == [[1]]

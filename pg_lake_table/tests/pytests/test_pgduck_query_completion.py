"""Normal scans must consume ReadyForQuery before closing the engine connection."""

import os
import functools
import http.server
import queue
import select
import socket
import struct
import threading

import psycopg2
import pytest
from utils_pytest import *


def _receive(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError
        data.extend(part)
    return bytes(data)


class ReadyForQueryProxy:
    """Delay the target query's Z, without changing any protocol messages.

    Forwarding CommandComplete separately makes the old early-close behavior
    observable even when the engine sends CommandComplete and ReadyForQuery in
    the same socket write. Startup and unrelated queries pass through normally.
    """

    def __init__(self, upstream_path):
        self.upstream_path = upstream_path
        self.target = None
        self.disconnect = False
        self.cancel_mode = False
        self.ready = threading.Event()
        self.results = queue.Queue()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stopped = threading.Event()
        self.workers = []
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            worker = threading.Thread(target=self._forward, args=(client,), daemon=True)
            self.workers.append(worker)
            worker.start()

    def _forward(self, client):
        target_query = None
        try:
            with client, socket.socket(socket.AF_UNIX) as engine:
                client.settimeout(10)
                engine.settimeout(10)
                engine.connect(self.upstream_path)
                # sslmode/gssencmode=disable ensures an ordinary StartupMessage.
                length = _receive(client, 4)
                engine.sendall(
                    length + _receive(client, struct.unpack("!I", length)[0] - 4)
                )
                while not self.stopped.is_set():
                    readable, _, _ = select.select([client, engine], [], [], 0.1)
                    for source in readable:
                        kind = _receive(source, 1)
                        length = _receive(source, 4)
                        payload = _receive(source, struct.unpack("!I", length)[0] - 4)
                        destination = engine if source is client else client
                        if source is client and kind == b"P":
                            query = payload.split(b"\0", 2)[1]
                            if (
                                self.target
                                and self.target in query
                                and b"read_csv" in query
                            ):
                                target_query = query
                        if source is engine and kind == b"Z" and target_query:
                            self.ready.set()
                            if self.disconnect:
                                self.results.put((target_query, "disconnected"))
                                target_query = None
                                return
                            # A completed scan must wait for this message. The
                            # old code instead sends Terminate (or closes).
                            delay = 10.0 if self.cancel_mode else 1.0
                            readable, _, _ = select.select([client], [], [], delay)
                            early_close = bool(readable)
                            self.results.put((target_query, early_close))
                            target_query = None
                            if early_close:
                                return
                        destination.sendall(kind + length + payload)
                        if source is client and kind == b"X":
                            return
        except (EOFError, OSError) as error:
            if target_query and not self.stopped.is_set():
                self.results.put((target_query, error))

    def close(self):
        self.stopped.set()
        self.listener.close()
        self.thread.join(timeout=2)
        for worker in self.workers:
            worker.join(timeout=2)


@pytest.fixture(scope="module")
def completion_connection(pgduck_server, tmp_path_factory):
    # pg_lake_engine.host is PGC_POSTMASTER, so use a separate PostgreSQL
    # instance rather than restart or reconfigure the suite's shared server.
    directory = tmp_path_factory.mktemp("query_completion")
    http_server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(
            http.server.SimpleHTTPRequestHandler, directory=str(directory)
        ),
    )
    http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    http_thread.start()
    proxy = ReadyForQueryProxy(
        f"{server_params.PGDUCK_UNIX_DOMAIN_PATH}/.s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1", 0))
        port = port_socket.getsockname()[1]
    data_directory = str(directory / "pgdata")
    conn = None
    try:
        with pytest.MonkeyPatch.context() as patch:
            # This extra cluster tests the installed version; the shared
            # suite cluster owns any requested pg_upgrade run.
            patch.delenv("TEST_PG_UPGRADE_FROM_BINDIR", raising=False)
            extra_args = os.environ.get("PGLAKE_EXTRA_INITDB_ARGS", "")
            patch.setenv(
                "PGLAKE_EXTRA_INITDB_ARGS",
                extra_args
                + " --set shared_preload_libraries=pg_extension_base"
                + " --set pg_lake_engine.enable_cache_manager=off"
                + f" --set 'pg_lake_engine.host=host=127.0.0.1 port={proxy.port}"
                + " sslmode=disable gssencmode=disable'",
            )
            start_postgres(data_directory, server_params.PG_USER, port)
        conn = psycopg2.connect(
            host="localhost", port=port, dbname="postgres", user=server_params.PG_USER
        )
        conn.autocommit = True
        run_command("CREATE EXTENSION pg_lake_table CASCADE", conn)
        run_command("SET statement_timeout = '10s'", conn)
        csv_path = directory / "rfq_drain.csv"
        csv_path.write_text("id\n1\n")
        csv_url = f"http://127.0.0.1:{http_server.server_port}/rfq_drain.csv"
        run_command(
            f"""CREATE FOREIGN TABLE rfq_drain_rows (id int)
                SERVER pg_lake OPTIONS
                (path '{csv_url}', format 'csv', header 'true')""",
            conn,
        )
        yield conn, proxy
    finally:
        if conn:
            conn.close()
        stop_postgres(data_directory)
        proxy.close()
        http_server.shutdown()
        http_server.server_close()
        http_thread.join(timeout=2)


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
@pytest.mark.parametrize("empty", [False, True], ids=["one_row", "zero_rows"])
def test_scan_consumes_ready_for_query(completion_connection, full_pushdown, empty):
    conn, proxy = completion_connection
    run_command(
        f"SET pg_lake_table.enable_full_query_pushdown = {'on' if full_pushdown else 'off'}",
        conn,
    )
    query = "SELECT id FROM rfq_drain_rows" + (" WHERE id > 1" if empty else "")
    plan = str(run_query("EXPLAIN " + query, conn))
    assert ("Custom Scan (Query Pushdown)" in plan) == full_pushdown
    if not full_pushdown:
        assert "Foreign Scan" in plan

    proxy.target = b"rfq_drain.csv"
    try:
        rows = run_query(query, conn)
        assert [row[0] for row in rows] == ([] if empty else [1])
        _, early_close = proxy.results.get(timeout=5)
        assert early_close is False, "Scan closed before receiving ReadyForQuery"
    finally:
        proxy.target = None


def test_explain_consumes_ready_for_query(completion_connection):
    conn, proxy = completion_connection
    run_command("SET pg_lake_table.enable_full_query_pushdown = on", conn)
    proxy.target = b"rfq_drain.csv"
    try:
        plan = str(run_query("EXPLAIN SELECT id FROM rfq_drain_rows", conn))
        assert "Custom Scan (Query Pushdown)" in plan
        query, early_close = proxy.results.get(timeout=5)
        assert query.lower().startswith(b"explain")
        assert early_close is False, "EXPLAIN closed before receiving ReadyForQuery"
    finally:
        proxy.target = None


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
@pytest.mark.parametrize("failure", ["cancel", "disconnect"])
def test_tail_error_preserves_backend(completion_connection, failure, full_pushdown):
    conn, proxy = completion_connection
    run_command(
        f"SET pg_lake_table.enable_full_query_pushdown = {'on' if full_pushdown else 'off'}",
        conn,
    )
    run_command("SET statement_timeout = '30s'", conn)
    proxy.ready.clear()
    proxy.target = b"rfq_drain.csv"
    proxy.disconnect = failure == "disconnect"
    proxy.cancel_mode = failure == "cancel"
    cancel_errors = []

    def cancel_at_ready():
        try:
            assert proxy.ready.wait(timeout=10), "Query did not reach ReadyForQuery"
            conn.cancel()
        except Exception as error:
            cancel_errors.append(error)

    canceller = None
    if failure == "cancel":
        # Cancel only once the target query has completed its result stream;
        # a timeout measured from query start can fire during planning on CI.
        canceller = threading.Thread(target=cancel_at_ready, daemon=True)
        canceller.start()
    try:
        expected_error = (
            psycopg2.errors.QueryCanceled if failure == "cancel" else psycopg2.Error
        )
        with pytest.raises(expected_error) as error:
            run_query("SELECT id FROM rfq_drain_rows", conn)
        if failure == "cancel":
            assert "canceling statement due to user request" in str(error.value)
    finally:
        if canceller:
            canceller.join(timeout=15)
        proxy.results.get(timeout=15)
        proxy.target = None
        proxy.disconnect = False
        proxy.cancel_mode = False
        run_command("SET statement_timeout = '10s'", conn)

    assert not cancel_errors
    # A stale PGresult in the FDW's PG_FINALLY cleanup used here would double
    # free the terminal result and crash the PostgreSQL backend.
    assert run_query("SELECT 1", conn)[0][0] == 1


def test_limit_can_close_before_ready_for_query(completion_connection):
    conn, proxy = completion_connection
    run_command("SET pg_lake_table.enable_full_query_pushdown = off", conn)
    # A PostgreSQL-only filter keeps LIMIT above the Foreign Scan.
    query = "SELECT id FROM rfq_drain_rows WHERE pg_column_size(id) > 0 LIMIT 1"
    plan = str(run_query("EXPLAIN " + query, conn))
    assert "Limit" in plan and "Foreign Scan" in plan
    assert "Custom Scan (Query Pushdown)" not in plan

    proxy.target = b"rfq_drain.csv"
    try:
        assert run_query(query, conn)[0][0] == 1
        _, early_close = proxy.results.get(timeout=5)
        assert early_close is True, "LIMIT unnecessarily drained the remaining result"
    finally:
        proxy.target = None

import pytest
import subprocess
import os
import signal
import time
import tempfile
import threading
from utils_pytest import *
import platform


PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
PGDUCK_PORT = 8254  # lets a less common port
DUCKDB_DATABASE_FILE_PATH = "/tmp/duckdb.db"
PGDUCK_CACHE_DIR = f"/tmp/cache.{PGDUCK_PORT}"


def test_server_start():
    server = PgDuckServer(port=PGDUCK_PORT)
    assert is_server_listening(server.socket_path)
    assert has_duckdb_created_file(DUCKDB_DATABASE_FILE_PATH)


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="Abstract sockets no supported on Mac"
)
def test_server_start_abstract_socket():
    server = PgDuckServer(
        unix_socket_directory="@" + PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT
    )
    assert is_server_listening(server.socket_path)
    assert has_duckdb_created_file(DUCKDB_DATABASE_FILE_PATH)


def test_multiple_server_instances_on_same_socket():
    server1 = PgDuckServer(port=PGDUCK_PORT)
    assert is_server_listening(server1.socket_path)

    # Attempt to start a second server on the same socket
    server2 = PgDuckServer(port=PGDUCK_PORT)

    # Check if server2 has terminated (indicating failure to start)
    server2.process.poll()
    assert server2.process.returncode != 0

    # we should be able to connect to the socket again
    assert is_server_listening(server1.socket_path)


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="Abstract sockets no supported on Mac"
)
def test_multiple_server_instances_on_same_abstract_socket():
    abstract_path = "@" + PGDUCK_UNIX_DOMAIN_PATH
    server1 = PgDuckServer(unix_socket_directory=abstract_path, port=PGDUCK_PORT)
    assert is_server_listening(server1.socket_path)

    # Attempt to start a second server on the same socket
    server2 = PgDuckServer(unix_socket_directory=abstract_path, port=PGDUCK_PORT)

    server2.process.poll()
    assert server2.process.returncode != 0

    # we should be able to connect to the socket again
    assert is_server_listening(server1.socket_path)


def test_multiple_server_instances_on_duckdb_file_path_socket():
    server1 = PgDuckServer(port=PGDUCK_PORT, duckdb_database_file_path="/tmp/data1.db")
    assert is_server_listening(server1.socket_path)

    # Attempt to start a second server on the same duckdb_database_file_path.
    server2 = PgDuckServer(
        port=PGDUCK_PORT + 1,
        duckdb_database_file_path="/tmp/data1.db",
        need_output=True,
    )

    start_time = time.time()
    found_error = False
    while (time.time() - start_time) < 20:  # loop at most 20 seconds
        try:
            line = server2.output_queue.get_nowait()
            if line and "error initialization DuckDB" in line:
                found_error = True
                break
        except queue.Empty:
            time.sleep(0.1)  # No output yet, continue waiting

    # Check if server2 has terminated (indicating failure to start)
    server2.process.poll()
    assert server2.process.returncode != 0
    assert found_error == True

    # we should be able to connect to the socket again
    assert is_server_listening(server1.socket_path)
    assert has_duckdb_created_file("/tmp/data1.db")


def test_two_servers_different_ports():
    server1 = PgDuckServer(port=PGDUCK_PORT, duckdb_database_file_path="/tmp/data1.db")
    server2 = PgDuckServer(
        port=PGDUCK_PORT + 1, duckdb_database_file_path="/tmp/data2.db"
    )

    assert is_server_listening(server1.socket_path)
    assert is_server_listening(server2.socket_path)

    assert has_duckdb_created_file("/tmp/data1.db")
    assert has_duckdb_created_file("/tmp/data2.db")


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="Abstract sockets no supported on Mac"
)
def test_two_servers_different_abstract_ports():
    abstract_path = "@" + PGDUCK_UNIX_DOMAIN_PATH
    server1 = PgDuckServer(
        unix_socket_directory=abstract_path,
        port=PGDUCK_PORT,
        duckdb_database_file_path="/tmp/data1.db",
    )
    server2 = PgDuckServer(
        unix_socket_directory=abstract_path,
        port=PGDUCK_PORT + 1,
        duckdb_database_file_path="/tmp/data2.db",
    )

    assert is_server_listening(server1.socket_path)
    assert is_server_listening(server2.socket_path)

    assert has_duckdb_created_file("/tmp/data1.db")
    assert has_duckdb_created_file("/tmp/data2.db")


def test_two_servers_different_paths():
    # Create a temporary directory
    with tempfile.TemporaryDirectory(dir="/tmp") as temp_dir:
        server1 = PgDuckServer(
            port=PGDUCK_PORT, duckdb_database_file_path="/tmp/data1.db"
        )
        server2 = PgDuckServer(
            unix_socket_directory=temp_dir,
            port=PGDUCK_PORT,
            duckdb_database_file_path="/tmp/data2.db",
        )

        assert is_server_listening(server1.socket_path)
        assert is_server_listening(server2.socket_path)


# Failure scenario tests
def test_server_invalid_port():
    server = PgDuckServer(port="invalid_port")
    server.process.poll()
    assert server.process.returncode != 0


def test_server_excessively_high_port():
    server = PgDuckServer(port=65536)
    server.process.poll()
    assert server.process.returncode != 0


def test_server_with_nonexistent_socket_directory():
    server = PgDuckServer(
        unix_socket_directory="/nonexistent/directory", port=PGDUCK_PORT
    )
    server.process.poll()
    assert server.process.returncode != 0


def test_server_exit_code_and_error_message_for_invalid_socket():
    server = PgDuckServer(unix_socket_directory="/invalid/path", port=PGDUCK_PORT)
    assert server.process.returncode != 0


def test_long_unix_socket_path():
    server = PgDuckServer(unix_socket_directory="/tmp/" + "a" * 100, port=PGDUCK_PORT)
    assert server.process.returncode != 0


@pytest.mark.parametrize("use_debug", [False, True])
def test_server_debug_messages(use_debug):
    server = PgDuckServer(port=PGDUCK_PORT, debug=use_debug, need_output=True)

    assert is_server_listening(server.socket_path)
    assert has_duckdb_created_file(DUCKDB_DATABASE_FILE_PATH)

    # connect to our server, issue our command
    conn = psycopg2.connect(host=PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT)

    # verify we find our log message at debug level
    cur = conn.cursor()
    query = "SELECT 'query_appears_in_output'"

    cur.execute(query)

    server_output = get_server_output(server.output_queue)
    found = query in server_output

    if use_debug:
        assert found, "Missing expected query in output"
    else:
        assert not found, "Unexpectedly found query in output (should be suppressed)"

    cur.close()
    conn.close()


def test_server_pidfile():
    pidfile_path = f"/tmp/pgduck_server_test_{os.getpid()}.pid"

    assert not os.path.exists(pidfile_path)

    server = PgDuckServer(
        port=PGDUCK_PORT,
        pidfile=pidfile_path,
        duckdb_database_file_path="/tmp/data1.db",
    )

    assert is_server_listening(server.socket_path)
    assert os.path.exists(pidfile_path)

    # Give the server a moment to finish handling the is_server_listening connection
    time.sleep(0.1)

    # Verify the server removes its pidfile on clean SIGTERM shutdown.
    # Don't use server.stop() here: the SIGKILL fallback would bypass the
    # server's signal handler and leave the pidfile behind.
    # Use a generous timeout (60s) to allow for clean shutdown even under
    # heavy load or when multiple test instances are running concurrently.
    server.process.terminate()
    try:
        server.process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        server.process.kill()
        server.process.wait(timeout=10)
        pytest.fail(
            "server did not exit on SIGTERM; pidfile cleanup could not be verified"
        )

    time.sleep(1)

    # pidfile cleaned up
    assert not os.path.exists(pidfile_path)


# Verify the server exits promptly on SIGINT/SIGTERM even while a client
# connection is open.  Before the signal-masking fix, the OS could deliver
# the signal to a client thread instead of the main thread; the client
# thread's handler would set ``running = 0`` but the main thread's
# ``accept()`` would never be interrupted, causing a hang.
@pytest.mark.parametrize("send_signal", [signal.SIGINT, signal.SIGTERM])
def test_server_exits_on_signal_with_active_client(send_signal):
    server = PgDuckServer(port=PGDUCK_PORT, need_output=True)
    assert is_server_listening(server.socket_path)

    # Open a client connection so the server has an active client thread.
    conn = psycopg2.connect(host=PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT)

    # Send the signal directly to the server process.
    server.process.send_signal(send_signal)

    # The server must exit within a reasonable timeout.  If the signal was
    # delivered to the client thread (the old bug) the main thread's
    # accept() would block indefinitely and this would time out.
    try:
        server.process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.process.kill()
        server.process.wait(timeout=5)
        conn.close()
        pytest.fail(
            f"Server did not exit within 10s after {send_signal.name}; "
            "signal was likely delivered to a client thread"
        )

    conn.close()
    assert server.process.returncode is not None

    # The server should have reached its clean shutdown path and logged this.
    server_output = get_server_output(server.output_queue)
    assert "Done running" in server_output


def test_server_survives_sigstop_sigcont():
    """SIGSTOP/SIGCONT should pause and resume the server, not terminate it."""
    server = PgDuckServer(port=PGDUCK_PORT)
    assert is_server_listening(server.socket_path)

    # Pause the server.
    server.process.send_signal(signal.SIGSTOP)
    time.sleep(1)

    # Server process must still be alive (suspended, not exited).
    assert server.process.poll() is None

    # Resume the server.
    server.process.send_signal(signal.SIGCONT)
    time.sleep(1)

    # Server should still be running and accepting connections.
    assert server.process.poll() is None
    assert is_server_listening(server.socket_path)


# ensure we handle pidfiles properly when sending normal stop signals or interrupt
@pytest.mark.parametrize("send_signal", [signal.SIGINT, signal.SIGTERM])
def test_server_pidfile_signal(send_signal):
    pidfile_path = f"/tmp/pgduck_server_test_{os.getpid()}.pid"

    assert not os.path.exists(pidfile_path)

    server = PgDuckServer(
        port=PGDUCK_PORT,
        pidfile=pidfile_path,
        duckdb_database_file_path="/tmp/data1.db",
    )

    assert is_server_listening(server.socket_path)
    assert os.path.exists(pidfile_path)

    # test sending external signal
    with open(pidfile_path, "r") as f:
        pid = f.readline().strip()
        assert pid.isdigit()
        pid = int(pid)
        os.kill(pid, send_signal)

    # wait for the process to exit (graceful shutdown may take a moment)
    server.process.wait(timeout=10)

    # pidfile cleaned up
    assert not os.path.exists(pidfile_path)


@pytest.mark.parametrize("num_clients", [3, 5])
@pytest.mark.parametrize("send_signal", [signal.SIGINT, signal.SIGTERM])
def test_graceful_shutdown_with_active_clients(send_signal, num_clients):
    """Server should interrupt active queries and shut down gracefully.

    Starts *num_clients* connections each running a long query, sends
    *send_signal*, and verifies:
      - the server exits within a reasonable timeout,
      - the "interrupted N active connection(s)" message is logged,
      - the "Done running" message is logged,
      - each client thread received an error (connection reset or interrupt).
    """
    server = PgDuckServer(port=PGDUCK_PORT, need_output=True)
    assert is_server_listening(server.socket_path)

    # Thread-safe queue to collect errors from client threads.
    error_queue = queue.Queue()
    barrier = threading.Barrier(num_clients + 1)

    long_running_query = (
        "SELECT SUM(generate_series) FROM generate_series(0, 999999999999)"
    )

    def run_query_on_client(idx):
        conn = psycopg2.connect(host=PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT)
        cur = conn.cursor()
        try:
            # Signal that this client is connected and about to run its query.
            barrier.wait(timeout=10)
            cur.execute(long_running_query)
        except Exception as e:
            error_queue.put((idx, e))
        finally:
            try:
                conn.close()
            except Exception:
                pass

    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=run_query_on_client, args=(i,))
        t.start()
        threads.append(t)

    # Wait until all clients are connected and have issued their query.
    barrier.wait(timeout=10)
    # Small extra delay so the queries are actually running server-side.
    time.sleep(0.3)

    # Send the signal.
    server.process.send_signal(send_signal)

    # The server runs a 2-second grace period, so give it a generous timeout.
    try:
        server.process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        server.process.kill()
        server.process.wait(timeout=5)
        pytest.fail(
            f"Server did not exit within 15s after {send_signal.name} "
            f"with {num_clients} active clients"
        )

    # Wait for all client threads to finish.
    for t in threads:
        t.join(timeout=5)

    server_output = get_server_output(server.output_queue)

    # The server must have interrupted active connections.
    assert "interrupted" in server_output and "active connection(s)" in server_output, (
        f"Expected 'interrupted ... active connection(s)' in server output, "
        f"got: {server_output}"
    )

    # Clean shutdown path must have been reached.
    assert "Done running" in server_output

    # Drain the error queue and verify every client saw an error.
    errors = {}
    while not error_queue.empty():
        idx, err = error_queue.get_nowait()
        errors[idx] = err

    assert len(errors) == num_clients, (
        f"Expected {num_clients} client errors, got {len(errors)}; "
        f"missing clients: {set(range(num_clients)) - errors.keys()}"
    )


# ---------------------------------------------------------------------------
# max_temp_directory_size graceful-failure tests
#
# A blocking operator (here a large hash aggregation) that exceeds memory_limit
# spills intermediate data to the temp directory. When that spill would exceed
# max_temp_directory_size, DuckDB raises an out-of-memory error. pgduck_server
# normally treats OOM as fatal (it exit()s so it can be restarted), but
# exceeding the *temp directory cap* is a recoverable, query-scoped failure:
# crashing the whole server would let a single oversized COPY repeatedly take
# down a process shared by every client. duckdb.c special-cases this by matching
# the DuckDB error text 'max_temp_directory_size'.
#
# To make this deterministic we use a cap of 0KiB: the very first block the
# operator tries to offload fails immediately with the temp-size error, before
# any data piles up in memory. (A small-but-nonzero cap is NOT a reliable
# trigger: DuckDB spills up to the cap, then keeps the rest in memory and dies
# on a genuine -- and correctly fatal -- memory OOM instead.) memory_limit is
# kept well above the 256KiB block size and threads=1 so we never hit a genuine
# memory OOM or a thread race.
# ---------------------------------------------------------------------------

SPILL_MEMORY_LIMIT = "32MB"

# A hash aggregation over 10M distinct keys builds a hash table far larger than
# SPILL_MEMORY_LIMIT, forcing DuckDB to offload to the temp directory. Wrapped
# in COUNT(*) so the operator must run to completion but almost no rows cross
# the wire. (CREATE TABLE AS would NOT work: table data is written to the
# database file, not the temp/spill directory.)
SPILL_QUERY = "SELECT count(*) FROM (SELECT i FROM range(10000000) t(i) GROUP BY i) g"

# Load-bearing token: duckdb.c keeps a temp-cap overflow non-fatal by matching
# this substring in the DuckDB error message. If DuckDB changes the wording,
# these assertions fail (the server would start crashing again) -- update the
# DUCKDB_MAX_TEMP_DIR_SIZE_ERROR_TOKEN constant in pgduck_server's duckdb.c too.
TEMP_DIR_SIZE_TOKEN = "max_temp_directory_size"


def _spill_connection():
    conn = psycopg2.connect(host=PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT)
    conn.autocommit = True
    return conn


def _assert_server_alive(server):
    """Prove the server survived a graceful failure without a fixed sleep.

    is_server_listening() actively polls the socket: it returns as soon as the
    server accepts a connection (fast on the happy path) and only consumes its
    full timeout if the server has actually gone away. Combined with poll(),
    this asserts the same process is still serving.
    """
    assert is_server_listening(
        server.socket_path
    ), "pgduck_server stopped accepting connections after a graceful failure"
    assert server.process.poll() is None, "pgduck_server process exited"


def _apply_spill_limits(cur, max_temp_directory_size):
    cur.execute(f"SET GLOBAL memory_limit='{SPILL_MEMORY_LIMIT}'")
    # Single-threaded so the failure is deterministically the temp-size cap
    # (no race between a spilling thread and a thread that fails to pin).
    cur.execute("SET GLOBAL threads='1'")
    cur.execute(f"SET GLOBAL max_temp_directory_size='{max_temp_directory_size}'")
    # pgduck_server silently swallows failed SET commands; if memory_limit had
    # not actually shrunk, the query below would never spill and the test would
    # pass for the wrong reason. Sanity-check it took effect.
    cur.execute("SELECT current_setting('memory_limit')")
    applied = cur.fetchone()[0]
    assert applied not in (None, ""), "memory_limit SET was ignored"


def test_temp_directory_limit_does_not_crash_server():
    """A max_temp_directory_size overflow must fail the query gracefully and
    leave pgduck_server -- and other connections -- alive.

    This is the regression guard for the duckdb.c fix: the server used to exit()
    on this OOM, dropping every connection. It also guards the DuckDB error
    wording (see TEMP_DIR_SIZE_TOKEN): if the token disappears, duckdb.c would
    reclassify the error as fatal, the server would exit(), and the
    poll()/reuse assertions below would fail.
    """
    server = PgDuckServer(port=PGDUCK_PORT, need_output=True)
    assert is_server_listening(server.socket_path)

    # A connection opened BEFORE the failing query. A server crash/restart would
    # drop it, so its survival proves the failure stayed query-scoped.
    bystander = _spill_connection()
    bystander_cur = bystander.cursor()
    bystander_cur.execute("SELECT 1")
    assert bystander_cur.fetchone()[0] == 1

    conn = _spill_connection()
    cur = conn.cursor()
    _apply_spill_limits(cur, "0KiB")

    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(SPILL_QUERY)

    assert TEMP_DIR_SIZE_TOKEN in str(exc_info.value), (
        f"DuckDB error no longer mentions '{TEMP_DIR_SIZE_TOKEN}'; duckdb.c can "
        f"no longer classify it as non-fatal. Got: {exc_info.value}"
    )

    # The server must still be up and serving (not exited on the overflow).
    _assert_server_alive(server)

    # The offending connection is still usable after the graceful error.
    cur.execute("SELECT 42")
    assert cur.fetchone()[0] == 42

    # The bystander connection was never disturbed.
    bystander_cur.execute("SELECT 7")
    assert bystander_cur.fetchone()[0] == 7

    # The server must not have taken its fatal "terminating" path.
    server_output = get_server_output(server.output_queue)
    assert (
        "terminating" not in server_output
    ), f"pgduck_server logged a fatal/terminating path: {server_output}"


def test_temp_directory_limit_reclaims_space():
    """Repeated temp-size failures must not accumulate temp usage, and the
    server must keep doing real work afterwards.

    The original bug was a COPY loop that hit this limit on every iteration, so
    the key properties are: each graceful failure leaves no orphaned spill
    behind (duckdb_temporary_files() stays empty), the failure is repeatable
    without degrading, and a subsequent query still succeeds on the same
    connection.
    """
    server = PgDuckServer(port=PGDUCK_PORT)
    assert is_server_listening(server.socket_path)

    conn = _spill_connection()
    cur = conn.cursor()
    _apply_spill_limits(cur, "0KiB")

    # Hit the limit several times in a row, as the reported COPY loop did.
    for _ in range(3):
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(SPILL_QUERY)
        assert TEMP_DIR_SIZE_TOKEN in str(exc_info.value)

        # No spilled blocks must be left behind after the failure unwinds.
        cur.execute('SELECT COALESCE(SUM("size"), 0) FROM duckdb_temporary_files()')
        assert cur.fetchone()[0] == 0, "temp space was not reclaimed after failure"

    # The server still does real work afterwards: an ungrouped aggregate streams
    # with constant memory (no spill), so it succeeds on the same connection
    # even with the cap still at 0.
    cur.execute("SELECT count(*) FROM range(1000000)")
    assert cur.fetchone()[0] == 1000000


# ---------------------------------------------------------------------------
# Genuine (fatal) OOM over the extended protocol
#
# Exceeding the temp-cap is recoverable (above). A *genuine* RAM out-of-memory
# is NOT: DuckDB cannot guarantee a clean state, so pgduck_server must terminate
# and let systemd restart it. That already worked for the simple-query protocol,
# but the extended (Parse/Bind/Execute) protocol -- the one pg_lake uses --
# reported the error and kept running, leaving a wedged DuckDB that answered
# every subsequent query with a bare "Unknown Error". This guards that fix.
# ---------------------------------------------------------------------------

# Small enough that even a single-threaded hash aggregate cannot keep its working
# set pinned -> a genuine "could not allocate/pin block" OOM, not the temp-cap
# overflow. Single-threaded is the *hardest* case to OOM (smallest working set),
# so forcing threads=1 keeps this deterministic on multi-core CI as well.
GENUINE_OOM_MEMORY_LIMIT = "16MB"

# Same spilling aggregate as SPILL_QUERY, but with a bind parameter so psycopg2
# uses the extended protocol (PQexecParams) -- the prepared-statement path that
# previously failed to terminate on a fatal error.
GENUINE_OOM_QUERY = (
    "SELECT count(*) FROM (SELECT i FROM range(100000000) t(i) GROUP BY i) g "
    "WHERE %s IS NOT NULL"
)


def _wait_for_exit(server, timeout):
    """Poll until the server process exits, up to *timeout* seconds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if server.process.poll() is not None:
            return True
        time.sleep(0.1)
    return False


def test_genuine_oom_over_extended_protocol_terminates_server():
    """A genuine RAM OOM delivered over the extended protocol must terminate the
    server (so it restarts clean), not leave it wedged answering "Unknown Error".

    Regression guard for the pgsession.c fix: the Parse/Bind/Execute handlers
    reported a fatal DuckDB error but, unlike the simple-query path, never
    exit()ed -- so a single oversized query could permanently wedge the process
    shared by every client.
    """
    server = PgDuckServer(
        port=PGDUCK_PORT,
        need_output=True,
        extra_args=["--memory_limit", GENUINE_OOM_MEMORY_LIMIT],
    )
    assert is_server_listening(server.socket_path)

    conn = _spill_connection()
    cur = conn.cursor()
    # threads=1 keeps the genuine-OOM trigger deterministic (see comment above);
    # a large cap means spilling is allowed, so this is NOT the temp-cap path.
    cur.execute("SET GLOBAL threads='1'")
    cur.execute("SET GLOBAL max_temp_directory_size='10GiB'")

    # Passing a parameter forces psycopg2 onto the extended protocol.
    with pytest.raises(psycopg2.Error):
        cur.execute(GENUINE_OOM_QUERY, ("x",))

    assert _wait_for_exit(server, timeout=15), (
        "pgduck_server did not terminate on a genuine OOM over the extended "
        "protocol; it would stay wedged and answer 'Unknown Error' forever"
    )

    # The temp-cap overflow is recoverable and never terminates, so a fatal
    # 'terminating' path proves this was a genuine RAM OOM -- whose real DuckDB
    # message we now forward to the client instead of a bare "Unknown Error".
    server_output = get_server_output(server.output_queue)
    assert (
        "terminating" in server_output
    ), f"expected a fatal 'terminating' path, got: {server_output}"
    assert (
        "Out of Memory Error" in server_output
    ), f"expected the genuine OOM message to be surfaced, got: {server_output}"


# ---------------------------------------------------------------------------
# --temp_directory / --max_temp_directory_size command-line flag tests
#
# The cap and spill location are first-class CLI flags so operators can point
# DuckDB's spill at a chosen disk and bound it without an init file. Crucially
# pgduck_server always applies a bounded default cap (never DuckDB's
# 90%-of-disk default), so spill can never silently consume the whole disk
# shared with PostgreSQL. The flag values are applied via SET GLOBAL during
# startup, before any --init_file_path, so the init file can still override
# them. These tests exercise the flag path end to end (no SET GLOBAL by the
# client), which is what production deployments actually use.
# ---------------------------------------------------------------------------

# Must track DEFAULT_MAX_TEMP_DIRECTORY_SIZE in pgduck_server's command_line.c.
# Asserting the exact value guards against silently regressing to DuckDB's
# disk-relative default (which scales to ~90% of the disk).
DEFAULT_MAX_TEMP_DIRECTORY_SIZE_FORMATTED = "10.0 GiB"


def test_max_temp_directory_size_flag_enforced_gracefully():
    """--max_temp_directory_size is applied at startup and, when exceeded,
    fails the query gracefully without crashing the server -- proving the flag
    is plumbed through and that operators need no init file / SET GLOBAL.

    A 0KiB cap makes the first spilled block fail immediately and
    deterministically (see the SPILL_QUERY notes above).
    """
    server = PgDuckServer(
        port=PGDUCK_PORT,
        need_output=True,
        extra_args=[
            "--memory_limit",
            SPILL_MEMORY_LIMIT,
            "--max_temp_directory_size",
            "0KiB",
        ],
    )
    assert is_server_listening(server.socket_path)

    conn = _spill_connection()
    cur = conn.cursor()

    # The flag -- not a client SET -- put the cap in place.
    cur.execute("SELECT current_setting('max_temp_directory_size')")
    assert cur.fetchone()[0] == "0 bytes"

    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(SPILL_QUERY)
    assert TEMP_DIR_SIZE_TOKEN in str(exc_info.value)

    # Server survived the cap overflow and still serves.
    _assert_server_alive(server)

    cur.execute("SELECT 42")
    assert cur.fetchone()[0] == 42


def test_default_max_temp_directory_size_is_bounded():
    """Without the flag, pgduck_server must still apply its own bounded cap
    rather than inheriting DuckDB's ~90%-of-disk default. This is the guard
    that keeps spill from silently filling a disk shared with PostgreSQL.
    """
    server = PgDuckServer(port=PGDUCK_PORT)
    assert is_server_listening(server.socket_path)

    conn = _spill_connection()
    cur = conn.cursor()
    cur.execute("SELECT current_setting('max_temp_directory_size')")
    assert cur.fetchone()[0] == DEFAULT_MAX_TEMP_DIRECTORY_SIZE_FORMATTED


def test_temp_directory_flag_sets_spill_location():
    """--temp_directory points DuckDB's spill directory at operator-chosen
    storage (e.g. a folder on the PostgreSQL disk)."""
    with tempfile.TemporaryDirectory(dir="/tmp") as spill_dir:
        server = PgDuckServer(
            port=PGDUCK_PORT,
            extra_args=["--temp_directory", spill_dir],
        )
        assert is_server_listening(server.socket_path)

        conn = _spill_connection()
        cur = conn.cursor()
        cur.execute("SELECT current_setting('temp_directory')")
        assert cur.fetchone()[0] == spill_dir

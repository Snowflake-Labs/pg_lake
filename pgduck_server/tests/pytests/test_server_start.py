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

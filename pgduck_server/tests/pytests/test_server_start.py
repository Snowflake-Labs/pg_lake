import pytest
import subprocess
import os
import signal
import time
import tempfile
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
        time.sleep(1)

    # pidfile cleaned up
    assert not os.path.exists(pidfile_path)

import pytest
import subprocess
import os
import signal
import time
from pathlib import Path
import socket
import tempfile
from utils_pytest import *
import platform


PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
PGDUCK_PORT = 8254  # lets a less common port
DUCKDB_DATABASE_FILE_PATH = "/tmp/duckdb.db"
PGDUCK_CACHE_DIR = f"/tmp/cache.{PGDUCK_PORT}"


# runs before each test that uses domain sockets
@pytest.fixture
def clean_socket_path():
    # Setup: remove any existing socket file
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    if socket_path.exists():
        os.remove(socket_path)

    duckdb_database_file_path_p = Path(DUCKDB_DATABASE_FILE_PATH)
    if duckdb_database_file_path_p.exists():
        os.remove(duckdb_database_file_path_p)

    pidfile_path_p = Path(f"/tmp/pgduck_server_test_{os.getpid()}.pid")
    if pidfile_path_p.exists():
        os.remove(pidfile_path_p)

    yield
    # Teardown: remove socket file after test
    if socket_path.exists():
        os.remove(socket_path)
    if duckdb_database_file_path_p.exists():
        os.remove(duckdb_database_file_path_p)
    if pidfile_path_p.exists():
        os.remove(pidfile_path_p)


def test_server_start(clean_socket_path):
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    server = start_server_in_background(
        ["--unix_socket_directory", PGDUCK_UNIX_DOMAIN_PATH, "--port", str(PGDUCK_PORT)]
    )
    assert is_server_listening(socket_path)
    assert has_duckdb_created_file(DUCKDB_DATABASE_FILE_PATH)
    server.terminate()
    server.wait()


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="Abstract sockets no supported on Mac"
)
def test_server_start_abstract_socket():
    abstract_path = Path("@" + PGDUCK_UNIX_DOMAIN_PATH)
    socket_path = abstract_path / f".s.PGSQL.{PGDUCK_PORT}"
    server = start_server_in_background(
        ["--unix_socket_directory", str(abstract_path), "--port", str(PGDUCK_PORT)]
    )
    assert is_server_listening(socket_path)
    assert has_duckdb_created_file(DUCKDB_DATABASE_FILE_PATH)
    server.terminate()
    server.wait()


def test_multiple_server_instances_on_same_socket(clean_socket_path):
    # Start the first server
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    server1 = start_server_in_background(
        ["--unix_socket_directory", PGDUCK_UNIX_DOMAIN_PATH, "--port", str(PGDUCK_PORT)]
    )
    assert is_server_listening(socket_path)

    # Attempt to start a second server on the same socket
    server2 = start_server_in_background(
        ["--unix_socket_directory", PGDUCK_UNIX_DOMAIN_PATH, "--port", str(PGDUCK_PORT)]
    )

    # Check if server2 has terminated (indicating failure to start)
    server2.poll()  # Update server2's status
    assert server2.returncode != 0  # server2 should have exited by now

    # we should be able to connect to the socket again
    assert is_server_listening(socket_path)

    # Clean up
    server1.terminate()
    server1.wait()
    server2.terminate()
    server2.wait()


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="Abstract sockets no supported on Mac"
)
def test_multiple_server_instances_on_same_abstract_socket():
    # Start the first server
    abstract_path = Path("@" + PGDUCK_UNIX_DOMAIN_PATH)
    socket_path = abstract_path / f".s.PGSQL.{PGDUCK_PORT}"
    server1 = start_server_in_background(
        ["--unix_socket_directory", str(abstract_path), "--port", str(PGDUCK_PORT)]
    )
    assert is_server_listening(socket_path)

    # Attempt to start a second server on the same socket
    server2 = start_server_in_background(
        ["--unix_socket_directory", str(abstract_path), "--port", str(PGDUCK_PORT)]
    )

    # Check if server2 has terminated (indicating failure to start)
    server2.poll()  # Update server2's status
    assert server2.returncode != 0  # server2 should have exited by now

    # we should be able to connect to the socket again
    assert is_server_listening(socket_path)

    # Clean up
    server1.terminate()
    server1.wait()
    server2.terminate()
    server2.wait()


def test_multiple_server_instances_on_duckdb_file_path_socket(clean_socket_path):
    # Start the first server
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    server1 = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(PGDUCK_PORT),
            "--duckdb_database_file_path",
            "/tmp/data1.db",
        ]
    )
    assert is_server_listening(socket_path)

    # Attempt to start a second server on the duckdb_database_file_path
    server2 = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(PGDUCK_PORT + 1),
            "--duckdb_database_file_path",
            "/tmp/data1.db",
        ],
        True,
    )

    output_queue_2 = queue.Queue()
    output_thread_2 = threading.Thread(
        target=capture_output, args=(server2.stderr, output_queue_2)
    )
    output_thread_2.start()

    start_time = time.time()
    found_error = False
    while (time.time() - start_time) < 20:  # loop at most 20 seconds
        try:
            # Check if there is any output indicating the server is ready
            line = output_queue_2.get_nowait()
            if line and "error initialization DuckDB" in line:
                found_error = True
                break
        except queue.Empty:
            time.sleep(0.1)  # No output yet, continue waiting

    # Check if server2 has terminated (indicating failure to start)
    server2.poll()  # Update server2's status
    assert server2.returncode != 0  # server2 should have exited by now
    assert found_error == True

    # we should be able to connect to the socket again
    assert is_server_listening(socket_path)
    assert has_duckdb_created_file("/tmp/data1.db")

    # Clean up
    try:
        _, _ = server1.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        server1.kill()
        _, _ = server1.communicate()


def test_two_servers_different_ports(clean_socket_path):
    socket_path1 = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    socket_path2 = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT + 1}"

    server1 = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(PGDUCK_PORT),
            "--duckdb_database_file_path",
            "/tmp/data1.db",
        ]
    )
    server2 = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(PGDUCK_PORT + 1),
            "--duckdb_database_file_path",
            "/tmp/data2.db",
        ]
    )

    assert is_server_listening(socket_path1)
    assert is_server_listening(socket_path2)

    assert has_duckdb_created_file("/tmp/data1.db")
    assert has_duckdb_created_file("/tmp/data2.db")

    server1.terminate()
    server1.wait()
    server2.terminate()
    server2.wait()


@pytest.mark.skipif(
    platform.system() == "Darwin", reason="Abstract sockets no supported on Mac"
)
def test_two_servers_different_abstract_ports():
    abstract_path = Path("@" + PGDUCK_UNIX_DOMAIN_PATH)
    socket_path1 = abstract_path / f".s.PGSQL.{PGDUCK_PORT}"
    socket_path2 = abstract_path / f".s.PGSQL.{PGDUCK_PORT + 1}"

    server1 = start_server_in_background(
        [
            "--unix_socket_directory",
            str(abstract_path),
            "--port",
            str(PGDUCK_PORT),
            "--duckdb_database_file_path",
            "/tmp/data1.db",
        ]
    )
    server2 = start_server_in_background(
        [
            "--unix_socket_directory",
            str(abstract_path),
            "--port",
            str(PGDUCK_PORT + 1),
            "--duckdb_database_file_path",
            "/tmp/data2.db",
        ]
    )

    assert is_server_listening(socket_path1)
    assert is_server_listening(socket_path2)

    assert has_duckdb_created_file("/tmp/data1.db")
    assert has_duckdb_created_file("/tmp/data2.db")

    server1.terminate()
    server1.wait()
    server2.terminate()
    server2.wait()


def test_two_servers_different_paths(clean_socket_path):
    socket_path1 = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"

    # Create a temporary directory
    with tempfile.TemporaryDirectory(dir="/tmp") as temp_dir:
        socket_path2 = Path(temp_dir) / f".s.PGSQL.{PGDUCK_PORT}"

        server1 = start_server_in_background(
            [
                "--unix_socket_directory",
                PGDUCK_UNIX_DOMAIN_PATH,
                "--port",
                str(PGDUCK_PORT),
                "--duckdb_database_file_path",
                "/tmp/data1.db",
            ]
        )
        server2 = start_server_in_background(
            [
                "--unix_socket_directory",
                temp_dir,
                "--port",
                str(PGDUCK_PORT),
                "--duckdb_database_file_path",
                "/tmp/data2.db",
            ]
        )

        assert is_server_listening(socket_path1)
        assert is_server_listening(socket_path2)

        server1.terminate()
        server1.wait()
        server2.terminate()
        server2.wait()


# Failure scenario tests
def test_server_invalid_port(clean_socket_path):
    invalid_port = "invalid_port"
    server = start_server_in_background(
        ["--unix_socket_directory", PGDUCK_UNIX_DOMAIN_PATH, "--port", invalid_port]
    )
    server.poll()
    assert server.returncode != 0
    server.terminate()
    server.wait()


def test_server_excessively_high_port(clean_socket_path):
    excessively_high_port = "65536"  # Above the valid port range
    server = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            excessively_high_port,
        ]
    )
    server.poll()
    assert server.returncode != 0
    server.terminate()
    server.wait()


def test_server_with_nonexistent_socket_directory(clean_socket_path):
    nonexistent_directory = "/nonexistent/directory"
    server = start_server_in_background(
        ["--unix_socket_directory", nonexistent_directory, "--port", str(PGDUCK_PORT)]
    )
    server.poll()
    assert server.returncode != 0
    server.terminate()
    server.wait()


def test_server_exit_code_and_error_message_for_invalid_socket(clean_socket_path):
    invalid_socket_path = "/invalid/path"
    server = start_server_in_background(
        ["--unix_socket_directory", invalid_socket_path, "--port", str(PGDUCK_PORT)]
    )
    assert server.returncode != 0
    server.terminate()


def test_long_unix_socket_path(clean_socket_path):
    long_socket_path = "/tmp/" + "a" * 100  # Create an overly long socket path
    server = start_server_in_background(
        ["--unix_socket_directory", long_socket_path, "--port", str(PGDUCK_PORT)]
    )
    assert server.returncode != 0
    server.terminate()


@pytest.mark.parametrize("use_debug", [False, True])
def test_server_debug_messages(clean_socket_path, use_debug):
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    params = [
        "--unix_socket_directory",
        PGDUCK_UNIX_DOMAIN_PATH,
        "--port",
        str(PGDUCK_PORT),
    ]

    if use_debug:
        params.append("--debug")

    server, output_queue, stderr_thread = capture_output_queue(
        start_server_in_background(params, True)
    )

    assert is_server_listening(socket_path)
    assert has_duckdb_created_file(DUCKDB_DATABASE_FILE_PATH)

    # connect to our server, issue our command
    conn = psycopg2.connect(host=PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT)

    # verify we find our log message at debug level
    cur = conn.cursor()
    query = "SELECT 'query_appears_in_output'"

    cur.execute(query)

    server_output = get_server_output(output_queue)
    found = query in server_output

    if use_debug:
        assert found, "Missing expected query in output"
    else:
        assert not found, "Unexpectedly found query in output (should be suppressed)"

    cur.close()
    conn.close()

    server.terminate()
    server.wait()


def test_server_pidfile(clean_socket_path):
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    pidfile_path = f"/tmp/pgduck_server_test_{os.getpid()}.pid"

    assert not os.path.exists(pidfile_path)

    server = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(PGDUCK_PORT),
            "--pidfile",
            pidfile_path,
            "--duckdb_database_file_path",
            "/tmp/data1.db",
        ]
    )

    assert is_server_listening(socket_path)
    assert os.path.exists(pidfile_path)

    # test sending external signal

    server.terminate()
    server.wait()

    time.sleep(1)

    # pidfile cleaned up
    assert not os.path.exists(pidfile_path)


# ensure we handle pidfiles properly when sending normal stop signals or interrupt
@pytest.mark.parametrize("send_signal", [signal.SIGINT, signal.SIGTERM])
def test_server_pidfile_signal(clean_socket_path, send_signal):
    socket_path = Path(PGDUCK_UNIX_DOMAIN_PATH) / f".s.PGSQL.{PGDUCK_PORT}"
    pidfile_path = f"/tmp/pgduck_server_test_{os.getpid()}.pid"

    assert not os.path.exists(pidfile_path)

    server = start_server_in_background(
        [
            "--unix_socket_directory",
            PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(PGDUCK_PORT),
            "--pidfile",
            pidfile_path,
            "--duckdb_database_file_path",
            "/tmp/data1.db",
        ]
    )

    assert is_server_listening(socket_path)
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

import pytest
from utils_pytest import *
from utils_protocol import *
from pathlib import Path
import socket

# use > 1 to make sure multiple failures
# do not cause problems
consecutive_fail_cnt = 3


def _read_until_message(sock, target_type):
    while True:
        header = b""
        while len(header) < 5:
            chunk = sock.recv(5 - len(header))
            if not chunk:
                raise AssertionError(f"connection closed before {target_type!r}")
            header += chunk

        message_type = chr(header[0])
        message_length = int.from_bytes(header[1:5], byteorder="big")
        remaining = message_length - 4
        while remaining > 0:
            chunk = sock.recv(remaining)
            if not chunk:
                raise AssertionError(f"truncated {message_type!r} response")
            remaining -= len(chunk)

        if message_type == target_type:
            return


def test_wrong_startup_msg(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    for _ in range(0, consecutive_fail_cnt):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path_str)

            # Send a malformed message
            message = "This is not a valid PostgreSQL protocol message"
            s.sendall(message.encode())

            # Receive response (if any)
            response = s.recv(1024)

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_wrong_frontend_protocol(pgduck_server):

    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    # send wrong protocol message 5 times
    for _ in range(0, consecutive_fail_cnt):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path_str)

            send_startup_message(s)

            # Send a malformed message
            message = "This is not a valid PostgreSQL protocol message"
            s.sendall(message.encode())

            # Receive response (if any)
            response = s.recv(1024)

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_termination_message(pgduck_server):

    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )

    # send termination message 5 times
    for _ in range(0, consecutive_fail_cnt):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path_str)
            send_startup_message(s)

            # receive whatever the server gives back to us
            s.recv(10000)

            # after sending the termination the socket is closed
            # which means that recv wont return any data
            send_termination(s)
            data = s.recv(1)
            is_socket_closed = len(data) == 0

            assert is_socket_closed, "Socket should be closed after termination"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_close_message(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    # send bind message 3 times
    for _ in range(0, 3):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path_str)
            send_startup_message(s)

            # receive whatever the server gives back to us
            s.recv(10000)

            send_close_message(s)

            res = s.recv(10000)

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_copy_data_message(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    # send bind message 3 times
    for _ in range(0, 3):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path_str)
            send_startup_message(s)

            # receive whatever the server gives back to us
            s.recv(10000)

            send_copy_data_message(s, b"Hello, PostgreSQL!")

            res = s.recv(10000)

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


@pytest.mark.parametrize("parameter_length", [-2, 100])
def test_bind_invalid_parameter_length(pgduck_server, parameter_length):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect(socket_path_str)
        send_startup_message(s)
        _read_until_message(s, "Z")

        send_parse_message(s, "SELECT $1::INTEGER")
        # message bytes are invalid.
        payload = b"\x00\x00\x00\x00\x00\x01" + parameter_length.to_bytes(
            4, byteorder="big", signed=True
        )
        send_message(s, "B", payload)
        assert s.recv(1) == b""

    # A malformed bind must not affect other connections.
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_describe_message_without_type(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect(socket_path_str)
        send_startup_message(s)
        _read_until_message(s, "Z")

        send_message(s, "D")
        assert s.recv(1) == b""

    # A truncated describe must not affect other connections.
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

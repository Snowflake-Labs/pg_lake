import pytest
from utils_pytest import *
from utils_protocol import *
from pathlib import Path
import socket

# use > 1 to make sure multiple failures
# do not cause problems
consecutive_fail_cnt = 3


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

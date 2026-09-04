"""Wire-protocol coverage for pgduck_server's NegotiateProtocolVersion handling.

The NegotiateProtocolVersion ('v') backend message itself predates these
releases, but two newer changes made pgduck_server actually have to handle it.
PostgreSQL 18 bumped the latest wire protocol to 3.2, and PostgreSQL 19's libpq
now probes servers on connect by requesting a deliberately-too-high minor
version (PG_PROTOCOL_GREASE = 3.9999) and/or by sending forward-compatible
"_pq_.*" protocol options. A server that only speaks 3.0 -- like pgduck_server
-- must:

  * accept the connection,
  * reply with a 'v' message advertising the version it actually speaks (3.0),
  * echo back the name of every "_pq_.*" option it did not recognize,

otherwise modern libpq aborts with "server did not report the unsupported ...
parameter in its protocol negotiation message".

These tests drive the raw wire protocol so they pin pgduck_server's behaviour
deterministically, independent of whatever libpq version happens to run the
suite. They also guard against regressing the plain-3.0 fast path (which must
NOT emit a 'v' message) and against a malformed higher-major request taking the
server down.
"""

import socket
from pathlib import Path

import pytest
from utils_pytest import *
from utils_protocol import *


def _connect():
    socket_path = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(socket_path)
    return s


def _assert_server_still_usable():
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_plain_3_0_startup_has_no_negotiation(pgduck_server):
    """A vanilla 3.0 client must NOT receive a NegotiateProtocolVersion message;
    the very first backend message is AuthenticationOk ('R')."""
    with _connect() as s:
        send_startup_message(s)  # defaults to protocol 3.0
        msg_type, _ = read_backend_message(s)
        assert (
            msg_type == "R"
        ), f"expected AuthenticationOk first on a 3.0 connection, got {msg_type!r}"
        # And the session reaches ReadyForQuery as usual.
        read_until(s, "Z")
    _assert_server_still_usable()


@pytest.mark.parametrize(
    "protocol_version",
    [PG_PROTOCOL_3_2, PG_PROTOCOL_GREASE],
    ids=["minor_3_2", "grease_3_9999"],
)
def test_negotiate_downgrades_to_3_0(pgduck_server, protocol_version):
    """A client requesting a 3.x minor version > 0 receives a 'v' that advertises
    exactly 3.0 with no unrecognized options, then a fully usable session."""
    with _connect() as s:
        send_startup_message(s, protocol_version=protocol_version)

        msg_type, payload = read_backend_message(s)
        assert (
            msg_type == "v"
        ), f"expected NegotiateProtocolVersion for {protocol_version}, got {msg_type!r}"
        version, names = parse_negotiate_protocol_version(payload)
        assert version == PG_PROTOCOL_3_0, f"server advertised {version}, expected 3.0"
        assert names == [], f"unexpected unrecognized options: {names}"

        # Startup must still complete after negotiation.
        read_until(s, "Z")

        # ... and the negotiated session must actually execute queries.
        send_query_message(s, "SELECT 1")
        types = [m[0] for m in read_until(s, "Z")]
        assert "C" in types, f"SELECT 1 did not complete (saw {types})"
    _assert_server_still_usable()


def test_negotiate_without_pq_options_reports_zero(pgduck_server):
    """A higher minor version carrying only ordinary parameters yields a 'v'
    message with a zero unrecognized-option count."""
    with _connect() as s:
        send_startup_message(
            s,
            protocol_version=PG_PROTOCOL_3_2,
            params={"user": "postgres", "application_name": "pg19-protocol-test"},
        )
        msg_type, payload = read_backend_message(s)
        assert msg_type == "v", f"expected NegotiateProtocolVersion, got {msg_type!r}"
        version, names = parse_negotiate_protocol_version(payload)
        assert version == PG_PROTOCOL_3_0
        assert names == []
        read_until(s, "Z")
    _assert_server_still_usable()


def test_negotiate_echoes_unrecognized_pq_options(pgduck_server):
    """Every "_pq_.*" option the client sends must be echoed back verbatim in the
    'v' message (libpq aborts otherwise), while ordinary parameters are not."""
    params = {
        "_pq_.protocol_managed_a": "1",
        "user": "postgres",
        "_pq_.protocol_managed_b": "on",
        "database": "regression",
    }
    with _connect() as s:
        send_startup_message(s, protocol_version=PG_PROTOCOL_GREASE, params=params)

        msg_type, payload = read_backend_message(s)
        assert msg_type == "v", f"expected NegotiateProtocolVersion, got {msg_type!r}"
        version, names = parse_negotiate_protocol_version(payload)
        assert version == PG_PROTOCOL_3_0
        assert set(names) == {
            "_pq_.protocol_managed_a",
            "_pq_.protocol_managed_b",
        }, f"echoed option names were {names}"

        read_until(s, "Z")
    _assert_server_still_usable()


def test_negotiate_echoes_single_pq_option(pgduck_server):
    """The common libpq case: a single unrecognized _pq_ option at GREASE."""
    with _connect() as s:
        send_startup_message(
            s,
            protocol_version=PG_PROTOCOL_GREASE,
            params={"_pq_.report_meaningless_option": "1", "user": "postgres"},
        )
        msg_type, payload = read_backend_message(s)
        assert msg_type == "v"
        version, names = parse_negotiate_protocol_version(payload)
        assert version == PG_PROTOCOL_3_0
        assert names == ["_pq_.report_meaningless_option"]
        read_until(s, "Z")
    _assert_server_still_usable()


def test_unsupported_major_version_rejected(pgduck_server):
    """A non-3 major protocol version must be rejected and the connection closed,
    without ever emitting a 'v'/auth message and without taking the server down."""
    # Repeat so a failure can't leave the listener in a bad state for others.
    for _ in range(3):
        with _connect() as s:
            send_startup_message(s, protocol_version=PG_PROTOCOL_4_0)
            data = s.recv(1024)
            assert (
                data == b""
            ), f"expected connection close for major != 3, got {data!r}"
    _assert_server_still_usable()

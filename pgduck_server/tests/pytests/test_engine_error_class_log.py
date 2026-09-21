"""Coverage for the classified log line pgduck_server emits for engine errors.

The line carries a canned class and nothing else, so that a log collector can
match on its prefix and collect nothing else from this process. Every other line
pgduck_server writes may legitimately contain the DuckDB message, the failing
statement, or the client address, which is why the assertions here isolate the
classified line instead of searching the whole output.
"""

import os
import tempfile
from contextlib import contextmanager

import pytest
from utils_pytest import *


PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
PGDUCK_PORT = 8259  # its own port, so a shared server cannot answer instead
DUCKDB_DATABASE_FILE_PATH = "/tmp/pgduck_engine_error_class.db"

ENGINE_ERROR_PREFIX = "pgduck_engine_error: "

# Spill setup copied from test_server_start.py: a 0-byte temp cap makes the
# overflow deterministic, while a memory_limit well above DuckDB's block size and
# threads=1 keep it a recoverable temp-cap OOM rather than a fatal memory OOM.
SPILL_MEMORY_LIMIT = "32MB"
SPILL_QUERY = "SELECT count(*) FROM (SELECT i FROM range(10000000) t(i) GROUP BY i) g"

# A path that does not exist, distinctive enough that finding it on the classified
# line can only mean the line leaked its query.
MISSING_FILE = "/tmp/pgduck_class_log_absent_9f3c1d.parquet"


def _connect():
    conn = psycopg2.connect(host=PGDUCK_UNIX_DOMAIN_PATH, port=PGDUCK_PORT)
    conn.autocommit = True
    return conn


def _start_server(need_output=True, extra_args=None):
    """Start a server and wait for it to accept connections.

    pgduck_server binds its socket only after DuckDB is initialized, so
    connecting without this wait races that startup.
    """
    server = PgDuckServer(
        port=PGDUCK_PORT,
        duckdb_database_file_path=DUCKDB_DATABASE_FILE_PATH,
        need_output=need_output,
        extra_args=extra_args,
    )
    assert is_server_listening(server.socket_path), "pgduck_server did not start"
    return server


@contextmanager
def _spill_server(extra_args=None):
    """Start a server whose spill knobs come from an init file, as production does."""
    with tempfile.TemporaryDirectory(dir="/tmp") as cfg_dir:
        init_file = os.path.join(cfg_dir, "init.sql")
        with open(init_file, "w") as f:
            f.write(f"SET GLOBAL memory_limit='{SPILL_MEMORY_LIMIT}';\n")
            f.write("SET GLOBAL threads='1';\n")
            f.write("SET GLOBAL max_temp_directory_size='0KiB';\n")
        yield _start_server(
            extra_args=["--init_file_path", init_file] + list(extra_args or [])
        )


def _class_lines(server):
    output = get_server_output(server.output_queue)
    return [line for line in output.splitlines() if ENGINE_ERROR_PREFIX in line]


def _assert_class(server, expected, *must_not_appear):
    """Assert the expected class was logged, and that the line carries nothing else.

    *must_not_appear* holds fragments of the statement and of the DuckDB message.
    They are checked against the classified lines only: the neighbouring WARNING
    lines contain all of them by design and would mask a leak.
    """
    lines = _class_lines(server)

    assert lines, f"pgduck_server logged no {ENGINE_ERROR_PREFIX!r} line"

    assert any(
        ENGINE_ERROR_PREFIX + expected in line for line in lines
    ), f"expected class {expected!r}, got: {lines!r}"

    for line in lines:
        for fragment in must_not_appear:
            assert fragment not in line, (
                f"classified line leaked {fragment!r}, which must never reach a "
                f"collected log line: {line!r}"
            )


def test_io_error_class_is_logged_without_the_path():
    """A missing file is an IO error, and the path must not ride along."""
    server = _start_server()

    cur = _connect().cursor()
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(f"SELECT * FROM read_parquet('{MISSING_FILE}')")

    assert (
        exc_info.value.pgcode == "58030"
    ), f"missing file should report SQLSTATE 58030, got {exc_info.value.pgcode}"

    _assert_class(server, "io_error", MISSING_FILE, "read_parquet")


def test_invalid_input_class_is_logged_without_the_value():
    """A bad format string is invalid input; neither it nor the value may appear."""
    server = _start_server()
    cur = _connect().cursor()
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT strptime('not-a-date', '%Y-%m-%d')")

    assert (
        exc_info.value.pgcode == "22023"
    ), f"invalid input should report SQLSTATE 22023, got {exc_info.value.pgcode}"

    _assert_class(server, "invalid_input", "not-a-date", "strptime")


def test_invalid_input_class_is_logged_over_extended_protocol():
    """A bind parameter puts psycopg2 on Parse/Bind/Execute, the path pg_lake uses."""
    server = _start_server()
    cur = _connect().cursor()
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT strptime(%s, %s)", ("not-a-date", "%Y-%m-%d"))

    assert (
        exc_info.value.pgcode == "22023"
    ), f"invalid input should report SQLSTATE 22023, got {exc_info.value.pgcode}"

    _assert_class(server, "invalid_input", "not-a-date", "strptime")


def test_unmapped_error_is_logged_as_other():
    """A catalog error is not a class we distinguish, so it lands in "other"."""
    server = _start_server()
    cur = _connect().cursor()
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT * FROM no_such_table_1a2b3c")

    assert (
        exc_info.value.pgcode == "0A000"
    ), f"unmapped error should stay SQLSTATE 0A000, got {exc_info.value.pgcode}"

    _assert_class(server, "other", "no_such_table_1a2b3c")


def test_recoverable_out_of_memory_class_is_logged():
    """A temp-cap overflow is a recoverable OOM: classified, and the server lives."""
    with _spill_server() as server:
        assert is_server_listening(server.socket_path)

        cur = _connect().cursor()
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(SPILL_QUERY)

        assert (
            exc_info.value.pgcode == "53200"
        ), f"recoverable OOM should report SQLSTATE 53200, got {exc_info.value.pgcode}"

        _assert_class(server, "out_of_memory", "range(10000000)")

        assert is_server_listening(
            server.socket_path
        ), "pgduck_server stopped accepting connections after a recoverable OOM"
        assert server.process.poll() is None, "pgduck_server process exited"


def test_no_log_engine_errors_suppresses_the_class_line():
    """--no_log_engine_errors drops the line but not the error itself."""
    server = _start_server(extra_args=["--no_log_engine_errors"])
    cur = _connect().cursor()
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(f"SELECT * FROM read_parquet('{MISSING_FILE}')")

    # The client still learns what went wrong; only the log line is gone.
    assert exc_info.value.pgcode == "58030"

    assert not _class_lines(
        server
    ), "--no_log_engine_errors still logged a classified line"


def test_no_log_engine_errors_is_announced_at_startup():
    """Disabling classification must be visible, since silence otherwise reads
    as "no errors occurred" rather than "nothing is being classified".

    The announcement must also stay invisible to a collector allow-listing the
    "pgduck_engine_error: " prefix, so it carries the bare name without the
    colon.
    """
    server = _start_server(extra_args=["--no_log_engine_errors"])
    output = get_server_output(server.output_queue)

    assert (
        "Engine error classification is off" in output
    ), f"startup did not announce the disabled classification: {output!r}"

    assert (
        ENGINE_ERROR_PREFIX not in output
    ), f"startup line matches the collected prefix {ENGINE_ERROR_PREFIX!r}: {output!r}"

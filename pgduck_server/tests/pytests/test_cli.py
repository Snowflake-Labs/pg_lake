import subprocess
import pytest
from pathlib import Path
from utils_pytest import *


PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
PGDUCK_PORT = 5332
DUCKDB_DATABASE_FILE_PATH = "/tmp/duckdb.db"
PGDUCK_CACHE_DIR = f"/tmp/cache.{PGDUCK_PORT}"
DEFAULT_MAX_CLIENTS = 10000
DEFAULT_MEMORY_LIMIT = "80 percent of the system memory"
DEFAULT_CACHE_ON_WRITE_SIZE = 1024 * 1024 * 1024


def assert_common_output(
    stderr,
    verbose=False,
    unix_socket_directory=PGDUCK_UNIX_DOMAIN_PATH,
    port=PGDUCK_PORT,
    duckdb_database_file_path=DUCKDB_DATABASE_FILE_PATH,
    max_clients=DEFAULT_MAX_CLIENTS,
    memory_limit=DEFAULT_MEMORY_LIMIT,
    max_cache_on_write=DEFAULT_CACHE_ON_WRITE_SIZE,
    extension_installation=True,
    debug=False,
):
    print(stderr)
    assert (
        f"pgduck_server is listening on unix_socket_directory: {unix_socket_directory} with port {port}, max_clients allowed {max_clients}"
        in stderr
    )
    assert f"DuckDB is using database file path: {duckdb_database_file_path}" in stderr
    assert f"{memory_limit}" in stderr
    assert f"Cache on write max size is set to: {max_cache_on_write}" in stderr

    if verbose:
        assert "Verbose mode enabled." in stderr
    else:
        assert "Verbose mode enabled." not in stderr

    if debug:
        assert "Debugging mode on" in stderr
    else:
        assert "Debugging mode on" not in stderr
    if not extension_installation:
        assert "Using local extension binaries only" in stderr


def test_default_run():
    returncode, stdout, stderr = run_cli_command([])
    assert returncode == 0
    assert_common_output(stderr)


def test_verbose():
    returncode, stdout, stderr = run_cli_command(["--verbose"])
    assert returncode == 0
    assert_common_output(stderr, verbose=True)


def test_max_clients():
    returncode, stdout, stderr = run_cli_command(["--max_clients", "300"])
    assert returncode == 0
    assert_common_output(stderr, max_clients=300)


def test_memory_limit():
    returncode, stdout, stderr = run_cli_command(["--memory_limit", "1GB"])
    assert returncode == 0
    assert_common_output(stderr, memory_limit="Memory limit is set to: 1GB")


def test_cache_on_write():
    returncode, stdout, stderr = run_cli_command(["--cache_on_write_max_size", "1000"])
    assert returncode == 0
    assert_common_output(stderr, max_cache_on_write="1000")


def test_cache_on_write_large_value():
    # Test 10GB value (larger than INT_MAX ~2.1GB)
    returncode, stdout, stderr = run_cli_command(
        ["--cache_on_write_max_size", "10737418240"]
    )
    assert returncode == 0
    assert_common_output(stderr, max_cache_on_write="10737418240")


def test_no_extension_installation():
    returncode, stdout, stderr = run_cli_command(["--no_extension_install"])
    assert returncode == 0
    assert_common_output(stderr, extension_installation=False)


def test_multiple_memory_limits():
    # when the same argument is specified twice, we always pick the last one
    returncode, stdout, stderr = run_cli_command(
        ["--memory_limit=1GB", "--memory_limit=100MB"]
    )
    assert returncode == 0
    assert_common_output(stderr, memory_limit="Memory limit is set to: 100MB")


def test_multiple_max_clients():
    returncode, stdout, stderr = run_cli_command(
        ["--max_clients", "300", "--max_clients", "600"]
    )
    assert returncode == 0
    assert_common_output(stderr, max_clients=600)


def test_unix_socket_directory():
    returncode, stdout, stderr = run_cli_command(
        ["--unix_socket_directory", "/tmp/testdir"]
    )
    assert returncode == 0
    assert_common_output(stderr, unix_socket_directory="/tmp/testdir")


def test_duckdb_file_path():
    returncode, stdout, stderr = run_cli_command(
        ["--duckdb_database_file_path", "/tmp/db.db"]
    )
    assert returncode == 0
    assert_common_output(stderr, duckdb_database_file_path="/tmp/db.db")


def test_port():
    returncode, stdout, stderr = run_cli_command(["--port", "8080"])
    assert returncode == 0
    assert_common_output(stderr, port=8080)


def test_verbose_unix_socket_directory():
    returncode, stdout, stderr = run_cli_command(
        ["--verbose", "--unix_socket_directory", "/tmp/testdir"]
    )
    assert returncode == 0
    assert_common_output(stderr, verbose=True, unix_socket_directory="/tmp/testdir")


def test_verbose_port():
    returncode, stdout, stderr = run_cli_command(["--verbose", "--port", "8080"])
    assert returncode == 0
    assert_common_output(stderr, verbose=True, port=8080)


def test_unix_socket_directory_port():
    returncode, stdout, stderr = run_cli_command(
        ["--unix_socket_directory", "/tmp/testdir", "--port", "8080"]
    )
    assert returncode == 0
    assert_common_output(stderr, unix_socket_directory="/tmp/testdir", port=8080)


def test_verbose_unix_socket_directory_port():
    returncode, stdout, stderr = run_cli_command(
        ["--verbose", "--unix_socket_directory", "/tmp/testdir", "--port", "8080"]
    )
    assert returncode == 0
    assert_common_output(
        stderr, verbose=True, unix_socket_directory="/tmp/testdir", port=8080
    )


def test_short_verbose():
    returncode, stdout, stderr = run_cli_command(["-v"])
    assert returncode == 0
    assert_common_output(stderr, verbose=True)


def test_debug():
    returncode, stdout, stderr = run_cli_command(["--debug"])
    assert returncode == 0
    assert_common_output(stderr, debug=True)


def test_short_debug():
    returncode, stdout, stderr = run_cli_command(["-d"])
    assert returncode == 0
    assert_common_output(stderr, debug=True)


def test_short_unix_socket_directory():
    returncode, stdout, stderr = run_cli_command(["-U", "/tmp/testdir"])
    assert returncode == 0
    assert_common_output(stderr, unix_socket_directory="/tmp/testdir")


def test_short_port():
    returncode, stdout, stderr = run_cli_command(["-P", "8080"])
    assert returncode == 0
    assert_common_output(stderr, port=8080)


def test_short_verbose_unix_socket_directory():
    returncode, stdout, stderr = run_cli_command(["-v", "-U", "/tmp/testdir"])
    assert returncode == 0
    assert_common_output(stderr, verbose=True, unix_socket_directory="/tmp/testdir")


def test_short_verbose_port():
    returncode, stdout, stderr = run_cli_command(["-v", "-P", "8080"])
    assert returncode == 0
    assert_common_output(stderr, verbose=True, port=8080)


def test_short_unix_socket_directory_port():
    returncode, stdout, stderr = run_cli_command(["-U", "/tmp/testdir", "-P", "8080"])
    assert returncode == 0
    assert_common_output(stderr, unix_socket_directory="/tmp/testdir", port=8080)


def test_short_verbose_unix_socket_directory_port():
    returncode, stdout, stderr = run_cli_command(
        ["-v", "-U", "/tmp/testdir", "-P", "8080"]
    )
    assert returncode == 0
    assert_common_output(
        stderr, verbose=True, unix_socket_directory="/tmp/testdir", port=8080
    )


def test_invalid_port_text():
    returncode, stdout, stderr = run_cli_command(["--port", "notaport"])
    assert returncode == 1
    assert "Port should be an integer" in stderr


def test_invalid_option():
    returncode, stdout, stderr = run_cli_command(["--invalidoption"])
    assert returncode == 1
    assert "unrecognized option" in stderr


def test_port_out_of_range():
    returncode, stdout, stderr = run_cli_command(["--port", "70000"])
    assert returncode == 1
    assert "Port should be in between [1, 65535]" in stderr


def test_no_argument_for_required_option():
    returncode, stdout, stderr = run_cli_command(["--unix_socket_directory"])
    assert returncode == 1
    assert "requires an argument" in stderr
    assert "--unix_socket_directory" in stderr


def test_help_with_other_options():
    returncode, stdout, stderr = run_cli_command(["--help", "--verbose"])
    assert returncode == 0
    assert "Usage:" in stdout


def test_short_option_no_argument():
    returncode, stdout, stderr = run_cli_command(["-U"])
    assert returncode == 1
    assert "option requires an argument" in stderr


def test_multiple_unix_socket_directory_last_one_picked():
    returncode, stdout, stderr = run_cli_command(
        ["--unix_socket_directory", "dir1", "--unix_socket_directory", "dir2"]
    )
    assert returncode == 0
    assert_common_output(stderr, unix_socket_directory="dir2")


def test_multiple_database_file_last_one_picked():
    returncode, stdout, stderr = run_cli_command(
        [
            "--duckdb_database_file_path",
            "/tmp/f1.db",
            "--duckdb_database_file_path",
            "/tmp/f2.db",
        ]
    )
    assert returncode == 0
    assert_common_output(stderr, duckdb_database_file_path="/tmp/f2.db")


def test_long_short_unix_socket_directory_combination():
    returncode, stdout, stderr = run_cli_command(
        ["--unix_socket_directory", "dir1", "-U", "dir3"]
    )
    assert returncode == 0
    assert_common_output(stderr, unix_socket_directory="dir3")


def test_multiple_port_last_one_picked():
    returncode, stdout, stderr = run_cli_command(["--port", "8080", "--port", "9090"])
    assert returncode == 0
    assert_common_output(stderr, port=9090)


def test_long_short_port_combination():
    returncode, stdout, stderr = run_cli_command(["--port", "8080", "-P", "9090"])
    assert returncode == 0
    assert_common_output(stderr, port=9090)

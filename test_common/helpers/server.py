"""PgDuck server and PostgreSQL server management utilities."""

import atexit
import glob
import grp
import os
import platform
import queue
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import psycopg2
import pytest

from . import server_params
from .cloud_storage import (
    AZURITE_CONNECTION_STRING,
    MANAGED_STORAGE_BUCKET,
    MOTO_PORT,
    MOTO_PORT_GCS,
    TEST_AWS_ACCESS_KEY_ID,
    TEST_AWS_SECRET_ACCESS_KEY,
    TEST_BUCKET,
    TEST_BUCKET_GCS,
)
from .db import (
    capture_output,
    default_connection_string,
    get_server_output,
    open_pg_conn,
    run_command,
    run_query,
    terminate_process,
)


PGDUCK_SERVER_PROCESS_NAME = "pgduck_server"

PG_CONFIG = os.environ.get("PG_CONFIG", "pg_config")
PG_BINDIR = subprocess.run(
    [PG_CONFIG, "--bindir"], capture_output=True, text=True
).stdout.rstrip()

log_level = os.getenv("LOG_MIN_MESSAGES", "notice")


# ---------------------------------------------------------------------------
# PgDuck server helpers
# ---------------------------------------------------------------------------

def get_pgduck_server_path():
    pgduck_server = subprocess.run(
        ["which", "pgduck_server"], capture_output=True, text=True
    ).stdout.rstrip()
    # error if not found?
    return Path(pgduck_server)


def setup_pgduck_server():

    # Stop any leftover pgduck_server from a previous interrupted run.
    # This must happen before PgDuckServer.__init__ removes stale artifacts
    # because a leftover server may still be running; removing its socket
    # without killing the process first leaves it silently orphaned.
    cleanup_stale_pidfiles()

    # Use some arbitrary group to test the unix_socket_group logic
    gid = os.getgroups()[0]
    group_name = grp.getgrgid(gid).gr_name

    # Set up test secrets
    temp_dir = tempfile.gettempdir()
    init_file_path = temp_dir + "/init.sql"
    with open(init_file_path, "w") as file:
        file.write(
            f"""
          -- Add a secret for testbucketcdw
          CREATE SECRET s3test (
            TYPE S3,
            KEY_ID '{TEST_AWS_ACCESS_KEY_ID}',
            SECRET '{TEST_AWS_SECRET_ACCESS_KEY}',
            ENDPOINT 'localhost:{MOTO_PORT}',
            SCOPE 's3://{TEST_BUCKET}',
            URL_STYLE 'path', USE_SSL false
          );

          -- Add a secret for testbucketcdw
          CREATE SECRET s3managed (
            TYPE S3,
            KEY_ID '{TEST_AWS_ACCESS_KEY_ID}',
            SECRET '{TEST_AWS_SECRET_ACCESS_KEY}',
            ENDPOINT 'localhost:{MOTO_PORT}',
            SCOPE 's3://{MANAGED_STORAGE_BUCKET}',
            URL_STYLE 'path', USE_SSL false
          );

          -- Add a secret for testbucketgcs
          CREATE SECRET gcstest (
            TYPE GCS,
            KEY_ID '{TEST_AWS_ACCESS_KEY_ID}',
            SECRET '{TEST_AWS_SECRET_ACCESS_KEY}',
            ENDPOINT 'localhost:{MOTO_PORT_GCS}',
            SCOPE 'gs://{TEST_BUCKET_GCS}',
            URL_STYLE 'path', USE_SSL false
          );

          -- Add a secret for Azurite
          CREATE SECRET aztest (
            TYPE AZURE,
            CONNECTION_STRING '{AZURITE_CONNECTION_STRING}'
          );

          SET GLOBAL pg_lake_region TO 'ca-west-1';
          SET GLOBAL pg_lake_managed_storage_bucket TO 's3://{MANAGED_STORAGE_BUCKET}';
          SET GLOBAL pg_lake_managed_storage_key_id TO '{server_params.MANAGED_STORAGE_CMK_ID}';
          SET GLOBAL enable_external_file_cache = false;
        """
        )

    server = PgDuckServer(
        unix_socket_directory=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
        duckdb_database_file_path=str(server_params.DUCKDB_DATABASE_FILE_PATH),
        pidfile=server_params.PGDUCK_PID_FILE,
        cache_dir=str(server_params.PGDUCK_CACHE_DIR),
        debug=True,
        need_output=True,
        extra_args=[
            "--unix_socket_permissions",
            server_params.PGDUCK_UNIX_DOMAIN_PERMISSIONS,
            "--unix_socket_group",
            group_name,
            "--init_file_path",
            str(init_file_path),
        ],
    )

    # Wait for the server to create the socket before attempting to stat it
    if not is_server_listening(server.socket_path):
        raise RuntimeError(
            f"Server failed to start - socket not listening: {server.socket_path}"
        )

    socket_stat = os.stat(server.socket_path)

    # check socket permissions
    socket_perms = socket_stat.st_mode & 0o777
    assert socket_perms == int(server_params.PGDUCK_UNIX_DOMAIN_PERMISSIONS, 8)

    # check socket group
    assert socket_stat.st_gid == gid

    # normalize timezone to UTC
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    run_command("SET GLOBAL TimeZone = 'Etc/UTC'", conn)
    conn.close()

    return server


def stop_process_via_pidfile(pid_file, timeout=10):
    """Stop a process identified by a PID file.

    Reads the PID from *pid_file*, sends SIGTERM, waits up to *timeout*
    seconds, and escalates to SIGKILL if the process is still alive.
    The PID file is removed afterwards.  Safe to call when the process
    is already stopped or the PID file does not exist.
    """
    pid_path = Path(pid_file)
    if not pid_path.exists():
        return

    try:
        pid = int(pid_path.read_text().strip())
    except (ValueError, OSError):
        return

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        # Already gone
        pid_path.unlink(missing_ok=True)
        return

    # Wait for the process to exit
    for _ in range(timeout * 10):
        try:
            os.kill(pid, 0)  # check if alive
        except ProcessLookupError:
            pid_path.unlink(missing_ok=True)
            return
        time.sleep(0.1)

    # Still alive after timeout – escalate to SIGKILL
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

    pid_path.unlink(missing_ok=True)



def remove_duckdb_cache():
    duckdb_database_file_path_p = Path(server_params.DUCKDB_DATABASE_FILE_PATH)
    if duckdb_database_file_path_p.exists():
        os.remove(duckdb_database_file_path_p)

    cache_dir = Path(server_params.PGDUCK_CACHE_DIR)
    if cache_dir.exists():
        shutil.rmtree(cache_dir, ignore_errors=True)


def start_server_in_background(command, need_output=False):
    pgduck_server_path = get_pgduck_server_path()
    if not pgduck_server_path.exists():
        raise FileNotFoundError(f"Executable not found: {pgduck_server_path}")

    # Set up mock credentials to quickly resolve the default credentials chain
    # when loading DuckDB. We use different values as the test credentials to
    # not confuse the two.
    extra_env = {
        **os.environ,
        "AWS_DEFAULT_REGION": "ca-west-1",
        "AWS_ACCESS_KEY_ID": "notreals3key",
        "AWS_SECRET_ACCESS_KEY": "notrealskey",
    }

    stderr = None

    if need_output:
        stderr = subprocess.PIPE

    full_command = [str(pgduck_server_path)] + command
    print(full_command)
    process = subprocess.Popen(
        full_command,
        stdout=None,
        stderr=stderr,
        text=True,
        bufsize=1,
        universal_newlines=True,
        env=extra_env,
        start_new_session=True,
    )
    return process


class PgDuckServer:
    """A managed pgduck_server instance for testing.

    Builds CLI args from keyword parameters, starts the server via
    ``start_server_in_background()``, and derives ``socket_path``
    automatically.  When *need_output* is ``True``, stderr is captured
    into ``output_queue`` via a background thread.

    Each instance tracks the filesystem artifacts (socket, lock file,
    DuckDB database + WAL, PID file, cache directory) it is expected to
    create.  Calling ``cleanup()`` terminates the process **and** removes
    those artifacts.  The autouse ``cleanup_test_servers`` fixture calls
    ``cleanup()`` automatically, so explicit calls are optional.

    *port* accepts ``int`` or ``str`` so that failure tests can pass
    deliberately invalid values (e.g. ``"invalid_port"``).

    Example::

        server = PgDuckServer(port=8254, debug=True)
        assert is_server_listening(server.socket_path)
    """

    def __init__(
        self,
        *,
        unix_socket_directory="/tmp",
        port,
        duckdb_database_file_path=None,
        pidfile=None,
        cache_dir=None,
        debug=False,
        need_output=False,
        extra_args=None,
    ):
        self.unix_socket_directory = unix_socket_directory
        self.port = port

        # -- Compute artifact paths ------------------------------------------
        db = duckdb_database_file_path or "/tmp/duckdb.db"
        self._artifact_paths = [db, f"{db}.wal"]
        self._artifact_dirs = []

        if not unix_socket_directory.startswith("@"):
            socket_file = f"{unix_socket_directory}/.s.PGSQL.{port}"
            self._artifact_paths += [socket_file, f"{socket_file}.lock"]

        if pidfile is not None:
            self._artifact_paths.append(pidfile)
        if cache_dir is not None:
            self._artifact_dirs.append(cache_dir)

        # -- Build CLI args and start ----------------------------------------
        args = [
            "--unix_socket_directory", unix_socket_directory,
            "--port", str(port),
        ]
        if duckdb_database_file_path is not None:
            args += ["--duckdb_database_file_path", duckdb_database_file_path]
        if pidfile is not None:
            args += ["--pidfile", pidfile]
        if cache_dir is not None:
            args += ["--cache_dir", cache_dir]
        if debug:
            args.append("--debug")
        if extra_args:
            args += list(extra_args)

        self.process = start_server_in_background(args, need_output)
        self.socket_path = Path(unix_socket_directory) / f".s.PGSQL.{port}"

        # Set up a background reader thread so the stderr pipe never fills.
        # Tests read from output_queue whenever they need server output.
        self.output_queue = None
        self.stderr_thread = None
        if need_output:
            self.output_queue = queue.Queue()
            self.stderr_thread = threading.Thread(
                target=capture_output,
                args=(self.process.stderr, self.output_queue),
            )
            self.stderr_thread.start()

        _pgduck_servers.append(self)

    def cleanup(self):
        """Terminate the server and remove its filesystem artifacts.

        Safe to call multiple times.
        """
        terminate_process(self.process)
        if self.stderr_thread:
            self.stderr_thread.join(timeout=10)
        self._remove_artifacts()

    def _remove_artifacts(self):
        """Delete every filesystem artifact this instance is expected to create."""
        for p in self._artifact_paths:
            Path(p).unlink(missing_ok=True)
        for d in self._artifact_dirs:
            if Path(d).exists():
                shutil.rmtree(d, ignore_errors=True)


# Tracks PgDuckServer instances so the autouse fixture can call cleanup()
# on each one (terminates the process AND removes filesystem artifacts).
_pgduck_servers = []


def cleanup_stale_pidfiles():
    """Remove leftover PID files from a crashed previous test session.

    PID files embed the runner PID which changes across sessions, so a
    glob is needed rather than tracking specific paths.
    """
    for p in glob.glob("/tmp/pgduck_server_test_*.pid"):
        Path(p).unlink(missing_ok=True)


@pytest.fixture(autouse=True)
def cleanup_test_servers():
    """Terminate server processes and remove filesystem artifacts.

    Any PgDuckServer instances created during a test are automatically
    cleaned up (process terminated + artifacts removed) when the test
    finishes — even if an assertion fails before the test's own cleanup
    code runs.  Stale PID files from a crashed previous session are also
    removed during setup.
    """
    cleanup_stale_pidfiles()
    before = len(_pgduck_servers)
    yield
    for server in _pgduck_servers[before:]:
        server.cleanup()
    _pgduck_servers[:] = _pgduck_servers[:before]


def is_server_listening(socket_path, timeout=5, interval=0.01):
    """Check if the server is listening on the specified UNIX socket, looping for a maximum of 'timeout' seconds."""
    end_time = time.time() + timeout
    socket_path_str = str(socket_path)

    # Treat "@" as an abstract socket name.
    # https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-UNIX-SOCKET-DIRECTORIES
    if socket_path_str.startswith("@"):
        socket_path_str = "\0" + socket_path_str[1:]

    while time.time() < end_time:
        if socket_path_str.startswith("\0") or Path(socket_path_str).exists():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                    # Set a short timeout to avoid hanging connections
                    s.settimeout(1.0)
                    s.connect(socket_path_str)
                    # Socket will be automatically closed when exiting the with block
                print("is_server_listening: socket connected")
                return True
            except socket.error as e:
                print(f"is_server_listening: waiting for socket, error: {e}")
                # Continue the loop if the connection is refused
        time.sleep(interval)

    print("is_server_listening: timeout reached, server not listening")
    return False


def has_duckdb_created_file(duckdb_database_file_path):
    pattern = struct.Struct("<8x4sQ")

    with open(duckdb_database_file_path, "rb") as fh:
        return pattern.unpack(fh.read(pattern.size)) == (b"DUCK", 64)

    return False


def run_cli_command(command):

    pgduck_server_path = get_pgduck_server_path()
    if not pgduck_server_path.exists():
        raise FileNotFoundError(f"Executable not found: {pgduck_server_path}")

    # for the purposes of these test, always use check_cli_params_only
    full_command = [str(pgduck_server_path)] + ["--check_cli_params_only"] + command
    process = subprocess.Popen(
        full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


# ---------------------------------------------------------------------------
# PostgreSQL server helpers
# ---------------------------------------------------------------------------

def start_postgres(db_path, db_user, db_port):
    # Stop any leftover PostgreSQL from a previous interrupted run.
    stop_postgres(db_path)

    # Ensure the database directory is clean
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    old_db_path = ""
    log_file_path = f"{db_path}/logfile"

    # Initialize the database directory (current PG version)
    initdb(PG_BINDIR, db_path, db_user)

    # If TEST_PG_UPGRADE_FROM_BINDIR is set, we create another database directory
    # on an old PG version and upgrade it
    upgrade_from_bindir = os.getenv("TEST_PG_UPGRADE_FROM_BINDIR")
    if upgrade_from_bindir:
        # Run initdb using the old PG version in a separate directory
        old_db_path = "/tmp/pgl_tests_pg_before_upgrade"

        # Ensure the database directory is clean
        if os.path.exists(old_db_path):
            shutil.rmtree(old_db_path)

        initdb(upgrade_from_bindir, old_db_path, db_user)

        # Start postgres with the new version in the regular directory
        subprocess.run(
            [
                f"{upgrade_from_bindir}/pg_ctl",
                "-D",
                old_db_path,
                "-o",
                f"-p {db_port} -k /tmp",
                "-l",
                log_file_path,
                "-w",
                "start",
            ]
        )

        file = open(log_file_path, "r")
        content = file.read()
        print(content)
        file.close()

        # Put some stuff in the database with the old PG version
        run_pre_upgrade_script()

        # Stop postgres
        stop_postgres(old_db_path, upgrade_from_bindir)

        # Run pg_upgrade
        run_pg_upgrade(old_db_path, db_path, upgrade_from_bindir)

    # Register cleanup before starting so PostgreSQL is stopped even when
    # the test process is interrupted (e.g. Ctrl+C) and fixture teardown
    # is skipped.  stop_postgres() is safe to call when PostgreSQL is not
    # running.
    atexit.register(stop_postgres, db_path)

    subprocess.run(
        [
            f"{PG_BINDIR}/pg_ctl",
            "-D",
            db_path,
            "-o",
            f"-p {db_port} -k /tmp",
            "-l",
            log_file_path,
            "-w",
            "start",
        ]
    )

    file = open(log_file_path, "r")
    content = file.read()
    print(content)
    file.close()


def initdb(initdb_bindir, db_path, db_user):
    locale_setting = None
    if platform.system() == "Darwin":
        # macOS
        locale_setting = "en_US.UTF-8"
    elif platform.system() == "Linux":
        # Linux
        locale_setting = "C.UTF-8"

    subprocess.run(
        [
            f"{initdb_bindir}/initdb",
            "-U",
            db_user,
            "--locale",
            locale_setting,
            "--data-checksums",
            "--set",
            "shared_preload_libraries=pgaudit,pg_cron,pg_extension_base,auto_explain,pg_stat_statements",
            "--set",
            "pg_stat_statements.track=all",
            "--set",
            # in make check, use a limited audit
            # in make installcheck use pgaudit.log='all'
            "pgaudit.log=role",
            "--set",
            "wal_level=logical",
            "--set",
            "auto_explain.log_min_duration=10ms",
            "--set",
            "synchronous_commit=local",
            "--set",
            "max_prepared_transactions=100",
            "--set",
            "max_worker_processes=100",
            # get rid of unused files, as well as stress test file removal
            "--set",
            "pg_lake_engine.orphaned_file_retention_period=0",
            "--set",
            "pg_lake_iceberg.autovacuum_naptime=5",
            "--set",
            "synchronous_standby_names=pg_lake",
            "--set",
            "cluster_name=pg_lake",
            "--set",
            "timezone=UTC",
            "--set",
            f"log_min_messages={log_level}",
            "--set",
            f"pg_lake_engine.host=host={server_params.PGDUCK_UNIX_DOMAIN_PATH} port={server_params.PGDUCK_PORT}",
            "--set",
            "pg_lake_engine.enable_heavy_asserts=on",
            db_path,
        ]
    )


def create_read_replica(db_path, db_port):
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    log_file_path = f"{db_path}/logfile"

    subprocess.run(
        [
            f"{PG_BINDIR}/pg_basebackup",
            "--write-recovery-conf",
            "--create-slot",
            "--wal-method=stream",
            "--slot=pg_lake",
            "-d",
            default_connection_string(),
            "-D",
            db_path,
        ]
    )
    subprocess.run(
        [
            f"{PG_BINDIR}/pg_ctl",
            "-D",
            db_path,
            "-o",
            f"-p {db_port} -k /tmp",
            "-l",
            log_file_path,
            "start",
        ]
    )

    file = open(log_file_path, "r")
    content = file.read()
    print(content)
    file.close()


def run_pre_upgrade_script():
    """Prepare the pre-upgrade database with some artifacts"""

    command = f"""
        CREATE EXTENSION pg_lake_table CASCADE;
        CREATE SCHEMA pre_upgrade;

        -- Create an Iceberg table
        CREATE TABLE pre_upgrade.iceberg (
            id bigserial,
            value text
        )
        USING pg_lake_iceberg
        WITH (location = 's3://{TEST_BUCKET}/pre_upgrade/iceberg');

        INSERT INTO pre_upgrade.iceberg (value) VALUES ('hello'), ('world');

        -- Create a writable table
        CREATE TYPE pre_upgrade.xy AS (x bigint, y bigint);
        SELECT map_type.create('text', 'text');

        CREATE FOREIGN TABLE pre_upgrade.writable (
            key text not null,
            value pre_upgrade.xy,
            properties map_type.key_text_val_text
        )
        SERVER pg_lake
        OPTIONS (location 's3://{TEST_BUCKET}/pre_upgrade/writable/', format 'parquet', writable 'true');

        INSERT INTO pre_upgrade.writable
        SELECT
            'two-times-'||x,
            (x,x*2)::pre_upgrade.xy,
            ARRAY[('origin','pre-upgrade'),('reason','tests')]::map_type.key_text_val_text
        FROM generate_series(1,100) x;

        COPY (SELECT s AS id, 'hello-'||s AS value, 3.14 AS pi FROM generate_series(1,10) s)
        TO 's3://{TEST_BUCKET}/pre_upgrade/csv/data.csv' WITH (header);

        CREATE FOREIGN TABLE pre_upgrade.csv_table ()
        SERVER pg_lake
        OPTIONS (path 's3://{TEST_BUCKET}/pre_upgrade/csv/data.csv');

        -- Throw another extension into the mix
        CREATE EXTENSION postgres_fdw CASCADE;
    """

    pg_conn = open_pg_conn()
    run_command(command, pg_conn)
    pg_conn.commit()
    pg_conn.close()


def run_pg_upgrade(old_data_dir, new_data_dir, old_bin_dir):
    """Run pg_upgrade to upgrade PostgreSQL to the target version."""

    subprocess.run(
        [
            f"{PG_BINDIR}/pg_upgrade",
            f"--old-datadir={old_data_dir}",
            f"--new-datadir={new_data_dir}",
            f"--old-bindir={old_bin_dir}",
            f"--new-bindir={PG_BINDIR}",
            f"--old-port={server_params.PG_PORT}",
            f"--new-port={server_params.PG_PORT}",
            f"--socketdir=/tmp",
            f"--username={server_params.PG_USER}",
        ],
        check=True,
    )


def stop_postgres(db_path, bindir=PG_BINDIR):
    pidfile = os.path.join(db_path, "postmaster.pid")
    if not os.path.exists(pidfile):
        return
    subprocess.run([f"{bindir}/pg_ctl", "-D", db_path, "stop"])

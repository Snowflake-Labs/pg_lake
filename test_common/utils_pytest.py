import subprocess
import threading
import queue
import time
from pathlib import Path
from decimal import Decimal
import psycopg2
import psycopg2.extras
import socket
import struct
import os
import zoneinfo
import math
import signal
import shutil
import pytest
import grp
import getpass
from moto.server import ThreadedMotoServer
import boto3
import server_params
import threading
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from urllib.parse import urlparse
import json
import platform
import tempfile
import os
import uuid
from deepdiff import DeepDiff
import importlib.metadata
from pyspark.errors import NumberFormatException
from pyspark.sql import SparkSession
import random
import string
import datetime
import re
from azure.storage.blob import BlobServiceClient
import sys
import duckdb
import requests
from urllib.parse import quote

PGDUCK_SERVER_PROCESS_NAME = "pgduck_server"

PG_CONFIG = os.environ.get("PG_CONFIG", "pg_config")
PG_BINDIR = subprocess.run(
    [PG_CONFIG, "--bindir"], capture_output=True, text=True
).stdout.rstrip()

# S3 configuration
# should match test secret in duckdb.c
MOTO_PORT = 5999
TEST_BUCKET = "testbucketcdw"
MANAGED_STORAGE_BUCKET = "pglakemanaged1"
TEST_AWS_ACCESS_KEY_ID = "testing"
TEST_AWS_SECRET_ACCESS_KEY = "testing"
TEST_AWS_REGION = "us-west-1"
TEST_AWS_FAKE_ROLE_NAME = "FakeRoleForTest"
AWS_ROLE_ARN = f"arn:aws:iam::000000000000:role/{TEST_AWS_FAKE_ROLE_NAME}"


MOTO_PORT_GCS = 5998
TEST_BUCKET_GCS = "testbucketgcs"
TEST_GCS_REGION = "europe-west4"

AZURITE_CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1"

log_level = os.getenv("LOG_MIN_MESSAGES", "notice")


def get_pgduck_server_path():
    pgduck_server = subprocess.run(
        ["which", "pgduck_server"], capture_output=True, text=True
    ).stdout.rstrip()
    # error if not found?
    return Path(pgduck_server)


def get_polaris_server_path():

    base_dir = Path(__file__).resolve().parent
    polaris_server_path = base_dir / "rest_catalog/rest_catalog_server.sh"

    return polaris_server_path


def setup_pgduck_server():

    remove_duckdb_cache()

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

    args = [
        "--unix_socket_directory",
        server_params.PGDUCK_UNIX_DOMAIN_PATH,
        "--unix_socket_permissions",
        server_params.PGDUCK_UNIX_DOMAIN_PERMISSIONS,
        "--unix_socket_group",
        group_name,
        "--port",
        str(server_params.PGDUCK_PORT),
        "--duckdb_database_file_path",
        str(server_params.DUCKDB_DATABASE_FILE_PATH),
        "--init_file_path",
        str(init_file_path),
        "--cache_dir",
        str(server_params.PGDUCK_CACHE_DIR),
        "--debug",
    ]

    server, output_queue, stderr_thread = capture_output_queue(
        start_server_in_background(args, True)
    )

    socket_path = (
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )
    socket_stat = os.stat(socket_path)

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

    return server, output_queue, stderr_thread


def start_polaris_server_in_background():
    polaris_server_path = get_polaris_server_path()
    if not polaris_server_path.exists():
        raise FileNotFoundError(f"Executable not found: {polaris_server_path}")

    env = os.environ.copy()
    env.update(
        {
            "PGHOST": server_params.PG_HOST,
            "PGPORT": str(server_params.PG_PORT),
            "PGUSER": server_params.PG_USER,
            "PGDATABASE": server_params.PG_DATABASE,
            "PGPASSWORD": server_params.PG_PASSWORD,
            "CLIENT_ID": "client_id",
            "CLIENT_SECRET": "client_secret",
            "AWS_ROLE_ARN": AWS_ROLE_ARN,
            # todo: this polaris server only works for the default database
            # in the regression tests.
            "STORAGE_LOCATION": f"s3://{TEST_BUCKET}/{server_params.PG_DATABASE}",
            "POLARIS_HOSTNAME": server_params.POLARIS_HOSTNAME,
            "POLARIS_PORT": str(server_params.POLARIS_PORT),
            "POLARIS_PRINCIPAL_CREDS_FILE": server_params.POLARIS_PRINCIPAL_CREDS_FILE,
            "POLARIS_PYICEBERG_SAMPLE": server_params.POLARIS_PYICEBERG_SAMPLE,
            "POLARIS_PID_FILE": server_params.POLARIS_PID_FILE,
        }
    )

    log_path = Path("/tmp/polaris_startup.log").resolve()
    log_file = log_path.open("w+", buffering=1)

    proc = subprocess.Popen(
        [str(polaris_server_path), "--purge", "--no-wait"],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )

    exit_code = proc.wait()

    if exit_code != 0:
        raise Exception(f"polaris server start failed with return code {exit_code}")

    return proc


def capture_output_queue(server):
    output_queue = queue.Queue()

    stderr_thread = threading.Thread(
        target=capture_output, args=(server.stderr, output_queue)
    )
    stderr_thread.start()

    start_time = time.time()
    timeout = 20  # seconds
    server_ready = False

    # Loop until server is ready or timeout is reached
    while (time.time() - start_time) < timeout:
        try:
            # Check if there is any output indicating the server is ready
            line = output_queue.get_nowait()
            if line and "pgduck_server is running with pid" in line:
                server_ready = True
                break
        except queue.Empty:
            # No output yet, continue waiting
            time.sleep(0.1)
            continue

    if not server_ready:
        assert False, "Server did not start within the expected time."

    return server, output_queue, stderr_thread


def remove_duckdb_cache():
    duckdb_database_file_path_p = Path(server_params.DUCKDB_DATABASE_FILE_PATH)
    if duckdb_database_file_path_p.exists():
        os.remove(duckdb_database_file_path_p)

    cache_dir = Path(server_params.PGDUCK_CACHE_DIR)
    if cache_dir.exists():
        shutil.rmtree(cache_dir, ignore_errors=True)


def capture_output(file, output_queue):
    try:
        with file as pipe:
            for line in iter(pipe.readline, ""):
                output_queue.put(line)
                print(line)
    finally:
        output_queue.put(None)


def perform_query(query, conn):
    cur = conn.cursor()
    cur.execute(query)
    cur.close()


def get_server_output(output_queue):
    server_output = ""
    try:
        while True:
            line = output_queue.get(timeout=0.2)
            if line is not None:
                server_output += line
    except:
        # No more output in the queue
        pass

    return server_output


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
                    s.connect(socket_path_str)
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


def start_postgres(db_path, db_user, db_port):
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


def default_connection_string(user=server_params.PG_USER):
    return f"dbname={server_params.PG_DATABASE} user={user} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"


def open_pg_conn(user=server_params.PG_USER):
    return psycopg2.connect(default_connection_string(user))


def stop_postgres(db_path, bindir=PG_BINDIR):
    subprocess.run([f"{bindir}/pg_ctl", "-D", db_path, "stop"])


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


def run_simple_command(hostname, serverport):

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    r = cur.fetchall()
    print(r)
    assert r == [("1",)]


def run_pgbench_command(commands):

    # for the purposes of these test, always use check_cli_params_only
    full_command = ["pgbench"] + commands
    process = subprocess.Popen(
        full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


def run_psql_command(commands):

    # for the purposes of these test, always use check_cli_params_only
    full_command = ["psql", "-X", "-q"] + commands
    process = subprocess.Popen(
        full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


def perform_query_on_cursor(query, conn):
    cur = conn.cursor()
    try:
        cur.execute(query)
        # Fetching all results
        results = cur.fetchall()
        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        cur.close()


def run_query(query, conn, raise_error=True):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cur.close()


def run_command(query, conn, raise_error=True):
    cur = conn.cursor()
    try:
        cur.execute(query)
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cur.close()


def run_command_outside_tx(query_list, raise_error=True):
    conn = open_pg_conn()
    conn.autocommit = True

    cur = conn.cursor()

    try:
        for q in query_list:
            cur.execute(q)

    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        conn.close()


# Example usage for thread_run_query() and thread_run_command():
#   thread1 = thread_run_query("SELECT * FROM your_table", conn)
#   thread2 = thread_run_command("UPDATE your_table SET field='value'", conn)
#   thread1.join()  # Optionally wait for the thread to finish
#   thread2.join()  # Optionally wait for the thread to finish
def thread_run_query(query, conn, raise_error=True):
    thread = threading.Thread(target=run_query, args=(query, conn, raise_error))
    thread.start()
    return thread


def thread_run_command(query, conn, raise_error=True):
    thread = threading.Thread(target=run_command, args=(query, conn, raise_error))
    thread.start()
    return thread


def copy_to_file(copy_command, file_name, conn, raise_error=True):
    cursor = conn.cursor()
    try:
        with open(file_name, "wb") as file:
            cursor.copy_expert(copy_command, file)

        return None
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cursor.close()


def copy_from_file(copy_command, file_name, conn, raise_error=True):
    cursor = conn.cursor()
    try:
        with open(file_name, "rb") as file:
            cursor.copy_expert(copy_command, file)

        return None
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cursor.close()


# Create in-memory DuckDB connection
def create_duckdb_conn():
    conn = duckdb.connect(database=":memory:")
    conn.execute(
        """
        CREATE SECRET s3test (
            TYPE S3, KEY_ID 'testing', SECRET 'testing',
            ENDPOINT 'localhost:5999',
            SCOPE 's3://testbucketcdw', URL_STYLE 'path', USE_SSL false
        );
    """
    )
    conn.execute(
        """
        CREATE SECRET gcstest (
            TYPE GCS, KEY_ID 'testing', SECRET 'testing',
            ENDPOINT 'localhost:5998',
            SCOPE 'gs://testbucketgcs', URL_STYLE 'path', USE_SSL false
        );
    """
    )
    conn.execute(
        f"""
        CREATE SECRET ztest (
            TYPE AZURE,
            CONNECTION_STRING '{AZURITE_CONNECTION_STRING}'
        );
    """
    )
    return conn


def start_azurite():
    azurite_tmp_dir = Path("/tmp/pgl_tests_azurite")
    if azurite_tmp_dir.exists():
        shutil.rmtree(azurite_tmp_dir, ignore_errors=True)

    process = subprocess.Popen(["azurite", "--location", str(azurite_tmp_dir)])

    # Wait a bit for blob storage to start
    # If it doesn't, the SDK will still retry, but that might add more delay
    time.sleep(1)

    return process


# Start Azurite in the background
def create_mock_azure_blob_storage():
    process = start_azurite()

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURITE_CONNECTION_STRING
    )
    container_client = blob_service_client.create_container(TEST_BUCKET)

    return container_client, process


def dump_test_s3_object(key):
    client = mock_s3_client()
    response = client.get_object(Bucket=TEST_BUCKET, Key=key)
    print(response["Body"].read())


def mock_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )


# Start a background server that pretends to be Amazon Simple Storage Service (S3)
def create_mock_s3():

    # Service-specific endpoints so SDK v2 (used by Polaris) knows where to call
    os.environ["AWS_ENDPOINT_URL_S3"] = f"http://127.0.0.1:{MOTO_PORT}"
    os.environ["AWS_ENDPOINT_URL_STS"] = f"http://127.0.0.1:{MOTO_PORT}"
    os.environ["AWS_REGION"] = TEST_AWS_REGION
    os.environ["AWS_ACCESS_KEY_ID"] = TEST_AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = TEST_AWS_SECRET_ACCESS_KEY

    # ---- start Moto ----
    server = ThreadedMotoServer(port=MOTO_PORT)
    server.start()

    client = mock_s3_client()
    client.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
    )
    client.create_bucket(
        Bucket=MANAGED_STORAGE_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION},
    )

    # Create a customer-managed key
    kms_client = create_kms_client()
    response = kms_client.create_key(
        Description="Customer Managed Key for testing",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )

    # Extract the KeyId from the response
    server_params.MANAGED_STORAGE_CMK_ID = response["KeyMetadata"]["KeyId"]

    # Setting up STS + assume-role is not strictly required for Polaris version 1.2+
    # But we prefer to keep for now, as that's more closer to production workloads

    # Create IAM role + STS assume-role
    # required for Polaris
    iam = boto3.client(
        "iam",
        endpoint_url=f"http://127.0.0.1:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )

    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}
        ],
    }

    role = iam.create_role(
        RoleName=f"{TEST_AWS_FAKE_ROLE_NAME}",
        AssumeRolePolicyDocument=json.dumps(trust),
        Description="Moto test role for Polaris",
    )["Role"]

    # attach wide S3 policy; Moto is lax but this mirrors real life
    iam.put_role_policy(
        RoleName=f"{TEST_AWS_FAKE_ROLE_NAME}",
        PolicyName="S3All",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        ),
    )

    # Prove STS works
    # if not, should throw exception
    sts = boto3.client(
        "sts",
        endpoint_url=f"http://127.0.0.1:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )
    assumed = sts.assume_role(
        RoleArn=role["Arn"], RoleSessionName="polaris-test-session"
    )["Credentials"]

    return client, server


# Start a background server that pretends to be Google Cloud Storage (GCS)
#
# GCS offers S3 API compatibility, which is what DuckDB uses to access GCS.
# Hence, we can also use moto to mock GCS, but we run it on a separate port
# to not get confused with mock S3.
def create_mock_gcs():
    server = ThreadedMotoServer(port=MOTO_PORT_GCS)
    server.start()

    # "s3" refers to the name of the AWS API within boto
    client = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{MOTO_PORT_GCS}",
        region_name=TEST_GCS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )
    client.create_bucket(
        Bucket=TEST_BUCKET_GCS,
        CreateBucketConfiguration={"LocationConstraint": TEST_GCS_REGION},
    )
    return client, server


def create_kms_client():
    return boto3.client(
        "kms",
        endpoint_url=f"http://localhost:{MOTO_PORT}",
        region_name=TEST_AWS_REGION,
        aws_access_key_id=TEST_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=TEST_AWS_SECRET_ACCESS_KEY,
    )


def get_object_size(client, bucket_name, key):
    response = client.head_object(Bucket=bucket_name, Key=key)
    return int(response["ContentLength"])


def list_objects(client, bucket_name, prefix=""):
    keys = []

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" in response:
        for content in response["Contents"]:
            print(content["Key"])
            keys.append(content["Key"])

    return keys


def sampledata_filepath(datafile):
    from pathlib import Path

    return str(Path(__file__).parent / "sample_data" / datafile)


def sample_avro_filepath(avrofile):
    from pathlib import Path

    return str(Path(__file__).parent / "sample_avro" / avrofile)


def compare_values(val1, val2, tolerance):
    if isinstance(val1, float) and isinstance(val2, float):
        return abs(val1 - val2) <= tolerance
    elif isinstance(val1, Decimal) and isinstance(val2, Decimal):
        return abs(val1 - val2) <= Decimal(str(tolerance))
    elif isinstance(val1, list) and isinstance(val2, list):
        for v1, v2 in zip(val1, val2):
            if not compare_values(v1, v2, tolerance):
                return False
        return True
    elif isinstance(val1, memoryview) and isinstance(val2, memoryview):
        return bytes(val1) == bytes(val2)

    # Add more comparison strategies here for other data types if needed
    return val1 == val2


def compare_rows(row1, row2, tolerance):
    return all(compare_values(val1, val2, tolerance) for val1, val2 in zip(row1, row2))


def transform_item_for_sort(item):
    if item is None:
        return "RESERVED_KEY_FOR_NULLS_AS_STRING"

    if isinstance(item, memoryview):
        return bytes(item)

    return str(item)


def custom_key(x):
    # Transform the tuple by converting all items to strings, except None, which gets a placeholder that sorts last
    return tuple(transform_item_for_sort(item) for item in x)


def sort_with_none_at_end(lst):
    return sorted(lst, key=custom_key)


def iceberg_metadata_json_folder_path():
    from pathlib import Path

    return str(Path(__file__).parent / "iceberg/metadata_json")


def iceberg_sample_table_folder_path():
    from pathlib import Path

    return str(Path(__file__).parent / "iceberg/sample_tables")


def iceberg_metadata_manifest_folder_path():
    from pathlib import Path

    return str(Path(__file__).parent / "iceberg/manifests")


def assert_query_results_on_tables(
    query, pg_conn, first_table_names, second_table_names, tolerance=0.001
):

    if len(first_table_names) != len(second_table_names):
        raise ValueError(
            "The lists of first and second table names must have the same length."
        )

    fdw_query_result = perform_query_on_cursor(query, pg_conn)

    heap_query = query
    for first_table_name, second_table_name in zip(
        first_table_names, second_table_names
    ):
        heap_query = heap_query.replace(first_table_name, second_table_name)

    heap_query_result = perform_query_on_cursor(heap_query, pg_conn)

    sorted_fdw_result = sort_with_none_at_end(fdw_query_result)
    sorted_heap_result = sort_with_none_at_end(heap_query_result)

    assert (
        len(sorted_heap_result) > 0
    ), "No rows returned, make sure at least one row returns"
    assert len(sorted_fdw_result) == len(
        sorted_heap_result
    ), "Result sets have different lengths"

    for row_fdw, row_heap in zip(sorted_fdw_result, sorted_heap_result):
        assert compare_rows(
            row_fdw, row_heap, tolerance
        ), f"Results do not match: {row_fdw} and {row_heap} ({sorted_fdw_result} vs {sorted_heap_result})"


def assert_query_results_on_search_path(
    query, pg_conn, first_search_path, second_search_path, tolerance=0.001
):

    run_command("SET search_path TO " + first_search_path + ";", pg_conn)
    first_query_result = perform_query_on_cursor(query, pg_conn)

    run_command("SET search_path TO " + second_search_path + ";", pg_conn)
    second_query_result = perform_query_on_cursor(query, pg_conn)

    sorted_first_result = sort_with_none_at_end(first_query_result)
    sorted_second_result = sort_with_none_at_end(second_query_result)

    assert len(sorted_first_result) == len(
        sorted_second_result
    ), "Result sets have different lengths"

    for row_fdw, row_heap in zip(sorted_first_result, sorted_second_result):
        assert compare_rows(
            row_fdw, row_heap, tolerance
        ), f"Results do not match: {row_fdw} and {row_heap}"


def assert_remote_query_contains_expression(query, expression, pg_conn):
    explain = "EXPLAIN (ANALYZE, VERBOSE, format " "JSON" ") " + query
    explain_result = perform_query_on_cursor(explain, pg_conn)[0]
    remote_sql = fetch_remote_sql(explain_result)
    print(remote_sql)
    assert expression in remote_sql


def assert_remote_query_not_contains_expression(query, expression, pg_conn):
    explain = "EXPLAIN (ANALYZE, VERBOSE, format " "JSON" ") " + query
    explain_result = perform_query_on_cursor(explain, pg_conn)[0]
    remote_sql = fetch_remote_sql(explain_result)
    print(remote_sql)
    assert expression not in remote_sql


def assert_query_result_on_duckdb_and_pg(duckdb_conn, pg_conn, duckdb_query, pg_query):
    duckdb_conn.execute(duckdb_query)
    duckdb_result = duckdb_conn.fetchall()

    pg_result = run_query(pg_query, pg_conn)
    # duckdb returns [(), (), ...] while pg returns [[], [], ...]
    pg_result = [tuple(row) for row in pg_result]

    assert duckdb_result == pg_result

    return pg_result


def assert_query_result_on_spark_and_pg(
    installcheck, spark_session, pg_conn, spark_query, pg_query
):
    if installcheck:
        return

    # run query on spark table
    spark_result = spark_session.sql(spark_query).collect()

    # compare spark result with pg_lake result
    pg_lake_result = run_query(pg_query, pg_conn)

    assert (
        len(pg_lake_result) > 0
    ), "No rows returned, make sure at least one row returns"
    assert len(pg_lake_result) == len(
        spark_result
    ), "Result sets have different lengths"

    for row_pg_lake, row_spark in zip(pg_lake_result, spark_result):
        assert compare_rows_as_string_or_float(
            row_pg_lake, row_spark, 0.001
        ), f"Results do not match: {row_pg_lake} and {row_spark}"

    return pg_lake_result


def install_duckdb_extension(duckdb_conn, extension):
    run_command(f"INSTALL {extension};", duckdb_conn)
    run_command(f"LOAD {extension};", duckdb_conn)


def fetch_remote_sql(explain_result):
    return find_key_in_json(explain_result[0][0], "Vectorized SQL")


def fetch_data_files_used(explain_result):
    return find_key_in_json(explain_result[0][0], "Data Files Scanned")


def fetch_delete_files_used(explain_result):
    return find_key_in_json(explain_result[0][0], "Deletion Files Scanned")


def fetch_data_files_skipped(explain_result):
    return find_key_in_json(explain_result[0][0], "Data Files Skipped")


def data_file_count(pg_conn, table_name):
    result = run_query(
        f"select count(*) from lake_table.files where table_name = '{table_name}'::regclass",
        pg_conn,
    )
    return result[0]["count"]


# By ChatGPT
def find_key_in_json(data, target_key):
    # If the current data is a dictionary, check each key-value pair
    if isinstance(data, dict):
        for key, value in data.items():
            # If the key matches the target key, return the value
            if key == target_key:
                return value
            # Otherwise, recurse into the value
            found = find_key_in_json(value, target_key)
            if found is not None:
                return found

    # If the current data is a list, iterate over each element and recurse
    elif isinstance(data, list):
        for item in data:
            found = find_key_in_json(item, target_key)
            if found is not None:
                return found

    # Return None if the key is not found
    return None


def validate_shape(typeid, shape, conn=None):
    """Recursively validate that a postgres type matches an expected format.

    A "shape" in this context is a data structure which matches the overall
    structure of the postgres type tree given a starting typeid.

    The initial typeid is the typeid of the underlying relation (since each
    table has an underlying composite datatype for that type).

    The interpretation of the datastruct is as follows:

    The top-level is an array, where the ordered entries is the expected
    order of the columns.

    If an entry is just a scalar, this is interpreted as a non-composite
    field name, so anything that is not typtype = 'c'.

    If the entry is a dict, then we have additional attributes associated
    with this type.  Available attributes are:

    - name: the name of the attribute itself

    - type: if provided, the type of the attribute must match this type

    - typelike: if provided, the type name must contain this substring

    - cols: if provided, this is a composite type.  The value should also be
        an array, and the elements here are subject to the same validation
        as the base-level composite type.

    - isarray: if provided, verify that the given type is or is not an array
        type (the used type name for `type` or `typelike`) is still the element
        type name in this case, not the `_text` or whatever type of just the
        array)

    """
    print(f"validate_shape: typeid: {typeid}; shape: {shape} ")
    # verify that we are starting with a list for our shape, anything else is an error
    assert isinstance(shape, list), "shape is list"
    assert typeid != 0, "empty typeid"
    assert len(shape) > 0, "zero-column table"

    # Do some validation on our passed-in typeid; the order of these fields
    # should match our constants up top.
    result = run_query(
        f"""
    SELECT
        attname, atttypid, typtype, typname, typarray = 0 as isarray,
        case when typarray = 0 then typelem::regtype::name else typname end as basename,
        case when typarray = 0 then typelem else atttypid end as childtype
    FROM pg_attribute, pg_type
    WHERE
        pg_type.oid = atttypid AND
        attnum > 0 AND
        NOT attisdropped AND
        attrelid = (SELECT typrelid FROM pg_type WHERE oid = {typeid})
    ORDER BY attnum
    """,
        conn,
    )

    # Since this was a composite type, first verify the number of columns
    # matches our shape.  If we did not find the underlying composite type,
    # this would not pass, so we'd get 0 rows from this query.

    print(result)
    assert len(result) == len(shape), "different number of columns"

    # now check out our shape attributes against what we discovered
    for shatt, pgatt in zip(shape, result):
        print(f"checking shape: {shatt} against found pginfo: {pgatt}")
        if isinstance(shatt, list):
            assert "unsupported type for shape" == False, "column cannot be a list"
        elif isinstance(shatt, str):
            # column name check
            assert pgatt["attname"] == shatt, "scalar column non-matching name"
            assert pgatt["typtype"] != "c", "scalar column expected non-composite type"
        elif isinstance(shatt, dict):
            # name should match
            assert pgatt["attname"] == shatt.pop("name"), "att:name"
            # type exact match
            if "type" in shatt:
                assert pgatt["basename"] == shatt.pop("type"), "att:type"
            # type substring match
            if "typelike" in shatt:
                assert shatt.pop("typelike") in pgatt["basename"], "att:typelike"
            if "isarray" in shatt:
                assert pgatt["isarray"] == shatt.pop("isarray"), "att:isarray"
            # explicit composite type check
            if "cols" in shatt:
                # checking other validation
                cols = shatt.pop("cols")
                assert isinstance(cols, list), "cols is list"
                # recursive validation for the given shape
                validate_shape(pgatt["childtype"], cols, conn=conn)
            # check for unknown shape params; sanity-check
            assert len(shatt) == 0, "no extra args"
        else:
            assert "unknown shape object type" == False


def setup_testdef(desc, conn=None, duckdb_conn=None):
    """Common setup for loading data and returning the base typeid for the
    created relation"""

    pg_conn = conn

    table = f"test_create_table_definitions_{desc['name']}"
    select = desc["select"]
    shape = desc["shape"]

    url = f"s3://{TEST_BUCKET}/{table}/data.parquet"

    run_command(
        f"""
    COPY (SELECT {select}) TO '{url}' WITH (format 'parquet');
    """,
        duckdb_conn,
    )

    run_command(
        f"""
    CREATE TABLE {table} () WITH (load_from='{url}');
    """,
        pg_conn,
    )

    res = run_query(
        f"""
    SELECT reltype FROM pg_class WHERE relname = '{table}'
    """,
        pg_conn,
    )

    return res[0][0]


def create_iceberg_test_catalog(pg_conn):
    catalog_user = "iceberg_test_catalog"

    result = run_query(
        f"SELECT 1 FROM pg_roles WHERE rolname='{catalog_user}'", pg_conn
    )
    if len(result) == 0:
        run_command(f"CREATE USER {catalog_user}", pg_conn)

    run_command(f"GRANT iceberg_catalog TO {catalog_user}", pg_conn)
    pg_conn.commit()

    catalog = SqlCatalog(
        "pyiceberg",
        **{
            "uri": f"postgresql+psycopg2://{catalog_user}@localhost:{server_params.PG_PORT}/{server_params.PG_DATABASE}",
            "warehouse": f"s3://{TEST_BUCKET}/iceberg/",
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
        },
    )
    catalog.create_namespace("public")

    return catalog


def create_iceberg_rest_catalog(namespace):

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]

    # Polaris REST endpoints
    base_uri = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog"
    oauth_token_url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/oauth/tokens"

    catalog = RestCatalog(
        "pyiceberg",  # local name for the catalog in your test
        **{
            # REST Catalog connection (Polaris)
            "uri": base_uri,  # required
            "warehouse": "postgres",  # Polaris catalog name
            "oauth2-server-uri": oauth_token_url,  # token endpoint
            "scope": "PRINCIPAL_ROLE:ALL",  # typical Polaris scope
            "credential": f"{client_id}:{client_secret}",  # "id:secret"
            # S3/Moto settings (same as your JDBC helper)
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
            "s3.path-style-access": "true",
        },
    )

    try:
        catalog.create_namespace(f"{namespace}")
    except NamespaceAlreadyExistsError:
        pass  # ignore only this case

    return catalog


def create_test_types(pg_conn, app_user):
    run_command("""create extension if not exists pg_map""", pg_conn)
    pg_conn.commit()
    create_map_type("int", "text")

    run_command(
        """create schema if not exists lake_struct;
    create type lake_struct.custom_type as (x int, y int);
    """,
        pg_conn,
    )

    create_table_command = """
    create table test_types (
    c_array int[],
    c_bit bit,
    c_bool bool,
    c_bpchar bpchar,
    c_bytea bytea,
    c_char char,
    c_cidr cidr,
    c_custom lake_struct.custom_type,
    c_date date,
    c_float4 float4,
    c_float8 float8,
    c_inet inet,
    c_int2 int2,
    c_int4 int4,
    c_int8 int8,
    c_interval interval,
    c_json json,
    c_jsonb jsonb,
    c_map map_type.key_int_val_text,
    c_money money,
    c_name name,
    c_numeric numeric,
    c_numeric_large numeric(39,2),
    c_numeric_mod numeric(4,2),
    c_oid oid,
    c_text text,
    c_tid tid,
    c_time time,
    c_timestamp timestamp,
    c_timestamptz timestamptz,
    c_timetz timetz,
    c_uuid uuid,
    c_varbit varbit,
    c_varchar varchar
    );
    """
    run_command(create_table_command, pg_conn)

    insert_command = f"""
    insert into test_types values (
    /* c_array */ ARRAY[1,2],
    /* c_bit */ 1::bit,
    /* c_bool */ true,
    /* c_bpchar */ 'hello',
    /* c_bytea */ '\\x0001',
    /* c_char */ 'a',
    /* c_cidr */ '192.168.0.0/16'::cidr,
    /* c_custom */ (2,4),
    /* c_date */ '2024-01-01',
    /* c_float4 */ 3.4,
    /* c_float8 */ 33333333.33444444,
    /* c_inet */ '192.168.1.1'::inet,
    /* c_int2 */ 14,
    /* c_int4 */ 100000,
    /* c_int8 */ 10000000000,
    /* c_interval */ '3 days',
    /* c_json */ '{{"hello":"world" }}',
    /* c_jsonb */ '{{"hello":"world" }}',
    /* c_map */ '{{"(1,a)","(2,b)","(3,c)"}}',
    /* c_money */ '$4.5',
    /* c_name */ 'test',
    /* c_numeric */ 199.123,
    /* c_numeric_large */ 123456789012345678901234.99,
    /* c_numeric_mod */ 99.99,
    /* c_oid */ 11,
    /* c_text */ 'fork',
    /* c_tid */ '(3,4)',
    /* c_time */ '19:34',
    /* c_timestamp */ '2024-01-01 15:00:00',
    /* c_timestamptz */ '2024-01-01 15:00:00 UTC',
    /* c_timetz */ '19:34 UTC',
    /* c_uuid */ 'acd661ca-d18c-42e2-9c4e-61794318935e',
    /* c_varbit */ '0110',
    /* c_varchar */ 'abc'
    );
    grant select on test_types to {app_user};
    """
    run_command(insert_command, pg_conn)

    pg_conn.commit()


# Function to parse the S3 path and return the bucket name and key
def parse_s3_path(s3_path):
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    return bucket_name, key


# Utility function to upload an entire local dir to s3 rooted at the target path
def s3_upload_dir(s3, local_dir, s3_bucket, target_dir):
    for root, _, files in os.walk(local_dir):
        for filename in files:
            # we need to remove the original localdir from the root as a prefix
            dirFrag = root.removeprefix(local_dir + "/")
            s3.upload_file(
                os.path.join(root, filename),
                s3_bucket,
                f"{target_dir}/{dirFrag}/{filename}",
            )


def check_table_size(pg_conn, table_name, count):
    result = run_query(f"SELECT count(*) FROM {table_name}", pg_conn)
    assert result[0]["count"] == count


def read_s3_operations(s3, spath, is_text=True):
    bucket, s3_key = parse_s3_path(spath)
    # Read from the S3 bucket
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    read_content = response["Body"].read()

    if is_text:
        read_content = read_content.decode("utf-8")

    return read_content


def assert_valid_json(result):

    # Check if the result is a dictionary, which means it's already a valid JSON object
    assert isinstance(result, dict), "The result is not a valid JSON object"

    # Check if the dictionary can be serialized back to JSON
    try:
        json_string = json.dumps(result)
        is_valid_json = True
    except ValueError:
        is_valid_json = False

    assert is_valid_json, "The result is not a valid JSON:" + result


# make sure two json objects have the exact same content, the order
# is not relevant
def assert_jsons_equivalent(json_obj1, json_obj2):

    # handle small escape char differences in representations
    normalized_1, normalized_2 = normalize_json(json_obj1), normalize_json(json_obj2)

    diff = DeepDiff(normalized_1, normalized_2, ignore_order=True)
    if diff:
        print("Differences found:")
        print(diff)
        assert False
    else:
        assert True


def normalize_json(value):
    """
    Recursively convert JSON string representations to dictionaries and handle escape characters in keys.
    """
    if isinstance(value, dict):
        new_dict = {}
        for k, v in value.items():
            # Handle escape characters in keys
            new_key = k.replace('\\"', '"')
            new_dict[new_key] = normalize_json(v)
        return new_dict
    elif isinstance(value, list):
        return [normalize_json(i) for i in value]
    elif isinstance(value, str):
        try:
            # Try to convert string to dictionary if it's valid JSON
            parsed_value = json.loads(value)
            if isinstance(parsed_value, (dict, list)):
                return normalize_json(parsed_value)
            return value
        except json.JSONDecodeError:
            return value
    else:
        return value


# Function to read JSON from a file
def read_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def read_binary(file_path):
    with open(file_path, "rb") as file:
        return file.read()


def write_to_file(file_path, content):
    with open(file_path, "w") as file:
        json.dump(content, file, indent=4)


def write_json_to_file(file_path, data):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


"""
The table we use in this test is generated as follows:

    CREATE TABLE postgres.public.spark_generated_iceberg_test (
        id bigint )
    USING iceberg
    TBLPROPERTIES ('write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read');

    INSERT INTO postgres.public.spark_generated_iceberg_test VALUES (1), (2);
    INSERT INTO postgres.public.spark_generated_iceberg_test VALUES (3), (4) ;
    INSERT INTO postgres.public.spark_generated_iceberg_test VALUES (5), (6);
    INSERT INTO postgres.public.spark_generated_iceberg_test SELECT * FROM postgres.public.spark_generated_iceberg_test;
    INSERT INTO postgres.public.spark_generated_iceberg_test SELECT explode(sequence(1, 5));
    INSERT INTO postgres.public.spark_generated_iceberg_test SELECT explode(sequence(1, 100));
    DELETE FROM postgres.public.spark_generated_iceberg_test WHERE id = 3;
    DELETE FROM postgres.public.spark_generated_iceberg_test WHERE id = 6;
    UPDATE postgres.public.spark_generated_iceberg_test SET id = id + 1 WHERE id > 25;

"""


@pytest.fixture(scope="module")
def spark_generated_iceberg_test(s3):

    for iceberg_prefix_end in [
        "spark_generated_iceberg_test",
        "spark_generated_iceberg_test_2",
        "spark_generated_iceberg_ddl_test",
    ]:
        iceberg_prefix = f"spark_test/public/" + iceberg_prefix_end
        iceberg_url = f"s3://{TEST_BUCKET}/{iceberg_prefix}"
        iceberg_path = (
            iceberg_sample_table_folder_path() + "/public/" + iceberg_prefix_end
        )

        # Upload data files
        for root, dirs, files in os.walk(iceberg_path + "/data"):
            for filename in files:
                s3.upload_file(
                    os.path.join(root, filename),
                    TEST_BUCKET,
                    f"{iceberg_prefix}/data/{filename}",
                )

        # Upload metadata files
        for root, dirs, files in os.walk(iceberg_path + "/metadata"):
            for filename in files:
                s3.upload_file(
                    os.path.join(root, filename),
                    TEST_BUCKET,
                    f"{iceberg_prefix}/metadata/{filename}",
                )


@pytest.fixture(scope="module")
def extension(superuser_conn, pg_conn, app_user):
    # we do not want to expose 1.3 features to all users, but expose in the tests
    # so we have this check on enable_experimental_features
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
        GRANT lake_read_write TO {app_user};
        GRANT USAGE ON SCHEMA lake_table TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.default_location_prefix TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.enable_manifest_merge_on_write TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.manifest_min_count_to_merge TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.target_manifest_size_kb TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.max_snapshot_age TO {app_user};
        GRANT SET ON PARAMETER pg_lake_engine.orphaned_file_retention_period TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.max_file_removals_per_vacuum TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.max_compactions_per_vacuum TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.target_row_group_size_mb TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.default_parquet_version TO {app_user};

        CREATE EXTENSION IF NOT EXISTS pgaudit;
        GRANT SET ON PARAMETER pgaudit.log TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    pg_conn.rollback()
    superuser_conn.rollback()
    run_command(
        f"""
        DROP EXTENSION pg_lake_table CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module")
def pg_conn(postgres, app_user):
    conn = open_pg_conn(app_user)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def superuser_conn(postgres):
    conn = open_pg_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def with_default_location(request, s3, extension, pg_conn, superuser_conn):
    marker = request.node.get_closest_marker("location_prefix")
    if marker is None:
        location_prefix = None
    else:
        location_prefix = marker.args[0]

    if location_prefix is None:
        location_prefix = f"s3://{TEST_BUCKET}"

    run_command(
        f"""
        SET pg_lake_iceberg.default_location_prefix TO '{location_prefix}';
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        SET pg_lake_iceberg.default_location_prefix TO '{location_prefix}';
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    pg_conn.rollback()
    superuser_conn.rollback()

    run_command(
        f"""
        RESET pg_lake_iceberg.default_location_prefix;
    """,
        pg_conn,
    )

    run_command(
        f"""
        RESET pg_lake_iceberg.default_location_prefix;
    """,
        superuser_conn,
    )

    pg_conn.commit()
    superuser_conn.commit()


def generate_random_file_path():
    temp_dir = tempfile.gettempdir()  # Get the system's temporary directory
    random_file_name = str(uuid.uuid4())  # Generate a unique file name
    return os.path.join(temp_dir, random_file_name)


# This is a wrapper to allow the creation of a specific map type by calling the
# `map_type.create()` function as a superuser using the passed-in parameters.
def create_map_type(keytype, valtype, raise_error=True):
    superuser_conn = open_pg_conn()
    if raise_error:
        res = run_query(
            f"SELECT map_type.create('{keytype}','{valtype}')",
            superuser_conn,
            raise_error=True,
        )
        superuser_conn.commit()
        return res[0][0]

    err = run_command(
        f"SELECT map_type.create('{keytype}','{valtype}')",
        superuser_conn,
        raise_error=False,
    )
    if err:
        superuser_conn.rollback()
        return err

    superuser_conn.commit()
    return None


# This fixture is intended only to simplify the process of attaching a debugger
# to a specific test that is misbehaving.  To use this, add `debug_pg_conn` to
# the list of fixures in the test, then when you hit the test in question you
# can snarf the pid in question and attach a debugger to the given running test
# backend like so:
#
# gdb -p $(</tmp/backend.pid)
#
# This is only used for debugging tests; no committed/pushed code should include
# this as a fixture.
@pytest.fixture()
def debug_pg_conn(pg_conn):
    res = run_query("select pg_backend_pid()", pg_conn)

    with open("/tmp/backend.pid", "w") as f:
        f.write(str(res[0][0]))

    time.sleep(15)

    yield


@pytest.fixture(scope="module")
def app_user(postgres):
    conn = open_pg_conn()

    # This query will generate a new unique application user that does not exist on the server
    res = run_query(
        """
        SELECT rolname, current_database() FROM pg_roles
        RIGHT JOIN (
            SELECT ('app_user_' || floor(random() * 1000)) rolname FROM generate_series(1,10)
        ) USING (rolname) WHERE pg_roles.rolname IS NULL LIMIT 1
    """,
        conn,
    )

    app_user, db_name = res[0]

    assert app_user is not None

    run_command(
        f"""
        CREATE USER {app_user};
        GRANT USAGE, CREATE ON SCHEMA public TO {app_user};
        GRANT CREATE ON DATABASE {db_name} TO {app_user};
    """,
        conn,
    )

    conn.commit()

    yield app_user

    run_command(
        f"""
        DROP OWNED BY {app_user} CASCADE;
        DROP ROLE {app_user};
    """,
        conn,
    )

    conn.commit()


# for test consistency
def file_sort_key(file_entry):
    filename = file_entry[0]

    if ".metadata.json" in filename:
        return (1, filename)  # Metadata files come first
    elif "snap-" in filename and ".avro" in filename:
        return (2, filename)  # Snapshot files come next
    elif ".avro" in filename and "-m0" in filename:
        return (3, filename)  # Manifest files come after metadata
    elif ".avro" in filename and "-m1" in filename:
        return (4, filename)  # Manifest files come after metadata
    elif ".avro" in filename and "-m2" in filename:
        return (5, filename)  # Manifest files come after metadata
    elif ".avro" in filename and "-m3" in filename:
        return (6, filename)  # Manifest files come after metadata
    elif ".parquet" in filename:
        if "data_" in filename:
            # Second element in tuple sorts by filename within the data files
            return (7, filename)  # Data files come after delete files
        else:
            return (8, filename)  # Positional delete files first

    return (0, filename)  # Default order, sorted by filename


# get files from iceberg metadata
# get files from listing s3
# then compare results
# this should be called after VACUUM which triggers removal of
# unreferenced files
def assert_iceberg_s3_file_consistency(pg_conn, s3, table_namespace, table_name):

    files_via_s3_list = iceberg_s3_list_all_files_for_table(
        pg_conn, s3, table_namespace, table_name
    )

    current_metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]
    files_via_current_iceberg_metadata = iceberg_get_referenced_files_metadata_path(
        pg_conn, current_metadata_location
    )

    prev_metadata_location = run_query(
        f"SELECT previous_metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]
    if prev_metadata_location:
        files_via_current_iceberg_metadata.append(prev_metadata_location)

    # Apply normalization
    normalized_s3 = set([normalize_dictrow(row) for row in files_via_s3_list])
    normalized_iceberg = set(
        [normalize_dictrow(row) for row in files_via_current_iceberg_metadata]
    )

    # Find files that are only in one of the sets
    only_in_s3 = normalized_s3 - normalized_iceberg
    only_in_iceberg = normalized_iceberg - normalized_s3

    # Assert if both sets are the same
    assert len(normalized_s3) > 0
    assert len(normalized_iceberg) > 0
    assert (
        normalized_s3 == normalized_iceberg
    ), f"Files differ:\nOnly in S3: {only_in_s3}\nOnly in Iceberg: {only_in_iceberg}"


def iceberg_get_referenced_files(pg_conn, table_name):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]

    return iceberg_get_referenced_files_metadata_path(pg_conn, metadata_location)


def iceberg_get_referenced_files_metadata_path(pg_conn, metadata_location):
    referenced_files = run_query(
        f"""SELECT * FROM lake_iceberg.find_all_referenced_files('{metadata_location}')""",
        pg_conn,
    )

    # get consistent results
    referenced_files.sort(key=file_sort_key)

    return referenced_files


def iceberg_s3_list_all_files_for_table(pg_conn, s3, table_namespace, table_name):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]
    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    table_location = metadata_json["location"]

    all_files = run_query(
        f"SELECT path FROM lake_file.list('{table_location}/**')", pg_conn
    )

    return all_files


# full path : s3://testbucketcdw/postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/metadata/4dcf5d74-ef51-494c-b5f1-87c4707a8c62/metadata0.json
# table_metadata_prefix: postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/metadata/
def table_metadata_prefix(pg_conn, table_namespace, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]

    bucket, s3_key = parse_s3_path(metadata_location)

    return f"s3://{bucket}/" + "/".join(s3_key.split("/")[:-1]) + "/**"


# full path : s3://testbucketcdw/postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/metadata/4dcf5d74-ef51-494c-b5f1-87c4707a8c62/metadata0.json
# table_data_prefix: postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/data/
def table_data_prefix(pg_conn, table_namespace, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]

    bucket, s3_key = parse_s3_path(metadata_location)

    return f"s3://{bucket}/" + "/".join(s3_key.split("/")[:-2]) + "/data/**"


def s3_list(pg_conn, uri):
    result = run_query(f"select * from lake_file.list('{uri}')", pg_conn)

    return result


def s3_prefix_contains_any_file(pg_conn, prefix):
    exists = run_query(f"select count(*) > 0 from lake_file.list('{prefix}')", pg_conn)[
        0
    ][0]

    return exists


# Normalize both lists to use the same key (e.g., 'path')
def normalize_dictrow(row):
    if "filename" in row:
        return row["filename"]  # Return the file path if the key is 'filename'
    elif "path" in row:
        return row["path"]  # Return the file path if the key is 'path'
    else:
        return row  # Handle unexpected cases


# prevent things like
# INFO     werkzeug:_internal.py:97 127.0.0.1 - - [19/Sep/2024 12:06:40] "POST /testbucketcdw/?delete= HTTP/1.1" 200 -
# when test fails
def reduce_werkzeug_log_level():
    import logging

    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def get_pg_version_num(pg_conn) -> int:
    """
    Retrieves the server_version_num from PostgreSQL using the given connection.

    Args:
        pg_conn: The PostgreSQL connection object.

    Returns:
        int: The server version number as an integer.
    """
    # Query to get the server version number
    query = "SHOW server_version_num;"

    # Execute the query using the provided run_query function
    result = run_query(query, pg_conn)

    # Extract the version number from the result and convert it to an integer
    if result and isinstance(result, list) and len(result) > 0:
        server_version_num = result[0][0]
        return int(server_version_num)

    raise ValueError("Server version number not found in the query result.")


def pytest_addoption(parser):
    if "__initialized" not in parser.extra_info:
        parser.addoption(
            "--installcheck",
            action="store_true",
            default=False,
            help="installcheck: True or False",
        )
        parser.addoption(
            "--isolationtester",
            action="store_true",
            default=False,
            help="isolationtester: True or False",
        )
        parser.addoption(
            "--pglog",
            action="store",
            default=None,
            help="set log_min_messages on generated clusters.",
        )
        parser.extra_info["__initialized"] = True


@pytest.fixture(scope="session")
def installcheck(request):
    myopt = request.config.getoption("--installcheck")

    if myopt:
        os.environ["INSTALLCHECK"] = "1"

    return myopt


@pytest.fixture(scope="session")
def isolationtester(request):
    return request.config.getoption("--isolationtester")


def compare_results_with_reference_iceberg_implementations(
    installcheck,
    pg_conn,
    duckdb_conn,
    spark_session,
    table_name,
    table_namespace,
    query,
):

    if table_namespace is not None:
        get_metadata_location_query = f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'"
        metadata_location = run_query(get_metadata_location_query, pg_conn)[0][0]
    else:
        get_metadata_location_query = f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'"
        locations = run_query(get_metadata_location_query, pg_conn)
        assert len(locations) == 1, "use a different table name"
        metadata_location = locations[0][0]

    if spark_session is not None:
        compare_results_with_pyspark(
            installcheck,
            pg_conn,
            spark_session,
            table_name,
            table_namespace,
            metadata_location,
            query,
        )

    if duckdb_conn is not None:
        compare_results_with_duckdb(
            pg_conn, duckdb_conn, table_name, table_namespace, metadata_location, query
        )


def compare_results_with_duckdb(
    pg_conn, duckdb_conn, table_name, table_namespace, metadata_location, query
):

    pg_lake_result = run_query(query, pg_conn)

    if table_namespace is not None:
        query = query.replace(
            table_namespace + "." + table_name, f"iceberg_scan('{metadata_location}')"
        )
    else:
        query = query.replace(table_name, f"iceberg_scan('{metadata_location}')")

    duckdb_conn.execute(query)
    duckdb_result = duckdb_conn.fetchall()

    pg_lake_result = sort_with_none_at_end(pg_lake_result)
    duckdb_result = sort_with_none_at_end(duckdb_result)
    assert (
        len(pg_lake_result) > 0
    ), "No rows returned, make sure at least one row returns"
    assert len(pg_lake_result) == len(
        duckdb_result
    ), "Result sets have different lengths"

    for row_pg_lake, row_duckdb in zip(pg_lake_result, duckdb_result):
        assert compare_rows_as_string_or_float(
            row_pg_lake, row_duckdb, 0.001
        ), f"Results do not match: {row_pg_lake} and {row_duckdb}"


def spark_register_table(
    installcheck, spark_session, table_name, table_namespace, metadata_location
):
    if installcheck:
        return

    table_name_str = (
        f"{table_namespace}.{table_name}" if table_namespace else table_name
    )

    # todo: remove try block. NumberFormatException is thrown even if the table is successfully registered
    try:
        spark_session.sql(
            f"""
            CALL {server_params.SPARK_CATALOG}.system.register_table(
                table => '{table_name_str}',
                metadata_file => '{metadata_location}'
            )
            """
        )
    except NumberFormatException:
        pass


def spark_unregister_table(installcheck, spark_session, table_name, table_namespace):
    if installcheck:
        return

    table_name_str = (
        f"{table_namespace}.{table_name}" if table_namespace else table_name
    )
    spark_session.sql(f"DROP TABLE {table_name_str}")


def compare_results_with_pyspark(
    installcheck,
    pg_conn,
    spark_session,
    table_name,
    table_namespace,
    metadata_location,
    query,
):
    if installcheck:
        return

    spark_register_table(
        installcheck, spark_session, table_name, table_namespace, metadata_location
    )

    assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, query, query
    )

    spark_unregister_table(installcheck, spark_session, table_name, table_namespace)


def date_days_since_epoch(d: datetime.date) -> int:
    return (d - datetime.date(1970, 1, 1)).days


def compare_rows_as_string_or_float(row_pg_lake, row_other, tolerance):
    for val_pg_lake, val_other in zip(row_pg_lake, row_other):
        if isinstance(val_pg_lake, memoryview) and isinstance(val_other, bytearray):
            val_pg_lake = bytearray(val_pg_lake)
        elif isinstance(val_pg_lake, int) and isinstance(val_other, datetime.date):
            # sometimes val_pg_lake is day count from unix epoch but spark returns it as datetime
            val_other = date_days_since_epoch(val_other)
        elif isinstance(val_pg_lake, datetime.datetime) and isinstance(
            val_other, datetime.datetime
        ):
            # remove timezone info from both values by adjusting time
            val_pg_lake_tzinfo = val_pg_lake.tzinfo
            if val_pg_lake_tzinfo is not None:
                val_pg_lake = val_pg_lake.replace(tzinfo=None) + datetime.timedelta(
                    seconds=val_pg_lake_tzinfo.utcoffset(val_pg_lake).total_seconds()
                )

                # spark already adjust the time to the local timezone (we set utc when creating the session)

        # Convert both values to strings for comparison
        if str(val_pg_lake) != str(val_other):
            try:
                # If they can't be compared as floats, just return False
                if abs(float(val_pg_lake) - float(val_other)) > tolerance:
                    print(f"Mismatch: {val_pg_lake} vs {val_other}")
                    return False
            except ValueError:
                # If they aren't numeric, they must be exactly equal as strings
                print(f"Mismatch: {val_pg_lake} vs {val_other}")
                return False
            except TypeError:
                print(f"Mismatch: {val_pg_lake} vs {val_other}")
                return False
    return True


@pytest.fixture(scope="module")
def create_spark_catalog_database(installcheck, superuser_conn):
    if not installcheck:
        run_command_outside_tx(
            [f"CREATE DATABASE {server_params.SPARK_CATALOG_PG_DATABASE}"],
            superuser_conn,
        )

    yield

    if not installcheck:
        run_command_outside_tx(
            [f"DROP DATABASE {server_params.SPARK_CATALOG_PG_DATABASE} WITH (FORCE)"],
            superuser_conn,
        )


@pytest.fixture(scope="module")
def spark_session(installcheck, create_spark_catalog_database):
    spark_session = None

    if not installcheck:
        spark_version = ".".join(importlib.metadata.version("pyspark").split(".")[:2])
        # got these versions from pyiceberg tests
        # (https://github.com/apache/iceberg-python/blob/ff3a24981c59c2d1935cde02b72ff4249e4d8525/tests/conftest.py#L2224)
        scala_version = "2.12"
        iceberg_version = "1.4.3"

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version},"
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version} pyspark-shell"
        )
        os.environ["AWS_REGION"] = TEST_AWS_REGION
        os.environ["AWS_ACCESS_KEY_ID"] = TEST_AWS_ACCESS_KEY_ID
        os.environ["AWS_SECRET_ACCESS_KEY"] = TEST_AWS_SECRET_ACCESS_KEY

        # spark timestamp are alias of timestamp_ltz (it adjusts the time to the local timezone.
        # To compare Postgres timestamptz values with spark's timestamp,
        # we set system timezone to utc so soark uses ut for local timezone)
        prev_tz = os.environ.get("TZ")
        os.environ["TZ"] = "UTC"

        spark_session = (
            SparkSession.builder.appName("spark catalog test")
            .config(
                f"spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.driver.host", "localhost")
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.catalog-impl",
                "org.apache.iceberg.jdbc.JdbcCatalog",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.uri",
                f"jdbc:postgresql://localhost:{server_params.PG_PORT}/{server_params.SPARK_CATALOG_PG_DATABASE}",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.jdbc.user",
                server_params.PG_USER,
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.jdbc.password",
                server_params.PG_PASSWORD,
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.warehouse",
                f"s3://{TEST_BUCKET}/",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.s3.endpoint",
                f"http://localhost:{MOTO_PORT}",
            )
            .config(
                f"spark.sql.catalog.{server_params.SPARK_CATALOG}.s3.path-style-access",
                "true",
            )
            .config(f"spark.driver.extraClassPath", os.getenv("JDBC_DRIVER_PATH"))
            .getOrCreate()
        )

        # we do not need to specify catalog name in queries or DDLs anymore
        spark_session.catalog.setCurrentCatalog(server_params.SPARK_CATALOG)

        # set system timezone to old one
        if prev_tz:
            os.environ["TZ"] = prev_tz

    yield spark_session

    if not installcheck:
        spark_session.stop()


def generate_random_numeric_typename(max_precision=38):
    precision = random.randint(1, max_precision)
    scale = random.randint(0, precision - 1)
    return f"numeric({precision},{scale})"


def generate_random_value(pg_type_name):
    if pg_type_name == "smallint" or pg_type_name == "int2":
        pg_val = random.randint(-32768, 32767)
        spark_val = pg_val
    elif pg_type_name == "int" or pg_type_name == "int4":
        pg_val = random.randint(-2147483648, 2147483647)
        spark_val = pg_val
    elif pg_type_name == "bigint" or pg_type_name == "int8":
        pg_val = random.randint(-9223372036854775808, 9223372036854775807)
        spark_val = pg_val
    elif pg_type_name == "float4":
        pg_val = f"{random.uniform(-(10**4), 10**4):e}"
        spark_val = pg_val
    elif pg_type_name == "float8":
        pg_val = f"{random.uniform(-(10**4), 10**4):e}"
        spark_val = pg_val
    elif pg_type_name.startswith("numeric"):
        match = re.match(r"numeric\((\d+),-?(\d+)\)", pg_type_name)
        precision = int(match.group(1)) if match is not None else 38
        scale = int(match.group(2)) if match is not None else 9
        to_power = precision - scale

        def truncate_float(x):
            if x == 0:
                return 0
            else:
                factor = 10.0**scale
                return math.trunc(x * factor) / factor

        pg_val = truncate_float(
            random.uniform(-(10**to_power) + 1e-6, 10**to_power - 1e-6)
        )
        spark_val = f"{pg_val}BD"
    elif pg_type_name.startswith("varchar"):
        match = re.match(r"varchar\((\d+)\)", pg_type_name)
        length = int(match.group(1)) if match is not None else 20
        val = "".join(random.choices(string.ascii_lowercase, k=length))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name.startswith("bpchar"):
        match = re.match(r"bpchar\((\d+)\)", pg_type_name)
        length = int(match.group(1)) if match is not None else 20
        val = "".join(random.choices(string.ascii_lowercase, k=length))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name == "text":
        val = "".join(random.choices(string.ascii_lowercase, k=10))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name == '"char"':
        val = "".join(random.choices(string.ascii_lowercase, k=1))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name.startswith("char"):
        match = re.match(r"char\((\d+)\)", pg_type_name)
        length = int(match.group(1)) if match is not None else 1
        val = "".join(random.choices(string.ascii_lowercase, k=length))
        pg_val = f"'{val}'"
        spark_val = pg_val
    elif pg_type_name == "bytea":
        val = bytes(random.choices(range(256), k=10))
        pg_val = psycopg2.Binary(val)
        spark_val = f"X'{val.hex()}'"
    elif pg_type_name == "date":
        month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        year = random.randint(1, 9999)
        month = random.randint(1, 12)
        day = random.randint(1, month_days[month - 1])
        val = datetime.date(year, month, day)
        pg_val = psycopg2.Date(year, month, day)
        spark_val = f"date '{str(val)}'"
    elif pg_type_name == "time":
        pg_val = psycopg2.Time(
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "timetz":
        pg_val = psycopg2.Time(
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        )
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "timestamp":
        month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        year = random.randint(1, 9999)
        month = random.randint(1, 12)
        day = random.randint(1, month_days[month - 1])
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        val = datetime.datetime(year, month, day, hour, minute, second)
        pg_val = psycopg2.Timestamp(year, month, day, hour, minute, second)
        spark_val = f"timestamp_ntz '{str(val)}'"
    elif pg_type_name == "timestamptz":
        month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        # bug for years <= 1910 (Old repo: issues/1279)
        year = random.randint(1911, 9998)
        month = random.randint(1, 12)
        day = random.randint(1, month_days[month - 1])
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        tzinfo_utc = zoneinfo.ZoneInfo("UTC")
        val = datetime.datetime(
            year, month, day, hour, minute, second, tzinfo=tzinfo_utc
        )
        pg_val = psycopg2.Timestamp(year, month, day, hour, minute, second, tzinfo_utc)
        spark_val = f"timestamp '{str(val)}'"
    elif pg_type_name == "boolean" or pg_type_name == "bool":
        pg_val = random.choice([True, False])
        spark_val = pg_val
    elif pg_type_name == "interval":
        years = random.randint(2, 1000)
        months = random.randint(2, 100)
        days = random.randint(2, 1000)
        hours = random.randint(2, 100)
        minutes = random.randint(2, 100)
        seconds = random.randint(2, 100)
        pg_val = f"'{years} years {months} months {days} days {hours} hours {minutes} minutes {seconds} seconds'"
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "uuid":
        pg_val = f"'{uuid.uuid4()}'"
        spark_val = None  # not applicable type for spark
    elif pg_type_name == "json" or pg_type_name == "jsonb":
        generate_random_key = lambda: generate_random_value("varchar(10)").strip("'")
        generate_random_str_value = lambda: generate_random_value("varchar(10)").strip(
            "'"
        )
        generate_random_int_value = lambda: generate_random_value("int")
        generate_random_float_value = lambda: generate_random_value("float8")

        num_keys = random.randint(1, 5)
        json_obj = {}
        for _ in range(num_keys):
            key, _ = generate_random_key()
            val, _ = random.choice(
                [
                    generate_random_str_value,
                    generate_random_int_value,
                    generate_random_float_value,
                ]
            )()
            json_obj[key] = val

        pg_val = f"'{json.dumps(json_obj)}'"
        spark_val = None  # not applicable type for spark
    else:
        raise ValueError(f"Unsupported pg type: {pg_type_name}")

    return (str(pg_val), str(spark_val))


def pg_lake_version_from_env():
    return os.getenv("PG_LAKE_GIT_VERSION", "0.0.0")


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and before performing collection.
    We use this hook to set the environment variable if '--setenv' is provided.
    """
    config = session.config
    log_level = config.getoption("--pglog")
    if log_level is not None:
        if log_level not in [
            "panic",
            "log",
            "notice",
            "fatal",
            "error",
            "warning",
            "debug",
            "debug1",
            "debug2",
            "debug3",
            "debug4",
            "debug5",
        ]:
            raise Exception("invalid --pglog setting: expected valid log_min_messages")

        os.environ["LOG_MIN_MESSAGES"] = log_level


@pytest.fixture(scope="module")
def create_reserialize_helper_functions(superuser_conn, iceberg_extension):
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_table_metadata(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_table_metadata$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_manifest(
                manifestInputPath TEXT,
                manifestOutputPath TEXT
        ) RETURNS VOID
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_manifest$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_manifest_list(
                manifestListInputPath TEXT,
                manifestListOutputPath TEXT
        ) RETURNS VOID
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_manifest_list$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.manifest_list_path_from_table_metadata(
                tableMetadataPath TEXT
        ) RETURNS TEXT
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$manifest_list_path_from_table_metadata$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.manifest_paths_from_manifest_list(
                manifestListPath TEXT
        ) RETURNS TEXT[]
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$manifest_paths_from_manifest_list$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.datafile_paths_from_table_metadata(
                tableMetadataPath TEXT,
                isDelete bool DEFAULT false
        ) RETURNS TEXT[]
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$datafile_paths_from_table_metadata$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.current_manifests(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                manifest_path TEXT,
                manifest_length BIGINT,
                partition_spec_id INT,
                manifest_content TEXT,
                sequence_number BIGINT,
                min_sequence_number BIGINT,
                added_snapshot_id BIGINT,
                added_files_count INT,
                existing_files_count INT,
                deleted_files_count INT,
                added_rows_count BIGINT,
                existing_rows_count BIGINT,
                deleted_rows_count BIGINT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_manifests$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.current_partition_fields(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                datafile_path TEXT,
                partition_field_id INT,
                partition_field_name TEXT,
                partition_field_physical_type TEXT,
                partition_field_logical_type TEXT,
                partition_field_value TEXT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_partition_fields$function$;
""",
        superuser_conn,
    )

    yield

    run_command(
        """
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_iceberg_table_metadata(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_iceberg_manifest(TEXT, TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_iceberg_manifest_list(TEXT, TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.manifest_list_path_from_table_metadata(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.manifest_paths_from_manifest_list(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.datafile_paths_from_table_metadata(TEXT, bool);
        DROP FUNCTION IF EXISTS lake_iceberg.current_manifests(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.current_partition_fields(TEXT);
                """,
        superuser_conn,
    )


def regenerate_metadata_json(superuser_conn, metadata_location, s3):

    command = f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_location}')::json"
    res = run_query(command, superuser_conn)

    json_string = res[0][0]

    metadata_tmpfile = tempfile.NamedTemporaryFile()

    write_json_to_file(metadata_tmpfile.name, json_string)
    bucket, s3_key = parse_s3_path(metadata_location)
    s3.upload_file(metadata_tmpfile.name, bucket, s3_key)


def regenerate_manifest_file(superuser_conn, manifest_location, s3):

    manifest_tmpfile = tempfile.NamedTemporaryFile()
    command = f"SELECT lake_iceberg.reserialize_iceberg_manifest('{manifest_location}', '{manifest_tmpfile.name}')"

    run_command(command, superuser_conn)

    bucket, s3_key = parse_s3_path(manifest_location)
    s3.upload_file(manifest_tmpfile.name, bucket, s3_key)


def regenerate_manifest_list_file(superuser_conn, manifest_list_location, s3):

    manifest_list_tmpfile = tempfile.NamedTemporaryFile()
    command = f"SELECT lake_iceberg.reserialize_iceberg_manifest_list('{manifest_list_location}', '{manifest_list_tmpfile.name}')"

    run_command(command, superuser_conn)

    bucket, s3_key = parse_s3_path(manifest_list_location)
    s3.upload_file(manifest_list_tmpfile.name, bucket, s3_key)


def manifest_list_file_location(superuser_conn, metadata_location):
    res = run_query(
        f"""
        SELECT lake_iceberg.manifest_list_path_from_table_metadata('{metadata_location}');
""",
        superuser_conn,
    )

    manifest_list_path = res[0][0]
    return manifest_list_path


def manifest_file_locations(superuser_conn, manifest_list_location):
    res = run_query(
        f"""
        SELECT lake_iceberg.manifest_paths_from_manifest_list('{manifest_list_location}');
""",
        superuser_conn,
    )

    manifest_paths = res[0][0]
    return manifest_paths


def change_timezone(superuser_conn, tz):
    old_timezone = run_query("SHOW timezone", superuser_conn)[0][0]

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET timezone = '{tz}';",
            "SELECT pg_reload_conf();",
        ],
        superuser_conn,
    )

    return old_timezone


def get_polaris_access_token(
    credentials_file: str = server_params.POLARIS_PRINCIPAL_CREDS_FILE,
    host: str = server_params.POLARIS_HOSTNAME,
    port: int = server_params.POLARIS_PORT,
    timeout: int = 2,
) -> str:
    creds = json.loads(Path(credentials_file).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]

    token_url = f"http://{host}:{port}/api/catalog/v1/oauth/tokens"

    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        auth=(client_id, client_secret),
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# DONOT add postgres fixture as dep. that causes circular deps and consumes all shared memory
@pytest.fixture(scope="module")
def polaris_session(installcheck) -> requests.Session:
    if installcheck:
        return None
    """
    Ready‑to‑use requests.Session that attaches the bearer token
    to every call.
    """
    polaris_token = get_polaris_access_token()
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {polaris_token}"})
    return sess


@pytest.fixture(scope="function")
def grant_access_to_data_file_partition(
    extension,
    app_user,
    superuser_conn,
):
    run_command(
        f"""
        GRANT SELECT ON lake_table.data_file_partition_values TO {app_user};
    GRANT SELECT ON lake_iceberg.tables TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        REVOKE SELECT ON lake_table.data_file_partition_values FROM {app_user};
    REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module")
def create_injection_extension(superuser_conn):
    if get_pg_version_num(superuser_conn) >= 170000:
        run_command("CREATE EXTENSION injection_points", superuser_conn)
        superuser_conn.commit()
    yield

    if get_pg_version_num(superuser_conn) >= 170000:
        run_command("DROP EXTENSION injection_points", superuser_conn)
        superuser_conn.commit()


def table_partition_specs(pg_conn, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]

    pg_query = f"SELECT * FROM lake_iceberg.metadata('{metadata_location}')"

    metadata = run_query(pg_query, pg_conn)[0][0]
    return metadata["partition-specs"]

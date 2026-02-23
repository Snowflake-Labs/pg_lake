import os
import psycopg2
import psycopg2.extras
import pytest
import shutil
import subprocess
import queue
import threading
import time
from pathlib import Path
from utils_pytest import *
import server_params

reduce_werkzeug_log_level()


# This fixture ensures that the app_user can read/write URLs for all tests in this file
@pytest.fixture(scope="module", autouse=True)
def setup_readwrite_perms(superuser_conn, app_user):
    db_name = server_params.PG_DATABASE

    run_command(
        f"CREATE EXTENSION IF NOT EXISTS pg_lake_engine CASCADE;", superuser_conn
    )
    run_command(f"GRANT lake_read_write TO {app_user};", superuser_conn)
    superuser_conn.commit()

    yield


@pytest.fixture(scope="module")
def postgres(installcheck):

    if not installcheck:
        server, output_queue, output_thread = setup_pgduck_server()

        start_postgres(
            server_params.PG_DIR, server_params.PG_USER, server_params.PG_PORT
        )

    yield

    if not installcheck:
        stop_postgres(server_params.PG_DIR)

        if os.path.isdir(server_params.PG_DIR + "/base/pgsql_tmp"):
            assert len(os.listdir(server_params.PG_DIR + "/base/pgsql_tmp")) == 0

        terminate_server(server, output_thread)


@pytest.fixture(scope="module")
def pg_conn(postgres, app_user):
    conn = open_pg_conn(app_user)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pgduck_conn(postgres):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3():
    client, server = create_mock_s3()
    yield client
    server.stop()


@pytest.fixture(scope="session")
def gcs():
    client, server = create_mock_gcs()
    yield client
    server.stop()


@pytest.fixture(scope="session")
def azure():
    client, server = create_mock_azure_blob_storage()
    yield client
    terminate_process(server)


@pytest.fixture(scope="module")
def duckdb_conn(s3):
    conn = create_duckdb_conn()
    yield conn
    conn.close()


# when --installcheck is passed to pytests,
# override the variables to point to the
# official pgduck_server settings
# this trick helps us to use the existing
# pgduck_server
@pytest.fixture(autouse=True, scope="session")
def configure_server_params(request):
    if request.config.getoption("--installcheck"):
        server_params.PGDUCK_PORT = 5332
        server_params.DUCKDB_DATABASE_FILE_PATH = "/tmp/duckdb.db"
        server_params.PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
        server_params.PGDUCK_CACHE_DIR = "/tmp/cache"

        # Access environment variables if exists
        server_params.PG_DATABASE = os.getenv(
            "PGDATABASE", "regression"
        )  # 'postgres' or a default
        server_params.PG_USER = os.getenv(
            "PGUSER", "postgres"
        )  # 'postgres' or a postgres
        server_params.PG_PASSWORD = os.getenv(
            "PGPASSWORD", "postgres"
        )  # 'postgres' or a postgres
        server_params.PG_PORT = os.getenv("PGPORT", "5432")  # '5432' or a default
        server_params.PG_HOST = os.getenv(
            "PGHOST", "localhost"
        )  # 'localhost' or a default

        # mostly relevant for CI
        server_params.PG_DIR = "/tmp/pg_installcheck_tests"

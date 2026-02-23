import duckdb
import os
import psycopg2
import psycopg2.extras
import pytest
import shutil
import signal
import subprocess
import queue
import threading
import time
from pathlib import Path
from utils_pytest import *
import server_params

reduce_werkzeug_log_level()


@pytest.fixture(scope="session")
def postgres(installcheck):
    if installcheck:
        # re-running installcheck might cause different results otherwise
        remove_duckdb_cache()
    else:
        pgduck_server, pgduck_output_queue, pgduck_output_thread = setup_pgduck_server()
        start_postgres(
            server_params.PG_DIR, server_params.PG_USER, server_params.PG_PORT
        )

        polaris_server = start_polaris_server_in_background()

    yield

    if not installcheck:
        stop_postgres(server_params.PG_DIR)

        if os.path.isdir(server_params.PG_DIR + "/base/pgsql_tmp"):
            assert len(os.listdir(server_params.PG_DIR + "/base/pgsql_tmp")) == 0

        terminate_server(pgduck_server, pgduck_output_thread)

        polaris_server.terminate()
        try:
            polaris_server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            polaris_server.kill()
            polaris_server.wait(timeout=10)
        polaris_pid = Path(server_params.POLARIS_PID_FILE)
        if polaris_pid.exists():
            os.kill(int(polaris_pid.read_text().strip()), signal.SIGTERM)


@pytest.fixture(scope="module")
def superuser_conn(postgres):
    conn = open_pg_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pgduck_conn(postgres):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3():
    client, server = create_mock_s3()
    yield client
    server.stop()


@pytest.fixture(scope="module")
def test_s3_path(request, s3):
    return f"s3://{TEST_BUCKET}/{request.node.name}"


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


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, iceberg_extension, s3):
    catalog = create_iceberg_test_catalog(superuser_conn)
    yield catalog
    tables = catalog.list_tables("public")
    for table in tables:
        catalog.drop_table(table)
    catalog.drop_namespace("public")
    catalog.engine.dispose()


@pytest.fixture(scope="module")
def duckdb_conn(s3):
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
    yield conn
    conn.close()

import duckdb
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

reduce_werkzeug_log_level()


@pytest.fixture(scope="session")
def server_state():
    data = {"pgduck_server_started": False}
    return data


@pytest.fixture(scope="session")
def pgduck_server(installcheck, server_state):

    if installcheck:
        yield None
    else:
        if not server_state["pgduck_server_started"]:
            server, output_queue, stderr_thread = setup_pgduck_server()

            server_state["pgduck_server_started"] = True

            yield server, output_queue, stderr_thread

            terminate_server(server, stderr_thread)

            server_state["pgduck_server_started"] = False
        else:
            yield None


@pytest.fixture(scope="session")
def postgres(installcheck, server_state):
    if installcheck:
        # re-running installcheck might cause different results otherwise
        remove_duckdb_cache()
    else:
        pgduck_started = server_state["pgduck_server_started"]

        if not pgduck_started:
            server, output_queue, stderr_thread = setup_pgduck_server()
            server_state["pgduck_server_started"] = True

        start_postgres(
            server_params.PG_DIR, server_params.PG_USER, server_params.PG_PORT
        )

    yield

    if not installcheck:
        stop_postgres(server_params.PG_DIR)

        if os.path.isdir(server_params.PG_DIR + "/base/pgsql_tmp"):
            assert len(os.listdir(server_params.PG_DIR + "/base/pgsql_tmp")) == 0

        # we had to start ourselves
        if not pgduck_started:
            terminate_server(server, stderr_thread)
            server_state["pgduck_server_started"] = False


@pytest.fixture(scope="module")
def test_user(pg_lake_table_extension):
    username = "test_application"

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        CREATE USER {username};
        GRANT ALL ON SCHEMA public TO {username};
        GRANT CREATE ON DATABASE {server_params.PG_DATABASE} TO {username};
        GRANT lake_read_write TO {username};
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield username

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP OWNED BY {username};
        DROP USER {username};
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="module")
def superuser_conn(postgres):
    conn = open_pg_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def user_conn(test_user):
    conn = open_pg_conn(user=test_user)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pg_lake_table_extension(extension):
    # Wrapper around extension fixture, probably should rename more broadly
    pass


@pytest.fixture(scope="module")
def spatial_analytics_extension(postgis_extension):
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        SET pgaudit.log TO 'none';
        CREATE EXTENSION IF NOT EXISTS pg_lake_spatial CASCADE;
        RESET pgaudit.log;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_spatial CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="session")
def postgis_extension(postgres):
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        SET pgaudit.log TO 'none';
        CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
        RESET pgaudit.log;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS postgis CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="session")
def s3():
    client, server = create_mock_s3()
    yield client
    server.stop()


@pytest.fixture(scope="session")
def azure():
    client, server = create_mock_azure_blob_storage()
    yield client
    terminate_process(server)


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
def pgduck_conn(postgres):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()

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


@pytest.fixture(scope="module")
def test_s3_path(request, s3):
    return f"s3://{TEST_BUCKET}/{request.node.name}"


@pytest.fixture(scope="module")
def pgduck_conn(postgres):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()

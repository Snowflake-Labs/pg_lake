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


@pytest.fixture(scope="module")
def duckdb_conn(s3):
    conn = create_duckdb_conn()
    yield conn
    conn.close()

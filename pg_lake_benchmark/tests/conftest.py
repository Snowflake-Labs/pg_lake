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


@pytest.fixture(scope="module")
def pg_lake_benchmark_extension():
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_benchmark CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_benchmark;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

import pytest
import psycopg2
import server_params
import time
import os
from utils_pytest import *

PG_DIR = "/tmp/pg_extension_base_tests"


@pytest.fixture(scope="module")
def pg_extension_base():
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_extension_base CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()

    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_extension_base CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

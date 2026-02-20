import pytest
import psycopg2
import server_params
import time
import os
from utils_pytest import *


@pytest.fixture(scope="session")
def postgres(installcheck):
    if not installcheck:
        start_postgres(
            server_params.PG_DIR, server_params.PG_USER, server_params.PG_PORT
        )

    yield

    if not installcheck:
        stop_postgres(server_params.PG_DIR)


@pytest.fixture(scope="module")
def pg_job_scheduler():
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_job_scheduler CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()

    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_job_scheduler CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


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

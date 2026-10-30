"""Lakekeeper (second REST catalog, #739) helper functions and fixtures.

Importing this module starts Lakekeeper for the session, like helpers.polaris
does for Polaris.  Lakekeeper runs without authentication, so pg_lake reaches
it through a server with rest_auth_type 'none' and no user mapping.
"""

import atexit
import os
import subprocess
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog

from . import server_params

from utils_pytest import (
    MOTO_PORT,
    TEST_BUCKET,
    TEST_AWS_ACCESS_KEY_ID,
    TEST_AWS_SECRET_ACCESS_KEY,
    TEST_AWS_REGION,
    run_command,
    stop_process_via_pidfile,
)

LAKEKEEPER_URI = f"http://localhost:{server_params.LAKEKEEPER_PORT}/catalog"
LAKEKEEPER_LOCATION_PREFIX = f"s3://{TEST_BUCKET}/lakekeeper"


def get_lakekeeper_server_path():
    base_dir = Path(__file__).resolve().parent.parent
    return base_dir / "rest_catalog/lakekeeper_server.sh"


def start_lakekeeper_server_in_background():
    # Stop any leftover Lakekeeper from a previous interrupted run.
    stop_lakekeeper()

    env = os.environ.copy()
    env.update(
        {
            "PGHOST": server_params.PG_HOST,
            "PGPORT": str(server_params.PG_PORT),
            "PGUSER": server_params.PG_USER,
            "PGDATABASE": server_params.PG_DATABASE,
            "PGPASSWORD": server_params.PG_PASSWORD,
            "LAKEKEEPER_PORT": str(server_params.LAKEKEEPER_PORT),
            "LAKEKEEPER_WAREHOUSE": server_params.LAKEKEEPER_WAREHOUSE,
            "LAKEKEEPER_PID_FILE": server_params.LAKEKEEPER_PID_FILE,
            "LAKEKEEPER_WAREHOUSE_FILE": server_params.LAKEKEEPER_WAREHOUSE_FILE,
            "S3_ENDPOINT": f"http://localhost:{MOTO_PORT}",
            "S3_BUCKET": TEST_BUCKET,
            "S3_REGION": TEST_AWS_REGION,
            "S3_ACCESS_KEY_ID": TEST_AWS_ACCESS_KEY_ID,
            "S3_SECRET_ACCESS_KEY": TEST_AWS_SECRET_ACCESS_KEY,
        }
    )

    log_file = Path("/tmp/lakekeeper_startup.log").open("w+", buffering=1)

    # Register cleanup first so the server is stopped even if setup fails.
    atexit.register(stop_lakekeeper)

    exit_code = subprocess.Popen(
        ["bash", str(get_lakekeeper_server_path()), "--purge", "--no-wait"],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    ).wait()

    if exit_code != 0:
        raise Exception(
            f"lakekeeper server start failed with return code {exit_code}, "
            "see /tmp/lakekeeper_startup.log and /tmp/lakekeeper.log"
        )


def stop_lakekeeper(timeout=10):
    stop_process_via_pidfile(server_params.LAKEKEEPER_PID_FILE, timeout)


def lakekeeper_warehouse_id():
    return Path(server_params.LAKEKEEPER_WAREHOUSE_FILE).read_text().strip()


def create_lakekeeper_catalog():
    """A pyiceberg client for the Lakekeeper warehouse, reading moto with the
    test credentials rather than anything the catalog vends."""
    return RestCatalog(
        "lakekeeper",
        **{
            "uri": LAKEKEEPER_URI,
            "warehouse": server_params.LAKEKEEPER_WAREHOUSE,
            "header.X-Iceberg-Access-Delegation": "client-managed",
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
            "s3.region": TEST_AWS_REGION,
            "s3.path-style-access": "true",
        },
    )


def create_lakekeeper_server(conn, name, location_prefix=LAKEKEEPER_LOCATION_PREFIX):
    prefix_option = (
        f", location_prefix '{location_prefix}'" if location_prefix is not None else ""
    )
    run_command(
        f"""
        CREATE SERVER {name} TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{LAKEKEEPER_URI}',
                     warehouse '{server_params.LAKEKEEPER_WAREHOUSE}',
                     rest_auth_type 'none'{prefix_option});
        GRANT USAGE ON FOREIGN SERVER {name} TO PUBLIC;
        """,
        conn,
    )
    conn.commit()


def drop_lakekeeper_server(conn, name):
    conn.rollback()
    run_command(f"DROP SERVER IF EXISTS {name} CASCADE", conn)
    conn.commit()


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

_lakekeeper_started = False


# Depends on s3 so moto is up before Lakekeeper validates the warehouse storage.
@pytest.fixture(scope="session", autouse=True)
def start_lakekeeper(postgres, s3, installcheck):
    global _lakekeeper_started
    if installcheck or _lakekeeper_started:
        yield
        return
    start_lakekeeper_server_in_background()
    _lakekeeper_started = True
    yield
    stop_lakekeeper()


@pytest.fixture(scope="module")
def lakekeeper_server(superuser_conn, iceberg_extension, installcheck):
    """A REST server named 'lakekeeper' with the default location prefix."""
    if installcheck:
        yield None
        return
    create_lakekeeper_server(superuser_conn, "lakekeeper")
    yield "lakekeeper"
    drop_lakekeeper_server(superuser_conn, "lakekeeper")


@pytest.fixture(scope="module")
def lakekeeper_catalog(installcheck):
    if installcheck:
        return None
    return create_lakekeeper_catalog()

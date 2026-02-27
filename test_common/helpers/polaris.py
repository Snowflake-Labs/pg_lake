"""Polaris (REST catalog) helper functions and fixtures."""

import atexit
import json
import os
import subprocess

import pytest
import requests
from pathlib import Path
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from . import server_params

# Lazy imports from utils_pytest to avoid circular-import issues.
# These names are resolved at *call* time, not import time.
from utils_pytest import (
    MOTO_PORT,
    TEST_BUCKET,
    TEST_AWS_ACCESS_KEY_ID,
    TEST_AWS_SECRET_ACCESS_KEY,
    AWS_ROLE_ARN,
    run_command_outside_tx,
    stop_process_via_pidfile,
)


# ---------------------------------------------------------------------------
# Polaris server management
# ---------------------------------------------------------------------------


def get_polaris_server_path():

    base_dir = Path(__file__).resolve().parent.parent
    polaris_server_path = base_dir / "rest_catalog/rest_catalog_server.sh"

    return polaris_server_path


def start_polaris_server_in_background():
    # Stop any leftover Polaris from a previous interrupted run.
    stop_polaris()

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

    # Register cleanup before starting so the Java process is stopped even
    # if the startup script or subsequent code throws.  stop_polaris() is
    # safe to call when the PID file does not exist yet.
    atexit.register(stop_polaris)

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


def stop_polaris(timeout=10):
    """Stop the Polaris Java process using its PID file."""
    stop_process_via_pidfile(server_params.POLARIS_PID_FILE, timeout)


# ---------------------------------------------------------------------------
# Iceberg REST catalog (pyiceberg)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Polaris access token / session helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


# Guard against duplicate starts when multiple conftest files import via star.
_polaris_started = False


@pytest.fixture(scope="session", autouse=True)
def start_polaris(postgres):
    global _polaris_started
    if _polaris_started:
        yield
        return
    start_polaris_server_in_background()
    _polaris_started = True
    yield
    stop_polaris()


# DONOT add postgres fixture as dep. that causes circular deps and consumes all shared memory
@pytest.fixture(scope="module")
def polaris_session(installcheck) -> requests.Session:
    """
    Ready‑to‑use requests.Session that attaches the bearer token
    to every call.
    """
    if installcheck:
        return None
    polaris_token = get_polaris_access_token()
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {polaris_token}"})
    return sess


@pytest.fixture(scope="module")
def set_polaris_gucs(
    superuser_conn,
    iceberg_extension,
    installcheck,
    credentials_file: str = server_params.POLARIS_PRINCIPAL_CREDS_FILE,
):
    if not installcheck:

        creds = json.loads(Path(credentials_file).read_text())
        client_id = creds["credentials"]["clientId"]
        client_secret = creds["credentials"]["clientSecret"]

        run_command_outside_tx(
            [
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}'""",
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO '{client_id}'""",
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO '{client_secret}'""",
                "SELECT pg_reload_conf()",
            ],
            superuser_conn,
        )

    yield

    if not installcheck:

        run_command_outside_tx(
            [
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host""",
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id""",
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret""",
                "SELECT pg_reload_conf()",
            ],
            superuser_conn,
        )

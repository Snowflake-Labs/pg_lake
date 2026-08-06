"""End-to-end vended-credentials test against a *real* Iceberg REST catalog.

This is the most realistic test in the pyramid: a real REST catalog vends
real, scoped, short-lived storage credentials (e.g. STS) for data that
lives in real object storage which actually enforces those credentials.
Nothing is mocked.

Because it needs live cloud infrastructure, it is skipped unless the
required environment variables are set -- exactly like
``test_region_switch.py`` skips unless ``CDWREGIONTEST_ACCESS_KEY_ID`` is
present.  Point it at your catalog and run ``make check-e2e``.

--------------------------------------------------------------------------
AWS S3 Tables caveat
--------------------------------------------------------------------------
The intended target for this suite is AWS S3 Tables.  However, the S3
Tables Iceberg REST endpoint authenticates with **AWS SigV4**, while
pg_lake's REST catalog client currently supports only ``oauth2`` /
``horizon`` auth (see ``rest_auth_type`` in
``pg_lake_iceberg/src/rest_catalog/rest_catalog_options.c``).  Until
pg_lake gains SigV4 REST auth, S3 Tables must be fronted by a SigV4-aware
gateway, or you must point this test at an OAuth2 catalog that vends
credentials (Polaris, Lakekeeper, Unity Catalog, Snowflake Open Catalog,
Cloudflare R2 Data Catalog, ...).  The test below is written to be
catalog-agnostic for that reason.

--------------------------------------------------------------------------
Environment variables
--------------------------------------------------------------------------
Required:
  PGLAKE_E2E_REST_ENDPOINT   REST catalog base URL (e.g. https://host/iceberg)
  PGLAKE_E2E_CLIENT_ID       OAuth2 client id
  PGLAKE_E2E_CLIENT_SECRET   OAuth2 client secret
  PGLAKE_E2E_NAMESPACE       existing namespace holding the table
  PGLAKE_E2E_TABLE           existing table name to read

Optional:
  PGLAKE_E2E_OAUTH_ENDPOINT  OAuth2 token endpoint (if not the catalog default)
  PGLAKE_E2E_SCOPE           OAuth2 scope (default: PRINCIPAL_ROLE:ALL)
  PGLAKE_E2E_EXPECTED_COUNT  exact row count to assert (default: assert >= 0)

The AWS credentials pgduck_server uses to *reach* object storage come from
the standard AWS credential chain via the built-in ``s3default`` secret;
the vended credentials returned by the catalog are what actually authorize
the data scan.
"""

import os

import pytest
from utils_pytest import *

_REQUIRED_ENV = (
    "PGLAKE_E2E_REST_ENDPOINT",
    "PGLAKE_E2E_CLIENT_ID",
    "PGLAKE_E2E_CLIENT_SECRET",
    "PGLAKE_E2E_NAMESPACE",
    "PGLAKE_E2E_TABLE",
)

requires_e2e_catalog = pytest.mark.skipif(
    any(not os.getenv(v) for v in _REQUIRED_ENV),
    reason=(
        "vended-credentials e2e needs a live OAuth2 Iceberg REST catalog; set "
        + ", ".join(_REQUIRED_ENV)
    ),
)


@requires_e2e_catalog
def test_vended_credentials_e2e_read(superuser_conn, pgduck_conn, extension):
    """Read a real table whose data access is authorized by vended creds."""
    endpoint = os.environ["PGLAKE_E2E_REST_ENDPOINT"]
    client_id = os.environ["PGLAKE_E2E_CLIENT_ID"]
    client_secret = os.environ["PGLAKE_E2E_CLIENT_SECRET"]
    namespace = os.environ["PGLAKE_E2E_NAMESPACE"]
    table = os.environ["PGLAKE_E2E_TABLE"]
    oauth_endpoint = os.getenv("PGLAKE_E2E_OAUTH_ENDPOINT")
    scope = os.getenv("PGLAKE_E2E_SCOPE", "PRINCIPAL_ROLE:ALL")
    expected_count = os.getenv("PGLAKE_E2E_EXPECTED_COUNT")

    server_name = "pglake_e2e_vended_srv"
    schema = "pglake_e2e_vended"

    options = [
        f"rest_endpoint '{endpoint}'",
        "rest_auth_type 'oauth2'",
        f"scope '{scope}'",
        "enable_vended_credentials 'true'",
    ]
    if oauth_endpoint:
        options.append(f"oauth_endpoint '{oauth_endpoint}'")
    options_sql = ",\n                ".join(options)

    try:
        run_command(f"DROP SERVER IF EXISTS {server_name} CASCADE", superuser_conn)
        run_command(
            f"""
            CREATE SERVER {server_name} TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (
                {options_sql}
                )
            """,
            superuser_conn,
        )
        run_command(
            f"""
            CREATE USER MAPPING FOR CURRENT_USER SERVER {server_name}
                OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
            """,
            superuser_conn,
        )
        run_command(f"CREATE SCHEMA IF NOT EXISTS {schema}", superuser_conn)
        superuser_conn.commit()

        # Attach the existing catalog table read-only.
        run_command(
            f"""CREATE TABLE {schema}.{table} ()
                USING iceberg
                WITH (catalog='{server_name}', read_only=True,
                      catalog_namespace='{namespace}',
                      catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        # The scan is authorized by the catalog-vended credential.
        result = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        count = result[0][0]
        if expected_count is not None:
            assert count == int(
                expected_count
            ), f"expected {expected_count} rows, got {count}"
        else:
            assert count >= 0

        # A scoped vended secret should have been pushed to pgduck_server.
        secrets = run_query(
            "SELECT name, scope FROM duckdb_secrets() WHERE name LIKE 'pglake_vended_%'",
            pgduck_conn,
        )
        assert len(secrets) >= 1, "expected a pglake_vended_* secret after the scan"

    finally:
        run_command(f"DROP TABLE IF EXISTS {schema}.{table}", superuser_conn)
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"DROP SERVER IF EXISTS {server_name} CASCADE", superuser_conn)
        superuser_conn.commit()

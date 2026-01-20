import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def ducklake_server(extension, superuser_conn, app_user):
    """Create pg_lake_ducklake server for DuckLake tests"""
    # Create the pg_lake_ducklake extension which creates the server
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_ducklake CASCADE;
        -- Grant ducklake_catalog role to the test app_user
        GRANT ducklake_catalog TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Cleanup
    run_command(
        """
        DROP EXTENSION IF EXISTS pg_lake_ducklake CASCADE;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

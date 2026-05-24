import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def ducklake_server(extension, superuser_conn, app_user):
    """
    The `extension` fixture has already run CREATE EXTENSION pg_lake_table
    CASCADE which pulls in pg_lake_ducklake. Just grant the test app_user
    membership in ducklake_catalog so it can write to the lake_ducklake
    metadata tables. Don't DROP pg_lake_ducklake on teardown — it would
    cascade-drop pg_lake_table out from under the parent fixture.
    """
    run_command(
        f"GRANT ducklake_catalog TO {app_user};",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

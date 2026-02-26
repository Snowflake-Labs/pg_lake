import pytest
from utils_pytest import *


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

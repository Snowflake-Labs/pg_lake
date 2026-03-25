import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def setup_pg_lake_log(superuser_conn, app_user):
    """
    Ensure the required extensions exist and the app user has write access.
    """
    run_command("CREATE EXTENSION IF NOT EXISTS pg_lake_engine CASCADE;", superuser_conn)
    run_command("GRANT lake_read_write TO {};".format(app_user), superuser_conn)
    superuser_conn.commit()

    yield

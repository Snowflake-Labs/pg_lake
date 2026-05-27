import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def ducklake_server(extension, superuser_conn, app_user):
    """
    The shared `extension` fixture creates pg_lake_table but not
    pg_lake_ducklake (pg_lake_table doesn't depend on it). Install
    pg_lake_ducklake here, then grant app_user membership in
    ducklake_writer so it can write through the public.ducklake_*
    views, plus SELECT on the underlying lake_ducklake.* tables so
    test assertions can inspect catalog state directly. Don't DROP
    pg_lake_ducklake on teardown -- it would cascade-drop pg_lake_table
    out from under the parent fixture.
    """
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_ducklake CASCADE;
        GRANT ducklake_writer TO {app_user};
        GRANT USAGE ON SCHEMA lake_ducklake TO {app_user};
        GRANT SELECT ON ALL TABLES IN SCHEMA lake_ducklake TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def hideable_extension(superuser_conn, extension):
    run_command(
        "CREATE EXTENSION IF NOT EXISTS pg_lake_table_test_hideable CASCADE",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP EXTENSION IF EXISTS pg_lake_table_test_hideable CASCADE", superuser_conn
    )
    superuser_conn.commit()


@pytest.mark.parametrize("hide_enabled", [True, False])
def test_registered_extension_objects_visibility(
    superuser_conn, hideable_extension, hide_enabled
):
    """Objects from an extension that called RegisterHideableExtension()
    should be hidden or visible depending on hide_objects_created_by_lake."""

    guc_value = "true" if hide_enabled else "false"
    run_command(
        f"SET pg_lake_table.hide_objects_created_by_lake TO {guc_value}",
        superuser_conn,
    )

    expected = 0 if hide_enabled else 1

    result = run_query(
        "SELECT count(*) FROM pg_namespace WHERE nspname = 'test_hideable_nsp'",
        superuser_conn,
    )
    assert result[0][0] == expected

    result = run_query(
        """SELECT count(*) FROM pg_proc p
           JOIN pg_namespace n ON p.pronamespace = n.oid
           WHERE n.nspname = 'test_hideable_nsp' AND p.proname = 'test_func'""",
        superuser_conn,
    )
    assert result[0][0] == expected

    superuser_conn.rollback()

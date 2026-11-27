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


@pytest.mark.parametrize("hide_enabled", [True, False])
def test_hidden_objects_on_nullable_side_of_left_join(
    superuser_conn, hideable_extension, hide_enabled
):
    """Hiding a catalog table on the nullable side of a LEFT JOIN should
    null-extend hidden rows, and must not break outer join removal."""

    guc_value = "true" if hide_enabled else "false"
    run_command(
        f"SET pg_lake_table.hide_objects_created_by_lake TO {guc_value}",
        superuser_conn,
    )

    # pg_am is not referenced above the join, so the planner removes it
    result = run_query(
        """SELECT c.relname FROM pg_class c
           LEFT JOIN pg_am am ON c.relam = am.oid
           WHERE c.oid = 'pg_class'::regclass""",
        superuser_conn,
    )
    assert result == [["pg_class"]]

    # pg_depend is not hidden, so its row survives with a hidden namespace
    result = run_query(
        """SELECT n.nspname FROM pg_depend d
           LEFT JOIN pg_namespace n ON d.objid = n.oid
           WHERE d.classid = 'pg_namespace'::regclass
             AND d.objid = 'test_hideable_nsp'::regnamespace""",
        superuser_conn,
    )
    assert result == [[None if hide_enabled else "test_hideable_nsp"]]

    superuser_conn.rollback()

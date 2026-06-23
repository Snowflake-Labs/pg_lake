"""
Role-grant boundary tests for the DuckLake catalog API.

The extension defines three group roles tracking the upstream DuckLake
docs (https://ducklake.select/docs/stable/duckdb/guides/access_control):

  - ducklake_superuser : SELECT/INSERT/UPDATE/DELETE on public.ducklake_*
  - ducklake_writer    : SELECT/INSERT/UPDATE/DELETE on public.ducklake_*
  - ducklake_reader    : SELECT only

GRANTs are placed on the public.ducklake_* views (the catalog API
surface). The underlying lake_ducklake.* tables stay private to the
extension owner; writes go through the views' INSTEAD-OF triggers.
These tests pin that surface so a future change cannot accidentally
GRANT lake_ducklake.* directly or weaken the reader role.
"""

import psycopg2
import pytest

from utils_pytest import (
    TEST_BUCKET,
    server_params,
)  # noqa: F401  (re-exported pytest fixtures rely on it)


PUBLIC_VIEWS_QUERY = (
    "SELECT viewname FROM pg_catalog.pg_views "
    "WHERE schemaname = 'public' AND viewname LIKE 'ducklake_%' "
    "ORDER BY viewname"
)

CATALOG_TABLES_QUERY = (
    "SELECT tablename FROM pg_catalog.pg_tables "
    "WHERE schemaname = 'lake_ducklake' "
    "ORDER BY tablename"
)


def _grant_role_to_app_user(superuser_conn, role, app_user):
    cur = superuser_conn.cursor()
    cur.execute(f"GRANT {role} TO {app_user}")
    superuser_conn.commit()
    cur.close()


def _revoke_role_from_app_user(superuser_conn, role, app_user):
    cur = superuser_conn.cursor()
    cur.execute(f"REVOKE {role} FROM {app_user}")
    superuser_conn.commit()
    cur.close()


def _has_priv(cur, schema_table, priv):
    cur.execute(
        "SELECT pg_catalog.has_table_privilege(%s, %s)",
        (schema_table, priv),
    )
    return cur.fetchone()[0]


def test_ducklake_reader_select_only_on_views(pg_cursor, superuser_conn, app_user):
    """
    A user with ducklake_reader can SELECT every public.ducklake_*
    view. INSERT/UPDATE/DELETE must be denied on every view --
    those slots are reserved for ducklake_writer / ducklake_superuser.
    """
    _grant_role_to_app_user(superuser_conn, "ducklake_reader", app_user)
    try:
        pg_cursor.execute("SET ROLE ducklake_reader")

        pg_cursor.execute(PUBLIC_VIEWS_QUERY)
        views = [r[0] for r in pg_cursor.fetchall()]
        assert views, "expected at least one public.ducklake_* view"

        missing_select = []
        unexpected_write = []
        for view in views:
            qualified = f"public.{view}"
            if not _has_priv(pg_cursor, qualified, "SELECT"):
                missing_select.append(qualified)
            for priv in ("INSERT", "UPDATE", "DELETE"):
                if _has_priv(pg_cursor, qualified, priv):
                    unexpected_write.append((qualified, priv))

        assert not missing_select, f"reader missing SELECT on: {missing_select}"
        assert not unexpected_write, f"reader unexpectedly has: {unexpected_write}"

        pg_cursor.execute("RESET ROLE")
    finally:
        _revoke_role_from_app_user(superuser_conn, "ducklake_reader", app_user)


def test_ducklake_writer_full_dml_on_views(pg_cursor, superuser_conn, app_user):
    """
    ducklake_writer must have SELECT/INSERT/UPDATE/DELETE on every
    public.ducklake_* view -- that's the catalog API surface a DuckDB
    ATTACHment with that role drives writes through. Pinning every
    view (not just a hand-picked subset) guards against a future
    CREATE VIEW landing after the role-grant loop and silently
    losing writer privileges.
    """
    _grant_role_to_app_user(superuser_conn, "ducklake_writer", app_user)
    try:
        pg_cursor.execute("SET ROLE ducklake_writer")

        pg_cursor.execute(PUBLIC_VIEWS_QUERY)
        views = [r[0] for r in pg_cursor.fetchall()]
        assert views

        missing = []
        for view in views:
            qualified = f"public.{view}"
            for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                if not _has_priv(pg_cursor, qualified, priv):
                    missing.append((qualified, priv))
        assert not missing, f"writer missing privileges: {missing}"

        pg_cursor.execute("RESET ROLE")
    finally:
        _revoke_role_from_app_user(superuser_conn, "ducklake_writer", app_user)


def test_lake_ducklake_catalog_private(pg_cursor, superuser_conn, app_user):
    """
    The underlying lake_ducklake.* tables stay private to the extension
    owner -- no DuckLake role gets direct access. All writes flow
    through the public.ducklake_* views' INSTEAD-OF triggers.

    A future patch that adds a GRANT on lake_ducklake.* to one of these
    roles would bypass the trigger validation and is what this test
    catches.
    """
    for role in ("ducklake_reader", "ducklake_writer", "ducklake_superuser"):
        _grant_role_to_app_user(superuser_conn, role, app_user)
        try:
            pg_cursor.execute(f"SET ROLE {role}")

            pg_cursor.execute(CATALOG_TABLES_QUERY)
            tables = [r[0] for r in pg_cursor.fetchall()]
            assert tables, "expected catalog tables in lake_ducklake schema"

            leaked = []
            for tbl in tables:
                qualified = f"lake_ducklake.{tbl}"
                for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                    if _has_priv(pg_cursor, qualified, priv):
                        leaked.append((role, qualified, priv))

            assert not leaked, f"role got direct catalog access: {leaked}"

            pg_cursor.execute("RESET ROLE")
        finally:
            _revoke_role_from_app_user(superuser_conn, role, app_user)


def test_schema_revive_preserves_schema_id(pg_cursor, s3):
    """
    pg_lake_ducklake's object-access hook end-snapshots
    lake_ducklake.schema rows on PG DROP SCHEMA, then revives them
    when a schema is re-CREATEd with the same name -- carrying over
    the original schema_id and schema_uuid (catalog.c:2480-2584).

    The lake_ducklake.schema row is lazy-created at first ducklake-
    table registration in the schema (DucklakeRegisterTable,
    catalog.c:648), so the test creates a table to materialise the
    row before dropping.
    """
    pg_cursor.execute("DROP SCHEMA IF EXISTS revive_probe CASCADE")
    pg_cursor.connection.commit()

    pg_cursor.execute("CREATE SCHEMA revive_probe")
    pg_cursor.execute(
        f"""
        CREATE TABLE revive_probe.t (id INT, val TEXT)
            USING ducklake WITH (location = 's3://{TEST_BUCKET}/revive_probe')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id, schema_uuid FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_probe' AND end_snapshot IS NULL"
    )
    initial = pg_cursor.fetchone()
    assert (
        initial is not None
    ), "lake_ducklake.schema row should exist after CREATE TABLE"
    initial_id, initial_uuid = initial

    pg_cursor.execute("DROP SCHEMA revive_probe CASCADE")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_probe' AND end_snapshot IS NULL"
    )
    assert pg_cursor.fetchone()[0] == 0, "DROP SCHEMA must end-snapshot the row"

    pg_cursor.execute("CREATE SCHEMA revive_probe")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id, schema_uuid FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_probe' AND end_snapshot IS NULL"
    )
    revived = pg_cursor.fetchone()
    assert revived is not None, "re-CREATE must restore a live schema row"
    assert (
        revived[0] == initial_id
    ), f"schema_id must be reused on revive: was {initial_id}, got {revived[0]}"
    assert (
        revived[1] == initial_uuid
    ), f"schema_uuid must be reused on revive: was {initial_uuid}, got {revived[1]}"

    pg_cursor.execute("DROP SCHEMA revive_probe CASCADE")
    pg_cursor.connection.commit()

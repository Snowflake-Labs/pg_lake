"""
Pytest configuration and fixtures for pg_lake_ducklake tests.

Brings in the shared test_common framework so `make check` can start
postgres + pgduck_server + moto S3 by itself, and exposes the legacy
pg_cursor fixture the existing tests in this directory rely on.
"""

import pytest
import psycopg2
from utils_pytest import *
from helpers import server_params


@pytest.fixture(scope="module", autouse=True)
def ducklake_extension(extension, superuser_conn, app_user):
    """Ensure pg_lake_ducklake is created and the test app_user can read its schema."""
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_ducklake CASCADE;
        GRANT USAGE ON SCHEMA lake_ducklake TO {app_user};
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lake_ducklake TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield


@pytest.fixture(scope="session")
def _legacy_session_pg_conn(postgres):
    """Plain superuser connection used by the legacy pg_cursor fixture.

    Depends on the `postgres` session fixture so the test_common framework
    starts postgres + pgduck_server before we try to connect.
    """
    conn = psycopg2.connect(
        dbname=server_params.PG_DATABASE,
        user=server_params.PG_USER,
        host=server_params.PG_HOST,
        port=server_params.PG_PORT,
    )
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture
def pg_cursor(_legacy_session_pg_conn, ducklake_extension):
    """Per-test cursor matching the legacy interface used by these tests."""
    cursor = _legacy_session_pg_conn.cursor()
    yield cursor
    _legacy_session_pg_conn.rollback()
    cursor.close()


@pytest.fixture(autouse=True)
def cleanup_test_tables(pg_cursor):
    """
    Clean up test tables and DuckLake metadata after each test so the
    next test starts from snapshot 0 with id 1.
    """
    yield

    try:
        pg_cursor.connection.rollback()
    except Exception:
        pass

    try:
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.snapshot_changes WHERE snapshot_id > 0"
        )
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.file_column_stats WHERE data_file_id > 0"
        )
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.file_partition_value WHERE data_file_id > 0"
        )
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.delete_file WHERE delete_file_id > 0"
        )
        pg_cursor.execute("DELETE FROM lake_ducklake.data_file WHERE data_file_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.partition_column")
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.partition_info WHERE partition_id > 0"
        )
        pg_cursor.execute("DELETE FROM lake_ducklake.table_column_stats")
        pg_cursor.execute("DELETE FROM lake_ducklake.table_stats")
        pg_cursor.execute("DELETE FROM lake_ducklake.column_tag")
        pg_cursor.execute("DELETE FROM lake_ducklake.name_mapping WHERE mapping_id > 0")
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.column_mapping WHERE mapping_id > 0"
        )
        pg_cursor.execute("DELETE FROM lake_ducklake.column WHERE column_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.schema_versions")
        pg_cursor.execute("DELETE FROM lake_ducklake.table WHERE table_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.schema WHERE schema_id > 0")
        pg_cursor.execute("DELETE FROM lake_ducklake.snapshot WHERE snapshot_id > 0")
        pg_cursor.execute(
            "DELETE FROM lake_ducklake.metadata "
            "WHERE key NOT IN ('ducklake_version', 'data_inlining_row_limit')"
        )
        pg_cursor.execute(
            "UPDATE lake_ducklake.snapshot SET next_catalog_id = 1, next_file_id = 1 "
            "WHERE snapshot_id = 0"
        )
        pg_cursor.connection.commit()
        # Reset GUCs after the metadata cleanup commits — RESET is a
        # session-state change but doesn't need to be in the same xact.
        try:
            pg_cursor.execute("RESET pg_lake_ducklake.default_location_prefix")
            pg_cursor.connection.commit()
        except Exception:
            pg_cursor.connection.rollback()
    except Exception as e:
        print(f"Warning: Could not clean metadata: {e}")
        pg_cursor.connection.rollback()

    # Drop any foreign / regular test tables we created in public.
    for ddl in [
        """
        SELECT foreign_table_name FROM information_schema.foreign_tables
        WHERE foreign_table_schema = 'public'
          AND (foreign_table_name LIKE '%_test%'
               OR foreign_table_name LIKE 'test_%'
               OR foreign_table_name LIKE 'rt_%'
               OR foreign_table_name LIKE 'foo_%')
        """,
        """
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
          AND (tablename LIKE '%_test%'
               OR tablename LIKE 'test_%'
               OR tablename LIKE 'rt_%'
               OR tablename LIKE 'foo_%')
        """,
    ]:
        try:
            pg_cursor.execute(ddl)
            for row in pg_cursor.fetchall():
                try:
                    pg_cursor.execute(f"DROP TABLE IF EXISTS {row[0]} CASCADE")
                except Exception as e:
                    print(f"Warning: Could not drop {row[0]}: {e}")
            pg_cursor.connection.commit()
        except Exception as e:
            print(f"Warning: cleanup query failed: {e}")
            pg_cursor.connection.rollback()

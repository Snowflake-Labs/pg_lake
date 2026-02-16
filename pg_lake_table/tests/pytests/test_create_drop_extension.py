import pytest
from utils_pytest import *


def test_create_drop_query_engine(superuser_conn, s3, app_user):
    other_conn = open_pg_conn()

    run_command(
        """
        CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
    """,
        superuser_conn,
    )

    create_table_with_data(superuser_conn)

    superuser_conn.commit()

    # Do a query in the same connection (trigger caching)
    result = run_query("SELECT count(*) FROM test", superuser_conn)
    assert result[0]["count"] == 10

    # Do a query in another connection (trigger caching)
    result = run_query("SELECT count(*) FROM test", other_conn)
    assert result[0]["count"] == 10

    # Release locks on other connection
    other_conn.commit()

    run_command(
        f"""
        DROP EXTENSION pg_lake_engine CASCADE;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        CREATE EXTENSION pg_lake_table CASCADE;
        GRANT lake_read_write TO {app_user};
    """,
        superuser_conn,
    )

    create_table_with_data(superuser_conn)

    superuser_conn.commit()

    # Should be able to run queries after dropping and recreating the extension
    result = run_query("SELECT count(*) FROM test", superuser_conn)
    assert result[0]["count"] == 10

    # Also in the other connection
    result = run_query("SELECT count(*) FROM test", other_conn)
    assert result[0]["count"] == 10

    # Release locks on other connection
    other_conn.commit()

    run_command("DROP FOREIGN TABLE test", superuser_conn)
    superuser_conn.commit()


# Make sure we can create extension with default_table_access_method iceberg
def test_create_extension_table_access_method(superuser_conn):
    run_command(
        """
        -- Can only set default_table_access_method to Iceberg if extension exists
        CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
        SET default_table_access_method TO 'iceberg';

        -- Recreating might fail if we try to create metadata tables as Iceberg
        DROP EXTENSION pg_lake_table;
        CREATE EXTENSION pg_lake_table CASCADE;
    """,
        superuser_conn,
    )

    superuser_conn.rollback()


def test_drop_create_extension_insert_same_tx(superuser_conn, s3, app_user):
    """
    DROP + CREATE EXTENSION + CREATE TABLE + INSERT all in the same transaction.

    This exercises InProgressTableVisibleToExternalTx(): after DROP + CREATE
    EXTENSION, the in_progress_files table is recreated in the current tx.
    The visibility check must detect that the table was created in the current
    transaction (via rd_createSubid) and skip the run_attached insert to avoid
    a self-deadlock on the AccessExclusiveLock held by the DDL.
    """
    run_command(
        """
        CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    url = f"s3://{TEST_BUCKET}/test_drop_create_ext_same_tx/"

    run_command(
        f"""
        DROP EXTENSION pg_lake_engine CASCADE;
        CREATE EXTENSION pg_lake_table CASCADE;
        GRANT lake_read_write TO {app_user};

        SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}';

        CREATE TABLE test_same_tx (x int) USING iceberg;
        INSERT INTO test_same_tx VALUES (1), (2), (3);
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query("SELECT count(*) FROM test_same_tx", superuser_conn)
    assert result[0]["count"] == 3

    result = run_query("SELECT * FROM test_same_tx ORDER BY x", superuser_conn)
    assert [r["x"] for r in result] == [1, 2, 3]

    # second transaction should work normally (visibility cached as true)
    run_command("INSERT INTO test_same_tx VALUES (4)", superuser_conn)
    superuser_conn.commit()

    result = run_query("SELECT count(*) FROM test_same_tx", superuser_conn)
    assert result[0]["count"] == 4

    run_command("DROP TABLE test_same_tx", superuser_conn)
    superuser_conn.commit()


def create_table_with_data(superuser_conn):
    url = f"s3://{TEST_BUCKET}/test_create_drop_extension/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test (x int, y int)
        SERVER pg_lake
        OPTIONS (writable 'true', location '{url}', format 'parquet');

        INSERT INTO test SELECT s, s FROM generate_series(1,10) s;
    """,
        superuser_conn,
    )

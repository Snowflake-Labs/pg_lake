"""
Regression test for SPI_START_EXTENSION_OWNER_ALLOW_TEMP.

PgLakeAddDataFileHook is an extension point used by sfpg-extension-pg_lake_replication
(and any other module that needs to know which data file IDs were added in
the current transaction). When the hook is set and returns true, pg_lake_table
records each new data file ID in a per-transaction temp table created lazily
inside SPI_START_EXTENSION_OWNER.

Before SPI_START_EXTENSION_OWNER_ALLOW_TEMP existed, the temp-table create
ran under SECURITY_RESTRICTED_OPERATION, which PostgreSQL forbids: the temp
namespace cannot be safely established within a restricted context, so the
INSERT failed with "cannot create temporary table within security-restricted
operation".

This test loads pg_lake_table_test_data_file_hook (a sample extension that
sets the hook to always-true) and runs an INSERT into an Iceberg table. If
the temp-table site regresses to the restricted helper, the INSERT fails
with the above error.
"""

import psycopg2
import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def with_data_file_hook(superuser_conn):
    """Load the test extension that registers PgLakeAddDataFileHook."""
    run_command(
        "CREATE EXTENSION IF NOT EXISTS pg_lake_table_test_data_file_hook CASCADE",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP EXTENSION IF EXISTS pg_lake_table_test_data_file_hook",
        superuser_conn,
    )
    superuser_conn.commit()


def test_iceberg_insert_with_data_file_hook(
    s3, pg_conn, extension, with_default_location, with_data_file_hook
):
    """An INSERT into an Iceberg table with PgLakeAddDataFileHook active
    must successfully create and populate the per-transaction temp table.
    """
    run_command(
        "CREATE TABLE test_data_file_hook_temp_table(a int, b text) USING iceberg",
        pg_conn,
    )

    # The INSERT triggers AddDataFileToCatalog -> InsertDataFileIdIntoTransactionTable
    # -> CreateTxDataFileIdsTempTableIfNotExists, which is the
    # SPI_START_EXTENSION_OWNER_ALLOW_TEMP call site under test.
    run_command(
        "INSERT INTO test_data_file_hook_temp_table VALUES (1, 'one'), (2, 'two')",
        pg_conn,
    )

    res = run_query(
        "SELECT a, b FROM test_data_file_hook_temp_table ORDER BY a",
        pg_conn,
    )
    assert res == [[1, "one"], [2, "two"]]

    # Run a second INSERT in a fresh transaction to exercise the
    # "if not exists" branch as well.
    run_command(
        "INSERT INTO test_data_file_hook_temp_table VALUES (3, 'three')",
        pg_conn,
    )

    res = run_query(
        "SELECT count(*) FROM test_data_file_hook_temp_table",
        pg_conn,
    )
    assert res == [[3]]

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


@pytest.mark.parametrize(
    "dml",
    [
        "DELETE FROM public.test_iceberg_dml_under_owner WHERE a OPERATOR(pg_catalog.>=) 2",
        "UPDATE public.test_iceberg_dml_under_owner "
        "SET b = b OPERATOR(pg_catalog.||) '!' "
        "WHERE a OPERATOR(pg_catalog.>=) 2",
    ],
    ids=["delete", "update"],
)
def test_iceberg_dml_under_extension_owner(
    s3,
    superuser_conn,
    pg_conn,
    extension,
    with_default_location,
    with_data_file_hook,
    dml,
):
    """UPDATE/DELETE on an Iceberg foreign table issued from inside
    SPI_START_EXTENSION_OWNER must succeed.

    pg_lake_table's BeginForeignModify creates a per-statement temp tracking
    table for both UPDATE and DELETE via CreateUpdateTrackingTable, which
    calls DefineRelation + DefineIndex directly.  Under the lockdown added
    in the parent commit those would fail with "cannot create temporary
    table within security-restricted operation" without the narrow
    ALLOW_TEMP_OBJECTS_BEGIN/END scope around the create.

    Downstream extensions that wrap their own SPI calls in
    SPI_START_EXTENSION_OWNER and then issue Iceberg DML hit this -- the
    motivating example was sfpg-extension-pg_lake_replication's expiry path
    running DELETE on the change-log Iceberg table.
    """
    run_command(
        "CREATE TABLE test_iceberg_dml_under_owner(a int, b text) USING iceberg",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_iceberg_dml_under_owner VALUES "
        "(1, 'one'), (2, 'two'), (3, 'three')",
        pg_conn,
    )
    pg_conn.commit()

    # Run the DML under the extension-owner lockdown.
    run_command(
        f"SELECT run_iceberg_dml_under_extension_owner({_quote_literal(dml)})",
        superuser_conn,
    )
    superuser_conn.commit()

    if dml.startswith("DELETE"):
        res = run_query(
            "SELECT count(*) FROM test_iceberg_dml_under_owner",
            pg_conn,
        )
        assert res == [[1]]
    else:
        res = run_query(
            "SELECT a, b FROM test_iceberg_dml_under_owner ORDER BY a",
            pg_conn,
        )
        assert res == [[1, "one"], [2, "two!"], [3, "three!"]]


def _quote_literal(s: str) -> str:
    """Quote a string for embedding in a SQL statement as a literal."""
    return "'" + s.replace("'", "''") + "'"

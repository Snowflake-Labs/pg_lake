import pytest
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import LongType

from utils_pytest import *


TABLE_NAME = "test_ext_write"
TABLE_NAMESPACE = "public"


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, s3):
    """
    Create a PyIceberg SqlCatalog whose catalog_name matches the current
    database name.  This way, PyIceberg commits (append, overwrite,
    schema evolution) go through the iceberg_tables INSTEAD OF trigger's
    internal-catalog path and automatically sync the pg_lake catalog.
    """
    catalog_user = "iceberg_ext_writer"

    result = run_query(
        f"SELECT 1 FROM pg_roles WHERE rolname='{catalog_user}'", superuser_conn
    )
    if len(result) == 0:
        run_command(f"CREATE USER {catalog_user}", superuser_conn)

    run_command(f"GRANT iceberg_catalog TO {catalog_user}", superuser_conn)
    superuser_conn.commit()

    db_name = run_query("SELECT current_database()", superuser_conn)[0][0]

    catalog = SqlCatalog(
        db_name,
        **{
            "uri": f"postgresql+psycopg2://{catalog_user}@localhost:{server_params.PG_PORT}/{server_params.PG_DATABASE}",
            "warehouse": f"s3://{TEST_BUCKET}/iceberg/",
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
        },
    )

    try:
        catalog.create_namespace(TABLE_NAMESPACE)
    except Exception:
        pass  # namespace may already exist

    yield catalog
    catalog.engine.dispose()


@pytest.fixture(scope="function")
def grant_iceberg_tables_access(extension, app_user, superuser_conn):
    """Grant the app_user UPDATE on iceberg_tables for manual UPDATE tests."""
    run_command(
        f"""
        GRANT SELECT ON lake_iceberg.tables_internal TO {app_user};
        GRANT UPDATE ON pg_catalog.iceberg_tables TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield
    run_command(
        f"""
        REVOKE SELECT ON lake_iceberg.tables_internal FROM {app_user};
        REVOKE UPDATE ON pg_catalog.iceberg_tables FROM {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_external_write_basic(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Create an Iceberg table via pg_lake, insert data, then append data
    via PyIceberg.  PyIceberg's commit automatically updates the
    iceberg_tables view, triggering the sync of internal catalog state.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_basic"

    # create and populate via pg_lake
    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,5) i",
        pg_conn,
    )
    pg_conn.commit()

    # load the table via PyIceberg and append rows
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_basic"
    )

    new_data = pa.table(
        {"a": [6, 7, 8], "b": ["ext6", "ext7", "ext8"]},
        schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
    )
    pyiceberg_table.append(new_data)

    # verify data: should see all 8 rows
    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 8

    result = run_query(f"SELECT a, b FROM {tbl} ORDER BY a", pg_conn)
    assert result == [
        [1, "row1"],
        [2, "row2"],
        [3, "row3"],
        [4, "row4"],
        [5, "row5"],
        [6, "ext6"],
        [7, "ext7"],
        [8, "ext8"],
    ]

    pg_conn.rollback()


def test_external_write_schema_add_column(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    External writer adds a new column to the Iceberg schema and writes
    data with it.  After the PyIceberg commits, the foreign table should
    gain the new column.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_add_col"

    # create and populate via pg_lake
    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,3) i",
        pg_conn,
    )
    pg_conn.commit()

    # evolve schema via PyIceberg: add column c (long)
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_add_col"
    )

    with pyiceberg_table.update_schema() as update:
        update.add_column("c", LongType())

    # write data with the new column
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_add_col"
    )

    new_data = pa.table(
        {"a": [4], "b": ["ext4"], "c": [100]},
        schema=pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
                pa.field("c", pa.int64()),
            ]
        ),
    )
    pyiceberg_table.append(new_data)

    # verify column c exists and data is correct
    result = run_query(f"SELECT a, b, c FROM {tbl} ORDER BY a", pg_conn)
    assert len(result) == 4
    # old rows have NULL for column c
    assert result[0] == [1, "row1", None]
    assert result[1] == [2, "row2", None]
    assert result[2] == [3, "row3", None]
    assert result[3] == [4, "ext4", 100]

    pg_conn.rollback()


def test_external_write_schema_drop_column(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    External writer drops a column from the Iceberg schema.
    After the PyIceberg commit, the foreign table should lose that column.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_drop_col"

    # create with 3 columns
    run_command(
        f"CREATE TABLE {tbl} (a int, b text, c int) USING iceberg",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i, i * 10 FROM generate_series(1,3) i",
        pg_conn,
    )
    pg_conn.commit()

    # drop column c via PyIceberg schema evolution
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_drop_col"
    )

    with pyiceberg_table.update_schema() as update:
        update.delete_column("c")

    # verify column c is gone; only a and b remain
    result = run_query(f"SELECT a, b FROM {tbl} ORDER BY a", pg_conn)
    assert len(result) == 3
    assert result[0] == [1, "row1"]

    # column c should not be queryable
    error_raised = False
    try:
        run_query(f"SELECT c FROM {tbl}", pg_conn)
    except Exception:
        error_raised = True
        pg_conn.rollback()

    assert error_raised

    pg_conn.rollback()


def test_external_write_optimistic_concurrency_failure(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_access,
):
    """
    Attempt to UPDATE iceberg_tables with a wrong previous_metadata_location.
    Should fail with a concurrency error.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_concurrency"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    error_raised = False
    try:
        run_command(
            f"""
            UPDATE iceberg_tables
            SET metadata_location = 's3://fake/path/v2.metadata.json',
                previous_metadata_location = 's3://wrong/prev/v1.metadata.json'
            WHERE table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_concurrency'
            """,
            pg_conn,
        )
    except Exception as e:
        error_raised = True
        assert "metadata_location has been modified concurrently" in str(e)
        pg_conn.rollback()

    assert error_raised

    pg_conn.rollback()


def test_external_write_null_previous_metadata_location(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_access,
):
    """
    Attempt to UPDATE iceberg_tables without previous_metadata_location.
    Should fail because it is required for concurrency control.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_null_prev"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    error_raised = False
    try:
        run_command(
            f"""
            UPDATE iceberg_tables
            SET metadata_location = 's3://fake/path/v2.metadata.json'
            WHERE table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_null_prev'
            """,
            pg_conn,
        )
    except Exception as e:
        error_raised = True
        assert "previous_metadata_location" in str(e)
        pg_conn.rollback()

    assert error_raised

    pg_conn.rollback()


def test_external_write_empty_table(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    External write that overwrites a table with empty data via PyIceberg.
    The internal catalog should be cleared of data files.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_empty"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1), (2), (3)", pg_conn)
    pg_conn.commit()

    # verify we have data
    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 3

    # use PyIceberg to overwrite with empty data
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_empty"
    )
    pyiceberg_table.overwrite(
        pa.table({"a": pa.array([], type=pa.int32())}),
    )

    # table should now return 0 rows
    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 0

    pg_conn.rollback()

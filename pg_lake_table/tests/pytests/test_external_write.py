import threading
from decimal import Decimal

import pytest
import pyarrow as pa
import pyarrow.parquet
import pyiceberg.io.pyarrow
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StructType

from utils_pytest import *


TABLE_NAME = "test_ext_write"
TABLE_NAMESPACE = "public"


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, app_user, s3):
    """
    Create a PyIceberg SqlCatalog whose catalog_name matches the current
    database name.  This way, PyIceberg commits (append, overwrite,
    schema evolution) go through the iceberg_tables INSTEAD OF trigger's
    internal-catalog path and automatically sync the pg_lake catalog.

    External writes require table ownership, so the catalog user is a
    member of app_user, which creates the tables.
    """
    catalog_user = "iceberg_ext_writer"

    result = run_query(
        f"SELECT 1 FROM pg_roles WHERE rolname='{catalog_user}'", superuser_conn
    )
    if len(result) == 0:
        run_command(f"CREATE USER {catalog_user}", superuser_conn)

    run_command(f"GRANT iceberg_catalog TO {catalog_user}", superuser_conn)
    run_command(f"GRANT {app_user} TO {catalog_user}", superuser_conn)
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


@pytest.fixture(scope="function")
def grant_iceberg_tables_full_access(extension, app_user, superuser_conn):
    """Grant the app_user INSERT/UPDATE/DELETE on iceberg_tables for trigger tests."""
    run_command(
        f"""
        GRANT SELECT ON lake_iceberg.tables_internal TO {app_user};
        GRANT INSERT, UPDATE, DELETE ON pg_catalog.iceberg_tables TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield
    run_command(
        f"""
        REVOKE SELECT ON lake_iceberg.tables_internal FROM {app_user};
        REVOKE INSERT, UPDATE, DELETE ON pg_catalog.iceberg_tables FROM {app_user};
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


def test_external_write_metadata_location_outside_table_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_access,
):
    """
    A metadata_location outside the table's own metadata directory is
    rejected, even when previous_metadata_location is correct.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_outside"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    current = run_query(
        f"""SELECT metadata_location FROM iceberg_tables
            WHERE table_name = '{TABLE_NAME}_outside'""",
        pg_conn,
    )[0][0]

    for bad_location in [
        "s3://otherbucket/other/metadata/v2.metadata.json",
        current.rsplit("/metadata/", 1)[0] + "/metadata/../x/v2.metadata.json",
        "file:///etc/passwd",
    ]:
        error = run_command(
            f"""
            UPDATE iceberg_tables
            SET metadata_location = '{bad_location}',
                previous_metadata_location = '{current}'
            WHERE table_name = '{TABLE_NAME}_outside'
            """,
            pg_conn,
            raise_error=False,
        )
        assert "metadata directory" in str(error)
        pg_conn.rollback()

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


def test_concurrent_external_writes_second_writer_fails(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Two external writes to the same table. The second sees a stale
    previous_metadata_location and gets a serialization error.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_race"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{TABLE_NAME}_race")

    # capture the current metadata_location before any external write
    stale_prev = run_query(
        f"""
        SELECT metadata_location FROM iceberg_tables
        WHERE table_namespace = '{TABLE_NAMESPACE}'
          AND table_name = '{TABLE_NAME}_race'
        """,
        pg_conn,
    )[0][0]
    pg_conn.rollback()

    # first external write succeeds (advances metadata_location)
    data1 = pa.table(
        {"a": [10]},
        schema=pa.schema([pa.field("a", pa.int32())]),
    )
    pyiceberg_table.append(data1)

    # second writer uses stale previous_metadata_location — should fail
    ext_conn = open_pg_conn(server_params.PG_USER)
    ext_conn.set_isolation_level(0)

    run_command(
        f"""
        GRANT SELECT ON lake_iceberg.tables_internal TO {server_params.PG_USER};
        GRANT UPDATE ON pg_catalog.iceberg_tables TO {server_params.PG_USER};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    error_raised = False
    try:
        run_command(
            f"""
            UPDATE iceberg_tables
            SET metadata_location = 's3://fake/stale-write.metadata.json',
                previous_metadata_location = '{stale_prev}'
            WHERE table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_race'
            """,
            ext_conn,
        )
    except Exception as e:
        error_raised = True
        assert "metadata_location has been modified concurrently" in str(e)
        ext_conn.rollback()

    assert error_raised
    ext_conn.close()
    pg_conn.rollback()


def test_concurrent_pglake_insert_blocks_external_write(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_access,
):
    """
    A pg_lake INSERT holds the catalog row lock from DML time through
    commit. An external write UPDATE blocks until the INSERT commits,
    then fails because the metadata changed.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_lock"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    pg_conn.commit()

    prev_meta = run_query(
        f"""
        SELECT metadata_location FROM iceberg_tables
        WHERE table_namespace = '{TABLE_NAMESPACE}'
          AND table_name = '{TABLE_NAME}_lock'
        """,
        pg_conn,
    )[0][0]
    pg_conn.rollback()

    # session 1: begin a pg_lake INSERT (acquires catalog row lock)
    run_command("BEGIN", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (42)", pg_conn)

    # session 2: try an external write UPDATE — should block then fail
    ext_conn = open_pg_conn(server_params.PG_USER)
    ext_conn.set_isolation_level(0)
    run_command("SET statement_timeout = '3s'", ext_conn)

    error_raised = False
    try:
        run_command(
            f"""
            UPDATE iceberg_tables
            SET metadata_location = 's3://fake/blocked.metadata.json',
                previous_metadata_location = '{prev_meta}'
            WHERE table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_lock'
            """,
            ext_conn,
        )
    except Exception as e:
        error_raised = True
        # either statement_timeout (blocked on lock) or serialization failure
        assert "timeout" in str(e).lower() or "concurrently" in str(e).lower()
        ext_conn.rollback()

    assert error_raised

    pg_conn.commit()

    result = run_query(f"SELECT a FROM {tbl}", pg_conn)
    assert result == [[42]]

    ext_conn.close()
    pg_conn.rollback()


def test_concurrent_read_during_external_write_sees_consistent_data(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    A SELECT under REPEATABLE READ during an external write sync sees
    either all old data or all new data, never a partial state from the
    delete-then-reinsert cycle in SyncDataFilesFromMetadata.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_read_consistency"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i FROM generate_series(1,100) i",
        pg_conn,
    )
    pg_conn.commit()

    # reader holds a REPEATABLE READ snapshot
    reader_conn = open_pg_conn(server_params.PG_USER)
    reader_conn.set_isolation_level(2)  # REPEATABLE READ
    run_command("BEGIN", reader_conn)
    count_before = run_query(f"SELECT count(*) FROM {tbl}", reader_conn)[0][0]
    assert count_before == 100

    # external write appends rows (sync clears and repopulates catalog)
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_read_consistency"
    )
    new_data = pa.table(
        {"a": list(range(101, 151))},
        schema=pa.schema([pa.field("a", pa.int32())]),
    )
    pyiceberg_table.append(new_data)

    # reader still sees old count (MVCC snapshot isolation)
    count_during = run_query(f"SELECT count(*) FROM {tbl}", reader_conn)[0][0]
    assert count_during == 100

    reader_conn.commit()

    # new read sees all 150 rows
    count_after = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)[0][0]
    assert count_after == 150

    reader_conn.close()
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


def test_external_write_deletion_queue_on_overwrite(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    PyIceberg overwrite() retains the previous snapshot by default, so
    the old data files are still referenced via that snapshot. The sync
    must NOT queue them for deletion — doing so would break time-travel
    reads from external clients. (Snapshot expiration eventually orphans
    them, but that's a separate path not exercised here; pyiceberg has
    no Python API for expiration as of this writing.)
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_overwrite"

    # create and insert initial data
    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,5) i",
        pg_conn,
    )
    pg_conn.commit()

    # get the old data file paths before overwrite
    old_files = run_query(
        f"""
        SELECT f.path
        FROM lake_table.files f
        JOIN pg_class c ON c.oid = f.table_name
        WHERE c.relname = '{TABLE_NAME}_del_overwrite'
        ORDER BY f.path
        """,
        superuser_conn,
    )
    assert len(old_files) > 0, "Should have at least one data file before overwrite"
    old_file_paths = [row[0] for row in old_files]

    # use PyIceberg to overwrite with new data
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_overwrite"
    )
    new_data = pa.table(
        {"a": [10, 20], "b": ["new1", "new2"]},
        schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
    )
    pyiceberg_table.overwrite(new_data)

    # verify the new data is visible
    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 2

    # the old data files are still referenced by the retained pre-overwrite
    # snapshot, so they must NOT be in the deletion queue.
    queued_files = run_query(
        f"""
        SELECT dq.path
        FROM lake_engine.deletion_queue dq
        JOIN pg_class c ON c.oid = dq.table_name
        WHERE c.relname = '{TABLE_NAME}_del_overwrite'
        """,
        superuser_conn,
    )
    queued_file_paths = [row[0] for row in queued_files]

    for old_path in old_file_paths:
        assert (
            old_path not in queued_file_paths
        ), f"Old file {old_path} is still referenced by a retained snapshot and must not be queued"

    pg_conn.rollback()


def test_external_write_deletion_queue_on_empty(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Overwriting with empty data via PyIceberg still retains the previous
    snapshot (which references the original data files), so those files
    must NOT be queued for deletion despite the table being logically
    empty. They become orphaned only after the retaining snapshot is
    expired.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_empty"

    # create and insert initial data
    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1), (2), (3), (4), (5)", pg_conn)
    pg_conn.commit()

    # get the old data file paths
    old_files = run_query(
        f"""
        SELECT f.path
        FROM lake_table.files f
        JOIN pg_class c ON c.oid = f.table_name
        WHERE c.relname = '{TABLE_NAME}_del_empty'
        ORDER BY f.path
        """,
        superuser_conn,
    )
    old_file_count = len(old_files)
    assert old_file_count > 0, "Should have at least one data file"
    old_file_paths = [row[0] for row in old_files]

    # use PyIceberg to overwrite with empty data
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_empty"
    )
    pyiceberg_table.overwrite(
        pa.table({"a": pa.array([], type=pa.int32())}),
    )

    # verify table is empty
    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 0

    # the original files are still referenced by the retained pre-overwrite
    # snapshot, so they must NOT be in the deletion queue.
    queued_files = run_query(
        f"""
        SELECT dq.path
        FROM lake_engine.deletion_queue dq
        JOIN pg_class c ON c.oid = dq.table_name
        WHERE c.relname = '{TABLE_NAME}_del_empty'
        """,
        superuser_conn,
    )
    queued_file_paths = [row[0] for row in queued_files]

    for old_path in old_file_paths:
        assert (
            old_path not in queued_file_paths
        ), f"Old file {old_path} is still referenced by a retained snapshot and must not be queued"

    pg_conn.rollback()


def test_external_write_deletion_queue_only_old_files(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Across an append + overwrite sequence, the deletion queue must stay
    empty: every file remains referenced by at least one retained snapshot
    (the pre-append snapshot for the originals, the post-append snapshot
    for the appended ones, the post-overwrite snapshot for the new ones).
    Nothing is orphaned until snapshot expiration runs, which happens
    outside the sync path.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_selective"

    # create and insert initial data
    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1), (2), (3)", pg_conn)
    pg_conn.commit()

    # append more data via PyIceberg (creates a second snapshot)
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_selective"
    )
    pyiceberg_table.append(
        pa.table(
            {"a": [4, 5, 6]},
            schema=pa.schema([pa.field("a", pa.int32())]),
        )
    )

    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 6

    queued_after_append = run_query(
        f"""
        SELECT dq.path
        FROM lake_engine.deletion_queue dq
        JOIN pg_class c ON c.oid = dq.table_name
        WHERE c.relname = '{TABLE_NAME}_del_selective'
        """,
        superuser_conn,
    )
    assert (
        len(queued_after_append) == 0
    ), "After append, no files should be queued for deletion"

    # now overwrite (creates a third snapshot; the first two are still retained)
    pyiceberg_table.overwrite(
        pa.table(
            {"a": [100, 200]},
            schema=pa.schema([pa.field("a", pa.int32())]),
        )
    )

    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 2

    queued_after_overwrite = run_query(
        f"""
        SELECT dq.path
        FROM lake_engine.deletion_queue dq
        JOIN pg_class c ON c.oid = dq.table_name
        WHERE c.relname = '{TABLE_NAME}_del_selective'
        """,
        superuser_conn,
    )
    assert (
        len(queued_after_overwrite) == 0
    ), "After overwrite, files retained by older snapshots must not be queued"

    pg_conn.rollback()


def test_external_write_preserves_history_files(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    After an external append, the data files from the previous (now
    historic) snapshot must NOT be queued for deletion: they are still
    referenced by retained snapshots and external clients may rely on
    them for time-travel reads.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_history"

    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,3) i",
        pg_conn,
    )
    pg_conn.commit()

    # snapshot the file paths before the external append
    historic_files = run_query(
        f"""
        SELECT f.path
        FROM lake_table.files f
        JOIN pg_class c ON c.oid = f.table_name
        WHERE c.relname = '{TABLE_NAME}_history'
        ORDER BY f.path
        """,
        superuser_conn,
    )
    historic_paths = [row[0] for row in historic_files]
    assert len(historic_paths) > 0

    # external client appends. Old snapshot is retained by default; new
    # snapshot is what pg_lake reads from.
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_history"
    )
    pyiceberg_table.append(
        pa.table(
            {"a": [4, 5], "b": ["ext4", "ext5"]},
            schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
        )
    )

    # historic files must NOT be in the deletion queue — they remain
    # referenced by the retained pre-append snapshot.
    queued = run_query(
        f"""
        SELECT dq.path
        FROM lake_engine.deletion_queue dq
        JOIN pg_class c ON c.oid = dq.table_name
        WHERE c.relname = '{TABLE_NAME}_history'
        """,
        superuser_conn,
    )
    queued_paths = [row[0] for row in queued]

    for hp in historic_paths:
        assert (
            hp not in queued_paths
        ), f"Historic file {hp} must not be queued for deletion"

    # sanity: the table reads correctly after the append (5 total rows)
    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 5

    pg_conn.rollback()


def test_external_write_partition_spec_evolution(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    External client evolves the partition spec (adds a new identity-
    partitioned column). pg_lake must register the new spec and serve
    correct results when reading rows written under it.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_part_evol"

    run_command(
        f"CREATE TABLE {tbl} (a int, b int) USING iceberg",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {tbl} SELECT i, i * 10 FROM generate_series(1,3) i",
        pg_conn,
    )
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_part_evol"
    )

    with pyiceberg_table.update_spec() as update:
        update.add_field("b", IdentityTransform())

    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_part_evol"
    )
    pyiceberg_table.append(
        pa.table(
            {"a": [4, 5], "b": [40, 50]},
            schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.int32())]),
        )
    )

    # the new spec must be visible in the pg_lake catalog
    spec_count = run_query(
        f"""
        SELECT count(*)
        FROM lake_table.partition_specs ps
        JOIN pg_class c ON c.oid = ps.table_name
        WHERE c.relname = '{TABLE_NAME}_part_evol'
        """,
        superuser_conn,
    )
    assert spec_count[0][0] >= 2, "expected the new partition spec to be registered"

    # full result set is correct across both specs
    result = run_query(f"SELECT a, b FROM {tbl} ORDER BY a", pg_conn)
    assert result == [
        [1, 10],
        [2, 20],
        [3, 30],
        [4, 40],
        [5, 50],
    ]

    pg_conn.rollback()


def test_external_write_rename_column(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    External client renames a column. Iceberg field IDs are stable across
    rename, so the sync must detect the name change and issue an ALTER
    FOREIGN TABLE RENAME COLUMN — not silently drop+add (which would lose
    the column's data on subsequent reads through the old name) and not
    leave the foreign table out of sync with the new schema.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_rename"

    run_command(
        f"CREATE TABLE {tbl} (a int, b text) USING iceberg",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,3) i",
        pg_conn,
    )
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_rename"
    )

    with pyiceberg_table.update_schema() as update:
        update.rename_column("b", "b_renamed")

    # the new column name must exist in the foreign table; old name must not
    columns = run_query(
        f"""
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = '{tbl}'::regclass
          AND attnum > 0
          AND NOT attisdropped
        ORDER BY attnum
        """,
        pg_conn,
    )
    column_names = [row[0] for row in columns]
    assert "b_renamed" in column_names, f"expected 'b_renamed' in {column_names}"
    assert "b" not in column_names, f"old name 'b' still present in {column_names}"

    # data is intact — the rename is non-destructive
    result = run_query(f"SELECT a, b_renamed FROM {tbl} ORDER BY a", pg_conn)
    assert result == [[1, "row1"], [2, "row2"], [3, "row3"]]

    pg_conn.rollback()


def test_external_write_schema_and_data_in_two_commits(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Two PyIceberg commits land between consecutive pg_lake reads:
    first a schema add_column, then a write that uses the new column.
    The sync triggered by the second commit must see both the new column
    in the foreign table and the new file in the catalog. PyIceberg's
    Python API serialises schema and data into separate commits, so this
    is the closest "transactional" coverage we can express here.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_combined"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1), (2)", pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_combined"
    )

    # commit 1: add column
    with pyiceberg_table.update_schema() as update:
        update.add_column("b", LongType())

    # commit 2: write data using the new column
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_combined"
    )
    pyiceberg_table.append(
        pa.table(
            {"a": [3, 4], "b": [30, 40]},
            schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.int64())]),
        )
    )

    # both reads must reflect the post-second-commit state
    result = run_query(f"SELECT a, b FROM {tbl} ORDER BY a", pg_conn)
    assert result == [
        [1, None],
        [2, None],
        [3, 30],
        [4, 40],
    ]

    pg_conn.rollback()


def test_external_write_nested_list_append(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Append rows to a table that has a list column. The schema doesn't
    change, but the sync still walks the field mapping for the list
    element. If the data-file resync chokes on nested types, the file
    won't be cataloged and reads will under-report.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_list"

    run_command(f"CREATE TABLE {tbl} (a int, tags int[]) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} VALUES (1, ARRAY[10, 20]), (2, ARRAY[30])",
        pg_conn,
    )
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{TABLE_NAME}_list")
    pyiceberg_table.append(
        pa.table(
            {"a": [3], "tags": [[40, 50, 60]]},
            schema=pa.schema(
                [
                    pa.field("a", pa.int32()),
                    pa.field("tags", pa.list_(pa.field("element", pa.int32()))),
                ]
            ),
        )
    )

    result = run_query(f"SELECT a, tags FROM {tbl} ORDER BY a", pg_conn)
    assert result == [
        [1, [10, 20]],
        [2, [30]],
        [3, [40, 50, 60]],
    ]

    pg_conn.rollback()


def test_external_write_delete_rows(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    External client deletes a subset of rows via PyIceberg's delete(),
    which rewrites the affected data files (copy-on-write). Position
    deletes are covered by test_external_write_keeps_position_deletes.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_delete"

    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,10) i",
        pg_conn,
    )
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_delete"
    )

    # delete rows where a > 5 — should leave 5 rows
    pyiceberg_table.delete(delete_filter="a > 5")

    result = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert result[0][0] == 5

    result = run_query(f"SELECT a, b FROM {tbl} ORDER BY a", pg_conn)
    assert result == [
        [1, "row1"],
        [2, "row2"],
        [3, "row3"],
        [4, "row4"],
        [5, "row5"],
    ]

    pg_conn.rollback()


def test_external_write_stats_populated(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    After PyIceberg writes a data file, the sync must populate
    lake_table.data_file_column_stats with the lower/upper bounds parsed
    from the Iceberg manifest. Without this, file-level pruning regresses
    silently for externally-written files.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_stats"

    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    pg_conn.commit()

    # write a single file via PyIceberg with a known a-range [10, 30]
    pyiceberg_table = iceberg_catalog.load_table(
        f"{TABLE_NAMESPACE}.{TABLE_NAME}_stats"
    )
    pyiceberg_table.append(
        pa.table(
            {"a": [10, 20, 30], "b": ["x", "y", "z"]},
            schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
        )
    )

    # the sync should have populated column stats for both fields. Field IDs
    # 1 and 2 correspond to a and b in the order they were declared.
    stats = run_query(
        f"""
        SELECT field_id, lower_bound, upper_bound
        FROM lake_table.data_file_column_stats
        WHERE table_name = '{tbl}'::regclass
        ORDER BY field_id
        """,
        superuser_conn,
    )
    assert stats == [
        [1, "10", "30"],
        [2, "x", "z"],
    ], f"expected bounds for a in [10,30] and b in [x,z]; got {stats}"

    pg_conn.rollback()


def test_external_write_internal_catalog_insert_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_full_access,
):
    """
    INSERT into iceberg_tables for the current database catalog must be
    rejected — internal catalog is only mutable via pg_lake DDL.
    """
    db_name = run_query("SELECT current_database()", pg_conn)[0][0]

    error_raised = False
    try:
        run_command(
            f"""
            INSERT INTO iceberg_tables
                (catalog_name, table_namespace, table_name, metadata_location)
            VALUES
                ('{db_name}', '{TABLE_NAMESPACE}', 'fake_internal',
                 's3://fake/v1.metadata.json')
            """,
            pg_conn,
        )
    except Exception as e:
        error_raised = True
        assert (
            "internal catalog is currently only supported via pg_lake_iceberg" in str(e)
        )
        pg_conn.rollback()

    assert error_raised


def test_external_write_internal_catalog_delete_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_full_access,
):
    """
    DELETE from iceberg_tables for the current database catalog must be
    rejected — dropping internal tables goes through pg_lake DDL.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_del_internal"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    pg_conn.commit()

    error_raised = False
    try:
        run_command(
            f"""
            DELETE FROM iceberg_tables
            WHERE table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_del_internal'
            """,
            pg_conn,
        )
    except Exception as e:
        error_raised = True
        assert (
            "internal catalog is currently only supported via pg_lake_iceberg" in str(e)
        )
        pg_conn.rollback()

    assert error_raised

    pg_conn.rollback()


def test_external_write_catalog_name_change_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_access,
):
    """
    UPDATE that changes catalog_name across the internal/external boundary
    must be rejected — the row's identity is tied to which catalog owns it.
    """
    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_catalog_change"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    # current row is internal; flip catalog_name to a foreign value
    error_raised = False
    try:
        run_command(
            f"""
            UPDATE iceberg_tables
            SET catalog_name = 'some_other_catalog',
                metadata_location = metadata_location,
                previous_metadata_location = metadata_location
            WHERE table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_catalog_change'
            """,
            pg_conn,
        )
    except Exception as e:
        error_raised = True
        assert (
            "internal catalog is currently only supported via pg_lake_iceberg" in str(e)
        )
        pg_conn.rollback()

    assert error_raised

    pg_conn.rollback()


def _create_table(pg_conn, name, row_count=5):
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,{row_count}) i",
        pg_conn,
    )
    pg_conn.commit()
    return tbl


def _metadata_location(conn, name):
    return run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{name}'",
        conn,
    )[0][0]


def _rows(values):
    return pa.table(
        {"a": values, "b": [f"ext{value}" for value in values]},
        schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
    )


def _columns(conn, name):
    return run_query(
        f"""SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
            WHERE attrelid = '{TABLE_NAMESPACE}.{name}'::regclass
              AND attnum > 0 AND NOT attisdropped ORDER BY attnum""",
        conn,
    )


def test_external_write_keeps_position_deletes(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    A pg_lake DELETE writes a position delete file. After an external
    append, the resync must still apply it to the data file it deletes from.
    """
    name = f"{TABLE_NAME}_posdel"
    tbl = _create_table(pg_conn, name, row_count=1000)

    run_command(f"DELETE FROM {tbl} WHERE a = 7", pg_conn)
    pg_conn.commit()

    iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}").append(_rows([5000]))

    result = run_query(
        f"SELECT count(*), count(*) FILTER (WHERE a = 7) FROM {tbl}", pg_conn
    )
    assert result == [[1000, 0]]

    result = run_query(
        f"""SELECT sum(deleted_row_count) FROM lake_table.files
            WHERE table_name = '{tbl}'::regclass""",
        pg_conn,
    )
    assert result[0][0] == 1

    pg_conn.rollback()


def test_external_write_same_transaction_as_pglake_write(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
    grant_iceberg_tables_access,
):
    """
    A pg_lake write to another table in the same transaction does not stop
    the sync, while a pg_lake write to the same table is rejected.
    """
    other_tbl = _create_table(pg_conn, f"{TABLE_NAME}_tx_other")
    name = f"{TABLE_NAME}_tx_target"
    tbl = _create_table(pg_conn, name)

    old_location = _metadata_location(pg_conn, name)
    iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}").append(_rows([6]))
    new_location = _metadata_location(pg_conn, name)
    pg_conn.commit()

    # point the table back at the old metadata, so we can redo the write
    run_command(
        f"""UPDATE iceberg_tables
            SET metadata_location = '{old_location}',
                previous_metadata_location = '{new_location}'
            WHERE table_name = '{name}'""",
        pg_conn,
    )
    pg_conn.commit()
    assert run_query(f"SELECT count(*) FROM {tbl}", pg_conn)[0][0] == 5

    run_command(f"INSERT INTO {other_tbl} VALUES (99, 'x')", pg_conn)
    run_command(
        f"""UPDATE iceberg_tables
            SET metadata_location = '{new_location}',
                previous_metadata_location = '{old_location}'
            WHERE table_name = '{name}'""",
        pg_conn,
    )
    pg_conn.commit()
    assert run_query(f"SELECT count(*) FROM {tbl}", pg_conn)[0][0] == 6
    assert run_query(f"SELECT count(*) FROM {other_tbl}", pg_conn)[0][0] == 6

    run_command(f"INSERT INTO {tbl} VALUES (100, 'x')", pg_conn)
    error = run_command(
        f"""UPDATE iceberg_tables
            SET metadata_location = '{old_location}',
                previous_metadata_location = '{new_location}'
            WHERE table_name = '{name}'""",
        pg_conn,
        raise_error=False,
    )
    assert "modified in the same transaction" in str(error)
    pg_conn.rollback()


def test_external_write_drop_and_readd_column(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """Dropping a column and adding it back with another type in one commit."""
    name = f"{TABLE_NAME}_readd"
    tbl = _create_table(pg_conn, name)

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pyiceberg_table.update_schema() as update:
        update.delete_column("b")
        update.add_column("b", LongType())

    assert _columns(pg_conn, name) == [["a", "integer"], ["b", "bigint"]]
    result = run_query(f"SELECT count(*), count(b) FROM {tbl}", pg_conn)
    assert result == [[5, 0]]

    pg_conn.rollback()


def test_external_write_swap_column_names(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """Two columns swap names in one commit."""
    name = f"{TABLE_NAME}_swap"
    tbl = _create_table(pg_conn, name)

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pyiceberg_table.update_schema() as update:
        update.rename_column("a", "b")
        update.rename_column("b", "a")

    assert _columns(pg_conn, name) == [["b", "integer"], ["a", "text"]]
    result = run_query(f"SELECT b, a FROM {tbl} ORDER BY b LIMIT 1", pg_conn)
    assert result == [[1, "row1"]]

    pg_conn.rollback()


def test_external_write_type_promotion(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """An int -> long promotion widens the column."""
    name = f"{TABLE_NAME}_promote"
    tbl = _create_table(pg_conn, name)

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pyiceberg_table.update_schema() as update:
        update.update_column("a", LongType())

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    pyiceberg_table.append(
        pa.table(
            {"a": [5000000000], "b": ["big"]},
            schema=pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())]),
        )
    )

    assert _columns(pg_conn, name) == [["a", "bigint"], ["b", "text"]]
    result = run_query(f"SELECT max(a), count(*) FROM {tbl}", pg_conn)
    assert result == [[5000000000, 6]]
    result = run_query(f"SELECT count(*) FROM {tbl} WHERE a > 4000000000", pg_conn)
    assert result[0][0] == 1

    pg_conn.rollback()


def test_external_write_table_name_change_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_access,
):
    """
    table_name and table_namespace cannot be changed through the view, also
    not to point the row at another table.
    """
    name = f"{TABLE_NAME}_rename_row"
    _create_table(pg_conn, name)
    run_command(f"CREATE TABLE {TABLE_NAMESPACE}.{name}_heap (x int)", pg_conn)
    pg_conn.commit()

    location = _metadata_location(pg_conn, name)
    error = run_command(
        f"""UPDATE iceberg_tables
            SET table_name = '{name}_heap',
                metadata_location = '{location}',
                previous_metadata_location = '{location}'
            WHERE table_name = '{name}'""",
        pg_conn,
        raise_error=False,
    )
    assert "cannot change table_namespace or table_name" in str(error)
    pg_conn.rollback()


def test_external_write_requires_ownership(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
):
    """Members of iceberg_catalog cannot write to tables they do not own."""
    name = f"{TABLE_NAME}_not_owner"
    _create_table(pg_conn, name)
    location = _metadata_location(pg_conn, name)
    pg_conn.commit()

    run_command(
        """
        DROP ROLE IF EXISTS iceberg_not_owner;
        CREATE ROLE iceberg_not_owner;
        GRANT iceberg_catalog TO iceberg_not_owner;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("SET ROLE iceberg_not_owner", superuser_conn)
    error = run_command(
        f"""UPDATE iceberg_tables
            SET metadata_location = '{location}',
                previous_metadata_location = '{location}'
            WHERE table_name = '{name}'""",
        superuser_conn,
        raise_error=False,
    )
    assert "must be owner" in str(error)
    superuser_conn.rollback()

    run_command("DROP ROLE iceberg_not_owner", superuser_conn)
    superuser_conn.commit()


def test_external_write_data_file_outside_table_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Metadata that references a data file outside the table location is
    rejected, since the sync would otherwise adopt, and later delete, a file
    that belongs to someone else.
    """
    name = f"{TABLE_NAME}_foreign_file"
    tbl = _create_table(pg_conn, name)

    buffer = pa.BufferOutputStream()
    pa.parquet.write_table(_rows([42]), buffer)
    key = f"external_write_elsewhere/{name}.parquet"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue().to_pybytes())

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pytest.raises(Exception, match="is not in the table location"):
        pyiceberg_table.add_files([f"s3://{TEST_BUCKET}/{key}"])

    assert run_query(f"SELECT count(*) FROM {tbl}", pg_conn)[0][0] == 5
    pg_conn.rollback()


def test_external_write_row_ids_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    Tables with row_ids reject external writes, since the writer cannot
    preserve row IDs.
    """
    name = f"{TABLE_NAME}_row_ids"
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(
        f"CREATE TABLE {tbl} (a int, b text) USING iceberg WITH (row_ids = 'true')",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {tbl} SELECT i, 'row' || i FROM generate_series(1,5) i",
        pg_conn,
    )
    pg_conn.commit()
    location = _metadata_location(pg_conn, name)

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pytest.raises(Exception, match="not supported on tables with row_ids"):
        pyiceberg_table.append(_rows([6]))

    assert _metadata_location(pg_conn, name) == location
    assert run_query(f"SELECT count(*) FROM {tbl}", pg_conn)[0][0] == 5
    pg_conn.rollback()


@pytest.mark.parametrize(
    "setup, message",
    [
        ("ALTER TABLE {tbl} ADD CHECK (a > 0)", "CHECK constraints"),
        (
            "CREATE FUNCTION {name}_fn() RETURNS trigger LANGUAGE plpgsql "
            "AS $$BEGIN RETURN NEW; END$$; "
            "CREATE TRIGGER {name}_trg AFTER INSERT ON {tbl} "
            "FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
            "triggers",
        ),
        (
            "CREATE TABLE {name}_parent (a int, b text) PARTITION BY RANGE (a); "
            "ALTER TABLE {name}_parent ATTACH PARTITION {tbl} FOR VALUES FROM (0) TO (10)",
            "a partition",
        ),
    ],
    ids=["check", "trigger", "partition"],
)
def test_external_write_postgres_only_rules_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
    setup,
    message,
):
    """
    Tables whose rules only Postgres applies reject external writes, since
    the writer would bypass them (e.g. a row outside the partition bounds
    disappears from filtered queries on the parent).
    """
    name = f"{TABLE_NAME}_rule_{message.split()[-1].lower()}"
    tbl = _create_table(pg_conn, name)
    run_command(setup.format(tbl=tbl, name=name), pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pytest.raises(Exception, match=f"not supported on tables with {message}"):
        pyiceberg_table.append(_rows([500]))

    assert run_query(f"SELECT count(*) FROM {tbl}", pg_conn)[0][0] == 5
    pg_conn.rollback()


def test_external_write_generated_column_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """A stored generated column would get whatever the writer puts there."""
    name = f"{TABLE_NAME}_generated"
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(
        f"CREATE TABLE {tbl} (a int, b int GENERATED ALWAYS AS (a * 10) STORED) USING iceberg",
        pg_conn,
    )
    run_command(f"INSERT INTO {tbl} (a) VALUES (1)", pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pytest.raises(
        Exception, match="not supported on tables with generated columns"
    ):
        pyiceberg_table.append(
            pa.table(
                {"a": [2], "b": [12345]},
                schema=pa.schema(
                    [pa.field("a", pa.int32()), pa.field("b", pa.int32())]
                ),
            )
        )

    assert run_query(f"SELECT a, b FROM {tbl}", pg_conn) == [[1, 10]]
    pg_conn.rollback()


def test_external_write_nulls_in_required_column_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
    monkeypatch,
):
    """
    pyiceberg refuses to write nulls to a required column, so turn off its
    schema check to act like a writer that does not, and let the manifest
    record the nulls.
    """
    name = f"{TABLE_NAME}_required"
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(f"CREATE TABLE {tbl} (a int NOT NULL, b text) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1, 'x')", pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    table_location = pyiceberg_table.location().split(f"s3://{TEST_BUCKET}/", 1)[1]

    buffer = pa.BufferOutputStream()
    pa.parquet.write_table(
        pa.table(
            {"a": pa.array([None, 2], pa.int32()), "b": ["null", "two"]},
        ),
        buffer,
    )
    key = f"{table_location}/data/with_nulls.parquet"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue().to_pybytes())

    monkeypatch.setattr(
        pyiceberg.io.pyarrow,
        "_check_pyarrow_schema_compatible",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(Exception, match='has null values in column "a"'):
        pyiceberg_table.add_files([f"s3://{TEST_BUCKET}/{key}"])

    assert run_query(f"SELECT count(*), count(a) FROM {tbl}", pg_conn) == [[1, 1]]
    pg_conn.rollback()


def test_external_write_column_becomes_optional(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """A required column that becomes optional drops NOT NULL in Postgres."""
    name = f"{TABLE_NAME}_optional"
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(f"CREATE TABLE {tbl} (a int NOT NULL, b text) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1, 'x')", pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pyiceberg_table.update_schema() as update:
        update.update_column("a", required=False)

    run_command(f"INSERT INTO {tbl} VALUES (NULL, 'null')", pg_conn)
    pg_conn.commit()
    assert run_query(f"SELECT count(*), count(a) FROM {tbl}", pg_conn) == [[2, 1]]
    pg_conn.rollback()


@pytest.mark.parametrize(
    "change, message",
    [
        (
            lambda update: update.update_column("b", required=True),
            'cannot make column "b" required',
        ),
        (
            lambda update: update.add_column("c", LongType(), required=True),
            'cannot add required column "c"',
        ),
    ],
    ids=["make_required", "add_required"],
)
def test_external_write_required_column_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
    change,
    message,
):
    """Postgres cannot apply NOT NULL to rows that are already there."""
    name = f"{TABLE_NAME}_{message.split()[1]}_required"
    tbl = _create_table(pg_conn, name)

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pytest.raises(Exception, match=message):
        with pyiceberg_table.update_schema(allow_incompatible_changes=True) as update:
            change(update)

    assert _columns(pg_conn, name) == [["a", "integer"], ["b", "text"]]
    pg_conn.rollback()


@pytest.mark.parametrize(
    "column_type, change",
    [
        (
            "{name}_point",
            lambda update: update.add_column(("p", "z"), LongType()),
        ),
        (
            "{name}_point",
            lambda update: update.rename_column("p.x", "x_renamed"),
        ),
        (
            "int[]",
            lambda update: update.update_column("p.element", LongType()),
        ),
    ],
    ids=["struct_add_field", "struct_rename_field", "list_promote_element"],
)
def test_external_write_nested_change_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
    column_type,
    change,
    request,
):
    """Changes inside struct and list columns are rejected."""
    name = f"{TABLE_NAME}_{request.node.callspec.id}"
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(f"CREATE TYPE {name}_point AS (x int, y int)", pg_conn)
    run_command(
        f"CREATE TABLE {tbl} (a int, p {column_type.format(name=name)}) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pytest.raises(Exception, match='cannot change the type of column "p"'):
        with pyiceberg_table.update_schema() as update:
            change(update)

    pg_conn.rollback()


def test_external_write_add_struct_column(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """A new struct column, and later appends that use it, sync."""
    name = f"{TABLE_NAME}_add_struct"
    tbl = _create_table(pg_conn, name)

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    with pyiceberg_table.update_schema() as update:
        update.add_column(
            "s",
            StructType(
                NestedField(field_id=1, name="x", field_type=IntegerType()),
                NestedField(field_id=2, name="y", field_type=IntegerType()),
            ),
        )

    # a second commit compares the registered struct against the metadata
    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    pyiceberg_table.append(
        pa.table(
            {"a": [6], "b": ["ext6"], "s": [{"x": 1, "y": 2}]},
            schema=pyiceberg_table.schema().as_arrow(),
        )
    )

    result = run_query(
        f"SELECT a, (s).x, (s).y FROM {tbl} WHERE s IS NOT NULL", pg_conn
    )
    assert result == [[6, 1, 2]]
    pg_conn.rollback()


def test_external_write_decimal_column_unchanged(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    iceberg_catalog,
):
    """
    pyiceberg writes decimal(10, 2) with a space; that must not look like a
    type change.
    """
    name = f"{TABLE_NAME}_decimal"
    tbl = f"{TABLE_NAMESPACE}.{name}"
    run_command(f"CREATE TABLE {tbl} (a numeric(10,2)) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1.25)", pg_conn)
    pg_conn.commit()

    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    pyiceberg_table.append(
        pa.table({"a": [Decimal("2.5")]}, schema=pyiceberg_table.schema().as_arrow())
    )
    pyiceberg_table = iceberg_catalog.load_table(f"{TABLE_NAMESPACE}.{name}")
    pyiceberg_table.append(
        pa.table({"a": [Decimal("3.75")]}, schema=pyiceberg_table.schema().as_arrow())
    )

    assert _columns(pg_conn, name) == [["a", "numeric(10,2)"]]
    assert run_query(f"SELECT sum(a) FROM {tbl}", pg_conn)[0][0] == Decimal("7.50")
    pg_conn.rollback()

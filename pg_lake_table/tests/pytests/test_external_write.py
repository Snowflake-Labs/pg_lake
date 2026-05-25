import pytest
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform
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
    External client deletes a subset of rows via PyIceberg's delete().
    Whether pyiceberg uses copy-on-write or merge-on-read (position
    deletes), the sync must end up serving the correct row set; this
    exercises the CONTENT_POSITION_DELETES branch in SyncDataFiles when
    pyiceberg writes delete files, and the COW path when it rewrites.
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


def test_external_write_unknown_table_rejected(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_iceberg_tables_full_access,
):
    """
    UPDATE on iceberg_tables for the internal catalog with a namespace/table
    that doesn't resolve to a real relation must raise an undefined-table
    error rather than silently no-op'ing.
    """
    db_name = run_query("SELECT current_database()", pg_conn)[0][0]

    tbl = f"{TABLE_NAMESPACE}.{TABLE_NAME}_unknown"
    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    error_raised = False
    try:
        run_command(
            f"""
            UPDATE iceberg_tables
            SET table_name = 'definitely_not_a_real_table',
                metadata_location = metadata_location,
                previous_metadata_location = metadata_location
            WHERE catalog_name = '{db_name}'
              AND table_namespace = '{TABLE_NAMESPACE}'
              AND table_name = '{TABLE_NAME}_unknown'
            """,
            pg_conn,
        )
    except Exception as e:
        error_raised = True
        assert "does not exist" in str(e)
        pg_conn.rollback()

    assert error_raised

    pg_conn.rollback()

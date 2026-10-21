"""
Tests for the catalog-based commit path.

A transaction that did not TRUNCATE does not need the diff against the last
pushed metadata: the files it added and removed are the ones the catalog
recorded for it. These tests check that such a commit really does skip the
diff, and that the files of every write shape end up in the metadata.
"""

import pytest
from utils_pytest import *

TEST_TABLE_NAMESPACE = "test_iceberg_append_only_commit_nsp"
DIFF_INJECTION_POINT = "commit-data-file-diff"

table_counter = 0


@pytest.fixture
def generate_table_name():
    global table_counter
    table_counter += 1

    return "test_iceberg_append_only_commit_" + str(table_counter)


@pytest.fixture
def create_iceberg_table(pg_conn, s3, with_default_location, generate_table_name):
    table_name = generate_table_name

    run_command(f"CREATE SCHEMA {TEST_TABLE_NAMESPACE}", pg_conn)
    run_command(
        f"""
      CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name} (id int) USING pg_lake_iceberg;
      ALTER FOREIGN TABLE {TEST_TABLE_NAMESPACE}.{table_name} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # A commit past the immediately-preceding one queues the metadata.json
    # before that for deletion (see ApplyIcebergMetadataChanges), but nothing
    # drains the queue until this retention period elapses or something
    # flushes it explicitly. Zero it so flush_deletion_queue below, called
    # before every consistency check, actually removes what it queued.
    run_command("SET pg_lake_engine.orphaned_file_retention_period TO 0", pg_conn)

    yield table_name

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {TEST_TABLE_NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()


def error_on_data_file_diff(pg_conn):
    """Make the commit-time data file diff fail, for this session only."""

    run_command("SELECT public.injection_points_set_local();", pg_conn)
    pg_conn.commit()
    run_command(
        f"SELECT public.injection_points_attach('{DIFF_INJECTION_POINT}', 'error');",
        pg_conn,
    )


def detach_data_file_diff_injection(pg_conn):
    """Undo error_on_data_file_diff for a commit that never triggered it.

    create_injection_extension is module-scoped, so the point stays attached
    for the rest of the tests in this file if the commit that follows
    error_on_data_file_diff skips the diff and never fires it. The next test
    to attach the same point then fails with "already defined".
    """

    run_command(
        f"SELECT public.injection_points_detach('{DIFF_INJECTION_POINT}');",
        pg_conn,
    )
    pg_conn.commit()


def assert_iceberg_metadata_matches_storage(pg_conn, s3, table_namespace, table_name):
    """assert_iceberg_s3_file_consistency, but first drains what pg_lake has
    already decided is safe to remove and just hasn't gotten to yet:

    - a data file a rolled-back subtransaction wrote is only cleaned up by
      VACUUM (see in_progress_files.c), not by the rollback itself
    - the metadata.json from two commits back is queued for deletion the
      moment a third commit lands (previous_metadata_location is a
      single-entry pointer), but nothing drains that queue until VACUUM or
      flush_deletion_queue runs

    Skipping either leaves a file in storage that pg_lake has already
    written off, which reads as a bug in the catalog commit path but is not
    one: it is the same lag any multi-commit test hits.
    """

    qualified_name = f"{table_namespace}.{table_name}"
    run_command_outside_tx([f"VACUUM {qualified_name}"])
    run_command(
        f"SELECT lake_engine.flush_deletion_queue('{qualified_name}'::regclass)",
        pg_conn,
    )
    pg_conn.commit()
    assert_iceberg_s3_file_consistency(pg_conn, s3, table_namespace, table_name)


def test_append_only_commit_skips_the_diff(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_injection_extension,
):
    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table
    qualified_name = f"{TEST_TABLE_NAMESPACE}.{table_name}"

    run_command(f"INSERT INTO {qualified_name} VALUES (1)", pg_conn)
    pg_conn.commit()

    error_on_data_file_diff(pg_conn)

    run_command(f"INSERT INTO {qualified_name} VALUES (2)", pg_conn)
    run_command(
        f"INSERT INTO {qualified_name} SELECT i FROM generate_series(3,10)i", pg_conn
    )

    assert run_command("COMMIT;", pg_conn, raise_error=False) is None
    detach_data_file_diff_injection(pg_conn)

    assert run_query(f"SELECT count(*) FROM {qualified_name}", pg_conn)[0][0] == 10
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )


def test_commit_with_a_delete_skips_the_diff(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_injection_extension,
):
    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table
    qualified_name = f"{TEST_TABLE_NAMESPACE}.{table_name}"

    run_command(
        f"INSERT INTO {qualified_name} SELECT i FROM generate_series(1,10)i", pg_conn
    )
    run_command(
        f"INSERT INTO {qualified_name} SELECT i FROM generate_series(11,20)i", pg_conn
    )
    pg_conn.commit()

    error_on_data_file_diff(pg_conn)

    # deletes every row of one file, so the data file is removed rather than
    # kept with a delete file next to it
    run_command(f"DELETE FROM {qualified_name} WHERE id <= 10", pg_conn)

    assert run_command("COMMIT;", pg_conn, raise_error=False) is None
    detach_data_file_diff_injection(pg_conn)

    assert run_query(f"SELECT count(*) FROM {qualified_name}", pg_conn)[0][0] == 10
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )


def test_deletes_on_two_tables_in_one_transaction(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
):
    """Each table's commit only removes that table's files."""

    table_name = create_iceberg_table
    first_table = f"{TEST_TABLE_NAMESPACE}.{table_name}"
    second_table = f"{TEST_TABLE_NAMESPACE}.{table_name}_second"

    run_command(
        f"""
      CREATE TABLE {second_table} (id int) USING pg_lake_iceberg;
      ALTER FOREIGN TABLE {second_table} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )
    for qualified_name in [first_table, second_table]:
        run_command(f"INSERT INTO {qualified_name} VALUES (1)", pg_conn)
        run_command(f"INSERT INTO {qualified_name} VALUES (2)", pg_conn)
    pg_conn.commit()

    run_command(f"DELETE FROM {first_table} WHERE id = 1", pg_conn)
    run_command(f"DELETE FROM {second_table} WHERE id = 2", pg_conn)
    pg_conn.commit()

    assert run_query(f"SELECT id FROM {first_table}", pg_conn) == [[2]]
    assert run_query(f"SELECT id FROM {second_table}", pg_conn) == [[1]]
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, f"{table_name}_second"
    )


def test_merge_on_read_delete_takes_the_append_only_path(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_injection_extension,
):
    """A delete that stays under copy_on_write_threshold is merge-on-read: it
    adds a position-delete file and removes no data file, so it still takes
    the catalog commit path. The added set has to include that delete file, not
    just plain data files, or the row it marks stays visible.
    """

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table
    qualified_name = f"{TEST_TABLE_NAMESPACE}.{table_name}"

    # one file with enough rows that deleting one of them stays well under
    # the default 20% copy_on_write_threshold
    run_command(
        f"INSERT INTO {qualified_name} SELECT i FROM generate_series(1,100)i", pg_conn
    )
    pg_conn.commit()

    error_on_data_file_diff(pg_conn)

    run_command(f"DELETE FROM {qualified_name} WHERE id = 1", pg_conn)

    assert run_command("COMMIT;", pg_conn, raise_error=False) is None
    detach_data_file_diff_injection(pg_conn)

    assert run_query(f"SELECT count(*) FROM {qualified_name}", pg_conn)[0][0] == 99
    assert (
        run_query(f"SELECT count(*) FROM {qualified_name} WHERE id = 1", pg_conn)[0][0]
        == 0
    )
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )


def test_every_write_shape_lands_in_metadata(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
):
    """The metadata has to match the data files whichever path the commit took."""

    table_name = create_iceberg_table
    qualified_name = f"{TEST_TABLE_NAMESPACE}.{table_name}"

    # add only, one file
    run_command(f"INSERT INTO {qualified_name} VALUES (1)", pg_conn)
    pg_conn.commit()
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )

    # add only, several files in one transaction
    run_command(
        f"INSERT INTO {qualified_name} SELECT i FROM generate_series(2,100)i", pg_conn
    )
    run_command(f"INSERT INTO {qualified_name} VALUES (101)", pg_conn)
    pg_conn.commit()
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )

    # add and remove in the same transaction
    run_command(f"INSERT INTO {qualified_name} VALUES (102)", pg_conn)
    run_command(f"DELETE FROM {qualified_name} WHERE id = 1", pg_conn)
    pg_conn.commit()
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )

    # a file added in a subtransaction that rolled back must not be added
    run_command(f"INSERT INTO {qualified_name} VALUES (103)", pg_conn)
    run_command("SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {qualified_name} VALUES (999)", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {qualified_name} VALUES (104)", pg_conn)
    pg_conn.commit()
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )

    # a file added and removed in the same transaction is in neither set
    run_command(f"INSERT INTO {qualified_name} VALUES (105)", pg_conn)
    run_command(f"DELETE FROM {qualified_name} WHERE id = 105", pg_conn)
    pg_conn.commit()
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )

    # update, which removes and adds
    run_command(f"UPDATE {qualified_name} SET id = id + 1000 WHERE id > 100", pg_conn)
    pg_conn.commit()
    assert_iceberg_metadata_matches_storage(
        pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
    )

    assert run_query(f"SELECT count(*) FROM {qualified_name}", pg_conn)[0][0] == 103
    assert (
        run_query(f"SELECT count(*) FROM {qualified_name} WHERE id = 999", pg_conn)[0][
            0
        ]
        == 0
    )

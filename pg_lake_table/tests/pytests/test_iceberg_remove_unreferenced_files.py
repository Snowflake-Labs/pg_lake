import pytest
from utils_pytest import *

TEST_TABLE_NAMESPACE = "test_iceberg_remove_unreferenced_files_nsp"


# insert and vacuum/vacuum full
def test_remove_unreferenced_files_1(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    table_name = create_iceberg_table

    # make sure we always remove unreferenced files
    run_command("SET pg_lake_engine.orphaned_file_retention_period TO 0", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )

    # trigger snapshot retention
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)
    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM FULL {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)
    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)

    pg_conn.rollback()


# insert and positional update/delete
def test_remove_unreferenced_files_2(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    table_name = create_iceberg_table

    # make sure we always remove unreferenced files
    run_command("SET pg_lake_engine.orphaned_file_retention_period TO 0", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 2", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 4", pg_conn
    )
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)
    pg_conn.rollback()


# insert and copy-on-wrote update/delete
def test_remove_unreferenced_files_3(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    table_name = create_iceberg_table

    # make sure we always remove unreferenced files
    run_command("SET pg_lake_engine.orphaned_file_retention_period TO 0", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 50", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 75", pg_conn
    )
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)
    pg_conn.rollback()


# insert and manifest compaction
def test_remove_unreferenced_files_4(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    table_name = create_iceberg_table

    # make sure we always remove unreferenced files
    run_command("SET pg_lake_engine.orphaned_file_retention_period TO 0", pg_conn)

    # make sure we do manifest compaction
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write TO on", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)
    for i in range(0, 10):
        run_command(
            f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
            pg_conn,
        )

    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]

    # now trigger removal of unreferenced files
    # doing this in a loop should not change the outcome
    for i in range(0, 3):
        run_command_outside_tx(vacuum_commands)
        assert_iceberg_s3_file_consistency(
            pg_conn, s3, TEST_TABLE_NAMESPACE, table_name
        )

    pg_conn.rollback()


# insert, update, delete and truncate
def test_remove_unreferenced_files_5(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    table_name = create_iceberg_table

    # make sure we always remove unreferenced files
    run_command("SET pg_lake_engine.orphaned_file_retention_period TO 0", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 2", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 5", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 50", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 75", pg_conn
    )
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)
    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)

    run_command(f"TRUNCATE {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn)
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)
    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)

    pg_conn.rollback()


def test_remove_unreferenced_files_6(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table

    # make sure the files are retained for 1 seconds
    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 2", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 5", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 50", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 75", pg_conn
    )
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_table.vacuum_compact_min_input_files TO 1",
        "SET pg_lake_engine.orphaned_file_retention_period TO 10",
        "RESET pg_lake_iceberg.manifest_min_count_to_merge",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.max_file_removals_per_vacuum TO 100",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    file_paths_q = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )

    # Extract the file paths
    file_paths = [item[0] for item in file_paths_q]

    # Assert the total number of files
    print(file_paths)
    assert len(file_paths) == 40, f"Expected 40 total files, found {len(file_paths)}"

    metadata_json_files = [f for f in file_paths if f.endswith(".metadata.json")]
    assert (
        len(metadata_json_files) == 7
    ), f"Expected 7 metadata.json files, found {len(metadata_json_files)}"

    files = [f for f in file_paths if "/data/" in f and f.endswith(".parquet")]
    assert len(files) == 12, f"Expected 12 data files, found {len(files)}"

    snapshot_files = [f for f in file_paths if "metadata/snap-" in f]
    assert (
        len(snapshot_files) == 7
    ), f"Expected 7 snapshot files, found {len(snapshot_files)}"

    manifest_files = [
        f for f in file_paths if f.endswith("-m0.avro") or f.endswith("-m1.avro")
    ]
    assert (
        len(manifest_files) == 11
    ), f"Expected 11 manifest files, found {len(manifest_files)}"

    # sleep 1 seconds (`pg_lake_engine.orphaned_file_retention_period`)
    # and commit such that we get a new timestamp in the next command
    run_command("SELECT pg_sleep(1)", pg_conn)
    pg_conn.commit()

    run_command(
        f"BEGIN;SELECT lake_engine.flush_deletion_queue('{TEST_TABLE_NAMESPACE}.{table_name}'::regclass); COMMIT;",
        pg_conn,
    )

    file_paths_q = run_query(
        f"SELECT path  FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )

    assert len(file_paths_q) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)

    pg_conn.rollback()


# pass PER_LOOP_FILE_CLEANUP_LIMIT (10) and MAX_FILE_REMOVALS_PER_VACUUM (100)
def test_remove_unreferenced_files_7(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table

    for i in range(0, 25):
        run_command(
            f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1)", pg_conn
        )
        pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 100",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_table.max_file_removals_per_vacuum TO 100",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    file_paths = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )

    # Assert the total number of files
    assert len(file_paths) == 127, f"Expected 127 total files, found {len(file_paths)}"

    # sleep 1 seconds (`pg_lake_engine.orphaned_file_retention_period`)
    # and commit such that we get a new timestamp in the next command
    run_command("SELECT pg_sleep(1)", pg_conn)

    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.max_file_removals_per_vacuum TO 100",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    # should remove up to pg_lake_table.max_file_removals_per_vacuum (100)
    file_paths_q = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )

    # 27 files remain
    assert len(file_paths_q) == 27

    # should remove the rest
    run_command_outside_tx(vacuum_commands)
    file_paths_q = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )

    # due to new vacuum
    assert len(file_paths_q) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)

    # verify the data is still there :)
    res = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )[0][0]
    assert res == 25
    pg_conn.rollback()


# drop table
def test_remove_unreferenced_files_8(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )
    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (2)", pg_conn)
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 2", pg_conn
    )
    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 5", pg_conn
    )
    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 50", pg_conn
    )
    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 75", pg_conn
    )
    pg_conn.commit()

    result_before_drop = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )

    files = iceberg_get_referenced_files(pg_conn, f"{table_name}")

    # DROP + ROLLBACK should not impact anything
    run_command(
        f"BEGIN;DROP TABLE {TEST_TABLE_NAMESPACE}.{table_name};ROLLBACK;", pg_conn
    )

    # make sure we do not added the files to the deletion queue after a ROLLBACK
    for file in files:
        cnt = run_query(
            f"SELECT count(*) FROM lake_engine.deletion_queue WHERE path = '{file[0]}'",
            superuser_conn,
        )[0][0]
        assert cnt == 0

    # and, we can still query the table
    result_after_drop_rollback = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )
    assert result_before_drop == result_after_drop_rollback

    run_command(
        f"BEGIN;DROP TABLE {TEST_TABLE_NAMESPACE}.{table_name};COMMIT;", pg_conn
    )
    pg_conn.commit()

    # make sure all files are marked for dropping
    for file in files:
        cnt = run_query(
            f"SELECT count(*) FROM lake_engine.deletion_queue WHERE path = '{file[0]}'",
            superuser_conn,
        )[0][0]
        assert cnt == 1

    # now, do the clean-up
    run_command(f"BEGIN;SELECT lake_engine.flush_deletion_queue(0); COMMIT;", pg_conn)

    # make sure all files are dropped
    for file in files:
        cnt = run_query(
            f"SELECT count(*) FROM lake_engine.deletion_queue WHERE path = '{file[0]}'",
            superuser_conn,
        )[0][0]
        assert cnt == 0


# even if previous step in VACUUM fails, we can continue VACUUM
def test_remove_unreferenced_files_9(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table

    # create some snapshots such that
    # snapshot expiration can remove
    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1)", pg_conn)
    pg_conn.commit()

    # retain snapshots
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 1000",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    deletion_file_paths = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )
    assert len(deletion_file_paths) == 0

    # also, generate few entries for in_progress_tables
    run_command("BEGIN", pg_conn)
    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1)", pg_conn)
    run_command("ROLLBACK", pg_conn)

    pg_conn.rollback()

    in_progress_files = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ILIKE '%{table_name}%'",
        superuser_conn,
    )
    assert len(in_progress_files) > 0

    # now, synthetically rename the deletion queue table
    run_command(
        f"ALTER TABLE lake_engine.deletion_queue RENAME TO deletion_queue_old",
        superuser_conn,
    )
    superuser_conn.commit()

    # now, the VACUUM for in_progress_files should work fine, even if the deletion_queue related work fails
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    in_progress_files = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ILIKE '%{table_name}%'",
        superuser_conn,
    )
    assert len(in_progress_files) == 0

    # now, fix the table name and re-run VACUUM
    run_command(
        f"ALTER TABLE lake_engine.deletion_queue_old RENAME TO deletion_queue",
        superuser_conn,
    )
    superuser_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]
    run_command_outside_tx(vacuum_commands)

    deletion_file_paths = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )
    assert len(deletion_file_paths) == 0


# VACUUM (iceberg); removes unreferenced files of dropped tables
def test_remove_unreferenced_files_10(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table

    # let's make sure there are no dropped table in the deletion queue, to prevent any flakiness
    superuser_conn.commit()
    run_command(
        "SET pg_lake_engine.orphaned_file_retention_period TO 0", superuser_conn
    )
    run_command(f"SELECT lake_engine.flush_deletion_queue(0)", superuser_conn)

    file_paths_q = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) == 0

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(101,200)i",
        pg_conn,
    )
    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id = 1", pg_conn
    )
    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id > 150", pg_conn
    )
    pg_conn.commit()

    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn)
    pg_conn.commit()

    #  '-'::regclass == invalidOid
    file_paths_q = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) > 0
    superuser_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        "VACUUM (ICEBERG, VERBOSE)",
    ]
    run_command_outside_tx(vacuum_commands)

    # make sure deletion from the queue is done
    file_paths_q = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) == 0


# VACUUM (iceberg); removes unreferenced files of dropped tables as well as other existing iceberg tables
def test_remove_unreferenced_files_11(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table
    # create two more tables
    run_command(
        f"""
        CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name}_for_test_11_iceberg (id int) USING iceberg;
        ALTER FOREIGN TABLE {TEST_TABLE_NAMESPACE}.{table_name}_for_test_11_iceberg OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )
    run_command(
        f"CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name}_for_test_11_heap (id int) USING heap",
        pg_conn,
    )

    # let's make sure there are no dropped table in the deletion queue, to prevent any flakiness
    superuser_conn.commit()
    run_command(
        "SET pg_lake_engine.orphaned_file_retention_period TO 0", superuser_conn
    )
    run_command(f"SELECT lake_engine.flush_deletion_queue(0)", superuser_conn)

    file_paths_q = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) == 0

    table_names = [
        f"{TEST_TABLE_NAMESPACE}.{table_name}",
        f"{TEST_TABLE_NAMESPACE}.{table_name}_for_test_11_iceberg",
        f"{TEST_TABLE_NAMESPACE}.{table_name}_for_test_11_heap",
    ]

    for table in table_names:
        run_command(
            f"INSERT INTO {table} SELECT i FROM generate_series(0,100)i", pg_conn
        )
        run_command(
            f"INSERT INTO {table} SELECT i FROM generate_series(101,200)i", pg_conn
        )
        run_command(f"DELETE FROM {table} WHERE id = 1", pg_conn)
        run_command(f"UPDATE {table} SET id = 0 WHERE id > 150", pg_conn)

    pg_conn.commit()

    # drop the first iceberg table
    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn)
    pg_conn.commit()

    #  '-'::regclass == invalidOid
    file_paths_for_dropped = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_for_dropped) > 0

    file_paths_for_non_dropped = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_for_non_dropped) > 0

    superuser_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        "VACUUM (ICEBERG, VERBOSE)",
    ]
    run_command_outside_tx(vacuum_commands)

    # make sure deletion from the queue is cleared for the dropped table
    file_paths_for_dropped = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_for_dropped) == 0

    # and make sure we also clean-up the entries for non-dropped
    file_paths_for_non_dropped = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}_for_test_11_iceberg'::regclass",
        superuser_conn,
    )
    assert len(file_paths_for_non_dropped) == 0


# VACUUM table_1, table_2; make sure that this works along with dropped tables
def test_remove_unreferenced_files_12(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
):

    table_name = create_iceberg_table

    # create two more tables
    run_command(
        f"CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_iceberg (id int) USING iceberg",
        pg_conn,
    )
    run_command(
        f"CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_heap (id int) USING heap",
        pg_conn,
    )

    # let's make sure there are no dropped table in the deletion queue, to prevent any flakiness
    superuser_conn.commit()
    run_command(
        "SET pg_lake_engine.orphaned_file_retention_period TO 0", superuser_conn
    )
    run_command(f"SELECT lake_engine.flush_deletion_queue(0)", superuser_conn)

    file_paths_q = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) == 0

    table_names = [
        f"{TEST_TABLE_NAMESPACE}.{table_name}",
        f"{TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_iceberg",
        f"{TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_heap",
    ]

    for table in table_names:
        run_command(
            f"INSERT INTO {table} SELECT i FROM generate_series(0,100)i", pg_conn
        )
        run_command(
            f"INSERT INTO {table} SELECT i FROM generate_series(101,200)i", pg_conn
        )
        run_command(f"DELETE FROM {table} WHERE id = 1", pg_conn)
        run_command(f"UPDATE {table} SET id = 0 WHERE id > 150", pg_conn)

    pg_conn.commit()

    # drop the first iceberg table
    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn)
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period TO 0",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_iceberg, {TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_heap ",
    ]
    run_command_outside_tx(vacuum_commands)

    # make sure deletion from the queue stays as-is for the dropped table
    file_paths_for_dropped = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '-'::regclass",
        superuser_conn,
    )
    assert len(file_paths_for_dropped) > 0

    # and make sure we also clean-up the entries for non-dropped
    file_paths_for_non_dropped = run_query(
        f"SELECT * FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}_for_test_12_iceberg'::regclass",
        superuser_conn,
    )
    assert len(file_paths_for_non_dropped) == 0


# zero row inserts
def test_remove_unreferenced_files_13(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    pgduck_conn,
):

    table_name = create_iceberg_table

    # make sure the files are retained for 1 seconds
    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i LIMIT 0",
        pg_conn,
    )
    pg_conn.commit()

    # we should have 1 file in the deletion queue
    # that's the file with zero rows. Normally it should be fine
    # but Snowflake complains about zero row parquet files, so we
    # do not add that to the table. Note the queued path is the concrete
    # data file DuckDB wrote (e.g. data_0.parquet), not the directory
    # prefix, since DropAndQueueEmptyDataFiles queues the exact zero-row
    # file it drops from the write.
    file_paths_q = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE path ilike '%{TEST_TABLE_NAMESPACE}/{table_name}%'",
        superuser_conn,
    )
    assert len(file_paths_q) == 1

    # ensure zero rows
    res = run_query(
        f"SELECT count(*) FROM read_parquet('{file_paths_q[0][0]}')", pgduck_conn
    )
    assert res[0][0] == 0

    # no files inserted
    file_paths = run_query(
        f"SELECT path FROM lake_table.files WHERE table_name = '{TEST_TABLE_NAMESPACE}.{table_name}'::regclass",
        superuser_conn,
    )
    assert len(file_paths) == 0

    run_command("RESET pg_lake_table.enable_insert_select_pushdown", pg_conn)


# DuckDB writes at most this many rows per Parquet row group by default
# (row_group_size). When target_file_size_mb is smaller than the compressed
# size of a single such row group, DuckDB rotates to a new file after every
# row group. If the number of rows written is an exact multiple of the row
# group size, the final rotation opens one more file that never receives a
# row, leaving a trailing zero-row Parquet file (valid footer, no row groups).
# Snowflake's Iceberg reader rejects such files ("Invalid parquet file with no
# data (zero rowgroups)"), so DropAndQueueEmptyDataFiles must keep them out of
# the manifest while still queuing them for deletion. These tests reproduce the
# split boundary deterministically for both write paths.
DUCKDB_DEFAULT_ROW_GROUP_SIZE = 122880


def _assert_empty_split_file_dropped_and_queued(
    conn, pgduck_conn, table_fqn, expected_rows
):
    """Assert a split write left a trailing zero-row file that was kept out of
    lake_table.files but queued for deletion, without losing any rows."""

    # Only non-empty files are registered, and together they hold every row.
    files = run_query(
        f"""SELECT path, row_count FROM lake_table.files
            WHERE table_name = '{table_fqn}'::regclass ORDER BY path""",
        conn,
    )
    assert len(files) >= 1
    assert all(row["row_count"] > 0 for row in files)
    assert sum(row["row_count"] for row in files) == expected_rows

    # The trailing zero-row file must be queued for deletion instead of being
    # registered. It is queued as the concrete data file path, not a prefix.
    queued = run_query(
        f"""SELECT path FROM lake_engine.deletion_queue
            WHERE table_name = '{table_fqn}'::regclass AND path LIKE '%.parquet'
            ORDER BY path""",
        conn,
    )
    assert len(queued) >= 1

    registered_paths = {row["path"] for row in files}
    for row in queued:
        # a queued file must not be registered ...
        assert row["path"] not in registered_paths
        # ... and must really contain zero rows.
        empty = run_query(
            f"SELECT count(*) FROM read_parquet('{row['path']}')", pgduck_conn
        )
        assert empty[0][0] == 0

    # The table still reads back exactly the rows we inserted.
    total = run_query(f"SELECT count(*) FROM {table_fqn}", conn)
    assert total[0][0] == expected_rows


# The trailing empty split file can be produced on either write path:
#  - pushdown=True:  INSERT .. SELECT is pushed to pgduck, exercising
#    PrepareToAddQueryResultToTable. We use an Iceberg table, the real
#    Snowflake CDC scenario that triggered this bug.
#  - pushdown=False: with insert-select pushdown disabled the write goes
#    row-at-a-time through MultiDataFileUploadDestReceiver ->
#    PrepareCSVInsertion. Splitting there requires reservedRowIdStart == 0, so
#    we use a plain (non-Iceberg) pg_lake table, which does not reserve row ids.
@pytest.mark.parametrize("pushdown", [True, False], ids=["pushdown", "non_pushdown"])
def test_remove_unreferenced_files_14_empty_split_file(
    s3, superuser_conn, extension, with_default_location, pgduck_conn, pushdown
):
    run_command("DROP SCHEMA IF EXISTS test_empty_split_nsp CASCADE", superuser_conn)
    run_command("CREATE SCHEMA test_empty_split_nsp", superuser_conn)
    superuser_conn.commit()

    try:
        if pushdown:
            run_command(
                "CREATE TABLE test_empty_split_nsp.t (v text) USING pg_lake_iceberg",
                superuser_conn,
            )
        else:
            location = f"s3://{TEST_BUCKET}/test_empty_split_non_pushdown/"
            run_command(
                f"""CREATE FOREIGN TABLE test_empty_split_nsp.t (v text)
                    SERVER pg_lake
                    OPTIONS (writable 'true', format 'parquet', location '{location}')""",
                superuser_conn,
            )
            run_command(
                "SET pg_lake_table.enable_insert_select_pushdown TO off",
                superuser_conn,
            )

        # A target below one row group's compressed size forces DuckDB to
        # rotate files after every row group; requires superuser to go under
        # the 16MB floor.
        run_command("SET pg_lake_table.target_file_size_mb TO '3MB'", superuser_conn)

        insert = (
            "INSERT INTO test_empty_split_nsp.t "
            f"SELECT md5(i::text) FROM generate_series(1,{DUCKDB_DEFAULT_ROW_GROUP_SIZE}) i"
        )

        # Confirm we are actually exercising the intended write path.
        explain = run_query(f"EXPLAIN (VERBOSE) {insert}", superuser_conn)
        if pushdown:
            assert "Custom Scan (Query Pushdown)" in str(explain)
        else:
            assert "Custom Scan (Query Pushdown)" not in str(explain)

        run_command(insert, superuser_conn)
        superuser_conn.commit()

        _assert_empty_split_file_dropped_and_queued(
            superuser_conn,
            pgduck_conn,
            "test_empty_split_nsp.t",
            DUCKDB_DEFAULT_ROW_GROUP_SIZE,
        )
    finally:
        run_command("RESET pg_lake_table.target_file_size_mb", superuser_conn)
        run_command("RESET pg_lake_table.enable_insert_select_pushdown", superuser_conn)
        run_command(
            "DROP SCHEMA IF EXISTS test_empty_split_nsp CASCADE", superuser_conn
        )
        superuser_conn.commit()


# Sequence number to generate unique table names
table_counter = 0


# we need to generate unique table names
# otherwise we cannot call assert_iceberg_s3_file_consistency()
# as the s3 bucket would have artifacts from earlier tests
@pytest.fixture
def generate_table_name():
    global table_counter
    table_counter += 1

    TEST_TABLE_NAME = "test_iceberg_remove_unreferenced_files_" + str(table_counter)

    return f"{TEST_TABLE_NAME}_" + str(table_counter)


@pytest.fixture
def create_iceberg_table(pg_conn, with_default_location, generate_table_name):
    table_name = generate_table_name  # Get the generated table name

    # Create schema and table
    run_command(f"CREATE SCHEMA {TEST_TABLE_NAMESPACE}", pg_conn)
    run_command(
        f"""
                    CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name} (id int) USING pg_lake_iceberg;
                    ALTER FOREIGN TABLE {TEST_TABLE_NAMESPACE}.{table_name} OPTIONS (ADD autovacuum_enabled 'false');
                """,
        pg_conn,
    )

    pg_conn.commit()

    yield table_name  # Yield the table name for further operations in the test

    # Rollback and clean up after test
    pg_conn.rollback()
    run_command(f"DROP SCHEMA {TEST_TABLE_NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn):

    run_command(
        f"""

        -- find_all_referenced_files is owned by pg_lake_iceberg and REVOKEd
        -- from public by the migration; just grant EXECUTE back for tests.
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files(text) TO public;

        CREATE OR REPLACE FUNCTION lake_iceberg.find_unreferenced_files(previous_paths text[], current_path text, OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_unreferenced_files$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_unreferenced_files(previous_paths text[], current_path text, OUT path text) TO public;

""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the function after the test(s) are done.
    # find_all_referenced_files is installed by pg_lake_iceberg, so it must
    # not be dropped here (it belongs to the extension).
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.find_unreferenced_files;

""",
        superuser_conn,
    )
    superuser_conn.commit()

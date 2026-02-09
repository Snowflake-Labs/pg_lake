import pytest
import psycopg2
from utils_pytest import *
import json
import re

from test_writable_iceberg_common import *


def test_writable_iceberg_create_table(pg_conn, s3, extension):
    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_create_table;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_writable_iceberg_create_table"
    # should be able to create a writable table with location
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_create_table.ft1 (
                    id int,
                    value text[]
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )

    # show that we can read empty tables
    results = run_query(
        "SELECT count(*) FROM test_writable_iceberg_create_table.ft1", pg_conn
    )
    assert results[0][0] == 0

    run_command("DROP SCHEMA test_writable_iceberg_create_table CASCADE", pg_conn)
    pg_conn.commit()


def test_writable_iceberg_create_table_no_cols(pg_conn, s3, extension):
    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_create_table_no_cols;
                """,
        pg_conn,
    )

    location = "s3://" + TEST_BUCKET + "/test_writable_iceberg_create_table_no_cols"
    # should be able to create a writable table with location
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_create_table_no_cols.ft1 (
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )

    error = run_query(
        "SELECT count(*) FROM test_writable_iceberg_create_table_no_cols.ft1",
        pg_conn,
        raise_error=False,
    )
    assert "SELECT clause without selection list" in str(error)
    pg_conn.rollback()

    # now, make sure we do not leak any intermediate files even if we rollback
    assert_postgres_tmp_folder_empty()


def test_writable_iceberg_create_wrong_option(pg_conn, extension):

    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_create_table;
                """,
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        """CREATE FOREIGN TABLE test_writable_iceberg_create_table.ft1 (
                    id int,
                    value text[]
                ) SERVER pg_lake_iceberg
    """,
        pg_conn,
        raise_error=False,
    )
    assert '"location" option is required for pg_lake_iceberg tables' in str(error)
    pg_conn.rollback()

    error = run_command(
        """CREATE FOREIGN TABLE test_writable_iceberg_create_table.ft1 (
                    id int,
                    value text[]
                ) SERVER pg_lake_iceberg OPTIONS (path 's3://somebucket');
    """,
        pg_conn,
        raise_error=False,
    )
    assert 'invalid option "path"' in str(error)
    pg_conn.rollback()

    error = run_command(
        """CREATE FOREIGN TABLE test_writable_iceberg_create_table.ft1 (
                    id int,
                    value text[]
                ) SERVER pg_lake_iceberg OPTIONS (location 'file://somebucket');
    """,
        pg_conn,
        raise_error=False,
    )
    assert (
        'pg_lake_iceberg: only s3://, gs://, az://, azure://, and abfss:// URLs are currently supported for the "location" option.'
        in str(error)
    )
    pg_conn.rollback()

    run_command("DROP SCHEMA test_writable_iceberg_create_table CASCADE", pg_conn)
    pg_conn.commit()


def test_writable_iceberg_table_failure(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
):

    TABLE_NAME = create_iceberg_table

    # throw error with SELECT
    query = f"SELECT count(*)/0 FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn, raise_error=False)
    pg_conn.rollback()

    # throw error with INSERT
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1/0, 'test')",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # throw error with UPDATE
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1, 'test')",
        pg_conn,
        raise_error=False,
    )
    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 1/0",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # now, make sure we do not leak anything
    assert_postgres_tmp_folder_empty()

    run_command_outside_tx([f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}"], pg_conn)
    run_command(
        f"BEGIN;SELECT lake_engine.flush_deletion_queue('{TABLE_NAMESPACE}.{TABLE_NAME}'::regclass); COMMIT;",
        pg_conn,
    )

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)


def test_writable_iceberg_table_manifests(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    create_test_helper_functions,
    superuser_conn,
    create_iceberg_table,
):

    TABLE_NAME = create_iceberg_table

    # generate some data, so some positional delete and copy-on-write deletes
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 0", pg_conn)
    pg_conn.commit()

    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 1", pg_conn)
    pg_conn.commit()

    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id < 82", pg_conn)
    pg_conn.commit()

    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 82", pg_conn)
    pg_conn.commit()

    # first, check the result
    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
    assert results[0][0] == 17
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    results = run_query(
        f"SELECT sequence_number, added_files_count, added_rows_count, existing_files_count, existing_rows_count, deleted_files_count,deleted_rows_count FROM lake_iceberg.current_manifests('{metadata_location}') ORDER BY sequence_number, added_files_count, added_rows_count, deleted_files_count,deleted_rows_count ASC",
        pg_conn,
    )

    assert results == [[4, 1, 18, 0, 0, 0, 0], [5, 1, 1, 0, 0, 0, 0]]

    run_command_outside_tx([f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}"], superuser_conn)

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)


def test_writable_iceberg_table_manifests_on_truncate(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    azure,
    extension,
    create_test_helper_functions,
    allow_iceberg_guc_perms,
):
    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_table_manifests_on_truncate;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = (
        "az://" + TEST_BUCKET + "/test_writable_iceberg_table_manifests_on_truncate"
    )
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_table_manifests_on_truncate.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_table_manifests_on_truncate.ft2 (
                    id int,
                    value text
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}_2');
    """,
        pg_conn,
    )

    # generate some data, so some positional delete and copy-on-write deletes
    run_command(
        "INSERT INTO test_writable_iceberg_table_manifests_on_truncate.ft1 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_table_manifests_on_truncate.ft1 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "TRUNCATE test_writable_iceberg_table_manifests_on_truncate.ft1", pg_conn
    )
    pg_conn.commit()

    query = "SELECT count(*) FROM test_writable_iceberg_table_manifests_on_truncate.ft1"
    results = run_query(query, pg_conn)
    assert results[0][0] == 0

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_writable_iceberg_table_manifests_on_truncate'",
        pg_conn,
    )[0][0]
    results = run_query(
        f"SELECT sequence_number, added_files_count, added_rows_count, existing_files_count, existing_rows_count, deleted_files_count,deleted_rows_count FROM lake_iceberg.current_manifests('{metadata_location}') ORDER BY sequence_number, added_files_count, added_rows_count, deleted_files_count,deleted_rows_count ASC",
        pg_conn,
    )

    # two files each 100 rows deleted
    assert results == [[2, 0, 0, 0, 0, 1, 100], [2, 0, 0, 0, 0, 1, 100]]

    # now same test with DELETE FROM
    run_command(
        "INSERT INTO test_writable_iceberg_table_manifests_on_truncate.ft2 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_table_manifests_on_truncate.ft2 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "DELETE FROM test_writable_iceberg_table_manifests_on_truncate.ft2", pg_conn
    )
    pg_conn.commit()

    query = "SELECT count(*) FROM test_writable_iceberg_table_manifests_on_truncate.ft2"
    results = run_query(query, pg_conn)
    assert results[0][0] == 0

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft2' and table_namespace='test_writable_iceberg_table_manifests_on_truncate'",
        pg_conn,
    )[0][0]
    results = run_query(
        f"SELECT sequence_number, added_files_count, added_rows_count, existing_files_count, existing_rows_count, deleted_files_count,deleted_rows_count FROM lake_iceberg.current_manifests('{metadata_location}') ORDER BY sequence_number, added_files_count, added_rows_count, deleted_files_count,deleted_rows_count ASC",
        pg_conn,
    )

    # two files each 100 rows deleted
    assert results == [[3, 0, 0, 0, 0, 1, 100], [3, 0, 0, 0, 0, 1, 100]]


def test_writable_iceberg_table_manifest_entries(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    create_test_helper_functions,
    superuser_conn,
    create_iceberg_table,
):

    TABLE_NAME = create_iceberg_table

    # generate some data, so some positional delete and copy-on-write deletes
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT i, i::text FROM generate_series(0,100)i",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT snapshot_id, sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )

    first_snapshot_id = manifest_entries[0][0]
    assert [entry[1:] for entry in manifest_entries] == [[1, "ADDED"]]

    # trigger rewrite
    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id < 50", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT snapshot_id, sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )

    # all entries has the same snapshot id
    second_snapshot_id = manifest_entries[0][0]
    assert second_snapshot_id != first_snapshot_id
    assert all(entry[0] == manifest_entries[0][0] for entry in manifest_entries)
    assert [entry[1:] for entry in manifest_entries] == [[1, "DELETED"], [2, "ADDED"]]

    # insert some more data
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT i, i::text FROM generate_series(101,200)i",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT snapshot_id, sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )

    # deleted entry from previous snapshot is removed, and new entry is added into new snapshot
    third_snapshot_id = manifest_entries[0][1]
    assert third_snapshot_id != second_snapshot_id
    assert not all(entry[0] == manifest_entries[0][0] for entry in manifest_entries)
    assert [entry[1:] for entry in manifest_entries] == [[2, "ADDED"], [3, "ADDED"]]

    run_command_outside_tx(
        [f"VACUUM FULL {TABLE_NAMESPACE}.{TABLE_NAME}"], superuser_conn
    )

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT snapshot_id, sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )

    # after vacuum we should have 1 manifest entry due to data file compaction
    fourth_snapshot_id = manifest_entries[0][2]
    assert fourth_snapshot_id != third_snapshot_id
    assert [entry[1:] for entry in manifest_entries] == [[4, "ADDED"]]

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)


def test_writable_iceberg_table_metadata_logs(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    create_test_helper_functions,
    allow_iceberg_guc_perms,
):

    # test relies on some snapshots to exists
    run_command(
        f"""
            SET pg_lake_iceberg.max_snapshot_age TO 100000;
        """,
        pg_conn,
    )

    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_table_metadata_logs;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_writable_iceberg_table_metadata_logs"
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_table_metadata_logs.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )

    # generate some data, so some positional delete and copy-on-write deletes
    run_command(
        "INSERT INTO test_writable_iceberg_table_metadata_logs.ft1 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_table_metadata_logs.ft1 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command("TRUNCATE test_writable_iceberg_table_metadata_logs.ft1", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_writable_iceberg_table_metadata_logs'",
        pg_conn,
    )[0][0]

    refs_command = f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_location}')::json->'refs'"
    result = run_query(refs_command, pg_conn)
    refs = result[0][0]
    assert "main" in refs
    assert refs["main"]["type"] == "branch"

    metadata_logs_command = f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_location}')::json->'metadata-log'"
    result = run_query(metadata_logs_command, pg_conn)
    metadata_logs = result[0][0]

    assert len(metadata_logs) == 2
    for i in range(0, 2):
        assert metadata_logs[i]["metadata-file"].endswith(".metadata.json")
        assert metadata_logs[i]["timestamp-ms"] > 0

    snapshot_logs_command = f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_location}')::json->'snapshot-log'"
    result = run_query(snapshot_logs_command, pg_conn)
    snapshot_logs = result[0][0]
    assert len(snapshot_logs) == 3
    for i in range(0, 3):
        assert snapshot_logs[i]["snapshot-id"] > 0
        assert snapshot_logs[i]["timestamp-ms"] > 0
    pg_conn.rollback()

    run_command(
        f"""
        RESET pg_lake_iceberg.max_snapshot_age;
    """,
        pg_conn,
    )
    pg_conn.commit()


def test_writable_iceberg_table_parent_snapshot_id(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    s3,
    duckdb_conn,
    extension,
    create_test_helper_functions,
):
    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_table_parent_snapshot_id;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_writable_iceberg_table_parent_snapshot_id"
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_table_parent_snapshot_id.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}', autovacuum_enabled 'false');
    """,
        pg_conn,
    )

    # generate some data, so some positional delete and copy-on-write deletes
    run_command(
        "INSERT INTO test_writable_iceberg_table_parent_snapshot_id.ft1 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_table_parent_snapshot_id.ft1 SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    run_command("TRUNCATE test_writable_iceberg_table_parent_snapshot_id.ft1", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_writable_iceberg_table_parent_snapshot_id'",
        pg_conn,
    )[0][0]

    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    current_snapshot_id = metadata_json["current-snapshot-id"]
    snapshots = metadata_json["snapshots"]
    current_snapshot = list(
        filter(lambda x: x["snapshot-id"] == current_snapshot_id, snapshots)
    )[0]
    parent_snapshot_id = current_snapshot["parent-snapshot-id"]

    assert parent_snapshot_id > 0

    run_command(
        """
                DROP SCHEMA test_writable_iceberg_table_parent_snapshot_id CASCADE
                """,
        pg_conn,
    )
    pg_conn.commit()


def test_writable_iceberg_snapshot_count(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    create_test_helper_functions,
    allow_iceberg_guc_perms,
):
    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_snapshot_count;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_writable_iceberg_snapshot_count"
    run_command(
        f"""CREATE FOREIGN TABLE test_writable_iceberg_snapshot_count.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}', autovacuum_enabled 'false');

               SET pg_lake_iceberg.max_snapshot_age TO 10000000;

    """,
        pg_conn,
    )

    # generate 5 snapshots for inserts, 1 for update, 1 for delete, 1 for TRUNCATE and 1 for VACUUM
    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (1)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (2)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (3)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (4)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (5)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "UPDATE test_writable_iceberg_snapshot_count.ft1 SET id = 7 WHERE id = 1",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "DELETE FROM test_writable_iceberg_snapshot_count.ft1 WHERE id = 1 OR id = 7",
        pg_conn,
    )
    pg_conn.commit()
    run_command_outside_tx(
        [
            "SET pg_lake_table.vacuum_compact_min_input_files TO 2;",
            "SET pg_lake_iceberg.max_snapshot_age TO 10000000;;",
            "VACUUM (FULL) test_writable_iceberg_snapshot_count.ft1",
        ]
    )

    run_command("TRUNCATE test_writable_iceberg_snapshot_count.ft1", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_writable_iceberg_snapshot_count'",
        pg_conn,
    )[0][0]

    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    current_snapshot_id = metadata_json["current-snapshot-id"]
    snapshots = metadata_json["snapshots"]
    snapshot_logs = metadata_json["snapshot-log"]
    metadata_logs = metadata_json["metadata-log"]

    assert len(snapshots) == 10

    assert len(snapshot_logs) == 10
    assert_snapshot_log_order(snapshot_logs)

    assert len(metadata_logs) == 9
    assert_metadata_log_order(metadata_logs)

    # lets add a test for throwing away old snapshots
    run_command(
        f"""
               INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (1);
    """,
        pg_conn,
    )

    # trigger snapshot retention
    pg_conn.commit()

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_table.vacuum_compact_min_input_files TO 2;",
        "SET search_path TO test_writable_iceberg_table_tx",
        "SET pg_lake_iceberg.max_snapshot_age TO 0;",
        "VACUUM test_writable_iceberg_snapshot_count.ft1;",
    ]
    run_command_outside_tx(vacuum_commands)

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_writable_iceberg_snapshot_count'",
        pg_conn,
    )[0][0]
    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    current_snapshot_id = metadata_json["current-snapshot-id"]
    snapshots = metadata_json["snapshots"]
    snapshot_logs = metadata_json["snapshot-log"]
    metadata_logs = metadata_json["metadata-log"]

    assert len(snapshots) == 1
    assert len(snapshot_logs) == 1
    assert len(metadata_logs) == 1

    # show that until a VACUUM, we do not apply snapshot retention
    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (1)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (2)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (3)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (4)", pg_conn
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO test_writable_iceberg_snapshot_count.ft1 VALUES (5)", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_writable_iceberg_snapshot_count'",
        pg_conn,
    )[0][0]
    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    snapshots = metadata_json["snapshots"]
    snapshot_logs = metadata_json["snapshot-log"]
    metadata_logs = metadata_json["metadata-log"]

    # done 5 inserts, so end up with 6 snapshots
    assert len(snapshots) == 6

    assert len(snapshot_logs) == 6
    assert_snapshot_log_order(snapshot_logs)

    assert len(metadata_logs) == 6
    assert_metadata_log_order(metadata_logs)

    run_command(
        """
                DROP SCHEMA test_writable_iceberg_snapshot_count CASCADE;
                RESET pg_lake_iceberg.max_snapshot_age;
                """,
        pg_conn,
    )
    pg_conn.commit()


def test_inherits_with_correlated_subquery(
    pg_conn, s3, extension, with_default_location, create_test_helper_functions
):

    for table_type in ["iceberg", "heap"]:
        run_command(
            f"""
            CREATE SCHEMA test_inherits_{table_type};
            SET search_path TO test_inherits_{table_type};
            SET default_table_access_method TO '{table_type}';

            CREATE TABLE t1 (a int, b float, c text);
            INSERT INTO t1
            SELECT i,i,'t1' FROM generate_series(1,10) g(i);

            CREATE TABLE t11 (a int, b float, c text, d text);
            INSERT INTO t11
            SELECT i,i,'t11','t11d' FROM generate_series(1,10) g(i);

            CREATE TABLE t12 (a int, b float, c text, d text,e int[]);
            INSERT INTO t12
            SELECT i,i,'t12', array[1,2] FROM generate_series(1,10) g(i);

            CREATE TABLE t111 () INHERITS (t11, t12);
            INSERT INTO t111
            SELECT i,i,'t111','t111d',array[1,1,1] FROM generate_series(1,10) g(i);

        """,
            pg_conn,
        )

    heap_result = run_query(
        """
        SELECT *, (SELECT d FROM test_inherits_heap.t11 WHERE t11.a = t1.a ORDER BY d LIMIT 1) AS d
        FROM test_inherits_heap.t1
        WHERE a > 5
        ORDER BY a
    """,
        pg_conn,
    )

    iceberg_result = run_query(
        """
        SELECT *, (SELECT d FROM test_inherits_iceberg.t11 WHERE t11.a = t1.a ORDER BY d LIMIT 1) AS d
        FROM test_inherits_iceberg.t1
        WHERE a > 5
        ORDER BY a
    """,
        pg_conn,
    )

    assert len(iceberg_result) == 5
    assert heap_result == iceberg_result

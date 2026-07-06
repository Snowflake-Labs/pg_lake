import pytest
from utils_pytest import *

TEST_TABLE_NAME = "test_referenced_files"


# insert and positional update/delete
def test_referenced_files_1(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    # an empty table still has one entry, which is the metadata file itself
    referenced_files = get_referenced_files(pg_conn)
    assert len(referenced_files) == 1
    assert ".metadata.json" in referenced_files[0][0]

    # insert the first row, now we should have
    # metadata file, manifest_list, manifest, and data file
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 4
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "-m0.avro" in referenced_files[2][0]
    assert ".parquet" in referenced_files[3][0]

    # now, lets do a positional delete, we should have
    # 1 deletion file as well as a new manifest file for content deletes
    run_command(f"DELETE FROM {TEST_TABLE_NAME} WHERE id = 0", pg_conn)
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 7
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[2][0]
    assert "-m0.avro" in referenced_files[3][0]  # data manifest
    assert "-m0.avro" in referenced_files[4][0]  # delete manifest
    assert ".parquet" in referenced_files[5][0]  # data file
    assert ".parquet" in referenced_files[6][0]  # deletion file

    # now, lets do a positional update, we should have
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = 1000 WHERE id = 1", pg_conn)
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 12
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[2][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[3][0]

    assert "-m0.avro" in referenced_files[4][0]  # data manifest
    assert "-m0.avro" in referenced_files[5][0]  # delete manifest

    assert "-m0.avro" in referenced_files[6][0]  # data manifest
    assert "-m1.avro" in referenced_files[7][0]  # delete manifest

    assert ".parquet" in referenced_files[8][0]  # data file
    assert ".parquet" in referenced_files[9][0]  # data file

    assert ".parquet" in referenced_files[10][0]  # deletion file
    assert ".parquet" in referenced_files[11][0]  # deletion file

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    pg_conn.commit()


# insert, copy on write update/deletes
def test_referenced_files_2(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    # an empty table still has one entry, which is the metadata file itself
    referenced_files = get_referenced_files(pg_conn)
    assert len(referenced_files) == 1
    assert ".metadata.json" in referenced_files[0][0]

    # insert the first row, now we should have
    # metadata file, manifest_list, manifest, and data file
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 4
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "-m0.avro" in referenced_files[2][0]
    assert ".parquet" in referenced_files[3][0]

    # now, delete half of the rows
    # will trigger re-write of the table
    run_command(f"DELETE FROM {TEST_TABLE_NAME} WHERE id < 50", pg_conn)
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 8
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "snap-" in referenced_files[2][0] and ".avro" in referenced_files[2][0]

    assert "-m0.avro" in referenced_files[3][0]
    assert "-m0.avro" in referenced_files[4][0]
    assert "-m1.avro" in referenced_files[5][0]
    assert ".parquet" in referenced_files[6][0]
    assert ".parquet" in referenced_files[7][0]

    # now, update half of the rows
    # will trigger re-write of the table
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = 1000+id WHERE id < 75", pg_conn)
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 13
    assert ".parquet" in referenced_files[10][0]
    assert (
        ".parquet" in referenced_files[11][0]
        and "data_0" not in referenced_files[11][0]
    )
    assert (
        ".parquet" in referenced_files[12][0]
        and "data_0" not in referenced_files[12][0]
    )

    # re-write the table, so only have data file, no deletion files anymor
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_table.vacuum_compact_min_input_files = 1;",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        f"VACUUM FULL {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    referenced_files = get_referenced_files(pg_conn)

    assert len(referenced_files) == 4
    assert ".avro" in referenced_files[2][0]
    assert ".parquet" in referenced_files[3][0]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    pg_conn.commit()


# test with manifest compaction
def test_referenced_files_3(
    s3, pg_conn, extension, create_iceberg_table, create_helper_functions
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 2", pg_conn)

    # adding two manifest files should trigger manifest compaction
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    pg_conn.commit()

    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 4
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "-m0.avro" in referenced_files[2][0]
    assert ".parquet" in referenced_files[3][0]

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    pg_conn.commit()

    # an empty table still has one entry, which is the metadata file itself
    referenced_files = get_referenced_files(pg_conn)
    referenced_files_via_snapshot = get_referenced_files_via_all_snapshots(pg_conn)
    assert referenced_files_via_snapshot == referenced_files

    assert len(referenced_files) == 7
    assert ".metadata.json" in referenced_files[0][0]
    assert "snap-" in referenced_files[1][0] and ".avro" in referenced_files[1][0]
    assert "snap-" in referenced_files[2][0] and ".avro" in referenced_files[2][0]

    assert "-m0.avro" in referenced_files[3][0]
    assert "-m1.avro" in referenced_files[4][0]

    assert ".parquet" in referenced_files[5][0]
    assert ".parquet" in referenced_files[6][0]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    pg_conn.commit()


def get_referenced_files(pg_conn):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]

    referenced_files = run_query(
        f"""SELECT * FROM lake_iceberg.find_all_referenced_files('{metadata_location}')""",
        pg_conn,
    )

    # get consistent results
    referenced_files.sort(key=file_sort_key)

    return referenced_files


def get_referenced_files_via_all_snapshots(pg_conn):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    snapshot_ids = run_query(
        f"""SELECT array_agg(snapshot_id) FROM lake_iceberg.snapshots('{metadata_location}')""",
        pg_conn,
    )[0][0]
    referenced_files = run_query(
        f"""SELECT * FROM lake_iceberg.find_all_referenced_files_via_snapshot_ids('{TEST_TABLE_NAME}'::regclass, ARRAY{snapshot_ids})""",
        pg_conn,
    )
    referenced_files.append([f"{metadata_location}"])

    # get consistent results
    referenced_files.sort(key=file_sort_key)

    return referenced_files


@pytest.fixture
def create_iceberg_table(pg_conn, with_default_location):
    run_command(f"CREATE TABLE {TEST_TABLE_NAME} (id int) USING iceberg", pg_conn)
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS (ADD autovacuum_enabled 'false')",
        pg_conn,
    )

    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP FOREIGN TABLE {TEST_TABLE_NAME}", pg_conn)
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
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_unreferenced_files(previous_paths text[], current_path text, OUT path text) TO lake_read;

        CREATE OR REPLACE FUNCTION lake_iceberg.find_all_referenced_files_via_snapshot_ids(table_name regclass,snapshot_id bigint[], OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_all_referenced_files_via_snapshot_ids$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files_via_snapshot_ids(table_name regclass,snapshot_id bigint[], OUT path text) TO lake_read;



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
        DROP FUNCTION lake_iceberg.find_all_referenced_files_via_snapshot_ids;

""",
        superuser_conn,
    )
    superuser_conn.commit()

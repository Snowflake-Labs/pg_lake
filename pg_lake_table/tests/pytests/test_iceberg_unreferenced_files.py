import pytest
from utils_pytest import *

TEST_TABLE_NAME = "test_unreferenced_files"


# insert and VACUUM FULL
def test_unreferenced_files_1(
    s3, pg_conn, extension, create_helper_functions, create_iceberg_table
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    # we don't want every test to have data file compaction for test readability
    # purposes
    run_command("SET pg_lake_table.target_file_size_mb TO -1", pg_conn)

    # insert the first batch, now we should have
    # only have the previous metadata.json missing
    # given there are no snapshots yet
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    # trigger snapshot retention
    pg_conn.commit()

    vacuum_commands = [
        "SET pg_lake_table.vacuum_compact_min_input_files=1",
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert len(unreferenced_files) == 1
    assert ".metadata.json" in unreferenced_files[0][0]

    # get snapshot ids before VACUUM, as VACUUM will expire them
    snapshot_id_1 = get_current_snapshot_id(pg_conn)

    # for the second INSERT, we'd also have a new snapshot and
    # so the manifest_list from the previous snapshot is gone
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )

    # trigger snapshot retention
    pg_conn.commit()
    snapshot_id_2 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_1], [snapshot_id_2]
    )

    vacuum_commands = [
        "SET pg_lake_table.vacuum_compact_min_input_files=1",
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert [
        [metadata_location]
    ] + unreferenced_files_via_snapshots == unreferenced_files
    assert len(unreferenced_files) == 2
    assert ".metadata.json" in unreferenced_files[0][0]
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # now, lets do a VACUUM FULL, which will re-write the table hence getting rid
    # of all the existing metadata
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    prev_metadata_list = [metadata_location]

    # trigger snapshot retention and this test relies on data file compaction
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_table.vacuum_compact_min_input_files=1",
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "RESET pg_lake_table.target_file_size_mb",
        f"VACUUM FULL {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=prev_metadata_list
    )
    assert len(unreferenced_files) == 6

    # then the metadata.json file as usual
    assert ".metadata.json" in unreferenced_files[0][0]

    # then the snapshot
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # then two manifests
    assert "-m0.avro" in unreferenced_files[2][0]
    assert "-m0.avro" in unreferenced_files[3][0]

    # first, 2 data files will be gone
    assert ".parquet" in unreferenced_files[4][0]
    assert ".parquet" in unreferenced_files[5][0]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)

    pg_conn.rollback()


# positional DELETE followed by copy-on-write DELETE
def test_unreferenced_files_2(
    s3, pg_conn, extension, create_helper_functions, create_iceberg_table
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    # we don't want every test to have data file compaction for test readability
    # purposes
    run_command("SET pg_lake_table.target_file_size_mb TO -1", pg_conn)

    # insert the first batch, now we should have
    # only have the previous metadata.json missing
    # given there are no snapshots yet
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )

    # trigger snapshot retention
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert len(unreferenced_files) == 1
    assert ".metadata.json" in unreferenced_files[0][0]

    # get snapshot ids before VACUUM, as VACUUM will expire them
    snapshot_id_1 = get_current_snapshot_id(pg_conn)

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(f"DELETE FROM {TEST_TABLE_NAME} WHERE id = 0", pg_conn)
    pg_conn.commit()

    snapshot_id_2 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_1], [snapshot_id_2]
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert [
        [metadata_location]
    ] + unreferenced_files_via_snapshots == unreferenced_files

    # positional DELETE would not make any data file unreferenced, only
    # adding one positinal delete file, hence we only have metadata/snapshot
    # files as the diff
    assert len(unreferenced_files) == 2
    assert ".metadata.json" in unreferenced_files[0][0]
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    snapshot_id_3 = get_current_snapshot_id(pg_conn)

    # now, copy-on-write delete
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(f"DELETE FROM {TEST_TABLE_NAME} WHERE id <50 ", pg_conn)
    pg_conn.commit()

    snapshot_id_4 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_3], [snapshot_id_4]
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert [
        [metadata_location]
    ] + unreferenced_files_via_snapshots == unreferenced_files

    assert len(unreferenced_files) == 6

    # then the metadata.json file as usual
    assert ".metadata.json" in unreferenced_files[0][0]
    # the snapshot
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # then two manifests, one data manifest one delete manifest
    assert "-m0.avro" in unreferenced_files[2][0]
    assert "-m0.avro" in unreferenced_files[3][0]

    # finally, the data and positional delete files will be gone
    assert ".parquet" in unreferenced_files[4][0]
    assert ".parquet" in unreferenced_files[5][0]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    run_command("RESET pg_lake_iceberg.target_file_size_mb", pg_conn)
    pg_conn.commit()


# positional UPDATE followed by copy-on-write UPDATE
def test_unreferenced_files_3(
    s3, pg_conn, extension, create_helper_functions, create_iceberg_table
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    # we don't want every test to have data file compaction for test readability
    # purposes
    run_command("SET pg_lake_table.target_file_size_mb TO -1", pg_conn)

    # insert the first batch, now we should have
    # only have the previous metadata.json missing
    # given there are no snapshots yet
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )

    # trigger snapshot retention
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )

    assert len(unreferenced_files) == 1
    assert ".metadata.json" in unreferenced_files[0][0]

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]

    # get snapshot ids before VACUUM, as VACUUM will expire them
    snapshot_id_1 = get_current_snapshot_id(pg_conn)

    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = 1 WHERE id = 0", pg_conn)
    pg_conn.commit()

    snapshot_id_2 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_1], [snapshot_id_2]
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert [
        [metadata_location]
    ] + unreferenced_files_via_snapshots == unreferenced_files

    # positional UPDATE would not make any data file unreferenced, only
    # adding one positinal delete file and one data file, hence we only
    # have metadata/snapshot files as the diff
    assert len(unreferenced_files) == 2
    assert ".metadata.json" in unreferenced_files[0][0]
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # now, copy-on-write update
    snapshot_id_3 = get_current_snapshot_id(pg_conn)
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = 1000 WHERE id <50 ", pg_conn)
    pg_conn.commit()

    snapshot_id_4 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_3], [snapshot_id_4]
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert [
        [metadata_location]
    ] + unreferenced_files_via_snapshots == unreferenced_files

    assert len(unreferenced_files) == 8

    # then the metadata.json file as usual
    assert ".metadata.json" in unreferenced_files[0][0]

    #  the snapshot
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # then three manifests, one data manifest one delete manifest, then another data manifest
    assert "avro" in unreferenced_files[2][0] and "-m" in unreferenced_files[2][0]
    assert "avro" in unreferenced_files[3][0] and "-m" in unreferenced_files[3][0]
    assert "avro" in unreferenced_files[4][0] and "-m" in unreferenced_files[4][0]

    # then 2 data files
    assert ".parquet" in unreferenced_files[5][0]
    assert ".parquet" in unreferenced_files[6][0]

    # finally, positional delete files will be gone
    assert ".parquet" in unreferenced_files[7][0]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    run_command("RESET pg_lake_iceberg.target_file_size_mb", pg_conn)
    pg_conn.commit()


# insert and TRUNCATE
def test_unreferenced_files_4(
    s3, pg_conn, extension, create_helper_functions, create_iceberg_table
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    # we don't want every test to have data file compaction for test readability
    # purposes
    run_command("SET pg_lake_table.target_file_size_mb TO -1", pg_conn)

    # insert the first batch, now we should have
    # only have the previous metadata.json missing
    # given there are no snapshots yet
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    # trigger snapshot retention
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert len(unreferenced_files) == 1
    assert ".metadata.json" in unreferenced_files[0][0]

    # for the second INSERT, we'd also have a new snapshot and
    # so the manifest_list from the previous snapshot is gon
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )

    # trigger snapshot retention
    pg_conn.commit()
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert len(unreferenced_files) == 2
    assert ".metadata.json" in unreferenced_files[0][0]
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # get snapshot ids before VACUUM, as VACUUM will expire them
    snapshot_id_1 = get_current_snapshot_id(pg_conn)

    # now, lets do a VACUUM FULL, which will re-write the table hence getting rid
    # of all the existing metadata
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    run_command(f"TRUNCATE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()

    # get snapshot ids before VACUUM, as VACUUM will expire them
    snapshot_id_2 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_1], [snapshot_id_2]
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        "SET pg_lake_table.target_file_size_mb TO -1",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=[metadata_location]
    )
    assert [
        [metadata_location]
    ] + unreferenced_files_via_snapshots == unreferenced_files

    assert len(unreferenced_files) == 6

    # then the metadata.json file as usual
    assert ".metadata.json" in unreferenced_files[0][0]

    # then the snapshot
    assert "snap-" in unreferenced_files[1][0] and ".avro" in unreferenced_files[1][0]

    # then two manifests
    assert "-m0.avro" in unreferenced_files[2][0]
    assert "-m0.avro" in unreferenced_files[3][0]

    # first, 2 data files will be gone
    assert ".parquet" in unreferenced_files[4][0]
    assert ".parquet" in unreferenced_files[5][0]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    run_command("RESET pg_lake_iceberg.target_file_size_mb", pg_conn)
    pg_conn.commit()


# this time, multi-statement difference is calculated
def test_unreferenced_files_5(
    s3, pg_conn, extension, create_helper_functions, create_iceberg_table
):

    # make sure we do not interfere with any external changes to this
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000", pg_conn)

    prev_metadata_list = []

    # insert
    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(0,100)i", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    prev_metadata_list.append(metadata_location)

    snapshot_id_1 = get_current_snapshot_id(pg_conn)

    # positional update
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = 1 WHERE id = 0", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    prev_metadata_list.append(metadata_location)

    snapshot_id_2 = get_current_snapshot_id(pg_conn)

    # copy-on-write update
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = 1 WHERE id < 50", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    prev_metadata_list.append(metadata_location)

    snapshot_id_3 = get_current_snapshot_id(pg_conn)

    # now, lets do a VACUUM FULL, which will re-write the table hence getting rid
    # of all the existing metadata
    run_command(f"TRUNCATE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()

    snapshot_id_4 = get_current_snapshot_id(pg_conn)
    unreferenced_files_via_snapshots = get_unreferenced_files_via_all_snapshots(
        pg_conn, [snapshot_id_1, snapshot_id_2, snapshot_id_3], [snapshot_id_4]
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=1000",
        "SET pg_lake_iceberg.manifest_min_count_to_merge TO 1000",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        f"VACUUM {TEST_TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)
    unreferenced_files = get_unreferenced_files_prev_metadata(
        pg_conn, prev_list=prev_metadata_list
    )

    # minor formatting
    prev_metadata_list = [[x] for x in prev_metadata_list]
    assert prev_metadata_list + unreferenced_files_via_snapshots == unreferenced_files

    assert len(unreferenced_files) == 18

    # 3 metadata the metadata.json files for INSERT, UPDATE and UPDATE
    assert ".metadata.json" in unreferenced_files[0][0]
    assert ".metadata.json" in unreferenced_files[1][0]
    assert ".metadata.json" in unreferenced_files[2][0]

    # 3 snapshots for INSERT, UPDATE and UPDATE
    assert "snap-" in unreferenced_files[3][0] and ".avro" in unreferenced_files[3][0]
    assert "snap-" in unreferenced_files[4][0] and ".avro" in unreferenced_files[4][0]
    assert "snap-" in unreferenced_files[5][0] and ".avro" in unreferenced_files[5][0]

    # total of 7 manifests
    assert "avro" in unreferenced_files[6][0] and "-m" in unreferenced_files[6][0]
    assert "avro" in unreferenced_files[7][0] and "-m" in unreferenced_files[7][0]
    assert "avro" in unreferenced_files[8][0] and "-m" in unreferenced_files[8][0]
    assert "avro" in unreferenced_files[9][0] and "-m" in unreferenced_files[9][0]
    assert "avro" in unreferenced_files[10][0] and "-m" in unreferenced_files[10][0]
    assert "avro" in unreferenced_files[11][0] and "-m" in unreferenced_files[11][0]
    assert "avro" in unreferenced_files[12][0] and "-m" in unreferenced_files[12][0]

    # 3 data files
    assert ".parquet" in unreferenced_files[13][0]
    assert ".parquet" in unreferenced_files[14][0]
    assert ".parquet" in unreferenced_files[15][0]

    # and 2 deletion files
    assert (
        ".parquet" in unreferenced_files[16][0]
        and "data_0" not in unreferenced_files[16][0]
    )
    assert (
        ".parquet" in unreferenced_files[17][0]
        and "data_0" not in unreferenced_files[17][0]
    )

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    run_command("RESET pg_lake_iceberg.target_file_size_mb", pg_conn)

    pg_conn.commit()


def get_unreferenced_files_prev_metadata(pg_conn, prev_list=None):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]

    if prev_list is None:
        previous_metadata_location = run_query(
            f"SELECT previous_metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
            pg_conn,
        )[0][0]
        unreferenced_files = run_query(
            f"""select * FROM lake_iceberg.find_unreferenced_files(ARRAY['{previous_metadata_location}'], '{metadata_location}') ORDER BY 1;
""",
            pg_conn,
        )
    else:
        array_string = "ARRAY[" + ", ".join([f"'{item}'" for item in prev_list]) + "]"

        unreferenced_files = run_query(
            f"""select * FROM lake_iceberg.find_unreferenced_files({array_string}, '{metadata_location}') ORDER BY 1;
""",
            pg_conn,
        )

    unreferenced_files.sort(key=file_sort_key)
    return unreferenced_files


def get_current_snapshot_id(pg_conn):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]
    return run_query(
        f"""SELECT snapshot_id FROM lake_iceberg.snapshots('{metadata_location}') ORDER BY sequence_number DESC LIMIT 1""",
        pg_conn,
    )[0][0]


def get_unreferenced_files_via_all_snapshots(
    pg_conn, prev_snapshot_ids, current_snapshot_ids
):

    unreferenced_files = run_query(
        f"""SELECT * FROM lake_iceberg.find_unreferenced_files_via_snapshot_ids('{TEST_TABLE_NAME}'::regclass, ARRAY{prev_snapshot_ids}, ARRAY{current_snapshot_ids})""",
        pg_conn,
    )

    # unreferenced_files.append([f'{metadata_location}'])

    unreferenced_files.sort(key=file_sort_key)
    return unreferenced_files


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

        CREATE OR REPLACE FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_all_referenced_files$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text) TO public;

        CREATE OR REPLACE FUNCTION lake_iceberg.find_unreferenced_files(previous_paths text[], current_path text, OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_unreferenced_files$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_unreferenced_files(previous_paths text[], current_path text, OUT path text) TO public;

        CREATE OR REPLACE FUNCTION lake_iceberg.find_unreferenced_files_via_snapshot_ids(table_name regclass,prev_snapshot_id bigint[],snapshot_id bigint[], OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_unreferenced_files_via_snapshot_ids$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_unreferenced_files_via_snapshot_ids(table_name regclass,prev_snapshot_id bigint[], snapshot_id bigint[], OUT path text) TO lake_read;


""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.find_all_referenced_files;
        DROP FUNCTION lake_iceberg.find_unreferenced_files;
        DROP FUNCTION lake_iceberg.find_unreferenced_files_via_snapshot_ids;

""",
        superuser_conn,
    )
    superuser_conn.commit()

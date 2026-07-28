import pytest
from utils_pytest import *

TEST_TABLE_NAME = "test_manifest_merge"


def test_manifest_merge_disabled_on_write(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[1, 0, 1, 1, 0, 0, 0, 0], [2, 0, 1, 1, 0, 0, 0, 0]]
    pg_conn.commit()

    # manifest merge should work with vacuum even if "enable_manifest_merge_on_write" = off
    run_command_outside_tx(
        [
            # do not trigger data file compaction to simplify tests
            "SET pg_lake_table.vacuum_compact_min_input_files = 5;",
            "SET pg_lake_iceberg.manifest_min_count_to_merge = 2;",
            f"VACUUM {TEST_TABLE_NAME}",
        ]
    )

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[3, 0, 0, 0, 2, 2, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_with_inserts_only(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (4)", pg_conn)
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 5", pg_conn)

    # insert 1 more row to trigger manifest merge (1 manifest at the final snapshot)
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (5)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 0, 1, 1, 4, 4, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_with_copy_pushdown(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    url = f"s3://{TEST_BUCKET}/test_manifest_merge_with_copy_pushdown/data.parquet"

    run_command(f"COPY (SELECT i FROM generate_series(0,10)i) TO '{url}'", pg_conn)
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    # insert multiple copies, and trigger manifest compaction
    run_command(f"COPY {TEST_TABLE_NAME} FROM '{url}'", pg_conn)
    pg_conn.commit()

    run_command(f"COPY {TEST_TABLE_NAME} FROM '{url}'", pg_conn)
    pg_conn.commit()

    run_command(f"COPY {TEST_TABLE_NAME} FROM '{url}'", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)

    # we should have a single manifest as COPY pushdown triggers manifest compaction
    assert len(manifests) == 1
    pg_conn.rollback()


def test_manifest_merge_with_deletes(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (4)", pg_conn)
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)

    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    # delete some rows to trigger manifest merge
    run_command(f"DELETE FROM {TEST_TABLE_NAME} WHERE id <= 2", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 0, 0, 0, 2, 2, 2, 2]]

    pg_conn.rollback()


def test_manifest_merge_with_updates(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(1,25) i", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(26,50) i", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(51,75) i", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(76,100) i",
        pg_conn,
    )
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    # update some rows to trigger manifest merge
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE id <= 2", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)

    #                    DELETES                DATA
    assert manifests == [[5, 0, 1, 2, 0, 0, 0, 0], [5, 0, 1, 2, 4, 100, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_via_vacuum(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(1,25) i", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(26,50) i", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(51,75) i", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i FROM generate_series(76,100) i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE id <= 2", pg_conn)
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    pg_conn.commit()

    # vacuum to trigger manifest merge
    # manifest merge should work with vacuum even if "enable_manifest_merge_on_write" = off
    run_command_outside_tx(
        [
            "SET pg_lake_table.vacuum_compact_min_input_files = 100",
            "SET pg_lake_iceberg.manifest_min_count_to_merge = 2;",
            f"VACUUM {TEST_TABLE_NAME}",
        ]
    )

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 0, 1, 2, 0, 0, 0, 0], [6, 0, 0, 0, 5, 102, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_min_count(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 4;
                    INSERT INTO {TEST_TABLE_NAME} VALUES (1)""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 4;
                    INSERT INTO {TEST_TABLE_NAME} VALUES (2)""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 4;
                    INSERT INTO {TEST_TABLE_NAME} VALUES (3)""",
        pg_conn,
    )
    pg_conn.commit()

    # nothing merged yet
    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [1, 0, 1, 1, 0, 0, 0, 0],
        [2, 0, 1, 1, 0, 0, 0, 0],
        [3, 0, 1, 1, 0, 0, 0, 0],
    ]

    # insert 1 more row to trigger manifest merge
    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 4;
            INSERT INTO {TEST_TABLE_NAME} VALUES (4)""",
        pg_conn,
    )
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[4, 0, 1, 1, 3, 3, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_target_size(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    # let one manifest be unmerged after the next INSERT
    total_manifest_size_in_kb = get_current_manifests_size_in_kb(pg_conn)
    run_command(
        f"SET pg_lake_iceberg.target_manifest_size_kb = {total_manifest_size_in_kb}",
        pg_conn,
    )

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3)", pg_conn)
    pg_conn.commit()

    run_command("RESET pg_lake_iceberg.target_manifest_size_kb", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[3, 0, 0, 0, 2, 2, 0, 0], [3, 0, 1, 1, 0, 0, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_by_lowered_min_count(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 100;
                    INSERT INTO {TEST_TABLE_NAME} VALUES (1)""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 100;
                    INSERT INTO {TEST_TABLE_NAME} VALUES (2)""",
        pg_conn,
    )
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[1, 0, 1, 1, 0, 0, 0, 0], [2, 0, 1, 1, 0, 0, 0, 0]]

    # lower the min count to trigger manifest merge on the next INSERT
    run_command(
        f"""SET pg_lake_iceberg.manifest_min_count_to_merge = 3;
                    INSERT INTO {TEST_TABLE_NAME} VALUES (3)""",
        pg_conn,
    )
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[3, 0, 1, 1, 2, 2, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_by_lowered_target_size(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 100", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    single_manifest_size_in_kb = get_current_manifests_size_in_kb(pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[1, 0, 1, 1, 0, 0, 0, 0], [2, 0, 1, 1, 0, 0, 0, 0]]

    # lower the target size to trigger manifest merge on the next INSERT
    run_command(
        f"SET pg_lake_iceberg.target_manifest_size_kb = {2 * single_manifest_size_in_kb}",
        pg_conn,
    )

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[3, 0, 0, 0, 2, 2, 0, 0], [3, 0, 1, 1, 0, 0, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_with_multiple_groups_due_to_disabled_merge_on_write(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    # minimum 2 manifests are needed to merge
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    # at most, a group will have 2 manifests
    total_manifest_size_in_kb = get_current_manifests_size_in_kb(pg_conn)
    run_command(
        f"SET pg_lake_iceberg.target_manifest_size_kb = {total_manifest_size_in_kb}",
        pg_conn,
    )

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (4)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [1, 0, 1, 1, 0, 0, 0, 0],
        [2, 0, 1, 1, 0, 0, 0, 0],
        [3, 0, 1, 1, 0, 0, 0, 0],
        [4, 0, 1, 1, 0, 0, 0, 0],
    ]

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)

    # after the next insert, we will have 2 groups with 2 manifests each, and also a single manifest
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (5)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [5, 0, 0, 0, 2, 2, 0, 0],
        [5, 0, 0, 0, 2, 2, 0, 0],
        [5, 0, 1, 1, 0, 0, 0, 0],
    ]
    pg_conn.rollback()


def test_manifest_merge_with_multiple_groups_due_to_small_target_size(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # group with the latest manifest will never be merged until 2000 manifests are created
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2000", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    # at most, a group will have 2 manifests, this will unblock other manifest groups merge
    total_manifest_size_in_kb = get_current_manifests_size_in_kb(pg_conn)
    run_command(
        f"SET pg_lake_iceberg.target_manifest_size_kb = {total_manifest_size_in_kb}",
        pg_conn,
    )

    # the group with the latest manifest will be blocked due to min count
    # but the other group will be merged
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (4)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[4, 0, 0, 0, 3, 3, 0, 0], [4, 0, 1, 1, 0, 0, 0, 0]]

    pg_conn.rollback()


def test_manifest_with_multiple_manifest_entries(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = on", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[4, 0, 1, 1, 3, 3, 0, 0]]

    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[4, 0, 1, 1, 3, 3, 0, 0], [5, 0, 1, 1, 0, 0, 0, 0]]

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)

    # next INSERT wont trigger manifest merge due to target size
    total_manifest_size_in_kb = get_current_manifests_size_in_kb(pg_conn)
    run_command(
        f"SET pg_lake_iceberg.target_manifest_size_kb = {total_manifest_size_in_kb / 5}",
        pg_conn,
    )

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [4, 0, 1, 1, 3, 3, 0, 0],
        [5, 0, 1, 1, 0, 0, 0, 0],
        [6, 0, 1, 1, 0, 0, 0, 0],
    ]

    # reset target size to trigger manifest merge
    run_command(f"RESET pg_lake_iceberg.target_manifest_size_kb", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[7, 0, 1, 1, 6, 6, 0, 0]]

    pg_conn.rollback()


def test_manifest_merge_with_partitioned_table(
    s3,
    pg_conn,
    extension,
    create_identity_partitioned_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = on", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 5", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2,2)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (3,3)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (4,4)", pg_conn)
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)

    # insert 1 more row to trigger manifest merge (1 manifest at the final snapshot)
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (5,5)", pg_conn)
    pg_conn.commit()

    # show that basic manifest merge work on partitioned table
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 1, 1, 1, 4, 4, 0, 0]]

    # now, create one more spec
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS  (SET partition_by 'b')",
        pg_conn,
    )
    pg_conn.commit()

    # changing partitioning does not change the manifests
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 1, 1, 1, 4, 4, 0, 0]]

    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2,2)", pg_conn)
    pg_conn.commit()

    # now, we have partition_spec_id bumped to 1
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 1, 1, 1, 4, 4, 0, 0], [8, 2, 1, 1, 1, 1, 0, 0]]

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2,2)", pg_conn)
    pg_conn.commit()

    # we do manifest compaction on partition_spec_id 1
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 1, 1, 1, 4, 4, 0, 0], [10, 2, 1, 1, 3, 3, 0, 0]]

    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 30", pg_conn)
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2,2)", pg_conn)
    pg_conn.commit()

    # we do not do manifest compaction on partition_spec_id 2
    # as we have not passed manifest_min_count_to_merge
    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [5, 1, 1, 1, 4, 4, 0, 0],
        [10, 2, 1, 1, 3, 3, 0, 0],
        [11, 2, 1, 1, 0, 0, 0, 0],
        [12, 2, 1, 1, 0, 0, 0, 0],
    ]

    # now, trigger one more compaction
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[5, 1, 1, 1, 4, 4, 0, 0], [13, 2, 1, 1, 6, 6, 0, 0]]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    pg_conn.commit()

    # sanity check, read the table
    res = run_query(f"SELECT * FROM {TEST_TABLE_NAME}", pg_conn)
    assert len(res) == 12
    pg_conn.rollback()


def test_old_manifest_merge_with_partitioned_table(
    s3,
    pg_conn,
    extension,
    create_identity_partitioned_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # in this test, we'll trigger manifest compaction for multiple partition specs
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    # now, create one more spec
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS  (SET partition_by 'b')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    # now, drop the spec
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS  (DROP partition_by)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    # now, create the third spec
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS  (ADD partition_by 'id,b')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    # now, drop the spec second time
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS  (DROP partition_by)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1,1)", pg_conn)
    pg_conn.commit()

    # show that basic manifest merge work on partitioned table
    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [1, 1, 1, 1, 0, 0, 0, 0],
        [2, 1, 1, 1, 0, 0, 0, 0],
        [4, 2, 1, 1, 0, 0, 0, 0],
        [5, 2, 1, 1, 0, 0, 0, 0],
        [7, 0, 1, 1, 0, 0, 0, 0],
        [8, 0, 1, 1, 0, 0, 0, 0],
        [10, 3, 1, 1, 0, 0, 0, 0],
        [11, 3, 1, 1, 0, 0, 0, 0],
        [13, 0, 1, 1, 0, 0, 0, 0],
        [14, 0, 1, 1, 0, 0, 0, 0],
    ]

    pg_conn.commit()
    run_command_outside_tx(
        [
            # don't do any data file compaction
            "SET pg_lake_table.vacuum_compact_min_input_files TO 1000",
            # do manifest compaction
            "SET pg_lake_iceberg.manifest_min_count_to_merge = 2;",
            f"VACUUM {TEST_TABLE_NAME}",
        ]
    )

    # we do not cross partition boundaries when compacting manifests
    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [15, 0, 0, 0, 4, 4, 0, 0],
        [15, 1, 0, 0, 2, 2, 0, 0],
        [15, 2, 0, 0, 2, 2, 0, 0],
        [15, 3, 0, 0, 2, 2, 0, 0],
    ]

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)

    # sanity check, read the table
    res = run_query(f"SELECT * FROM {TEST_TABLE_NAME}", pg_conn)
    assert len(res) == 10

    pg_conn.commit()

    run_command_outside_tx(
        [
            "RESET pg_lake_iceberg.manifest_min_count_to_merge;",
            "RESET pg_lake_table.vacuum_compact_min_input_files;",
        ]
    )


def test_partitioned_manifest_merge_with_updates(
    s3,
    pg_conn,
    extension,
    create_truncate_partitioned_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    # disable manifest merge on write
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i,i FROM generate_series(0,24) i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i,i FROM generate_series(25,49) i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i,i FROM generate_series(50,74) i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {TEST_TABLE_NAME} SELECT i,i FROM generate_series(75,99) i",
        pg_conn,
    )
    pg_conn.commit()

    # enable manifest merge on write
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    # update some rows to trigger manifest merge, create positional delete file
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE id <= 2", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)

    #                    DELETES                DATA
    assert manifests == [[5, 1, 1, 3, 0, 0, 0, 0], [5, 1, 1, 3, 4, 100, 0, 0]]

    # update some rows on another partition as well trigger manifest merge, create positional delete file
    run_command(
        f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE id >=25 and id <=27", pg_conn
    )
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [5, 1, 1, 3, 0, 0, 0, 0],
        [6, 1, 1, 3, 0, 0, 0, 0],
        [6, 1, 1, 3, 5, 103, 0, 0],
    ]

    # now, update but create copy-on-write files
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE id <= 10", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [
        [6, 1, 1, 3, 0, 0, 0, 0],
        [7, 1, 0, 0, 0, 0, 1, 3],
        [7, 1, 2, 25, 4, 78, 2, 28],
    ]

    # now, update but create copy-on-write files for the other partition
    run_command(
        f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE id>= 25 AND id <= 40", pg_conn
    )
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[8, 1, 0, 0, 0, 0, 1, 3], [8, 1, 2, 25, 4, 75, 2, 28]]

    # sanity check, read the table
    res = run_query(f"SELECT * FROM {TEST_TABLE_NAME}", pg_conn)
    assert len(res) == 100

    # now, create the second spec
    run_command(
        f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS  (SET partition_by 'truncate(25,b)')",
        pg_conn,
    )
    pg_conn.commit()

    # update some rows to trigger manifest merge, create positional delete file
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE b <= 2", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[10, 1, 1, 8, 5, 89, 1, 11], [10, 2, 1, 3, 0, 0, 0, 0]]

    # sanity check, read the table
    res = run_query(f"SELECT * FROM {TEST_TABLE_NAME}", pg_conn)
    assert len(res) == 100

    # update some rows to trigger manifest merge, create copy-on-write
    run_command(f"UPDATE {TEST_TABLE_NAME} SET id = id + 1 WHERE b <= 20", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[11, 1, 1, 4, 4, 75, 2, 22], [11, 2, 1, 21, 0, 0, 1, 3]]

    # sanity check, read the table
    res = run_query(f"SELECT * FROM {TEST_TABLE_NAME}", pg_conn)
    assert len(res) == 100

    # a slightly more complex sanity check
    res = run_query(f"SELECT sum(id) FROM {TEST_TABLE_NAME}", pg_conn)
    assert res[0][0] == 5007

    pg_conn.rollback()


def test_manifest_merge_after_float4_to_float8_type_promotion(
    s3,
    pg_conn,
    extension,
    create_test_helper_functions,
    reset_manifest_merge_settings,
    with_default_location,
):
    """
    Verify that manifest merge preserves correct min-max statistics after
    a float4 -> float8 (real -> double precision) type promotion.

    After promotion, older data files carry 4-byte float column bounds while
    newer files carry 8-byte double bounds.  A manifest merge must handle
    both widths correctly so that:
      - all data remains readable,
      - data-file pruning still works (no rows silently dropped),
      - values with precision differences between float4 and float8 are
        handled without rounding errors.
    """
    table_name = "test_merge_float_promo"
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    run_command(
        f"""
        CREATE TABLE {table_name} (a real, b int)
        USING pg_lake_iceberg;
        ALTER FOREIGN TABLE {table_name} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # disable manifest merge so we accumulate multiple manifests
    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)

    # Use values with precision that differs between float4 and float8:
    #   float4(1.1)  ≈ 1.10000002  (in float8 terms)
    #   float8(1.1)  = 1.1000000000000001
    # These are the kinds of values most likely to expose rounding issues.
    run_command(f"INSERT INTO {table_name} VALUES (1.1, 1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {table_name} VALUES (2.2, 2)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {table_name} VALUES (3.3, 3)", pg_conn)
    pg_conn.commit()

    # verify data before type change
    results = run_query(f"SELECT a, b FROM {table_name} ORDER BY b", pg_conn)
    assert len(results) == 3

    # promote float4 -> float8
    run_command(
        f"ALTER TABLE {table_name} ALTER COLUMN a TYPE double precision;", pg_conn
    )
    pg_conn.commit()

    # insert data with full float8 precision (values that cannot be
    # represented exactly in float4)
    run_command(f"INSERT INTO {table_name} VALUES (1.23456789012345, 4)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {table_name} VALUES (4.56789012345678, 5)", pg_conn)
    pg_conn.commit()

    # now enable manifest merge and trigger it
    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    # this insert triggers the merge (6th manifest, well above min_count=2)
    run_command(f"INSERT INTO {table_name} VALUES (5.5, 6)", pg_conn)
    pg_conn.commit()

    # after merge we should have a single manifest (all merged)
    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]

    manifests = run_query(
        f"""SELECT added_files_count + existing_files_count
            FROM lake_iceberg.current_manifests('{metadata_location}')""",
        pg_conn,
    )
    total_files = sum(row[0] for row in manifests)
    assert total_files == 6, f"Expected 6 data files after merge, got {total_files}"

    # ── Verify all data is still readable and correct ──
    results = run_query(f"SELECT a, b FROM {table_name} ORDER BY b", pg_conn)
    assert len(results) == 6

    # float4 values are read back as float8 after promotion
    # verify the pre-promotion values survived the merge
    assert results[0][1] == 1  # b=1
    assert results[1][1] == 2  # b=2
    assert results[2][1] == 3  # b=3

    # post-promotion values with full float8 precision
    assert results[3] == [1.23456789012345, 4]
    assert results[4] == [4.56789012345678, 5]
    assert results[5][1] == 6  # b=6

    # ── Verify data file pruning works correctly after merge ──

    # query with an int filter that isolates a pre-promotion data file
    results = run_query(
        f"{explain_prefix} SELECT * FROM {table_name} WHERE b = 1", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # query for post-promotion float8 data file
    results = run_query(
        f"{explain_prefix} SELECT * FROM {table_name} WHERE b = 4", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # ── Pruning on the promoted float column ──
    # This is the critical test: min-max bounds on column "a" must survive
    # the float4→float8 widening during manifest merge.
    #
    # Data files (each with 1 row):
    #   File 1: a ≈ 1.1000000238 (float4 widened), b=1
    #   File 2: a ≈ 2.2000000477 (float4 widened), b=2
    #   File 3: a ≈ 3.2999999523 (float4 widened), b=3
    #   File 4: a = 1.23456789012345 (native float8), b=4
    #   File 5: a = 4.56789012345678 (native float8), b=5
    #   File 6: a = 5.5 (native float8), b=6

    # WHERE a < 1.15 should scan only File 1
    #   (float4(1.1) widens to ≈1.1000000238 which is < 1.15)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {table_name} WHERE a < 1.15", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # WHERE a > 4.0 should scan Files 5 and 6 only
    results = run_query(
        f"{explain_prefix} SELECT * FROM {table_name} WHERE a > 4.0", pg_conn
    )
    assert fetch_data_files_used(results) == "2"

    # verify no rows are lost with a range query that spans pre- and
    # post-promotion data files
    results = run_query(
        f"SELECT count(*) FROM {table_name} WHERE a >= 1.0 AND a <= 6.0", pg_conn
    )
    assert results[0][0] == 6

    # additional sanity: sum should match
    results = run_query(f"SELECT sum(b) FROM {table_name}", pg_conn)
    assert results[0][0] == 21  # 1+2+3+4+5+6

    # cleanup
    pg_conn.rollback()
    run_command(f"DROP FOREIGN TABLE {table_name}", pg_conn)
    pg_conn.commit()


# a stress testing for partition changes and manifest compaction
# randomly insert and update rows, then in between change the partition
# spec so that we ensure multiple specs, positional deletes, copy-on-write
# works happily
def test_random_partition_changes(
    s3,
    pg_conn,
    extension,
    create_identity_partitioned_iceberg_table,
    create_test_helper_functions,
    disable_data_file_pruning,
    reset_manifest_merge_settings,
):
    import random

    loop_len = 7  # adjust for longer / shorter runs
    total_rows = 0
    expected_sum = 0
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    current_has_partition = True  # we start with a partitioned table

    # also stress test about schema evolution
    added_column = False

    for i in range(loop_len):
        # stress test schema evolution
        if added_column == False:
            run_command(
                f"ALTER TABLE {TEST_TABLE_NAME} ADD COLUMN test_col INT", pg_conn
            )
            added_column = True
        else:
            run_command(f"ALTER TABLE {TEST_TABLE_NAME} DROP COLUMN test_col", pg_conn)
            added_column = False

        # ---- INSERT --------------------------------------------------------
        start_id = i * 10
        end_id = start_id + 9

        # randomize INSERT .. SELECT pushdown vs non-pushdown
        # as they might use different code-paths
        run_command(
            f"SET LOCAL pg_lake_table.enable_insert_select_pushdown TO {random.randint(0,1)}",
            pg_conn,
        )

        run_command(
            f"INSERT INTO {TEST_TABLE_NAME} "
            f"SELECT i,i FROM generate_series({start_id},{end_id}) i",
            pg_conn,
        )
        total_rows += 10
        expected_sum += sum(range(start_id, end_id + 1))

        # ---- UPDATE --------------------------------------------------------
        for _ in range(4):  # multiple updates inside the same spec
            upd_cnt = random.randint(1, 4)
            run_command(
                f"""
                WITH picked AS (
                    SELECT id FROM {TEST_TABLE_NAME}
                    ORDER BY id
                    LIMIT {upd_cnt}
                )
                UPDATE {TEST_TABLE_NAME}
                   SET b = b + 1
                  WHERE id IN (SELECT id FROM picked)
                """,
                pg_conn,
            )
            expected_sum += upd_cnt  # every updated row adds +1

        # ---- CHECK ---------------------------------------------------------
        row_cnt = run_query(f"SELECT count(*) FROM {TEST_TABLE_NAME}", pg_conn)[0][0]
        assert row_cnt == total_rows

        sum_b = run_query(f"SELECT sum(b) FROM {TEST_TABLE_NAME}", pg_conn)[0][0]
        assert sum_b == expected_sum

        if current_has_partition:
            top_id = int(
                run_query(f"SELECT max(id) FROM {TEST_TABLE_NAME}", pg_conn)[0][0]
            )
            results = run_query(
                f"""
                EXPLAIN (verbose, format 'json') SELECT count(*) FROM {TEST_TABLE_NAME};
                """,
                pg_conn,
            )
            # make sure pruning works as expected
            data_files_fetch_full_scan = int(fetch_data_files_used(results))

            results = run_query(
                f"""
                EXPLAIN (verbose, format 'json') SELECT * FROM {TEST_TABLE_NAME} WHERE id = {top_id};
                """,
                pg_conn,
            )
            # make sure pruning works as expected
            data_files_fetch_filter = int(fetch_data_files_used(results))

            # pruning should always eliminate data files
            assert int(data_files_fetch_full_scan) > int(data_files_fetch_filter)

        # ---- CHANGE PARTITIONING ------------------------------------------
        choice = random.choice(["identity", "truncate", "bucket", "none"])
        col = random.choice(["id", "b"])

        if choice == "none":
            # drop partitioning only if the table is currently partitioned
            if current_has_partition:
                run_command(
                    f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} "
                    f"OPTIONS (DROP partition_by)",
                    pg_conn,
                )
                current_has_partition = False
            # no extra push when we just dropped partitioning
            continue

        # build a new spec (identity / truncate / bucket)
        if choice == "identity":
            spec = f"{col}"
        elif choice == "truncate":
            width = random.choice([5, 10, 25, 50])
            spec = f"truncate({width},{col})"
        else:  # bucket
            buckets = random.choice([4, 8, 16])
            spec = f"bucket({buckets},{col})"

        # decide ADD vs SET depending on current state
        verb = "SET" if current_has_partition else "ADD"
        run_command(
            f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} "
            f"OPTIONS ({verb} partition_by '{spec}')",
            pg_conn,
        )

        current_has_partition = True
        # intentionally push the same spec once more as before any modifications
        # as part of stress testing
        run_command(
            f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} "
            f"OPTIONS (SET partition_by '{spec}')",
            pg_conn,
        )

    # sanity checks
    assert total_rows == 10 * loop_len
    assert expected_sum > 0

    # cleanup
    pg_conn.rollback()
    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)


# stress testing for TRUNCATE and manifest compaction
def test_insert_and_truncate_each_loop(
    s3,
    pg_conn,
    extension,
    create_identity_partitioned_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    """
    A very small stress test:

      • loop 5 times (change loop_len to run longer)
      • each loop:
          – pick a random partition spec
          – INSERT 10 rows
          – TRUNCATE the table
          – assert table is empty (row‑count 0, sum(b) 0)

    No manifest checks, no intermediate rollbacks.  All results should end up zero.
    """
    loop_len = 3
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    for i in range(loop_len):
        # ---- (optional) change partition spec each round ------------------
        choice = random.choice(["identity", "truncate", "bucket"])
        col = random.choice(["id", "b"])

        if choice == "identity":
            spec = f"{col}"
        elif choice == "truncate":
            width = random.choice([5, 10, 25, 50])
            spec = f"truncate({width},{col})"
        else:  # bucket
            buckets = random.choice([4, 8, 16])
            spec = f"bucket({buckets},{col})"

        run_command(
            f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} "
            f"OPTIONS (SET partition_by '{spec}')",
            pg_conn,
        )

        # ---- INSERT --------------------------------------------------------
        start_id = i * 10
        end_id = start_id + 9
        for i in range(0, 3):
            run_command(
                f"INSERT INTO {TEST_TABLE_NAME} "
                f"SELECT i, i FROM generate_series({start_id},{end_id}) i",
                pg_conn,
            )

        # quick check: should have 10 rows after insert
        cnt = run_query(f"SELECT count(*) FROM {TEST_TABLE_NAME}", pg_conn)[0][0]
        assert cnt == 30

        # ---- TRUNCATE ------------------------------------------------------
        run_command(f"TRUNCATE {TEST_TABLE_NAME}", pg_conn)

        # table must be empty, sums zero
        cnt = run_query(f"SELECT count(*) FROM {TEST_TABLE_NAME}", pg_conn)[0][0]
        assert cnt == 0

        sum_b = run_query(f"SELECT COALESCE(sum(b),0) FROM {TEST_TABLE_NAME}", pg_conn)[
            0
        ][0]
        assert sum_b == 0

    # leave the DB clean for other tests
    pg_conn.rollback()
    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)


def test_add_column_with_partition_change(
    s3,
    pg_conn,
    extension,
    create_identity_partitioned_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    """
    Workflow
    --------
    loop 3 times (edit loop_len to increase):
      1. ALTER TABLE … ADD COLUMN c_i INT
      2. UPDATE existing rows so c_i is never NULL
      3. Pick a *random* partition spec that uses the new column
      4. INSERT 10 new rows, filling the new column
      5. UPDATE a random subset of rows, bumping the new column by +1
      6. Assert:
           • row‑count equals expected_rows
           • new column has zero NULLs
           • sum(id) matches the arithmetic we keep
    No manifest checks, one open transaction, final rollback for cleanliness.
    """
    loop_len = 3
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)

    expected_rows = 0
    expected_sum_id = 0

    for i in range(loop_len):
        col_name = f"c{i}"

        # -- 1. add the new column ------------------------------------------
        run_command(
            f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} ADD COLUMN {col_name} int",
            pg_conn,
        )

        # -- 2. ensure no NULLs for existing rows ----------------------------
        run_command(
            f"UPDATE {TEST_TABLE_NAME} SET {col_name} = 0 WHERE {col_name} IS NULL",
            pg_conn,
        )

        # -- 3. switch partition spec (random) ------------------------------
        choice = random.choice(["identity", "truncate", "bucket"])

        if choice == "identity":
            spec = f"{col_name}"
        elif choice == "truncate":
            width = random.choice([5, 10, 25, 50])
            spec = f"truncate({width},{col_name})"
        else:  # bucket
            buckets = random.choice([4, 8, 16])
            spec = f"bucket({buckets},{col_name})"

        run_command(
            f"ALTER FOREIGN TABLE {TEST_TABLE_NAME} "
            f"OPTIONS (SET partition_by '{spec}')",
            pg_conn,
        )

        # -- 4. INSERT 10 rows ----------------------------------------------
        start_id = i * 100  # keep ranges disjoint
        end_id = start_id + 9
        ingest_cnt = 3
        for i in range(0, ingest_cnt):
            run_command(
                f"INSERT INTO {TEST_TABLE_NAME} (id, b, {col_name}) "
                f"SELECT i, i, i FROM generate_series({start_id},{end_id}) i",
                pg_conn,
            )
        expected_rows += 30
        expected_sum_id += sum(range(start_id, end_id + 1)) * ingest_cnt

        # -- 5. UPDATE a random subset (bump new column) --------------------
        upd_cnt = random.randint(1, expected_rows)
        run_command(
            f"""
            WITH picked AS (
                SELECT id FROM {TEST_TABLE_NAME}
                ORDER BY id
                LIMIT {upd_cnt}
            )
            UPDATE {TEST_TABLE_NAME}
               SET {col_name} = {col_name} + 1
              WHERE id IN (SELECT id FROM picked)
            """,
            pg_conn,
        )
        # we don’t track sums for the new column; only ensure no NULLs.

        # -- 6. ASSERT correctness ------------------------------------------
        row_cnt = run_query(f"SELECT * FROM {TEST_TABLE_NAME}", pg_conn)
        assert len(row_cnt) == expected_rows

        nulls = run_query(
            f"SELECT * FROM {TEST_TABLE_NAME} " f"WHERE {col_name} IS NULL",
            pg_conn,
        )
        assert len(nulls) == 0

        sum_id = run_query(f"SELECT sum(id) FROM {TEST_TABLE_NAME}", pg_conn)[0][0]
        assert sum_id == expected_sum_id

    # clean out side‑effects for the rest of the suite
    pg_conn.rollback()

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)


def test_manifest_on_multiple_truncate(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    reset_manifest_merge_settings,
):
    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    run_command(f"TRUNCATE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()

    # we should have 2 manifests with all deleted entries
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[3, 0, 0, 0, 0, 0, 1, 1], [3, 0, 0, 0, 0, 0, 1, 1]]

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (1)", pg_conn)
    pg_conn.commit()

    # manifests with deleted entries are pruned away after insert
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[4, 0, 1, 1, 0, 0, 0, 0]]

    run_command(f"INSERT INTO {TEST_TABLE_NAME} VALUES (2)", pg_conn)
    pg_conn.commit()

    manifests = get_current_manifests(pg_conn)
    assert manifests == [[4, 0, 1, 1, 0, 0, 0, 0], [5, 0, 1, 1, 0, 0, 0, 0]]

    run_command(f"TRUNCATE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()

    # we should have 2 manifests with all deleted entries
    manifests = get_current_manifests(pg_conn)
    assert manifests == [[6, 0, 0, 0, 0, 0, 1, 1], [6, 0, 0, 0, 0, 0, 1, 1]]


def get_current_manifests(pg_conn):
    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]

    manifests = run_query(
        f"""SELECT sequence_number, partition_spec_id,
                                   added_files_count, added_rows_count,
                                   existing_files_count, existing_rows_count,
                                   deleted_files_count, deleted_rows_count
                              FROM lake_iceberg.current_manifests('{metadata_location}')
                              ORDER BY sequence_number, partition_spec_id,
                                 added_files_count, added_rows_count,
                                 existing_files_count, existing_rows_count,
                                 deleted_files_count, deleted_rows_count ASC""",
        pg_conn,
    )
    return manifests


def get_current_manifests_size_in_kb(pg_conn):
    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{TEST_TABLE_NAME}'",
        pg_conn,
    )[0][0]

    total_manifest_size = run_query(
        f"""SELECT SUM(manifest_length)
                                        FROM lake_iceberg.current_manifests('{metadata_location}')
                                     """,
        pg_conn,
    )[0][0]

    return total_manifest_size / 1024


@pytest.fixture
def create_iceberg_table(pg_conn, with_default_location):
    run_command(
        f"""
            CREATE TABLE {TEST_TABLE_NAME} (id int) USING pg_lake_iceberg;
            ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP FOREIGN TABLE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()


@pytest.fixture
def create_identity_partitioned_iceberg_table(pg_conn, with_default_location):
    run_command(
        f"""
            CREATE TABLE {TEST_TABLE_NAME} (id int, b int) USING pg_lake_iceberg WITH (partition_by=id);
            ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP FOREIGN TABLE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()


@pytest.fixture
def create_truncate_partitioned_iceberg_table(pg_conn, with_default_location):
    run_command(
        f"""
            CREATE TABLE {TEST_TABLE_NAME} (id int, b int) USING pg_lake_iceberg WITH (partition_by='truncate(25,id)');
            ALTER FOREIGN TABLE {TEST_TABLE_NAME} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP FOREIGN TABLE {TEST_TABLE_NAME}", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_test_helper_functions(superuser_conn, app_user, s3, extension):
    run_command(
        f"""

        CREATE OR REPLACE FUNCTION lake_iceberg.current_manifests(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                manifest_path TEXT,
                manifest_length BIGINT,
                partition_spec_id INT,
                manifest_content TEXT,
                sequence_number BIGINT,
                min_sequence_number BIGINT,
                added_snapshot_id BIGINT,
                added_files_count INT,
                existing_files_count INT,
                deleted_files_count INT,
                added_rows_count BIGINT,
                existing_rows_count BIGINT,
                deleted_rows_count BIGINT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_manifests$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.current_manifests(text) TO {app_user};
        GRANT SELECT ON lake_iceberg.tables TO {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the functions after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.current_manifests;
        REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()


# this test file aims to ensure partition pruning works
@pytest.fixture(scope="module")
def disable_data_file_pruning(superuser_conn):

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_table.enable_data_file_pruning TO off;",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_table.enable_data_file_pruning",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture
def reset_manifest_merge_settings(pg_conn):
    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge;", pg_conn)
    run_command("RESET pg_lake_iceberg.target_manifest_size_kb;", pg_conn)
    pg_conn.commit()

    yield

    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge;", pg_conn)
    run_command("RESET pg_lake_iceberg.target_manifest_size_kb;", pg_conn)
    pg_conn.commit()

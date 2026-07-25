from utils_pytest import *

# The pre-upgrade script was run using superuser, since it predates the
# creation of app_user, so these tests use superuser as well.


@pytest.mark.skipif(
    os.getenv("TEST_PG_UPGRADE_FROM_BINDIR") == None,
    reason="set TEST_PG_UPGRADE_FROM_BINDIR to the old PG version to run",
)
def test_data_file_cascade_indexes(superuser_conn, extension):
    result = run_query(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'lake_table'
          AND indexname IN (
              'deletion_file_map_table_name_path_idx',
              'data_file_column_stats_table_path_idx',
              'data_file_partition_values_id_idx'
          )
        ORDER BY indexname
        """,
        superuser_conn,
    )

    assert result == [
        ["data_file_column_stats_table_path_idx"],
        ["data_file_partition_values_id_idx"],
        ["deletion_file_map_table_name_path_idx"],
    ]


@pytest.mark.skipif(
    os.getenv("TEST_PG_UPGRADE_FROM_BINDIR") == None,
    reason="set TEST_PG_UPGRADE_FROM_BINDIR to the old PG version to run",
)
def test_iceberg_query(superuser_conn, s3, extension):
    # Check whether we can update post-upgrade
    run_command("UPDATE pre_upgrade.iceberg SET id = id + 1", superuser_conn)

    # Check whether we can query post-upgrdae
    result = run_query("SELECT * FROM pre_upgrade.iceberg ORDER BY id", superuser_conn)
    assert len(result) == 2
    assert result[0] == [2, "hello"]
    assert result[1] == [3, "world"]

    superuser_conn.rollback()


@pytest.mark.skipif(
    os.getenv("TEST_PG_UPGRADE_FROM_BINDIR") == None,
    reason="set TEST_PG_UPGRADE_FROM_BINDIR to the old PG version to run",
)
def test_writable(superuser_conn, s3, extension):
    # Check whether we can delete post-upgrade
    run_command(
        "DELETE FROM pre_upgrade.writable WHERE (value).x % 2 = 0", superuser_conn
    )

    # Check whether we can query post-upgrade
    result = run_query(
        "SELECT key, map_type.extract(properties, 'reason') FROM pre_upgrade.writable WHERE (value).x < 10 ORDER BY Key",
        superuser_conn,
    )
    assert len(result) == 5
    assert result[0] == ["two-times-1", "tests"]
    assert result[1] == ["two-times-3", "tests"]

    superuser_conn.rollback()


@pytest.mark.skipif(
    os.getenv("TEST_PG_UPGRADE_FROM_BINDIR") == None,
    reason="set TEST_PG_UPGRADE_FROM_BINDIR to the old PG version to run",
)
def test_csv_table(superuser_conn, s3, extension):
    # Check whether we can query post-upgrade
    result = run_query(
        "SELECT value FROM pre_upgrade.csv_table WHERE id = 3 AND pi > 3",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0] == ["hello-3"]

    superuser_conn.rollback()

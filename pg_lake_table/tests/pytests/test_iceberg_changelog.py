import pytest
from utils_pytest import *
from helpers.spark import *


def test_copy_on_write_changelog(
    installcheck,
    spark_session,
    extension,
    s3,
    with_default_location,
    pg_conn,
    grant_access_to_data_file_partition,
):
    if installcheck:
        return
    run_command(
        """
					CREATE SCHEMA test_copy_on_write_changelog;

					CREATE TABLE test_copy_on_write_changelog.tbl(key text, value text) USING iceberg;

					INSERT INTO test_copy_on_write_changelog.tbl SELECT i::text, i::text FROM generate_series(0, 4)i;
	""",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_copy_on_write_changelog'",
        pg_conn,
    )[0][0]

    pre_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    # now, do a copy on write
    run_command("UPDATE test_copy_on_write_changelog.tbl SET value = 'ttt'", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_copy_on_write_changelog'",
        pg_conn,
    )[0][0]

    post_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    spark_register_table(
        installcheck,
        spark_session,
        "tbl",
        "test_copy_on_write_changelog",
        metadata_location,
    )

    spark_query = f"SELECT count(*) FROM test_copy_on_write_changelog.tbl"

    # sanity check
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == 5

    # now, create the change log on Spark
    spark_result = spark_session.sql(
        f"""
		CALL {server_params.SPARK_CATALOG}.system.create_changelog_view(
			  table => '{server_params.SPARK_CATALOG}.test_copy_on_write_changelog.tbl',
			  options => map('start-snapshot-id','{pre_snapshot_id}','end-snapshot-id', '{post_snapshot_id}'),
			  changelog_view => 'copy_on_write_change_log_view'
			  )
	"""
    ).collect()

    spark_result = spark_session.sql(
        f"""SELECT * FROM copy_on_write_change_log_view"""
    ).collect()

    # 5 UPDATEs == 5 INSERTs + 5 DELETEs
    assert len(spark_result) == 10

    insert_cnt, delete_cnt = 0, 0
    for spark_row in spark_result:
        spark_row["_commit_snapshot_id"] == post_snapshot_id
        spark_row["_change_ordinal"] == 0

        _change_type = spark_row["_change_type"]
        if _change_type == "DELETE":
            delete_cnt = delete_cnt + 1
        elif _change_type == "INSERT":
            insert_cnt = insert_cnt + 1

            assert spark_row["value"] == "ttt"
        else:
            assert False

    assert insert_cnt == 5
    assert delete_cnt == 5

    spark_unregister_table(
        installcheck, spark_session, "tbl", "test_copy_on_write_changelog"
    )

    run_command("DROP SCHEMA test_copy_on_write_changelog CASCADE", pg_conn)
    pg_conn.commit()


def test_truncate_changelog(
    installcheck,
    spark_session,
    extension,
    s3,
    with_default_location,
    pg_conn,
    grant_access_to_data_file_partition,
):
    if installcheck:
        return
    run_command(
        """
					CREATE SCHEMA test_truncate_changelog;

					CREATE TABLE test_truncate_changelog.tbl(key text, value text) USING iceberg;

					INSERT INTO test_truncate_changelog.tbl SELECT i::text, i::text FROM generate_series(0, 4)i;
	""",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_truncate_changelog'",
        pg_conn,
    )[0][0]

    pre_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    # now, do a copy on write
    run_command("TRUNCATE test_truncate_changelog.tbl", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_truncate_changelog'",
        pg_conn,
    )[0][0]

    post_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    spark_register_table(
        installcheck,
        spark_session,
        "tbl",
        "test_truncate_changelog",
        metadata_location,
    )

    spark_query = f"SELECT count(*) FROM test_truncate_changelog.tbl"

    # sanity check
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == 0

    # now, create the change log on Spark
    spark_result = spark_session.sql(
        f"""
		CALL {server_params.SPARK_CATALOG}.system.create_changelog_view(
			  table => '{server_params.SPARK_CATALOG}.test_truncate_changelog.tbl',
			  options => map('start-snapshot-id','{pre_snapshot_id}','end-snapshot-id', '{post_snapshot_id}'),
			  changelog_view => 'copy_on_write_change_log_view'
			  )
	"""
    ).collect()

    spark_result = spark_session.sql(
        f"""SELECT * FROM copy_on_write_change_log_view"""
    ).collect()

    # 5 DELETEs
    assert len(spark_result) == 5

    insert_cnt, delete_cnt = 0, 0
    for spark_row in spark_result:
        spark_row["_commit_snapshot_id"] == post_snapshot_id
        spark_row["_change_ordinal"] == 0

        _change_type = spark_row["_change_type"]
        if _change_type == "DELETE":
            delete_cnt = delete_cnt + 1
        elif _change_type == "INSERT":
            insert_cnt = insert_cnt + 1

            assert spark_row["value"] == "ttt"
        else:
            assert False

    assert insert_cnt == 0
    assert delete_cnt == 5

    spark_unregister_table(
        installcheck, spark_session, "tbl", "test_truncate_changelog"
    )

    run_command("DROP SCHEMA test_truncate_changelog CASCADE", pg_conn)
    pg_conn.commit()


def test_positional_delete_changelog(
    installcheck,
    spark_session,
    extension,
    s3,
    with_default_location,
    pg_conn,
    grant_access_to_data_file_partition,
):
    if installcheck:
        return
    run_command(
        """
					CREATE SCHEMA test_positional_delete_changelog;

					CREATE TABLE test_positional_delete_changelog.tbl(key text, value text) USING iceberg;

					INSERT INTO test_positional_delete_changelog.tbl SELECT i::text, i::text FROM generate_series(0, 99)i;
	""",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_positional_delete_changelog'",
        pg_conn,
    )[0][0]

    pre_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    # now, do a copy on write
    run_command(
        "DELETE FROM test_positional_delete_changelog.tbl WHERE key = '0'", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_positional_delete_changelog'",
        pg_conn,
    )[0][0]

    post_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    spark_register_table(
        installcheck,
        spark_session,
        "tbl",
        "test_positional_delete_changelog",
        metadata_location,
    )

    spark_query = f"SELECT count(*) FROM test_positional_delete_changelog.tbl"

    # sanity check
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == 99

    # now, create the change log on Spark
    spark_result = spark_session.sql(
        f"""
		CALL {server_params.SPARK_CATALOG}.system.create_changelog_view(
			  table => '{server_params.SPARK_CATALOG}.test_positional_delete_changelog.tbl',
			  options => map('start-snapshot-id','{pre_snapshot_id}','end-snapshot-id', '{post_snapshot_id}'),
			  changelog_view => 'copy_on_write_change_log_view'
			  )
	"""
    ).collect()

    # spark currently does not support positional deletes on the changelog
    try:
        spark_result = spark_session.sql(
            "SELECT * FROM copy_on_write_change_log_view"
        ).collect()
    except Exception as e:
        assert str(e) == "Delete files are currently not supported in changelog scans"

    spark_unregister_table(
        installcheck, spark_session, "tbl", "test_positional_delete_changelog"
    )

    run_command("DROP SCHEMA test_positional_delete_changelog CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_copy_changelog(
    installcheck,
    spark_session,
    extension,
    s3,
    with_default_location,
    pg_conn,
    grant_access_to_data_file_partition,
):

    if installcheck:
        return

    run_command(
        """
					CREATE SCHEMA test_insert_copy_changelog;

					CREATE TABLE test_insert_copy_changelog.tbl(key text, value text) USING iceberg;
	""",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_insert_copy_changelog'",
        pg_conn,
    )[0][0]

    pre_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    run_command(
        "INSERT INTO test_insert_copy_changelog.tbl SELECT i::text, i::text FROM generate_series(0, 4)i;",
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/test_insert_copy_changelog/data.parquet"

    run_command(
        f"COPY (SELECT * FROM test_insert_copy_changelog.tbl) TO '{url}'", pg_conn
    )
    run_command(f"COPY test_insert_copy_changelog.tbl FROM '{url}'", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_insert_copy_changelog'",
        pg_conn,
    )[0][0]

    post_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    spark_register_table(
        installcheck,
        spark_session,
        "tbl",
        "test_insert_copy_changelog",
        metadata_location,
    )

    spark_query = f"SELECT count(*) FROM test_insert_copy_changelog.tbl"

    # sanity check
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == 10

    # now, create the change log on Spark
    spark_result = spark_session.sql(
        f"""
		CALL {server_params.SPARK_CATALOG}.system.create_changelog_view(
			  table => '{server_params.SPARK_CATALOG}.test_insert_copy_changelog.tbl',
			  options => map('start-snapshot-id','{pre_snapshot_id}','end-snapshot-id', '{post_snapshot_id}'),
			  changelog_view => 'copy_on_write_change_log_view'
			  )
	"""
    ).collect()

    spark_result = spark_session.sql(
        f"""SELECT * FROM copy_on_write_change_log_view"""
    ).collect()

    # 10 INSERTs
    assert len(spark_result) == 10

    insert_cnt, delete_cnt = 0, 0
    for spark_row in spark_result:
        spark_row["_commit_snapshot_id"] == post_snapshot_id
        spark_row["_change_ordinal"] == 0

        _change_type = spark_row["_change_type"]
        if _change_type == "DELETE":
            delete_cnt = delete_cnt + 1
        elif _change_type == "INSERT":
            insert_cnt = insert_cnt + 1
        else:
            assert False

    assert insert_cnt == 10
    assert delete_cnt == 0

    spark_unregister_table(
        installcheck, spark_session, "tbl", "test_insert_copy_changelog"
    )

    run_command("DROP SCHEMA test_insert_copy_changelog CASCADE", pg_conn)
    pg_conn.commit()


def test_partitioned_change_log(
    installcheck,
    spark_session,
    extension,
    s3,
    with_default_location,
    pg_conn,
    grant_access_to_data_file_partition,
):

    if installcheck:
        return

    run_command(
        """
                    CREATE SCHEMA test_partitioned_change_log;

                    CREATE TABLE test_partitioned_change_log.tbl(customer_id int, id bigserial, t timestamptz default now()) using iceberg with (partition_by = 'customer_id,year(t)');

    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_partitioned_change_log'",
        pg_conn,
    )[0][0]

    pre_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    run_command(
        """
                    INSERT INTO test_partitioned_change_log.tbl values (1);
                    INSERT INTO test_partitioned_change_log.tbl values (3);
                    INSERT INTO test_partitioned_change_log.tbl select s % 10 from generate_series(1,1000) s; 
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_partitioned_change_log'",
        pg_conn,
    )[0][0]
    pg_conn.commit()

    run_command_outside_tx(
        [
            "VACUUM FULL test_partitioned_change_log.tbl;",
        ],
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'tbl' and table_namespace='test_partitioned_change_log'",
        pg_conn,
    )[0][0]
    post_snapshot_id = get_current_snapshot_id(s3, metadata_location)

    spark_register_table(
        installcheck,
        spark_session,
        "tbl",
        "test_partitioned_change_log",
        metadata_location,
    )

    spark_query = f"SELECT count(*) FROM test_partitioned_change_log.tbl"

    # sanity check
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == 1002

    # now, create the change log on Spark
    spark_result = spark_session.sql(
        f"""
        CALL {server_params.SPARK_CATALOG}.system.create_changelog_view(
              table => '{server_params.SPARK_CATALOG}.test_partitioned_change_log.tbl',
              options => map('start-snapshot-id','{pre_snapshot_id}','end-snapshot-id', '{post_snapshot_id}'),
              changelog_view => 'partitioned_change_log_view'
              )
    """
    ).collect()

    spark_result = spark_session.sql(
        f"""SELECT * FROM partitioned_change_log_view"""
    ).collect()

    assert len(spark_result) == 1002

    insert_cnt, delete_cnt = 0, 0
    for spark_row in spark_result:
        spark_row["_commit_snapshot_id"] == post_snapshot_id
        spark_row["_change_ordinal"] == 0

        _change_type = spark_row["_change_type"]
        if _change_type == "DELETE":
            delete_cnt = delete_cnt + 1
        elif _change_type == "INSERT":
            insert_cnt = insert_cnt + 1
        else:
            assert False

    assert insert_cnt == 1002
    assert delete_cnt == 0

    spark_unregister_table(
        installcheck, spark_session, "tbl", "test_partitioned_change_log"
    )

    run_command("DROP SCHEMA test_partitioned_change_log CASCADE", pg_conn)
    pg_conn.commit()


def get_current_snapshot_id(s3, metadata_location):
    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    return metadata_json["current-snapshot-id"]

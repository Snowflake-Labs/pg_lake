import pytest
from utils_pytest import *
from spark_utils import *

from test_writable_iceberg_common import *


def test_in_tx_with_partition_by(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_partition_by"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN b int;
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (ADD partition_by 'a');
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1, 1);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (2, 2);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3, 3);
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN c int;
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (SET partition_by 'b');
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (4, 1);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (5, 2);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (6, 3);
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data still exists after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a,b,c;"

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    manifests = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    assert len(manifests) == 2

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_tx_with_insert_only(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_insert_only"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
    """,
        pg_conn,
    )

    metadata_location_before_commit = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]

    pg_conn.commit()

    metadata_location_after_commit = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]

    assert metadata_location_before_commit == metadata_location_after_commit

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # no data pushed yet
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (2);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3);
    """,
        pg_conn,
    )

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 2

    # data still exists after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    manifests = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    assert len(manifests) == 1

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_tx_with_ddls(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_ddls"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');

        -- width: 1 (a)
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a) VALUES (1);

        -- ===== EXPAND (add columns one by one, insert each time) =====

        -- width: 2 (a,b)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN b INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b) VALUES (2,2);

        -- width: 3 (a,b,c)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN c INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c) VALUES (3,3,3);

        ALTER FOREIGN TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (ADD partition_by 'bucket(4, a)');

        -- width: 4 (a,b,c,d)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN d INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d) VALUES (4,4,4,4);

        -- width: 5 (a,b,c,d,e)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN e INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e) VALUES (5,5,5,5,5);

        -- width: 6 (a,b,c,d,e,f)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN f INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f) VALUES (6,6,6,6,6,6);

        -- width: 7 (a,b,c,d,e,f,g)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN g INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g) VALUES (7,7,7,7,7,7,7);

        -- width: 8 (a,b,c,d,e,f,g,h)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN h INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h) VALUES (8,8,8,8,8,8,8,8);

        -- width: 9 (a,b,c,d,e,f,g,h,i)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN i INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h,i) VALUES (9,9,9,9,9,9,9,9,9);

        -- width: 10 (a,b,c,d,e,f,g,h,i,j)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN j INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h,i,j) VALUES (10,10,10,10,10,10,10,10,10,10);

        -- width: 11 (a,b,c,d,e,f,g,h,i,j,k)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} ADD COLUMN k INT;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h,i,j,k) VALUES (11,11,11,11,11,11,11,11,11,11,11);

        -- ===== CONTRACT (drop columns in reverse, insert each time) =====

        ALTER FOREIGN TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (DROP partition_by);

        -- width: 10 (drop k)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN k;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h,i,j) VALUES (12,12,12,12,12,12,12,12,12,12);

        -- width: 9 (drop j)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN j;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h,i) VALUES (13,13,13,13,13,13,13,13,13);

        -- width: 8 (drop i)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN i;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g,h) VALUES (14,14,14,14,14,14,14,14);

        -- width: 7 (drop h)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN h;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f,g) VALUES (15,15,15,15,15,15,15);

        -- width: 6 (drop g)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN g;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e,f) VALUES (16,16,16,16,16,16);

        -- width: 5 (drop f)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN f;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d,e) VALUES (17,17,17,17,17);

        -- width: 4 (drop e)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN e;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c,d) VALUES (18,18,18,18);

        -- width: 3 (drop d)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN d;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b,c) VALUES (19,19,19);

        -- width: 2 (drop c)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN c;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a,b) VALUES (20,20);

        -- width: 1 (drop b)
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} DROP COLUMN b;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} (a) VALUES (21);
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data still exists after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    manifests = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    assert len(manifests) == 2

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_tx_with_create_drop_success(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_create_drop_success"

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # Clear any existing notices
    pg_conn.notices.clear()

    run_command(f"DROP SCHEMA {TABLE_NAMESPACE} CASCADE;", pg_conn)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    assert len(pg_conn.notices) > 0
    assert not any("WARNING:" in notice for notice in pg_conn.notices)

    # no metadata pushed because table is dropped in the same transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 0

    # no metadata pushed even after commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data still exists after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # vacuum should remove metadata and data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # metadata and data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)


def test_in_tx_with_create_drop_fail(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_create_drop_fail"

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # Clear any existing notices
    pg_conn.notices.clear()

    run_command(f"DROP SCHEMA {TABLE_NAMESPACE} CASCADE;", pg_conn)

    # no metadata pushed before rollback
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before rollback
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.rollback()

    assert len(pg_conn.notices) > 0
    assert not any("WARNING:" in notice for notice in pg_conn.notices)

    # no metadata pushed
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data still exists after rollback
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # vacuum should remove data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # metadata and data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)


def test_in_tx_with_drop_success(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_drop_success"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (2);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3);
    """,
        pg_conn,
    )

    run_command(f"DROP SCHEMA {TABLE_NAMESPACE} CASCADE;", pg_conn)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    before_commit_metadata_files = s3_list(pg_conn, metadata_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()

    # we did not push new metadata for the last tx changes in the transaction because table is dropped in the same transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    after_commit_metadata_files = s3_list(pg_conn, metadata_prefix)

    # metadata folder should contain same files as before
    assert len(after_commit_metadata_files) > 0
    assert len(after_commit_metadata_files) == len(before_commit_metadata_files)

    # unreferenced metadata and data files should exist after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # vacuum should remove metadata and data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # metadata and data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)


def test_in_tx_with_drop_fail(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_drop_fail"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    run_command(
        f"""
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3);
    """,
        pg_conn,
    )

    run_command(f"DROP SCHEMA {TABLE_NAMESPACE} CASCADE;", pg_conn)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # we push a single metadata file for all changes in the previous transaction
    before_rollback_metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(before_rollback_metadata_files) == 1

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.rollback()

    # we did not push new metadata for the last tx changes in the transaction because table is dropped in the same transaction
    after_rollback_metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(before_rollback_metadata_files) == len(after_rollback_metadata_files)

    # unreferenced data files should exist after rollback
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # vacuum should remove data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()

    # metadata files should still exist after drop table
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # vacuum should remove metadata and data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # metadata and data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)


def test_in_subtx_fail_with_drop(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_subtx_fail_with_drop"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;

        SAVEPOINT sp1;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (2);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3);
    """,
        pg_conn,
    )

    run_command(
        f"""
        DROP SCHEMA {TABLE_NAMESPACE} CASCADE;
        ROLLBACK TO SAVEPOINT sp1;
    """,
        pg_conn,
    )

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    before_commit_metadata_files = s3_list(pg_conn, metadata_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()

    # we did not push new metadata for the last tx changes in the transaction because table is dropped in the same transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    after_commit_metadata_files = s3_list(pg_conn, metadata_prefix)

    # metadata folder should contain same files as before
    assert len(after_commit_metadata_files) > 0
    assert len(after_commit_metadata_files) == len(before_commit_metadata_files)

    # unreferenced metadata and data files should exist after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # vacuum should remove metadata and data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # metadata and data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)


def test_in_subtx_success_with_drop(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_subtx_success_with_drop"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;

        SAVEPOINT sp1;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (2);
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3);
    """,
        pg_conn,
    )

    run_command(
        f"""
        DROP SCHEMA {TABLE_NAMESPACE} CASCADE;
        RELEASE SAVEPOINT sp1;
    """,
        pg_conn,
    )

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    before_commit_metadata_files = s3_list(pg_conn, metadata_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()

    # we did not push new metadata for the last tx changes in the transaction because table is dropped in the same transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    after_commit_metadata_files = s3_list(pg_conn, metadata_prefix)

    # metadata folder should contain same files as before
    assert len(after_commit_metadata_files) > 0
    assert len(after_commit_metadata_files) == len(before_commit_metadata_files)

    # unreferenced metadata and data files should exist after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # vacuum should remove metadata and data files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # metadata and data files should not exist in s3 after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)


def test_in_tx_with_truncate(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_truncate"

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} select i from generate_series(1,100) i;
        UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} set a = 101 where a = 100;
        TRUNCATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME};
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"
    results = run_query(query, pg_conn)
    assert results == []

    try:
        _ = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
        assert False, "Should not have any snapshot"
    except Exception as e:
        pg_conn.rollback()
        assert "No current snapshot found" in str(e)
    else:
        raise

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data files should exist after commit even if not exists in catalog
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # vacuum should remove transient files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # data files should not exist in s3 as well after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_subtx_fail_with_truncate(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_subtx_fail_with_truncate"

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');

        SAVEPOINT sp1;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} select i from generate_series(1,100) i;
        UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} set a = 101 where a = 100;
        TRUNCATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME};
        ROLLBACK TO SAVEPOINT sp1;
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"
    results = run_query(query, pg_conn)
    assert results == []

    try:
        _ = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
        assert False, "Should not have any snapshot"
    except Exception as e:
        pg_conn.rollback()
        assert "No current snapshot found" in str(e)
    else:
        raise

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data files should exist after commit even if not exists in catalog
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # vacuum should remove transient files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # data files should not exist in s3 as well after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_subtx_success_with_truncate(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_subtx_success_with_truncate"

    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');

        SAVEPOINT sp1;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} select i from generate_series(1,100) i;
        UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} set a = 101 where a = 100;
        TRUNCATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME};
        RELEASE SAVEPOINT sp1;
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"
    results = run_query(query, pg_conn)
    assert results == []

    try:
        _ = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
        assert False, "Should not have any snapshot"
    except Exception as e:
        pg_conn.rollback()
        assert "No current snapshot found" in str(e)
    else:
        raise

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data files should exist after commit even if not exists in catalog
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    # vacuum should remove transient files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    # data files should not exist in s3 as well after vacuum
    assert not s3_prefix_contains_any_file(pg_conn, data_prefix)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_deleted_manifest_entry_pruning(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
    create_iceberg_table,
):
    if installcheck:
        return

    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_deleted_manifest_entry_pruning"

    # nothing pruned yet, we need another change to prune previous snapshot's deleted entries
    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} select i from generate_series(1,100) i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )
    assert manifest_entries == [[0, "ADDED"]]

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        None,  # duckdb_conn
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    run_command(
        f"""
        TRUNCATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME};
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )
    assert manifest_entries == [[0, "DELETED"]]

    # manifest merge is disabled so pruning does not happen during the partition_by change
    run_command(
        f"""
        SET pg_lake_iceberg.enable_manifest_merge_on_write = false;
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (ADD partition_by 'a');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )
    assert manifest_entries == [[0, "DELETED"]]

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        None,  # duckdb_conn
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # manifest merge is enabled so pruning happens during the partition_by change
    run_command(
        f"""
        SET pg_lake_iceberg.enable_manifest_merge_on_write = true;
        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (SET partition_by 'bucket(4, a)');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    manifest_entries = run_query(
        f"SELECT sequence_number, status FROM lake_iceberg.current_manifest_entries('{metadata_location}') ORDER BY sequence_number, status ASC;",
        pg_conn,
    )
    assert manifest_entries == []

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        None,  # duckdb_conn
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_tx_with_multiple_partition_by(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_with_multiple_partition_by"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');

        -- insert with default spec
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);

        ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (ADD partition_by 'a');

        -- insert with the identity spec
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
        
        DO $$
        DECLARE
          i int;
        BEGIN
          FOR i IN 1..10 LOOP
           EXECUTE 'ALTER TABLE {TABLE_NAMESPACE}.{TABLE_NAME} OPTIONS (SET partition_by ''bucket(' || i || ', a)'')';

           -- insert with the bucket spec
           INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
          END LOOP;
        END
        $$ LANGUAGE plpgsql;
    """,
        pg_conn,
    )

    metadata_prefix = table_metadata_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    data_prefix = table_data_prefix(pg_conn, TABLE_NAMESPACE, TABLE_NAME)

    # no metadata pushed before commit
    assert not s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data pushed before commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    pg_conn.commit()

    # we push a single metadata file for all changes in the transaction
    metadata_files = s3_list(pg_conn, metadata_prefix + "/*.json")
    assert len(metadata_files) == 1

    # metadata pushed after commit
    assert s3_prefix_contains_any_file(pg_conn, metadata_prefix)

    # data still exists after commit
    assert s3_prefix_contains_any_file(pg_conn, data_prefix)

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # 1 default spec + 1 identity spec + 10 bucket specs = 12 specs
    partition_specs = table_partition_specs(pg_conn, TABLE_NAME)
    assert len(partition_specs) == 12

    manifests = get_current_manifests(pg_conn, TABLE_NAMESPACE, TABLE_NAME)
    assert len(manifests) == 12

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def test_in_tx_transient_files(
    installcheck,
    s3,
    pg_conn,
    duckdb_conn,
    spark_session,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    TABLE_NAMESPACE = "test_multiple_ddl_dml_in_tx"
    TABLE_NAME = "test_in_tx_transient_files"

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE};
        CREATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME} (a int) USING iceberg WITH (autovacuum_enabled='False');
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (1);
    """,
        pg_conn,
    )
    pg_conn.commit()

    # there should be transient data files due to inserts + updates + truncate
    # there should be transient manifests (2 merged into 1) due to manifest merge
    run_command(
        f"""
        SET LOCAL pg_lake_iceberg.manifest_min_count_to_merge TO 2;
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT i FROM generate_series(1,100) i;
        UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET a = 101 WHERE a = 100;
        UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET a = 102 WHERE a = 101;
        TRUNCATE TABLE {TABLE_NAMESPACE}.{TABLE_NAME};
        INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} VALUES (3);
    """,
        pg_conn,
    )
    pg_conn.commit()

    in_progress_files = run_query(
        f"SELECT path FROM lake_engine.in_progress_files;",
        pg_conn,
    )
    assert len(in_progress_files) == 6
    assert sum(1 for path in in_progress_files if path[0].endswith(".avro")) == 2
    assert sum(1 for path in in_progress_files if path[0].endswith(".parquet")) == 4

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY a;"

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # vacuum should remove transient files from in-progress queue
    run_command_outside_tx(["VACUUM (iceberg)"], pg_conn)
    pg_conn.commit()

    in_progress_files = run_query(
        f"SELECT path FROM lake_engine.in_progress_files;",
        pg_conn,
    )
    assert in_progress_files == []

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE} CASCADE;", pg_conn)
    pg_conn.commit()


def get_current_manifests(pg_conn, table_namespace, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{table_name}' AND table_namespace = '{table_namespace}';",
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

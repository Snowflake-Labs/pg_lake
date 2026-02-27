import pytest
import psycopg2
from utils_pytest import *
from helpers.spark import *
import json
import re

from test_writable_iceberg_common import *


@pytest.mark.parametrize(
    "manifest_min_count_to_merge, target_manifest_size_kb, max_snapshot_age_params ",
    manifest_snapshot_settings,
)
def test_writable_iceberg_table(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    superuser_conn,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    manifest_min_count_to_merge,
    target_manifest_size_kb,
    max_snapshot_age_params,
    allow_iceberg_guc_perms,
    create_iceberg_table,
    create_test_helper_functions,
):
    # make sure that we can on iceberg tables with the
    # default value of the setting
    # note that we use non-default setting in the tests
    # because we have lots of external catalog modifications

    run_command(
        f"""
            SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};
            SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb};
            SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    TABLE_NAME = create_iceberg_table

    # show that we can read empty tables
    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
    assert results[0][0] == 0

    # generate some data
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 100
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # delete rows
    run_command(
        f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (0, 1, 2)", pg_conn
    )
    pg_conn.commit()

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
    assert results[0][0] == 97
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (0, 1, 2)"
    results = run_query(
        f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (0, 1, 2)",
        pg_conn,
    )
    assert results[0][0] == 0
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # update merge on read
    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = -1 WHERE id IN (3, 4, 5)",
        pg_conn,
    )
    pg_conn.commit()

    # trigger snapshot retention
    vacuum_commands = [
        f"SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};",
        f"SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb}",
        f"SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};",
        f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
    assert results[0][0] == 97
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (3, 4, 5)"
    results = run_query(query, pg_conn)
    assert results[0][0] == 0
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = -1"
    results = run_query(
        f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = -1", pg_conn
    )
    assert results[0][0] == 3
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # generate deleted file
    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = -2 WHERE id IN (10, 11, 12)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = -2", pg_conn)
    pg_conn.commit()

    # trigger snapshot retention
    run_command_outside_tx(vacuum_commands)

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
    assert results[0][0] == 94
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    query = (
        f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (10, 11, 12)"
    )
    results = run_query(query, pg_conn)
    assert results[0][0] == 0
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = -2"
    results = run_query(query, pg_conn)
    assert results[0][0] == 0
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # some more bulk inserts
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME}",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 94 * 2

    # trigger snapshot retention
    run_command_outside_tx(vacuum_commands)

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
    assert results[0][0] == 94 * 2
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    run_command(f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET value ='iceberg'", pg_conn)
    pg_conn.commit()

    query = (
        f"SELECT count(*), count(DISTINCT value) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    )
    results = run_query(query, pg_conn)
    assert results[0][0] == 94 * 2
    assert results[0][1] == 1

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        # duckdb_conn,  # broken in duckdb-iceberg 1.3.2; retest after 1.3.3 is available
        None,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    run_command(f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id <90", pg_conn)

    pg_conn.commit()

    # trigger snapshot retention
    run_command_outside_tx(vacuum_commands)

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY 1,2"
    results = run_query(query, pg_conn)
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        # duckdb_conn,  # broken in duckdb-iceberg 1.3.2; retest after 1.3.3 is available
        None,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )
    pg_conn.commit()

    # copy command with superuser as we cannot use psql's \copy command inside a transaction
    run_command(
        f"COPY (SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME}) TO '/tmp/some_random_file_name.data'",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"COPY {TABLE_NAMESPACE}.{TABLE_NAME} FROM '/tmp/some_random_file_name.data'",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id <80", superuser_conn
    )
    superuser_conn.commit()

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET value = 'some val' WHERE id <3",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET value = 'some val' WHERE id <60",
        superuser_conn,
    )
    superuser_conn.commit()

    query = f"SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME} ORDER BY 1,2"
    results = run_query(query, superuser_conn)
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        # duckdb_conn,  # broken in duckdb-iceberg 1.3.2; retest after 1.3.3 is available
        None,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # trigger snapshot retention
    run_command_outside_tx(vacuum_commands)
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        # duckdb_conn,  # broken in duckdb-iceberg 1.3.2; retest after 1.3.3 is available
        None,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )
    superuser_conn.commit()

    # now, make sure we do not leak any intermediate files
    assert_postgres_tmp_folder_empty()

    run_command_outside_tx([f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}"], pg_conn)

    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    run_command(
        f"""
            RESET pg_lake_iceberg.manifest_min_count_to_merge;
            RESET pg_lake_iceberg.target_manifest_size_kb;
            RESET pg_lake_iceberg.max_snapshot_age;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

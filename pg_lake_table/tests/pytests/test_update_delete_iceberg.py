import pytest
import psycopg2
from utils_pytest import *
from spark_utils import *
import json
import re

from test_writable_iceberg_common import *


@pytest.mark.parametrize(
    "manifest_min_count_to_merge, target_manifest_size_kb, max_snapshot_age_params ",
    manifest_snapshot_settings,
)
def test_writable_iceberg_table_multiple_del_update(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    create_test_helper_functions,
    manifest_min_count_to_merge,
    target_manifest_size_kb,
    max_snapshot_age_params,
    allow_iceberg_guc_perms,
    create_iceberg_table,
):
    run_command(
        f"""
            SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};
            SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb};
            SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};
        """,
        pg_conn,
    )
    pg_conn.commit()

    TABLE_NAME = create_iceberg_table

    # generate some data
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT i, i::text FROM generate_series(0,99)i",
        pg_conn,
    )
    pg_conn.commit()

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}"
    results = run_query(query, pg_conn)
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

    run_command(
        f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (2, 3, 4)", pg_conn
    )
    pg_conn.commit()

    run_command(
        f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id IN (3, 4, 6)", pg_conn
    )
    pg_conn.commit()

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

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{TABLE_NAME}' and table_namespace='{TABLE_NAMESPACE}'",
        pg_conn,
    )[0][0]
    results = run_query(
        f"SELECT sequence_number, added_files_count, added_rows_count FROM lake_iceberg.current_manifests('{metadata_location}') ORDER BY sequence_number ASC",
        pg_conn,
    )

    # first command added 100 rows via generate series
    # the second removed 3 rows (0,1,2)
    # the third removed 2 rows (3,4)
    # the fourth removed 1 rows (6)
    assert results == [[1, 1, 100], [2, 1, 3], [3, 1, 2], [4, 1, 1]]

    # zero update rows
    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100000 WHERE id IN (0, 1, 2)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100000 WHERE id IN (2, 3, 4)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100000 WHERE id IN (3, 4, 6)",
        pg_conn,
    )
    pg_conn.commit()

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 100000"
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

    # few update rows
    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100000 WHERE id IN (10)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100000 WHERE id IN (11, 12, 13)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100000 WHERE id IN (14, 15, 16)",
        pg_conn,
    )
    pg_conn.commit()

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 100000"
    results = run_query(query, pg_conn)
    assert results[0][0] == 7
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # large delete and update
    run_command(
        f"DELETE FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id > 30 and id < 55", pg_conn
    )
    pg_conn.commit()

    query = (
        f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id > 30 and id < 55"
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

    run_command(
        f"UPDATE {TABLE_NAMESPACE}.{TABLE_NAME} SET id = 100001 WHERE id > 70 and id < 100",
        pg_conn,
    )
    pg_conn.commit()

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 100001"
    results = run_query(query, pg_conn)
    assert results[0][0] == 29
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

    # trigger snapshot retention
    vacuum_commands = [
        f"SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};",
        f"SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb}",
        f"SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};",
        f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}",
    ]
    run_command_outside_tx(vacuum_commands)

    query = f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME} WHERE id = 100001"
    results = run_query(query, pg_conn)
    assert results[0][0] == 29
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

    run_command("COMMIT;", pg_conn)

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
        pg_conn,
    )
    pg_conn.commit()

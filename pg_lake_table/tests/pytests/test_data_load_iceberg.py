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
def test_writable_iceberg_table_insert(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    superuser_conn,
    duckdb_conn,
    s3,
    extension,
    create_iceberg_table,
    create_test_helper_functions,
    manifest_min_count_to_merge,
    target_manifest_size_kb,
    max_snapshot_age_params,
):

    TABLE_NAME = create_iceberg_table

    run_command(
        f"""
            SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};
            SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb};
            SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};
        """,
        pg_conn,
    )
    pg_conn.commit()

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

    # basic INSERT .. SELECT
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.{TABLE_NAME} SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME}",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 200
    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    # basic COPY into
    pg_conn.commit()
    superuser_conn.commit()

    tmpfilepath = generate_random_file_path()
    print("tmpfilepath:" + tmpfilepath)
    run_command(
        f"COPY (SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME}) TO '{tmpfilepath}'",
        superuser_conn,
    )
    run_command(
        f"COPY {TABLE_NAMESPACE}.{TABLE_NAME} FROM '{tmpfilepath}'", superuser_conn
    )
    superuser_conn.commit()

    results = run_query(f"SELECT count(*) FROM {TABLE_NAMESPACE}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 400
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        TABLE_NAME,
        TABLE_NAMESPACE,
        query,
    )

    run_command_outside_tx([f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}"], pg_conn)
    assert_iceberg_s3_file_consistency(pg_conn, s3, TABLE_NAMESPACE, TABLE_NAME)

    pg_conn.commit()

    # now, make sure we do not leak any intermediate files
    assert_postgres_tmp_folder_empty()

    run_command(
        f"""
            RESET pg_lake_iceberg.manifest_min_count_to_merge;
            RESET pg_lake_iceberg.target_manifest_size_kb;
            RESET pg_lake_iceberg.max_snapshot_age;
        """,
        pg_conn,
    )
    pg_conn.commit()

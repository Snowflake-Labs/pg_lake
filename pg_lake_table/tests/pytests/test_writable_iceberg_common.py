import pytest
import psycopg2
from utils_pytest import *
import json
import re
import random

TABLE_NAMESPACE = "test_writable_iceberg"


@pytest.fixture
def allow_iceberg_guc_perms(superuser_conn, app_user):
    run_command(
        f"""
        GRANT SET ON PARAMETER pg_lake_iceberg.manifest_min_count_to_merge TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.target_manifest_size_kb TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.max_snapshot_age TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.max_compactions_per_vacuum TO {app_user};
        GRANT USAGE ON SCHEMA lake_iceberg TO {app_user};
        GRANT SELECT ON lake_iceberg.tables TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield


# many tests relies on this array, and execution
# times of these are non-negligible
# in order to improve CI times, we pick only one per run
# if there is a bug, we'll easily catch in some of the runs
_all_manifest_snapshot_settings = [
    ("2", "8_000", "1"),  # very aggressive (easy to merge)
    ("10", "1_000", "0"),  # less aggressive
    ("100", "8", "0"),  # default values
    ("10000", "1", "10000"),  # not aggressive (hard to merge)
]
manifest_snapshot_settings = [random.choice(_all_manifest_snapshot_settings)]


def assert_metadata_log_order(metadata_logs):
    prev_timestamp = None
    prev_file_num = None

    for entry in metadata_logs:
        timestamp = entry["timestamp-ms"]
        file_num_match = re.search(r"(\d{5})-", entry["metadata-file"])
        assert file_num_match, f"Could not extract number from {entry['metadata-file']}"

        file_num = int(file_num_match.group(1))

        if prev_timestamp is not None:
            assert timestamp >= prev_timestamp, "Metadata timestamps are not increasing"

        if prev_file_num is not None:
            assert file_num > prev_file_num, "File numbers are not increasing"

        prev_timestamp = timestamp
        prev_file_num = file_num


def assert_snapshot_log_order(snapshot_logs):
    prev_timestamp = None

    for entry in snapshot_logs:
        timestamp = entry["timestamp-ms"]
        if prev_timestamp is not None:
            assert timestamp >= prev_timestamp, "Snapshot timestamps are not increasing"

        prev_timestamp = timestamp


def assert_postgres_tmp_folder_empty():
    tmp_folder = f"{server_params.PG_DIR}/base/pgsql_tmp/"

    # Check if the directory exists
    if os.path.exists(tmp_folder):
        # Assert that the directory is empty
        assert not os.listdir(
            tmp_folder
        ), f"Temporary PostgreSQL folder '{tmp_folder}' is not empty"
    else:
        # If the folder does not exist, it is effectively empty
        assert True


@pytest.fixture(scope="module")
def install_iceberg_to_duckdb(duckdb_conn):
    duckdb_conn.execute("INSTALL iceberg")
    duckdb_conn.execute("LOAD iceberg")


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

        CREATE OR REPLACE FUNCTION lake_iceberg.current_manifest_entries(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                status TEXT,
                snapshot_id BIGINT,
                sequence_number BIGINT,
                data_file TEXT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_manifest_entries$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_table_metadata(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_table_metadata$function$;

     CREATE OR REPLACE FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_all_referenced_files$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text) TO public;

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
        DROP FUNCTION lake_iceberg.current_manifest_entries;
        DROP FUNCTION lake_iceberg.reserialize_iceberg_table_metadata;
        DROP FUNCTION lake_iceberg.find_all_referenced_files;
""",
        superuser_conn,
    )
    superuser_conn.commit()


# Sequence number to generate unique table names
table_counter = 0


# we need to generate unique table names
# otherwise we cannot call assert_iceberg_s3_file_consistency()
# as the s3 bucket would have artifacts from earlier tests
@pytest.fixture
def generate_table_name():
    global table_counter
    table_counter += 1

    TEST_TABLE_NAME = "test_writable_iceberg" + str(table_counter)

    return f"{TEST_TABLE_NAME}_" + str(table_counter)


@pytest.fixture
def create_iceberg_table(pg_conn, with_default_location, generate_table_name):
    table_name = generate_table_name  # Get the generated table name

    # Create schema and table
    run_command(f"CREATE SCHEMA {TABLE_NAMESPACE}_tmp", pg_conn)
    run_command(f"CREATE SCHEMA {TABLE_NAMESPACE}", pg_conn)
    run_command(
        f"CREATE TABLE {TABLE_NAMESPACE}_tmp.{table_name}_tmp (drop_col_1 INT, id_old bigint, drop_col_2 INT) USING iceberg",
        pg_conn,
    )
    run_command(
        f"ALTER TABLE {TABLE_NAMESPACE}_tmp.{table_name}_tmp SET SCHEMA {TABLE_NAMESPACE}",
        pg_conn,
    )
    run_command(
        f"ALTER TABLE {TABLE_NAMESPACE}.{table_name}_tmp RENAME TO {table_name}",
        pg_conn,
    )

    # adding/dropping column triggers a new schema generation, so make the test
    # slightly more complicated
    run_command(
        f"ALTER TABLE {TABLE_NAMESPACE}.{table_name} DROP COLUMN drop_col_2, ADD COLUMN value text, DROP COLUMN drop_col_1",
        pg_conn,
    )
    run_command(
        f"ALTER TABLE {TABLE_NAMESPACE}.{table_name} RENAME COLUMN id_old TO id",
        pg_conn,
    )
    run_command(
        f"ALTER FOREIGN TABLE {TABLE_NAMESPACE}.{table_name} OPTIONS (ADD autovacuum_enabled 'false')",
        pg_conn,
    )

    pg_conn.commit()

    yield table_name  # Yield the table name for further operations in the test

    # Rollback and clean up after test
    pg_conn.rollback()
    run_command(
        f"DROP SCHEMA {TABLE_NAMESPACE}, {TABLE_NAMESPACE}_tmp CASCADE", pg_conn
    )
    pg_conn.commit()


@pytest.fixture
def create_iceberg_rest_table(
    pg_conn,
    with_default_location,
    generate_table_name,
    polaris_session,
    set_polaris_gucs,
    installcheck,
):
    if installcheck:
        return
    table_name = generate_table_name  # Get the generated table name

    # Create schema and table
    run_command(f"CREATE SCHEMA {TABLE_NAMESPACE}", pg_conn)
    run_command(
        f"CREATE TABLE {TABLE_NAMESPACE}.{table_name} (drop_col_1 INT, id_old bigint, drop_col_2 INT) USING iceberg WITH (catalog='rest')",
        pg_conn,
    )

    # adding/dropping column triggers a new schema generation, so make the test
    # slightly more complicated
    run_command(
        f"ALTER TABLE {TABLE_NAMESPACE}.{table_name} DROP COLUMN drop_col_2, ADD COLUMN value text, DROP COLUMN drop_col_1",
        pg_conn,
    )
    run_command(
        f"ALTER TABLE {TABLE_NAMESPACE}.{table_name} RENAME COLUMN id_old TO id",
        pg_conn,
    )
    run_command(
        f"ALTER FOREIGN TABLE {TABLE_NAMESPACE}.{table_name} OPTIONS (ADD autovacuum_enabled 'false')",
        pg_conn,
    )

    pg_conn.commit()

    yield table_name  # Yield the table name for further operations in the test

    # Rollback and clean up after test
    pg_conn.rollback()
    run_command(f"DROP SCHEMA {TABLE_NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()

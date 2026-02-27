"""Iceberg-specific test utilities, path helpers, and fixtures."""

import json
import os
import tempfile
import time
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from . import server_params
from .cloud_storage import (
    MOTO_PORT,
    TEST_AWS_ACCESS_KEY_ID,
    TEST_AWS_SECRET_ACCESS_KEY,
    TEST_BUCKET,
    parse_s3_path,
    read_s3_operations,
    s3_upload_dir,
)
from .db import (
    open_pg_conn,
    run_command,
    run_command_outside_tx,
    run_query,
)
from .json import write_json_to_file


# ---------------------------------------------------------------------------
# Iceberg file sorting / normalisation helpers
# ---------------------------------------------------------------------------

# for test consistency
def file_sort_key(file_entry):
    filename = file_entry[0]

    if ".metadata.json" in filename:
        return (1, filename)  # Metadata files come first
    elif "snap-" in filename and ".avro" in filename:
        return (2, filename)  # Snapshot files come next
    elif ".avro" in filename and "-m0" in filename:
        return (3, filename)  # Manifest files come after metadata
    elif ".avro" in filename and "-m1" in filename:
        return (4, filename)  # Manifest files come after metadata
    elif ".avro" in filename and "-m2" in filename:
        return (5, filename)  # Manifest files come after metadata
    elif ".avro" in filename and "-m3" in filename:
        return (6, filename)  # Manifest files come after metadata
    elif ".parquet" in filename:
        if "data_" in filename:
            # Second element in tuple sorts by filename within the data files
            return (7, filename)  # Data files come after delete files
        else:
            return (8, filename)  # Positional delete files first

    return (0, filename)  # Default order, sorted by filename


# Normalize both lists to use the same key (e.g., 'path')
def normalize_dictrow(row):
    if "filename" in row:
        return row["filename"]  # Return the file path if the key is 'filename'
    elif "path" in row:
        return row["path"]  # Return the file path if the key is 'path'
    else:
        return row  # Handle unexpected cases


# ---------------------------------------------------------------------------
# Iceberg sample-data paths
# ---------------------------------------------------------------------------

def iceberg_metadata_json_folder_path():
    return str(Path(__file__).parent.parent / "sample" / "iceberg" / "metadata_json")


def iceberg_sample_table_folder_path():
    return str(Path(__file__).parent.parent / "sample" / "iceberg" / "sample_tables")


def iceberg_metadata_manifest_folder_path():
    return str(Path(__file__).parent.parent / "sample" / "iceberg" / "manifests")


# ---------------------------------------------------------------------------
# Iceberg catalog helpers
# ---------------------------------------------------------------------------

def create_iceberg_test_catalog(pg_conn):
    catalog_user = "iceberg_test_catalog"

    result = run_query(
        f"SELECT 1 FROM pg_roles WHERE rolname='{catalog_user}'", pg_conn
    )
    if len(result) == 0:
        run_command(f"CREATE USER {catalog_user}", pg_conn)

    run_command(f"GRANT iceberg_catalog TO {catalog_user}", pg_conn)
    pg_conn.commit()

    catalog = SqlCatalog(
        "pyiceberg",
        **{
            "uri": f"postgresql+psycopg2://{catalog_user}@localhost:{server_params.PG_PORT}/{server_params.PG_DATABASE}",
            "warehouse": f"s3://{TEST_BUCKET}/iceberg/",
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
        },
    )
    catalog.create_namespace("public")

    return catalog


# ---------------------------------------------------------------------------
# Iceberg S3 file inspection / consistency checks
# ---------------------------------------------------------------------------

def assert_iceberg_s3_file_consistency(
    pg_conn,
    s3,
    table_namespace,
    table_name,
    metadata_location=None,
    current_metadata_path=None,
    prev_metadata_path=None,
):

    files_via_s3_list = iceberg_s3_list_all_files_for_table(
        pg_conn, s3, table_namespace, table_name, metadata_location
    )

    if current_metadata_path is None:
        current_metadata_path = run_query(
            f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
            pg_conn,
        )[0][0]
    files_via_current_iceberg_metadata = iceberg_get_referenced_files_metadata_path(
        pg_conn, current_metadata_path
    )

    if prev_metadata_path is None:
        prev_metadata_path = run_query(
            f"SELECT previous_metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
            pg_conn,
        )[0][0]
    if prev_metadata_path:
        files_via_current_iceberg_metadata.append(prev_metadata_path)

    # Apply normalization
    normalized_s3 = set([normalize_dictrow(row) for row in files_via_s3_list])
    normalized_iceberg = set(
        [normalize_dictrow(row) for row in files_via_current_iceberg_metadata]
    )

    # Find files that are only in one of the sets
    only_in_s3 = normalized_s3 - normalized_iceberg
    only_in_iceberg = normalized_iceberg - normalized_s3

    # Assert if both sets are the same
    assert len(normalized_s3) > 0
    assert len(normalized_iceberg) > 0
    assert (
        normalized_s3 == normalized_iceberg
    ), f"Files differ:\nOnly in S3: {only_in_s3}\nOnly in Iceberg: {only_in_iceberg}"


def iceberg_get_referenced_files(pg_conn, table_name):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]

    return iceberg_get_referenced_files_metadata_path(pg_conn, metadata_location)


def iceberg_get_referenced_files_metadata_path(pg_conn, metadata_location):
    referenced_files = run_query(
        f"""SELECT * FROM lake_iceberg.find_all_referenced_files('{metadata_location}')""",
        pg_conn,
    )

    # get consistent results
    referenced_files.sort(key=file_sort_key)

    return referenced_files


def iceberg_s3_list_all_files_for_table(
    pg_conn, s3, table_namespace, table_name, table_location=None
):

    if table_location is None:
        metadata_location = run_query(
            f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
            pg_conn,
        )[0][0]
        metadata_json = read_s3_operations(s3, metadata_location)
        metadata_json = json.loads(metadata_json)
        table_location = metadata_json["location"]

    all_files = run_query(
        f"SELECT path FROM lake_file.list('{table_location}/**')", pg_conn
    )
    print("iceberg_s3_list_all_files_for_table: table_location=", table_location)
    print("iceberg_s3_list_all_files_for_table: all_files=", all_files)
    return all_files


# full path : s3://testbucketcdw/postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/metadata/4dcf5d74-ef51-494c-b5f1-87c4707a8c62/metadata0.json
# table_metadata_prefix: postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/metadata/
def table_metadata_prefix(pg_conn, table_namespace, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]

    bucket, s3_key = parse_s3_path(metadata_location)

    return f"s3://{bucket}/" + "/".join(s3_key.split("/")[:-1]) + "/**"


# full path : s3://testbucketcdw/postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/metadata/4dcf5d74-ef51-494c-b5f1-87c4707a8c62/metadata0.json
# table_data_prefix: postgres/test_multiple_ddl_dml_in_tx/test_in_tx_with_create_drop/17432/data/
def table_data_prefix(pg_conn, table_namespace, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]

    bucket, s3_key = parse_s3_path(metadata_location)

    return f"s3://{bucket}/" + "/".join(s3_key.split("/")[:-2]) + "/data/**"


def s3_list(pg_conn, uri):
    result = run_query(f"select * from lake_file.list('{uri}')", pg_conn)

    return result


def s3_prefix_contains_any_file(pg_conn, prefix):
    exists = run_query(f"select count(*) > 0 from lake_file.list('{prefix}')", pg_conn)[
        0
    ][0]

    return exists


def table_partition_specs(pg_conn, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]

    pg_query = f"SELECT * FROM lake_iceberg.metadata('{metadata_location}')"

    metadata = run_query(pg_query, pg_conn)[0][0]
    return metadata["partition-specs"]


# ---------------------------------------------------------------------------
# Iceberg metadata regeneration helpers
# ---------------------------------------------------------------------------

def regenerate_metadata_json(superuser_conn, metadata_location, s3):

    command = f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_location}')::json"
    res = run_query(command, superuser_conn)

    json_string = res[0][0]

    metadata_tmpfile = tempfile.NamedTemporaryFile()

    write_json_to_file(metadata_tmpfile.name, json_string)
    bucket, s3_key = parse_s3_path(metadata_location)
    s3.upload_file(metadata_tmpfile.name, bucket, s3_key)


def regenerate_manifest_file(superuser_conn, manifest_location, s3):

    manifest_tmpfile = tempfile.NamedTemporaryFile()
    command = f"SELECT lake_iceberg.reserialize_iceberg_manifest('{manifest_location}', '{manifest_tmpfile.name}')"

    run_command(command, superuser_conn)

    bucket, s3_key = parse_s3_path(manifest_location)
    s3.upload_file(manifest_tmpfile.name, bucket, s3_key)


def regenerate_manifest_list_file(superuser_conn, manifest_list_location, s3):

    manifest_list_tmpfile = tempfile.NamedTemporaryFile()
    command = f"SELECT lake_iceberg.reserialize_iceberg_manifest_list('{manifest_list_location}', '{manifest_list_tmpfile.name}')"

    run_command(command, superuser_conn)

    bucket, s3_key = parse_s3_path(manifest_list_location)
    s3.upload_file(manifest_list_tmpfile.name, bucket, s3_key)


def manifest_list_file_location(superuser_conn, metadata_location):
    res = run_query(
        f"""
        SELECT lake_iceberg.manifest_list_path_from_table_metadata('{metadata_location}');
""",
        superuser_conn,
    )

    manifest_list_path = res[0][0]
    return manifest_list_path


def manifest_file_locations(superuser_conn, manifest_list_location):
    res = run_query(
        f"""
        SELECT lake_iceberg.manifest_paths_from_manifest_list('{manifest_list_location}');
""",
        superuser_conn,
    )

    manifest_paths = res[0][0]
    return manifest_paths


def change_timezone(superuser_conn, tz):
    old_timezone = run_query("SHOW timezone", superuser_conn)[0][0]

    run_command_outside_tx(
        [
            f"ALTER SYSTEM SET timezone = '{tz}';",
            "SELECT pg_reload_conf();",
        ],
        superuser_conn,
    )

    return old_timezone


def wait_until_object_store_writable_table_pushed(
    superuser_conn, table_namespace, table_name
):

    cmd_1 = f"""SELECT metadata_location FROM lake_iceberg.list_object_store_tables(current_database()) WHERE catalog_table_name = '{table_name}' and catalog_namespace='{table_namespace}'"""
    cmd_2 = f"""SELECT metadata_location FROM iceberg_tables WHERE table_name='{table_name}' and table_namespace ilike '%{table_namespace}%'"""

    cnt = 0

    while True:
        run_command("SELECT pg_sleep(0.1)", superuser_conn)
        cnt += 1
        # up to 4 seconds
        # the default is 1 second
        if cnt == 40:
            break

        res1 = run_query(cmd_1, superuser_conn)
        if res1 is None or len(res1) == 0:
            continue

        res2 = run_query(cmd_2, superuser_conn)

        if res2 == res1:
            return
    dbname = run_query("SELECT current_database()", superuser_conn)

    res1 = run_query(
        "SELECT *  FROM lake_iceberg.list_object_store_tables(current_database())",
        superuser_conn,
    )
    res2 = run_query(
        "SELECT * FROM iceberg_tables",
        superuser_conn,
    )
    assert (
        False
    ), f"failed to refresh object catalog table {dbname}: {str(res1)}: {str(res2)}"


def wait_until_object_store_writable_table_removed(
    superuser_conn, table_namespace, table_name
):

    cmd = f"""SELECT * FROM lake_iceberg.list_object_store_tables(current_database()) WHERE catalog_table_name = '{table_name}' and catalog_namespace='{table_namespace}'"""

    cnt = 0

    while True:
        run_command("SELECT pg_sleep(0.1)", superuser_conn)
        cnt += 1
        # up to 4 seconds
        # the default is 1 second
        if cnt == 40:
            break

        res = run_query(cmd, superuser_conn)
        if res is None or len(res) == 0:
            return

    # Give a nice assertion error
    dbname = run_query("SELECT current_database()", superuser_conn)
    res = run_query(
        "SELECT *  FROM lake_iceberg.list_object_store_tables(current_database())",
        superuser_conn,
    )
    assert False, f"failed to refresh object catalog table {dbname}: {str(res)}"


# ---------------------------------------------------------------------------
# Iceberg fixtures
# ---------------------------------------------------------------------------

"""
The table we use in this test is generated as follows:

    CREATE TABLE postgres.public.spark_generated_iceberg_test (
        id bigint )
    USING iceberg
    TBLPROPERTIES ('write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read');

    INSERT INTO postgres.public.spark_generated_iceberg_test VALUES (1), (2);
    INSERT INTO postgres.public.spark_generated_iceberg_test VALUES (3), (4) ;
    INSERT INTO postgres.public.spark_generated_iceberg_test VALUES (5), (6);
    INSERT INTO postgres.public.spark_generated_iceberg_test SELECT * FROM postgres.public.spark_generated_iceberg_test;
    INSERT INTO postgres.public.spark_generated_iceberg_test SELECT explode(sequence(1, 5));
    INSERT INTO postgres.public.spark_generated_iceberg_test SELECT explode(sequence(1, 100));
    DELETE FROM postgres.public.spark_generated_iceberg_test WHERE id = 3;
    DELETE FROM postgres.public.spark_generated_iceberg_test WHERE id = 6;
    UPDATE postgres.public.spark_generated_iceberg_test SET id = id + 1 WHERE id > 25;

"""


@pytest.fixture(scope="module")
def spark_generated_iceberg_test(s3):

    for iceberg_prefix_end in [
        "spark_generated_iceberg_test",
        "spark_generated_iceberg_test_2",
        "spark_generated_iceberg_ddl_test",
    ]:
        iceberg_prefix = f"spark_test/public/" + iceberg_prefix_end
        iceberg_url = f"s3://{TEST_BUCKET}/{iceberg_prefix}"
        iceberg_path = (
            iceberg_sample_table_folder_path() + "/public/" + iceberg_prefix_end
        )

        # Upload data files
        for root, dirs, files in os.walk(iceberg_path + "/data"):
            for filename in files:
                s3.upload_file(
                    os.path.join(root, filename),
                    TEST_BUCKET,
                    f"{iceberg_prefix}/data/{filename}",
                )

        # Upload metadata files
        for root, dirs, files in os.walk(iceberg_path + "/metadata"):
            for filename in files:
                s3.upload_file(
                    os.path.join(root, filename),
                    TEST_BUCKET,
                    f"{iceberg_prefix}/metadata/{filename}",
                )


@pytest.fixture(scope="module")
def create_reserialize_helper_functions(superuser_conn, iceberg_extension):
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_table_metadata(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_table_metadata$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_manifest(
                manifestInputPath TEXT,
                manifestOutputPath TEXT
        ) RETURNS VOID
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_manifest$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_manifest_list(
                manifestListInputPath TEXT,
                manifestListOutputPath TEXT
        ) RETURNS VOID
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_manifest_list$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.manifest_list_path_from_table_metadata(
                tableMetadataPath TEXT
        ) RETURNS TEXT
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$manifest_list_path_from_table_metadata$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.manifest_paths_from_manifest_list(
                manifestListPath TEXT
        ) RETURNS TEXT[]
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$manifest_paths_from_manifest_list$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.datafile_paths_from_table_metadata(
                tableMetadataPath TEXT,
                isDelete bool DEFAULT false
        ) RETURNS TEXT[]
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$datafile_paths_from_table_metadata$function$;

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

        CREATE OR REPLACE FUNCTION lake_iceberg.current_partition_fields(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                datafile_path TEXT,
                partition_field_id INT,
                partition_field_name TEXT,
                partition_field_physical_type TEXT,
                partition_field_logical_type TEXT,
                partition_field_value TEXT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_partition_fields$function$;
""",
        superuser_conn,
    )

    yield

    run_command(
        """
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_iceberg_table_metadata(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_iceberg_manifest(TEXT, TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_iceberg_manifest_list(TEXT, TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.manifest_list_path_from_table_metadata(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.manifest_paths_from_manifest_list(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.datafile_paths_from_table_metadata(TEXT, bool);
        DROP FUNCTION IF EXISTS lake_iceberg.current_manifests(TEXT);
        DROP FUNCTION IF EXISTS lake_iceberg.current_partition_fields(TEXT);
                """,
        superuser_conn,
    )


@pytest.fixture(scope="module")
def create_http_helper_functions(superuser_conn, iceberg_extension):
    run_command(
        f"""
       CREATE TYPE lake_iceberg.http_result AS (
            status        int,
            body          text,
            resp_headers  text
        );

        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_get(
                url     text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_get'
        LANGUAGE C;


        -- HEAD
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_head(
                url     text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_head'
        LANGUAGE C;

        -- POST
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_post(
                url     text,
                body    text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_post'
        LANGUAGE C;

        -- PUT
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_put(
                url     text,
                body    text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_put'
        LANGUAGE C;

        -- DELETE
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_delete(
                url     text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_delete'
        LANGUAGE C;

        -- http with retry
         CREATE OR REPLACE FUNCTION lake_iceberg.test_http_with_retry(
                method text,
                url     text,
                body    text DEFAULT NULL,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_with_retry'
        LANGUAGE C;

        -- URL encode function
        CREATE OR REPLACE FUNCTION lake_iceberg.url_encode(input TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$url_encode_path$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.url_encode_path(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$url_encode_path$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.register_namespace_to_rest_catalog(TEXT,TEXT)
        RETURNS void
         LANGUAGE C
         VOLATILE STRICT
        AS 'pg_lake_iceberg', $function$register_namespace_to_rest_catalog$function$;

""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        """
        DROP FUNCTION IF EXISTS lake_iceberg.url_encode;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_get;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_head;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_post;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_put;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_delete;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_with_retry;
        DROP TYPE lake_iceberg.http_result;
        DROP FUNCTION IF EXISTS lake_iceberg.url_encode_path;
        DROP FUNCTION IF EXISTS lake_iceberg.register_namespace_to_rest_catalog;
                """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="session")
def iceberg_extension(postgres):
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_iceberg CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield
    superuser_conn.rollback()

    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_iceberg CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, s3):
    catalog = create_iceberg_test_catalog(superuser_conn)
    yield catalog
    tables = catalog.list_tables("public")
    for table in tables:
        catalog.drop_table(table)
    catalog.drop_namespace("public")
    catalog.engine.dispose()


@pytest.fixture(scope="function")
def grant_access_to_data_file_partition(
    extension,
    app_user,
    superuser_conn,
):
    run_command(
        f"""
        GRANT SELECT ON lake_table.data_file_partition_values TO {app_user};
    GRANT SELECT ON lake_iceberg.tables TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        REVOKE SELECT ON lake_table.data_file_partition_values FROM {app_user};
    REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="function")
def adjust_object_store_settings(superuser_conn):
    superuser_conn.autocommit = True

    # catalog=object_store requires the IcebergDefaultLocationPrefix set
    # and accessible by other sessions (e.g., push catalog worker),
    # and with_default_location only does a session level
    run_command(
        f"""ALTER SYSTEM SET pg_lake_iceberg.object_store_catalog_location_prefix = 's3://{TEST_BUCKET}';""",
        superuser_conn,
    )

    # to be able to read the same tables that we write, use the same prefix
    run_command(
        f"""
        ALTER SYSTEM SET pg_lake_iceberg.internal_object_store_catalog_prefix = 'tmp';
        """,
        superuser_conn,
    )

    run_command(
        f"""
		ALTER SYSTEM SET pg_lake_iceberg.external_object_store_catalog_prefix = 'tmp';
        """,
        superuser_conn,
    )

    superuser_conn.autocommit = False

    run_command("SELECT pg_reload_conf()", superuser_conn)

    # unfortunate, but Postgres requires a bit of time before
    # bg workers get the reload
    run_command("SELECT pg_sleep(0.1)", superuser_conn)
    superuser_conn.commit()
    yield

    superuser_conn.autocommit = True
    run_command(
        f"""
        ALTER SYSTEM RESET pg_lake_iceberg.object_store_catalog_location_prefix;
        """,
        superuser_conn,
    )
    run_command(
        f"""
        ALTER SYSTEM RESET pg_lake_iceberg.internal_object_store_catalog_prefix;
	   """,
        superuser_conn,
    )
    run_command(
        f"""
     	ALTER SYSTEM RESET pg_lake_iceberg.external_object_store_catalog_prefix;
        """,
        superuser_conn,
    )
    superuser_conn.autocommit = False

    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.commit()

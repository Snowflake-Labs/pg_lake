import pytest
import psycopg2
from utils_pytest import *
from helpers.polaris import *
from helpers.spark import *
import json
import re

from test_writable_iceberg_common import *


@pytest.mark.parametrize(
    "manifest_min_count_to_merge, target_manifest_size_kb, max_snapshot_age_params ",
    manifest_snapshot_settings,
)
def test_writable_rest_iceberg_table(
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
    create_iceberg_rest_table,
    create_test_helper_functions,
    create_http_helper_functions,
):
    if installcheck:
        return

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

    TABLE_NAME = create_iceberg_rest_table

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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
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
        get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn),
    )
    superuser_conn.commit()

    # now, make sure we do not leak any intermediate files
    assert_postgres_tmp_folder_empty()

    prev_path = get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn)

    run_command_outside_tx([f"VACUUM {TABLE_NAMESPACE}.{TABLE_NAME}"], pg_conn)
    current_path = get_rest_table_metadata_path(TABLE_NAMESPACE, TABLE_NAME, pg_conn)
    current_location = get_rest_table_metadata_location(
        TABLE_NAMESPACE, TABLE_NAME, pg_conn
    )

    assert_iceberg_s3_file_consistency(
        pg_conn,
        s3,
        TABLE_NAMESPACE,
        TABLE_NAME,
        current_location,
        current_path,
        prev_path,
    )

    run_command(
        f"""
            RESET pg_lake_iceberg.manifest_min_count_to_merge;
            RESET pg_lake_iceberg.target_manifest_size_kb;
            RESET pg_lake_iceberg.max_snapshot_age;
        """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module")
def set_polaris_gucs(
    superuser_conn,
    extension,
    installcheck,
    credentials_file: str = server_params.POLARIS_PRINCIPAL_CREDS_FILE,
):
    if not installcheck:

        creds = json.loads(Path(credentials_file).read_text())
        client_id = creds["credentials"]["clientId"]
        client_secret = creds["credentials"]["clientSecret"]

        run_command_outside_tx(
            [
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}'""",
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO '{client_id}'""",
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO '{client_secret}'""",
                "SELECT pg_reload_conf()",
            ],
            superuser_conn,
        )

    yield

    if not installcheck:

        run_command_outside_tx(
            [
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host""",
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id""",
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret""",
                "SELECT pg_reload_conf()",
            ],
            superuser_conn,
        )


@pytest.fixture(scope="module")
def create_http_helper_functions(superuser_conn, extension):
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
        DROP TYPE lake_iceberg.http_result;
        DROP FUNCTION IF EXISTS lake_iceberg.url_encode_path;
        DROP FUNCTION IF EXISTS lake_iceberg.register_namespace_to_rest_catalog;
        DROP FUNCTION IF EXISTS lake_iceberg.datafile_paths_from_table_metadata;
                """,
        superuser_conn,
    )
    superuser_conn.commit()


def get_rest_table_metadata_path(encoded_namespace, encoded_table_name, pg_conn):

    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{encoded_namespace}/tables/{encoded_table_name}"
    token = get_polaris_access_token()

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    assert res[0][0] == 200
    status, json_str, headers = res[0]
    metadata = json.loads(json_str)
    return metadata["metadata-location"]


def get_rest_table_metadata_location(encoded_namespace, encoded_table_name, pg_conn):

    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{encoded_namespace}/tables/{encoded_table_name}"
    token = get_polaris_access_token()

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    assert res[0][0] == 200
    status, json_str, headers = res[0]
    metadata = json.loads(json_str)
    return metadata["metadata"]["location"]


def test_server_location_prefix_overrides_guc(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    polaris_session,
    create_http_helper_functions,
):
    """
    When a REST catalog server has a location_prefix option, tables must use
    that prefix for their storage location. We verify this by setting the
    GUC to a broken S3 bucket.
    """
    if installcheck:
        return

    BROKEN_PREFIX = "s3://nonexistent-broken-bucket-xyz"
    VALID_PREFIX = f"s3://{TEST_BUCKET}/"
    SERVER_NAME = "rest_catalog_loc_prefix"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "loc_prefix_test"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"SET pg_lake_iceberg.default_location_prefix TO '{BROKEN_PREFIX}'",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     location_prefix '{VALID_PREFIX}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint, value text) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} "
        f"SELECT i, i::text FROM generate_series(1, 10) i",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 10

    table_location = get_rest_table_metadata_location(SCHEMA_NAME, TABLE_NAME, pg_conn)
    stripped_prefix = VALID_PREFIX.rstrip("/")
    assert table_location.startswith(stripped_prefix), (
        f"Expected location to start with server prefix '{stripped_prefix}', "
        f"got '{table_location}'"
    )
    assert BROKEN_PREFIX not in table_location
    assert (
        "//" not in table_location.split("://", 1)[1]
    ), f"Double slash found in location path: '{table_location}'"

    run_command_outside_tx([f"VACUUM {SCHEMA_NAME}.{TABLE_NAME}"])

    run_command(
        f"ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME} ADD COLUMN extra int",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} "
        f"SELECT i, i::text, i FROM generate_series(11, 20) i",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 20

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command("RESET pg_lake_iceberg.default_location_prefix", pg_conn)
    pg_conn.commit()


def test_reject_modify_different_rest_catalogs_in_single_transaction(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    Modifying tables from two different REST catalog servers in the same
    transaction must be rejected.
    """
    if installcheck:
        return

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    for name in ["rest_catalog_a", "rest_catalog_b"]:
        run_command(
            f"""
            CREATE SERVER {name} TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint '{endpoint}',
                         client_id '{client_id}',
                         client_secret '{client_secret}')
            """,
            superuser_conn,
        )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {TABLE_NAMESPACE}", pg_conn)
    pg_conn.commit()

    for name, catalog in [("table_a", "rest_catalog_a"), ("table_b", "rest_catalog_b")]:
        run_command(
            f"CREATE TABLE {TABLE_NAMESPACE}.{name} (id bigint) USING iceberg WITH (catalog='{catalog}')",
            pg_conn,
        )
        pg_conn.commit()

    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.table_a SELECT i FROM generate_series(1, 10) i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.table_b SELECT i FROM generate_series(1, 10) i",
        pg_conn,
    )

    with pytest.raises(
        psycopg2.errors.FeatureNotSupported, match="different REST catalogs"
    ):
        pg_conn.commit()

    pg_conn.rollback()


def test_reject_writable_table_on_server_with_catalog_name(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    Creating a writable table on a server that has catalog_name set must
    be rejected, because writable tables always derive the catalog name
    from the database name.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_catalog_has_catname"
    SCHEMA_NAME = TABLE_NAMESPACE

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     catalog_name '{server_params.PG_DATABASE}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    err = run_command(
        f"CREATE TABLE {SCHEMA_NAME}.reject_catname (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert (
        "writable REST catalog tables cannot use a server with catalog_name set"
        in str(err)
    )
    pg_conn.rollback()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()


def test_server_catalog_name_overrides_default(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    polaris_session,
    create_http_helper_functions,
):
    """
    The server's catalog_name must override the default (database name).
    We prove this by creating a server with a wrong catalog_name and
    creating a read-only table that does not set catalog_name itself.
    The REST metadata lookup should fail because it uses the server's
    value, not the default database name.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_catalog_wrong_name"
    SCHEMA_NAME = TABLE_NAMESPACE

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     catalog_name 'nonexistent_catalog')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        f"CREATE TABLE {SCHEMA_NAME}.srv_catname_fail () "
        f"USING iceberg WITH (catalog='{SERVER_NAME}', read_only='true')",
        pg_conn,
        raise_error=False,
    )
    assert err is not None, (
        "Expected failure because server's catalog_name 'nonexistent_catalog' "
        "should be used instead of the default database name"
    )
    pg_conn.rollback()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()


def test_table_catalog_name_overrides_server(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    """
    The table-level catalog_name takes precedence over the server's.
    We create a writable table via the built-in 'rest' catalog, then
    create a read-only table via a server whose catalog_name is wrong,
    overriding it on the table with the correct one.  If the table
    option did not take precedence, the metadata lookup would fail.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_catname_bad"
    SCHEMA_NAME = "test_catname_override"
    SRC_TABLE = "catname_src"
    RO_TABLE = "catname_ro"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{SRC_TABLE} (id bigint) "
        f"USING iceberg WITH (catalog='rest')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{SRC_TABLE} "
        f"SELECT i FROM generate_series(1, 5) i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     catalog_name 'nonexistent_catalog')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{RO_TABLE} () "
        f"USING iceberg WITH (catalog='{SERVER_NAME}', read_only='true', "
        f"catalog_name='{server_params.PG_DATABASE}', "
        f"catalog_namespace='{SCHEMA_NAME}', "
        f"catalog_table_name='{SRC_TABLE}')",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {SCHEMA_NAME}.{RO_TABLE}", pg_conn)
    assert results[0][0] == 5

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()

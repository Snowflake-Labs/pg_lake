import pytest
import psycopg2
from utils_pytest import *
from helpers.polaris import *
from helpers.spark import *
import json
import re
import threading
import time

from test_writable_iceberg_common import *


@pytest.mark.parametrize(
    "manifest_min_count_to_merge, target_manifest_size_kb, max_snapshot_age_params",
    manifest_snapshot_settings,
)
@pytest.mark.parametrize(
    "create_iceberg_rest_table_parametrized",
    ["rest", "user_server"],
    indirect=True,
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
    create_iceberg_rest_table_parametrized,
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

    TABLE_NAME = create_iceberg_rest_table_parametrized

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


server_option_override_params = [
    pytest.param(
        "rest_endpoint",
        "pg_lake_iceberg.rest_catalog_host",
        "http://localhost:1",
        id="rest_endpoint",
    ),
    pytest.param(
        "location_prefix",
        "pg_lake_iceberg.default_location_prefix",
        "s3://nonexistent-broken-bucket-xyz",
        id="location_prefix",
    ),
    pytest.param(
        "catalog_name",
        None,
        None,
        id="catalog_name",
    ),
    # client_id / client_secret are user-mapping options, not server
    # options; see test_user_mapping_credential_overrides_guc below for
    # the equivalent override coverage.
]


@pytest.mark.parametrize(
    "option_name, guc_name, broken_guc_value", server_option_override_params
)
def test_server_option_overrides_guc(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    polaris_session,
    create_http_helper_functions,
    option_name,
    guc_name,
    broken_guc_value,
):
    """
    Verify that each overridable server option takes precedence over
    its corresponding GUC.  For most options the GUC is set to a broken
    value while the server option is set to the correct value, then we
    prove the table works.  For catalog_name the server option is set to
    a wrong value and we prove it is used (instead of the default).
    """
    if installcheck:
        return

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"
    VALID_PREFIX = f"s3://{TEST_BUCKET}/"

    SERVER_NAME = f"rest_opt_override_{option_name}"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = f"opt_override_{option_name}"

    server_options = {
        "rest_endpoint": endpoint,
        "location_prefix": VALID_PREFIX,
    }

    if option_name == "catalog_name":
        server_options["catalog_name"] = "nonexistent_catalog"

    options_sql = ", ".join(f"{k} '{v}'" for k, v in server_options.items())

    if guc_name is not None:
        run_command(
            f"SET {guc_name} TO '{broken_guc_value}'",
            superuser_conn,
        )
        superuser_conn.commit()

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS ({options_sql})
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    if option_name == "catalog_name":
        err = run_command(
            f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} () "
            f"USING iceberg WITH (catalog='{SERVER_NAME}', read_only='true')",
            pg_conn,
            raise_error=False,
        )
        assert err is not None, (
            "Expected failure because server's catalog_name 'nonexistent_catalog' "
            "should be used instead of the default database name"
        )
        pg_conn.rollback()
    else:
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

        if option_name == "location_prefix":
            table_location = get_rest_table_metadata_location(
                SCHEMA_NAME, TABLE_NAME, pg_conn
            )
            stripped_prefix = VALID_PREFIX.rstrip("/")
            assert table_location.startswith(stripped_prefix), (
                f"Expected location to start with server prefix "
                f"'{stripped_prefix}', got '{table_location}'"
            )
            assert broken_guc_value not in table_location
            assert (
                "//" not in table_location.split("://", 1)[1]
            ), f"Double slash found in location path: '{table_location}'"

        pg_conn.rollback()
        run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
        pg_conn.commit()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    if guc_name is not None:
        run_command(f"RESET {guc_name}", superuser_conn)
        superuser_conn.commit()


@pytest.mark.parametrize(
    "option_name, guc_name",
    [
        ("client_id", "pg_lake_iceberg.rest_catalog_client_id"),
        ("client_secret", "pg_lake_iceberg.rest_catalog_client_secret"),
    ],
)
def test_user_mapping_credential_overrides_guc(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    polaris_session,
    create_http_helper_functions,
    option_name,
    guc_name,
):
    """
    Credentials on a USER MAPPING win over the corresponding GUC: we
    poison the GUC, place the correct value on the mapping, and prove
    DML still works.  This is the resolution-order test for the user
    mapping (highest priority) vs GUC (lowest priority) layers.
    """
    if installcheck:
        return

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"
    VALID_PREFIX = f"s3://{TEST_BUCKET}/"

    SERVER_NAME = f"rest_um_override_{option_name}"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = f"um_override_{option_name}"

    um_options = {"client_id": client_id, "client_secret": client_secret}

    run_command(f"SET {guc_name} TO 'wrong-{option_name}'", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix '{VALID_PREFIX}')
        """,
        superuser_conn,
    )
    um_sql = ", ".join(f"{k} '{v}'" for k, v in um_options.items())
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS ({um_sql})
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
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
        f"SELECT i, i::text FROM generate_series(1, 5) i",
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}", pg_conn)
    assert results[0][0] == 5

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    run_command(f"RESET {guc_name}", superuser_conn)
    superuser_conn.commit()


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
    transaction must be rejected at statement time -- before any Parquet is
    written to S3 for the offending statement.  The first mutation binds the
    transaction to its REST catalog; the second mutation to a different
    catalog must raise immediately rather than waiting for
    XACT_EVENT_PRE_COMMIT.

    Covers both DML (INSERT) and DDL-like (TRUNCATE) paths through
    BindRelationToXactRestCatalog.  The "the current statement targets"
    detail wording is emitted only by BindRelationToXactRestCatalog(); the
    XACT_EVENT_PRE_COMMIT fallback uses "table %u belongs to" instead, so
    this match pins down which code path fired.
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
                OPTIONS (rest_endpoint '{endpoint}')
            """,
            superuser_conn,
        )
        run_command(
            f"""
            CREATE USER MAPPING FOR PUBLIC SERVER {name}
                OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
            """,
            superuser_conn,
        )
        run_command(
            f"GRANT USAGE ON FOREIGN SERVER {name} TO PUBLIC",
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

    # ── INSERT path ──────────────────────────────────────────────────────

    run_command(
        f"INSERT INTO {TABLE_NAMESPACE}.table_a SELECT i FROM generate_series(1, 10) i",
        pg_conn,
    )

    with pytest.raises(
        psycopg2.errors.FeatureNotSupported,
        match=r"the current statement targets",
    ):
        run_command(
            f"INSERT INTO {TABLE_NAMESPACE}.table_b SELECT i FROM generate_series(1, 10) i",
            pg_conn,
        )

    pg_conn.rollback()

    # ── TRUNCATE path ────────────────────────────────────────────────────

    run_command(
        f"TRUNCATE {TABLE_NAMESPACE}.table_a",
        pg_conn,
    )

    with pytest.raises(
        psycopg2.errors.FeatureNotSupported,
        match=r"the current statement targets",
    ):
        run_command(
            f"TRUNCATE {TABLE_NAMESPACE}.table_b",
            pg_conn,
        )

    pg_conn.rollback()

    # ── cleanup ──────────────────────────────────────────────────────────

    for name, catalog in [("table_a", "rest_catalog_a"), ("table_b", "rest_catalog_b")]:
        run_command(
            f"DROP TABLE IF EXISTS {TABLE_NAMESPACE}.{name}",
            pg_conn,
        )
        pg_conn.commit()

    run_command(f"DROP SCHEMA IF EXISTS {TABLE_NAMESPACE}", pg_conn)
    pg_conn.commit()

    superuser_conn.rollback()
    for name in ["rest_catalog_a", "rest_catalog_b"]:
        run_command(f"DROP SERVER IF EXISTS {name} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_multi_table_single_transaction_on_same_server(
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
    Two tables on the *same* user-created server: INSERT into one and
    UPDATE the other in a single transaction must succeed.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_multi_tbl"
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
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.multi_a (id bigint, value text) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        pg_conn,
    )
    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.multi_b (id bigint, value text) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.multi_b SELECT i, 'old' FROM generate_series(1, 5) i",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.multi_a SELECT i, i::text FROM generate_series(1, 10) i",
        pg_conn,
    )
    run_command(
        f"UPDATE {SCHEMA_NAME}.multi_b SET value = 'new' WHERE id <= 3",
        pg_conn,
    )
    pg_conn.commit()

    results_a = run_query(f"SELECT count(*) FROM {SCHEMA_NAME}.multi_a", pg_conn)
    assert results_a[0][0] == 10

    results_b = run_query(
        f"SELECT count(*) FROM {SCHEMA_NAME}.multi_b WHERE value = 'new'", pg_conn
    )
    assert results_b[0][0] == 3

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_token_cache_reuses_token_across_catalog_ops(
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
    The per-catalog token cache must reuse a single OAuth token across
    multiple back-to-back catalog operations in the same session.
    A cache miss on every call would double request latency.

    Uses pg_lake_iceberg.http_client_trace_traffic to observe actual
    HTTP traffic: each token fetch shows up as a POST to .../oauth/tokens
    in the connection notices. Does 3 back-to-back inserts.
    """
    if installcheck:
        return

    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "token_cache_test"

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        "SET pg_lake_iceberg.http_client_trace_traffic TO on",
        pg_conn,
    )
    pg_conn.notices.clear()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint, value text) "
        f"USING iceberg WITH (catalog='rest')",
        pg_conn,
    )
    pg_conn.commit()

    for i in range(3):
        run_command(
            f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES ({i}, 'v')",
            pg_conn,
        )
        pg_conn.commit()

    token_fetches = sum(
        1 for n in pg_conn.notices if "oauth/tokens" in n and "POST" in n
    )
    assert token_fetches == 1, (
        f"Expected exactly 1 OAuth token fetch (cached), got {token_fetches}. "
        f"Notices:\n" + "\n".join(pg_conn.notices)
    )

    run_command(
        "RESET pg_lake_iceberg.http_client_trace_traffic",
        pg_conn,
    )

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()


def test_alter_user_mapping_credentials_invalidates_token_cache(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    After ALTER USER MAPPING, the cached OAuth token must be discarded
    so the next catalog operation re-fetches.  Credentials live on the
    user mapping, so the relevant invalidation arm is the
    USERMAPPINGOID syscache callback (registered next to
    FOREIGNSERVEROID in InitTokenCacheIfNeeded).

    We verify the behaviour by enabling HTTP traffic tracing and
    counting POSTs to .../oauth/tokens around each rotation: cache is
    invalidated on bogus credentials (1 fetch, commit fails), then
    invalidated again on restored credentials (1 fetch, commit
    succeeds).
    """
    if installcheck:
        return

    SERVER_NAME = "rest_token_inval"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "token_inval_test"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (1)",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        "SET pg_lake_iceberg.http_client_trace_traffic TO on",
        superuser_conn,
    )
    superuser_conn.notices.clear()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (2)",
        superuser_conn,
    )
    superuser_conn.commit()

    pre_alter_fetches = sum(
        1 for n in superuser_conn.notices if "oauth/tokens" in n and "POST" in n
    )
    assert pre_alter_fetches == 0, (
        f"Expected no token fetch before ALTER USER MAPPING (token cached), "
        f"got {pre_alter_fetches}. Notices:\n" + "\n".join(superuser_conn.notices)
    )

    run_command(
        f"ALTER USER MAPPING FOR PUBLIC SERVER {SERVER_NAME} "
        f"OPTIONS (SET client_id 'rotated-id')",
        superuser_conn,
    )
    superuser_conn.commit()

    superuser_conn.notices.clear()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (3)",
        superuser_conn,
    )

    commit_failed = False
    try:
        superuser_conn.commit()
    except psycopg2.DatabaseError:
        commit_failed = True
        superuser_conn.rollback()

    post_alter_notices = list(superuser_conn.notices)
    post_alter_fetches = sum(
        1 for n in post_alter_notices if "oauth/tokens" in n and "POST" in n
    )

    assert commit_failed, (
        "Expected COMMIT to fail after ALTER USER MAPPING set bogus client_id "
        "(cache should have been invalidated, forcing re-auth with bad creds)"
    )
    assert post_alter_fetches == 1, (
        f"Expected exactly 1 token re-fetch after ALTER USER MAPPING "
        f"(cache invalidated), got {post_alter_fetches}. Notices "
        f"({len(post_alter_notices)}):\n" + "\n".join(post_alter_notices)
    )

    run_command(
        f"ALTER USER MAPPING FOR PUBLIC SERVER {SERVER_NAME} "
        f"OPTIONS (SET client_id '{client_id}')",
        superuser_conn,
    )
    superuser_conn.commit()

    superuser_conn.notices.clear()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (4)",
        superuser_conn,
    )
    superuser_conn.commit()

    restore_fetches = sum(
        1 for n in superuser_conn.notices if "oauth/tokens" in n and "POST" in n
    )
    assert restore_fetches == 1, (
        f"Expected exactly 1 token re-fetch after restoring credentials, "
        f"got {restore_fetches}. Notices:\n" + "\n".join(superuser_conn.notices)
    )

    results = run_query(
        f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}", superuser_conn
    )
    assert results[0][0] == 3

    run_command(
        "RESET pg_lake_iceberg.http_client_trace_traffic",
        superuser_conn,
    )

    superuser_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_drop_server_with_dependent_iceberg_table(
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
    Tables created with catalog='<server>' record a pg_depend entry on
    the server.  DROP SERVER without CASCADE must be blocked, and
    DROP SERVER CASCADE must drop the dependent table.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_dep_test"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "dep_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        pg_conn,
    )
    pg_conn.commit()

    err = run_command(
        f"DROP SERVER {SERVER_NAME}",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot drop" in str(err).lower()
    superuser_conn.rollback()

    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    result = run_query(
        f"SELECT count(*) FROM pg_class WHERE relname = '{TABLE_NAME}'",
        pg_conn,
    )
    assert result[0][0] == 0


def test_drop_owned_by_with_dependent_iceberg_table(
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
    When a role owns an iceberg_catalog server that has dependent iceberg
    tables, DROP OWNED BY must propagate the dependency:
      - DROP OWNED BY <role> RESTRICT  -> blocked
      - DROP OWNED BY <role> CASCADE   -> drops the dependent table too
    """
    if installcheck:
        return

    ROLE_NAME = "rest_owned_test_role"
    SERVER_NAME = "rest_owned_test_srv"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "owned_dep_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(f"DROP ROLE IF EXISTS {ROLE_NAME}", superuser_conn, raise_error=False)
    superuser_conn.commit()

    run_command(
        f"CREATE ROLE {ROLE_NAME} WITH CREATEDB LOGIN",
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN DATA WRAPPER iceberg_catalog TO {ROLE_NAME}",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"SET ROLE {ROLE_NAME}", superuser_conn)
    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
        superuser_conn,
    )
    run_command("RESET ROLE", superuser_conn)
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        pg_conn,
    )
    pg_conn.commit()

    err = run_command(
        f"DROP OWNED BY {ROLE_NAME} RESTRICT",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot drop" in str(err).lower()
    superuser_conn.rollback()

    server_exists = run_query(
        f"SELECT count(*) FROM pg_foreign_server WHERE srvname = '{SERVER_NAME}'",
        superuser_conn,
    )
    assert server_exists[0][0] == 1

    table_exists = run_query(
        f"SELECT count(*) FROM pg_class WHERE relname = '{TABLE_NAME}'",
        pg_conn,
    )
    assert table_exists[0][0] == 1

    run_command(f"DROP OWNED BY {ROLE_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    server_exists = run_query(
        f"SELECT count(*) FROM pg_foreign_server WHERE srvname = '{SERVER_NAME}'",
        superuser_conn,
    )
    assert server_exists[0][0] == 0

    table_exists = run_query(
        f"SELECT count(*) FROM pg_class WHERE relname = '{TABLE_NAME}'",
        pg_conn,
    )
    assert table_exists[0][0] == 0

    run_command(f"DROP ROLE {ROLE_NAME}", superuser_conn)
    superuser_conn.commit()


def test_drop_server_blocks_on_active_query(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    DROP SERVER ... CASCADE on a server with a dependent iceberg table must
    block while another transaction holds AccessShareLock on that table.
    This verifies that the pg_depend edge registered by
    RecordIcebergCatalogServerDependency is honored by core's dependency
    walker, which acquires AccessExclusiveLock on each dependent table.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_lock_test_srv"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "lock_dep_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    holder_conn = open_pg_conn("postgres")
    dropper_conn = open_pg_conn("postgres")
    try:
        run_command("BEGIN", holder_conn)
        run_command(
            f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}",
            holder_conn,
        )

        drop_done = threading.Event()
        drop_error = []

        def run_drop():
            try:
                run_command(f"DROP SERVER {SERVER_NAME} CASCADE", dropper_conn)
                dropper_conn.commit()
            except Exception as e:
                drop_error.append(e)
            finally:
                drop_done.set()

        drop_thread = threading.Thread(target=run_drop)
        drop_thread.start()

        deadline = time.time() + 10
        blocked = False
        while time.time() < deadline:
            rows = run_query(
                """
                SELECT count(*) FROM pg_stat_activity
                WHERE wait_event_type = 'Lock'
                  AND query LIKE 'DROP SERVER%'
                """,
                superuser_conn,
            )
            superuser_conn.rollback()
            if int(rows[0][0]) == 1:
                blocked = True
                break
            time.sleep(0.1)

        assert blocked, "Expected DROP SERVER to be blocked on AccessShareLock"
        assert not drop_done.is_set(), "DROP SERVER should not have completed yet"

        run_command("COMMIT", holder_conn)

        drop_thread.join(timeout=15)
        assert drop_done.is_set(), "DROP SERVER did not complete after lock released"
        assert not drop_error, f"DROP SERVER failed: {drop_error[0]}"

        result = run_query(
            f"SELECT count(*) FROM pg_class WHERE relname = '{TABLE_NAME}'",
            superuser_conn,
        )
        assert result[0][0] == 0
    finally:
        try:
            holder_conn.rollback()
        except Exception:
            pass
        holder_conn.close()
        dropper_conn.close()


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
                     catalog_name '{server_params.PG_DATABASE}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
        superuser_conn,
    )
    superuser_conn.commit()

    # Own the schema with pg_conn so the CREATE TABLE below reaches the
    # catalog_name rejection rather than tripping a permission check. Drop any
    # leftover first (as superuser, so it works regardless of who leaked it):
    # this test shares TABLE_NAMESPACE with the other writable-REST tests, and
    # they may land in the same pytest-split group in any order.
    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()
    run_command(f"CREATE SCHEMA {SCHEMA_NAME}", pg_conn)
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

    # Clean up so we do not leak the schema to a later test in the same group.
    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()

    superuser_conn.rollback()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_alter_server_add_catalog_name_does_not_reroute_writable_table(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    Writable tables always use the database name for REST catalog routing,
    ignoring the server's catalog_name.  Adding catalog_name to the server
    after a writable table exists must NOT change where requests go.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_catname_reroute"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "catname_reroute_test"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (1)",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (ADD catalog_name 'nonexistent_db')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (2)",
        superuser_conn,
    )
    superuser_conn.commit()

    results = run_query(
        f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}", superuser_conn
    )
    assert results[0][0] == 2

    superuser_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_alter_server_rest_endpoint_blocked_with_dependent_tables(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    Changing rest_endpoint on a server that has dependent iceberg tables
    (writable or read-only) must be blocked, because both types make REST
    API calls against the server's endpoint at runtime.
    """
    if installcheck:
        return

    SERVER_NAME = "rest_endpoint_block"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "endpoint_block_test"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (SET rest_endpoint 'http://other:8181')",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "dependent iceberg tables" in str(err)
    superuser_conn.rollback()

    # Confirm a non-endpoint server-level ALTER still works while
    # dependent tables exist (scope is a SERVER-context option, so the
    # rest_endpoint guard above does not fire on it).
    run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (ADD scope 'PRINCIPAL_ROLE:ALL')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    # Drop the PUBLIC mapping so this assertion isolates the
    # dependent-tables guard from the user-mapping guard.
    run_command(
        f"DROP USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}",
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (SET rest_endpoint 'http://other:8181')",
        superuser_conn,
        raise_error=False,
    )
    assert err is None, (
        "ALTER SERVER rest_endpoint should succeed after dependent tables "
        "and user mappings are dropped"
    )
    superuser_conn.rollback()

    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


# ── DROP USER MAPPING / ALTER USER MAPPING credential-removal guard ──────


def test_drop_user_mapping_in_isolation_succeeds(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """DROP USER MAPPING by itself succeeds even when the server has
    dependent iceberg tables: the OAT_DROP hook captures the
    about-to-vanish credentials into the txn, but with no concurrent
    DROP TABLE the capture goes unused and the mapping is simply
    removed.  The cross-transaction failure mode is covered by
    test_drop_user_mapping_then_drop_table_in_separate_txn_fails_clearly."""
    if installcheck:
        return

    SERVER_NAME = "wedge_um_iso"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "wedge_um_iso_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"DROP USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()

    rows = run_query(
        f"SELECT 1 FROM pg_user_mappings WHERE srvname = '{SERVER_NAME}'",
        superuser_conn,
    )
    assert rows == [], rows

    # Cleanup: recreate the mapping so DROP SCHEMA / DROP SERVER can
    # tear the dependent table down through the normal credentialed
    # path.
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_drop_user_mapping_then_drop_table_in_same_txn_succeeds(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """A same-transaction DROP USER MAPPING followed by DROP TABLE
    completes cleanly: the OAT_DROP user-mapping capture binds the
    about-to-vanish credentials to the transaction so the table-drop
    fired afterwards can authenticate its post-commit REST DELETE
    against the captured snapshot."""
    if installcheck:
        return

    SERVER_NAME = "wedge_sametxn"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "wedge_sametxn_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    # Single transaction: drop the mapping first, then the table.
    run_command(f"DROP USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}", superuser_conn)
    run_command(f"DROP TABLE {SCHEMA_NAME}.{TABLE_NAME}", superuser_conn)
    superuser_conn.commit()

    rows = run_query(
        f"SELECT 1 FROM pg_user_mappings WHERE srvname = '{SERVER_NAME}'",
        superuser_conn,
    )
    assert rows == [], rows
    rows = run_query(
        f"SELECT 1 FROM pg_class WHERE relname = '{TABLE_NAME}'",
        superuser_conn,
    )
    assert rows == [], rows

    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_drop_user_mapping_then_drop_table_in_separate_txn_fails_clearly(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """If DROP USER MAPPING commits in one transaction and a later
    transaction tries to DROP a dependent iceberg table, the table
    drop fails cleanly with the standard "no credentials found"
    error.  The OAT_DROP capture is txn-local, so it doesn't survive
    the commit.  Recovery: recreate the mapping (or do both drops in
    one transaction, see
    test_drop_user_mapping_then_drop_table_in_same_txn_succeeds)."""
    if installcheck:
        return

    SERVER_NAME = "wedge_xtxn"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "wedge_xtxn_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"DROP USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()

    err = run_command(
        f"DROP TABLE {SCHEMA_NAME}.{TABLE_NAME}",
        superuser_conn,
        raise_error=False,
    )
    superuser_conn.rollback()
    assert err is not None
    msg = str(err)
    assert "no credentials found" in msg, msg
    assert SERVER_NAME in msg, msg
    assert client_secret not in msg, msg

    # Recovery path: recreate the mapping, then the cleanup succeeds.
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_server_owner_dropping_another_users_mapping_does_not_capture_credentials(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """A server owner is allowed by Postgres to DROP another role's
    USER MAPPING on the server.  The OAT_DROP capture hook must
    skip such mappings: snapshotting a foreign role's credentials
    would let the dropping session silently authenticate later
    same-transaction REST requests as that role (confused-deputy).

    Setup: PUBLIC mapping has valid creds, the victim's mapping has
    deliberately-invalid creds.  As the server owner, drop the
    victim's mapping and INSERT into a writable iceberg table in
    the same transaction.  Capture is skipped so the live resolver
    picks up the dropping session's own (PUBLIC) credentials and
    the post-commit ADD_SNAPSHOT POST succeeds."""
    if installcheck:
        return

    ATTACKER = "test_um_capture_attacker"
    VICTIM = "test_um_capture_victim"
    SERVER_NAME = "um_capture_srv"
    SCHEMA_NAME = "um_capture_ns"
    TABLE_NAME = "um_capture_attacker_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(f"DROP ROLE IF EXISTS {ATTACKER}", superuser_conn, raise_error=False)
    run_command(f"DROP ROLE IF EXISTS {VICTIM}", superuser_conn, raise_error=False)
    superuser_conn.commit()

    run_command(f"CREATE ROLE {ATTACKER} LOGIN", superuser_conn)
    run_command(f"CREATE ROLE {VICTIM} LOGIN", superuser_conn)
    run_command(
        f"GRANT USAGE ON FOREIGN DATA WRAPPER iceberg_catalog "
        f"TO {ATTACKER}, {VICTIM}",
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER pg_lake_iceberg TO {ATTACKER}, {VICTIM}",
        superuser_conn,
    )
    run_command(f"GRANT lake_write TO {ATTACKER}, {VICTIM}", superuser_conn)
    superuser_conn.commit()

    try:
        run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
        run_command(
            f"GRANT ALL ON SCHEMA {SCHEMA_NAME} TO {ATTACKER}, {VICTIM}",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(f"SET ROLE {ATTACKER}", superuser_conn)
        run_command(
            f"""
            CREATE SERVER {SERVER_NAME} TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint '{endpoint}',
                         location_prefix 's3://{TEST_BUCKET}')
            """,
            superuser_conn,
        )
        run_command(
            f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO {VICTIM}",
            superuser_conn,
        )
        run_command(
            f"""
            CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
                OPTIONS (client_id '{client_id}',
                         client_secret '{client_secret}')
            """,
            superuser_conn,
        )
        run_command(
            f"""
            CREATE USER MAPPING FOR {VICTIM} SERVER {SERVER_NAME}
                OPTIONS (client_id 'um-capture-bogus-id',
                         client_secret 'um-capture-bogus-secret')
            """,
            superuser_conn,
        )
        run_command(
            f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
            f"USING iceberg WITH (catalog='{SERVER_NAME}')",
            superuser_conn,
        )
        run_command("RESET ROLE", superuser_conn)
        superuser_conn.commit()

        # As the server owner: drop the victim's mapping and INSERT
        # in the same transaction.  Capture must skip the foreign
        # mapping; the post-commit ADD_SNAPSHOT POST then runs under
        # PUBLIC, not under the victim's bogus credentials.
        run_command(f"SET ROLE {ATTACKER}", superuser_conn)
        err = run_command(
            f"""
            BEGIN;
            DROP USER MAPPING FOR {VICTIM} SERVER {SERVER_NAME};
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (1);
            COMMIT;
            """,
            superuser_conn,
            raise_error=False,
        )
        assert err is None, (
            f"attack txn errored; foreign mapping capture may have "
            f"bound the victim's bogus credentials. err={err!r}"
        )
        run_command("RESET ROLE", superuser_conn)
        superuser_conn.commit()

        rows = run_query(
            f"SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}",
            superuser_conn,
        )
        assert rows[0][0] == 1, rows

        # And the victim mapping is actually gone.
        rows = run_query(
            f"SELECT 1 FROM pg_user_mappings WHERE srvname = '{SERVER_NAME}' "
            f"AND usename = '{VICTIM}'",
            superuser_conn,
        )
        assert rows == [], rows
    finally:
        run_command("RESET ROLE", superuser_conn, raise_error=False)
        superuser_conn.rollback()
        run_command(
            f"DROP SERVER IF EXISTS {SERVER_NAME} CASCADE",
            superuser_conn,
            raise_error=False,
        )
        run_command(
            f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE",
            superuser_conn,
            raise_error=False,
        )
        run_command(
            f"DROP ROLE IF EXISTS {ATTACKER}", superuser_conn, raise_error=False
        )
        run_command(f"DROP ROLE IF EXISTS {VICTIM}", superuser_conn, raise_error=False)
        superuser_conn.commit()


@pytest.mark.parametrize("creation_order", ["um_before_table", "table_before_um"])
def test_drop_server_cascade_does_not_wedge(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
    creation_order,
):
    """DROP SERVER ... CASCADE must succeed regardless of which
    dependent Postgres visits first.  Cascade siblings are visited
    in descending-OID order:

      um_before_table  -- table has the higher OID, dropped first;
                          capture happens but isn't strictly needed.
      table_before_um  -- user mapping has the higher OID, dropped
                          first; its OAT_DROP capture binds the
                          credentials so the table-drop later in
                          the same cascade can authenticate its
                          post-commit REST DELETE.
    """
    if installcheck:
        return

    SERVER_NAME = f"wedge_cascade_{creation_order}"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = f"wedge_cascade_{creation_order}_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    if creation_order == "table_before_um":
        # Reorder: drop and recreate the user mapping after the table
        # exists so the new UM has a higher OID than the table.
        # Cascade will then visit the UM first.
        run_command(
            f"DROP USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}",
            superuser_conn,
        )
        run_command(
            f"""
            CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
                OPTIONS (client_id '{client_id}',
                         client_secret '{client_secret}')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    rows = run_query(
        f"SELECT 1 FROM pg_foreign_server WHERE srvname = '{SERVER_NAME}'",
        superuser_conn,
    )
    assert rows == [], rows
    rows = run_query(
        f"SELECT 1 FROM pg_class WHERE relname = '{TABLE_NAME}'",
        superuser_conn,
    )
    assert rows == [], rows


@pytest.mark.parametrize("dropped_option", ["client_id", "client_secret"])
def test_alter_user_mapping_drop_credential_blocked_with_dependent_tables(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
    dropped_option,
):
    """OPTIONS (DROP client_id|client_secret) must be rejected while
    the server has dependent iceberg tables."""
    if installcheck:
        return

    SERVER_NAME = "wedge_alter_um"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "wedge_alter_um_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        f"ALTER USER MAPPING FOR PUBLIC SERVER {SERVER_NAME} "
        f"OPTIONS (DROP {dropped_option})",
        superuser_conn,
        raise_error=False,
    )
    superuser_conn.rollback()
    assert err is not None
    msg = str(err)
    assert dropped_option in msg, msg
    assert "dependent iceberg tables" in msg, msg
    assert SERVER_NAME in msg, msg
    assert client_secret not in msg, msg

    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_alter_user_mapping_set_credential_allowed_with_dependent_tables(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """SET on client_id / client_secret is rotation, not removal, and
    must remain allowed while dependent tables exist."""
    if installcheck:
        return

    SERVER_NAME = "wedge_set_allowed"
    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "wedge_set_allowed_tbl"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", superuser_conn)
    superuser_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint) "
        f"USING iceberg WITH (catalog='{SERVER_NAME}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"ALTER USER MAPPING FOR PUBLIC SERVER {SERVER_NAME} "
        f"OPTIONS (SET client_id '{client_id}', "
        f"         SET client_secret '{client_secret}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def test_drop_user_mapping_allowed_without_dependent_tables(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """Without dependent tables, DROP USER MAPPING and DROP SERVER are
    allowed; the guard must not over-block."""
    if installcheck:
        return

    SERVER_NAME = "wedge_no_deps"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     location_prefix 's3://{TEST_BUCKET}')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"DROP USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()

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
                     catalog_name 'nonexistent_catalog')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}')
        """,
        superuser_conn,
    )
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
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
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

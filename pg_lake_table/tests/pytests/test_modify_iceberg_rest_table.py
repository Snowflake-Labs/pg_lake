import pytest
import psycopg2
from utils_pytest import *
from helpers.polaris import *
from helpers.spark import *
import json
import re

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
        "client_id",
        "pg_lake_iceberg.rest_catalog_client_id",
        "wrong_id",
        id="client_id",
    ),
    pytest.param(
        "client_secret",
        "pg_lake_iceberg.rest_catalog_client_secret",
        "wrong_secret",
        id="client_secret",
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
        "client_id": client_id,
        "client_secret": client_secret,
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
        run_command(f"DROP SERVER IF EXISTS {name}", superuser_conn)
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
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     location_prefix 's3://{TEST_BUCKET}')
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
    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
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
    in the connection notices.
    """
    if installcheck:
        return

    SCHEMA_NAME = TABLE_NAMESPACE
    TABLE_NAME = "token_cache_test"

    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (id bigint, value text) "
        f"USING iceberg WITH (catalog='rest')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "SET pg_lake_iceberg.http_client_trace_traffic TO on",
        pg_conn,
    )

    pg_conn.notices.clear()

    for i in range(3):
        run_command(
            f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES ({i}, 'v')",
            pg_conn,
        )
        pg_conn.commit()

    token_fetches = sum(
        1 for n in pg_conn.notices if "oauth/tokens" in n and "POST" in n
    )
    assert token_fetches <= 1, (
        f"Expected at most 1 OAuth token fetch (cached), got {token_fetches}. "
        f"Notices:\n" + "\n".join(pg_conn.notices)
    )

    run_command(
        "RESET pg_lake_iceberg.http_client_trace_traffic",
        pg_conn,
    )

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()


def test_alter_server_credentials_invalidates_token_cache(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    After ALTER SERVER, the cached OAuth token must be discarded so the
    next catalog operation re-fetches it.  We verify this by enabling
    HTTP traffic tracing and checking that a POST to .../oauth/tokens
    appears after the ALTER SERVER (proving the cache was invalidated).
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
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     location_prefix 's3://{TEST_BUCKET}')
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
        f"Expected no token fetch before ALTER SERVER (token cached), "
        f"got {pre_alter_fetches}. Notices:\n" + "\n".join(superuser_conn.notices)
    )

    run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (SET client_id 'rotated-id')",
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
        "Expected COMMIT to fail after ALTER SERVER set bogus client_id "
        "(cache should have been invalidated, forcing re-auth with bad creds)"
    )
    assert post_alter_fetches >= 1, (
        f"Expected token re-fetch after ALTER SERVER (cache invalidated), "
        f"got {post_alter_fetches}. Notices ({len(post_alter_notices)}):\n"
        + "\n".join(post_alter_notices)
    )

    run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (SET client_id '{client_id}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} VALUES (4)",
        superuser_conn,
    )
    superuser_conn.commit()

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

    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()


def test_drop_server_blocked_by_dependent_table(
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
            OPTIONS (rest_endpoint '{endpoint}',
                     client_id '{client_id}',
                     client_secret '{client_secret}')
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
    run_command(
        f"GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC",
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
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     location_prefix 's3://{TEST_BUCKET}')
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

    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()


def test_alter_server_rest_endpoint_blocked_with_dependent_writable_tables(
    installcheck,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    create_http_helper_functions,
):
    """
    Changing rest_endpoint on a server that has dependent writable iceberg
    tables must be blocked.
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
                     client_id '{client_id}',
                     client_secret '{client_secret}',
                     location_prefix 's3://{TEST_BUCKET}')
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
    assert "dependent writable iceberg tables" in str(err)
    superuser_conn.rollback()

    run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (SET client_id '{client_id}')",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()

    err = run_command(
        f"ALTER SERVER {SERVER_NAME} OPTIONS (SET rest_endpoint 'http://other:8181')",
        superuser_conn,
        raise_error=False,
    )
    assert (
        err is None
    ), "ALTER SERVER rest_endpoint should succeed after dependent tables are dropped"
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
    run_command(f"DROP SERVER {SERVER_NAME}", superuser_conn)
    superuser_conn.commit()

import os
import pytest
from utils_pytest import *


# ── FDW --------------------------─────────────────────────────────────────-


def test_iceberg_catalog_fdw_exists(pg_conn, extension):
    """The iceberg_catalog FDW should be created by the extension."""
    result = run_query(
        "SELECT fdwname FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_catalog'",
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["fdwname"] == "iceberg_catalog"


def test_iceberg_catalog_fdw_has_no_handler(pg_conn, extension):
    """iceberg_catalog is configuration-only, so it should have no handler."""
    result = run_query(
        "SELECT fdwhandler FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_catalog'",
        pg_conn,
    )
    assert result[0]["fdwhandler"] == 0


# ── CREATE SERVER with valid options ───────────────────────────────────────


def test_create_rest_server_with_all_options(superuser_conn, extension):
    """All documented non-secret options should be accepted for a REST-type server.
    Uses a mix of quoted upper-case and plain lower-case option names to
    verify case-insensitive matching."""
    run_command(
        """
        CREATE SERVER test_rest_all_opts TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (
                "Rest_Endpoint" 'http://localhost:8181',
                "REST_AUTH_TYPE" 'OAuth2',
                "OAuth_Endpoint" 'http://localhost:8181/oauth/tokens',
                scope 'PRINCIPAL_ROLE:ALL',
                enable_vended_credentials 'true',
                "Location_Prefix" 's3://bucket/prefix',
                catalog_name 'my_catalog'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_rest_server_no_options(superuser_conn, extension):
    """A server with no options is valid; all settings fall back to GUCs."""
    run_command(
        """
        CREATE SERVER test_rest_no_opts TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_lake_write_user_can_create_server(pg_conn, extension):
    """A non-superuser with lake_write should be able to create a server."""
    run_command(
        """
        CREATE SERVER test_lake_write_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        pg_conn,
    )
    pg_conn.rollback()


def test_create_rest_server_minimal(superuser_conn, extension):
    """A server with just rest_endpoint should be accepted."""
    run_command(
        """
        CREATE SERVER test_rest_minimal TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_server_horizon_auth(superuser_conn, extension):
    """Horizon auth type should be accepted."""
    run_command(
        """
        CREATE SERVER test_horizon TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (
                rest_endpoint 'https://horizon.example.com',
                rest_auth_type 'horizon'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


# ── CREATE SERVER with invalid options ─────────────────────────────────────


def test_reject_unknown_server_option(superuser_conn, extension):
    """Unknown options should be rejected by the validator."""
    err = run_command(
        """
        CREATE SERVER test_bad_opt TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', bogus_option 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    assert "bogus_option" in str(err)
    superuser_conn.rollback()


def test_reject_invalid_auth_type(superuser_conn, extension):
    """Only 'oauth2', 'default', and 'horizon' are valid for rest_auth_type."""
    err = run_command(
        """
        CREATE SERVER test_bad_auth TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', rest_auth_type 'basic')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid rest_auth_type" in str(err)
    superuser_conn.rollback()


def test_reject_invalid_vended_creds(superuser_conn, extension):
    """enable_vended_credentials must be a valid boolean."""
    err = run_command(
        """
        CREATE SERVER test_bad_bool TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', enable_vended_credentials 'maybe')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    superuser_conn.rollback()


def test_reject_options_on_non_server(superuser_conn, extension):
    """Options on the FDW itself should be rejected."""
    err = run_command(
        """
        ALTER FOREIGN DATA WRAPPER iceberg_catalog OPTIONS (ADD rest_endpoint 'http://x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "only valid for SERVER" in str(err)
    superuser_conn.rollback()


def test_reject_secrets_in_server_options(superuser_conn, extension):
    """client_id and client_secret are not valid server options (they belong to user mappings)."""
    err = run_command(
        """
        CREATE SERVER test_secrets_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', client_id 'id')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    assert "client_id" in str(err)
    superuser_conn.rollback()


# ── CREATE USER MAPPING ───────────────────────────────────────────────────


def test_create_user_mapping_valid(superuser_conn, extension):
    """client_id, client_secret, and scope should be accepted as user mapping options."""
    run_command(
        """
        CREATE SERVER test_um_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_srv
            OPTIONS (client_id 'my-id', client_secret 'my-secret', scope 'PRINCIPAL_ROLE:ADMIN')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_user_mapping_public(superuser_conn, extension):
    """A PUBLIC user mapping should be accepted."""
    run_command(
        """
        CREATE SERVER test_um_pub_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR PUBLIC SERVER test_um_pub_srv
            OPTIONS (client_id 'pub-id', client_secret 'pub-secret')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_reject_unknown_user_mapping_option(superuser_conn, extension):
    """Unknown options in user mapping should be rejected."""
    run_command(
        """
        CREATE SERVER test_um_bad_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_bad_srv
            OPTIONS (client_id 'id', unknown_opt 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    assert "unknown_opt" in str(err)
    superuser_conn.rollback()


def test_reject_server_options_in_user_mapping(superuser_conn, extension):
    """Server-level options like rest_endpoint should not be accepted in user mapping."""
    run_command(
        """
        CREATE SERVER test_um_wrong_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_wrong_srv
            OPTIONS (rest_endpoint 'http://other:8181')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    superuser_conn.rollback()


def test_per_user_scope_in_user_mapping(superuser_conn, extension):
    """Different users can have different scopes on the same server."""
    run_command(
        """
        CREATE SERVER test_scope_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command("CREATE ROLE analyst LOGIN", superuser_conn)
    run_command("CREATE ROLE admin_user LOGIN", superuser_conn)
    run_command(
        """
        CREATE USER MAPPING FOR analyst SERVER test_scope_srv
            OPTIONS (client_id 'a-id', client_secret 'a-secret',
                     scope 'PRINCIPAL_ROLE:READER')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR admin_user SERVER test_scope_srv
            OPTIONS (client_id 'b-id', client_secret 'b-secret',
                     scope 'PRINCIPAL_ROLE:ALL')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT u.rolname, array_to_string(um.umoptions, ', ') AS opts
        FROM pg_user_mapping um
        JOIN pg_roles u ON u.oid = um.umuser
        WHERE um.umserver = (SELECT oid FROM pg_foreign_server WHERE srvname = 'test_scope_srv')
        ORDER BY u.rolname
        """,
        superuser_conn,
    )
    assert len(result) == 2
    assert "PRINCIPAL_ROLE:ALL" in result[0]["opts"]  # admin_user
    assert "PRINCIPAL_ROLE:READER" in result[1]["opts"]  # analyst

    run_command("DROP USER MAPPING FOR analyst SERVER test_scope_srv", superuser_conn)
    run_command(
        "DROP USER MAPPING FOR admin_user SERVER test_scope_srv", superuser_conn
    )
    run_command("DROP SERVER test_scope_srv", superuser_conn)
    run_command("DROP ROLE analyst", superuser_conn)
    run_command("DROP ROLE admin_user", superuser_conn)
    superuser_conn.rollback()


# ── CREATE FOREIGN TABLE on iceberg_catalog servers is blocked ──────────────


def test_reject_create_foreign_table_on_iceberg_catalog_server(
    superuser_conn, extension
):
    """CREATE FOREIGN TABLE on an iceberg_catalog server is blocked."""
    run_command(
        """
        CREATE SERVER test_ft_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        """
        CREATE FOREIGN TABLE test_ft_tbl (id int)
            SERVER test_ft_srv
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot create foreign tables on iceberg_catalog server" in str(err)
    superuser_conn.rollback()


# ── ALTER SERVER ───────────────────────────────────────────────────────────


def test_alter_server_add_option(superuser_conn, extension):
    """ALTER SERVER should allow adding new options."""
    run_command(
        """
        CREATE SERVER test_alter TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command(
        """
        ALTER SERVER test_alter OPTIONS (ADD scope 'PRINCIPAL_ROLE:ADMIN')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'test_alter'
        """,
        superuser_conn,
    )
    opts = result[0]["srvoptions"]
    assert "scope=PRINCIPAL_ROLE:ADMIN" in opts
    superuser_conn.rollback()


def test_alter_server_set_option(superuser_conn, extension):
    """ALTER SERVER should allow changing existing options."""
    run_command(
        """
        CREATE SERVER test_alter_set TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command(
        """
        ALTER SERVER test_alter_set OPTIONS (SET rest_endpoint 'http://new-host:8181')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'test_alter_set'
        """,
        superuser_conn,
    )
    opts = result[0]["srvoptions"]
    assert "rest_endpoint=http://new-host:8181" in opts
    superuser_conn.rollback()


def test_alter_server_reject_unknown_option(superuser_conn, extension):
    """ALTER SERVER should reject unknown options."""
    run_command(
        """
        CREATE SERVER test_alter_bad TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    err = run_command(
        """
        ALTER SERVER test_alter_bad OPTIONS (ADD unknown_opt 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    superuser_conn.rollback()


# ── DROP SERVER ────────────────────────────────────────────────────────────


def test_drop_server(superuser_conn, extension):
    """DROP SERVER should work for iceberg_catalog servers."""
    run_command(
        """
        CREATE SERVER test_drop_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command("DROP SERVER test_drop_srv", superuser_conn)

    result = run_query(
        "SELECT count(*) FROM pg_foreign_server WHERE srvname = 'test_drop_srv'",
        superuser_conn,
    )
    assert result[0]["count"] == 0
    superuser_conn.rollback()


# ── Using a server-based catalog in CREATE TABLE ───────────────────────────


def test_create_table_with_server_catalog(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """CREATE TABLE ... USING iceberg WITH (catalog = '<server_name>') should
    recognize the catalog option as a server-based REST catalog."""
    run_command(
        """
        CREATE SERVER test_srv_catalog TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_srv_catalog
            OPTIONS (client_id 'id', client_secret 'secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_srv_tbl ()
            USING iceberg
            WITH (catalog = 'test_srv_catalog', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # The REST endpoint is fake, so we expect a connection error, NOT a
    # "invalid catalog option" error. This proves the server was resolved.
    assert err is not None
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_srv_catalog", superuser_conn
    )
    run_command("DROP SERVER test_srv_catalog CASCADE", superuser_conn)
    superuser_conn.commit()


def test_invalid_catalog_name_errors(pg_conn, s3, extension, with_default_location):
    """A catalog name that is neither a known literal nor a valid server should error."""
    err = run_command(
        """
        CREATE TABLE test_bad_cat ()
            USING iceberg
            WITH (catalog = 'nonexistent_server', read_only = 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert "invalid catalog option" in str(err)
    pg_conn.rollback()


def test_non_iceberg_catalog_server_rejected(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """A foreign server not under iceberg_catalog FDW should not be accepted
    as a catalog value."""
    err = run_command(
        """
        CREATE TABLE test_wrong_fdw ()
            USING iceberg
            WITH (catalog = 'pg_lake_iceberg', read_only = 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert "invalid catalog option" in str(err)
    pg_conn.rollback()


# ── Credential resolution: catalogs.conf ──────────────────────────────────


@pytest.fixture
def pg_data_dir(superuser_conn):
    """Returns the PGDATA directory path."""
    result = run_query("SHOW data_directory", superuser_conn)
    return result[0]["data_directory"]


@pytest.fixture
def catalogs_conf(pg_data_dir):
    """Write a temporary catalogs.conf in $PGDATA and clean up after."""
    conf_path = os.path.join(pg_data_dir, "catalogs.conf")
    created = not os.path.exists(conf_path)
    original_content = None
    if not created:
        with open(conf_path, "r") as f:
            original_content = f.read()

    def _write(content):
        with open(conf_path, "w") as f:
            f.write(content)

    yield _write

    if created:
        if os.path.exists(conf_path):
            os.remove(conf_path)
    else:
        with open(conf_path, "w") as f:
            f.write(original_content or "")


def test_credentials_from_catalogs_conf(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """Credentials should be resolved from $PGDATA/catalogs.conf when no
    user mapping exists."""
    catalogs_conf(
        "test_conf_srv.client_id = 'conf-id'\n"
        "test_conf_srv.client_secret = 'conf-secret'\n"
    )
    run_command(
        """
        CREATE SERVER test_conf_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_conf_tbl ()
            USING iceberg
            WITH (catalog = 'test_conf_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # Should get a connection error (fake endpoint), not a credentials error
    assert err is not None
    assert "no credentials found" not in str(err)
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_conf_srv CASCADE", superuser_conn)
    superuser_conn.commit()


def test_user_mapping_overrides_catalogs_conf(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """User mapping credentials should take priority over catalogs.conf."""
    catalogs_conf(
        "test_override_srv.client_id = 'conf-id'\n"
        "test_override_srv.client_secret = 'conf-secret'\n"
    )
    run_command(
        """
        CREATE SERVER test_override_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_override_srv
            OPTIONS (client_id 'um-id', client_secret 'um-secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_override_tbl ()
            USING iceberg
            WITH (catalog = 'test_override_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" not in str(err)
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_override_srv", superuser_conn
    )
    run_command("DROP SERVER test_override_srv CASCADE", superuser_conn)
    superuser_conn.commit()


def test_no_credentials_errors(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
):
    """Without user mapping, catalogs.conf, or GUCs, credential resolution should error."""
    run_command(
        """
        CREATE SERVER test_no_creds TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_no_creds_tbl ()
            USING iceberg
            WITH (catalog = 'test_no_creds', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_no_creds CASCADE", superuser_conn)
    superuser_conn.commit()


def test_scope_from_catalogs_conf(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """catalogs.conf can also provide scope alongside credentials."""
    catalogs_conf(
        "test_scope_srv.client_id = 'conf-id'\n"
        "test_scope_srv.client_secret = 'conf-secret'\n"
        "test_scope_srv.scope = 'PRINCIPAL_ROLE:ADMIN'\n"
    )
    run_command(
        """
        CREATE SERVER test_scope_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_scope_tbl ()
            USING iceberg
            WITH (catalog = 'test_scope_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" not in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_scope_srv CASCADE", superuser_conn)
    superuser_conn.commit()


def test_credentials_from_catalogs_conf_for_rest_server(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """catalogs.conf credentials for the extension-owned 'rest' server"""

    # Clearing the GUCs shouldn't matter when catalogs.conf is set (it should
    # take priority), but there is currently no way to verify that credentials
    # are actually coming from catalogs.conf rather than GUC fallback.
    run_command("SET pg_lake_iceberg.rest_catalog_client_id TO ''", superuser_conn)
    run_command("SET pg_lake_iceberg.rest_catalog_client_secret TO ''", superuser_conn)

    # Without catalogs.conf, credentials should be missing
    err = run_command(
        """
        CREATE TABLE test_rest_conf_tbl ()
            USING iceberg
            WITH (catalog = 'rest', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" in str(err)
    pg_conn.rollback()

    # Now write catalogs.conf — credentials should resolve
    catalogs_conf(
        "rest.client_id = 'platform-id'\n" "rest.client_secret = 'platform-secret'\n"
    )

    err = run_command(
        """
        CREATE TABLE test_rest_conf_tbl ()
            USING iceberg
            WITH (catalog = 'rest', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" not in str(err)
    pg_conn.rollback()


# ── Backward compatibility ─────────────────────────────────────────────────


def test_catalog_rest_literal_still_works(
    pg_conn, s3, extension, with_default_location
):
    """catalog='rest' (literal) should still work via GUC fallback."""
    err = run_command(
        """
        CREATE TABLE test_rest_literal ()
            USING iceberg
            WITH (catalog = 'rest', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # Will fail because REST GUCs aren't configured, but should NOT fail
    # with "invalid catalog option"
    if err is not None:
        assert "invalid catalog option" not in str(err)
    pg_conn.rollback()


def test_catalog_postgres_literal_still_works(
    pg_conn, s3, extension, with_default_location
):
    """catalog='postgres' (literal) should still work."""
    run_command(
        """
        CREATE TABLE test_pg_literal (id int)
            USING iceberg
            WITH (catalog = 'postgres')
        """,
        pg_conn,
    )
    pg_conn.rollback()


def test_catalog_object_store_literal_still_works(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
):
    """catalog='object_store' (literal) should still work."""
    run_command(
        """
        CREATE TABLE test_os_literal (id int)
            USING iceberg
            WITH (catalog = 'object_store')
        """,
        pg_conn,
    )
    pg_conn.rollback()


# ── Protection of reserved catalog names ───────────────────────────────────


def test_reject_create_server_type_postgres(superuser_conn, extension):
    """Users cannot create a new server with TYPE 'postgres'."""
    err = run_command(
        """
        CREATE SERVER my_postgres TYPE 'postgres'
            FOREIGN DATA WRAPPER iceberg_catalog
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot create iceberg_catalog server with TYPE 'postgres'" in str(err)
    superuser_conn.rollback()


def test_reject_create_server_type_object_store(superuser_conn, extension):
    """Users cannot create a new server with TYPE 'object_store'."""
    err = run_command(
        """
        CREATE SERVER my_obj_store TYPE 'object_store'
            FOREIGN DATA WRAPPER iceberg_catalog
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot create iceberg_catalog server with TYPE 'object_store'" in str(err)
    superuser_conn.rollback()


def test_reject_create_server_non_rest_type(superuser_conn, extension):
    """Any TYPE other than 'rest' is rejected for user-created iceberg_catalog servers."""
    err = run_command(
        """
        CREATE SERVER my_server TYPE 'something_else'
            FOREIGN DATA WRAPPER iceberg_catalog
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "iceberg_catalog server requires TYPE 'rest'" in str(err)
    superuser_conn.rollback()


def test_reject_create_server_without_type(superuser_conn, extension):
    """CREATE SERVER without TYPE is rejected for iceberg_catalog servers."""
    err = run_command(
        """
        CREATE SERVER my_server
            FOREIGN DATA WRAPPER iceberg_catalog
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "iceberg_catalog server requires TYPE 'rest'" in str(err)
    superuser_conn.rollback()


def test_reject_create_server_reserved_name(superuser_conn, extension):
    """CREATE SERVER with a reserved catalog name (case-insensitive) is blocked."""
    reserved_names = [
        "Postgres",
        "OBJECT_STORE",
        "ReSt",
    ]
    for name in reserved_names:
        err = run_command(
            f"""
            CREATE SERVER "{name}" TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
            """,
            superuser_conn,
            raise_error=False,
        )
        assert err is not None, f"Expected error for reserved name '{name}'"
        assert "is reserved" in str(err)
        superuser_conn.rollback()


def test_reject_rename_to_reserved_name(superuser_conn, extension):
    """Renaming a user-created server TO a reserved name is blocked."""
    run_command(
        """
        CREATE SERVER tmp_rename_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    for reserved in ["POSTGRES", "Object_Store", "REST"]:
        err = run_command(
            f'ALTER SERVER tmp_rename_srv RENAME TO "{reserved}"',
            superuser_conn,
            raise_error=False,
        )
        assert err is not None, f"Expected error for renaming to '{reserved}'"
        assert "reserved for the extension-owned catalog" in str(err)
        superuser_conn.rollback()
        run_command(
            """
            CREATE SERVER tmp_rename_srv TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint 'http://localhost:8181')
            """,
            superuser_conn,
        )
    superuser_conn.rollback()


def test_allow_drop_user_created_server(superuser_conn, extension):
    """DROP SERVER on a user-created server should work fine."""
    run_command(
        """
        CREATE SERVER user_rest_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command("DROP SERVER user_rest_srv", superuser_conn)
    superuser_conn.rollback()


def test_allow_rename_user_created_server(superuser_conn, extension):
    """RENAME on a user-created server should work fine."""
    run_command(
        """
        CREATE SERVER user_rename_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        "ALTER SERVER user_rename_srv RENAME TO user_renamed_srv", superuser_conn
    )
    superuser_conn.rollback()


def test_allow_owner_change_user_created_server(superuser_conn, extension):
    """ALTER SERVER ... OWNER TO on a user-created server should work fine."""
    run_command(
        """
        CREATE SERVER user_owner_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        "ALTER SERVER user_owner_srv OWNER TO CURRENT_USER",
        superuser_conn,
    )
    superuser_conn.rollback()


# ── default_catalog GUC with user-created servers ──────────────────────────


def test_set_default_catalog_to_user_created_rest_server(superuser_conn, extension):
    """SET pg_lake_iceberg.default_catalog should accept a user-created
    iceberg_catalog server with TYPE 'rest'."""
    run_command(
        """
        CREATE SERVER my_rest_catalog TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        "SET pg_lake_iceberg.default_catalog = 'my_rest_catalog'",
        superuser_conn,
    )

    result = run_query(
        "SHOW pg_lake_iceberg.default_catalog",
        superuser_conn,
    )
    assert result[0]["pg_lake_iceberg.default_catalog"] == "my_rest_catalog"
    superuser_conn.rollback()


def test_set_default_catalog_rejects_nonexistent_server(pg_conn, extension):
    """SET pg_lake_iceberg.default_catalog should reject a name that is
    neither a built-in literal nor an existing server."""
    err = run_command(
        "SET pg_lake_iceberg.default_catalog = 'no_such_server'",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "user-created iceberg_catalog server" in str(err)
    pg_conn.rollback()


# ── Case-sensitive server names ────────────────────────────────────────────


def test_case_sensitive_server_names(superuser_conn, extension):
    """Server names are case-sensitive: "test_cs" and "TEST_CS" are distinct
    servers that can coexist with different options."""
    run_command(
        """
        CREATE SERVER test_cs TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://host-lower:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE SERVER "TEST_CS" TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://host-upper:8181')
        """,
        superuser_conn,
    )
    lower_opts = run_query(
        "SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'test_cs'",
        superuser_conn,
    )
    upper_opts = run_query(
        "SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'TEST_CS'",
        superuser_conn,
    )

    assert len(lower_opts) == 1
    assert len(upper_opts) == 1
    assert "host-lower" in str(lower_opts[0]["srvoptions"])
    assert "host-upper" in str(upper_opts[0]["srvoptions"])

    run_command("DROP SERVER test_cs", superuser_conn)
    run_command('DROP SERVER "TEST_CS"', superuser_conn)
    superuser_conn.rollback()


# ── Query string scrubbing for user mapping DDL ───────────────────────────


def test_scrub_create_user_mapping_in_pg_stat_statements(
    installcheck, superuser_conn, extension
):
    """CREATE USER MAPPING secrets should be scrubbed in pg_stat_statements."""
    if installcheck:
        return

    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        CREATE SERVER test_scrub_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    superuser_conn.commit()

    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_srv
            OPTIONS (client_id 'secret_id_value', client_secret 'secret_key_value')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        """
        SELECT query FROM pg_stat_statements
        WHERE query ILIKE '%CREATE USER MAPPING%test_scrub_srv%'
        """,
        superuser_conn,
    )

    assert len(result) >= 1
    query_text = result[0]["query"]
    assert "secret_id_value" not in query_text
    assert "secret_key_value" not in query_text
    assert "'***" in query_text

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_scrub_srv", superuser_conn
    )
    run_command("DROP SERVER test_scrub_srv", superuser_conn)
    superuser_conn.commit()


def test_scrub_alter_user_mapping_in_pg_stat_statements(
    installcheck, superuser_conn, extension
):
    """ALTER USER MAPPING secrets should also be scrubbed in pg_stat_statements."""
    if installcheck:
        return

    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        CREATE SERVER test_scrub_alter_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_alter_srv
            OPTIONS (client_id 'old_id', client_secret 'old_secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        ALTER USER MAPPING FOR CURRENT_USER SERVER test_scrub_alter_srv
            OPTIONS (SET client_id 'new_secret_id', SET client_secret 'new_secret_key')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        """
        SELECT query FROM pg_stat_statements
        WHERE query ILIKE '%ALTER USER MAPPING%test_scrub_alter_srv%'
        """,
        superuser_conn,
    )

    assert len(result) >= 1
    query_text = result[0]["query"]
    assert "new_secret_id" not in query_text
    assert "new_secret_key" not in query_text
    assert "'***" in query_text

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_scrub_alter_srv",
        superuser_conn,
    )
    run_command("DROP SERVER test_scrub_alter_srv", superuser_conn)
    superuser_conn.commit()


def test_scrub_preserves_actual_credentials(superuser_conn, extension):
    """Scrubbing the query string should not affect the stored credentials."""
    run_command(
        """
        CREATE SERVER test_scrub_creds_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_creds_srv
            OPTIONS (client_id 'real_id', client_secret 'real_secret')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT umoptions FROM pg_user_mapping um
            JOIN pg_foreign_server fs ON um.umserver = fs.oid
        WHERE fs.srvname = 'test_scrub_creds_srv'
        """,
        superuser_conn,
    )

    assert len(result) == 1
    opts = result[0]["umoptions"]
    assert "client_id=real_id" in opts
    assert "client_secret=real_secret" in opts

    superuser_conn.rollback()


def test_scrub_leaves_scope_visible(installcheck, superuser_conn, extension):
    """scope is not a secret — it should remain visible after scrubbing."""
    if installcheck:
        return

    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        CREATE SERVER test_scrub_scope_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_scope_srv
            OPTIONS (client_id 'id123', client_secret 'secret456', scope 'PRINCIPAL_ROLE:ALL')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        """
        SELECT query FROM pg_stat_statements
        WHERE query ILIKE '%CREATE USER MAPPING%test_scrub_scope_srv%'
        """,
        superuser_conn,
    )

    assert len(result) >= 1
    query_text = result[0]["query"]
    assert "id123" not in query_text
    assert "secret456" not in query_text
    assert "PRINCIPAL_ROLE:ALL" in query_text

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_scrub_scope_srv",
        superuser_conn,
    )
    run_command("DROP SERVER test_scrub_scope_srv", superuser_conn)
    superuser_conn.commit()

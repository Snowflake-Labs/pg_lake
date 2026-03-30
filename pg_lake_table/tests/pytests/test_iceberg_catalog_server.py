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
    """All documented options should be accepted for a REST-type server.
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
                catalog_name 'my_catalog',
                "Client_Id" 'test-id',
                "Client_Secret" 'test-secret'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


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
                rest_auth_type 'horizon',
                client_secret 'secret'
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
            OPTIONS (
                rest_endpoint 'http://localhost:8181',
                client_id 'id',
                client_secret 'secret'
            )
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

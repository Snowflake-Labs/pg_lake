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
                rest_auth_type 'horizon',
                client_secret 'secret'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


# ── CREATE SERVER with invalid options ─────────────────────────────────────


def test_reject_unknown_server_option(superuser_conn, extension):
    """
    Unknown options should be rejected by the validator.

    Issued twice on the same connection because the validator caches the
    hint string in a static; the second call must hit the cached path and
    must still produce a well-formed hint (regression guard against the
    hint being palloc'd in a per-statement memory context).
    """
    EXPECTED_OPTIONS = [
        "rest_endpoint",
        "rest_auth_type",
        "oauth_endpoint",
        "scope",
        "enable_vended_credentials",
        "location_prefix",
        "catalog_name",
        "client_id",
        "client_secret",
    ]

    for typo in ("bogus_option", "another_typo"):
        err = run_command(
            f"""
            CREATE SERVER test_bad_opt_{typo} TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint 'http://localhost:8181', {typo} 'x')
            """,
            superuser_conn,
            raise_error=False,
        )
        msg = str(err)
        assert "invalid option" in msg
        assert typo in msg
        assert "Valid options are:" in msg
        for opt in EXPECTED_OPTIONS:
            assert opt in msg, f"hint missing {opt!r} on attempt {typo!r}: {msg}"
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
    """ALTER FOREIGN DATA WRAPPER iceberg_catalog is blocked entirely,
    so passing OPTIONS on the FDW never reaches the validator."""
    err = run_command(
        """
        ALTER FOREIGN DATA WRAPPER iceberg_catalog OPTIONS (ADD rest_endpoint 'http://x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert 'cannot alter the "iceberg_catalog" foreign data wrapper' in str(err)
    superuser_conn.rollback()


# ── Option value validation ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "option, bad_value, expected_error",
    [
        ("rest_endpoint", "", "must not be empty"),
        ("rest_endpoint", "localhost:8181", "URI scheme"),
        ("oauth_endpoint", "", "must not be empty"),
        ("oauth_endpoint", "localhost/oauth/tokens", "URI scheme"),
        ("location_prefix", "", "must not be empty"),
        ("location_prefix", "my-bucket/prefix", "URI scheme"),
        ("catalog_name", "", "must not be empty"),
        ("client_id", "", "must not be empty"),
        ("client_secret", "", "must not be empty"),
        ("scope", "", "must not be empty"),
    ],
)
def test_reject_bad_option_values(
    superuser_conn, extension, option, bad_value, expected_error
):
    err = run_command(
        f"""
        CREATE SERVER test_bad_val TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS ({option} '{bad_value}')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None, f"Expected error for {option}='{bad_value}'"
    assert expected_error in str(err), f"Expected '{expected_error}' in: {err}"
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
    run_command(
        "GRANT USAGE ON FOREIGN SERVER test_srv_catalog TO PUBLIC",
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
    assert "permission denied" not in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_srv_catalog CASCADE", superuser_conn)
    superuser_conn.commit()


def test_create_table_requires_usage_on_catalog_server(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """A non-superuser without USAGE on the catalog server must be
    denied when creating a table that references it."""
    run_command(
        """
        CREATE SERVER test_no_usage_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_no_usage_tbl (id bigint)
            USING iceberg
            WITH (catalog = 'test_no_usage_srv')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "permission denied for foreign server" in str(err).lower()
    pg_conn.rollback()

    run_command("DROP SERVER test_no_usage_srv", superuser_conn)
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


def test_reject_rename_iceberg_catalog_server(superuser_conn, extension):
    """Renaming an iceberg_catalog server is blocked because dependent tables
    store the server name as a string option in ftoptions."""
    run_command(
        """
        CREATE SERVER user_rename_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        "ALTER SERVER user_rename_srv RENAME TO user_renamed_srv",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot rename iceberg_catalog server" in str(err)
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


def test_alter_system_default_catalog_defers_validation(
    superuser_conn, pg_conn, extension
):
    """The GUC check hook for pg_lake_iceberg.default_catalog cannot do catalog
    lookups during SIGHUP reload (!IsTransactionState()), so it accepts the
    value on faith — mirroring PostgreSQL's check_default_tablespace.

    Sequence: create a server, ALTER SYSTEM SET to it (passes in-transaction
    validation), drop the server, then pg_reload_conf() re-applies the
    now-stale name.  The check hook must let it through.  A subsequent
    CREATE TABLE must fail at runtime."""
    run_command(
        """
        CREATE SERVER alter_sys_cat TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    superuser_conn.autocommit = True
    run_command(
        "ALTER SYSTEM SET pg_lake_iceberg.default_catalog = 'alter_sys_cat'",
        superuser_conn,
    )
    run_command("SELECT pg_reload_conf()", superuser_conn)
    run_command("SELECT pg_sleep(0.2)", superuser_conn)

    result = run_query(
        "SHOW pg_lake_iceberg.default_catalog",
        superuser_conn,
    )
    assert result[0][0] == "alter_sys_cat"

    run_command("DROP SERVER alter_sys_cat", superuser_conn)
    run_command("SELECT pg_reload_conf()", superuser_conn)
    run_command("SELECT pg_sleep(0.2)", superuser_conn)

    result = run_query(
        "SHOW pg_lake_iceberg.default_catalog",
        superuser_conn,
    )
    assert result[0][0] == "alter_sys_cat"
    superuser_conn.autocommit = False

    err = run_command(
        "CREATE TABLE alter_sys_test (id bigint) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "invalid catalog option" in str(err).lower()
    pg_conn.rollback()

    superuser_conn.autocommit = True
    run_command(
        "ALTER SYSTEM RESET pg_lake_iceberg.default_catalog",
        superuser_conn,
    )
    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.autocommit = False


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


# ── Built-in catalog servers ───────────────────────────────────────────────


BUILTIN_CATALOG_SERVERS = [
    ("pg_lake_postgres_catalog", "postgres"),
    ("pg_lake_object_store_catalog", "object_store"),
    ("pg_lake_rest_catalog", "rest"),
]


def test_builtin_catalog_servers_exist(pg_conn, extension):
    """The three built-in iceberg_catalog servers are pre-created by the
    extension and survive as anchors for pg_depend edges."""
    result = run_query(
        """
        SELECT srvname, srvtype
        FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper fdw ON s.srvfdw = fdw.oid
        WHERE fdw.fdwname = 'iceberg_catalog'
          AND srvname LIKE 'pg_lake_%_catalog'
        ORDER BY srvname
        """,
        pg_conn,
    )
    names_types = [(row["srvname"], row["srvtype"]) for row in result]
    assert names_types == sorted(BUILTIN_CATALOG_SERVERS)


def test_builtin_servers_are_extension_owned(pg_conn, extension):
    """Built-in servers are members of the pg_lake_iceberg extension so
    they get included in pg_dump's CREATE EXTENSION shadow rather than
    emitted as standalone CREATE SERVER statements."""
    result = run_query(
        """
        SELECT srvname
        FROM pg_foreign_server s
        JOIN pg_depend d ON d.objid = s.oid
        JOIN pg_extension e ON e.oid = d.refobjid
        WHERE e.extname = 'pg_lake_iceberg'
          AND d.deptype = 'e'
          AND d.classid = 'pg_foreign_server'::regclass
        ORDER BY srvname
        """,
        pg_conn,
    )
    assert [r["srvname"] for r in result] == [
        name for name, _ in sorted(BUILTIN_CATALOG_SERVERS)
    ]


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_reject_create_server_with_builtin_long_name(
    long_name, _short_name, superuser_conn, extension
):
    """CREATE SERVER with the internal long name is blocked; the long
    names are implementation details and must never be addressable as
    a user-typed server name."""
    err = run_command(
        f"""
        CREATE SERVER {long_name} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "reserved for an internal" in str(err)
    superuser_conn.rollback()


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_reject_rename_to_builtin_long_name(
    long_name, _short_name, superuser_conn, extension
):
    """Renaming a user-created server TO an internal long name is blocked."""
    run_command(
        """
        CREATE SERVER rn_long_src TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        f"ALTER SERVER rn_long_src RENAME TO {long_name}",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "reserved for an internal" in str(err)
    superuser_conn.rollback()


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_alter_options_on_builtin_server_blocked(
    long_name, _short_name, superuser_conn, extension
):
    """ALTER SERVER OPTIONS on a built-in server is unconditionally blocked
    — built-ins are immutable structural anchors and all configuration
    lives in GUCs."""
    err = run_command(
        f"ALTER SERVER {long_name} OPTIONS (ADD rest_endpoint 'http://x:8181')",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot alter the built-in pg_lake_iceberg catalog server" in str(err)
    superuser_conn.rollback()


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_alter_owner_on_builtin_server_blocked(
    long_name, _short_name, superuser_conn, extension
):
    """ALTER SERVER ... OWNER TO on a built-in server is blocked."""
    err = run_command(
        f"ALTER SERVER {long_name} OWNER TO lake_write",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot change owner of the built-in" in str(err)
    superuser_conn.rollback()


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_drop_builtin_server_blocked(long_name, _short_name, superuser_conn, extension):
    """DROP SERVER on a built-in server is blocked by PostgreSQL itself,
    because the server is extension-owned (pg_depend deptype = 'e')."""
    err = run_command(f"DROP SERVER {long_name}", superuser_conn, raise_error=False)
    assert err is not None
    assert "extension pg_lake_iceberg" in str(err) or "depends on" in str(err)
    superuser_conn.rollback()


def test_reject_rename_builtin_server(superuser_conn, extension):
    """Renaming the built-in servers is blocked by the iceberg_catalog
    rename guard (which already blocks renaming any iceberg_catalog
    server, built-in or user-created)."""
    err = run_command(
        "ALTER SERVER pg_lake_rest_catalog RENAME TO renamed_builtin",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot rename iceberg_catalog server" in str(err)
    superuser_conn.rollback()


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_alter_extension_drop_builtin_server_blocked(
    long_name, _short_name, superuser_conn, extension
):
    """ALTER EXTENSION pg_lake_iceberg DROP SERVER <built-in> would remove
    the DEPENDENCY_EXTENSION edge, allowing a subsequent DROP SERVER to
    succeed and break every table using that catalog.  Both operations
    require superuser, but this is defense-in-depth against accidental
    or ill-advised detachment."""
    err = run_command(
        f"ALTER EXTENSION pg_lake_iceberg DROP SERVER {long_name}",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert "cannot remove the built-in catalog server" in str(err)
    superuser_conn.rollback()


def test_alter_extension_drop_iceberg_catalog_fdw_blocked(superuser_conn, extension):
    """ALTER EXTENSION pg_lake_iceberg DROP FOREIGN DATA WRAPPER
    iceberg_catalog would detach the FDW from the extension, allowing
    DROP FOREIGN DATA WRAPPER iceberg_catalog to succeed and cascade
    to every server.  Requires superuser; blocked as defense-in-depth."""
    err = run_command(
        "ALTER EXTENSION pg_lake_iceberg DROP FOREIGN DATA WRAPPER iceberg_catalog",
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    assert 'cannot remove the "iceberg_catalog" foreign data wrapper' in str(err)
    superuser_conn.rollback()


def test_alter_fdw_iceberg_catalog_blocked(superuser_conn, extension):
    """ALTER FOREIGN DATA WRAPPER iceberg_catalog is blocked entirely.
    Replacing the validator would silently disable option checking;
    adding a handler would change execution semantics.  Both require
    superuser, but this guards against silent breakage of all downstream
    servers and tables."""
    for variant in [
        "ALTER FOREIGN DATA WRAPPER iceberg_catalog NO VALIDATOR",
        "ALTER FOREIGN DATA WRAPPER iceberg_catalog VALIDATOR pg_catalog.postgresql_fdw_validator",
        "ALTER FOREIGN DATA WRAPPER iceberg_catalog OPTIONS (ADD debug 'true')",
    ]:
        err = run_command(variant, superuser_conn, raise_error=False)
        assert err is not None, f"expected {variant!r} to be blocked"
        assert 'cannot alter the "iceberg_catalog" foreign data wrapper' in str(
            err
        ), f"wrong error for {variant!r}: {err}"
        superuser_conn.rollback()


@pytest.mark.parametrize("long_name,_short_name", BUILTIN_CATALOG_SERVERS)
def test_reject_catalog_option_with_builtin_long_name(
    long_name, _short_name, pg_conn, extension
):
    """The catalog= option on CREATE TABLE must use the short names
    ('rest', 'postgres', 'object_store'); the internal long names are
    rejected with a clear error pointing to the short forms."""
    err = run_command(
        f"CREATE TABLE long_name_tbl (id int) USING iceberg WITH (catalog='{long_name}')",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "reserved for an internal" in str(err)
    pg_conn.rollback()


def test_postgres_fdw_named_postgres_does_not_conflict(superuser_conn, extension):
    """A pre-existing CREATE SERVER named 'postgres' (e.g. from postgres_fdw)
    does not collide with pg_lake's built-in postgres catalog.  The user-facing
    short name 'postgres' maps internally to 'pg_lake_postgres_catalog', so the
    two coexist without interaction."""
    run_command("CREATE EXTENSION IF NOT EXISTS postgres_fdw", superuser_conn)

    run_command(
        """
        CREATE SERVER postgres FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host 'localhost', port '5432', dbname 'postgres')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        coexist = run_query(
            """
            SELECT srvname, fdw.fdwname
            FROM pg_foreign_server s
            JOIN pg_foreign_data_wrapper fdw ON s.srvfdw = fdw.oid
            WHERE srvname IN ('postgres', 'pg_lake_postgres_catalog')
            ORDER BY srvname
            """,
            superuser_conn,
        )
        rows = [(r["srvname"], r["fdwname"]) for r in coexist]
        assert ("postgres", "postgres_fdw") in rows
        assert ("pg_lake_postgres_catalog", "iceberg_catalog") in rows
    finally:
        run_command("DROP SERVER postgres", superuser_conn)
        superuser_conn.commit()


def test_pg_depend_edge_for_builtin_postgres_catalog_table(
    pg_conn, s3, extension, with_default_location
):
    """A table created with catalog='postgres' (the short reserved name)
    records a pg_depend edge against the built-in pg_lake_postgres_catalog
    server.  This is what makes DROP EXTENSION CASCADE clean up such
    tables, and is the structural reason for pre-creating the built-in
    servers in the first place."""
    run_command(
        """
        CREATE TABLE pg_depend_tbl (id int)
            USING iceberg
            WITH (catalog = 'postgres')
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        result = run_query(
            """
            SELECT s.srvname
            FROM pg_class c
            JOIN pg_depend d ON d.objid = c.oid
                            AND d.classid = 'pg_class'::regclass
                            AND d.refclassid = 'pg_foreign_server'::regclass
            JOIN pg_foreign_server s ON s.oid = d.refobjid
            JOIN pg_foreign_data_wrapper fdw ON s.srvfdw = fdw.oid
            WHERE c.relname = 'pg_depend_tbl'
              AND fdw.fdwname = 'iceberg_catalog'
            """,
            pg_conn,
        )
        assert len(result) == 1
        assert result[0]["srvname"] == "pg_lake_postgres_catalog"
    finally:
        run_command("DROP TABLE pg_depend_tbl", pg_conn)
        pg_conn.commit()

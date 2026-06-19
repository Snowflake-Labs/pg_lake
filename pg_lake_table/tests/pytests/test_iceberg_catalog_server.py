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
    """All server-context options should be accepted for a REST-type server.
    Credentials (client_id, client_secret) live on user mappings now and
    are exercised in test_create_user_mapping_with_credentials below.
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
    """Horizon auth type should be accepted at the SERVER level; the
    matching credentials are carried by the user mapping."""
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
    run_command(
        """
        CREATE USER MAPPING FOR PUBLIC SERVER test_horizon
            OPTIONS (client_secret 'secret')
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

    The SERVER hint must NOT mention user-mapping-only options
    (client_id, client_secret); credentials live on user mappings.
    """
    EXPECTED_OPTIONS = [
        "rest_endpoint",
        "rest_auth_type",
        "oauth_endpoint",
        "scope",
        "enable_vended_credentials",
        "location_prefix",
        "catalog_name",
    ]
    USER_MAPPING_ONLY_OPTIONS = ["client_id", "client_secret"]

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
        assert "iceberg_catalog server" in msg
        assert typo in msg
        assert "Valid options are:" in msg
        for opt in EXPECTED_OPTIONS:
            assert opt in msg, f"hint missing {opt!r} on attempt {typo!r}: {msg}"
        for opt in USER_MAPPING_ONLY_OPTIONS:
            assert opt not in msg, (
                f"hint should NOT list user-mapping-only option {opt!r} "
                f"on attempt {typo!r}: {msg}"
            )
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
        ("scope", "", "must not be empty"),
    ],
)
def test_reject_bad_server_option_values(
    superuser_conn, extension, option, bad_value, expected_error
):
    """Value-level validation on SERVER options."""
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


@pytest.mark.parametrize(
    "option, bad_value, expected_error",
    [
        ("client_id", "", "must not be empty"),
        ("client_secret", "", "must not be empty"),
        ("scope", "", "must not be empty"),
    ],
)
def test_reject_bad_user_mapping_option_values(
    superuser_conn, extension, option, bad_value, expected_error
):
    """Value-level validation on USER MAPPING options (client_id,
    client_secret, and scope when set on the mapping).

    CREATE SERVER is committed first so that the rollback() triggered by
    the failed CREATE USER MAPPING below does not undo it; the server
    is then dropped CASCADE in a finally block."""
    run_command(
        """
        CREATE SERVER test_bad_um_val TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    try:
        err = run_command(
            f"""
            CREATE USER MAPPING FOR PUBLIC SERVER test_bad_um_val
                OPTIONS ({option} '{bad_value}')
            """,
            superuser_conn,
            raise_error=False,
        )
        assert err is not None, f"Expected error for {option}='{bad_value}'"
        assert expected_error in str(err), f"Expected '{expected_error}' in: {err}"
        superuser_conn.rollback()
    finally:
        run_command("DROP SERVER IF EXISTS test_bad_um_val CASCADE", superuser_conn)
        superuser_conn.commit()


# ── USER MAPPING context: validator rejects server-only options ─────────────


def test_reject_server_option_on_user_mapping(superuser_conn, extension):
    """Server-only options (rest_endpoint, location_prefix, etc.) must
    be rejected when set on a USER MAPPING.  The validator branch on
    UserMappingRelationId picks them out from the descriptor table by
    context bit."""
    run_command(
        """
        CREATE SERVER test_um_rej_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    # Commit the SERVER outside the txn that the failed CREATE USER
    # MAPPING below will abort -- otherwise rollback() in each iteration
    # would also undo the SERVER, breaking the loop on the second pass.
    superuser_conn.commit()
    try:
        for srv_only in [
            ("rest_endpoint", "http://other:8181"),
            ("location_prefix", "s3://bucket/x"),
            ("catalog_name", "foo"),
            ("oauth_endpoint", "http://o:8181/t"),
            ("rest_auth_type", "OAuth2"),
            ("enable_vended_credentials", "true"),
        ]:
            err = run_command(
                f"""
                CREATE USER MAPPING FOR PUBLIC SERVER test_um_rej_srv
                    OPTIONS ({srv_only[0]} '{srv_only[1]}')
                """,
                superuser_conn,
                raise_error=False,
            )
            assert (
                err is not None
            ), f"Expected rejection for {srv_only[0]} on user mapping"
            msg = str(err)
            assert "invalid option" in msg
            assert srv_only[0] in msg
            assert (
                "iceberg_catalog user mapping" in msg
            ), f"Error should identify user-mapping context: {msg}"
            superuser_conn.rollback()
    finally:
        run_command("DROP SERVER IF EXISTS test_um_rej_srv CASCADE", superuser_conn)
        superuser_conn.commit()


def test_reject_user_mapping_option_on_server(superuser_conn, extension):
    """User-mapping-only options (client_id, client_secret) must be
    rejected when set on a SERVER.  This is the inverse of the test
    above and proves the context bitmap denies in both directions."""
    for um_only in ["client_id", "client_secret"]:
        err = run_command(
            f"""
            CREATE SERVER test_srv_rej_um TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint 'http://localhost:8181', {um_only} 'x')
            """,
            superuser_conn,
            raise_error=False,
        )
        assert err is not None, f"Expected rejection for {um_only} on server"
        msg = str(err)
        assert "invalid option" in msg
        assert um_only in msg
        assert "iceberg_catalog server" in msg
        superuser_conn.rollback()


def test_user_mapping_hint_lists_user_mapping_options_only(superuser_conn, extension):
    """When an unknown option is rejected at USER MAPPING context, the
    hint must enumerate ONLY user-mapping-context options (client_id,
    client_secret, scope) -- never server-only ones.

    Issued twice on the same connection to exercise the per-context
    static hint cache, mirroring the SERVER-side hint test."""
    run_command(
        """
        CREATE SERVER test_um_hint_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    EXPECTED = ["client_id", "client_secret", "scope"]
    SERVER_ONLY_NEGATIVES = [
        "rest_endpoint",
        "rest_auth_type",
        "oauth_endpoint",
        "enable_vended_credentials",
        "location_prefix",
        "catalog_name",
    ]

    try:
        for typo in ("um_bogus", "um_another"):
            err = run_command(
                f"""
                CREATE USER MAPPING FOR PUBLIC SERVER test_um_hint_srv
                    OPTIONS ({typo} 'x')
                """,
                superuser_conn,
                raise_error=False,
            )
            msg = str(err)
            assert "invalid option" in msg
            assert "iceberg_catalog user mapping" in msg
            assert "Valid options are:" in msg
            for opt in EXPECTED:
                assert opt in msg, f"hint missing {opt!r} on attempt {typo!r}: {msg}"
            for opt in SERVER_ONLY_NEGATIVES:
                assert opt not in msg, (
                    f"USER MAPPING hint should NOT list server-only option "
                    f"{opt!r} on attempt {typo!r}: {msg}"
                )
            superuser_conn.rollback()
    finally:
        run_command("DROP SERVER IF EXISTS test_um_hint_srv CASCADE", superuser_conn)
        superuser_conn.commit()


# ── USER MAPPING happy paths (acceptance + per-user storage) ────────────────


def test_create_user_mapping_for_current_user_with_all_options(
    superuser_conn, extension
):
    """USER MAPPING FOR CURRENT_USER must accept the full credential
    bundle (client_id, client_secret, scope) in a single DDL.  Other
    tests exercise each option in isolation or only against PUBLIC;
    this is the explicit guard for the common "wire up my own
    credentials" path with a real umuser != InvalidOid."""
    run_command(
        """
        CREATE SERVER test_um_full TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_full
            OPTIONS (
                client_id 'my-id',
                client_secret 'my-secret',
                scope 'PRINCIPAL_ROLE:ADMIN'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_per_role_user_mappings_on_same_server(superuser_conn, extension):
    """Two distinct roles can each hold their own credentials and
    scope on the same iceberg_catalog server.  This is the headline
    reason credentials live on USER MAPPING and not on SERVER -- the
    SERVER is shared infrastructure, the credentials are per-identity.

    Asserts on pg_user_mapping rather than running a real REST call,
    because the runtime resolution path is exercised separately under
    test_modify_iceberg_rest_table; here the contract is just "DDL
    accepts two independent mappings, and the umoptions land
    correctly per umuser"."""
    ANALYST = "test_um_analyst"
    ADMIN = "test_um_admin"

    # CREATE ROLE outside the transaction guarded by `try` so cleanup
    # is robust even if any of the DDL below errors.
    run_command(f"CREATE ROLE {ANALYST} LOGIN", superuser_conn)
    run_command(f"CREATE ROLE {ADMIN} LOGIN", superuser_conn)
    run_command(
        """
        CREATE SERVER test_per_role_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        run_command(
            f"""
            CREATE USER MAPPING FOR {ANALYST} SERVER test_per_role_srv
                OPTIONS (
                    client_id 'a-id', client_secret 'a-secret',
                    scope 'PRINCIPAL_ROLE:READER'
                )
            """,
            superuser_conn,
        )
        run_command(
            f"""
            CREATE USER MAPPING FOR {ADMIN} SERVER test_per_role_srv
                OPTIONS (
                    client_id 'b-id', client_secret 'b-secret',
                    scope 'PRINCIPAL_ROLE:ALL'
                )
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"""
            SELECT u.rolname,
                   array_to_string(um.umoptions, ', ') AS opts
            FROM pg_user_mapping um
            JOIN pg_roles u ON u.oid = um.umuser
            WHERE um.umserver = (
                SELECT oid FROM pg_foreign_server
                WHERE srvname = 'test_per_role_srv'
            )
              AND u.rolname IN ('{ANALYST}', '{ADMIN}')
            """,
            superuser_conn,
        )
        assert len(rows) == 2, f"expected 2 mappings on test_per_role_srv, got {rows}"

        admin_opts = next(r["opts"] for r in rows if r["rolname"] == ADMIN)
        analyst_opts = next(r["opts"] for r in rows if r["rolname"] == ANALYST)

        # The two mappings must NOT cross-contaminate -- each role's
        # credentials and scope are stored verbatim and independently.
        assert "client_id=b-id" in admin_opts
        assert "client_secret=b-secret" in admin_opts
        assert "scope=PRINCIPAL_ROLE:ALL" in admin_opts
        assert "a-id" not in admin_opts
        assert "READER" not in admin_opts

        assert "client_id=a-id" in analyst_opts
        assert "client_secret=a-secret" in analyst_opts
        assert "scope=PRINCIPAL_ROLE:READER" in analyst_opts
        assert "b-id" not in analyst_opts
        assert "ALL" not in analyst_opts
    finally:
        # CASCADE on the server unhooks both user mappings in one
        # statement; the roles then drop cleanly.  IF EXISTS keeps
        # this robust even if the body bailed early.
        run_command("DROP SERVER IF EXISTS test_per_role_srv CASCADE", superuser_conn)
        run_command(f"DROP ROLE IF EXISTS {ANALYST}", superuser_conn)
        run_command(f"DROP ROLE IF EXISTS {ADMIN}", superuser_conn)
        superuser_conn.commit()


# ── USER MAPPING blocked on built-in iceberg_catalog servers ────────────────


BUILTIN_LONG_NAMES_FOR_UM = [
    "pg_lake_rest_catalog",
    "pg_lake_postgres_catalog",
    "pg_lake_object_store_catalog",
]


@pytest.mark.parametrize("long_name", BUILTIN_LONG_NAMES_FOR_UM)
def test_reject_create_user_mapping_on_builtin_server(
    long_name, superuser_conn, extension
):
    """CREATE USER MAPPING on the three internal long names is blocked:
    pg_lake_rest_catalog never reads user mappings (GUCs only), and the
    postgres / object_store built-ins have no notion of OAuth at all.
    Blocked uniformly at DDL time rather than silently ignored."""
    err = run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {long_name}
            OPTIONS (client_id 'x', client_secret 'y')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    msg = str(err)
    assert "cannot create user mapping for the built-in" in msg
    assert long_name in msg
    superuser_conn.rollback()


@pytest.mark.parametrize("long_name", BUILTIN_LONG_NAMES_FOR_UM)
def test_reject_alter_user_mapping_on_builtin_server(
    long_name, superuser_conn, extension
):
    """ALTER USER MAPPING on the built-in long names is blocked.
    There can never be a mapping to alter (CREATE is blocked too), but
    we trap ALTER at the parser-level guard so the user sees a clean
    pg_lake error rather than the generic "user mapping not found"."""
    err = run_command(
        f"""
        ALTER USER MAPPING FOR PUBLIC SERVER {long_name}
            OPTIONS (ADD client_id 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    msg = str(err)
    assert "cannot alter user mapping for the built-in" in msg
    assert long_name in msg
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


# ── ALTER SERVER endpoint-redirection guard ────────────────────────────────


@pytest.mark.parametrize(
    "alter_clause,leaked_word",
    [
        ("SET rest_endpoint 'http://attacker.example/'", "rest_endpoint"),
        (
            "ADD oauth_endpoint 'http://attacker.example/token'",
            "oauth_endpoint",
        ),
    ],
)
def test_alter_server_endpoint_blocked_with_user_mapping(
    superuser_conn, extension, alter_clause, leaked_word
):
    """rest_endpoint and oauth_endpoint must not be changeable while
    a USER MAPPING is attached to the server (PUBLIC counts)."""
    run_command(
        """
        CREATE SERVER test_endpoint_um TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR PUBLIC SERVER test_endpoint_um
            OPTIONS (client_id 'sentinel-id',
                     client_secret 'sentinel-secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        err = run_command(
            f"ALTER SERVER test_endpoint_um OPTIONS ({alter_clause})",
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert err is not None, (
            f"ALTER SERVER ({alter_clause}) must be rejected while a "
            "user mapping is attached to the server"
        )
        msg = str(err)
        assert leaked_word in msg, msg
        assert "user mappings" in msg, msg
        assert "redirect catalog credentials" in msg, msg
        # Stored secrets must not echo back into the error.
        assert "sentinel-secret" not in msg, msg
    finally:
        run_command("DROP SERVER IF EXISTS test_endpoint_um CASCADE", superuser_conn)
        superuser_conn.commit()


def test_alter_server_endpoints_allowed_without_dependents(superuser_conn, extension):
    """Without any user mapping or dependent iceberg table, both
    rest_endpoint and oauth_endpoint can be freely changed."""
    run_command(
        """
        CREATE SERVER test_endpoint_no_dep TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        run_command(
            "ALTER SERVER test_endpoint_no_dep "
            "OPTIONS (SET rest_endpoint 'http://other:8181')",
            superuser_conn,
        )
        run_command(
            "ALTER SERVER test_endpoint_no_dep "
            "OPTIONS (ADD oauth_endpoint 'http://other:8181/token')",
            superuser_conn,
        )
        run_command(
            "ALTER SERVER test_endpoint_no_dep " "OPTIONS (DROP oauth_endpoint)",
            superuser_conn,
        )
        superuser_conn.commit()
    finally:
        run_command(
            "DROP SERVER IF EXISTS test_endpoint_no_dep CASCADE",
            superuser_conn,
        )
        superuser_conn.commit()


def test_alter_server_endpoint_redirect_repro_blocked(superuser_conn, extension):
    """Two-role end-to-end form: server owner attempts to redirect
    oauth_endpoint on a server another role has a USER MAPPING on.
    The ALTER must be rejected at DDL time."""
    ADAM = "test_endpoint_adam"
    SARAH = "test_endpoint_sarah"

    run_command(f"CREATE ROLE {ADAM} LOGIN", superuser_conn)
    run_command(f"CREATE ROLE {SARAH} LOGIN", superuser_conn)
    run_command(f"GRANT lake_write TO {ADAM}, {SARAH}", superuser_conn)
    superuser_conn.commit()

    try:
        run_command(
            f"""
            SET ROLE {ADAM};
            CREATE SERVER test_endpoint_repro TYPE 'rest'
                FOREIGN DATA WRAPPER iceberg_catalog
                OPTIONS (rest_endpoint 'http://polaris.example/');
            GRANT USAGE ON FOREIGN SERVER test_endpoint_repro TO {SARAH};
            RESET ROLE;
            """,
            superuser_conn,
        )
        run_command(
            f"""
            SET ROLE {SARAH};
            CREATE USER MAPPING FOR {SARAH} SERVER test_endpoint_repro
                OPTIONS (client_id 'sarah-real-id',
                         client_secret 'sarah-real-secret');
            RESET ROLE;
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        err = run_command(
            f"""
            SET ROLE {ADAM};
            ALTER SERVER test_endpoint_repro
                OPTIONS (ADD oauth_endpoint 'http://attacker.example/token');
            RESET ROLE;
            """,
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert err is not None, (
            "ALTER SERVER ADD oauth_endpoint by the server owner must be "
            "rejected while another role's USER MAPPING is attached"
        )
        msg = str(err)
        assert "oauth_endpoint" in msg, msg
        assert "user mappings" in msg, msg
        assert "redirect catalog credentials" in msg, msg
        # Stored secrets must not echo back into the error.
        assert "sarah-real-secret" not in msg, msg
        assert "sarah-real-id" not in msg, msg
    finally:
        run_command("DROP SERVER IF EXISTS test_endpoint_repro CASCADE", superuser_conn)
        run_command(f"DROP ROLE IF EXISTS {ADAM}", superuser_conn)
        run_command(f"DROP ROLE IF EXISTS {SARAH}", superuser_conn)
        superuser_conn.commit()


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
        CREATE USER MAPPING FOR PUBLIC SERVER test_srv_catalog
            OPTIONS (client_id 'id', client_secret 'secret')
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


def test_user_server_does_not_inherit_guc_credentials(
    superuser_conn, s3, extension, with_default_location
):
    """A user-created iceberg_catalog server must not silently fall
    back to the system-wide rest_catalog_client_id /
    rest_catalog_client_secret GUCs.  Those GUCs feed only the
    built-in 'rest' catalog -- on a user server they would let any
    role with USAGE on the FDW (lake_write) point rest_endpoint /
    oauth_endpoint at a third-party URL and harvest the production
    credentials.

    This test pins three properties of the resolver:

      1. CREATE TABLE through a user server with no USER MAPPING
         fails up front with "no credentials found", proving the
         resolver did not inherit the GUC credentials.
      2. The hint points the user at USER MAPPING and explicitly
         calls out that GUC credentials don't apply to user servers
         -- so honest config mistakes don't hide behind GUC fallback.
      3. The seeded GUC client_id / client_secret values must not
         leak into the error message or hint, even when the validator
         is the path raising the error.
    """
    SECURITY_ID = "gucPrincipalId_FOREVER_SECRET"
    SECURITY_SECRET = "gucPrincipalSecret_FOREVER_SECRET"

    run_command(
        f"SET pg_lake_iceberg.rest_catalog_client_id TO '{SECURITY_ID}'",
        superuser_conn,
    )
    run_command(
        f"SET pg_lake_iceberg.rest_catalog_client_secret TO '{SECURITY_SECRET}'",
        superuser_conn,
    )
    run_command(
        """
        CREATE SERVER test_user_srv_no_um TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://attacker.example.invalid/',
                     oauth_endpoint 'http://attacker.example.invalid/token')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        err = run_command(
            """
            CREATE TABLE test_user_srv_no_um_tbl ()
                USING iceberg
                WITH (catalog = 'test_user_srv_no_um', read_only = 'true',
                      catalog_namespace = 'ns', catalog_table_name = 'tbl')
            """,
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert err is not None
        msg = str(err)
        assert (
            "no credentials found" in msg
        ), f"validator should reject up front, got: {msg}"
        # Hint must point at USER MAPPING and disavow GUC fallback.
        assert (
            "USER MAPPING" in msg
        ), f"hint should direct user to USER MAPPING, got: {msg}"
        assert (
            "GUC credentials are restricted" in msg
        ), f"hint should disavow GUC fallback for user servers, got: {msg}"
        # The seeded GUC credentials must not have leaked anywhere
        # in the error surface.
        assert SECURITY_ID not in msg, f"GUC client_id leaked into error: {msg}"
        assert SECURITY_SECRET not in msg, f"GUC client_secret leaked into error: {msg}"
    finally:
        run_command("DROP SERVER IF EXISTS test_user_srv_no_um CASCADE", superuser_conn)
        run_command("RESET pg_lake_iceberg.rest_catalog_client_id", superuser_conn)
        run_command("RESET pg_lake_iceberg.rest_catalog_client_secret", superuser_conn)
        superuser_conn.commit()


def test_user_server_with_partial_user_mapping_errors(
    superuser_conn, s3, extension, with_default_location
):
    """A USER MAPPING that exists on a user server but omits one of
    client_id / client_secret must be rejected by the validator.
    Without the resolver's credential-gating rule, the missing field
    would silently inherit from the system-wide GUC and the request
    would proceed -- defeating the per-user credential boundary that
    USER MAPPING is supposed to enforce.

    This test seeds GUC credentials AND creates a USER MAPPING with
    only client_id.  The expected outcome is "no credentials found"
    (client_secret is missing) with the user-mapping hint, and the
    seeded GUC client_secret must not have crept in to satisfy the
    validator.
    """
    GUC_SECRET = "gucSecret_must_not_fill_in_for_user_server"

    run_command(
        f"SET pg_lake_iceberg.rest_catalog_client_secret TO '{GUC_SECRET}'",
        superuser_conn,
    )
    run_command(
        """
        CREATE SERVER test_user_srv_partial_um TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR PUBLIC SERVER test_user_srv_partial_um
            OPTIONS (client_id 'mapping_only_has_id')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        err = run_command(
            """
            CREATE TABLE test_user_srv_partial_um_tbl ()
                USING iceberg
                WITH (catalog = 'test_user_srv_partial_um', read_only = 'true',
                      catalog_namespace = 'ns', catalog_table_name = 'tbl')
            """,
            superuser_conn,
            raise_error=False,
        )
        superuser_conn.rollback()
        assert err is not None
        msg = str(err)
        assert (
            "no credentials found" in msg
        ), f"partial user mapping should error: {msg}"
        assert (
            GUC_SECRET not in msg
        ), f"GUC client_secret leaked despite user-server gating: {msg}"
    finally:
        run_command(
            "DROP SERVER IF EXISTS test_user_srv_partial_um CASCADE",
            superuser_conn,
        )
        run_command("RESET pg_lake_iceberg.rest_catalog_client_secret", superuser_conn)
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


# ── User-mapping secret redaction in queryString ────────────────────────────
#
# The ProcessUtility hook RedactRestCatalogUserMappingSecrets scrubs
# client_id / client_secret out of the queryString in CREATE/ALTER
# USER MAPPING on iceberg_catalog servers, before any downstream
# observer (pg_stat_statements, ereport error context,
# log_min_duration_statement) reads it.  These tests pin that
# behaviour: secrets must never round-trip back via pg_stat_statements
# in plaintext.


SECRET_VALUE = "topsecretvalue"
ANOTHER_SECRET = "anothertopsecretvalue"

# Must match the marker stamped by RedactWholeStatement() in
# rest_catalog.c.  Kept identical here so the tests can find the
# redacted row in pg_stat_statements -- the rest of the statement
# (CREATE/ALTER keyword, server name, OPTIONS list) is wiped, so the
# marker is the only stable anchor.
REDACTION_MARKER = "<redacted: USER MAPPING with credentials>"


def _latest_user_mapping_stmt_text(superuser_conn, like_pattern):
    """Return the most recent pg_stat_statements row whose query matches
    `like_pattern`.  Sorting by total_exec_time is sufficient because
    each test issues exactly one matching utility statement after a
    fresh _reset()."""
    rows = run_query(
        f"""
        SELECT query
        FROM pg_stat_statements
        WHERE query ILIKE '{like_pattern}'
        ORDER BY total_exec_time DESC
        LIMIT 1
        """,
        superuser_conn,
    )
    assert len(rows) == 1, (
        f"expected exactly one pg_stat_statements row for "
        f"{like_pattern!r}, got: {rows}"
    )
    return rows[0]["query"]


def _reset_pg_stat_statements(superuser_conn):
    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()


def test_redact_create_user_mapping_client_secret(
    installcheck, superuser_conn, extension
):
    """Plain CREATE USER MAPPING with client_secret 'topsecretvalue'
    must NOT leave the literal secret in pg_stat_statements.  The
    redactor wipes the entire statement slice with REDACTION_MARKER,
    so neither the secret nor the client_id literal can survive."""
    if installcheck:
        return

    run_command(
        """
        CREATE SERVER test_redact_create TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        _reset_pg_stat_statements(superuser_conn)
        run_command(
            f"""
            CREATE USER MAPPING FOR PUBLIC SERVER test_redact_create
                OPTIONS (client_id 'someid', client_secret '{SECRET_VALUE}')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        stored = _latest_user_mapping_stmt_text(superuser_conn, f"%{REDACTION_MARKER}%")
        assert (
            REDACTION_MARKER in stored
        ), f"expected redaction marker to be present: {stored}"
        assert (
            SECRET_VALUE not in stored
        ), f"plaintext client_secret leaked to pg_stat_statements: {stored}"
        assert (
            "someid" not in stored
        ), f"plaintext client_id leaked to pg_stat_statements: {stored}"
    finally:
        run_command("DROP SERVER IF EXISTS test_redact_create CASCADE", superuser_conn)
        superuser_conn.commit()


def test_redact_alter_user_mapping_client_secret(
    installcheck, superuser_conn, extension
):
    """ALTER USER MAPPING ... OPTIONS (SET client_secret '<secret>')
    must also be redacted.  SET / ADD / DROP all flow through the same
    AlterUserMappingStmt parsetree, so this exercises a different
    DefElem defaction than CREATE."""
    if installcheck:
        return

    run_command(
        """
        CREATE SERVER test_redact_alter TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR PUBLIC SERVER test_redact_alter
            OPTIONS (client_id 'initial_id', client_secret 'initial_secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        _reset_pg_stat_statements(superuser_conn)
        run_command(
            f"""
            ALTER USER MAPPING FOR PUBLIC SERVER test_redact_alter
                OPTIONS (SET client_secret '{ANOTHER_SECRET}')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        stored = _latest_user_mapping_stmt_text(superuser_conn, f"%{REDACTION_MARKER}%")
        assert (
            REDACTION_MARKER in stored
        ), f"expected redaction marker to be present: {stored}"
        assert (
            ANOTHER_SECRET not in stored
        ), f"plaintext client_secret leaked via ALTER: {stored}"
    finally:
        run_command("DROP SERVER IF EXISTS test_redact_alter CASCADE", superuser_conn)
        superuser_conn.commit()


def test_redact_handles_doubled_quote_escape(installcheck, superuser_conn, extension):
    """A secret containing an embedded single quote written via the
    standard '' escape (e.g. 'foo''bar') is the most common non-trivial
    string form for credentials.  Coarse statement-level erasure has
    no string-literal parser, so it does not need to "consume" the
    doubled quote -- it just stamps the marker over the whole
    statement slice.  This test pins that no byte of the value
    survives, regardless of the literal's internal quoting."""
    if installcheck:
        return

    run_command(
        """
        CREATE SERVER test_redact_dq TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        _reset_pg_stat_statements(superuser_conn)
        # SQL string 'foo''bar' represents the value foo'bar.
        run_command(
            """
            CREATE USER MAPPING FOR PUBLIC SERVER test_redact_dq
                OPTIONS (client_secret 'foo''bar')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        stored = _latest_user_mapping_stmt_text(superuser_conn, f"%{REDACTION_MARKER}%")
        assert (
            REDACTION_MARKER in stored
        ), f"expected redaction marker to be present: {stored}"
        assert (
            "foo" not in stored
        ), f"prefix 'foo' leaked through doubled-quote escape: {stored}"
        assert (
            "bar" not in stored
        ), f"suffix 'bar' leaked through doubled-quote escape: {stored}"
    finally:
        run_command("DROP SERVER IF EXISTS test_redact_dq CASCADE", superuser_conn)
        superuser_conn.commit()


def test_redact_handles_e_string_escape(installcheck, superuser_conn, extension):
    """E'pref\\'redactedsuffix' is an extended-quote string with a
    backslash escape inside.  A hand-rolled literal parser would have
    to consume \\X as a two-byte escape so that \\' is not mistaken
    for a closing quote.  Coarse statement-level erasure sidesteps
    that entirely -- the marker stamps the whole statement, so no
    byte of the literal can survive regardless of the escape form."""
    if installcheck:
        return

    run_command(
        """
        CREATE SERVER test_redact_e TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        _reset_pg_stat_statements(superuser_conn)
        run_command(
            r"""
            CREATE USER MAPPING FOR PUBLIC SERVER test_redact_e
                OPTIONS (client_secret E'pref\'redactedsuffix')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        stored = _latest_user_mapping_stmt_text(superuser_conn, f"%{REDACTION_MARKER}%")
        assert (
            REDACTION_MARKER in stored
        ), f"expected redaction marker to be present: {stored}"
        assert "pref" not in stored, f"prefix leaked from E-string escape: {stored}"
        assert (
            "redactedsuffix" not in stored
        ), f"suffix leaked from E-string escape: {stored}"
    finally:
        run_command("DROP SERVER IF EXISTS test_redact_e CASCADE", superuser_conn)
        superuser_conn.commit()


def test_redact_skips_scope_only_alter_user_mapping(
    installcheck, superuser_conn, extension
):
    """`scope` is configuration, not a secret.  The redactor gates on
    "this statement carries a credential option", so an ALTER USER
    MAPPING that only touches `scope` is left fully visible in
    pg_stat_statements -- operators can still grep their statement
    history for non-secret configuration changes.

    When client_id / client_secret are present in the same statement
    (the credential-changing case), the whole statement is redacted,
    including any scope value that happens to ride along.  That
    coarseness is the trade-off for not having a hand-rolled
    string-literal parser; see RedactWholeStatement() in
    rest_catalog.c."""
    if installcheck:
        return

    run_command(
        """
        CREATE SERVER test_redact_scope TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER test_redact_scope
            OPTIONS (client_secret '{SECRET_VALUE}')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        _reset_pg_stat_statements(superuser_conn)
        run_command(
            """
            ALTER USER MAPPING FOR PUBLIC SERVER test_redact_scope
                OPTIONS (ADD scope 'PRINCIPAL_ROLE:VISIBLE')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        # The gate (HasRedactableUserMappingSecret) returns false for
        # a scope-only OPTIONS list, so the original statement text
        # should be intact and the marker absent.
        stored = _latest_user_mapping_stmt_text(
            superuser_conn,
            "%ALTER USER MAPPING%test_redact_scope%",
        )
        assert (
            REDACTION_MARKER not in stored
        ), f"scope-only ALTER was wrongly redacted: {stored}"
        assert (
            "PRINCIPAL_ROLE:VISIBLE" in stored
        ), f"non-secret scope value was wrongly redacted: {stored}"
    finally:
        run_command("DROP SERVER IF EXISTS test_redact_scope CASCADE", superuser_conn)
        superuser_conn.commit()


def test_redact_skips_non_iceberg_catalog_server(
    installcheck, superuser_conn, extension
):
    """A USER MAPPING on a non-iceberg_catalog FDW (postgres_fdw here)
    must NOT be redacted.  The redactor scopes itself by FDW so it
    never touches statements unrelated to pg_lake_iceberg."""
    if installcheck:
        return

    run_command("CREATE EXTENSION IF NOT EXISTS postgres_fdw", superuser_conn)
    run_command(
        """
        CREATE SERVER test_redact_other_fdw
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host 'localhost', dbname 'postgres')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        _reset_pg_stat_statements(superuser_conn)
        # postgres_fdw accepts 'user' and 'password' on user mappings;
        # neither is in our redaction list, and the FDW gate should
        # short-circuit us out before we inspect option names anyway.
        run_command(
            f"""
            CREATE USER MAPPING FOR PUBLIC SERVER test_redact_other_fdw
                OPTIONS (user 'fdw_user', password '{SECRET_VALUE}')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        stored = _latest_user_mapping_stmt_text(
            superuser_conn, "%CREATE USER MAPPING%test_redact_other_fdw%"
        )
        # If the FDW gate were wrong we would scrub by option name
        # match alone; since postgres_fdw uses different option names
        # (user/password) AND the server is non-iceberg, the literal
        # secret must survive verbatim.
        assert (
            SECRET_VALUE in stored
        ), f"redactor incorrectly fired on non-iceberg_catalog server: {stored}"
    finally:
        run_command(
            "DROP SERVER IF EXISTS test_redact_other_fdw CASCADE", superuser_conn
        )
        superuser_conn.commit()


@pytest.mark.parametrize("long_name", BUILTIN_LONG_NAMES_FOR_UM)
def test_redact_runs_before_builtin_rejection(
    long_name, installcheck, superuser_conn, extension
):
    """CREATE USER MAPPING on a built-in catalog server is rejected by
    ValidateIcebergCatalogServerDDL.  The redaction handler is
    registered AFTER the validator so it runs FIRST (handlers are
    prepend-LIFO).  Therefore the error context surfaced via ereport
    -- and any pg_stat_statements row recorded for the failed
    statement -- must already carry the redacted form.

    We use the DETAIL-style error string: run_command(raise_error=False)
    captures the libpq exception whose .pgerror surfaces the
    PostgreSQL error message (which does NOT include queryString by
    default), so we mainly assert the secret never landed in
    pg_stat_statements.  Many builds skip pgss recording for failed
    utility statements; in that case the redaction is still observable
    because the error message and any log_statement entry would have
    seen the scrubbed buffer.  Either way the secret must not leak."""
    if installcheck:
        return

    _reset_pg_stat_statements(superuser_conn)

    err = run_command(
        f"""
        CREATE USER MAPPING FOR PUBLIC SERVER {long_name}
            OPTIONS (client_secret '{SECRET_VALUE}')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    superuser_conn.rollback()

    # The validator must have rejected it.
    assert "cannot create user mapping for the built-in" in str(err)
    # Whatever did get captured anywhere must NOT have the secret.
    assert SECRET_VALUE not in str(err)

    # After redaction the statement reads "<redacted: USER MAPPING
    # with credentials>" -- a LIKE %CREATE USER MAPPING%<long_name>%
    # would not match it and the loop below would be vacuously empty.
    # Scan ALL pg_stat_statements rows instead so a regression that
    # let the cleartext through would actually be caught.
    rows = run_query("SELECT query FROM pg_stat_statements", superuser_conn)
    for r in rows:
        assert SECRET_VALUE not in r["query"], (
            f"failed CREATE USER MAPPING leaked secret to pg_stat_statements: "
            f"{r['query']}"
        )


def test_redact_preserves_stored_credentials(superuser_conn, extension):
    """Redaction scrubs queryString *in place* so observers
    (pg_stat_statements, ereport context, log_min_duration_statement)
    see the marker instead of the plaintext.  The DDL itself, however,
    reads option values from DefElem->arg -- a separate buffer
    untouched by the redactor -- so pg_user_mapping must still carry
    the real plaintext.

    Without this guard, a future refactor that copied the redacted
    queryString back into DefElem nodes would silently corrupt every
    stored credential.  Test #3 in the design spec pins exactly this
    invariant."""
    run_command(
        """
        CREATE SERVER test_redact_storage TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    try:
        run_command(
            """
            CREATE USER MAPPING FOR PUBLIC SERVER test_redact_storage
                OPTIONS (client_id 'real_id_xyz', client_secret 'real_secret_abc')
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            """
            SELECT umoptions
              FROM pg_user_mapping um
              JOIN pg_foreign_server fs ON um.umserver = fs.oid
             WHERE fs.srvname = 'test_redact_storage'
            """,
            superuser_conn,
        )
        assert len(rows) == 1, (
            f"expected exactly one user mapping for test_redact_storage, "
            f"got: {rows}"
        )
        # umoptions is text[] of "key=value" entries; psycopg2 returns it
        # as a Python list of strings.
        opts = rows[0]["umoptions"]
        joined = ",".join(opts)
        assert "client_id=real_id_xyz" in joined, (
            f"client_id was not stored verbatim (queryString redaction must "
            f"not touch DefElem->arg): umoptions={opts}"
        )
        assert "client_secret=real_secret_abc" in joined, (
            f"client_secret was not stored verbatim (queryString redaction "
            f"must not touch DefElem->arg): umoptions={opts}"
        )
    finally:
        run_command("DROP SERVER IF EXISTS test_redact_storage CASCADE", superuser_conn)
        superuser_conn.commit()

"""Vended-credential tests against a real Iceberg REST catalog (Apache Polaris).

These complement ``test_vended_credentials_enforced.py`` rather than
duplicating it.

That suite proves a vended credential is genuinely *load-bearing* -- an
enforcing S3 mock denies the scan without it -- against a catalog that is
itself a mock.  These tests use a real catalog instead, where a table is
registered by Polaris itself and attached read-only afterwards, which is
the shape a production REST catalog actually serves.

Storage here is the shared moto fixture, which accepts any credential and
never denies a request.  So these tests assert the *resolver's* observable
behavior -- that the correctly scoped secret is pushed to pgduck_server
when the catalog vends one, and that it is not pushed when it should not
be -- rather than that access fails without it.  Between the two suites:

    Enforced -> the credential is load-bearing
    Polaris  -> the credential is resolved against a real catalog, and
                withheld from writable tables

Vending is restricted to read-only tables, so the writable case is
covered here as a *negative*: pg_lake owns a writable table's files and
must be able to delete them long after any vended credential has expired,
which is a lifecycle this feature does not yet answer.
"""

import json
from pathlib import Path

from utils_pytest import *
from helpers.polaris import *


VENDED_SECRET_PREFIX = "pglake_vended_"


def _set_vending(conn, enabled):
    """Enable/disable vending on a single backend.

    The GUC is PGC_SUSET, so a session-level SET on a superuser connection
    takes effect immediately for the next statement -- unlike ALTER SYSTEM
    + pg_reload_conf(), whose SIGHUP lands at an unpredictable command
    boundary.
    """
    value = "true" if enabled else "false"
    run_command(
        f"SET pg_lake_iceberg.rest_catalog_enable_vended_credentials = {value}",
        conn,
    )
    conn.commit()


def _vended_secrets_for(pgduck_conn, schema, table):
    """Vended secrets whose scope points at this test's table.

    Filtering by the table's own path keeps the assertions immune to
    secrets other tests left behind on the shared pgduck_server.
    """
    rows = run_query(
        f"""
        SELECT name, scope
          FROM duckdb_secrets()
         WHERE name LIKE '{VENDED_SECRET_PREFIX}%'
        """,
        pgduck_conn,
    )
    pgduck_conn.commit()

    needle = f"/{schema}/{table}/"
    return [r for r in rows if r[1] and needle in r[1]]


def _drop_vended_secrets_for(pgduck_conn, schema, table):
    """Remove this table's vended secrets from the shared pgduck_server.

    Secrets are process-global, so one pushed by an earlier backend stays
    visible afterwards.  Clearing them lets a later assertion attribute a
    secret to the read path alone.
    """
    for name, _scope in _vended_secrets_for(pgduck_conn, schema, table):
        run_command(f'DROP SECRET IF EXISTS "{name}"', pgduck_conn)
    pgduck_conn.commit()


def test_polaris_read_only_table_is_vended_on_a_cold_cache(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """A read-only REST table resolves and pushes a vended credential.

    The read happens on a *different* backend than the one that attached
    the table, so the credential cannot have been warmed by the DDL.  A
    cold cache is the case most easily missed.
    """
    if installcheck:
        return

    schema = "vc_polaris_ro"
    table = "rest_tbl"
    attached = "attached_ro"

    _set_vending(superuser_conn, True)

    reader = None
    try:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"CREATE SCHEMA {schema}", superuser_conn)
        superuser_conn.commit()

        # Register the table in Polaris and give it files.  Vending is on,
        # but this is the writable side, which is not served.
        run_command(
            f"""CREATE TABLE {schema}.{table} USING iceberg
                WITH (catalog='rest', autovacuum_enabled=False)
                AS SELECT g AS id FROM generate_series(1, 10) g""",
            superuser_conn,
        )
        superuser_conn.commit()

        # Attach the same catalog table read-only: the shape a REST catalog
        # serves to a reader that does not own the table.
        run_command(
            f"""CREATE TABLE {schema}.{attached}() USING iceberg
                WITH (catalog='rest', read_only=True,
                      catalog_namespace='{schema}',
                      catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        # Secrets outlive the backend that pushed them, so clear anything
        # already present: a secret found after the scan below can then only
        # have come from the read path.
        _drop_vended_secrets_for(pgduck_conn, schema, table)
        assert not _vended_secrets_for(pgduck_conn, schema, table)

        # Fresh backend => cold credential cache.
        reader = open_pg_conn()
        _set_vending(reader, True)

        rows = run_query(f"SELECT count(*) FROM {schema}.{attached}", reader)
        reader.commit()
        assert rows[0][0] == 10

        secrets = _vended_secrets_for(pgduck_conn, schema, table)
        assert secrets, (
            "expected the read path to resolve and push a vended secret on a "
            "cold cache, found none"
        )

    finally:
        if reader is not None:
            reader.close()
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _set_vending(superuser_conn, False)


def test_polaris_writable_table_is_not_vended(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """A writable REST table gets no vended credential, even with vending on.

    pg_lake owns a writable table's files, and owning them means deleting
    them: a DROP only queues its files, and the queue holds them for
    ``orphaned_file_retention_period`` (10 days by default) -- long after
    any vended credential has expired and the table has left the catalog
    that could vend another.  Until that has an answer, writable tables
    reach storage exactly as they do without vending.
    """
    if installcheck:
        return

    schema = "vc_polaris_rw"
    table = "writable_rest"

    _set_vending(superuser_conn, True)

    try:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"CREATE SCHEMA {schema}", superuser_conn)
        superuser_conn.commit()

        run_command(
            f"""CREATE TABLE {schema}.{table} USING iceberg
                WITH (catalog='rest', autovacuum_enabled=False)
                AS SELECT g AS id FROM generate_series(1, 10) g""",
            superuser_conn,
        )
        superuser_conn.commit()

        # Every path a writable table has: write, read, and delete.
        run_command(f"INSERT INTO {schema}.{table} SELECT 11", superuser_conn)
        superuser_conn.commit()

        rows = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()
        assert rows[0][0] == 11

        run_command(f"TRUNCATE {schema}.{table}", superuser_conn)
        superuser_conn.commit()

        secrets = _vended_secrets_for(pgduck_conn, schema, table)
        assert not secrets, (
            f"vending is restricted to read-only tables, but a writable one "
            f"was pushed a secret: {secrets}"
        )

    finally:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _set_vending(superuser_conn, False)


def test_polaris_no_vended_secret_when_disabled(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """With vending off, nothing is requested and no secret is pushed.

    Guards the opt-in default: the delegation header must not be sent and
    no vended secret may appear just because a REST table was scanned.
    """
    if installcheck:
        return

    schema = "vc_polaris_off"
    table = "writable_rest"

    _set_vending(superuser_conn, False)

    reader = None
    try:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"CREATE SCHEMA {schema}", superuser_conn)
        superuser_conn.commit()

        run_command(
            f"""CREATE TABLE {schema}.{table} USING iceberg
                WITH (catalog='rest', autovacuum_enabled=False)
                AS SELECT g AS id FROM generate_series(1, 5) g""",
            superuser_conn,
        )
        superuser_conn.commit()

        reader = open_pg_conn()
        _set_vending(reader, False)

        rows = run_query(f"SELECT count(*) FROM {schema}.{table}", reader)
        reader.commit()
        assert rows[0][0] == 5

        secrets = _vended_secrets_for(pgduck_conn, schema, table)
        assert not secrets, f"expected no vended secret with vending off, got {secrets}"

    finally:
        if reader is not None:
            reader.close()
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()


def test_polaris_server_option_enables_vending_with_guc_off(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """A server that opts in vends even with the GUC left off.

    `enable_vended_credentials` is a per-server option that overrides the
    GUC, and the GUC now defaults to off.  Deciding from the GUC alone
    would therefore ignore a server that explicitly asked for vending and
    silently vend nothing.
    """
    if installcheck:
        return

    schema = "vc_polaris_srvopt"
    table = "opt_in_rest"
    server = "vc_polaris_serveropt_srv"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    # rest_endpoint wants a scheme, unlike the rest_catalog_host GUC.
    endpoint = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"

    # Only the server opts in; the GUC stays off.
    _set_vending(superuser_conn, False)

    try:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"DROP SERVER IF EXISTS {server} CASCADE", superuser_conn)
        run_command(f"CREATE SCHEMA {schema}", superuser_conn)
        run_command(
            f"""CREATE SERVER {server} TYPE 'rest'
                    FOREIGN DATA WRAPPER iceberg_catalog
                    OPTIONS (rest_endpoint '{endpoint}',
                             enable_vended_credentials 'true',
                             location_prefix 's3://{TEST_BUCKET}')""",
            superuser_conn,
        )
        run_command(
            f"""CREATE USER MAPPING FOR PUBLIC SERVER {server}
                    OPTIONS (client_id '{client_id}',
                             client_secret '{client_secret}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(
            f"""CREATE TABLE {schema}.{table} USING iceberg
                WITH (catalog='{server}', autovacuum_enabled=False)
                AS SELECT g AS id FROM generate_series(1, 5) g""",
            superuser_conn,
        )
        superuser_conn.commit()

        # Vending serves read-only tables, so read through an attachment.
        run_command(
            f"""CREATE TABLE {schema}.attached_ro() USING iceberg
                WITH (catalog='{server}', read_only=True,
                      catalog_namespace='{schema}',
                      catalog_table_name='{table}')""",
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(f"SELECT count(*) FROM {schema}.attached_ro", superuser_conn)
        superuser_conn.commit()
        assert rows[0][0] == 5

        secrets = _vended_secrets_for(pgduck_conn, schema, table)
        assert secrets, (
            "the server set enable_vended_credentials but no secret was "
            "pushed, so the per-server opt-in is being ignored"
        )

    finally:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        run_command(f"DROP SERVER IF EXISTS {server} CASCADE", superuser_conn)
        superuser_conn.commit()

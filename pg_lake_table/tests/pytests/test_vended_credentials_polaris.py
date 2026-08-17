"""Vended-credential tests against a real Iceberg REST catalog (Apache Polaris).

These complement the MinIO suite rather than duplicating it.

The MinIO suite proves a vended credential is genuinely *load-bearing* --
a real policy engine denies the scan without it -- but it can only do so
for read-only tables, because its catalog is a mock.  For a *writable*
REST table the catalog itself materializes the new ``metadata.json`` from
the update list pg_lake sends at commit, so a mock HTTP server cannot
serve the next ``loadTable``.  Writable tables therefore need a real
catalog, which is what these tests use.

Storage here is moto, which accepts any credential and never denies a
request.  So these tests assert the *resolver's* observable behavior --
that the correctly scoped secret is pushed to pgduck_server when the
catalog vends one, and dropped again when it should be -- rather than
that access fails without it.  Between the two suites:

    MinIO   -> the credential is load-bearing (read-only tables)
    Polaris -> the credential is resolved, pushed and dropped on every
               table shape, including writable ones

The writable case matters most: a writable REST table is flagged as an
*internal* Iceberg table, so its read path never issues a ``loadTable``
and nothing warms the credential cache for it.  Only resolving on demand
at the scan choke point gets it a credential, which is what these tests
pin down.
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


def _scope_key_prefix(scope, bucket):
    """The bucket-relative prefix a secret's scope points at.

    duckdb_secrets() renders scope as a list, so strip the decoration
    before taking the path apart.
    """
    first = scope.strip("[]{}").split(",")[0].strip(" '\"")
    return first.split(f"{bucket}/", 1)[1]


def _drop_vended_secrets_for(pgduck_conn, schema, table):
    """Remove this table's vended secrets from the shared pgduck_server.

    Secrets are process-global, so a secret pushed by the *writing*
    backend stays visible afterwards.  Clearing them lets a later
    assertion attribute a secret to the read path alone.
    """
    for name, _scope in _vended_secrets_for(pgduck_conn, schema, table):
        run_command(f'DROP SECRET IF EXISTS "{name}"', pgduck_conn)
    pgduck_conn.commit()


def test_polaris_vended_credentials_writable_table(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """A writable REST table resolves and pushes a vended credential.

    The read happens on a *different* backend than the one that created
    the table, so the credential cannot have been warmed by CREATE TABLE.
    A cold cache on a writable table is the case most easily missed.
    """
    if installcheck:
        return

    schema = "vc_polaris_rw"
    table = "writable_rest"

    _set_vending(superuser_conn, True)

    reader = None
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

        # Exercises the write path (AddQueryResultToTable) with vending on.
        run_command(f"INSERT INTO {schema}.{table} SELECT 11", superuser_conn)
        superuser_conn.commit()

        # The writes above already pushed a secret, and secrets outlive the
        # backend that pushed them.  Clear them so that anything found after
        # the scan below can only have come from the read path.
        _drop_vended_secrets_for(pgduck_conn, schema, table)
        assert not _vended_secrets_for(pgduck_conn, schema, table)

        # Fresh backend => cold credential cache.
        reader = open_pg_conn()
        _set_vending(reader, True)

        rows = run_query(f"SELECT count(*) FROM {schema}.{table}", reader)
        reader.commit()
        assert rows[0][0] == 11

        secrets = _vended_secrets_for(pgduck_conn, schema, table)
        assert secrets, (
            "expected the read path to resolve and push a vended secret for "
            "the writable table on a cold cache, found none"
        )

    finally:
        if reader is not None:
            reader.close()
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

        rows = run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
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


def test_polaris_dropped_table_keeps_its_secret_until_files_are_deleted(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """A dropped table's secret survives long enough to delete its files.

    DROP TABLE only queues the table's files; the deletes run later, in
    another transaction, against a relation that no longer exists and so
    can no longer be resolved for credentials.  Dropping the secret at
    DROP time is therefore what strands the data on vended-only storage.

    moto accepts any credential, so this cannot show the delete failing
    without the secret.  What it does pin is the ordering -- the secret
    is still there when the drain runs -- and that the drain empties the
    table's prefix rather than leaving it behind.
    """
    if installcheck:
        return

    schema = "vc_polaris_drop"
    table = "writable_rest"

    _set_vending(superuser_conn, True)

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

        run_query(f"SELECT count(*) FROM {schema}.{table}", superuser_conn)
        superuser_conn.commit()

        secrets = _vended_secrets_for(pgduck_conn, schema, table)
        assert secrets, "expected a vended secret to be pushed before the drop"

        # The secret's scope is the table's storage prefix, which is also
        # what the queued deletes have to reach.
        key_prefix = _scope_key_prefix(secrets[0][1], TEST_BUCKET)
        assert list_objects(
            s3, TEST_BUCKET, key_prefix
        ), f"expected the table to have files under {key_prefix}"

        run_command(f"DROP TABLE {schema}.{table}", superuser_conn)
        superuser_conn.commit()

        assert _vended_secrets_for(pgduck_conn, schema, table), (
            "the vended secret was dropped with the table, so the deletes "
            "queued by the drop have no credentials left to run under"
        )

        # 0 drains every table, which is the only option here: the relation
        # the rows belonged to is gone.
        run_command("SELECT lake_engine.flush_deletion_queue(0)", superuser_conn)
        superuser_conn.commit()

        # Superseded metadata.json files are not reachable from the current
        # metadata, so the drop's enumeration never sees them and the drain
        # leaves them behind.  That gap is pre-existing and has nothing to do
        # with credentials; the data and manifests are what this asserts on.
        leftover = [
            key
            for key in list_objects(s3, TEST_BUCKET, key_prefix)
            if not key.endswith(".metadata.json")
        ]
        assert not leftover, f"drop left files behind under {key_prefix}: {leftover}"

    finally:
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _set_vending(superuser_conn, False)

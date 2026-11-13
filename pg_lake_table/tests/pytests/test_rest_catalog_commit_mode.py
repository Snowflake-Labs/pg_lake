import io
import threading
import time

import psycopg2
import pytest
from utils_pytest import *
from helpers.polaris import *

SCHEMA = "test_rest_catalog_commit_mode"
WAIT_POINT = "after-apply-iceberg-changes"


@pytest.fixture(scope="module")
def commit_mode_schema(pg_conn, s3, set_polaris_gucs, installcheck):
    if installcheck:
        yield
        return

    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    run_command(f"CREATE TABLE {SCHEMA}.heap (a int)", pg_conn)
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command("RESET pg_lake_iceberg.default_rest_catalog_commit_mode", pg_conn)
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def catalog_count(table_name, pg_conn):
    """Count the rows the REST catalog has for the table, read through a
    read-only table attached to it."""
    reader = f"{table_name}_reader"
    run_command(
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{reader} () USING iceberg
            WITH (catalog = 'rest', read_only = true, catalog_table_name = '{table_name}')""",
        pg_conn,
    )
    pg_conn.commit()
    count = run_query(f"SELECT count(*) FROM {SCHEMA}.{reader}", pg_conn)[0][0]
    pg_conn.commit()
    return count


def test_catalog_commit(
    pg_conn, commit_mode_schema, with_default_location, installcheck
):
    if installcheck:
        return

    run_command(
        "SET pg_lake_iceberg.default_rest_catalog_commit_mode TO 'catalog'", pg_conn
    )

    # create and load in one transaction, then modify in later ones
    run_command(
        f"""CREATE TABLE {SCHEMA}.basic USING iceberg WITH (catalog = 'rest')
            AS SELECT s AS a FROM generate_series(1, 10) s""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA}.basic SELECT s FROM generate_series(11, 20) s", pg_conn
    )
    pg_conn.commit()

    run_command(f"DELETE FROM {SCHEMA}.basic WHERE a <= 5", pg_conn)
    pg_conn.commit()

    run_command(f"ALTER TABLE {SCHEMA}.basic ADD COLUMN b text", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.basic VALUES (21, 'x')", pg_conn)
    pg_conn.commit()

    assert run_query(f"SELECT count(*) FROM {SCHEMA}.basic", pg_conn)[0][0] == 16
    assert catalog_count("basic", pg_conn) == 16

    run_command("RESET pg_lake_iceberg.default_rest_catalog_commit_mode", pg_conn)
    pg_conn.commit()


def test_catalog_commit_rejects_postgres_writes(
    pg_conn, commit_mode_schema, with_default_location, installcheck
):
    if installcheck:
        return

    run_command(
        f"""CREATE TABLE {SCHEMA}.mixed USING iceberg WITH (catalog = 'rest')
            AS SELECT 1 AS a""",
        pg_conn,
    )
    run_command(f"CREATE TABLE {SCHEMA}.pg_iceberg (a int) USING iceberg", pg_conn)
    pg_conn.commit()

    # post_commit mode allows mixing the two
    run_command(f"INSERT INTO {SCHEMA}.heap VALUES (1)", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (2)", pg_conn)
    pg_conn.commit()

    run_command(
        "SET pg_lake_iceberg.default_rest_catalog_commit_mode TO 'catalog'", pg_conn
    )
    pg_conn.commit()

    # a PostgreSQL table first
    run_command(f"INSERT INTO {SCHEMA}.heap VALUES (3)", pg_conn)
    error = run_command(
        f"INSERT INTO {SCHEMA}.mixed VALUES (4)", pg_conn, raise_error=False
    )
    assert (
        'cannot modify REST catalog table "mixed" after writing to PostgreSQL table "heap"'
        in error
    )
    pg_conn.rollback()

    # a REST catalog table first
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (5)", pg_conn)
    error = run_command(
        f"UPDATE {SCHEMA}.heap SET a = a + 1", pg_conn, raise_error=False
    )
    assert 'cannot write to PostgreSQL table "heap"' in error
    pg_conn.rollback()

    # COPY does not go through the executor
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (6)", pg_conn)
    with pytest.raises(psycopg2.errors.FeatureNotSupported, match='"heap"'):
        pg_conn.cursor().copy_expert(
            f"COPY {SCHEMA}.heap FROM STDIN", io.StringIO("7\n")
        )
    pg_conn.rollback()

    # Iceberg tables in the PostgreSQL catalog also commit through PostgreSQL
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (8)", pg_conn)
    error = run_command(
        f"INSERT INTO {SCHEMA}.pg_iceberg VALUES (9)", pg_conn, raise_error=False
    )
    assert 'cannot write to PostgreSQL table "pg_iceberg"' in error
    pg_conn.rollback()

    # temporary tables are fine, and so is work in a rolled back savepoint
    run_command(f"CREATE TEMP TABLE scratch AS SELECT 10 AS a", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.mixed SELECT a FROM scratch", pg_conn)
    run_command("SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (11)", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT s1", pg_conn)
    pg_conn.commit()

    run_command("SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (12)", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.heap VALUES (13)", pg_conn)
    pg_conn.commit()

    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.mixed", pg_conn)[0][
        0
    ] == [1, 2, 10]
    assert catalog_count("mixed", pg_conn) == 3
    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.heap", pg_conn)[0][
        0
    ] == [1, 13]

    run_command("RESET pg_lake_iceberg.default_rest_catalog_commit_mode", pg_conn)
    pg_conn.commit()


def test_catalog_commit_conflict(
    pg_conn,
    superuser_conn,
    polaris_session,
    commit_mode_schema,
    with_default_location,
    create_injection_extension,
    installcheck,
):
    """A commit that another writer beat to the catalog aborts the transaction
    instead of overwriting the other writer's change."""
    if installcheck or get_pg_version_num(pg_conn) < 170000:
        return

    run_command(
        f"""CREATE TABLE {SCHEMA}.conflict USING iceberg WITH (catalog = 'rest')
            AS SELECT 1::bigint AS a""",
        pg_conn,
    )
    pg_conn.commit()
    run_command(f"INSERT INTO {SCHEMA}.conflict VALUES (2)", pg_conn)
    pg_conn.commit()

    other_engine = create_iceberg_rest_catalog(SCHEMA)
    first_snapshot_id = min(
        other_engine.load_table(f"{SCHEMA}.conflict").snapshots(),
        key=lambda snapshot: snapshot.sequence_number,
    ).snapshot_id

    errors = []

    def insert_in_catalog_mode():
        conn = open_pg_conn()
        try:
            run_command(
                "SET pg_lake_iceberg.default_rest_catalog_commit_mode TO 'catalog'",
                conn,
            )
            run_command("SELECT public.injection_points_set_local()", conn)
            run_command(
                f"SELECT public.injection_points_attach('{WAIT_POINT}', 'wait')", conn
            )
            conn.commit()

            run_command(f"INSERT INTO {SCHEMA}.conflict VALUES (3)", conn)
            conn.commit()
        except psycopg2.Error as error:
            errors.append(error)
        finally:
            conn.close()

    writer = threading.Thread(target=insert_in_catalog_mode, daemon=True)
    writer.start()

    deadline = time.time() + 30
    while True:
        superuser_conn.rollback()
        waiting = run_query(
            f"""SELECT count(*) FROM pg_stat_activity
                WHERE wait_event_type = 'InjectionPoint' AND wait_event = '{WAIT_POINT}'""",
            superuser_conn,
        )[0][0]
        if waiting > 0:
            break
        assert time.time() < deadline, "commit did not reach the injection point"
        time.sleep(0.05)

    # Another engine rolls the table back to its first snapshot while the
    # transaction is paused. A rollback adds no snapshot, so only the
    # requirements tell the catalog that the paused commit is stale.
    url = (
        f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        f"/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{SCHEMA}/tables/conflict"
    )
    response = polaris_session.post(
        url,
        json={
            "requirements": [],
            "updates": [
                {
                    "action": "set-snapshot-ref",
                    "type": "branch",
                    "ref-name": "main",
                    "snapshot-id": first_snapshot_id,
                }
            ],
        },
    )
    assert response.ok, response.text

    run_command(
        f"SELECT public.injection_points_wakeup('{WAIT_POINT}')", superuser_conn
    )
    superuser_conn.commit()
    writer.join(timeout=60)

    assert not writer.is_alive()
    assert len(errors) == 1
    assert "main" in str(errors[0])

    # neither PostgreSQL nor the catalog has the rejected row
    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.conflict", pg_conn)[
        0
    ][0] == [1, 2]
    pg_conn.commit()

    rows = other_engine.load_table(f"{SCHEMA}.conflict").scan().to_arrow()
    assert rows.column("a").to_pylist() == [1]


def test_commit_mode_options(
    pg_conn, superuser_conn, commit_mode_schema, with_default_location, installcheck
):
    """commit_mode on the table wins over commit_mode on its server, which
    wins over pg_lake_iceberg.default_rest_catalog_commit_mode."""
    if installcheck:
        return

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}/api/catalog"
    server = "commit_mode_srv"

    error = run_command(
        f"""CREATE SERVER bad_commit_mode_srv TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}', commit_mode 'eventually')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'invalid commit_mode option: "eventually"' in error
    superuser_conn.rollback()

    run_command(
        f"""CREATE SERVER {server} TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}', location_prefix 's3://{TEST_BUCKET}/',
                     commit_mode 'catalog')""",
        superuser_conn,
    )
    run_command(
        f"""CREATE USER MAPPING FOR PUBLIC SERVER {server}
            OPTIONS (client_id '{creds["credentials"]["clientId"]}',
                     client_secret '{creds["credentials"]["clientSecret"]}')""",
        superuser_conn,
    )
    run_command(f"GRANT USAGE ON FOREIGN SERVER {server} TO PUBLIC", superuser_conn)
    superuser_conn.commit()

    try:
        run_command(
            f"""CREATE TABLE {SCHEMA}.from_server (a int) USING iceberg
                WITH (catalog = '{server}')""",
            pg_conn,
        )
        pg_conn.commit()
        run_command(
            f"""CREATE TABLE {SCHEMA}.table_post_commit (a int) USING iceberg
                WITH (catalog = '{server}', commit_mode = 'post_commit')""",
            pg_conn,
        )
        pg_conn.commit()
        run_command(
            f"""CREATE TABLE {SCHEMA}.table_catalog (a int) USING iceberg
                WITH (catalog = 'rest', commit_mode = 'catalog')""",
            pg_conn,
        )
        pg_conn.commit()

        # the server's commit_mode applies
        run_command(f"INSERT INTO {SCHEMA}.from_server VALUES (1)", pg_conn)
        error = run_command(
            f"INSERT INTO {SCHEMA}.heap VALUES (1)", pg_conn, raise_error=False
        )
        assert 'cannot write to PostgreSQL table "heap"' in error
        pg_conn.rollback()

        # the table's commit_mode overrides the server's
        run_command(f"INSERT INTO {SCHEMA}.heap VALUES (2)", pg_conn)
        run_command(f"INSERT INTO {SCHEMA}.table_post_commit VALUES (2)", pg_conn)
        pg_conn.commit()

        # and the default
        run_command(f"INSERT INTO {SCHEMA}.heap VALUES (3)", pg_conn)
        error = run_command(
            f"INSERT INTO {SCHEMA}.table_catalog VALUES (3)", pg_conn, raise_error=False
        )
        assert (
            'cannot modify REST catalog table "table_catalog" after writing to PostgreSQL table "heap"'
            in error
        )
        pg_conn.rollback()

        # all tables in a transaction need the same commit mode
        run_command(f"INSERT INTO {SCHEMA}.from_server VALUES (4)", pg_conn)
        error = run_command(
            f"INSERT INTO {SCHEMA}.table_post_commit VALUES (4)",
            pg_conn,
            raise_error=False,
        )
        assert "cannot modify REST catalog tables with different commit modes" in error
        pg_conn.rollback()

        # the table option can be changed
        run_command(
            f"ALTER TABLE {SCHEMA}.table_catalog OPTIONS (SET commit_mode 'post_commit')",
            pg_conn,
        )
        pg_conn.commit()
        run_command(f"INSERT INTO {SCHEMA}.heap VALUES (5)", pg_conn)
        run_command(f"INSERT INTO {SCHEMA}.table_catalog VALUES (5)", pg_conn)
        pg_conn.commit()

        assert run_query(
            f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.table_post_commit", pg_conn
        )[0][0] == [2]
        assert run_query(
            f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.table_catalog", pg_conn
        )[0][0] == [5]

        # the option is only valid on writable REST catalog tables, with a known value
        error = run_command(
            f"""CREATE TABLE {SCHEMA}.not_rest (a int) USING iceberg
                WITH (commit_mode = 'catalog')""",
            pg_conn,
            raise_error=False,
        )
        assert (
            '"commit_mode" option is only valid for writable rest catalog tables'
            in error
        )
        pg_conn.rollback()

        error = run_command(
            f"ALTER TABLE {SCHEMA}.from_server OPTIONS (ADD commit_mode 'later')",
            pg_conn,
            raise_error=False,
        )
        assert 'invalid commit_mode option: "later"' in error
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command(f"DROP TABLE IF EXISTS {SCHEMA}.from_server", pg_conn)
        run_command(f"DROP TABLE IF EXISTS {SCHEMA}.table_post_commit", pg_conn)
        pg_conn.commit()
        run_command(f"DROP SERVER {server} CASCADE", superuser_conn)
        superuser_conn.commit()

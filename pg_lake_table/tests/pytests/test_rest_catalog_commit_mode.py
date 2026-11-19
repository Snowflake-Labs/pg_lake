import io
import threading
import time

import psycopg2
import pyarrow
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


def catalog_rows(table_name):
    """The values of column a the REST catalog has for the table, read by
    another engine."""
    other_engine = create_iceberg_rest_catalog(SCHEMA)
    rows = other_engine.load_table(f"{SCHEMA}.{table_name}").scan().to_arrow()
    return sorted(rows.column("a").to_pylist())


def postgres_file_count(table_name, superuser_conn):
    """The number of data files PostgreSQL keeps for the table."""
    count = run_query(
        f"""SELECT count(*) FROM lake_table.files
            WHERE table_name = '{SCHEMA}.{table_name}'::regclass""",
        superuser_conn,
    )[0][0]
    superuser_conn.commit()
    return count


def test_catalog_commit(
    pg_conn, superuser_conn, commit_mode_schema, with_default_location, installcheck
):
    """A table in catalog commit mode keeps its data files only in the
    catalog."""
    if installcheck:
        return

    # create and load in one transaction, then modify in later ones
    run_command(
        f"""CREATE TABLE {SCHEMA}.basic USING iceberg
            WITH (catalog = 'rest', commit_mode = 'catalog')
            AS SELECT s::bigint AS a FROM generate_series(1, 10) s""",
        pg_conn,
    )
    pg_conn.commit()

    # the transaction sees its own inserts before they reach the catalog
    run_command(
        f"INSERT INTO {SCHEMA}.basic SELECT s FROM generate_series(11, 20) s", pg_conn
    )
    assert run_query(f"SELECT count(*) FROM {SCHEMA}.basic", pg_conn)[0][0] == 20
    assert (
        run_query(f"SELECT count(*) FROM {SCHEMA}.basic WHERE a > 15", pg_conn)[0][0]
        == 5
    )
    pg_conn.commit()

    # a rolled back savepoint drops its inserts
    run_command("SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.basic VALUES (100)", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.basic VALUES (21)", pg_conn)
    pg_conn.commit()

    run_command(f"ALTER TABLE {SCHEMA}.basic ADD COLUMN b text", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.basic VALUES (22, 'x')", pg_conn)
    pg_conn.commit()

    assert catalog_rows("basic") == list(range(1, 23))
    assert run_query(f"SELECT count(*), max(b) FROM {SCHEMA}.basic", pg_conn)[0] == [
        22,
        "x",
    ]
    assert (
        run_query(f"SELECT lake_iceberg.table_size('{SCHEMA}.basic')", pg_conn)[0][0]
        > 0
    )
    pg_conn.commit()

    # TRUNCATE and INSERT in one transaction replace the catalog's files
    run_command(f"TRUNCATE {SCHEMA}.basic", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.basic VALUES (30, 'y')", pg_conn)
    assert run_query(f"SELECT array_agg(a) FROM {SCHEMA}.basic", pg_conn)[0][0] == [30]
    pg_conn.commit()
    assert catalog_rows("basic") == [30]

    # another engine's commits show up, since reads go to the catalog
    other_engine = create_iceberg_rest_catalog(SCHEMA)
    other_table = other_engine.load_table(f"{SCHEMA}.basic")
    other_table.append(
        pyarrow.Table.from_pylist(
            [{"a": 31, "b": "z"}], schema=other_table.schema().as_arrow()
        )
    )
    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.basic", pg_conn)[0][
        0
    ] == [30, 31]
    pg_conn.commit()

    # and PostgreSQL never kept any of the table's files
    assert postgres_file_count("basic", superuser_conn) == 0

    # UPDATE and DELETE need the data files catalog
    for statement in [
        f"DELETE FROM {SCHEMA}.basic WHERE a = 30",
        f"UPDATE {SCHEMA}.basic SET b = 'w'",
    ]:
        error = run_command(statement, pg_conn, raise_error=False)
        assert (
            "is not supported for table \"basic\" with commit_mode 'catalog'" in error
        )
        pg_conn.rollback()


def test_catalog_commit_rejects_postgres_writes(
    pg_conn, commit_mode_schema, with_default_location, installcheck
):
    if installcheck:
        return

    run_command(
        f"""CREATE TABLE {SCHEMA}.mixed USING iceberg
            WITH (catalog = 'rest', commit_mode = 'catalog')
            AS SELECT 1 AS a""",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"""CREATE TABLE {SCHEMA}.mixed_post_commit USING iceberg
            WITH (catalog = 'rest') AS SELECT 1 AS a""",
        pg_conn,
    )
    pg_conn.commit()
    run_command(f"CREATE TABLE {SCHEMA}.pg_iceberg (a int) USING iceberg", pg_conn)
    pg_conn.commit()

    # post_commit mode allows mixing the two
    run_command(f"INSERT INTO {SCHEMA}.heap VALUES (1)", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.mixed_post_commit VALUES (2)", pg_conn)
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
    pg_conn.cursor().copy_expert(f"COPY {SCHEMA}.mixed FROM STDIN", io.StringIO("11\n"))
    pg_conn.commit()

    run_command("SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.mixed VALUES (12)", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT s1", pg_conn)
    run_command(f"INSERT INTO {SCHEMA}.heap VALUES (13)", pg_conn)
    pg_conn.commit()

    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.mixed", pg_conn)[0][
        0
    ] == [1, 10, 11]
    assert catalog_rows("mixed") == [1, 10, 11]
    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.heap", pg_conn)[0][
        0
    ] == [1, 13]
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
        f"""CREATE TABLE {SCHEMA}.conflict USING iceberg
            WITH (catalog = 'rest', commit_mode = 'catalog')
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

    # the rejected row is nowhere, and PostgreSQL reads the catalog's state
    assert catalog_rows("conflict") == [1]
    assert run_query(f"SELECT array_agg(a ORDER BY a) FROM {SCHEMA}.conflict", pg_conn)[
        0
    ][0] == [1]
    pg_conn.commit()


def test_commit_mode_options(
    pg_conn, superuser_conn, commit_mode_schema, with_default_location, installcheck
):
    """A new table takes commit_mode from its own option, then from its server,
    then from pg_lake_iceberg.default_rest_catalog_commit_mode, and keeps it."""
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

    def commit_mode_of(table_name):
        option = run_query(
            f"""SELECT option_value FROM pg_options_to_table(
                    (SELECT ftoptions FROM pg_foreign_table
                     WHERE ftrelid = '{SCHEMA}.{table_name}'::regclass))
                WHERE option_name = 'commit_mode'""",
            pg_conn,
        )
        pg_conn.commit()
        return option[0][0] if option else None

    try:
        for table_name, options in [
            ("from_server", f"catalog = '{server}'"),
            ("table_post_commit", f"catalog = '{server}', commit_mode = 'post_commit'"),
            ("default_post_commit", "catalog = 'rest'"),
        ]:
            run_command(
                f"CREATE TABLE {SCHEMA}.{table_name} (a int) USING iceberg WITH ({options})",
                pg_conn,
            )
            pg_conn.commit()

        run_command(
            "SET pg_lake_iceberg.default_rest_catalog_commit_mode TO 'catalog'", pg_conn
        )
        run_command(
            f"CREATE TABLE {SCHEMA}.from_default (a int) USING iceberg WITH (catalog = 'rest')",
            pg_conn,
        )
        pg_conn.commit()
        run_command("RESET pg_lake_iceberg.default_rest_catalog_commit_mode", pg_conn)
        pg_conn.commit()

        assert commit_mode_of("from_server") == "catalog"
        assert commit_mode_of("table_post_commit") == "post_commit"
        assert commit_mode_of("default_post_commit") is None
        assert commit_mode_of("from_default") == "catalog"

        # the mode was fixed at creation
        run_command(
            "SET pg_lake_iceberg.default_rest_catalog_commit_mode TO 'catalog'", pg_conn
        )
        run_command(f"INSERT INTO {SCHEMA}.heap VALUES (1)", pg_conn)
        run_command(f"INSERT INTO {SCHEMA}.default_post_commit VALUES (1)", pg_conn)
        pg_conn.commit()
        run_command("RESET pg_lake_iceberg.default_rest_catalog_commit_mode", pg_conn)
        pg_conn.commit()

        run_command(f"INSERT INTO {SCHEMA}.from_default VALUES (2)", pg_conn)
        error = run_command(
            f"INSERT INTO {SCHEMA}.heap VALUES (2)", pg_conn, raise_error=False
        )
        assert 'cannot write to PostgreSQL table "heap"' in error
        pg_conn.rollback()

        # all tables in a transaction need the same commit mode
        run_command(f"INSERT INTO {SCHEMA}.from_server VALUES (3)", pg_conn)
        error = run_command(
            f"INSERT INTO {SCHEMA}.table_post_commit VALUES (3)",
            pg_conn,
            raise_error=False,
        )
        assert "cannot modify REST catalog tables with different commit modes" in error
        pg_conn.rollback()

        for table_name, value in [
            ("from_server", "post_commit"),
            ("default_post_commit", "catalog"),
        ]:
            error = run_command(
                (
                    f"ALTER TABLE {SCHEMA}.{table_name} OPTIONS (SET commit_mode '{value}')"
                    if commit_mode_of(table_name)
                    else f"ALTER TABLE {SCHEMA}.{table_name} OPTIONS (ADD commit_mode '{value}')"
                ),
                pg_conn,
                raise_error=False,
            )
            assert 'Changing option "commit_mode" is not supported' in error
            pg_conn.rollback()

        # the option is only valid on writable REST catalog tables
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
            f"""CREATE TABLE {SCHEMA}.with_row_ids (a int) USING iceberg
                WITH (catalog = 'rest', commit_mode = 'catalog', row_ids = true)""",
            pg_conn,
            raise_error=False,
        )
        assert '"row_ids" option is not supported' in error
        pg_conn.rollback()
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_iceberg.default_rest_catalog_commit_mode", pg_conn)
        run_command(f"DROP TABLE IF EXISTS {SCHEMA}.from_server", pg_conn)
        run_command(f"DROP TABLE IF EXISTS {SCHEMA}.table_post_commit", pg_conn)
        pg_conn.commit()
        run_command(f"DROP SERVER {server} CASCADE", superuser_conn)
        superuser_conn.commit()


def test_catalog_commit_pruning(
    pg_conn, commit_mode_schema, with_default_location, installcheck
):
    """Filters prune both the catalog's files and the transaction's own."""
    if installcheck:
        return

    run_command(
        f"""CREATE TABLE {SCHEMA}.pruned (a bigint, b text) USING iceberg
            WITH (catalog = 'rest', commit_mode = 'catalog', partition_by = 'b')""",
        pg_conn,
    )
    pg_conn.commit()
    for value in ["x", "y"]:
        run_command(
            f"INSERT INTO {SCHEMA}.pruned SELECT s, '{value}' FROM generate_series(1, 10) s",
            pg_conn,
        )
        pg_conn.commit()

    run_command(
        f"INSERT INTO {SCHEMA}.pruned SELECT s, 'z' FROM generate_series(1, 10) s",
        pg_conn,
    )
    for value in ["x", "y", "z"]:
        assert run_query(
            f"SELECT count(*), sum(a) FROM {SCHEMA}.pruned WHERE b = '{value}'", pg_conn
        )[0] == [10, 55]

    plan = "\n".join(
        row[0]
        for row in run_query(
            f"EXPLAIN (VERBOSE) SELECT * FROM {SCHEMA}.pruned WHERE b = 'y' AND a > 5",
            pg_conn,
        )
    )
    assert "Data Files Scanned: 1" in plan, plan
    pg_conn.commit()

    assert run_query(f"SELECT count(*) FROM {SCHEMA}.pruned", pg_conn)[0][0] == 30
    pg_conn.commit()

"""
Tests for external=true REST catalog tables: the REST catalog is the
authority for the table, and pg_lake keeps only a pointer to it, like a
read_only table. pyiceberg plays the other engine that commits to the
same table.
"""

import io
import threading
import time

import psycopg2
import pyarrow
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from utils_pytest import *
from helpers.polaris import *


NAMESPACE = "external_writable"


@pytest.fixture(scope="module")
def rest_catalog(installcheck, pg_conn, s3, polaris_session):
    if installcheck:
        yield None
        return

    catalog = create_iceberg_rest_catalog(NAMESPACE)
    run_command(f'CREATE SCHEMA IF NOT EXISTS "{NAMESPACE}"', pg_conn)
    pg_conn.commit()

    yield catalog

    pg_conn.rollback()
    run_command(f'DROP SCHEMA IF EXISTS "{NAMESPACE}" CASCADE', pg_conn)
    pg_conn.commit()


PA_SCHEMA = pyarrow.schema(
    [
        pyarrow.field("id", pyarrow.int64(), nullable=False),
        pyarrow.field("val", pyarrow.string(), nullable=True),
    ]
)


def external_append(table, rows):
    """Commit rows to the table as another engine would."""
    table.refresh()
    table.append(
        pyarrow.Table.from_pylist(
            [{"id": id, "val": val} for id, val in rows], schema=PA_SCHEMA
        )
    )


@pytest.fixture
def shared_table(request, rest_catalog, pg_conn, set_polaris_gucs, installcheck):
    """A table another engine created, attached with external=true."""
    if installcheck:
        pytest.skip("needs the test Polaris server")

    table_name = request.node.name.split("[")[0][: len("test_") + 40]
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "val", StringType(), required=False),
    )
    table = rest_catalog.create_table(f"{NAMESPACE}.{table_name}", schema=schema)
    external_append(table, [(1, "spark-1"), (2, "spark-2")])

    run_command(
        f"""
        CREATE TABLE "{NAMESPACE}"."{table_name}" ()
        USING iceberg
        WITH (catalog='rest', external='true')
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield table, f'"{NAMESPACE}"."{table_name}"'

    pg_conn.rollback()
    run_command(f'DROP TABLE IF EXISTS "{NAMESPACE}"."{table_name}"', pg_conn)
    pg_conn.commit()
    rest_catalog.drop_table(f"{NAMESPACE}.{table_name}")


def test_external_option_requires_rest(pg_conn, with_default_location, installcheck):
    error = run_command(
        """CREATE TABLE external_not_rest (a int) USING iceberg WITH (external='true')""",
        pg_conn,
        raise_error=False,
    )
    assert '"external" option is only valid for catalog="rest"' in str(error)
    pg_conn.rollback()


def test_external_table_reads_catalog_snapshot(pg_conn, shared_table):
    table, pg_name = shared_table

    # only a pointer: no internal catalog rows for the table
    res = run_query(
        f"SELECT count(*) FROM lake_iceberg.tables WHERE table_namespace = '{NAMESPACE}'",
        pg_conn,
    )
    assert res == [[0]]
    res = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{pg_name}'::regclass",
        pg_conn,
    )
    assert res == [[0]]

    res = run_query(f"SELECT id, val FROM {pg_name} ORDER BY id", pg_conn)
    assert res == [[1, "spark-1"], [2, "spark-2"]]
    pg_conn.commit()

    # another engine commits, and our next read sees it
    external_append(table, [(3, "spark-3")])

    res = run_query(f"SELECT id, val FROM {pg_name} ORDER BY id", pg_conn)
    assert res == [[1, "spark-1"], [2, "spark-2"], [3, "spark-3"]]
    pg_conn.commit()


def catalog_rows(table):
    """The rows of the table as the catalog has them, read by pyiceberg."""
    table.refresh()
    return sorted(
        (row["id"], row["val"]) for row in table.scan().to_arrow().to_pylist()
    )


def test_insert_commits_to_the_catalog(pg_conn, shared_table):
    table, pg_name = shared_table
    base_snapshot = table.current_snapshot().snapshot_id

    conn = autocommit_conn()
    run_command(f"INSERT INTO {pg_name} VALUES (10, 'pg-10'), (11, 'pg-11')", conn)
    conn.close()

    # the catalog has a child of the snapshot we read
    table.refresh()
    assert table.current_snapshot().parent_snapshot_id == base_snapshot
    assert table.current_snapshot().summary.operation.value == "append"
    assert catalog_rows(table) == [
        (1, "spark-1"),
        (2, "spark-2"),
        (10, "pg-10"),
        (11, "pg-11"),
    ]

    # and we keep nothing about the table ourselves
    res = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{pg_name}'::regclass",
        pg_conn,
    )
    assert res == [[0]]

    res = run_query(f"SELECT id FROM {pg_name} ORDER BY id", pg_conn)
    assert res == [[1], [2], [10], [11]]
    pg_conn.commit()


def test_write_after_external_commit(pg_conn, shared_table):
    """
    Another engine commits between two of our writes. Our next write is a
    child of its snapshot and keeps its files.
    """
    table, pg_name = shared_table
    conn = autocommit_conn()

    run_command(f"INSERT INTO {pg_name} VALUES (10, 'pg-10')", conn)

    external_append(table, [(20, "spark-20")])
    external_snapshot = table.current_snapshot().snapshot_id

    run_command(f"INSERT INTO {pg_name} VALUES (11, 'pg-11')", conn)

    table.refresh()
    assert table.current_snapshot().parent_snapshot_id == external_snapshot
    assert catalog_rows(table) == [
        (1, "spark-1"),
        (2, "spark-2"),
        (10, "pg-10"),
        (11, "pg-11"),
        (20, "spark-20"),
    ]

    # deletes and updates also apply on top of the other engine's commits
    external_append(table, [(21, "spark-21")])

    run_command(f"DELETE FROM {pg_name} WHERE id IN (2, 20)", conn)
    table.refresh()
    assert table.current_snapshot().summary.operation.value in ("delete", "overwrite")

    run_command(f"UPDATE {pg_name} SET val = 'pg-upd' WHERE id = 21", conn)
    conn.close()

    expected = [
        (1, "spark-1"),
        (10, "pg-10"),
        (11, "pg-11"),
        (21, "pg-upd"),
    ]
    assert catalog_rows(table) == expected

    res = run_query(f"SELECT id, val FROM {pg_name} ORDER BY id", pg_conn)
    assert [tuple(row) for row in res] == expected
    pg_conn.commit()

    # the other engine can keep writing on top of ours
    external_append(table, [(30, "spark-30")])
    res = run_query(f"SELECT count(*) FROM {pg_name}", pg_conn)
    assert res == [[5]]
    pg_conn.commit()


def autocommit_conn():
    """A connection on which each statement is its own transaction, which
    psycopg2 otherwise wraps in a transaction block."""
    conn = open_pg_conn()
    conn.autocommit = True
    return conn


def wait_for_injection_point(superuser_conn, name, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        res = run_query(
            f"""SELECT count(*) FROM pg_stat_activity
                WHERE wait_event_type = 'InjectionPoint' AND wait_event = '{name}'""",
            superuser_conn,
        )
        superuser_conn.rollback()
        if res[0][0] > 0:
            return True
        time.sleep(0.05)
    return False


def test_commit_conflict_aborts(
    superuser_conn, shared_table, create_injection_extension
):
    """
    A commit that lands between our read and our commit makes the catalog
    reject ours, and the PostgreSQL transaction aborts. Retrying succeeds.
    """
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("injection_points 'wait' requires PG17+")

    table, pg_name = shared_table
    point = "before-external-rest-catalog-commit"

    run_command(f"SELECT injection_points_attach('{point}', 'wait')", superuser_conn)
    superuser_conn.commit()

    errors = []

    def insert():
        conn = autocommit_conn()
        try:
            run_command(f"INSERT INTO {pg_name} VALUES (40, 'pg-40')", conn)
        except psycopg2.Error as error:
            errors.append(error)
        finally:
            conn.close()

    writer = threading.Thread(target=insert, daemon=True)
    try:
        writer.start()
        assert wait_for_injection_point(superuser_conn, point)

        # the other engine commits while our commit is in flight
        external_append(table, [(41, "spark-41")])
    finally:
        run_command(f"SELECT injection_points_detach('{point}')", superuser_conn)
        run_command(f"SELECT injection_points_wakeup('{point}')", superuser_conn)
        superuser_conn.commit()
        writer.join(timeout=60)

    assert len(errors) == 1
    assert isinstance(errors[0], psycopg2.errors.SerializationFailure)
    assert "due to a concurrent change" in str(errors[0])

    # our row did not land, theirs did
    assert catalog_rows(table) == [(1, "spark-1"), (2, "spark-2"), (41, "spark-41")]

    # nothing of ours records the files of the rejected commit: like those
    # of any other Iceberg writer, they are left for orphan file cleanup
    res = run_query(
        f"""SELECT count(*) FROM lake_engine.in_progress_files
            WHERE path LIKE '{table.location()}/%'""",
        superuser_conn,
    )
    superuser_conn.rollback()
    assert res == [[0]]

    # a retry reads the new snapshot and commits on top of it
    conn = autocommit_conn()
    run_command(f"INSERT INTO {pg_name} VALUES (40, 'pg-40')", conn)
    conn.close()
    assert catalog_rows(table) == [
        (1, "spark-1"),
        (2, "spark-2"),
        (40, "pg-40"),
        (41, "spark-41"),
    ]


def test_error_after_catalog_commit_keeps_files(
    superuser_conn, shared_table, create_injection_extension
):
    """
    The catalog commits, then the PostgreSQL transaction fails. The REST
    table is the only copy of the table, so there is nothing to disagree
    with, and the files it references must not be cleaned up.
    """
    if get_pg_version_num(superuser_conn) < 170000:
        pytest.skip("injection_points requires PG17+")

    table, pg_name = shared_table
    point = "after-external-rest-catalog-commit"

    run_command(f"SELECT injection_points_attach('{point}', 'error')", superuser_conn)
    superuser_conn.commit()

    conn = autocommit_conn()
    try:
        error = run_command(
            f"INSERT INTO {pg_name} VALUES (50, 'pg-50')", conn, raise_error=False
        )
    finally:
        conn.close()
        run_command(f"SELECT injection_points_detach('{point}')", superuser_conn)
        superuser_conn.commit()

    assert "after-external-rest-catalog-commit" in str(error)

    # the catalog has the row, and so do we, since we read the catalog
    expected = [(1, "spark-1"), (2, "spark-2"), (50, "pg-50")]
    assert catalog_rows(table) == expected

    res = run_query(
        f"""SELECT count(*) FROM lake_engine.in_progress_files
            WHERE path LIKE '{table.location()}/%'""",
        superuser_conn,
    )
    superuser_conn.rollback()
    assert res == [[0]]

    conn = autocommit_conn()
    run_command("VACUUM", conn)
    res = run_query(f"SELECT id, val FROM {pg_name} ORDER BY id", conn)
    conn.close()
    assert [tuple(row) for row in res] == expected


def test_insert_select_and_copy(pg_conn, shared_table, tmp_path):
    table, pg_name = shared_table

    conn = autocommit_conn()

    # split output by size, so the file is written to a prefix first
    run_command("SET pg_lake_table.target_file_size_mb TO 1", conn)
    run_command(
        f"INSERT INTO {pg_name} SELECT s, 'gen-' || s FROM generate_series(100, 199) s",
        conn,
    )
    run_command("RESET pg_lake_table.target_file_size_mb", conn)

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("200,copy-200\n201,copy-201\n")
    cursor = conn.cursor()
    with open(csv_path) as csv_file:
        cursor.copy_expert(f"COPY {pg_name} FROM STDIN WITH (FORMAT csv)", csv_file)
    conn.close()

    rows = catalog_rows(table)
    assert len(rows) == 2 + 100 + 2
    assert (150, "gen-150") in rows and (201, "copy-201") in rows


def test_insert_select_pushdown(pg_conn, rest_catalog, shared_table, set_polaris_gucs):
    """INSERT..SELECT into an external table without NOT NULL columns runs in
    pgduck_server, and commits like any other write."""
    table, pg_name = shared_table
    target_name = f'"{NAMESPACE}"."pushdown_target"'

    conn = autocommit_conn()
    run_command(
        f"""CREATE TABLE {target_name} (id bigint, val text) USING iceberg
            WITH (catalog='rest', external='true')""",
        conn,
    )

    try:
        insert_select = f"INSERT INTO {target_name} SELECT id * 10, val FROM {pg_name}"
        plan = run_query(f"EXPLAIN {insert_select}", conn)
        assert "Custom Scan (Query Pushdown)" in str(plan)

        run_command("BEGIN", conn)
        run_command(insert_select, conn)
        run_command(insert_select, conn)
        assert run_query(f"SELECT count(*) FROM {target_name}", conn) == [[4]]
        run_command("COMMIT", conn)

        target = rest_catalog.load_table(f"{NAMESPACE}.pushdown_target")
        assert catalog_rows(target) == [
            (10, "spark-1"),
            (10, "spark-1"),
            (20, "spark-2"),
            (20, "spark-2"),
        ]
    finally:
        run_command(f"DROP TABLE IF EXISTS {target_name}", conn)
        conn.close()
        rest_catalog.drop_table(f"{NAMESPACE}.pushdown_target")


def test_writes_in_transaction_block(pg_conn, shared_table):
    """
    A transaction block sees its own changes, savepoints roll them back, and
    the commit sends them to the catalog as one snapshot.
    """
    table, pg_name = shared_table
    base_snapshot = table.current_snapshot().snapshot_id

    run_command(f"INSERT INTO {pg_name} VALUES (10, 'pg-10'), (11, 'pg-11')", pg_conn)
    run_command(f"INSERT INTO {pg_name} VALUES (12, 'pg-12')", pg_conn)
    res = run_query(f"SELECT id FROM {pg_name} ORDER BY id", pg_conn)
    assert res == [[1], [2], [10], [11], [12]]

    # from the other engine's file, and from files of this transaction
    run_command(f"DELETE FROM {pg_name} WHERE id IN (2, 11, 12)", pg_conn)
    run_command("SAVEPOINT s1", pg_conn)
    run_command(f"DELETE FROM {pg_name} WHERE id = 1", pg_conn)
    run_command(f"INSERT INTO {pg_name} VALUES (13, 'pg-13')", pg_conn)
    run_command("ROLLBACK TO SAVEPOINT s1", pg_conn)
    run_command(f"UPDATE {pg_name} SET val = 'pg-upd' WHERE id IN (1, 10)", pg_conn)
    res = run_query(f"SELECT id, val FROM {pg_name} ORDER BY id", pg_conn)
    assert [tuple(row) for row in res] == [(1, "pg-upd"), (10, "pg-upd")]

    # nothing reaches the catalog before the commit
    assert catalog_rows(table) == [(1, "spark-1"), (2, "spark-2")]
    pg_conn.commit()

    table.refresh()
    assert table.current_snapshot().parent_snapshot_id == base_snapshot
    assert catalog_rows(table) == [(1, "pg-upd"), (10, "pg-upd")]

    # a rolled back transaction leaves the catalog alone
    run_command(f"INSERT INTO {pg_name} VALUES (20, 'pg-20')", pg_conn)
    pg_conn.rollback()

    # TRUNCATE and INSERT in one transaction, also as one query string
    conn = autocommit_conn()
    run_command(f"TRUNCATE {pg_name}; INSERT INTO {pg_name} VALUES (30, 'pg-30')", conn)
    conn.close()
    assert catalog_rows(table) == [(30, "pg-30")]

    res = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{pg_name}'::regclass",
        pg_conn,
    )
    assert res == [[0]]
    pg_conn.commit()


def test_snapshot_per_transaction(pg_conn, shared_table):
    """
    A transaction reads one snapshot of the table, and cannot commit changes
    after another engine committed on top of it.
    """
    table, pg_name = shared_table

    assert run_query(f"SELECT count(*) FROM {pg_name}", pg_conn) == [[2]]
    external_append(table, [(20, "spark-20")])

    # still the snapshot the transaction read first, also in READ COMMITTED
    assert run_query(f"SELECT count(*) FROM {pg_name}", pg_conn) == [[2]]

    run_command(f"DELETE FROM {pg_name} WHERE id = 1", pg_conn)
    with pytest.raises(psycopg2.errors.SerializationFailure):
        pg_conn.commit()
    pg_conn.rollback()

    # running it again reads the new snapshot
    run_command(f"DELETE FROM {pg_name} WHERE id = 1", pg_conn)
    pg_conn.commit()
    assert catalog_rows(table) == [(2, "spark-2"), (20, "spark-20")]

    # a schema change adds no snapshot, but fails the commit as well
    assert run_query(f"SELECT count(*) FROM {pg_name}", pg_conn) == [[2]]
    with table.update_schema() as update:
        update.add_column("extra", StringType())
    run_command(f"INSERT INTO {pg_name} VALUES (40, 'pg-40')", pg_conn)
    with pytest.raises(psycopg2.errors.SerializationFailure):
        pg_conn.commit()
    pg_conn.rollback()
    assert catalog_rows(table) == [(2, "spark-2"), (20, "spark-20")]


def test_writes_with_postgres_tables_rejected(
    pg_conn, shared_table, with_default_location
):
    """
    The catalog commits the changes to external tables before PostgreSQL
    commits, so a transaction cannot also write tables that PostgreSQL
    commits.
    """
    table, pg_name = shared_table

    run_command(f'CREATE TABLE "{NAMESPACE}".heap (a int)', pg_conn)
    run_command(f'CREATE TABLE "{NAMESPACE}".internal (a int) USING iceberg', pg_conn)
    pg_conn.commit()

    try:
        # a PostgreSQL table first
        run_command(f'INSERT INTO "{NAMESPACE}".heap VALUES (1)', pg_conn)
        error = run_command(
            f"INSERT INTO {pg_name} VALUES (10, 'pg-10')", pg_conn, raise_error=False
        )
        assert 'after writing to table "heap"' in str(error)
        pg_conn.rollback()

        # an external table first, then a heap table, COPY or an internal table
        for statement in [
            f'INSERT INTO "{NAMESPACE}".heap VALUES (1)',
            f'INSERT INTO "{NAMESPACE}".internal VALUES (1)',
        ]:
            run_command(f"INSERT INTO {pg_name} VALUES (10, 'pg-10')", pg_conn)
            error = run_command(statement, pg_conn, raise_error=False)
            assert "in a transaction that modifies external REST catalog tables" in str(
                error
            ), statement
            pg_conn.rollback()

        run_command(f"INSERT INTO {pg_name} VALUES (10, 'pg-10')", pg_conn)
        with pytest.raises(psycopg2.errors.FeatureNotSupported, match='"heap"'):
            pg_conn.cursor().copy_expert(
                f'COPY "{NAMESPACE}".heap FROM STDIN', io.StringIO("1\n")
            )
        pg_conn.rollback()

        # temporary tables are fine
        run_command("CREATE TEMP TABLE scratch AS SELECT 11 AS a", pg_conn)
        run_command(f"INSERT INTO {pg_name} SELECT a, 'tmp' FROM scratch", pg_conn)
        run_command("INSERT INTO scratch VALUES (12)", pg_conn)
        pg_conn.commit()

        assert catalog_rows(table) == [(1, "spark-1"), (2, "spark-2"), (11, "tmp")]
        assert run_query(f'SELECT count(*) FROM "{NAMESPACE}".heap', pg_conn) == [[0]]
        pg_conn.commit()
    finally:
        pg_conn.rollback()
        run_command(f'DROP TABLE "{NAMESPACE}".heap, "{NAMESPACE}".internal', pg_conn)
        pg_conn.commit()


def test_drop_table_leaves_catalog_table(pg_conn, rest_catalog, shared_table):
    table, pg_name = shared_table

    conn = autocommit_conn()
    run_command(f"INSERT INTO {pg_name} VALUES (60, 'pg-60')", conn)
    run_command(f"DROP TABLE {pg_name}", conn)
    run_command("VACUUM", conn)
    conn.close()

    # dropping our pointer does not touch the table or its files
    assert catalog_rows(table) == [(1, "spark-1"), (2, "spark-2"), (60, "pg-60")]


def test_schema_change_by_other_engine(pg_conn, shared_table):
    table, pg_name = shared_table

    with table.update_schema() as update:
        update.add_column("extra", StringType())

    conn = autocommit_conn()
    for statement in [
        f"SELECT * FROM {pg_name}",
        f"INSERT INTO {pg_name} VALUES (70, 'pg-70')",
    ]:
        error = run_command(statement, conn, raise_error=False)
        assert "Schema mismatch between Iceberg and Postgres" in str(error)

    # attaching again picks up the new schema, and writes work again
    run_command(f"DROP TABLE {pg_name}", conn)
    run_command(
        f"""CREATE TABLE {pg_name} () USING iceberg
            WITH (catalog='rest', external='true', catalog_table_name='{table.name()[-1]}')""",
        conn,
    )
    run_command(f"INSERT INTO {pg_name} VALUES (70, 'pg-70', 'x')", conn)
    res = run_query(f"SELECT id, val, extra FROM {pg_name} ORDER BY id", conn)
    conn.close()
    assert res == [[1, "spark-1", None], [2, "spark-2", None], [70, "pg-70", "x"]]


def test_partitioned_table_writes_rejected(pg_conn, shared_table):
    table, pg_name = shared_table

    with table.update_spec() as update:
        update.add_identity("id")

    conn = autocommit_conn()
    error = run_command(
        f"INSERT INTO {pg_name} VALUES (80, 'pg-80')", conn, raise_error=False
    )
    conn.close()
    assert "partitioned external REST catalog table" in str(error)


def test_external_option_cannot_change(pg_conn, shared_table, with_default_location):
    table, pg_name = shared_table

    error = run_command(
        f"ALTER FOREIGN TABLE {pg_name} OPTIONS (SET external 'false')",
        pg_conn,
        raise_error=False,
    )
    assert "can be changed: catalog_table_name" in str(error)
    pg_conn.rollback()

    # nor can a table pg_lake owns become a pointer to a table it does not
    run_command(
        f"CREATE TABLE \"{NAMESPACE}\".owned (id bigint) USING iceberg WITH (catalog='rest')",
        pg_conn,
    )
    pg_conn.commit()
    error = run_command(
        f"""ALTER FOREIGN TABLE "{NAMESPACE}".owned OPTIONS (ADD external 'true',
            ADD catalog_namespace '{NAMESPACE}', ADD catalog_table_name 'owned')""",
        pg_conn,
        raise_error=False,
    )
    assert 'Changing option "external" is not supported' in str(error)
    pg_conn.rollback()

    run_command(f'DROP TABLE "{NAMESPACE}".owned', pg_conn)
    pg_conn.commit()


def test_create_table_in_catalog(pg_conn, rest_catalog, set_polaris_gucs, installcheck):
    """
    CREATE TABLE with columns creates the table in the catalog when it does
    not exist, and then it is like any attached table: other engines see it
    and write it, and dropping it leaves it in the catalog.
    """
    if installcheck:
        pytest.skip("needs the test Polaris server")

    pg_name = f'"{NAMESPACE}".created_by_pg'
    conn = autocommit_conn()
    run_command(
        f"""CREATE TABLE {pg_name} (id bigint NOT NULL, val text)
            USING iceberg WITH (catalog='rest', external='true')""",
        conn,
    )
    run_command(f"INSERT INTO {pg_name} VALUES (1, 'pg-1')", conn)

    table = rest_catalog.load_table(f"{NAMESPACE}.created_by_pg")
    assert [field.name for field in table.schema().fields] == ["id", "val"]
    assert table.schema().find_field("id").required

    external_append(table, [(2, "spark-2")])
    res = run_query(f"SELECT id, val FROM {pg_name} ORDER BY id", conn)
    assert res == [[1, "pg-1"], [2, "spark-2"]]

    # columns given for a table the catalog already has attach to it
    run_command(f"DROP TABLE {pg_name}", conn)
    run_command(
        f"""CREATE TABLE {pg_name} (id bigint NOT NULL, val text)
            USING iceberg WITH (catalog='rest', external='true')""",
        conn,
    )
    res = run_query(f"SELECT count(*) FROM {pg_name}", conn)
    assert res == [[2]]

    run_command(f"DROP TABLE {pg_name}", conn)
    conn.close()
    assert catalog_rows(table) == [(1, "pg-1"), (2, "spark-2")]
    rest_catalog.drop_table(f"{NAMESPACE}.created_by_pg")


def test_create_table_in_catalog_rejected(
    pg_conn, rest_catalog, set_polaris_gucs, installcheck
):
    if installcheck:
        pytest.skip("needs the test Polaris server")

    # a type Iceberg would hand back as another one
    conn = autocommit_conn()
    error = run_command(
        f"""CREATE TABLE "{NAMESPACE}".bad_type (id bigint, val varchar(10))
            USING iceberg WITH (catalog='rest', external='true')""",
        conn,
        raise_error=False,
    )
    conn.close()
    assert "is stored in Iceberg as" in str(error)

    # the catalog would create it outside the transaction
    error = run_command(
        f"""CREATE TABLE "{NAMESPACE}".in_block (id bigint)
            USING iceberg WITH (catalog='rest', external='true')""",
        pg_conn,
        raise_error=False,
    )
    assert "in a transaction block" in str(error)
    pg_conn.rollback()

    tables = [name for _, name in rest_catalog.list_tables(NAMESPACE)]
    assert "bad_type" not in tables and "in_block" not in tables

"""Writable and read-only Iceberg tables on Lakekeeper (#739).

Polaris is the other real REST catalog the tests run against. Lakekeeper
differs from it where pg_lake has broken before: its prefix is a warehouse ID
that /v1/config only returns when given the warehouse, it keeps the staged
metadata of a staged create (#745), and it runs without authentication here.
"""

import psycopg2
import pyarrow
import pytest
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from utils_pytest import *
from helpers.lakekeeper import *

SCHEMA = "lk_tests"


def _drop_namespace(catalog, namespace):
    """Drop a namespace and whatever tables pyiceberg (or a failed test) left
    in it. Dropping the PostgreSQL schema only drops pg_lake's writable tables
    from the catalog, never read-only ones or the namespace itself."""
    try:
        for identifier in catalog.list_tables(namespace):
            catalog.drop_table(identifier)
        catalog.drop_namespace(namespace)
    except NoSuchNamespaceError:
        pass


@pytest.fixture
def lk_schema(superuser_conn, s3, extension, lakekeeper_server, lakekeeper_catalog):
    # Start every test from an empty schema and namespace, so a test can be
    # rerun in the same session.
    superuser_conn.rollback()
    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", superuser_conn)
    run_command(f"CREATE SCHEMA {SCHEMA}", superuser_conn)
    superuser_conn.commit()
    _drop_namespace(lakekeeper_catalog, SCHEMA)
    yield SCHEMA
    superuser_conn.rollback()
    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", superuser_conn)
    superuser_conn.commit()
    _drop_namespace(lakekeeper_catalog, SCHEMA)


def _table_option(conn, table, option, schema=SCHEMA):
    rows = run_query(
        f"""
        SELECT option_value FROM pg_options_to_table(
            (SELECT ftoptions FROM pg_foreign_table
             WHERE ftrelid = '{schema}.{table}'::regclass))
        WHERE option_name = '{option}'
        """,
        conn,
    )
    return rows[0][0] if rows else None


def _create_nested_table(
    conn, table, map_type_name, server="lakekeeper", schema=SCHEMA
):
    run_command(
        f"""
        CREATE TYPE {schema}.{table}_point AS (x int, y text);
        CREATE TABLE {schema}.{table} (
            id int,
            point {schema}.{table}_point,
            tags text[],
            attrs {map_type_name}
        ) USING iceberg WITH (catalog = '{server}')
        """,
        conn,
    )


def test_config_prefix_is_the_warehouse_id(installcheck, superuser_conn, lk_schema):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    run_command(
        f"CREATE TABLE {SCHEMA}.t (a int) USING iceberg WITH (catalog = 'lakekeeper')",
        superuser_conn,
    )
    superuser_conn.commit()

    assert _table_option(superuser_conn, "t", "catalog_name") == (
        lakekeeper_warehouse_id()
    )


def _create_external_table(catalog, name, properties):
    catalog.create_namespace_if_not_exists(SCHEMA)
    assert (SCHEMA,) in catalog.list_namespaces()

    table = catalog.create_table(
        (SCHEMA, name),
        pyarrow.schema([("id", pyarrow.int64()), ("name", pyarrow.string())]),
        properties=properties,
    )
    table.append(pyarrow.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
    return table


def _attach_and_read(conn, name):
    run_command(
        f"""CREATE TABLE {SCHEMA}.{name} () USING iceberg
            WITH (catalog = 'lakekeeper', read_only = true)""",
        conn,
    )
    return [
        tuple(r)
        for r in run_query(f"SELECT id, name FROM {SCHEMA}.{name} ORDER BY id", conn)
    ]


def test_read_only_table_created_outside_pg_lake(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    # pg_lake reads only uncompressed metadata JSON, see the test below.
    _create_external_table(
        lakekeeper_catalog, "external", {"write.metadata.compression-codec": "none"}
    )
    assert _attach_and_read(superuser_conn, "external") == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]
    superuser_conn.commit()


def test_read_only_table_with_gzip_metadata_is_not_readable_yet(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    """Lakekeeper writes gzip-compressed metadata JSON by default, which
    pg_lake cannot read yet. When it can, this test fails: replace the
    pytest.raises with the read-back assertion from the test above."""
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    table = _create_external_table(lakekeeper_catalog, "external_gz", {})
    assert table.metadata_location.endswith(".gz.metadata.json")

    with pytest.raises(psycopg2.Error, match="valid UTF-8"):
        _attach_and_read(superuser_conn, "external_gz")
    superuser_conn.rollback()


def test_nested_types_round_trip_through_the_catalog(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    """Lakekeeper renumbers the staged schema's field IDs; unless pg_lake's own
    schema becomes current, nested values written by pg_lake no longer match
    the schema other engines read them with (#745)."""
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    map_type_name = create_map_type("text", "int")
    _create_nested_table(superuser_conn, "nested", map_type_name)
    run_command(
        f"""INSERT INTO {SCHEMA}.nested VALUES
            (1, (10, 'ten')::{SCHEMA}.nested_point, ARRAY['a', 'b'],
             ARRAY[('k', 1)]::{map_type_name}),
            (2, (20, 'twenty')::{SCHEMA}.nested_point, ARRAY['c'],
             ARRAY[('k', 2)]::{map_type_name})""",
        superuser_conn,
    )
    superuser_conn.commit()

    table = lakekeeper_catalog.load_table((SCHEMA, "nested"))
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["id"])
    assert [r["point"] for r in rows] == [
        {"x": 10, "y": "ten"},
        {"x": 20, "y": "twenty"},
    ]
    assert [r["tags"] for r in rows] == [["a", "b"], ["c"]]
    assert [dict(r["attrs"]) for r in rows] == [{"k": 1}, {"k": 2}]


def test_dml_is_visible_to_other_engines(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    run_command(
        f"""CREATE TABLE {SCHEMA}.dml (id int, v text)
            USING iceberg WITH (catalog = 'lakekeeper');
            INSERT INTO {SCHEMA}.dml SELECT g, 'v' || g FROM generate_series(1, 5) g""",
        superuser_conn,
    )
    superuser_conn.commit()
    run_command(f"UPDATE {SCHEMA}.dml SET v = 'updated' WHERE id = 2", superuser_conn)
    run_command(f"DELETE FROM {SCHEMA}.dml WHERE id > 3", superuser_conn)
    superuser_conn.commit()

    expected = [(1, "v1"), (2, "updated"), (3, "v3")]
    rows = run_query(f"SELECT id, v FROM {SCHEMA}.dml ORDER BY id", superuser_conn)
    assert [tuple(r) for r in rows] == expected

    table = lakekeeper_catalog.load_table((SCHEMA, "dml"))
    other = sorted((r["id"], r["v"]) for r in table.scan().to_arrow().to_pylist())
    assert other == expected


def test_location_is_the_requested_per_relation_path(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    run_command(
        f"CREATE TABLE {SCHEMA}.loc (a int) USING iceberg WITH (catalog = 'lakekeeper')",
        superuser_conn,
    )
    superuser_conn.commit()

    relid = run_query(f"SELECT '{SCHEMA}.loc'::regclass::oid", superuser_conn)[0][0]
    expected = (
        f"{LAKEKEEPER_LOCATION_PREFIX}/{server_params.PG_DATABASE}/{SCHEMA}/loc/{relid}"
    )
    assert _table_option(superuser_conn, "loc", "location") == expected
    assert (
        lakekeeper_catalog.load_table((SCHEMA, "loc")).location().rstrip("/")
        == expected
    )


def test_create_in_a_namespace_the_catalog_has_not_seen(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    schema = "lk_new_namespace"
    _drop_namespace(lakekeeper_catalog, schema)
    try:
        run_command(
            f"""CREATE SCHEMA {schema};
                CREATE TABLE {schema}.t (a int) USING iceberg WITH (catalog = 'lakekeeper');
                INSERT INTO {schema}.t VALUES (1)""",
            superuser_conn,
        )
        superuser_conn.commit()
        table = lakekeeper_catalog.load_table((schema, "t"))
        assert table.scan().to_arrow().num_rows == 1
    finally:
        superuser_conn.rollback()
        run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE", superuser_conn)
        superuser_conn.commit()
        _drop_namespace(lakekeeper_catalog, schema)


def test_server_without_location_prefix_uses_catalog_location(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    """Without location_prefix (and without the default-prefix GUC), pg_lake
    sends no location and the table lives where Lakekeeper puts it."""
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    create_lakekeeper_server(
        superuser_conn, "lakekeeper_noprefix", location_prefix=None
    )
    try:
        run_command(
            f"""CREATE TABLE {SCHEMA}.np (a int) USING iceberg
                WITH (catalog = 'lakekeeper_noprefix');
                INSERT INTO {SCHEMA}.np VALUES (1), (2)""",
            superuser_conn,
        )
        superuser_conn.commit()

        location = _table_option(superuser_conn, "np", "location")
        table = lakekeeper_catalog.load_table((SCHEMA, "np"))
        assert location == table.location().rstrip("/")
        assert location.startswith(LAKEKEEPER_LOCATION_PREFIX + "/")
        assert table.scan().to_arrow().num_rows == 2
    finally:
        superuser_conn.rollback()
        run_command(f"DROP TABLE IF EXISTS {SCHEMA}.np", superuser_conn)
        superuser_conn.commit()
        drop_lakekeeper_server(superuser_conn, "lakekeeper_noprefix")


def test_drop_table_removes_it_from_the_catalog(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    run_command(
        f"""CREATE TABLE {SCHEMA}.dropped (a int) USING iceberg WITH (catalog = 'lakekeeper');
            INSERT INTO {SCHEMA}.dropped VALUES (1)""",
        superuser_conn,
    )
    superuser_conn.commit()
    lakekeeper_catalog.load_table((SCHEMA, "dropped"))

    run_command(f"DROP TABLE {SCHEMA}.dropped", superuser_conn)
    superuser_conn.commit()

    with pytest.raises(NoSuchTableError):
        lakekeeper_catalog.load_table((SCHEMA, "dropped"))


def test_create_rolled_back_to_a_savepoint_leaves_no_table(
    installcheck, superuser_conn, lk_schema, lakekeeper_catalog
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    run_command(
        f"""CREATE TABLE {SCHEMA}.kept (a int) USING iceberg WITH (catalog = 'lakekeeper');
            SAVEPOINT s;
            CREATE TABLE {SCHEMA}.undone (a int) USING iceberg WITH (catalog = 'lakekeeper');
            ROLLBACK TO SAVEPOINT s""",
        superuser_conn,
    )
    superuser_conn.commit()

    lakekeeper_catalog.load_table((SCHEMA, "kept"))
    with pytest.raises(NoSuchTableError):
        lakekeeper_catalog.load_table((SCHEMA, "undone"))

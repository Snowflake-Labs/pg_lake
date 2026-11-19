"""An external Iceberg column whose name is longer than a Postgres identifier
reads as the column with the truncated name."""

import uuid

import pyarrow
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, StructType

from helpers.polaris import *
from utils_pytest import *

LONG_NAME = "c" * 70
TRUNCATED_NAME = "c" * 63


def test_long_column_name(pg_conn, rest_table):
    namespace, table = rest_table(LONG_NAME)
    create_read_only_table(pg_conn, namespace)

    assert column_names(pg_conn, namespace) == ["id", TRUNCATED_NAME]
    assert run_query(f'SELECT * FROM "{namespace}".t', pg_conn) == [[1, "v"]]
    assert run_query(
        f'SELECT id FROM "{namespace}".t WHERE "{TRUNCATED_NAME}" = \'v\'', pg_conn
    ) == [[1]]

    run_command(
        f"""CREATE FOREIGN TABLE "{namespace}".by_path () SERVER pg_lake
            OPTIONS (path '{table.metadata_location}', format 'iceberg')""",
        pg_conn,
    )
    assert run_query(f'SELECT * FROM "{namespace}".by_path', pg_conn) == [[1, "v"]]
    pg_conn.rollback()


def test_long_column_name_listed_in_create(pg_conn, rest_table):
    namespace, _ = rest_table(LONG_NAME)
    create_read_only_table(pg_conn, namespace, f'(id bigint, "{LONG_NAME}" text)')

    assert run_query(f'SELECT * FROM "{namespace}".t', pg_conn) == [[1, "v"]]
    pg_conn.rollback()


def test_multibyte_name_truncated_at_a_character(pg_conn, rest_table):
    namespace, _ = rest_table("é" * 40)
    create_read_only_table(pg_conn, namespace)

    assert column_names(pg_conn, namespace) == ["id", "é" * 31]
    assert run_query(f'SELECT * FROM "{namespace}".t', pg_conn) == [[1, "v"]]
    pg_conn.rollback()


def test_multibyte_nested_field_name(pg_conn, rest_table):
    namespace, _ = rest_table(nested="é" * 40)
    create_read_only_table(pg_conn, namespace)

    struct_type = run_query(
        "SELECT atttypid FROM pg_attribute "
        f"WHERE attrelid = '\"{namespace}\".t'::regclass AND attname = 's'",
        pg_conn,
    )[0][0]
    assert run_query(
        "SELECT attname FROM pg_attribute "
        f"WHERE attrelid = (SELECT typrelid FROM pg_type WHERE oid = {struct_type}) "
        "AND attnum > 0",
        pg_conn,
    ) == [["é" * 31]]
    assert run_query(f'SELECT id, s FROM "{namespace}".t', pg_conn) == [[1, "(v)"]]
    pg_conn.rollback()


def test_names_that_truncate_to_the_same_name(pg_conn, rest_table):
    long_names = ["x" * 63 + "a", "x" * 63 + "b"]
    namespace, table = rest_table(*long_names)

    run_command(f'CREATE SCHEMA "{namespace}"', pg_conn)
    error = run_command(
        f"""CREATE TABLE "{namespace}".t () USING iceberg
            WITH (catalog='rest', read_only=True)""",
        pg_conn,
        raise_error=False,
    )
    assert "have the same name when truncated to a Postgres identifier" in str(error)
    pg_conn.rollback()

    # neither names a column, so both keep their names
    assert run_query(
        f"SELECT column_name FROM lake_file.preview('{table.metadata_location}')",
        pg_conn,
    ) == [["id"]] + [[name] for name in long_names]
    run_command("CREATE TEMP TABLE copied (id bigint, a text, b text)", pg_conn)
    run_command(
        f"COPY copied FROM '{table.metadata_location}' WITH (format 'iceberg')",
        pg_conn,
    )
    assert run_query("SELECT * FROM copied", pg_conn) == [[1, "v", "v"]]
    pg_conn.rollback()


@pytest.fixture
def rest_table(
    installcheck, pg_conn, s3, polaris_session, set_polaris_gucs, with_default_location
):
    if installcheck:
        pytest.skip("requires the Polaris fixture")

    namespace = f"long_names_{uuid.uuid4().hex[:8]}"
    catalog = create_iceberg_rest_catalog(namespace)

    def create(*names, nested=None):
        fields = [NestedField(1, "id", LongType())] + [
            NestedField(i + 2, name, StringType()) for i, name in enumerate(names)
        ]
        row = {"id": 1} | {name: "v" for name in names}
        if nested is not None:
            struct = StructType(NestedField(len(fields) + 2, nested, StringType()))
            fields.append(NestedField(len(fields) + 1, "s", struct, required=False))
            row["s"] = {nested: "v"}
        table = catalog.create_table(f"{namespace}.t", schema=Schema(*fields))
        table.append(pyarrow.Table.from_pylist([row], table.schema().as_arrow()))
        return namespace, table

    yield create

    pg_conn.rollback()
    run_command(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE', pg_conn)
    pg_conn.commit()
    catalog.drop_table(f"{namespace}.t")
    catalog.drop_namespace(namespace)


def create_read_only_table(pg_conn, namespace, columns="()"):
    run_command(f'CREATE SCHEMA "{namespace}"', pg_conn)
    run_command(
        f"""CREATE TABLE "{namespace}".t {columns} USING iceberg
            WITH (catalog='rest', read_only=True)""",
        pg_conn,
    )
    pg_conn.commit()


def column_names(pg_conn, namespace):
    rows = run_query(
        "SELECT attname FROM pg_attribute "
        f"WHERE attrelid = '\"{namespace}\".t'::regclass AND attnum > 0 "
        "ORDER BY attnum",
        pg_conn,
    )
    return [row[0] for row in rows]

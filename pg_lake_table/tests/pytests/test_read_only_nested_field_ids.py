"""Fields nested in a column of an external Iceberg table are read by their
field ids, so a nested field renamed, or dropped and added again, in the
source catalog reads from the right field of each data file."""

import json
import uuid

import pyarrow
import pyarrow.parquet
import pytest
from pyarrow import fs
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from helpers.polaris import *
from utils_pytest import *


NESTED_FIELDS_TYPE = (
    "STRUCT(parent_id INTEGER, field_id INTEGER, name VARCHAR, names VARCHAR[])[]"
)


@pytest.mark.parametrize("fields", [["a", "k"], ["a"]])
def test_renamed_struct_field(rest_namespace, fields):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t", schema=Schema(id_field(), struct_field(2, "s", fields, 3))
    )
    table.append(rows(table, [{"id": 1, "s": {name: f"v_{name}" for name in fields}}]))
    with table.update_schema() as update:
        update.rename_column("s.a", "renamed")
    create_pg_table(namespace, pg_conn)

    expected = "(" + ",".join(f"v_{name}" for name in fields) + ")"
    assert run_query(f'SELECT id, s FROM "{namespace}".t', pg_conn) == [[1, expected]]
    assert run_query(f'SELECT (s).renamed FROM "{namespace}".t', pg_conn) == [["v_a"]]


def test_struct_field_dropped_and_added_again(rest_namespace):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t", schema=Schema(id_field(), struct_field(2, "s", ["a", "k"], 3))
    )
    table.append(rows(table, [{"id": 1, "s": {"a": "old", "k": "k1"}}]))
    with table.update_schema() as update:
        update.delete_column("s.a")
    with table.update_schema() as update:
        update.add_column(("s", "a"), StringType())
    table.append(rows(table, [{"id": 2, "s": {"k": "k2", "a": "new"}}]))
    create_pg_table(namespace, pg_conn)

    # the new field shares only the name of the dropped one
    assert run_query(
        f'SELECT id, (s).a, (s).k FROM "{namespace}".t ORDER BY id', pg_conn
    ) == [[1, None, "k1"], [2, "new", "k2"]]


def test_renamed_field_in_a_list_and_a_map(rest_namespace):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(
            id_field(),
            NestedField(
                2,
                "l",
                ListType(3, StructType(NestedField(4, "a", StringType())), False),
                required=False,
            ),
            NestedField(
                5,
                "m",
                MapType(
                    6,
                    StringType(),
                    7,
                    StructType(NestedField(8, "a", StringType())),
                    False,
                ),
                required=False,
            ),
        ),
    )
    table.append(
        rows(table, [{"id": 1, "l": [{"a": "in_list"}], "m": [("k", {"a": "in_map"})]}])
    )
    with table.update_schema() as update:
        update.rename_column("l.element.a", "list_renamed")
        update.rename_column("m.value.a", "map_renamed")
    create_pg_table(namespace, pg_conn)

    assert (
        run_query(
            f"""SELECT (l[1]).list_renamed, (map_type.extract(m, 'k')).map_renamed
            FROM "{namespace}".t""",
            pg_conn,
        )
        == [["in_list", "in_map"]]
    )


def test_renamed_struct_field_in_a_file_without_field_ids(rest_namespace):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t", schema=Schema(id_field(), struct_field(2, "s", ["a", "k"], 3))
    )
    add_file_without_field_ids(
        namespace,
        table,
        pyarrow.Table.from_pylist([{"id": 1, "s": {"a": "v_a", "k": "v_k"}}]),
    )
    with table.update_schema() as update:
        update.rename_column("s.a", "renamed")
    create_pg_table(namespace, pg_conn)

    # the name mapping keeps the field's old name, which the file has
    assert run_query(f'SELECT (s).renamed, (s).k FROM "{namespace}".t', pg_conn) == [
        ["v_a", "v_k"]
    ]


def test_struct_field_the_name_mapping_leaves_out_reads_null(rest_namespace):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t", schema=Schema(id_field(), struct_field(2, "s", ["a", "k"], 3))
    )
    add_file_without_field_ids(
        namespace,
        table,
        pyarrow.Table.from_pylist([{"id": 1, "s": {"a": "v_a", "k": "v_k"}}]),
    )
    # the file names its field k as the table does, but the mapping leaves it out
    table.transaction().set_properties(
        {
            "schema.name-mapping.default": json.dumps(
                [
                    {"field-id": 1, "names": ["id"]},
                    {
                        "field-id": 2,
                        "names": ["s"],
                        "fields": [{"field-id": 3, "names": ["a"]}],
                    },
                ]
            )
        }
    ).commit_transaction()
    create_pg_table(namespace, pg_conn)

    assert run_query(f'SELECT (s).a, (s).k FROM "{namespace}".t', pg_conn) == [
        ["v_a", None]
    ]


def test_fields_in_a_list_and_a_map_in_a_file_without_field_ids(rest_namespace):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(
            id_field(),
            NestedField(
                2,
                "l",
                ListType(3, StructType(NestedField(4, "a", StringType())), False),
                required=False,
            ),
            NestedField(
                5,
                "m",
                MapType(
                    6,
                    StringType(),
                    7,
                    StructType(NestedField(8, "a", StringType())),
                    False,
                ),
                required=False,
            ),
        ),
    )
    element = pyarrow.struct([pyarrow.field("a", pyarrow.string())])
    file_schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64()),
            pyarrow.field("l", pyarrow.list_(pyarrow.field("element", element))),
            pyarrow.field("m", pyarrow.map_(pyarrow.string(), element)),
        ]
    )
    add_file_without_field_ids(
        namespace,
        table,
        pyarrow.Table.from_pylist(
            [{"id": 1, "l": [{"a": "in_list"}], "m": [("k", {"a": "in_map"})]}],
            file_schema,
        ),
    )
    with table.update_schema() as update:
        update.rename_column("l.element.a", "list_renamed")
        update.rename_column("m.value.a", "map_renamed")
    create_pg_table(namespace, pg_conn)

    assert (
        run_query(
            f"""SELECT (l[1]).list_renamed, (map_type.extract(m, 'k')).map_renamed
            FROM "{namespace}".t""",
            pg_conn,
        )
        == [["in_list", "in_map"]]
    )


def test_mixed_case_and_long_struct_field_names_in_a_file_without_field_ids(
    rest_namespace,
):
    """The file's names are matched as the name mapping spells them, whatever
    PostgreSQL makes of them."""
    namespace, catalog, pg_conn = rest_namespace
    long_name = "f" * 70
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(id_field(), struct_field(2, "s", ["MixedCase", long_name], 3)),
    )
    add_file_without_field_ids(
        namespace,
        table,
        pyarrow.Table.from_pylist(
            [{"id": 1, "s": {"MixedCase": "v1", long_name: "v2"}}]
        ),
    )
    create_pg_table(namespace, pg_conn, "lowercase_column_names = true")

    assert run_query(
        f'SELECT (s).mixedcase, (s)."{long_name[:63]}" FROM "{namespace}".t', pg_conn
    ) == [["v1", "v2"]]


def test_file_that_nests_a_field_differently_is_refused(pgduck_conn, s3):
    path = f"s3://{TEST_BUCKET}/nested_ids_{uuid.uuid4().hex[:8]}/f.parquet"
    run_command(
        f"""COPY (SELECT {{'a': {{'x': 1}}}} AS s) TO '{path}'
            (FORMAT PARQUET, FIELD_IDS {{s: {{__duckdb_field_id: 1, a: {{__duckdb_field_id: 2, x: 3}}}}}})""",
        pgduck_conn,
    )
    pgduck_conn.commit()

    error = run_query(
        f"""SELECT s FROM read_parquet('{path}', schema=map {{
                1: {{name: 's', type: 'STRUCT(a INTEGER)', default_value: NULL,
                     nested: [{{parent_id: 1, field_id: 2, name: 'a', names: NULL}}]::{NESTED_FIELDS_TYPE}}}}})""",
        pgduck_conn,
        False,
    )
    assert 'nests the fields of column "s" differently' in str(error)
    pgduck_conn.rollback()


def test_struct_with_field_ids_read_without_a_schema(pgduck_conn, s3):
    """A file whose struct fields have field ids, read without a schema, has no
    field ids in the scan's columns to compare the file's against."""
    path = f"s3://{TEST_BUCKET}/nested_ids_{uuid.uuid4().hex[:8]}/f.parquet"
    run_command(
        f"""COPY (SELECT 1 AS id, {{'a': 2, 'b': 'x'}} AS s) TO '{path}'
            (FORMAT PARQUET, FIELD_IDS {{id: 1, s: {{__duckdb_field_id: 2, a: 3, b: 4}}}})""",
        pgduck_conn,
    )
    pgduck_conn.commit()

    assert run_query(
        f"SELECT id, s::VARCHAR FROM read_parquet(['{path}'])", pgduck_conn
    ) == [[1, "{'a': 2, 'b': x}"]]
    pgduck_conn.commit()


def test_promoted_struct_field(rest_namespace):
    namespace, catalog, pg_conn = rest_namespace
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(
            id_field(),
            NestedField(
                2, "s", StructType(NestedField(3, "n", IntegerType())), required=False
            ),
        ),
    )
    table.append(rows(table, [{"id": 1, "s": {"n": 7}}]))
    with table.update_schema() as update:
        update.update_column("s.n", LongType())
    create_pg_table(namespace, pg_conn)

    assert run_query(f'SELECT (s).n FROM "{namespace}".t', pg_conn) == [[7]]


@pytest.fixture
def rest_namespace(
    installcheck, pg_conn, s3, polaris_session, set_polaris_gucs, with_default_location
):
    if installcheck:
        pytest.skip("requires the Polaris fixture")

    namespace = f"nested_ids_{uuid.uuid4().hex[:8]}"
    catalog = create_iceberg_rest_catalog(namespace)

    yield namespace, catalog, pg_conn

    pg_conn.rollback()
    run_command(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE', pg_conn)
    pg_conn.commit()
    if catalog.table_exists(f"{namespace}.t"):
        catalog.drop_table(f"{namespace}.t")
    catalog.drop_namespace(namespace)


def id_field():
    return NestedField(1, "id", LongType())


def struct_field(field_id, name, child_names, first_child_id):
    return NestedField(
        field_id,
        name,
        StructType(
            *[
                NestedField(first_child_id + i, child, StringType())
                for i, child in enumerate(child_names)
            ]
        ),
        required=False,
    )


def rows(table, values):
    return pyarrow.Table.from_pylist(values, table.schema().as_arrow())


def add_file_without_field_ids(namespace, table, rows):
    path = f"s3://{TEST_BUCKET}/{namespace}/added/f.parquet"
    pyarrow.parquet.write_table(
        rows,
        path.removeprefix("s3://"),
        filesystem=fs.S3FileSystem(
            access_key=TEST_AWS_ACCESS_KEY_ID,
            secret_key=TEST_AWS_SECRET_ACCESS_KEY,
            endpoint_override=f"http://localhost:{MOTO_PORT}",
            scheme="http",
            region=TEST_AWS_REGION,
        ),
    )
    table.add_files([path])


def create_pg_table(namespace, pg_conn, options=""):
    run_command(f'CREATE SCHEMA "{namespace}"', pg_conn)
    run_command(
        f"""CREATE TABLE "{namespace}".t () USING iceberg
            WITH (catalog='rest', read_only=True{", " + options if options else ""})""",
        pg_conn,
    )
    pg_conn.commit()

"""Data files written without Iceberg field ids, as by add_files or migrate,
are read through the table's name mapping (schema.name-mapping.default).
Without a name mapping, reading a column from such a file fails rather than
returning NULLs."""

import json
import uuid

import pyarrow
import pyarrow.parquet
import pytest
from pyarrow import fs
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from helpers.polaris import *
from utils_pytest import *

NAME_MAPPING = "schema.name-mapping.default"


def test_file_without_field_ids_reads_through_name_mapping(table_with_added_file):
    namespace, table, pg_conn = table_with_added_file({"id": [1, 2], "v": ["a", "b"]})
    table.append(pyarrow.table({"id": pyarrow.array([3], pyarrow.int64()), "v": ["c"]}))
    create_pg_table(namespace, pg_conn)

    assert run_query(f'SELECT id, v FROM "{namespace}".t ORDER BY id', pg_conn) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
    ]
    assert run_query(f"""SELECT id FROM "{namespace}".t WHERE v = 'b'""", pg_conn) == [
        [2]
    ]


def test_name_mapping_keeps_the_names_of_renamed_columns(table_with_added_file):
    namespace, table, pg_conn = table_with_added_file({"id": [1, 2], "v": ["a", "b"]})
    with table.update_schema() as update:
        update.rename_column("v", "renamed")
    table.refresh()
    mapping = {
        entry["field-id"]: entry["names"]
        for entry in json.loads(table.properties[NAME_MAPPING])
    }
    assert set(mapping[2]) == {"v", "renamed"}
    create_pg_table(namespace, pg_conn)

    assert run_query(
        f'SELECT id, renamed FROM "{namespace}".t ORDER BY id', pg_conn
    ) == [[1, "a"], [2, "b"]]


def test_name_mapping_reads_unmapped_columns_as_null(table_with_added_file):
    namespace, table, pg_conn = table_with_added_file(
        {"id": [1], "x": ["unmapped"], "v": ["b"]}
    )
    # the column the mapping leaves out comes before one it maps
    table.transaction().set_properties(
        {
            NAME_MAPPING: json.dumps(
                [{"field-id": 1, "names": ["id"]}, {"field-id": 3, "names": ["v"]}]
            )
        }
    ).commit_transaction()
    create_pg_table(namespace, pg_conn)

    assert run_query(f'SELECT id, x, v FROM "{namespace}".t', pg_conn) == [
        [1, None, "b"]
    ]


def test_file_without_field_ids_and_no_name_mapping_fails(table_with_added_file):
    namespace, table, pg_conn = table_with_added_file({"id": [1, 2], "v": ["a", "b"]})
    table.transaction().remove_properties(NAME_MAPPING).commit_transaction()
    create_pg_table(namespace, pg_conn)

    # no column is read, so the missing field ids don't matter
    assert run_query(f'SELECT count(*) FROM "{namespace}".t', pg_conn) == [[2]]

    error = run_query(f'SELECT id, v FROM "{namespace}".t', pg_conn, False)
    assert "the file has no field ids to match its columns by" in str(error)
    assert f"{namespace}/added/f.parquet" in str(error)


def test_read_parquet_assigns_field_ids_by_mapped_name(pgduck_conn, s3):
    path = f"s3://{TEST_BUCKET}/mapped_names_{uuid.uuid4().hex[:8]}/f.parquet"
    run_command(
        f"COPY (SELECT 1::BIGINT AS id, 'unmapped' AS x, 'b' AS v) TO '{path}' (FORMAT PARQUET)",
        pgduck_conn,
    )
    pgduck_conn.commit()

    def read(names_of_v):
        return run_query(
            f"""SELECT id, v, file_row_number FROM read_parquet('{path}', schema=map {{
                    1: {{name: 'id', type: 'BIGINT', default_value: NULL, names: ['id']::VARCHAR[]}},
                    3: {{name: 'v', type: 'VARCHAR', default_value: NULL, names: {names_of_v}}}}},
                    file_row_number=true)""",
            pgduck_conn,
        )

    assert read("['old_v', 'v']::VARCHAR[]") == [[1, "b", 0]]
    # a field none of whose names is a file column reads as its default
    assert read("NULL::VARCHAR[]") == [[1, None, 0]]


def test_file_without_field_ids_fails_with_row_numbers(pgduck_conn, s3):
    """A scan of a table with position deletes reads row numbers, whose column
    has a field id even in a file written without any."""
    path = f"s3://{TEST_BUCKET}/no_field_ids_{uuid.uuid4().hex[:8]}/f.parquet"
    run_command(
        f"COPY (SELECT 1::BIGINT AS id, 'a' AS v) TO '{path}' (FORMAT PARQUET)",
        pgduck_conn,
    )
    pgduck_conn.commit()
    read = f"""read_parquet('{path}', schema=map {{
                   1: {{name: 'id', type: 'BIGINT', default_value: NULL}},
                   2: {{name: 'v', type: 'VARCHAR', default_value: NULL}}}},
                   filename=true, file_row_number=true)"""

    error = run_query(f"SELECT id, v FROM {read}", pgduck_conn, False)
    assert "the file has no field ids to match its columns by" in str(error)
    pgduck_conn.rollback()

    assert run_query(f"SELECT file_row_number FROM {read}", pgduck_conn) == [[0]]


@pytest.fixture
def table_with_added_file(
    installcheck, pg_conn, s3, polaris_session, set_polaris_gucs, with_default_location
):
    """Creates a REST catalog table with a bigint id column and string columns
    for the rest, and adds a Parquet file without field ids holding the given
    columns to it."""
    if installcheck:
        pytest.skip("requires the Polaris fixture")

    namespace = f"no_field_ids_{uuid.uuid4().hex[:8]}"
    catalog = create_iceberg_rest_catalog(namespace)

    def create(columns):
        table = catalog.create_table(
            f"{namespace}.t",
            schema=Schema(
                *[
                    NestedField(
                        i + 1, name, LongType() if name == "id" else StringType()
                    )
                    for i, name in enumerate(columns)
                ]
            ),
        )

        path = f"s3://{TEST_BUCKET}/{namespace}/added/f.parquet"
        pyarrow.parquet.write_table(
            pyarrow.table(
                {
                    name: pyarrow.array(
                        values, pyarrow.int64() if name == "id" else pyarrow.string()
                    )
                    for name, values in columns.items()
                }
            ),
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
        assert NAME_MAPPING in table.properties
        return namespace, table, pg_conn

    yield create

    pg_conn.rollback()
    run_command(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE', pg_conn)
    pg_conn.commit()
    if catalog.table_exists(f"{namespace}.t"):
        catalog.drop_table(f"{namespace}.t")
    catalog.drop_namespace(namespace)


def create_pg_table(namespace, pg_conn):
    run_command(f'CREATE SCHEMA "{namespace}"', pg_conn)
    run_command(
        f"""CREATE TABLE "{namespace}".t () USING iceberg
            WITH (catalog='rest', read_only=True)""",
        pg_conn,
    )
    pg_conn.commit()

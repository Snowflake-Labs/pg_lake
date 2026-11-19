"""A read-only REST table reads each column from the Iceberg field of the
same name, whatever order the catalog keeps its fields in."""

import uuid

import pyarrow
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from helpers.polaris import *
from utils_pytest import *


def test_reordered_fields(pg_conn, iceberg_table):
    namespace, table = iceberg_table
    create_read_only_table(pg_conn, namespace)

    with table.update_schema() as update:
        update.move_after("b", "id")

    assert read_rows(pg_conn, namespace) == [[1, "A", "B"]]


def test_field_dropped_and_added_back(pg_conn, iceberg_table):
    """The field added back gets a new id and moves to the end."""
    namespace, table = iceberg_table
    create_read_only_table(pg_conn, namespace)

    with table.update_schema() as update:
        update.delete_column("a")
    with table.update_schema() as update:
        update.add_column("a", StringType())
    table.append(
        pyarrow.Table.from_pylist(
            [{"id": 2, "b": "B2", "a": "A2"}], table.schema().as_arrow()
        )
    )

    assert read_rows(pg_conn, namespace) == [[1, None, "B"], [2, "A2", "B2"]]


def test_columns_listed_in_another_order(pg_conn, iceberg_table):
    namespace, _ = iceberg_table
    create_read_only_table(pg_conn, namespace, "(b text, id bigint, a text)")

    assert read_rows(pg_conn, namespace) == [[1, "A", "B"]]


def test_column_dropped_in_catalog(pg_conn, iceberg_table):
    namespace, table = iceberg_table
    create_read_only_table(pg_conn, namespace)

    with table.update_schema() as update:
        update.delete_column("a")

    error = run_query(f'SELECT * FROM "{namespace}".t', pg_conn, raise_error=False)
    assert 'Postgres column "a" has no matching Iceberg field' in str(error)
    pg_conn.rollback()


def test_filename_column(pg_conn, s3, extension, with_default_location):
    """The filename column is filled by the scan, not by an Iceberg field."""
    run_command("CREATE TABLE filename_source (a int, b text) USING iceberg", pg_conn)
    run_command("INSERT INTO filename_source VALUES (1, 'x')", pg_conn)
    pg_conn.commit()
    metadata = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'filename_source'",
        pg_conn,
    )[0][0]

    run_command(
        "CREATE FOREIGN TABLE filename_read () SERVER pg_lake "
        f"OPTIONS (path '{metadata}', format 'iceberg', filename 'true')",
        pg_conn,
    )
    rows = run_query("SELECT a, b, _filename LIKE 's3://%' FROM filename_read", pg_conn)
    assert rows == [[1, "x", True]]

    pg_conn.rollback()
    run_command("DROP TABLE filename_source", pg_conn)
    pg_conn.commit()


@pytest.fixture
def iceberg_table(
    installcheck, pg_conn, s3, polaris_session, set_polaris_gucs, with_default_location
):
    if installcheck:
        pytest.skip("requires the Polaris fixture")

    namespace = f"column_order_{uuid.uuid4().hex[:8]}"
    catalog = create_iceberg_rest_catalog(namespace)
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(
            NestedField(1, "id", LongType()),
            NestedField(2, "a", StringType()),
            NestedField(3, "b", StringType()),
        ),
    )
    table.append(
        pyarrow.Table.from_pylist(
            [{"id": 1, "a": "A", "b": "B"}], table.schema().as_arrow()
        )
    )

    yield namespace, table

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


def read_rows(pg_conn, namespace):
    rows = run_query(f'SELECT id, a, b FROM "{namespace}".t ORDER BY id', pg_conn)
    pg_conn.commit()
    return rows

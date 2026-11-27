"""A read-only REST table reads a table upgraded from Iceberg v1 to v2, whose
snapshots from before the upgrade carry no sequence number."""

import json
import uuid

import pyarrow
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

from helpers.polaris import *
from utils_pytest import *


def test_table_upgraded_from_v1(pg_conn, v1_table):
    namespace, table = v1_table

    with table.transaction() as transaction:
        transaction.upgrade_table_version(format_version=2)
    snapshots = read_metadata(table)["snapshots"]
    assert snapshots and all("sequence-number" not in s for s in snapshots)

    run_command(f'CREATE SCHEMA "{namespace}"', pg_conn)
    run_command(
        f"""CREATE TABLE "{namespace}".t () USING iceberg
            WITH (catalog='rest', read_only=True)""",
        pg_conn,
    )
    pg_conn.commit()
    assert read_ids(pg_conn, namespace) == [[1]]

    table.append(pyarrow.Table.from_pylist([{"id": 2}], table.schema().as_arrow()))
    assert read_ids(pg_conn, namespace) == [[1], [2]]


@pytest.fixture
def v1_table(
    installcheck, pg_conn, s3, polaris_session, set_polaris_gucs, with_default_location
):
    if installcheck:
        pytest.skip("requires the Polaris fixture")

    namespace = f"v1_upgraded_{uuid.uuid4().hex[:8]}"
    catalog = create_iceberg_rest_catalog(namespace)
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(NestedField(1, "id", LongType())),
        properties={"format-version": "1"},
    )
    table.append(pyarrow.Table.from_pylist([{"id": 1}], table.schema().as_arrow()))

    yield namespace, table

    pg_conn.rollback()
    run_command(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE', pg_conn)
    pg_conn.commit()
    catalog.drop_table(f"{namespace}.t")
    catalog.drop_namespace(namespace)


def read_metadata(table):
    with table.io.new_input(table.metadata_location).open() as metadata_file:
        return json.load(metadata_file)


def read_ids(pg_conn, namespace):
    rows = run_query(f'SELECT id FROM "{namespace}".t ORDER BY id', pg_conn)
    pg_conn.commit()
    return rows

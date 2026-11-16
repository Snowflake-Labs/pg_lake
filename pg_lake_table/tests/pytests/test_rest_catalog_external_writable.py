"""
Tests for external=true REST catalog tables: the REST catalog is the
authority for the table, and pg_lake keeps only a pointer to it, like a
read_only table. pyiceberg plays the other engine that commits to the
same table.
"""

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

"""Data files written without Iceberg field ids, as by add_files or migrate,
can only be read through a name mapping, which pg_lake does not support. Their
columns would otherwise read back NULL, so reading one fails instead."""

import uuid

import pyarrow
import pyarrow.parquet
import pytest
from pyarrow import fs
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from helpers.polaris import *
from utils_pytest import *


def test_file_without_field_ids(
    installcheck, pg_conn, s3, polaris_session, set_polaris_gucs, with_default_location
):
    if installcheck:
        pytest.skip("requires the Polaris fixture")

    namespace = f"no_field_ids_{uuid.uuid4().hex[:8]}"
    catalog = create_iceberg_rest_catalog(namespace)
    table = catalog.create_table(
        f"{namespace}.t",
        schema=Schema(
            NestedField(1, "id", LongType()), NestedField(2, "v", StringType())
        ),
    )

    path = f"s3://{TEST_BUCKET}/{namespace}/added/f.parquet"
    pyarrow.parquet.write_table(
        pyarrow.table({"id": pyarrow.array([1, 2], pyarrow.int64()), "v": ["a", "b"]}),
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
    assert "schema.name-mapping.default" in table.properties

    try:
        run_command(f'CREATE SCHEMA "{namespace}"', pg_conn)
        run_command(
            f"""CREATE TABLE "{namespace}".t () USING iceberg
                WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )
        pg_conn.commit()

        # no column is read, so the missing field ids don't matter
        assert run_query(f'SELECT count(*) FROM "{namespace}".t', pg_conn) == [[2]]

        error = run_query(f'SELECT id, v FROM "{namespace}".t', pg_conn, False)
        assert "the file has no field ids to match its columns by" in str(error)
        assert path in str(error)
    finally:
        pg_conn.rollback()
        run_command(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE', pg_conn)
        pg_conn.commit()
        catalog.drop_table(f"{namespace}.t")
        catalog.drop_namespace(namespace)

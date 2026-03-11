import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    ListType,
    StructType,
)
import pyarrow
from sqlalchemy.exc import NotSupportedError
from datetime import datetime, date, timezone


def test_iceberg_catalog_permissions(catalog_conn, cities_table, iceberg_extension):
    assert None is run_query(
        "SELECT * FROM lake_iceberg.tables limit 0", raise_error=False
    )
    assert None is run_query(
        "SELECT * FROM lake_iceberg.tables_internal limit 0", raise_error=False
    )
    assert None is run_query(
        "SELECT * FROM lake_iceberg.tables_external limit 0", raise_error=False
    )


def test_iceberg_catalog_queries(catalog_conn, cities_table, iceberg_extension):
    result = run_query("SELECT * FROM iceberg_tables", catalog_conn)
    assert len(result) == 1
    assert result[0]["table_name"] == "cities"

    result = run_query("SELECT * FROM iceberg_namespace_properties", catalog_conn)
    assert len(result) == 1
    assert result[0] == ["pyiceberg", "public", "exists", "true"]

    catalog_conn.rollback()


def test_create_in_internal_catalog(iceberg_catalog, iceberg_extension):
    with pytest.raises(NotSupportedError) as exc_info:
        internal_catalog = SqlCatalog(
            server_params.PG_DATABASE,
            **{
                "uri": f"postgresql+psycopg2://iceberg_test_catalog@localhost:{server_params.PG_PORT}/{server_params.PG_DATABASE}",
                "warehouse": f"s3://{TEST_BUCKET}/iceberg/",
                "s3.endpoint": f"http://localhost:{MOTO_PORT}",
                "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
            },
        )
        internal_catalog.create_namespace("public")

        schema = Schema(
            NestedField(
                field_id=1, name="datetime", field_type=TimestampType(), required=False
            )
        )

        iceberg_table = internal_catalog.create_table(
            identifier="public.simple_internal_table",
            schema=schema,
            location=f"s3://{TEST_BUCKET}/iceberg/public/simple_internal_table",
        )

    assert "currently only supported via pg_lake_iceberg tables" in str(exc_info.value)


def test_create_in_external_catalog(
    catalog_conn, superuser_conn, iceberg_catalog, iceberg_extension
):
    schema = Schema(
        NestedField(
            field_id=1, name="datetime", field_type=TimestampType(), required=False
        )
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.simple_external_table",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/simple_external_table",
    )

    # Confirm that creating in external catalog succeeds
    results = run_query(
        """
        SELECT count(*) FROM iceberg_tables WHERE table_namespace = 'public' AND table_name = 'simple_external_table'
    """,
        catalog_conn,
    )
    assert results[0]["count"] == 1

    # Confirm that changing the catalog to the database name fails
    error = run_command(
        f"""
        UPDATE iceberg_tables SET catalog_name = '{server_params.PG_DATABASE}'
        WHERE table_namespace = 'public' AND table_name = 'simple_external_table'
    """,
        catalog_conn,
        raise_error=False,
    )
    assert "currently only supported via pg_lake_iceberg tables" in error

    catalog_conn.rollback()

    # Sneak around the trigger by writing database name to lake_iceberg.tables_external as superuser
    run_command(
        f"""
        UPDATE lake_iceberg.tables_external SET catalog_name = '{server_params.PG_DATABASE}'
        WHERE table_namespace = 'public' AND table_name = 'simple_external_table'
    """,
        superuser_conn,
    )

    # but now my table is not visible
    results = run_query(
        """
        SELECT count(*) FROM iceberg_tables WHERE table_namespace = 'public' AND table_name = 'simple_external_table'
    """,
        superuser_conn,
    )
    assert results[0]["count"] == 0

    superuser_conn.rollback()


@pytest.fixture(scope="module")
def catalog_conn(iceberg_catalog):
    conn = open_pg_conn(user="iceberg_test_catalog")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def cities_table(iceberg_catalog):
    schema = Schema(
        NestedField(1, "city", StringType(), required=False),
        NestedField(2, "lat", DoubleType(), required=False),
        NestedField(3, "long", DoubleType(), required=False),
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.cities",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/cities",
    )

    data = pyarrow.Table.from_pylist(
        [
            {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
            {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
            {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
            {"city": "Paris", "lat": 48.864716, "long": 2.349014},
            {"city": "Istanbul", "lat": 41.091242, "long": 29.060915},
        ],
    )

    iceberg_table.append(data)

    return iceberg_table

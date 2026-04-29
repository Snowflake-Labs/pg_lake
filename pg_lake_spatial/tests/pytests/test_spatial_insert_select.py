"""
Tests for geometry columns in INSERT .. SELECT pushdown write paths.

Exercises the iceberg-to-iceberg pushdown path where
IcebergWrapQueryWithNativeTypeConversion rewrites geometry columns
via ST_AsWKB(col::GEOMETRY) before writing to Parquet.
"""

import pytest
from utils_pytest import *


SCHEMA = "test_spatial_insert_select"


@pytest.fixture(autouse=True)
def setup_schema(
    pg_conn,
    s3,
    spatial_analytics_extension,
    pg_lake_extension,
    extension,
    with_default_location,
):
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    yield

    run_command(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_select_pushdown_iceberg_to_iceberg(pg_conn):
    """
    INSERT INTO iceberg SELECT ... FROM iceberg exercises the pushdown path.
    DuckDB reads WKB blobs from the source Parquet and writes them to the
    destination via IcebergWrapQueryWithNativeTypeConversion, which applies
    ST_AsWKB(col::GEOMETRY) to produce standard WKB.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.ice_src (id int, g geometry) USING iceberg;
        INSERT INTO {SCHEMA}.ice_src VALUES
            (1, ST_Point(10, 20)),
            (2, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
            (3, ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')),
            (4, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {SCHEMA}.ice_dest (id int, g geometry) USING iceberg;
        INSERT INTO {SCHEMA}.ice_dest SELECT id, g FROM {SCHEMA}.ice_src;
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT id, ST_AsText(g) geom FROM {SCHEMA}.ice_dest ORDER BY id",
        pg_conn,
    )
    assert len(result) == 4
    assert result[0]["geom"] == "POINT (10 20)"
    assert result[1]["geom"] == "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
    assert result[2]["geom"] == "LINESTRING (0 0, 1 1, 2 2)"
    assert result[3]["geom"] is None


def test_insert_select_pushdown_geometry_with_other_columns(pg_conn):
    """
    Geometry column mixed with other types in iceberg-to-iceberg pushdown
    to verify the conversion targets only geometry columns.
    """
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.mixed_src (
            id int, name text, g geometry, val float
        ) USING iceberg;
        INSERT INTO {SCHEMA}.mixed_src VALUES
            (1, 'alpha', ST_Point(1, 2), 3.14),
            (2, 'beta', ST_Point(3, 4), 2.72),
            (3, 'gamma', NULL, NULL);
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {SCHEMA}.mixed_dest (
            id int, name text, g geometry, val float
        ) USING iceberg;
        INSERT INTO {SCHEMA}.mixed_dest SELECT * FROM {SCHEMA}.mixed_src;
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        f"SELECT id, name, ST_AsText(g) geom, val FROM {SCHEMA}.mixed_dest ORDER BY id",
        pg_conn,
    )
    assert len(result) == 3
    assert result[0] == {"id": 1, "name": "alpha", "geom": "POINT (1 2)", "val": 3.14}
    assert result[1] == {"id": 2, "name": "beta", "geom": "POINT (3 4)", "val": 2.72}
    assert result[2] == {"id": 3, "name": "gamma", "geom": None, "val": None}

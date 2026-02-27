import pytest
import psycopg2
import time
import math
import os
import uuid
from utils_pytest import *

gdal_formats = [
    ("GeoJSON", "geojson"),
    ("GPKG", "gpkg"),
    ("FlatGeoBuf", "fgb"),
    # Disabled as of v1.1.2 due to GDAL parsing error: https://github.com/duckdb/duckdb_spatial/issues/431
    # ("KML", "kml"),
]

ids = [gdal_format[0] for gdal_format in gdal_formats]


@pytest.mark.parametrize("driver,file_extension", gdal_formats, ids=ids)
def test_gdal_read(
    pgduck_conn,
    user_conn,
    s3,
    spatial_analytics_extension,
    pg_lake_extension,
    tmp_path,
    driver,
    file_extension,
):
    filename = f"data.{file_extension}"
    file_key = f"test_gdal/{filename}"
    url = f"s3://{TEST_BUCKET}/{file_key}"
    local_file_path = tmp_path / filename

    data_query = """
        SELECT
          'hello-'||generate_series AS name,
          'world-'||generate_series AS desc,
          format('POINT(52.{} 4.{})', generate_series, 2 * generate_series)::geometry AS geom
        FROM generate_series(1,100)
    """

    # Generate the file using DuckDB and upload it
    run_command(
        f"""
        COPY ({data_query}) TO '{local_file_path}' WITH (format GDAL, driver '{driver}')
    """,
        pgduck_conn,
    )
    s3.upload_file(local_file_path, TEST_BUCKET, file_key)

    # Read the file using FDW
    run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE FOREIGN TABLE test_gdal.fdw ()
        SERVER pg_lake OPTIONS (path '{url}');
    """,
        user_conn,
    )

    # Read the file using a regular table
    run_command(
        f"""
        CREATE TABLE test_gdal.heap ()
        WITH (load_from = '{url}');

    """,
        user_conn,
    )

    # COPY into the expected format
    run_command(
        f"""
        CREATE TABLE test_gdal.heap_defined (LIKE test_gdal.fdw);
        COPY test_gdal.heap_defined FROM '{url}' WITH (format 'gdal');
    """,
        user_conn,
    )

    query = "SELECT * FROM test_gdal.fdw"
    assert_query_results_on_tables(query, user_conn, ["fdw"], ["heap"])
    assert_query_results_on_tables(query, user_conn, ["fdw"], ["heap_defined"])

    user_conn.rollback()


def test_gdal_zip_shapefile(
    user_conn, sample_shapefile, spatial_analytics_extension, pg_lake_extension
):
    # Read the file using FDW
    run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE FOREIGN TABLE test_gdal.fdw ()
        SERVER pg_lake
        OPTIONS (path '{sample_shapefile}', zip_path 'S_USA.OtherSubSurfaceRight.shp');
    """,
        user_conn,
    )

    # Read the file using a regular table
    run_command(
        f"""
        CREATE TABLE test_gdal.heap ()
        WITH (load_from = '{sample_shapefile}');
    """,
        user_conn,
    )

    # COPY into the expected format
    run_command(
        f"""
        CREATE TABLE test_gdal.heap_defined (LIKE test_gdal.fdw);
        COPY test_gdal.heap_defined FROM '{sample_shapefile}' WITH (format 'gdal');
    """,
        user_conn,
    )

    query = "SELECT * FROM test_gdal.fdw"
    assert_query_results_on_tables(query, user_conn, ["fdw"], ["heap"])
    assert_query_results_on_tables(query, user_conn, ["fdw"], ["heap_defined"])

    user_conn.rollback()


def test_gdal_to_iceberg(
    user_conn, sample_shapefile, spatial_analytics_extension, pg_lake_extension
):
    location = f"s3://{TEST_BUCKET}/test_gdal_to_iceberg"

    # Read the file using FDW
    run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE TABLE test_gdal.iceberg ()
        USING pg_lake_iceberg
        WITH (location = '{location}', load_from = '{sample_shapefile}');
    """,
        user_conn,
    )

    # Read the file using a regular table
    run_command(
        f"""
        CREATE TABLE test_gdal.heap ()
        WITH (load_from = '{sample_shapefile}', zip_path = 'S_USA.OtherSubSurfaceRight.shp');
    """,
        user_conn,
    )

    query = "SELECT * FROM test_gdal.iceberg "
    assert_query_results_on_tables(query, user_conn, ["iceberg"], ["heap"])

    user_conn.rollback()


def test_gdal_zip_gml(
    user_conn, sample_gml, spatial_analytics_extension, pg_lake_extension
):
    # Read the file using a regular table, zip_path is required
    run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE TABLE test_gdal.heap ()
        WITH (load_from = '{sample_gml}', zip_path = 'Fahrradrouten.gml');
    """,
        user_conn,
    )

    # Querying is supported because WKB is converted by PostGIS
    result = run_query("SELECT shape FROM test_gdal.heap", user_conn)
    assert len(result) == 5

    # Read a different layer from the same GML
    run_command(
        f"""
        CREATE TEMP TABLE heap_layer ()
        WITH (load_from = '{sample_gml}', zip_path = 'Fahrradrouten.gml', layer = 'Radfernweg_Hamburg_Bremen');

        -- COPY syntax also works
        COPY heap_layer FROM '{sample_gml}'
        WITH (zip_path 'Fahrradrouten.gml', layer 'Radfernweg_Hamburg_Bremen');
    """,
        user_conn,
    )

    # Should have different contents now
    result = run_query("SELECT shape FROM heap_layer", user_conn)
    assert len(result) == 4

    # Read the file using FDW
    run_command(
        f"""
        CREATE FOREIGN TABLE test_gdal.fdw ()
        SERVER pg_lake
        OPTIONS (path '{sample_gml}', zip_path 'Fahrradrouten.gml');
    """,
        user_conn,
    )

    # Querying is currently not supported because WKB is converted by DuckDB
    # Error messages reflect different DuckDB spatial versions
    error = run_query("SELECT shape FROM test_gdal.fdw", user_conn, raise_error=False)
    assert (
        "Geometry type 10 not supported" in error
        or "'MULTICURVE' is not supported" in error
    )

    user_conn.rollback()

    # Read a non-existent layer
    error = run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE FOREIGN TABLE test_gdal.fdw ()
        SERVER pg_lake
        OPTIONS (path '{sample_gml}', zip_path 'Fahrradrouten.gml', layer 'notexists');
    """,
        user_conn,
        raise_error=False,
    )
    assert "could not be found" in error

    user_conn.rollback()


def test_invalid_options(s3, user_conn, spatial_analytics_extension, pg_lake_extension):
    url = f"s3://{TEST_BUCKET}/test_invalid_options/data.zip"

    # Cannot use zip_path with non-GDAL
    error = run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE FOREIGN TABLE test_gdal.fdw (x int, y int)
        SERVER pg_lake
        OPTIONS (path '{url}', format 'csv', zip_path 'foo.shp');
    """,
        user_conn,
        raise_error=False,
    )
    assert "only supported for GDAL" in error

    user_conn.rollback()

    # Cannot use snappy with GDAL
    error = run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE FOREIGN TABLE test_gdal.fdw (x int, y int)
        SERVER pg_lake
        OPTIONS (path '{url}', compression 'snappy');
    """,
        user_conn,
        raise_error=False,
    )
    assert "not supported" in error

    user_conn.rollback()

    # Cannot use layer with non-GDAL
    error = run_command(
        f"""
        CREATE SCHEMA test_gdal;
        CREATE FOREIGN TABLE test_gdal.fdw (x int, y int)
        SERVER pg_lake
        OPTIONS (path '{url}', format 'csv', layer 'foo');
    """,
        user_conn,
        raise_error=False,
    )
    assert "only supported for GDAL" in error

    user_conn.rollback()


def test_no_extension_gdal(
    sample_shapefile,
    superuser_conn,
    spatial_analytics_extension,
    pg_lake_extension,
):
    # Cannot use zip_path with non-GDAL
    error = run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_spatial CASCADE;
        CREATE FOREIGN TABLE test_no_extension ()
        SERVER pg_lake
        OPTIONS (path '{sample_shapefile}');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "pg_lake_spatial extension is required" in error

    superuser_conn.rollback()


@pytest.fixture(scope="module")
def sample_shapefile(s3):
    filename = "geospatial/S_USA.OtherSubSurfaceRight.zip"
    folder_path = sampledata_filepath("")
    local_file_path = os.path.join(folder_path, filename)

    file_key = f"test_gdal/{filename}"

    s3.upload_file(local_file_path, TEST_BUCKET, file_key)

    yield f"s3://{TEST_BUCKET}/{file_key}"


# This file requires explicit zip_path and has an unsupported geometry type
@pytest.fixture(scope="module")
def sample_gml(s3):
    filename = "geospatial/Fahrradrouten.zip"
    folder_path = sampledata_filepath("")
    local_file_path = os.path.join(folder_path, filename)

    file_key = f"test_gdal/{filename}"

    s3.upload_file(local_file_path, TEST_BUCKET, file_key)

    yield f"s3://{TEST_BUCKET}/{file_key}"

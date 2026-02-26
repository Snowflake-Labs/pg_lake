import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

# This file only tests pushdown, not the expected behavior of any/all variants of these functions

test_cases = [
    # (Test name, pushdownable WHERE clause, expected pattern in query)
    ("geometrytype", "WHERE geometrytype(col_poly) = 'POLYGON'", "st_geometrytype"),
    ("ST_Area", "WHERE ST_Area(col_poly) > 0", "st_area("),
    ("ST_Area", "WHERE ST_Area(col_poly,true) > 0", "st_area_spheroid"),
    ("ST_Area", "WHERE ST_Area(col_poly,false) > 0", "st_area_spheroid"),
    (
        "ST_Area(named)",
        "WHERE ST_Area(col_poly,use_spheroid := true) > 0",
        "st_area_spheroid",
    ),  # validates named argument pushdown in #596
    (
        "ST_Area(named)",
        "WHERE ST_Area(col_poly,use_spheroid := false) > 0",
        "st_area_spheroid",
    ),  # validates named argument pushdown in #596
    ("ST_AsGeoJSON", "WHERE length(ST_AsGeoJSON(col_poly)) > 0", "st_asgeojson"),
    ("ST_AsText", "WHERE length(ST_AsText(col_poly)) > 0", "st_astext"),
    (
        "ST_AsBinary",
        "WHERE ST_AsBinary(col_point) = '\\x010100000000000000000000000000000000000000'::bytea",
        "st_aswkb",
    ),
    (
        "ST_AsBinary",
        "WHERE ST_AsBinary(col_point, 'NDR') = '\\x010100000000000000000000000000000000000000'::bytea",
        "st_aswkb",
    ),
    ("ST_Boundary", "WHERE NOT ST_IsEmpty(ST_Boundary(col_poly))", "st_boundary"),
    ("ST_Buffer", "WHERE NOT ST_IsEmpty(ST_Buffer(col_poly,1,2))", "st_buffer"),
    ("ST_Centroid", "WHERE NOT ST_IsEmpty(ST_Centroid(col_poly))", "st_centroid"),
    ("ST_Collect", "WHERE NOT ST_IsEmpty(ST_Collect(ARRAY[col_poly]))", "st_collect"),
    (
        "ST_CollectionExtract(geom)",
        "WHERE NOT ST_IsEmpty(ST_CollectionExtract(col_poly))",
        "st_collectionextract",
    ),
    (
        "ST_CollectionExtract(geom,int)",
        "WHERE ST_IsEmpty(ST_CollectionExtract(col_poly,1))",
        "st_collectionextract",
    ),
    ("ST_Contains", "WHERE ST_Contains(col_poly, col_poly)", "st_contains"),
    (
        "ST_ContainsProperly",
        "WHERE NOT ST_ContainsProperly(col_poly, col_poly)",
        "st_containsproperly",
    ),
    ("ST_Convexhull", "WHERE NOT ST_IsEmpty(ST_Convexhull(col_line))", "st_convexhull"),
    ("ST_Coveredby", "WHERE ST_Coveredby(col_poly, col_poly)", "st_coveredby"),
    ("ST_Covers", "WHERE ST_Covers(col_poly, col_poly)", "st_covers"),
    ("ST_Crosses", "WHERE NOT ST_Crosses(col_point, col_line)", "st_crosses"),
    (
        "ST_Difference(geometry,geometry)",
        "WHERE ST_IsEmpty(ST_Difference(col_poly,col_poly))",
        "st_difference",
    ),
    (
        "ST_Difference(geometry,geometry,float)",
        "WHERE ST_IsEmpty(ST_Difference(col_poly,col_poly,-1.0))",
        "st_difference",
    ),
    ("ST_Dimension", "WHERE ST_Dimension(col_poly) > 1 ", "st_dimension"),
    ("ST_Disjoint", "WHERE ST_Disjoint(col_point, col_line)", "st_disjoint"),
    ("ST_Distance", "WHERE NOT ST_Distance(col_line, col_poly) > 0", "st_distance"),
    (
        "ST_Distance",
        "WHERE ST_Distance(col_point_typemod, col_point_typemod, true) = 0",
        "st_distance_spheroid",
    ),
    (
        "ST_Distance",
        "WHERE ST_Distance(col_point_typemod, col_point_typemod, false) = 0",
        "st_distance_sphere",
    ),
    ("ST_DWithin", "WHERE ST_DWithin(col_poly, col_line, 0.1)", "st_dwithin"),
    ("ST_EndPoint", "WHERE NOT ST_IsEmpty(ST_EndPoint(col_line))", "st_endpoint"),
    ("ST_Envelope", "WHERE NOT ST_IsEmpty(ST_Envelope(col_point))", "st_envelope"),
    ("ST_Equals", "WHERE ST_Equals(col_point, col_point)", "st_equals"),
    # ("ST_Extent", "WHERE ST_IsEmpty(ST_Extent(col_poly))", "st_extent"),
    (
        "ST_ExteriorRing",
        "WHERE NOT ST_IsEmpty(ST_ExteriorRing(col_poly))",
        "st_exteriorring",
    ),
    (
        "ST_FlipCoordinates",
        "WHERE NOT ST_IsEmpty(ST_FlipCoordinates(col_poly))",
        "st_flipcoordinates",
    ),
    ("ST_Force2d", "WHERE NOT ST_IsEmpty(ST_Force2d(col_poly))", "st_force2d"),
    # Disabled due to faulty DuckDB release
    # ("ST_Force3dm", "WHERE NOT ST_IsEmpty(ST_Force3dm(col_poly,1))", "st_force3dm"),
    # ("ST_Force3dz", "WHERE NOT ST_IsEmpty(ST_Force3dz(col_poly,1))", "st_force3dz"),
    # ("ST_Force4d", "WHERE NOT ST_IsEmpty(ST_Force4d(col_poly,1,2))", "st_force4d"),
    # ("ST_GeometryType", "WHERE ST_GeometryType(col_point) = 'ST_Point'", "st_geometrytype"),
    (
        "ST_GeomFromGeoJSON(json)",
        "WHERE NOT ST_GeomFromGeoJSON(col_json) = col_point",
        "st_geomfromgeojson",
    ),
    (
        "ST_GeomFromGeoJSON(text)",
        "WHERE NOT ST_GeomFromGeoJSON(col_json::text) = col_point",
        "st_geomfromgeojson",
    ),
    (
        "ST_GeomFromText",
        "WHERE ST_GeomFromText(ST_AsText(col_point)) = col_point",
        "st_geomfromtext",
    ),
    (
        "ST_GeometryFromText",
        "WHERE ST_GeometryFromText(ST_AsText(col_point)) = col_point",
        "st_geomfromtext",
    ),
    ("ST_GeomFromWKB", "WHERE ST_GeomFromWKB(col_point) = col_point", "st_geomfromwkb"),
    (
        "ST_Intersection(geometry,geometry)",
        "WHERE NOT ST_IsEmpty(ST_Intersection(col_poly, col_poly))",
        "st_intersection",
    ),
    (
        "ST_Intersection(geometry,geometry,float)",
        "WHERE NOT ST_IsEmpty(ST_Intersection(col_poly, col_poly, -1))",
        "st_intersection",
    ),
    ("ST_Intersects", "WHERE ST_Intersects(col_poly, col_line)", "st_intersects"),
    ("ST_IsClosed", "WHERE NOT ST_IsClosed(col_line)", "st_isclosed"),
    ("ST_IsEmpty", "WHERE NOT ST_IsEmpty(col_poly)", "st_isempty"),
    ("ST_IsRing", "WHERE ST_IsRing(col_line)", "st_isring"),
    ("ST_IsSimple", "WHERE ST_IsSimple(col_poly)", "st_issimple"),
    ("ST_IsValid", "WHERE ST_IsValid(col_poly)", "st_isvalid"),
    ("ST_Length", "WHERE ST_Length(col_line) > 0", "st_length("),
    ("ST_Length", "WHERE ST_Length(col_line,true) > 0.1", "st_length_spheroid"),
    ("ST_Length", "WHERE ST_Length(col_line,false) > 0.1", "st_length_spheroid"),
    # ("ST_LineMerge(geometry)", "WHERE NOT ST_IsEmpty(ST_LineMerge(col_poly))", "st_linemerge"),
    # ("ST_LineMerge(geometry,bool)", "WHERE NOT ST_IsEmpty(ST_LineMerge(col_poly,'t'))", "st_linemerge"),
    (
        "ST_MakeLine(geometry[])",
        "WHERE NOT ST_IsEmpty(ST_MakeLine(ARRAY['POINT(1 0)','POINT(2 1)']::geometry[]))",
        "st_makeline",
    ),
    (
        "ST_MakeLine(geometry,geometry)",
        "WHERE NOT ST_IsEmpty(ST_MakeLine('POINT(1 0)'::geometry,'POINT(2 1)'::geometry))",
        "st_makeline",
    ),
    # ("ST_MakePolygon(geometry)", "WHERE NOT ST_IsEmpty(ST_MakePolygon(col_line,ARRAY['POINT(1 0)','POINT(2 1)']::geometry[]))", "st_makepolygon"),
    # ("ST_MakePolygon(geometry[])", "WHERE NOT ST_IsEmpty(ST_MakePolygon(col_line,ARRAY['POINT(1 0)','POINT(2 1)']::geometry[]))", "st_makepolygon"),
    ("ST_MakeValid", "WHERE NOT ST_IsEmpty(ST_MakeValid(col_poly))", "st_makevalid"),
    ("ST_M", "WHERE ST_M('POINT(1 2 3 4)'::geometry) = 4", "st_m"),
    ("ST_Normalize", "WHERE NOT ST_IsEmpty(ST_Normalize(col_point))", "st_normalize"),
    ("ST_Npoints", "WHERE ST_Npoints(col_point) > 0", "st_npoints"),
    ("ST_NumGeometries", "WHERE ST_NumGeometries(col_poly) > 0", "st_numgeometries"),
    (
        "ST_NumInteriorRings",
        "WHERE ST_NumInteriorRings(col_poly) < 1",
        "st_numinteriorrings",
    ),
    ("ST_Overlaps", "WHERE NOT ST_Overlaps(col_poly, col_poly)", "st_overlaps"),
    ("ST_Perimeter", "WHERE ST_Perimeter(col_poly) > 0", "st_perimeter("),
    (
        "ST_Perimeter",
        "WHERE ST_Perimeter(col_poly,true) > 0.1",
        "st_perimeter_spheroid",
    ),
    (
        "ST_Perimeter",
        "WHERE ST_Perimeter(col_poly,false) > 0.1",
        "st_perimeter_spheroid",
    ),
    ("ST_Point", "WHERE NOT ST_Point(1,2) = ST_Point(3,4)", "st_point"),
    ("ST_PointN", "WHERE NOT ST_IsEmpty(ST_PointN(col_line, 1))", "st_pointn"),
    (
        "ST_PointOnSurface",
        "WHERE NOT ST_IsEmpty(ST_PointOnSurface(col_point))",
        "st_pointonsurface",
    ),
    (
        "ST_ReducePrecision",
        "WHERE NOT ST_IsEmpty(ST_ReducePrecision(col_point,4))",
        "st_reduceprecision",
    ),
    (
        "ST_RemoveRepeatedPoints",
        "WHERE NOT ST_IsEmpty(ST_RemoveRepeatedPoints(col_line))",
        "st_removerepeatedpoints",
    ),
    ("ST_Reverse", "WHERE NOT ST_IsEmpty(ST_Reverse(col_point))", "st_reverse"),
    (
        "ST_ShortestLine",
        "WHERE NOT ST_IsEmpty(ST_ShortestLine(col_line, col_line))",
        "st_shortestline",
    ),
    ("ST_Simplify", "WHERE NOT ST_IsEmpty(ST_Simplify(col_poly, 0.1))", "st_simplify"),
    (
        "ST_SimplifyPreserveTopology",
        "WHERE NOT ST_IsEmpty(ST_SimplifyPreserveTopology(col_point, 0.1))",
        "st_simplifypreservetopology",
    ),
    ("ST_Srid", "WHERE ST_Srid(col_point) = 0", "ELSE 0"),
    ("ST_StartPoint", "WHERE NOT ST_IsEmpty(ST_StartPoint(col_line))", "st_startpoint"),
    ("ST_Touches", "WHERE NOT ST_Touches(col_poly, col_poly)", "st_touches"),
    (
        "ST_Transform",
        "WHERE NOT ST_IsEmpty(ST_Transform(col_poly, 'EPSG:4326', 'EPSG:4326'))",
        "st_transform",
    ),
    ("ST_Union", "WHERE NOT ST_IsEmpty(ST_Union(col_poly, 'POINT(1 3)'))", "st_union"),
    ("ST_Within", "WHERE ST_Within(col_point, col_point)", "st_within"),
    ("ST_X", "WHERE ST_X(col_point) > 0", "st_x"),
    ("ST_Y", "WHERE ST_Y(col_point) > 0", "st_y"),
    ("ST_Z", "WHERE ST_Z('POINT(1 2 3 4)'::geometry) = 3", "st_z"),
    ("ST_Zmflag", "WHERE ST_Zmflag(col_point) = 0", "ELSE '0'::smallint"),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_spatial_function_pushdown(
    create_test_spatial_function_pushdown_table,
    user_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM test_spatial_function_pushdown.tbl " + operator_expression

    assert_remote_query_contains_expression(query, expected_expression, user_conn)
    assert_query_results_on_tables(
        query,
        user_conn,
        ["test_spatial_function_pushdown.tbl"],
        ["test_spatial_function_pushdown.heap_tbl"],
    )


def test_agg_function_pushdown(create_test_spatial_function_pushdown_table, user_conn):

    # try with one column and one constant
    agg_values = ["col_point", "ST_GeomFromText('POINT(0 0)')"]
    for agg_value in agg_values:
        for query in [
            f"SELECT ST_AsBinary(ST_Union({agg_value})) FROM test_spatial_function_pushdown.tbl ",
            f"SELECT ST_AsBinary(ST_Union({agg_value} ORDER BY 1)) FROM test_spatial_function_pushdown.tbl ",
        ]:

            assert_query_results_on_tables(
                query,
                user_conn,
                ["test_spatial_function_pushdown.tbl"],
                ["test_spatial_function_pushdown.heap_tbl"],
            )
            assert_remote_query_contains_expression(query, "st_union_agg", user_conn)


@pytest.fixture(scope="module")
def create_test_spatial_function_pushdown_table(
    user_conn, s3, spatial_analytics_extension, pg_lake_extension
):
    url = f"s3://{TEST_BUCKET}/create_test_spatial_function_pushdown_table/data.parquet"

    run_command(
        f"""
        COPY ( SELECT * FROM (VALUES
          (NULL, NULL::geometry, NULL::geometry, NULL::geometry, NULL::jsonb, NULL::geometry(point,4326)),
          ('', 'POINT(0 0)'::geometry, 'LINESTRING(30 10, 40 40, 20 40, 10 20, 30 10)', 'POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'::geometry, '{{"type":"Point","coordinates":[1,2]}}', 'POINT(5.4 22.3)'::geometry(point,4326)),
          ('pi', 'POINT(3.14 6.28)'::geometry, 'LINESTRING(3 1, 4 4, 2 4, 1 2)', 'POLYGON((3 1, 4 4, 2 4, 1 2, 3 1))'::geometry, '{{"type":"Point","coordinates":[0,0]}}', 'POINT(3.14 6.28)'::geometry(point,4326))
        ) AS res (col_text, col_point, col_area, col_json)) TO '{url}' WITH (FORMAT 'parquet');

        CREATE SCHEMA test_spatial_function_pushdown;
        CREATE FOREIGN TABLE test_spatial_function_pushdown.tbl
        (
            col_text text,
            col_point geometry,
            col_line geometry,
            col_poly geometry,
            col_json json,
            col_point_typemod geometry(point, 4326)
        ) SERVER pg_lake OPTIONS (format 'parquet', path '{url}');

        CREATE TABLE test_spatial_function_pushdown.heap_tbl
        AS SELECT * FROM test_spatial_function_pushdown.tbl
    """,
        user_conn,
    )

    user_conn.commit()

    yield
    user_conn.rollback()

    run_command("DROP SCHEMA test_spatial_function_pushdown CASCADE", user_conn)
    user_conn.commit()

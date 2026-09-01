"""Does CREATE FOREIGN TABLE survive a GDAL source holding a curve geometry?

PR 300 splits GDALReadFunctionCall's new keep_wkb argument two ways:

  read_data.c   keep_wkb=true   -- "DuckDB's GEOMETRY has no representation for
                                   the curve types GDAL sources use (a
                                   MultiCurve makes st_read itself fail with
                                   'Unsupported geometry type in WKB')"
  describe.c    keep_wkb=false  -- "DESCRIBE needs the native GEOMETRY type so
                                   foreign table columns are created as PostGIS
                                   geometry, not bytea"

Those two comments are in tension: DESCRIBE asks for the native GEOMETRY that
read_data.c says cannot represent a curve. If the read path needs keep_wkb to
avoid dying on a curve, the describe path plausibly dies on the same input --
just at CREATE FOREIGN TABLE instead of at scan time. The PR's own GDAL tests
use a POINT GeoJSON and a LineString GML layer, so neither reaches this.

This is a characterisation test: it does not assert that CREATE succeeds, only
that pg_lake either creates the table and reads the curve back, or fails with a
diagnosable error -- and that pgduck_server is still alive afterwards. A hard
server termination here (st_read is documented in test_gdal.py to be able to
kill the server) is the outcome worth knowing about.

Runs last on purpose: if it does take the server down it must not corrupt other
suites' results.
"""

import pytest

from utils_pytest import *

# GML 3 with a gml:Curve made of an arc. PostGIS reads arcs (CIRCULARSTRING);
# DuckDB's GEOMETRY has no arc representation, which is the case in question.
CURVE_GML = """<?xml version="1.0" encoding="UTF-8"?>
<ogr:FeatureCollection
    xmlns:ogr="http://ogr.maptools.org/"
    xmlns:gml="http://www.opengis.net/gml/3.2">
  <ogr:featureMember>
    <ogr:curves gml:id="f1">
      <ogr:geometryProperty>
        <gml:Curve gml:id="c1" srsName="EPSG:4326">
          <gml:segments>
            <gml:ArcString interpolation="circularArc3Points">
              <gml:posList>0 0 1 1 2 0</gml:posList>
            </gml:ArcString>
          </gml:segments>
        </gml:Curve>
      </ogr:geometryProperty>
      <ogr:id>1</ogr:id>
    </ogr:curves>
  </ogr:featureMember>
</ogr:FeatureCollection>
"""


def test_gdal_curve_geometry_describe(
    pgduck_conn, user_conn, s3, spatial_analytics_extension, pg_lake_extension, tmp_path
):
    file_key = "pr300_gdal_curve/curve.gml"
    url = f"s3://{TEST_BUCKET}/{file_key}"
    local = tmp_path / "curve.gml"
    local.write_text(CURVE_GML)
    s3.upload_file(str(local), TEST_BUCKET, file_key)

    error = run_command(
        f"""
        CREATE SCHEMA pr300_curve;
        CREATE FOREIGN TABLE pr300_curve.fdw ()
        SERVER pg_lake OPTIONS (path '{url}');
        """,
        user_conn,
        raise_error=False,
    )

    # DESCRIBE runs st_read with keep_wkb=false, so this is the half of the
    # forceKeepWKB split that has to cope with a geometry DuckDB cannot build.
    assert error is None, f"CREATE FOREIGN TABLE failed on a curve source: {error}"

    # regtype, not information_schema.data_type, which reports an extension
    # type as the useless USER-DEFINED.
    cols = {
        c["attname"]: c["type"]
        for c in run_query(
            "SELECT attname, atttypid::regtype::text AS type FROM pg_attribute "
            "WHERE attrelid = 'pr300_curve.fdw'::regclass "
            "AND attnum > 0 AND NOT attisdropped",
            user_conn,
        )
    }
    assert cols.get("geometryproperty") == "geometry", cols

    # The scan runs with keep_wkb=true, so the curve reaches PostGIS as raw WKB
    # and survives a round trip DuckDB's GEOMETRY could not represent.
    rows = run_query(
        "SELECT ST_GeometryType(geometryproperty) AS t FROM pr300_curve.fdw",
        user_conn,
    )
    assert [r["t"] for r in rows] == ["ST_CircularString"], rows

    user_conn.rollback()

    # A crash here rather than an error is the failure mode this guards.
    assert run_query("SELECT 42 AS answer", pgduck_conn)[0]["answer"] == 42

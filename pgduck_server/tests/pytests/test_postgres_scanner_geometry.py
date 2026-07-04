"""
Verify postgres_scan handles PostGIS geometry columns correctly.

PostGIS emits EWKB (with the SRID flag bit and embedded SRID) for binary
output, but DuckDB spatial expects standard WKB. The scanner rewrites
geometry projections as ST_AsBinary(col) so canonical WKB reaches DuckDB.

Without that rewrite, scans of any geometry column with SRID set fail with
"invalid WKB format".
"""

import pytest
from utils_pytest import *


def _connstr():
    return (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )


def _scan(table, schema="public"):
    return f"postgres_scan('{_connstr()}', '{schema}', '{table}')"


@pytest.fixture(scope="module")
def pg_geom_tables(postgres, postgis_extension):
    """Create PostGIS-backed test tables on the upstream Postgres."""
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS scanner_geom_basic")
    cur.execute(
        "CREATE TABLE scanner_geom_basic (" "  id int PRIMARY KEY," "  g geometry" ")"
    )
    # Mix of SRIDs (0, 4326), 2D/3D/M dimensions, varied types,
    # plus a NULL row to ensure null handling is preserved.
    cur.execute(
        "INSERT INTO scanner_geom_basic VALUES "
        "(1, ST_GeomFromText('POINT(1 2)')),"  # SRID 0
        "(2, ST_GeomFromText('POINT(3 4)', 4326)),"  # SRID 4326
        "(3, ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)', 4326)),"
        "(4, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 4326)),"
        "(5, ST_GeomFromText('POINT Z(1 2 3)', 4326)),"  # 3D
        "(6, ST_GeomFromText('POINT M(1 2 3)', 4326)),"  # M dim
        "(7, NULL)"
    )

    cur.execute("DROP TABLE IF EXISTS scanner_geom_typed")
    cur.execute(
        "CREATE TABLE scanner_geom_typed ("
        "  id int PRIMARY KEY,"
        "  g geometry(Point, 4326)"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_geom_typed VALUES "
        "(1, ST_SetSRID(ST_MakePoint(10, 20), 4326)),"
        "(2, ST_SetSRID(ST_MakePoint(-1.5, 47.0), 4326))"
    )

    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_geom_basic")
    cur.execute("DROP TABLE IF EXISTS scanner_geom_typed")
    cur.close()
    conn.close()


# -------------------------------------------------------------------
# Geometry columns return canonical WKB readable by DuckDB spatial
# -------------------------------------------------------------------


def test_geometry_count(pg_geom_tables, pgduck_conn):
    """Sanity: scan completes and row count matches (no WKB parse error)."""
    scan = _scan("scanner_geom_basic")
    rows = perform_query_on_cursor(f"SELECT count(*) FROM {scan}", pgduck_conn)
    assert rows == [(7,)]


def test_geometry_roundtrip_to_wkt(pg_geom_tables, pgduck_conn):
    """
    DuckDB spatial parses the canonical WKB into GEOMETRY and ST_AsText
    round-trips to WKT. Anything other than valid WKB would fail to parse
    or produce malformed text.

    Note: ST_AsBinary drops SRID, so we compare WKT geometry only.
    """
    scan = _scan("scanner_geom_basic")
    rows = perform_query_on_cursor(
        f"SELECT id, ST_AsText(g) FROM {scan} ORDER BY id",
        pgduck_conn,
    )
    assert rows == [
        (1, "POINT (1 2)"),
        (2, "POINT (3 4)"),
        (3, "LINESTRING (0 0, 1 1, 2 2)"),
        (4, "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
        (5, "POINT Z (1 2 3)"),
        (6, "POINT M (1 2 3)"),
        (7, None),
    ]


def test_geometry_typed_column(pg_geom_tables, pgduck_conn):
    """Typed geometry(Point,4326) columns round-trip the same way."""
    scan = _scan("scanner_geom_typed")
    rows = perform_query_on_cursor(
        f"SELECT id, ST_AsText(g) FROM {scan} ORDER BY id",
        pgduck_conn,
    )
    assert rows == [
        (1, "POINT (10 20)"),
        (2, "POINT (-1.5 47)"),
    ]


def test_geometry_blob_alias(pg_geom_tables, pgduck_conn):
    """The scanned column is a recognized geometry type, not a struct or text."""
    scan = _scan("scanner_geom_typed")
    rows = perform_query_on_cursor(f"SELECT typeof(g) FROM {scan} LIMIT 1", pgduck_conn)
    # DuckDB 1.5+ reports geometry columns as GEOMETRY; older versions used
    # the BLOB-backed WKB_BLOB alias.
    assert rows[0][0] in ("GEOMETRY", "WKB_BLOB", "BLOB")


def test_geometry_filter_pushdown(pg_geom_tables, pgduck_conn):
    """Projection rewrite must coexist with a non-geometry WHERE filter."""
    scan = _scan("scanner_geom_basic")
    rows = perform_query_on_cursor(
        f"SELECT id, ST_AsText(g) FROM {scan} WHERE id = 2",
        pgduck_conn,
    )
    assert rows == [(2, "POINT (3 4)")]

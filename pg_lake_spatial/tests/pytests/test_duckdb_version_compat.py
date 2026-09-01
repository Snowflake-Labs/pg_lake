"""End-to-end checks for paths a DuckDB version change can silently alter.

Geometry read and write, decimal statistics, glob expansion and URL decoding,
including fixtures written by DuckDB 1.4.3 so an older writer's output stays
readable.
"""

import json
import os
import struct
import subprocess

import pytest
from utils_pytest import *

FIXTURES = os.path.join(os.path.dirname(__file__), "duckdb_143_fixtures")

# Interpreter with the pre-bump duckdb (1.4.3) installed, used to prove an
# older external reader still parses what this build writes. See
# duckdb_143_fixtures/generate.py for how to make one.
DUCKDB143_PYTHON = os.environ.get("PG_LAKE_DUCKDB143_PYTHON")


def wkt(s):
    """ST_AsText is pushed down to DuckDB, which spells it "POINT (3 4)"
    where PostGIS spells it "POINT(3 4)". Compare without the spacing."""
    return s.replace(" ", "")


@pytest.fixture(autouse=True)
def _clean_tx(request):
    """user_conn is module-scoped, so one failed statement would otherwise
    abort every later test in this file. Reset the transaction around each."""
    yield
    for name in ("user_conn", "pgduck_conn"):
        if name in request.fixturenames:
            try:
                request.getfixturevalue(name).rollback()
            except Exception:
                pass


# ---------------------------------------------------------------- old writer


@pytest.fixture(scope="module")
def duckdb143_files(s3):
    """Uploads files written by DuckDB 1.4.3, the pin before this PR."""
    for name in (
        "duckdb143_geo.parquet",
        "duckdb143_wkb.parquet",
        "duckdb143_decimal.parquet",
    ):
        s3.upload_file(f"{FIXTURES}/{name}", TEST_BUCKET, f"duckdb_compat/{name}")
    yield


def test_duckdb143_geoparquet_reads_after_bump(
    user_conn, spatial_analytics_extension, duckdb143_files
):
    """A GeoParquet file written by the old DuckDB still reads, as geometry."""
    url = f"s3://{TEST_BUCKET}/duckdb_compat/duckdb143_geo.parquet"
    run_command(
        f"CREATE FOREIGN TABLE compat_duckdb143_geo () SERVER pg_lake OPTIONS (path '{url}');",
        user_conn,
    )

    cols = run_query(
        """
        SELECT attname, atttypid::regtype::text FROM pg_attribute
        WHERE attrelid = 'compat_duckdb143_geo'::regclass AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
        """,
        user_conn,
    )
    assert cols == [["id", "integer"], ["geom", "geometry"]]

    rows = run_query(
        "SELECT id, ST_AsText(geom) AS wkt FROM compat_duckdb143_geo ORDER BY id",
        user_conn,
    )
    assert [wkt(r["wkt"]) for r in rows] == [
        wkt("POINT(3 4)"),
        wkt("LINESTRING(0 0,1 1,2 2)"),
        wkt("POLYGON((0 0,1 0,1 1,0 1,0 0))"),
    ]
    user_conn.rollback()


def test_duckdb143_wkb_blob_reads_after_bump(
    user_conn, spatial_analytics_extension, duckdb143_files
):
    """The plain-WKB shape pg_lake itself writes still reads as geometry."""
    url = f"s3://{TEST_BUCKET}/duckdb_compat/duckdb143_wkb.parquet"
    run_command(
        f"""
        CREATE FOREIGN TABLE compat_duckdb143_wkb (id int, geom geometry)
        SERVER pg_lake OPTIONS (path '{url}');
        """,
        user_conn,
    )
    rows = run_query(
        "SELECT id, ST_AsText(geom) AS wkt FROM compat_duckdb143_wkb ORDER BY id",
        user_conn,
    )
    assert [wkt(r["wkt"]) for r in rows] == [
        wkt("POINT(3 4)"),
        wkt("LINESTRING(0 0,1 1,2 2)"),
    ]
    user_conn.rollback()


# ------------------------------------------------- write path / new writer


@pytest.mark.skipif(
    not (DUCKDB143_PYTHON and os.path.exists(DUCKDB143_PYTHON)),
    reason="set PG_LAKE_DUCKDB143_PYTHON to an interpreter with duckdb 1.4.3",
)
def test_geometry_write_bytes_and_footer(
    user_conn, spatial_analytics_extension, s3, tmp_path
):
    """The written parquet is plain WKB with no CRS-bearing 'geo' key from DuckDB.

    write_data.c projects ST_AsWKB(col), so the column must land as a plain
    BYTE_ARRAY and pg_lake's own geo metadata must be the only one present.
    Read back with the pre-bump DuckDB as well, so an external older reader is
    covered.
    """
    url = f"s3://{TEST_BUCKET}/duckdb_compat/written_geo.parquet"
    run_command(
        f"""
        COPY (
            SELECT 1 AS id, 'SRID=4326;POINT(3 4)'::geometry AS geom
            UNION ALL SELECT 2, 'SRID=4326;LINESTRING(0 0, 1 1)'::geometry
        ) TO '{url}';
        """,
        user_conn,
    )
    user_conn.commit()

    local = str(tmp_path / "written_geo.parquet")
    s3.download_file(TEST_BUCKET, "duckdb_compat/written_geo.parquet", local)

    script = f"""
import duckdb, json
c = duckdb.connect()
c.execute("INSTALL spatial"); c.execute("LOAD spatial")
schema = c.execute("SELECT name, type FROM parquet_schema('{local}')").fetchall()
kv = c.execute("SELECT key, value FROM parquet_kv_metadata('{local}')").fetchall()
# our geo metadata makes 1.4.3 hand back a GEOMETRY, so it reads either way
conv = c.execute("SELECT id, ST_AsText(geom) FROM read_parquet('{local}') ORDER BY id").fetchall()
c.execute("SET enable_geoparquet_conversion = false")
raw_type = c.execute("DESCRIBE SELECT geom FROM read_parquet('{local}')").fetchall()[0][1]
raw = c.execute("SELECT id, ST_AsText(ST_GeomFromWKB(geom)) FROM read_parquet('{local}') ORDER BY id").fetchall()
print(json.dumps({{
  "schema": [[n, t] for n, t in schema],
  "kv": [[k.decode(), v.decode()] for k, v in kv],
  "rows": [[r[0], r[1]] for r in conv],
  "raw_type": raw_type,
  "raw_rows": [[r[0], r[1]] for r in raw],
}}))
"""
    out = (
        subprocess.run(
            [DUCKDB143_PYTHON, "-c", script],
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.strip()
        .splitlines()[-1]
    )
    info = json.loads(out)

    geom_types = [t for n, t in info["schema"] if n == "geom"]
    assert geom_types == ["BYTE_ARRAY"], info["schema"]

    # exactly one geo key, and it is pg_lake's (WKB encoding, no crs)
    geo = [v for k, v in info["kv"] if k == "geo"]
    assert len(geo) == 1, info["kv"]
    geo = json.loads(geo[0])
    assert geo["columns"]["geom"]["encoding"] == "WKB"
    assert "crs" not in geo["columns"]["geom"], geo

    # the old reader still parses the bytes, converted and raw
    expected = [[1, wkt("POINT(3 4)")], [2, wkt("LINESTRING(0 0,1 1)")]]
    assert [[i, wkt(t)] for i, t in info["rows"]] == expected
    assert info["raw_type"] == "BLOB", info["raw_type"]
    assert [[i, wkt(t)] for i, t in info["raw_rows"]] == expected


def test_geometry_round_trip_srid_preserved(user_conn, spatial_analytics_extension, s3):
    """SRID survives a write/read cycle through the column typmod."""
    url = f"s3://{TEST_BUCKET}/duckdb_compat/srid_roundtrip.parquet"
    run_command(
        f"""
        COPY (SELECT 1 AS id, 'SRID=4326;POINT(3 4)'::geometry AS geom)
        TO '{url}';
        CREATE FOREIGN TABLE compat_srid (id int, geom geometry(Point, 4326))
        SERVER pg_lake OPTIONS (path '{url}');
        """,
        user_conn,
    )
    rows = run_query(
        "SELECT ST_SRID(geom) AS srid, ST_AsText(geom) AS wkt FROM compat_srid",
        user_conn,
    )
    assert rows[0]["srid"] == 4326
    assert wkt(rows[0]["wkt"]) == wkt("POINT(3 4)")
    user_conn.rollback()


# ----------------------------------------------------- decimal / bool stats


def test_decimal38_stats_after_dropping_return_stats_patch(
    user_conn, extension, duckdb143_files
):
    """DECIMAL(38,0) min/max must not prune correct rows.

    patches/duckdb/return_stats.patch carried a big-endian FLBA bounds fix and
    is dropped here as upstream in 1.5.5. If the upstream fix is not equivalent,
    a filter on a 38-digit decimal prunes row groups wrongly and rows vanish.
    """
    url = f"s3://{TEST_BUCKET}/duckdb_compat/duckdb143_decimal.parquet"
    run_command(
        f"CREATE FOREIGN TABLE compat_dec () SERVER pg_lake OPTIONS (path '{url}');",
        user_conn,
    )

    # Bare `numeric` ships to DuckDB as DECIMAL(38,3) (the
    # pg_lake_table.unbounded_numeric_default_* GUCs), which cannot hold 38
    # integral digits, so every literal here carries an explicit typmod. Note
    # `-X::numeric(38,0)` negates *after* the cast and lands back on unbounded
    # numeric, hence the parenthesised form.
    pos = "99999999999999999999999999999999999999::numeric(38,0)"
    neg = "(-99999999999999999999999999999999999999)::numeric(38,0)"
    zero = "0::numeric(38,0)"
    assert run_query("SELECT count(*) AS c FROM compat_dec", user_conn)[0]["c"] == 3

    for predicate, expected in [
        (f"d = {pos}", [1]),
        (f"d = {neg}", [2]),
        (f"d = {zero}", [3]),
        (f"d > {zero}", [1]),
        (f"d < {zero}", [2]),
        (f"d >= {neg}", [1, 2, 3]),
        ("b IS TRUE", [1]),
        ("b IS FALSE", [2]),
        ("b IS NULL", [3]),
    ]:
        got = [
            r["id"]
            for r in run_query(
                f"SELECT id FROM compat_dec WHERE {predicate} ORDER BY id", user_conn
            )
        ]
        assert got == expected, f"{predicate} -> {got}, want {expected}"

    user_conn.rollback()


def test_written_decimal38_stats_roundtrip(user_conn, extension, s3):
    """Same, for a file this build writes: stats must match the values."""
    url = f"s3://{TEST_BUCKET}/duckdb_compat/written_decimal.parquet"
    pos = "99999999999999999999999999999999999999::numeric(38,0)"
    neg = "(-99999999999999999999999999999999999999)::numeric(38,0)"
    zero = "0::numeric(38,0)"
    run_command(
        f"""
        COPY (
            SELECT 1 AS id, {pos} AS d
            UNION ALL SELECT 2, {neg}
            UNION ALL SELECT 3, {zero}
        ) TO '{url}';
        CREATE FOREIGN TABLE compat_wdec () SERVER pg_lake OPTIONS (path '{url}');
        """,
        user_conn,
    )
    for predicate, expected in [
        (f"d = {pos}", [1]),
        (f"d = {neg}", [2]),
        # arithmetic on numeric(38,0) yields unbounded numeric, so spell it out
        ("d > 99999999999999999999999999999999999998::numeric(38,0)", [1]),
        (f"d = {zero}", [3]),
    ]:
        got = [
            r["id"]
            for r in run_query(
                f"SELECT id FROM compat_wdec WHERE {predicate} ORDER BY id", user_conn
            )
        ]
        assert got == expected, f"{predicate} -> {got}, want {expected}"
    user_conn.rollback()


# ----------------------------------------------- virtual column stats patch


@pytest.mark.location_prefix(f"s3://{TEST_BUCKET}/compat_evo")
def test_added_column_filter_after_schema_evolution(
    pg_conn, extension, with_default_location, s3
):
    """patches/duckdb/parquet-virtual-column-stats.patch.

    Files written before an ADD COLUMN have no row-group stats for the new
    column. Reading stats for a column that is not in the row group is the
    out-of-bounds the patch guards.
    """
    run_command(
        """
        CREATE TABLE compat_evo (id int) USING iceberg;
        INSERT INTO compat_evo VALUES (1), (2);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        ALTER TABLE compat_evo ADD COLUMN added_int bigint;
        ALTER TABLE compat_evo ADD COLUMN added_text text;
        INSERT INTO compat_evo VALUES (3, 30, 'c');
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert run_query("SELECT count(*) AS c FROM compat_evo", pg_conn)[0]["c"] == 3
    got = [
        r["id"]
        for r in run_query(
            "SELECT id FROM compat_evo WHERE added_int IS NULL ORDER BY id", pg_conn
        )
    ]
    assert got == [1, 2]
    got = [
        r["id"]
        for r in run_query(
            "SELECT id FROM compat_evo WHERE added_int = 30 ORDER BY id", pg_conn
        )
    ]
    assert got == [3]
    got = [
        r["id"]
        for r in run_query(
            "SELECT id FROM compat_evo WHERE added_text = 'c' ORDER BY id", pg_conn
        )
    ]
    assert got == [3]
    run_command("DROP TABLE compat_evo", pg_conn)
    pg_conn.commit()


# ------------------------------------------------------------ glob handling


def test_literal_glob_chars_in_path(user_conn, extension, s3):
    """A key whose name contains * reads as a literal path."""
    key = "duckdb_compat/specialChars!@#$%^&*()_+/data.csv"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"id,name\n1,a\n2,b\n")

    url = f"s3://{TEST_BUCKET}/{key}"
    run_command(
        f"""
        CREATE FOREIGN TABLE compat_special (id int, name text)
        SERVER pg_lake OPTIONS (path '{url}', format 'csv', header 'true');
        """,
        user_conn,
    )
    rows = run_query("SELECT id, name FROM compat_special ORDER BY id", user_conn)
    assert [(r["id"], r["name"]) for r in rows] == [(1, "a"), (2, "b")]
    user_conn.rollback()


def test_glob_still_expands_with_special_chars_present(user_conn, extension, s3):
    """The exact-file fallback must not shadow a real wildcard expansion."""
    for i in (1, 2):
        s3.put_object(
            Bucket=TEST_BUCKET,
            Key=f"duckdb_compat/globdir/part{i}.csv",
            Body=f"id,name\n{i},n{i}\n".encode(),
        )
    url = f"s3://{TEST_BUCKET}/duckdb_compat/globdir/*.csv"
    run_command(
        f"""
        CREATE FOREIGN TABLE compat_glob (id int, name text)
        SERVER pg_lake OPTIONS (path '{url}', format 'csv', header 'true');
        """,
        user_conn,
    )
    rows = run_query("SELECT id FROM compat_glob ORDER BY id", user_conn)
    assert [r["id"] for r in rows] == [1, 2]
    user_conn.rollback()


def test_url_encoded_prefix_partition(user_conn, extension, s3):
    """A partition directory with a space: prefixes come back percent-encoded."""
    s3.put_object(
        Bucket=TEST_BUCKET, Key="duckdb_compat/spacedir/a b/data.csv", Body=b"id\n7\n"
    )
    url = f"s3://{TEST_BUCKET}/duckdb_compat/spacedir/**/*.csv"
    run_command(
        f"""CREATE FOREIGN TABLE compat_space (id int) SERVER pg_lake
            OPTIONS (path '{url}', format 'csv', header 'true');""",
        user_conn,
    )
    rows = run_query("SELECT id FROM compat_space", user_conn)
    assert [r["id"] for r in rows] == [7]
    user_conn.rollback()


# ------------------------------------------------------------- gdal / layer


def test_missing_gdal_layer_errors_and_server_survives(
    user_conn, spatial_analytics_extension, pgduck_conn, s3
):
    """A layer that does not exist must error, not take pgduck_server down."""
    geojson = json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "a"},
                    "geometry": {"type": "Point", "coordinates": [3, 4]},
                }
            ],
        }
    ).encode()
    s3.put_object(Bucket=TEST_BUCKET, Key="duckdb_compat/layers.geojson", Body=geojson)
    url = f"s3://{TEST_BUCKET}/duckdb_compat/layers.geojson"

    error = run_command(
        f"""
        CREATE FOREIGN TABLE compat_badlayer () SERVER pg_lake
        OPTIONS (path '{url}', format 'gdal', layer 'no_such_layer');
        """,
        user_conn,
        raise_error=False,
    )
    assert error is not None, "creating a table on a missing layer should fail"
    assert "no_such_layer" in error, error
    user_conn.rollback()

    # the server is still there and still serving
    assert run_query("SELECT 1 AS one", pgduck_conn)[0]["one"] == 1
    assert run_query("SELECT count(*) AS c FROM (SELECT 1) t", user_conn)[0]["c"] == 1
    user_conn.rollback()


def test_gdal_geometry_reads_and_is_not_shipped(
    user_conn, spatial_analytics_extension, s3
):
    """A GDAL geometry column is hex WKB on the DuckDB side, so no pushdown."""
    geojson = json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "a"},
                    "geometry": {"type": "Point", "coordinates": [3, 4]},
                }
            ],
        }
    ).encode()
    s3.put_object(Bucket=TEST_BUCKET, Key="duckdb_compat/gdal_pt.geojson", Body=geojson)
    url = f"s3://{TEST_BUCKET}/duckdb_compat/gdal_pt.geojson"

    run_command(
        f"""
        CREATE FOREIGN TABLE compat_gdal () SERVER pg_lake
        OPTIONS (path '{url}', format 'gdal');
        """,
        user_conn,
    )
    cols = dict(
        (r["attname"], r["atttypid"])
        for r in run_query(
            """
            SELECT attname, atttypid::regtype::text AS atttypid FROM pg_attribute
            WHERE attrelid = 'compat_gdal'::regclass AND attnum > 0 AND NOT attisdropped
            """,
            user_conn,
        )
    )
    assert cols.get("geom") == "geometry", cols

    rows = run_query("SELECT ST_AsText(geom) AS wkt FROM compat_gdal", user_conn)
    assert wkt(rows[0]["wkt"]) == wkt("POINT(3 4)")

    # a spatial function over the GDAL geometry evaluates locally, and is right
    rows = run_query(
        "SELECT ST_X(geom) AS x, ST_Y(geom) AS y FROM compat_gdal", user_conn
    )
    assert (rows[0]["x"], rows[0]["y"]) == (3.0, 4.0)
    user_conn.rollback()

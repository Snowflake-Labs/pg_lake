"""Regenerate the fixtures in this directory with the pre-bump DuckDB (1.4.3),
so the current build reads them as an older writer's output.

    python3 -m venv /tmp/duckdb143venv
    /tmp/duckdb143venv/bin/pip install 'duckdb==1.4.3'
    /tmp/duckdb143venv/bin/python generate.py

The same interpreter is what PG_LAKE_DUCKDB143_PYTHON should point at when running
test_duckdb_version_compat.py.
"""

import duckdb, os

out = os.path.dirname(os.path.abspath(__file__))
c = duckdb.connect()
c.execute("INSTALL spatial")
c.execute("LOAD spatial")
print("writer duckdb", duckdb.__version__)

# 1. GeoParquet written by duckdb 1.4.3's own writer, with CRS metadata
c.execute(
    f"""
COPY (
  SELECT 1 AS id, ST_Point(3, 4) AS geom
  UNION ALL SELECT 2, ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')
  UNION ALL SELECT 3, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')
) TO '{out}/duckdb143_geo.parquet' (FORMAT parquet)
"""
)

# 2. plain WKB blob parquet, the shape pg_lake itself writes (ST_AsWKB)
c.execute(
    f"""
COPY (
  SELECT 1 AS id, ST_AsWKB(ST_Point(3, 4)) AS geom
  UNION ALL SELECT 2, ST_AsWKB(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))
) TO '{out}/duckdb143_wkb.parquet' (FORMAT parquet)
"""
)

# 3. DECIMAL(38,0) at the edges, to exercise the FLBA big-endian stats path
#    that return_stats.patch used to fix and 1.5.5 is claimed to fix upstream
c.execute(
    f"""
COPY (
  SELECT 1 AS id, 99999999999999999999999999999999999999::DECIMAL(38,0) AS d, true AS b
  UNION ALL SELECT 2, -99999999999999999999999999999999999999::DECIMAL(38,0), false
  UNION ALL SELECT 3, 0::DECIMAL(38,0), NULL
) TO '{out}/duckdb143_decimal.parquet' (FORMAT parquet)
"""
)

for f in [
    "duckdb143_geo.parquet",
    "duckdb143_wkb.parquet",
    "duckdb143_decimal.parquet",
]:
    print(f, os.path.getsize(f"{out}/{f}"), "bytes")

meta = c.execute(
    f"SELECT key, value FROM parquet_kv_metadata('{out}/duckdb143_geo.parquet')"
).fetchall()
print("geo footer keys:", [(k.decode(), v.decode()[:120]) for k, v in meta])

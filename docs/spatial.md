---
title: Geospatial
parent: User guide
nav_order: 4
---

# Geospatial
{: .no_toc }

pg_lake combines [PostGIS](https://postgis.net/) with the geospatial capabilities of the data
lake. You can query almost any geospatial data set in object storage or on the web with one
statement, store geometry in Iceberg tables, run spatial filters and joins on DuckDB's
vectorized engine, and export results as GeoParquet for other tools, while all of PostGIS
remains available.

1. TOC
{:toc}

## Set up

Geospatial support is in the `pg_lake_spatial` extension, which requires PostGIS to be installed
on the server:

```sql
CREATE EXTENSION pg_lake_spatial CASCADE;
NOTICE:  installing required extension "postgis"
CREATE EXTENSION
```

Without `pg_lake_spatial`, creating a table on a GeoParquet file fails with an error that asks you to create the extension, and Iceberg tables cannot have `geometry` columns.

## What you can do

- **Query geospatial files in place**, from object storage or public URLs, without downloading
  them: GeoParquet (including [Overture Maps](https://overturemaps.org/)), GeoJSON and GeoJSONSeq,
  Shapefiles, GeoPackage, FlatGeobuf, KML, File Geodatabase and other formats supported by
  [GDAL](https://gdal.org/en/latest/drivers/vector/index.html), and WKB or WKT columns in
  regular Parquet, CSV and JSON files.
- **Store geometry in Iceberg tables**, so large spatial data sets are compressed, columnar,
  transactional and readable by other engines.
- **Run spatial queries on DuckDB.** Common PostGIS functions and operators, such as
  `ST_Intersects`, `ST_DWithin`, `ST_Contains` and `&&`, are pushed down to DuckDB, which
  evaluates them in parallel over Parquet files.
- **Use PostGIS for everything else**, including GiST indexes, geography, raster and topology,
  on regular tables loaded from the same files.
- **Export GeoParquet** with `COPY ... TO`, for QGIS, GeoPandas, DuckDB or Snowflake.

The [geospatial analytics use case](use-case-geospatial.md) combines these in one workflow, from
public data sets to a map in QGIS.

## Bringing geospatial data into PostgreSQL

There are three ways to work with a geospatial data set:

```sql
-- 1) query a remote data set in place, with columns inferred from the file
CREATE FOREIGN TABLE countries () SERVER pg_lake
OPTIONS (path 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson');

\d countries
                      Foreign table "public.countries"
      Column       |   Type   | Collation | Nullable | Default | FDW options
-------------------+----------+-----------+----------+---------+-------------
 ogc_fid           | bigint   |           |          |         |
 name              | text     |           |          |         |
 iso3166-1-alpha-3 | text     |           |          |         |
 iso3166-1-alpha-2 | text     |           |          |         |
 geom              | geometry |           |          |         |

SELECT name, round(ST_Area(geom::geography) / 1e6) AS km2
FROM countries ORDER BY 2 DESC LIMIT 3;

    name    |   km2
------------+----------
 Russia     | 16980200
 Antarctica | 12358174
 Canada     |  9945629

-- 2) create a regular table from the data set, and index it
CREATE TABLE countries_local ()
WITH (load_from = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson');
CREATE INDEX ON countries_local USING gist (geom);

-- 3) load the data set into an existing table
COPY countries_local FROM 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson';
```

### Choosing a table type

| | Foreign table on files | Iceberg table | Regular table with a GiST index |
|:--|:--|:--|:--|
| Best for | Exploring, one-off queries | Large data sets, scans and aggregations | Precise lookups, such as "which area contains this point?" |
| Spatial filters | Pushed down to DuckDB | Pushed down to DuckDB | Index scan in PostGIS |
| Updates | No | Yes | Yes |

A common pattern is to keep large point data sets in Iceberg and smaller polygon sets, such
as boundaries, in regular tables with a GiST index. Joins within one table type are usually
faster than joins across types, since a join between an Iceberg table and a regular table runs
in PostgreSQL.

You can also build a regular materialized view with a spatial index on top of lake data, and
refresh it periodically with [pg_cron](https://github.com/citusdata/pg_cron).

## GeoParquet and Overture Maps

[GeoParquet](https://geoparquet.org/) stores geometry as WKB in Parquet, with metadata that
identifies the geometry columns. pg_lake maps those columns to PostGIS `geometry`
automatically.

[Overture Maps](https://docs.overturemaps.org/) publishes worldwide places, buildings,
transportation, addresses and administrative boundaries as GeoParquet in a public S3 bucket in
`us-west-2`, which pg_lake can read without credentials. Overture keeps only recent releases
in the bucket; replace `2026-09-23.0` in these examples with a
[current release](https://docs.overturemaps.org/release-calendar/).

```sql
CREATE FOREIGN TABLE ov_places () SERVER pg_lake
OPTIONS (path 's3://overturemaps-us-west-2/release/2026-09-23.0/theme=places/type=place/*.parquet');
```

Overture files have a `bbox` column with the bounding box of each feature. Filtering on it lets
DuckDB skip most of the data set using the Parquet statistics, so a query over the worldwide
data set only reads the parts it needs:

```sql
-- cafes near the center of Amsterdam
SELECT (names).primary AS name, ST_AsText(geometry)
FROM ov_places
WHERE (bbox).xmin >= 4.88 AND (bbox).xmax <= 4.90
  AND (bbox).ymin >= 52.36 AND (bbox).ymax <= 52.38
  AND basic_category = 'cafe'
LIMIT 3;

       name        |                  st_astext
-------------------+----------------------------------------------
 Café PC           | POINT (4.880073638841409 52.360015605566495)
 Le Patron         | POINT (4.89127572 52.3602098)
 Wakuli            | POINT (4.89146402 52.3608083)
```

Nested Parquet structs, such as `names` and `bbox`, become composite types; use parentheses
to access their fields. Running pg_lake in `us-west-2` avoids cross-region transfer costs,
and the [file cache](performance.md#file-cache) keeps repeatedly used files on local disk.

## GDAL formats: Shapefile, GeoPackage and more

Geospatial data is published in many formats. The [GDAL](https://gdal.org/) library reads most
of them, and pg_lake uses it when you set `format 'gdal'`, or when the file extension implies it:

| Extension | Format | Compression |
|:--|:--|:--|
| `.zip` | Shapefile, File Geodatabase or other GDAL formats | zip |
| `.geojson` | GeoJSON | none |
| `.geojson.gz` | GeoJSON | gzip |
| `.gpkg` | GeoPackage | none |
| `.gpkg.gz` | GeoPackage | gzip |
| `.kml` | KML | none |
| `.kmz` | KML | zip |
| `.fgb` | FlatGeobuf | none |

The [file formats reference](file-formats-reference.md#gdal-format) has a longer list. For a URL
without a recognizable extension, specify the format and compression:

```sql
-- a zip file containing a Shapefile
CREATE FOREIGN TABLE nld () SERVER pg_lake
OPTIONS (
  format 'gdal',
  compression 'zip',
  path 'https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/netherlands-shapefile/at_download/file'
);
```

When a zip archive contains several data sets, select one with `zip_path`, and select a layer
of a multi-layer file with `layer`:

```sql
-- the 1km grid from a zip that contains several grids
CREATE FOREIGN TABLE nld_1km () SERVER pg_lake
OPTIONS (
  format 'gdal',
  compression 'zip',
  zip_path 'nl_1km.shp',
  path 'https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/netherlands-shapefile/at_download/file'
);
```

GDAL files are downloaded when you create the table, which can take a while for large files,
and are then served from the cache. They are only downloaded again when evicted from the
cache.

### GeoJSON

GeoJSON comes in two forms. For a single `FeatureCollection` object, use GDAL (the default for
`.geojson` files), which turns the feature properties into columns, as in the countries example
above. For newline-delimited GeoJSON (GeoJSONSeq, one feature per line), GDAL works too, but
`format 'json'` is faster. It does not detect the geometry, so convert it with
`ST_GeomFromGeoJSON`:

```sql
CREATE FOREIGN TABLE features () SERVER pg_lake
OPTIONS (path 's3://mybucket/features/*.geojsonl', format 'json');

SELECT properties->>'name', ST_GeomFromGeoJSON(geometry) FROM features;
```

## Geometry in Parquet, CSV and JSON files

Regular data files often contain geometry encoded as WKB or WKT. pg_lake reads and writes
geometry columns in each format with a standard encoding:

| Format | Encoding | Decode with |
|:--|:--|:--|
| Parquet | WKB, with GeoParquet metadata | Detected automatically |
| CSV | WKT | `ST_GeomFromText` |
| JSON | GeoJSON | `ST_GeomFromGeoJSON` |
| GDAL | Native | Detected automatically |

If you declare a `geometry` column explicitly, pg_lake decodes it for you:

```sql
-- write a CSV file with a geometry column, which is encoded as WKT
COPY (SELECT 'POINT(3.14 6.28)'::geometry AS geom) TO 's3://mybucket/demo.csv' WITH (header);

-- inferred columns: geom is text, decode it yourself
CREATE FOREIGN TABLE csv_text () SERVER pg_lake OPTIONS (path 's3://mybucket/demo.csv');
SELECT ST_AsText(ST_GeomFromText(geom)) FROM csv_text;

-- declared columns: geom is decoded as geometry
CREATE FOREIGN TABLE csv_geom (geom geometry) SERVER pg_lake
OPTIONS (path 's3://mybucket/demo.csv', header 'true');
SELECT ST_AsText(geom) FROM csv_geom;
```

When you declare the columns of a CSV table, also set `header` if the file has one: the CSV
format is only detected automatically when pg_lake infers the columns.

### Writing GeoParquet

`COPY ... TO` a Parquet file writes geometry as WKB, and adds
[GeoParquet 1.1](https://geoparquet.org/releases/v1.1.0/) metadata, so tools such as QGIS,
GeoPandas and DuckDB recognize the geometry columns. A column declared with a geometry type,
such as `geometry(Point)`, is also recorded as that type in the metadata:

```sql
COPY (SELECT name, category, geom FROM amsterdam_places WHERE category = 'cafe')
TO 's3://mybucket/exports/amsterdam_cafes.parquet';
```

Writing in GDAL formats, such as Shapefile or GeoJSON, is not supported.

## Geometry in Iceberg tables

Iceberg tables can have `geometry` columns. The values are stored as WKB in Iceberg `binary`
columns, which other engines can decode with their own WKB functions:

```sql
CREATE TABLE places (
  id bigint,
  name text,
  category text,
  geom geometry
)
USING iceberg;

INSERT INTO places VALUES (1, 'Dam Square', 'landmark', ST_Point(4.8932, 52.3731));
```

A few things to keep in mind:

- **Supported geometry types** are `Point`, `LineString`, `Polygon`, `MultiPoint`,
  `MultiLineString`, `MultiPolygon` and `GeometryCollection`. Curved geometries are not
  supported.
- **Geometry cannot be nested** in an array or composite type.
- **SRIDs.** WKB does not include an SRID, so values in a plain `geometry` column read back
  with an SRID of 0. Declare the SRID in the column type, such as `geometry(Point, 4326)`, to
  have PostGIS apply it on read.
- **File pruning** does not use geometry columns. For large tables that are often filtered by
  area, add bounding box columns (for example with `ST_XMin(geom)`) or a region column, and
  filter on those in addition to the spatial predicate. Partitioning by a region column also
  works well.

## Spatial query pushdown

The following PostGIS functions and operators run in DuckDB when a query on lake tables uses
them, so spatial filters and joins are evaluated in parallel, next to the data:

| Category | Functions and operators |
|:--|:--|
| Predicates | `ST_Intersects`, `ST_Contains`, `ST_ContainsProperly`, `ST_Within`, `ST_Covers`, `ST_CoveredBy`, `ST_Crosses`, `ST_Disjoint`, `ST_Equals`, `ST_Overlaps`, `ST_Touches`, `ST_DWithin`, `&&`, `=` |
| Measurement | `ST_Area`, `ST_Length`, `ST_Perimeter`, `ST_Distance`, `<->` |
| Constructors | `ST_Point`, `ST_MakeLine`, `ST_MakePolygon`, `ST_MakeEnvelope`, `ST_Collect`, `ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON` |
| Processing | `ST_Buffer`, `ST_Centroid`, `ST_ConvexHull`, `ST_Envelope`, `ST_Intersection`, `ST_Difference`, `ST_Union`, `ST_Simplify`, `ST_SimplifyPreserveTopology`, `ST_MakeValid`, `ST_Transform`, `ST_Boundary`, `ST_PointOnSurface`, `ST_ReducePrecision`, `ST_ShortestLine` |
| Accessors | `ST_X`, `ST_Y`, `ST_Z`, `ST_M`, `ST_NPoints`, `ST_NumGeometries`, `ST_GeometryType`, `ST_IsValid`, `ST_IsEmpty`, `ST_StartPoint`, `ST_EndPoint`, `ST_ExteriorRing`, and others |
| Output | `ST_AsText`, `ST_AsBinary`, `ST_AsGeoJSON` |

Some functions are only pushed down for specific argument types; `EXPLAIN (VERBOSE)` shows what
ran where. Here a radius search on an Iceberg table runs entirely in DuckDB:

```sql
EXPLAIN (VERBOSE, COSTS OFF)
SELECT category, count(*) FROM amsterdam_places
WHERE ST_DWithin(geom, ST_Point(4.8932, 52.3731), 0.005)
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

 Custom Scan (Query Pushdown)
   Engine: DuckDB
   Vectorized SQL:  SELECT "category",
     "count"(*) AS "count"
    FROM public.amsterdam_places "amsterdam_places"("id", "name", "category", "geom")
   WHERE "st_dwithin"("geom", "st_point"((4.8932)::double precision, (52.3731)::double precision), (0.005)::double precision)
   GROUP BY "category"
   ORDER BY ("count"(*)) DESC
  LIMIT (5)::bigint
   ->  TOP_N
         ...
                     ->  READ_PARQUET
                           Filters: ST_DWithin(st_geomfromwkb(geom), POINT (4.8932 52.3731))
```

### Limitations of spatial pushdown

pg_lake uses [DuckDB spatial](https://duckdb.org/docs/stable/core_extensions/spatial/overview),
which mimics PostGIS but differs from it in places. pg_lake only pushes down functions whose
results match PostGIS, and runs everything else in PostGIS, so results are always correct.
Running a function in PostGIS means transferring the geometry from DuckDB, which is slower,
and usually means the rest of the expression runs in PostGIS as well. Some things to be aware
of:

- **SRIDs are not tracked in DuckDB.** Functions such as `ST_SetSRID` run in PostGIS. Prefer
  `ST_Point(x, y)` over `ST_SetSRID(ST_MakePoint(x, y), 4326)` in filters, or declare the SRID
  on the column type.
- **Geography** is not pushed down. Distances in meters with `::geography` run in PostGIS,
  after DuckDB has applied any pushed-down filters.
- **Text output** of pushed-down functions follows DuckDB's formatting. For example, `ST_AsText`
  returns `POINT (4.89 52.37)`, with a space after the type name, where PostGIS returns
  `POINT(4.89 52.37)`.

## Mapping tools

[QGIS](https://qgis.org/) and other GIS tools connect to pg_lake like to any PostgreSQL
database, and show tables and views with geometry columns as layers. Declare the SRID on
geometry columns, such as `geometry(Point, 4326)`, so that these tools can detect the
coordinate system. See [visualizing in QGIS](use-case-geospatial.md#visualize-in-qgis) for a
walkthrough.

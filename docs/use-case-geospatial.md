---
title: Geospatial analytics
parent: Use cases
nav_order: 4
---

# Geospatial analytics on public data
{: .no_toc }

A lot of geospatial data is published as files: open map data as GeoParquet in object storage,
government boundaries and observations as Shapefiles or GeoPackages on the web. pg_lake lets
you query those files where they are, keep the parts you need in your own tables, analyze them
with PostGIS and DuckDB, and look at the results in [QGIS](https://qgis.org/).

The examples on this page use two sources:

- [Overture Maps](https://overturemaps.org/), worldwide places and administrative areas as
  GeoParquet in a public S3 bucket.
- The [US Forest Service](https://data.fs.usda.gov/geodata/edw/datasets.php), national forest
  boundaries and fire occurrences as zipped Shapefiles on a website.

The same steps work for any data set in a [format that pg_lake reads](spatial.md#bringing-geospatial-data-into-postgresql).

1. TOC
{:toc}

## Set up

You need pg_lake with [pg_lake_spatial](spatial.md#set-up):

```sql
CREATE EXTENSION pg_lake_spatial CASCADE;
```

The Overture bucket is in `us-west-2` and can be read without credentials. Running pg_lake in
the same region makes the Overture queries faster and avoids data transfer charges.

## Query the data where it is

### Overture Maps on S3

Create foreign tables for Overture's places and administrative areas. Overture keeps only
recent releases in the bucket, so replace `2026-09-23.0` with a
[current release](https://docs.overturemaps.org/release-calendar/):

```sql
CREATE FOREIGN TABLE ov_places () SERVER pg_lake
OPTIONS (path 's3://overturemaps-us-west-2/release/2026-09-23.0/theme=places/type=place/*.parquet');

CREATE FOREIGN TABLE ov_division_areas () SERVER pg_lake
OPTIONS (path 's3://overturemaps-us-west-2/release/2026-09-23.0/theme=divisions/type=division_area/*.parquet');
```

The columns, including the `geometry` column and nested structs such as `names` and `bbox`,
are inferred from the files. Every Overture feature has a bounding box in `bbox`. Filtering on
it lets DuckDB skip most of the files using Parquet statistics, so you can query the worldwide
data set without reading all of it:

```sql
-- which kinds of areas cover Dam Square in Amsterdam?
SELECT DISTINCT subtype, (names).primary AS name
FROM ov_division_areas
WHERE country = 'NL'
  AND (bbox).xmin <= 4.8932 AND (bbox).xmax >= 4.8932
  AND (bbox).ymin <= 52.3731 AND (bbox).ymax >= 52.3731
  AND ST_Contains(geometry, ST_Point(4.8932, 52.3731))
ORDER BY 1;

  subtype  |     name
-----------+---------------
 country   | Nederland
 county    | Amsterdam
 locality  | Amsterdam
 macrohood | Centrum
 microhood | Dam
 region    | Noord-Holland
```

### Shapefiles on the web

A zipped Shapefile on a website is also one `CREATE FOREIGN TABLE` away. pgduck_server
downloads the file once and keeps it in its [file cache](performance.md#file-cache):

```sql
-- National Forest System boundaries
CREATE FOREIGN TABLE usfs_forests () SERVER pg_lake
OPTIONS (path 'https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.AdministrativeForest.zip');

-- fire occurrence points since 1984
CREATE FOREIGN TABLE usfs_fires () SERVER pg_lake
OPTIONS (path 'https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.MTBS_FIRE_OCCURRENCE_PT.zip');

SELECT fire_name, ig_date, acres FROM usfs_fires ORDER BY acres DESC LIMIT 3;
```

### Coordinate systems

Geometry read from files has an SRID of 0: pg_lake does not know which coordinate reference
system the coordinates are in. Check the data set's documentation. Overture uses WGS 84
(EPSG:4326), and these Forest Service Shapefiles use NAD83 (EPSG:4269), as their `.prj` files
say. You declare the SRID when you copy the data into tables, in the next step.

## Extract what you need into tables

Queries against remote files read them over the network every time, or from the file cache,
and they cannot use indexes. For repeated analysis, copy the area and the columns you need into
your own tables, and choose the table type by how you will use the data:

- **Large data sets that you scan and aggregate** go into Iceberg tables: compressed, columnar
  and queried on DuckDB.
- **Polygons that you look up or join against** go into regular tables with a GiST index.

Declare the SRID in the column type, such as `geometry(Point, 4326)`. PostGIS functions then
know the coordinate system, and QGIS detects it when you add the table as a layer.

Places in Amsterdam, a large point data set, go into an Iceberg table:

```sql
CREATE TABLE amsterdam_places USING iceberg AS
SELECT id, (names).primary AS name, basic_category AS category,
       ST_SetSRID(geometry, 4326)::geometry(Point, 4326) AS geom
FROM ov_places
WHERE (bbox).xmin >= 4.73 AND (bbox).xmax <= 5.07
  AND (bbox).ymin >= 52.28 AND (bbox).ymax <= 52.43;

SELECT 52667
```

Amsterdam's neighborhood boundaries go into a regular table with a GiST index:

```sql
CREATE TABLE amsterdam_neighborhoods AS
SELECT id, (names).primary AS name,
       ST_SetSRID(geometry, 4326)::geometry(Geometry, 4326) AS geom
FROM ov_division_areas
WHERE country = 'NL' AND subtype = 'microhood'
  AND (bbox).xmin >= 4.73 AND (bbox).xmax <= 5.07
  AND (bbox).ymin >= 52.28 AND (bbox).ymax <= 52.43;

CREATE INDEX ON amsterdam_neighborhoods USING gist (geom);
```

Both statements take a few seconds, since they read only the files that overlap the bounding
box. The Forest Service data is extracted the same way:

```sql
CREATE TABLE forests AS
SELECT adminfores AS forest_id, forestname AS name, gis_acres AS acres,
       ST_Multi(ST_SetSRID(geom, 4269))::geometry(MultiPolygon, 4269) AS geom
FROM usfs_forests;

CREATE INDEX ON forests USING gist (geom);

CREATE TABLE fires USING iceberg AS
SELECT fire_id, fire_name, fire_type, ig_date, acres,
       ST_SetSRID(geom, 4269)::geometry(Point, 4269) AS geom
FROM usfs_fires;
```

Copying first matters most for spatial joins. A join between the two remote files cannot use an
index, and can take many minutes; the same join between the tables above takes well under a
second.

## Analyze

Aggregations over the Iceberg table run on DuckDB:

```sql
SELECT category, count(*)
FROM amsterdam_places
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

          category          | count
----------------------------+-------
 restaurant                 |  4214
 professional_service       |  2808
 fashion_and_apparel_store  |  2360
 personal_or_beauty_service |  1808
                            |  1451
```

Spatial filters are pushed down as well. This radius search around Dam Square, 0.005 degrees
or roughly 500 meters, runs entirely in DuckDB (see
[spatial query pushdown](spatial.md#spatial-query-pushdown)):

```sql
SELECT category, count(*)
FROM amsterdam_places
WHERE ST_DWithin(geom, ST_Point(4.8932, 52.3731, 4326), 0.005)
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

          category          | count
----------------------------+-------
 restaurant                 |   287
 fashion_and_apparel_store  |   266
 bar                        |    99
 personal_or_beauty_service |    90
 professional_service       |    88
```

Point-in-polygon lookups on the neighborhoods use the GiST index:

```sql
SELECT name FROM amsterdam_neighborhoods
WHERE ST_Contains(geom, ST_Point(4.8932, 52.3731, 4326));

 name
------
 Dam
```

Spatial joins combine the two. PostgreSQL reads the matching rows from Iceberg, with the
non-spatial filters applied in DuckDB, and looks up each one in the indexed polygons:

```sql
-- the national forests with the most fires in 2022
SELECT f.name, count(*) AS fires, round(sum(x.acres)) AS acres_burned
FROM fires x
JOIN forests f ON ST_Within(x.geom, f.geom)
WHERE x.ig_date >= '2022-01-01' AND x.ig_date < '2023-01-01'
GROUP BY f.name
ORDER BY fires DESC
LIMIT 5;

              name               | fires | acres_burned
---------------------------------+-------+--------------
 National Forests in Mississippi |   111 |       153958
 Kisatchie National Forest       |    80 |       110427
 National Forests in Alabama     |    68 |        91442
 Ouachita National Forest        |    64 |       152035
 National Forests in Texas       |    51 |       107093
```

```sql
-- the Amsterdam neighborhoods with the most cafes
SELECT n.name, count(*) AS cafes
FROM amsterdam_places p
JOIN amsterdam_neighborhoods n ON ST_Contains(n.geom, p.geom)
WHERE p.category = 'cafe'
GROUP BY n.name
ORDER BY cafes DESC
LIMIT 5;

         name          | cafes
-----------------------+-------
 Grachtengordel        |    40
 Oud-West              |    39
 Burgwallen-Oude Zijde |    38
 De Pijp               |    36
 Jordaan               |    34
```

For distances in meters, use `geography`, which PostGIS evaluates on the rows DuckDB returns:

```sql
-- the five nearest museums to Dam Square
SELECT name, round(ST_Distance(geom::geography, ST_Point(4.8932, 52.3731, 4326)::geography)) AS meters
FROM amsterdam_places
WHERE category = 'museum'
ORDER BY geom <-> ST_Point(4.8932, 52.3731, 4326)
LIMIT 5;
```

## Visualize in QGIS

[QGIS](https://qgis.org/) connects to pg_lake like to any PostgreSQL database. Add a PostgreSQL
connection that points to your server:

![Adding a PostgreSQL connection in QGIS](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/7f7ead6d-60ac-43c2-2231-340c6d920700/public)

Your tables and views appear in the Browser panel under their schema, and you can add them to
the project as layers:

![Adding tables as layers in QGIS](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/118e795d-9f60-4ae4-40b4-b1608f072000/public)

QGIS reads the coordinate system from the column type, so the tables created above are placed
on the map correctly. For foreign tables and other columns with SRID 0, set the coordinate
system on the layer:

![Setting the layer CRS in QGIS](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/b9341a4b-df14-4105-bed2-b0166fe00a00/public)

or create a view that sets or transforms it, for example to show both data sets in WGS 84:

```sql
CREATE VIEW forests_wgs84 AS
SELECT forest_id, name, ST_Transform(geom, 4326)::geometry(MultiPolygon, 4326) AS geom
FROM forests;
```

Views of analysis results are useful layers too. These show the national forests that had
fires in 2022, and the fires themselves:

```sql
CREATE VIEW nfs_fires_in_2022 AS
SELECT x.*, f.forest_id
FROM fires x
JOIN forests f ON ST_Within(x.geom, f.geom)
WHERE x.ig_date >= '2022-01-01' AND x.ig_date < '2023-01-01';

CREATE VIEW forests_with_fires_in_2022 AS
SELECT * FROM forests
WHERE forest_id IN (SELECT forest_id FROM nfs_fires_in_2022);
```

![National forests with fires in 2022 in QGIS](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/184c88a3-15bd-49af-6755-42d714d3d800/public)

QGIS runs the view's query each time it redraws the map. For large results, materialize them
in a table first.

## Share the results

Export query results as GeoParquet, which QGIS, GeoPandas, DuckDB and Snowflake can read:

```sql
COPY (SELECT name, category, geom FROM amsterdam_places WHERE category = 'cafe')
TO 's3://mybucket/exports/amsterdam_cafes.parquet';
```

The Iceberg tables can be read by other engines too, through their
[catalog](iceberg-catalogs.md).

## Keep it up to date

Refresh an extract in one transaction, so that queries keep seeing the old data until the new
data is complete:

```sql
BEGIN;
DELETE FROM fires;
INSERT INTO fires
SELECT fire_id, fire_name, fire_type, ig_date, acres,
       ST_SetSRID(geom, 4269)::geometry(Point, 4269)
FROM usfs_fires;
COMMIT;
```

Before that, make sure the foreign table reads the new data:

- **Files whose URL stays the same**, such as the Forest Service downloads, are served from the
  file cache once downloaded. Download the new version with
  `SELECT lake_file_cache.add('<url>', refresh := true)`.
- **Data sets with versioned paths**, such as Overture releases, need the foreign table to
  point at the new release:

  ```sql
  ALTER FOREIGN TABLE ov_places
    OPTIONS (SET path 's3://overturemaps-us-west-2/release/<new release>/theme=places/type=place/*.parquet');
  ```

A [pg_cron](https://github.com/citusdata/pg_cron) job can run the refresh on a schedule.

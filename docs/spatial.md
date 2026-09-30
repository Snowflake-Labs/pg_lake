---
title: Geospatial
parent: User guide
nav_order: 4
---

# Geospatial features for pg_lake

## Add spatial for pg_lake
To add the `PostGIS` extension and other geospatial features for pg_lake, run the following:

```sql
CREATE EXTENSION pg_lake_spatial CASCADE;
```

## Geospatial features

`pg_lake` adds advanced geospatial features on top of PostGIS. The
most powerful feature is to instantly import almost any geospatial data set with
a single command. We also leverage DuckDB to accelerate spatial queries,
especially on (Geo)Parquet files.

Geospatial features:

- Query and import directly from public data sets (available over https or S3)
  or private data sets in S3 without downloading to your computer.
- Long-lived caching of geospatial files on the NVMe drives of your warehouse
  cluster
- Query acceleration using DuckDB
- Support for many geospatial file formats:
  - GeoParquet and Overture
  - GeoJSON and GeoJSONSeq
  - WKB in regular Parquet
  - WKT in CSV/JSON/other
  - GDAL-supported formats, including Shapefile in zip, Geopackage, Geodatabase,
    FlatGeoBuf, and more

In addition, all of PostgreSQL and PostGIS can be directly used with the new
geospatial analytics features. For instance, you might use `pg_lake` tables to
import and transform a data set, and then convert it into a regular PostgreSQL
materialized view
with a spatial index. You can periodically rebuild the view using pg_cron. 

## Connecting remote data sets to PostGIS

`pg_lake` has three different ways of interacting with geospatial data
sets:

- Creating foreign tables
- Creating local tables with indexes
- Loading data with COPY into an existing table

```sql
-- 1) Create a remote table for querying a geospatial data set
create foreign table world ()
server pg_lake
options (path 'https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM0.zip')

\d world
                      Foreign table "public.world"
┌────────────┬──────────┬───────────┬──────────┬─────────┬─────────────┐
│   Column   │   Type   │ Collation │ Nullable │ Default │ FDW options │
├────────────┼──────────┼───────────┼──────────┼─────────┼─────────────┤
│ shapegroup │ text     │           │          │         │             │
│ shapetype  │ text     │           │          │         │             │
│ shapename  │ text     │           │          │         │             │
│ geom       │ geometry │           │          │         │             │
└────────────┴──────────┴───────────┴──────────┴─────────┴─────────────┘

-- 2) Create a regular table (and index) directly from a geospatial data set
create table world ()
with (load_from = 'https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM0.zip');
create index on world using gist (geom);

-- 3) Load data from a geospatial data set into an existing table
copy world from 'https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM0.zip';
```

You can combine these mechanisms in different ways, depending on what makes
sense for your scenario.

Queries across large (Geo)Parquet data sets benefit from using `pg_lake` tables,
using compression, caching, enhanced parallelism and vectorized execution.

Precise geospatial lookups (e.g. which ZIP Code is this point?) benefit a lot
more from regular tables with spatial indexes.

For smaller data sets, the differences are less pronounced. It is worth noting
that joins between the same table types (e.g. `pg_lake` join `pg_lake` or
regular join regular) may be more efficient than joins across two table types
(e.g. `pg_lake` join regular).

## GeoParquet example: querying Overture data

GeoParquet is a relatively new format for storing geospatial data that enables
compression and efficient analytics queries via columnar storage.

When creating a `pg_lake` table from a URL pointing to a
GeoParquet file, geometry columns are automatically detected and mapped
into the PostGIS geometry type, such that you can immediately use PostGIS
functions and operators.

A large and well-known GeoParquet data set is
[Overture Maps](https://docs.overturemaps.org/). `pg_lake` is a
good choice for working with Overture, because bigger instances
can cache the full data set on a local drive, the vectorized query execution and
parallelism give excellent performance when querying Parquet files, and clusters
are available in the `us-west-2` region where Amazon hosts the Overture data set.

You can easily query Overture data by creating a table:

```sql
-- Create a table to query the Overture (GeoParquet) Places data set
create foreign table ov_places ()
server pg_lake
options (path 's3://overturemaps-us-west-2/release/2024-08-20.0/theme=places/type=*/*.parquet');

\d ov_places

                                                    Foreign table "public.ov_places"
   Column   |                                    Type                                     | Collation | Nullable | Default | FDW options
------------+-----------------------------------------------------------------------------+-----------+----------+---------+-------------
 id         | text                                                                        |           |          |         |
 geometry   | geometry                                                                       |           |          |         |
 bbox       | lake_struct.xmin_xmax_ymin_ymax_35464140                                 |           |          |         |
 version    | integer                                                                     |           |          |         |
 sources    | lake_struct.property_dataset_record_id_update_time_confidence_ff84a944[] |           |          |         |
 names      | lake_struct.primary_common_rules_a6b9407b                                |           |          |         |
 categories | lake_struct.primary_alternate__7ead223                                   |           |          |         |
 confidence | double precision                                                            |           |          |         |
 websites   | text[]                                                                      |           |          |         |
 socials    | text[]                                                                      |           |          |         |
 emails     | text[]                                                                      |           |          |         |
 phones     | text[]                                                                      |           |          |         |
 brand      | lake_struct.wikidata_names_55767648                                      |           |          |         |
 lake_struct  | lake_struct.freeform_locality_postcode_region_country__4d9a9db[]         |           |          |         |
 theme      | text                                                                        |           |          |         |
 type       | text                                                                        |           |          |         |
Server: pg_lake
FDW options: (path 's3://overturemaps-us-west-2/release/2024-07-22.0/theme=places/type=*/*.parquet', format 'parquet')
```

When querying Overture data sets, it's often a good idea to use a bounding box
(bbox) filter. The query engine will use Parquet metadata to reduce the amount
of data that needs to be read.

```sql
-- Query for gas station locations in St. Louis, Missouri
SELECT (categories).primary AS cat, (addresses[1]).locality as city,
geometry
FROM ov_places
WHERE (bbox).xmin >= -90.24
AND (bbox).xmax <= -90.15
AND (bbox).ymin >= 38.59
AND (bbox).ymax <= 38.66
AND (categories).primary = 'gas_station';
```

Note: Only WKB-encoded geometries are currently supported.

## GDAL data sets: Shapefile zip, Geopackage, and more

Geospatial data sets are published in a large variety of formats. The
[GDAL](https://gdal.org/en/latest/) library is able to read many/most of these
formats, and you can use it to import data into PostgreSQL using the
`format 'gdal'` option when creating a table:

```sql
-- Create a table from a zip containing a Shapefile
-- Add compression option
create foreign table nld ()
server pg_lake
options (
  format 'gdal',
  compression 'zip',
  path 'https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/netherlands-shapefile/at_download/file'
);
```

For some commonly used formats, the GDAL format will be inferred:

| **Extension** | **GDAL format**                            | **Compression** |
| ------------- | ------------------------------------------ | --------------- |
| .zip          | Shapefile, Geodatabase, other GDAL formats | zip             |
| .geojson      | GeoJSON                                    | none            |
| .geojson.gz   | GeoJSON                                    | gzip            |
| .gpkg         | Geopackage                                 | none            |
| .gpkg.gz      | Geopackage                                 | gzip            |
| .kml          | Key Markup Language                        | none            |
| .kmz          | Key Markup Language                        | zip             |
| .fgb          | FlatGeoBuf                                 | none            |

When working with a URL that does not have an explicit extension, it may be
necessary to explicitly specify both the format and compression (usually
`compression 'zip'`). When a .zip archive can contain multiple geospatial data
sets, you can also distinguish them by adding a `zip_path` option:

```sql
-- Map of the Netherlands at 100km granularity from
-- a .zip containing multiple shape files
create foreign table nld_100km ()
server pg_lake
options (
  format 'gdal',
  compression 'zip',
  path 'https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/netherlands-shapefile/at_download/file'
);

-- Map of the Netherlands at 1km granularity from
-- by selecting a specific .zip
create foreign table nld_1km ()
server pg_lake
options (
  format 'gdal',
  compression 'zip',
  zip_path 'nl_1km.shp',
  path 'https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/netherlands-shapefile/at_download/file'
);
```

GDAL files are downloaded when creating the table. This means it may take a
little longer, but it avoids repeatedly requesting the same file from the web
server.

The file is only re-downloaded if it is evicted from the cache or the PostgreSQL
server is replaced.

## GeoJSON

There are roughly two forms of GeoJSON: Single JSON object containing a
FeatureCollection, or newline-delimited JSON with a single feature per JSON
object. The latter kind is sometimes referred to as GeoJSONSeq.

Both forms of GeoJSON are supported in `pg_lake`, though in
different ways. For GeoJSON with a FeatureCollection, you always want to use
`format 'gdal'` , which will unwind the attributes into columns:

```sql
create foreign table gemeentes ()
server pg_lake
options (format 'gdal', path 'https://service.pdok.nl/cbs/gebiedsindelingen/2017/wfs/v1_0?request=GetFeature&service=WFS&version=2.0.0&typeName=gemeente_gegeneraliseerd&srsName=EPSG:4326&outputFormat=json');

\d gemeentes
                    Foreign table "public.gemeentes"
┌────────────┬──────────┬───────────┬──────────┬─────────┬─────────────┐
│   Column   │   Type   │ Collation │ Nullable │ Default │ FDW options │
├────────────┼──────────┼───────────┼──────────┼─────────┼─────────────┤
│ id         │ integer  │           │          │         │             │
│ statcode   │ text     │           │          │         │             │
│ statnaam   │ text     │           │          │         │             │
│ jrstatcode │ text     │           │          │         │             │
│ rubriek    │ text     │           │          │         │             │
│ geom       │ geometry │           │          │         │             │
└────────────┴──────────┴───────────┴──────────┴─────────┴─────────────┘
Server: pg_lake
FDW options: (format 'gdal', path 'https://service.pdok.nl/cbs/gebiedsindelingen/2017/wfs/v1_0?request=GetFeature&service=WFS&version=2.0.0&typeName=gemeente_gegeneraliseerd&outputFormat=json')
```

For GeoJSONSeq, GDAL also works, but `format 'json'` is likely to give better
performance. However, it does not automatically infer the geometry and attribute
columns. You can use the `ST_GeomFromGeoJSON` function to convert a feature to
the geometry type.

## Reading and writing regular Parquet/CSV/JSON data with geometry data

Sometimes you have a Parquet, CSV, or JSON files that contain encoded
geometries. You can use PostGIS functions to decode these geometries.

For example:

```sql
-- create a CSV file with a geometry, will encode as WKT
copy (select 'POINT(3.14 6.28)'::geometry) to 's3://mybucket/demo.csv' with header;

-- Create a table from the CSV, will use text column
create foreign table csv_table ()
server pg_lake options (path 's3://mybucket/demo.csv');

-- Convert the WKT into a geometry
select sum(ST_Area(ST_GeomFromText(wkt))) from csv_table;

-- Alternatively, create a table from the CSV with an explicit geometry column
create foreign table csv_table (g geometry)
server pg_lake options (path 's3://mybucket/demo.csv');

-- Select the geometry, will use ST_GeomFromText under the covers
select sum(ST_Area(g)) from csv_table;
```

The expected encodings and functions to use for each format are given below:

| **Format** | **Geometry encoding** | **Read function**  |
| ---------- | --------------------- | ------------------ |
| Parquet    | WKB                   | ST_GeomFromWKB     |
| CSV        | WKT                   | ST_GeomFromText    |
| JSON       | GeoJSON               | ST_GeomFromGeoJSON |
| GDAL       | Native                | n/a                |

Apart from querying files with geometry data, you can also write geometry
columns in Parquet/CSV/JSON using `COPY ... TO`. They will automatically use the
encoding listed in the table. Writing using GDAL is not yet supported.

## Use QGIS with pg_lake

Using QGIS with `pg_lake` is straightforward. First, add a
PostgreSQL connection that points to your cluster.

![connect warehouse to qgis](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/7f7ead6d-60ac-43c2-2231-340c6d920700/public)

You'll find your tables and view in the Browser under your schema. You can add
layers to the project to visualize them.

![add tables qgis](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/118e795d-9f60-4ae4-40b4-b1608f072000/public)

Finally, you may need to set your coordinate reference system to match the data
set. Note that the SRID is currently set to 0 on all analytics tables, so it
cannot be automatically detected.

![qgis update srid](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/b9341a4b-df14-4105-bed2-b0166fe00a00/public)

If your data set uses an alternative coordinate reference system, you can also
create a view that transforms it using the `st_transform` function:

```sql
create view points_transformed as
select point_id, st_transform(geom, 'epsg:27700', 'epsg:4326')
from points;
```

## Advanced scenarios: combining geospatial data sets in QGIS

With the ability to create tables for almost any geospatial data set, you can
also combine them in interesting ways.

For instance, the following queries create tables for data sets containing
national forest boundaries and occurrences of fires over the past few years, and
we then create views to find the forests that had fires in 2022:

```sql
-- National Forest System boundaries
create foreign table forests ()
server pg_lake
options (path 'https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.AdministrativeForest.zip');

-- Fire occurrence points in US
create foreign table fires ()
server pg_lake
options (path 'https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.MTBS_FIRE_OCCURRENCE_PT.zip');

-- Only consider fires in national forests in 2022
create view nfs_fires_in_2022 as
select fires.*, forests.adminfores
from forests, fires
where st_within(fires.geom, forests.geom)
and date_trunc('year', ig_date) = '2022-01-01';

-- Find the forests which had fires in 2022
create view forests_with_fires_in_2022 as
select *
from forests
where adminfores in (
  select adminfores from nfs_fires_in_2022
);
```

Loading these tables and views as layers into QGIS gives a good overview of
which national forests suffered from fires in 2022:

![qgis from shapefile in s3](https://imagedelivery.net/lPM0ntuwQfh8VQgJRu0mFg/184c88a3-15bd-49af-6755-42d714d3d800/public)

## Limitations of spatial query acceleration

`pg_lake` takes advantage of some of the logic in
[DuckDB spatial](https://duckdb.org/docs/extensions/spatial.html), though it is
still in an early stage of development. While DuckDB spatial it aims to
[mimic PostGIS](https://duckdb.org/2023/04/28/spatial.html), it still has many
inconsistencies. We correct for those inconsistencies such that you'll always
get the default PostGIS experience, even if DuckDB changes in the future. It
does mean that functions may use the PostGIS implementation by transferring
geometry data from DuckDB into PostgreSQL, which adds some overhead.

For instance, if you use the `ST_SetSRID` function, then we'll execute that in
PostGIS, since DuckDB does not currently track SRID information. In most cases,
that will cause other functions to also be executed in PostGIS instead of
DuckDB. That means everything always works correctly, but the use of certain
functions can sometimes cause unexpected performance degradation.

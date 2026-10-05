---
title: Home
layout: home
nav_order: 1
permalink: /
description: pg_lake integrates Iceberg and data lake files into Postgres.
---

<div class="pglake-hero">
  <p class="pglake-eyebrow">Open source PostgreSQL extensions</p>
  <h1 class="pglake-hero-title">Postgres for Iceberg and data lakes</h1>
  <p class="pglake-hero-lead">
    pg_lake lets you create and query Iceberg tables, and read and write Parquet, CSV and JSON
    files in object storage, all from PostgreSQL. Queries run on DuckDB's columnar engine, with
    full transactional guarantees and no SQL limitations.
  </p>
  <div class="pglake-hero-actions">
    <a class="btn btn-primary fs-5 mr-2" href="{{ '/get-started.html' | relative_url }}">Get started</a>
    <a class="btn fs-5" href="https://github.com/Snowflake-Labs/pg_lake">View on GitHub</a>
  </div>
</div>

## A quick look

pg_lake turns PostgreSQL into a lakehouse. Add `USING iceberg` to `CREATE TABLE` and you get a
transactional table whose data is stored as Parquet files in your object storage bucket, in the
open Iceberg format that Spark, DuckDB and Snowflake can read too. The same extensions let you
query raw Parquet, CSV and JSON files where they are, and `COPY` to and from URLs. Everything is
plain SQL from psql or any PostgreSQL client, and the heavy lifting runs on DuckDB.

**Create an Iceberg table.** `load_from` creates the table from a file and loads it in one step,
here a public file with 3 million New York taxi trips. Analytical queries run on DuckDB's
columnar engine:

```sql
CREATE TABLE trips () USING iceberg
  WITH (load_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');

-- which hours have the best tips?
SELECT extract(hour FROM tpep_pickup_datetime) AS hour, count(*) AS trips,
       round(avg(tip_amount)::numeric, 2) AS avg_tip
FROM trips GROUP BY 1 ORDER BY avg_tip DESC LIMIT 3;

 hour | trips  | avg_tip
------+--------+---------
    5 |  18764 |    3.82
   16 | 190201 |    3.68
   22 | 143261 |    3.56
```

**Change it like any other table.** Iceberg tables support `INSERT`, `UPDATE`, `DELETE`,
`MERGE` and transactions, and pg_lake compacts the files and expires old snapshots in the
background:

```sql
BEGIN;
DELETE FROM trips WHERE total_amount < 0;
UPDATE trips SET passenger_count = 1 WHERE passenger_count = 0;
COMMIT;
```

**Move old rows out of PostgreSQL.** Regular tables and Iceberg tables can be used in the same
statement, so moving rows from an `orders` table to cheaper storage happens in one transaction,
without losing or duplicating any:

```sql
CREATE TABLE orders_history (LIKE orders) USING iceberg;

WITH moved AS (
  DELETE FROM orders WHERE order_date < '2026-01-01' RETURNING *
)
INSERT INTO orders_history SELECT * FROM moved;
```

**Read the tables from other engines.** pg_lake writes standard Iceberg metadata, so Spark,
pyiceberg, DuckDB and Snowflake can read the same tables, through PostgreSQL as the catalog or
from the metadata file:

```sql
SELECT table_name, metadata_location FROM iceberg_tables;
```

**Query files where they are.** A foreign table reads Parquet, CSV or JSON files in place,
including every file that matches a wildcard. Leave the column list empty to infer the columns:

```sql
CREATE FOREIGN TABLE clicks () SERVER pg_lake
  OPTIONS (path 's3://mybucket/clicks/2026/*/*.parquet');

SELECT page, count(*) FROM clicks GROUP BY page ORDER BY 2 DESC LIMIT 10;
```

**Load new files as they arrive.** With [pg_incremental](https://github.com/CrunchyData/pg_incremental),
a pipeline loads every file that is already in a bucket into an Iceberg table, then each new
one exactly once:

```sql
CREATE FOREIGN TABLE order_files () SERVER pg_lake
  OPTIONS (path 's3://mybucket/inbox/*.csv', filename 'true');

SELECT incremental.create_file_list_pipeline('import-orders',
  file_pattern := 's3://mybucket/inbox/*.csv',
  batched := true,
  command := $$
    INSERT INTO orders_history SELECT order_id, order_date, amount
    FROM order_files WHERE _filename = any($1)
  $$);
```

**Export a report.** `COPY ... TO` writes any query result to object storage as Parquet, CSV or
JSON:

```sql
COPY (SELECT tpep_pickup_datetime::date AS day, count(*) AS trips FROM trips GROUP BY 1 ORDER BY 1)
TO 's3://mybucket/reports/trips_per_day.csv' WITH (header true);
```

## What you can do

<div class="pglake-cards">
  <a class="pglake-card" href="{{ '/iceberg-tables.html' | relative_url }}">
    <span class="pglake-card-title">Iceberg tables</span>
    <span class="pglake-card-text">Create, update and query transactional Iceberg tables with <code>USING iceberg</code>, with hidden partitioning and automatic maintenance.</span>
  </a>
  <a class="pglake-card" href="{{ '/query-data-lake-files.html' | relative_url }}">
    <span class="pglake-card-title">Query data lake files</span>
    <span class="pglake-card-text">Query Parquet, CSV, JSON and Iceberg files in object storage or at public URLs, with wildcards and inferred schemas.</span>
  </a>
  <a class="pglake-card" href="{{ '/data-lake-import-export.html' | relative_url }}">
    <span class="pglake-card-title">Import and export</span>
    <span class="pglake-card-text">Load data from object storage and write query results back out with <code>COPY</code>, in any supported format.</span>
  </a>
  <a class="pglake-card" href="{{ '/iceberg-catalogs.html' | relative_url }}">
    <span class="pglake-card-title">Interoperability</span>
    <span class="pglake-card-text">Share tables with Snowflake, Spark and pyiceberg through PostgreSQL's catalog or an Iceberg REST catalog.</span>
  </a>
  <a class="pglake-card" href="{{ '/spatial.html' | relative_url }}">
    <span class="pglake-card-title">Geospatial</span>
    <span class="pglake-card-text">Query GeoParquet, Shapefiles, GeoJSON and more with PostGIS, store geometry in Iceberg, and push spatial filters down to DuckDB.</span>
  </a>
  <a class="pglake-card" href="{{ '/performance.html' | relative_url }}">
    <span class="pglake-card-title">Performance</span>
    <span class="pglake-card-text">See what runs on DuckDB, how files are skipped and cached, and how to keep writes fast.</span>
  </a>
</div>

## Use cases

<div class="pglake-cards">
  <a class="pglake-card" href="{{ '/use-case-iceberg-sync.html' | relative_url }}">
    <span class="pglake-card-title">Sync Postgres tables to Iceberg</span>
    <span class="pglake-card-text">Keep an Iceberg copy of operational tables up to date, and query it from Spark, DuckDB or Snowflake without ETL.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-archiving.html' | relative_url }}">
    <span class="pglake-card-title">Archive partitions to Iceberg</span>
    <span class="pglake-card-text">Keep recent rows in PostgreSQL and move old months to cheaper Iceberg storage.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-dashboards.html' | relative_url }}">
    <span class="pglake-card-title">Fast analytics dashboards</span>
    <span class="pglake-card-text">Serve dashboards from Iceberg tables with sub-second aggregates over millions of rows, and keep rollups up to date for busy panels.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-geospatial.html' | relative_url }}">
    <span class="pglake-card-title">Geospatial analytics</span>
    <span class="pglake-card-text">Query public map data in place, extract it into Iceberg and PostGIS tables, and map the results in QGIS.</span>
  </a>
</div>

## How it works

A pg_lake instance has two parts: **PostgreSQL with the pg_lake extensions**, and
**pgduck_server**. You only ever connect to PostgreSQL. The extensions handle query planning,
transaction boundaries and the Iceberg catalog, and delegate scanning and computation to
pgduck_server, a separate multi-threaded process that runs DuckDB behind the PostgreSQL wire
protocol.

<figure class="pglake-figure">
  <img src="{{ '/assets/images/pglake-arch.png' | relative_url }}" alt="pg_lake architecture: Postgres with pg_lake sends queries and files over a Unix socket to pgduck_server, which runs DuckDB against S3">
</figure>

Running DuckDB in its own process avoids the threading and memory-safety problems of
embedding it in PostgreSQL's process-per-connection model, and lets you connect to the query
engine directly with any Postgres client. [How pg_lake works](concepts.md) describes the components, the kinds of tables, and what
happens when you query and write.

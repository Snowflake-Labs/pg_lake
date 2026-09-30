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

```sql
-- Create an Iceberg table, stored in your object store
CREATE TABLE events USING iceberg
  AS SELECT i AS id, 'event_' || i AS name FROM generate_series(1, 1000) i;

-- Export query results to Parquet
COPY (SELECT * FROM events) TO 's3://mybucket/export/events.parquet';

-- Query files in place; columns are inferred from the files
CREATE FOREIGN TABLE raw_events () SERVER pg_lake
  OPTIONS (path 's3://mybucket/export/*.parquet');

SELECT count(*) FROM raw_events JOIN events USING (id);
```

## What you can do

<div class="pglake-cards">
  <a class="pglake-card" href="{{ '/iceberg-tables.html' | relative_url }}">
    <span class="pglake-card-title">Iceberg tables</span>
    <span class="pglake-card-text">Create, modify and query transactional Iceberg tables with <code>USING iceberg</code>, and read them from Spark and other engines.</span>
  </a>
  <a class="pglake-card" href="{{ '/query-data-lake-files.html' | relative_url }}">
    <span class="pglake-card-title">Query data lake files</span>
    <span class="pglake-card-text">Query Parquet, CSV, JSON and Iceberg files in object storage or at public URLs, with wildcards and inferred schemas.</span>
  </a>
  <a class="pglake-card" href="{{ '/data-lake-import-export.html' | relative_url }}">
    <span class="pglake-card-title">Import and export</span>
    <span class="pglake-card-text">Load data from object storage and write query results back out with <code>COPY</code>, in any supported format.</span>
  </a>
  <a class="pglake-card" href="{{ '/file-formats-reference.html' | relative_url }}">
    <span class="pglake-card-title">File formats</span>
    <span class="pglake-card-text">Options for Parquet, CSV, JSON, GDAL, log files, external Iceberg tables and Hugging Face datasets.</span>
  </a>
  <a class="pglake-card" href="{{ '/spatial.html' | relative_url }}">
    <span class="pglake-card-title">Geospatial</span>
    <span class="pglake-card-text">Import GeoParquet, Shapefiles, GeoJSON and more into PostGIS, with DuckDB-accelerated spatial queries.</span>
  </a>
  <a class="pglake-card" href="{{ '/dbt.html' | relative_url }}">
    <span class="pglake-card-title">dbt</span>
    <span class="pglake-card-text">Build and incrementally update Postgres and Iceberg tables with <code>dbt-postgres</code>.</span>
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
engine directly with any Postgres client. The
[project README](https://github.com/Snowflake-Labs/pg_lake#architecture) describes each
component.

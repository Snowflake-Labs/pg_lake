---
title: Fast analytics dashboards
parent: Use cases
nav_order: 4
---

# Fast analytics dashboards
{: .no_toc }

Dashboards ask the same few questions over and over: how many orders per day, revenue per
region, the busiest hour of the week. On a large heap table, each of those is a full scan. With
pg_lake, you keep the raw data in an Iceberg table, where DuckDB answers aggregates over
millions of rows in about a tenth of a second, and keep small rollup tables up to date for the
panels that refresh most often. Grafana, Metabase, Superset and other BI tools connect to
PostgreSQL as usual, with no separate warehouse or connector.

This page builds a dashboard backend for 9.5 million New York taxi trips from the public
[TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

1. TOC
{:toc}

## How it works

- **Raw data in Iceberg.** An Iceberg table partitioned by day keeps every row in compressed
  Parquet files. Queries with a time filter only read the files for those days, and aggregates
  run entirely in DuckDB.
- **Small dimension tables in Iceberg too.** A join between two Iceberg tables runs in DuckDB;
  a join with a heap table pulls the Iceberg rows into PostgreSQL first.
- **Rollups for busy panels.** A [pg_incremental](https://github.com/CrunchyData/pg_incremental)
  pipeline aggregates new rows into a small heap table every minute. Rollup queries are cheap
  enough to serve many dashboard users at once.

## Prerequisites

You need pg_lake with `pg_lake_iceberg.default_location_prefix` set (see
[getting started](get-started.md)), and [pg_cron](https://github.com/citusdata/pg_cron) and
[pg_incremental](https://github.com/CrunchyData/pg_incremental) for the rollup:

```sql
CREATE EXTENSION pg_incremental CASCADE;
```

pg_incremental schedules its pipelines with pg_cron, so create them in the database where
pg_cron is installed (`cron.database_name`, `postgres` by default).

## Load the raw data

The trip records are published as one Parquet file per month. Create an Iceberg table from the
first month, with the columns inferred from the file and partitioned by day, so that a
dashboard's time range only reads the files for those days. Then add the next two months with
`COPY`:

```sql
CREATE TABLE trips () USING iceberg
  WITH (load_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
        partition_by = 'day(tpep_pickup_datetime)');

COPY trips FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet';
COPY trips FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet';
```

Each file takes 15 to 20 seconds to load. The files contain a few trips with timestamps outside
their month, some as old as 2002, which would show up as stray days on a dashboard. Remove them:

```sql
DELETE FROM trips WHERE tpep_pickup_datetime < '2024-01-01' OR tpep_pickup_datetime >= '2024-04-01';
```

That leaves 9,554,757 trips in 95 files, mostly one per day, 193 MB in total. For data that
keeps arriving as files, use a
[file list pipeline](data-lake-import-export.md#load-new-files-as-they-arrive) instead; for rows
written to PostgreSQL, see [syncing tables to Iceberg](use-case-iceberg-sync.md).

The taxi data has local times without a time zone. For your own events, prefer `timestamptz`.

Then load the lookup table that maps zone numbers to names, also into Iceberg:

```sql
CREATE TABLE zones (zone_id int, borough text, zone text, service_zone text) USING iceberg;

COPY zones FROM 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv' WITH (header true);
```

## Dashboard queries

A time series panel with trips and revenue per day:

```sql
SELECT date_trunc('day', tpep_pickup_datetime) AS day, count(*) AS trips, round(sum(total_amount)) AS revenue
FROM trips
WHERE tpep_pickup_datetime >= '2024-03-01' AND tpep_pickup_datetime < '2024-04-01'
GROUP BY 1 ORDER BY 1;

         day         | trips  | revenue
---------------------+--------+---------
 2024-03-01 00:00:00 | 117640 | 3142126
 2024-03-02 00:00:00 | 122463 | 3037189
 2024-03-03 00:00:00 |  97000 | 2694354
 ...
```

The busiest pickup zones, with their tip percentage:

```sql
SELECT z.borough, z.zone, count(*) AS trips, round((100 * sum(t.tip_amount) / sum(t.fare_amount))::numeric, 1) AS tip_pct
FROM trips t JOIN zones z ON z.zone_id = t.pulocationid
WHERE t.tpep_pickup_datetime >= '2024-03-01' AND t.tpep_pickup_datetime < '2024-04-01'
GROUP BY 1, 2 ORDER BY trips DESC LIMIT 5;

  borough  |         zone          | trips  | tip_pct
-----------+-----------------------+--------+---------
 Manhattan | Midtown Center        | 163267 |    19.3
 Queens    | JFK Airport           | 157703 |    15.3
 Manhattan | Upper East Side South | 155631 |    20.1
 Manhattan | Upper East Side North | 146044 |    19.3
 Manhattan | Midtown East          | 123805 |    19.6
```

A heatmap of trips by day of the week and hour, over all three months, and the median trip
duration:

```sql
SELECT extract(isodow FROM tpep_pickup_datetime) AS dow, extract(hour FROM tpep_pickup_datetime) AS hour, count(*)
FROM trips GROUP BY 1, 2 ORDER BY 1, 2;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM tpep_dropoff_datetime - tpep_pickup_datetime) / 60)
FROM trips WHERE tpep_pickup_datetime >= '2024-03-01' AND tpep_pickup_datetime < '2024-04-01';
```

Each of these took between 40 and 110 milliseconds once the files were in pgduck_server's
[file cache](performance.md#file-cache), measured on a 16-core machine. `EXPLAIN VERBOSE` shows
that the whole query runs in DuckDB, and how many files it reads:

```sql
EXPLAIN VERBOSE
SELECT count(*) FROM trips WHERE tpep_pickup_datetime >= '2024-03-10' AND tpep_pickup_datetime < '2024-03-17';

 Custom Scan (Query Pushdown)
   Engine: DuckDB
   Data Files Scanned: 7
   ...
```

Two things keep these queries fast:

- **Filter on the partition column.** A week of data reads 7 of the 95 files.
- **Keep lookup tables in Iceberg.** The same zones query with `zones` as a heap table took
  about 4 seconds instead of 0.06, because PostgreSQL joins the 3.6 million matching trips itself.
  If a lookup table is maintained as a regular table, refresh an Iceberg copy when it changes,
  for example with
  `BEGIN; DELETE FROM zones; INSERT INTO zones SELECT * FROM zones_source; COMMIT;`.

## Keep a rollup for busy panels

All queries on Iceberg tables share pgduck_server. On the same machine, the daily query above
ran at about 75 to 80 queries per second, whether 8 or 32 clients sent it, so with 32 clients
each query took about 400 milliseconds. A wall display, or a dashboard that dozens of people
refresh every few seconds, is better served from a rollup.

Create an hourly rollup per pickup zone as a heap table:

```sql
CREATE TABLE trips_hourly (
  hour timestamp NOT NULL,
  pickup_zone int NOT NULL,
  trips bigint NOT NULL,
  fares numeric NOT NULL,
  tips numeric NOT NULL,
  revenue numeric NOT NULL,
  PRIMARY KEY (hour, pickup_zone)
);
```

A time interval pipeline aggregates each hour once it has passed, starting with the backfill:

```sql
SELECT incremental.create_time_interval_pipeline(
  pipeline_name := 'trips-hourly',
  time_interval := '1 hour',
  source_table_name := 'trips',
  start_time := '2024-01-01',
  command := $$
    INSERT INTO trips_hourly
    SELECT date_trunc('hour', tpep_pickup_datetime), coalesce(pulocationid, 0), count(*),
           coalesce(sum(fare_amount), 0), coalesce(sum(tip_amount), 0), coalesce(sum(total_amount), 0)
    FROM trips
    WHERE tpep_pickup_datetime >= $1::timestamp AND tpep_pickup_datetime < $2::timestamp
    GROUP BY 1, 2
    ON CONFLICT (hour, pickup_zone) DO UPDATE SET
      trips = trips_hourly.trips + excluded.trips,
      fares = trips_hourly.fares + excluded.fares,
      tips = trips_hourly.tips + excluded.tips,
      revenue = trips_hourly.revenue + excluded.revenue
  $$);
```

The backfill of 9.5 million trips into 240,917 rollup rows takes about 20 seconds. After that, the pipeline runs every minute and processes the hours
that have passed since the last run. Rows that arrive for an hour that was already processed
are not counted; the [sync use case](use-case-iceberg-sync.md#syncing-by-time-instead)
describes this trade-off, and the `ON CONFLICT` clause makes it safe to reprocess a range by
hand.

Dashboard queries on the rollup return the same results:

```sql
SELECT date_trunc('day', hour) AS day, sum(trips) AS trips, round(sum(revenue)) AS revenue
FROM trips_hourly
WHERE hour >= '2024-03-01' AND hour < '2024-04-01'
GROUP BY 1 ORDER BY 1;
```

They stayed under 100 milliseconds at about 340 queries per second with 32 clients, four times
the throughput of the raw table. Use the rollup for the summary panels, and the Iceberg table
for drill-downs and ad-hoc questions that the rollup cannot answer.

## Connect a dashboard tool

Create a user for the dashboard tool with read access to the tables it needs. Reading Iceberg
tables only requires `SELECT`, not the `lake_read` role:

```sql
CREATE ROLE dashboards LOGIN PASSWORD '...';
GRANT SELECT ON trips, zones, trips_hourly TO dashboards;
```

Then add PostgreSQL as a data source with that user. In Grafana, use the built-in PostgreSQL
data source and the `$__timeFilter` macro, which turns the dashboard's time range into a
`BETWEEN` filter that DuckDB uses to skip files:

```sql
SELECT date_trunc('hour', tpep_pickup_datetime) AS time, count(*) AS trips
FROM trips
WHERE $__timeFilter(tpep_pickup_datetime)
GROUP BY 1 ORDER BY 1;
```

Metabase, Superset, Tableau and other tools work the same way through their PostgreSQL
connectors.

## Going further

- [Performance](performance.md) explains pushdown, file skipping, the file cache and how to size
  pgduck_server's memory and threads.
- [Partitioning](iceberg-partitioning.md) covers other partition transforms, such as
  `month()` for longer histories or `bucket()` for filters on IDs.
- [Archiving partitions to Iceberg](use-case-archiving.md) shows how to keep recent rows in
  PostgreSQL and move older months into a table like `trips`.

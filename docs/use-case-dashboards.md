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

Create an Iceberg table partitioned by day, so that a dashboard's time range only reads the
files for those days:

```sql
CREATE TABLE trips (
  pickup_time timestamp NOT NULL,
  dropoff_time timestamp NOT NULL,
  pickup_zone int,
  dropoff_zone int,
  passengers int,
  distance double precision,
  fare numeric(10,2),
  tip numeric(10,2),
  total numeric(10,2),
  payment_type int
) USING iceberg WITH (partition_by = 'day(pickup_time)');
```

The trip records are published as one Parquet file per month. Load three months, keeping only
the trips that started in that month (the files contain a few rows with bad timestamps):

```sql
DO $$
DECLARE
  month date;
BEGIN
  FOR month IN SELECT generate_series('2024-01-01'::date, '2024-03-01', interval '1 month') LOOP
    EXECUTE format($sql$
      CREATE FOREIGN TABLE taxi_file () SERVER pg_lake
        OPTIONS (path 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s.parquet')
    $sql$, to_char(month, 'YYYY-MM'));

    EXECUTE format($sql$
      INSERT INTO trips
      SELECT tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid,
             passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type
      FROM taxi_file
      WHERE tpep_pickup_datetime >= %L AND tpep_pickup_datetime < %L
    $sql$, month, month + interval '1 month');

    DROP FOREIGN TABLE taxi_file;
  END LOOP;
END $$;
```

This takes about a minute and produces 9,554,722 rows in 91 files, one per day, 180 MB in
total. For data that keeps arriving as files, use a
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
SELECT date_trunc('day', pickup_time) AS day, count(*) AS trips, sum(total) AS revenue
FROM trips
WHERE pickup_time >= '2024-03-01' AND pickup_time < '2024-04-01'
GROUP BY 1 ORDER BY 1;

         day         | trips  |  revenue
---------------------+--------+------------
 2024-03-01 00:00:00 | 117638 | 3142038.32
 2024-03-02 00:00:00 | 122463 | 3037189.16
 2024-03-03 00:00:00 |  97000 | 2694353.59
 ...
```

The busiest pickup zones, with their tip percentage:

```sql
SELECT z.borough, z.zone, count(*) AS trips, round(100 * sum(t.tip) / sum(t.fare), 1) AS tip_pct
FROM trips t JOIN zones z ON z.zone_id = t.pickup_zone
WHERE t.pickup_time >= '2024-03-01' AND t.pickup_time < '2024-04-01'
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
SELECT extract(isodow FROM pickup_time) AS dow, extract(hour FROM pickup_time) AS hour, count(*)
FROM trips GROUP BY 1, 2 ORDER BY 1, 2;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM dropoff_time - pickup_time) / 60)
FROM trips WHERE pickup_time >= '2024-03-01' AND pickup_time < '2024-04-01';
```

Each of these took between 40 and 120 milliseconds once the files were in pgduck_server's
[file cache](performance.md#file-cache), measured on a 16-core machine. `EXPLAIN VERBOSE` shows
that the whole query runs in DuckDB, and how many files it reads:

```sql
EXPLAIN VERBOSE
SELECT count(*) FROM trips WHERE pickup_time >= '2024-03-10' AND pickup_time < '2024-03-17';

 Custom Scan (Query Pushdown)
   Engine: DuckDB
   Data Files Scanned: 7
   ...
```

Two things keep these queries fast:

- **Filter on the partition column.** A week of data reads 7 of the 91 files.
- **Keep lookup tables in Iceberg.** The same zones query with `zones` as a heap table took
  4.7 seconds instead of 0.12, because PostgreSQL joins the 3.6 million matching trips itself.
  If a lookup table is maintained as a regular table, refresh an Iceberg copy when it changes,
  for example with
  `BEGIN; DELETE FROM zones; INSERT INTO zones SELECT * FROM zones_source; COMMIT;`.

## Keep a rollup for busy panels

All queries on Iceberg tables share pgduck_server. On the same machine, the daily query above
ran at about 70 queries per second, whether 8 or 32 clients sent it, so with 32 clients each
query took 450 milliseconds. A wall display, or a dashboard that dozens of people refresh every
few seconds, is better served from a rollup.

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
    SELECT date_trunc('hour', pickup_time), coalesce(pickup_zone, 0), count(*),
           coalesce(sum(fare), 0), coalesce(sum(tip), 0), coalesce(sum(total), 0)
    FROM trips
    WHERE pickup_time >= $1::timestamp AND pickup_time < $2::timestamp
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
SELECT date_trunc('day', hour) AS day, sum(trips) AS trips, sum(revenue) AS revenue
FROM trips_hourly
WHERE hour >= '2024-03-01' AND hour < '2024-04-01'
GROUP BY 1 ORDER BY 1;
```

They took about 25 milliseconds with one client, and stayed under 100 milliseconds at about 350
queries per second with 32 clients. Use the rollup for the summary panels, and the Iceberg table
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
SELECT date_trunc('hour', pickup_time) AS time, count(*) AS trips
FROM trips
WHERE $__timeFilter(pickup_time)
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

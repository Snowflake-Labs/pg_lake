---
title: Performance
parent: User guide
nav_order: 5
---

# Performance
{: .no_toc }

pg_lake runs analytical queries on DuckDB's vectorized, multi-threaded engine, and keeps
remote files in a local cache. This page explains how to check what runs where, and what to
tune when a query or a load is slow.

1. TOC
{:toc}

## Query pushdown

When a query touches Iceberg tables or data lake files, pg_lake translates as much of it as it
can into a DuckDB query and sends it to pgduck_server. This is called *pushdown*. What cannot
be pushed down, such as a function DuckDB does not implement the same way, runs in PostgreSQL
on the rows DuckDB returns. Results are always the same; only the speed differs.

There are two levels of pushdown:

- **Full pushdown.** If every table in the query is an Iceberg table or data lake file, and
  every function, operator and type can be pushed down, the whole query, including joins,
  aggregates, sorting and `LIMIT`, runs in DuckDB. The plan shows a single
  `Custom Scan (Query Pushdown)`.
- **Partial pushdown.** Otherwise, each lake table is read with a `Foreign Scan` that pushes
  down the columns it needs and the filters it can, and PostgreSQL does the rest.

### Reading EXPLAIN output

`EXPLAIN (VERBOSE)` shows the SQL sent to DuckDB as `Vectorized SQL`, DuckDB's own plan below
it, and how many Iceberg files are read. Here the whole query is pushed down, and partition
pruning limits the scan to two data files:

```sql
EXPLAIN (VERBOSE, COSTS OFF)
SELECT event_type, count(*), sum(amount)
FROM events WHERE event_time >= '2026-05-01'
GROUP BY 1 ORDER BY 2 DESC;

                                            QUERY PLAN
---------------------------------------------------------------------------------------------------
 Custom Scan (Query Pushdown)
   Output: pushdown_query.event_type, pushdown_query.count, pushdown_query.sum
   Engine: DuckDB
   Data Files Scanned: 2
   Deletion Files Scanned: 0
   Vectorized SQL:  SELECT "event_type",
     "count"(*) AS "count",
     "sum"("amount") AS "sum"
    FROM public.events "events"("event_time", "user_id", "event_type", "amount")
   WHERE ("event_time" >= ('2026-05-01 00:00:00+00'::"text")::timestamp with time zone)
   GROUP BY "event_type"
   ORDER BY ("count"(*)) DESC
   ->  ORDER_BY
         Order By: count_star() DESC
         ->  HASH_GROUP_BY
               ->  PROJECTION
                     ->  READ_PARQUET
                           Filters: event_time>='2026-05-01 00:00:00+00'::TIMESTAMP WITH TIME ZONE
```

When part of a query cannot be pushed down, `EXPLAIN (VERBOSE)` lists the reason under
`Not Vectorized Constructs`. Here `width_bucket` is not available in DuckDB, so DuckDB only
reads the `amount` column, and PostgreSQL applies the filter and counts:

```sql
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM events WHERE width_bucket(amount, 0, 100, 5) > 3;

                                     QUERY PLAN
------------------------------------------------------------------------------------
 Aggregate
   Output: count(*)
   ->  Foreign Scan on public.events
         Output: event_time, user_id, event_type, amount
         Filter: (width_bucket(events.amount, '0'::numeric, '100'::numeric, 5) > 3)
         Engine: DuckDB
         Data Files Scanned: 6
         Deletion Files Scanned: 0
         Vectorized SQL:  SELECT "amount"
    FROM public.events "r192"("event_time", "user_id", "event_type", "amount")
         ->  READ_PARQUET
               Projections: amount
 Not Vectorized Constructs:
 1:      Function
         Description: pg_catalog.width_bucket(numeric,numeric,numeric,integer)
```

If you run into a function or operator that you need pushed down, please
[open an issue](https://github.com/Snowflake-Labs/pg_lake/issues).

### Joins between heap and Iceberg tables

A join between a heap table and an Iceberg table is partially pushed down: DuckDB scans the
Iceberg table, and PostgreSQL joins the rows with the heap table. `Not Vectorized Constructs`
then lists the heap table:

```sql
EXPLAIN (VERBOSE, COSTS OFF)
SELECT u.country, count(*) FROM events e JOIN users u USING (user_id) GROUP BY 1;

 HashAggregate
   Group Key: u.country
   ->  Hash Join
         Hash Cond: (e.user_id = u.user_id)
         ->  Foreign Scan on public.events e
               Vectorized SQL:  SELECT "user_id"
    FROM public.events "r209"("event_time", "user_id", "event_type", "amount")
         ->  Hash
               ->  Seq Scan on public.users u
 Not Vectorized Constructs:
 1:      Table
         Description: public.users
```

This works well when the Iceberg side is filtered down to a modest number of rows. For large
analytical joins, keep both sides in Iceberg, for example by keeping a copy of small dimension
tables in Iceberg, so the whole query runs in DuckDB.

### Controlling pushdown

| Setting or function | Effect |
|:--|:--|
| `pg_lake_table.enable_full_query_pushdown` | Set to `off` to only use per-table scans, for example to compare plans. |
| `pg_lake_table.enable_strict_pushdown` | When `on` (default), only functions and operators known to behave exactly as in PostgreSQL are pushed down. |

## Skipping files

Scans are fastest when they do not read most files at all. pg_lake skips Iceberg data files in
two ways, both visible as `Data Files Scanned` and `Data Files Skipped` in `EXPLAIN (VERBOSE)`:

- **Partition pruning** uses the table's [partition spec](iceberg-partitioning.md#partition-pruning)
  to skip files whose partition cannot match the filter.
- **Data file pruning** uses the minimum and maximum value of each column in each file, which
  pg_lake records in the Iceberg metadata. A filter like `WHERE event_time >= now() - interval
  '1 day'` then skips files that only contain older data, even without partitioning.

Data file pruning works best when rows that are queried together are written together, which is
naturally the case for time-ordered data. For string columns, pg_lake keeps the first 16 bytes
of the bounds by default; the `column_stats_mode` [table option](table-options.md) changes this.
Pruning does not apply to arrays, composite types, maps or geometry.

For files in object storage outside Iceberg, DuckDB uses the row group statistics in Parquet
files in the same way, and hive-style directory names such as `year=2026/` can be used as
columns in filters.

Deletes benefit too: a `DELETE` whose filter covers whole files only removes them from the
metadata. `Data Files Skipped` shows files that did not need to be touched:

```sql
EXPLAIN (VERBOSE, COSTS OFF) DELETE FROM events WHERE event_time < '2026-02-01';

 Delete on public.events
   ->  Foreign Scan on public.events
         Engine: DuckDB
         Data Files Scanned: 0
         Deletion Files Scanned: 0
         Data Files Skipped: 1
```

## File cache

Reading from object storage has high latency, so pgduck_server keeps copies of remote files on
local disk in the directory given by `--cache_dir`. Queries that find their files in the cache
read them from local disk instead.

- **Cache on write.** Files that pg_lake writes, up to `--cache_on_write_max_size` each (1 GB by
  default), are added to the cache as they are written, so recently written data is fast to
  query.
- **Cache on read.** The cache manager, a background worker in PostgreSQL, periodically
  downloads files that queries have recently read, and evicts the least recently used files to
  stay under `pg_lake_engine.max_cache_size` (20 GB by default). Size the cache to your working
  set, and put it on fast local storage such as NVMe.

You can look at and control the cache from SQL:

```sql
-- what is cached
SELECT path, pg_size_pretty(file_size), last_access_time
FROM lake_file_cache.list() ORDER BY last_access_time DESC;

-- warm the cache ahead of a query
SELECT lake_file_cache.add(path) FROM lake_file.list('s3://mybucket/sales/2026/*.parquet');
```

## Faster writes

- **Write in batches.** Each `INSERT`, `COPY` or `UPDATE` statement writes at least one new
  file. Batch rows into larger statements, or use a [staging table](iceberg-tables.md#loading-data).
- **Use `INSERT ... SELECT` and `COPY`.** When the source is itself a lake table or file, pg_lake
  pushes `INSERT ... SELECT` down to DuckDB, which writes the Parquet files directly.
- **Partitioned writes.** For large loads into a partitioned table, try
  `SET pg_lake_table.enable_partitioned_write_pushdown = on`; see
  [partitioning](iceberg-partitioning.md#configuring-open-files-for-partitioned-writes).
- **Uploads.** pg_lake uploads up to `pg_lake_engine.max_parallel_file_uploads` files at a time.
- **Keep files large.** Autovacuum [compacts](iceberg-maintenance.md#vacuum) small files, but
  it is cheaper not to create them in the first place.

## Prefer Parquet for files you query often

CSV and JSON files have to be parsed on every query, and cannot skip data using statistics.
If you query the same text files repeatedly, convert them to Parquet or load them into an
Iceberg table once; see [converting CSV and JSON to Parquet](data-lake-import-export.md#converting-csv-and-json-to-parquet).

## Sizing pgduck_server

pgduck_server uses up to 80 percent of system memory by default, and one thread per core.
On a server that also runs a busy PostgreSQL workload, limit it with `--memory_limit`, and
adjust DuckDB settings such as `threads` by connecting to pgduck_server directly:

```sql
-- psql -h /tmp -p 5332
SET GLOBAL threads = 8;
```

See [pgduck_server options](configuration.md#pgduck_server-options) for all options.

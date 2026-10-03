---
title: Import and export
parent: User guide
nav_order: 3
---

# Import and export
{: .no_toc }

pg_lake extends `COPY` and `CREATE TABLE` so that you can load data from files in object
storage or on the web into any table, and write any query result out as Parquet, CSV or JSON.

1. TOC
{:toc}

## Import data

### Create a table from a file

`load_from` creates a table with the columns of a file and loads its data, in one step:

```sql
-- a regular PostgreSQL table
CREATE TABLE trips () WITH (load_from = 's3://mybucket/trips/2026-09.parquet');

-- an Iceberg table
CREATE TABLE trips_iceberg () USING iceberg
WITH (load_from = 's3://mybucket/trips/2026-09.parquet');
```

`definition_from` only creates the columns, so you can load the data later, or add
indexes first:

```sql
CREATE TABLE trips () WITH (definition_from = 's3://mybucket/trips/2026-09.parquet');
```

With both options, leave the column list empty to infer the columns, or specify columns to use
your own types. Columns are matched by position, as with `COPY`.

### Load into an existing table

`COPY ... FROM` accepts a URL:

```sql
COPY trips FROM 's3://mybucket/trips/2026-10.parquet';

-- CSV options work as usual
COPY trips FROM 's3://mybucket/trips/2026-10.csv.gz' WITH (header true, delimiter ';');
```

Like PostgreSQL's own `COPY`, the file's columns are matched to the table's columns by
position; use `COPY table (col1, col2, ...)` to load into specific columns.

To load many files at once, or only some rows or columns, create a
[foreign table](query-data-lake-files.md) on the files and use `INSERT ... SELECT`:

```sql
CREATE FOREIGN TABLE trips_files () SERVER pg_lake
OPTIONS (path 's3://mybucket/trips/*.parquet');

INSERT INTO trips SELECT * FROM trips_files WHERE pickup_time >= '2026-10-01';
```

### Load new files as they arrive

When files keep landing in a bucket, for example a daily export from another system or logs
written every few minutes, you can load each file exactly once, as soon as it appears, with a
file list pipeline from [pg_incremental](https://github.com/CrunchyData/pg_incremental). The
pipeline first loads all files that already exist, then checks for new ones on a schedule, and
works for Iceberg and regular PostgreSQL tables alike.

It needs two pieces. First, a foreign table on the files with the `filename` option, which adds
a `_filename` column with the URL that each row came from:

```sql
CREATE FOREIGN TABLE orders_files () SERVER pg_lake
OPTIONS (path 's3://mybucket/inbox/*.csv', filename 'true');

-- the table to load into: Iceberg here, or a regular table without USING iceberg
CREATE TABLE orders (LIKE orders_files) USING iceberg;
```

Because `orders` is created with `LIKE`, it also gets the `_filename` column, so you can always
tell which file a row came from. To leave it out, list the columns in the pipeline command.

The columns of `orders_files` are inferred once, from the files that exist when you create it.
For a pipeline that will run for a long time, consider listing the columns explicitly, with
`_filename text` as the last one, so that a malformed file cannot change the inferred types.

Second, the pipeline. pg_incremental lists the files that match `file_pattern` with
`lake_file.list`, and runs the command with `$1` set to the files it has not processed yet.
Filtering on `_filename` means each run only reads those files:

```sql
SELECT incremental.create_file_list_pipeline('import-orders',
  file_pattern := 's3://mybucket/inbox/*.csv',
  batched := true,
  command := $$
    INSERT INTO orders SELECT * FROM orders_files WHERE _filename = any($1)
  $$);

NOTICE:  pipeline import-orders: processing file list pipeline for 2 files
NOTICE:  pipeline import-orders: scheduled cron job with ID 1 and schedule */15 * * * *
```

The files a pipeline has processed are recorded in `incremental.processed_files` in the same
transaction as the insert, so each file is loaded exactly once, even if a run fails and is
retried. Options that are worth knowing:

| Argument | Description |
|:--|:--|
| `batched` | `true` passes up to `max_batch_size` (default 100) paths as a `text[]`, loaded in one transaction. `false` (default) runs the command once per file, with `$1` a single path: `WHERE _filename = $1`. Batches write fewer, larger files, which is better for Iceberg. |
| `schedule` | pg_cron schedule for checking for new files. Default every 15 minutes. |
| `execute_immediately` | `false` skips loading the existing files when the pipeline is created; they are loaded on the first scheduled run instead. |
| `max_batches_per_run` | Limit the number of batches per run, to spread out a large backfill. Default no limit. |

To run a pipeline right away instead of waiting for the schedule, use
`CALL incremental.execute_pipeline('import-orders')`. If a file cannot be loaded, the whole
batch fails and is retried on the next run, without loading part of it. Fix or remove the file,
or mark it as processed so the pipeline moves on:

```sql
SELECT incremental.skip_file('import-orders', 's3://mybucket/inbox/orders-2026-09-04.csv');
```

`incremental.drop_pipeline('import-orders')` stops the pipeline. See the
[pg_incremental documentation](https://github.com/CrunchyData/pg_incremental) for monitoring,
and the [log management use case](use-case-log-management.md) for a complete example that also
transforms the rows on the way in. pg_incremental needs [pg_cron](https://github.com/citusdata/pg_cron)
in `shared_preload_libraries`, and pipelines are created in the database where pg_cron is
installed (`cron.database_name`).

### Formats and compression

The format is detected from the file extension; specify `format` for files without one:

```sql
COPY trips FROM 's3://mybucket/trips/latest' WITH (format 'parquet');
```

| Format | Extensions | Compression |
|:--|:--|:--|
| Parquet | `.parquet` | Detected from the file metadata. |
| CSV | `.csv`, `.csv.gz`, `.csv.zst` | `gzip`, `zstd` |
| JSON (newline-delimited) | `.json`, `.json.gz`, `.json.zst` | `gzip`, `zstd` |
| GDAL (geospatial) | See [geospatial](spatial.md#gdal-formats-shapefile-geopackage-and-more) | `zip`, `gzip` |

Parquet files record their compression internally. For CSV and JSON files whose name does not
show the compression, specify it:

```sql
-- without compression 'gzip', the file would be read as uncompressed CSV and fail
CREATE FOREIGN TABLE compressed () SERVER pg_lake
OPTIONS (path 's3://mybucket/data/export_file', format 'csv', compression 'gzip');
```

For CSV, pg_lake supports PostgreSQL's options such as `header`, `delimiter`, `quote`,
`escape` and `null`. See the [file formats reference](file-formats-reference.md) for all
options.

## Export data

`COPY ... TO` a URL writes a table or query result to object storage. The format and
compression follow from the file extension:

```sql
-- Parquet, with snappy compression by default
COPY trips TO 's3://mybucket/exports/trips.parquet';

-- the result of a query
COPY (SELECT * FROM trips JOIN zones USING (zone_id) WHERE pickup_time >= '2026-10-01')
TO 's3://mybucket/exports/october.parquet';

-- CSV, uncompressed and gzip-compressed
COPY trips TO 's3://mybucket/exports/trips.csv' WITH (header true);
COPY trips TO 's3://mybucket/exports/trips.csv.gz' WITH (header true);

-- newline-delimited JSON, compressed with zstd
COPY trips TO 's3://mybucket/exports/trips.json.zst';

-- Parquet with zstd compression
COPY trips TO 's3://mybucket/exports/trips.parquet' WITH (compression 'zstd');
```

The export runs on DuckDB when the query can be [pushed down](performance.md#query-pushdown),
which is typically the case for queries on Iceberg tables and files. Exports are written as
one file per `COPY`; to write a data set in many files, run several `COPY` statements, for
example one per day.

## Delete files

`lake_file.delete(url)` deletes one file from object storage. Deletion cannot be undone, so the
function is off until a superuser enables it with `pg_lake_table.enable_delete_file_function`,
which can be set for the whole server, for one database, or for one role:

```sql
-- let the exporter role delete files, as a superuser
ALTER ROLE exporter SET pg_lake_table.enable_delete_file_function = on;
```

The caller also needs the `lake_write` role (or `lake_read_write`), like any other write to a
URL. Only superusers can change the setting, so a role cannot turn it on for itself.

The URL must name a single file; wildcards are not expanded. To delete several files, list
them first:

```sql
-- delete last year's exports
SELECT lake_file.delete(path)
FROM lake_file.list('s3://mybucket/exports/2025/*.parquet');
```

Deleting a file that does not exist is not an error.

{: .warning }
Do not use `lake_file.delete` on files that belong to an Iceberg table. pg_lake removes those
itself through the [deletion queue](iceberg-maintenance.md#the-deletion-queue), after
`pg_lake_engine.orphaned_file_retention_period`. Deleting a file that a table still references
breaks queries on the table.

## Client-side import and export

pg_lake's formats also work with psql's `\copy`, which reads and writes files on the client
machine. Always specify the format and compression, since the server cannot see the local
file name:

```sql
-- import a compressed JSON file from local disk
\copy trips FROM '/tmp/trips.json.gz' WITH (format 'json', compression 'gzip')

-- export a Parquet file to local disk
\copy trips TO '/tmp/trips.parquet' WITH (format 'parquet')
```

## Converting CSV and JSON to Parquet

Parquet is columnar, compressed, and carries statistics that let queries skip data, so it is
much faster to query than CSV or JSON. To convert a set of text files, query them through a
foreign table and export the result:

```sql
CREATE FOREIGN TABLE thermostat_csv () SERVER pg_lake
OPTIONS (path 's3://mybucket/thermostat/*.csv');

COPY (SELECT * FROM thermostat_csv) TO 's3://mybucket/thermostat.parquet';

CREATE FOREIGN TABLE thermostat_parquet () SERVER pg_lake
OPTIONS (path 's3://mybucket/thermostat.parquet');
```

Even on a small file, the difference shows:

```sql
EXPLAIN ANALYZE SELECT * FROM thermostat_csv;
 Foreign Scan on thermostat_csv  (actual time=38.885..59.216 rows=7205 loops=1)
 Execution Time: 60.624 ms

EXPLAIN ANALYZE SELECT * FROM thermostat_parquet;
 Foreign Scan on thermostat_parquet  (actual time=5.427..21.359 rows=7205 loops=1)
 Execution Time: 26.496 ms
```

The gap grows with the size of the data, and with queries that only need some columns or
rows. If the data keeps growing or changing, load it into an
[Iceberg table](iceberg-tables.md) instead.

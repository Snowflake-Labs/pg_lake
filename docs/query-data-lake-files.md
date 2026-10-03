---
title: Query data lake files
parent: User guide
nav_order: 2
---

# Query data lake files
{: .no_toc }

You can query files in object storage or at public URLs directly, without loading them first,
by creating a foreign table on the `pg_lake` server. pg_lake reads Parquet, CSV, JSON, GDAL
formats, external Iceberg and Delta tables and more; see the
[file formats reference](file-formats-reference.md).

1. TOC
{:toc}

## Create a table for files

With an empty column list, the columns are inferred from the files:

```sql
CREATE FOREIGN TABLE taxi_trips () SERVER pg_lake
OPTIONS (path 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');

SELECT payment_type, count(*), round(avg(tip_amount)::numeric, 2) AS avg_tip
FROM taxi_trips
GROUP BY 1 ORDER BY 2 DESC;
```

The format and compression are detected from the file extension, and for CSV files, the
delimiter, quote character and header as well.

You can also declare the columns yourself, for example to use different types. Declared columns
are matched to the file's columns **by position**, not by name, so list them in the same order
as in the file. You can leave out trailing columns, but not columns in the middle. To check
what the file contains, and what pg_lake would infer, use `lake_file.preview`:

```sql
SELECT * FROM lake_file.preview('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');

      column_name      |         column_type
-----------------------+-----------------------------
 vendorid              | integer
 tpep_pickup_datetime  | timestamp without time zone
 tpep_dropoff_datetime | timestamp without time zone
 passenger_count       | bigint
 ...
```

To use only some columns of a wide file, infer all columns and create a view on the ones you
need.

Nested Parquet structs become composite types, and arrays and maps become PostgreSQL arrays
and [map types](https://github.com/Snowflake-Labs/pg_lake/blob/main/pg_map/README.md). Access a
struct field with parentheses, as in `(names).primary`.

## Explore your object store

`lake_file.list` lists the files that match a pattern, with their size and modification time:

```sql
SELECT path, file_size FROM lake_file.list('s3://pglakedemobucket/**/*.parquet');

                    path                    | file_size
--------------------------------------------+-----------
 s3://pglakedemobucket/out.parquet          |      1082
 s3://pglakedemobucket/table1/part1.parquet |   8142233
 s3://pglakedemobucket/table1/part2.parquet |   8140119
 s3://pglakedemobucket/table1/part3.parquet |   8139402
```

## Wildcards

A `path` can match many files. `*` matches any characters within one directory level, and `**`
matches any number of levels:

```sql
-- all Parquet files directly under table1/
CREATE FOREIGN TABLE table1 () SERVER pg_lake
OPTIONS (path 's3://pglakedemobucket/table1/*.parquet');

-- all compressed CSV files anywhere under logs/
CREATE FOREIGN TABLE all_logs () SERVER pg_lake
OPTIONS (path 's3://pglakedemobucket/logs/**/*.csv.gz');
```

The set of files is determined when you run a query, so new files that match the pattern are
included automatically.

### The source file of each row

With `filename 'true'`, the table gets an extra `_filename` column with the URL of the file
each row came from. Filtering on it only reads the matching files, which makes it useful for
[loading new files as they arrive](data-lake-import-export.md#load-new-files-as-they-arrive):

```sql
CREATE FOREIGN TABLE events_source () SERVER pg_lake
OPTIONS (path 's3://pglakedemobucket/events/*.csv', filename 'true');

SELECT _filename, count(*) FROM events_source GROUP BY 1;
```

If you list the columns yourself, add `_filename text` as the last column.

### Hive-style partitions

Data sets are often organized in directories named after a column value, like
`year=2026/month=09/`. pg_lake turns these directory names into columns:

```sql
-- files under s3://mybucket/hive/year=2025/ and s3://mybucket/hive/year=2026/
CREATE FOREIGN TABLE hive_events () SERVER pg_lake
OPTIONS (path 's3://mybucket/hive/**/*.parquet');

\d hive_events
                Foreign table "public.hive_events"
 Column |  Type   | Collation | Nullable | Default | FDW options
--------+---------+-----------+----------+---------+-------------
 id     | integer |           |          |         |
 v      | text    |           |          |         |
 year   | bigint  |           |          |         |
```

Filters on these columns skip the directories that do not match.

## Writable tables

A foreign table with `writable 'true'` accepts `INSERT`: each statement writes new files under
`location`. This is a simple way to append data to a directory that other tools read:

```sql
CREATE FOREIGN TABLE exported_events (id bigint, event_time timestamptz, payload text)
SERVER pg_lake
OPTIONS (writable 'true', format 'parquet', location 's3://mybucket/exported_events/');

INSERT INTO exported_events SELECT id, event_time, payload FROM events WHERE event_time >= current_date;
```

Writable tables support Parquet, CSV and JSON, and are append-only. For a table you want to
update or delete from, or that other engines should see transactionally, use an
[Iceberg table](iceberg-tables.md).

## Querying across regions and providers

pg_lake detects the region of S3 buckets automatically, so you can query buckets in any region
and different storage providers in the same query, given [credentials](configuration.md#object-storage-credentials).
The [file cache](performance.md#file-cache) keeps files you query on local disk, which reduces
repeated transfers. For data you query often, a bucket in the same region as your server is
still the fastest and cheapest option.

## Performance

Queries on data lake files run on DuckDB's vectorized engine, and filters on Parquet files use
the row group statistics to skip data. Parquet is much faster to query than CSV or JSON; if you
query the same text files repeatedly, [convert them to Parquet](data-lake-import-export.md#converting-csv-and-json-to-parquet)
or load them into an Iceberg table. See [performance](performance.md) for how to check what is
pushed down.

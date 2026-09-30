---
title: Query data lake files
parent: User guide
nav_order: 2
---

# Query data lake files 
You can query files in object storage or at public URLs directly by creating
a lake analytics table. See [data lake formats](file-formats-reference.md) for a list of supported file types.

Lake analytics tables are foreign tables with `server pg_lake`
pointing to external files in your data lake in a variety of supported formats.

For instance, you can create a table from a directory of Parquet files in S3 and
immediately start querying them:

```sql
-- Load a GeoParquet file without specifying columns
-- Geometry columns are automatically recognized
-- Struct/map types are auto-generated
create foreign table ov_buildings ()
server pg_lake
options (path 's3://overturemaps-us-west-2/release/2024-08-20.0/theme=buildings/type=*/*.parquet');

postgres=> \d ov_buildings
                                                          Foreign table "public.ov_buildings"
┌────────────────────────┬─────────────────────────────────────────────────────────────────────────────┬───────────┬──────────┬─────────┬─────────────┐
│         Column         │                                    Type                                     │ Collation │ Nullable │ Default │ FDW options │
├────────────────────────┼─────────────────────────────────────────────────────────────────────────────┼───────────┼──────────┼─────────┼─────────────┤
│ id                     │ text                                                                        │           │          │         │             │
│ geometry               │ geometry                                                                    │           │          │         │             │
│ bbox                   │ lake_struct.xmin_xmax_ymin_ymax_35464140                                 │           │          │         │             │
...
│ type                   │ text                                                                        │           │          │         │             │
└────────────────────────┴─────────────────────────────────────────────────────────────────────────────┴───────────┴──────────┴─────────┴─────────────┘
Server: pg_lake
FDW options: (path 's3://overturemaps-us-west-2/release/2024-08-20.0/theme=buildings/type=*/*.parquet')
```

pg_lake tables support multiple formats, including
Parquet, CSV, and newline-delimited JSON. Additionally, each format has more
specialized options. 

## Explore your object store files

You can view the list of files in your supported cloud storage bucket directly
from your database, using the `lake_file.list()` utility function. Just
pass in the URL pattern for your bucket to see the results:

```sql
SELECT path FROM lake_file.list('s3://pglakedemobucket/**/*.parquet');
```

```
                                    path  
-----------------------------------------------------------------------------
 s3://pglakedemobucket/out.parquet
 s3://pglakedemobucket/table1/part1.parquet
 s3://pglakedemobucket/table1/part2.parquet
 s3://pglakedemobucket/table1/part3.parquet
 s3://pglakedemobucket/tmp/AI.parquet
 s3://pglakedemobucket/tmp/map.parquet
 s3://pglakedemobucket/tmp/map/dec98925-3ad9-4056-8bd6-9ec6bdb8082c.parquet
 s3://pglakedemobucket/tmp/out.parquet
(8 rows)
```
## Wildcards

You can use wildcards to find all `*.parquet` files anywhere in your
S3 bucket, or use a more restrictive pattern to limit to a single directory:

```sql
SELECT path FROM lake_file.list('s3://pglakedemobucket/table1/*');
```

```
                    path  
---------------------------------------------
 s3://pglakedemobucket/table1/part1.parquet
 s3://pglakedemobucket/table1/part2.parquet
 s3://pglakedemobucket/table1/part3.parquet
(3 rows)
```

### Show filename in table

When working with wildcards and larger groups of files, you can include filename 'true' in the create options to add a column to the foreign table to include the filename. 

```sql
create foreign table events_source ()
server pg_lake 
options (path 's3://pglaketest/events/*.csv', filename 'true');
```

## Querying across regions

pg_lake will automatically detect the region of the S3 bucket you are querying and configure itself accordingly to provide a seamless experience. Our automatic caching will download files you query to fast local drives to minimize network traffic across regions and maximize query performance. As a best practice, we still recommend that S3 buckets you access frequently and your `pg_lake` server are in the same region to get the fastest caching performance and avoid network charges altogether.

## Vectorized query execution

`pg_lake` extends PostgreSQL with a vectorized query engine
designed to accelerate analytics tasks. Vectorized execution improves efficiency
by processing data in batches, improving computational throughput. Performance
is improved by pushing down query computation to this engine when possible.
This is especially beneficial for tables of Parquet files.

### Query pushdown

When computations are pushed down, they are processed directly within the
vectorized query engine. However, if certain computations cannot be handled by
the vectorized engine, they are executed normally in PostgreSQL instead. See the
Iceberg tables page for more information about
[query pushdown](iceberg-tables.md#query-pushdown-with-iceberg-tables).

# Iceberg tables

Iceberg tables are transactional, columnar tables stored in object storage, optimized for fast analytics. 

## Creating an Iceberg table

Iceberg tables can be created via the regular CREATE TABLE syntax by appending using iceberg (or its alias using `pg_lake_iceberg`).

For instance, here’s how to create an Iceberg table and insert a row:

```sql
-- create a new Iceberg table in managed storage
create table measurements (
  station_name text not null,
  measurement double precision not null
)
using iceberg;

insert into measurements values ('Istanbul', 18.5);
```

You can also create an Iceberg table in your own S3 bucket, as long as it is in the same region as your Postgres server running. **Note that you must have set your [credentials](../README.md#connecting-pg_lake-to-s3-or-compatible)** to be able to access your private bucket.

```sql
-- create a table in your own S3 bucket (must be in same region)
create table measurements (
  station_name text not null,
  measurement double precision not null
)
using iceberg with (location = 's3://mybucket/measurements/');

-- or, configure the default_location_prefix for the session or the user (requires superuser)
set pg_lake_iceberg.default_location_prefix to 's3://mybucket/iceberg';

create table measurements (
  station_name text not null,
  measurement double precision not null
)
using iceberg;
```


Creating an Iceberg table from a query result is also supported:

```sql
-- create a table in a specific bucket from a query result
create table measurements
using iceberg with (location = 's3://mybucket/measurements/');
as select md5(s::text), s from generate_series(1,1000000) s;
```

Finally, you can create an Iceberg table from a file using the load_from or definition_from option:

```sql
-- Convert a file directly into an Iceberg table
create table taxi_yellow ()
using iceberg
with (load_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');

-- or, inherit the columns from a file, but do not load any data (yet)
create table taxi_yellow ()
using iceberg
with (definition_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');
```

## Iceberg table options

Iceberg tables support the following options when creating the table:

| Option   | Description                                                          |
| -------- | -------------------------------------------------------------------- |
| location | URL prefix for the Iceberg table (e.g. `s3://mybucket/measurements`) |

Additionally, when creating the Iceberg table from a file, the following options are supported along with the format-specific options listed in the [data lake formats](../docs/file-formats-reference) section:

| Option          | Description                                                                 |
| --------------- | --------------------------------------------------------------------------- |
| format          | The format of the file to load and/or infer columns from                    |
| definition_from | URL of a file to infer the columns from                                     |
| load_from       | URL of a file to infer the columns from and immediately load into the table |


When creating an Iceberg table, you can use various advanced PostgreSQL features, including:

- [**Serial types**](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL)
- [**Identity columns**](https://www.postgresql.org/docs/current/ddl-identity-columns.html)
- [**Check and not null constraints**](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS)
- [**Generated columns**](https://www.postgresql.org/docs/current/ddl-identity-columns.html)
- [**Custom composite types**](https://www.postgresql.org/docs/current/sql-createtype.html)
- [**Custom functions in expressions**](https://www.postgresql.org/docs/current/sql-createfunction.html)
- [**Triggers**](https://www.postgresql.org/docs/current/sql-createtrigger.html)
- [**PostgreSQL declarative partitioning**](https://www.postgresql.org/docs/current/ddl-partitioning.html) (not recommended, see recommended [**Iceberg (Hidden) Partitioning**](#iceberg-hidden-partitioning)
- [**Inheritance**](https://www.postgresql.org/docs/current/tutorial-inheritance.html) (not recommended)
- [**Collations**](https://www.postgresql.org/docs/current/collation.html) (not recommended)
- [**PostGIS geometry type**](https://postgis.net/workshops/postgis-intro/geometries.html)

Consult the [**PostgreSQL documentation**](https://www.postgresql.org/docs/current/sql-createtable.html) on how to use each feature.

There are a few limitations to be aware of:

- Collations, declarative partitioning, and inheritance work as expected, but may result in suboptimal query plans.
- Numeric types that do not specify a precision and scale are treated as numeric(38,9).
- Numeric values cannot be NaN or infinite.
- Some types are not yet supported as column types in Iceberg tables:
    - Numeric with precision > 38
    - Intervals
    - Other tables used as column types.
    - Not all geometry types are supported in Iceberg tables (only point, linestring, polygon, multipoint, multilinestring, multipolygon, geometrycollection), and geometry types cannot be nested in an array or composite type.
- Custom base types other than geometry are stored using their text representation, which may lead to suboptimal performance.


## Loading data into an Iceberg table

There are a few different ways of loading data into an existing Iceberg table:

1. `COPY ... FROM '<url>'` - Load data from S3 or a http(s) URL
2. `COPY ... FROM STDIN` - Load data from the client (\copy in psql)
3. `INSERT INTO ... SELECT` - Load data from a query result
4. `INSERT INTO ... VALUES` - Insert individual row values

It is recommended to load data in batches. Each statement creates one or more new Parquet files and adds them to the Iceberg table. Doing single row inserts will result in many small Parquet files, which can slow down queries.

If your applications needs to do single row inserts, it is recommended to create a staging table and periodically flush new batches into the Iceberg table:

```sql
-- create a staging table
create table measurements_staging (like measurements);

-- do fast inserts on a staging table
insert into measurements_staging values ('Haarlem', 9.3);

-- periodically move all the rows from a queue table into Iceberg using pg_cron
select cron.schedule('flush-queue', '* * * * *', $$
  with new_rows as (
    delete from measurements_staging returning *
  )
  insert into measurements select * from new_rows;
$$);
```

Whether or not you use batches or a staging table, it is important to regularly vacuum your table to optimize the file sizes.

## Iceberg (Hidden) Partitioning

Iceberg's hidden partitioning physically organizes data based on column values (like **`event_time`** or **`user_id`**) to accelerate queries and data management operations. Unlike PostgreSQL's [**declarative partitioning**](https://www.postgresql.org/docs/current/ddl-partitioning.html), which requires creating child tables manually, Iceberg partitioning is defined once at the table level and managed automatically.

**Key benefits:**
- **Automatic organization**: Define partitioning expressions like **`day(event_time)`** or **`bucket(16, user_id)`**, and Iceberg handles file organization
- **Multi-column support**: Partition by multiple expressions simultaneously (e.g., by day and user bucket)
- **Evolvable**: Change partitioning strategies without rewriting the table—new data uses the new layout while old data remains readable
- **Fast metadata operations**: Deletes matching entire partitions skip data scanning entirely

Each partition stores data in one or more Parquet files. The query engine automatically tracks partition membership, and [**VACUUM**](#vacuuming-an-iceberg-table) merges small files over time to maintain performance.

### Defining partitions

To define partitioning for an Iceberg table, use the `WITH (partition_by = '...')` option when creating the table.

The following example defines two partition expressions:
- `day(event_time)` for time-based filtering
- `bucket(32, user_id)` for distributing data evenly by user and filtering by `user_id`

```sql
CREATE TABLE events (
  event_time timestamptz NOT NULL,
  user_id    bigint       NOT NULL,
  region     text         NOT NULL,
  event_type text,
  payload    jsonb
)
USING iceberg
WITH (
  partition_by = 'day(event_time), bucket(32, user_id)'
);
```

The `partition_by` option can be used with other Iceberg table options as well as with `CREATE TABLE AS`.

### Evolving partitions

You can change the partitioning strategy later without rewriting or recreating the table. Partition changes apply only to newly written data. Existing files retain their original layout, and Iceberg tracks partition history internally. This makes it easy to experiment and adapt as access patterns evolve.

**Changing an existing partition strategy:**
```sql
ALTER TABLE events 
OPTIONS (
  SET partition_by 'truncate(4, region), day(event_time)'
);
```

**Adding partitioning to an unpartitioned table:**
```sql
ALTER TABLE events
OPTIONS (
  ADD partition_by 'truncate(4, region), day(event_time)'
);
```

**Removing partitioning entirely:**
```sql
ALTER TABLE events 
OPTIONS (
  DROP partition_by
);
```

### Inspecting partitions

You can inspect the current partitioning with `\d` in `psql`. Look for `partition_by` in the `FDW options` section:

```sql
\d events 
                            Foreign table "public.events"
   Column   |           Type           | Collation | Nullable | Default | FDW options 
------------+--------------------------+-----------+----------+---------+-------------
 event_time | timestamp with time zone |           | not null |         | 
 user_id    | bigint                   |           | not null |         | 
 region     | text                     |           | not null |         | 
 event_type | text                     |           |          |         | 
 payload    | jsonb                    |           |          |         | 
Server: pg_lake_iceberg

FDW options: (partition_by 'day(event_time), bucket(32, user_id)', location 's3://mybucket/postgres/public/events/123085')
```

### Supported partition transforms

Iceberg supports several transform functions that determine how data is partitioned. Choose the transform based on your data type and query patterns.

| **Transform** | **Description** | **Supported types** |
| **`col`** | Identity partitioning, stores the column’s value as-is. Useful for low-cardinality columns like **`region`** or **`status`**. | **`date`**, **`timestamp(tz)`**, **`time`**, **`int2`**, **`int4`**, **`int8`**, **`bool`**, **`float4`**, **`float8`**, **`numeric`**, **`text`**, **`varchar`**, **`bpchar`**, **`bytea`**, **`uuid`** |
| **`year(col)`** | Extracts the year part of a date or timestamp. Good for coarse time partitioning. | **`date`**, **`timestamp(tz)`** |
| **`month(col)`** | Extracts the year and month. Typically used for monthly aggregates or logs. | **`date`**, **`timestamp(tz)`** |
| **`day(col)`** | Extracts the full date (year-month-day). Ideal for time-series or log data. | **`date`**, **`timestamp(tz)`** |
| **`hour(col)`** | Extracts the hour of day. Useful for high-volume hourly events. | **`time`**, **`timestamp(tz)`** |
| **`bucket(N, col)`** | Hashes the column and assigns it to one of **`N`** evenly distributed buckets. | **`int2`**, **`int4`**, **`int8`**, **`numeric`**, **`text`**, **`varchar`**, **`char(C)`**, **`bytea`**, **`uuid`**, **`date`**, **`timestamp(tz)`**, **`time`** |
| **`truncate(N, col)`** | Truncates the value to a multiple of **`N`** (for integers) or to a prefix of length **`N`** (for string and binary). | **`int2`**, **`int4`**, **`int8`**, **`text`**, **`varchar`**, **`char(C)`**, **`bytea`** |

### Partition pruning

Partition pruning is how Iceberg avoids scanning unnecessary data files. When you filter by a partition column (or one of its transforms), only the matching partition files are read—the rest are skipped entirely. The pruning happens at the file level using Iceberg's metadata.

**Supported operators by transform:**
- **Time transforms** (`year`, `month`, `day`, `hour`): `>`, `<`, `>=`, `<=`, `=`, `IN`, `ANY`, `BETWEEN`
- **`bucket` transform**: Only `=` (equality)
- **Other transforms** (`truncate`, identity): `>`, `<`, `>=`, `<=`, `=`, `IN`, `ANY`, `BETWEEN`

**Verifying partition pruning:**

You can confirm pruning by checking the `Data Files Scanned:` line in `EXPLAIN (verbose)` output. Fewer files scanned means better pruning.

Example: Create a table partitioned by year and insert rows from different years:

```sql
CREATE TABLE t_year_partitioned (
  event_time timestamptz NOT NULL,
  message     text
)
USING iceberg
WITH (
  partition_by = 'year(event_time)'
);

INSERT INTO t_year_partitioned VALUES
  ('2023-05-20', 'hello from 2023'),
  ('2024-08-10', 'hello from 2024');
```

Query with a filter that matches only one partition:

```sql
EXPLAIN (verbose)
SELECT * FROM t_year_partitioned
WHERE event_time >= '2024-01-01';
                                      QUERY PLAN                                      
--------------------------------------------------------------------------------------
 Custom Scan....
   ....
   Data Files Scanned: 1
```

**Partition pruning for DELETE operations:**

Iceberg only processes relevant data files when removing data for a given partition. Look for `Data Files Skipped:` in the `EXPLAIN (verbose)` output—it shows how many files were avoided entirely. This makes large-scale data retention operations fast and efficient.

```sql
EXPLAIN (verbose) 
DELETE FROM t_year_partitioned WHERE event_time >= '2024-01-01';

                                      QUERY PLAN                                      
--------------------------------------------------------------------------------------
 Delete on public.t_year_partitioned ...
   ...
   Data Files Skipped: 1
```

In this example, the partition for `year=2023` is skipped because the filter only matches `year=2024`.

### Best practices

Partitioning can greatly improve query performance at scale, but it also introduces overhead during writes and maintenance. Use these guidelines to get the most out of Iceberg's hidden partitioning.

**When to use partitioning:**
- **Use partitioning** for datasets larger than ~10GB that are queried with predictable filters (e.g., `WHERE event_time >= ...`)
- **Skip partitioning** for small tables or tables that are mostly scanned in full

**Performance trade-offs:**
- **Write performance**: Partitioned tables have slower writes because the engine must evaluate partition expressions, organize data into partition files, and manage more metadata
- **Query performance**: Partitioning allows the query engine to skip most files during filtered queries, dramatically reducing scan time

**Avoid high-cardinality partitions:**

More partitions mean more small files, which hurt performance and increase operational overhead. If you partition by high-cardinality columns (e.g., `user_id`, `uuid`, or raw `event_time`), Iceberg may create thousands of tiny files, leading to:
- Slower write performance
- Increased metadata size
- Higher planning and listing overhead
- More frequent `VACUUM` needs to merge files

**Use transforms to control cardinality:**
- Instead of `user_id`, use `bucket(32, user_id)` to spread users across a fixed number of partitions
- Instead of raw timestamps, use `year(event_time)` or `month(event_time)` depending on query patterns
- For strings, use `truncate(N, col)` to limit cardinality

**Match partitioning to query patterns:**

Let your common filter patterns guide your partitioning strategy:
- **Time-based data** → `year(event_time)`, `month(event_time)`
- **User-specific queries** → `bucket(32, user_id)`
- **Region or category filters** → `region` or `truncate(4, region)`

### Iceberg hidden partitioning limitations

- Renaming or dropping columns that are ever used in **`partition_by`** is not supported.
- After changing a table’s partitioning strategy, existing data continues to use the old layout — there’s currently no built-in way to rewrite past data with the new partitioning.
- Columns with collations are not allowed in **`partition_by`**.
- Composite types are not allowed in **`partition_by`**.


## Query pushdown with Iceberg tables

`pg_lake` extends PostgreSQL with a vectorized query engine designed to accelerate analytics tasks. Vectorized execution improves efficiency by processing data in batches, improving computational throughput. Performance is improved by pushing down query computation to this engine when possible.

### How query pushdown works
When computations are pushed down, they are processed directly within the vectorized query engine. However, if certain computations cannot be handled by the vectorized engine, they are executed normally in PostgreSQL instead.

### Monitoring query pushdown
To monitor which parts of your query are pushed down, you can use the `EXPLAIN` command with the `VERBOSE` option. This command shows how the query is executed, showing whether elements are processed by the vectorized engine or on the Postgres server. Look for `Vectorized SQL` in the output to see what is executed by the vectorized engine. If you see `Custom Scan (Query Pushdown)` in the output, you are all set, the computation is delegated to the vectorized engine.

You can also see the full list of objects (relations, collations, types, functions, operators, etc.) that cannot be processed by the vectorized engine when you use EXPLAIN (VERBOSE); they are listed at the bottom of the output. When you see any such object in the list, it means that only part of the query (or multiple parts) will be delegated to the vectorized query engine, indicated by a ForeignScan.

### Full pushdown example
Here is an example where the entire computation is pushed down:

```sql
EXPLAIN (VERBOSE) SELECT inv_warehouse_sk, count(*) FROM inventory GROUP BY inv_warehouse_sk ORDER BY count(*) DESC;
                                              QUERY PLAN
-------------------------------------------------------------------------------------------------------
 Custom Scan (Query Pushdown)  (cost=0.00..0.00 rows=0 width=0)
   Output: pushdown_query.inv_warehouse_sk, pushdown_query.count
   Engine: DuckDB
   Vectorized SQL:  SELECT inv_warehouse_sk,
     count(*) AS count
    FROM public.inventory inventory(inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
   GROUP BY inv_warehouse_sk
   ORDER BY (count(*)) DESC
   ->  PROJECTION
         Estimated Cardinality: 66555000
         ->  ORDER_BY
               Order By: count_star() DESC
               ->  PROJECTION
                     Estimated Cardinality: 66555000
                     ->  PROJECTION
                           Estimated Cardinality: 66555000
                           ->  PERFECT_HASH_GROUP_BY
                                 Groups: #0
                                 Aggregates: count_star()
                                 ->  PROJECTION
                                       Projections: inv_warehouse_sk
                                       Estimated Cardinality: 133110000
                                       ->  PROJECTION
                                             Projections: __internal_compress_integral_utinyint(#0, 1)
                                             Estimated Cardinality: 133110000
                                             ->  READ_PARQUET
                                                   Function: READ_PARQUET
                                                   Projections: inv_warehouse_sk
                                                   Estimated Cardinality: 133110000
 Query Identifier: -4465180302191104329
(30 rows)
```

### Partial pushdown example
In this example not all computations are pushed down:
EXPLAIN (VERBOSE) SELECT count(*) FROM inventory WHERE width_bucket(inv_item_sk, 0.024, 10.06, 5)  > 19 ;

```sql
                                              QUERY PLAN
------------------------------------------------------------------------------------------------------
 Aggregate  (cost=128.33..128.34 rows=1 width=8)
   Output: count(*)
   ->  Foreign Scan on public.inventory  (cost=100.00..127.50 rows=333 width=0)
         Output: inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand
         Filter: (width_bucket((inventory.inv_item_sk)::numeric, 0.024, 10.06, 5) > 19)
         Engine: DuckDB
         Vectorized SQL:  SELECT inv_item_sk
    FROM public.inventory inventory(inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
         ->  READ_PARQUET
               Function: READ_PARQUET
               Projections: inv_item_sk
               Estimated Cardinality: 133110000
Not Vectorized Constructs: 
 1:      Function
         Description: pg_catalog.width_bucket(numeric,numeric,numeric,integer)
```

In the example above, the `width_bucket` function is not yet supported by the vectorized query engine, hence it is executed on the Postgres server.

Future versions of pg_lake aim to support more SQL functions and operators within the vectorized engine. Open an issue if there are specific functions or operators you need for your use case.

## Making Iceberg the default table format

It is possible to make all create table statements automatically use Iceberg:

```sql
set default_table_access_method to 'iceberg';

-- automatically created as Iceberg
create table users (userid bigint, username text, email text);
```

It can be useful to assign `default_table_access_method` to a specific database user for tools that are unaware of Iceberg (see below). However, some PostgreSQL features such as temporary and unlogged tables stop working unless you explicitly add using heap.

## Copy external PostgreSQL tables to Iceberg
A big advantage of being able to change the `default_table_access_method` to Iceberg is that it can significantly simplify data migrations.

For instance, you can connect to your database as the `postgres superuser` and (temporarily) make the following configuration change to a regular user to make all create table statements use Iceberg by default:

```sql
alter user application set default_table_access_method to 'iceberg';
```

You can then replicate a PostgreSQL table from a regular database cluster on any managed Postgres service (e.g. Amazon RDS, Azure Database for PostgreSQL, Google Cloud SQL) into Iceberg using `pg_dump` and `psql`:

```shell
pg_dump --table=my_table --section=pre-data --section=data --no-table-access-method --no-owner --clean \
  | psql postgres://application@to563mempze.sfengineering-pgtest.preprod.us-west-2.aws.postgres.snowflake.app:5432/postgres
```

Several important pg_dump options are shown in the above command:
--`section=pre-data` and --`section=data` are required to ensure that we get the create table statement and a copy statement with data, but not other objects like indexes, which are not supported on Iceberg.
--`no-table-access-method` is required to prevent pg_dump from setting the default_table_access_method to the original value (usually heap), which would override the user-level setting (Iceberg).
--`no-owner` is not required, but useful if database users and access controls do not match between source and destination.
--`clean` is needed only if you want to replace the original table.

With these options, you can replicate from any heap table on any PostgreSQL server to Iceberg managed by pg_lake in a single step.


## Viewing the Iceberg catalog
After creating a table, the Iceberg table appears in the `iceberg_tables` view:

```sql
-- query the main Iceberg catalog table
select catalog_name, table_namespace, table_name, metadata_location from iceberg_tables;

┌──────────────┬─────────────────┬──────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ catalog_name │ table_namespace │  table_name  │                                                       metadata_location                                                        │
├──────────────┼─────────────────┼──────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ postgres     │ public          │ measurements │ s3://testbucket/iceberg/postgres/public/measurements/metadata/00003-6403833e-0766-4496-ad47-ec9641ee965f.metadata.json │
└──────────────┴─────────────────┴──────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

The schema of this view matches the expectations of the [**Iceberg catalog JDBC driver**](https://iceberg.apache.org/docs/latest/jdbc/#configurations), and the SQL catalog drivers of [**pyiceberg**](https://py.iceberg.apache.org/reference/pyiceberg/catalog/sql/) and [**iceberg-rust**](https://rust.iceberg.apache.org/api/iceberg_catalog_sql/struct.SqlCatalog.html). That means that Spark and other Iceberg tools can directly access the latest version of your Iceberg tables by connecting to Postgres with `pg_lake` as a catalog.

For Iceberg tables created via PostgreSQL, the `catalog_name` value will always match the database name. If the database is renamed, the `catalog_name` will change as well. External drivers cannot yet write to Iceberg tables created via `pg_lake`. They can create Iceberg tables with other catalog names, but those do not have a corresponding PostgreSQL table in the database.

### Inspecting an Iceberg table
Iceberg tables are internally represented by foreign tables, but they can mostly be used as though they were regular tables.
If you show all the tables in `psql` using `\d+` , the Iceberg table will show up with Type “foreign table”. It will also show the total size of the data files that are currently part of the table.

```sql
postgres=> \d+
                                                        List of relations
┌────────┬─────────────────────────┬───────────────┬───────────────────┬─────────────┬───────────────┬────────────┬─────────────┐
│ Schema │          Name           │     Type      │       Owner       │ Persistence │ Access method │    Size    │ Description │
├────────┼─────────────────────────┼───────────────┼───────────────────┼─────────────┼───────────────┼────────────┼─────────────┤
│ public │ measurements            │ foreign table │ application       │ permanent   │               │ 5238 MB    │             │
│ public │ taxi_yellow             │ foreign table │ application       │ permanent   │               │ 92 MB      │             │
└────────┴─────────────────────────┴───────────────┴───────────────────┴─────────────┴───────────────┴────────────┴─────────────┘
```

You can also get the size from the `pg_table_size` function, thoughpg_total_relation_size is not yet implemented and returns 0.

```sql
postgres=> select pg_table_size('measurements');
┌───────────────┐
│ pg_table_size │
├───────────────┤
│    5492615447 │
└───────────────┘
(1 row)
```

You can also inspect the columns of the table with `\d table_name` when using `psql`.

```sql
postgres=> \d measurements
                       Foreign table "public.measurements"
┌──────────────┬──────────────────┬───────────┬──────────┬─────────┬─────────────┐
│    Column    │       Type       │ Collation │ Nullable │ Default │ FDW options │
├──────────────┼──────────────────┼───────────┼──────────┼─────────┼─────────────┤
│ station_name │ text             │           │ not null │         │             │
│ measurement  │ double precision │           │ not null │         │             │
└──────────────┴──────────────────┴───────────┴──────────┴─────────┴─────────────┘
Server: pg_lake_iceberg
FDW options: (location 's3://testbucket/iceberg/postgres/public/measurements')
```

To inspect the [Iceberg table metadata](https://iceberg.apache.org/spec/#table-metadata), you can use the `lake_iceberg.metadata(url text)` function:

```sql
-- Returns metadata JSONB according to the Iceberg spec
select lake_iceberg.metadata(metadata_location) metadata from iceberg_tables
where table_name = 'measurements';

{ ... }

-- For instance, see the current schema in the Iceberg metadata
with ice as (select lake_iceberg.metadata(metadata_location) metadata from iceberg_tables where table_name = 'measurements')
select jsonb_pretty(metadata->'schemas'->(metadata->>'current-schema-id')::int) from ice;

┌─────────────────────────────────────┐
│            jsonb_pretty             │
├─────────────────────────────────────┤
│ {                                  ↵│
│     "type": "struct",              ↵│
│     "fields": [                    ↵│
│         {                          ↵│
│             "id": 1,               ↵│
│             "name": "station_name",↵│
│             "type": "string",      ↵│
│             "required": true       ↵│
│         },                         ↵│
│         {                          ↵│
│             "id": 2,               ↵│
│             "name": "measurement", ↵│
│             "type": "double",      ↵│
│             "required": true       ↵│
│         }                          ↵│
│     ],                             ↵│
│     "schema-id": 2                 ↵│
│ }                                   │
└─────────────────────────────────────┘
(1 row)
```

To see which files are currently part of the table, you can use the `lake_iceberg.files(metadata_url text)` function:

```sql
-- list all the data files in an Iceberg table
select
  manifest_path,
  content,
  file_path,
  file_format,
  spec_id,
  record_count,
  file_size_in_bytes
from
  iceberg_tables,
  lake_iceberg.files(metadata_location) f
where
  table_name = 'measurements';
```

## Update/delete on an Iceberg table
pg_lake can perform advanced update/delete queries in Iceberg tables, including modifying CTEs and updates/deletes with joins or subqueries.

```sql
-- delete rows from one table and move them to another table
with deleted_rows as (
   delete from user_assets where userid = 319931 returning * ) 
insert into deleted_assets select * from deleted_rows;
```

Queries and transaction blocks involving multiple tables (Iceberg or heap) always preserve PostgreSQL's ACID properties.

An update/delete command locks the table, such that only one update/delete can run at a time.

Some related modification queries are not supported on  Iceberg tables, namely:

- Queries with system columns (`ctid`)
- `SELECT ... FOR UPDATE`
- `MERGE`
- `INSERT ... ON CONFLICT`

## Altering an Iceberg table
You can change the schema of your Iceberg tables via the ALTER TABLE command. You can add, change, rename, and drop columns. Several other commands related to triggers and ownership are also supported.

```sql
-- Add a column to an Iceberg table
alter table measurements add column measurement_tim timestamptz;

-- Set the default for new values
alter table measurements alter column measurement_tim set default now();

-- Rename a column
alter table measurements rename column measurement_tim to measurement_time;

-- Drop a column
alter table measurements drop column measurement_time;

-- Change owner to another postgres user
alter table measurements owner to oceanographer;

-- Set schema
create schema ocean;
alter table measurements set schema ocean;
```

When adding a column that is meant to have a default value, there is a difference between specifying it in `ADD COLUMN ... DEFAULT ... `or setting on an existing column using `ALTER COLUMN ... SET DEFAULT ....` When adding a column with a default, all existing rows are implicitly assigned the default value. Currently, you can only use a constant, which does not require rewriting the table. However, you can still set the default for new rows to an expression.

```sql
-- allowed: add a column with all existing rows being assigned a constant
alter table measurements add column last_update_time timestamptz default '2024-01-01 00:00:00';

-- disallowed: add a column with all existing rows being assigned an expression
alter table measurements add column last_update_time timestamptz default now();
ERROR:  ALTER TABLE ADD COLUMN with default expression command not supported for pg_lake_iceberg tables

-- allowed: set the default expression of a column
alter table measurements alter column last_update_time set default now();
```

It is worth noting that `psql` does not perform auto-completion for Iceberg tables using the above syntax because they are recognized as foreign tables. You can use `ALTER FOREIGN TABLE` if you need auto-completion.
There are a few additional limitations regarding `ALTER TABLE` compared to regular PostgreSQL that will be resolved over time:

- Adding a column with a generated value, (big)serial pseudotype, or constraint
- Changing the type of a column (`ALTER COLUMN ... SET TYPE ...`)
- Adding or validating constraints
- Adding a column with an unsupported types (nested geometry, tables as types, numeric with precision >38)

Some advanced table management features such as declarative partitioning and inheritance are supported. Do be careful with those features, since the PostgreSQL planner does not always know how to efficiently perform aggregations across Iceberg (read: foreign) partitions.

## Vacuuming an Iceberg table

Each insert/update/delete operation on an Iceberg table results in additional data files being written. Quite often, this can result in the table being made up of many small files. The solution to that is to vacuum the table, similar to (auto)vacuum for regular heap tables. VACUUM on Iceberg tables does the following tasks:

- Data compaction: When you INSERT rows into your Iceberg tables, the data is stored as Parquet files. Iceberg data compaction merges small data files into larger ones to improve read performance and reduce metadata overhead.
- Managing expired metadata and data files: Modifying Iceberg tables generates new data and metadata files, leaving some older files unused and inaccessible. The VACUUM operation ensures these obsolete files are cleaned up, maintaining storage efficiency and preventing unnecessary accumulation of unused files.

### Autovacuum on Iceberg tables
`pg_lake` automatically runs vacuum on Iceberg tables every 10 minutes.
Autovacuum can be turned on and off for a single table:

```sql
-- on creation
CREATE TABLE test_auto_vacuum(id INT) USING iceberg WITH (autovacuum_enabled='False');

-- or, change anytime
CREATE TABLE other_test_auto_vacuum(id INT) USING iceberg;
ALTER FOREIGN TABLE other_test_auto_vacuum OPTIONS (ADD autovacuum_enabled 'false');
```

**Manual vacuum**

The following syntax will run VACUUM on all Iceberg tables:
```sql
VACUUM (ICEBERG);
```
The above will find all the Iceberg tables and run VACUUM on them one by one.

## Iceberg interoperability

`pg_lake` acts as its own Iceberg catalog. When updating Iceberg tables, it also updates its catalog tables as part of the ongoing transaction. Once created, your Iceberg table will appear in the `iceberg_tables` view, which can be used by third-party tools such as Spark, pyiceberg, or iceberg-rust. These tools can discover and access your warehouse tables via their SQL/JDBC catalog implementations.

## Accessing Iceberg tables with Spark

You can use Spark to access the Iceberg tables created on Postgres with `pg_lake`. The following commands will provide access to the Iceberg tables in Postgres. Note that the database name and catalog is `postgres`.

Here's an example of the pg_lake connection information needed:

```sql
export PGHOST="db host"
export PGDATABASE="db name"
export PGUSER="user name"
export PGPASSWORD="your password"
export AWS_REGION="region"
export JDBC_CONN_STR="jdbc:postgresql://${PGHOST}/${PGDATABASE}?user=${PGUSER}&password=${PGPASSWORD}"
```

This is an example of Spark connection configurations:

```
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1 \
--conf spark.sql.catalog.postgres=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.postgres.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
--conf spark.sql.catalog.postgres.uri=$JDBC_CONN_STR \
--conf spark.sql.catalog.postgres.warehouse=s3:// \
--conf spark.sql.catalog.postgres.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.postgres.s3.endpoint=https://s3.${AWS_REGION}.amazonaws.com \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
```

For example, a sample table created on `pg_lake`:

```sql
CREATE TABLE public.pg_lake_iceberg_table
USING iceberg
AS SELECT id
FROM generate_series(0,1000) id;
```

Once connected to Spark, you can query the table directly via Spark-sql:

```sql
spark-sql (default)> SELECT avg(id) FROM postgres.public.pg_lake_iceberg_table;
500.0
Time taken: 7.444 seconds, Fetched 1 row(s)
```

## Dropping an Iceberg table
You can drop Iceberg tables using the regular DROP TABLE command:

```sql
drop table measurements;
```

When you drop an Iceberg table, the files that make up the Iceberg table will be added to the “deletion queue”, but are not immediately deleted. This allows for restoring a backup if you need to revert to a previous point in time (see Point-in-time recoveries). When you run `VACUUM (iceberg);`, any files that have been in the deletion queue for more than 10 days will be deleted.

## Backups

When an Iceberg table is modified, the files that make up the previous version of the Iceberg table remain in object storage until they are explicitly deleted (by VACUUM). Our default policy is to keep files for at least 10 days to allow past versions to be restored.
pg_lake does not support table-level restores yet, but you can simulate it by creating an “external Iceberg table” from an old (dereferenced) metadata.json file.

```sql
do $$
begin
  execute format('create foreign table iceberg_old () server pg_lake options (path %L)',  (
    select path
    from lake_engine.deletion_queue
    where table_name = 'iceberg'::regclass and orphaned_at < now() - interval '3 days' and path like '%.metadata.json'
    order by orphaned_at desc limit 1
  ));
end;
$$;
```



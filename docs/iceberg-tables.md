---
title: Iceberg tables
parent: User guide
nav_order: 1
has_children: true
has_toc: false
---

# Iceberg tables
{: .no_toc }

Iceberg tables are transactional, columnar tables stored as Parquet files in object storage and
described by [Apache Iceberg](https://iceberg.apache.org/) metadata. They behave like regular
PostgreSQL tables: you can insert, update, delete, join them with heap tables and use them in
transactions. Because the data and metadata follow the Iceberg specification, other engines
such as Spark, Snowflake and pyiceberg can read the same tables.

1. TOC
{:toc}

## When to use Iceberg tables

pg_lake adds Iceberg next to PostgreSQL's own heap storage; it does not replace it. Use each
where it fits:

| | Heap tables | Iceberg tables |
|:--|:--|:--|
| Storage | Local disk, row-oriented | Object storage, columnar Parquet |
| Best for | Point lookups, single-row writes, high-concurrency OLTP | Scans, aggregations and joins over large data sets |
| Indexes and unique constraints | Yes | No |
| Size | Bounded by disk | Effectively unbounded |
| Readable by other engines | No | Yes, through an Iceberg catalog |
| Compression | Large values only (TOAST) | Columnar compression, often much smaller than heap |

A common pattern is to keep recent, frequently updated rows in heap tables and move data into
Iceberg for analytics and long-term retention. The [use cases](use-cases.md) section has
worked examples.

## Creating an Iceberg table

Add `USING iceberg` (or its alias `USING pg_lake_iceberg`) to a regular `CREATE TABLE`
statement:

```sql
CREATE TABLE measurements (
  station_name text NOT NULL,
  measurement double precision NOT NULL
)
USING iceberg;

INSERT INTO measurements VALUES ('Istanbul', 18.5);
```

The table's files go under `pg_lake_iceberg.default_location_prefix`, in a path derived from
the database, schema and table name. You can set the prefix for a session, a user or the
whole server (setting it requires superuser), or give one table an explicit `location`:

```sql
-- set the default location for this session
SET pg_lake_iceberg.default_location_prefix TO 's3://mybucket/iceberg';

-- or give a single table its own location
CREATE TABLE measurements (
  station_name text NOT NULL,
  measurement double precision NOT NULL
)
USING iceberg WITH (location = 's3://mybucket/measurements/');
```

Make sure pgduck_server has [credentials](configuration.md#object-storage-credentials) for
the bucket. For the best performance, keep the bucket in the same region as your PostgreSQL
server.

### Create a table from a query or a file

`CREATE TABLE ... AS` works as usual:

```sql
CREATE TABLE measurements_copy USING iceberg
AS SELECT md5(s::text) AS id, s AS value FROM generate_series(1, 1000000) s;
```

You can also create an Iceberg table from a data file. `load_from` infers the columns from the
file and loads its contents; `definition_from` only infers the columns:

```sql
-- convert a Parquet file directly into an Iceberg table
CREATE TABLE taxi_yellow ()
USING iceberg
WITH (load_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');

-- inherit the columns from a file, but do not load any data (yet)
CREATE TABLE taxi_yellow_empty ()
USING iceberg
WITH (definition_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');
```

Both options accept the `format` and `compression` options and the format-specific options
described in the [file formats reference](file-formats-reference.md). Before creating a table,
you can see which columns pg_lake would infer with `lake_file.preview`:

```sql
SELECT * FROM lake_file.preview('s3://mybucket/data/events.parquet');
```

### Iceberg table options

The most common options are listed below; the [table options reference](table-options.md)
has the complete list.

| Option | Description |
|:--|:--|
| `location` | URL prefix for the table's data and metadata. Defaults to a path under `pg_lake_iceberg.default_location_prefix`. |
| `partition_by` | Iceberg partition spec, such as `'day(event_time), bucket(16, user_id)'`. See [partitioning](iceberg-partitioning.md). |
| `catalog` | Where the table is registered: `postgres` (default), `rest`, `object_store` or the name of a [catalog server](iceberg-catalogs.md#external-catalogs-with-create-server). See [catalogs](iceberg-catalogs.md). |
| `autovacuum_enabled` | Whether the pg_lake autovacuum worker maintains this table. Default `true`. |
| `max_snapshot_age` | Snapshot retention in seconds, overriding `pg_lake_iceberg.max_snapshot_age`. |
| `out_of_range_values` | `error` (default) or `clamp` for values Iceberg cannot represent. See [data types](data-types.md#out-of-range-values). |
| `compatibility_mode` | `auto` (default) or `snowflake`, to shape storage for engines with narrower type support. |

### Supported PostgreSQL features

Iceberg tables work with most of the table features you already use:

- [Serial types](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL)
  and [identity columns](https://www.postgresql.org/docs/current/ddl-identity-columns.html)
- [`NOT NULL` and `CHECK` constraints](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS)
- [Generated columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [Composite types](https://www.postgresql.org/docs/current/sql-createtype.html), arrays and
  [maps](https://github.com/Snowflake-Labs/pg_lake/blob/main/pg_map/README.md)
- [Custom functions in expressions](https://www.postgresql.org/docs/current/sql-createfunction.html)
  and [triggers](https://www.postgresql.org/docs/current/sql-createtrigger.html)
- [PostGIS geometry](spatial.md#geometry-in-iceberg-tables) columns
- [Inheritance](https://www.postgresql.org/docs/current/tutorial-inheritance.html) and
  [collations](https://www.postgresql.org/docs/current/collation.html), though these can lead
  to less efficient query plans

Indexes, unique constraints, foreign keys, and temporary or unlogged Iceberg tables are not
supported. Some types are stored differently in Iceberg than in PostgreSQL, such as unbounded
`numeric` and multidimensional arrays; [data types](data-types.md) describes the mapping.

## Loading data

There are several ways to load data into an existing Iceberg table:

1. `COPY ... FROM '<url>'` loads a file from object storage or an HTTP(S) URL.
2. `COPY ... FROM STDIN` loads data from the client (`\copy` in psql).
3. `INSERT INTO ... SELECT` loads a query result.
4. `INSERT INTO ... VALUES` inserts individual rows.

Load data in batches when you can. Each statement writes one or more new Parquet files, so many
single-row inserts produce many small files, which slows down queries until
[VACUUM](iceberg-maintenance.md#vacuum) compacts them.

If your application needs single-row inserts, write them to a heap staging table and
periodically move them into Iceberg, for example with
[pg_cron](https://github.com/citusdata/pg_cron):

```sql
-- create a staging table
CREATE TABLE measurements_staging (LIKE measurements);

-- do fast inserts on the staging table
INSERT INTO measurements_staging VALUES ('Haarlem', 9.3);

-- every minute, move all staged rows into Iceberg in a single transaction
SELECT cron.schedule('flush-staging', '* * * * *', $$
  WITH new_rows AS (
    DELETE FROM measurements_staging RETURNING *
  )
  INSERT INTO measurements SELECT * FROM new_rows;
$$);
```

Because the `DELETE` and `INSERT` run in the same transaction, each row moves exactly once.
For append-only tables, [pg_incremental](https://github.com/CrunchyData/pg_incremental) is an
alternative that processes new rows by sequence or time range; see
[syncing tables to Iceberg](use-case-iceberg-sync.md) for an example.

## Making Iceberg the default table format

You can make every `CREATE TABLE` use Iceberg by default:

```sql
SET default_table_access_method TO 'iceberg';

-- automatically created as Iceberg
CREATE TABLE users (userid bigint, username text, email text);
```

This is useful for tools that do not know about Iceberg, such as `pg_dump` restores or
[dbt](dbt.md). Assign it to a specific user with
`ALTER USER ... SET default_table_access_method`, since temporary and unlogged tables fail
under this setting unless you add `USING heap`.

### Copying tables from another PostgreSQL server

To copy tables from another PostgreSQL server, such as Amazon RDS, Cloud SQL or your own
servers, into Iceberg tables, restore a `pg_dump` as a user with Iceberg as the default:

```sql
CREATE ROLE migration LOGIN PASSWORD '...';
GRANT lake_read_write TO migration;
GRANT CREATE ON SCHEMA public TO migration;
ALTER ROLE migration SET default_table_access_method TO 'iceberg';
```

```bash
pg_dump --table=orders --section=pre-data --section=data \
        --no-table-access-method --no-owner \
        "postgres://user@source-host:5432/sourcedb" \
  | psql "postgres://migration@pglake-host:5432/postgres"
```

`--section=pre-data --section=data` leaves out indexes, which Iceberg tables do not support, and
`--no-table-access-method` stops `pg_dump` from switching the default back to `heap`. Use
`--table` several times, or `--schema`, to copy several tables from one consistent snapshot.
Identity columns cause two errors during the restore, since Iceberg tables do not support adding
an identity; the data still loads and the column becomes a plain `NOT NULL` column.

If the data contains values Iceberg cannot store, such as `infinity` timestamps or `NaN` in
`numeric` columns, create the table first with `out_of_range_values = 'clamp'` (see
[data types](data-types.md)) and restore with `pg_dump --data-only`.

## Inspecting an Iceberg table

Iceberg tables are implemented as foreign tables, so `\d+` in psql lists them as
`foreign table`, with the total size of their current data files:

```sql
postgres=> \d+
                                         List of relations
 Schema |     Name     |     Type      |    Owner    | Persistence | Access method |  Size   |
--------+--------------+---------------+-------------+-------------+---------------+---------+
 public | measurements | foreign table | application | permanent   |               | 5238 MB |
 public | taxi_yellow  | foreign table | application | permanent   |               | 92 MB   |
```

`\d table_name` shows the columns and the table's options, such as its location:

```sql
postgres=> \d measurements
                    Foreign table "public.measurements"
    Column    |       Type       | Collation | Nullable | Default | FDW options
--------------+------------------+-----------+----------+---------+-------------
 station_name | text             |           | not null |         |
 measurement  | double precision |           | not null |         |
Server: pg_lake_iceberg
FDW options: (location 's3://testbucket/iceberg/postgres/public/measurements')
```

`pg_table_size` and `lake_iceberg.table_size` return the size of the current data files.
`pg_total_relation_size` is not implemented for Iceberg tables and returns 0.

```sql
SELECT pg_size_pretty(lake_iceberg.table_size('measurements'));
```

The `iceberg_tables` view shows each table's current metadata file, which you can pass to the
[metadata functions](iceberg-maintenance.md#inspecting-iceberg-metadata) to look at snapshots,
schemas and data files.

## Dropping an Iceberg table

`DROP TABLE` works as usual. The table's files are not deleted right away: they are added to a
deletion queue and removed by VACUUM once they are older than
`pg_lake_engine.orphaned_file_retention_period` (10 days by default). Until then, the old
metadata can be used to [recover the data](iceberg-maintenance.md#recovering-old-data).

```sql
DROP TABLE measurements;
```

## Next steps

- [Partitioning](iceberg-partitioning.md): hidden partitioning, transforms and pruning.
- [Modifying tables](iceberg-modifying.md): `UPDATE`, `DELETE` and schema changes.
- [Catalogs and interoperability](iceberg-catalogs.md): REST catalogs, Spark, Snowflake and
  other engines.
- [Maintenance](iceberg-maintenance.md): vacuum, snapshots, metadata and recovery.

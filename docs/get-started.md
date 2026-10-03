---
title: Get started
nav_order: 2
has_children: true
has_toc: false
---

# Get started
{: .no_toc }

This page gets you from nothing to querying Iceberg tables and files in object storage. It
takes about ten minutes with Docker.

1. TOC
{:toc}

## Install pg_lake

There are two ways to set up pg_lake:

- **[Docker](https://github.com/Snowflake-Labs/pg_lake/blob/main/docker/README.md)** runs
  PostgreSQL with pg_lake, pgduck_server and S3-compatible storage
  ([LocalStack](https://localstack.cloud/)) with one command. This is the quickest way to try
  pg_lake.
- **[Building from source](building-from-source.md)** installs pg_lake into an existing
  PostgreSQL 16, 17, 18 or 19 installation, or sets up a full development environment.

### With Docker

You need Docker and [Task](https://taskfile.dev/installation/):

```bash
git clone https://github.com/Snowflake-Labs/pg_lake.git
cd pg_lake/docker
task compose:up
```

The first build takes a while. Once the services are up, connect with psql:

```bash
psql -h localhost -p 5432 -U postgres
```

The Docker setup creates the extensions (including `pg_lake_spatial`), gives pgduck_server
credentials for the LocalStack bucket `s3://testbucket`, and sets
`pg_lake_iceberg.default_location_prefix` to `s3://testbucket/pg_lake/`. You can skip to
[creating your first Iceberg table](#create-your-first-iceberg-table); in the examples below,
use `s3://testbucket` wherever they say `s3://mybucket`.

### From source

After [building and installing](building-from-source.md) pg_lake:

1. Add `pg_extension_base` to `shared_preload_libraries` in `postgresql.conf`, and restart
   PostgreSQL:

   ```ini
   shared_preload_libraries = 'pg_extension_base'
   ```

2. Start pgduck_server, which listens on a Unix socket in `/tmp` by default:

   ```bash
   pgduck_server --cache_dir /var/lib/pgduck/cache
   ```

3. Make sure pgduck_server can reach your object storage. For AWS, it uses the usual credential
   chain, such as `~/.aws/credentials` or an instance profile; see
   [object storage credentials](configuration.md#object-storage-credentials) for other options.

4. Create the extensions, and tell pg_lake where to store Iceberg tables:

   ```sql
   CREATE EXTENSION pg_lake CASCADE;
   NOTICE:  installing required extension "pg_lake_table"
   NOTICE:  installing required extension "pg_lake_engine"
   NOTICE:  installing required extension "pg_extension_base"
   NOTICE:  installing required extension "pg_map"
   NOTICE:  installing required extension "pg_lake_iceberg"
   NOTICE:  installing required extension "btree_gist"
   NOTICE:  installing required extension "pg_lake_copy"
   CREATE EXTENSION

   ALTER DATABASE postgres SET pg_lake_iceberg.default_location_prefix TO 's3://mybucket/iceberg';
   ```

   The setting takes effect in new sessions, so reconnect before the next step.

## Create your first Iceberg table

Add `USING iceberg` to `CREATE TABLE`:

```sql
CREATE TABLE measurements (
  station text NOT NULL,
  measured_at timestamptz NOT NULL,
  temperature double precision
)
USING iceberg;

INSERT INTO measurements
SELECT (ARRAY['Amsterdam', 'Istanbul', 'Seattle'])[1 + i % 3],
       now() - i * interval '1 minute',
       10 + 15 * random()
FROM generate_series(1, 100000) i;

SELECT station, count(*), round(avg(temperature)::numeric, 1) AS avg_temp
FROM measurements
GROUP BY station
ORDER BY station;

  station  | count | avg_temp
-----------+-------+----------
 Amsterdam | 33333 |     17.5
 Istanbul  | 33334 |     17.5
 Seattle   | 33333 |     17.5
(3 rows)
```

The table is stored as Parquet files and Iceberg metadata in object storage, and the
aggregate ran on DuckDB. It is still a PostgreSQL table: you can update it, join it with other
tables, and use it in transactions.

```sql
BEGIN;
DELETE FROM measurements WHERE temperature < 11;
UPDATE measurements SET station = 'New York' WHERE station = 'Seattle';
COMMIT;
```

The `iceberg_tables` view shows where each table's current metadata is, so that other engines
such as Spark or Snowflake can read it:

```sql
SELECT table_name, metadata_location FROM iceberg_tables;
```

## Export and query files

`COPY` can write any query result to object storage, in Parquet, CSV or JSON:

```sql
COPY (SELECT * FROM measurements WHERE station = 'Istanbul')
TO 's3://mybucket/exports/istanbul.parquet';
```

To query files in place, create a foreign table on the `pg_lake` server. With an empty column
list, the columns are inferred from the files:

```sql
CREATE FOREIGN TABLE istanbul () SERVER pg_lake
OPTIONS (path 's3://mybucket/exports/*.parquet');

SELECT count(*) FROM istanbul;
```

This works for public data too. For example, the New York City taxi trip records are published
as Parquet files over HTTPS:

```sql
CREATE FOREIGN TABLE taxi_trips () SERVER pg_lake
OPTIONS (path 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');

SELECT date_trunc('day', tpep_pickup_datetime) AS day, count(*), avg(total_amount)
FROM taxi_trips
GROUP BY 1 ORDER BY 1 LIMIT 5;
```

To keep a copy, load the file into an Iceberg table in one step:

```sql
CREATE TABLE taxi_trips_2024 () USING iceberg
WITH (load_from = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');
```

## Next steps

- [How pg_lake works](concepts.md) explains the architecture, table types and transactions.
- [Configuration](configuration.md) covers credentials, pgduck_server options and permissions
  for a real deployment.
- [Iceberg tables](iceberg-tables.md) covers partitioning, updates, catalogs and maintenance.
- [Use cases](use-cases.md) has end-to-end examples, such as
  [syncing Postgres tables to Iceberg](use-case-iceberg-sync.md).

# pg_lake: Postgres for Iceberg and Data lakes

`pg_lake` integrates Iceberg and data lake files into Postgres. With the `pg_lake` extensions, you can use Postgres as a stand-alone lakehouse system that supports transactions and fast queries on Iceberg tables, and can directly work with raw data files in object stores like S3.

At a high level, `pg_lake` lets you:

- **Create and modify [Iceberg](https://iceberg.apache.org/)** tables directly from PostgreSQL, with full transactional guarantees and query them from other engines
- **Query and import data files in object storage** in [Parquet](https://parquet.apache.org/), CSV, JSON, and Iceberg format
- **Export query results back to object storage** in Parquet, CSV, or JSON formats using COPY commands
- **Read geospatial formats** supported by GDAL, such as GeoJSON and Shapefiles
- **Use the built-in [map type](./pg_map/README.md)** for semi-structured or key–value data  
- **Combine heap, Iceberg, and external Parquet/CSV/JSON** files in the same SQL queries and modifications — all with full transactional guarantees and no SQL limitations  
- **Infer table columns and types** from external data sources such as Iceberg, Parquet, JSON, and CSV files
- **Leverage DuckDB’s query engine** underneath for fast execution without leaving Postgres  

## Setting up `pg_lake`

There are two ways to set up `pg_lake`:  
- **Using Docker**, for an easy, ready-to-run test environment.  
- **Building from source**, for a manual setup or development use.  

Both approaches include the PostgreSQL extensions, the `pgduck_server` application and setting up S3-compatible storage.

### Using Docker

Follow the [Docker README](./docker/README.md) to set up and run `pg_lake` with Docker.


### Building from source

Once you’ve [built and installed the required components](./docs/building-from-source.md), you can initialize `pg_lake` inside Postgres.

#### Creating the extensions

Create all required extensions at once using `CASCADE`:

```sql
CREATE EXTENSION pg_lake CASCADE;
NOTICE:  installing required extension "pg_lake_table"
NOTICE:  installing required extension "pg_lake_engine"
NOTICE:  installing required extension "pg_extension_base"
NOTICE:  installing required extension "pg_lake_iceberg"
NOTICE:  installing required extension "pg_lake_copy"
CREATE EXTENSION
```

#### Running `pgduck_server`

`pgduck_server` is a standalone process that implements the Postgres wire-protocol (locally), and underneath uses `DuckDB` to execute queries.

When you run `pgduck_server` it starts listening to port `5332` on unix domain socket:
```
pgduck_server
LOG pgduck_server is listening on unix_socket_directory: /tmp with port 5332, max_clients allowed 10000
```

As `pgduck_server` implements Postgres wire protocol, you can access it via `psql` on port `5332` and host `/tmp` and run commands via DuckDB. 

For example, you can get the DuckDB version:

```sql
psql -p 5332 -h /tmp

select version() as duckdb_version; 
duckdb_version 
---------------- 
v1.3.2 (1 row)
```

You can also provide some additional settings while starting the server, to see all:
```
pgduck_server --help
```

There are some important settings that should be adjusted, especially on production systems:


- `--memory_limit`: Optionally specify the maximum memory of pgduck_server similar to DuckDB's memory_limit, the default is 80 percent of the system memory
- `--init_file_path <path>`: Execute all statements in this file on start-up
- `--cache_dir`: Specify the directory to use to cache remote files (from S3)


#### Connecting `pg_lake` to s3 (or compatible)

`pgduck_server` relies on the DuckDB [secrets manager](https://duckdb.org/docs/stable/configuration/secrets_manager) for credentials and it follows the credentials chain by default for AWS and GCP. Make sure your cloud credentials are configured properly — for example, by setting them in ~/.aws/credentials.  

Once you set up the credential chain, you should set the `pg_lake_iceberg.default_location_prefix`. This is the location where Iceberg tables are stored:

```sql
SET pg_lake_iceberg.default_location_prefix TO 's3://testbucketpglake';
```

You can also set the credentials on `pgduck_server` for [local development with `minio`](docs/building-from-source.md#running-s3-compatible-service-minio-locally).

## Using pg_lake

### Create an Iceberg table

You can create Iceberg tables by adding `USING iceberg` to your `CREATE TABLE` statements.

```sql
CREATE TABLE iceberg_test USING iceberg 
      AS SELECT 
            i as key, 'val_'|| i  as val
         FROM 
            generate_series(0,99)i;
```

Then, query it:
```sql
SELECT count(*) FROM iceberg_test;
 count 
-------
   100
(1 row)
```

You can then see the Iceberg metadata location:

```sql
SELECT table_name, metadata_location FROM iceberg_tables;


    table_name     |                                                metadata_location
-------------------+--------------------------------------------------------------------------------------------------------------------
 iceberg_test      | s3://testbucketpglake/postgres/public/test/435029/metadata/00001-f0c6e20a-fd1c-4645-87c9-c0c64b92992b.metadata.json
```

### COPY to/from S3

You can import or export data directly using `COPY` in **Parquet**, **CSV**, or **newline-delimited JSON** formats.  The format is automatically inferred from the file extension, or you can specify it explicitly with `COPY` options like `WITH (format 'csv', compression 'gzip')`.


```sql
-- Copy data from Postgres to S3 with format parquet
-- Read from any data source, including iceberg tables, heap tables or any query results
COPY (SELECT * FROM iceberg_test) TO 's3://testbucketpglake/parquet_data/iceberg_test.parquet';

-- Copy back from S3 to any table in Postgres
-- This example copies into an iceberg table, but could be heap table as well
COPY iceberg_test FROM 's3://testbucketpglake/parquet_data/iceberg_test.parquet';
```

### Create foreign table for files on s3

You can create a foreign table directly from a file or set of files without having to specify column names or types.

```sql
-- use the files under the path, can use * for all files
CREATE FOREIGN TABLE parquet_table() 
SERVER pg_lake 
OPTIONS (path 's3://testbucketpglake/parquet_data/*.parquet');

-- note that we infer the columns from the file
\d parquet_table
              Foreign table "public.parquet_table"
 Column |  Type   | Collation | Nullable | Default | FDW options 
--------+---------+-----------+----------+---------+-------------
 key    | integer |           |          |         | 
 val    | text    |           |          |         | 
Server: pg_lake
FDW options: (path 's3://testbucketpglake/parquet_data/*.parquet')

-- and, query it
select count(*) from parquet_table;
 count 
-------
   100
(1 row)

```

## Architecture

A `pg_lake` instance consists of two main components: **PostgreSQL with the pg_lake extensions** and **pgduck_server**.

Users connect to PostgreSQL to run SQL queries, and the `pg_lake` extensions integrate with Postgres’s hooks to handle query planning, transaction boundaries, and overall orchestration of execution.  

Behind the scenes, _parts_ of query execution are delegated to DuckDB through pgduck_server, a separate multi-threaded process that implements the PostgreSQL wire protocol (locally).  This process runs DuckDB together with our **duckdb_pglake** extension, which adds PostgreSQL-compatible functions and behavior.  

Users typically don’t need to be aware of `pgduck_server`; it operates transparently to improve performance. When appropriate, `pg_lake` delegates scanning of the data and the computation to DuckDB’s highly parallel, column-oriented execution engine.

This separation also avoids the threading and memory-safety limitations that would arise from embedding DuckDB directly inside the Postgres process, which is designed around process isolation rather than multi-threaded execution. Moreover, it lets us interact with the query engine directly by connecting to it using standard Postgres clients.


![pg_lake Architecture](pglake-arch.png)


### Components

The team behind pg_lake has a lot of experience building Postgres extensions (e.g. Citus, pg_cron, pg_documentdb). Over time, we’ve learned that large, monolithic PostgreSQL extensions are harder to evolve and maintain.  

`pg_lake` follows a modular design built around a **set of interoperating components** — mostly implemented as PostgreSQL extensions, others as supporting services or libraries.  
Each part focuses on a well-defined layer, such as table and metadata management, catalog and object store integration, query execution, or data format handling.  This approach makes it easier to extend, test, and evolve the system, while keeping it familiar to anyone with a PostgreSQL background.

The current set of components are:

- **pg_lake_iceberg**: a PostgreSQL extension that implements the [Iceberg specification](https://iceberg.apache.org/)
- **pg_lake_table**: a PostgreSQL extension that implements a foreign data wrapper to query files in object storage 
- **pg_lake_copy**: a PostgreSQL extension that implements COPY to/from your data lake
- **pg_lake_engine**: a common module for different pg_lake extensions
- **pg_extension_base**: A foundational building block for other extensions
- **pg_extension_updater**: An extension for updating all extensions on start-up. See [README.md](./pg_extension_updater/README.md).
- **pg_lake_benchmark**: a PostgreSQL extension that performs various benchmarks on lake tables. See [README.md](./pg_lake_benchmark/README.md).
- **pg_map**: A generic map type generator
- **pgduck_server**: a stand-alone server that loads DuckDB into the same server machine and exposes DuckDB via the PostgreSQL protocol
- **duckdb_pglake**: a DuckDB extension that adds missing PostgreSQL functions to DuckDB

## History
`pg_lake` development started in early 2024 at [Crunchy Data](https://www.crunchydata.com/) with the goal of bringing Iceberg to PostgreSQL. The first few months were focused on building a robust integration of an external query engine (DuckDB). To get to market early, we made the query/import/export features available to [Crunchy Bridge](https://docs.crunchybridge.com/) customers as [Crunchy Bridge for Analytics](https://www.crunchydata.com/blog/crunchy-bridge-for-analytics-your-data-lake-in-postgresql).

Next, we started building a comprehensive implementation of the Iceberg (v2) protocol with support for transactions and almost all PostgreSQL features. In November 2024, we relaunched Crunchy Bridge for Analytics as Crunchy Data Warehouse available on Crunchy Bridge and on-premises.

In June 2025, [Crunchy Data was acquired by Snowflake](https://www.crunchydata.com/blog/crunchy-data-joins-snowflake). Following the acquisition, Snowflake decided to open source the project as `pg_lake` in November 2025. The initial version is 3.0 because of the two prior generations. If you’re currently a Crunchy Data Warehouse user there will be an automatic upgrade path, though some names will change.

## Documentation

Full project documentation can be found in the [docs](./docs) directory.


## License
Copyright (c) Snowflake Inc. All rights reserved.
Licensed under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license.

#### Note on Dependencies
`pg_lake` is dependent on third-party projects Apache Avro and DuckDB. During build, `pg_lake` applies patches to Avro and certain DuckDB extensions in order to provide the `pg_lake` functionality. The source code associated with the Avro and DuckDB extensions is downloaded from the applicable upstream repos and the source code associated with those projects remains under the original licenses. If you are packaging or redistributing packages that include `pg_lake`, please note that you should review those upstream license terms.

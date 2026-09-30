---
title: Get started
nav_order: 2
has_children: true
---

# Get started

There are two ways to set up pg_lake:

- **[Docker](https://github.com/Snowflake-Labs/pg_lake/blob/main/docker/README.md)** gives you a
  ready-to-run test environment with PostgreSQL, pgduck_server and S3-compatible storage.
- **[Building from source](building-from-source.md)** installs pg_lake into an existing
  PostgreSQL installation, or sets up a full development environment.

Once pg_lake is installed, the steps below get you to a first Iceberg table.

## Create the extensions

pg_lake needs `pg_extension_base` in `shared_preload_libraries`:

```ini
# postgresql.conf
shared_preload_libraries = 'pg_extension_base'
```

After restarting PostgreSQL, create all required extensions at once with `CASCADE`:

```sql
CREATE EXTENSION pg_lake CASCADE;
NOTICE:  installing required extension "pg_lake_table"
NOTICE:  installing required extension "pg_lake_engine"
NOTICE:  installing required extension "pg_extension_base"
NOTICE:  installing required extension "pg_lake_iceberg"
NOTICE:  installing required extension "pg_lake_copy"
CREATE EXTENSION
```

## Run pgduck_server

`pgduck_server` is a standalone process that implements the PostgreSQL wire protocol locally
and executes queries with DuckDB. pg_lake needs it to be running. By default it listens on
port `5332` on a Unix domain socket in `/tmp`:

```bash
pgduck_server
LOG pgduck_server is listening on unix_socket_directory: /tmp with port 5332, max_clients allowed 10000
```

These settings are worth adjusting, especially on production systems. Run
`pgduck_server --help` to see them all.

| Option | Description |
|:-------|:------------|
| `--memory_limit` | Maximum memory for pgduck_server, like DuckDB's `memory_limit`. Defaults to 80 percent of system memory. |
| `--init_file_path <path>` | Execute all statements in this file on start-up. |
| `--cache_dir` | Directory used to cache remote files from object storage. |

You can also connect to pgduck_server itself with `psql` to inspect or change DuckDB settings.
This is a connection to pgduck_server on port 5332, not to PostgreSQL:

```sql
$ psql -h /tmp -p 5332
postgres=> select version() as duckdb_version;
postgres=> set global threads = 16;
```

## Connect to object storage

`pgduck_server` uses the DuckDB
[secrets manager](https://duckdb.org/docs/stable/configuration/secrets_manager) for
credentials, and follows the credential chain by default for AWS and GCP. Make sure your cloud
credentials are configured, for example in `~/.aws/credentials`.

Then set the location where pg_lake stores Iceberg tables:

```sql
SET pg_lake_iceberg.default_location_prefix TO 's3://testbucket/pglake';
```

For local development you can use MinIO instead of S3; see
[running MinIO locally](building-from-source.md#running-s3-compatible-service-minio-locally).

## Create your first Iceberg table

Add `USING iceberg` to a `CREATE TABLE` statement:

```sql
CREATE TABLE iceberg_test USING iceberg
  AS SELECT i AS key, 'val_' || i AS val
     FROM generate_series(0, 99) i;

SELECT count(*) FROM iceberg_test;
 count
-------
   100
(1 row)
```

The `iceberg_tables` view shows where the table's Iceberg metadata lives, so other engines can
read it:

```sql
SELECT table_name, metadata_location FROM iceberg_tables;
```

## Next steps

- [Iceberg tables](iceberg-tables.md): partitioning, REST catalogs, updates, vacuum and interoperability.
- [Query data lake files](query-data-lake-files.md): query files in object storage without loading them.
- [Data lake import and export](data-lake-import-export.md): move data in and out with `COPY`.

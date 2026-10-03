---
title: Configuration
parent: Get started
nav_order: 2
---

# Configuration
{: .no_toc }

This page covers what you need to run pg_lake beyond a first test: PostgreSQL settings,
pgduck_server options, object storage credentials and permissions.

1. TOC
{:toc}

## PostgreSQL settings

pg_lake needs `pg_extension_base` in `shared_preload_libraries`, which starts pg_lake's
background workers such as autovacuum and the cache manager:

```ini
# postgresql.conf
shared_preload_libraries = 'pg_extension_base'

# where new Iceberg tables are stored (can also be set per database or user)
pg_lake_iceberg.default_location_prefix = 's3://mybucket/iceberg'

# only needed if pgduck_server does not use the default socket (/tmp, port 5332)
#pg_lake_engine.host = 'host=/var/run/pgduck port=5332'
```

Restart PostgreSQL after changing `shared_preload_libraries` or `pg_lake_engine.host`, then
create the extensions in each database that uses pg_lake:

```sql
CREATE EXTENSION pg_lake CASCADE;
```

The [configuration parameters reference](settings-reference.md) lists every pg_lake setting.

## pgduck_server options

pgduck_server is configured on its command line:

| Option | Default | Description |
|:--|:--|:--|
| `--unix_socket_directory <path>` | `/tmp` | Directory for the Unix socket that PostgreSQL connects to. |
| `--port <port>` | `5332` | Port number, which is part of the socket file name. |
| `--unix_socket_group <group>` | current group | Group owner of the socket. |
| `--unix_socket_permissions <mask>` | `0770` | Permissions of the socket. |
| `--max_clients <n>` | `10000` | Maximum number of connections. |
| `--memory_limit <size>` | 80% of system memory | DuckDB memory limit, such as `16GB`. |
| `--cache_dir <path>` | none | Directory for the local [file cache](performance.md#file-cache). Put it on fast local storage. |
| `--cache_on_write_max_size <bytes>` | 1 GB | Largest newly written file that is also added to the cache. |
| `--init_file_path <path>` | none | SQL file that is run on start-up, for example to create secrets. |
| `--duckdb_database_file_path <path>` | `~/.pglake/pgduck_server.db` | DuckDB database file. |
| `--extensions_dir <path>` | DuckDB default | Directory for DuckDB extensions. |
| `--no_extension_install` | off | Do not install DuckDB extensions at start-up; use this when they are preinstalled. |
| `--continue_on_oom` | off | Keep running after an out-of-memory error. |
| `--pidfile <path>` | none | Write the process ID to this file. |
| `--debug`, `--verbose` | off | More logging; `--debug` includes full query text. |

pgduck_server only listens on a Unix socket, so it must run on the same machine as PostgreSQL,
and the PostgreSQL server user needs access to the socket. It also reads temporary files that
PostgreSQL writes, so for production, run it as a separate user in the `postgres` group; see
[running pgduck_server under a separate user](building-from-source.md#running-pgduck_server-under-a-separate-linux-user).

You can connect to pgduck_server with `psql` to check or change DuckDB settings. This is a
connection to DuckDB, not to PostgreSQL:

```sql
$ psql -h /tmp -p 5332
postgres=> SELECT version() AS duckdb_version;
postgres=> SET GLOBAL threads = 16;
```

## Object storage credentials

pgduck_server, not PostgreSQL, holds the credentials for object storage. It accesses object storage
using DuckDB's [secrets manager](https://duckdb.org/docs/stable/configuration/secrets_manager):

- **AWS and Google Cloud:** by default, pgduck_server uses the standard credential chain:
  environment variables, `~/.aws/credentials`, instance profiles and so on. On a cloud VM with
  an attached role, there may be nothing to configure.
- **Anything else:** create a secret. Put `CREATE SECRET` statements in the file passed with
  `--init_file_path`, so they are recreated every time pgduck_server starts.

Secrets can be scoped to a bucket or prefix, so different buckets can use different credentials:

```sql
-- Amazon S3 with an access key
CREATE SECRET s3_analytics (
  TYPE s3,
  KEY_ID 'AKIA...',
  SECRET '...',
  REGION 'us-east-1',
  SCOPE 's3://analytics-bucket'
);

-- S3-compatible storage such as MinIO
CREATE SECRET minio (
  TYPE s3,
  KEY_ID 'minioadmin',
  SECRET 'minioadmin',
  ENDPOINT 'localhost:9000',
  URL_STYLE 'path',
  USE_SSL false,
  SCOPE 's3://localbucket'
);

-- Google Cloud Storage with an HMAC key
CREATE SECRET gcs (
  TYPE gcs,
  KEY_ID 'GOOG...',
  SECRET '...'
);

-- Cloudflare R2
CREATE SECRET r2 (
  TYPE r2,
  KEY_ID '...',
  SECRET '...',
  ACCOUNT_ID 'my-account-id'
);

-- Azure Blob Storage
CREATE SECRET azure (
  TYPE azure,
  CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...'
);
```

Keep the init file readable only by the user that runs pgduck_server. For local development
with MinIO, see [running MinIO locally](building-from-source.md#running-s3-compatible-service-minio-locally).

Credentials only go through PostgreSQL when a REST catalog vends them
(`enable_vended_credentials`). PostgreSQL then asks the catalog for temporary S3 credentials for
each table it reads or writes, and passes them to pgduck_server as in-memory secrets scoped to
that table's location, where they take precedence over pgduck_server's own secrets. See
[REST catalogs](iceberg-catalogs.md#rest-catalogs).

### Supported storage URLs

| Scheme | Storage | Read | Write |
|:--|:--|:--|:--|
| `s3://` | Amazon S3 and S3-compatible storage | Yes | Yes |
| `gs://`, `gcs://` | Google Cloud Storage | Yes | Yes |
| `az://`, `azure://`, `abfss://` | Azure Blob Storage and Data Lake Storage | Yes | Yes |
| `r2://` | Cloudflare R2 | Yes | Yes |
| `https://`, `http://` | Public web servers | Yes | No |

pg_lake detects the region of S3 buckets automatically. For the best performance and to avoid
data transfer charges, keep buckets that you write to or query often in the same region as
your server.

## Roles and permissions

Superusers can use everything. Other users need one of the roles that the extensions create:

| Role | Allows |
|:--|:--|
| `lake_read` | Reading files: `pg_lake` foreign tables, `COPY ... FROM` a URL, `load_from`, and the `lake_file` functions. |
| `lake_write` | Writing files with `COPY ... TO` a URL, and creating catalog servers. |
| `lake_read_write` | Both, plus creating and using Iceberg tables. |

```sql
GRANT lake_read_write TO application;
```

Because these roles give access to everything pgduck_server's credentials can reach, grant them
only to users you would trust with those credentials. Once an Iceberg table exists, access to
it is controlled with regular `GRANT` and `REVOKE`, like any other table.

The Iceberg catalog is also readable by external Iceberg clients that connect to PostgreSQL
(see [catalogs](iceberg-catalogs.md#the-postgresql-catalog)). Create a separate user for them
with the `iceberg_catalog` role.

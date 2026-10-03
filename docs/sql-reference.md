---
title: SQL functions and views
parent: Reference
nav_order: 1
---

# SQL functions and views
{: .no_toc }

The pg_lake extensions add the functions, views and roles below. Most take a URL, which can use
any [supported storage scheme](configuration.md#supported-storage-urls).

1. TOC
{:toc}

## Files in object storage

### lake_file.list

```sql
lake_file.list(url_wildcard text)
  RETURNS TABLE (path text, file_size bigint, last_modified_time timestamptz, etag text)
```

Lists the files matching a URL pattern. `*` matches within one directory level, and `**`
matches any number of levels.

```sql
SELECT path, pg_size_pretty(file_size) FROM lake_file.list('s3://mybucket/logs/2026/**/*.json.gz');
```

### lake_file.preview

```sql
lake_file.preview(url text, format text DEFAULT NULL, compression text DEFAULT NULL)
  RETURNS TABLE (column_name text, column_type text)
```

Shows the columns and PostgreSQL types that pg_lake infers for a file, as it would for
`CREATE FOREIGN TABLE ... ()` or `load_from`.

### lake_file.size, lake_file.exists

```sql
lake_file.size(path text) RETURNS bigint
lake_file.exists(path text) RETURNS boolean
```

Return the size of a file in bytes, and whether it exists.

### lake_file.delete

```sql
lake_file.delete(url text) RETURNS void
```

Deletes a file from object storage. Requires `lake_write`, and is disabled unless a superuser
sets `pg_lake_table.enable_delete_file_function = on`. Wildcards are not expanded. See
[delete files](data-lake-import-export.md#delete-files).

## File cache

pgduck_server caches remote files on local disk. These functions inspect and control the
cache. See [performance](performance.md#file-cache).

### lake_file_cache.list

```sql
lake_file_cache.list() RETURNS TABLE (path text, file_size bigint, last_access_time timestamp)
```

Lists the files currently in the cache.

### lake_file_cache.add

```sql
lake_file_cache.add(path text, refresh boolean DEFAULT false) RETURNS bigint
```

Downloads a file into the cache and returns its size. With `refresh`, downloads it again even
if it is already cached.

### lake_file_cache.remove

```sql
lake_file_cache.remove(path text) RETURNS boolean
```

Removes a file from the cache.

## Iceberg metadata

These functions read Iceberg metadata files directly, so they work for tables written by any
engine. For pg_lake's own tables, pass `metadata_location` from `iceberg_tables`.

### lake_iceberg.metadata

```sql
lake_iceberg.metadata(metadata_uri text) RETURNS jsonb
```

Returns the contents of a metadata file, as defined by the
[Iceberg specification](https://iceberg.apache.org/spec/#table-metadata).

### lake_iceberg.snapshots

```sql
lake_iceberg.snapshots(metadata_uri text)
  RETURNS TABLE (sequence_number bigint, snapshot_id bigint, timestamp_ms timestamp, manifest_list text)
```

Lists the snapshots retained in a metadata file.

### lake_iceberg.files

```sql
lake_iceberg.files(metadata_uri text)
  RETURNS TABLE (manifest_path text, content text, file_path text, file_format text,
                 spec_id bigint, record_count bigint, file_size_in_bytes bigint)
```

Lists the data and delete files of the current snapshot.

### lake_iceberg.data_file_stats

```sql
lake_iceberg.data_file_stats(metadata_location text)
  RETURNS TABLE (path text, sequence_number bigint, lower_bounds json, upper_bounds json)
```

Returns the per-column lower and upper bounds of each data file, which pg_lake uses for file
pruning.

### lake_iceberg.table_size

```sql
lake_iceberg.table_size(table_name regclass) RETURNS bigint
```

Returns the total size of an Iceberg table's current data files, in bytes.

## Maintenance

### lake_engine.flush_deletion_queue

```sql
lake_engine.flush_deletion_queue(table_name regclass) RETURNS SETOF text
```

Deletes a table's queued files that are past `pg_lake_engine.orphaned_file_retention_period`
right away, and returns their paths.

### lake_engine.flush_in_progress_queue

```sql
lake_engine.flush_in_progress_queue() RETURNS SETOF text
```

Deletes files left behind by writes that did not commit, and returns their paths.

## Other functions

### map_type.create

```sql
map_type.create(keytype regtype, valtype regtype, typname text DEFAULT NULL) RETURNS regtype
```

Creates a map type with the given key and value types, for use in columns that map to Iceberg
and Parquet maps. See the [pg_map documentation](https://github.com/Snowflake-Labs/pg_lake/blob/main/pg_map/README.md).
Requires superuser by default.

### lake.version

```sql
lake.version() RETURNS text
```

Returns the version of the installed pg_lake build.

## Views and tables

### iceberg_tables

The PostgreSQL Iceberg catalog, one row per Iceberg table. The layout matches the Iceberg SQL
catalog specification, so Iceberg tools can use it as a
[JDBC or SQL catalog](iceberg-catalogs.md#the-postgresql-catalog).

| Column | Description |
|:--|:--|
| `catalog_name` | Catalog name; the database name for pg_lake tables. |
| `table_namespace` | Namespace; the schema name for pg_lake tables. |
| `table_name` | Table name. |
| `metadata_location` | URL of the current metadata file. |
| `previous_metadata_location` | URL of the previous metadata file. |

### iceberg_namespace_properties

Namespace properties, in the layout of the Iceberg SQL catalog specification.

### lake_engine.deletion_queue

Files waiting to be deleted by VACUUM, with the table they belonged to (`table_name`) and when
they stopped being referenced (`orphaned_at`). See
[the deletion queue](iceberg-maintenance.md#the-deletion-queue).

## Roles

The extensions create these roles. Superusers have all of their privileges.

| Role | Grants |
|:--|:--|
| `lake_read` | Reading files from URLs: foreign tables on the `pg_lake` server, `COPY ... FROM` a URL, `load_from` and `definition_from`, and the `lake_file` and `lake_file_cache` functions. |
| `lake_write` | Writing files to URLs with `COPY ... TO`, the `lake_engine` maintenance functions, and creating [catalog servers](iceberg-catalogs.md#external-catalogs-with-create-server). |
| `lake_read_write` | Member of both roles above, and the only role with access to the `pg_lake_iceberg` server, so it is required to create Iceberg tables. |
| `iceberg_catalog` | Read and write access to the `iceberg_tables` and `iceberg_namespace_properties` views, for external Iceberg clients that use PostgreSQL as their catalog. |

For most application users, grant `lake_read_write`:

```sql
GRANT lake_read_write TO application;
```

---
title: Configuration parameters
parent: Reference
nav_order: 3
---

# Configuration parameters
{: .no_toc }

pg_lake is configured through PostgreSQL settings. Set them in `postgresql.conf`, with
`ALTER SYSTEM`, per database or user with `ALTER DATABASE ... SET` and `ALTER ROLE ... SET`, or
per session with `SET`. For each parameter, **set by** says who can change it and when the change
takes effect:

- **user**: any user, with `SET`.
- **superuser**: superusers, with `SET` or in the configuration.
- **reload**: in the configuration, followed by `SELECT pg_reload_conf()`.
- **restart**: in the configuration, followed by a server restart.

The query engine itself, pgduck_server, is configured with command-line options; see
[pgduck_server options](configuration.md#pgduck_server-options).

1. TOC
{:toc}

## Query engine connection and cache

| Parameter | Description |
|:--|:--|
| `pg_lake_engine.host`<span class="pglake-meta">Default `host=/tmp port=5332`, set by restart</span> | Connection string for pgduck_server. |
| `pg_lake_engine.enable_cache_manager`<span class="pglake-meta">Default `on`, set by superuser</span> | Run a background worker that keeps the pgduck_server file cache under `max_cache_size` and fills it with recently queried files. |
| `pg_lake_engine.max_cache_size`<span class="pglake-meta">Default `20GB`, set by superuser</span> | Size limit for the file cache. |
| `pg_lake_engine.cache_manager_interval`<span class="pglake-meta">Default `10s`, set by superuser</span> | Delay between cache manager runs. |
| `pg_lake_engine.max_parallel_file_uploads`<span class="pglake-meta">Default `12`, set by user</span> | Maximum number of concurrent file uploads to object storage per write. |
| `pg_lake_engine.log_engine_errors`<span class="pglake-meta">Default `on`, set by user</span> | Log a short classification of query engine errors in the PostgreSQL log. |

## Iceberg tables

| Parameter | Description |
|:--|:--|
| `pg_lake_iceberg.default_location_prefix`<span class="pglake-meta">Default none, set by superuser</span> | URL prefix under which new Iceberg tables store their files, such as `s3://bucket/iceberg`. |
| `pg_lake_iceberg.default_catalog`<span class="pglake-meta">Default `postgres`, set by user</span> | Catalog for new Iceberg tables: `postgres`, `rest`, `object_store`, or the name of a [catalog server](iceberg-catalogs.md#external-catalogs-with-create-server). |
| `pg_lake_iceberg.default_compatibility_mode`<span class="pglake-meta">Default `auto`, set by user</span> | `compatibility_mode` for new Iceberg tables: `auto` or `snowflake`. |
| `pg_lake_iceberg.unsupported_numeric_as_double`<span class="pglake-meta">Default `on`, set by user</span> | Store unbounded `numeric` and `numeric` with precision above 38 as `double precision`. When `off`, such columns are rejected. |
| `pg_lake_iceberg.max_snapshot_age`<span class="pglake-meta">Default `1800`, set by superuser</span> | Seconds to retain old snapshots before VACUUM expires them. Overridden per table by `max_snapshot_age`. |
| `pg_lake_iceberg.default_avro_writer_block_size_kb`<span class="pglake-meta">Default `64kB`, set by superuser</span> | Block size for manifest (Avro) files. |

## Writes and files

| Parameter | Description |
|:--|:--|
| `pg_lake_table.target_file_size_mb`<span class="pglake-meta">Default `512MB`, set by user</span> | Target size of data files; larger files are split during writes and compaction. A value below 1 disables splitting. |
| `pg_lake_table.target_row_group_size_mb`<span class="pglake-meta">Default `128MB`, set by superuser</span> | Target Parquet row group size. `0` disables the target. |
| `pg_lake_table.default_parquet_version`<span class="pglake-meta">Default `v1`, set by superuser</span> | Parquet format version for new files: `v1` or `v2`. |
| `pg_lake_table.copy_on_write_threshold`<span class="pglake-meta">Default `20`, set by user</span> | Percentage of deleted rows in a file above which a delete rewrites the file instead of writing a position delete file. `0` always rewrites, `100` never does. |
| `pg_lake_table.max_open_files_for_partitioned_write`<span class="pglake-meta">Default `5000`, set by superuser</span> | Maximum number of partition staging files a write keeps open. See [partitioning](iceberg-partitioning.md#configuring-open-files-for-partitioned-writes). |
| `pg_lake_table.enable_partitioned_write_pushdown`<span class="pglake-meta">Default `off`, set by user</span> | Let DuckDB write partitioned `INSERT ... SELECT` and `COPY FROM` statements directly. |

## Query planning

| Parameter | Description |
|:--|:--|
| `pg_lake_table.enable_full_query_pushdown`<span class="pglake-meta">Default `on`, set by user</span> | Push whole queries down to DuckDB when every part of the query can run there. |
| `pg_lake_table.enable_strict_pushdown`<span class="pglake-meta">Default `on`, set by user</span> | Only push down functions, operators and types whose DuckDB behavior is known to match PostgreSQL. |
| `pg_lake_table.enable_data_file_pruning`<span class="pglake-meta">Default `on`, set by superuser</span> | Skip Iceberg data files whose column statistics rule out a match. |
| `pg_lake_table.enable_partition_pruning`<span class="pglake-meta">Default `on`, set by superuser</span> | Skip Iceberg data files whose partition values rule out a match. |

See [performance](performance.md) for how these affect queries.

## Maintenance

| Parameter | Description |
|:--|:--|
| `pg_lake_iceberg.autovacuum`<span class="pglake-meta">Default `on`, set by reload</span> | Run the Iceberg autovacuum worker. |
| `pg_lake_iceberg.autovacuum_naptime`<span class="pglake-meta">Default `10min`, set by reload</span> | Time between autovacuum runs. |
| `pg_lake_iceberg.autovacuum_lock_timeout`<span class="pglake-meta">Default `1min`, set by reload</span> | How long autovacuum waits for a table lock before skipping the table. |
| `pg_lake_iceberg.log_autovacuum_min_duration`<span class="pglake-meta">Default `10min`, set by reload</span> | Log autovacuum runs that take longer than this. `-1` disables. |
| `pg_lake_table.max_compactions_per_vacuum`<span class="pglake-meta">Default `100`, set by superuser</span> | Maximum compactions in a single VACUUM run. |
| `pg_lake_table.max_file_removals_per_vacuum`<span class="pglake-meta">Default `10000`, set by superuser</span> | Maximum file deletions in a single VACUUM run. |
| `pg_lake_engine.orphaned_file_retention_period`<span class="pglake-meta">Default `10d`, set by superuser</span> | How long unreferenced files are kept before VACUUM deletes them. |
| `pg_lake_engine.vacuum_file_remove_max_retries`<span class="pglake-meta">Default `145`, set by superuser</span> | Attempts to delete a file before giving up on it. |
| `pg_lake_table.enable_delete_file_function`<span class="pglake-meta">Default `off`, set by superuser</span> | Allow `lake_file.delete()` to delete files. |

## Catalogs

| Parameter | Description |
|:--|:--|
| `pg_lake_iceberg.rest_catalog_host`<span class="pglake-meta">Default `http://localhost:8181/api/catalog`, set by superuser</span> | Base URL of the built-in `rest` catalog. |
| `pg_lake_iceberg.rest_catalog_client_id`<span class="pglake-meta">Default none, set by superuser</span> | OAuth2 client ID for the `rest` catalog. |
| `pg_lake_iceberg.rest_catalog_client_secret`<span class="pglake-meta">Default none, set by superuser</span> | OAuth2 client secret for the `rest` catalog. |
| `pg_lake_iceberg.rest_catalog_oauth_host_path`<span class="pglake-meta">Default none, set by superuser</span> | OAuth2 token endpoint URL, if not the catalog's default. |
| `pg_lake_iceberg.rest_catalog_scope`<span class="pglake-meta">Default `PRINCIPAL_ROLE:ALL`, set by superuser</span> | OAuth2 scope. |
| `pg_lake_iceberg.rest_catalog_enable_vended_credentials`<span class="pglake-meta">Default `off`, set by superuser</span> | Request temporary storage credentials from the catalog. |
| `pg_lake_iceberg.enable_object_store_catalog`<span class="pglake-meta">Default `on`, set by reload</span> | Publish tables to the object store catalog. |
| `pg_lake_iceberg.object_store_catalog_location_prefix`<span class="pglake-meta">Default none, set by reload</span> | Storage location of the object store catalog. |
| `pg_lake_iceberg.object_store_catalog_max_age`<span class="pglake-meta">Default `1min`, set by reload</span> | Time after which the object store catalog file is rewritten even without changes. |

## Security

| Parameter | Description |
|:--|:--|
| `pg_lake.allowed_azure_host_suffixes`<span class="pglake-meta">Default Azure public, US government and China cloud endpoints, set by superuser</span> | Host suffixes that user-supplied Azure URLs may point to. |

Access to object storage from SQL is controlled by roles rather than settings; see
[roles and permissions](configuration.md#roles-and-permissions).

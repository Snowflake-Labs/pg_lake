---
title: Table options
parent: Reference
nav_order: 2
---

# Table options
{: .no_toc }

pg_lake tables are configured with options on `CREATE TABLE` and `CREATE FOREIGN TABLE`.
PostgreSQL uses a slightly different syntax for each statement:

```sql
-- CREATE TABLE uses WITH and =
CREATE TABLE t (...) USING iceberg WITH (partition_by = 'day(ts)');

-- CREATE FOREIGN TABLE and ALTER TABLE use OPTIONS without =
CREATE FOREIGN TABLE f () SERVER pg_lake OPTIONS (path 's3://bucket/data/*.parquet');
ALTER TABLE t OPTIONS (SET partition_by 'month(ts)');

-- COPY uses WITH without =
COPY t FROM 's3://bucket/data.csv' WITH (format 'csv', header true);
```

1. TOC
{:toc}

## Iceberg tables

Options for `CREATE TABLE ... USING iceberg`. Iceberg tables are foreign tables on the
`pg_lake_iceberg` server, so `CREATE FOREIGN TABLE ... SERVER pg_lake_iceberg OPTIONS (...)`
accepts the same options.

| Option | Description |
|:--|:--|
| `location`<span class="pglake-meta">Default under `pg_lake_iceberg.default_location_prefix`; changes apply to new data files</span> | URL prefix for the table's data and metadata files. Changing it sends new data files to the new location, while metadata and existing files stay where they are. |
| `partition_by`<span class="pglake-meta">Default none; can be changed</span> | Partition spec, a comma-separated list of transforms such as `'day(ts), bucket(16, id)'`. See [partitioning](iceberg-partitioning.md). |
| `catalog`<span class="pglake-meta">Default `pg_lake_iceberg.default_catalog`; fixed at creation</span> | `postgres`, `rest`, `object_store`, or the name of a [catalog server](iceberg-catalogs.md#external-catalogs-with-create-server). See [catalogs](iceberg-catalogs.md). |
| `read_only`<span class="pglake-meta">Default `false`; fixed at creation</span> | Attach an existing table from a REST or `object_store` catalog for reading. See [query tables from an external catalog](iceberg-catalogs.md#query-tables-from-an-external-catalog). |
| `catalog_name`<span class="pglake-meta">Default the catalog server's `catalog_name`, or the database name; can be changed for read-only tables</span> | Catalog name of a read-only table. |
| `catalog_namespace`<span class="pglake-meta">Default schema name; can be changed for read-only tables</span> | Namespace of a read-only table. |
| `catalog_table_name`<span class="pglake-meta">Default table name; can be changed for read-only tables</span> | Table name of a read-only table. |
| `autovacuum_enabled`<span class="pglake-meta">Default `true`; can be changed</span> | Whether the autovacuum worker processes the table. |
| `autovacuum_compact_data_files`<span class="pglake-meta">Default `true`; can be changed</span> | Whether autovacuum compacts the table's data files. |
| `max_snapshot_age`<span class="pglake-meta">Default `pg_lake_iceberg.max_snapshot_age`; can be changed</span> | Snapshot retention in seconds. `0` expires old snapshots on every write. |
| `column_stats_mode`<span class="pglake-meta">Default `truncate(16)`; can be changed</span> | Which column statistics to keep for file pruning: `full`, `none`, or `truncate(N)` to keep the first `N` bytes of string bounds. |
| `out_of_range_values`<span class="pglake-meta">Default `error`; can be changed</span> | `error` or `clamp`. See [out-of-range values](data-types.md#out-of-range-values). |
| `compatibility_mode`<span class="pglake-meta">Default `pg_lake_iceberg.default_compatibility_mode`; fixed at creation</span> | `auto` or `snowflake`. With `snowflake`, `uuid` values nested in arrays or composites are stored as strings. |

`CREATE TABLE ... USING iceberg` also accepts the options for
[creating a table from a file](#creating-a-table-from-a-file).

## Data lake files

Options for `CREATE FOREIGN TABLE ... SERVER pg_lake`, which queries files in place.

| Option | Description |
|:--|:--|
| `path` | URL of a file, or a pattern with `*` and `**` wildcards. Required unless `writable` is set. |
| `format` | `parquet`, `csv`, `json`, `gdal`, `iceberg`, `delta` or `log`. Inferred from the file extension if omitted. |
| `compression` | `gzip`, `zstd`, `snappy`, `zip` or `none`, depending on the format. Inferred from the extension if omitted. |
| `filename` | `true` adds a `_filename` column with the source file of each row. With an explicit column list, declare `_filename text` as the last column. See [loading new files as they arrive](data-lake-import-export.md#load-new-files-as-they-arrive). |
| `writable` | `true` makes the table accept `INSERT`, writing new files under `location`. |
| `location` | URL prefix for new files of a writable table. |

Writable tables support the `parquet`, `csv` and `json` formats.

Format-specific options:

| Option | Formats | Description |
|:--|:--|:--|
| `header` | csv | Whether the first line is a header. Detected if omitted. |
| `delimiter` | csv | Field separator, such as `';'`. Detected if omitted. |
| `quote` | csv | Quote character. |
| `escape` | csv | Escape character within quoted values. |
| `null` | csv | String that represents `NULL`. |
| `new_line` | csv | Line terminator. |
| `null_padding` | csv | `true` fills missing trailing columns with `NULL`. |
| `maximum_object_size` | json | Largest JSON object to accept, in bytes. |
| `layer` | gdal | Layer within a multi-layer file, such as a sheet name. |
| `zip_path` | gdal | File within a `.zip` archive, such as `'roads.shp'`. |
| `log_format` | log | Log template. Currently `s3` for S3 access logs. |

The [file formats reference](file-formats-reference.md) describes each format.

## Creating a table from a file

Heap and Iceberg tables accept these options on `CREATE TABLE`, along with the format-specific
options above:

| Option | Description |
|:--|:--|
| `definition_from` | URL of a file to infer the columns from. The column list must be empty. |
| `load_from` | URL of a file to infer the columns from (if the column list is empty) and load into the new table. |
| `format` | Format of the file. Inferred from the extension if omitted. |
| `compression` | Compression of the file. Inferred from the extension if omitted. |

```sql
CREATE TABLE trips () WITH (load_from = 's3://bucket/trips.csv.gz', header = true);
```

## COPY

`COPY ... FROM` and `COPY ... TO` accept a URL instead of a file name, and these options in
addition to PostgreSQL's own:

| Option | Description |
|:--|:--|
| `format` | `parquet`, `csv`, `json`, or `gdal` (`FROM` only). Inferred from the URL's extension if omitted. |
| `compression` | For `TO`: `snappy` (Parquet default), `gzip`, `zstd` or `none`. For `FROM`: inferred if omitted. |
| `header`, `delimiter`, `quote`, `escape`, `null` | As in PostgreSQL's CSV format. |

```sql
COPY (SELECT * FROM orders WHERE order_date >= '2026-01-01')
TO 's3://bucket/exports/orders-2026.parquet' WITH (compression 'zstd');
```

See [import and export](data-lake-import-export.md) for more examples.

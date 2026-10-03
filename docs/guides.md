---
title: User guide
nav_order: 4
has_children: true
has_toc: false
---

# User guide

How to work with Iceberg tables and data lake files from PostgreSQL.

| Page | What it covers |
|:--|:--|
| [Iceberg tables](iceberg-tables.md) | Creating and loading Iceberg tables, and when to use them. |
| &nbsp;&nbsp;[Partitioning](iceberg-partitioning.md) | Hidden partitioning, transforms, pruning and partitioned writes. |
| &nbsp;&nbsp;[Modifying tables](iceberg-modifying.md) | `UPDATE`, `DELETE`, schema changes and table options. |
| &nbsp;&nbsp;[Catalogs and interoperability](iceberg-catalogs.md) | The PostgreSQL catalog, REST catalogs, Spark, Python and Snowflake. |
| &nbsp;&nbsp;[Maintenance](iceberg-maintenance.md) | VACUUM, autovacuum, snapshots, metadata functions and recovery. |
| [Query data lake files](query-data-lake-files.md) | Querying files in place, wildcards, hive partitions and writable tables. |
| [Import and export](data-lake-import-export.md) | `COPY` and `load_from` for loading and exporting files, and deleting files. |
| [Geospatial](spatial.md) | GeoParquet, GDAL formats, geometry in Iceberg and spatial pushdown. |
| [Performance](performance.md) | Query pushdown, file pruning, the file cache and faster writes. |
| [dbt](dbt.md) | Building Iceberg and PostgreSQL tables with dbt. |

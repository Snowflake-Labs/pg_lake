# File formats reference

`pg_lake` supports querying, importing, and exporting in a variety of formats. The following commands can use all supported data lake formats:

- **`CREATE FOREIGN TABLE ... SERVER pg_lake ...`** (querying)
- **`COPY ... FROM`** (importing)
- **`CREATE TABLE ... WITH load_from / definition_from option`** (importing)
- **`COPY ... TO`** (exporting)

The format can be specified as an option in the command. Note that PostgreSQL has a slightly different syntax for specifying options in each case:

```sql
-- COPY command uses WITH and no =
COPY ... TO/FROM '<url>' WITH (format 'csv');

-- CREATE TABLE ... uses WITH and =
CREATE TABLE ... USING iceberg WITH (load_from = '<url>', format = 'csv');

-- CREATE FOREIGN TABLE ... uses OPTIONS and no =
CREATE FOREIGN TABLE ... SERVER pg_lake OPTIONS (path '<url>', format 'csv');
```

When using a common file name extension, the format will be automatically inferred from the extension.

The following formats are supported:

| **Format** | **Description** | **Extensions** | **Wildcard** |
| --- | --- | --- | --- |
| parquet | Parquet (including GeoParquet) | .parquet | Yes (for s3:) |
| csv | Comma-separated values | .csv .csv.gz csv.zst | Yes (for s3:) |
| json | Newline-delimited JSON | .json .json.gz .json.zst | Yes (for s3:) |
| gdal | Formats loaded using [**GDAL**](https://gdal.org/en/latest/drivers/vector/index.html) | See below | No |
| iceberg | External [**Apache Iceberg**](https://iceberg.apache.org/) snapshot | .metadata.json | No |
| delta | External [**Delta Lake table**](https://docs.databricks.com/en/delta/index.html) | n/a | No |
| log | Log files such as [**S3 logs**](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html) | n/a | Yes (for s3:) |

Note: delta requires compiling pg\_lake with `export PG_LAKE_DELTA_SUPPORT=1`.

## Parquet format

Parquet files are the most versatile format for importing and exporting data in `pg_lake`. They are self-describing in terms of column names, types, and compression, and use columnar storage.

The Parquet format supports the following additional options:

| **Option** | **Description** |
| --- | --- |
| compression | Compression method to use in COPY TO (**`'gzip'`**, **`'snappy'`**, **`'zstd'`**, or **`'none'`**), defaults to **`'snappy'`** |

## CSV format

When querying or importing CSV files, the CSV format properties like the presence of a header, delimiter, and quote character are automatically detected. However, in some cases the detection fails and you can specify it manually.

```sql
-- Example: create a table pointing to a list of compressed CSV files
create foreign table your_table_csv () 
server pg_lake 
options (path 's3://your_bucket_name/data/*.csv.gz');
```

When writing CSV with **`COPY ... TO`**, the defaults are the same as PostgreSQL, but you can use the options to override them.

The CSV format supports the following additional options:

| **Option** | **Description** |
| --- | --- |
| compression | (De)compression method to use (**`'gzip'`** , **`'zstd'`** , or **`'none'`**), inferred from extension, otherwise **`'none'`** |
| header | Whether to use a header (**`'true'`** or **`'false'`**) |
| delimiter | Character to use to separate values (e.g. **`';'`** ) |
| quote | Character to use as a quote around string literals (e.g. **`'"'`** ) |
| escape | Character to use to escape characters within string literals (e.g. **`'"'`** ) |

## JSON format

`pg_lake` can query, import, and export newline-delimited JSON files. For query and import, it can also support JSON arrays, though they may be less efficient.

```sql
-- Example: create a table pointing to a single compressed JSON file
create foreign table your_table_json () 
server pg_lake 
options (path 's3://your_bucket_name/values.json.z', compression 'zstd');
```

The JSON format supports the following additional options:

| **Option** | **Description** |
| --- | --- |
| compression | (De)compression method to use (**`'gzip'`**, **`'zstd'`**, or **`'none'`**), inferred from extension, otherwise **`'none'`** |

## GDAL format

`pg_lake` can use GDAL to query or import many different file formats. When using format 'gdal' explicitly, some file formats are recognized from the extension.

```sql
-- Example: create an Iceberg table from a .zip shapefile
create table forest_fires ()
using iceberg
with (load_from = 'https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.FireOccurrence.zip');
```


Below is an incomplete list:

| **Extension** | **Description** | **GDAL format inferred** |
| --- | --- | --- |
| .dxf .dwg | AutoCAD files | Yes |
| .fgb | FlatGeoBuf | Yes |
| .gdb | Geodatabase | No |
| .geojson .geojson.gz | GeoJSON | Yes |
| .geojsons .geojsonl | GeoJSONSeq | No |
| .gml | Geography Markup Language | Yes |
| .gpkg .gpkg.gz | Geopackage | Yes |
| .kml .kmz | Key-Hole Markup Language | Yes |
| .map | WAsP .map format | No |
| .mif .mit .tab | MapInfo datasets | No |
| .shp | Shapefile | No |
| .xls .xlsx | Excel files | No |
| .xodr | OpenDRIVE Road Description Format | No |
| .zip | Auto-detected or derived from zip_path extension (e.g. .shp for Shapefile, ) | Yes |

The GDAL format supports the following additional options:

| **Option** | **Description** |
| --- | --- |
| compression | Decompression method to use (**`'gzip'`** , **`'zip'`** , or **`'none'`**), inferred from extension, otherwise **`'none'`** |
| layer | Name of a layer within a file (e.g. **`'Sheet 1'`**) |
| zip_path | Relative path within a .zip file (e.g. **`'S_USA.OtherSubSurfaceRight.shp'`**) |

When using format GDAL, files are downloaded immediately when creating a table, which may therefore take longer than creating other types of tables.

## Log format

The log format is intended to make it easy to query and import raw log files based on a pre-defined template. Currently, only [**S3 access logs**](https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html) are supported.

```sql
create foreign table s3log ()
server pg_lake
options (
  path 's3://my-s3-logs/000000000000/eu-central-1/**',
  format 'log', log_format 's3'
);
```

The log format supports the following additional options:

| **Option** | **Description** |
| --- | --- |
| compression | (De)compression method to use (**`'gzip'`** , **`'zstd'`** , or **`'none'`**), defaults to**`'none'`** |
| log_format | Format of the log file (**`'s3'`**), required |

## External Iceberg format

`pg_lake` can create and manage Iceberg tables, but you may already have Iceberg tables managed by other systems. In that case, you can still query or import from those tables by creating a foreign table that points to the metadata JSON file that represents a specific version of the table:

```sql
create foreign table external_iceberg()
server pg_lake
options (path 's3://mybucket/table/v14.metadata.json');
```

Note that changes to the external Iceberg table will **not** be reflected in the foreign table unless you update the path to point to the new metadata file.

## Hugging Face

[**Hugging Face**](https://huggingface.co/) is a widely used platform for sharing machine learning models and training data. You can query files directly using a **`hf://`** prefix instead of **`s3`**. Hugging Face file URLs will look something 

```
https://huggingface.co/datasets/microsoft/orca-math-word-problems-200k/blob/main/data/train-00000-of-00001.parquet
```

The URL for the system will remove the extra **`/blob/main/`** and a final creation to create a foreign table with Hugging Face data will look like this:

```sql
CREATE FOREIGN TABLE word_problems ()
SERVER pg_lake OPTIONS
(path 'hf://datasets/microsoft/orca-math-word-problems-200k/data/train-00000-of-00001.parquet');
```

You can also use the wildcard path with the user and project name to create a foreign table for a batch of parquet files:

```sql
CREATE FOREIGN TABLE word_problems ()
SERVER pg_lake OPTIONS
(path 'hf://datasets/microsoft/orca-math-word-problems-200k@~parquet/**/*.parquet');
```
The Hugging Face URLs currently do not use caching. If you access a data set frequently, we recommend moving the data to S3 or loading it into a Postgres table.

## Postgres tables

You can create regular (”heap”) tables in PostgreSQL as usual.

The **`load_from`** and **`definition_from`** options can also be used when creating heap tables:

```sql
create table vegetation()
with (load_from = 'https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.Activity_RngVegImprove.zip');
```

Similarly, you can use the **`COPY`** command with a URL to import/export data in a heap table.

An advantage of heap tables over Iceberg tables is that you can create indexes and more efficiently handle **`insert`** operations, and selective **`select`** / **`update`**/ **`delete`** operations, as well as certain types of joins.

In addition to built-in [**storage parameters**](https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS) and the format-specific options, heap tables support the following options in the `CREATE TABLE` statement:

| Option          | Description                                                                 |
| --------------- | --------------------------------------------------------------------------- |
| format          | Format of the file to load and/or infer columns from                        |
| definition_from | URL of a file to infer the columns from                                     |
| load_from       | URL of a file to infer the columns from and immediately load into the table |

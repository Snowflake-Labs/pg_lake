# Data lake import and export

Once your credentials are set so your cloud storage can be accessed by `pg_lake`, 
you can easily import data from CSV, newline delimited JSON or
Parquet files with automatic schema detection.

## Importing data from cloud object storage

The following example will create a regular PostgreSQL table where columns are
inferred from the data file on S3 using the warehouse auto-detection feature.

```sql
CREATE TABLE your_table_name () WITH (definition_from = 's3://your_bucket_name/your_file_name.xx');
```

When you’re using `definition_from` , the column list must be empty. If you want
to specify column definitions manually, simply follow the regular PostgreSQL
`CREATE TABLE` syntax.

You can then use the familiar COPY command to load data into the table from S3:

```sql
COPY your_table_name FROM 's3://your_bucket_name/your_file_name.xx';
```

`pg_lake` support the [options available in COPY](https://www.postgresql.org/docs/current/sql-copy.html),
`with csv` , `delimiter`, `headers`, and more.

Alternatively, if you want to create a regular PostgreSQL table and immediately
load in the data, you can use the following:

```sql
CREATE TABLE your_table_name () WITH (load_from = 's3://your_bucket_name/your_file_name.xx');
```

When using `load_from` you can specify column definitions manually as you would
when creating regular tables in Postgres.

For the `COPY` and `CREATE TABLE` commands, having the full Postgres syntax
allows passing various options such as the file type, compression algorithm,
delimiter and CSV header information. Check the PostgreSQL documentation for the
full list of the standard options.

The supported file types and compression algorithms for `pg_lake`
are:

| File Type / Compression | gzip | zstd | snappy |
| ----------------------- | ---- | ---- | ------ |
| CSV                     | ☑️   | ☑️   |        |
| JSON                    | ☑️   | ☑️   |        |
| Parquet                 | ☑️   | ☑️   | ☑️     |

### File formats

If your data file extension is `parquet`, `csv` or `json`, `pg_lake` 
can automatically detect the format. However, if the file doesn’t have
the proper file extension, you can provide the format in the foreign table
creation options.

```sql
CREATE FOREIGN TABLE ft () SERVER pg_lake
 OPTIONS (path 's3://your_bucket_name/data_file', format 'parquet');
```

## Compression examples

`pg_lake` can automatically detect the file and compression type from the extension of the file. Compressed Parquet files do not have extensions that indicate compression type, but the file metadata does; auto-detection will work without any options. However, for CSV and JSON files without extensions indicating the compression algorithm, you will need to specify it in the options.

```sql
—- gzipped files when you attempt a create foreign table
create foreign table ft3 () SERVER pg_lake
OPTIONS (path 's3://your_bucket_name/data_file.csv');
ERROR:  Invalid Input Error: CSV Error on Line: 1
```

```sql
—-now, provide compression for gzip
create foreign table ft3 () SERVER pg_lake
OPTIONS (path 's3://your_bucket_name/data_file.csv', compression 'gzip');
```

## Copy syntax

`pg_lake` allows for a large range of PostgreSQL’s COPY CSV options
such as `header`, `delimiter`, `quote`, `escape`, `null` . See the
[Postgres copy docs](https://www.postgresql.org/docs/current/sql-copy.html) for
additional options.

Here are a few examples of how to create a table and immediately load data from
a file in Amazon S3. Note that this can be optionally broken into two steps,
first creating the table with `definitions_from` the source file and then
copying data into the table.

```sql
--Columns, file format and compression are automatically detected from parquet file extension and metadata

CREATE TABLE your_table_name () WITH (load_from = 's3://your_bucket_name/your_file_name.parquet';

--Delimiter and header information provided in options; columns, file format and compression are automatically detected from csv.gz extension and file content

CREATE TABLE your_table_name () WITH (load_from = 's3://your_bucket_name/your_file_name.csv.gz', delimiter = ',', header = false);

--Columns provided (optional), since file extension is missing format and compression type need to be provided

CREATE TABLE your_table_name (id integer, name VARCHAR(50)) WITH (load_from = 's3://your_bucket_name/your_file_name', format ='json', compression ='gzip');
```

## Exporting data out to cloud object storage

After setting up object storage credentials, you can start exporting data into
cloud object storage in CSV, newline delimited JSON or the efficient columnar
Parquet format. The following examples of `copy` statements show how to export a
table to different file types with or without compression, where the compression
type is inferred from the extension in the file name:

```sql
-- data exported in uncompressed CSV format
COPY table_to_export TO 's3://your_bucket_name/your_file_name.csv';

-- data exported with gzip compression in CSV format
COPY table_to_export TO 's3://your_bucket_name/your_file_name.csv.gz';

-- data exported uncompressed in new-line-delimited JSON format
COPY table_to_export TO 's3://your_bucket_name/your_file_name.json';

-- data exported with zstd compression in new-line-delimited JSON format
COPY table_to_export TO 's3://your_bucket_name/your_file_name.json.zst';

-- data exported in Parquet format using snappy compression (default)
COPY table_to_export TO 's3://your_bucket_name/your_file_name.parquet';

-- results of a query are saved in a file
COPY (SELECT * FROM table_to_export JOIN other_table USING (id)) TO 's3://your_bucket_name/your_file_name.parquet';
```

You can also pass options to the COPY command based on the format of the file
you want to write.

### Generated columns

By default, `COPY ... TO` includes generated columns in the exported file. This
differs from core PostgreSQL, which excludes generated columns so that the
output can be reloaded with `COPY ... FROM`.

To get the core PostgreSQL behavior and exclude generated columns from the
output, turn off the `pg_lake_copy.include_generated_columns` setting:

```sql
-- exclude generated columns from COPY TO output (default: on)
SET pg_lake_copy.include_generated_columns TO off;

COPY table_to_export TO 's3://your_bucket_name/your_file_name.parquet';
```

The setting only affects `COPY ... TO` when no explicit column list is given;
an explicit column list (for example `COPY t (a, b) TO ...`) is exported as
specified regardless of this setting.

### psql copy

pg_lake also lets you import and export with the copy formats
from the psql client using the `\copy` meta-command. Note that you’ll always
want to specify format and compression when using psql \copy because the local
file extension is not visible to the server.

```sql
-- Import a compressed JSON file from local disk
\copy data from '/tmp/data.json.gz' with (format 'json', compression 'gzip');

-- Export a Parquet file to local disk
\copy data to '/tmp/data.parquet' with (format 'parquet');
```

## Converting CSV and JSON to Parquet

If you need to run high performance analytical queries against CSV and JSON
files, you can convert them to the Parquet format using tools inside `pg_lake`. 
The Parquet format is notably more performant.

Copy the files into your database:

```sql
CREATE TABLE your_table_name () WITH (load_from = 's3://your_bucket_name/your_file_name.xx');
```

Then export the foreign table into a Parquet file:

```sql
COPY table_to_export TO 's3://your_bucket_name/your_file_name.parquet';
```

Finally, create your foreign table:

```sql
CREATE FOREIGN TABLE your_table_name_parquet () SERVER pg_lake OPTIONS (path 's3://your_bucket_name/your_file_name.parquet');
```

Querying the resulting Parquet file will take advantage of its compression
features while also making use of vectorized execution, multi-threading and data
statistics to dramatically speed up your analytical queries.

#### How much faster is Parquet than CSV?

Here is a small data example with the same query against both CSV and Parquet:

```sql
EXPLAIN ANALYZE SELECT * FROM thermostat_csv;
                                                       QUERY PLAN
------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on thermostat_csv  (cost=100.00..120.00 rows=1000 width=0) (actual time=38.885..59.216 rows=7205 loops=1)
 Planning Time: 0.141 ms
 Execution Time: 60.624 ms
```

```sql
EXPLAIN ANALYZE SELECT * FROM thermostat_parquet;
                                                        QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------
 Foreign Scan on thermostat_parquet  (cost=100.00..120.00 rows=1000 width=0) (actual time=5.427..21.359 rows=7205 loops=1)
 Planning Time: 1.139 ms
 Execution Time: 26.496 ms
```

This shows Parquet executing a little faster than 2x than the same CSV. Results
will vary by workload.

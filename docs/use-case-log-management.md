## Use case: Log management

A sample `pg_lake` use case is storing logs from a variety of applications in Iceberg and analyzing them using PostgreSQL queries.

Many tools can archive log files into S3. The log files are usually in a text format, such as newline-delimited JSON or tab-separated values. You can set up lake tables to query these files directly, but parsing and processing a large number of log files may take a long time. Hence, it is preferable to append the log data to an Iceberg table, which is compressed and optimized for fast queries.

We developed the [pg_incremental](https://github.com/crunchydata/pg_incremental) extension to incrementally process files (or rows in a table). It periodically runs a SQL command with a parameter that is set to the next file or array of files to process. Since the bookkeeping of the processed files is done in the same transaction as the command, files are always processed exactly once. Using this technique, we can automatically append all existing and new log files that appear in object storage to an Iceberg table in an efficient, reliable manner.

## Converting logs to Iceberg incrementally

It is possible to directly load log files into an Iceberg table using `COPY`. However, it can be convenient to still create a lake analytics table for the log files. By leaving the column list in the `create  foreign table` statement empty, the columns will be automatically detected from the existing log files. The `filename 'true'` option additionally adds a `_filename` column that will contain the source file name.

Build pg_incremental:

```bash
git clone https://github.com/CrunchyData/pg_incremental.git
cd pg_incremental
make install
```

```sql
-- Create a table to query CSV logs, infer columns and add a _filename column
create foreign table logs_csv () 
server pg_lake
options (path 's3://mybucket/logs/*.csv.gz', filename 'true');
```

Next, we set up an Iceberg table. In this case we use the detected columns from the CSV logs.
```sql
create table logs_iceberg (like logs_csv) 
using iceberg;
```

Finally, we set up a pg\_incremental job with simple insert..select command that filters by the `_filename` column. Only the files that match the filter will be scanned by the query engine, which makes incremental processing relatively efficient. By default, the existing files will be processed immediately. 

```sql
-- Set up a pg_incremental job to process existing and new files
select incremental.create_file_list_pipeline('process-logs', 
   file_pattern := 's3://mybucket/logs/*.csv.gz', 
   batched := true,
   command := $$
       insert into logs_iceberg select * from logs_csv where _filename = any($1) 
   $$);
```

Using the `batched := true` argument means that the parameter will be an array of up to 100 files (changeable via `max_batch_size` argument) to process files in batches. If there is a very large number of small files, increasing the batch size is recommended.


## Transforming logs

When converting logs to Iceberg, you may additionally want to transform values. You should decide which fields you need in the Iceberg table, and convert the values accordingly in the insert..select command.

```sql
-- Create a table to query JSON logs
-- Top-level keys become columns, either scalar types or jsonb for nested JSON
create foreign table logs_json () 
server pg_lake 
options (path 's3://mybucket/logs/*.json', filename 'true');

-- Create an Iceberg table with the same schema
create table logs_iceberg (
    event_id bigserial,
    event_time timestamptz,
    machine_id uuid,
    response_time double precision,
    error_code int,
    message text
)
using iceberg;

-- Set up a pg_incremental job to extract relevant fields from logs
select incremental.create_file_list_pipeline('process-logs', 
   file_pattern := 's3://mybucket/logs/*.json.gz', 
   batched := true, 
   command := $$
       insert into
         logs_iceberg (event_time, machine_id, response_time, error_code, message)
       select
         to_timestamp("timestamp"),
         (payload->>'machine-id')::uuid,
         payload->>'response-time',
         payload->>'error-code',
         payload->>'message'
       from
         logs_json
       where
         _filename = any($1) 
   $$);
```

For JSON logs, we recommend "unwinding" nested fields into columns, since that will enable min-max statistics and generally faster computation.

## Querying Iceberg logs

Once your logs are converted to Iceberg, you can query the Iceberg table like any other Postgres table. 

```sql
-- Example SQL on Iceberg: Get errors by hour for the last week
select
  date_trunc('hour', event_time),
  count(*)
from
  logs_iceberg
where
  error_code between 500 and 599 and
  event_time >= date_trunc('day', now() - interval '7 days')
group by 1
order by 1 asc;
```

It is useful to select specific columns (avoid `select *`), since the query engine will only read necessary columns from the underlying Parquet files. Adding relevant filters (e.g. on time range) further improves performance, since the query engine can skip files and row groups within the files based on filters.

# DuckLake tables

DuckLake tables are transactional, columnar tables stored in object storage,
optimized for analytics. The catalog -- snapshots, schemas, tables, columns,
data files, partition specs, statistics -- is held inside PostgreSQL itself,
following the [DuckLake v1 specification](https://ducklake.select). DuckDB
clients can attach to the same catalog over the postgres bridge and read or
write the same tables that PostgreSQL writes.

This is provided by the `pg_lake_ducklake` extension. Install it with
`CREATE EXTENSION pg_lake_ducklake CASCADE` -- the `CASCADE` pulls in
`pg_lake` and the rest of the dependencies on a fresh database.

## Creating a DuckLake table

DuckLake tables are created by appending `using ducklake` (or its alias
`using pg_lake_ducklake`) to a regular `CREATE TABLE`:

```sql
-- create a new DuckLake table in managed storage
create table measurements (
  station_name text not null,
  measurement double precision not null
)
using ducklake;

insert into measurements values ('Istanbul', 18.5);
```

To use your own object-store bucket, pass `location`. The bucket must be in
the same region as your PostgreSQL server, and your
[credentials](../README.md#connecting-pg_lake-to-s3-or-compatible) must be
configured.

```sql
-- pin a single table to a specific bucket
create table measurements (
  station_name text not null,
  measurement double precision not null
)
using ducklake with (location = 's3://mybucket/measurements/');
```

Or set a default prefix once and let pg_lake derive
`<prefix>/<schema>/<table>/` for every new table:

```sql
set pg_lake_ducklake.default_location_prefix to 's3://mybucket/lake';

create table measurements (
  station_name text not null,
  measurement double precision not null
)
using ducklake;     -- ends up at s3://mybucket/lake/public/measurements/
```

`CREATE TABLE ... AS SELECT` is supported, including the `AS TABLE`, `AS
VALUES`, and `WITH NO DATA` variants. `WITH (load_from = '<url>')` works
too -- DuckLake tables share the `pg_lake_table` FDW infrastructure for
both, so column inference from a file (`definition_from`) and
column-typed CTAS hand off the same way.

```sql
-- CTAS from a SELECT
create table summary
using ducklake with (location = 's3://mybucket/summary/')
as select region, count(*) AS n
   from events
   group by region;

-- inherit columns from a remote file, no data yet
create table taxi ()
using ducklake
with (definition_from = 's3://mybucket/taxi-2024-01.parquet');

-- inherit columns + immediately load
create table taxi_2024_01 ()
using ducklake
with (load_from = 's3://mybucket/taxi-2024-01.parquet');
```

## DuckLake table options

| Option         | Description                                                                                  |
| -------------- | -------------------------------------------------------------------------------------------- |
| `location`     | URL prefix for the table's data files (e.g. `s3://mybucket/measurements/`).                  |
| `partition_by` | Comma-separated list of partition transforms, e.g. `'region, year(ts), bucket(16, user_id)'`. See [Partitioning](#partitioning). |

## Loading and modifying data

DuckLake tables behave like regular PostgreSQL tables for DML:

```sql
insert into measurements values ('Athens', 21.7);

update measurements
set measurement = measurement + 0.5
where station_name = 'Istanbul';

delete from measurements where measurement < 0;

copy measurements from 's3://mybucket/raw/measurements.csv' with (format csv, header);
```

Each statement opens a new DuckLake snapshot and registers the resulting
Parquet files with their column statistics in `lake_ducklake.data_file`.
Position-delete files are used for `UPDATE`/`DELETE`, so historical snapshots
remain readable.

It is recommended to load data in batches. A single statement that writes
many rows produces fewer, larger Parquet files than a row-at-a-time loop.

## Reading

`SELECT` against a DuckLake table is no different from any other table:

```sql
select station_name, avg(measurement)
from measurements
group by station_name
order by 2 desc;
```

Reads delegate scanning and execution to DuckDB through pgduck_server.
Partition pruning, predicate pushdown into Parquet, and column statistics
pruning are applied automatically.

## Partitioning

Pass `partition_by` at create time to declare a partition spec:

```sql
create table events (
  user_id   bigint,
  event_ts  timestamptz,
  region    text,
  payload   jsonb
)
using ducklake
with (
  location     = 's3://mybucket/events/',
  partition_by = 'region, day(event_ts), bucket(32, user_id)'
);
```

Supported transforms: `identity` (a bare column reference), `year(col)`,
`month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)`, `truncate(N, col)`.
Files are routed into per-partition Parquet outputs, partition values are
recorded in `lake_ducklake.file_partition_value`, and read-side queries are
pruned by `WHERE` predicates on the partition columns.

Partition spec evolution after `CREATE TABLE` is not yet supported.

## Schema layout

`pg_lake_ducklake` exposes the DuckLake catalog as two layers:

* `lake_ducklake.*` -- the underlying catalog tables (`snapshot`,
  `schema`, `table`, `column`, `data_file`, `delete_file`, `partition_info`,
  `partition_column`, `file_partition_value`, `file_column_stats`,
  `table_stats`, `metadata`, ...). These match the DuckLake v1 spec
  one-for-one. Read access only; user writes go through the views.
* `public.ducklake_*` -- the spec-defined views (`ducklake_table`,
  `ducklake_snapshot`, `ducklake_data_file`, etc.) that DuckDB's ducklake
  extension expects. INSTEAD-OF triggers route writes back to the underlying
  catalog tables under transaction-aware validation.

The roles follow the DuckLake
[access-control](https://ducklake.select/docs/stable/duckdb/guides/access_control)
recommendation:

| Role                  | Privileges on `public.ducklake_*` views |
| --------------------- | --------------------------------------- |
| `ducklake_superuser`  | `SELECT, INSERT, UPDATE, DELETE`        |
| `ducklake_writer`     | `SELECT, INSERT, UPDATE, DELETE`        |
| `ducklake_reader`     | `SELECT`                                |

These are PostgreSQL group roles (no `LOGIN`); grant a user membership with
`GRANT ducklake_writer TO alice;`.

## Attaching from DuckDB

DuckDB's ducklake extension can attach the same catalog over the postgres
bridge. PostgreSQL serves as the control plane; DuckDB clients write Parquet
files directly to object storage and append catalog rows through the views.

```sql
-- in DuckDB
INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;

-- point ducklake at our PG-hosted catalog
ATTACH 'postgres:host=postgres.example dbname=lakehouse user=alice'
  AS dl
  (TYPE DUCKLAKE);

-- DuckLake tables created from PostgreSQL show up as dl.public.*
SELECT * FROM dl.public.measurements;

-- Writes from DuckDB land in the same catalog and same data_path
INSERT INTO dl.public.measurements VALUES ('Berlin', 12.0);
```

You can also create new DuckLake tables from DuckDB; pg_lake replays the
`CREATE TABLE` and `CREATE SCHEMA` events into PostgreSQL so the foreign
table is visible from there immediately.

If `pg_lake_ducklake.default_location_prefix` is set on the PG side and
`lake_ducklake.metadata.data_path` is populated from it, the DuckDB ATTACH
needs no `DATA_PATH` clause -- the catalog tells DuckDB where to put files.

## Architecture and transactions

DuckLake tables are foreign tables on a dedicated `pg_lake_ducklake` server,
backed by the same FDW machinery as `pg_lake_table`. PostgreSQL handles
transaction boundaries and DDL replay; DuckDB (running inside pgduck_server)
handles columnar execution. The catalog itself lives entirely inside
PostgreSQL.

* **Transactions.** Every PG-side write is part of the surrounding
  PostgreSQL transaction. Snapshots are allocated lazily on first write
  and committed atomically when the transaction commits. Aborts roll back
  both the catalog state and the per-tx Parquet files (their rows are
  removed from `lake_ducklake.data_file` so future scans skip them).
* **Concurrency.** Concurrent PostgreSQL writers serialise behind a
  transaction-scoped advisory lock around snapshot allocation, mirroring
  pg_lake_table's per-relation update lock. External DuckDB writers don't
  take that lock; they rely on DuckLake's primary-key-based optimistic
  concurrency on `lake_ducklake.snapshot.snapshot_id`.
* **DDL propagation.** PG-side `CREATE TABLE`, `DROP TABLE`,
  `ALTER TABLE ... ADD/DROP/RENAME COLUMN`, `ALTER TABLE ... RENAME TO`,
  and `ALTER SCHEMA RENAME` flow into the catalog. DuckDB-side DDL is
  replayed back into PostgreSQL via triggers on the `public.ducklake_*`
  views.

## Supported features

* DuckLake v1 metadata catalog (snapshot, schema, table, column,
  data_file/delete_file, partition_info/column/value, name_mapping,
  column_mapping, table_stats, table_column_stats, file_column_stats,
  inlined_data_tables tracking, files_scheduled_for_deletion).
* `CREATE TABLE`, `CREATE TABLE AS SELECT`, `INSERT`, `UPDATE`, `DELETE`,
  `COPY ... FROM/TO`, `TRUNCATE`, `DROP TABLE`,
  `ALTER TABLE ADD/DROP/RENAME COLUMN`, `ALTER TABLE RENAME TO`,
  `ALTER SCHEMA RENAME`, `CREATE SCHEMA`, `DROP SCHEMA`.
* Bidirectional read/write interop with DuckDB's ducklake extension over
  the postgres bridge, including snapshot-changes replay and DuckDB-driven
  compaction.
* Partition routing on `INSERT` for `identity`, `year`, `month`, `day`,
  `hour`, `bucket(N)`, `truncate(N)` transforms.
* Position-delete files for transactional `UPDATE` / `DELETE`.
* Column statistics (min/max/null count/value count) recorded per Parquet
  file and rolled up to per-table.
* Relative-path storage: schemas and tables can be moved by changing
  `lake_ducklake.metadata.data_path`.
* Default-value backfill on `ADD COLUMN ... DEFAULT` for previously written
  files.

## Limitations

* No partition spec evolution after `CREATE TABLE`.
* No time-travel reads; `lake_ducklake.snapshot` is fully versioned but
  no GUC plumbs a historical `snapshot_id` to scans yet.
* No incremental (change-feed) reads; scans always read the latest
  snapshot, so there is no way to read just the rows changed between two
  snapshots.
* Inlined data is disabled. `data_inlining_row_limit` is forced to 0 so
  DuckDB always writes a Parquet file instead of materialising small
  INSERTs into a catalog table, because the FDW can't read inlined rows
  yet.
* `CREATE VIEW` from the DuckLake side persists `lake_ducklake.view` rows
  but isn't materialised as a PostgreSQL view; views aren't visible from
  PostgreSQL.
* `dropped_schema` events from DuckDB are not propagated back to drop the
  PostgreSQL schema (intentionally, to preserve grants).
* `column_tag`, `tag`, `sort_info`/`sort_expression`, `column_mapping` /
  `name_mapping` columns are stored but not yet exercised by reads.
* DuckDB macros (`macro`, `macro_impl`, `macro_parameters`) are stored but
  not surfaced in PostgreSQL.
* External (non-PG) writers must rely on DuckLake's PK-based OCC retry;
  pg_lake's serialising lock only covers PG-side writers.
* Multidimensional array values, `infinity`/`-infinity` temporals, and
  out-of-range numeric scales follow the same rules as Iceberg tables; see
  [Iceberg tables -- Out-of-range value handling](./iceberg-tables.md#out-of-range-value-handling).

## Inspecting catalog state

```sql
-- list live DuckLake tables and their storage paths
select t.table_name, s.schema_name, t.path
from public.ducklake_table  t
join public.ducklake_schema s using (schema_id)
where t.end_snapshot is null;

-- snapshot history
select * from public.ducklake_snapshot order by snapshot_id desc limit 10;

-- live data files for a specific table
select path, record_count, file_size_bytes
from public.ducklake_data_file
where table_id = (
  select table_id from public.ducklake_table
  where table_name = 'measurements' and end_snapshot is null
)
and end_snapshot is null;
```

---
title: Partitioning
parent: Iceberg tables
grand_parent: User guide
nav_order: 1
---

# Partitioning Iceberg tables
{: .no_toc }

1. TOC
{:toc}

Partitioning splits a large table into groups of files by the value of one or more columns,
so that queries filtering on those columns can skip most of the data. If you know PostgreSQL's
[declarative partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html), the
goal is the same, but Iceberg gets there differently:

- **Partitioning is hidden.** Instead of creating a child table per partition, you declare
  expressions such as `day(event_time)` or `bucket(16, user_id)` on the table. pg_lake computes
  the partition of each row as it writes, and tracks partitions in the Iceberg metadata. Queries
  filter on the original columns (`WHERE event_time >= ...`); you never reference a partition
  directly.
- **Multiple expressions combine freely.** You can partition by `day(event_time)` and
  `bucket(16, user_id)` at the same time, without nesting sub-partitions.
- **The partition spec can evolve.** You can switch from `day(event_time)` to
  `month(event_time)`, or add and drop expressions, without rewriting the table. New data uses
  the new spec, and existing files keep the layout they were written with.
- **Retention is cheap.** A `DELETE` whose filter covers whole partitions removes the data files
  from the table metadata without scanning them.

Each partition is stored in one or more Parquet files. If a partition accumulates many small
files, [VACUUM](iceberg-maintenance.md#vacuum) merges them.

## Defining and evolving partitions

To define partitioning for an Iceberg table, use the `WITH (partition_by = '...')` option when creating the table. You can also modify or drop the partitioning strategy later using `ALTER TABLE ... OPTIONS`.

The following example defines two partition expressions:

- `day(event_time)` for time-based filtering
- `bucket(32, user_id)` for distributing data evenly by user and filtering by `user_id`

```sql
CREATE TABLE events (
  event_time timestamptz NOT NULL,
  user_id    bigint       NOT NULL,
  region     text         NOT NULL,
  event_type text,
  payload    jsonb
)
USING iceberg
WITH (
  partition_by = 'day(event_time), bucket(32, user_id)'
);
```

You can change the partitioning strategy later without rewriting or recreating the table. Partition changes apply only to newly written data. Existing files retain their original layout, and Iceberg tracks partition history internally. This makes it easy to experiment and adapt as access patterns evolve. To change the partitioning:

```sql
ALTER TABLE events 
OPTIONS (
  SET partition_by 'truncate(4, region), day(event_time)'
);
```

If your Iceberg table was not partitioned at `CREATE TABLE` time, you can add it via using the `ADD` keyword instead of `SET`:

```sql
ALTER TABLE events
OPTIONS (
  ADD partition_by 'truncate(4, region), day(event_time)'
);
```

To remove partitioning entirely:

```sql
ALTER TABLE events 
OPTIONS (
  DROP partition_by
);
```

You can inspect the current partitioning with `\d` in `psql`, look for `partition_by` in `FDW options`:

```sql
\d events 
                            Foreign table "public.events"
   Column   |           Type           | Collation | Nullable | Default | FDW options 
------------+--------------------------+-----------+----------+---------+-------------
 event_time | timestamp with time zone |           | not null |         | 
 user_id    | bigint                   |           | not null |         | 
 region     | text                     |           | not null |         | 
 event_type | text                     |           |          |         | 
 payload    | jsonb                    |           |          |         | 
Server: pg_lake_iceberg

FDW options: (partition_by 'day(event_time), bucket(32, user_id)', location 's3://mybucket/postgres/public/events/123085')
```

The `partition_by` option can be combined with other Iceberg table options, and with `CREATE TABLE ... AS`.

## Supported partition transforms
| **Transform** | **Description** | **Supported types** |
| --- | --- | --- |
| `col` | Identity partitioning, stores the column’s value as-is. Useful for low-cardinality columns like `region` or `status`. | `date`, `timestamp(tz)`, `time(tz)`, `int2`, `int4`, `int8`, `bool`, `float4`, `float8`, `numeric`, `text`, `varchar`, `bpchar`, `bytea`, `uuid` |
| `year(col)` | Extracts the year part of a date or timestamp. Good for coarse time partitioning. | `date`, `timestamp(tz)` |
| `month(col)` | Extracts the year and month. Typically used for monthly aggregates or logs. | `date`, `timestamp(tz)` |
| `day(col)` | Extracts the full date (year-month-day). Ideal for time-series or log data. | `date`, `timestamp(tz)` |
| `hour(col)` | Extracts the hour of day. Useful for high-volume hourly events. | `timestamp(tz)` |
| `bucket(N, col)` | Hashes the column and assigns it to one of `N` evenly distributed buckets. | `int2`, `int4`, `int8`, `numeric`, `text`, `varchar`, `char(C)`, `bytea`, `uuid`, `date`, `timestamp(tz)`, `time(tz)` |
| `truncate(N, col)` | Truncates the value to a multiple of `N` (for integers) or to a prefix of length `N` (for string and binary). | `int2`, `int4`, `int8`, `text`, `varchar`, `char(C)`, `bytea` |

## Best practices
Partitioning can greatly improve query performance at scale — but it also introduces overhead, especially during writes and maintenance. If your table is small or mostly used with full-table scans, you might not benefit from partitioning.

Below are some guidelines to help you get the most out of Iceberg’s hidden partitioning.

- Writes to partitioned Iceberg tables are often **slower** than writes to non-partitioned tables. This is expected: The engine must evaluate partition expressions, organize data into the right partition files, and manage more metadata.
- For datasets larger than `~10GB` — especially those queried with filters like `WHERE event_time >= ...` — partitioning allows the query engine to **skip most files**, dramatically reducing scan time. If your workload includes large scans with predictable filter conditions, partitioning can lead to significant end-to-end speedups.
- More partitions mean more small files, which can **hurt performance** and increase operational overhead. Each distinct partition value results in a separate set of files. If you partition by high-cardinality columns (e.g. `user_id`, `uuid`, or `event_time`), Iceberg may create **thousands of tiny files**. This leads to:
    - Slower write performance
    - Increased metadata size
    - Higher planning and listing overhead
    - More frequent `VACUUM` needs to merge files

Instead of `user_id`, prefer `bucket(32, user_id)` to spread users across a fixed number of partitions. Instead of partitioning by raw timestamp, use `year(event_time)` or `month(event_time)` depending on query patterns.

- Let your common filter patterns guide your partitioning strategy. A few examples:
    - **Time-based data** → `year(event_time)`, `month(event_time)`
    - **User-specific queries** → `bucket(32, user_id)`
    - **Region or category filters** → `region` or `truncate(4, region)`

## Configuring open files for partitioned writes

When a partitioned write cannot be pushed down, pg_lake keeps one staging file open for each distinct partition tuple touched by the statement. The number of open files therefore depends on the number of partitions, not the number of rows. A write that exceeds PostgreSQL's transient file descriptor limit fails with an error similar to:

```text
ERROR: exceeded maxAllocatedDescs (...) while trying to open file "..."
```

`pg_lake_table.max_open_files_for_partitioned_write` controls when pg_lake flushes a staging file. PostgreSQL reserves only approximately one third of [`max_files_per_process`](https://www.postgresql.org/docs/current/runtime-config-resource.html#GUC-MAX-FILES-PER-PROCESS) for transient file descriptors, so configure:

```text
max_files_per_process > 3 * pg_lake_table.max_open_files_for_partitioned_write
```

Leave additional headroom for files opened internally by PostgreSQL and make sure the operating system's per-process file descriptor limit is high enough. The pg_lake setting defaults to `5000`, while PostgreSQL's `max_files_per_process` defaults to `1000`. You can either lower the pg_lake setting to fit the PostgreSQL limit:

```sql
-- requires superuser
SET pg_lake_table.max_open_files_for_partitioned_write = 250;
```

or raise `max_files_per_process` to more than `15000`. Changing `max_files_per_process` requires a PostgreSQL restart. Lowering `pg_lake_table.max_open_files_for_partitioned_write` causes partitions to be flushed sooner, which can produce smaller files.

Partitioned write pushdown is disabled by default. For eligible `INSERT ... SELECT` and `COPY FROM` statements, enable it for the current session to delegate partitioned writes to DuckDB:

```sql
SET pg_lake_table.enable_partitioned_write_pushdown = on;
```

This avoids PostgreSQL's staging-file path and its transient file descriptor limit. Partitioned write pushdown supports identity, `year`, `month`, `day`, and `hour` partition transforms. Statements using `bucket` or `truncate`, as well as statements that otherwise cannot be pushed down, use the staging-file path described above. DuckDB does not support target file size splitting together with partitioned write pushdown.

## Partition pruning
Partition pruning is how Iceberg avoids scanning unnecessary data files. When you filter by a partition column (or one of its transforms), only the matching partition files are read — the rest are skipped entirely. The pruning happens at the file level, using Iceberg’s metadata.

For `bucket` transforms, only equality filters (e.g., `=`) trigger partition pruning. For the rest of the transforms, many more operators trigger pruning such as `>`, `<`, `>=`, `<=`, `=`, `IN`, `ANY` and `BETWEEN`.

You can confirm pruning by checking the `Data Files Scanned:` line in the output of `EXPLAIN (verbose)`. Fewer files scanned means better pruning.

Let’s create a table partitioned by year and insert two rows from different years:

```sql
CREATE TABLE t_year_partitioned (
  event_time timestamptz NOT NULL,
  message     text
)
USING iceberg
WITH (
  partition_by = 'year(event_time)'
);

INSERT INTO t_year_partitioned VALUES
  ('2023-05-20', 'hello from 2023'),
  ('2024-08-10', 'hello from 2024');
```

Now query with a filter that matches only one partition, see `Data Files Scanned:`:

```sql
EXPLAIN (verbose)
SELECT * FROM t_year_partitioned
WHERE event_time >= '2024-01-01';
                                      QUERY PLAN                                      
--------------------------------------------------------------------------------------
 Custom Scan....
   ....
   Data Files Scanned: 1
```

Similarly, Iceberg only processes relevant data files when removing data for a given partition. In this case, look for `Data Files Skipped:` in the `EXPLAIN (verbose)` output — it shows how many files were avoided entirely. In the example below, the partition for `year=2023` is skipped because the filter only matches `year=2024`. This makes large-scale data retention operations fast and efficient.

```sql
EXPLAIN (verbose) 
DELETE FROM t_year_partitioned WHERE event_time >= '2024-01-01';

                                      QUERY PLAN                                      
--------------------------------------------------------------------------------------
 Delete on public.t_year_partitioned ...
   ...
   Data Files Skipped: 1
```

## Limitations

- Renaming or dropping columns that are ever used in `partition_by` is not supported.
- After changing a table’s partitioning strategy, existing data continues to use the old layout — there’s currently no built-in way to rewrite past data with the new partitioning.
- Columns with collations are not allowed in `partition_by`.
- Composite types are not allowed in `partition_by`.

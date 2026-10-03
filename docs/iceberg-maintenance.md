---
title: Maintenance
parent: Iceberg tables
grand_parent: User guide
nav_order: 4
---

# Maintaining Iceberg tables
{: .no_toc }

Every write to an Iceberg table adds new data files and a new snapshot, and leaves the files it
replaced behind. pg_lake cleans this up automatically, much like autovacuum does for heap
tables. This page explains what happens, how to tune it, and how to look inside a table.

1. TOC
{:toc}

## VACUUM

Running `VACUUM` on an Iceberg table does four things:

1. **Compacts data files.** Small Parquet files, for example from many small inserts, are
   merged into files of up to `pg_lake_table.target_file_size_mb` (512 MB by default), and
   position delete files are merged back into the data files they apply to.
2. **Expires old snapshots and merges manifests.** Snapshots older than the retention period
   are removed from the table metadata, and small manifest files are combined.
3. **Deletes unreferenced files.** Files that no snapshot references any more, including those
   of dropped tables, are deleted from object storage once they have been unreferenced for
   longer than `pg_lake_engine.orphaned_file_retention_period` (10 days by default).
4. **Cleans up after failed writes.** Files left behind by aborted transactions are removed.

```sql
-- vacuum a single Iceberg table
VACUUM measurements;

-- vacuum all Iceberg tables in the database, one at a time
VACUUM (ICEBERG);

-- show what is being done
VACUUM (ICEBERG, VERBOSE);
```

`VACUUM` on Iceberg tables cannot run inside a transaction block. It commits after each step,
so it makes progress even if it is interrupted, and it does not block reads or inserts while it
works. A single run is capped at `pg_lake_table.max_compactions_per_vacuum` compactions and
`pg_lake_table.max_file_removals_per_vacuum` file deletions. `VACUUM FULL` lifts the
file-removal cap and also compacts files that a regular VACUUM would leave alone, which can
take a long time on a large table.

### Autovacuum

A background worker vacuums every Iceberg table every `pg_lake_iceberg.autovacuum_naptime`
seconds (10 minutes by default), so you usually do not need to run `VACUUM` yourself.

| Setting | Level | Description |
|:--|:--|:--|
| `pg_lake_iceberg.autovacuum` | server | Turns the autovacuum worker on or off. Default `on`. |
| `pg_lake_iceberg.autovacuum_naptime` | server | Seconds between autovacuum runs. Default `600`. |
| `pg_lake_iceberg.log_autovacuum_min_duration` | server | Log autovacuum runs that take longer than this many milliseconds. Default `600000`. |
| `autovacuum_enabled` | table | Whether autovacuum processes this table at all. Default `true`. |
| `autovacuum_compact_data_files` | table | Whether autovacuum compacts this table's data files. Snapshot expiry and file cleanup still run. A manual `VACUUM` always compacts. Default `true`. |

```sql
-- no autovacuum for this table
CREATE TABLE test_auto_vacuum (id int) USING iceberg WITH (autovacuum_enabled = 'false');

-- or change it later
ALTER FOREIGN TABLE measurements OPTIONS (ADD autovacuum_enabled 'false');

-- keep expiring snapshots and deleting old files, but never rewrite data files
ALTER FOREIGN TABLE events OPTIONS (ADD autovacuum_compact_data_files 'false');
```

Turning off compaction is useful for append-only tables whose files are already large, or when
you want to control when data files are rewritten, for example because another engine caches
them.

## Snapshot retention

Each write creates a new snapshot. Old snapshots are expired by VACUUM once they are older than
`pg_lake_iceberg.max_snapshot_age` seconds (1800 by default). Other engines can read a table
at an older snapshot only while it is retained.

You can override the retention per table with the `max_snapshot_age` option. With `0`, old
snapshots are expired during every write, so the metadata stays small without waiting for
VACUUM:

```sql
-- expire old snapshots immediately on every write
CREATE TABLE events (id int, payload text)
USING iceberg WITH (max_snapshot_age = 0);

-- or change an existing table
ALTER FOREIGN TABLE events OPTIONS (ADD max_snapshot_age '0');

-- return to the server default
ALTER FOREIGN TABLE events OPTIONS (DROP max_snapshot_age);
```

Expiring a snapshot does not delete its files right away: they enter the deletion queue and are
kept for `pg_lake_engine.orphaned_file_retention_period` first, which is what makes
[recovery](#recovering-old-data) possible.

## File sizes

pg_lake aims for large data files, which are faster to read and cheaper to list:

| Setting | Default | Description |
|:--|:--|:--|
| `pg_lake_table.target_file_size_mb` | `512` | Files are split during writes and compaction once they reach this size. |
| `pg_lake_table.target_row_group_size_mb` | `128` | Target Parquet row group size. Superuser only. |
| `pg_lake_table.default_parquet_version` | `v1` | Parquet format version for new data files: `v1` or `v2`. Superuser only. |

Small files mostly come from small writes. Batch your inserts where you can, and see
[loading data](iceberg-tables.md#loading-data) for a staging-table pattern.

## Inspecting Iceberg metadata

pg_lake has functions to read the metadata of any Iceberg table, including tables written by
other engines. They take the URL of a metadata file, which for pg_lake's own tables you can get
from the `iceberg_tables` view.

`lake_iceberg.metadata` returns the whole
[table metadata](https://iceberg.apache.org/spec/#table-metadata) as `jsonb`:

```sql
-- the current schema of the table
WITH ice AS (
  SELECT lake_iceberg.metadata(metadata_location) AS metadata
  FROM iceberg_tables WHERE table_name = 'measurements'
)
SELECT jsonb_pretty(metadata->'schemas'->(metadata->>'current-schema-id')::int) FROM ice;

              jsonb_pretty
----------------------------------------
 {
     "type": "struct",
     "fields": [
         {
             "id": 1,
             "name": "station_name",
             "type": "string",
             "required": true
         },
         {
             "id": 2,
             "name": "measurement",
             "type": "double",
             "required": true
         }
     ],
     "schema-id": 0
 }
```

`lake_iceberg.snapshots` lists the retained snapshots:

```sql
SELECT snapshot_id, timestamp_ms, manifest_list
FROM iceberg_tables, lake_iceberg.snapshots(metadata_location)
WHERE table_name = 'measurements'
ORDER BY sequence_number;
```

`lake_iceberg.files` lists the data and delete files in the current snapshot, which is the
quickest way to see whether a table needs compaction:

```sql
SELECT content, count(*) AS files,
       pg_size_pretty(sum(file_size_in_bytes)) AS size,
       sum(record_count) AS records
FROM iceberg_tables, lake_iceberg.files(metadata_location)
WHERE table_name = 'measurements'
GROUP BY content;
```

`lake_iceberg.data_file_stats` returns the per-file column bounds that pg_lake uses to skip
files during queries:

```sql
SELECT path, lower_bounds, upper_bounds
FROM iceberg_tables, lake_iceberg.data_file_stats(metadata_location)
WHERE table_name = 'measurements';
```

## The deletion queue

Files that are no longer needed, because they were replaced, expired or belong to a dropped
table, are recorded in `lake_engine.deletion_queue` together with the time they were orphaned.
VACUUM deletes them once the retention period has passed.

```sql
SELECT path, orphaned_at
FROM lake_engine.deletion_queue
WHERE table_name = 'measurements'::regclass
ORDER BY orphaned_at DESC
LIMIT 10;
```

`lake_engine.flush_deletion_queue(table_name)` deletes all of a table's queued files that are
past the retention period right away, without the per-run limits and retry intervals that
VACUUM applies. It returns the paths it deleted.

Files written by transactions that did not commit, including `COPY ... TO` exports that failed
or were rolled back, are tracked separately, and VACUUM removes them too.
`lake_engine.flush_in_progress_queue()` removes them right away and returns their paths; files
of transactions that are still running are left alone.

The deletion queue and both functions are available to the `lake_write` role. To delete files
that do not belong to a table, such as old exports, see
[delete files](data-lake-import-export.md#delete-files).

## Recovering old data

pg_lake does not have table-level restore yet. However, since old data and metadata files stay
in object storage for the retention period, you can create an
[external Iceberg table](iceberg-catalogs.md#external-iceberg-tables-from-metadata-files) from an
old metadata file, and copy the rows you need back:

```sql
-- find the most recent metadata file that was orphaned more than 3 days ago
DO $$
BEGIN
  EXECUTE format('CREATE FOREIGN TABLE measurements_old () SERVER pg_lake OPTIONS (path %L)', (
    SELECT path
    FROM lake_engine.deletion_queue
    WHERE table_name = 'measurements'::regclass
      AND orphaned_at < now() - interval '3 days'
      AND path LIKE '%.metadata.json'
    ORDER BY orphaned_at DESC
    LIMIT 1
  ));
END;
$$;

-- restore rows that were deleted by mistake
INSERT INTO measurements
SELECT * FROM measurements_old WHERE station_name = 'Istanbul'
EXCEPT
SELECT * FROM measurements WHERE station_name = 'Istanbul';
```

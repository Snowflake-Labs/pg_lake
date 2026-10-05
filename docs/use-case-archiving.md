---
title: Archive partitions to Iceberg
parent: Use cases
nav_order: 3
---

# Archive partitions to Iceberg
{: .no_toc }

Many tables grow forever, but only recent rows are updated or looked up by key: orders, events,
audit logs, measurements. Keeping years of history in heap tables makes backups, vacuum and
storage expensive. With pg_lake, you can keep recent data in a heap table and move older rows
into an Iceberg table, where they take far less space and analytical queries over history run
on DuckDB.

1. TOC
{:toc}

## How it works

The operational table stays a regular heap table, partitioned by month with PostgreSQL's
[declarative partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html).
[pg_partman](https://github.com/pgpartman/pg_partman) creates its partitions ahead of time.
Next to it, a single Iceberg table holds the history, partitioned by month with
[Iceberg partitioning](iceberg-partitioning.md):

```text
app_events                    heap, partitioned by month of event_time
  +-- app_events_p20260901    heap, with indexes
  +-- app_events_p20261001    heap, with indexes
  +-- ...                     future months, created by pg_partman
  +-- app_events_default      rows that match no other partition

app_events_archive            Iceberg, partition_by = 'month(event_time)'
                              July and August 2026, in object storage
```

A scheduled procedure copies each partition that is old enough into the archive and drops the
partition. Dropping a partition is instant and leaves no dead rows to vacuum, unlike deleting
the rows.

## Prerequisites

- pg_lake.
- [pg_partman](https://github.com/pgpartman/pg_partman) 5 or later. These examples install it
  in a `partman` schema:

  ```sql
  CREATE SCHEMA partman;
  CREATE EXTENSION pg_partman SCHEMA partman;
  ```

- [pg_cron](https://github.com/citusdata/pg_cron) to run the jobs, or another scheduler.

## Create the tables

Create the partitioned table, and let pg_partman create monthly partitions from July 2026,
plus four months ahead of the current month:

```sql
CREATE TABLE app_events (
  event_id bigint GENERATED ALWAYS AS IDENTITY,
  event_time timestamptz NOT NULL,
  user_id bigint NOT NULL,
  event_type text,
  payload jsonb
) PARTITION BY RANGE (event_time);

SELECT partman.create_parent(
  p_parent_table := 'public.app_events',
  p_control := 'event_time',
  p_interval := '1 month',
  p_premake := 4,
  p_start_partition := '2026-07-01');

CREATE INDEX ON app_events (user_id, event_time);
```

pg_partman names the partitions after the start of their range, such as
`app_events_p20260701`, and also creates a default partition, `app_events_default`, for rows
that match no other partition.

Then create the Iceberg table for the history:

```sql
CREATE TABLE app_events_archive (LIKE app_events)
USING iceberg WITH (partition_by = 'month(event_time)');
```

`LIKE` gives the archive the same columns as `app_events`, in the same order, so rows can be
copied with `SELECT *`. It also copies the `NOT NULL` constraints, but not the identity, so the
archive keeps the `event_id` values the heap table assigned.

## Move old partitions to Iceberg

The procedure below archives every partition whose range ends before a cutoff, by default the
start of the month two months ago. It asks pg_partman which partitions exist and what range
each one covers, so it does not rely on partition names, skips months that were already
archived, and catches up if a run was missed:

```sql
CREATE PROCEDURE archive_app_events(older_than interval DEFAULT '2 months')
LANGUAGE plpgsql AS $$
DECLARE
  cutoff timestamptz := date_trunc('month', now() - older_than);
  part regclass;
BEGIN
  -- move late rows for archived months out of the default partition
  WITH moved AS (
    DELETE FROM app_events_default WHERE event_time < cutoff RETURNING *
  )
  INSERT INTO app_events_archive SELECT * FROM moved;
  COMMIT;

  -- move each partition that ends before the cutoff, one transaction per partition
  FOR part IN
    SELECT format('%I.%I', p.partition_schemaname, p.partition_tablename)::regclass
    FROM partman.show_partitions('public.app_events') p,
         partman.show_partition_info(format('%I.%I', p.partition_schemaname, p.partition_tablename)) i
    WHERE i.child_end_time <= cutoff
    ORDER BY i.child_start_time
  LOOP
    -- block writes to the partition, so that no row is lost between the copy and the drop
    EXECUTE format('LOCK TABLE %s IN SHARE MODE', part);
    EXECUTE format('INSERT INTO app_events_archive SELECT * FROM %s', part);
    EXECUTE format('DROP TABLE %s', part);
    COMMIT;
  END LOOP;
END;
$$;

CALL archive_app_events();
```

Because the Iceberg catalog lives in PostgreSQL, moving a partition is atomic: every query sees
its rows either in `app_events` or in `app_events_archive`, never in both or neither. Dropping
a partition locks `app_events` until the transaction commits, so the procedure commits after
each partition rather than holding the lock while it copies the next one. For the same reason,
`CALL` it outside a transaction block.

### Late rows

A row that arrives for a month that is already archived has no partition left, so PostgreSQL
puts it in `app_events_default`, where queries on `app_events` still find it. The next run of
`archive_app_events` moves such rows to the archive.

Rows too far in the future, beyond the months pg_partman has created, also land in
`app_events_default`. Those rows block pg_partman from creating the partition for their month
later, which fails with:

```text
ERROR:  updated partition constraint for default partition "app_events_default" would be violated by some row
```

Choose `p_premake` so that it covers every future timestamp your application writes, or reject
such rows before they are inserted.

## Automate it

Schedule pg_partman's maintenance, which creates future partitions, and the archive procedure
with [pg_cron](https://github.com/citusdata/pg_cron):

```sql
-- keep creating partitions ahead of time
SELECT cron.schedule('partman-maintenance', '@hourly',
  $$CALL partman.run_maintenance_proc()$$);

-- archive old months, on the first of every month
SELECT cron.schedule('archive-app-events', '0 3 1 * *',
  $$CALL archive_app_events()$$);
```

If pg_cron runs in a different database, use `cron.schedule_in_database` instead.

Do not configure pg_partman's own retention (`retention` in `partman.part_config`) for this
table: it drops or detaches old partitions on its own schedule, without copying them to the
archive.

## Query the archive

Queries on recent data use `app_events` and its indexes as before. Analytical queries over
history use `app_events_archive`, and pg_lake pushes the whole query down to DuckDB, which only
reads the months and columns the query needs:

```sql
EXPLAIN (COSTS OFF)
SELECT event_type, count(*) FROM app_events_archive WHERE event_time < '2026-08-01' GROUP BY 1;

 Custom Scan (Query Pushdown)
   Engine: DuckDB
   ->  HASH_GROUP_BY
         Groups: #0
         Aggregates: count_star()
         ->  PROJECTION
               Projections: event_type
               ->  READ_PARQUET
                     Filters: event_time<'2026-08-01 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                     Projections: event_type
```

You can combine both tables with `UNION ALL`, but then PostgreSQL computes the aggregate and
scans the heap side itself, and only the scan of the archive runs on DuckDB. If most
analytical queries need recent rows too, keep a full copy in Iceberg instead (see
[alternatives](#alternatives)).

`UPDATE` and `DELETE` work on archived rows, for example to correct data or to erase a user's
events. `DELETE` statements that remove whole months only change the Iceberg metadata, so
expiring the oldest data is cheap:

```sql
DELETE FROM app_events_archive WHERE event_time < now() - interval '3 years';
```

## Alternatives

- **No heap partitions.** If `app_events` is not partitioned, move rows with a `DELETE ...
  RETURNING` into the archive, as the procedure does for the default partition. This is simpler
  to set up, but the `DELETE` leaves dead rows behind for vacuum.
- **Keep a full copy in Iceberg.** If analytics should see all data, including recent rows,
  sync new rows into Iceberg continuously, as in
  [syncing tables to Iceberg](use-case-iceberg-sync.md), and drop old heap partitions once they
  are in Iceberg. Analytical queries then only read the Iceberg table.

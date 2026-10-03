---
title: Modifying tables
parent: Iceberg tables
grand_parent: User guide
nav_order: 2
---

# Modifying Iceberg tables
{: .no_toc }

1. TOC
{:toc}

## Updates and deletes

pg_lake supports `UPDATE` and `DELETE` on Iceberg tables, including updates and deletes with
joins or subqueries, and data-modifying CTEs:

```sql
-- delete rows from one table and move them to another table
WITH deleted_rows AS (
  DELETE FROM user_assets WHERE userid = 319931 RETURNING *
)
INSERT INTO deleted_assets SELECT * FROM deleted_rows;
```

Statements and transaction blocks that touch several tables, Iceberg or heap, keep PostgreSQL's
ACID guarantees: either all changes commit, or none do. An `UPDATE` or `DELETE` locks the
Iceberg table, so only one runs at a time per table; inserts and reads are not blocked.

### How deletes are written

Iceberg files are immutable, so pg_lake records a delete in one of two ways, per data file:

- **Merge-on-read:** writes a small *position delete* file that lists the deleted rows. This is
  fast when only a few rows of a file change. Readers skip the listed rows.
- **Copy-on-write:** rewrites the data file without the deleted rows. This is better when a
  large part of the file changes.

pg_lake picks copy-on-write once more than `pg_lake_table.copy_on_write_threshold` percent
(default 20) of a file's rows are deleted. When a `DELETE` matches whole files, such as whole
partitions, the files are simply removed from the table metadata. [VACUUM](iceberg-maintenance.md#vacuum)
later merges delete files back into the data files.

### Unsupported modifications

The following are not yet supported on Iceberg tables:

- `MERGE`
- `INSERT ... ON CONFLICT`
- `SELECT ... FOR UPDATE` and `FOR SHARE`
- Queries that use system columns such as `ctid`

If you need upsert semantics, stage the changes in a heap table and apply them with a
`DELETE` followed by an `INSERT` in one transaction. The [dbt integration](dbt.md) uses the same
approach.

## Schema changes

You can change an Iceberg table's schema with `ALTER TABLE`. pg_lake records the change as
[Iceberg schema evolution](https://iceberg.apache.org/docs/latest/evolution/), so no data files
are rewritten, and engines reading the table through its catalog see the new schema.

```sql
-- add a column
ALTER TABLE measurements ADD COLUMN measurement_tim timestamptz;

-- set the default for new rows
ALTER TABLE measurements ALTER COLUMN measurement_tim SET DEFAULT now();

-- rename a column
ALTER TABLE measurements RENAME COLUMN measurement_tim TO measurement_time;

-- drop a column
ALTER TABLE measurements DROP COLUMN measurement_time;

-- rename the table, change its owner or move it to another schema
ALTER TABLE measurements RENAME TO ocean_measurements;
ALTER TABLE ocean_measurements OWNER TO oceanographer;
CREATE SCHEMA ocean;
ALTER TABLE ocean_measurements SET SCHEMA ocean;
```

psql's tab completion does not offer these commands for Iceberg tables, because they are
foreign tables. `ALTER FOREIGN TABLE` accepts the same commands and does complete.

### Adding a column with a default

`ADD COLUMN ... DEFAULT` assigns the default to all existing rows. pg_lake only accepts a
constant there, because an expression would require rewriting every data file. You can still
set an expression as the default for new rows afterwards:

```sql
-- allowed: existing rows get a constant
ALTER TABLE measurements ADD COLUMN last_update_time timestamptz DEFAULT '2024-01-01 00:00:00';

-- not allowed: existing rows would need an expression
ALTER TABLE measurements ADD COLUMN last_update_time timestamptz DEFAULT now();
ERROR:  ALTER TABLE ADD COLUMN with default expression command not supported for pg_lake_iceberg tables

-- allowed: new rows get an expression
ALTER TABLE measurements ALTER COLUMN last_update_time SET DEFAULT now();
```

### Changing the type of a column

`ALTER COLUMN ... TYPE` accepts the type promotions that Iceberg allows without rewriting data
files. Existing data files keep their old type, and readers widen the values when they read
them:

| From | To |
|:--|:--|
| `smallint`, `integer` | `bigint` (and `smallint` to `integer`) |
| `real` | `double precision` |
| `numeric(P, S)` | `numeric(P2, S)` with `P2` greater than `P` and the same scale |

```sql
CREATE TABLE readings (sensor_id int, value real, cost numeric(10,2)) USING iceberg;

ALTER TABLE readings ALTER COLUMN sensor_id TYPE bigint;
ALTER TABLE readings ALTER COLUMN value TYPE double precision;
ALTER TABLE readings ALTER COLUMN cost TYPE numeric(14,2);
```

Any other change is rejected, including a narrower type, a different numeric scale, a longer
`varchar`, `text`, `timestamp` to `timestamptz`, and a `USING` clause. For those, add a column
of the new type, fill it and swap it in, which does rewrite the data files and moves the column
to the end of the table:

```sql
-- turn sensor_id into text
BEGIN;
ALTER TABLE readings ADD COLUMN sensor_id_new text;
UPDATE readings SET sensor_id_new = sensor_id::text;
ALTER TABLE readings DROP COLUMN sensor_id;
ALTER TABLE readings RENAME COLUMN sensor_id_new TO sensor_id;
COMMIT;
```

### Unsupported schema changes

These `ALTER TABLE` forms are not yet supported:

- Changing the type of a column, other than the [type promotions](#changing-the-type-of-a-column) above
- Adding or validating constraints
- Adding a generated column, a `serial` column, or a column with a constraint
- Adding a column of a type Iceberg cannot store (see [data types](data-types.md))
- Renaming or dropping a column that is, or ever was, used in `partition_by`

## Changing table options

Table options such as the partition spec, snapshot retention or autovacuum can be changed with
`ALTER TABLE ... OPTIONS`, using `ADD` for an option that is not set yet, `SET` for one that is,
and `DROP` to return to the default:

```sql
ALTER TABLE events OPTIONS (ADD partition_by 'month(event_time)');
ALTER TABLE events OPTIONS (SET partition_by 'day(event_time)');
ALTER TABLE events OPTIONS (ADD autovacuum_enabled 'false');
ALTER TABLE events OPTIONS (DROP max_snapshot_age);
```

The [table options reference](table-options.md) lists which options can be changed after
creation.

## Read-only tables

Tables attached from an external catalog with `read_only = true` can be queried but not
modified. External Iceberg tables created from a `metadata.json` file support one change:
pointing the table at a newer metadata file.

```sql
-- redirect an external Iceberg table to a newer snapshot
ALTER FOREIGN TABLE external_iceberg OPTIONS (SET path 's3://mybucket/table/metadata/v15.metadata.json');
```

This keeps dependent views and grants in place, which a `DROP` and `CREATE` would not. See
[catalogs and interoperability](iceberg-catalogs.md) for both kinds of table.

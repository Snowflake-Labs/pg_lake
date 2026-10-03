---
title: Migrate tables to Iceberg
parent: Use cases
nav_order: 5
---

# Migrate PostgreSQL tables to Iceberg
{: .no_toc }

You can copy tables from any PostgreSQL server, such as Amazon RDS, Azure Database for
PostgreSQL, Google Cloud SQL or your own servers, into Iceberg tables managed by pg_lake,
using the standard `pg_dump` and `psql` tools. This is useful to move large, mostly historical
tables to cheaper, columnar storage, or to make an operational database available for
analytics.

1. TOC
{:toc}

## Make Iceberg the default for a user

`pg_dump` output contains plain `CREATE TABLE` statements. To turn them into Iceberg tables,
make Iceberg the default table format for the user that restores them. As a superuser on the
pg_lake server:

```sql
CREATE ROLE application LOGIN PASSWORD '...';
GRANT lake_read_write TO application;
GRANT CREATE ON SCHEMA public TO application;

ALTER ROLE application SET default_table_access_method TO 'iceberg';
```

Only use this user for the migration, or reset the setting afterwards: with Iceberg as the
default, creating temporary or unlogged tables fails unless you add `USING heap`.

## Copy a table

Pipe `pg_dump` from the source server into `psql` on the pg_lake server:

```bash
pg_dump --table=orders --section=pre-data --section=data \
        --no-table-access-method --no-owner \
        "postgres://user@source-host:5432/sourcedb" \
  | psql "postgres://application@pglake-host:5432/postgres"
```

The options matter:

| Option | Why |
|:--|:--|
| `--section=pre-data --section=data` | Dump the table definition and its data, but not indexes, which Iceberg tables do not support. |
| `--no-table-access-method` | Stop `pg_dump` from setting `default_table_access_method` back to `heap`, which would override the user's setting. |
| `--no-owner` | Optional. Avoids errors when users and roles differ between the servers. |
| `--clean` | Optional. Replaces a table that already exists on the pg_lake server. |

The result is an Iceberg table with the same columns and data:

```sql
postgres=> \d orders
                             Foreign table "public.orders"
   Column    |           Type           | Collation | Nullable | Default | FDW options
-------------+--------------------------+-----------+----------+---------+-------------
 order_id    | bigint                   |           | not null |         |
 customer_id | integer                  |           |          |         |
 amount      | numeric(10,2)            |           |          |         |
 ordered_at  | timestamp with time zone |           |          | now()   |
Server: pg_lake_iceberg
FDW options: (catalog 'postgres', location 's3://mybucket/iceberg/postgres/public/orders/21361')
```

Tables with identity columns cause two errors during the restore, because `pg_dump` adds the
identity with an `ALTER TABLE` that Iceberg tables do not support, and then sets its sequence.
The data still loads; the column becomes a plain `NOT NULL` column.

Use `--table` several times, or `--schema`, to copy several tables at once. `pg_dump` takes a
consistent snapshot of all of them.

## Values Iceberg cannot store

Some PostgreSQL values have no Iceberg representation, such as `infinity` timestamps, `NaN` in
`numeric` columns, dates after the year 9999 and multidimensional arrays. By default, loading
such a value fails. If your data contains them, create the table first with
`out_of_range_values = 'clamp'`, which replaces them with the nearest valid value or `NULL`,
and load only the data:

```sql
CREATE TABLE orders (...) USING iceberg WITH (out_of_range_values = 'clamp');
```

```bash
pg_dump --table=orders --data-only "postgres://user@source-host:5432/sourcedb" \
  | psql "postgres://application@pglake-host:5432/postgres"
```

See [data types](data-types.md) for the full mapping.

## Copying within the same server

To convert a heap table on the pg_lake server itself, `CREATE TABLE ... AS` is simpler:

```sql
CREATE TABLE orders_iceberg USING iceberg AS SELECT * FROM orders;
```

To keep the two in sync afterwards, see [syncing tables](use-case-iceberg-sync.md#sync-new-rows-automatically)
and [archiving old data](use-case-archiving.md).

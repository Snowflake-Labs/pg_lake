---
title: Data types
parent: Reference
nav_order: 4
---

# Data types
{: .no_toc }

pg_lake stores Iceberg tables and writes Parquet files using the Iceberg and Parquet type
systems, which are narrower than PostgreSQL's. This page shows how PostgreSQL types map, and
what happens to values that do not fit.

1. TOC
{:toc}

## Type mapping

| PostgreSQL type | Iceberg type | Notes |
|:--|:--|:--|
| `boolean` | `boolean` | |
| `smallint`, `integer` | `int` | |
| `bigint`, `oid` | `long` | |
| `real` | `float` | |
| `double precision` | `double` | |
| `numeric(p,s)` with `p` ≤ 38 | `decimal(p,s)` | `NaN` cannot be stored; see [out-of-range values](#out-of-range-values). |
| `numeric` without precision, or `p` > 38 | `double` | Converted when the table is created; see [numeric](#numeric). |
| `text`, `varchar`, `char(n)` | `string` | |
| `bytea` | `binary` | |
| `uuid` | `uuid` | Stored as `string` when nested in an array or composite type under `compatibility_mode = 'snowflake'`. |
| `date` | `date` | Range `-4712-01-01` to `9999-12-31`. |
| `time`, `timetz` | `time` | |
| `timestamp` | `timestamp` | Range `0001-01-01` to `9999-12-31`, microsecond precision. |
| `timestamptz` | `timestamptz` | Stored in UTC. |
| `interval` | `struct<months, days, microseconds>` | Transparent in pg_lake; other engines see the struct. |
| `json`, `jsonb` | `string` | Stored as JSON text. |
| Arrays, e.g. `int[]` | `list` | One-dimensional values only. |
| Composite types | `struct` | Nested composites, arrays of composites and composites of arrays are supported. |
| [Map types](https://github.com/Snowflake-Labs/pg_lake/blob/main/pg_map/README.md) | `map` | Created with `map_type.create`. |
| PostGIS `geometry` | `binary` (WKB) | Requires `pg_lake_spatial`. See [geospatial](spatial.md#geometry-in-iceberg-tables). |
| Other types, such as `hstore` or enums | `string` | Stored in their text representation. |

Domains are stored as their base type. Types that cannot be used as Iceberg columns at all are
tables used as row types, and `geometry` nested inside an array or composite type.

When pg_lake reads Parquet, CSV or JSON files with an empty column list, it infers PostgreSQL
types from the file. Nested Parquet structs become composite types in the `lake_struct` schema,
with names derived from their field names, so similar files share the same types.

## Numeric

A bounded `numeric(p,s)` with a precision of up to 38 is stored as an Iceberg decimal. Iceberg
has no decimal wider than 38 digits, so by default an unbounded `numeric`, or one with a larger
precision, is created as `double precision` instead. Set
`pg_lake_iceberg.unsupported_numeric_as_double = off` to reject those columns at
`CREATE TABLE` time instead, so that you can choose a precision yourself.

`NaN` and infinity are valid in `double precision` columns, and are not subject to
`out_of_range_values`.

## Arrays

PostgreSQL has no separate multidimensional array type: an `int[]` column can hold both
`ARRAY[1,2,3]` and `ARRAY[ARRAY[1,2], ARRAY[3,4]]`. Iceberg maps `int[]` to a flat `list`, so
only one-dimensional values can be stored. Multidimensional values are handled according to
`out_of_range_values`.

## Out-of-range values

The Iceberg specification defines strict boundaries for temporal types that are narrower than what PostgreSQL allows, and some PostgreSQL values have no Iceberg/Parquet equivalent. When writing data to an Iceberg table, pg_lake validates these values. The `out_of_range_values` table option controls what happens when a value falls outside the representable range.

### Affected types and boundaries

| Type | Constraint |
| --- | --- |
| `date` | Range: `-4712-01-01` to `9999-12-31` |
| `timestamp` | Range: `0001-01-01 00:00:00` to `9999-12-31 23:59:59.999999` |
| `timestamptz` | Range: `0001-01-01 00:00:00+00` to `9999-12-31 23:59:59.999999+00` |
| `numeric(p,s)` (precision ≤ 38) | NaN is not representable in Iceberg decimals |
| Array columns (e.g. `int[]`, `text[]`) | Multidimensional arrays are not representable (Iceberg maps `int[]` to a flat `list`) |

PostgreSQL supports dates and timestamps well beyond year 9999, as well as special values like `infinity`, `-infinity`, and `NaN` for numerics. These values cannot be stored in Iceberg decimal columns (Parquet). Similarly, PostgreSQL allows multidimensional values in a plain array type (e.g. `ARRAY[ARRAY[1,2]]` in an `int[]` column), but Iceberg only supports flat lists.

{: .note }
Unbounded `numeric` and `numeric` with precision > 38 are stored as `double precision`, not as Iceberg decimals. NaN and infinity are valid in double precision columns and are **not** subject to `out_of_range_values` handling.

### Behavior: `clamp` vs `error`

The `out_of_range_values` option accepts two values:

- **`error`** (default): An error is raised if any value falls outside the Iceberg-representable range, including out-of-range temporals, NaN in bounded numerics, and multidimensional arrays. The write is aborted entirely.
- **`clamp`**: Out-of-range temporal values are silently adjusted to the nearest Iceberg boundary. `NaN` values in bounded `numeric(p,s)` columns (precision ≤ 38) are replaced with `NULL`. Multidimensional array values are replaced with `NULL`. **No error is raised and no warning is emitted.** This means your stored data may differ from what was inserted.

### Example

```sql
-- Default behavior (error): out-of-range values cause an error
CREATE TABLE events (
  event_time timestamptz NOT NULL,
  score numeric(10,2)
)
USING iceberg;

-- This fails with: "timestamptz out of range"
INSERT INTO events VALUES ('infinity', 3.14);

-- Clamp mode: out-of-range values are silently adjusted
CREATE TABLE events_clamp (
  event_time timestamptz NOT NULL,
  score numeric(10,2)
)
USING iceberg WITH (out_of_range_values = 'clamp');

-- This succeeds, but 'infinity' is stored as '9999-12-31 23:59:59.999999+00'
INSERT INTO events_clamp VALUES ('infinity', 3.14) RETURNING *;
          event_time           | score
-------------------------------+-------
 9999-12-31 23:59:59.999999+00 |  3.14
(1 row)

INSERT 0 1
```

The option can also be changed on an existing Iceberg table:

```sql
ALTER TABLE events OPTIONS (ADD out_of_range_values 'error');
```

### When to use `clamp`

The default `error` mode ensures data integrity by catching unexpected values early. However, if your pipeline may produce edge-case temporal values (e.g. sentinel dates like `9999-12-31` or `infinity` from PostgreSQL) and you want to avoid write failures, set `out_of_range_values` to `clamp`. This is useful when:

- Your pipeline produces sentinel values like `infinity` that you want silently mapped to the Iceberg boundary
- You are migrating data from PostgreSQL heap tables that might contain `infinity` or extreme dates and want to complete the migration without errors
- You prefer silent adjustments over strict error handling

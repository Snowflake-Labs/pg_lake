# pg_lake_timeseries

> **Status: working, including the vectorised cross-tier plan.** The routing view,
> the two tiers, the delta overlay, seal/repair/retention and the maintenance
> worker are implemented and covered by `tests/pytests/test_timeseries.py`. A
> query that spans both tiers becomes a single vectorised DuckDB plan once
> `pg_lake_table.enable_heap_query_pushdown` is on, which admits heap relations
> into pg_lake_table's whole-query pushdown ([`DESIGN.md`](DESIGN.md) §13.5
> option 2, phase 9); with the GUC off — the default — such a query still ships
> Iceberg rows into PostgreSQL. What was built and how it differs from the spec is
> in [`DESIGN.md`](DESIGN.md) §18.

`pg_lake_timeseries` makes a single PostgreSQL relation behave like a live,
indexed, mutable time-series table whose bulk lives in Apache Iceberg.

The target shape is **two overlapping tiers with automatic routing**:

- the last **N** days (say 7) live in PostgreSQL, indexed and deduplicated, up to
  `now()`;
- the last **M** days (say 365) live in one internally-partitioned pg_lake
  Iceberg table, up to the last materialisation;
- reads are routed by a stored **authority boundary**, so a query that stays on
  one side of it runs as a plain single-tier query — the other tier is pruned at
  plan time, by partition pruning rather than by constraint exclusion
  ([`DESIGN.md`](DESIGN.md) §18.3) — and a query that spans it returns each row
  exactly once. A spanning query runs as **one vectorised DuckDB plan** rather
  than shipping Iceberg rows into PostgreSQL, once
  `pg_lake_table.enable_heap_query_pushdown` is on.

Updates and upserts are allowed in the PostgreSQL tier. Mutations that reach
already-sealed Iceberg partitions land in a delta, are merged over Iceberg on
read, and are folded back in by a background repair, which returns the merge to a
plain Iceberg scan. See [`DESIGN.md`](DESIGN.md) §13 for the design and §18.4 for
why the merge is currently present on every cold read instead of only on the
recently-mutated partitions.

The underlying mechanisms are specified separately: §4–§11 the **base + delta
merge-on-read** machinery (the merge itself, executed inside DuckDB over a
snapshot-pinned reverse connection), §12 the **hot-authoritative tiering** family
and its sync modes including a CDC-fed Iceberg mirror.

A background worker seals aged partitions into Iceberg, manages the heap
partition frontier without DDL on the insert path, repairs mutated cold
partitions, and lets compaction/retention run on the cold tier.

The goals, the correctness argument, the query-optimizer analysis (why a
`UNION ALL` view or partitioned parent loses DuckDB aggregation today, and what
change fixes it), and the phased implementation plan are all in
[`DESIGN.md`](DESIGN.md).

## Why not existing options

- **TimescaleDB** is not available in this environment.
- **Native declarative partitioning** with older partitions as Iceberg foreign
  tables produces one foreign table per partition (poor plans), and partition
  management takes heavy locks.
- **A `UNION ALL` view or a partitioned parent over a heap and an Iceberg table**
  routes correctly by time, but any query spanning both tiers loses aggregate
  pushdown *unless the whole query is pushed down*: the FDW cannot push a
  *partial* aggregate, so every qualifying Iceberg row would otherwise be shipped
  into PostgreSQL and aggregated there. See [`DESIGN.md`](DESIGN.md) §13.5.

`pg_lake_timeseries` keeps *one* Iceberg table (internally partitioned) alongside
the indexed PostgreSQL tier, which removes the first two problems. The third is
solved by pushing the spanning query down as a whole: with
`pg_lake_table.enable_heap_query_pushdown` on, the heap tier is read back through
`postgres_scan_pushdown` on a snapshot-pinned reverse connection and unioned with
the Iceberg scan inside DuckDB, so the plan is a single
`Custom Scan (Query Pushdown)` instead of an `Append` over a heap scan and an
Iceberg `Foreign Scan` aggregated in PostgreSQL.

## Dependencies

Requires `pg_lake_engine`, `pg_lake_iceberg`, and `pg_lake_table`. It is an
optional add-on and is **not** installed by `CREATE EXTENSION pg_lake CASCADE`.

```sql
CREATE EXTENSION pg_lake_timeseries CASCADE;
```

## Usage

```sql
CREATE TABLE metrics (ts timestamptz, device int, value float8);

SELECT timeseries.create_table('metrics', 'ts',
                               key_columns => '{ts,device}',
                               partition_interval => '1 day',
                               hot_retention => '7 days',
                               cold_retention => '365 days');
```

`metrics` is now a view over `metrics_hot` (heap, range-partitioned, indexed,
mutable) and `metrics_cold` (one internally-partitioned Iceberg table). Writes go
through it and are routed by timestamp; the maintenance worker keeps the Iceberg
copy fresh, seals partitions as they age out of the hot window, repairs mutated
cold partitions and expires cold data.

The boundary and the per-table state are visible in `timeseries.tables` and
`timeseries.partitions`; `timeseries.sync()`, `seal()`, `repair()`,
`apply_retention()` and `maintain()` can also be called by hand, and
`timeseries.drop_table()` unregisters a table, keeping its data unless
`drop_data => true`.

## Cross-tier queries in one vectorised plan

A query that only touches one side of the boundary is already a plain single-tier
query. A query that spans it reads both tiers, and by default the Iceberg side is
a `Foreign Scan` whose rows are aggregated in PostgreSQL. To run the whole query
inside DuckDB instead:

```sql
SET pg_lake_table.enable_heap_query_pushdown TO on;

EXPLAIN SELECT device, avg(value) FROM metrics GROUP BY device;
```

The plan becomes a single `Custom Scan (Query Pushdown)`: the heap tier is read
back over a loopback connection with `postgres_scan_pushdown`, pinned to the
snapshot of the query that asked for it, with filters and projections pushed into
that scan; the cold tier is read from Parquet; and the union, the join against the
delta and the aggregate all run vectorised.

Leave it off for workloads that only read the hot window. With the pushdown on,
the whole view goes to DuckDB, so the planner no longer prunes the cold tier away
for a `WHERE ts >= <recent>` query: it returns the same rows, but it opens the
cold Parquet files to do it.

See `pg_lake_table`'s `enable_heap_query_pushdown` and `heap_pushdown_dsn`
settings for how the reverse connection is configured, and
[`DESIGN.md`](DESIGN.md) §18.11 and §18.12 for the details and the trade-off.

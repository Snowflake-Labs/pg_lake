# pg_lake_timeseries

> **Status: working, without the vectorised cross-tier plan.** The routing view,
> the two tiers, the delta overlay, seal/repair/retention and the maintenance
> worker are implemented and covered by `tests/pytests/test_timeseries.py`. A
> query that spans both tiers still ships Iceberg rows into PostgreSQL: the
> whole-query pushdown does not admit heap relations yet ([`DESIGN.md`](DESIGN.md)
> §13.5 option 2, phase 9). What was built and how it differs from the spec is in
> [`DESIGN.md`](DESIGN.md) §18.

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
  exactly once. The target for spanning queries is **one vectorised DuckDB plan**
  rather than shipping Iceberg rows into PostgreSQL; that part is not built yet.

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
  pushdown: the FDW cannot push a *partial* aggregate, so every qualifying
  Iceberg row is shipped into PostgreSQL and aggregated there. See
  [`DESIGN.md`](DESIGN.md) §13.5.

`pg_lake_timeseries` keeps *one* Iceberg table (internally partitioned) alongside
the indexed PostgreSQL tier, which removes the first two problems. The third is
the one still open: the plan for a spanning query is an `Append` over a heap scan
and an Iceberg `Foreign Scan`, aggregated in PostgreSQL, until heap relations are
admitted into the whole-query pushdown.

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

# pg_lake_timeseries

> **Status: skeleton / design.** This directory currently contains the catalog
> surface, the extension entry point, and a full specification in
> [`DESIGN.md`](DESIGN.md). The background worker and CustomScan described in
> the spec are not implemented yet; the SQL functions are stubs.

`pg_lake_timeseries` makes a single PostgreSQL relation behave like a live,
indexed, mutable time-series table whose bulk lives in Apache Iceberg.

The target shape is **two overlapping tiers with automatic routing**:

- the last **N** days (say 7) live in PostgreSQL, indexed and deduplicated, up to
  `now()`;
- the last **M** days (say 365) live in one internally-partitioned pg_lake
  Iceberg table, up to the last materialisation;
- reads are routed per partition by a stored **authority boundary**, so a query
  that stays on one side of it runs as a plain single-tier query, and a query
  that spans it is executed as **one vectorised DuckDB plan** rather than by
  shipping Iceberg rows into PostgreSQL.

Updates and upserts are allowed in the PostgreSQL tier. Mutations that reach
already-sealed Iceberg partitions land in a per-partition delta, are merged over
Iceberg on read, and are folded back in by a background repair — so the merge
cost is bounded by the set of recently-mutated cold partitions, not by the size
of the table. See [`DESIGN.md`](DESIGN.md) §13.

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
the indexed PostgreSQL tier, and pushes spanning queries into DuckDB as a single
plan — avoiding all three problems.

## Dependencies

Requires `pg_lake_engine`, `pg_lake_iceberg`, and `pg_lake_table`. It is an
optional add-on and is **not** installed by `CREATE EXTENSION pg_lake CASCADE`.

```sql
CREATE EXTENSION pg_lake_timeseries CASCADE;
```

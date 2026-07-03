# pg_lake_timeseries

> **Status: skeleton / design.** This directory currently contains the catalog
> surface, the extension entry point, and a full specification in
> [`DESIGN.md`](DESIGN.md). The background worker and CustomScan described in
> the spec are not implemented yet; the SQL functions are stubs.

`pg_lake_timeseries` makes a single PostgreSQL relation behave like a live,
indexed, mutable time-series table whose bulk lives in Apache Iceberg.

It does this with a **base + delta merge-on-read** design:

- a **base** — one internally-partitioned pg_lake Iceberg table holding (nearly
  all of) the data, scanned by DuckDB with file-level pruning;
- a **delta** — a small, timed-index heap holding recent inserts/updates/deletes
  that have not yet been folded into the base;
- a **merge-on-read** parent that overlays the delta on the base by key (newest
  version wins, tombstones suppress), so reads see a single consistent table.

A background worker keeps the base fresh (flushing the delta into Iceberg),
manages the heap partition frontier without DDL on the insert path, and lets
compaction/retention run on the cold tier.

The goals, the correctness argument (exactly-once under concurrent flush without
snapshot pinning), the read/write paths, and the phased implementation plan are
all in [`DESIGN.md`](DESIGN.md).

## Why not existing options

- **TimescaleDB** is not available in this environment.
- **Native declarative partitioning** with older partitions as Iceberg foreign
  tables produces one foreign table per partition (poor plans), and partition
  management takes heavy locks.

`pg_lake_timeseries` keeps *one* Iceberg table (internally partitioned) plus a
small heap delta, unified by a single parent — avoiding both problems.

## Dependencies

Requires `pg_lake_engine`, `pg_lake_iceberg`, and `pg_lake_table`. It is an
optional add-on and is **not** installed by `CREATE EXTENSION pg_lake CASCADE`.

```sql
CREATE EXTENSION pg_lake_timeseries CASCADE;
```

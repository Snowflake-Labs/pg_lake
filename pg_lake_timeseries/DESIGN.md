# pg_lake_timeseries — Design & Specification

Status: **draft / pre-implementation.** This document is the source of truth for
the extension; the code in this directory is a skeleton (catalog surface +
entry point + stub functions).

---

## 1. Motivation

We want a good way to store large volumes of time-series data in PostgreSQL,
with the bulk of the data living in Apache Iceberg (via pg_lake) so that
analytical scans run on DuckDB's columnar engine and storage is cheap.

The obvious approaches don't fit:

- **TimescaleDB** is not an option in this environment.
- **Native declarative partitioning with older partitions turned into Iceberg
  foreign tables** yields *one foreign table per partition*. That produces poor
  plans (per-partition foreign scans, limited partition-wise optimization) and
  fragments what should be one Iceberg table. We want **one** Iceberg table,
  internally partitioned, so DuckDB prunes files with manifest statistics.
- **Partition management in PostgreSQL takes heavy locks** and requires
  ongoing operational effort (create/attach/detach/drop).

### The idea

Present a **single relation** whose data is split across two physical stores:

- **Base** — one pg_lake **Iceberg** table, internally partitioned (e.g.
  `days(ts)`), holding nearly all of the data. Scanned by DuckDB with
  file-level pruning; the target for all heavy analytical work.
- **Delta** — a small **heap** table (range-partitioned by time, with timed
  secondary indexes) holding recent inserts, updates, and delete tombstones
  that have **not yet** been folded into the base.

Reads **merge** the delta over the base by logical key: the newest version of
each key wins, and tombstones suppress the base row. A background worker
periodically **flushes** the delta into the base (merge-on-read into Iceberg),
keeping the base "reasonably up to date, including most of the last N days," so
the delta stays small regardless of retention.

This is a base+delta / merge-on-read design (we call it **model A** below, as
distinct from a strict watermark-split tiering we call model B). It is
essentially a Postgres-fronted LSM layer over an Iceberg table.

---

## 2. Goals and non-goals

### Goals

1. A single logical relation combining fresh heap data and an Iceberg base.
2. Fast, indexed access to recent data (point lookups, selective queries).
3. Full columnar pushdown (predicates, projection, aggregation) to DuckDB for
   the parts of a query that don't touch the delta.
4. No DDL on the insert hot path; no per-partition foreign tables.
5. Correct results (exactly-once, updates/deletes applied) even while a flush
   runs concurrently with readers — **without** requiring Iceberg snapshot
   pinning.
6. Bounded delta size and bounded index working set regardless of total data or
   retention window.

### Non-goals (initially)

- Strict `REPEATABLE READ`/`SERIALIZABLE` isolation over the Iceberg base (see
  §7.3 — pg_lake reads the latest Iceberg snapshot; time-travel pinning is a
  later enhancement).
- Continuous aggregates / automatic rollups (natural follow-on, not v1).
- Cross-tier uniqueness enforcement (see §6.4).
- Arbitrary schema; v1 targets append-mostly time-series with a logical key.

---

## 3. Architecture overview

```
                    ┌──────────────────────────────────────────┐
   INSERT/UPDATE/   │            parent relation                │   SELECT / aggregate
   DELETE  ───────► │   (merge-on-read: delta overlays base)    │ ◄────────────────
                    └───────────────┬───────────────┬───────────┘
                                    │               │
                         overlay (by key)      base (pruned)
                                    │               │
                        ┌───────────▼──────┐  ┌─────▼───────────────────────┐
                        │  DELTA (heap)    │  │  BASE (Iceberg via pg_lake)  │
                        │  recent writes   │  │  ~complete, internally       │
                        │  timed indexes   │  │  partitioned days(ts),       │
                        │  range part. ts  │  │  scanned by DuckDB           │
                        └───────────▲──────┘  └─────▲───────────────────────┘
                                    │               │
                                    └──── flush ─────┘   (background worker:
                                     merge-on-read into    move committed delta
                                     Iceberg, atomically   rows into base, then
                                     remove from delta)    delete from delta)
```

- The **parent** is initially a SQL **view** (for semantics/validation) and
  later a **CustomScan**-backed relation (for efficient per-partition
  reconciliation and aggregate pushdown).
- The **delta** is small because the flusher keeps draining it into the base.
- The **base** is where analytical scans and aggregation get pushed.

---

## 4. Data model

### 4.1 Versioning: `seq`

Every row carries a monotonic `seq bigint`, assigned from a global sequence at
insert time. `seq` is used for two things:

1. **Read reconciliation** — for a given logical key, the row with the highest
   `seq` is the current version.
2. **Flush batch delineation** — a flush processes rows with `seq <= hwm` for a
   *safe* high-water mark `hwm` (§7.2).

`seq` is **not** a read-side watermark; read dedup is by key, not by a global
cut. (That distinguishes model A from model B; see §12.)

### 4.2 Logical key

A time-series row is identified by `key_columns`, e.g. `{series_id, ts}`. The
key is what an update/delete targets and what read reconciliation dedups on.
Model A **requires** a key (§6.4 discusses the keyless case).

### 4.3 Base (Iceberg) schema

```sql
CREATE TABLE <base> (
    <key columns...>,          -- e.g. series_id bigint, ts timestamptz
    <value columns...>,
    seq bigint NOT NULL        -- version of the row currently in the base
) USING iceberg PARTITIONED BY (days(<time_column>));
```

The base carries no `deleted` column: the flusher applies deletes physically
(removes the base row), so a base row is always a live, current-as-of-last-flush
version.

### 4.4 Delta (heap) schema

```sql
CREATE TABLE <delta> (
    <key columns...>,
    <value columns...>,
    seq     bigint  NOT NULL,
    deleted boolean NOT NULL DEFAULT false   -- tombstone
) PARTITION BY RANGE (<time_column>);
-- timed secondary indexes are partitioned indexes on the parent (§6.3)
```

The delta holds inserts, new versions of updated rows, and tombstones for
deletes, all tagged with `seq`.

### 4.5 Catalog

Defined in `pg_lake_timeseries--3.4.sql`:

- `timeseries.tables` — one row per managed table (parent, time column, key
  columns, partition interval, hot retention, precreate-ahead, base table,
  cluster columns, version sequence, enabled flag).
- `timeseries.delta_partitions` — the set of delta partitions that currently
  hold unflushed rows ("dirty"). Drives the CustomScan's dirty/clean split
  (§5.3). A partition is removed from this table when a flush empties it.

Both are marked with `pg_extension_config_dump` so they are dumped/restored with
the database, not treated as extension-owned static data.

---

## 5. Read path

### 5.1 Merge-on-read view (semantics reference)

The correct reconciliation, expressed as a view (this is the *specification* of
the result; the CustomScan must produce the same rows):

```sql
CREATE VIEW <parent> AS
-- base rows NOT overridden by any delta row for the same key
SELECT <cols>
FROM   <base> c
WHERE  NOT EXISTS (SELECT 1 FROM <delta> d
                   WHERE d.series_id = c.series_id AND d.ts = c.ts)
UNION ALL
-- current (max-seq) non-tombstone delta version per key
SELECT <cols>
FROM  (SELECT DISTINCT ON (series_id, ts) <cols>, deleted
       FROM   <delta>
       ORDER  BY series_id, ts, seq DESC) d
WHERE  NOT d.deleted;
```

Notes:

- The **anti-join form** scans the base once (with pushdown) and hashes the tiny
  delta for the `NOT EXISTS` anti-semijoin — it does **not** sort the whole
  base. The simpler `DISTINCT ON (key) ORDER BY key, seq DESC` over
  `base UNION ALL delta` is also correct but sorts everything; acceptable only
  as a correctness oracle in tests.
- Tombstones: a delete lives in the delta as a `deleted = true` row. The
  `NOT EXISTS` suppresses the base row; the outer `WHERE NOT deleted` drops the
  tombstone itself, so the key correctly disappears.

### 5.2 Pruning & pushdown

For a query with predicates on the parent:

- **Base branch** — predicates on the time/key columns push into the Iceberg
  foreign scan; DuckDB prunes partitions/files via manifest min/max.
- **Delta branch** — predicates prune delta partitions (and use the timed
  indexes).

**Pushdown safety (important):** quals on **key columns** (`series_id`, `ts`)
push *into* the `DISTINCT ON` subquery — PostgreSQL allows this because they are
the DISTINCT ON columns. Quals on **non-key columns** (`value > 5`) do **not**
push below the `DISTINCT ON` (`check_output_expressions` marks non-DISTINCT
output columns unsafe). This is exactly what correctness requires: a non-key
filter must apply *after* the current version is chosen, or a filtered-out
newest version would let a stale older version surface. Verify with `EXPLAIN`
that `value` predicates stay above the dedup while `ts`/`series_id` predicates
sink into both branches.

### 5.3 CustomScan: per-partition aggregate decision

The view always reconciles the whole scanned range, so it cannot push
aggregation to DuckDB. The CustomScan makes reconciliation *pay-per-dirty-
partition*. At plan time it reads `timeseries.delta_partitions` and splits the
time-pruned partition set:

- **Clean partitions** (no unflushed delta) — data is entirely current in the
  base. Push the scan *and* any `GROUP BY`/aggregate straight to the Iceberg
  foreign scan. This is the common case for anything older than the flush lag.
- **Dirty partitions** (delta overlaps) — reconcile (anti-join base + delta),
  then aggregate in PostgreSQL. Optimization: synthesize the delta's key set as
  a `NOT IN` filter pushed into the Iceberg scan so DuckDB drops superseded base
  rows itself.

Resulting plan for `SELECT time_bucket, avg(value) ... GROUP BY 1`:

```
Finalize Aggregate
  Append
    Foreign Aggregate          -- clean partitions: GROUP BY pushed to DuckDB
    GroupAggregate             -- dirty partitions (few, recent)
      Merge (anti-join base ⟕ delta)
```

A partition flips back to *clean* the instant a flush empties its delta, so the
set that loses pushdown is bounded by the flush lag, not by N.

**Dependency / open question:** whether pg_lake pushes *partial* (2-stage)
aggregates or only full grouping (the code survey found `add_foreign_grouping_
paths`, i.e. grouping pushdown; the partial split is unconfirmed). If full-only,
combine by full-aggregating each branch and re-aggregating on top — valid for
decomposable aggregates (`sum`, `count`, `min`, `max`, `avg` = `sum`/`count`)
and needs explicit handling for holistic ones (`count(distinct)`, percentiles).
See §11.

---

## 6. Write path

### 6.1 Ingest routing

Inserts/updates/deletes to the parent are routed to the **delta**:

- INSERT → delta row, `seq` from the sequence, `deleted = false`.
- UPDATE → new delta row for the key with a higher `seq` (the read path picks
  the max-`seq` version; the base version is suppressed by the anti-join).
- DELETE → delta tombstone row (`deleted = true`) with a higher `seq`.

Routing is done by the parent relation's `ModifyTable` path (INSTEAD OF triggers
on the view for the skeleton; the CustomScan/`ModifyTable` integration later).

### 6.2 Frontier & maintenance (no DDL on the insert path)

Native PostgreSQL does **not** auto-create partitions; a non-matching insert
errors unless a `DEFAULT` partition exists. So the delta is managed as:

1. **Pre-create ahead** — the background worker keeps `precreate_ahead`
   partitions ready in front of `now()`. Almost every insert hits an existing
   partition. Partition creation happens in the worker, never on the insert
   path.
2. **DEFAULT catch-all** — a `DEFAULT` partition absorbs any row that misses
   (clock skew, worker lag, late data), so inserts never fail.
3. **Drain DEFAULT** — the worker empties DEFAULT each pass: in-window rows are
   moved into a freshly-created/attached partition (delete-from-default →
   attach → the attach's default-scan then finds no conflict); out-of-window
   (old late) rows are flushed straight into the base.

Keeping DEFAULT small is essential: attaching a partition while DEFAULT holds
rows forces a scan of DEFAULT under a strong lock. Drain before pre-create.

### 6.3 Timed indexes

Secondary indexes (e.g. on `(series_id, ts)`) are declared as **partitioned
indexes** on the delta parent. They propagate to each partition and are dropped
with the partition. Because the delta only spans the recent window, the index
working set is bounded and cache-resident, insert-time maintenance is cheap, and
retiring a partition reclaims its index instantly (no vacuum of index tuples).
This is the reason the delta is chunked rather than a single heap.

### 6.4 Keys, uniqueness, keyless case

- A **logical key** is required for model A (reconciliation dedups by key).
  `{series_id, ts}` is typical; a synthetic ingest id works too.
- **Uniqueness is not enforced across tiers.** A per-partition unique index only
  enforces uniqueness *within* a delta partition, never against the base. For
  append-only or last-writer-wins semantics this is fine; strict cross-tier
  uniqueness is out of scope.
- **Keyless append-only** workloads can't dedup by key. They must either use
  model B (a version-seq watermark split; see §12) or accept snapshot pinning.
  Model A assumes a key.

---

## 7. Flush (keeping the base fresh)

### 7.1 The flush operation

Runs as one transaction so the Iceberg commit and the delta cleanup are atomic
(pg_lake ties the Iceberg metadata commit to the Postgres transaction commit):

```sql
-- hwm = timeseries.safe_hwm(parent)  (see §7.2)
CREATE TEMP TABLE batch ON COMMIT DROP AS
  SELECT DISTINCT ON (series_id, ts) series_id, ts, value, seq, deleted
  FROM   <delta> WHERE seq <= hwm
  ORDER  BY series_id, ts, seq DESC;             -- current version per key

-- supersede base for every touched key (merge-on-read: Iceberg writes deletes)
DELETE FROM <base>
WHERE (series_id, ts) IN (SELECT series_id, ts FROM batch);

-- (re)insert current, non-deleted versions, clustered for tight file min/max
INSERT INTO <base> (series_id, ts, value, seq)
SELECT series_id, ts, value, seq FROM batch WHERE NOT deleted
ORDER  BY <cluster_columns>;

-- remove exactly the flushed rows; rows with seq > hwm remain in the delta
DELETE FROM <delta> WHERE seq <= hwm;

-- update the dirty-partition catalog for now-empty partitions
```

If the FDW cannot take a semi-join `DELETE ... WHERE (k) IN (subquery)`,
materialize the key set into an array and use `= ANY`, or delete per touched
partition.

### 7.2 Safe high-water mark

`seq` is assigned at insert time, but transactions commit out of order. If the
flush naively used `max(seq)` it could delete a delta row whose lower-`seq`
sibling for the same key was inserted by a still-in-flight transaction, or skip
a lower `seq` that commits *after* the flush's snapshot — losing data.

`timeseries.safe_hwm()` returns the largest `seq` such that **every** `seq` `<=`
it is committed and visible — computed LSN-style as `(oldest in-flight seq − 1)`
using a small tracking structure (or by inspecting the sequence vs. the oldest
active snapshot). The flush only processes `seq <= safe_hwm`; anything newer or
still in flight stays in the delta for the next pass.

### 7.3 Compaction & retention

- **Compaction** — frequent small flushes create many small Iceberg files.
  Schedule pg_lake's existing `VACUUM`-based compaction
  (`VacuumCompactDataFiles`) on the base.
- **Retention** — dropping data past a retention horizon is a metadata-level
  Iceberg partition delete on the base (cheap), plus snapshot expiry, which
  pg_lake already supports.

---

## 8. Correctness

### 8.1 Exactly-once under concurrent flush (no snapshot pinning)

pg_lake reads the **latest** Iceberg snapshot (it does not pin to the reader's
Postgres snapshot). Model A is nonetheless exactly-once because reconciliation
is **logical** (by key), not physical.

Take key `k`: base version `seq_b`, delta update `seq_d > seq_b`; flush `F`
commits at time `C`, moving `k` into the base and deleting it from the delta.

| reader snapshot `S` | delta (at `S`) | base (latest) | base branch (`NOT EXISTS delta.k`) | delta branch | result |
|---|---|---|---|---|---|
| `S` before `C` | has `k`@`seq_d` | `seq_b` or (post-`C`) `seq_d` | **excluded** (delta has `k`) | `seq_d` | `k` once, `seq_d` |
| `S` after `C`  | no `k`         | `seq_d`                      | **included**                      | —            | `k` once, `seq_d` |

Invariant: **a base row is surfaced only when the delta lacks that key at the
reader's snapshot**, and delta contents are MVCC-consistent with that snapshot.
The transient "row physically in both stores" during a flush cannot
double-count, because the anti-join drops the base copy whenever the delta still
holds the key. Deletes are symmetric (the tombstone suppresses the base via
`NOT EXISTS` until the flush removes both atomically).

### 8.2 No lost writes

Guaranteed by §7.2: the flush only removes `seq <= safe_hwm`, and `safe_hwm`
never advances past an uncommitted/in-flight `seq`. A newer version (`seq >
hwm`) for a key already partially flushed stays in the delta and wins on the
next read.

### 8.3 Isolation levels

- **READ COMMITTED** — fully correct. Each statement sees a fresh snapshot;
  "latest Iceberg" ≈ current, consistent with RC semantics.
- **REPEATABLE READ / SERIALIZABLE** — one anomaly: because the base is read at
  latest, a key inserted *and* flushed entirely after the reader's snapshot can
  surface from the base (the reader's delta view never had it). This yields
  "see slightly-newer-than-snapshot" reads on cold data. It **never** causes
  duplicates or lost rows. If strict RR/SERIALIZABLE over the base is required,
  add Iceberg **snapshot pinning**: store, per flush, the resulting
  `iceberg_snapshot_id` in a Postgres row; a reader reads the max id visible at
  its snapshot and scans the base `AS OF` that id. pg_lake already has
  `GetIcebergSnapshotViaId`; the missing piece is threading a chosen snapshot id
  through the FDW/pgduck_server scan. This is a deliberate later enhancement, not
  required for model-A correctness.

---

## 9. Background worker

Registered from `_PG_init` (skeleton has a TODO where `RegisterBackgroundWorker`
goes). Every `pg_lake_timeseries.maintenance_naptime` ms it iterates
`timeseries.tables` and, per enabled table, runs `timeseries.maintain()`:

1. Pre-create the delta frontier (§6.2).
2. Drain the DEFAULT partition (§6.2).
3. Flush aged/committed delta into the base (§7).

Each unit (per-partition create, per-flush) is its **own transaction** so
Iceberg commits are independent and locks release between units — hence
`maintain()`/`flush()` are `PROCEDURE`-shaped internally (they `COMMIT` between
units) even though the SQL surface exposes function wrappers.

Concurrency: a single worker per database avoids two maintainers racing the same
table; if multiple workers are ever used, guard per-table with an advisory lock.

---

## 10. Configuration & API

### GUCs (defined in `src/init.c`)

- `pg_lake_timeseries.enable` (bool, default on) — enable background maintenance.
- `pg_lake_timeseries.maintenance_naptime` (ms, default 10000) — interval
  between maintenance passes.

Future: per-table overrides for flush cadence, target file size, retention.

### SQL API (stubs in the skeleton)

- `timeseries.create_table(parent, time_column, key_columns, partition_interval,
  hot_retention, cold_table, precreate_ahead, cluster_columns)` — register a
  table; create delta + timed indexes + base + sequence + parent relation.
- `timeseries.flush(parent, hwm)` — flush committed delta into the base.
- `timeseries.maintain(parent)` — one maintenance pass (frontier + drain +
  flush).
- `timeseries.safe_hwm(parent)` — largest fully-committed `seq` safe to flush.

---

## 11. Open questions & risks

1. **Partial-aggregate pushdown** — does pg_lake push 2-stage partial aggregates
   or only full grouping? Determines whether §5.3's `Finalize/Partial` plan is
   native or whether we re-aggregate on top (fine for decomposable aggregates,
   special-cased for holistic ones). *Gating for the aggregate path.*
2. **Snapshot pinning** — needed only for strict RR/SERIALIZABLE over the base
   (§8.3). Threading a snapshot id through the scan is the main new pg_lake
   capability it would require.
3. **Late-data partition sprawl** — updates scattered across old data times
   create old-dated delta partitions. Mitigation: route out-of-window late rows
   straight to the base at flush (don't keep a partition), or hold a single
   "late" partition. Needs a policy.
4. **Keyless append-only** — not supported by model A; would use model B (§12).
5. **`MERGE`/joined-DELETE support in the FDW** — the flush uses a semi-join
   DELETE against the Iceberg base; confirm the FDW supports it, else use the
   `= ANY` / per-partition fallback (§7.1).
6. **Single-row insert cost into Iceberg** — the flush must be **bulk**; never
   trickle single rows into the base (the FDW has no batch insert and single-row
   inserts are expensive). Ingest goes to the heap delta; only the flusher
   writes the base, in batches.

---

## 12. Alternative considered: model B (watermark split)

Instead of key-based merge-on-read, split *both* branches by an MVCC-read
version watermark `W` on `seq`:

```sql
SELECT * FROM <base>  WHERE seq <= W AND <range>
UNION ALL
SELECT * FROM <delta> WHERE seq >  W AND <range>
```

- **Pros:** no anti-join, no key required (works for keyless append-only);
  exactly-once without pinning (both branches filter on the same MVCC-read `W`).
- **Cons:** does not handle in-place **updates** (a re-inserted key would appear
  in both `seq <= W` base and `seq > W` delta → double count) or deletes without
  extra machinery; needs the same safe-`W` care as §7.2.

Model A subsumes updates/deletes and is the chosen default. Model B is noted as
the right tool for strictly append-only, keyless streams and could be offered as
a per-table mode later.

---

## 13. Phased implementation plan

1. **Semantics first (SQL only).** `create_table` builds delta + base + view +
   INSTEAD OF routing. Implement `flush`/`safe_hwm`/`maintain` as PL/pgSQL.
   Validate reconciliation with the concurrency test harness (§14). No C yet.
2. **Background worker (C).** `RegisterBackgroundWorker` in `_PG_init`; loop
   calling `timeseries.maintain()` per table via SPI.
3. **Frontier hardening.** Pre-create/drain edge cases, DEFAULT management,
   late-data policy.
4. **CustomScan.** Replace the view with a CustomScan that does per-partition
   dirty/clean splitting, delta-key pushdown into the base scan, and the
   aggregate decision (§5.3). Resolve open question #1 first.
5. **Snapshot pinning (optional).** Only if strict RR/SERIALIZABLE over the base
   is required (§8.3).

Each phase is independently shippable and testable.

---

## 14. Testing strategy

pytest suites under `tests/pytests/` (skeleton has a placeholder). Priorities:

1. **Reconciliation correctness** — seed base; apply inserts/updates/one delete
   to the delta; assert the parent returns one row per key with the max-`seq`
   value and tombstoned keys absent.
2. **Concurrency vs. flush** — run a reader (RR txn) across a concurrent
   `flush()`; assert stability within the txn, no duplicate keys, no lost rows.
3. **Stress** — a writer loop (insert/update/delete into delta) + a flush loop +
   a checker asserting `count(*) == count(distinct key)` and every key's value
   matches its max-`seq` write. Catches double-count and the safe-`hwm`
   batch-deletion hazard (a `seq > hwm` write must survive a flush).
4. **Pruning/pushdown** — `EXPLAIN` assertions: `ts`/key predicates reach both
   branches; non-key predicates stay above the dedup; clean partitions push
   aggregation to DuckDB; dirty partitions reconcile then aggregate.
5. **Frontier** — inserts never fail across a partition boundary; DEFAULT stays
   small; late data lands correctly.

---

## 15. Naming & placement

- Extension name: `pg_lake_timeseries` (descriptive, matches
  `pg_lake_iceberg`/`pg_lake_table`/`pg_lake_copy`). `pg_lake_live` was
  considered as a mechanism-oriented alternative.
- Dependencies: `requires = pg_lake_engine, pg_lake_iceberg, pg_lake_table`.
- Placement: an **optional add-on**, like `pg_lake_spatial`/`pg_lake_benchmark`.
  It is **not** installed by `CREATE EXTENSION pg_lake CASCADE`; users opt in
  with `CREATE EXTENSION pg_lake_timeseries CASCADE`.
- Wired into the top-level `Makefile` (`EXTENSION_TARGETS` + module
  declarations) so the standard `*-pg_lake_timeseries` targets work, without
  altering the default `make install` (which builds the `pg_lake` meta only).

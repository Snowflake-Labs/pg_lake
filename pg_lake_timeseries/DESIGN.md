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
- Preferred read execution pushes the *whole* merge — delta included, via a
  snapshot-pinned DuckDB `postgres_scan` — **into DuckDB**, so one columnar plan
  reconciles and aggregates (§5.3–§5.4).

> **Two storage families, and their combination.** §4–§11 detail the
> **delta/overlay** family described above (Iceberg base + small Postgres delta,
> reads merge). §12 describes the **hot-authoritative tiering** family the design
> later converged toward (Postgres owns the full deduplicated indexed last N days;
> Iceberg is a tier/mirror; reads route by tier with no merge), including **Mirror
> mode (C)**, the CDC-fed Iceberg mirror. §13 describes the shape there is actual
> demand for, which is neither family alone: **overlapping** tiers (Postgres to
> `now()`, Iceberg to the last materialisation) with tier routing as the fast path
> and a cross-tier merge for the cases routing cannot cover. §13 is the primary
> target; §4–§11 supply its merge machinery and §12 its tiering machinery.

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
cut. (That distinguishes model A from model B; see §14.)

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

### 5.3 Where the merge runs

Reconciling in PostgreSQL (the §5.1 view) means the base is scanned as a foreign
scan, base rows are shipped up to PostgreSQL, and the anti-join/aggregation run
in PostgreSQL. That is correct but forfeits DuckDB aggregation for any range the
delta touches. The **preferred** execution instead pushes the *entire* merge —
including the delta read — **into DuckDB**, using DuckDB's `postgres_scanner`
(`postgres_scan`), which is already bundled and loaded in `duckdb_pglake`. Then
one columnar plan reconciles and aggregates, returning only final results.

The CustomScan reads `timeseries.delta_partitions` at plan time and splits the
time-pruned partition set:

- **Clean partitions** (no unflushed delta) — emit plain Iceberg DuckDB SQL, no
  `postgres_scan`, no reverse connection. This is the common case for anything
  older than the flush lag.
- **Dirty partitions** (delta overlaps) — emit a single DuckDB query that reads
  the base via `read_parquet(...)` **and** the delta via a snapshot-pinned
  `postgres_scan(...)` (§5.4), does the anti-join + max-`seq` dedup + tombstone
  filter, and aggregates — all in DuckDB.

Emitted DuckDB SQL for `SELECT time_bucket, avg(value) ... GROUP BY 1` over a
dirty range:

```sql
SELECT time_bucket, avg(value)                 -- aggregation runs in DuckDB
FROM (
    SELECT base.*
    FROM   read_parquet([...iceberg files...]) base
    WHERE  NOT EXISTS (SELECT 1 FROM delta d
                       WHERE d.series_id = base.series_id AND d.ts = base.ts)
    UNION ALL
    SELECT * EXCLUDE (deleted, seq)
    FROM  (SELECT DISTINCT ON (series_id, ts) *
           FROM postgres_scan_pushdown('<loopback dsn>', 'public', 'metrics_delta',
                                       snapshot => '<exported id>')
           ORDER BY series_id, ts, seq DESC) d
    WHERE NOT d.deleted
) merged
GROUP BY 1;
```

`postgres_scan` supports filter/projection pushdown
(`PostgresScanFunctionFilterPushdown`), so the query's `ts`/key predicates push
to the PostgreSQL side and only the tiny indexed delta slice is read; DuckDB
supports `DISTINCT ON` and anti-joins natively. A partition flips back to
*clean* the instant a flush empties its delta, so the set that pays the reverse
connection is bounded by the flush lag, not by N.

This collapses the earlier "clean → push aggregate / dirty → aggregate-in-PG"
split into "clean → plain Iceberg SQL / dirty → Iceberg + `postgres_scan`,
merged and aggregated in DuckDB", and largely **removes the dependency on
partial-aggregate pushdown** (§11#1): DuckDB performs the whole aggregation over
the merged result, so there is no cross-engine partial/finalize split to rely on.

**Fallback:** the §5.1 PostgreSQL-side reconciliation remains valid where a
reverse connection is undesirable (e.g. no suitable role, or `postgres_scan`
overhead not justified for a given query); the CustomScan can choose it per
query.

### 5.4 Snapshot-consistent `postgres_scan` (reading the delta inside DuckDB)

Reading the delta inside DuckDB is only correct if the delta read uses the
**same snapshot** as the base file resolution. pg_lake resolves the base file
set at the transaction snapshot `S` (`CreatePgLakeScanSnapshot` →
`GetTransactionSnapshot()`), but pgduck_server executes *after* that, so a naive
`postgres_scan` at "latest" would read the delta at a later instant `S'`. A
flush committing between the base pin and `S'` moves a row out of the delta that
the base snapshot did not yet contain → the row is in **neither** → **lost row**.

The fix is already present: `duckdb_pglake` carries a patch
(`patches/duckdb-postgres/snapshot.patch`) adding a `snapshot => '<id>'` named
parameter to `postgres_scan`/`postgres_query`. When set, the scanner's
back-connection adopts that snapshot (`PostgresScanConnect(con, snapshot)` →
`SET TRANSACTION SNAPSHOT`) instead of reading latest.

Flow per query touching a dirty range:

1. The driving backend calls `pg_export_snapshot()` inside the query's
   transaction and threads the id into the emitted DuckDB SQL.
2. The backend stays blocked awaiting pgduck_server, so the exporting
   transaction remains open and the snapshot importable.
3. `postgres_scan(..., snapshot => id)` opens the reverse connection and
   `SET TRANSACTION SNAPSHOT '<id>'`, reading the delta at exactly `S`.

Result: base **and** delta are both read at `S`. Dedup is by key, so the merge
is exactly-once *and* fully snapshot-consistent — it does not even exhibit the
REPEATABLE READ "see-slightly-newer" anomaly of the PostgreSQL-side path (§8.3).
The `snapshot` parameter is therefore **mandatory**, not an optimization.

**Reverse-connection topology & auth.** Today pgduck_server only *receives*
connections (PostgreSQL → pgduck_server over `/tmp:5332`); here pgduck_server's
`postgres_scanner` connects *back* to PostgreSQL. This is a new surface:

- The `postgres_scan` DSN must authenticate as a role that can read the delta.
  Preferred: a dedicated, read-only role over the local unix socket using peer
  authentication — **no password in the DSN**. Avoid embedding credentials in
  generated SQL.
- The delta is a physical heap; a `SELECT`-only grant to that role on the delta
  tables is sufficient (the reverse connection never touches the base).
- **Connection reuse**: `postgres_attach` / a persistent ATTACH to pool the
  reverse connection if query rates are high. Only dirty-range queries open it.

**Caveat:** correctness depends on the exported snapshot staying importable,
which holds because the driving query *synchronously* blocks on pgduck_server.
If the pgduck_server call is ever made asynchronous, this invariant must be
re-established (e.g. hold the exporting transaction explicitly).

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
  model B (a version-seq watermark split; see §14) or accept snapshot pinning.
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
- **REPEATABLE READ / SERIALIZABLE** — for the **PostgreSQL-side** reconciliation
  (§5.1) there is one anomaly: because the base is read at latest, a key inserted
  *and* flushed entirely after the reader's snapshot can surface from the base
  (the reader's delta view never had it). This yields "see
  slightly-newer-than-snapshot" reads on cold data. It **never** causes
  duplicates or lost rows.
- **The preferred DuckDB-side merge (§5.3–§5.4) does not have this anomaly**:
  the base file set is resolved at the transaction snapshot `S`, and the delta is
  read at the same `S` via the exported-snapshot `postgres_scan`, so both stores
  are consistent at `S`. This is *stronger* isolation than the PostgreSQL-side
  path, achieved without any Iceberg time-travel.
- If strict RR/SERIALIZABLE is required for the **PostgreSQL-side fallback** too,
  add Iceberg **snapshot pinning**: store, per flush, the resulting
  `iceberg_snapshot_id` in a Postgres row; a reader reads the max id visible at
  its snapshot and scans the base `AS OF` that id. pg_lake already has
  `GetIcebergSnapshotViaId`; the missing piece is threading a chosen snapshot id
  through the FDW scan. Deliberate later enhancement, not required for model-A
  correctness.

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

1. **Partial-aggregate pushdown** — largely **moot** under the preferred
   DuckDB-side merge (§5.3): DuckDB aggregates the merged result, so there is no
   cross-engine partial/finalize split. It only matters for the PostgreSQL-side
   fallback (§5.1), where dirty ranges would re-aggregate on top (fine for
   decomposable aggregates, special-cased for holistic ones).
2. **Reverse-connection auth** — the DuckDB-side merge has pgduck_server connect
   *back* to PostgreSQL via `postgres_scan`. Needs a read-only role reachable
   over the local unix socket (peer auth, no password in the DSN) with `SELECT`
   on the delta tables. New surface; see §5.4.
3. **Reverse-connection overhead / pooling** — only dirty-range queries open the
   reverse connection; use `postgres_attach` / persistent ATTACH to pool it under
   high query rates. Clean-range queries never open it.
4. **Async pgduck calls** — DuckDB-side correctness relies on the driving query
   blocking synchronously so the exported snapshot stays importable (§5.4). Any
   move to async pgduck execution must re-establish this.
5. **Snapshot pinning (base)** — needed only for strict RR/SERIALIZABLE on the
   PostgreSQL-side fallback (§8.3); the DuckDB-side merge is already
   snapshot-consistent. Threading a snapshot id through the FDW scan is the new
   capability it would require.
6. **Late-data partition sprawl** — updates scattered across old data times
   create old-dated delta partitions. Mitigation: route out-of-window late rows
   straight to the base at flush (don't keep a partition), or hold a single
   "late" partition. Needs a policy.
7. **Keyless append-only** — not supported by model A; would use model B (§14).
8. **`MERGE`/joined-DELETE support in the FDW** — the flush uses a semi-join
   DELETE against the Iceberg base; confirm the FDW supports it, else use the
   `= ANY` / per-partition fallback (§7.1).
9. **Single-row insert cost into Iceberg** — the flush must be **bulk**; never
   trickle single rows into the base (the FDW has no batch insert and single-row
   inserts are expensive). Ingest goes to the heap delta; only the flusher
   writes the base, in batches.
10. **Equality-delete write support** — pg_lake writes only *position* deletes;
    equality deletes (content=2) are recognized on read but have no write path.
    Efficient high-frequency Mirror mode (C) apply-by-key would want
    equality-delete writes (§12.3). Gating capability for that path; scope
    separately in pg_lake_iceberg.
11. **Equality deletes vs. overwrite-from-Postgres** — undecided whether Mirror
    mode should use pure Iceberg equality deletes (read-amplifying, needs new
    write support) or lean on overwrite-from-Postgres resets (§12.3), which the
    authoritative hot store uniquely enables.
12. **Heap relations in the whole-query pushdown** — the overlapping-tier shape
    (§13) needs `pg_lake_table`'s existing full-query CustomScan to admit heap
    RTEs, deparsed as DuckDB `postgres_scan_pushdown`/`postgres_query` calls.
    This is the single largest enabler in the design: it unlocks cross-tier
    vectorised reads (§13.5), the fresh-tail overlay (§12.4) and a bulk
    Postgres → Iceberg seal (§13.8). Its own open questions are in §13.9.
13. **`snapshot` is ignored on the `postgres_query` path** — the in-tree
    `snapshot.patch` accepts the parameter but only applies it to DSN-opened
    connections, not to the attached-catalog connection `postgres_query` uses
    (§13.6). A latent correctness bug today and a prerequisite for the
    index-friendly hot-tier read.

---

## 12. Hot-authoritative tiering & sync modes (incl. Mirror mode C)

§4–§11 describe the **delta/overlay** family (Iceberg is the base, Postgres a
small change-delta, reads *merge*). This section describes the second family the
design has converged toward: **hot-authoritative tiering**, where Postgres owns
the full recent window and Iceberg is a tier/mirror rather than a base to
overlay.

### 12.1 The model

- **Postgres owns the full, deduplicated, indexed last N days** and is the
  source of truth for that window. Ingest is an **upsert** on the key into the
  hot partitions (unique index → dedup + fast lookups + secondary indexes).
- **Iceberg holds data older than N days** and — depending on sync mode — a copy
  of the recent data too.
- **Reads route by tier; they do not merge**: recent → Postgres (indexed,
  deduped, fresh); old → Iceberg (columnar); spanning → `UNION ALL` on the time
  boundary. The tiers are **disjoint by time**, so there is no cross-tier dedup.
- **Mutation contract:** all inserts/updates/deletes happen within the N-day
  window in Postgres; once a partition is sealed to Iceberg it is immutable.
  Late data older than N days is rejected/buffered or applied as a rare Iceberg
  correction.

This eliminates the read-side merge machinery of §5 (no anti-join, no DISTINCT
ON, no `postgres_scan` overlay) for the primary path — that machinery reappears
only as an *optional* fresh-tail overlay in Mirror mode (§12.4).

### 12.2 Sync modes

How recent data reaches Iceberg, in increasing complexity. **"Delta tables"
exist only in mode C, and even there as a *write-side* change stream, not a read
overlay.**

- **(A) Sync-at-seal — no delta tables.** Postgres owns the entire mutable
  window. When a partition ages out, bulk-write its current (deduped) contents to
  Iceberg once, then `DROP` it from Postgres. Iceberg lags by the active window
  (fine — recent reads hit Postgres). Simplest; no delete files, no merge.
- **(B) Partition-overwrite — no delta tables.** Periodically overwrite the
  *changed* hot partitions' files in Iceberg from Postgres's current state. Keeps
  Iceberg fresh with no delete files; cost is write amplification confined to
  actively-changing partitions.
- **(C) Mirror mode — a CDC change stream feeds Iceberg.** Detailed below.

### 12.3 Mirror mode (C)

A **dual-store CDC mirror**: Postgres is the authoritative indexed hot window; a
change stream from the hot tables is **continuously applied into Iceberg**, so
Iceberg is a near-fresh columnar mirror of recent data plus the historical
archive. Reads still route by tier — the change stream is a **write-side sync**,
not a read overlay.

**Change capture.**

- **Logical decoding (preferred).** Tail the hot tables via a replication slot,
  translate INSERT/UPDATE/DELETE to Iceberg operations, track an applied-LSN
  watermark. No physical delta table, low write overhead, LSN-ordered. This is
  the same shape as pg_lake's Postgres mirroring (change batches + APPLY + LSN
  tracking) and should reuse that experience.
- **Change-log table (simpler prototype).** An AFTER-trigger or the upsert site
  writes `(key, op, values, seq)` into a table; apply reads it in batches.
  Transactional and simple, but per-write amplification; worse at high rate.

**Applying to Iceberg.** INSERT → data-file append; DELETE → delete of the key;
UPDATE → delete + insert. The delete mechanism is the open decision (pg_lake
writes *position* deletes today; equality deletes are read-recognized but have no
write path):

1. **Position deletes (works today).** An apply batch issues
   `DELETE FROM <iceberg> WHERE key IN (<changed/deleted keys>)`; pg_lake locates
   the rows (a scan) and writes position deletes, then inserts new versions.
   Correct now; each apply pays a locate-scan proportional to touched data. Fine
   at moderate rates.
2. **Overwrite-from-Postgres reset (preferred for update-heavy partitions).**
   Because Postgres holds the clean deduped state, rematerialize a whole hot
   partition from Postgres (mode B) instead of accumulating deletes — a "reset
   button" that stateless CDC sinks (Flink/Debezium) lack. Blends B and C:
   CDC-append for freshness on insert-dominated partitions; overwrite-from-PG to
   collapse delete accumulation on update-heavy ones. **Seal = one final
   overwrite → a pristine, delete-file-free cold partition, then drop from PG.**
3. **Equality deletes (under consideration, not decided).** Write key-only delete
   files (content=2) and defer the locate to read/compaction — the classic
   streaming-upsert-to-Iceberg pattern. Cheapest apply, but adds read
   amplification (every scan anti-joins data against delete files until
   compaction) and requires new equality-delete write support (§11#10). Whether
   pure Iceberg equality deletes are the right tool — versus leaning on
   overwrite-from-PG (#2), which the authoritative hot store uniquely enables — is
   open (§11#11).

Each apply batch is one Iceberg commit; batch size trades freshness against
snapshot/file churn. Apply is idempotent via the applied-LSN watermark. pg_lake
already flips a data file to copy-on-write past a delete threshold, which bounds
read amplification automatically.

### 12.4 Reads & consistency in Mirror mode

- Fresh point lookups / dedup / read-your-writes → **Postgres** (authoritative).
- Columnar recent analytics + external Iceberg engines (Spark/Trino/Snowflake) →
  **Iceberg**, lagging by the apply interval; each Iceberg snapshot is internally
  consistent as of an applied LSN. Applying at transaction boundaries plus an
  LSN→snapshot map gives external readers transactionally consistent points.
- Old data → Iceberg.
- *Optional* strict-fresh columnar path = the Iceberg mirror **plus** a
  `postgres_scan` overlay of only the *un-applied tail* since the last apply
  (§5.3–§5.4) — the read overlay reappears, but for a tiny tail, and only when
  columnar *and* fully-fresh recent analytics are both required.
- Route recent from exactly one store (the time boundary) to avoid
  double-counting data that physically exists in both.

### 12.5 Costs & when Mirror mode earns its keep

Costs: two physical copies of recent data; delete-amplification + compaction
load; a real CDC pipeline (slot lifecycle, apply lag, backpressure, crash
recovery); Iceberg snapshot churn from frequent commits (→ expiry + compaction);
schema evolution fanned out to both stores.

Worth it when external engines need fresh recent Iceberg data, or recent
analytics needs columnar speed at scale, or the recent window must be durable in
the lake — *and* the change rate makes mode B's whole-partition overwrite too
amplifying to be the only sync. Otherwise A or B is simpler.

---

## 13. Overlapping tiers with cross-tier merge (the demanded shape)

§12 routes reads by tier on the assumption that the tiers are **disjoint by
time**. The shape there is actual demand for is not disjoint:

- Postgres holds the last **N** days (say 7) **up to `now()`** — indexed,
  deduplicated, mutable.
- Iceberg holds the last **M** days (say 365) **up to the last materialisation**
  (say last night), so it also holds a lagging copy of most of the hot window.
- The database picks the right store per query, and that choice has to keep
  working when updates and upserts land in Postgres.

The tiers therefore **overlap physically** while still owing a single logical
answer. This section reconciles §5 (where a merge executes) with §12 (tiering):
tiering is the fast path, and the §5 merge machinery services exactly the cases
tiering cannot route around.

### 13.1 Authority is a stored boundary, not `now() - N days`

Define, per table, an **authority boundary** `B`: Postgres is authoritative for
`ts >= B`, Iceberg is authoritative for `ts < B`. Both stores may physically hold
rows on either side of `B`; readers never consult the non-authoritative copy.

`B` is a stored value updated transactionally, **not** an expression over `now()`,
because the two events that would otherwise define it — Iceberg gaining the data
and Postgres dropping it — are separate and independently fallible. `B` advances
only via a **seal** that has proven Iceberg completeness below the new value, and
hot-tier retention only ever deletes rows below `B`.

`B` should be **aligned to the partition granularity** of both stores. An
unaligned `B` splits one physical partition across the boundary, which is the one
easily-avoidable reason to merge.

### 13.2 The lagging Iceberg copy of the hot window is not a correctness problem

With `B` in place, nothing reads Iceberg's copy of the last N days. That removes
the invalidation problem for the common case: an upsert into the hot window
invalidates only data nobody reads, so **no per-partition delta and no
invalidation bookkeeping is needed above `B`**. The stale copy is simply
overwritten when the partition seals.

Invalidation bookkeeping is needed only for mutations **below** `B` (§13.3), and
separately if external Iceberg engines are promised freshness for the hot window,
which is Mirror mode (§12.3), not this section.

### 13.3 Where a cross-tier merge is genuinely required

Four cases, in decreasing order of how easily they are designed away:

1. **A partition straddling `B`** — removed by aligning `B` (§13.1).
2. **Late data for `ts < B`** — the row belongs to a sealed partition. Either
   reject it, or accept it into a delta for that partition and merge on read
   until a repair folds it in.
3. **Update or delete of a row with `ts < B`** — the same, and unavoidable if the
   table is genuinely mutable across its whole retention. Rewriting an Iceberg
   partition synchronously inside the user's transaction is not viable, so the
   write lands in a delta and reads merge until repair.
4. **Fresh columnar reads of the hot window** — an external-engine requirement,
   answered by Mirror mode (§12.3), or internally by reading the hot tier from
   DuckDB (§13.6) rather than by a merge.

So the merge is confined to **cold partitions mutated since they sealed**. For
time-series that set is normally small and shrinks as repair runs, which is what
makes the expensive path affordable: the fast path (single tier, no reverse
connection) covers everything else.

### 13.4 Per-partition authority

Track state per (table, partition) rather than one boundary per table; `B` is
then derived, and the late-data and straddle cases have somewhere to live.

| state | authoritative | read plan |
| --- | --- | --- |
| `HOT` | Postgres | Postgres only, indexed |
| `SEALING` | Postgres | Postgres only (Iceberg write in flight) |
| `COLD_CLEAN` | Iceberg | Iceberg only, no reverse connection |
| `COLD_DIRTY` | Iceberg + delta | merge (§13.5–§13.6), until repaired |

Transitions: `HOT → SEALING → COLD_CLEAN` on seal, `COLD_CLEAN → COLD_DIRTY` when
a mutation lands below `B`, `COLD_DIRTY → COLD_CLEAN` on repair (§13.8). Only
`COLD_DIRTY` pays for a merge, and only for the partitions in that state — which
is the point of tracking state per partition instead of per table.

This subsumes `timeseries.delta_partitions` (§4.5): that catalog is this one, with
`HOT` and `COLD_DIRTY` collapsed into a single "dirty" notion.

### 13.5 The optimizer problem, and what the tree can do today

The concern is exact: the obvious ways of presenting two stores as one relation
pull Iceberg rows into Postgres and lose DuckDB's vectorised execution. Three
options, assessed against the current code.

**Option 1 — partitioned parent or `UNION ALL` view (works today, insufficient).**
Range-partition a parent by time with heap children for the hot window and the
pg_lake Iceberg table as the cold child. Partition pruning then does the tier
routing for free, and a query confined to one tier plans well: with a single
surviving foreign child, `postgresGetForeignUpperPaths` /
`add_foreign_grouping_paths` (`pg_lake_table/src/fdw/pg_lake_table.c`) push
grouping, ordering and `LIMIT` into DuckDB.

Cross-tier queries lose that, for two independent reasons:

- `add_foreign_grouping_paths` asserts
  `extra->patype == PARTITIONWISE_AGGREGATE_NONE || PARTITIONWISE_AGGREGATE_FULL`,
  inherited from `postgres_fdw`. There is no **partial**-aggregate pushdown, so a
  mixed heap+foreign `Append` can never push aggregation to the Iceberg side:
  every qualifying Iceberg row is shipped into Postgres and aggregated there.
- Full partitionwise aggregation additionally requires the partition key to appear
  in the `GROUP BY`. The dominant time-series shape, `GROUP BY time_bucket(...)`
  over a `ts`-partitioned table, does not satisfy that, so even the supported
  `FULL` path does not fire.

**Option 2 — let the existing whole-query CustomScan admit heap relations
(recommended).**
pg_lake already pushes entire `SELECT`s to DuckDB:
`pg_lake_table/src/planner/query_pushdown.c` installs a planner hook that deparses
the query with each lake relation replaced by a `pg_lake.read_table(name, id)`
placeholder, and `ReplaceReadTableFunctionCalls`
(`pg_lake_table/src/duckdb/transform_query_to_duckdb.c`) substitutes the real
`read_parquet([...])` call built from the plan-time pruned file list. Inheritance
children are already emitted as `UNION ALL` branches of one DuckDB query.

What blocks the hybrid is one shippability rule in
`ProcessNotShippableExpressionWalker`: an `RTE_RELATION` that is not
`IsAnyLakeForeignTable` is recorded as `NOT_SHIPPABLE_TABLE`, and a parent with
`has_subclass` must pass `AllInheritorsAreLakeTable`. Relaxing exactly that —
deparsing a heap RTE into a DuckDB `postgres_scan_pushdown(...)` /
`postgres_query(...)` call the same way a lake RTE becomes `read_parquet(...)` —
turns a cross-tier query into a single vectorised DuckDB plan: tier union,
anti-join against the delta, dedup by version, aggregation, with only final groups
returned to Postgres.

This is the smallest change with the widest reach. It is the same capability §5.3
already assumes for dirty-range reads, and it fixes the seal write path (§13.8),
which is an independent reason to want it.

**Option 3 — partial-aggregate pushdown in the FDW (complementary, larger).**
Teach the FDW to accept `PARTITIONWISE_AGGREGATE_PARTIAL` and deparse decomposable
aggregates as their components (`sum`/`count` for `avg`), finalising in Postgres.
More general than option 2 — it survives arbitrary Postgres plans, joins against
local tables, and non-shippable expressions — but it is a substantial change to a
`postgres_fdw`-derived file, it does nothing for the non-aggregate parts of a merge
(anti-join, dedup), and holistic aggregates stay local. Worth doing eventually as
the graceful fallback for queries option 2 refuses; not the first move.

### 13.6 Reading the hot tier from DuckDB: what works, and one trap

Verified against the vendored scanner (`duckdb_pglake/duckdb-postgres`, statically
linked into `duckdb_pglake`, so it is available inside pgduck_server):

- **Filter pushdown exists, but only in the pushdown variant.** `postgres_scan`
  registers without `filter_pushdown`; `postgres_scan_pushdown` (the
  `PostgresScanFunctionFilterPushdown` registration) sets both `filter_pushdown`
  and `projection_pushdown`. Filters are DuckDB `TableFilter`s rendered by
  `PostgresFilterPushdown::TransformFilters` — comparisons, `IN`, conjunctions,
  null tests; anything else stays in DuckDB.
- **Pushed-down filters cut transfer, not Postgres-side work.**
  `PostgresInitInternal` builds
  `COPY (SELECT cols FROM schema.table WHERE ctid BETWEEN '(a,0)' AND '(b,0)' AND <filters>) TO STDOUT (FORMAT binary)`.
  The Postgres-side access path is a **ctid range scan**, never an index scan.
  That is the right shape for bulk reads of a hot range (parallelised over page
  ranges, `pg_pages_per_task`) and the wrong shape for the selective indexed
  lookups the hot tier exists to serve.
- **`postgres_query` is the index-friendly path.** It runs our SQL verbatim, so
  Postgres's planner sees the predicate and can use the timed indexes, and it lets
  us dedup or pre-aggregate Postgres-side. It requires an `ATTACH`ed Postgres
  database (which also gives connection pooling, §5.4) and is single-threaded
  (`pages_approx == 0` → one task).
- **Trap: `snapshot` is silently ignored on the `postgres_query` path.** The
  in-tree `patches/duckdb-postgres/snapshot.patch` adds the `snapshot` named
  parameter to `postgres_scan`, `postgres_scan_pushdown` and `postgres_query`, but
  `PostgresInitGlobalState` only applies it in the **DSN** branch, via
  `PostgresScanConnect(con, snapshot)` →
  `BEGIN ... REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT`. On the
  attached-catalog branch — which `postgres_query` always takes, since it resolves
  its first argument through the database manager — the connection is the attached
  transaction's and is left untouched, while any *additional* parallel connection
  from `TryOpenNewConnection` does adopt the snapshot. So today
  `postgres_scan[_pushdown]('<dsn>', ...)` is snapshot-correct, and
  `postgres_query(..., snapshot => ...)` accepts the parameter and reads at the
  wrong snapshot. Using the index-friendly path for consistent cross-tier reads
  needs another hunk in that patch (apply the snapshot to the attached connection,
  or open a dedicated one); the split behaviour is worth fixing regardless.

The planner therefore has a real choice to cost per query: `postgres_scan_pushdown`
for wide hot ranges, `postgres_query` for selective or pre-aggregatable ones,
Postgres-side execution when no reverse connection is available.

### 13.7 Consistency

A cross-tier read pins one instant on both sides: pg_lake resolves the Iceberg
file set at the transaction snapshot `S` (`CreatePgLakeScanSnapshot`), and the
hot-tier read adopts `S` through the exported snapshot id (§5.4). Everything in
§5.4 carries over unchanged, including the caveat that the driving backend must
stay blocked on pgduck_server for the exported snapshot to remain importable.

`COLD_CLEAN` partitions open no reverse connection at all, so an analytical query
over a year of sealed data behaves exactly as it does today.

### 13.8 Write path: seal and repair

- **Seal** (`HOT → COLD_CLEAN`): write the partition's current deduplicated
  contents to Iceberg, overwriting whatever lagging copy is there, then advance
  `B` and drop the Postgres partition. Note that
  `INSERT INTO <iceberg> SELECT ... FROM <heap>` is **not** pushed down today: the
  target passes `IsPushdownableInsertSelectQuery`, but the heap source fails the
  same shippability rule as in §13.5, so the insert falls back to the FDW's
  row-at-a-time path (§11#9). Option 2 fixes this too — DuckDB reads the heap over
  `postgres_scan` and writes Parquet directly — which is the second, independent
  argument for it. Until then, seal via `COPY`/staged Parquet rather than
  `INSERT ... SELECT`.
- **Repair** (`COLD_DIRTY → COLD_CLEAN`): rematerialise the whole affected
  partition from Iceberg plus its delta. There is no partition-overwrite primitive
  today; partitioned *writes* exist (`GetPartitionByExpressionsForRelation`, DuckDB
  `PARTITION_BY`, gated behind `pg_lake_table.enable_partitioned_write_pushdown`,
  default off), so a repair is either a range `DELETE` (position deletes) plus a
  partitioned insert, or a full rematerialisation. Rematerialisation is preferable:
  it leaves no delete files behind.

### 13.9 Open questions

1. **Cost model for the read choice** — `postgres_scan_pushdown` vs
   `postgres_query` vs Postgres-side, per query. Needs selectivity and row-count
   estimates for the hot slice; the FDW's `estimate_path_cost_size` covers only
   the Iceberg side.
2. **Upsert key** — hot-tier dedup by unique index requires a key (§6.4). Keyless
   append-only tables can tier (§12 mode A) but cannot upsert.
3. **Boundary vs. retention** — `B` and the Iceberg horizon `M` are independent;
   decide what reads for `ts < now() - M` do (error, empty, or a third archive
   tier).
4. **Late-data policy below `B`** — reject, or accept into a delta and merge until
   repair (§11#6).
5. **Snapshot on the `ATTACH` path** — §13.6; prerequisite for consistent
   index-friendly hot-tier reads (§11#13).
6. **Reverse-connection auth** — unchanged from §11#2: read-only role, unix
   socket, peer auth, no password in generated SQL.
7. **Fallback quality** — when option 2's shippability check refuses a query, the
   plan silently reverts to option 1's row-shipping behaviour. Whether that is
   acceptable or needs a warning/`EXPLAIN` signal is open.

---

## 14. Alternative considered: model B (watermark split)

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

Model A subsumes updates/deletes and is the chosen default for the overlay
family. Model B is noted as the right tool for strictly append-only, keyless
streams and could be offered as a per-table mode later.

---

## 15. Phased implementation plan

1. **Semantics first (SQL only).** `create_table` builds delta + base + view +
   INSTEAD OF routing. Implement `flush`/`safe_hwm`/`maintain` as PL/pgSQL.
   Validate reconciliation with the concurrency test harness (§16). No C yet.
2. **Background worker (C).** `RegisterBackgroundWorker` in `_PG_init`; loop
   calling `timeseries.maintain()` per table via SPI.
3. **Frontier hardening.** Pre-create/drain edge cases, DEFAULT management,
   late-data policy.
4. **CustomScan + DuckDB-side merge.** Replace the view with a CustomScan that
   does clean/dirty splitting (§5.3): clean ranges emit plain Iceberg SQL; dirty
   ranges emit one DuckDB query merging `read_parquet` with a snapshot-pinned
   `postgres_scan` of the delta (§5.4). Requires: exporting `pg_export_snapshot()`
   from the driving backend and threading the id into the emitted SQL; a
   read-only reverse-connection role (peer auth); and choosing the PostgreSQL-side
   fallback (§5.1) when a reverse connection isn't available. The DuckDB-side
   merge removes the partial-aggregate-pushdown dependency (§11#1).
5. **Snapshot pinning (optional).** Only if strict RR/SERIALIZABLE is required
   for the PostgreSQL-side fallback (§8.3); the DuckDB-side merge is already
   snapshot-consistent.
6. **Hot-authoritative tiering (§12) — alternative track.** Upsert-deduped hot
   partitions + sync-at-seal (A) or partition-overwrite (B); reads route by tier
   with no merge. Simpler than the overlay family (phases 1, 3–5) and can ship
   independently.
7. **Mirror mode (C) — optional (§12.3).** Logical-decoding capture of the hot
   tables; apply to Iceberg via position deletes or overwrite-from-PG reset
   (equality-delete writes only if pursued, §11#10); applied-LSN watermark +
   idempotent apply; compaction/seal. Reuses pg_lake mirroring patterns.
8. **Overlapping tiers: authority boundary + per-partition state (§13.1–§13.4).**
   The per-partition authority catalog, seal, retention below `B`, and read
   routing by partition state. Single-tier reads only — no merge yet — which
   already answers "last N days in Postgres, last M in Iceberg, switch
   automatically" for queries that stay on one side of `B`.
9. **Heap relations in the whole-query pushdown (§13.5, option 2).** Relax the
   shippability rule in `query_pushdown.c`, deparse heap RTEs as
   `postgres_scan_pushdown`/`postgres_query`, thread the exported snapshot id.
   Unlocks cross-tier vectorised reads, the §12.4 fresh-tail overlay, and a bulk
   Postgres → Iceberg seal (§13.8). Prerequisite for a useful phase 10; the
   highest-leverage item in the plan.
10. **`COLD_DIRTY` merge and repair (§13.3, §13.8).** Accept mutations below `B`
    into a per-partition delta, merge on read, and rematerialise the partition in
    the background to return it to `COLD_CLEAN`. Needs the snapshot hunk from
    §13.6 if the index-friendly read path is used.
11. **Partial-aggregate pushdown (§13.5, option 3) — later.** The graceful
    fallback for cross-tier queries phase 9 refuses.

Phases 8–10 are the path to the shape in §13 and can lead, drawing on phases 1–5
for merge machinery as needed; phases 6–7 remain the simpler tiering-only tracks.

Each phase is independently shippable and testable.

---

## 16. Testing strategy

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
6. **Tier routing (§13.1–§13.4)** — a query wholly above `B` touches only
   Postgres and opens no reverse connection; a query wholly below `B` emits plain
   Iceberg SQL; a spanning query returns each row exactly once. Assert on the
   emitted DuckDB SQL, not just results, so a regression that silently ships rows
   into Postgres is caught.
7. **Cross-tier aggregation stays in DuckDB (§13.5)** — `EXPLAIN` a
   `GROUP BY time_bucket(...)` spanning `B` and assert the aggregate is pushed
   down, not computed above an `Append`. This is the specific regression option 1
   exhibits today.
8. **Seal and repair (§13.8)** — sealing a partition leaves results unchanged
   across the transition; a mutation below `B` marks the partition `COLD_DIRTY`
   and is visible immediately; repair returns it to `COLD_CLEAN` with identical
   results and no delete files.

---

## 17. Naming & placement

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

---

## 18. As built

Phases 1–3 and 8 of §15 are implemented, together with the `COLD_DIRTY` overlay
and repair of phase 10, in `pg_lake_timeseries--3.4.sql` (SQL API + routing view)
and `src/maintenance_worker.c` (base worker). Phase 9 — heap relations inside the
whole-query pushdown — is **not** implemented, so a cross-tier read still ships
Iceberg rows into PostgreSQL and aggregates them there. Everything below records
where the implementation differs from the specification above, and why.

### 18.1 Objects created per table

`timeseries.create_table('metrics', 'ts', ...)` turns an empty template table
into:

| object | kind | role |
| --- | --- | --- |
| `metrics` | view + `INSTEAD OF` trigger | the user-facing relation; routes reads and writes |
| `metrics_hot` | heap, `PARTITION BY RANGE (ts)` | authoritative for `ts >= B` |
| `metrics_cold` | pg_lake Iceberg table | authoritative for `ts < B`, plus a lagging copy above `B` |
| `metrics_cold_scan` | partitioned table, one partition | pruning wrapper over `metrics_cold` (§18.3) |
| `metrics_delta` | heap + `_ts_seq`, `_ts_deleted` | mutations that land below `B` |
| `metrics_seq` | sequence | version order for delta rows |

The template must be empty: conversion would otherwise have to decide which tier
each existing row belongs to. History is loaded after conversion, either straight
into `metrics_cold` or through the view (which routes it into the delta, from
where `repair()` folds it into Iceberg).

### 18.2 Restrictions

- `partition_interval` must be a positive **fixed-length** interval. `month` and
  `year` would need `date_trunc` rather than the epoch arithmetic in
  `partition_start`, and are rejected.
- `hot_retention >= partition_interval`.
- `key_columns` must contain the time column: it is the partition key of the hot
  tier, and PostgreSQL requires a unique index on a partitioned table to include
  the partition key.
- A keyless table is append-only — `UPDATE`/`DELETE` through the view raise —
  because merge-on-read has nothing to match versions on. `upsert => true`
  therefore also requires a key.
- Column names starting with `_ts_` are reserved.
- The initial boundary is `partition_start(now() - hot_retention)`, so a freshly
  created table is entirely hot and the Iceberg tier is empty.

### 18.3 Tier elimination is partition pruning, not constraint exclusion

The design assumed a `UNION ALL` view with literal boundary predicates would let
the planner drop the branch whose predicate the query contradicts. Measured on
PostgreSQL 18, it does not:

- with the default `constraint_exclusion = partition`, a self-contradictory
  `UNION ALL` branch is **kept** — the scan stays in the plan and returns no rows;
- union-leaf flattening (which would expose the branch's quals to the outer
  query) happens only for `SELECT *`; any narrower projection or an aggregate
  leaves a `Subquery Scan` wrapper in place, and even when flattening does happen
  it does not by itself prune;
- `constraint_exclusion = on` does prune it, but that is a session-wide setting
  the extension has no business changing.

Partition pruning, by contrast, is unconditional and shape-independent. The hot
tier prunes through its own range partitions. The cold tier therefore gets the
same mechanism: `metrics_cold_scan` is a range-partitioned table whose only
partition is the Iceberg table, attached `FOR VALUES FROM (MINVALUE) TO (B)`, and
the view reads the wrapper. A query with `ts >= B` prunes the Iceberg scan at plan
time whatever its shape; `seal()` re-bounds the wrapper (detach + attach) when it
advances `B`.

The bound is a **pruning hint only**. PostgreSQL neither enforces nor applies
partition constraints as filters for a foreign table, and the Iceberg table
deliberately holds rows above `B` (the lagging copy), so the view keeps its own
`ts < B` predicate to mask them. A single partitioned parent over both tiers is
still rejected: it would double-count exactly those rows.

### 18.4 The view cannot specialise on dirty partitions

The view is a stand-in for the §5.3 CustomScan, and this is where the stand-in
runs out: **a view cannot be replaced while a statement that references it is
running.** The write path is an `INSTEAD OF` trigger on that very view, so a
`CREATE OR REPLACE VIEW` from inside it fails with
`cannot CREATE OR REPLACE VIEW ... because it is being used by active queries in
this session`. A write below `B` is therefore in no position to add the delta
branch that makes its own effect visible.

The consequences, and the choice made:

- The view's shape depends on the boundary and the key, and on nothing else. The
  delta overlay is **permanent**: every cold-tier read carries
  `NOT EXISTS (SELECT 1 FROM <delta> d WHERE <key match>)` plus a `DISTINCT ON`
  branch over the delta.
- `refresh_view()` is called only on a boundary advance, i.e. from `seal()` and
  `create_table()` — never from inside a statement that reads the view.
- Deferring the regeneration to commit (a deferred constraint trigger on the
  delta) was rejected: it would leave the rest of the writing transaction reading
  a view that does not know about its own write.
- Per-partition `cold_dirty` state is still maintained, but it is now purely a
  work list for `repair()` rather than an input to the plan.

The cost of the permanent overlay is smaller than it looks, because the clean
cold branch was never whole-query-pushdownable in the first place: under a
`UNION ALL` the Iceberg branch is a `Foreign Scan` below an `Append`, so
aggregation already happens in PostgreSQL (§13.5, option 1). What the anti-join
adds is a hash probe per cold row against a table that is empty on a repaired
table. What it costs is the *option* of pushing a single-tier cold query down as
a whole query — and that is the concrete, measured argument for doing §5.3
(CustomScan) and phase 9 rather than living with the view.

### 18.5 Iceberg partition transform is aligned with the hot partitions

`sync()` and `repair()` overwrite a whole partition range with `DELETE` +
`INSERT`, and pg_lake turns a `DELETE` that matches whole Iceberg partitions into
a metadata-only file removal instead of position deletes. The transform is
therefore chosen to keep a hot partition inside one Iceberg partition:
`hour(ts)` for `partition_interval <= 1 hour`, `day(ts)` otherwise. A sub-hour
interval necessarily lands inside an hour partition and does pay for position
deletes on re-sync.

### 18.6 Freshness of the lagging copy

`sync()` copies a hot partition into Iceberg once it is **complete**
(`part_end <= now()`) and re-copies it only if it was written to since
(`synced_at < part_end`). External Iceberg readers therefore see data up to the
end of the last completed partition — "up to last night" for a daily interval —
while PostgreSQL readers see everything. Nobody reads the copy through the view,
so its staleness is not a correctness concern; `seal()` overwrites the range
again before it becomes authoritative.

### 18.7 Privileges: SECURITY INVOKER plus row-level security

The API functions are `SECURITY INVOKER`, so `current_user` is the caller,
`check_owner()` means something, and the hot/cold/delta objects a call creates are
owned by the caller rather than by the extension owner. The extension catalogs are
protected by RLS instead of by function privileges:

- `timeseries.tables` / `timeseries.partitions` have RLS enabled and are granted
  to `public`;
- the `USING` clause restricts every row to a caller who owns the parent;
- the `WITH CHECK` clause additionally requires ownership of **every relation the
  row names**. This is what stops a user from registering a row that points the
  superuser maintenance worker at someone else's table.

`route_write()` is the one `SECURITY DEFINER` function: a grantee who has only
`INSERT` on the view must still be able to write the heap, bump the sequence, and
record a dirty partition. Everything it touches is derived from `TG_RELID`, which
has to be a registered parent view. Making the whole API `SECURITY DEFINER` was
tried and reverted: inside a definer function `current_user` is the definer, so
`check_owner()` would succeed for anybody whenever the definer is a superuser, and
there is no portable way to recover the invoker.

### 18.8 Maintenance worker

Registered through `pg_extension_base`'s base-worker framework
(`extension_base.register_worker`), so there is one worker per database with the
extension, started on `CREATE EXTENSION` and on server start. Each pass lists the
enabled tables in one transaction and then runs `timeseries.maintain()` per table
in its own transaction, downgrading errors to `WARNING` so one table cannot hold
back the others. `maintain()` runs, in order: `add_partitions()` (extend the
frontier), `repair()`, `sync()`, `seal()`, `apply_retention()` -- repair first, so
that a partition is not copied into Iceberg and then immediately rematerialised.

Two GUCs: `pg_lake_timeseries.enable` (default on) and
`pg_lake_timeseries.maintenance_naptime`. The test suite pins `enable = off` and
drives maintenance explicitly so that assertions about the boundary and the
partition states are deterministic; one test re-enables it to prove the worker
performs a pass.

### 18.9 Tests

`tests/pytests/test_timeseries.py`, 16 cases, run against a live pg_lake test
cluster with the in-tree S3 mock. They assert the properties the design rests on
rather than the implementation: the boundary decides which tier owns a row, the
lagging Iceberg copy is never double-counted, tier elimination happens at plan
time (`EXPLAIN` must not mention `Engine: DuckDB` for a hot-window query, nor a
heap partition for a cold-window one), mutations below `B` are visible immediately
and are folded back by `repair()` with identical results, a boundary advance is
only ever the result of a completed seal, and a keyless table refuses mutations.

Each test gets its own S3 prefix: `drop_table(drop_data => true)` leaves the
Iceberg files behind, and `CREATE TABLE ... USING iceberg` refuses a non-empty
location.

### 18.10 Not implemented

- **Phase 9 / §13.5 option 2** — heap RTEs in the whole-query pushdown. Without
  it there is no vectorised cross-tier plan, `INSERT INTO <iceberg> SELECT ... FROM
  <heap>` still falls back to the row-at-a-time FDW path (so `seal()` pays it),
  and §18.4's permanent anti-join has no cheaper alternative.
- **§5.3 CustomScan** — the view stand-in cannot specialise on dirty partitions
  (§18.4) and cannot choose between `postgres_scan_pushdown` and `postgres_query`.
- **§13.6 snapshot hunk** — not needed yet, because no reverse connection is
  opened.
- **Compaction** of the cold tier beyond `apply_retention()`, and the
  `month`/`year` partition intervals of §18.2.

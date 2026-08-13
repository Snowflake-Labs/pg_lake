# pg_lake_timeseries — Design & Specification

Status: **implemented.** This document describes what the code in this directory
does and why. §14 records the designs that were tried and dropped on the way here,
because their measurements are the reason the current one looks like it does.

---

## 1. Motivation

We want a good way to store large volumes of time-series data in PostgreSQL, with
the bulk of the data living in Apache Iceberg (via pg_lake) so that analytical
scans run on DuckDB's columnar engine and storage is cheap.

The obvious approaches don't fit:

- **TimescaleDB** is not an option in this environment.
- **Native declarative partitioning with older partitions turned into Iceberg
  foreign tables** yields *one foreign table per partition*. That produces poor
  plans (per-partition foreign scans, limited partition-wise optimization) and
  fragments what should be one Iceberg table. We want **one** Iceberg table,
  internally partitioned, so DuckDB prunes files with manifest statistics.
- **Partition management in PostgreSQL takes heavy locks** and requires ongoing
  operational effort (create/attach/detach/drop).

### The idea

Two relations, and a boundary between them:

- the **hot tier** is the user's own table — an ordinary heap, `PARTITION BY
  RANGE` on a `timestamptz` column, with their indexes, constraints and native
  tuple routing. It is authoritative for `time_column >= B`;
- the **cold tier** is one pg_lake **Iceberg** table, internally partitioned,
  authoritative for `time_column < B`;
- `B` is a stored **authority boundary**. It is advanced only by `seal()`, which
  copies a heap partition into Iceberg and drops it in the same transaction.

Neither relation knows about the other. What makes them one table is a
`planner_hook` that rewrites a query on the hot relation into the union of the two
tiers, bounded at `B`. Every range of time is read from exactly one tier, so there
is no merge, no anti-join and no deduplication anywhere in the read path.

---

## 2. Goals and non-goals

### Goals

1. A single logical relation combining fresh heap data and an Iceberg tier.
2. Fast, indexed access to recent data (point lookups, selective queries), at the
   cost of an ordinary partitioned table — the hot tier *is* an ordinary
   partitioned table.
3. Full columnar pushdown (predicates, projection, aggregation) to DuckDB for the
   part of a query Iceberg owns, and optionally for the whole query (§13).
4. No DDL on the insert hot path; no per-partition foreign tables.
5. The boundary never advances over data that is not in Iceberg, under crashes and
   under concurrency.
6. A query that only touches the hot window costs what it would cost without the
   extension.

### Non-goals (initially)

- `REPEATABLE READ`/`SERIALIZABLE` over the Iceberg tier: pg_lake reads the latest
  Iceberg snapshot, and time-travel pinning is a later enhancement.
- Mutating sealed history. Below `B` there is no heap partition to hold a change,
  so an `UPDATE`/`DELETE` there matches nothing and an `INSERT` fails with "no
  partition of relation". Rewriting cold history is an Iceberg-side operation.
- Continuous aggregates / automatic rollups.
- Cross-tier uniqueness enforcement: a unique index covers the hot tier only.
- `month`/`year` partition intervals (§5).

---

## 3. Architecture overview

```
     INSERT / UPDATE / DELETE                         SELECT
     (native tuple routing,                    (planner_hook rewrites the
      no trigger, no view)                      relation into both tiers)
              │                                          │
              ▼                                          ▼
   ┌──────────────────────────┐              ┌──────────────────────────────┐
   │  metrics                 │              │ metrics WHERE ts >= B        │
   │  heap, PARTITION BY      │─────────────►│   UNION ALL                  │
   │  RANGE (ts)              │              │ metrics_cold WHERE ts < B    │
   │  authoritative: ts >= B  │              └──────────────────────────────┘
   └──────────┬───────────────┘                             ▲
              │  seal(): copy + DROP TABLE + advance B      │
              │  sync(): copy only (B unchanged)            │
              ▼                                             │
   ┌──────────────────────────────────────────────┐          │
   │  metrics_cold — pg_lake Iceberg table        │──────────┘
   │  authoritative: ts < B, plus a lagging copy  │
   │  of completed partitions above B             │
   └──────────────────────────────────────────────┘
```

The registration that ties the two together lives in `timeseries.tables`
(§6). Maintenance (§9) moves data upward through `sync()` and `seal()` and expires
it at the far end.

---

## 4. The authority boundary

`B` is one `timestamptz` per table, and it is the only thing that decides which
tier owns a row:

- **PostgreSQL owns `time_column >= B`.** Its partitions cover that range and
  nothing below it, so a write below `B` has no partition and raises.
- **Iceberg owns `time_column < B`.** It is also allowed to hold a *copy* of
  ranges above `B` (§9.2), which the read path masks with its own `< B` predicate.
- `B` starts at `-infinity`, so a new table is entirely hot and the cold branch of
  every plan returns nothing. Nothing else ever writes `B` backwards, and there is
  no way to start a table with a boundary already in the past: the Iceberg tier is
  created empty along with the heap (§5).
- `B` moves **only forward, only in `seal()`**, and only in the transaction that
  has already copied the range into Iceberg and dropped the heap partition.

That last point is the whole correctness argument, and it is why the boundary is a
single scalar rather than per-partition state: a reader needs one value to split a
query, and a maintenance pass needs one value to know where to continue.

---

## 5. DDL

A tiered table is made with one statement, and everything after that is ordinary
DDL on the user's own relation:

```sql
CREATE TABLE metrics (ts timestamptz NOT NULL, device int, value float8)
USING timeseries WITH (partition_interval = '1 day',
                       hot_retention = '7 days',
                       location = 's3://bucket/metrics');
```

`USING timeseries` names a placeholder access method (`timeseries_am_handler()`
exists only so that the name resolves, and raises if it is ever called). `src/ddl.c`
intercepts the statement through pg_lake's utility handler framework
(`RegisterUtilityStatementHandler()`, the same way the `iceberg` access method is
implemented) and turns it into:

1. the partitioned heap the user asked for — `PARTITION BY RANGE (<time column>)`
   is filled in if they did not write it, and the access method is set to the
   default one rather than left blank, so that a `default_table_access_method` of
   `iceberg` cannot turn the hot tier into a second lake table;
2. `CREATE TABLE <name>_cold (<the heap's columns>) USING iceberg WITH (...)`, with
   `partition_by` defaulted from `partition_interval` and every option this
   extension does not consume passed through for pg_lake to validate;
3. an internal dependency of the cold tier on the heap, so `DROP TABLE metrics`
   drops it too (§11.1);
4. the registration row in `timeseries.tables`;
5. enough partitions to cover the hot window and `precreate_ahead`, so the table is
   writable immediately.

The options this extension consumes are `time_column`, `partition_interval`
(default `1 day`), `hot_retention` (`7 days`), `cold_retention` (unlimited) and
`precreate_ahead` (`7` intervals). `time_column` may be left out when the table has
exactly one `timestamptz` column.

### 5.1 What is refused, and why

The invariants the planner hook trusts (§7) are established here by construction
rather than validated afterwards. There is deliberately **no** function that adopts
an existing heap and an existing Iceberg table as a pair, and no
`ALTER TABLE ... SET ACCESS METHOD timeseries`: a registration whose assumptions
were checked once, on tables that were built elsewhere, is a registration that can
be made to lie.

What `CREATE TABLE` refuses:

- a temporary or unlogged table — the cold tier outlives the session, and skipping
  WAL for a tier whose contents get copied into Iceberg is not crash-safe;
- `USING timeseries` on a partition, or on `CREATE TABLE ... AS`;
- `INHERITS` — the hook substitutes a subquery for the relation, which inheritance
  expansion has no place for;
- a partitioning that is not `RANGE` on the single time column, an ambiguous time
  column (no `timestamptz` column, or more than one and no `time_column` option), a
  time column that is `timestamp` rather than `timestamptz`, or one that is
  nullable. `timestamp` is rejected because the boundary, the partition bounds and
  every predicate the hook adds are `timestamptz`, and reading one as the other
  shifts by the session time zone; a NULL time would belong to no partition, and so
  to no tier;
- a `partition_interval` that is not positive and **fixed-length**. `month`/`year`
  would need `date_trunc` rather than the epoch arithmetic in `partition_start()`.

### 5.2 Keeping the tiers the same shape

The two tiers only have to agree about the *shape* of the table: the hook
substitutes one for the other by position, so their column names, types and order
must match. `ALTER TABLE` on a tiered table therefore forwards exactly the
subcommands that change that — `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`,
`OWNER TO`, and `RENAME COLUMN` — to the Iceberg tier, and leaves everything else
(indexes, constraints, storage parameters, statistics targets) as a hot-tier matter.

The ordering is forced by two things at once:

- what the tier has to be told is computed **before** the user's statement runs, so
  that a `DROP COLUMN` is still looking at a column that exists;
- the tier is altered **after** it, because the nested DDL has to reach pgaudit
  after the top-level statement did. pgaudit raises "pgaudit stack is not empty" if
  it sees a top-level utility statement while its stack holds a DDL item, which is
  what running the tier's `ALTER TABLE` first would leave behind.

Both happen in one transaction, so pg_lake's Iceberg validation still decides the
outcome for both tiers: a type change Iceberg cannot express aborts the heap's
rewrite with it. The allowed changes are Iceberg's promotions — `int -> bigint`,
`float -> double`, wider `decimal` at the same scale.

Executing the user's statement is `ExecuteUserStatement()`, and it is not simply a
call to `PgLakeCommonProcessUtility()`: that restarts the handler chain from the
first handler, which would call this handler again on the same statement. The
statement being executed is remembered in a file-static for exactly as long as that
takes, and recognised on the way back in.

Three further changes are refused outright rather than half-propagated:

- anything to the **time column** — dropping it, changing its type, or renaming it.
  It is the heap's partition key, the Iceberg tier's partition field and the type of
  the boundary; pg_lake will not rename a field that a partition spec uses, so a
  rename could only end with the tiers disagreeing about the name;
- an `ALTER COLUMN TYPE` with a `USING` expression, which rewrites the values of a
  column. Iceberg promotes the type of a field without touching its data files, so
  the expression would only reach the rows PostgreSQL still owns;
- `TRUNCATE`, which would empty the heap and leave Iceberg holding everything below
  the boundary — the history the user was trying to remove. Truncating a single
  partition is still allowed: that is a range of the hot tier, which is what a
  `DELETE` over the same range would have done.

`ADD COLUMN ... DEFAULT` is propagated, but the default is not: Iceberg reads a
field its data files lack as `NULL`, and `pg_attrdef` on the tier stays empty. The
rows the heap fills in are the ones the default was written for.

The settings in `timeseries.tables` are not relation options, so `ALTER TABLE ...
SET (hot_retention = '3 days')` is absorbed by this extension and PostgreSQL never
sees it. A partitioned table has no storage parameters of its own — PostgreSQL
rejects every one of them with a hint about the leaf partitions — so `SET (...)` on
a tiered table is entirely this extension's namespace, and a name it does not know
is refused here. Settings cannot be changed in the same `ALTER TABLE` as anything
else, because the two halves would have to be applied by different code.

Renaming the table itself is deliberately **not** propagated: the registration
names relations by OID, so nothing depends on the cold tier still being called
`<table>_cold`.

---

## 6. Metadata

Two tables, both `pg_extension_config_dump`'d:

| table | grain | contents |
| --- | --- | --- |
| `timeseries.tables` | one row per tiered table | both tiers, time column, partition interval, **boundary**, retentions, `precreate_ahead` |
| `timeseries.partitions` | one row per partition-aligned range | `synced_at` (when the Iceberg copy was refreshed), `sealed_at` (when Iceberg became authoritative) |

The planner reads **only** `timeseries.tables`, and from it only the cold relation
and `B`. Per-range state is a maintenance concern, which is what lets the read
path be cached per backend and invalidated rarely (§11).

There is no mirror of the heap's own shape: `timeseries.heap_ranges()` reads
`relpartbound` directly, so the partitions the heap actually has are the truth,
and a partition somebody created by hand needs no registration.

### 6.1 Writes go through C, reads bypass ACLs

Every write to these tables goes through a C function in `src/registry.c` that
checks the caller owns the relation and then performs the write as the extension
owner (`SPI_START_EXTENSION_OWNER`). The check has to be in C: inside a
`SECURITY DEFINER` plpgsql function `current_user` is the definer, so plpgsql
cannot see who called it and could not authorize this at all.

`RegisterTieredTable()` additionally demands ownership of **both** tiers. That is
what stops one user from aiming the superuser maintenance worker — which drops
partitions and overwrites an Iceberg table on the strength of a catalog row — at
another user's tables.

The readers (`is_tiered`, `tiered_table`, `synced_ranges`, `heap_ranges`) use
`systable_beginscan` and apply no ACL check, deliberately and in the same way the
planner hook does: whether a relation is tiered, and where its boundary sits, is a
property of the relation that every backend must agree about whoever is asking. It
stays a privilege check rather than a visibility one — a user who may not read the
Iceberg tier gets a permission error on it, not a silently different answer.

### 6.2 Privileges: `pg_monitor` reads, nothing else

The catalogs grant nothing to `public` and carry **no row-level security**. RLS on
an extension configuration table makes `pg_dump` fail outright: it refuses to dump
a table whose policies could silently filter the rows it is copying, which is
exactly the data that has to survive a dump. `pg_monitor` gets `SELECT` so
monitoring can see the registrations and how far each boundary has moved.

The trade is that a plain `pg_dump` by a user who is neither superuser nor a member
of `pg_monitor`/`pg_read_all_data` fails on the two configuration tables. An
earlier revision used owner policies instead; §14.4 records how that went.

---

## 7. Read path: the planner hook

`src/planner.c` installs a `planner_hook`. For every `RTE_RELATION` in the parse
tree that names a registered relation, it substitutes a subquery:

```sql
SELECT <cols> FROM metrics       WHERE ts >= '<B>'
UNION ALL
SELECT <cols> FROM metrics_cold  WHERE ts <  '<B>'
```

This is exactly what the rewriter does for a view (`ApplyRetrieveRule`), done at
plan time instead. The walker is post-order and runs on every level of the query —
subqueries, CTEs, sublinks, set-operation branches — and the parse tree is copied
only when a registered relation is actually present, so a query that touches none
pays a walk and nothing else.

An RTE is expanded only if it is a partitioned-table `RTE_RELATION` with `inh`
set, no `TABLESAMPLE`, not the query's `resultRelation`, and carries no row mark.
The last three are the cases where a second tier would be wrong rather than slow:
`FOR UPDATE` cannot lock an Iceberg row, and a write must reach the heap alone.

`pg_lake_timeseries.expand_tiered_tables` (default on, `PGC_USERSET`) turns the
substitution off, which is how the heap can be read on its own.

### 7.1 The RTE stays the relation's

The converted RTE keeps `relid`, `rellockmode` and `perminfoindex`, so the
relation is still locked before execution and the caller's privileges on it are
still checked. Its `relkind` becomes `RELKIND_VIEW`, because that is the only
relkind PostgreSQL accepts on a subquery RTE that carries permission info —
`ExecCheckPermissions` asserts it (`execMain.c:604`), and without it the backend
crashes on the first expanded query. `AcquireExecutorLocks`/`ScanQueryForLocks`
use only `relid` and `rellockmode`, so claiming `RELKIND_VIEW` costs nothing.

Both branch RTEs get a fresh `RTEPermissionInfo` with `requiredPerms =
ACL_SELECT` and `checkAsUser` set to the **owner of the relation** — view
semantics. `SELECT` on the table is therefore enough to read both tiers, and the
Iceberg tier itself stays unreadable to a grantee who has no rights on it.

### 7.2 Row-level security is refused

If either tier has `relrowsecurity`, the hook raises
`ERRCODE_FEATURE_NOT_SUPPORTED`. Policies are applied by the rewriter, which has
already run: adding a second tier afterwards would return rows the policy would
have filtered. Refusing the query is the only honest option, and
`ALTER TABLE ... DISABLE ROW LEVEL SECURITY` restores the table.

### 7.3 Plan-cache invalidation

`B` is planted in the plan as a constant, and "is this relation tiered" is not a
plan dependency at all, so neither would invalidate a cached plan by itself. Every
write to `timeseries.tables` — the registration a `CREATE TABLE` makes, the
settings an `ALTER TABLE` changes, the boundary a seal advances, the sweep after a
`DROP TABLE` — therefore calls `CacheInvalidateRelcacheByRelid()` on the relation,
so a prepared statement is replanned after a seal.

### 7.4 Tier elimination: what prunes and what does not

The heap side prunes properly: the branch predicate `ts >= B` plus the query's own
predicate go through ordinary partition pruning, so a cold-window query reads no
heap partition at all. With a constant cutoff the heap branch is gone from the plan;
with a `now()`-based one it survives as an `Append` reporting `Subplans Removed: N`,
since that pruning happens at execution start.

The Iceberg side does not disappear from the plan. PostgreSQL only drops a
self-contradictory qual under `constraint_exclusion = on`, which is a session-wide
setting the extension has no business changing — so a hot-window query keeps its
`Foreign Scan on metrics_cold`, with `ts >= <window> AND ts < B` on it. The scan runs
and reads nothing: DuckDB folds the contradiction to `EMPTY_RESULT`. `Data Files
Scanned` is 0 when the cutoff is a constant, and the tier's whole file count when it
is `now()`-based, because that list is built at plan time from what pg_lake can
evaluate then — it is a file *list*, not I/O. The cost is a node in the plan.

### 7.5 Whole-query pushdown

With `pg_lake_table.enable_heap_query_pushdown` on, the union the hook built is
admitted into pg_lake's whole-query pushdown and both tiers are read inside
DuckDB — one vectorised plan for a spanning aggregate (§13). This is why
`pg_lake_timeseries` must come **after** `pg_lake_table` in
`shared_preload_libraries`: its hook has to have inserted the cold branch by the
time `HasLakeRTE` looks at the parse tree (`query_pushdown.c:284`, `:385`).

### 7.6 The one path that is not the planner

`COPY <relation> TO` reads a relation without planning a query — but PostgreSQL
does not support `COPY` from a partitioned table, and a registered relation is
always partitioned, so there is no way to read the table through it. `COPY (SELECT
... ) TO` is planned and sees both tiers. Anything that reads the heap without
planning, such as logical replication, sees the hot tier alone; that is the
correct behaviour for replication, which should not replicate Iceberg's rows.

---

## 8. Write path

There is no write path. The relation is the user's table, so `INSERT`, `UPDATE`,
`DELETE`, `COPY ... FROM`, tuple routing, indexes, constraints, defaults,
`TRUNCATE`, `ALTER TABLE` and `pg_dump` all behave as they would without the
extension. `resultRelation` is excluded from the expansion, so a write never sees
the cold tier.

The consequences of the boundary follow from partitioning alone:

- a write above the frontier fails with "no partition of relation" rather than
  landing somewhere silently — a `DEFAULT` partition is deliberately not created,
  since it would also block attaching the next range;
- a write below `B` fails the same way, which is what makes sealed history
  immutable through PostgreSQL;
- an `UPDATE`/`DELETE` with a predicate below `B` matches nothing.

---

## 9. Maintenance

All four operations check ownership, take the relation's own locks and nothing
more, and are driven either explicitly or by the worker.

### 9.1 `add_partitions(relation, upto)`

Extends the frontier to `now() + precreate_ahead * partition_interval`. It starts
at the frontier the heap itself reports (`max(part_end)`), so hand-made partitions
are respected; for a table with no partitions it starts at
`partition_start(now() - hot_retention)`, or at the boundary when the cold tier was
pre-loaded past it. Partitions are named
`<relation>_<yyyymmdd>t<hhmm>` from the UTC rendering of `part_start`.

### 9.2 `sync(relation, only_start)`

Refreshes the Iceberg copy of partitions that are **entirely in the past**
(`part_end <= now()`), and re-copies one only if it was written to since
(`synced_at < part_end`). `B` does not move, so these rows stay authoritative in
PostgreSQL and the copy is invisible to queries through the relation — which is
what makes it repeatable. What it buys is that external Iceberg readers see data
up to the end of the last completed partition instead of only sealed history.

Each range is overwritten (`DELETE` + `INSERT`) rather than appended to, and the
rows are read **from the partition**, not from the relation: reading the relation
would go through the hook's own expansion, i.e. read the table it is writing.

### 9.3 `seal(relation, upto)`

The only operation that moves `B`. For each partition ending at or before
`partition_start(coalesce(upto, now() - hot_retention))`, in order:

1. overwrite the range in Iceberg;
2. `DROP TABLE` the heap partition;
3. advance `B` to `part_end` and record `sealed_at`.

All three are one transaction, so a crash or error anywhere rolls back all three:
`B` never advances past data that is not in the cold tier. The copy is not read
back to verify it, because it cannot be — the Iceberg snapshot this transaction
wrote does not exist until it commits. Atomicity is the argument, not verification.

Sealing walks contiguously upward from `B`. A gap — a partition dropped without
being sealed — would mean claiming Iceberg authority for a range that was never
copied, so a gap raises a `WARNING` and stops the pass instead of being skipped.

### 9.4 `apply_retention(relation)`

Deletes cold data below `least(partition_start(now() - cold_retention), B)` and
forgets its ranges. The `least()` is what makes a `cold_retention` shorter than
`hot_retention` harmless: it can never delete a range PostgreSQL still owns, only
a copy of one.

Deleting on partition boundaries is what keeps the removal metadata-only in
Iceberg: pg_lake turns a `DELETE` that matches whole Iceberg partitions into a file
removal rather than position deletes. Aligning the Iceberg transform with the
heap's `partition_interval` (`hour(ts)` for sub-hourly, `day(ts)` otherwise) is
therefore worth doing, though the cold table is the user's to define.

### 9.5 The worker

`src/maintenance_worker.c`, registered through `pg_extension_base`'s base-worker
framework: one worker per database with the extension, started on
`CREATE EXTENSION` and on server start. Each pass lists the registered tables in
one transaction, then runs `timeseries.maintain()` per table in its own
transaction, downgrading errors to `WARNING` so one table cannot hold back the
others. `maintain()` runs `add_partitions()`, `sync()`, `seal()`,
`apply_retention()` in that order — syncing before sealing means a partition that
was already copied is copied once more before it is dropped, which is what makes
the copy complete rather than merely recent.

GUCs, all defined in `src/init.c`: `pg_lake_timeseries.enable` (default on),
`pg_lake_timeseries.maintenance_naptime`, and
`pg_lake_timeseries.expand_tiered_tables` (§7).

---

## 10. Correctness

- **The boundary is never ahead of Iceberg.** Only `seal()` moves it, in the
  transaction that copied the range and dropped the partition (§9.3).
- **No row is returned twice.** The two branches carry complementary predicates
  around the same `B` from the same catalog row, so the lagging copy above `B` is
  masked. `sync()` deliberately creates rows that exist in both tiers, and
  `test_the_iceberg_copy_of_a_hot_partition_is_not_returned_twice` asserts the
  count is unchanged.
- **No row is lost by a move.** `seal()` copies before dropping, atomically.
- **A cached plan cannot outlive its boundary** (§7.3).
- **Concurrency.** `seal()` holds `ACCESS EXCLUSIVE` on the partition it drops
  (`DROP TABLE`) and updates the boundary row; a reader that planned with the old
  `B` holds `ACCESS SHARE` on that partition, so the drop waits for it. The
  invalidation is broadcast on commit, so plans built afterwards use the new `B`.
- **Snapshot consistency under pushdown** is pg_lake's: the heap side is read back
  over a loopback connection at an exported snapshot of the driving transaction
  (§13), and pg_lake reads the latest Iceberg snapshot.

---

## 11. The C lookup path is not SPI

Everything user-facing in this extension reads its catalogs through SPI from
plpgsql, which a planner hook cannot do: SPI re-enters the planner.
`src/metadata.c` reads `timeseries.tables` with `systable_beginscan` and caches
registrations per backend in a hash in `CacheMemoryContext` — positive **and**
negative answers, since it is the negative ones that keep a repeated query on an
ordinary table off the catalog.

A query that touches no tiered table has to cost nothing, so `IsTieredTable()`
short-circuits three times before it reaches a catalog: the extension is not
created (`pg_extension_base`'s ID cache), the registry is empty (a cached tristate
over one keyless scan), or the relation is not a partitioned table (a syscache
hit). An untouched cluster pays one scan of an empty table per backend.

**DML on an extension table invalidates nothing by itself.** A statement trigger
on `timeseries.tables` calls into C, which calls
`CacheInvalidateRelcacheByRelid()` on the catalog — the same device
`pg_lake_iceberg`'s object-store catalog uses. Putting it in a trigger rather than
in the `CREATE TABLE` path means a hand-written `INSERT` or a `pg_restore` is picked
up too. It is a *trigger* function rather than a plain function called from plpgsql
because `EXECUTE` on a trigger function is checked when the trigger is created, not
when it fires, so the invalidation works for whoever writes the catalog — including
a `pg_restore` running as someone with no rights to this schema. The relcache
callback can run outside a transaction, so it compares against the OID already
resolved in the ID cache rather than resolving one.

### 11.1 Cleaning up after `DROP TABLE` takes an event trigger, and not the obvious one

A `regclass` column is a plain OID with no `pg_depend` entry behind it, so dropping
either tier leaves a registration naming a relation that is gone — and OIDs are
reused. `sql_drop` looks like the answer and is not: **a pg_lake table's `DROP`
fires no `sql_drop` event at all.** `ProcessDropPgLakeTable`
(`pg_lake_table/src/ddl/drop_table.c`) drops lake tables by calling
`RemoveRelations()` directly and then strips them from the statement, so they never
pass through the path that collects dropped objects and
`pg_event_trigger_dropped_objects()` returns nothing for them. `ddl_command_end`
does fire for both tiers, so the cleanup hangs off that and *sweeps* for
registrations naming a missing relation rather than looking up what the command
dropped. Such a row describes no table and nobody owns it, so `forget_dropped()`
checks no ownership and needs none. The sweep collects before deleting, so a `DROP`
with nothing to clean up does not fire the invalidation trigger and broadcast a
pointless cache reset to every backend.

---

## 12. API

A tiered table is made, changed and unmade with DDL (§5); there is no function that
registers one.

| statement | what it does |
| --- | --- |
| `CREATE TABLE ... USING timeseries WITH (...)` | both tiers, the registration and the initial partitions (§5) |
| `ALTER TABLE ... SET (hot_retention = ..., ...)` | change the settings in the registration (§5.2) |
| `ALTER TABLE ...` (column added, dropped, retyped, renamed; `OWNER TO`) | applied to both tiers, under Iceberg's rules (§5.2) |
| `DROP TABLE ...` | drops the Iceberg tier with it; an event trigger sweeps the registration (§11.1) |

The functions are maintenance and introspection.

| function | what it does |
| --- | --- |
| `add_partitions(relation, upto)` | extend the frontier (§9.1) |
| `sync(relation, only_start)` | refresh the lagging Iceberg copy (§9.2) |
| `seal(relation, upto)` | hand ranges to Iceberg and advance `B` (§9.3) |
| `apply_retention(relation)` | expire cold history (§9.4) |
| `maintain(relation)` | one pass of all four (§9.5) |
| `is_tiered(relation)`, `tiered_table(relation)`, `tiered_tables()` | what is registered |
| `synced_ranges(relation)`, `heap_ranges(relation)` | what Iceberg has a copy of, and what the heap has |
| `partition_start(ts, interval)` | floor a timestamp to a partition bound |

---

## 13. Heap relations in whole-query pushdown

This is a `pg_lake_table` feature (`enable_heap_query_pushdown`, bool, default
off, `PGC_USERSET`, plus `heap_pushdown_dsn`, string, `PGC_SUSET`) that this
extension is the main consumer of: with it on, a query spanning both tiers is one
vectorised DuckDB plan instead of an aggregate over an `Append`.

Its foundation was checked against the running `pgduck_server` (DuckDB v1.4.4)
before any of it was written:

- the Postgres scanner is statically linked into `duckdb_pglake`
  (`postgres_scanner_duckdb_cpp_init`), and the in-tree
  `patches/duckdb-postgres/snapshot.patch` is live in the deployed binary:
  `snapshot VARCHAR` is a named parameter of both `postgres_scan` and
  `postgres_query`;
- a heap table read through `postgres_scan(dsn, schema, table, snapshot => '<id>')`
  observes exactly the exporting backend's snapshot. With three rows committed
  before `pg_export_snapshot()` and a fourth after it, DuckDB returned 3 rows at
  the snapshot and 4 without it;
- `postgres_scan` takes a raw DSN, so no `ATTACH` is needed — which is fortunate,
  because `ATTACH ... (TYPE postgres)` **fails** in this build: it lazily loads the
  community `postgres_scanner` from `~/.duckdb/extensions`, which collides with the
  statically linked copy (`function "postgres_scan" already exists`).

What was then built, in `pg_lake_table`:

1. **Admission.** `HasLakeRTE` and the two RTE rejections in
   `src/planner/query_pushdown.c` accept a heap `RTE_RELATION` when
   `HeapRteIsPushdownable` says so: an ordinary or partitioned table, not a
   catalog, temporary or row-level-security relation, no `securityQuals`, no
   `TABLESAMPLE`, no dropped or virtual generated column, and not a legacy
   inheritance parent with storage of its own. Each is a way the loopback scan
   would return different rows or columns than a local scan; the dropped-column
   case is the subtlest, because the scanner returns only live columns and would
   shift everything after the hole.
2. **Rewrite and substitution.** An admitted heap RTE goes through the same
   `__lake_read_table('<name>', <id>)` placeholder as a lake table, and at
   execution `ReplaceHeapTableFunctionCalls` substitutes
   `postgres_scan_pushdown(dsn, schema, table, snapshot => '<id>')`, so DuckDB
   pushes filters and projections into the reverse scan. `EXPLAIN` without
   `ANALYZE` substitutes the relation name instead, which keeps `Vectorized SQL`
   readable.
3. **Snapshot.** The driving backend calls `ExportSnapshot` once per pushed-down
   query and threads the id into every deparsed call, never cached across
   statements: under `READ COMMITTED` each statement gets a fresh snapshot, and
   reusing an older one would read stale rows. Two states make an export useless
   rather than merely unavailable, and both fall back to the local plan — inside a
   subtransaction (an importer cannot tell it is still running), and after the
   transaction has been assigned an XID (an exported snapshot shows the exporter as
   in-progress, so the loopback would not see the driving transaction's writes).
4. **Reverse connection.** The loopback DSN is built from the running cluster — the
   first `unix_socket_directories` entry, or `localhost` if empty, plus port,
   database and current user — unless `heap_pushdown_dsn` overrides it. That makes
   the permissions the same as a local scan's.

`IsLakePartitionedTable` additionally makes a partitioned parent whose leaves are
all lake tables pushdownable in its own right, deparsed as the `UNION ALL` of its
partitions over the parent's tuple descriptor. A partition is accepted only if its
column layout matches the parent exactly (`ATTACH PARTITION` matches by name, so a
partition may reorder columns or carry a dropped one), and `FROM ONLY` is honoured
rather than forced. This one is not behind a GUC — it is the treatment a lake
inheritance tree already got — but it does mean whole-query pushdown takes over
queries on an all-lake partitioned parent that PostgreSQL used to plan per
partition, so `enable_partitionwise_aggregate`/`_join` no longer decide anything for
them; `pg_lake_table.enable_full_query_pushdown = off` gets the old plans back.

### 13.1 A partitioned heap has to be named partition by partition

The first version of the heap substitution named the relation from the RTE, which
for a partitioned hot tier is the parent. That does not merely read the wrong rows
— it hangs.

`postgres_scan_pushdown` splits a relation into parallel ctid range scans and sizes
the split from `pg_class.relpages`:
`duckdb-postgres/src/storage/postgres_table_set.cpp:174` assigns
`result->GetInt64(0, 2)` into `approx_num_pages`, an `idx_t`. A partitioned table
has no storage, so once analysed its `relpages` is `-1`, which as an unsigned page
count is 2^64-1. `PostgresParallelStateNext` then hands out a task per iteration of
`if (gstate.page_idx < bind_data->pages_approx)`, clamping each task's upper bound
to `POSTGRES_TID_MAX` (2^32-1) — and since it assigns that clamp straight back into
`gstate.page_idx`, the index stops advancing while the condition stays true. Tasks
are handed out forever. The symptom is a query that never returns, and it appears
only after `ANALYZE`, because a never-analysed parent still has `relpages = 0`.

So `BuildHeapScanQuery` expands a `RELKIND_PARTITIONED_TABLE` into a parenthesised
`UNION ALL` over the leaves of `find_all_inheritors`, skipping anything that is not
`RELKIND_RELATION`, each branch projecting the parent's column names explicitly. A
parent with no partitions falls back to a typed-empty source, keeping its row type.
The timeseries test runs `ANALYZE metrics` before asserting, so the hang would be
caught rather than depending on the parent being unanalysed.

### 13.2 What the pushdown costs: tier elimination

Pushing the whole query down moves the tier decision from the PostgreSQL planner
into DuckDB, which is a real loss for a hot-window query: the cold branch reaches
DuckDB with a contradictory range instead of being pruned, so its files are opened
(row groups are still skipped, and the answer is the same). The window predicate
does reach `read_parquet` as a filter; what is missing is plan-time *file* pruning,
because `PruneDataFiles` prunes from the restrictions on the cold relation and the
outer window predicate is not among them. Until that gap closes the choice is per
session, which is what the GUC is for: on for spanning queries, off for hot-window
ones.

### 13.3 Measured

1 000 001 rows over 120 days and 8 devices, `partition_interval '1 day'`,
`hot_retention '7 days'`, one `CREATE INDEX ON metrics (device, ts)`, sealed once and
analysed. After the seal: 61 170 heap rows in 8 daily partitions with data (15 in the
plan, 7 precreated ahead of `now()`) and 938 831 Iceberg rows in 113 Parquet data
files, boundary `2026-08-06 00:00+00`. An arm64 dev box with pgduck_server and a
MotoServer S3 mock on the same host — no object-store latency. Best of three. The
setup and the plans are in [`README.md`](README.md#performance).

| query | off | on |
| --- | --- | --- |
| whole table, `GROUP BY device` | 979 ms | 327 ms |
| last 30 days (spans the boundary, 188k Iceberg rows) | 256 ms | 325 ms |
| cold only, `ts < now() - interval '30 days'` | 776 ms | 275 ms |
| hot window only, `ts >= now() - interval '1 day'` | 32 ms | 303 ms |
| point lookup, `device = 3`, last 2 hours | 29 ms | 320 ms |
| the same aggregate straight at `metrics_cold` | 40 ms | 40 ms |

Off, the cost is the FDW shipping qualifying Iceberg rows into PostgreSQL to be
aggregated there — roughly 1 µs per row here, so it tracks how much of the cold tier
the query needs. On, it is nearly flat, because it is a floor rather than a slope:
each hot partition is one loopback `postgres_scan_pushdown`. Varying only
`partition_interval` on 100 000 rows over 3 days with `hot_retention '1 day'`, the
hot-window aggregate costs 184 ms at 2 partitions, 311 ms at 9 and 745 ms at 33, or
about 18 ms per partition, against 24–31 ms with the pushdown off. So a short
`partition_interval` over a wide hot window is the worst case, the crossover on the
table above is around 300 000 Iceberg rows, and at 100 000 rows the pushdown does not
pay for itself at all (66 ms off, 321 ms on for the spanning aggregate).

The last row of the table is the one that bounds the remaining work: an all-lake
query is already pushed down whole by default, and the same aggregate over 938 831
rows costs 40 ms straight at the Iceberg tier against 275 ms through the tiered table
with the pushdown on, even though the heap contributes nothing to that query. The
difference is the 15 loopback scans the union keeps in the plan. Collapsing them into
one scan spanning the partitions is the obvious next optimisation.

For the record, the measurement that killed the routing view of §14.2, on a 1M-row
table split 37 089 hot / 987 911 cold over 9 hot partitions: 3975 ms through the view
with the pushdown off, 269 ms with it on, and 21 ms straight at Iceberg. About 250 ms
of the spanning query was the tiering machinery — the per-partition loopback scans
and the view's permanent delta branches. The current design removes the delta
branches and the anti-join; the per-partition floor remains.

---

## 14. Designs considered and rejected

### 14.1 The Iceberg tier as a partition of the hot table

The tidiest-looking design attaches the cold tier as a partition of the parent,
bounded below `B`. Probed against a live cluster (PG 18, a parent with one heap
partition and one lake table attached below it), the reasons it fails are not the
obvious ones:

- **Moving the boundary is multi-relation DDL, not one re-attach.** Widening the
  cold partition's upper bound fails while a hot partition still covers that range
  (`partition "cold_a" would overlap partition "par_a_hot"`), so every seal is a
  `DETACH` of both partitions and two `ATTACH`es, all under `ACCESS EXCLUSIVE` on
  the parent — a heavier version of the lock cost §1 gives as a reason not to use
  native partitioning.
- **Plain indexes are fine; unique ones are not.** `CREATE INDEX` on the parent
  works in either order and heap partitions get real indexes, but `ADD PRIMARY KEY`
  and `CREATE UNIQUE INDEX` both raise `cannot create unique index on partitioned
  table`, `DETAIL: Table "…" contains partitions that are foreign tables`
  (`indexcmds.c:1399`); in the other order, a parent that already has a primary key
  refuses the lake table with `column "ts" in child table "cold_c" must be marked
  NOT NULL`.
- **The pushdown never fires, which is the whole feature.** `HasLakeRTE` walks the
  *parse tree's* range table at `planner_hook` entry, before inheritance expansion.
  The parse tree holds one RTE — the heap parent — so `IsAnyLakeRelation` is false
  and `FullQueryIsPushdownable` is never reached. Measured: a spanning `GROUP BY`
  plans as `HashAggregate → Append → (Foreign Scan, Seq Scan)` identically with the
  GUC on and off, and cold-only queries regress from today's default whole-query
  pushdown.

The third point settles it: partitioning hides the lake table from the exact check
that decides to vectorise. So the cold tier stays outside the table and the union
happens in the plan.

### 14.2 A routing view over a hot tier, a delta and merge-on-read

The design this replaced. `timeseries.create_table()` turned an empty template into
six objects: a view with an `INSTEAD OF` trigger, a partitioned hot heap, the
Iceberg tier, a single-partition `_cold_scan` wrapper for pruning, a `_delta` heap
with `_ts_seq`/`_ts_deleted` columns, and a sequence. Reads merged the delta over
the cold tier by logical key (newest version wins, tombstones suppress); a
`repair()` folded the delta back into Iceberg. Writes below `B` were absorbed by
the delta instead of failing.

It worked, and it was dropped for four measured reasons:

1. **Every write went through a plpgsql `INSTEAD OF` trigger** doing
   `EXECUTE format(...)` per row, so `COPY` and bulk `INSERT` paid a per-row
   plpgsql cost, and the parent could carry no indexes, constraints or defaults of
   its own.
2. **The view could not specialise on dirty partitions.** A view cannot be
   replaced while a statement referencing it runs, and the write path was a trigger
   on that very view: `CREATE OR REPLACE VIEW` from inside it fails with `cannot
   CREATE OR REPLACE VIEW ... because it is being used by active queries in this
   session`. So the delta overlay had to be **permanent** — every cold read carried
   a `NOT EXISTS` anti-join and a `DISTINCT ON` branch, whether or not the delta
   had a single row.
3. **The machinery dominated the query.** §13.3's last baseline: 269 ms through the
   view with the pushdown on against 21 ms straight at Iceberg.
4. **Tier elimination needed a catalog trick.** A `UNION ALL` branch whose
   predicate the query contradicts is *kept* under the default
   `constraint_exclusion = partition`; union-leaf flattening happens only for
   `SELECT *` and does not prune by itself; `constraint_exclusion = on` prunes but
   is session-wide. Hence `_cold_scan`, a range-partitioned wrapper whose only
   partition was the Iceberg table, re-bounded by `seal()` on every boundary
   advance — which pruned only against a literal, not against `date_trunc('day',
   now())`, because a single-partition `Append` is removed at plan time.

The current design keeps finding (4) — it is why §7.4 says what it says — and
discards the rest with the delta. What was given up is mutating sealed history,
which is now an error rather than an overlay (§2).

### 14.3 Base+delta as the primary model, and mirror mode

Earlier revisions of this document specified a base+delta LSM (all writes to a
small heap delta, a background flush folding them into Iceberg, reads merging by
key), and a "mirror mode" where Iceberg is fed by CDC and PostgreSQL keeps the full
deduplicated last N days. Both are strictly more machinery than a boundary split,
and both need the merge-on-read whose cost §14.2 measures. The demand was for
overlapping tiers with a fast routing path, which is what a boundary plus a lagging
copy gives directly.

### 14.4 Row-level security on the extension catalogs

The catalogs were originally granted to `public` with owner policies (`USING
(is_owner(relation))`, plus a `WITH CHECK` requiring ownership of every relation the
row names) and the API was `SECURITY INVOKER`. Two things went wrong:

- **`pg_dump` refuses a table with policies**, and these are
  `pg_extension_config_dump` tables whose contents must survive a dump. That is
  fatal, and it is why §6.2 uses a grant to `pg_monitor` and a C-level ownership
  check instead.
- **RLS hid the row from the very user who had to delete it.** `is_owner()` of an
  already-dropped relation is NULL, not true, so a registration naming a dropped
  table was invisible to its owner's `DELETE` — which is how the cleanup function
  ended up `SECURITY DEFINER` in the first place.

Making the whole API `SECURITY DEFINER` was also tried and reverted: inside a
definer function `current_user` is the definer, so `check_owner()` would succeed for
anybody whenever the definer is a superuser, and there is no portable way to
recover the invoker. The ownership check therefore lives in C (§6.1).

---

## 15. Tests

`tests/pytests/`, run against a live pg_lake test cluster with the in-tree S3 mock.
The `tiered` fixture in `tests/conftest.py` runs one `CREATE TABLE ... USING
timeseries`, which is all it takes to get both tiers and the registration; each test
gets its own S3 prefix, because `DROP TABLE` leaves the Iceberg files behind and
`CREATE TABLE ... USING iceberg` refuses a non-empty location. The worker is pinned
off and maintenance is driven explicitly so that assertions about the boundary are
deterministic; one test re-enables it.

| file | cases | asserts |
| --- | --- | --- |
| `test_tiered_ddl.py` | 23 | what `CREATE TABLE ... USING timeseries` builds and refuses (§5.1), that `ALTER TABLE` reaches both tiers under Iceberg's rules, the `SET (...)` settings, the three refusals, and that `DROP TABLE` takes the cold tier and the registration with it |
| `test_tiered_reads.py` | 12 | every row returned exactly once across the boundary, the lagging copy not double-counted, expansion at every query level, pruning on both sides (§7.4), writes unaffected, cached plans replanned, RLS refused, view-like privileges, `COPY`, and one whole-query-pushdown plan |
| `test_timeseries_maintenance.py` | 12 | the frontier, `sync()` being repeatable and non-authoritative, `seal()` advancing `B` and stopping at a gap, retention bounded by `B`, one `maintain()` pass, the worker, and the ownership/registration refusals |

The pushdown side is tested in `pg_lake_table`:
`test_heap_query_pushdown.py` (12 cases — default-off, one vectorised plan for a
spanning query, a cross-tier join, a heap-only query left untouched, the snapshot
honoured under `REPEATABLE READ`, the writing-transaction fallback, a partitioned
heap tier with and without partitions, and the four ineligibility paths) and
`test_lake_partitioned_parent.py` (4 cases).

---

## 16. Not implemented

- **`month`/`year` partition intervals** (§5).
- **Converting an existing table.** There is no `SET ACCESS METHOD timeseries`, and
  nothing adopts a heap and an Iceberg table as a pair; the invariants §7 relies on
  are established by `CREATE TABLE` (§5.1). Converting would additionally have to
  place the boundary somewhere other than `-infinity`, which nothing does today.
- **Compaction** of the cold tier beyond `apply_retention()`.
- **Plan-time file pruning** for a pushed-down hot-window query (§13.2).
- **`seal()`'s copy is row-at-a-time.** `INSERT INTO <iceberg> SELECT ... FROM
  <heap>` uses the FDW path; the heap pushdown admits read-only queries only
  (`FullQueryIsPushdownable` rejects anything that is not a plain `SELECT`).
- **Mutating sealed history** (§2), and cross-tier uniqueness.
- **A `CustomScan`** that picks per partition between reading Iceberg and reading
  the heap. The planner hook covers the two shapes that matter (heap-only by
  pruning, and the union), and a `CustomScan` would only add the ability to
  specialise per partition — which, without a delta, nothing needs yet.

# pg_lake_timeseries

Make one PostgreSQL relation behave like a live, indexed, mutable time-series table
whose history lives in Apache Iceberg.

- the last **N** days (say 7) live in **your own** partitioned table — a heap, with
  your indexes, constraints and native tuple routing — up to `now()`;
- older data lives in one internally-partitioned pg_lake **Iceberg** table;
- reads are split by a stored **authority boundary**, so every row is returned
  exactly once and a query that stays on one side of the boundary reads only that
  tier;
- a query that *spans* the boundary can run as **one vectorised DuckDB plan**
  instead of shipping Iceberg rows into PostgreSQL — see
  [Performance](#performance).

One `CREATE TABLE ... USING timeseries` makes both tiers, and after that `metrics`
is an ordinary table: `CREATE INDEX`, `ALTER TABLE` and `DROP TABLE` all work, with
the Iceberg tier following the changes it shares. A `planner_hook` rewrites a query
on your table into the union of the two tiers. A background worker extends the
partition frontier ahead of the writes, copies completed partitions into Iceberg,
seals aged partitions (the only thing that moves the boundary) and applies
retention.

Design, correctness argument and the query-optimizer analysis:
[`DESIGN.md`](DESIGN.md).

> **Status:** working and covered by 47 tests in `tests/pytests/`, including the
> vectorised cross-tier plan. Not production-hardened; read [Caveats](#caveats)
> before using it, in particular that whole-query pushdown is off by default and is
> a trade-off rather than a free win.

## Install

Requires `pg_lake_engine`, `pg_lake_iceberg` and `pg_lake_table`. It is an optional
add-on, **not** installed by `CREATE EXTENSION pg_lake CASCADE`.

```sql
CREATE EXTENSION pg_lake_timeseries CASCADE;
```

`pg_lake_timeseries` must be in `shared_preload_libraries`, **after**
`pg_lake_table`: the hook has to add the cold tier before pg_lake decides whether
the query can be pushed down whole.

## Usage

```sql
CREATE TABLE metrics (ts timestamptz NOT NULL, device int, value float8)
USING timeseries WITH (partition_interval = '1 day',
                       hot_retention = '7 days',
                       cold_retention = '365 days',
                       location = 's3://bucket/metrics');
```

That one statement leaves you with a partitioned heap called `metrics`, an Iceberg
table called `metrics_cold` holding the same columns, a registration in
`timeseries.tables`, and enough partitions to write into immediately.

| option | default | |
| --- | --- | --- |
| `time_column` | the table's only `timestamptz` column | what the tiers are divided on, and the heap's partition key |
| `partition_interval` | `1 day` | heap partition width, and the Iceberg tier's `partition_by` transform |
| `hot_retention` | `7 days` | how much history PostgreSQL keeps before a partition is sealed |
| `cold_retention` | never expire | how much history Iceberg keeps after that |
| `precreate_ahead` | `7` | partitions to keep ready ahead of `now()`, in `partition_interval`s |

Anything else — `location`, `catalog`, `max_snapshot_age`, … — is passed to the
Iceberg tier, and pg_lake validates it. `PARTITION BY RANGE (<time column>)` may be
written out instead of relying on `time_column`.

`metrics` is your table: indexes, constraints, defaults, `COPY`, tuple routing and
`pg_dump` behave as they did, and a `SELECT` on it also reads `metrics_cold`:

```sql
CREATE INDEX ON metrics (device, ts);                 -- an ordinary index
INSERT INTO metrics VALUES (now(), 3, 0.5);           -- an ordinary insert
UPDATE metrics SET value = 1.5 WHERE ts > now() - interval '1 hour';

SELECT device, avg(value) FROM metrics
WHERE ts >= now() - interval '30 days' GROUP BY device;   -- reads both tiers
```

`ALTER TABLE` reaches the Iceberg tier for the things the two tiers share — a
column added, dropped, retyped or renamed — and Iceberg's rules apply to it, so a
type change it cannot express fails and changes neither tier. The settings above
are `ALTER TABLE` options of their own:

```sql
ALTER TABLE metrics ADD COLUMN site text;            -- both tiers
ALTER TABLE metrics SET (hot_retention = '3 days');  -- the registration
```

`DROP TABLE metrics` drops the Iceberg tier with it, as an internal dependency, and
leaves the Parquet files in object storage.

Maintenance runs in a background worker, and every step can also be run by hand:

```sql
SELECT timeseries.maintain('metrics');        -- one pass of all four, in order
SELECT timeseries.add_partitions('metrics');  -- extend the frontier ahead of writes
SELECT timeseries.sync('metrics');            -- copy completed partitions to Iceberg
SELECT timeseries.seal('metrics');            -- hand aged ranges over; move the boundary
SELECT timeseries.apply_retention('metrics'); -- expire cold history
```

State is in the catalogs, and in the functions that read them for a table's owner:

```sql
SELECT * FROM timeseries.tiered_table('metrics'::regclass);   -- tiers, boundary, retention
SELECT * FROM timeseries.synced_ranges('metrics'::regclass);  -- what Iceberg has, and since when
SELECT * FROM timeseries.heap_ranges('metrics'::regclass);    -- what PostgreSQL has
SELECT * FROM timeseries.tiered_tables();                     -- every tiered table
```

Only the table's owner can run maintenance on it. The two catalogs those functions
read — `timeseries.tables` and `timeseries.partitions` — grant `SELECT` to
`pg_monitor` and to nobody else, so read them directly only as a superuser or a
member of that role.

## How reads work

Every registered relation is expanded at plan time into

```sql
SELECT … FROM metrics      WHERE ts >= <boundary>
UNION ALL
SELECT … FROM metrics_cold WHERE ts <  <boundary>
```

which is what a view would do, done in a `planner_hook` so it applies to your own
table without renaming it. PostgreSQL owns everything at or above the boundary,
Iceberg everything below it, and only `seal()` moves the boundary — in the same
transaction that copied the range into Iceberg and dropped the heap partition. So
there is no merge, no anti-join and no deduplication on the read path: each range of
time comes from exactly one tier.

How visible that pruning is in `EXPLAIN` depends on whether the cutoff is a constant.
On a table whose boundary is `2026-08-11 00:00+00`, with 10 heap partitions and 8
Iceberg data files:

| `WHERE` | heap side | Iceberg side |
| --- | --- | --- |
| `ts < '2026-08-11'` | gone from the plan | `Foreign Scan`, `Data Files Scanned: 8` |
| `ts < now() - interval '5 days'` | `Append` with `Subplans Removed: 10` | `Foreign Scan`, `Data Files Scanned: 8` |
| `ts >= '2026-08-11'` | all 10 scanned | `Foreign Scan`, `Data Files Scanned: 0`, DuckDB side `EMPTY_RESULT` |
| `ts >= now() - interval '1 hour'` | `Append` with `Subplans Removed: 2` | `Foreign Scan`, `Data Files Scanned: 8`, DuckDB side `EMPTY_RESULT` |

A constant cutoff is pruned at plan time; a `now()`-based one at execution start,
which is why the heap side reports `Subplans Removed` instead of disappearing. The
Iceberg side never disappears — PostgreSQL will not exclude a foreign table on a
contradictory qual — and its `Data Files Scanned` is the file list pg_lake built at
plan time, which a `now()` cutoff cannot narrow. Nothing is read from those files:
DuckDB folds the contradictory predicate to `EMPTY_RESULT`, and a hot-only aggregate
on the million-row table below costs 32 ms.

`SET pg_lake_timeseries.expand_tiered_tables TO off` reads the heap alone.

## Performance

A query touching one tier is a plain single-tier query and needs nothing special.
A query that *spans* the boundary is the interesting case: PostgreSQL's FDW cannot
push a *partial* aggregate, so by default every qualifying Iceberg row is shipped
into PostgreSQL and aggregated there. Turning on whole-query pushdown moves the
union and the aggregate into DuckDB:

```sql
SET pg_lake_table.enable_heap_query_pushdown TO on;
```

The heap tier is then read back over a loopback connection with
`postgres_scan_pushdown`, pinned to the snapshot of the query that asked for it, so
both tiers see the same instant. One `Custom Scan (Query Pushdown)` replaces the
`Append`, the `HASH_GROUP_BY` runs above the union inside DuckDB, and the window
predicate reaches each heap scan as a `Filters:` entry rather than being applied
after the rows are shipped.

### The table the numbers were measured on

```sql
CREATE TABLE metrics (ts timestamptz NOT NULL, device int, value float8)
USING timeseries WITH (partition_interval = '1 day',
                       hot_retention = '7 days',
                       location = 's3://testbucket/metrics');

-- one wide partition below the hot window, so 120 days of history can be written
-- through the heap and handed to Iceberg by a single seal()
CREATE TABLE metrics_history PARTITION OF metrics
FOR VALUES FROM ('2026-04-08 00:00:00+00') TO ('2026-08-06 00:00:00+00');

INSERT INTO metrics
SELECT ts, (random() * 7)::int, random()
FROM generate_series(now() - interval '120 days', now(),
                     interval '10.368 seconds') ts;

CREATE INDEX ON metrics (device, ts);

SELECT timeseries.seal('metrics');   -- 4.3 s; moves the boundary to 2026-08-06
ANALYZE metrics;                     -- a partitioned parent has relpages = -1 until
                                     -- it is analyzed, and pg_lake needs the estimate
```

1 000 001 rows over 120 days, 8 devices. After the seal: **61 170 rows** in the heap,
in 8 daily partitions with data (15 appear in plans — 7 are precreated ahead of
`now()`), and **938 831 rows** in `metrics_cold`, in 113 Parquet data files, with the
boundary at `2026-08-06 00:00+00`.

An arm64 Linux dev box, with pgduck_server and a MotoServer S3 mock on the same host:
there is no object-store latency in these numbers, so the Iceberg side is faster here
than it would be against real S3. Best of 3 runs.

### The queries

| query | off | on |
| --- | --- | --- |
| `SELECT device, avg(value) FROM metrics GROUP BY device` | 979 ms | **327 ms** |
| … `WHERE ts >= now() - interval '30 days'` (spans the boundary, 188k Iceberg rows) | **256 ms** | 325 ms |
| … `WHERE ts < now() - interval '30 days'` (cold only) | 776 ms | **275 ms** |
| … `WHERE ts >= now() - interval '1 day'` (hot only) | **32 ms** | 303 ms |
| `SELECT * FROM metrics WHERE device = 3 AND ts >= now() - interval '2 hours'` | **29 ms** | 320 ms |
| `SELECT device, avg(value) FROM metrics_cold GROUP BY device` (one tier, pushed down either way) | 40 ms | 40 ms |

There are two different costs in that table. With pushdown **off**, the FDW ships
every qualifying Iceberg row into PostgreSQL and aggregates there, at roughly 1 µs
per row on this machine, so the time tracks how much of the Iceberg tier the query
needs: 939k rows 979 ms, 188k rows 256 ms, none of it 32 ms. With pushdown **on** the
time is nearly flat at 275–327 ms across all five, including a point lookup that
touches almost no data, because it is dominated by fixed per-scan overhead — one
loopback `postgres_scan_pushdown` per heap partition, 15 of them here. The crossover
on this table is around 300 000 Iceberg rows.

So: turn it on for a whole-table or cold-heavy aggregate, leave it off for anything
that stays inside the hot window. The GUC is `PGC_USERSET` — set it on the session or
transaction that runs the analytical query, not globally.

### The two plans for the spanning query

```sql
SELECT device, avg(value) FROM metrics
WHERE ts >= now() - interval '30 days' GROUP BY device;
```

Off: PostgreSQL aggregates above an `Append` of 15 heap scans and one `Foreign Scan`
that returns 188k rows (`EXPLAIN (VERBOSE, COSTS OFF)`, heap partitions elided):

```
HashAggregate
  Group Key: metrics_1.device
  ->  Append
        ->  Subquery Scan on metrics_1
              ->  Append
                    ->  Seq Scan on public.metrics_20260806t0000 metrics_4
                          Filter: ((metrics_4.ts >= '2026-08-06 00:00:00+00'::timestamp with time zone) AND (metrics_4.ts >= (now() - '30 days'::interval)))
                    ...  13 more
                    ->  Seq Scan on public.metrics_20260820t0000 metrics_18
                          Filter: ((metrics_18.ts >= '2026-08-06 00:00:00+00'::timestamp with time zone) AND (metrics_18.ts >= (now() - '30 days'::interval)))
        ->  Subquery Scan on metrics_2
              ->  Foreign Scan on public.metrics_cold
                    Engine: DuckDB
                    Data Files Scanned: 113
                    ->  READ_PARQUET
                          Filters: ts>='2026-07-14 08:10:14+00'::TIMESTAMP WITH TIME ZONE AND ts<'2026-08-06 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                          Estimated Cardinality: 187766
```

On: one custom scan, and the aggregate runs inside DuckDB above a union of the 15 heap
partitions and the Parquet files:

```
Custom Scan (Query Pushdown)
  Engine: DuckDB
  Data Files Scanned: 113
  Vectorized SQL:  SELECT "device", "avg"("value") AS "avg"
   FROM ( SELECT ...
           FROM (SELECT ts, device, value FROM public.metrics_20260806t0000 UNION ALL
                 ... UNION ALL
                 SELECT ts, device, value FROM public.metrics_20260820t0000) "metrics_3"
          WHERE "metrics_3"."ts" >= '2026-08-06 00:00:00+00'::timestamp with time zone
        UNION ALL
         SELECT ... FROM public.metrics_cold "metrics_cold"
          WHERE "metrics_cold"."ts" < '2026-08-06 00:00:00+00'::timestamp with time zone) "metrics"
  WHERE ("ts" >= ('2026-08-13 08:10:15+00'::timestamptz - '30 days'::interval))
  GROUP BY "device"
  ->  HASH_GROUP_BY
        Groups: #0
        Aggregates: avg(#1)
        ->  PROJECTION
              ->  UNION
                    ...
                          ->  POSTGRES_SCAN_PUSHDOWN
                                Table: metrics_20260806t0000
                                Filters: ts>='2026-08-06 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                    ...  one per heap partition
                    ->  READ_PARQUET
                          Filters: ts>='2026-07-14 08:10:15+00'::TIMESTAMP WITH TIME ZONE AND ts<'2026-08-06 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                          Estimated Cardinality: 187766
```

The heap branches of that union are each

```sql
postgres_scan_pushdown('host=/tmp port=25778 dbname=postgres user=app_user',
                       'public', 'metrics_20260806t0000',
                       snapshot => '00000001-00000022-1')
```

with the same `snapshot` argument on every one of them, which is what makes the two
tiers agree on one instant.

### The per-partition floor

100 000 rows over 3 days, `hot_retention '1 day'`, varying only
`partition_interval`, then

```sql
SELECT device, avg(value) FROM floor WHERE ts >= now() - interval '1 day'
GROUP BY device;
```

| `partition_interval` | `1 day` | `3 hours` | `45 minutes` |
| --- | --- | --- | --- |
| heap partitions with data | 2 | 9 | 33 |
| pushdown off | 24 ms | 30 ms | 31 ms |
| pushdown on | 184 ms | 311 ms | 745 ms |

About 18 ms per heap partition, paid before any data is read, which makes a wide hot
window with a short `partition_interval` the worst case for the pushdown. Small tables
never earn it back either: at 100 000 rows over the same 120 days, the spanning
aggregate above is 66 ms off and 321 ms on.

The same floor is why the cold-only query costs 275 ms with pushdown on while the
identical aggregate run straight at `metrics_cold` costs 40 ms: the union keeps a
loopback scan per heap partition in the plan even when the heap contributes no rows.
Collapsing those into one scan spanning the partitions is the obvious next
optimisation, and until it lands this is a per-query tool rather than something to
enable globally.

## Caveats

- **Sealed history is immutable through PostgreSQL.** Below the boundary there is no
  heap partition to hold a change, so an `INSERT` there fails with "no partition of
  relation" and an `UPDATE`/`DELETE` matches nothing. Rewriting cold history is an
  Iceberg-side operation.
- **A write beyond the frontier also fails** with "no partition of relation" — there
  is deliberately no `DEFAULT` partition. Keep the worker running, or call
  `add_partitions()`.
- **Row-level security on either tier makes queries fail.** Policies are applied
  before the hook runs, so adding a second tier afterwards would return rows a
  policy would have filtered; the hook raises instead
  ([`DESIGN.md`](DESIGN.md) §7.2).
- **`pg_dump` by a non-superuser needs `pg_monitor` or `pg_read_all_data`** to dump
  the two configuration tables ([`DESIGN.md`](DESIGN.md) §6.2).
- **Whole-query pushdown is off by default** and has to be opted into per query.
  See the table above: a 3x win for a whole-table aggregate, a 2.8x win for a
  cold-heavy one, roughly a wash for a spanning aggregate that only needs a few
  hundred thousand Iceberg rows, and a 10x loss inside the hot window.
- **Pushdown costs plan-time tier elimination.** With it on the whole query goes to
  DuckDB, so PostgreSQL no longer prunes the cold tier for a `WHERE ts >= <recent>`
  query. The answer is the same, but the cold Parquet files are opened to get it
  ([`DESIGN.md`](DESIGN.md) §13.2).
- **Pushdown is skipped in a writing transaction.** The loopback connection reads an
  exported snapshot, which cannot see the driving transaction's own uncommitted
  writes, so a transaction that has already written falls back to the normal plan.
  Same inside a subtransaction.
- **The Iceberg copy lags.** It is only as fresh as the last `sync()`, and the
  boundary only moves on `seal()`. That does not affect answers — everything at or
  above the boundary is served from the authoritative heap — but the Iceberg table
  on its own is not a complete copy, so don't read it directly and expect recent
  data ([`DESIGN.md`](DESIGN.md) §9.2).
- **`partition_interval` must be a fixed-length interval.** Hour/day/week
  granularities work; `month` and `year` are rejected.
- **The time column is fixed.** It cannot be dropped, retyped or renamed: it is the
  heap's partition key, the Iceberg tier's partition field and the type of the
  boundary.
- **Type changes are Iceberg's promotions only** — `int -> bigint`,
  `float -> double`, wider `decimal` — and a `USING` expression is refused, because
  it would rewrite the heap's values and leave Iceberg's as they were.
- **`ADD COLUMN ... DEFAULT` fills the hot tier only.** Iceberg reads a field its
  data files lack as `NULL`, so rows below the boundary have no default.
- **`TRUNCATE` is refused**, since it would empty the heap and leave the history
  Iceberg owns in place. Truncating a single partition is allowed.
- **An existing table cannot be converted.** There is no `SET ACCESS METHOD
  timeseries` and no function that adopts a heap and an Iceberg table as a pair; the
  invariants the planner hook relies on are established by `CREATE TABLE`.
- **`DROP TABLE` leaves the data files.** The Iceberg tier is dropped with the heap,
  but the Parquet and metadata files stay in object storage, as they do for any
  pg_lake Iceberg table.
- **Uniqueness is hot-tier only.** A unique index on your table says nothing about
  Iceberg's rows, and nothing checks across the boundary.
- **Anything that reads the heap without planning a query sees the hot tier only** —
  logical replication, for instance. That is the right behaviour for replication,
  but worth knowing.
- **Not production-hardened.** Concurrent DDL against the tiers is not covered by
  tests.

## Why not existing options

- **TimescaleDB** is not available in this environment.
- **Native partitioning with Iceberg foreign-table partitions** gives one foreign
  table per partition, partition management takes heavy locks, unique indexes are
  rejected outright, and — measured — a spanning query over such a parent is never
  pushed down whole, because pg_lake looks for a lake relation in the parse tree,
  before inheritance expansion, and finds only the heap parent
  ([`DESIGN.md`](DESIGN.md) §14.1).
- **A plain `UNION ALL` view** over a heap and an Iceberg table routes correctly by
  time but loses aggregate pushdown for any spanning query, for the FDW reason
  above, and cannot be your table — you have to write through the view
  ([`DESIGN.md`](DESIGN.md) §14.2).

`pg_lake_timeseries` keeps *one* internally-partitioned Iceberg table next to your
own indexed partitioned table, which removes the first two problems, and solves the
third by pushing the spanning query down whole. Configuration of the reverse
connection is `pg_lake_table`'s `enable_heap_query_pushdown` and
`heap_pushdown_dsn`; details in [`DESIGN.md`](DESIGN.md) §13.

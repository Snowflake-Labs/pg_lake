# pg_lake_timeseries

Make one PostgreSQL relation behave like a live, indexed, mutable time-series
table whose bulk lives in Apache Iceberg.

- the last **N** days (say 7) live in PostgreSQL, indexed and mutable, up to `now()`;
- the last **M** days (say 365) live in one internally-partitioned pg_lake Iceberg
  table, up to the last materialisation;
- reads are routed by a stored **authority boundary**, so every row is returned
  exactly once, and a query that stays on one side of the boundary is pruned to a
  single tier at plan time;
- a query that *spans* the boundary can run as **one vectorised DuckDB plan**
  instead of shipping Iceberg rows into PostgreSQL — see
  [Performance](#performance).

Updates and upserts are allowed in the PostgreSQL tier. Mutations that reach
already-sealed Iceberg partitions land in a delta, are merged over Iceberg on
read, and are folded back in by a background repair. A background worker seals
aged partitions into Iceberg, moves the heap partition frontier without DDL on the
insert path, repairs mutated cold partitions, and applies retention.

Design, correctness argument and the query-optimizer analysis:
[`DESIGN.md`](DESIGN.md) (§13 the tiering design, §18 what was actually built and
where it differs).

> **Status:** working and covered by `tests/pytests/test_timeseries.py`, including
> the vectorised cross-tier plan. Not production-hardened; read
> [Caveats](#caveats) before using it, in particular that whole-query pushdown is
> off by default and is a trade-off rather than a free win.

## Install

Requires `pg_lake_engine`, `pg_lake_iceberg` and `pg_lake_table`. It is an
optional add-on, **not** installed by `CREATE EXTENSION pg_lake CASCADE`.

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
mutable) and `metrics_cold` (one internally-partitioned Iceberg table). Read and
write it as if it were the original table:

```sql
INSERT INTO metrics VALUES (now(), 3, 0.5);           -- routed to the hot tier
INSERT INTO metrics VALUES (now() - interval '30 days', 3, 0.5);  -- to the delta

UPDATE metrics SET value = 1.5
WHERE ts = '2026-08-12 10:00:00+00' AND device = 3;
DELETE FROM metrics WHERE ts < now() - interval '90 days' AND device = 3;

SELECT device, avg(value) FROM metrics
WHERE ts >= now() - interval '30 days' GROUP BY device;
```

A write is routed by comparing its timestamp against the boundary: at or above it
goes to the heap partitions, below it goes to the per-table delta and is merged
over Iceberg on read.

Pass `upsert => true` to `create_table()` (which requires `key_columns`, and those
must include the time column) to have a write that collides on the key replace the
existing row instead of duplicating it.

Maintenance runs in a background worker, but every step can also be run by hand:

```sql
SELECT timeseries.maintain('metrics');   -- one pass: extend, repair, sync, seal, expire
SELECT timeseries.sync('metrics');       -- materialise past hot partitions into Iceberg
SELECT timeseries.seal('metrics');       -- move the boundary past aged partitions
SELECT timeseries.repair('metrics');     -- fold the delta back into Iceberg
SELECT timeseries.apply_retention('metrics');
```

State is in the catalogs:

```sql
SELECT * FROM timeseries.tables;       -- tiers, boundary, key columns, retention
SELECT * FROM timeseries.partitions;   -- per-partition tier and dirty state
```

`timeseries.drop_table('metrics')` unregisters a table and keeps its data unless
`drop_data => true`.

## Performance

A query touching one tier is a plain single-tier query and needs nothing special.
A query that *spans* the boundary is the interesting case: PostgreSQL's FDW cannot
push a *partial* aggregate, so by default every qualifying Iceberg row is shipped
into PostgreSQL and aggregated there. Turning on whole-query pushdown moves the
entire query — union, delta anti-join and aggregate — into DuckDB:

```sql
SET pg_lake_table.enable_heap_query_pushdown TO on;
```

The heap tier is then read back over a loopback connection with
`postgres_scan_pushdown`, pinned to the snapshot of the query that asked for it,
so both tiers see the same instant.

### The two plans

1 000 000 rows, 8 devices, 8 daily hot partitions (57 340 rows) and 7 Iceberg data
files (942 660 rows), boundary at midnight. `EXPLAIN (ANALYZE, VERBOSE)` of
`SELECT device, count(*), avg(value) FROM metrics GROUP BY device ORDER BY device`.

**Off (default).** Both tiers are scanned in PostgreSQL, and the ~940k Iceberg
rows are sorted on disk for the delta anti-join before being aggregated (abridged:
`cost=` and `Output:` lines dropped, 7 empty hot partitions elided):

```
Sort (actual time=2214.893..2214.902 rows=8)
  ->  HashAggregate (actual time=2214.883..2214.894 rows=8)
        ->  Append (actual time=0.011..2035.102 rows=1000000)
              ->  Append (actual rows=57340)                     -- 8 hot partitions
                    ->  Seq Scan on metrics_hot_20260812t0000 (actual rows=57340)
                    ...
              ->  Merge Right Anti Join (actual rows=942660)      -- delta overlay
                    ->  Index Only Scan on metrics_delta d (actual rows=0)
                    ->  Sort (actual time=1629.466..1722.923 rows=942660)
                          Sort Method: external sort  Disk: 35128kB
                          ->  Foreign Scan on metrics_cold c (actual time=9.090..1209.552 rows=942660)
                                Engine: DuckDB
Execution Time: 2221.775 ms
```

**On.** One node; nothing but the 8 result rows crosses back into PostgreSQL.
This is the verbatim `EXPLAIN (ANALYZE, VERBOSE)` output, with the repetitive
partition branches elided where marked:

```
Custom Scan (Query Pushdown)  (cost=0.00..0.00 rows=0 width=0) (actual time=228.720..228.730 rows=8.00 loops=1)
  Output: pushdown_query.device, pushdown_query.count, pushdown_query.avg
  Engine: DuckDB
  Data Files Scanned: 7
  Deletion Files Scanned: 0
  Vectorized SQL:  SELECT "device",
    "count"(*) AS "count",
    "avg"("value") AS "avg"
   FROM ( SELECT "metrics_hot"."ts",
            "metrics_hot"."device",
            "metrics_hot"."value"
           FROM (SELECT ts, device, value FROM public.metrics_hot_20260812t0000 UNION ALL SELECT ts, device, value FROM public.metrics_hot_20260813t0000 UNION ALL ... ) "metrics_hot"("ts", "device", "value")
          WHERE ("metrics_hot"."ts" >= ('2026-08-12 00:00:00+00'::"text")::timestamp with time zone)
        UNION ALL
         SELECT "c"."ts",
            "c"."device",
            "c"."value"
           FROM public.metrics_cold_scan "c"("ts", "device", "value")
          WHERE (("c"."ts" < ('2026-08-12 00:00:00+00'::"text")::timestamp with time zone) AND (NOT (EXISTS ( SELECT 1
                   FROM public.metrics_delta "d"("ts", "device", "value", "_ts_seq", "_ts_deleted")
                  WHERE (("d"."ts" = "c"."ts") AND ("d"."device" = "c"."device"))))))
        UNION ALL
         SELECT "l"."ts",
            "l"."device",
            "l"."value"
           FROM ( SELECT DISTINCT ON ("metrics_delta"."ts", "metrics_delta"."device") ...
                   FROM public.metrics_delta "metrics_delta"(...)
                  WHERE ("metrics_delta"."ts" < ('2026-08-12 00:00:00+00'::"text")::timestamp with time zone)
                  ORDER BY "metrics_delta"."ts", "metrics_delta"."device", "metrics_delta"."_ts_seq" DESC) "l"(...)
          WHERE (NOT "l"."_ts_deleted")) "metrics"
  GROUP BY "device"
  ORDER BY "device"
  ->  ORDER_BY
        Order By: metrics.device ASC
        ->  HASH_GROUP_BY
              Groups: #0
              Estimated Cardinality: 34738
              ->  PROJECTION
                    Estimated Cardinality: 50369
                    ->  UNION
                          ->  UNION                       -- one level per hot partition
                                ->  PROJECTION
                                      ->  POSTGRES_SCAN_PUSHDOWN
                                            Table: metrics_hot_20260812t0000
                                            Filters: ts>='2026-08-12 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                                            Estimated Cardinality: 12663
                                ->  PROJECTION
                                      ->  POSTGRES_SCAN_PUSHDOWN
                                            Table: metrics_hot_20260813t0000
                                            Filters: ts>='2026-08-12 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                                -- ... 6 more partitions, identical shape
                                ->  LEFT_DELIM_JOIN
                                      Join Type: ANTI
                                      ->  READ_PARQUET
                                            Filters: ts<'2026-08-12 00:00:00+00'::TIMESTAMP WITH TIME ZONE
                                            Function: READ_PARQUET
                                            Estimated Cardinality: 188532
                                      ->  HASH_JOIN
                                            Join Type: ANTI
                                            ->  COLUMN_DATA_SCAN
                                            ->  PROJECTION
                                                  ->  HASH_JOIN
                                                        ->  DELIM_SCAN
                                                        ->  POSTGRES_SCAN_PUSHDOWN
                                                              Table: metrics_delta
                          ->  ORDER_BY                    -- the delta's own branch
                                ->  FILTER
                                      Expression: (NOT _ts_deleted)
                                      ->  HASH_GROUP_BY
                                            ->  POSTGRES_SCAN_PUSHDOWN
                                                  Table: metrics_delta
                                                  Filters: ts<'2026-08-12 00:00:00+00'::TIMESTAMP WITH TIME ZONE
Planning Time: 0.684 ms
Execution Time: 232.091 ms
```

Two things to read off this plan. The aggregate really does run in DuckDB
(`HASH_GROUP_BY` above the union, not in PostgreSQL), and the window predicate
reaches each heap scan as a `Filters:` entry rather than being applied after the
rows are shipped. But also: there are **ten** `POSTGRES_SCAN_PUSHDOWN` nodes here —
eight hot partitions plus two for the delta — and each is its own loopback
connection. That is where the floor in the next section comes from.

Each `public.metrics_hot_*` in the `Vectorized SQL` is a `postgres_scan_pushdown()`
call at execution time; `EXPLAIN` prints the plain relation names so the SQL stays
readable, while the DuckDB tree below it shows the actual scan operators.

### Measured, best of 3 (same table)

| query | off | on |
| --- | --- | --- |
| whole table, `GROUP BY device` | 2231 ms | **388 ms** |
| last 3 days (spans boundary) | 878 ms | **386 ms** |
| cold only | 2074 ms | **376 ms** |
| hot window only | **18 ms** | 381 ms |
| point lookup (`device = 3`, last 2 hours) | **15 ms** | 359 ms |

Spanning and cold-heavy aggregates get 2–6x faster; hot-window and point queries
get ~20x *slower*. The pushdown is therefore a per-query decision, and the GUC is
`PGC_USERSET` — set it on the session or transaction that runs the analytical
query, not globally.

The "on" column is nearly flat — 359–388 ms across five very different queries,
including a point lookup that touches almost no data. That flatness is the point:
the pushed-down plan's cost is dominated by fixed per-scan overhead, not by how
much data the query reads. Each hot partition is one loopback
`postgres_scan_pushdown` (ten of them in the plan above), and on this machine each
costs roughly 19 ms:

| hot partitions | 1 | 8 | 32 |
| --- | --- | --- | --- |
| pushdown on | 25 ms | 154 ms | 608 ms |
| pushdown off | 5 ms | 5 ms | 5 ms |

So the rule is: **turn it on when the alternative is over a second** — a spanning or
cold-heavy aggregate over a large Iceberg tier — and leave it off otherwise. A wide
hot window with a short `partition_interval` is the worst case, because the
per-partition cost is paid before any data is read. Below roughly 100 000 rows the
pushdown never pays for itself: at 100k the same spanning aggregate goes from
233 ms off to 372 ms on.

Reducing that floor — one scan spanning the partitions instead of one per
partition — is the obvious next optimisation, and until it lands this is a
per-query tool rather than something to enable globally.

### What the tiering itself costs

Both columns above are the *view*. A third baseline is more revealing: the same
aggregate straight at the Iceberg tier, which needs none of this machinery because
an all-lake query is already pushed down whole by default. On a 1 000 000-row table
split 37 089 hot / 987 911 cold over 9 hot partitions:

| `SELECT device, count(*), avg(value) FROM … GROUP BY device` | best of 3 |
| --- | --- |
| through the view, pushdown off | 3975 ms |
| through the view, pushdown on | 269 ms |
| straight at `metrics_cold` — 988k Iceberg rows, no view, no delta | **21 ms** |

DuckDB aggregates the entire cold tier in 21 ms. The spanning query takes 269 ms to
do nearly the same work plus 37k heap rows, so roughly **250 ms of it is the
tiering machinery** — the per-partition loopback scans and the delta branches —
rather than the data. Read the pushdown's speedup with that in mind: its advantage
is mostly that it avoids a pathological baseline, not that 269 ms is a good number.

## Caveats

- **Whole-query pushdown is off by default** and has to be opted into per query.
  See the table above: it is a large win for spanning and cold-heavy aggregates
  and a large loss for hot-window and point queries.
- **Pushdown costs plan-time tier elimination.** With it on, the whole view goes
  to DuckDB, so PostgreSQL no longer prunes the cold tier for a
  `WHERE ts >= <recent>` query. The answer is the same, but the cold Parquet files
  are opened to get it ([`DESIGN.md`](DESIGN.md) §18.12).
- **Pushdown is skipped in a writing transaction.** The loopback connection reads
  an exported snapshot, which cannot see the driving transaction's own uncommitted
  writes, so a transaction that has already written falls back to the normal plan.
  Same for a subtransaction.
- **Row-level security, `TABLESAMPLE` and legacy inheritance parents** are never
  pushed down; those queries fall back silently.
- **The delta merge runs on every cold read**, not only on recently-mutated
  partitions, so it costs an anti-join even when the delta is empty
  ([`DESIGN.md`](DESIGN.md) §18.4).
- **The Iceberg copy lags.** It is only as fresh as the last `sync()`, and the
  boundary only moves on `seal()`. That does not affect answers — everything at or
  above the boundary is served from the authoritative hot tier — but the Iceberg
  table on its own is not a complete copy, so don't read it directly and expect
  recent data ([`DESIGN.md`](DESIGN.md) §18.6).
- **`create_table()` only converts an empty ordinary table.** It raises on a table
  that already has rows — load history into the Iceberg tier after conversion —
  and the original name becomes a view, with the data under `<name>_hot` /
  `<name>_cold`.
- **`partition_interval` must be a fixed-length interval.** Hour/day/week
  granularities work; `month` and `year` are rejected.
- **A table created without `key_columns` cannot be updated or deleted from.**
  It is append-only: `UPDATE` and `DELETE` through the view raise, because there is
  no key to identify a row across versions.
- **Not production-hardened.** Concurrent DDL against the tiers is not covered by
  tests.

## Why not existing options

- **TimescaleDB** is not available in this environment.
- **Native partitioning with Iceberg foreign-table partitions** gives one foreign
  table per partition, partition management takes heavy locks, and — measured —
  a spanning query over such a parent is never pushed down whole, because the
  planner sees only the heap parent before inheritance expansion
  ([`DESIGN.md`](DESIGN.md) §18.15).
- **A plain `UNION ALL` view or partitioned parent** over a heap and an Iceberg
  table routes correctly by time but loses aggregate pushdown for any spanning
  query, for the FDW reason above ([`DESIGN.md`](DESIGN.md) §13.5).

`pg_lake_timeseries` keeps *one* internally-partitioned Iceberg table next to the
indexed heap tier, which removes the first two problems, and solves the third by
pushing the spanning query down whole. Configuration of the reverse connection is
`pg_lake_table`'s `enable_heap_query_pushdown` and `heap_pushdown_dsn`; details in
[`DESIGN.md`](DESIGN.md) §18.11–§18.12.

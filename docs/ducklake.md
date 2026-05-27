# DuckLake tables in pg_lake

The `pg_lake_ducklake` extension hosts a [DuckLake](https://ducklake.select)
catalog inside PostgreSQL. PostgreSQL stores the catalog tables under
`lake_ducklake.*`; DuckDB's ducklake extension can ATTACH the same
catalog over the postgres bridge and read/write DuckLake tables
through the public.ducklake_* views.

This document captures the architectural decisions that aren't
obvious from the code, and the moving parts that have already burned
us at least once.

## Setup

```sql
SET pg_lake_ducklake.default_location_prefix = 's3://bucket/lake/';
CREATE EXTENSION pg_lake_ducklake CASCADE;
```

DuckDB-side ATTACH:

```sql
ATTACH 'postgres:host=... dbname=...' AS dl (TYPE DUCKLAKE);
```

`METADATA_SCHEMA` defaults to `public` on the DuckLake side, so the
extension is non-relocatable with `schema = public`. Re-locating means
`DROP EXTENSION; CREATE EXTENSION pg_lake_ducklake SCHEMA other`.

## `default_location_prefix` semantics

Setting the GUC matters at exactly two moments:

* **`CREATE EXTENSION`**: if the GUC is set, the install seed
  materialises `lake_ducklake.metadata.data_path` from it. The catalog
  is now usable as-is from a DuckDB ATTACH that omits `DATA_PATH`.
* **First PG-side `CREATE TABLE ... USING ducklake`** with no
  `data_path` row yet: `DucklakeRegisterTable` seeds it.

Once `lake_ducklake.metadata.data_path` exists, the GUC is no longer
consulted -- changing it does NOT redirect new writes. Relative
schema/table paths stay anchored to the original prefix. To genuinely
move a catalog you have to UPDATE the metadata row and physically move
the data.

## Schema lifecycle

There are four hooks that keep `lake_ducklake.schema` in sync with
`pg_namespace`:

| Direction | Trigger | What happens |
| --- | --- | --- |
| install seed | `pg_lake_ducklake--3.4.sql` | Iterates `pg_namespace` (filtered: not `pg_*`, not `information_schema`, not extension-owned) and inserts a row per visible PG schema. |
| PG `DROP SCHEMA` | `object_access_hook` (OAT_DROP, NamespaceRelationId) in `drop_schema.c` | End-snapshots the matching `lake_ducklake.schema` row. PG schema itself is dropped by core. |
| PG `CREATE SCHEMA` (revival only) | same hook, OAT_POST_CREATE | If an end-snapshotted row exists for the same name, INSERT a new live row reusing its `schema_id` / `schema_uuid`. Brand-new schemas are NOT auto-tracked (broad propagation deferred). |
| DuckDB `CREATE SCHEMA` | `created_schema:` op in `ducklake_snapshot_changes` replay (`replay.c`) | Runs `CREATE SCHEMA IF NOT EXISTS` on the PG side under the `DucklakeInDDLReplay` guard. |
| DuckDB `DROP SCHEMA` | `dropped_schema:` op | NOT propagated. PG schema is preserved (it may have grants the user wants). The lake_ducklake.schema row is end-snapshotted via DuckDB's INSTEAD-OF UPDATE on the public view. |

The PG-side hook at `OAT_POST_CREATE` fires from `NamespaceCreate`
*before* the surrounding command's `CommandCounterIncrement`, so the
just-inserted `pg_namespace` row isn't yet visible to syscache lookups.
`DucklakeReviveSchemaByOid` calls `CommandCounterIncrement()` itself
before reading.

## SPI conventions

All SPI from `pg_lake_ducklake` (catalog.c, replay.c,
data_file_schema.c, drop_schema.c) goes through
`DucklakeBeginPrivilegedSPI` / `DucklakeEndPrivilegedSPI` (in
`spi_priv.c`). These are function-form equivalents of pg_extension_base's
`SPI_START_EXTENSION_OWNER` macro. They:

* run as the pg_lake_ducklake extension owner so users without write
  access to `lake_ducklake.*` can still drive writes through their
  foreign tables;
* lock `search_path` to `pg_catalog, pg_temp` under
  `SECURITY_RESTRICTED_OPERATION` so unqualified operators / functions
  in the SPI queries can't be hijacked;
* mute `auto_explain`, `pgaudit`, and `pg_stat_statements` for the
  duration so internal queries don't pollute monitoring.

Function-form (rather than macro) lets one C function open multiple SPI
sessions without macro-local variables (`_savedUserId` etc.) colliding.

In addition to the search_path lock-down, SPI queries qualify
operators in predicate contexts (`WHERE foo OPERATOR(pg_catalog.=) ...`)
and prefix built-ins (`pg_catalog.coalesce`, `pg_catalog.count`, etc.).
Don't apply the predicate substitution to SET clauses in UPDATE
statements -- the `=` there is parser syntax, not an operator.

The install script in `pg_lake_ducklake--3.4.sql` does
`SET LOCAL search_path = pg_catalog` at the top so its procedural
inserts get the same protection, even though they're not SPI.

## Concurrency / OCC

DuckLake's spec puts the conflict detector on
`lake_ducklake.snapshot.snapshot_id`'s primary key. DuckDB's ducklake
extension catches the duplicate-PK error on its INSTEAD-OF inserts and
runs a retry+conflict-analysis loop.

`pg_lake_ducklake` doesn't have an equivalent retry loop: PG-side
writers (the PRE_COMMIT hook in `track_changes.c`) would just see a
raw constraint violation that aborts the whole transaction. We
serialize them instead:

* `DucklakeCreateSnapshot` takes a transaction-scoped advisory lock
  (`LOCKTAG_ADVISORY`, class 102, MyDatabaseId, objid 0/0). Class 101 is
  pg_lake_table's per-relation update lock; ours is a single global
  lock on snapshot allocation.
* PG-side UPDATE/DELETE additionally inherits `LockTableForUpdate` from
  pg_lake_table's BeginForeignScan (class 101, per-relation), so two
  PG UPDATEs to the same table serialize at statement start.
* External (DuckDB) writers don't take either lock -- their PK retry
  is unaffected.

`row_id_start` is allocated outside the snapshot lock today; concurrent
PG INSERTs to the same table could pick the same start. Tightening
this would mean widening the lock to cover all of
`ApplyTrackedDucklakeChanges`. Not done yet.

## Open spec gaps

Tracked in approximate priority order, not in our planning system:

* DuckLake `CREATE VIEW` -- DuckDB persists `lake_ducklake.view` rows
  but PG has no view materialisation; views are invisible across the
  bridge.
* Time-travel reads -- the catalog is fully versioned by
  `begin_snapshot/end_snapshot`, but no GUC plumbed to
  `DucklakeGetDataFiles` to read at a historical snapshot_id.
* Concurrent writer / OCC retry on the PG side. We serialise instead;
  the spec wants retry on detected conflict.
* `dropped_schema` op replay (intentionally not propagated; preserves
  PG grants).
* `created_view`, `altered_view`, `dropped_view`, `inlined_*`,
  `merge_adjacent`, `rewrite_delete` snapshot_changes ops: not
  propagated. Most are no-ops for our setup (we disable inlining;
  compaction is already handled).
* Niche metadata not surfaced: `column_tag`, `tag`, `sort_info`,
  `sort_expression`, `column_mapping` (Iceberg-compat name mapping),
  `name_mapping`, encryption keys on `data_file`, variant column
  stats, DuckDB macros (`macro`, `macro_impl`, `macro_parameters`).

## Watch-outs

* `IsExtensionCreated(PgLakeDucklake)` returns true throughout
  `DROP EXTENSION pg_lake_ducklake CASCADE` until the very end, while
  the schema's tables are dropped first. Hooks that touch
  `lake_ducklake.*` need a second guard
  (`LakeDucklakeSchemaCatalogExists()`) to avoid running against
  half-dropped catalogs.
* CI's pgindent runs in a non-UTF-8 locale that mangles em-dashes.
  Use ASCII `--` in C source comments.
* `foreach_ptr` is PG17+. For PG16 compat include
  `pg_extension_base/pg_compat.h` which provides the shim.
* The public.ducklake_metadata view exposes a synthetic `data_path` row
  derived from the GUC when no real row exists. Real writes (via the
  INSTEAD-OF INSERT trigger or the install seed) hide the synthetic
  one. Any C code that reads `lake_ducklake.metadata` directly bypasses
  the synthesis -- prefer the view, or add an explicit fallback
  through `DucklakeDefaultLocationPrefix`.

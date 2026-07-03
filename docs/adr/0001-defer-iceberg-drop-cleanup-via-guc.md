# Defer Iceberg drop cleanup via an explicit GUC opt-in

## Status

accepted

## Context

Dropping a writable, default-location Iceberg table normally walks object
storage (`IcebergFindAllReferencedFiles`) to enqueue every referenced file for
later removal by `VACUUM`. For a caller dropping many long-lived tables in one
statement — notably `snowflake_cdc` tearing down a mirror's internal
changelog/metalog tables on `DROP PUBLICATION` — that per-file walk can exceed
the request timeout and roll the whole drop back.

We let such callers **defer** the cleanup: instead of enumerating files, the
entire table storage prefix is queued once (an `is_prefix` record in
`lake_engine.deletion_queue`), and `VACUUM` removes the whole directory later.

The opt-in is an explicit GUC, `pg_lake_table.defer_drop_file_cleanup`
(default `off`). While it is on, the `DROP` hook for a writable,
default-location Iceberg table queues the table's location prefix
(`MarkWritableTableLocationPrefixForDeletion`) and skips both the object-store
enumeration and the `previous_metadata` single-file enqueue (the whole prefix
covers them). A caller turns it on for exactly the scope of its bulk drop:

```c
int nestLevel = NewGUCNestLevel();
SetConfigOption("pg_lake_table.defer_drop_file_cleanup", "on",
                PGC_USERSET, PGC_S_SESSION);
/* ... drop the tables ... */
AtEOXact_GUC(true, nestLevel);   /* transaction abort restores it otherwise */
```

Deferral is only sound for default-location tables, whose prefix is unique per
table and fully managed by pg_lake. Custom-location tables (whose prefix may be
shared) and tables created in the current transaction (no persisted prefix)
always take the normal enumeration path, even when the GUC is on.

## Considered options

- **Explicit GUC opt-in (chosen)**: the caller states intent directly ("do not
  expand references for this drop") and pg_lake queues the prefix. `SET LOCAL`
  semantics make it transactional and self-scoped; no cross-extension C symbol
  is exported (the downstream caller just sets a GUC).
- **Queue-inference**: enqueue an `is_prefix` record for the location first,
  and have the `DROP` hook infer "skip enumeration" from that row's presence.
  Rejected: it overloads the deletion queue as a side-channel signal and makes
  a queue row semantically meaningful to the `DROP` path. That introduces a new
  correctness dependency on *location uniqueness* — if a location is ever reused
  while a stale prefix row is still queued, a normal drop would wrongly skip
  enumeration. pg_lake has historically not had to reason about location reuse,
  and an inferred signal is harder to read than an explicit flag.
- **A dedicated cross-extension API call** ("drop this Iceberg table cheaply").
  Rejected: heavier coupling and a new exported entry point, when a GUC plus the
  existing prefix-queue + `VACUUM` machinery already expresses the intent and
  drives the asynchronous cleanup.

## Consequences

- The deferral is opt-in and explicit; the default drop path is unchanged.
- No new exported C symbol crosses the extension boundary — the contract is a
  GUC name and its semantics, which downstream callers set with `SET LOCAL`.
- Deferral is transactional: the prefix enqueue and the drop share one
  transaction, so a rollback leaves nothing behind.
- The `iceberg-find-referenced-files` injection point exists so tests can prove
  the enumeration path is reachable (and, by the prefix-vs-per-file row
  contrast, that a deferred drop does not enter it).

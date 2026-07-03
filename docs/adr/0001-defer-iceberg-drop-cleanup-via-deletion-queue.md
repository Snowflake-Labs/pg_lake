# Defer Iceberg drop cleanup by inferring intent from the deletion queue

## Status

accepted

## Context

Dropping a writable, default-location Iceberg table normally walks object
storage (`IcebergFindAllReferencedFiles`) to enqueue every referenced file for
later removal by `VACUUM`. For a caller dropping many long-lived tables in one
statement — notably `snowflake_cdc` tearing down a mirror's internal
changelog/metalog tables on `DROP PUBLICATION` — that per-file walk can exceed
the request timeout and roll the whole drop back.

We decided to let such callers **defer** the cleanup: instead of enumerating
files, the entire table storage prefix is queued once, and `VACUUM` removes the
whole directory later. The signalling channel is the existing
`lake_engine.deletion_queue`: a caller enqueues an `is_prefix` record for the
table's location (via `MarkWritableTableLocationPrefixForDeletion`) before
dropping it, and pg_lake's `DROP` hook probes for that record
(`PrefixDeletionRecordExists`) and, when present, skips the object-store
enumeration and the `previous_metadata` single-file enqueue.

This is only sound for default-location tables, whose prefix is unique per
table and fully managed by pg_lake; custom-location tables are never deferred.
Enqueue and drop run in the same transaction (with a `CommandCounterIncrement`
between so the hook sees the row), keeping the operation atomic on rollback.

## Considered options

- **A new parameter/GUC on the drop path** to request "skip enumeration".
  Rejected: it would thread a new flag through the object-access hook and the
  DDL apply path, and still needs a place to record that the prefix must be
  vacuumed — which is exactly what the deletion queue already is.
- **A dedicated cross-extension API call** for "drop this Iceberg table
  cheaply". Rejected: heavier coupling and a new exported entry point, when the
  queue already models "this prefix is orphaned, vacuum it" and already drives
  the asynchronous cleanup we want.
- **Queue-inference (chosen)**: reuse the deletion queue as both the signal and
  the work item. The queue row *is* the deferred work; the hook merely infers
  from its presence that enumeration is redundant.

## Consequences

- The presence of a prefix row for a table's location is now semantically
  meaningful to the `DROP` path, not just to `VACUUM`. Callers outside pg_lake
  (currently only `snowflake_cdc`) participate in this contract by enqueuing
  before dropping.
- Every writable-Iceberg-table drop now does one extra primary-key probe on
  `lake_engine.deletion_queue` to decide whether to defer; negligible cost.
- The `iceberg-find-referenced-files` injection point exists so tests can prove
  the enumeration path was skipped.

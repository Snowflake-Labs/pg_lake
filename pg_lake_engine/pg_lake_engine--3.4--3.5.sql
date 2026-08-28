-- Upgrade script for pg_lake_engine from 3.4 to 3.5

-- When we last tried and failed to remove a queued path. Retries are spaced by
-- pg_lake_engine.vacuum_file_remove_retry_interval, so that
-- vacuum_file_remove_max_retries bounds how long we keep trying a path rather
-- than how many VACUUM passes happen to reach it.
ALTER TABLE lake_engine.deletion_queue
    ADD COLUMN last_attempt_at timestamptz;

-- Where to delete from when a resolve_metadata row cannot be resolved into the
-- files it references. Only set for a table at its default managed location,
-- where nothing else may be stored under the prefix; NULL for every other row.
ALTER TABLE lake_engine.deletion_queue
    ADD COLUMN fallback_prefix text;

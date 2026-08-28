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

-- Recovery for a queued metadata.json that cleanup cannot resolve and has no
-- remembered fallback_prefix. Superuser only: both reach object storage with the
-- server's credentials over an operator-supplied path.
CREATE FUNCTION lake_engine.resolve_deletion_queue_path(queued_path text, delete_prefix text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$resolve_deletion_queue_path$function$;
REVOKE ALL ON FUNCTION lake_engine.resolve_deletion_queue_path(text, text) FROM public;

CREATE FUNCTION lake_engine.forget_deletion_queue_path(queued_path text)
 RETURNS void
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $function$forget_deletion_queue_path$function$;
REVOKE ALL ON FUNCTION lake_engine.forget_deletion_queue_path(text) FROM public;

-- Upgrade script for pg_lake_engine from 3.5 to 3.6

-- A pending_rest_confirmation row names an old metadata.json that a REST
-- catalog write is in the process of superseding. It is invisible to
-- flush_deletion_queue until the write is confirmed (REST catalog returned
-- 204), so a file is never deleted while the catalog might still reference
-- it. See track_iceberg_metadata_changes.c's deferred-deletion flow.
ALTER TABLE lake_engine.deletion_queue
    ADD COLUMN pending_rest_confirmation bool NOT NULL DEFAULT false;

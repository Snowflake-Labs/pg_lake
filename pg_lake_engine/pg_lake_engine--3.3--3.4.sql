-- Upgrade script for pg_lake_engine from 3.3 to 3.4

-- Rows flagged with resolve_metadata are deferred DROP entries: "path" points
-- at a table's metadata.json rather than a file to delete directly. VACUUM
-- resolves the metadata to the exact set of referenced files and deletes those,
-- so the expensive object-store walk is moved off the DROP path without losing
-- the file-accurate deletion semantics.
ALTER TABLE lake_engine.deletion_queue
    ADD COLUMN resolve_metadata bool NOT NULL DEFAULT false;

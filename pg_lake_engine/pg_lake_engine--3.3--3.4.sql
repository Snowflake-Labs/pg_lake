-- Upgrade script for pg_lake_engine from 3.3 to 3.4

-- resolve_metadata rows are deferred-drop entries: "path" is a table's
-- metadata.json, which VACUUM resolves into the exact referenced files to
-- delete, moving the object-store walk off the DROP path.
ALTER TABLE lake_engine.deletion_queue
    ADD COLUMN resolve_metadata bool NOT NULL DEFAULT false;

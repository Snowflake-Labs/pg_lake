-- Upgrade script for pg_lake_iceberg from 3.7 to 3.8

-- list_object_store_tables() takes an optional catalog_subdir, to list the
-- catalog.json under <database>/<catalog_subdir>/ of an external catalog. It is
-- not STRICT so that the default NULL means the shared <database>/catalog.json.
DROP FUNCTION lake_iceberg.list_object_store_tables(text, bool);

CREATE FUNCTION lake_iceberg.list_object_store_tables(catalog_name text, internalTables bool DEFAULT TRUE, catalog_subdir text DEFAULT NULL)
   RETURNS TABLE(
    metadata_location TEXT,
    catalog_table_name TEXT,
    catalog_namespace TEXT
  )
 LANGUAGE c
AS 'MODULE_PATHNAME', $function$list_object_store_tables$function$;

CREATE OR REPLACE FUNCTION lake_iceberg.list_object_store_tables(catalog_name text, internalTables bool DEFAULT TRUE)
 	RETURNS TABLE(
		metadata_location TEXT,
		catalog_table_name TEXT,
		catalog_namespace TEXT
	)
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$list_object_store_tables$function$;

CREATE OR REPLACE FUNCTION lake_iceberg.trigger_object_store_catalog_generation()
 	RETURNS VOID
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$trigger_object_store_catalog_generation$function$;

-- any role who can write into a table should be able to trigger this
REVOKE ALL ON FUNCTION lake_iceberg.trigger_object_store_catalog_generation() FROM public;
GRANT EXECUTE ON FUNCTION lake_iceberg.trigger_object_store_catalog_generation() TO lake_read_write;


CREATE OR REPLACE FUNCTION lake_iceberg.force_push_object_store_catalog()
 	RETURNS VOID
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$force_push_object_store_catalog$function$;


-- any role who can write into a table should be able to trigger this
REVOKE ALL ON FUNCTION lake_iceberg.force_push_object_store_catalog() FROM public;
GRANT EXECUTE ON FUNCTION lake_iceberg.force_push_object_store_catalog() TO lake_read_write;

-- track the last committed snapshot id
-- especially useful for tables backed by REST catalog
-- where we know exactly the last iceberg snapshot committed
-- from Postgres' perspective. If this ever diverges from
-- REST catalog, we can use this snapshot_id as a reference to
-- recover from the diverged states
ALTER TABLE lake_iceberg.tables_internal ADD COLUMN snapshot_id BIGINT;

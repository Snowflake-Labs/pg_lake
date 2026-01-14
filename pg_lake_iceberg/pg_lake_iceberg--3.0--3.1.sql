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

-- Create rest_catalog_sync table if it doesn't exist (for upgrades from 3.0)
CREATE TABLE lake_iceberg.rest_catalog_sync (
    table_name regclass NOT NULL,
    last_synced_snapshot_id BIGINT,
    PRIMARY KEY (table_name)
);

REVOKE ALL ON TABLE lake_iceberg.rest_catalog_sync FROM public;
GRANT SELECT, INSERT, UPDATE ON TABLE lake_iceberg.rest_catalog_sync TO lake_read_write;

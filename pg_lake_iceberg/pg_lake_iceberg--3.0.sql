CREATE SCHEMA lake_iceberg;
GRANT USAGE ON SCHEMA lake_iceberg TO public;

CREATE TABLE lake_iceberg.namespace_properties (
	catalog_name varchar(255) NOT NULL,
	namespace varchar(255) NOT NULL,
	property_key varchar(255),
	property_value varchar(1000),
	PRIMARY KEY (catalog_name, namespace, property_key)
);
CREATE VIEW lake_iceberg.iceberg_namespace_properties AS
	SELECT catalog_name, namespace, property_key, property_value
	FROM lake_iceberg.namespace_properties;

GRANT SELECT ON lake_iceberg.iceberg_namespace_properties TO public;
ALTER VIEW lake_iceberg.iceberg_namespace_properties SET SCHEMA pg_catalog;

-- the main UX for the users is pg_catalog.iceberg_tables view
-- but we represent iceberg in two separate tables:
-- tables_internal and tables_external
-- the former represents tables that are created on this server
-- the latter represents tables that are using this server only as catalog
-- two tables enables us to use "regclass" for the internal ones
CREATE TABLE lake_iceberg.tables_internal (
    table_name regclass NOT NULL,
    metadata_location varchar(1000),
    previous_metadata_location varchar(1000),
    read_only bool DEFAULT false,
    has_custom_location BOOL DEFAULT true,
    default_spec_id INT DEFAULT 0,
    PRIMARY KEY (table_name)
);

CREATE TABLE lake_iceberg.tables_external (
    catalog_name varchar(255) NOT NULL,
    table_namespace varchar(255) NOT NULL,
    table_name varchar(255) NOT NULL,
    metadata_location varchar(1000),
    previous_metadata_location varchar(1000),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

/*
 * The iceberg_tables view is defined as a union between the internal Iceberg tables
 * that have postgres tables associated with them and the external tables created by
 * other tools. The main distinction between these is that the catalog name for internal
 * tables is always equal to the database name. We currently block external tools from
 * using the database name as the catalog name, but since the database could be renamed
 * we also hide external tables that have a conflicting catalog name.
 */
CREATE OR REPLACE VIEW lake_iceberg.tables AS
SELECT
    current_database()::text as catalog_name,
    relnamespace::regnamespace::varchar(255) AS table_namespace,
    relname::varchar(255) AS table_name,
    metadata_location,
    previous_metadata_location
FROM lake_iceberg.tables_internal JOIN pg_class ON (pg_class.oid = lake_iceberg.tables_internal.table_name)
UNION ALL
SELECT
    catalog_name,
    table_namespace,
    table_name,
    metadata_location,
    previous_metadata_location
FROM lake_iceberg.tables_external
WHERE catalog_name <> current_database()::text;

CREATE VIEW lake_iceberg.iceberg_tables AS
	SELECT catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location
	FROM lake_iceberg.tables;
GRANT SELECT ON lake_iceberg.iceberg_tables TO public;
ALTER VIEW lake_iceberg.iceberg_tables SET SCHEMA pg_catalog;

-- lake_iceberg.metadata(metadata_uri)
CREATE FUNCTION lake_iceberg.metadata(metadata_uri TEXT)
	RETURNS JSONB
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'iceberg_metadata';

-- lake_iceberg.files(metadata_uri) 
CREATE FUNCTION lake_iceberg.files(metadata_uri TEXT)
	RETURNS TABLE(
		manifest_path TEXT,
		content TEXT,
		file_path TEXT,
		file_format TEXT,
		spec_id BIGINT,
		record_count BIGINT,
		file_size_in_bytes BIGINT
	)
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'iceberg_files';

-- lake_iceberg.snapshots(metadata_uri)
CREATE FUNCTION lake_iceberg.snapshots(metadata_uri TEXT)
	RETURNS TABLE(
		sequence_number BIGINT,
		snapshot_id BIGINT,
		timestamp_ms TIMESTAMP,
		manifest_list TEXT
	)
	LANGUAGE C
	IMMUTABLE PARALLEL SAFE STRICT
	AS 'MODULE_PATHNAME', 'iceberg_snapshots';

/*
 * Create roles if they do not yet exist. Roles are server-wide objects that
 * are not owned by the extension, so they could easily exist if the extension
 * was previously created and dropped, or exists in multiple databases.
 */
DO LANGUAGE plpgsql $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'iceberg_catalog') THEN
		CREATE ROLE iceberg_catalog;
	END IF;
	GRANT ALL ON pg_catalog.iceberg_namespace_properties TO iceberg_catalog;
	GRANT ALL ON pg_catalog.iceberg_tables TO iceberg_catalog;
END;
$$;

CREATE FUNCTION lake_iceberg.external_catalog_modification() RETURNS trigger
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

-- we do not (yet) let external modifications to the
-- iceberg metadata unless user explicitly allows
-- note that it should be avoided for majority of the
-- use cases as external modifications would break the
-- pg_lake iceberg query engine follow the changes to the
-- data/metadata
CREATE TRIGGER external_catalog_modifications_trg
INSTEAD OF INSERT OR UPDATE OR DELETE ON pg_catalog.iceberg_tables
FOR EACH ROW EXECUTE FUNCTION lake_iceberg.external_catalog_modification();

CREATE FUNCTION lake_iceberg.data_file_stats(metadata_location text)
RETURNS TABLE(path text, sequence_number bigint, lower_bounds json, upper_bounds json)
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pg_lake_read_data_file_stats$$;

REVOKE ALL ON FUNCTION lake_iceberg.data_file_stats(text) FROM public;
GRANT EXECUTE ON FUNCTION lake_iceberg.data_file_stats(text) TO lake_read;

/*
 * iceberg_catalog foreign data wrapper: allows defining multiple REST catalog
 * connections via CREATE SERVER so that users are not limited to a single
 * global REST catalog configured through GUC settings.
 *
 * Example:
 *   CREATE SERVER my_polaris TYPE 'rest'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (host 'http://polaris:8181',
 *              client_id '...',
 *              client_secret '...');
 *
 *   CREATE TABLE t (a int) USING iceberg WITH (catalog = 'my_polaris');
 */
CREATE FUNCTION lake_iceberg.iceberg_catalog_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER iceberg_catalog
  NO HANDLER
  VALIDATOR lake_iceberg.iceberg_catalog_validator;

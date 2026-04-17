-- Upgrade script for pg_lake_iceberg from 3.2 to 3.3

-- Set REPLICA IDENTITY FULL for catalog tables without primary keys
-- This is required for logical replication when using 'FOR ALL TABLES' publications
ALTER TABLE lake_iceberg.namespace_properties REPLICA IDENTITY FULL;
ALTER TABLE lake_iceberg.tables_internal REPLICA IDENTITY FULL;
ALTER TABLE lake_iceberg.tables_external REPLICA IDENTITY FULL;

GRANT SELECT ON lake_iceberg.tables TO public;
GRANT SELECT ON lake_iceberg.tables_internal TO public;
GRANT SELECT ON lake_iceberg.tables_external TO public;

CREATE OR REPLACE VIEW pg_catalog.iceberg_tables AS
	SELECT catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location
	FROM lake_iceberg.tables
    WHERE metadata_location IS NOT NULL;

/*
 * iceberg_catalog foreign data wrapper: allows defining named catalog
 * configurations via CREATE SERVER so that users are not limited to a
 * single global catalog configured through GUC settings.
 *
 * Supported server types:
 *   TYPE 'rest'          -- REST catalog (e.g. Polaris, Gravitino)
 *   TYPE 'object_store'  -- Object store catalog (catalog.json in S3)
 *
 * Server options (all optional):
 *   rest_endpoint, rest_auth_type, oauth_endpoint, scope,
 *   enable_vended_credentials, location_prefix, catalog_name,
 *   read_only (boolean).
 *
 * User mapping options (credentials, TYPE 'rest' only):
 *   client_id, client_secret, scope.
 *
 * scope is accepted in both server and user mapping; user mapping wins.
 * read_only on a server propagates to all tables unless overridden.
 *
 * Credential resolution order (TYPE 'rest'):
 *   1. CREATE USER MAPPING for the current user
 *   2. $PGDATA/catalogs.conf (platform-provided)
 *   3. GUC variables (backward compatibility)
 *
 * REST catalog example:
 *   CREATE SERVER my_polaris TYPE 'rest'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (rest_endpoint 'https://polaris.example.com');
 *
 *   CREATE USER MAPPING FOR user1 SERVER my_polaris
 *     OPTIONS (client_id '...', client_secret '...');
 *
 *   CREATE TABLE t (a int) USING iceberg WITH (catalog = 'my_polaris');
 *
 * Object store catalog example (shared writer/reader):
 *   -- Writer instance:
 *   CREATE SERVER shared_catalog TYPE 'object_store'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (location_prefix 's3://bucket/shared');
 *
 *   CREATE TABLE t (a int) USING iceberg
 *     WITH (catalog = 'shared_catalog');
 *
 *   -- Reader instance (same S3 path, read-only):
 *   CREATE SERVER shared_catalog TYPE 'object_store'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (location_prefix 's3://bucket/shared', read_only 'true');
 */
CREATE FUNCTION lake_iceberg.iceberg_catalog_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER iceberg_catalog
  NO HANDLER
  VALIDATOR lake_iceberg.iceberg_catalog_validator;

GRANT USAGE ON FOREIGN DATA WRAPPER iceberg_catalog TO lake_write;

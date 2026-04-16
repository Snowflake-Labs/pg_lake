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
 * single global REST catalog configured through GUC settings.
 *
 * Server options (non-secret): rest_endpoint, rest_auth_type,
 *   oauth_endpoint, scope, enable_vended_credentials, location_prefix,
 *   catalog_name.
 * User mapping options (credentials): client_id, client_secret, scope.
 *
 * scope is accepted in both server and user mapping; user mapping wins.
 *
 * Credential resolution order:
 *   1. CREATE USER MAPPING for the current user
 *   2. $PGDATA/catalogs.conf (platform-provided)
 *   3. GUC variables (backward compatibility)
 *
 * User-defined catalog example:
 *   CREATE SERVER my_polaris TYPE 'rest'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (rest_endpoint 'https://polaris.example.com');
 *
 *   CREATE USER MAPPING FOR user1 SERVER my_polaris
 *     OPTIONS (client_id '...', client_secret '...');
 *
 *   CREATE TABLE t (a int) USING iceberg WITH (catalog = 'my_polaris');
 *
 * Platform-provided catalog example:
 *   CREATE SERVER horizon TYPE 'rest'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (rest_endpoint 'https://horizon.example.com');
 *
 *   -- Credentials in $PGDATA/catalogs.conf:
 *   --   horizon.client_id = 'platform_id'
 *   --   horizon.client_secret = 'platform_secret'
 */
CREATE FUNCTION lake_iceberg.iceberg_catalog_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER iceberg_catalog
  NO HANDLER
  VALIDATOR lake_iceberg.iceberg_catalog_validator;

GRANT USAGE ON FOREIGN DATA WRAPPER iceberg_catalog TO lake_write;

-- Upgrade script for pg_lake_iceberg from 3.3 to 3.4

/*
 * iceberg_catalog foreign data wrapper: allows defining named catalog
 * configurations via CREATE SERVER so that users are not limited to a
 * single global REST catalog configured through GUC settings.
 *
 * Example:
 *   CREATE SERVER my_polaris TYPE 'rest'
 *     FOREIGN DATA WRAPPER iceberg_catalog
 *     OPTIONS (rest_endpoint 'http://polaris:8181',
 *              rest_auth_type 'default',
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

GRANT USAGE ON FOREIGN DATA WRAPPER iceberg_catalog TO lake_write;

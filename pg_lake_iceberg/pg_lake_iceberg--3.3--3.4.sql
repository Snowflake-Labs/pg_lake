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

/*
 * Built-in catalog servers.
 *
 * These three servers are pre-created as structural anchors for the
 * pg_depend dependency edges that iceberg tables record against their
 * catalog server.  They are extension-owned and immutable: ALTER, DROP,
 * RENAME, and OWNER changes on them are all blocked.  Configuration
 * for the built-in catalogs lives in GUCs, not in server options.
 *
 * Users keep typing the short names ('postgres', 'object_store', 'rest')
 * as the catalog= option value on CREATE TABLE; ResolveCatalogServerName
 * maps short -> long at server lookup time.  The long names are prefixed
 * so they cannot collide with names users may already have in their
 * databases (e.g. a postgres_fdw server literally named 'postgres').
 *
 * Pre-flight: error early with a clear hint if any of the long names is
 * already in use.  This prevents a confusing "server already exists"
 * mid-upgrade.
 */
DO $do$
DECLARE
  conflicting text;
BEGIN
  SELECT srvname INTO conflicting
  FROM pg_foreign_server
  WHERE srvname IN ('pg_lake_postgres_catalog',
                    'pg_lake_object_store_catalog',
                    'pg_lake_rest_catalog')
  LIMIT 1;

  IF conflicting IS NOT NULL THEN
    RAISE EXCEPTION
      'pg_lake_iceberg upgrade conflicts with existing foreign server %', conflicting
      USING HINT = 'Drop or rename the server and re-run ALTER EXTENSION pg_lake_iceberg UPDATE. '
                   'pg_lake_iceberg reserves the names pg_lake_postgres_catalog, '
                   'pg_lake_object_store_catalog, and pg_lake_rest_catalog for internal use.';
  END IF;
END $do$;

CREATE SERVER pg_lake_postgres_catalog
  TYPE 'postgres'
  FOREIGN DATA WRAPPER iceberg_catalog;

CREATE SERVER pg_lake_object_store_catalog
  TYPE 'object_store'
  FOREIGN DATA WRAPPER iceberg_catalog;

CREATE SERVER pg_lake_rest_catalog
  TYPE 'rest'
  FOREIGN DATA WRAPPER iceberg_catalog;

GRANT USAGE ON FOREIGN SERVER pg_lake_postgres_catalog TO lake_write;
GRANT USAGE ON FOREIGN SERVER pg_lake_object_store_catalog TO lake_write;
GRANT USAGE ON FOREIGN SERVER pg_lake_rest_catalog TO lake_write;

-- Upgrade script for pg_lake_iceberg from 3.3 to 3.4

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
 * Credential resolution depends on the catalog kind:
 *
 *   - The built-in 'rest' catalog reads credentials from the
 *     pg_lake_iceberg.rest_catalog_client_id /
 *     pg_lake_iceberg.rest_catalog_client_secret GUCs.
 *   - User-created catalog servers require credentials on a USER
 *     MAPPING.  They do NOT fall back to the GUCs above, so that a
 *     role with USAGE on the FDW cannot create a server pointing at
 *     a third-party endpoint and harvest the system-wide credentials.
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

-- lake_iceberg.find_all_referenced_files(metadata_path) returns every file
-- (data/delete files, manifests, manifest lists and the metadata.json itself)
-- referenced by an Iceberg metadata.json. VACUUM calls this by name via SPI to
-- resolve a deferred-drop deletion_queue row (resolve_metadata) into the exact
-- set of files to delete, so the pg_lake_engine layer needs no link-time
-- dependency on the iceberg layer that implements the walk.
CREATE FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text)
	RETURNS SETOF text
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', 'find_all_referenced_files';

-- The function walks an arbitrary object-store path with the server's
-- credentials, so it must not be world-callable (same reasoning as
-- lake_iceberg.data_file_stats above). The deferred-drop VACUUM path invokes
-- it via SPI as the extension owner, so revoking from PUBLIC does not affect
-- internal cleanup.
REVOKE ALL ON FUNCTION lake_iceberg.find_all_referenced_files(text) FROM public;

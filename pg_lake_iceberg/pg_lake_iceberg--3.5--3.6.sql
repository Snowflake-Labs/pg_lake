-- Upgrade script for pg_lake_iceberg from 3.5 to 3.6

-- catalog 'snowflake' is now an alias for the built-in REST catalog, so tables
-- that used an iceberg_catalog server with that name switch to the built-in one
DO $do$
BEGIN
  IF EXISTS (SELECT 1
             FROM pg_foreign_server s
             JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw
             WHERE s.srvname = 'snowflake' AND w.fdwname = 'iceberg_catalog') THEN
    RAISE WARNING 'iceberg_catalog server "snowflake" is ignored'
      USING DETAIL = 'catalog ''snowflake'' refers to the built-in REST catalog.',
            HINT = 'Create a server with a different name and recreate the tables that should use it.';
  END IF;
END
$do$;

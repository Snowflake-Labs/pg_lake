-- Upgrade script for pg_lake_iceberg from 3.6 to 3.7

-- lake_iceberg.catalog_stats() returns the server's REST catalog counters,
-- one row per counter that is not zero (see docs/iceberg-catalogs.md).
CREATE FUNCTION lake_iceberg.catalog_stats(
	OUT metric text,
	OUT catalog text,
	OUT operation text,
	OUT result text,
	OUT value bigint)
	RETURNS SETOF record
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', 'pg_lake_catalog_stats';

-- The counters cover every database on the server, so they are for roles
-- that may read server-wide statistics, as with pg_stat_* views.
REVOKE ALL ON FUNCTION lake_iceberg.catalog_stats() FROM public;
GRANT EXECUTE ON FUNCTION lake_iceberg.catalog_stats() TO pg_read_all_stats;

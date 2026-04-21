-- Upgrade script for pg_lake_engine from 3.3 to 3.4

-- __lake_postgres_scan is a placeholder for postgres_scan_pushdown
-- in DuckDB. We never call this function on Postgres, but only
-- push down the function to the query engine.
CREATE OR REPLACE FUNCTION __lake__internal__nsp__.__lake_postgres_scan(
    dsn TEXT, schema_name TEXT, table_name TEXT, unique_table_id INT)
RETURNS SETOF record AS $$
BEGIN
  RETURN;
END;
$$ LANGUAGE plpgsql;

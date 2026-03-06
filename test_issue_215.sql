-- Test for issue #215: Assert failure when using CTAS USING iceberg in PL/pgSQL
-- This test reproduces the exact scenario described in the issue

-- Create the test function that previously caused assert failure
CREATE OR REPLACE FUNCTION test_ctas_iceberg()
  RETURNS void
  LANGUAGE plpgsql
  AS $$
  BEGIN
      EXECUTE 'CREATE TABLE test_iceberg USING iceberg AS SELECT 1 AS id';
  END;
  $$;

-- Execute the function - this should now work without assert failure
SELECT test_ctas_iceberg();

-- Verify the table was created and has correct data
SELECT * FROM test_iceberg;

-- Clean up
DROP TABLE IF EXISTS test_iceberg;
DROP FUNCTION test_ctas_iceberg();
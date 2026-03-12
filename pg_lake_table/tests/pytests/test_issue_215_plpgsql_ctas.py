import pytest
from utils_pytest import *


def test_ctas_iceberg_in_plpgsql(s3, pg_conn, extension, with_default_location):
    """
    Test for issue #215: Assert failure when using CTAS USING iceberg in PL/pgSQL.
    
    This test reproduces the exact scenario described in the issue to ensure
    that CREATE TABLE AS SELECT with iceberg tables works correctly when
    executed through SPI (e.g., from PL/pgSQL EXECUTE).
    
    Previously, this would cause PostgreSQL's SPI layer to assert fail:
    Assert("ctastmt->if_not_exists || ctastmt->into->skipData")
    """
    
    # Create the test function that previously caused assert failure
    run_command(
        """
        CREATE OR REPLACE FUNCTION test_ctas_iceberg()
          RETURNS void
          LANGUAGE plpgsql
          AS $$
          BEGIN
              EXECUTE 'CREATE TABLE test_iceberg USING iceberg AS SELECT 1 AS id';
          END;
          $$;
        """,
        pg_conn,
    )

    # Execute the function - this should now work without assert failure
    run_command("SELECT test_ctas_iceberg();", pg_conn)

    # Verify the table was created and has correct data
    result = run_query("SELECT * FROM test_iceberg", pg_conn)
    assert len(result) == 1
    assert result[0][0] == 1  # id column should be 1

    # Clean up
    run_command("DROP TABLE IF EXISTS test_iceberg", pg_conn)
    run_command("DROP FUNCTION test_ctas_iceberg()", pg_conn)
    
    pg_conn.commit()
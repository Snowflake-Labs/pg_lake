import pytest
from utils_pytest import *


@pytest.mark.parametrize(
    "ctas_suffix",
    [
        "",
        " WITH DATA",
        " WITH NO DATA",
        " IF NOT EXISTS",
    ],
    ids=["plain", "with_data", "with_no_data", "if_not_exists"],
)
def test_ctas_iceberg_in_plpgsql(
    s3, pg_conn, extension, with_default_location, ctas_suffix
):
    """
    Test for issue #215: Assert failure when using CTAS USING iceberg in PL/pgSQL.

    Parametrized to cover WITH [NO] DATA and IF NOT EXISTS variants.

    Previously, this would cause PostgreSQL's SPI layer to assert fail:
    Assert("ctastmt->if_not_exists || ctastmt->into->skipData")
    """

    if_not_exists = " IF NOT EXISTS" if "IF NOT EXISTS" in ctas_suffix else ""
    data_clause = " WITH NO DATA" if "NO DATA" in ctas_suffix else ""
    skip_data = "NO DATA" in ctas_suffix

    ctas_sql = (
        f"CREATE TABLE{if_not_exists} test_iceberg "
        f"USING iceberg AS SELECT 1 AS id{data_clause}"
    )

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION test_ctas_iceberg()
          RETURNS void
          LANGUAGE plpgsql
          AS $$
          BEGIN
              EXECUTE '{ctas_sql}';
          END;
          $$;
        """,
        pg_conn,
    )

    # Execute the function - this should now work without assert failure
    run_command("SELECT test_ctas_iceberg();", pg_conn)

    # Verify the table was created
    result = run_query("SELECT * FROM test_iceberg", pg_conn)
    if skip_data:
        assert len(result) == 0
    else:
        assert len(result) == 1
        assert result[0][0] == 1

    pg_conn.rollback()


def test_ctas_iceberg_row_count_in_plpgsql(
    s3, pg_conn, extension, with_default_location
):
    """
    Test that CTAS row count propagates correctly through SPI_processed
    to PL/pgSQL GET DIAGNOSTICS ROW_COUNT.
    """

    run_command(
        """
        CREATE OR REPLACE FUNCTION test_ctas_iceberg_row_count()
          RETURNS bigint
          LANGUAGE plpgsql
          AS $$
          DECLARE
              cnt bigint;
          BEGIN
              EXECUTE 'CREATE TABLE test_iceberg_rc USING iceberg AS SELECT generate_series(1, 5) AS id';
              GET DIAGNOSTICS cnt = ROW_COUNT;
              RETURN cnt;
          END;
          $$;
        """,
        pg_conn,
    )

    result = run_query("SELECT test_ctas_iceberg_row_count()", pg_conn)
    assert result[0][0] == 5

    pg_conn.rollback()

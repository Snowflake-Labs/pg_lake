"""
Tests for @STAGE/ URL resolution feature.

This module tests both the GUC configuration and integration with pg_lake_table
UDFs and COPY commands.
"""

import pytest
from utils_pytest import *

BUCKET_SUBDIR = "test_stage_location"


# ============================================================================
# GUC Configuration Tests
# ============================================================================


def test_stage_location_set_valid_s3(superuser_conn):
    """Test setting pg_lake.stage_location to a valid S3 URL"""
    run_command("SET pg_lake.stage_location TO 's3://test-bucket/data'", superuser_conn)
    res = run_query("SHOW pg_lake.stage_location", superuser_conn)
    assert res[0][0] == "s3://test-bucket/data"
    superuser_conn.rollback()


def test_stage_location_set_valid_gs(superuser_conn):
    """Test setting pg_lake.stage_location to a valid GCS URL"""
    run_command(
        "SET pg_lake.stage_location TO 'gs://test-bucket/prefix'", superuser_conn
    )
    res = run_query("SHOW pg_lake.stage_location", superuser_conn)
    assert res[0][0] == "gs://test-bucket/prefix"
    superuser_conn.rollback()


def test_stage_location_set_valid_azure(superuser_conn):
    """Test setting pg_lake.stage_location to a valid Azure URL"""
    run_command(
        "SET pg_lake.stage_location TO 'abfss://container@account.dfs.core.windows.net/path'",
        superuser_conn,
    )
    res = run_query("SHOW pg_lake.stage_location", superuser_conn)
    assert res[0][0] == "abfss://container@account.dfs.core.windows.net/path"
    superuser_conn.rollback()


def test_stage_location_set_valid_with_trailing_slash(superuser_conn):
    """Test setting pg_lake.stage_location with trailing slash"""
    run_command(
        "SET pg_lake.stage_location TO 's3://test-bucket/prefix/'", superuser_conn
    )
    res = run_query("SHOW pg_lake.stage_location", superuser_conn)
    assert res[0][0] == "s3://test-bucket/prefix/"
    superuser_conn.rollback()


def test_stage_location_set_invalid_protocol(superuser_conn):
    """Test that invalid protocols are rejected"""
    error = run_command(
        "SET pg_lake.stage_location TO 'http://test-bucket/data'",
        superuser_conn,
        raise_error=False,
    )
    assert "invalid value for parameter" in error
    assert "must be a valid cloud storage URL" in error
    superuser_conn.rollback()


def test_stage_location_reject_query_params(superuser_conn):
    """Test that URLs with query parameters are rejected"""
    error = run_command(
        "SET pg_lake.stage_location TO 's3://bucket?region=us-east-1'",
        superuser_conn,
        raise_error=False,
    )
    assert "invalid value for parameter" in error
    assert "cannot contain query parameters" in error
    superuser_conn.rollback()


def test_stage_location_resolution_without_guc_set(superuser_conn, extension):
    """Test that @STAGE/ without GUC configured produces helpful error"""
    # Ensure GUC is not set
    run_command("SET pg_lake.stage_location TO DEFAULT", superuser_conn)

    # Try to use @STAGE/ - should error
    error = run_command(
        "SELECT lake_file.size('@STAGE/test.parquet')",
        superuser_conn,
        raise_error=False,
    )
    assert (
        "@STAGE/ URL prefix used but pg_lake.stage_location is not configured" in error
    )
    assert "Set pg_lake.stage_location to your bucket URL" in error
    superuser_conn.rollback()


# ============================================================================
# Integration Tests with S3
# ============================================================================


@pytest.fixture(scope="function")
def setup_stage_location(superuser_conn, s3):
    """Set up stage URL pointing to test bucket"""
    run_command(
        f"SET pg_lake.stage_location TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}'",
        superuser_conn,
    )
    yield
    # Rollback will reset the GUC to default
    try:
        superuser_conn.rollback()
    except:
        # Connection may be closed if test failed
        pass


def test_stage_list_files(superuser_conn, setup_stage_location, extension):
    """Test lake_file.list() with @STAGE/ prefix"""
    # Create test file
    run_command(
        f"COPY (SELECT 1 as id, 'test' as name) TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}/test1.parquet'",
        superuser_conn,
    )

    # List using @STAGE/
    res = run_query(
        "SELECT path FROM lake_file.list('@STAGE/*.parquet')", superuser_conn
    )
    assert len(res) > 0
    # Verify the returned paths are resolved (not @STAGE/)
    for row in res:
        assert row[0].startswith("s3://")
        assert "@STAGE" not in row[0]
    superuser_conn.rollback()


def test_stage_file_size(superuser_conn, setup_stage_location, extension):
    """Test lake_file.size() with @STAGE/ prefix"""
    # Create test file
    path = f"s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/size_test.parquet"
    run_command(
        f"COPY (SELECT generate_series(1, 100) as id) TO '{path}'",
        superuser_conn,
    )

    # Get size using @STAGE/
    res = run_query("SELECT lake_file.size('@STAGE/size_test.parquet')", superuser_conn)
    assert len(res) == 1
    file_size = res[0][0]
    assert file_size > 0

    # Compare with regular URL
    res2 = run_query(f"SELECT lake_file.size('{path}')", superuser_conn)
    assert res2[0][0] == file_size
    superuser_conn.rollback()


def test_stage_file_exists(superuser_conn, setup_stage_location, extension):
    """Test lake_file.exists() with @STAGE/ prefix"""
    # Create test file
    run_command(
        f"COPY (SELECT 1 as id) TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}/exists_test.parquet'",
        superuser_conn,
    )

    # Check existence using @STAGE/
    res = run_query(
        "SELECT lake_file.exists('@STAGE/exists_test.parquet')", superuser_conn
    )
    assert res[0][0] is True

    # Check non-existent file
    res2 = run_query(
        "SELECT lake_file.exists('@STAGE/nonexistent.parquet')", superuser_conn
    )
    assert res2[0][0] is False
    superuser_conn.rollback()


def test_stage_file_preview(superuser_conn, setup_stage_location, extension):
    """Test lake_file.preview() with @STAGE/ prefix"""
    # Create test file
    run_command(
        f"COPY (SELECT 42 as answer, 'hello' as greeting) TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}/preview_test.parquet'",
        superuser_conn,
    )

    # Preview using @STAGE/
    res = run_query(
        "SELECT column_name, column_type FROM lake_file.preview('@STAGE/preview_test.parquet')",
        superuser_conn,
    )
    assert len(res) == 2
    columns = {row[0]: row[1] for row in res}
    assert "answer" in columns
    assert "greeting" in columns
    superuser_conn.rollback()


def test_stage_create_foreign_table(superuser_conn, setup_stage_location, extension):
    """Test CREATE FOREIGN TABLE with @STAGE/ in path option"""
    # Create test file
    run_command(
        f"COPY (SELECT 1 as id, 'test' as value) TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}/ft_test.parquet'",
        superuser_conn,
    )

    # Create foreign table using @STAGE/
    run_command(
        """
        CREATE FOREIGN TABLE stage_ft_test (id int, value text)
        SERVER pg_lake
        OPTIONS (path '@STAGE/ft_test.parquet')
        """,
        superuser_conn,
    )

    # Query the table
    res = run_query("SELECT * FROM stage_ft_test", superuser_conn)
    assert len(res) == 1
    assert res[0][0] == 1
    assert res[0][1] == "test"
    superuser_conn.rollback()


def test_stage_without_leading_slash(superuser_conn, setup_stage_location, extension):
    """Test that @STAGE without / is not treated as stage URL"""
    # @STAGE (no slash) should NOT be resolved
    error = run_command(
        "SELECT lake_file.size('@STAGEtest.parquet')",
        superuser_conn,
        raise_error=False,
    )
    # Should fail with URL validation error, not @STAGE/ resolution error
    assert "only s3://, gs://" in error or "is not supported" in error
    superuser_conn.rollback()


def test_stage_in_middle_of_path(superuser_conn, setup_stage_location, extension):
    """Test that @STAGE/ only works at the beginning of path"""
    # s3://bucket/@STAGE/file should NOT be resolved
    error = run_command(
        f"SELECT lake_file.size('s3://{TEST_BUCKET}/@STAGE/test.parquet')",
        superuser_conn,
        raise_error=False,
    )
    # Should attempt to access literal @STAGE/ directory
    # (will fail but not due to resolution logic)
    assert "not configured" not in error
    superuser_conn.rollback()


def test_stage_with_different_bucket(superuser_conn, s3, extension):
    """Test that changing stage URL affects resolution"""
    # Set first bucket
    run_command(
        f"SET pg_lake.stage_location TO 's3://{TEST_BUCKET}/bucket1'", superuser_conn
    )

    run_command(
        f"COPY (SELECT 1 as value) TO 's3://{TEST_BUCKET}/bucket1/file.parquet'",
        superuser_conn,
    )

    res1 = run_query("SELECT * FROM lake_file.list('@STAGE/*.parquet')", superuser_conn)
    count1 = len(res1)

    # Change to different bucket
    run_command(
        f"SET pg_lake.stage_location TO 's3://{TEST_BUCKET}/bucket2'", superuser_conn
    )

    run_command(
        f"COPY (SELECT 2 as value) TO 's3://{TEST_BUCKET}/bucket2/file.parquet'",
        superuser_conn,
    )
    run_command(
        f"COPY (SELECT 3 as value) TO 's3://{TEST_BUCKET}/bucket2/file2.parquet'",
        superuser_conn,
    )

    res2 = run_query("SELECT * FROM lake_file.list('@STAGE/*.parquet')", superuser_conn)
    count2 = len(res2)

    # Should see different files
    assert count1 != count2 or count1 == 0
    superuser_conn.rollback()


def test_stage_location_resolution_order(
    superuser_conn, setup_stage_location, extension
):
    """Test that @STAGE/ resolution happens before validation"""
    # This ensures ResolveStageURL is called before IsSupportedURL
    # Set stage URL
    run_command(
        f"SET pg_lake.stage_location TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}'",
        superuser_conn,
    )

    # @STAGE/ should be resolved first, then validated
    error = run_command(
        "SELECT lake_file.size('@STAGE/test.parquet')",
        superuser_conn,
        raise_error=False,
    )

    # Should NOT have @STAGE/ validation error (it was resolved)
    assert "@STAGE/ URL prefix used but" not in error
    superuser_conn.rollback()


def test_stage_location_trailing_slash_normalization(superuser_conn, s3, extension):
    """Test that trailing slash in stage URL is handled correctly"""
    # Create test file
    path = f"s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/trailing_test.parquet"
    run_command(
        f"COPY (SELECT 1 as id) TO '{path}'",
        superuser_conn,
    )

    # Set stage URL with trailing slash
    run_command(
        f"SET pg_lake.stage_location TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}/'",
        superuser_conn,
    )

    # Should still work (trailing slash removed by GetPgLakeStageURL)
    res = run_query(
        "SELECT lake_file.exists('@STAGE/trailing_test.parquet')", superuser_conn
    )
    assert res[0][0] is True
    superuser_conn.rollback()


def test_stage_location_with_copy_from(superuser_conn, setup_stage_location, extension):
    """Test @STAGE/ with COPY FROM command"""
    # Create test file
    run_command(
        f"COPY (SELECT generate_series(1, 10) as id, 'row' as name) TO 's3://{TEST_BUCKET}/{BUCKET_SUBDIR}/copy_test.parquet'",
        superuser_conn,
    )

    # Create target table
    run_command("CREATE TEMP TABLE copy_target (id int, name text)", superuser_conn)

    # COPY using @STAGE/
    run_command("COPY copy_target FROM '@STAGE/copy_test.parquet'", superuser_conn)

    # Verify data copied
    res = run_query("SELECT COUNT(*) FROM copy_target", superuser_conn)
    assert res[0][0] == 10
    superuser_conn.rollback()


def test_stage_location_with_copy_to(superuser_conn, setup_stage_location, extension):
    """Test @STAGE/ with COPY TO command"""
    # Create source table
    run_command(
        "CREATE TEMP TABLE copy_source AS SELECT generate_series(1, 5) as id, 'data' as value",
        superuser_conn,
    )

    # COPY TO using @STAGE/
    run_command("COPY copy_source TO '@STAGE/copy_output.parquet'", superuser_conn)

    # Verify file was created
    res = run_query(
        "SELECT lake_file.exists('@STAGE/copy_output.parquet')", superuser_conn
    )
    assert res[0][0] is True
    superuser_conn.rollback()


def test_stage_location_with_create_table_load_from(
    superuser_conn, setup_stage_location, with_default_location
):
    """Test @STAGE/ with CREATE TABLE ... WITH (load_from)"""
    # Create source parquet file
    source_url = f"s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/load_from_test.parquet"
    run_command(
        f"""
        COPY (SELECT generate_series(1, 20) as id,
                     'test_' || generate_series(1, 20) as name,
                     generate_series(1, 20) * 10.5 as value)
        TO '{source_url}'
        """,
        superuser_conn,
    )

    # Create Iceberg table using @STAGE/ in load_from
    run_command(
        """
        CREATE TABLE stage_load_from_test ()
        USING iceberg
        WITH (load_from='@STAGE/load_from_test.parquet', autovacuum_enabled=false)
        """,
        superuser_conn,
    )

    # Verify table was created and data loaded
    res = run_query("SELECT COUNT(*) FROM stage_load_from_test", superuser_conn)
    assert res[0][0] == 20

    # Verify schema was inferred correctly
    res = run_query(
        """
        SELECT attname, atttypid::regtype
        FROM pg_attribute
        WHERE attrelid = 'stage_load_from_test'::regclass
        AND attnum > 0
        ORDER BY attnum
        """,
        superuser_conn,
    )
    assert len(res) == 3
    assert res[0][0] == "id"
    assert res[1][0] == "name"
    assert res[2][0] == "value"

    # Verify data integrity
    res = run_query(
        "SELECT id, name, value FROM stage_load_from_test ORDER BY id LIMIT 3",
        superuser_conn,
    )
    assert res[0] == [1, "test_1", 10.5]
    assert res[1] == [2, "test_2", 21.0]
    assert res[2] == [3, "test_3", 31.5]

    superuser_conn.rollback()


def test_stage_location_with_create_table_load_from_explicit_schema(
    superuser_conn, setup_stage_location, with_default_location
):
    """Test @STAGE/ with CREATE TABLE ... WITH (load_from) and explicit column definitions"""
    # Create source parquet file
    source_url = f"s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/load_from_explicit.parquet"
    run_command(
        f"""
        COPY (SELECT generate_series(1, 10) as id,
                     'value_' || generate_series(1, 10) as description)
        TO '{source_url}'
        """,
        superuser_conn,
    )

    # Create Iceberg table with explicit schema using @STAGE/
    run_command(
        """
        CREATE TABLE stage_load_explicit (
            id bigint,
            description text
        )
        USING iceberg
        WITH (load_from='@STAGE/load_from_explicit.parquet', autovacuum_enabled=false)
        """,
        superuser_conn,
    )

    # Verify data was loaded
    res = run_query("SELECT COUNT(*) FROM stage_load_explicit", superuser_conn)
    assert res[0][0] == 10

    # Verify explicit schema was used
    res = run_query(
        """
        SELECT attname, atttypid::regtype
        FROM pg_attribute
        WHERE attrelid = 'stage_load_explicit'::regclass
        AND attnum > 0
        ORDER BY attnum
        """,
        superuser_conn,
    )
    assert res[0] == ["id", "bigint"]
    assert res[1] == ["description", "text"]

    superuser_conn.rollback()


def test_stage_location_with_create_table_definition_from(
    superuser_conn, setup_stage_location, with_default_location
):
    """Test @STAGE/ with CREATE TABLE ... WITH (definition_from)"""
    # Create source parquet file
    source_url = f"s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/definition_from_test.parquet"
    run_command(
        f"""
        COPY (SELECT generate_series(1, 5) as num,
                     '2024-01-01'::date as created_date,
                     true as is_active)
        TO '{source_url}'
        """,
        superuser_conn,
    )

    # Create Iceberg table using @STAGE/ in definition_from (no data load)
    run_command(
        """
        CREATE TABLE stage_definition_test ()
        USING iceberg
        WITH (definition_from='@STAGE/definition_from_test.parquet')
        """,
        superuser_conn,
    )

    # Verify table was created with correct schema
    res = run_query(
        """
        SELECT attname, atttypid::regtype
        FROM pg_attribute
        WHERE attrelid = 'stage_definition_test'::regclass
        AND attnum > 0
        ORDER BY attnum
        """,
        superuser_conn,
    )
    assert len(res) == 3
    assert res[0][0] == "num"
    assert res[1][0] == "created_date"
    assert res[2][0] == "is_active"

    # Verify no data was loaded (definition_from only copies schema)
    res = run_query("SELECT COUNT(*) FROM stage_definition_test", superuser_conn)
    assert res[0][0] == 0

    superuser_conn.rollback()

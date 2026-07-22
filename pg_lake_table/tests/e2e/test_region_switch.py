import os
import pytest
from utils_pytest import *


# These tests need real AWS credentials pointed at a cross-region bucket. The
# credentials are supplied via CDWREGIONTEST_ACCESS_KEY_ID /
# CDWREGIONTEST_SECRET_ACCESS_KEY. When those are not set (e.g. in GHA CI, which
# no longer defines them) there is nothing meaningful to exercise, so skip.
requires_cdwregiontest = pytest.mark.skipif(
    not os.getenv("CDWREGIONTEST_ACCESS_KEY_ID"),
    reason="CDWREGIONTEST_ACCESS_KEY_ID / CDWREGIONTEST_SECRET_ACCESS_KEY not set",
)


@requires_cdwregiontest
def test_region_switch(s3, pg_conn, pgduck_conn, extension):
    # The following URLs point to real files in a real bucket which reside
    # in us-east-2
    url1 = f"s3://aws-public-blockchain/v1.0/btc/blocks/date=2022-01-01/part-00000-2effe04c-6bb4-4793-8265-5139b41cd751-c000.snappy.parquet"
    url2 = f"s3://aws-public-blockchain/v1.0/btc/blocks/date=2022-01-02/*.parquet"
    url3 = f"s3://aws-public-blockchain/v1.0/btc/blocks/date=2022-01-03/*.parquet"

    access_key_id = os.getenv("CDWREGIONTEST_ACCESS_KEY_ID")
    secret_access_key = os.getenv("CDWREGIONTEST_SECRET_ACCESS_KEY")

    # Set up credentials from environment variables (with the wrong region)
    # Otherwise, we sign requests with our nonsense credentials, which S3 rejects
    run_command(
        f"""
        CREATE OR REPLACE SECRET s3pglregiontest (
            TYPE S3,
            KEY_ID '{access_key_id}',
            SECRET '{secret_access_key}',
            SCOPE 's3://aws-public-blockchain',
            REGION 'us-east-1',
            ENDPOINT 's3.amazonaws.com'
        );
    """,
        pgduck_conn,
    )

    pgduck_conn.commit()

    # We will not see the region in glob operations
    result = run_query(
        f"""
        SELECT file FROM glob('{url2}')
    """,
        pgduck_conn,
    )
    assert "s3_region" not in result[0]["file"]

    # Check again for cached region
    result = run_query(
        f"""
        SELECT file FROM glob('{url2}')
    """,
        pgduck_conn,
    )
    assert "s3_region" not in result[0]["file"]

    # We should be able to access the file even if we do not specify region
    run_command(
        f"""
        CREATE FOREIGN TABLE blocks_1 () SERVER pg_lake
        OPTIONS (path '{url1}');
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*) FROM blocks_1
    """,
        pg_conn,
    )
    assert result[0]["count"] == 172

    # Other query arguments should not be an issue
    run_command(
        f"""
        CREATE FOREIGN TABLE blocks_2 () SERVER pg_lake
        OPTIONS (path '{url2}?s3_endpoint=s3.amazonaws.com');
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*) FROM blocks_2
    """,
        pg_conn,
    )
    assert result[0]["count"] == 158

    # If we explicitly specify the region, we still get an error
    error = run_command(
        f"""
        CREATE FOREIGN TABLE blocks_3 () SERVER pg_lake
        OPTIONS (path '{url3}?s3_region=us-east-1');
    """,
        pg_conn,
        raise_error=False,
    )
    # Depending on whether the AWS list request returns 400 or an empty body,
    # the surfaced error differs — both indicate a region mismatch.
    assert "HTTP 400" in error or "no list response" in error

    pg_conn.rollback()


@requires_cdwregiontest
def test_file_exists_cross_region(s3, pg_conn, pgduck_conn, extension):
    # Exercises the region-aware FileExists path. Without the fix,
    # pg_lake_file_exists() returned false on cross-region buckets because it
    # bypassed the region-resolution logic that OpenFile/Glob used.
    url = f"s3://aws-public-blockchain/v1.0/btc/blocks/date=2022-01-01/part-00000-2effe04c-6bb4-4793-8265-5139b41cd751-c000.snappy.parquet"
    missing_url = f"s3://aws-public-blockchain/v1.0/btc/blocks/date=2099-01-01/does-not-exist.parquet"

    access_key_id = os.getenv("CDWREGIONTEST_ACCESS_KEY_ID")
    secret_access_key = os.getenv("CDWREGIONTEST_SECRET_ACCESS_KEY")

    # Set up credentials with the wrong region (bucket is us-east-2)
    run_command(
        f"""
        CREATE OR REPLACE SECRET s3pglregiontest_exists (
            TYPE S3,
            KEY_ID '{access_key_id}',
            SECRET '{secret_access_key}',
            SCOPE 's3://aws-public-blockchain',
            REGION 'us-east-1',
            ENDPOINT 's3.amazonaws.com'
        );
    """,
        pgduck_conn,
    )

    pgduck_conn.commit()

    # pgduck_conn is module-scoped, so a prior test (test_region_switch) may
    # already have populated the region cache for aws-public-blockchain.
    # Clear it so this test actually exercises the 400/301 retry path.
    run_command(
        f"SELECT pg_lake_clear_region_cache('{url}')",
        pgduck_conn,
    )

    # File that exists in the us-east-2 bucket. Must return true even though
    # the configured region is us-east-1 — the fix makes FileExists refresh
    # the cached region on a 400/301 instead of silently returning false.
    result = run_query(
        f"SELECT pg_lake_file_exists('{url}') AS exists",
        pgduck_conn,
    )
    # pgduck_server speaks the PG wire protocol and returns booleans as
    # the text 't'/'f', so compare against those.
    assert result[0]["exists"] == "t"

    # File that does not exist in the same bucket. The region should be
    # cached from the previous call, so this is a straight 404.
    result = run_query(
        f"SELECT pg_lake_file_exists('{missing_url}') AS exists",
        pgduck_conn,
    )
    assert result[0]["exists"] == "f"


def test_managed_storage_region(s3, pg_conn, pgduck_conn, extension):
    # Attempt to create an Iceberg table in a different region will fail
    error = run_command(
        f"""
        CREATE TABLE region_iceberg (x int, y int) USING iceberg
        WITH (location = 's3://aws-public-blockchain/region_iceberg');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "can only create Iceberg tables in region" in error

    pg_conn.rollback()

    error = run_command(
        f"""
        SET pg_lake_iceberg.default_location_prefix = 's3://aws-public-blockchain/';
        CREATE TABLE region_iceberg (x int, y int) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "can only create Iceberg tables in region" in error

    pg_conn.rollback()

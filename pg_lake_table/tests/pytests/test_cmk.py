import pytest
import server_params
from utils_pytest import *


def test_cmk_encryption(pg_conn, s3, extension, installcheck):
    if installcheck:
        # We do not currently set up CMK for installcheck
        return

    key = "test_managed_storage/data.parquet"
    managed_storage_url = f"s3://{MANAGED_STORAGE_BUCKET}/{key}"
    test_bucket_url = f"s3://{TEST_BUCKET}/{key}"

    run_command(
        f"""
        copy (select s from generate_series(1,10) s) to '{managed_storage_url}';
        copy (select s from generate_series(1,10) s) to '{test_bucket_url}';

        create foreign table managed_storage_table()
        server pg_lake
        options (path '{managed_storage_url}');
    """,
        pg_conn,
    )

    # Confirm that the object in managed storage is using CMK
    response = s3.head_object(Bucket=MANAGED_STORAGE_BUCKET, Key=key)
    assert (
        response.get("ServerSideEncryption") == "aws:kms"
    ), f"Expected ServerSideEncryption 'aws:kms', got: {response}"
    assert (
        response.get("SSEKMSKeyId") == server_params.MANAGED_STORAGE_CMK_ID
    ), f"Expected SSEKMSKeyId '{server_params.MANAGED_STORAGE_CMK_ID}', got: {response}"

    # Confirm that the object out managed storage is not using CMK
    response = s3.head_object(Bucket=TEST_BUCKET, Key=key)
    assert "ServerSideEncryption" not in response

    # Confirm that we can query the file
    result = run_query("select count(*) from managed_storage_table", pg_conn)
    assert result[0]["count"] == 10

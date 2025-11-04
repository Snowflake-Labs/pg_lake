import os
import pytest
from utils_pytest import *

BUCKET_SUBDIR = "test_create_from_s3_helper"

testfiles = [
    "a/b/c/notest.parquet",
    "a/b/c/test.parquet",
    "a/out.csv",
    "a/out.json",
    "a/out.parquet",
    "a/test1.parquet",
    "a/test2.parquet",
    "b/out_csv.csv",
    "b/out_json.json",
    "b/out_parquet.parquet",
    "b/test1.parquet",
    "b/test2.parquet",
    "b/test3.parquet",
]


def test_list_files_non_s3_url(pg_conn, generate_data_on_s3):
    """Test and validate expected testing invariants with dummy data"""

    # error if not s3 url
    error = run_command(
        f"SELECT path FROM lake_file.list('https://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/*')",
        pg_conn,
        raise_error=False,
    )
    assert "only s3:// and gs:// urls are supported" in error
    pg_conn.rollback()


def test_list_files_simple_level(pg_conn, generate_data_on_s3):
    # list show all single-level
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/b/*')",
        pg_conn,
    )
    assert len(res) == 6
    pg_conn.rollback()


def test_list_files_with_s3_region(pg_conn, generate_data_on_s3):
    # list show all single-level
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/b/*?s3_region=us-east-1')",
        pg_conn,
    )
    assert len(res) == 6
    for bucket in res:
        assert "s3_region=us-east-1" not in str(bucket)
    pg_conn.rollback()


def test_list_files_single_level_despite_nested_contents(pg_conn, generate_data_on_s3):
    # list shows non-recursive list of same-level files
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/*')",
        pg_conn,
    )
    assert len(res) == 5
    assert (
        "a/out.csv"
        and "a/out.json"
        and "a/out.parquet"
        and "a/test1.parquet"
        and "a/test2.parquet"
    ) in str(res)
    pg_conn.rollback()


def test_list_files_all_files_subdir(pg_conn, generate_data_on_azure):
    # list shows recursive list of files
    res = run_query(
        f"SELECT path FROM lake_file.list('az://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/**')",
        pg_conn,
    )
    assert len(res) == 7
    assert (
        "a/out.csv"
        and "a/out.json"
        and "a/out.parquet"
        and "a/test1.parquet"
        and "a/test2.parquet"
        and "a/b/c/notest.parquet"
        and "a/b/c/test.parquet"
    ) in str(res)

    pg_conn.rollback()


def test_list_files_all_files_subdir_file_type(pg_conn, generate_data_on_s3):
    # list with recursive list of files matching extension
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/**/*.parquet')",
        pg_conn,
    )
    assert len(res) == 5
    assert (
        "a/out.parquet"
        and "a/test1.parquet"
        and "a/test2.parquet"
        and "a/b/c/notest.parquet"
        and "a/b/c/test.parquet"
    ) in str(res)
    pg_conn.rollback()


def test_list_files_all_files_subdir_patterned_file_type(pg_conn, generate_data_on_s3):
    # list with pattern of files matching extension and file prefix, regardless of level
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/**/test*.parquet')",
        pg_conn,
    )
    assert len(res) == 6
    assert (
        "a/test1.parquet"
        and "a/test2.parquet"
        and "a/b/c/test.parquet"
        and "b/test1.parquet"
        and "b/test2.parquet"
        and "b/test3.parquet"
    ) in str(res)
    pg_conn.rollback()


def test_list_files_subdir1(pg_conn, generate_data_on_s3):
    # list with pattern of files matching extension and file prefix, regardless of level
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/b/')",
        pg_conn,
    )
    assert len(res) == 0
    pg_conn.rollback()


def test_list_files_subdir2(pg_conn, generate_data_on_s3):
    # list with pattern of files matching extension and file prefix, regardless of level
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/**')",
        pg_conn,
    )
    assert len(res) == 7
    assert (
        "a/out.parquet"
        and "a/out.json"
        and "a/out.csv"
        and "a/test1.parquet"
        and "a/test2.parquet"
        and "a/b/c/notest.parquet"
        and "a/b/c/test.parquet"
    ) in str(res)
    pg_conn.rollback()


def test_list_files_subdir3(pg_conn, generate_data_on_s3):
    # list with pattern of files matching extension and file prefix, regardless of level
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/**')",
        pg_conn,
    )
    assert len(res) == 13
    for f in testfiles:
        assert f in str(res)
    pg_conn.rollback()


def test_list_files_subdir4(pg_conn, generate_data_on_s3):
    # list with pattern of files matching extension and file prefix, regardless of level, returns one matching file
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/b/c/no*')",
        pg_conn,
    )
    assert len(res) == 1
    pg_conn.rollback()


def test_list_files_does_not_exist(pg_conn, generate_data_on_s3):
    # should return empty result rather than the input
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/does_not_exist.csv')",
        pg_conn,
    )
    assert len(res) == 0
    pg_conn.rollback()


def test_list_files_does_not_exist_wildcard(pg_conn, generate_data_on_s3):
    # should return an empty result
    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/*does_not_exist.csv')",
        pg_conn,
    )
    assert len(res) == 0
    pg_conn.rollback()


def test_list_files_zero_size(pg_conn, s3, extension, tmp_path):
    csv_key = "test_empty_files_csv/zero_size.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    local_csv_path = tmp_path / "zero_size.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write("")

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    csv1_key = "test_empty_files_csv/zero1_size.csv"
    csv1_path = f"s3://{TEST_BUCKET}/{csv1_key}"

    local_csv1_path = tmp_path / "zero1_size.csv"
    with open(local_csv1_path, "w") as csv_file:
        csv_file.write("")

    s3.upload_file(local_csv1_path, TEST_BUCKET, csv1_key)

    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/test_empty_files_csv/zero_size.csv')",
        pg_conn,
    )
    assert len(res) == 1

    res = run_query(
        f"SELECT path FROM lake_file.list('s3://{TEST_BUCKET}/test_empty_files_csv/zero*_size.csv')",
        pg_conn,
    )
    assert len(res) == 2
    pg_conn.rollback()


def test_list_details(pg_conn, s3, extension):
    prefix = f"s3://{TEST_BUCKET}/test_list_details"

    run_command(
        f"""
        COPY (SELECT 1 id, 'list' val) TO '{prefix}/small.csv' with header;
        COPY (SELECT 1 id, 'list' val FROM generate_series(1,100)) TO '{prefix}/large.csv' with header;
    """,
        pg_conn,
    )

    result = run_query(
        f"select path, file_size, etag from lake_file.list('{prefix}/*')",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0] == [
        prefix + "/large.csv",
        707,
        '"81d51ab47a013964a866395b6921e30d"',
    ]
    assert result[1] == [
        prefix + "/small.csv",
        14,
        '"18a74ddc08d592acc32079bdedf76b99"',
    ]

    result = run_query(
        f"select sum(file_size) from lake_file.list('{prefix}/*')", pg_conn
    )
    assert result[0][0] == 721


@pytest.fixture(scope="module")
def generate_data_on_s3(s3, extension, pg_conn):
    """Create dummy files in s3 to be queried against"""

    # These all have the same structure; it doesn't matter, since we test object
    # creation not contents.

    for datafile in testfiles:
        url = f"s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/{datafile}"
        run_command(f"COPY (SELECT generate_series(1,10) as id) TO '{url}'", pg_conn)

    pg_conn.commit()

    yield


@pytest.fixture(scope="module")
def generate_data_on_azure(azure, extension, pg_conn):
    """Create dummy files in Azure to be queried against"""

    # These all have the same structure; it doesn't matter, since we test object
    # creation not contents.

    for datafile in testfiles:
        url = f"az://{TEST_BUCKET}/{BUCKET_SUBDIR}/{datafile}"
        run_command(f"COPY (SELECT generate_series(1,10) as id) TO '{url}'", pg_conn)

    pg_conn.commit()

    yield

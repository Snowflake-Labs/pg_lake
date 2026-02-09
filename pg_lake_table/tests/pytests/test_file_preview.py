import os
import pytest
from utils_pytest import *

BUCKET_SUBDIR = "test_create_from_s3_helper"

# (name, sql, results) tested with parquet files
file_preview_tests = [
    ("basic", "1::integer as a", [["a", "integer"]]),
    (
        "basic_2",
        """1::integer as a,
     ''::text as b""",
        [["a", "integer"], ["b", "text"]],
    ),
    (
        "array",
        """array_value(1::integer,2,3) as a,
     array_value('a','b','c','d') as b""",
        [["a", "integer[]"], ["b", "text[]"]],
    ),
    (
        "struct",
        """{ a: 1, b: 2 } struct_col""",
        # this generated type won't return the same type if we run on a replica
        # but is deterministic on a primary
        [["struct_col", "lake_struct.a_b_c1ddcbdb"]],
    ),
]

# (name, sql, results) tested with csv files with header
# without header columns will be named as column0, column1, ...
file_preview_tests_csv = [
    ("basic", "1::integer as a", [["a", "bigint"]]),
    (
        "basic_2",
        """1::integer as a,
     ''::text as b""",
        [["a", "bigint"], ["b", "text"]],
    ),
    (
        "array",
        """array_value(1::integer,2,3) as a,
     array_value('a','b','c','d') as b""",
        [["a", "text"], ["b", "text"]],
    ),
    ("struct", """{ a: 1, b: 2 } struct_col""", [["struct_col", "text"]]),
]


def test_file_preview_non_s3_url(pg_conn, extension):
    # error if not s3 url
    error = run_command(
        f"SELECT * from lake_file.preview('wonka://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/test.parquet', 'parquet')",
        pg_conn,
        raise_error=False,
    )
    assert (
        "only s3://, gs://, az://, azure://, and abfss:// urls are supported" in error
    )
    pg_conn.rollback()


def test_file_preview_invalid_second_argument(pg_conn, extension):
    # error if second argument is compression option key pair, we expect an explicit option
    error = run_command(
        f"SELECT * from lake_file.preview('s3://{TEST_BUCKET}/{BUCKET_SUBDIR}/a/test.parquet', 'wonka')",
        pg_conn,
        raise_error=False,
    )
    assert "not recognized" in error
    pg_conn.rollback()


@pytest.mark.parametrize(
    "name,input,expected", file_preview_tests, ids=[t[0] for t in file_preview_tests]
)
def test_file_preview(pg_conn, s3, extension, duckdb_conn, name, input, expected):
    # create files in s3 and verify that we get back what we expect from calling file_preview(), url only,
    # no compression option, no options, no wildcard

    url = f"s3://{TEST_BUCKET}/test_file_preview/{name}.parquet"
    run_command(f"COPY (SELECT {input}) TO '{url}'", duckdb_conn)

    res = run_query(f"SELECT * from lake_file.preview('{url}')", pg_conn)
    assert res == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "name,input,expected", file_preview_tests, ids=[t[0] for t in file_preview_tests]
)
def test_file_preview_with_wildcard(
    pg_conn, s3, extension, duckdb_conn, name, input, expected
):
    # create files in s3 and verify that we get back what we expect from calling file_preview() with a wildcard
    # no compression option, no options

    url = f"s3://{TEST_BUCKET}/test_file_preview_with_wildcard/{name}.parquet"
    wildcard = f"s3://{TEST_BUCKET}/*/{name}.parquet"
    run_command(f"COPY (SELECT {input}) TO '{url}'", duckdb_conn)

    res = run_query(f"SELECT * from lake_file.preview('{wildcard}')", pg_conn)
    assert res == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "name,input,expected",
    file_preview_tests_csv,
    ids=[t[0] for t in file_preview_tests_csv],
)
def test_file_preview_csv(pg_conn, s3, extension, duckdb_conn, name, input, expected):
    # create files in s3 and verify that we get back what we expect from calling file_preview() on a csv file

    url = f"s3://{TEST_BUCKET}/test_file_preview/{name}.csv"
    run_command(
        f"COPY (SELECT {input}) TO '{url}' with (format 'csv', header)", duckdb_conn
    )

    res = run_query(f"SELECT * from lake_file.preview('{url}')", pg_conn)
    assert res == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "name,input,expected",
    file_preview_tests_csv,
    ids=[t[0] for t in file_preview_tests_csv],
)
def test_file_preview_csv_compression_named_parameters(
    pg_conn, s3, extension, duckdb_conn, name, input, expected
):
    # create files in s3 and verify that we get back what we expect from calling file_preview() with named parameters

    url = f"s3://{TEST_BUCKET}/test_file_preview/{name}.csvgzipped"
    run_command(
        f"COPY (SELECT {input}) TO '{url}' with (format 'csv', header 'true', compression 'gzip')",
        duckdb_conn,
    )

    res = run_query(
        f"SELECT * from lake_file.preview(url := '{url}', format => 'csv', compression => 'gzip')",
        pg_conn,
    )
    assert res == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "name,input,expected",
    file_preview_tests_csv,
    ids=[t[0] for t in file_preview_tests_csv],
)
def test_file_preview_csv_compression_mixed_parameters(
    pg_conn, s3, extension, duckdb_conn, name, input, expected
):
    # create files in s3 and verify that we get back what we expect from calling file_preview() with mixed parameters

    url = f"s3://{TEST_BUCKET}/test_file_preview/{name}.csv.gz"
    run_command(
        f"COPY (SELECT {input}) TO '{url}' with (format 'csv', header 'true', compression 'gzip')",
        duckdb_conn,
    )

    res = run_query(
        f"SELECT * from lake_file.preview('{url}', compression => 'gzip')",
        pg_conn,
    )
    assert res == expected

    pg_conn.rollback()


def test_file_preview_csv_compression_invalid(pg_conn, s3, duckdb_conn, extension):
    # create files in s3 and verify that we get back an error from calling file_preview() with an invalid compression type
    # using named parameters

    url = f"s3://{TEST_BUCKET}/test_file_preview/basic.csv.gz"
    run_command(
        f"COPY (SELECT '1::integer as a') TO '{url}' with (format 'csv', header 'true', compression 'gzip')",
        duckdb_conn,
    )

    try:
        res = run_query(
            f"SELECT * from lake_file.preview(url := '{url}', compression => 'lz4')",
            pg_conn,
        )
    except psycopg2.errors.SyntaxError as e:
        assert 'compression "lz4" not recognized' in str(e)

    pg_conn.rollback()

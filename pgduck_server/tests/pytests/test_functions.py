import os
import pytest
from utils_pytest import *


def test_list_files_s3(pgduck_conn, s3):
    prefix = f"s3://{TEST_BUCKET}/test_list_files"

    run_command(
        f"""
        COPY (SELECT 1 id, 'list' val) TO '{prefix}/small.csv';
        COPY (SELECT 1 id, 'list' val FROM generate_series(1,100)) TO '{prefix}/large.csv';
    """,
        pgduck_conn,
    )

    result = run_query(
        f"select url, file_size, etag from pg_lake_list_files('{prefix}/*')",
        pgduck_conn,
    )
    assert len(result) == 2
    assert result[0] == [
        prefix + "/large.csv",
        "707",
        '"81d51ab47a013964a866395b6921e30d"',
    ]
    assert result[1] == [
        prefix + "/small.csv",
        "14",
        '"18a74ddc08d592acc32079bdedf76b99"',
    ]

    result = run_query(
        f"select sum(file_size) from pg_lake_list_files('{prefix}/*')", pgduck_conn
    )
    assert result[0][0] == "721"


def test_list_files_azure(pgduck_conn, azure):
    prefix = f"az://{TEST_BUCKET}/test_list_files"

    run_command(
        f"""
        COPY (SELECT 1 id, 'list' val) TO '{prefix}/small.csv';
        COPY (SELECT 1 id, 'list' val FROM generate_series(1,100)) TO '{prefix}/large.csv';
    """,
        pgduck_conn,
    )

    # Note: etag is not always the same in Azure blob storage (generated per request)
    result = run_query(
        f"select url, file_size from pg_lake_list_files('{prefix}/*')",
        pgduck_conn,
    )
    assert len(result) == 2
    assert result[0] == [prefix + "/large.csv", "707"]
    assert result[1] == [prefix + "/small.csv", "14"]

    result = run_query(
        f"select sum(file_size) from pg_lake_list_files('{prefix}/*')", pgduck_conn
    )
    assert result[0][0] == "721"


def test_list_files_paginate(pgduck_conn, s3, azure):
    prefix = f"s3://{TEST_BUCKET}/test_list_files_paginate"
    do_list_files_paginate(pgduck_conn)

    prefix = f"az://{TEST_BUCKET}/test_list_files_paginate"
    do_list_files_paginate(pgduck_conn)


def do_list_files_paginate(pgduck_conn):
    prefix = f"s3://{TEST_BUCKET}/test_list_files_paginate"

    # Generate >1000 files to hit pagination
    for id in range(1100):
        padded_id = f"{id:04}"
        run_command(
            f"""
            COPY (SELECT {id} id, 'list' val) TO '{prefix}/{padded_id}.csv'
        """,
            pgduck_conn,
        )

    result = run_query(
        f"select url from pg_lake_list_files('{prefix}/**')", pgduck_conn
    )
    assert len(result) == 1100
    assert result[1099][0] == prefix + "/1099.csv"

import pytest
import psycopg2
import datetime
from utils_pytest import *


test_pushdown_cases = [
    (
        "uuid_extract_version in target list",
        "SELECT uuid_extract_version(uuid_val) FROM test_uuid_function_pushdown.tbl ORDER BY 1",
        "uuid_extract_version",
        "170000",
    ),
    (
        "uuid_extract_version in where clause",
        "SELECT * FROM test_uuid_function_pushdown.tbl WHERE uuid_extract_version(uuid_val) = 1 ORDER BY uuid_val",
        "uuid_extract_version",
        "170000",
    ),
    (
        "uuid_extract_version in order by clause",
        "SELECT * FROM test_uuid_function_pushdown.tbl ORDER BY uuid_extract_version(uuid_val), uuid_val",
        "uuid_extract_version",
        "170000",
    ),
    (
        "uuid_extract_timestamp in target list",
        "SELECT uuid_val, uuid_extract_timestamp(uuid_val)::timestamp FROM test_uuid_function_pushdown.tbl ORDER BY 1,2",
        "uuid_extract_timestamp",
        "170000",
    ),
    (
        "uuid_extract_timestamp in where clause",
        "SELECT * FROM test_uuid_function_pushdown.tbl WHERE uuid_extract_timestamp(uuid_val) > now() ORDER BY uuid_val",
        "uuid_extract_timestamp",
        "170000",
    ),
    (
        "uuid_extract_timestamp in order by clause",
        "SELECT * FROM test_uuid_function_pushdown.tbl ORDER BY uuid_extract_timestamp(uuid_val), uuid_val",
        "uuid_extract_timestamp",
        "170000",
    ),
    (
        "uuidv7 in target list",
        "SELECT uuidv7() FROM test_uuid_function_pushdown.tbl ORDER BY 1",
        "uuidv7",
        "180000",
    ),
    (
        "uuidv7 in where clause",
        "SELECT * FROM test_uuid_function_pushdown.tbl WHERE uuid_val != uuidv7() ORDER BY uuid_val",
        "uuidv7",
        "180000",
    ),
    (
        "uuidv7 in order by clause",
        "SELECT * FROM test_uuid_function_pushdown.tbl ORDER BY uuidv7(), uuid_val",
        "uuidv7",
        "180000",
    ),
]


@pytest.mark.parametrize(
    "test_id, query, expected_function, exist_at_pg_version",
    test_pushdown_cases,
    ids=[t[0] for t in test_pushdown_cases],
)
def test_uuid_function_pushdown(
    create_uuid_test_table,
    pg_conn,
    test_id,
    query,
    expected_function,
    exist_at_pg_version,
):
    if get_server_version(pg_conn) < int(exist_at_pg_version):
        pytest.skip(
            f"Skipping test {test_id} as it requires PostgreSQL version {exist_at_pg_version}"
        )

    assert_remote_query_contains_expression(query, expected_function, pg_conn)

    if test_id == "uuidv7 in target list":
        # uuidv7 generates random UUIDs, so we cannot test the results
        return

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_uuid_function_pushdown.tbl"],
        ["test_uuid_function_pushdown.heap_tbl"],
    )


def test_uuid_extract_version_and_timestamp(create_uuid_test_table, pg_conn):
    pg_version = get_server_version(pg_conn)

    if pg_version < 170000:
        pytest.skip(
            "Skipping test_uuid_extract_version_and_timestamp as it requires PostgreSQL version 170000"
        )

    query = "SELECT uuid_extract_version(uuid_val), uuid_extract_timestamp(uuid_val)::timestamp FROM test_uuid_function_pushdown.tbl ORDER BY 1,2"

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_uuid_function_pushdown.tbl"],
        ["test_uuid_function_pushdown.heap_tbl"],
    )

    result = run_query(query, pg_conn)

    if pg_version < 180000:
        # no timestamp for UUID v7 in Postgres 17
        assert result == [
            [1, datetime.datetime(5001, 4, 26, 5, 27, 31, 380751)],
            [4, None],
            [5, None],
            [7, None],
            [None, None],
        ]
    else:
        # timestamp for UUID v7 in Postgres 18 and above
        assert result == [
            [1, datetime.datetime(5001, 4, 26, 5, 27, 31, 380751)],
            [4, None],
            [5, None],
            [7, datetime.datetime(2025, 5, 22, 19, 31, 40, 436000)],
            [None, None],
        ]


@pytest.fixture
def create_uuid_test_table(pg_conn, s3, extension, with_default_location):
    # UUID v7 test values (contains timestamp)
    uuid_v7_example = "0196f97a-db14-71c3-9132-9f0b1334466f"

    # UUID v1 test value (contains timestamp) - from PostgreSQL documentation
    # This is a valid UUID v1 that should extract a timestamp
    uuid_v1_example = "a0eebc99-9c0b-1ef8-bb6d-6bb9bd380a11"

    # UUID v4 test value (no timestamp)
    uuid_v4_example = "550e8400-e29b-41d4-a716-446655440000"

    # UUID v5 test value (no timestamp)
    uuid_v5_example = "886313e1-3b8a-5372-9b90-0c9aee199e5d"

    # create iceberg table
    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS test_uuid_function_pushdown;
        CREATE TABLE test_uuid_function_pushdown.tbl
        (
            uuid_val uuid
        ) USING iceberg;

        INSERT INTO test_uuid_function_pushdown.tbl VALUES
            (NULL),
            ('{uuid_v7_example}'),
            ('{uuid_v1_example}'),
            ('{uuid_v4_example}'),
            ('{uuid_v5_example}');
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_uuid_function_pushdown.heap_tbl
        (
            uuid_val uuid
        );
        """,
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_uuid_function_pushdown.heap_tbl VALUES
            (NULL),
            ('{uuid_v7_example}'::uuid),
            ('{uuid_v1_example}'::uuid),
            ('{uuid_v4_example}'::uuid),
            ('{uuid_v5_example}'::uuid);
        """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    pg_conn.rollback()

    run_command("DROP SCHEMA test_uuid_function_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def get_server_version(pg_conn):
    return int(run_query("show server_version_num", pg_conn)[0][0])

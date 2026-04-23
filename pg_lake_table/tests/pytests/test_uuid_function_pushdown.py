import pytest
from utils_pytest import *


# Test cases for UUID function pushdown (PostgreSQL 17+)
# uuidv7() generates a time-ordered UUID
# uuid_extract_timestamp() extracts the timestamp from a UUID
# uuid_extract_version() extracts the version from a UUID

test_cases = [
    (
        "uuidv7 in target list",
        "SELECT uuidv7() FROM uuid_test.tbl LIMIT 1",
        "uuidv7()",
    ),
    (
        "uuid_extract_version",
        "SELECT uuid_extract_version(uuid_val) FROM uuid_test.tbl",
        "uuid_extract_version",
    ),
    (
        "uuid_extract_timestamp",
        "SELECT uuid_extract_timestamp(uuid_val) FROM uuid_test.tbl",
        "uuid_extract_timestamp",
    ),
    (
        "uuid_extract_version in where",
        "SELECT * FROM uuid_test.tbl WHERE uuid_extract_version(uuid_val) = 4",
        "uuid_extract_version",
    ),
]


@pytest.mark.parametrize(
    "test_id, query, expected_expr",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_uuid_function_pushdown(
    create_uuid_test_table, pg_conn, test_id, query, expected_expr
):
    # UUID functions are only available in PostgreSQL 17+
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("UUID functions require PostgreSQL 17+")

    # Verify the expression is pushed down
    assert_remote_query_contains_expression(query, expected_expr, pg_conn)


def test_uuid_extract_timestamp_result(create_uuid_test_table, pg_conn):
    """Test that uuid_extract_timestamp returns correct results when pushed down."""
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("UUID functions require PostgreSQL 17+")

    # Compare results between foreign table and heap table
    query = "SELECT uuid_extract_timestamp(uuid_val) FROM uuid_test.tbl ORDER BY uuid_val"
    assert_query_results_on_tables(
        query, pg_conn, ["uuid_test.tbl"], ["uuid_test.heap_tbl"]
    )


def test_uuid_extract_version_result(create_uuid_test_table, pg_conn):
    """Test that uuid_extract_version returns correct results when pushed down."""
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("UUID functions require PostgreSQL 17+")

    # Compare results between foreign table and heap table
    query = "SELECT uuid_extract_version(uuid_val) FROM uuid_test.tbl ORDER BY uuid_val"
    assert_query_results_on_tables(
        query, pg_conn, ["uuid_test.tbl"], ["uuid_test.heap_tbl"]
    )


@pytest.fixture(scope="module")
def create_uuid_test_table(pg_conn, s3, extension):
    # UUID functions are only available in PostgreSQL 17+
    if get_pg_version_num(pg_conn) < 170000:
        yield
        return

    url = f"s3://{TEST_BUCKET}/create_uuid_test_table/data.parquet"

    run_command(
        f"""
        CREATE SCHEMA uuid_test;

        -- Create heap table with UUID values
        CREATE TABLE uuid_test.heap_tbl (
            id int,
            uuid_val uuid
        );

        -- Insert some UUIDs (version 4 random UUIDs)
        INSERT INTO uuid_test.heap_tbl VALUES
            (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
            (2, '85e1a478-36a5-4e5e-b5f4-4b5c53c86c4c'),
            (3, 'f47ac10b-58cc-4372-a567-0e02b2c3d479');

        -- Export to parquet
        COPY uuid_test.heap_tbl TO '{url}';

        -- Create foreign table
        CREATE FOREIGN TABLE uuid_test.tbl ()
        SERVER pg_lake OPTIONS (path '{url}');
        """,
        pg_conn,
    )

    yield

    pg_conn.rollback()
    run_command("DROP SCHEMA IF EXISTS uuid_test CASCADE", pg_conn)

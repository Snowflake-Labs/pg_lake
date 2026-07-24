import pytest
from utils_pytest import *


# Fixed sample UUIDs covering every semantic branch:
# - v1: RFC 9562 test vector, embeds 2022-02-22 19:22:22 UTC
# - v4: random UUID, has a version but no timestamp
# - v7: embeds a Unix millisecond timestamp
# - nil/max: not RFC 9562 variant, so version and timestamp are both NULL
UUID_SAMPLES = [
    ("v1", "c232ab00-9414-11ec-b3c8-9f68deced846"),
    ("v4", "5a8b2d1c-3f4e-4a6b-8c9d-0e1f2a3b4c5d"),
    ("v7", "01890a5d-ac96-774b-bcce-b302099a8057"),
    ("nil", "00000000-0000-0000-0000-000000000000"),
    ("max", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
]


@pytest.fixture(scope="module")
def create_uuid_pushdown_tables(pg_conn, s3, extension):
    if get_pg_version_num(pg_conn) < 180000:
        pytest.skip("uuidv7 and uuid_extract_* require PostgreSQL 18+")

    location = f"s3://{TEST_BUCKET}/uuid_f_pushdown/"

    values = ",".join(f"('{name}', '{uuid}')" for name, uuid in UUID_SAMPLES)

    run_command(
        f"""
        CREATE SCHEMA uuid_f_pushdown;
        CREATE TABLE uuid_f_pushdowntbl (name text, col_uuid uuid)
        USING pg_lake_iceberg WITH (location = '{location}');
        INSERT INTO uuid_f_pushdowntbl VALUES {values};
        CREATE TABLE uuid_f_pushdown_heap (name text, col_uuid uuid);
        INSERT INTO uuid_f_pushdown_heap VALUES {values};
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command(
        """
        DROP TABLE uuid_f_pushdowntbl;
        DROP TABLE uuid_f_pushdown_heap;
        DROP SCHEMA uuid_f_pushdown CASCADE;
        """,
        pg_conn,
    )
    pg_conn.commit()


@pytest.mark.parametrize(
    "test_id, func_expression, expected_expression",
    [
        (
            "uuid_extract_timestamp",
            "SELECT name, uuid_extract_timestamp(col_uuid) FROM uuid_f_pushdowntbl",
            '"uuid_extract_timestamp_pg"(',
        ),
        (
            "uuid_extract_version",
            "SELECT name, uuid_extract_version(col_uuid) FROM uuid_f_pushdowntbl",
            '"uuid_extract_version_pg"(',
        ),
    ],
    ids=["uuid_extract_timestamp", "uuid_extract_version"],
)
def test_uuid_extract_pushdown(
    create_uuid_pushdown_tables,
    pg_conn,
    test_id,
    func_expression,
    expected_expression,
):
    assert_remote_query_contains_expression(
        func_expression, expected_expression, pg_conn
    )

    # the pushed-down result must match Postgres evaluating the same
    # expression locally on an identical heap table, including the NULLs
    # for the v4, nil, and max UUIDs and the timestamp for the v1 UUID
    assert_query_results_on_tables(
        func_expression, pg_conn, ["uuid_f_pushdowntbl"], ["uuid_f_pushdown_heap"]
    )


def test_uuidv7_pushdown(create_uuid_pushdown_tables, pg_conn):
    query = "SELECT uuidv7() FROM uuid_f_pushdowntbl"

    assert_remote_query_contains_expression(query, '"uuidv7"()', pg_conn)

    # generated UUIDs must be version 7 with a timestamp close to now();
    # uuid_extract_* here also run inside DuckDB via the _pg wrappers
    result = run_query(
        """
        SELECT count(*)
        FROM (
            SELECT uuidv7() AS u FROM uuid_f_pushdowntbl
        ) s
        WHERE uuid_extract_version(u) = 7
          AND uuid_extract_timestamp(u) BETWEEN now() - interval '1 minute'
                                            AND now() + interval '1 minute'
        """,
        pg_conn,
    )
    assert result[0][0] == len(UUID_SAMPLES)


def test_uuidv7_interval_not_pushed_down(create_uuid_pushdown_tables, pg_conn):
    # uuidv7(interval) has no DuckDB equivalent and must stay unshipped;
    # the query still works with Postgres evaluating the function locally
    query = "SELECT uuidv7(interval '1 hour') FROM uuid_f_pushdowntbl"

    assert_remote_query_not_contains_expression(query, '"uuidv7"', pg_conn)

    result = run_query(
        f"""
        SELECT count(*)
        FROM (
            SELECT uuidv7(interval '1 hour') AS u FROM uuid_f_pushdowntbl
        ) s
        WHERE uuid_extract_version(u) = 7
        """,
        pg_conn,
    )
    assert result[0][0] == len(UUID_SAMPLES)

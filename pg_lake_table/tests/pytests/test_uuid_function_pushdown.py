import pytest
from utils_pytest import *


# Fixed sample UUIDs covering every semantic branch:
# - v1: RFC 9562 test vector, embeds 2022-02-22 19:22:22 UTC
# - v4: random UUID, has a version but no timestamp
# - v6: RFC 9562 test vector, same instant as v1 but Postgres only extracts
#   timestamps from v1 and v7, so the timestamp is NULL
# - v7: embeds a Unix millisecond timestamp
# - nil/max: not RFC 9562 variant, so version and timestamp are both NULL
# - null: NULL input, so version and timestamp are both NULL
UUID_SAMPLES = [
    ("v1", "c232ab00-9414-11ec-b3c8-9f68deced846"),
    ("v4", "5a8b2d1c-3f4e-4a6b-8c9d-0e1f2a3b4c5d"),
    ("v6", "1ec9414c-232a-6b00-b3c8-9f6bdeced846"),
    ("v7", "01890a5d-ac96-774b-bcce-b302099a8057"),
    ("nil", "00000000-0000-0000-0000-000000000000"),
    ("max", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
    ("null", None),
]


@pytest.fixture(scope="module")
def create_uuid_pushdown_tables(pg_conn, s3, extension):
    # uuid_extract_timestamp / uuid_extract_version exist in PostgreSQL 17+;
    # uuidv7 only in 18+ (those tests skip themselves below).
    if get_pg_version_num(pg_conn) < 170000:
        pytest.skip("uuid_extract_* require PostgreSQL 17+")

    location = f"s3://{TEST_BUCKET}/uuid_f_pushdown/"

    values = ",".join(
        f"('{name}', NULL)" if uuid is None else f"('{name}', '{uuid}')"
        for name, uuid in UUID_SAMPLES
    )

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
        # in a WHERE clause the wrappers must land in the pushed-down filter
        (
            "uuid_extract_timestamp_where",
            "SELECT name FROM uuid_f_pushdowntbl WHERE uuid_extract_timestamp(col_uuid) > '2020-01-01'",
            'WHERE ("uuid_extract_timestamp_pg"(',
        ),
        (
            "uuid_extract_version_where",
            "SELECT name FROM uuid_f_pushdowntbl WHERE uuid_extract_version(col_uuid) = 7",
            'WHERE ("uuid_extract_version_pg"(',
        ),
    ],
    ids=[
        "uuid_extract_timestamp",
        "uuid_extract_version",
        "uuid_extract_timestamp_where",
        "uuid_extract_version_where",
    ],
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
    # for the v4, v6, nil, max, and NULL UUIDs and the timestamp for the v1
    # UUID
    assert_query_results_on_tables(
        func_expression, pg_conn, ["uuid_f_pushdowntbl"], ["uuid_f_pushdown_heap"]
    )


@pytest.fixture
def create_uuidv7_generated_tables(pg_conn, s3, extension):
    if get_pg_version_num(pg_conn) < 180000:
        pytest.skip("uuidv7 requires PostgreSQL 18+")

    location = f"s3://{TEST_BUCKET}/uuid_f_pushdown_v7/"

    # Postgres generates the values into the heap table and the iceberg table
    # is filled from it, so both hold identical v7 UUIDs that carry Postgres'
    # sub-millisecond bits, including a shifted far-future one
    run_command(
        f"""
        CREATE TABLE uuid_f_pushdown_v7_heap (name text, col_uuid uuid);
        INSERT INTO uuid_f_pushdown_v7_heap
        SELECT 'now_' || i, uuidv7() FROM generate_series(1, 10) i;
        INSERT INTO uuid_f_pushdown_v7_heap
        VALUES ('shift_1000_years', uuidv7(interval '1000 years'));
        CREATE TABLE uuid_f_pushdown_v7tbl (name text, col_uuid uuid)
        USING pg_lake_iceberg WITH (location = '{location}');
        INSERT INTO uuid_f_pushdown_v7tbl SELECT * FROM uuid_f_pushdown_v7_heap;
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command(
        """
        DROP TABLE uuid_f_pushdown_v7tbl;
        DROP TABLE uuid_f_pushdown_v7_heap;
        """,
        pg_conn,
    )
    pg_conn.commit()


def test_uuid_extract_pushdown_postgres_generated_v7(
    create_uuidv7_generated_tables, pg_conn
):
    query = """
        SELECT name, uuid_extract_timestamp(col_uuid), uuid_extract_version(col_uuid)
        FROM uuid_f_pushdown_v7tbl
    """

    assert_remote_query_contains_expression(
        query, '"uuid_extract_timestamp_pg"(', pg_conn
    )
    assert_remote_query_contains_expression(
        query, '"uuid_extract_version_pg"(', pg_conn
    )

    # the wrappers must ignore the sub-millisecond bits exactly like Postgres
    assert_query_results_on_tables(
        query, pg_conn, ["uuid_f_pushdown_v7tbl"], ["uuid_f_pushdown_v7_heap"]
    )


def test_uuidv7_not_pushed_down(create_uuid_pushdown_tables, pg_conn):
    if get_pg_version_num(pg_conn) < 180000:
        pytest.skip("uuidv7 requires PostgreSQL 18+")

    # Postgres guarantees that uuidv7() values from one session strictly
    # increase, while DuckDB's sort randomly within a millisecond, so
    # uuidv7() must stay unshipped and be evaluated by Postgres locally
    query = "SELECT uuidv7() FROM uuid_f_pushdowntbl"

    assert_remote_query_not_contains_expression(query, '"uuidv7"', pg_conn)

    # generated UUIDs must be version 7 with a timestamp close to now()
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
    if get_pg_version_num(pg_conn) < 180000:
        pytest.skip("uuidv7 requires PostgreSQL 18+")

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

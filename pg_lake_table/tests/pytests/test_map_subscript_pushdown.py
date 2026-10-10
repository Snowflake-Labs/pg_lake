import pytest
from utils_pytest import *

# A map type from pg_map is a domain over an array of (key, val) pairs, so on
# the Postgres side m[1] is the first pair.  In DuckDB, m[1] on a MAP is the
# value for key 1, so a subscript on a map must not be pushed down.  Each test
# case compares the Iceberg table against a heap copy of the same data.
test_cases = [
    ("text_key_first_pair", "SELECT id, tm[1] FROM map_subscript.tbl"),
    ("text_key_pair_field", "SELECT id, (tm[1]).val FROM map_subscript.tbl"),
    ("int_key_first_pair", "SELECT id, im[1] FROM map_subscript.tbl"),
    ("int_key_second_pair", "SELECT id, im[2] FROM map_subscript.tbl"),
    ("int_key_pair_field", "SELECT id, (im[1]).val FROM map_subscript.tbl"),
    ("out_of_range", "SELECT id, im[4] FROM map_subscript.tbl"),
    ("non_constant_subscript", "SELECT id, im[id] FROM map_subscript.tbl"),
    ("slice", "SELECT id, im[1:2] FROM map_subscript.tbl"),
    ("where", "SELECT id FROM map_subscript.tbl WHERE (im[1]).val = 'five'"),
    ("order_by", "SELECT id FROM map_subscript.tbl ORDER BY (im[2]).key"),
    (
        "subscript_on_expression",
        "SELECT id, (COALESCE(im, im))[1] FROM map_subscript.tbl",
    ),
    (
        "map_array_element",
        "SELECT id, (marr[1])[2] FROM map_subscript.tbl",
    ),
]


@pytest.mark.parametrize(
    "test_id, query",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_map_subscript_results(create_map_subscript_table, pg_conn, test_id, query):
    assert_query_results_on_tables(
        query, pg_conn, ["map_subscript.tbl"], ["map_subscript.tbl_heap"]
    )


@pytest.mark.parametrize(
    "test_id, query",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_map_subscript_not_pushed_down(
    create_map_subscript_table, pg_conn, test_id, query
):
    assert_query_not_pushdownable(query, pg_conn)


def test_map_subscript_not_in_remote_where(create_map_subscript_table, pg_conn):
    query = "SELECT id FROM map_subscript.tbl WHERE (im[1]).val = 'five'"
    assert_remote_query_not_contains_expression(query, '"im"[1]', pg_conn)


# Subscripts on plain arrays, including arrays of composites and maps, keep the same
# meaning in DuckDB and should still be pushed down.
array_test_cases = [
    ("int_array", "SELECT id, ints[2] FROM map_subscript.tbl", '"ints"[2]'),
    (
        "composite_array",
        "SELECT id, (pairs[1]).b FROM map_subscript.tbl",
        '"pairs"[1]',
    ),
    ("map_array", "SELECT id, marr[1] FROM map_subscript.tbl", '"marr"[1]'),
]


@pytest.mark.parametrize(
    "test_id, query, expected_expression",
    array_test_cases,
    ids=[test_case[0] for test_case in array_test_cases],
)
def test_array_subscript_pushdown(
    create_map_subscript_table, pg_conn, test_id, query, expected_expression
):
    assert_query_pushdownable(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["map_subscript.tbl"], ["map_subscript.tbl_heap"]
    )


# Key lookups are rewritten to map_extract() and are not affected.
key_lookup_test_cases = [
    ("extract_function", "SELECT id, map_type.extract(im, 1) FROM map_subscript.tbl"),
    ("extract_operator", "SELECT id, im -> 1 FROM map_subscript.tbl"),
]


@pytest.mark.parametrize(
    "test_id, query",
    key_lookup_test_cases,
    ids=[test_case[0] for test_case in key_lookup_test_cases],
)
def test_map_key_lookup_pushdown(create_map_subscript_table, pg_conn, test_id, query):
    assert_query_pushdownable(query, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["map_subscript.tbl"], ["map_subscript.tbl_heap"]
    )


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


@pytest.fixture(scope="module")
def create_map_subscript_table(s3, pg_conn, superuser_conn, extension):
    run_command(
        """
        SELECT map_type.create('text', 'int');
        SELECT map_type.create('int', 'text');
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        CREATE SCHEMA map_subscript;
        CREATE TYPE map_subscript.pair AS (a int, b text);

        CREATE TABLE map_subscript.tbl (
            id int,
            tm map_type.key_text_val_int,
            im map_type.key_int_val_text,
            ints int[],
            pairs map_subscript.pair[],
            marr map_type.key_int_val_text[]
        )
        USING iceberg
        WITH (location = 's3://{TEST_BUCKET}/map_subscript/');

        INSERT INTO map_subscript.tbl VALUES
            (1,
             ARRAY[('a', 10), ('b', 20)]::map_type.key_text_val_int,
             ARRAY[(5, 'five'), (1, 'one')]::map_type.key_int_val_text,
             ARRAY[1, 2, 3],
             ARRAY[(1, 'x'), (2, 'y')]::map_subscript.pair[],
             ARRAY[ARRAY[(5, 'five'), (2, 'two')]::map_type.key_int_val_text]),
            (2,
             ARRAY[('c', 30)]::map_type.key_text_val_int,
             ARRAY[(4, 'four'), (7, 'seven')]::map_type.key_int_val_text,
             ARRAY[4, 5],
             ARRAY[(3, 'z')]::map_subscript.pair[],
             ARRAY[ARRAY[(2, 'two'), (9, 'nine')]::map_type.key_int_val_text]);

        CREATE TABLE map_subscript.tbl_heap AS SELECT * FROM map_subscript.tbl;
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command("DROP SCHEMA map_subscript CASCADE", pg_conn)
    pg_conn.commit()

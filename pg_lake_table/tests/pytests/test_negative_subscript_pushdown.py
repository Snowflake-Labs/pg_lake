import pytest
from utils_pytest import *

# DuckDB counts negative list indexes from the end, while Postgres treats them
# as out of range: an element is NULL and a slice bound is clamped to the
# array bounds.  Subscripts are only pushed down when every index is a
# non-negative constant, NULL or omitted.  Each test case compares the Iceberg
# table against a heap copy of the same data.


def assert_same_results_as_heap(query, pg_conn):
    # assert_query_results_on_tables() compares lists element by element with
    # zip(), so an empty slice matches any array.  Compare whole rows instead.
    iceberg = run_query(query, pg_conn)
    heap = run_query(
        query.replace("neg_subscript.tbl", "neg_subscript.tbl_heap"), pg_conn
    )
    assert len(heap) > 0, "No rows returned, make sure at least one row returns"
    assert sorted(map(tuple, iceberg), key=str) == sorted(map(tuple, heap), key=str)


negative_test_cases = [
    ("element_last", "SELECT id, ints[-1] FROM neg_subscript.tbl"),
    ("element_first", "SELECT id, ints[-3] FROM neg_subscript.tbl"),
    ("slice_negative_lower", "SELECT id, ints[-1:2] FROM neg_subscript.tbl"),
    ("slice_negative_upper", "SELECT id, ints[1:-1] FROM neg_subscript.tbl"),
    ("slice_both_negative", "SELECT id, ints[-5:-1] FROM neg_subscript.tbl"),
    # -1 for id 1 and 0 for id 2
    ("column_index", "SELECT id, ints[id - 2] FROM neg_subscript.tbl"),
    ("column_slice", "SELECT id, ints[id - 2:2] FROM neg_subscript.tbl"),
    ("where", "SELECT id FROM neg_subscript.tbl WHERE ints[-1] IS NULL"),
    (
        "where_column_index",
        "SELECT id FROM neg_subscript.tbl WHERE ints[id - 2] IS NULL",
    ),
]


@pytest.mark.parametrize(
    "test_id, query",
    negative_test_cases,
    ids=[test_case[0] for test_case in negative_test_cases],
)
def test_negative_subscript_results(
    create_neg_subscript_table, pg_conn, test_id, query
):
    assert_same_results_as_heap(query, pg_conn)


@pytest.mark.parametrize(
    "test_id, query",
    negative_test_cases,
    ids=[test_case[0] for test_case in negative_test_cases],
)
def test_negative_subscript_not_pushed_down(
    create_neg_subscript_table, pg_conn, test_id, query
):
    assert_query_not_pushdownable(query, pg_conn)


def test_negative_subscript_not_in_remote_where(create_neg_subscript_table, pg_conn):
    query = "SELECT id FROM neg_subscript.tbl WHERE ints[-1] IS NULL"
    assert_remote_query_not_contains_expression(query, "'-1'::integer", pg_conn)


@pytest.mark.parametrize("plan_cache_mode", ["force_custom_plan", "force_generic_plan"])
@pytest.mark.parametrize("index", [-1, -3, 0, 2])
def test_parameter_subscript_results(
    create_neg_subscript_table, pg_conn, plan_cache_mode, index
):
    run_command(f"SET LOCAL plan_cache_mode TO {plan_cache_mode}", pg_conn)
    results = {}
    for table in ["neg_subscript.tbl", "neg_subscript.tbl_heap"]:
        name = "p_" + table.split(".")[1]
        run_command(
            f"PREPARE {name}(int) AS SELECT id, ints[$1] FROM {table} ORDER BY id",
            pg_conn,
        )
        results[table] = run_query(f"EXECUTE {name}({index})", pg_conn)
        run_command(f"DEALLOCATE {name}", pg_conn)

    assert results["neg_subscript.tbl"] == results["neg_subscript.tbl_heap"]


# Index 0, positive indexes, NULL, omitted bounds and reversed slices mean the
# same thing in DuckDB and should still be pushed down.
pushdown_test_cases = [
    ("element_zero", "SELECT id, ints[0] FROM neg_subscript.tbl"),
    ("element_first", "SELECT id, ints[1] FROM neg_subscript.tbl"),
    ("element_out_of_range", "SELECT id, ints[4] FROM neg_subscript.tbl"),
    ("element_null", "SELECT id, ints[NULL::int] FROM neg_subscript.tbl"),
    ("slice_omitted_lower", "SELECT id, ints[:2] FROM neg_subscript.tbl"),
    ("slice_omitted_upper", "SELECT id, ints[2:] FROM neg_subscript.tbl"),
    ("slice_reversed", "SELECT id, ints[2:1] FROM neg_subscript.tbl"),
    ("slice_zero_lower", "SELECT id, ints[0:2] FROM neg_subscript.tbl"),
    ("slice_zero_both", "SELECT id, ints[0:0] FROM neg_subscript.tbl"),
]


@pytest.mark.parametrize(
    "test_id, query",
    pushdown_test_cases,
    ids=[test_case[0] for test_case in pushdown_test_cases],
)
def test_non_negative_subscript_pushdown(
    create_neg_subscript_table, pg_conn, test_id, query
):
    assert_query_pushdownable(query, pg_conn)
    assert_same_results_as_heap(query, pg_conn)


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


@pytest.fixture(scope="module")
def create_neg_subscript_table(s3, pg_conn, extension):
    run_command(
        f"""
        CREATE SCHEMA neg_subscript;

        CREATE TABLE neg_subscript.tbl (
            id int,
            ints int[]
        )
        USING iceberg
        WITH (location = 's3://{TEST_BUCKET}/neg_subscript/');

        INSERT INTO neg_subscript.tbl VALUES
            (1, ARRAY[1, 2, 3]),
            (2, ARRAY[4, 5]);

        CREATE TABLE neg_subscript.tbl_heap AS SELECT * FROM neg_subscript.tbl;
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command("DROP SCHEMA neg_subscript CASCADE", pg_conn)
    pg_conn.commit()

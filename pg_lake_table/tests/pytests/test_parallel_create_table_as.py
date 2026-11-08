import pytest
from utils_pytest import *

# CREATE TABLE AS, SELECT INTO and materialized views can run their query with
# a parallel plan.  The leader then reads Iceberg tables in parallel mode, after
# the command has already created the new relation.  Each case forces a parallel
# scan of a heap table next to an Iceberg table and compares the result with the
# same query on heap tables.

PARALLEL_SETTINGS = """
    SET LOCAL parallel_setup_cost TO 0;
    SET LOCAL parallel_tuple_cost TO 0;
    SET LOCAL min_parallel_table_scan_size TO 0;
    SET LOCAL max_parallel_workers_per_gather TO 2;
"""

UNION_QUERY = "SELECT a FROM parallel_ctas.heap UNION SELECT a FROM parallel_ctas.{ice}"
JOIN_QUERY = (
    "SELECT h.a FROM parallel_ctas.heap h JOIN parallel_ctas.{ice} i ON i.a = h.a"
)

statement_cases = [
    ("create_table_as", "CREATE TABLE parallel_ctas.result AS {query}", UNION_QUERY),
    (
        "create_temp_table_on_commit_drop",
        "CREATE TEMP TABLE result ON COMMIT DROP AS {query}",
        UNION_QUERY,
    ),
    (
        "create_table_as_join",
        "CREATE TABLE parallel_ctas.result AS {query}",
        JOIN_QUERY,
    ),
    ("select_into", "SELECT a INTO parallel_ctas.result FROM ({query}) s", UNION_QUERY),
    (
        "create_materialized_view",
        "CREATE MATERIALIZED VIEW parallel_ctas.result AS {query}",
        UNION_QUERY,
    ),
]


def assert_parallel_plan(query, pg_conn):
    plan = "\n".join(row[0] for row in run_query("EXPLAIN " + query, pg_conn))
    assert "Gather" in plan, plan


def result_rows(table, pg_conn):
    return run_query(f"SELECT a FROM {table} ORDER BY a", pg_conn)


@pytest.mark.parametrize("ice", ["ice_partitioned", "ice_plain"])
@pytest.mark.parametrize(
    "test_id, statement, query",
    statement_cases,
    ids=[case[0] for case in statement_cases],
)
def test_parallel_write_reads_iceberg_table(
    create_parallel_ctas_tables, pg_conn, ice, test_id, statement, query
):
    query = query.format(ice=ice)
    run_command(PARALLEL_SETTINGS, pg_conn)
    assert_parallel_plan(query, pg_conn)

    run_command(statement.format(query=query), pg_conn)

    result_table = "result" if "TEMP" in statement else "parallel_ctas.result"
    expected = run_query(
        f"SELECT a FROM ({query.replace(ice, ice + '_heap')}) s ORDER BY a", pg_conn
    )
    assert result_rows(result_table, pg_conn) == expected


def test_parallel_refresh_materialized_view(create_parallel_ctas_tables, pg_conn):
    query = UNION_QUERY.format(ice="ice_partitioned")
    run_command(f"CREATE MATERIALIZED VIEW parallel_ctas.result AS {query}", pg_conn)
    run_command("INSERT INTO parallel_ctas.ice_partitioned VALUES (102)", pg_conn)

    run_command(PARALLEL_SETTINGS, pg_conn)
    assert_parallel_plan(query, pg_conn)
    run_command("REFRESH MATERIALIZED VIEW parallel_ctas.result", pg_conn)

    assert [102] in result_rows("parallel_ctas.result", pg_conn)


def test_parallel_create_table_as_sees_earlier_changes_in_transaction(
    create_parallel_ctas_tables, pg_conn
):
    run_command(
        """
        ALTER TABLE parallel_ctas.ice_plain ADD COLUMN b int DEFAULT 7;
        INSERT INTO parallel_ctas.ice_plain VALUES (103, 9);
        """,
        pg_conn,
    )
    query = (
        "SELECT a, NULL::int AS b FROM parallel_ctas.heap WHERE a <= 2 "
        "UNION SELECT a, b FROM parallel_ctas.ice_plain"
    )
    run_command(PARALLEL_SETTINGS, pg_conn)
    assert_parallel_plan(query, pg_conn)

    run_command(f"CREATE TEMP TABLE result AS {query}", pg_conn)

    assert run_query("SELECT a, b FROM result ORDER BY a, b NULLS FIRST", pg_conn) == [
        [1, None],
        [1, 7],
        [2, None],
        [2, 7],
        [101, 7],
        [103, 9],
    ]


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


@pytest.fixture(scope="module")
def create_parallel_ctas_tables(s3, pg_conn, extension):
    run_command(
        f"""
        CREATE SCHEMA parallel_ctas;

        CREATE TABLE parallel_ctas.heap AS SELECT i AS a FROM generate_series(1, 100) i;
        ANALYZE parallel_ctas.heap;

        CREATE TABLE parallel_ctas.ice_partitioned (a int) USING iceberg
            WITH (location = 's3://{TEST_BUCKET}/parallel_ctas/partitioned/',
                  partition_by = 'a');
        INSERT INTO parallel_ctas.ice_partitioned VALUES (1), (2), (101);

        CREATE TABLE parallel_ctas.ice_plain (a int) USING iceberg
            WITH (location = 's3://{TEST_BUCKET}/parallel_ctas/plain/');
        INSERT INTO parallel_ctas.ice_plain VALUES (1), (2), (101);

        CREATE TABLE parallel_ctas.ice_partitioned_heap AS
            SELECT * FROM parallel_ctas.ice_partitioned;
        CREATE TABLE parallel_ctas.ice_plain_heap AS
            SELECT * FROM parallel_ctas.ice_plain;
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command("DROP SCHEMA parallel_ctas CASCADE", pg_conn)
    pg_conn.commit()

import pytest
from utils_pytest import *

# Tests for whole-query pushdown of a partitioned table whose partitions are all
# lake tables. Such a parent holds no data of its own, so it is read as the
# UNION ALL of its partitions inside DuckDB, which is what lets a query over the
# parent be pushed down instead of being aggregated in PostgreSQL.

AGGREGATE_QUERY = """
SELECT count(*), sum(id)::bigint FROM part_pushdown.parent
"""


@pytest.fixture(scope="module")
def lake_partitioned_table(extension, pg_conn, s3):
    prefix = f"s3://{TEST_BUCKET}/test_lake_partitioned_parent"

    for part in range(1, 3):
        run_command(
            f"""
            COPY (SELECT g AS id, g * 2 AS value
                    FROM generate_series({part * 10}, {part * 10 + 4}) g)
            TO '{prefix}/part{part}.parquet' WITH (FORMAT 'parquet')
            """,
            pg_conn,
        )

    run_command(
        f"""
        DROP SCHEMA IF EXISTS part_pushdown CASCADE;
        CREATE SCHEMA part_pushdown;

        CREATE TABLE part_pushdown.parent (id int, value int)
            PARTITION BY RANGE (id);

        CREATE FOREIGN TABLE part_pushdown.lake1
            PARTITION OF part_pushdown.parent FOR VALUES FROM (10) TO (20)
            SERVER pg_lake OPTIONS (format 'parquet', path '{prefix}/part1.parquet');

        CREATE FOREIGN TABLE part_pushdown.lake2
            PARTITION OF part_pushdown.parent FOR VALUES FROM (20) TO (30)
            SERVER pg_lake OPTIONS (format 'parquet', path '{prefix}/part2.parquet');
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA IF EXISTS part_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def test_all_lake_partitioned_parent_is_pushed_down(lake_partitioned_table, pg_conn):
    assert_query_pushdownable(AGGREGATE_QUERY, pg_conn)
    pg_conn.commit()

    # 10..14 and 20..24
    result = run_query(AGGREGATE_QUERY, pg_conn)
    pg_conn.commit()
    assert tuple(result[0]) == (10, 170)

    # both partitions are read, and EXPLAIN counts the files of both
    explain = perform_query_on_cursor(
        "EXPLAIN (ANALYZE, VERBOSE, format JSON) " + AGGREGATE_QUERY, pg_conn
    )[0]
    pg_conn.commit()
    assert fetch_data_files_used(explain) == "2"

    # the parent is addressed by name in the deparsed query, and DuckDB reads it
    # as the union of the two partitions
    assert "part_pushdown.parent" in fetch_remote_sql(explain)

    plan = str(run_query("EXPLAIN (ANALYZE, VERBOSE) " + AGGREGATE_QUERY, pg_conn))
    pg_conn.commit()
    assert "UNION" in plan
    assert plan.count("Function: READ_PARQUET") == 2


def test_from_only_parent_reads_no_rows(lake_partitioned_table, pg_conn):
    # a partitioned table has no storage of its own, so FROM ONLY reads nothing,
    # whether or not the query is pushed down
    result = run_query("SELECT count(*) FROM ONLY part_pushdown.parent", pg_conn)
    pg_conn.commit()
    assert tuple(result[0]) == (0,)


def test_parent_with_heap_partition_is_not_pushed_down(lake_partitioned_table, pg_conn):
    # one plain PostgreSQL partition, which pgduck_server cannot read on its own,
    # is enough to keep the whole parent out of the pushdown
    run_command(
        """
        CREATE TABLE part_pushdown.heap3
            PARTITION OF part_pushdown.parent FOR VALUES FROM (30) TO (40);
        INSERT INTO part_pushdown.heap3 SELECT g, g * 2 FROM generate_series(30, 34) g;
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        assert_query_not_pushdownable(AGGREGATE_QUERY, pg_conn)
        pg_conn.commit()

        # and the answer still covers every partition
        result = run_query(AGGREGATE_QUERY, pg_conn)
        pg_conn.commit()
        assert tuple(result[0]) == (15, 330)
    finally:
        run_command("DROP TABLE part_pushdown.heap3", pg_conn)
        pg_conn.commit()


def test_partition_with_reordered_columns_is_not_pushed_down(
    lake_partitioned_table, pg_conn
):
    # ATTACH PARTITION matches columns by name, so a partition may hold them in a
    # different order than the parent. Reading the tree as one scan addresses
    # every partition through the parent's tuple descriptor, which would mix up
    # the columns of such a partition, so we refuse to push it down.
    prefix = f"s3://{TEST_BUCKET}/test_lake_partitioned_parent"

    run_command(
        f"""
        COPY (SELECT g * 2 AS value, g AS id FROM generate_series(30, 34) g)
        TO '{prefix}/part3.parquet' WITH (FORMAT 'parquet');

        CREATE FOREIGN TABLE part_pushdown.reordered (value int, id int)
            SERVER pg_lake OPTIONS (format 'parquet', path '{prefix}/part3.parquet');

        ALTER TABLE part_pushdown.parent ATTACH PARTITION part_pushdown.reordered
            FOR VALUES FROM (30) TO (40);
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        assert_query_not_pushdownable(AGGREGATE_QUERY, pg_conn)
        pg_conn.commit()

        # the ordinary plan maps the columns by name, so the answer is right
        result = run_query(AGGREGATE_QUERY, pg_conn)
        pg_conn.commit()
        assert tuple(result[0]) == (15, 330)
    finally:
        run_command("DROP FOREIGN TABLE part_pushdown.reordered", pg_conn)
        pg_conn.commit()

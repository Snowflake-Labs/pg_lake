import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


@pytest.fixture(scope="module")
def create_partitioned_tables(s3, pg_conn, extension):

    # create Parquet files in mock S3 to serve as our partitions

    parquet_url = {}

    for part in range(1, 4):
        parquet_url[part] = (
            f"s3://{TEST_BUCKET}/test_partitioned_tables/part{part}.parquet"
        )

        run_command(
            f"""
            COPY (SELECT generate_series as id, '{chr(ord('a')+ part)}' as letter, now()::timestamptz as timestamptz_col from generate_series({part * 10},{part * 10 + 5}) ) TO '{parquet_url[part]}' WITH (FORMAT 'parquet');
        """,
            pg_conn,
        )

    pg_conn.commit()

    # Create the partitions
    run_command(
        """
                CREATE SCHEMA test_partition;
                CREATE TABLE test_partition.parent (id int, letter char, timestamptz_col timestamptz)
                    PARTITION BY RANGE (id);
                CREATE TABLE test_partition.parent_2 (id int, letter char, timestamptz_col timestamptz)
                    PARTITION BY RANGE (id);
    """,
        pg_conn,
    )

    for part in range(1, 4):
        run_command(
            f"""
                CREATE FOREIGN TABLE test_partition.ft{part} PARTITION OF test_partition.parent
                    FOR VALUES FROM ({part * 10}) TO ({(part+1) * 10})
                SERVER pg_lake OPTIONS (format 'parquet', path '%s');

                CREATE FOREIGN TABLE test_partition.ft{part}_2 PARTITION OF test_partition.parent_2
                    FOR VALUES FROM ({part * 10}) TO ({(part+1) * 10})
                SERVER pg_lake OPTIONS (format 'parquet', path '%s');


        """
            % (parquet_url[part], parquet_url[part]),
            pg_conn,
        )

    yield

    run_command("DROP SCHEMA test_partition CASCADE", pg_conn)
    pg_conn.commit()


def test_partitioned(create_partitioned_tables, pg_conn):
    """Tests a partitioned table made up of 3 separate foreign partitions"""

    res = run_query(
        "SELECT DISTINCT tableoid::regclass FROM test_partition.parent ORDER BY 1",
        pg_conn,
    )
    assert res == [
        ["test_partition.ft1"],
        ["test_partition.ft2"],
        ["test_partition.ft3"],
    ]

    res = run_query("SELECT id FROM test_partition.parent ORDER BY id", pg_conn)
    assert res == [
        [10],
        [11],
        [12],
        [13],
        [14],
        [15],
        [20],
        [21],
        [22],
        [23],
        [24],
        [25],
        [30],
        [31],
        [32],
        [33],
        [34],
        [35],
    ]

    res = run_query("SELECT id FROM test_partition.ft1 ORDER BY id", pg_conn)
    assert res == [[10], [11], [12], [13], [14], [15]]

    res = run_query("SELECT id FROM test_partition.ft2 ORDER BY id", pg_conn)
    assert res == [[20], [21], [22], [23], [24], [25]]

    res = run_query("SELECT id FROM test_partition.ft3 ORDER BY id", pg_conn)
    assert res == [[30], [31], [32], [33], [34], [35]]


def test_partition_wise_agg(create_partitioned_tables, pg_conn):

    # when enable_partitionwise_aggregate=True, the count(*) is pushed down to the Vectorized SQL
    run_command("BEGIN; SET enable_partitionwise_aggregate TO true;", pg_conn)
    partition_wise_agg = "SELECT count(*) FROM test_partition.parent GROUP BY id"
    partition_wise_explain = "EXPLAIN (ANALYZE, VERBOSE) " + partition_wise_agg
    partition_wise_explain_result = perform_query_on_cursor(
        partition_wise_explain, pg_conn
    )

    for partition_wise_line in partition_wise_explain_result:
        if "Vectorized SQL" in partition_wise_line[0]:
            assert "SELECT count(*)" in partition_wise_line[0]

    run_command("COMMIT", pg_conn)

    # when enable_partitionwise_aggregate=false, the count(*) is NOT pushed down to the Vectorized SQL
    run_command("BEGIN; SET enable_partitionwise_aggregate TO false;", pg_conn)
    non_partition_wise_agg = "SELECT count(*) FROM test_partition.parent GROUP BY id"
    non_partition_wise_explain = "EXPLAIN (ANALYZE, VERBOSE) " + non_partition_wise_agg
    non_partition_wise_explain_result = perform_query_on_cursor(
        non_partition_wise_explain, pg_conn
    )

    for non_partition_wise_line in non_partition_wise_explain_result:
        if "Vectorized SQL" in non_partition_wise_line[0]:
            assert "SELECT id" in non_partition_wise_line[0]

    run_command("COMMIT", pg_conn)


def test_partition_wise_partial_agg(create_partitioned_tables, pg_conn):

    # when enable_partitionwise_aggregate=True, we pull the data to Postgres
    # and apply partial aggs
    run_command("BEGIN; SET enable_partitionwise_aggregate TO true;", pg_conn)
    partition_wise_agg = "SELECT count(*) FROM test_partition.parent GROUP BY letter"
    partition_wise_explain = "EXPLAIN (ANALYZE, VERBOSE) " + partition_wise_agg
    partition_wise_explain_result = perform_query_on_cursor(
        partition_wise_explain, pg_conn
    )

    foundPartialAgg = False
    for partition_wise_line in partition_wise_explain_result:
        if "PARTIAL count(*)" in partition_wise_line[0]:
            foundPartialAgg = True

    assert foundPartialAgg

    run_command("COMMIT", pg_conn)

    # when enable_partitionwise_aggregate=false, the count(*) is NOT pushed down to the Vectorized SQL
    # GROUP BY on non-partition key
    run_command("BEGIN; SET enable_partitionwise_aggregate TO false;", pg_conn)
    non_partition_wise_agg = (
        "SELECT count(*) FROM test_partition.parent GROUP BY letter"
    )
    non_partition_wise_explain = "EXPLAIN (ANALYZE, VERBOSE) " + non_partition_wise_agg
    non_partition_wise_explain_result = perform_query_on_cursor(
        non_partition_wise_explain, pg_conn
    )

    for non_partition_wise_line in non_partition_wise_explain_result:
        print(non_partition_wise_line)
        if "Vectorized SQL" in non_partition_wise_line[0]:
            assert "SELECT letter" in non_partition_wise_line[0]

    run_command("COMMIT", pg_conn)


def test_partition_wise_join(create_partitioned_tables, pg_conn):

    # when enable_partitionwise_join=True, the JOIN is pushed down to the Vectorized SQL
    run_command("BEGIN; SET LOCAL enable_partitionwise_join TO true;", pg_conn)
    partition_wise_agg = "SELECT count(*) FROM test_partition.parent JOIN test_partition.parent_2 USING(id)"
    partition_wise_explain = "EXPLAIN (ANALYZE, VERBOSE) " + partition_wise_agg
    partition_wise_explain_result = perform_query_on_cursor(
        partition_wise_explain, pg_conn
    )

    foundRemoteSql, foundRemoteJoin = False, False
    for partition_wise_line in partition_wise_explain_result:
        if "Vectorized SQL" in partition_wise_line[0]:
            foundRemoteSql = True

        if foundRemoteSql and "JOIN" in partition_wise_line[0]:
            foundRemoteJoin = True
    assert foundRemoteJoin, "Couldn't find JOIN in the Vectorized SQL"
    run_command("COMMIT", pg_conn)

    # when enable_partitionwise_join=False, the JOIN is NOT pushed down to the Vectorized SQL
    run_command("BEGIN; SET LOCAL enable_partitionwise_join TO false;", pg_conn)
    non_partition_wise_agg = "SELECT count(*) FROM test_partition.parent JOIN test_partition.parent_2 USING(id)"
    non_partition_wise_explain = "EXPLAIN (ANALYZE, VERBOSE) " + non_partition_wise_agg
    non_partition_wise_explain_result = perform_query_on_cursor(
        non_partition_wise_explain, pg_conn
    )

    foundRemoteSql = False
    for non_partition_wise_line in non_partition_wise_explain_result:
        print(non_partition_wise_line)
        if "Vectorized SQL" in non_partition_wise_line[0]:
            foundRemoteSql = True

        if foundRemoteSql and "JOIN" in non_partition_wise_line[0]:
            assert False, "Unexpectedly found JOIN in the Vectorized SQL"

    run_command("COMMIT", pg_conn)


def test_partitioned_mixed(create_partitioned_tables, pg_conn):
    """Tests partitioned table with foreign partitions plus a local one"""

    run_command(
        """
        CREATE TABLE test_partition.loc1 PARTITION OF test_partition.parent
            FOR VALUES FROM (40) TO (50);
        INSERT INTO test_partition.loc1 SELECT generate_series, 'b', now()
            FROM generate_series(40, 45);
    """,
        pg_conn,
    )

    res = run_query("SELECT id FROM test_partition.parent ORDER BY id", pg_conn)
    assert res == [
        [10],
        [11],
        [12],
        [13],
        [14],
        [15],
        [20],
        [21],
        [22],
        [23],
        [24],
        [25],
        [30],
        [31],
        [32],
        [33],
        [34],
        [35],
        [40],
        [41],
        [42],
        [43],
        [44],
        [45],
    ]

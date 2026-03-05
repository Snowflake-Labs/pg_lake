import pytest
from utils_pytest import *
from decimal import Decimal

table_types = ["pg_lake_iceberg", "pg_lake", "heap"]
scale_factors = [0, 0.001]
iteration_counts = [1, 10]


@pytest.mark.parametrize(
    "table_type,scale_factor,iteration_count",
    [
        (table_type, scale_factor, iteration_count)
        for table_type in table_types
        for scale_factor in scale_factors
        for iteration_count in iteration_counts
    ],
)
def test_tpch_udfs(
    pg_conn,
    s3,
    pg_lake_benchmark_extension,
    with_default_location,
    table_type,
    scale_factor,
    iteration_count,
):
    location = f"s3://{TEST_BUCKET}/tpch"
    run_command(
        f"""select lake_tpch.gen(location => '{location}',
                                table_type => '{table_type}',
                                scale_factor => {scale_factor},
                                iteration_count => {iteration_count})""",
        pg_conn,
    )
    pg_conn.commit()

    total_nations_rows = run_query("select count(*) from nation", pg_conn)[0][0]
    assert total_nations_rows == 25 if scale_factor > 0 else total_nations_rows == 0

    total_parts_rows = run_query("select count(*) from part", pg_conn)[0][0]

    if scale_factor == 0.001:
        assert total_parts_rows == 200
    elif scale_factor == 0:
        assert total_parts_rows == 0

    # run the first query only (not meant to run all queries, EXPLAIN ANALYZE all queries below test)
    run_command(
        f"select lake_tpch.run_query(query_nr) from lake_tpch.queries() where query_nr = 1",
        pg_conn,
    )

    # try to run invalid query number
    error = run_command(f"select lake_tpch.run_query(23)", pg_conn, raise_error=False)

    assert "query id must be between 1 and 22" in error

    pg_conn.rollback()


def test_tpch_pushdown(pg_conn, s3, pg_lake_benchmark_extension, with_default_location):
    location = f"s3://{TEST_BUCKET}/tpch"
    run_command(
        f"""select lake_tpch.gen(location => '{location}',
                                table_type => 'pg_lake_iceberg',
                                scale_factor => 0.01)""",
        pg_conn,
    )
    pg_conn.commit()

    queries = run_query("select query_nr, query from lake_tpch.queries()", pg_conn)

    assert len(queries) == 22

    for query_nr, query in queries:
        explain = f"EXPLAIN (ANALYZE) {query}"
        result = perform_query_on_cursor(explain, pg_conn)
        pg_conn.commit()

        if "Custom Scan (Query Pushdown)" not in str(result):
            assert False, f"failed for query id: {query_nr}"

    pg_conn.rollback()


def test_tpch_answers(
    pg_conn, pgduck_conn, s3, pg_lake_benchmark_extension, with_default_location
):
    location = f"s3://{TEST_BUCKET}/tpch"
    run_command(
        f"""select lake_tpch.gen(location => '{location}',
                                table_type => 'pg_lake_iceberg',
                                scale_factor => 0.01)""",
        pg_conn,
    )
    pg_conn.commit()

    queries = run_query("select query_nr, query from lake_tpch.queries()", pg_conn)

    assert len(queries) == 22

    for query_nr, query in queries:
        query = query.strip(";")

        # create result table from query
        run_command(f"CREATE TABLE result_{query_nr} USING iceberg AS {query}", pg_conn)

        # create answer table with the same schema as the result table and copy the answer from tpch_answers
        answer_location = f"s3://{TEST_BUCKET}/tpch/answers/{query_nr}.csv"
        run_command(
            f"""COPY (select trim(answer, '\n') from tpch_answers() where scale_factor = 0.01 and query_nr = {query_nr})
                        TO '{answer_location}' (format csv, delim '|', header false, quote '');""",
            pgduck_conn,
        )

        # Load CSV into a temp heap table, then create the answer as an iceberg
        # table so both sides go through the same DECIMAL(38,9) truncation path.
        run_command(
            f"""
                    CREATE TEMP TABLE _load_answer_{query_nr} (LIKE result_{query_nr});
                    COPY _load_answer_{query_nr} FROM '{answer_location}' (format 'csv', delimiter '|',
                                                                     header 'true', quote '');
                    """,
            pg_conn,
        )

        run_command(
            f"CREATE TABLE answer_{query_nr} USING iceberg AS SELECT * FROM _load_answer_{query_nr}",
            pg_conn,
        )

        # compare the result and answer tables
        are_same = run_query(
            f"""
            SELECT COUNT(*) = 0
            FROM (
                SELECT * FROM result_{query_nr} EXCEPT SELECT * FROM answer_{query_nr}
                UNION ALL
                SELECT * FROM answer_{query_nr} EXCEPT SELECT * FROM result_{query_nr}
            ) AS t
                    """,
            pg_conn,
        )[0][0]

        assert are_same, f"failed for query id: {query_nr}"

    pg_conn.rollback()


def test_tpch_partitioned_answers(
    pg_conn,
    superuser_conn,
    pgduck_conn,
    s3,
    pg_lake_benchmark_extension,
    with_default_location,
):
    location = f"s3://{TEST_BUCKET}/tpch"
    run_command(
        f"""select lake_tpch.gen_partitioned(location => '{location}',
                                table_type => 'pg_lake_iceberg',
                                scale_factor => 0.01,
                                lineitem_partition_by:='bucket(32, l_orderkey)',
                                 customer_partition_by:='truncate(20, c_nationkey)',
                                 orders_partition_by:='month(o_orderdate)',
                                 part_partition_by:='truncate(10000, p_brand)',
                                 region_partition_by:='r_regionkey',
                                 supplier_partition_by:='s_nationkey, bucket(2, s_comment)',
                                 nation_partition_by:='n_nationkey'
                                )""",
        pg_conn,
    )
    pg_conn.commit()

    queries = run_query("select query_nr, query from lake_tpch.queries()", pg_conn)

    assert len(queries) == 22

    for query_nr, query in queries:
        query = query.strip(";")

        # create result table from query
        run_command(f"CREATE TABLE result_{query_nr} USING iceberg AS {query}", pg_conn)

        # create answer table with the same schema as the result table and copy the answer from tpch_answers
        answer_location = f"s3://{TEST_BUCKET}/tpch/answers/{query_nr}.csv"
        run_command(
            f"""COPY (select trim(answer, '\n') from tpch_answers() where scale_factor = 0.01 and query_nr = {query_nr})
                        TO '{answer_location}' (format csv, delim '|', header false, quote '');""",
            pgduck_conn,
        )

        # Load CSV into a temp heap table, then create the answer as an iceberg
        # table so both sides go through the same DECIMAL(38,9) truncation path.
        run_command(
            f"""
                    CREATE TEMP TABLE _load_answer_{query_nr} (LIKE result_{query_nr});
                    COPY _load_answer_{query_nr} FROM '{answer_location}' (format 'csv', delimiter '|',
                                                                     header 'true', quote '');
                    """,
            pg_conn,
        )

        run_command(
            f"CREATE TABLE answer_{query_nr} USING iceberg AS SELECT * FROM _load_answer_{query_nr}",
            pg_conn,
        )

        # compare the result and answer tables
        are_same = run_query(
            f"""
            SELECT COUNT(*) = 0
            FROM (
                SELECT * FROM result_{query_nr} EXCEPT SELECT * FROM answer_{query_nr}
                UNION ALL
                SELECT * FROM answer_{query_nr} EXCEPT SELECT * FROM result_{query_nr}
            ) AS t
                    """,
            pg_conn,
        )[0][0]

        assert are_same, f"failed for query id: {query_nr}"

    pg_conn.rollback()

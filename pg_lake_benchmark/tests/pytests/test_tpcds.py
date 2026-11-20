import pytest
from utils_pytest import *

table_types = ["pg_lake_iceberg", "pg_lake", "heap"]
scale_factors = [0, 0.01]


@pytest.mark.parametrize(
    "table_type,scale_factor",
    [
        (table_type, scale_factor)
        for table_type in table_types
        for scale_factor in scale_factors
    ],
)
def test_tpcds_udfs(
    pg_conn,
    s3,
    pg_lake_benchmark_extension,
    with_default_location,
    table_type,
    scale_factor,
):
    # takes too long to run for heap table type
    if table_type == "heap" and scale_factor > 0:
        return

    location = f"s3://{TEST_BUCKET}/tpcds"
    run_command(
        f"""select lake_tpcds.gen(location => '{location}',
                                 table_type => '{table_type}',
                                 scale_factor => {scale_factor})""",
        pg_conn,
    )
    pg_conn.commit()

    total_time_dim_rows = run_query("select count(*) from time_dim", pg_conn)[0][0]
    assert (
        total_time_dim_rows == 86400 if scale_factor > 0 else total_time_dim_rows == 0
    )

    total_store_sales_rows = run_query("select count(*) from store_sales", pg_conn)[0][
        0
    ]

    if scale_factor == 0.01:
        assert total_store_sales_rows == 28810
    elif scale_factor == 0:
        assert total_store_sales_rows == 0

    # run the first query only (not meant to run all queries, EXPLAIN ANALYZE all queries below test)
    run_command(
        f"select lake_tpcds.run_query(query_nr) from lake_tpcds.queries() where query_nr = 1",
        pg_conn,
    )

    # try to run invalid query number
    error = run_command(f"select lake_tpcds.run_query(100)", pg_conn, raise_error=False)

    assert "query id must be between 1 and 99" in error

    pg_conn.rollback()


def test_tpcds_pushdown(
    pg_conn, s3, pg_lake_benchmark_extension, with_default_location
):
    location = f"s3://{TEST_BUCKET}/tpcds"
    scale_factor = 0.01
    run_command(
        f"""select lake_tpcds.gen(location => '{location}',
                                 table_type => 'pg_lake_iceberg',
                                 scale_factor => {scale_factor})""",
        pg_conn,
    )
    pg_conn.commit()

    queries = run_query("select query_nr, query from lake_tpcds.queries()", pg_conn)

    assert len(queries) == 99

    for query_nr, query in queries:
        explain = f"EXPLAIN (ANALYZE) {query}"
        result = perform_query_on_cursor(explain, pg_conn)
        pg_conn.commit()

        if "Custom Scan (Query Pushdown)" not in str(result):
            assert False, f"failed for query id: {query_nr}"

    pg_conn.rollback()


def test_tpcds_answers(
    pg_conn, pgduck_conn, s3, pg_lake_benchmark_extension, with_default_location
):
    location = f"s3://{TEST_BUCKET}/tpcds"
    scale_factor = 0.01
    run_command(
        f"""select lake_tpcds.gen(location => '{location}',
                                 table_type => 'pg_lake_iceberg',
                                 scale_factor => {scale_factor})""",
        pg_conn,
    )
    pg_conn.commit()

    # NOTE: tpcds_answers() throw error from duckdb, so we check answers for a few queries

    # check query 6 result
    query_6 = run_query(
        "select query from lake_tpcds.queries() where query_nr = 6", pg_conn
    )[0][0]

    result = run_query(query_6, pg_conn)

    # DuckDB 2.0's dsdgen generates slightly different data at this scale
    # factor; on the data 1.5 generated, 2.0 also answers VA 12
    assert result == [["NC", 11], ["VA", 11]]

    # check query 94 result
    query_94 = run_query(
        "select query from lake_tpcds.queries() where query_nr = 94", pg_conn
    )[0][0]

    result = run_query(query_94, pg_conn)

    assert result == [[0, None, None]]

    pg_conn.rollback()

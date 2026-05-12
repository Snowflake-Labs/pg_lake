import pytest
import psycopg2
import time
import duckdb
import math
import re
from utils_pytest import *

from_clause_join_queries = [
    "SELECT * FROM test_fdw_join.ft1 t1 {join_type} JOIN test_fdw_join.ft1 t2 USING ({join_column})",  # join between parquet
    "SELECT * FROM test_fdw_join.ft2 t1 {join_type} JOIN test_fdw_join.ft2 t2 USING ({join_column})",  # join between csv
    "SELECT * FROM test_fdw_join.ft3 t1 {join_type} JOIN test_fdw_join.ft3 t2 USING ({join_column})",  # join between json
    "SELECT sum(t1.int_col), max(t2.text_col), max(t1.timestamp_col), min(t2.timestamptz_col), avg(t2.numeric_col) FROM test_fdw_join.ft1 t1 {join_type} JOIN test_fdw_join.ft2 t2 USING ({join_column})",
    "SELECT sum(t1.int_col), max(t2.text_col), max(t1.timestamp_col), min(t2.timestamptz_col), avg(t2.numeric_col) FROM test_fdw_join.ft1 t1 {join_type} JOIN test_fdw_join.ft2 t2 USING ({join_column}) GROUP BY t1.int_col, t2.text_col, t1.timestamp_col",
    "SELECT sum(t1.int_col), max(t2.text_col), max(t1.timestamp_col), min(t2.timestamptz_col), avg(t2.numeric_col) FROM test_fdw_join.ft1 t1 {join_type} JOIN test_fdw_join.ft2 t2 USING ({join_column}) GROUP BY t1.int_col, t2.text_col, t1.timestamp_col HAVING count(*) > 1000",
    "SELECT t1.int_col, MAX(t2.text_col) AS max_text, CASE WHEN t5.timestamptz_col > NOW() THEN 'Future' ELSE 'Past' END AS time_relation FROM test_fdw_join.ft1 t1 {join_type} JOIN test_fdw_join.ft1 t2 USING ({join_column}) {join_type} JOIN test_fdw_join.ft1 t3 USING ({join_column}) {join_type} JOIN test_fdw_join.ft3 t4 USING ({join_column}) {join_type} JOIN test_fdw_join.ft1 t5 USING ({join_column}) GROUP BY t1.int_col, t5.timestamptz_col, t4.timestamp_col, t3.numeric_col ORDER BY  max_text LIMIT 10",
    "SELECT t1.int_col, COUNT(*) FILTER (WHERE t2.text_col LIKE 'A%') AS count_start_a, MIN(t4.timestamp_col) AS min_timestamp, CASE WHEN COUNT(t5.int_col) > 5 THEN 'High' ELSE 'Low' END AS volume FROM test_fdw_join.ft2 t1 {join_type} JOIN test_fdw_join.ft2 t2 USING ({join_column}) {join_type} JOIN test_fdw_join.ft3 t3 USING ({join_column}) {join_type} JOIN test_fdw_join.ft1 t4 USING ({join_column}) {join_type} JOIN test_fdw_join.ft2 t5 USING ({join_column}) GROUP BY t1.int_col, t1.text_col, t3.numeric_col ORDER BY volume DESC, count_start_a LIMIT 5",
]

semi_join_queries = [
    """
    SELECT t1.*
    FROM test_fdw_join.ft1 t1
    WHERE EXISTS (
    SELECT 1
    FROM test_fdw_join.ft1 t2
    WHERE t1.int_col = t2.int_col AND t2.int_col > 100);""",
    """
    SELECT ft2.*, ft4.*
    FROM test_fdw_join.ft1 AS ft2
    INNER JOIN (
        SELECT * FROM test_fdw_join.ft1 AS ft4
        WHERE EXISTS (
            SELECT 1 FROM test_fdw_join.ft1 AS ft5
            WHERE ft4.int_col = ft5.int_col
        )
    ) AS ft4 ON ft2.numeric_col = ft4.int_col
    INNER JOIN (
        SELECT * FROM test_fdw_join.ft1 AS ft5
        WHERE EXISTS (
            SELECT 1 FROM test_fdw_join.ft1 AS ft4
            WHERE ft4.int_col = ft5.int_col
        )
    ) AS ft5 ON ft2.numeric_col <= ft5.int_col
    WHERE ft2.int_col > 900
    ORDER BY ft2.int_col LIMIT 10;
""",
]


def test_join_plans(create_join_tables, pg_conn):

    join_types = ["INNER", "LEFT", "FULL"]
    join_columns = [
        "int_col",
        "text_col",
        "timestamp_col",
        "timestamptz_col",
        "numeric_col",
    ]

    for query_template in from_clause_join_queries:
        for join_type in join_types:
            for join_column in join_columns:

                # more than 2 FULL JOINs cannot be pushed down
                if join_type == "FULL" and len(query_template.split("{join_type}")) > 2:
                    continue

                generated_query = query_template.format(
                    join_type=join_type, join_column=join_column
                )
                assert_has_foreign_scan_at_top(
                    query_to_assert := generated_query, pg_conn
                )


def test_semi_join(create_join_tables, pg_conn):
    for query in semi_join_queries:
        assert_has_foreign_scan_at_top(query_to_assert := query, pg_conn)


def assert_has_foreign_scan_at_top(query_to_assert, pg_conn):

    # TODO: make it EXPLAIN ANALYZE
    # However, it currently crashes on Duckdb with the following exception
    # libc++abi: terminating due to uncaught exception of type duckdb::InternalException: {"exception_type":"INTERNAL","exception_message":"Attempted to dereference unique_ptr that is NULL!"}
    explain = "EXPLAIN  " + query_to_assert
    result = perform_query_on_cursor(explain, pg_conn)
    pg_conn.commit()
    first_line = result[0][0]

    if not (
        "Custom Scan (Query Pushdown)" in str(result)
        or has_foreign_scan_at_top(first_line)
    ):
        assert False, "failed for query: " + query


def has_foreign_scan_at_top(explain_output: str) -> bool:

    if explain_output.strip().startswith("Foreign Scan"):
        return True

    return False


def test_join_explain_output(create_join_tables, pg_conn):

    query = "SELECT * FROM test_fdw_join.ft1 JOIN test_fdw_join.ft2 USING (int_col)"

    for execution_type in ["vectorize", "pushdown"]:
        if execution_type == "vectorize":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )
        else:
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )

        for prefix in [
            "EXPLAIN  (verbose, format json)",
            "EXPLAIN  (analyze, verbose, format json)",
        ]:

            explain = prefix + query
            result = perform_query_on_cursor(explain, pg_conn)
            explain_output = fetch_remote_sql(result[0])

            # explain shows the user tables
            # not internal representations
            assert "test_fdw_join" in explain_output
            assert "ft1" in explain_output

            if execution_type == "vectorize":
                assert "test_fdw_join.ft2" in explain_output
                assert '"int_col"' in explain_output
                pattern = re.compile(r'"r\d+"\."int_col" = "r\d+"\."int_col"')
                assert pattern.search(explain_output)
            else:
                assert "test_fdw_join" in explain_output
                assert "ft2" in explain_output
                assert 'USING ("int_col"' in explain_output
            pg_conn.commit()
        run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)


@pytest.fixture(scope="module")
def create_join_tables(s3, pg_conn, extension):

    # Create a Parquet file in mock S3
    parquet_url = f"s3://{TEST_BUCKET}/test_s3_join_tables/data.parquet"
    run_command(
        f"""
        COPY (SELECT  4 as int_col, '3.16' as text_col, now()::timestamp as timestamp_col, now()::timestamptz as timestamptz_col, 2.555::NUMERIC(6,2) as numeric_col FROM generate_series(0,0) s) TO '{parquet_url}' WITH (FORMAT 'parquet');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a CSV file in mock S3
    csv_url = f"s3://{TEST_BUCKET}/test_s3_join_tables/data.csv"
    run_command(
        f"""
        COPY (SELECT  4 as int_col, '3.16' as text_col, now()::timestamp as timestamp_col, now()::timestamptz as timestamptz_col, 2.555::NUMERIC(6,2) as numeric_col FROM generate_series(0,0) s) TO '{csv_url}' WITH (FORMAT 'csv');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a JSON file in mock S3
    json_url = f"s3://{TEST_BUCKET}/test_s3_join_tables/data.json"
    run_command(
        f"""
        COPY (SELECT  4 as int_col, '3.16' as text_col, now()::timestamp as timestamp_col, now()::timestamptz as timestamptz_col, 2.555::NUMERIC(6,2) as numeric_col FROM generate_series(0,0) s) TO '{json_url}' WITH (FORMAT 'json');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA test_fdw_join;
                CREATE FOREIGN TABLE test_fdw_join.ft1 (
                    int_col int,
                    text_col text,
                    timestamp_col timestamp,
                    timestamptz_col timestamptz,
                    numeric_col numeric(18,3)
                  -- we don't load data, so an empty path is OK
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');
        """
        % (parquet_url),
        pg_conn,
    )

    run_command(
        """
                CREATE FOREIGN TABLE test_fdw_join.ft2 (
                    int_col int,
                    text_col text,
                    timestamp_col timestamp,
                    timestamptz_col timestamptz,
                    numeric_col numeric(18,3)
                  -- we don't load data, so an empty path is OK
                ) SERVER pg_lake OPTIONS (format 'csv', path '%s');
        """
        % (csv_url),
        pg_conn,
    )

    run_command(
        """
                CREATE FOREIGN TABLE test_fdw_join.ft3 (
                    int_col int,
                    text_col text,
                    timestamp_col timestamp,
                    timestamptz_col timestamptz,
                    numeric_col numeric(18,3)
                  -- we don't load data, so an empty path is OK
                ) SERVER pg_lake OPTIONS (format 'json', path '%s');
        """
        % (json_url),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_fdw_join CASCADE", pg_conn)
    pg_conn.commit()

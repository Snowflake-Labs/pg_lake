import pytest
import re
from utils_pytest import *

queries = [
    "SELECT DISTINCT COUNT(DISTINCT metric1) AS unique_metric1 FROM Users_f GROUP BY metric2 ORDER BY unique_metric1 DESC LIMIT 5;",
    "SELECT DISTINCT id, MIN(float_value) * 2 AS min_doubled, MAX(float_value) / 2 AS max_halved, SUM(metric1), COUNT(metric2), AVG(metric1) FROM Users_f GROUP BY id;",
    "SELECT DISTINCT COUNT(*) FILTER (WHERE id = 3) AS count_id_3, SUM(metric2) FILTER (WHERE metric2 > 10) AS sum_metric2_filtered FROM Users_f;",
    "SELECT DISTINCT substring(CAST(metric1 AS VARCHAR), 4) AS metric1_label FROM Users_f;",
    "SELECT DISTINCT metric1, COALESCE((50000 / metric1)::int::BOOLEAN, FALSE) AS metric1_bool FROM Users_f WHERE metric1 > 0 ORDER BY metric1;",
    "SELECT DISTINCT NULLIF(metric2, 111110) AS safe_division FROM Users_f ORDER BY 1;",
    "SELECT DISTINCT id, ROW(metric1, metric2, float_value) > ROW(10, 20, 30.0) AS row_comparison FROM Users_f ORDER BY 1,2;",
    "SELECT DISTINCT id, (metric1 / 100)::INT::BOOLEAN AND CASE WHEN id > 100 THEN metric1 / 100 > 1 ELSE FALSE END AND COALESCE((metric2 / 50000)::BOOLEAN, FALSE) AND NULLIF((metric2 / 50000)::BOOLEAN, FALSE) AND metric1 IS DISTINCT FROM 500 AND ROW(metric1, metric2, float_value) > ROW(2000, 2, 30.0) AS complex_boolean FROM Users_f;",
    "SELECT DISTINCT COUNT(DISTINCT (SELECT metric1 FROM Users_f WHERE metric1 > 100)) AS distinct_metric1_over_100 FROM Users_f GROUP BY metric2 ORDER BY distinct_metric1_over_100 DESC;",
    "SELECT DISTINCT id, metric1, SUM(metric1) OVER (PARTITION BY id ORDER BY timestamp) AS running_total, MAX(metric2) OVER (PARTITION BY id) AS max_metric2_per_id, (CAST(metric1 AS VARCHAR)) AS metric1_label FROM Users_f;",
]


def test_select_expression_pushdown(create_pushdown_tables, pg_conn):

    for query in queries:

        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )

        # then make sure we are actually pushing down the expressions
        results = run_query("EXPLAIN (ANALYZE, VERBOSE) " + query, pg_conn)
        if not ("Custom Scan (Query Pushdown)" in str(results)):
            assert False, "select expression pushdown query failed:" + query


whole_var_queries = [
    "SELECT count(t1.*), count(t1.*)::text FROM whole_var.fdw_test t1 LEFT JOIN whole_var.fdw_test USING (id)",
    "SELECT count(t1.*), count(t2.*), count(*), count(t1.id), count(t2.id), count(t1.value), count(t2.value) FROM whole_var.fdw_test t1 RIGHT JOIN whole_var.fdw_test t2 USING (id)",
    "SELECT num_nulls(t1.*) FROM whole_var.fdw_test t1 LEFT JOIN whole_var.fdw_test USING (id)",
    "SELECT num_nulls(t1.*) FROM whole_var.fdw_test t1 RIGHT JOIN whole_var.fdw_test USING (id)",
    "SELECT (t1.*)::text, (t2.*)::text FROM whole_var.fdw_test t1 LEFT JOIN whole_var.fdw_test t2 USING (id)",
    "SELECT (t1.*)::text, (t2.*)::text FROM whole_var.fdw_test t1 RIGHT JOIN whole_var.fdw_test t2 USING (id)",
    "SELECT (t1.*)::text FROM whole_var.fdw_test t1, whole_var.fdw_test t2 WHERE t1.id = t2.id",
]


def test_whole_var_queries(s3, extension, pg_conn):

    location = f"s3://{TEST_BUCKET}/whole_var/"

    # Empty table syntax is not supported
    error = run_command(
        f"""
        CREATE SCHEMA whole_var;

		-- Add some timestamps with typemod that are not supported in DuckDB
        CREATE FOREIGN TABLE whole_var.fdw_test (id int, value text, t1 timestamp(6), t2 timestamptz(2))
		SERVER pg_lake OPTIONS (writable 'true', format 'parquet', location '{location}');

        INSERT INTO whole_var.fdw_test VALUES
            (-1, '-1', '2000-01-01', '2000-01-01'),
            (-1, '-1', '2000-01-01', '2000-01-01'),
            (0, '0','2000-02-02','2000-02-02'),
            (0, '0', '2000-02-02','2000-02-02'),
            (NULL, NULL, NULL,NULL),
            (NULL, NULL, NULL, NULL),
            (1, '1', '2001-01-01','2001-01-01'),
            (1, '1', '2001-01-01','2001-01-01');

        CREATE TABLE whole_var.heap_test AS SELECT * FROM whole_var.fdw_test;
    """,
        pg_conn,
    )

    for execution in ["pushdown", "fdw"]:
        if execution == "pushdown":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        elif execution == "fdw":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )
        for query in whole_var_queries:
            assert_query_results_on_tables(query, pg_conn, ["fdw_test"], ["heap_test"])
            results = run_query("EXPLAIN (VERBOSE) " + query, pg_conn)

            # this is very specifically tied to our implementation where
            # we deparse queries like whole_var_queries in a way that
            # each column is checked whether each column is NULL or NOT
            if not re.search(r'\("r\d+"\."id" IS NOT NULL\)', str(results)):
                assert False, "whole-var query failed:" + query

    run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
    run_command("DROP SCHEMA whole_var CASCADE;", pg_conn)

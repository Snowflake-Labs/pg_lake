import pytest
from utils_pytest import *


# we hide our internal queries from pg_stat_statements, pgaudit and autoexplain
# we make sure the mechanics are not broken, so test with pg_stat_statements
def test_hide_queries(pg_conn, superuser_conn, s3, extension, with_default_location):

    # we don't have pg_stat_statements on installcheck
    if installcheck:
        return

    run_command(
        f"""
		CREATE SCHEMA test_hide_queries;
		CREATE TABLE test_hide_queries.tbl(key int) USING iceberg WITH (partition_by='truncate(10, key)');
		INSERT INTO test_hide_queries.tbl SELECT i FROM generate_series(0,20)i;

		""",
        pg_conn,
    )

    pg_conn.commit()

    # make tests robust to other environments
    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT * FROM public.pg_stat_statements_reset()", superuser_conn)
    run_command('SET pg_stat_statements.track TO "all" ', superuser_conn)
    superuser_conn.commit()

    # let's run bunch of queries, and show that no internal
    run_command(
        "INSERT INTO test_hide_queries.tbl SELECT i FROM generate_series(0,20)i;",
        pg_conn,
    )
    run_command("UPDATE test_hide_queries.tbl SET key = -1 WHERE key = 1;", pg_conn)
    run_command("UPDATE test_hide_queries.tbl SET key = -1 WHERE key > 4;", pg_conn)
    run_command("SELECT count(*) FROM test_hide_queries.tbl", pg_conn)
    pg_conn.commit()

    res = run_query(
        """SELECT count(*) FROM pg_stat_statements WHERE 
						query ILIKE '%field_id_mappings%'
						OR query ILIKE '%data_file_column_stats%'
						OR query ILIKE '%partition_specs%'
						OR query ILIKE '%partition_fields%'
						OR query ILIKE '%data_file_partition_values%'
						OR query ILIKE '%files%'
					""",
        superuser_conn,
    )

    # we never access to above catalog tables
    assert res[0][0] == 0

    run_command("DROP SCHEMA test_hide_queries CASCADE", pg_conn)
    pg_conn.commit()

    run_command("RESET pg_stat_statements.track", superuser_conn)
    superuser_conn.commit()


# we had issues with parallel queries in Postgres combined with
# iceberg tables
def test_parallel_query(pg_conn, superuser_conn, s3, extension, with_default_location):

    run_command(
        """

			CREATE SCHEMA test_parallel_query;
			CREATE TABLE test_parallel_query.heap(a int);
			INSERT INTO test_parallel_query.heap SELECT i FROM generate_series(0,100)i;

			-- force parallel
			SET parallel_setup_cost TO 0;
			SET parallel_tuple_cost TO 0;
			SET min_parallel_table_scan_size TO 0;

			CREATE TABLE test_parallel_query.iceberg(a int) USING iceberg;
			INSERT INTO test_parallel_query.iceberg SELECT i FROM generate_series(0,100)i;

		""",
        pg_conn,
    )

    # first, make sure we use parallel query
    res = run_query("EXPLAIN (VERBOSE) SELECT * FROM test_parallel_query.heap", pg_conn)
    assert_parallel_query(res)

    res = run_query(
        "EXPLAIN (VERBOSE) SELECT * FROM test_parallel_query.heap h JOIN test_parallel_query.iceberg USING (a)",
        pg_conn,
    )
    assert_parallel_query(res)

    run_command(
        """
			RESET parallel_setup_cost;
			RESET parallel_tuple_cost;
			RESET min_parallel_table_scan_size;
		""",
        pg_conn,
    )

    pg_conn.rollback()


def assert_parallel_query(results):

    foundGather, foundWorkers = False, False
    for line in results:

        if "gather" in line[0].lower():
            foundGather = True
        if "workers planned:" in line[0].lower():
            foundWorkers = True

    assert foundGather and foundWorkers

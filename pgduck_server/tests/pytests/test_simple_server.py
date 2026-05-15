from utils_pytest import *
import csv


def test_queries(pgduck_server, tmp_path):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )

    queries = [
        {"query": "SELECT 1, NULL::int", "expected_rows": 1, "expected_columns": 2},
        {"query": "SELECT pow(2,30)::int", "expected_rows": 1, "expected_columns": 1},
        {
            "query": "SELECT * FROM (VALUES (1,2),(2,3),(3,4)) as foo",
            "expected_rows": 3,
            "expected_columns": 2,
        },
        {
            "query": "SELECT * FROM (VALUES (1,2),(2,3),(3,4)) as foo LIMIT 0",
            "expected_rows": 0,
            "expected_columns": 2,
        },
        {
            # lots of columns/rows
            "query": "SELECT "
            + ", ".join([f"i1::int" for i in range(1, 36)])
            + " FROM generate_series(0,100000) as gen(i1)",
            "expected_rows": 100001,  # Including 0
            "expected_columns": 35,
        },
        {
            # Large rows
            "query": "SELECT repeat('#', 2000000), repeat('$', 2000000) FROM generate_series(1,3)",
            "expected_rows": 3,
            "expected_columns": 2,
        },
        {
            # Subquery
            "query": "SELECT unnest(a)::int FROM (SELECT generate_series(1,10)) as subquery(a)",
            "expected_rows": 10,
            "expected_columns": 1,
        },
        {
            # Join operation
            "query": "SELECT t1.a FROM (VALUES (1, 'a'), (2, 'b')) as t1(a,b) JOIN (VALUES (1, 'x'), (2, 'y')) as t2 (a,b) ON t1.a = t2.a",
            "expected_rows": 2,
            "expected_columns": 1,
        },
        {
            # Aggregation
            "query": "SELECT COUNT(*)::int FROM generate_series(1,100)",
            "expected_rows": 1,
            "expected_columns": 1,
        },
        {"query": "SET test = 15", "expected_rows": None, "expected_columns": None},
        {"query": "BEGIN", "expected_rows": None, "expected_columns": None},
        # Avoid leaving the BEGIN open
        {"query": "COMMIT", "expected_rows": None, "expected_columns": None},
    ]

    # Test regular protocol
    for query_info in queries:
        result = perform_query_on_cursor(query_info["query"], conn)

        # failed queries
        if query_info["expected_rows"] is None:
            assert None == result
            assert None == query_info["expected_columns"]
        else:
            assert len(result) == query_info["expected_rows"]
            if result:
                assert len(result[0]) == query_info["expected_columns"]

    # Test transmit protocol
    for query_info in queries:
        command = "TrANSMIT  " + query_info["query"]
        print(command)

        # We cannot use copy_to_file on commands that don't use copy protocol,
        # but we can still run them using regular query protocol when they have
        # the transmit prefix.
        if query_info["expected_rows"] is None:
            result = perform_query_on_cursor(command, conn)
            assert None == result
            assert None == query_info["expected_columns"]
            continue

        csv_path = tmp_path / "data.csv"
        copy_to_file(command, csv_path, conn)

        row_count = 0
        column_count = 0

        with open(csv_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",", quotechar='"')
            for row in csv_reader:
                column_count = len(row)
                row_count += 1

        assert row_count == query_info["expected_rows"]

        if row_count > 0:
            assert column_count == query_info["expected_columns"]

    conn.close()


def test_basic_parquet_copy_and_query(pgduck_server):

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )

    gen_data_query = "COPY (SELECT i as col_1,i+i col_2,NULL as col_3 from generate_series(0,1000) as data(i)) TO '/tmp/example_table.parquet' (FORMAT 'parquet');"
    perform_query_on_cursor(gen_data_query, conn)

    select_data_query = "SELECT count(*), sum(col_1)::int, sum(col_2)::int, sum(col_3)::int FROM read_parquet('/tmp/example_table.parquet')"
    results = perform_query_on_cursor(select_data_query, conn)

    assert len(results) == 1
    assert len(results[0]) == 4
    assert results[0][0] == 1001
    assert results[0][1] == 500500
    assert results[0][2] == 1001000
    assert results[0][3] == None

    conn.close()


def test_query_failures(pgduck_server):

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )

    select_data_query = "SELECT log(-1)"

    # fail 5 times
    for _ in range(0, 5):
        cur = conn.cursor()
        try:
            cur.execute(select_data_query)
        except psycopg2.Error as e:
            assert (
                "Out of Range Error: cannot take logarithm of a negative number"
                in str(e)
            )
            conn.rollback()

    conn.close()

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

    conn.close()


def test_duckdb_failures(pgduck_server):

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )

    # duckdb does not support zero column queries
    select_data_query = "SELECT FROM (VALUES (1,2),(2,3),(3,4)) as foo"

    # fail 5 times
    for _ in range(0, 5):
        cur = conn.cursor()
        try:
            cur.execute(select_data_query)
        except psycopg2.Error as e:
            assert "Parser Error: SELECT clause without selection list" in str(e)
            conn.rollback()

    conn.close()

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

    conn.close()


def test_data_type_failures(pgduck_server):

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )

    select_data_query = "SELECT 'some text'"

    # fail 5 times
    for _ in range(0, 5):
        try:
            results = perform_query_on_cursor(select_data_query, conn)
            print(results)
        except Exception as e:
            assert "Unsupported type" in str(e)
            conn.rollback()

    conn.close()

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

    conn.close()

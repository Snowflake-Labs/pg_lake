import pytest
import psycopg2
import time
import duckdb
import math
from decimal import *
from utils_pytest import *


def test_simple_queries(s3, pg_conn, extension):

    url = f"s3://{TEST_BUCKET}/test_s3_simple_queries/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,100) s) TO '{url}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA test_fdw;
                CREATE FOREIGN TABLE test_fdw.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');

                CREATE VIEW test_fdw.simple_view AS SELECT * FROM test_fdw.ft1;
                CREATE VIEW test_fdw.complex_view AS SELECT * FROM test_fdw.ft1 JOIN (SELECT id FROM test_fdw.ft1 t1 JOIN test_fdw.ft1 t2 USING(id)) as bar USING (id);

                CREATE MATERIALIZED VIEW test_fdw.simple_mat_view AS SELECT * FROM test_fdw.ft1;
                CREATE MATERIALIZED VIEW test_fdw.complex_mat_view AS SELECT * FROM test_fdw.ft1 JOIN (SELECT id FROM test_fdw.ft1 t1 JOIN test_fdw.ft1 t2 USING(id)) as bar USING (id);


        """
        % (url),
        pg_conn,
    )

    # simple view
    result = perform_query_on_cursor(
        "SELECT sum(id) FROM test_fdw.simple_view", pg_conn
    )
    pg_conn.commit()
    assert [(5050,)] == result

    # complex view view
    result = perform_query_on_cursor(
        "SELECT sum(id) FROM test_fdw.complex_view", pg_conn
    )
    pg_conn.commit()
    assert [(5050,)] == result

    # simple mat view
    result = perform_query_on_cursor(
        "SELECT sum(id) FROM test_fdw.simple_mat_view", pg_conn
    )
    pg_conn.commit()
    assert [(5050,)] == result

    run_command("REFRESH MATERIALIZED VIEW test_fdw.simple_mat_view;", pg_conn)
    run_command("CREATE UNIQUE INDEX ON test_fdw.simple_mat_view(id);", pg_conn)
    run_command(
        "REFRESH MATERIALIZED VIEW CONCURRENTLY test_fdw.simple_mat_view;", pg_conn
    )

    # simple mat view after refreshed
    result = perform_query_on_cursor(
        "SELECT sum(id) FROM test_fdw.simple_mat_view", pg_conn
    )
    pg_conn.commit()
    assert [(5050,)] == result

    # complex mat view view
    result = perform_query_on_cursor(
        "SELECT sum(id) FROM test_fdw.complex_mat_view", pg_conn
    )
    pg_conn.commit()
    assert [(5050,)] == result

    # simple aggregate pushdown
    result = perform_query_on_cursor("SELECT sum(id) FROM test_fdw.ft1", pg_conn)
    pg_conn.commit()
    assert [(5050,)] == result

    # simple tableoid query
    result = run_query("SELECT tableoid FROM test_fdw.ft1", pg_conn)
    assert result[0]["tableoid"] > 0

    # simple re-scan
    result = perform_query_on_cursor(
        "SELECT COUNT(DISTINCT f.id) FROM test_fdw.ft1 f JOIN LATERAL (SELECT id, random() FROM test_fdw.ft1 WHERE f.id  = id) AS dummy ON true ;",
        pg_conn,
    )
    pg_conn.commit()
    assert [(100,)] == result

    # simple parametrized execution
    run_command(
        "PREPARE p1 (int) AS SELECT count(*) FROM test_fdw.ft1 WHERE id > $1;", pg_conn
    )
    pg_conn.commit()

    result = perform_query_on_cursor("EXECUTE p1(10)", pg_conn)
    assert [(90,)] == result
    result = perform_query_on_cursor("EXECUTE p1(20)", pg_conn)
    assert [(80,)] == result
    result = perform_query_on_cursor("EXECUTE p1(30)", pg_conn)
    assert [(70,)] == result
    result = perform_query_on_cursor("EXECUTE p1(40)", pg_conn)
    assert [(60,)] == result
    result = perform_query_on_cursor("EXECUTE p1(99)", pg_conn)
    assert [(1,)] == result
    result = perform_query_on_cursor("EXECUTE p1(100)", pg_conn)
    assert [(0,)] == result
    pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_fdw CASCADE;", pg_conn)
    run_command("DEALLOCATE ALL;", pg_conn)


def test_simple_queries_with_cursors(s3, pg_conn, extension):

    url = f"s3://{TEST_BUCKET}/test_s3_with_cursors/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,100) s) TO '{url}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA cursors_fdw;
                CREATE FOREIGN TABLE cursors_fdw.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');
        """
        % (url),
        pg_conn,
    )

    # simple aggregate pushdown using a cursor
    run_command("BEGIN;", pg_conn)
    run_command(
        "DECLARE agg_cursor CURSOR FOR SELECT sum(id) FROM cursors_fdw.ft1;", pg_conn
    )
    result = fetch_all("FETCH ALL FROM agg_cursor;", pg_conn)
    assert [(5050,)] == result
    run_command("CLOSE agg_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # simple re-scan using a cursor
    run_command("BEGIN;", pg_conn)
    run_command(
        """DECLARE re_scan_cursor CURSOR FOR
                SELECT COUNT(DISTINCT f.id)
                FROM cursors_fdw.ft1 f
                JOIN LATERAL (SELECT id, random() FROM cursors_fdw.ft1 WHERE f.id = id) AS dummy ON true;""",
        pg_conn,
    )
    result = fetch_all("FETCH ALL FROM re_scan_cursor;", pg_conn)
    assert [(100,)] == result
    run_command("CLOSE re_scan_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # simple parameterized execution using cursor
    run_command("BEGIN;", pg_conn)
    for param in [10, 20, 30, 40, 99, 100]:
        run_command(
            f"DECLARE param_cursor CURSOR FOR SELECT count(*) FROM cursors_fdw.ft1 WHERE id > {param};",
            pg_conn,
        )
        result = fetch_all("FETCH ALL FROM param_cursor;", pg_conn)
        expected_count = 100 - param if param < 100 else 0
        assert [(expected_count,)] == result
        run_command("CLOSE param_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # Start transaction and declare a cursor for a forward scan
    run_command("BEGIN;", pg_conn)
    run_command(
        "DECLARE forward_cursor SCROLL CURSOR FOR SELECT id FROM cursors_fdw.ft1 ORDER BY id;",
        pg_conn,
    )

    # Fetch the first 10 rows in a forward direction
    result = fetch_all("FETCH FORWARD 10 FROM forward_cursor;", pg_conn)
    assert len(result) == 10  # Ensure we fetched 10 rows

    # Move cursor forward by 5 rows without returning them
    run_command("MOVE FORWARD 5 FROM forward_cursor;", pg_conn)

    # Fetch the next 10 rows (should skip the 5 moved over)
    result = fetch_all("FETCH FORWARD 10 FROM forward_cursor;", pg_conn)
    assert (
        len(result) == 10
    )  # Ensure we fetched 10 rows after moving the cursor forward

    run_command("CLOSE forward_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # Start transaction and declare a cursor for a backward scan
    run_command("BEGIN;", pg_conn)
    run_command(
        "DECLARE backward_cursor SCROLL CURSOR FOR SELECT id FROM cursors_fdw.ft1 ORDER BY id;",
        pg_conn,
    )

    # Move to the end of the result set
    run_command("MOVE FORWARD ALL FROM backward_cursor;", pg_conn)

    # Fetch 10 rows in a backward direction from the end of the result set
    result = fetch_all("FETCH BACKWARD 10 FROM backward_cursor;", pg_conn)
    assert len(result) == 10  # Ensure we fetched 10 rows

    # Move cursor backward by 5 rows without returning them
    run_command("MOVE BACKWARD 5 FROM backward_cursor;", pg_conn)

    # Fetch the next 10 rows in a backward direction (after moving backward by 5 rows)
    result = fetch_all("FETCH BACKWARD 10 FROM backward_cursor;", pg_conn)
    assert (
        len(result) == 10
    )  # Ensure we fetched 10 rows after moving the cursor backward

    run_command("CLOSE backward_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # Start a transaction to declare a WITH HOLD cursor
    run_command("BEGIN;", pg_conn)
    run_command(
        """
        DECLARE complex_cursor CURSOR WITH HOLD FOR
        SELECT id, value FROM cursors_fdw.ft1 ORDER BY id;
    """,
        pg_conn,
    )
    run_command("COMMIT;", pg_conn)  # Commit to demonstrate WITH HOLD persistence

    # Fetch the first chunk of rows after the transaction has been committed
    result = fetch_all("FETCH FORWARD 20 FROM complex_cursor;", pg_conn)
    assert len(result) == 20  # Ensure we fetched 20 rows

    # Start a new transaction for further operations
    run_command("BEGIN;", pg_conn)
    # Move cursor forward by 10 rows to skip some entries
    run_command("MOVE FORWARD 10 FROM complex_cursor;", pg_conn)

    # Fetch the next chunk of rows
    result = fetch_all("FETCH FORWARD 20 FROM complex_cursor;", pg_conn)
    assert len(result) == 20  # Ensure we fetched 20 rows after moving the cursor

    # Cleanup: Close the cursor and commit the transaction
    run_command("CLOSE complex_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # Start a transaction and declare a simple cursor
    run_command("BEGIN;", pg_conn)
    run_command(
        """
        DECLARE simple_cursor CURSOR FOR
        SELECT id, value FROM cursors_fdw.ft1 ORDER BY id;
    """,
        pg_conn,
    )

    # Fetch the first chunk of rows using the simple cursor
    result = fetch_all("FETCH FORWARD 10 FROM simple_cursor;", pg_conn)
    assert len(result) == 10  # Verify that we fetched 10 rows

    # Fetch the next chunk of rows
    result = fetch_all("FETCH FORWARD 10 FROM simple_cursor;", pg_conn)
    assert len(result) == 10  # Verify that we fetched another set of 10 rows

    # Close the cursor and end the transaction
    run_command("CLOSE simple_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    run_command(
        """
   CREATE OR REPLACE FUNCTION fetch_data_with_cursor()
        RETURNS SETOF cursors_fdw.ft1 AS $$
        DECLARE
            -- Declare a cursor for a query that selects rows from the table
            cur CURSOR FOR SELECT id, value FROM cursors_fdw.ft1 ORDER BY id;
            rec cursors_fdw.ft1%ROWTYPE; -- Use table's row type for the record variable
        BEGIN
            -- Open the cursor
            OPEN cur;

            LOOP
                -- Fetch the next row from the cursor into the record variable
                FETCH cur INTO rec;
                -- Exit the loop if no more rows are found
                EXIT WHEN NOT FOUND;

                -- Return the current row; it accumulates in the function's result set
                RETURN NEXT rec;
            END LOOP;

            -- Close the cursor. It's automatically closed at the end of the function, but it's good practice to explicitly close it.
            CLOSE cur;

            -- Since this is a set-returning function, there's no need for an explicit RETURN statement at the end
        END;
        $$ LANGUAGE plpgsql;
""",
        pg_conn,
    )

    # Call the PL/pgSQL function to process the data with the cursor
    result = fetch_all("SELECT fetch_data_with_cursor();", pg_conn)
    assert len(result) == 100

    # Start a transaction and declare a non-scrollable cursor
    run_command("BEGIN;", pg_conn)
    run_command(
        """
        DECLARE non_scroll_cursor CURSOR FOR
        SELECT id, value FROM cursors_fdw.ft1 ORDER BY id;
    """,
        pg_conn,
    )

    # Fetch the first chunk of rows using the non-scrollable cursor
    result = fetch_all("FETCH FORWARD 10 FROM non_scroll_cursor;", pg_conn)
    assert len(result) == 10  # Verify that we fetched 10 rows

    # Fetch the next chunk of rows
    result = fetch_all("FETCH FORWARD 10 FROM non_scroll_cursor;", pg_conn)
    assert len(result) == 10  # Verify that we fetched another set of 10 rows

    # Close the cursor and end the transaction
    run_command("CLOSE non_scroll_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # cleanup
    run_command("DROP SCHEMA cursors_fdw CASCADE;", pg_conn)


# test cursors with pushdown
def test_window_functions_with_cursors(s3, pg_conn, extension):

    url = f"s3://{TEST_BUCKET}/test_s3_with_cursors/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(1,100) s) TO '{url}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA cursors_fdw;
                CREATE FOREIGN TABLE cursors_fdw.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');
        """
        % (url),
        pg_conn,
    )

    # Test cursor with simple window function for cumulative sum
    run_command("BEGIN;", pg_conn)
    run_command(
        "DECLARE window_cursor CURSOR FOR SELECT id, sum(id) OVER (ORDER BY id) AS cumulative_sum FROM cursors_fdw.ft1;",
        pg_conn,
    )
    result = fetch_all("FETCH ALL FROM window_cursor;", pg_conn)
    assert result == [(i, i * (i + 1) // 2) for i in range(1, 101)]
    run_command("CLOSE window_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # Test cursor with more complex window function using partitions
    run_command("BEGIN;", pg_conn)
    run_command(
        "DECLARE complex_window_cursor CURSOR FOR SELECT id, rank() OVER (PARTITION BY id % 10 ORDER BY id) AS rank_by_group FROM cursors_fdw.ft1;",
        pg_conn,
    )
    result = fetch_all("FETCH ALL FROM complex_window_cursor;", pg_conn)
    # This is a placeholder for actual test validation logic
    run_command("CLOSE complex_window_cursor;", pg_conn)
    run_command("COMMIT;", pg_conn)

    # cleanup
    run_command("DROP SCHEMA cursors_fdw CASCADE;", pg_conn)


def fetch_all(fetch_command, connection):
    # Function to fetch all results from a cursor
    cursor = connection.cursor()
    cursor.execute(fetch_command)
    result = cursor.fetchall()
    return result


def test_types_on_queries(s3, pg_conn, extension):
    parquet_url = f"s3://{TEST_BUCKET}/test_s3_types_on_queries/data.parquet"
    csv_url = f"s3://{TEST_BUCKET}/test_s3_types_on_queries_csv/data.csv"
    json_url = f"s3://{TEST_BUCKET}/test_s3_types_on_queries_json/data.json"

    for url in [parquet_url, csv_url, json_url]:
        run_command(
            f"""

          COPY (
            SELECT
                32767::SMALLINT AS small_int_col,
                2147483647::INTEGER AS integer_col,
                9223372036854775807::BIGINT AS big_int_col,
                123456.789::DECIMAL AS decimal_col,
                987654.321::NUMERIC AS numeric_col,
                123.456::REAL AS real_col,
                12345678.12345678::DOUBLE PRECISION AS double_precision_col,
                '2024-03-13 14:30:00'::TIMESTAMP AS timestamp_col,
                '2024-03-13 14:30:00+00'::TIMESTAMP WITH TIME ZONE AS timestamp_with_timezone_col,
                '2024-03-13'::DATE AS date_col,
                '14:30:00'::TIME AS time_col,
                '14:30:00+00'::TIME WITH TIME ZONE AS time_with_timezone_col,
                '1 year 2 months 3 days 04:05:06'::INTERVAL AS interval_col,
                'char data'::CHAR(10) AS char_col,
                'varchar data'::VARCHAR(50) AS varchar_col,
                E'This is a \\'test\\' string that includes "double quotes" and \\'single quotes\\' to check how escape characters are handled.\\nIt also includes a newline \\n, a carriage return \\r, and a tab \\t character.\\nAdditionally, let\\'s include some Unicode characters: ✓, ©, ™, and emojis 😊, 🚀 to check UTF-8 compatibility.'::TEXT AS text_col,
                '{{"key": "value"}}'::JSON AS json_col,
                '{{"key": "value"}}'::JSONB AS jsonb_col,
                '10101'::BIT(5) AS bit_col,
                '1010101010'::VARBIT(10) AS varbit_col FROM generate_series(0,100)
            UNION ALL
            SELECT
                -32767::SMALLINT,
                -2147483647::INTEGER,
                -9223372036854775807::BIGINT,
                -123456.789::DECIMAL,
                -987654.321::NUMERIC,
                -123.456::REAL,
                -12345678.12345678::DOUBLE PRECISION,
                '2024-03-14 14:30:00'::TIMESTAMP,
                '2024-03-14 14:30:00+01'::TIMESTAMP WITH TIME ZONE,
                '2024-03-14'::DATE,
                '15:30:00'::TIME,
                '15:30:00+01'::TIME WITH TIME ZONE,
                '2 years 4 months 6 days 08:10:12'::INTERVAL,
                'char datb'::CHAR(10),
                'Another varchar'::VARCHAR(50),
                'Another text'::TEXT,
                '{{"another": "item"}}'::JSON,
                '{{"another": "item"}}'::JSONB,
                '11011'::BIT(5),
                '1101101101'::VARBIT(10) FROM generate_series(0,100)
            ) TO """
            + f"'{url}'",
            pg_conn,
        )
        pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA test_fdw_types_on_queries;
                """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    for url in [parquet_url, csv_url, json_url]:
        table_name = "ft1" if "parquet" in url else ("ft2" if "csv" in url else "ft3")
        table_format = (
            "parquet" if "parquet" in url else ("csv" if "csv" in url else "json")
        )

        run_command(
            """
                    CREATE FOREIGN TABLE test_fdw_types_on_queries.%s (
                        small_int_col SMALLINT,
                        integer_col INTEGER,
                        big_int_col BIGINT,
                        decimal_col DECIMAL,
                        numeric_col NUMERIC,
                        real_col REAL,
                        double_precision_col DOUBLE PRECISION,
                        timestamp_col TIMESTAMP,
                        timestamp_with_timezone_col TIMESTAMP WITH TIME ZONE,
                        date_col DATE,
                        time_col TIME,
                        time_with_timezone_col TIME WITH TIME ZONE,
                        interval_col INTERVAL,
                        char_col CHAR(10),
                        varchar_col VARCHAR(50),
                        text_col TEXT,
                        json_col JSON,
                        jsonb_col JSONB,
                        bit_col BIT(5),
                        varbit_col VARBIT(10)
                    ) SERVER pg_lake OPTIONS (format '%s', path '%s');;
            """
            % (table_name, table_format, url),
            pg_conn,
        )
        pg_conn.commit()

    # simple aggregate pushdown

    # simple aggregate pushdown
    result = perform_query_on_cursor(
        "SELECT count(*) FROM test_fdw_types_on_queries.ft1", pg_conn
    )
    pg_conn.commit()
    assert [(202,)] == result

    # simple query on json
    result = perform_query_on_cursor(
        "SELECT count(*) FROM test_fdw_types_on_queries.ft3", pg_conn
    )
    pg_conn.commit()
    assert [(202,)] == result

    # having subquery
    result = perform_query_on_cursor(
        """
               SELECT integer_col, COUNT(*)
                FROM test_fdw_types_on_queries.ft1
                GROUP BY integer_col
                HAVING COUNT(*) > 10;
    """,
        pg_conn,
    )
    pg_conn.commit()
    print(result[0])
    assert (2147483647, 101) == result[0]
    assert (-2147483647, 101) == result[1]

    # subquery in WHERE
    result = perform_query_on_cursor(
        """
            SELECT count(DISTINCT integer_col)
            FROM test_fdw_types_on_queries.ft1
            WHERE small_int_col = (
            SELECT MAX(small_int_col)
            FROM test_fdw_types_on_queries.ft1
        );

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert (1) == result[0][0]

    # subquery in WHERE for CSV
    result = perform_query_on_cursor(
        """
            SELECT count(DISTINCT integer_col)
            FROM test_fdw_types_on_queries.ft2
            WHERE small_int_col = (
            SELECT MAX(small_int_col)
            FROM test_fdw_types_on_queries.ft2
        );

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert (1) == result[0][0]

    # cte query
    result = perform_query_on_cursor(
        """
            WITH all_rows AS (
                SELECT *
                FROM test_fdw_types_on_queries.ft1
            )
            SELECT COUNT(*), AVG(big_int_col-integer_col)
            FROM all_rows;

    """,
        pg_conn,
    )
    pg_conn.commit()
    print(result[0])
    assert (202, 0) == result[0]

    # case query
    result = perform_query_on_cursor(
        """
            SELECT
                CASE
                    WHEN small_int_col > 100 THEN 'Greater than 100'
                    ELSE '100 or less'
                END AS category,
                COUNT(*)
            FROM test_fdw_types_on_queries.ft1
            GROUP BY category ORDER BY 1;
    """,
        pg_conn,
    )
    pg_conn.commit()
    assert ("100 or less", 101) == result[0]
    assert ("Greater than 100", 101) == result[1]

    # complex query, re-using single table with CTEs
    result = perform_query_on_cursor(
        """
        WITH filtered_data AS (
            SELECT
                *,
                CASE WHEN small_int_col > 50 THEN 'High' ELSE 'Low' END AS small_int_category
            FROM test_fdw_types_on_queries.ft1
        ), aggregated_data AS (
            SELECT
                f.small_int_category,
                AVG(f.numeric_col) AS avg_numeric,
                MAX(f.double_precision_col) AS max_double_precision,
                (SELECT MIN(small_int_col) FROM filtered_data WHERE small_int_category = f.small_int_category) AS min_small_int_col
            FROM filtered_data f
            GROUP BY f.small_int_category
        ), detailed_data AS (
            SELECT
                a.*,
                (SELECT COUNT(*) FROM filtered_data fd WHERE fd.small_int_category = a.small_int_category) AS count_per_category
            FROM aggregated_data a
        ), joined_data AS (
            SELECT
                dd.*,
                fd.text_col,
                fd.timestamp_col
            FROM detailed_data dd
            JOIN filtered_data fd ON dd.small_int_category = fd.small_int_category AND dd.min_small_int_col = fd.small_int_col
        ), complex_conditions AS (
            SELECT
                jd.small_int_category,
                jd.avg_numeric,
                jd.max_double_precision,
                jd.count_per_category,
                jd.text_col,
                jd.timestamp_col,
                (SELECT AVG(avg_numeric) FROM aggregated_data) AS overall_avg_numeric,
                LAG(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS prev_avg_numeric,
                LEAD(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS next_avg_numeric
            FROM joined_data jd
            WHERE jd.avg_numeric > (SELECT AVG(avg_numeric) * 1.1 FROM aggregated_data)
        )
        SELECT
            cc.*,
            CASE
                WHEN cc.avg_numeric > cc.overall_avg_numeric AND cc.avg_numeric BETWEEN cc.prev_avg_numeric AND cc.next_avg_numeric THEN 'Target'
                ELSE 'Non-Target'
            END AS category_flag
        FROM complex_conditions cc
        WHERE cc.count_per_category <= (
            SELECT AVG(count_per_category) FROM complex_conditions
        )
        ORDER BY cc.small_int_category, cc.avg_numeric DESC LIMIT 1;

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert Decimal("987654.321000000000") == result[0][1]
    assert 12345678.12345678 == result[0][2]
    assert 101 == result[0][3]

    # complex query, re-using single table with CTEs with CSV
    result = perform_query_on_cursor(
        """
        WITH filtered_data AS (
            SELECT
                *,
                CASE WHEN small_int_col > 50 THEN 'High' ELSE 'Low' END AS small_int_category
            FROM test_fdw_types_on_queries.ft2
        ), aggregated_data AS (
            SELECT
                f.small_int_category,
                AVG(f.numeric_col) AS avg_numeric,
                MAX(f.double_precision_col) AS max_double_precision,
                (SELECT MIN(small_int_col) FROM filtered_data WHERE small_int_category = f.small_int_category) AS min_small_int_col
            FROM filtered_data f
            GROUP BY f.small_int_category
        ), detailed_data AS (
            SELECT
                a.*,
                (SELECT COUNT(*) FROM filtered_data fd WHERE fd.small_int_category = a.small_int_category) AS count_per_category
            FROM aggregated_data a
        ), joined_data AS (
            SELECT
                dd.*,
                fd.text_col,
                fd.timestamp_col
            FROM detailed_data dd
            JOIN filtered_data fd ON dd.small_int_category = fd.small_int_category AND dd.min_small_int_col = fd.small_int_col
        ), complex_conditions AS (
            SELECT
                jd.small_int_category,
                jd.avg_numeric,
                jd.max_double_precision,
                jd.count_per_category,
                jd.text_col,
                jd.timestamp_col,
                (SELECT AVG(avg_numeric) FROM aggregated_data) AS overall_avg_numeric,
                LAG(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS prev_avg_numeric,
                LEAD(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS next_avg_numeric
            FROM joined_data jd
            WHERE jd.avg_numeric > (SELECT AVG(avg_numeric) * 1.1 FROM aggregated_data)
        )
        SELECT
            cc.*,
            CASE
                WHEN cc.avg_numeric > cc.overall_avg_numeric AND cc.avg_numeric BETWEEN cc.prev_avg_numeric AND cc.next_avg_numeric THEN 'Target'
                ELSE 'Non-Target'
            END AS category_flag
        FROM complex_conditions cc
        WHERE cc.count_per_category <= (
            SELECT AVG(count_per_category) FROM complex_conditions
        )
        ORDER BY cc.small_int_category, cc.avg_numeric DESC LIMIT 1;

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert Decimal("987654.321000000000") == result[0][1]
    assert 12345678.12345678 == result[0][2]
    assert 101 == result[0][3]

    # complex query, using the same table multiple times
    result = perform_query_on_cursor(
        """
        WITH filtered_data AS (
            SELECT
                *,
                CASE WHEN small_int_col > 50 THEN 'High' ELSE 'Low' END AS small_int_category
            FROM test_fdw_types_on_queries.ft1
        ), aggregated_data AS (
            SELECT
                f.small_int_category,
                AVG(f.numeric_col) AS avg_numeric,
                MAX(f.double_precision_col) AS max_double_precision,
                (SELECT MIN(small_int_col) FROM filtered_data WHERE small_int_category = f.small_int_category) AS min_small_int_col
            FROM filtered_data f
            GROUP BY f.small_int_category
        ), detailed_data AS (
            SELECT
                a.*,
                (SELECT COUNT(*) FROM filtered_data fd WHERE fd.small_int_category = a.small_int_category) AS count_per_category
            FROM aggregated_data a
        ), joined_data AS (
            SELECT
                dd.*,
                fd.text_col,
                fd.timestamp_col
            FROM detailed_data dd
            JOIN filtered_data fd ON dd.small_int_category = fd.small_int_category AND dd.min_small_int_col = fd.small_int_col
        ), complex_conditions AS (
            SELECT
                jd.small_int_category,
                jd.avg_numeric,
                jd.max_double_precision,
                jd.count_per_category,
                jd.text_col,
                jd.timestamp_col,
                (SELECT AVG(avg_numeric) FROM aggregated_data) AS overall_avg_numeric,
                LAG(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS prev_avg_numeric,
                LEAD(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS next_avg_numeric
            FROM joined_data jd
            WHERE jd.avg_numeric > (SELECT AVG(avg_numeric) * 1.1 FROM aggregated_data)
        )
        SELECT
            cc.*,
            CASE
                WHEN cc.avg_numeric > cc.overall_avg_numeric AND cc.avg_numeric BETWEEN cc.prev_avg_numeric AND cc.next_avg_numeric THEN 'Target'
                ELSE 'Non-Target'
            END AS category_flag
        FROM complex_conditions cc
        WHERE cc.count_per_category <= (
            SELECT AVG(count_per_category) FROM complex_conditions
        )
        ORDER BY cc.small_int_category, cc.avg_numeric DESC LIMIT 1;

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert "High" == result[0][0]
    assert Decimal("987654.321000000000") == result[0][1]
    assert 12345678.12345678 == result[0][2]
    assert 101 == result[0][3]
    assert "Non-Target" == result[0][9]

    # complex query, using the same table multiple times
    # with deep subqueries and recursive CTE
    result = perform_query_on_cursor(
        """
            WITH recursive cte AS (
                SELECT
                    *,
                    CASE
                        WHEN small_int_col > 10 THEN 'High' -- Adjusted from 50 to 10 to broaden the categorization
                        ELSE 'Low'
                    END AS small_int_category,
                    ROW_NUMBER() OVER(PARTITION BY small_int_col ORDER BY numeric_col DESC) AS rn
                FROM
                    test_fdw_types_on_queries.ft1
            ), aggregated AS (
                SELECT
                    small_int_category,
                    AVG(numeric_col) AS avg_numeric,
                    MAX(double_precision_col) AS max_dbl_precision
                FROM
                    cte
                WHERE
                    rn <= 10
                GROUP BY
                    small_int_category
            ), filtered AS (
                SELECT
                    *,
                    (SELECT MAX(avg_numeric) FROM aggregated a2 WHERE a2.small_int_category = aggregated.small_int_category) AS max_avg_numeric
                FROM
                    aggregated
            ), second_level AS (
                SELECT
                    f.small_int_category,
                    f.avg_numeric,
                    (SELECT MIN(numeric_col) FROM test_fdw_types_on_queries.ft1 WHERE TRUE) AS min_numeric_filtered
                FROM
                    filtered f
                WHERE
                    f.avg_numeric <= (
                        SELECT
                            AVG(max_avg_numeric) * 1.5 FROM filtered
                    )
            ), third_level AS (
                SELECT
                    s.*,
                    (SELECT AVG(double_precision_col) FROM test_fdw_types_on_queries.ft1 WHERE TRUE) AS avg_dbl_precision_conditioned
                FROM
                    second_level s
            )
            SELECT
                t.small_int_category,
                t.avg_numeric,
                t.min_numeric_filtered,
                t.avg_dbl_precision_conditioned,
                (SELECT COUNT(*) FROM test_fdw_types_on_queries.ft1 WHERE TRUE) AS count_conditioned
            FROM
                third_level t
            WHERE
                EXISTS (
                    SELECT
                        1
                    FROM
                        test_fdw_types_on_queries.ft1 f1
                    WHERE
                        TRUE
                )
            ORDER BY
                count_conditioned DESC;

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert "Low" == result[0][0]
    assert Decimal("-987654.321000000000") == result[0][1]
    assert 202 == result[0][4]

    # due to numeric pushdown issues, we skip it for now
    run_command("SET pg_lake_table.enable_full_query_pushdown TO false;", pg_conn)

    # complex query, using the same table multiple times
    result = perform_query_on_cursor(
        """
        WITH filtered_data AS (
            SELECT
                numeric_col, small_int_col, double_precision_col, text_col, timestamp_col,
                CASE WHEN small_int_col > 50 THEN 'High' ELSE 'Low' END AS small_int_category
            FROM test_fdw_types_on_queries.ft3
        ), aggregated_data AS (
            SELECT
                f.small_int_category,
                AVG(f.numeric_col) AS avg_numeric,
                MAX(f.double_precision_col) AS max_double_precision,
                (SELECT MIN(small_int_col) FROM filtered_data WHERE small_int_category = f.small_int_category) AS min_small_int_col
            FROM filtered_data f
            GROUP BY f.small_int_category
        ), detailed_data AS (
            SELECT
                a.*,
                (SELECT COUNT(*) FROM filtered_data fd WHERE fd.small_int_category = a.small_int_category) AS count_per_category
            FROM aggregated_data a
        ), joined_data AS (
            SELECT
                dd.*,
                fd.text_col,
                fd.timestamp_col
            FROM detailed_data dd
            JOIN filtered_data fd ON dd.small_int_category = fd.small_int_category AND dd.min_small_int_col = fd.small_int_col
        ), complex_conditions AS (
            SELECT
                jd.small_int_category,
                jd.avg_numeric,
                jd.max_double_precision,
                jd.count_per_category,
                jd.text_col,
                jd.timestamp_col,
                (SELECT AVG(avg_numeric) FROM aggregated_data) AS overall_avg_numeric,
                LAG(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS prev_avg_numeric,
                LEAD(jd.avg_numeric) OVER (ORDER BY jd.small_int_category) AS next_avg_numeric
            FROM joined_data jd
            WHERE jd.avg_numeric > (SELECT AVG(avg_numeric) * 1.1 FROM aggregated_data)
        )
        SELECT
            cc.*,
            CASE
                WHEN cc.avg_numeric > cc.overall_avg_numeric AND cc.avg_numeric BETWEEN cc.prev_avg_numeric AND cc.next_avg_numeric THEN 'Target'
                ELSE 'Non-Target'
            END AS category_flag
        FROM complex_conditions cc
        WHERE cc.count_per_category <= (
            SELECT AVG(count_per_category) FROM complex_conditions
        )
        ORDER BY cc.small_int_category, cc.avg_numeric DESC LIMIT 1;

    """,
        pg_conn,
    )
    pg_conn.commit()
    run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)

    assert "High" == result[0][0]
    assert Decimal("987654.321000000000") == result[0][1]
    assert 12345678.12345678 == result[0][2]
    assert 101 == result[0][3]
    assert "Non-Target" == result[0][9]

    # complex query, using the same table multiple times
    # with deep subqueries and recursive CTE
    result = perform_query_on_cursor(
        """
            WITH recursive cte AS (
                SELECT
                    *,
                    CASE
                        WHEN small_int_col > 10 THEN 'High' -- Adjusted from 50 to 10 to broaden the categorization
                        ELSE 'Low'
                    END AS small_int_category,
                    ROW_NUMBER() OVER(PARTITION BY small_int_col ORDER BY numeric_col DESC) AS rn
                FROM
                    test_fdw_types_on_queries.ft3
            ), aggregated AS (
                SELECT
                    small_int_category,
                    AVG(numeric_col) AS avg_numeric,
                    MAX(double_precision_col) AS max_dbl_precision
                FROM
                    cte
                WHERE
                    rn <= 10
                GROUP BY
                    small_int_category
            ), filtered AS (
                SELECT
                    *,
                    (SELECT MAX(avg_numeric) FROM aggregated a2 WHERE a2.small_int_category = aggregated.small_int_category) AS max_avg_numeric
                FROM
                    aggregated
            ), second_level AS (
                SELECT
                    f.small_int_category,
                    f.avg_numeric,
                    (SELECT MIN(numeric_col) FROM test_fdw_types_on_queries.ft3 WHERE TRUE) AS min_numeric_filtered
                FROM
                    filtered f
                WHERE
                    f.avg_numeric <= (
                        SELECT
                            AVG(max_avg_numeric) * 1.5 FROM filtered
                    )
            ), third_level AS (
                SELECT
                    s.*,
                    (SELECT AVG(double_precision_col) FROM test_fdw_types_on_queries.ft3 WHERE TRUE) AS avg_dbl_precision_conditioned
                FROM
                    second_level s
            )
            SELECT
                t.small_int_category,
                t.avg_numeric,
                t.min_numeric_filtered,
                t.avg_dbl_precision_conditioned,
                (SELECT COUNT(*) FROM test_fdw_types_on_queries.ft3 WHERE TRUE) AS count_conditioned
            FROM
                third_level t
            WHERE
                EXISTS (
                    SELECT
                        1
                    FROM
                        test_fdw_types_on_queries.ft3 f1
                    WHERE
                        TRUE
                )
            ORDER BY
                count_conditioned DESC;

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert "Low" == result[0][0]
    assert Decimal("-987654.321000000000") == result[0][1]
    assert 202 == result[0][4]

    # complex query, using the both CSV/parquet/json tables multiple times
    # with deep subqueries and recursive CTE
    result = perform_query_on_cursor(
        """
            WITH recursive cte AS (
                SELECT
                    *,
                    CASE
                        WHEN small_int_col > 10 THEN 'High' -- Adjusted from 50 to 10 to broaden the categorization
                        ELSE 'Low'
                    END AS small_int_category,
                    ROW_NUMBER() OVER(PARTITION BY small_int_col ORDER BY numeric_col DESC) AS rn
                FROM
                    test_fdw_types_on_queries.ft1
            ), aggregated AS (
                SELECT
                    small_int_category,
                    AVG(numeric_col) AS avg_numeric,
                    MAX(double_precision_col) AS max_dbl_precision
                FROM
                    cte
                WHERE
                    rn <= 10
                GROUP BY
                    small_int_category
            ), filtered AS (
                SELECT
                    *,
                    (SELECT MAX(avg_numeric) FROM aggregated a2 WHERE a2.small_int_category = aggregated.small_int_category) AS max_avg_numeric
                FROM
                    aggregated
            ), second_level AS (
                SELECT
                    f.small_int_category,
                    f.avg_numeric,
                    (SELECT MIN(numeric_col) FROM test_fdw_types_on_queries.ft2 WHERE TRUE) AS min_numeric_filtered
                FROM
                    filtered f
                WHERE
                    f.avg_numeric <= (
                        SELECT
                            AVG(max_avg_numeric) * 1.5 FROM filtered
                    )
            ), third_level AS (
                SELECT
                    s.*,
                    (SELECT AVG(double_precision_col) FROM test_fdw_types_on_queries.ft3 WHERE TRUE) AS avg_dbl_precision_conditioned
                FROM
                    second_level s
            )
            SELECT
                t.small_int_category,
                t.avg_numeric,
                t.min_numeric_filtered,
                t.avg_dbl_precision_conditioned,
                (SELECT COUNT(*) FROM test_fdw_types_on_queries.ft2 WHERE TRUE) AS count_conditioned
            FROM
                third_level t
            WHERE
                EXISTS (
                    SELECT
                        1
                    FROM
                        test_fdw_types_on_queries.ft3 f1
                    WHERE
                        TRUE
                )
            ORDER BY
                count_conditioned DESC;

    """,
        pg_conn,
    )
    pg_conn.commit()
    assert "Low" == result[0][0]
    assert Decimal("-987654.321000000000") == result[0][1]
    assert 202 == result[0][4]

    # cleanup
    run_command("DROP SCHEMA test_fdw_types_on_queries CASCADE;", pg_conn)
    run_command("DEALLOCATE ALL;", pg_conn)


def test_parametrized_queries(s3, pg_conn, extension):

    url = f"s3://{TEST_BUCKET}/test_s3_parametrized_queries/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT 1.1102::double precision as double_col, 2.555::NUMERIC(6,2) as numeric_col, '3.16' as text_col, 4 as int_col FROM generate_series(0,0) s) TO '{url}';
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA test_fdw_parametrized;
                CREATE FOREIGN TABLE test_fdw_parametrized.ft1 (
                    double_col double PRECISION,
                    numeric_col numeric,
                    text_col text,
                    int_col int
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');
        """
        % (url),
        pg_conn,
    )
    pg_conn.commit()

    # make sure we can pushdown queries with casts working as expected
    run_command(
        "PREPARE p1 (double PRECISION) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor(double_col) < $1;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p1(10)", pg_conn)
    assert [(1,)] == result

    run_command(
        "SET search_path TO test_fdw_parametrized; PREPARE p2 (double PRECISION) AS SELECT count(*) FROM ft1 WHERE floor($1) > double_col;",
        pg_conn,
    )
    result = perform_query_on_cursor(
        "SET search_path TO test_fdw_parametrized; EXECUTE p2(10)", pg_conn
    )
    assert [(1,)] == result

    run_command(
        "PREPARE p3 (NUMERIC(6,2)) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor(numeric_col) < $1;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p3(10)", pg_conn)
    assert [(1,)] == result

    run_command(
        "PREPARE p4 (NUMERIC(6,2)) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor($1) > numeric_col;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p4(10)", pg_conn)
    assert [(1,)] == result

    run_command(
        "PREPARE p5 (text) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor(text_col::numeric) < $1::numeric;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p5(10)", pg_conn)
    assert [(1,)] == result

    run_command(
        "PREPARE p6 (text) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor($1::double PRECISION) > text_col::numeric;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p6(10)", pg_conn)
    assert [(1,)] == result

    run_command(
        "PREPARE p7 (int) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor(int_col) < $1;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p7(10)", pg_conn)
    assert [(1,)] == result

    run_command(
        "PREPARE p8 (int) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor($1) > int_col;",
        pg_conn,
    )
    result = perform_query_on_cursor("EXECUTE p8(10)", pg_conn)
    assert [(1,)] == result

    # volatile function parameter
    run_command(
        "PREPARE p9 (int) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor($1) > int_col;",
        pg_conn,
    )
    result = perform_query_on_cursor(
        "EXECUTE p8((random() + 0.0001) * 100000)", pg_conn
    )
    assert [(1,)] == result

    run_command(
        "PREPARE p10 (int) AS SELECT count(*) FROM test_fdw_parametrized.ft1 WHERE floor(int_col) < $1;",
        pg_conn,
    )
    result = perform_query_on_cursor(
        "EXECUTE p10((random() + 0.0001) * 100000)", pg_conn
    )
    assert [(1,)] == result

    result = perform_query_on_cursor(
        "EXECUTE p1((random() + 0.0001) * 100000)", pg_conn
    )
    assert [(1,)] == result

    result = perform_query_on_cursor(
        "EXECUTE p2((random() + 0.0001) * 100000)", pg_conn
    )
    assert [(1,)] == result

    result = perform_query_on_cursor(
        "EXECUTE p3((random() + 0.0001) * 100000)", pg_conn
    )
    assert [(1,)] == result

    result = perform_query_on_cursor(
        "EXECUTE p4((random() + 0.0001) * 100000)", pg_conn
    )
    assert [(1,)] == result

    pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_fdw_parametrized CASCADE", pg_conn)
    run_command("DEALLOCATE ALL;", pg_conn)


def test_load_more_data(s3, pg_conn, extension):

    path = f"s3://{TEST_BUCKET}/test_s3_load_more_data/*.parquet"

    data_url_1 = f"s3://{TEST_BUCKET}/test_s3_load_more_data/data_1.parquet"
    data_url_2 = f"s3://{TEST_BUCKET}/test_s3_load_more_data/data_2.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(0,1000) s) TO '{data_url_1}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA test_fdw;
                CREATE FOREIGN TABLE test_fdw.ft1 (
                    id bigserial,
                    value text
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');;
        """
        % (path),
        pg_conn,
    )

    # simple aggregate pushdown
    result = perform_query_on_cursor("SELECT sum(id) FROM test_fdw.ft1", pg_conn)
    pg_conn.commit()
    assert [(500500,)] == result

    # Create one more Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(0,1000) s) TO '{data_url_2}';
    """,
        pg_conn,
    )

    # now, should see more data
    result = perform_query_on_cursor("SELECT sum(id) FROM test_fdw.ft1", pg_conn)
    pg_conn.commit()
    assert [(1001000,)] == result

    # cleanup
    run_command("DROP SCHEMA test_fdw CASCADE", pg_conn)
    run_command("DEALLOCATE ALL;", pg_conn)


def test_fdw_table_without_path(pg_conn, s3, extension):

    # setup
    run_command(
        """
                CREATE SCHEMA test_fdw_test_fail_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table without option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_fdw_test_fail_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake;"""
        )
        pg_conn.commit()

    except psycopg2.errors.SyntaxError as e:
        assert str(e) == '"path" option is required for regular pg_lake tables\n'
        cur.close()
        pg_conn.commit()

    # table with an invalid option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_fdw_test_fail_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'parquet', path '/tmp');"""
        )
        pg_conn.commit()

    except psycopg2.errors.FeatureNotSupported as e:
        assert (
            "pg_lake_table: only s3://, gs://, az://, azure://, and abfss:// URLs are currently supported"
            in str(e)
        )
        cur.close()
        pg_conn.commit()

    cur = pg_conn.cursor()
    cur.execute(
        """  CREATE FOREIGN TABLE test_fdw_test_fail_sc.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'parquet', path 's3://');"""
    )
    pg_conn.commit()

    # table with an valid option, then SET to an invalid option
    try:
        cur.execute(
            """  ALTER FOREIGN TABLE test_fdw_test_fail_sc.ft1 OPTIONS (SET path '/tmp');"""
        )
        pg_conn.commit()

    except psycopg2.errors.FeatureNotSupported as e:
        assert (
            str(e)
            == 'pg_lake_table: only s3://, gs://, az://, azure://, and abfss:// URLs are currently supported for the "path" option.\n'
        )
        cur.close()
        pg_conn.commit()

    cur = pg_conn.cursor()
    # table with an valid option, then DROP the option
    try:
        cur.execute(
            """  ALTER FOREIGN TABLE test_fdw_test_fail_sc.ft1 OPTIONS (DROP path);"""
        )
        pg_conn.commit()

    except psycopg2.errors.SyntaxError as e:
        assert str(e) == '"path" option is required for regular pg_lake tables\n'
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_fdw_test_fail_sc CASCADE", pg_conn)


def test_fdw_table_without_format(pg_conn, s3, extension):

    # setup
    run_command(
        """
                CREATE SCHEMA test_fdw_test_fail_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table without option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_fdw_test_fail_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (path 's3://');"""
        )
        pg_conn.commit()

    except psycopg2.errors.SyntaxError as e:
        assert '"format" option is required for pg_lake tables' in str(e)
        cur.close()
        pg_conn.commit()

    # table with wrong
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_fdw_test_fail_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'test', path 's3://');"""
        )
        pg_conn.commit()

    except psycopg2.errors.FeatureNotSupported as e:
        assert "pg_lake_table: only csv, json, gdal, and parquet" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_fdw_test_fail_sc CASCADE", pg_conn)


def test_ensure_predefined_server(s3, superuser_conn, extension):

    url = f"s3://{TEST_BUCKET}/test_ensure_predefined_server/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(0,1000) s) TO '{url}';
    """,
        superuser_conn,
    )
    try:
        # Create a table with 2 columns on the fdw
        run_command(
            """
                    CREATE SERVER pgduck_server FOREIGN DATA WRAPPER pg_lake_table;
                    CREATE SCHEMA test_fdw;
                    CREATE FOREIGN TABLE test_fdw.ft1 (
                        id int,
                        value text
                    ) SERVER pgduck_server OPTIONS (format 'parquet', path '%s');;
            """
            % (url),
            superuser_conn,
        )
        superuser_conn.commit()

    except psycopg2.errors.FdwUnableToCreateExecution as e:
        assert (
            "unexpected state: foreign server pgduck_server is not a pg_lake table"
            in str(e)
        )
        superuser_conn.rollback()


def test_no_libpq_options_allowed_predefined_server(s3, superuser_conn, extension):

    # table without option
    cur = superuser_conn.cursor()
    try:
        cur.execute(
            """
                CREATE SERVER pgduck_server FOREIGN DATA WRAPPER pg_lake_table OPTIONS (port '5332');
                """
        )

    except psycopg2.errors.FdwInvalidOptionName as e:
        assert 'invalid option "port"' in str(e)
        cur.close()
        superuser_conn.commit()

    cur = superuser_conn.cursor()
    try:
        cur.execute(
            """
                CREATE SERVER pgduck_server FOREIGN DATA WRAPPER pg_lake_table OPTIONS (host 'localhost');
                """
        )

    except psycopg2.errors.FdwInvalidOptionName as e:
        assert 'invalid option "host"' in str(e)
        cur.close()
        superuser_conn.commit()

    cur = superuser_conn.cursor()
    try:
        cur.execute(
            """
                CREATE SERVER pgduck_server FOREIGN DATA WRAPPER pg_lake_table OPTIONS (dbname 'postgres');
                """
        )

    except psycopg2.errors.FdwInvalidOptionName as e:
        assert 'invalid option "dbname"' in str(e)
        cur.close()
        superuser_conn.commit()


import pytest

# Define a list of system columns to test
#
# We exclude tableoid, since PostgreSQL will produce the value
# and it will not be pushed down, since it's an OID type.
SYSTEM_COLUMNS = ["ctid", "xmin", "cmin", "xmax", "cmax"]


@pytest.fixture(scope="module")
def setup_fdw(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/test_system_columns/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(0,1000) s) TO '{url}';
    """,
        pg_conn,
    )

    # Set up the FDW and table
    run_command(
        """
        CREATE SCHEMA IF NOT EXISTS test_fdw;
        DROP FOREIGN TABLE IF EXISTS test_fdw.ft1;
        CREATE FOREIGN TABLE test_fdw.ft1 (
            id int,
            value text
        ) SERVER pg_lake OPTIONS (path '{}');
    """.format(
            url
        ),
        pg_conn,
    )
    pg_conn.commit()

    yield
    # Teardown FDW table and schema after tests
    run_command("DROP FOREIGN TABLE IF EXISTS test_fdw.ft1;", pg_conn)
    run_command("DROP SCHEMA IF EXISTS test_fdw CASCADE;", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize("system_column", SYSTEM_COLUMNS)
def test_system_columns_error(system_column, setup_fdw, pg_conn):
    with pytest.raises(Exception) as exc_info:
        run_command(f"SELECT {system_column} FROM test_fdw.ft1", pg_conn)
    pg_conn.rollback()
    assert (
        "System column" in str(exc_info.value)
        and f"{system_column}" in str(exc_info.value)
        and "is not supported for pg_lake table" in str(exc_info.value)
    )

    with pytest.raises(Exception) as exc_info:
        run_command(
            f"SELECT * FROM test_fdw.ft1 WHERE {system_column} = (SELECT {system_column} FROM pg_class)",
            pg_conn,
        )
    pg_conn.rollback()
    assert (
        "System column" in str(exc_info.value)
        and f"{system_column}" in str(exc_info.value)
        and "is not supported for pg_lake table" in str(exc_info.value)
    )

    # join
    with pytest.raises(Exception) as exc_info:
        run_command(
            f"SELECT t1.{system_column} FROM test_fdw.ft1 t1, test_fdw.ft1 t2",
            pg_conn,
        )
    pg_conn.rollback()
    assert (
        "System column" in str(exc_info.value)
        and f"{system_column}" in str(exc_info.value)
        and "is not supported for pg_lake table" in str(exc_info.value)
    )

    # this check is only for ctid
    if system_column == "ctid":
        with pytest.raises(Exception) as exc_info:
            run_command(
                f"SELECT * FROM test_fdw.ft1 t1 WHERE ctid = '(1234,0)'::tid",
                pg_conn,
            )
        pg_conn.rollback()
        assert (
            "System column" in str(exc_info.value)
            and f"{system_column}" in str(exc_info.value)
            and "is not supported for pg_lake table" in str(exc_info.value)
        )


def test_semi_join_issue(s3, pg_conn, extension, with_default_location):
    run_command(
        """
        create schema semi;
		create table semi.users (
		 id bigint,
		 team_id bigint,
		 deleted_at timestamp without time zone
		) using iceberg;
        insert into semi.users values (1, 1);

		create table semi.teams (
		 id bigint,
		 deleted_at timestamp(6) without time zone
		) using iceberg;
        insert into semi.teams values (1);

		create table semi.team_hierarchies (
		 id bigint,
		 ancestor_id bigint
		) using iceberg;
        insert into semi.team_hierarchies values (0,1);
    """,
        pg_conn,
    )

    # Failed due to bug in postgres_fdw (postgres bug 18852)
    result = run_query(
        """
		SELECT
			count(dxu.id)
		FROM
			semi.users dxu
			LEFT JOIN semi.teams dxt ON dxt.id = dxu.team_id
		WHERE
			dxt.deleted_at IS NULL
			AND dxu.deleted_at IS NULL
			AND dxu.id IN (
				SELECT
					dxu.id
				FROM
					semi.teams t
					JOIN semi.team_hierarchies h ON t.id = h.ancestor_id);
    """,
        pg_conn,
    )
    assert result[0]["count"] == 1

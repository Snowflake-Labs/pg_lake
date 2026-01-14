import pytest
import psycopg2
import time
import duckdb
import math
import datetime
from decimal import *
from utils_pytest import *


def test_s3_copy_to_csv_with_options(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.csv"

    # Generate CSV data on mock S3
    # Assume 'run_command' executes SQL commands in PostgreSQL
    # This CSV has a custom delimiter (;), uses double quotes for quoting,
    # and backslash for escape, and 'NULL' to represent null values.
    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_gen
              FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT CSV, HEADER, DELIMITER ';', QUOTE '"', ESCAPE '\\', NULL 'NULL');
    """,
        pg_conn,
    )

    # Create the foreign table using the options that match the CSV generation
    run_command(
        """
                CREATE FOREIGN TABLE test_s3_csv_with_options (
                    id int,
                    desc_col text
                ) SERVER pg_lake OPTIONS (format 'csv', header 'true', delimiter ';', quote '"', escape '\\', null 'NULL', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table and aggregate data
    # This query sums the IDs and counts the non-null descriptions
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT SUM(id) AS total_id, COUNT(desc_col) AS desc_count FROM test_s3_csv_with_options;
    """
    )
    result = cur.fetchone()
    cur.close()

    # Assert the correctness of the aggregated results
    print(result)
    assert result == (15, 3), f"Expected aggregated results (15, 3), got {result}"

    # Cleanup
    pg_conn.rollback()


def test_s3_copy_to_csv_with_defaults(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to_default/data.csv"

    # Generate CSV data on mock S3 with default settings
    # Here, the default FORMAT CSV is used without specifying HEADER, DELIMITER, QUOTE, ESCAPE, or NULL.
    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS text_desc
              FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT CSV);
    """,
        pg_conn,
    )

    # Create the foreign table without specifying format options, relying on defaults
    run_command(
        """
                CREATE FOREIGN TABLE test_s3_csv_defaults (
                    id int,
                    text_desc text
                ) SERVER pg_lake OPTIONS (format 'csv', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table and aggregate data
    # This query aims to verify that data is correctly interpreted using default options
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT SUM(id) AS total_id, COUNT(text_desc) AS text_desc_count FROM test_s3_csv_defaults;
    """
    )
    result = cur.fetchone()
    cur.close()

    # Assert the correctness of the aggregated results
    # Expected result: Sum of IDs (15) and count of non-null descriptions (3)
    assert result == (15, 3), f"Expected aggregated results (15, 3), got {result}"

    # Cleanup
    pg_conn.rollback()


def test_s3_csv_custom_options(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_custom_options/data.csv"

    # Generate CSV data with specific options: delimiter ';', quote '"', escape '\\', null 'NULL'
    # This data includes a quoted field with the delimiter inside, an escaped quote, and a NULL representation
    run_command(
        f"""
        COPY (VALUES
            (1, 'hello;world', 'active'),
            (2, '"hello\\"world"', NULL),
            (3, 'simple', 'active')
        ) TO '{url}' WITH (FORMAT CSV, DELIMITER ';', QUOTE '"', ESCAPE '\\', NULL 'NULL', HEADER);
    """,
        pg_conn,
    )

    # Create the foreign table using specified options
    run_command(
        """
                CREATE FOREIGN TABLE test_s3_custom_options_fdw (
                    id int,
                    text_desc text,
                    status text
                ) SERVER pg_lake OPTIONS (format 'csv', header 'true', delimiter ';', quote '"', escape '\\', null 'NULL', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table to retrieve data
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT id, text_desc, status FROM test_s3_custom_options_fdw ORDER BY id;
    """
    )
    results = cur.fetchall()
    cur.close()

    # Assert the correctness of the data
    expected_results = [
        (1, "hello;world", "active"),  # Delimiter inside quoted field
        (
            2,
            '"hello\\"world"',
            None,
        ),  # Escaped quote inside quotes and NULL represented as 'NULL' in the file, so we properly get it here as None
        (3, "simple", "active"),  # Simple case
    ]

    assert results == expected_results, f"Expected {expected_results}, got {results}"

    # Cleanup
    pg_conn.rollback()


def test_s3_csv_variety_types_handling(pg_conn, s3, extension):
    # Assuming `run_command` function executes SQL commands in PostgreSQL
    # and `csv_path` is the S3 URL for the CSV file
    csv_path = f"s3://{TEST_BUCKET}/test_variety_types_handling/data.csv"

    # Create table with a variety of data types
    create_table_command = """
    CREATE TABLE test_types (
        c_bit bit,
        c_bool bool,
        c_bpchar bpchar,
        c_bytea text, /* currently disabled / not supported */
        c_char char,
        c_cidr cidr,
        c_date date,
        c_float4 float4,
        c_float8 float8,
        c_inet inet,
        c_int2 int2,
        c_int4 int4,
        c_int8 int8,
        c_interval interval,
        c_json json,
        c_jsonb jsonb,
        c_money money,
        c_name name,
        c_numeric numeric,
        c_numeric_large numeric(39,2),
        c_numeric_mod numeric(4,2),
        c_oid oid,
        c_text text,
        c_time time,
        c_timestamp timestamp,
        c_timestamptz timestamptz,
        c_timetz timetz,
        c_uuid uuid,
        c_varbit varbit,
        c_varchar varchar
    );
    """
    run_command(create_table_command, pg_conn)

    # Insert data into the table
    insert_command = """
    INSERT INTO test_types VALUES (
        '1', true, 'hello', '\\x0001', 'a', '192.168.0.0/16',
        '2024-01-01', 3.4, 33333333.33444444, '192.168.1.1', 14, 100000, 10000000000,
        '3 days', '{"hello":"world"}', '{"hello":"world"}', '$4.5', 'test',
        199.123, 123456789012345678901234.99,
        99.99, 11, 'fork', '19:34', '2024-01-01 15:00:00', '2024-01-01 15:00:00 UTC', '19:34 UTC',
        'acd661ca-d18c-42e2-9c4e-61794318935e', '0110', 'abc'
    );
    """
    run_command(insert_command, pg_conn)

    # Copy data from the table to a CSV file on S3 with chosen CSV options
    copy_command = f"""
    COPY test_types TO '{csv_path}' WITH (FORMAT CSV, HEADER, DELIMITER ';', QUOTE '"', ESCAPE '\\');
    """
    run_command(copy_command, pg_conn)

    # Create a foreign table that reads the CSV data from S3
    create_fdw_table_command = f"""
    CREATE FOREIGN TABLE fdw_test_types (
        c_bit bit,
        c_bool bool,
        c_bpchar bpchar,
        c_bytea text, /* currently disabled / not supported */
        c_char char,
        c_cidr cidr,
        c_date date,
        c_float4 float4,
        c_float8 float8,
        c_inet inet,
        c_int2 int2,
        c_int4 int4,
        c_int8 int8,
        c_interval interval,
        c_json json,
        c_jsonb jsonb,
        c_money money,
        c_name name,
        c_numeric numeric,
        c_numeric_large numeric(39,2),
        c_numeric_mod numeric(4,2),
        c_oid oid,
        c_text text,
        c_time time,
        c_timestamp timestamp,
        c_timestamptz timestamptz,
        c_timetz timetz,
        c_uuid uuid,
        c_varbit varbit,
        c_varchar varchar
    ) SERVER pg_lake OPTIONS (format 'csv', header 'true', delimiter ';', quote '"', escape '\\', null 'NULL', path '{csv_path}');
    """
    run_command(create_fdw_table_command, pg_conn)

    # Query the foreign table to verify data integrity
    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM fdw_test_types;")
    result = cur.fetchall()
    cur.close()

    expected_result = [
        (
            "1",  # c_bit
            True,  # c_bool
            "hello",  # c_bpchar (expect trailing spaces due to character type padding)
            b"\x00\x01",  # c_bytea
            "a",  # c_char
            "192.168.0.0/16",  # c_cidr
            datetime.date(2024, 1, 1),  # c_date
            3.4,  # c_float4
            33333333.33444444,  # c_float8
            "192.168.1.1",  # c_inet
            14,  # c_int2
            100000,  # c_int4
            10000000000,  # c_int8
            datetime.timedelta(days=3),  # c_interval
            {"hello": "world"},  # c_json
            {"hello": "world"},  # c_jsonb
            "$4.50",  # c_money (format may vary by locale)
            "test",  # c_name
            Decimal("199.123"),  # c_numeric
            Decimal("123456789012345678901234.99"),  # c_numeric_large
            Decimal("99.99"),  # c_numeric_mod
            11,  # c_oid
            "fork",  # c_text
            datetime.time(19, 34),  # c_time
            datetime.datetime(2024, 1, 1, 15, 0),  # c_timestamp
            datetime.datetime(
                2024,
                1,
                1,
                15,
                0,
                tzinfo=datetime.timezone(datetime.timedelta(seconds=10800)),
            ),  # c_timestamptz
            datetime.time(
                19, 34, tzinfo=datetime.timezone.utc
            ),  # c_timetz (adjust timezone as necessary)
            "acd661ca-d18c-42e2-9c4e-61794318935e",  # c_uuid
            "0110",  # c_varbit
            "abc",  # c_varchar
        )
    ]

    # Now we assert each field individually to ensure data integrity
    assert result[0][0] == expected_result[0][0], "c_bit mismatch"
    assert result[0][1] == expected_result[0][1], "c_bool mismatch"
    assert (
        result[0][2].strip() == expected_result[0][2]
    ), "c_bpchar mismatch"  # Trimming any padding spaces
    # bytea is currently not supported
    # assert bytes(result[0][3]) == expected_result[0][3], "c_bytea mismatch"
    assert (
        result[0][4].strip() == expected_result[0][4]
    ), "c_char mismatch"  # Trimming any padding spaces
    assert result[0][5] == expected_result[0][5], "c_cidr mismatch"
    assert result[0][6] == expected_result[0][6], "c_date mismatch"
    assert result[0][7] == expected_result[0][7], "c_float4 mismatch"
    assert result[0][8] == expected_result[0][8], "c_float8 mismatch"
    assert result[0][9] == expected_result[0][9], "c_inet mismatch"
    assert result[0][10] == expected_result[0][10], "c_int2 mismatch"
    assert result[0][11] == expected_result[0][11], "c_int4 mismatch"
    assert result[0][12] == expected_result[0][12], "c_int8 mismatch"
    assert result[0][13] == expected_result[0][13], "c_interval mismatch"
    assert result[0][14] == expected_result[0][14], "c_json mismatch"
    assert result[0][15] == expected_result[0][15], "c_jsonb mismatch"
    assert result[0][16] == expected_result[0][16], "c_money mismatch"
    assert result[0][17] == expected_result[0][17], "c_name mismatch"
    assert result[0][18] == expected_result[0][18], "c_numeric mismatch"
    assert result[0][19] == expected_result[0][19], "c_numeric_large mismatch"
    assert result[0][20] == expected_result[0][20], "c_numeric_mod mismatch"
    assert result[0][21] == expected_result[0][21], "c_oid mismatch"
    assert result[0][22] == expected_result[0][22], "c_text mismatch"
    assert result[0][23] == expected_result[0][23], "c_time mismatch"
    assert result[0][24] == expected_result[0][24], "c_timestamp mismatch"

    # Adjusted assertions to compare only the dates of datetime objects
    assert (
        result[0][25].date() == expected_result[0][25].date()
    ), "c_timestamptz date mismatch"

    # Assuming you want to compare hours and minutes only for c_timetz
    assert (
        result[0][26].hour == expected_result[0][26].hour
        and result[0][26].minute == expected_result[0][26].minute
    ), "c_timetz hour and minute mismatch"

    assert result[0][27] == expected_result[0][27], "c_uuid mismatch"
    assert result[0][28] == expected_result[0][28], "c_varbit mismatch"
    assert result[0][29] == expected_result[0][29], "c_varchar mismatch"

    # Cleanup
    pg_conn.rollback()


# inspired from Postgresql's copy tests
def test_csv_features_with_s3_and_fdw(pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_csv_features_with_s3_and_fdw/data.csv"

    # Create a temporary table for the test
    pg_conn.cursor().execute(
        """
        CREATE TEMP TABLE copytest (
            style   text,
            test    text,
            filler  int
        );
    """
    )
    pg_conn.commit()

    # Insert data with various styles of embedded line-ending characters
    pg_conn.cursor().execute(
        """
        INSERT INTO copytest VALUES('DOS', E'abc\\r\\ndef', 1);
        INSERT INTO copytest VALUES('Unix', E'abc\\ndef', 2);
        INSERT INTO copytest VALUES('Mac', E'abc\\rdef', 3);
        INSERT INTO copytest VALUES(E'esc\\\\ape', E'a\\\\r\\\\\\\\r\\\\\\\\n\\\\nb', 4);
    """
    )
    pg_conn.commit()

    # Copy data from the table to a CSV file in S3
    pg_conn.cursor().execute(
        f"""
        COPY copytest TO '{url}' WITH (FORMAT CSV, HEADER, DELIMITER ';', QUOTE '"', ESCAPE '\\');
    """
    )
    pg_conn.commit()

    # Create a foreign table to read the CSV data from S3
    pg_conn.cursor().execute(
        """
        CREATE FOREIGN TABLE fdw_copytest (
            style text,
            test text,
            filler int
        ) SERVER pg_lake OPTIONS (path '{}', format 'csv', header 'true', delimiter ';', quote '"', escape '\\');
    """.format(
            url
        )
    )
    pg_conn.commit()

    cur = pg_conn.cursor()

    # Verify the data was copied and can be read correctly
    cur.execute(
        """
        SELECT * FROM copytest EXCEPT SELECT * FROM fdw_copytest;
    """
    )
    results = cur.fetchall()
    assert results == [], "Mismatch found between original and FDW-loaded data."

    cur.execute(
        """
        SELECT * FROM fdw_copytest EXCEPT SELECT * FROM copytest;
    """
    )
    results = cur.fetchall()
    assert results == [], "Mismatch found between FDW-loaded data and original."

    # Clean up
    pg_conn.cursor().execute("DROP TABLE copytest;")
    pg_conn.cursor().execute("DROP FOREIGN TABLE fdw_copytest;")
    pg_conn.commit()


def test_s3_copy_to_csv_with_complex_target_lists(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to_csv_with_complex_target_lists/data.csv"

    # Generate CSV data on mock S3
    # Assume 'run_command' executes SQL commands in PostgreSQL
    # This CSV has a custom delimiter (;), uses double quotes for quoting,
    # and backslash for escape, and 'NULL' to represent null values.
    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_gen
              FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT CSV, HEADER, DELIMITER ';', QUOTE '"', ESCAPE '\\', NULL 'NULL');
    """,
        pg_conn,
    )

    # Create the foreign table using the options that match the CSV generation
    run_command(
        """
                CREATE FOREIGN TABLE test_s3_csv_with_options (
                    id int,
                    desc_col text
                ) SERVER pg_lake OPTIONS (format 'csv', header 'true', delimiter ';', quote '"', escape '\\', null 'NULL', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # empty target list, random() is to prevent
    # optimizations by pulling subquery
    cur = pg_conn.cursor()
    cur.execute(
        """
        select count(*) FROM (select  from test_s3_csv_with_options ORDER BY random());
    """
    )
    result = cur.fetchone()
    cur.close()

    assert result[0] == 5, f"Expected 5 rows, got {result[0]}"

    # same column used multiple times
    cur = pg_conn.cursor()
    cur.execute(
        """
        select id, id, id+id from test_s3_csv_with_options ORDER BY 1 DESC;
    """
    )
    result = cur.fetchone()
    cur.close()

    assert result[0] == 5, f"unexpected query result for complex target list"
    assert result[1] == 5, f"unexpected query result for complex target list"
    assert result[2] == 10, f"unexpected query result for complex target list"

    # same column used multiple times
    cur = pg_conn.cursor()
    cur.execute(
        """
            SELECT *
            FROM
                (
                SELECT id, desc_col FROM test_s3_csv_with_options OFFSET 0
                ) as bar(x, y)
            ORDER BY 1 DESC, 2 DESC LIMIT 5;
    """
    )
    result = cur.fetchone()
    cur.close()
    assert result[0] == 5, f"unexpected query result for complex target list"
    assert result[1] == "hello-5", f"unexpected query result for complex target list"

    # complex expressions all over the query
    cur = pg_conn.cursor()
    cur.execute(
        """
            SELECT
               DISTINCT ON (avg_col) avg_col, cnt_1, cnt_2, cnt_3, sum_col, l_year_gt_2024, pos, count_id
            FROM
                (
                    SELECT avg(id * (5.0 / (length(desc_col) + 0.1))) as avg_col
                    FROM test_s3_csv_with_options
                    ORDER BY 1 DESC
                    LIMIT 3
                ) as foo,
                (
                    SELECT sum(id * (5.0 / (length(desc_col) + 0.1))) as cnt_1
                    FROM test_s3_csv_with_options
                    ORDER BY 1 DESC
                    LIMIT 3
                ) as bar,
                (
                    SELECT
                        avg(case
                            when id > 4
                            then length(desc_col)
                        end) as cnt_2,
                        avg(case
                            when id > 5
                            then length(desc_col)
                        end) as cnt_3,
                        sum(case
                            when position('a' in desc_col) > 0
                            then 1
                            else 0
                        end) as sum_col,
                        (extract(year FROM current_date))>=2024 as l_year_gt_2024, -- Using current_date for demo purposes
                        strpos(max(desc_col), 'a') as pos
                    FROM
                        test_s3_csv_with_options
                    ORDER BY
                        1 DESC
                    LIMIT 4
                ) as baz,
                (
                    SELECT COALESCE(length(desc_col), 20) AS count_id
                    FROM test_s3_csv_with_options
                    ORDER BY 1 OFFSET 2 LIMIT 5
                ) as tar
            ORDER BY 1 DESC;


    """
    )
    result = cur.fetchone()
    cur.close()
    assert result[2] == 7, f"unexpected query result for complex target list"
    assert result[3] == None, f"unexpected query result for complex target list"
    assert result[4] == 0, f"unexpected query result for complex target list"
    assert result[5] == True, f"unexpected query result for complex target list"
    assert result[6] == 0, f"unexpected query result for complex target list"
    assert result[7] == 7, f"unexpected query result for complex target list"

    # Cleanup
    pg_conn.rollback()


def test_s3_copy_to_csv_zero_rows_table(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to_zero_rows/data.csv"

    # Generate zero-row CSV data on mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_gen
              FROM generate_series(1,0) s) TO '{url}' WITH (FORMAT CSV, HEADER, DELIMITER ';', QUOTE '"', ESCAPE '\\', NULL 'NULL');
    """,
        pg_conn,
    )

    # Create the foreign table using the options that match the CSV generation
    run_command(
        """
                CREATE FOREIGN TABLE test_s3_csv_with_options (
                    id int,
                    desc_col text
                ) SERVER pg_lake OPTIONS (format 'csv', header 'true', delimiter ';', quote '"', escape '\\', null 'NULL', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table and aggregate data
    # This query sums the IDs and counts the non-null descriptions
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT COUNT(desc_col) AS desc_count FROM test_s3_csv_with_options;
    """
    )
    result = cur.fetchone()
    cur.close()

    # Assert the correctness of the aggregated results
    assert result == ((0,)), f"Expected zero rows, got {result}"

    # Cleanup
    pg_conn.rollback()


def test_quoted_null(pg_conn, s3, extension, tmp_path):
    csv_key = "test_quoted_null/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Use weird syntax
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write("abc,def\n")
        csv_file.write('"3.4","hello"\n')
        csv_file.write('"","world"\n')

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    run_command(
        f"""
       CREATE FOREIGN TABLE test_quoted_null ()
       SERVER pg_lake OPTIONS (path '{csv_path}', header 'true');
    """,
        pg_conn,
    )

    # we expect to interpret the "" as NULL
    result = run_query("SELECT * FROM test_quoted_null ORDER BY 1", pg_conn)
    assert len(result) == 2
    assert result[0] == [3.4, "hello"]
    assert result[1] == [None, "world"]

    pg_conn.rollback()


def test_csv_unsupported_column_array(pg_conn, extension):
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                """,
        pg_conn,
    )

    # table with invalid type
    error = run_command(
        """CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                    id int,
                    value text[]
                ) SERVER pg_lake OPTIONS (format 'csv', path 's3://');
    """,
        pg_conn,
        raise_error=False,
    )

    # can't get here, already failed
    assert "array types are not supported for JSON/CSV backed pg_lake tables" in error

    pg_conn.rollback()


def test_csv_unsupported_column_composite(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                CREATE TYPE test_invalid_data_format_sc.tmp_type AS (a int, b int);
                """,
        pg_conn,
    )

    # table with invalid type
    error = run_command(
        """CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                        id int,
                        value test_invalid_data_format_sc.tmp_type
                    ) SERVER pg_lake OPTIONS (format 'csv', path 's3://');
    """,
        pg_conn,
        raise_error=False,
    )

    assert (
        "composite types are not supported for JSON/CSV backed pg_lake tables" in error
    )

    pg_conn.rollback()


def test_csv_unsupported_column_bytea(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                CREATE TYPE test_invalid_data_format_sc.tmp_type AS (a int, b int);
                """,
        pg_conn,
    )

    # table with invalid type
    error = run_command(
        """CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                        id int,
                        value bytea
                    ) SERVER pg_lake OPTIONS (format 'csv', path 's3://');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "bytea type is not supported for JSON/CSV backed pg_lake tables" in error

    pg_conn.rollback()


# sniff_csv should be able to detect the header on foreign table creation
def test_s3_copy_to_csv_header_detection(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to_csv_header_detection/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/test_s3_copy_to_csv_header_detection/data.csv"

    # Generate CSV data on mock S3
    run_command(
        f"""
    COPY (SELECT s x, s y FROM generate_series(1,4) s) TO '{url}' WITH (FORMAT 'CSV', HEADER);
    """,
        pg_conn,
    )

    # Create the foreign table using the options that match the CSV generation
    run_command(
        """
                CREATE FOREIGN TABLE xy () SERVER pg_lake OPTIONS (path '{}');
    """.format(
            csv_path
        ),
        pg_conn,
    )

    # Query the foreign table for all rows
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT * from xy;
    """
    )
    result = cur.fetchall()
    cur.close()

    # Assert the correctness of the aggregated results
    assert result == [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
    ], f"Expected [(1, 1), (2, 2), (3, 3), (4, 4)]"

    # Cleanup
    pg_conn.rollback()


def test_s3_multiple_csv_file_sniff_detection(pg_conn, s3, extension):
    # test the detection of multiple files from a single directory

    base_url = f"s3://{TEST_BUCKET}/test_s3_multiple_csv_file_sniff_detection/"
    wildcard_url = base_url + "*"

    # Generate multiple CSV data files on mock S3
    for fileno in range(1, 5):
        run_command(
            f"""
        COPY (SELECT s x, s y FROM generate_series(1,4) s) TO '{base_url}/data{fileno}.csv' WITH (FORMAT 'CSV', HEADER);
        """,
            pg_conn,
        )

    # Create the foreign table using the options that match the CSV generation
    run_command(
        f"""
                CREATE FOREIGN TABLE xy () SERVER pg_lake OPTIONS (format 'csv', path '{wildcard_url}');
    """,
        pg_conn,
    )

    res = run_query("SELECT COUNT(*) FROM xy", pg_conn)

    # counts should match the totals
    assert res[0][0] == 16

    pg_conn.rollback()


def test_csv_with_windows_crlf_newlines(pg_conn, s3, extension, tmp_path):
    """Test that CSV files with Windows-style CRLF (\\r\\n) line endings are properly handled"""
    csv_key = "test_csv_crlf_newlines/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Create a CSV file with explicit Windows-style CRLF newlines
    local_csv_path = tmp_path / "crlf_data.csv"
    with open(local_csv_path, "wb") as csv_file:
        # Write CSV with CRLF line endings (Windows style)
        csv_file.write(b"id,name,value\r\n")
        csv_file.write(b"1,alice,100\r\n")
        csv_file.write(b"2,bob,200\r\n")
        csv_file.write(b"3,charlie,300\r\n")

    # Upload to S3
    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    # Create foreign table with column inference
    run_command(
        f"""
        CREATE FOREIGN TABLE test_crlf_csv ()
        SERVER pg_lake OPTIONS (path '{csv_path}', format 'csv', header 'true');
        """,
        pg_conn,
    )

    # Query the data and verify it's read correctly
    result = run_query("SELECT * FROM test_crlf_csv ORDER BY id", pg_conn)

    # Verify all rows are read correctly
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result[0] == [1, "alice", 100], f"Row 1 mismatch: {result[0]}"
    assert result[1] == [2, "bob", 200], f"Row 2 mismatch: {result[1]}"
    assert result[2] == [3, "charlie", 300], f"Row 3 mismatch: {result[2]}"

    # Verify aggregation works correctly
    count_result = run_query("SELECT COUNT(*), SUM(value) FROM test_crlf_csv", pg_conn)
    assert count_result[0] == [3, 600], f"Aggregation mismatch: {count_result[0]}"

    # Cleanup
    pg_conn.rollback()


def test_s3_csv_null_padding_with_missing_trailing_commas(pg_conn, s3, extension):
    # Test null_padding with CSV that has missing trailing commas (common CSV issue)
    url = f"s3://{TEST_BUCKET}/test_null_padding_missing_commas/data.csv"

    # Manually construct CSV with missing trailing commas for NULL values
    # This simulates a common CSV export issue where trailing empty fields omit commas
    csv_content = """id,name,value
1,Alice,A
2,,B
3,Charlie
4"""

    # Write the manually constructed CSV to S3
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key="test_null_padding_missing_commas/data.csv",
        Body=csv_content,
    )

    # Create foreign table with null_padding enabled
    run_command(
        f"""
        CREATE FOREIGN TABLE test_null_padding_missing_commas (
            id int,
            name text,
            value text
        ) SERVER pg_lake OPTIONS (
            format 'csv',
            path '{url}',
            header 'true',
            null_padding 'true'
        );
        """,
        pg_conn,
    )

    result = run_query(
        "SELECT * FROM test_null_padding_missing_commas ORDER BY id", pg_conn
    )

    # Verify that missing trailing commas are handled correctly as NULL
    # Row 3 has missing value field (no comma after Charlie)
    # Row 4 has missing name and value fields (no commas after 4)
    expected = [
        [1, "Alice", "A"],
        [2, None, "B"],
        [3, "Charlie", None],
        [4, None, None],
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    pg_conn.rollback()


def test_s3_csv_null_padding_with_empty_fields(pg_conn, s3, extension):
    # Test null_padding with empty fields (commas present but fields empty)
    url = f"s3://{TEST_BUCKET}/test_null_padding_empty/data.csv"

    # Manually construct CSV with empty fields (commas present)
    csv_content = """id,name,value
1,Alice,A
2,,B
3,Charlie,
4,,"""

    # Write the manually constructed CSV to S3
    s3.put_object(
        Bucket=TEST_BUCKET, Key="test_null_padding_empty/data.csv", Body=csv_content
    )

    # Test with null_padding enabled
    run_command(
        f"""
        CREATE FOREIGN TABLE test_null_padding_empty_enabled (
            id int,
            name text,
            value text
        ) SERVER pg_lake OPTIONS (
            format 'csv',
            path '{url}',
            header 'true',
            null_padding 'true'
        );
        """,
        pg_conn,
    )

    # Test with null_padding disabled
    run_command(
        f"""
        CREATE FOREIGN TABLE test_null_padding_empty_disabled (
            id int,
            name text,
            value text
        ) SERVER pg_lake OPTIONS (
            format 'csv',
            path '{url}',
            header 'true',
            null_padding 'false'
        );
        """,
        pg_conn,
    )

    result_enabled = run_query(
        "SELECT * FROM test_null_padding_empty_enabled ORDER BY id", pg_conn
    )
    result_disabled = run_query(
        "SELECT * FROM test_null_padding_empty_disabled ORDER BY id", pg_conn
    )

    # Empty fields should be treated as NULL
    expected = [
        [1, "Alice", "A"],
        [2, None, "B"],
        [3, "Charlie", None],
        [4, None, None],
    ]
    assert result_enabled == expected, f"Expected {expected}, got {result_enabled}"
    assert result_disabled == expected, f"Expected {expected}, got {result_disabled}"

    pg_conn.rollback()


def test_s3_csv_null_padding_with_custom_null_and_padding(pg_conn, s3, extension):
    # Test null_padding with custom NULL string and various padding scenarios
    url = f"s3://{TEST_BUCKET}/test_null_padding_custom_null/data.csv"

    # Manually construct CSV with custom NULL representation and whitespace
    csv_content = """id,status,code
1,active,A
2,N/A,B
3,inactive
4,N/A"""

    # Write the manually constructed CSV to S3
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key="test_null_padding_custom_null/data.csv",
        Body=csv_content,
    )

    # Create foreign table with null_padding and custom null string
    run_command(
        f"""
        CREATE FOREIGN TABLE test_null_padding_custom_null (
            id int,
            status text,
            code text
        ) SERVER pg_lake OPTIONS (
            format 'csv',
            path '{url}',
            header 'true',
            null 'N/A',
            null_padding 'true'
        );
        """,
        pg_conn,
    )

    result = run_query(
        "SELECT * FROM test_null_padding_custom_null ORDER BY id", pg_conn
    )

    # Verify that custom NULL string is recognized
    expected = [
        [1, "active", "A"],
        [2, None, "B"],
        [3, "inactive", None],
        [4, None, None],
    ]
    assert result == expected, f"Expected {expected}, got {result}"

    # Verify NULL counts
    null_count = run_query(
        "SELECT COUNT(*) FROM test_null_padding_custom_null WHERE status IS NULL OR code IS NULL",
        pg_conn,
    )
    assert null_count[0][0] == 3, "Expected 3 rows with NULL values"

    pg_conn.rollback()

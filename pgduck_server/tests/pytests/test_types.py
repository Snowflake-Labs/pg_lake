"""
This pytest script is designed to validate the consistency between
data accessed via pure duckdb and PGDuckServer. The testing process
is outlined as follows:

        1. `setup_table()`: This function is responsible for creating and loading a table in duckdb.
                It sets the initial testing environment by preparing the data schema and populating the table with data.

        2. `export_import_database()`: Following the setup, this function is tasked with recreating the database
                initialized by `setup_table()`, but with a significant change: it operates over PGDuckServer. As a result,
                by the completion of this step, there are two identical tables in terms of schema and data. One is
                accessible through pure duckdb, and the other through PGDuckServer.

        3. Test functions (prefixed with `test_`): Each of these functions conducts specific data type
           fetches from both the pure duckdb and PGDuckServer. The primary objective here is to compare
           the outputs from both sources. This comparison is crucial for ensuring that PGDuckServer replicates
           the results from pure duckdb accurately, maintaining consistency in data handling and retrieval across
           both platforms.

The tests aim to affirm the reliability and compatibility of PGDuckServer with duckdb by ensuring that data
fetched from both sources remains consistent across various data types and query operations.
"""

import pytest
import duckdb
import psycopg2
import os
import shutil
import csv
import sys
from utils_pytest import *
import server_params
from datetime import datetime, timezone

csv.field_size_limit(sys.maxsize)

CREATE_TABLE_CMD = """
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE duckdb_supported_types_table (
    boolean_col BOOLEAN,
    blob_col BYTEA,
    tiny_int_col TINYINT,
    smallint_col SMALLINT,
    int_col INT,
    bigint_col BIGINT,
    utinyint_col UTINYINT,
    usmallint_col USMALLINT,
    uinteger_col UINTEGER,
    uint64_col UINT64,
    float4_col float,
    float8_col double,
    varchar_col varchar,
    decimal_col decimal,
    uuid_col uuid,
    hugeint_col hugeint,
    uhugeint_col uhugeint,
    timestamp_ns_col timestamp_ns,
    timestamp_ms_col timestamp_ms,
    timestamp_s_col timestamp_s,
    timestamp_col timestamp,
    timestamp_tz_col timestamptz,
    time_col time,
    time_tz_col timetz,
    interval_col interval,
    date_col date,
    struct_col STRUCT (x int, y int),
    list_col int[],
    array_col int[3],
    multi_array_col int[3][3],
    map_col MAP(int, text),
    enum_col mood
);

CREATE TABLE duckdb_unsupported_types_table (
    union_col UNION (num int, str varchar)
);
"""


# for each data type, we insert one column filled
# such as "INSERT INTO duckdb_supported_types_table (boolean_col) VALUES (True)"
# where all other columns are NULL expect for the one that we are interested in
# we also have a single test where all columns are NULL, for that see test_nulls()
@pytest.fixture(scope="module")
def setup_table(duckdb_conn, pg_conn):

    duckdb_conn.execute(CREATE_TABLE_CMD)

    # load data
    with duckdb_conn.cursor() as cur:
        # Boolean edge cases
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (boolean_col) VALUES (True), (False);"
        )

        # blob edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (blob_col) VALUES ('\\x00'), ('\\xDE\\xAD\\xBE\\xAF');"
        )

        # TinyInt edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (tiny_int_col) VALUES (127), (-128), (0);"
        )

        # SmallInt edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (smallint_col) VALUES (32767), (-32768), (0);"
        )

        # Int edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (int_col) VALUES (2147483647), (-2147483648), (0);"
        )

        # BigInt edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (bigint_col) VALUES (9223372036854775807), (-9223372036854775808), (0);"
        )

        # UTinyInt edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (utinyint_col) VALUES (255), (0);"
        )

        # USmallInt edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (usmallint_col) VALUES (65535), (0);"
        )

        # UInteger edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (uinteger_col) VALUES (4294967295), (0);"
        )

        # UBigInt edge cases and regular values
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (uint64_col) VALUES (18446744073709551615), (9223372036854775807), (0);"
        )

        # float4
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (float4_col) VALUES (1e-30), (1e+30), (3.4028235E+38), (-3.4028235E+38), (0.1234567), (-0.1234567), (1e-40), (-1e-40),  (1.17549435E-38), (-1.17549435E-38), (4294967295), (123456789.987654321), ('NaN'), ('Infinity'), ('-Infinity')"
        )

        # float8
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (float8_col) VALUES  (2.2250738585072014e-308), (-2.2250738585072014e-307),(0.1234567890123456), (-0.1234567890123456), (1e-40), (-1e-40), (1e+40), (-1e+40), (9223372036854775807), (-9223372036854775808),(123456789.987654321), (-123456789.987654321),  ('NaN'), ('Infinity'), ('-Infinity'), (0.000001), (-0.000001), (1.7976931348623157e+307), (-1.7976931348623157e+307);"
        )

        # varchar
        cur.execute(
            """
		INSERT INTO duckdb_supported_types_table (varchar_col) VALUES
			('A'), -- Single character
			('This is a test string of length 31!'), -- 31 characters
			('This is a test string of length 32!!'), -- 32 characters
			('This is a test string of length 33!!!'), -- 33 characters
			('Short'), -- Short string
			('This is a much longer string that exceeds the typical lengths and is used to test the varchar implementation in the database, ensuring that it can handle a wide range of string lengths without issue.'),
			('Special characters !@#$%^&*()'),
			('String with spaces    '),
			('1234567890'), -- Numeric characters
			('中文字符测试'), -- Non-English characters
			('🙂😉👍'), -- Emoji characters,
			(repeat('AAAAAAAAAAAAAAAA', 100000)); -- very large text
		"""
        )

        cur.execute(
            """
		    INSERT INTO duckdb_supported_types_table (decimal_col) VALUES
		    ('99999999999999.999'), -- Max value for DECIMAL(18,3)
		    ('-99999999999999.999'), -- Min value for DECIMAL(18,3)
		    ('12345.678'), -- Normal value within precision and scale limits
		    ('0.001'), -- Small scale value
		    ('123456789.321'), -- Within precision and scale limits
		    ('0.1'), -- Simple fraction within scale
		    ('-0.1'), -- Negative simple fraction within scale
		    ('99999999.999'), -- Near max for width 18, scale 3
		    ('-99999999.999'), -- Negative near max for width 18, scale 3
		    ('1.234'), -- Test value within precision and scale
		    ('-1.234'), -- Negative test value within precision and scale
		    ('123.456'), -- Additional normal value
		    ('-123.456'), -- Negative normal value
		    ('0.123'), -- Test small decimal value
		    ('-0.123'), -- Test small negative decimal value
		    ('121'), -- Equivalent to casting 121 as decimal(18,3)
		    ('-121'), -- Negative equivalent
		    ('1000'), -- Round number
		    ('5555.5555'), -- lose precision, round to 5555.556
		    ('-1000'); -- Negative round number
		"""
        )

        cur.execute(
            """
			INSERT INTO duckdb_supported_types_table (uuid_col) VALUES
			('111e1179-01dd-4f7e-9a67-139fa9b2236c'), -- Normal UUID
			('00000000-0000-0000-0000-000000000000'), -- Min UUID
			('ffffffff-ffff-ffff-ffff-ffffffffffff'), -- Max UUID
			('12345678-1234-1234-1234-123456789abc'), -- Normal UUID with pattern
			('abcd1234-ab12-cd34-ef56-abcdef123456'), -- Mixed case, normal UUID
			('11111111-1111-1111-1111-111111111111'), -- Repeating digits
			('00000000-0000-0000-c000-000000000046'), -- Variant and version
			('deadbeef-dead-beef-dead-beefdeadbeef'), -- Hexadecimal words
			('cafebab3-cafe-babe-cafe-babecafebabe'), -- Cafe babe pattern
			('01234567-89ab-cdef-0123-456789abcdef'); -- Sequential hex pattern
			"""
        )

        ## TODO: The min and max hugeints are adjusted, the last numbers
        ## 		 are deleted because Python duckdb package fails to do the
        ##		 conversion. Once fixed, we should use
        ##		 -170141183460469231731687303715884105728 and
        ##		170141183460469231731687303715884105727 respectively
        cur.execute(
            """
			INSERT INTO duckdb_supported_types_table (hugeint_col) VALUES
			(-17014118346046923173168730371588410572), -- Min hugeint, see TODO
			(17014118346046923173168730371588410572), -- Max hugeint, see TODO
			(0), -- Zero
			(-1), -- Negative one
			(1), -- One
			(-9223372036854775808), -- Min bigint
			(9223372036854775807), -- Max bigint
			(-2147483648), -- Min int
			(2147483647), -- Max int
			(-32768), -- Min smallint
			(32767); -- Max smallint
			"""
        )

        cur.execute(
            """
			INSERT INTO duckdb_supported_types_table (uhugeint_col) VALUES
            ('340282366920938463463374607431768211455'), -- Max uhugeint
			(0), -- Zero
			(1), -- One
			(18446744073709551615), -- Max ubigint
			(4294967295), -- Max uint
			(65535); -- Max usmallint
			"""
        )

        # TODO: We should consider adding more tests for timestamp columns
        # 		However, the python libraries are has slight different output
        # 		formats, which requires some work to make all the outputs have the
        # 		the same format for comparison purposes
        # cur.execute("""
        # 	INSERT INTO duckdb_supported_types_table (timestamp_ns_col, timestamp_ms_col, timestamp_s_col, timestamp_col) VALUES
        # 	('1992-09-20 11:30:00.123456789', '1992-09-20 11:30:00.123456', '1992-09-20 11:30:00.123456', '1992-09-20 11:30:00.123456+00'), -- Normal precision
        # 	('1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.010', '1970-01-01 00:00:01', '1970-01-01 00:00:00+00'), -- Epoch + 1ns, 1ms, 1s
        # 	('2038-01-19 03:14:07.999999999', '2038-01-19 03:14:07.999', '2038-01-19 03:14:07', '2038-01-19 03:14:07+00'), -- 32-bit Unix time end + precision
        # 	('1901-12-13 20:45:52.000000001', '1901-12-13 20:45:52.000', '1901-12-13 20:45:52', '1901-12-13 20:45:52+00'); -- 32-bit Unix time start + precision
        # """)

        # Inserting data for timestamp column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (timestamp_col) VALUES ('2024-02-08 12:00:00')"
        )

        # Inserting data for timestamp_ms column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (timestamp_ms_col) VALUES ('2024-02-08 12:00:00.000')"
        )

        # Inserting data for timestamp_ns column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (timestamp_ns_col) VALUES ('2024-02-08 12:00:00.000000000')"
        )

        # Inserting data for timestamp_sec column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (timestamp_s_col) VALUES ('2024-02-08 12:00:00')"
        )

        # Inserting data for timestamp_tz column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (timestamp_tz_col) VALUES ('2024-02-08 12:00:00+03')"
        )

        # Inserting data for time column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (time_col) VALUES ('12:00:00')"
        )

        # Inserting data for time_tz column
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (time_tz_col) VALUES ('12:00:00+03')"
        )

        # TODO: Expand the tests
        # Pyth
        cur.execute(
            """
			INSERT INTO duckdb_supported_types_table (interval_col) VALUES
			('10 DAYS'), -- 10 days
			('1 YEAR'), -- 1 year
			('2 MONTHS'), -- 2 months
			('16 MONTHS 15 DAYS');
			"""
        )

        cur.execute(
            """
				INSERT INTO duckdb_supported_types_table (date_col) VALUES
				('1992-09-20'), -- A specific date
				('1970-01-01'), -- The 'epoch'
				('9999-12-31'), -- A far future date, often used to represent 'infinity' in practical scenarios
				('0001-01-01'); -- A far past date, representing the beginning of the Gregorian calendar in DuckDB

			"""
        )

        # struct type
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (struct_col) VALUES ({'x':3,'y':4})"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (struct_col) VALUES ({'x':NULL,'y':4})"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (struct_col) VALUES ({'x':3,'y':NULL})"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (struct_col) VALUES ({'x':NULL,'y':NULL})"
        )

        # list type
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([5,6])"
        )
        cur.execute("INSERT INTO duckdb_supported_types_table (list_col) VALUES ([])")
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([NULL])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([NULL,NULL,NULL])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([NULL,NULL,NULL,1])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([NULL,NULL,1,NULL])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([1,NULL,NULL])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (list_col) VALUES ([1,NULL,NULL,1])"
        )

        # array type
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (array_col) VALUES ([8,9,10])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (array_col) VALUES ([1,2,3])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (array_col) VALUES ([NULL,4,5])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (array_col) VALUES ([NULL,NULL,NULL])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (array_col) VALUES ([NULL,7,NULL])"
        )
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (array_col) VALUES ([1,NULL,2])"
        )

        # multi_array type
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (multi_array_col) VALUES ([[1,2,3],[4,5,6],[8,9,10]])"
        )

        # map type
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (map_col) VALUES (MAP {3:'three',4:'four'})"
        )

        # enum type
        cur.execute(
            "INSERT INTO duckdb_supported_types_table (enum_col) VALUES ('happy'), ('sad')"
        )

        # Add one row with all values NULL for each column
        nulls = ",".join(["NULL"] * 32)
        cur.execute(f"INSERT INTO duckdb_supported_types_table VALUES ({nulls});")

        # Insert unsupported types
        cur.execute(
            "INSERT INTO duckdb_unsupported_types_table (union_col) VALUES ('foo')"
        )
        cur.execute("INSERT INTO duckdb_unsupported_types_table (union_col) VALUES (5)")

    # setup the database
    export_import_database(duckdb_conn, pg_conn)

    yield

    duckdb_conn.execute("DROP TABLE duckdb_supported_types_table")


def test_boolean(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT boolean_col FROM duckdb_supported_types_table WHERE boolean_col IS NOT NULL ORDER BY boolean_col"
    expected = [(False,), (True,)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)

    # Convert each element in pg_results to a boolean
    pg_results_bool = [(strtobool(item[0]),) for item in pg_results]
    transmit_results_bool = [(strtobool(item[0]),) for item in transmit_results]

    assert (
        pg_results_bool == duckdb_results == transmit_results_bool == expected
    ), "BOOLEAN results do not match expected values!"


def test_blob(setup_table, duckdb_conn, pg_conn, tmp_path):
    # Yikes! When doing export/import in DuckDB, 0-length blobs get converted to NULL...
    # We insert it here instead of in setup_table to check whether we can at least handle
    # 0-length bytea.
    #
    # We skip duckdb_results for now, also because encoding is different (\x for every byte)

    run_command(
        "INSERT INTO duckdb_supported_types_table (blob_col) VALUES ('')", pg_conn
    )

    query = "SELECT blob_col FROM duckdb_supported_types_table WHERE blob_col IS NOT NULL ORDER BY blob_col"
    expected = [("\\x",), ("\\x00",), ("\\xdeadbeaf",)]
    pg_results = perform_query_on_cursor(query, pg_conn)
    print(pg_results)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "BLOB results do not match expected values!"


def test_tinyint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT tiny_int_col FROM duckdb_supported_types_table WHERE tiny_int_col IS NOT NULL ORDER BY tiny_int_col"
    expected = [("-128",), ("0",), ("127",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "TINYINT results do not match expected values!"


def test_smallint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT smallint_col FROM duckdb_supported_types_table WHERE smallint_col IS NOT NULL ORDER BY smallint_col"
    expected = [("-32768",), ("0",), ("32767",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "SMALLINT results do not match expected values!"


def test_int(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT int_col FROM duckdb_supported_types_table WHERE int_col IS NOT NULL ORDER BY int_col"
    expected = [("-2147483648",), ("0",), ("2147483647",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "INT results do not match expected values!"


def test_bigint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT bigint_col FROM duckdb_supported_types_table WHERE bigint_col IS NOT NULL ORDER BY bigint_col"
    expected = [("-9223372036854775808",), ("0",), ("9223372036854775807",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "BIGINT results do not match expected values!"


def test_utinyint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT utinyint_col FROM duckdb_supported_types_table WHERE utinyint_col IS NOT NULL ORDER BY utinyint_col"
    expected = [("0",), ("255",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "UTINYINT results do not match expected values!"


def test_usmallint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT usmallint_col FROM duckdb_supported_types_table WHERE usmallint_col IS NOT NULL ORDER BY usmallint_col"
    expected = [("0",), ("65535",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "USMALLINT results do not match expected values!"


def test_uinteger(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT uinteger_col FROM duckdb_supported_types_table WHERE uinteger_col IS NOT NULL ORDER BY uinteger_col"
    expected = [("0",), ("4294967295",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "UINTEGER results do not match expected values!"


def test_uint64(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT uint64_col FROM duckdb_supported_types_table WHERE uint64_col IS NOT NULL ORDER BY uint64_col"
    expected = [("0",), ("9223372036854775807",), ("18446744073709551615",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "UINT64 results do not match expected values!"


def test_float4(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT float4_col FROM duckdb_supported_types_table WHERE float4_col IS NOT NULL ORDER BY float4_col"

    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # we can safely support up-to 6 precision
    precision = 6

    # Function to round or pass through special values
    def round_or_special(value):
        try:
            return f"{float(value):.{precision}e}"
        except ValueError:
            return value

    # Apply the rounding or pass through for special values
    duckdb_results_processed = [(round_or_special(item[0]),) for item in duckdb_results]
    pg_results_processed = [(round_or_special(item[0]),) for item in pg_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(round_or_special(item[0]),) for item in transmit_results]

    assert (
        sorted(duckdb_results_processed)
        == sorted(pg_results_processed)
        == sorted(transmit_results)
    ), "float4 results do not match expected values!"


def test_float8(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT float8_col FROM duckdb_supported_types_table WHERE float8_col IS NOT NULL ORDER BY float8_col"

    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # We can safely support up-to 15 precision for float8
    precision = 15

    # Function to round or pass through special values
    def round_or_special(value):
        try:
            return f"{float(value):.{precision}e}"
        except ValueError:
            return value

    # Apply the rounding or pass through for special values
    duckdb_results_processed = [(round_or_special(item[0]),) for item in duckdb_results]
    pg_results_processed = [(round_or_special(item[0]),) for item in pg_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(round_or_special(item[0]),) for item in transmit_results]

    assert (
        sorted(duckdb_results_processed)
        == sorted(pg_results_processed)
        == sorted(transmit_results)
    ), "float8 results do not match expected values!"


def test_varchar(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT varchar_col FROM duckdb_supported_types_table WHERE varchar_col IS NOT NULL ORDER BY varchar_col"
    # We expect the results to be ordered alphabetically, considering special characters, numbers, and also non-English characters
    expected = [
        (repeat_string("AAAAAAAAAAAAAAAA", 100000),),
        ("1234567890",),
        ("A",),
        ("Short",),
        ("Special characters !@#$%^&*()",),
        (
            "This is a much longer string that exceeds the typical lengths and is used to test the varchar implementation in the database, ensuring that it can handle a wide range of string lengths without issue.",
        ),
        ("This is a test string of length 31!",),
        ("This is a test string of length 32!!",),
        ("This is a test string of length 33!!!",),
        ("String with spaces    ",),
        ("中文字符测试",),
        ("🙂😉👍",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == sorted(expected)
    ), "VARCHAR results do not match expected values!"


def repeat_string(s, n):
    """Imitates SQL's REPEAT function in Python."""
    return s * n


def test_decimal(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT decimal_col FROM duckdb_supported_types_table WHERE decimal_col IS NOT NULL ORDER BY decimal_col"
    expected = [
        ("-99999999999999.999",),
        ("-99999999.999",),
        ("-1000.000",),
        ("-123.456",),
        ("-121.000",),
        ("-1.234",),
        ("-0.123",),
        ("-0.100",),
        ("0.001",),
        ("0.100",),
        ("0.123",),
        ("1.234",),
        ("121.000",),
        ("123.456",),
        ("1000.000",),
        ("5555.556",),
        ("12345.678",),
        ("99999999.999",),
        ("123456789.321",),
        ("99999999999999.999",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    # Assuming pg_results is correctly fetched and formatted
    pg_results = perform_query_on_cursor(
        query, pg_conn
    )  # This should be adjusted to your actual comparison

    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "Decimal results do not match expected values!"


def test_uuid(setup_table, duckdb_conn, pg_conn, tmp_path):

    query = "SELECT uuid_col FROM duckdb_supported_types_table WHERE uuid_col IS NOT NULL ORDER BY uuid_col"
    expected = [
        ("00000000-0000-0000-0000-000000000000",),
        ("00000000-0000-0000-c000-000000000046",),
        ("01234567-89ab-cdef-0123-456789abcdef",),
        ("11111111-1111-1111-1111-111111111111",),
        ("111e1179-01dd-4f7e-9a67-139fa9b2236c",),
        ("12345678-1234-1234-1234-123456789abc",),
        ("abcd1234-ab12-cd34-ef56-abcdef123456",),
        ("cafebab3-cafe-babe-cafe-babecafebabe",),
        ("deadbeef-dead-beef-dead-beefdeadbeef",),
        ("ffffffff-ffff-ffff-ffff-ffffffffffff",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "UUID results do not match expected values!"


def test_hugeint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT hugeint_col FROM duckdb_supported_types_table WHERE hugeint_col IS NOT NULL ORDER BY hugeint_col"
    expected = [
        ("-17014118346046923173168730371588410572",),
        ("-9223372036854775808",),
        ("-2147483648",),
        ("-32768",),
        ("-1",),
        ("0",),
        ("1",),
        ("32767",),
        ("2147483647",),
        ("9223372036854775807",),
        ("17014118346046923173168730371588410572",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(
        query, pg_conn
    )  # Adjust accordingly if comparing with PostgreSQL

    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "HUGEINT results do not match expected values!"


def test_uhugeint(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT uhugeint_col FROM duckdb_supported_types_table WHERE uhugeint_col IS NOT NULL ORDER BY uhugeint_col"
    expected = [
        ("0",),
        ("1",),
        ("65535",),
        ("4294967295",),
        ("18446744073709551615",),
        ("340282366920938463463374607431768211455",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(
        query, pg_conn
    )  # Adjust accordingly if comparing with PostgreSQL

    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "UHUGEINT results do not match expected values!"


from datetime import timedelta
import re


def normalize_interval_to_timedelta(interval):
    """Normalize interval strings or timedelta objects to timedelta for comparison."""
    if isinstance(interval, timedelta):
        # Directly return timedelta objects from DuckDB
        return interval
    elif isinstance(interval, tuple):
        # Convert PostgreSQL interval string to timedelta
        interval_str = interval[0]
        return parse_interval_str_to_timedelta(interval_str)
    elif isinstance(interval, str):
        # Convert expected interval string to timedelta
        return parse_interval_str_to_timedelta(interval)
    else:
        raise ValueError("Unsupported interval format")


def parse_interval_str_to_timedelta(interval_str):
    """Parse interval string to timedelta, handling years, months, and days."""
    # Average lengths
    AVG_DAYS_PER_YEAR = 360
    AVG_DAYS_PER_MONTH = 30

    # Regex to match years, months, and days
    pattern = re.compile(
        r"(?:(\d+)\s*YEARS?\s*)?(?:(\d+)\s*MONTHS?\s*)?(?:(\d+)\s*DAYS?\s*)?",
        re.IGNORECASE,
    )
    match = pattern.match(interval_str)

    # Extract years, months, and days, defaulting to 0 if not found
    years = int(match.group(1)) if match.group(1) else 0
    months = int(match.group(2)) if match.group(2) else 0
    days = int(match.group(3)) if match.group(3) else 0

    # Calculate total days from years, months, and days
    total_days = (years * AVG_DAYS_PER_YEAR) + (months * AVG_DAYS_PER_MONTH) + days

    return timedelta(days=total_days)


def test_interval_types(duckdb_conn, pg_conn, tmp_path):
    query = "SELECT interval_col FROM duckdb_supported_types_table WHERE interval_col IS NOT NULL ORDER BY interval_col"
    expected_intervals = [
        "10 DAYS",
        "2 MONTHS",
        "1 YEAR",
        "16 MONTHS 15 DAYS",
    ]  # Your expected intervals in a simple format

    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Normalize all intervals to timedelta for comparison
    duckdb_intervals_td = [
        normalize_interval_to_timedelta(item[0]) for item in duckdb_results
    ]
    pg_intervals_td = [normalize_interval_to_timedelta(item) for item in pg_results]
    expected_intervals_td = [
        normalize_interval_to_timedelta(interval) for interval in expected_intervals
    ]

    print(duckdb_intervals_td)
    print(pg_intervals_td)
    print(expected_intervals_td)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [
        normalize_interval_to_timedelta(item[0]) for item in transmit_results
    ]

    # Compare timedelta representations for equality
    assert (
        duckdb_intervals_td == expected_intervals_td
    ), "DuckDB interval results do not match expected values!"
    assert (
        pg_intervals_td == expected_intervals_td
    ), "PostgreSQL interval results do not match expected values!"
    assert (
        transmit_results == expected_intervals_td
    ), "Transmit interval results do not match expected values!"


# Ensure this import statement is at the beginning of your script
from datetime import *


def test_date_types(duckdb_conn, pg_conn, tmp_path):
    query = "SELECT date_col FROM duckdb_supported_types_table WHERE date_col IS NOT NULL ORDER BY date_col"
    expected_dates = [
        "0001-01-01",
        "1970-01-01",
        "1992-09-20",
        "9999-12-31",
    ]

    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Normalize DuckDB dates for comparison
    duckdb_dates = [
        item[0].isoformat() if isinstance(item[0], date) else item[0]
        for item in duckdb_results
    ]

    # Normalize PostgreSQL dates for comparison; assuming pg_results need to be adjusted based on actual data format
    pg_dates = [
        item[0].isoformat() if isinstance(item[0], date) else item[0]
        for item in pg_results
    ]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [
        item[0].isoformat() if isinstance(item[0], date) else item[0]
        for item in transmit_results
    ]

    print("DuckDB Results:", duckdb_dates)
    print("Expected:", expected_dates)
    print("PostgreSQL Results:", pg_dates)

    assert (
        duckdb_dates == expected_dates
    ), "DuckDB date results do not match expected values!"
    assert (
        pg_dates == expected_dates
    ), "PostgreSQL date results do not match expected values!"
    assert (
        transmit_results == expected_dates
    ), "Transmit date results do not match expected values!"


def convert_datetime_to_str(results):
    return [(datetime.strftime(item[0], "%Y-%m-%d %H:%M:%S"),) for item in results]


def convert_datetimetz_to_str(results):
    return [(datetime.strftime(item[0], "%Y-%m-%d %H:%M:%S%z"),) for item in results]


def convert_time_to_str(results):
    return [(time.strftime(item[0], "%H:%M:%S"),) for item in results]


def convert_timetz_to_str(results):
    return [(time.strftime(item[0], "%H:%M:%S%z"),) for item in results]


def test_timestamp(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT timestamp_col FROM duckdb_supported_types_table WHERE timestamp_col IS NOT NULL ORDER BY timestamp_col"
    expected = [("2024-02-08 12:00:00",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    duckdb_results_str = convert_datetime_to_str(duckdb_results)
    print(duckdb_results_str)
    print(pg_results)
    print(expected)

    assert (
        duckdb_results_str == pg_results == expected
    ), "Timestamp results do not match expected values!"


def test_timestamp_ms(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT timestamp_ms_col FROM duckdb_supported_types_table WHERE timestamp_ms_col IS NOT NULL ORDER BY timestamp_ms_col"
    expected = [("2024-02-08 12:00:00",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    duckdb_results_str = convert_datetime_to_str(duckdb_results)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "Timestamp_ms results do not match expected values!"


def test_timestamp_ns(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT timestamp_ns_col FROM duckdb_supported_types_table WHERE timestamp_ns_col IS NOT NULL ORDER BY timestamp_ns_col"
    expected = [("2024-02-08 12:00:00",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    duckdb_results_str = convert_datetime_to_str(duckdb_results)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "Timestamp_ns results do not match expected values!"


def test_timestamp_sec(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT timestamp_s_col FROM duckdb_supported_types_table WHERE timestamp_s_col IS NOT NULL ORDER BY timestamp_s_col"
    expected = [("2024-02-08 12:00:00",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    duckdb_results_str = convert_datetime_to_str(duckdb_results)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "Timestamp_sec results do not match expected values!"


def test_timestamp_tz(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT timestamp_tz_col FROM duckdb_supported_types_table WHERE timestamp_tz_col IS NOT NULL ORDER BY timestamp_tz_col"
    expected = [("2024-02-08 09:00:00+00",)]
    pg_results = perform_query_on_cursor(query, pg_conn)

    # We skip getting duckdb_results, because the output
    # is dependent on local system timezone

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "Timestamp_tz results do not match expected values!"


def test_time(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT time_col FROM duckdb_supported_types_table WHERE time_col IS NOT NULL ORDER BY time_col"
    expected = [("12:00:00",)]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    duckdb_results_str = convert_time_to_str(duckdb_results)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        duckdb_results_str == pg_results == transmit_results == expected
    ), "Time results do not match expected values!"


def test_tz_time(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT time_tz_col FROM duckdb_supported_types_table WHERE time_tz_col IS NOT NULL ORDER BY time_tz_col"
    expected = [("12:00:00+03",)]

    # We skip getting duckdb_results, because time_tz handling is not currently
    # implemented in the Python DuckDB module (to be revisited)

    pg_results = perform_query_on_cursor(query, pg_conn)

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "Time_tz results do not match expected values!"


def test_struct(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT struct_col FROM duckdb_supported_types_table WHERE struct_col IS NOT NULL ORDER BY struct_col"
    expected = [
        ("(3,4)",),
        ("(3,)",),
        ("(,4)",),
        ("(,)",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "STRUCT results do not match expected values!"


def test_map(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT map_col FROM duckdb_supported_types_table WHERE map_col IS NOT NULL ORDER BY map_col"
    expected = [
        ('{"(3,three)","(4,four)"}',),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "MAP results do not match expected values!"


def test_enum(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT enum_col FROM duckdb_supported_types_table WHERE enum_col IS NOT NULL ORDER BY enum_col"
    expected = [
        ("sad",),
        ("happy",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "ENUM results do not match expected values!"


def test_array(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT array_col FROM duckdb_supported_types_table WHERE array_col IS NOT NULL ORDER BY array_col"
    expected = [
        ("{1,2,3}",),
        ("{1,NULL,2}",),
        ("{8,9,10}",),
        ("{NULL,4,5}",),
        ("{NULL,7,NULL}",),
        ("{NULL,NULL,NULL}",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "ARRAY results do not match expected values!"


def test_multi_array(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT multi_array_col FROM duckdb_supported_types_table WHERE multi_array_col IS NOT NULL ORDER BY multi_array_col"
    expected = [
        ("{{1,2,3},{4,5,6},{8,9,10}}",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "ARRAY results do not match expected values!"


def test_list(setup_table, duckdb_conn, pg_conn, tmp_path):
    query = "SELECT list_col FROM duckdb_supported_types_table WHERE list_col IS NOT NULL ORDER BY list_col"
    expected = [
        ("{}",),
        ("{1,NULL,NULL}",),
        ("{1,NULL,NULL,1}",),
        ("{5,6}",),
        ("{NULL}",),
        ("{NULL,NULL,1,NULL}",),
        ("{NULL,NULL,NULL}",),
        ("{NULL,NULL,NULL,1}",),
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)

    # Convert each element in duckdb_results to a string
    duckdb_results_str = [(str(item[0]),) for item in duckdb_results]

    # Get normalized transmit results
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)
    transmit_results = [(item[0],) for item in transmit_results]

    assert (
        pg_results == transmit_results == expected
    ), "LIST results do not match expected values!"


def test_nulls(setup_table, duckdb_conn, pg_conn, tmp_path):
    # we prefer * because we want to test NULL values for each data type
    query = """
    SELECT * FROM duckdb_supported_types_table WHERE
        boolean_col IS NULL AND
        blob_col IS NULL AND
        tiny_int_col IS NULL AND
        smallint_col IS NULL AND
        int_col IS NULL AND
        bigint_col IS NULL AND
        utinyint_col IS NULL AND
        usmallint_col IS NULL AND
        uinteger_col IS NULL AND
        uint64_col IS NULL AND
        float4_col IS NULL AND
        float8_col IS NULL AND
        varchar_col IS NULL AND
        decimal_col IS NULL AND
        uuid_col IS NULL AND
        hugeint_col IS NULL AND
        uhugeint_col IS NULL AND
        timestamp_ns_col IS NULL AND
        timestamp_ms_col IS NULL AND
        timestamp_s_col IS NULL AND
        timestamp_col IS NULL AND
        timestamp_tz_col IS NULL AND
        time_col IS NULL AND
        time_tz_col IS NULL AND
        interval_col IS NULL AND
        date_col IS NULL AND
        struct_col IS NULL AND
        list_col IS NULL AND
        array_col IS NULL AND
        multi_array_col IS NULL AND
        map_col IS NULL AND
        enum_col IS NULL
    """
    expected = [
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    ]
    duckdb_results = perform_query_on_cursor(query, duckdb_conn)
    pg_results = perform_query_on_cursor(query, pg_conn)
    transmit_results = perform_transmit_query(query, pg_conn, tmp_path)

    for row in transmit_results:
        assert len(row) == 32
        for value in row:
            assert value == "\\N"

    assert (
        duckdb_results == pg_results == expected
    ), "NULL test results do not match expected values!"


def test_unsupported(setup_table, pg_conn):
    unsupported_types = ["union"]

    for type_name in unsupported_types:
        query = f"SELECT {type_name}_col FROM duckdb_unsupported_types_table WHERE {type_name}_col IS NOT NULL"
        error = run_query(query, pg_conn, raise_error=False)

        # Expect errors for all unsupported types
        assert "Unsupported type" in error


def perform_transmit_query(command, pg_conn, tmp_path):
    csv_path = tmp_path / "data.csv"
    copy_to_file("transmit " + command, csv_path, pg_conn)

    rows = []

    with open(csv_path, newline="") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",", quotechar='"')
        for row in csv_reader:
            rows.append(row)

    return rows


def perform_copy_out_query(command, pg_conn, tmp_path):
    csv_path = tmp_path / "data.csv"
    copy_to_file(
        "copy (" + command + ") to stdout with (format 'csv')", csv_path, pg_conn
    )

    rows = []

    with open(csv_path, newline="") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",", quotechar='"')
        for row in csv_reader:
            rows.append(row)

    return rows


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


@pytest.fixture(scope="module")
def duckdb_conn():
    conn = duckdb.connect(database=":memory:", read_only=False)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pg_conn(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn

    conn.close()


def export_import_database(duckdb_conn, pg_conn):
    # Define the export path
    export_path = "/tmp/duck_types"

    # Clear the existing export directory
    if os.path.exists(export_path):
        shutil.rmtree(export_path)

    # Perform the export using the export_path variable
    duckdb_conn.execute(f"EXPORT DATABASE '{export_path}'")

    # Perform the import using the export_path variable
    with pg_conn.cursor() as cur:
        cur.execute(f"IMPORT DATABASE '{export_path}'")

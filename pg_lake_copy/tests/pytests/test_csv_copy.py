import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_validate_format(pg_conn, duckdb_conn, s3):
    csv_path = f"s3://{TEST_BUCKET}/test_validate_format/data.csv"

    run_command(
        f"""
        CREATE TABLE test_validate_format (x int, y int);
        INSERT INTO test_validate_format VALUES (1,2), (3,4);
        COPY test_validate_format TO '{csv_path}';
        COPY test_validate_format FROM '{csv_path}' WITH (format 'csv', FREEZE true);
    """,
        pg_conn,
    )

    # Check output
    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_validate_format",
        pg_conn,
    )
    assert result[0]["count"] == 4
    assert result[0]["distinct"] == 2

    # Make sure it's actually CSV
    duckdb_conn.execute("SELECT count(*) AS count FROM read_csv($1)", [str(csv_path)])
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 2

    pg_conn.rollback()


def test_types(pg_conn, duckdb_conn, s3):
    csv_path = f"s3://{TEST_BUCKET}/test_types/data.csv"

    run_command(
        """create schema if not exists lake_struct;
    create type lake_struct.custom_type as (x int, y int)""",
        pg_conn,
    )

    # need to add c_array back, but currently the CSV format is incompatible
    # between DuckDB and PostgreSQL
    create_table_command = """
    create table test_types (
    c_bit bit,
    c_bool bool,
    c_bpchar bpchar,
    c_bytea bytea,
    c_char char,
    c_cidr cidr,
    c_custom lake_struct.custom_type,
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
    c_tid tid,
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

    insert_command = """
    insert into test_types values (
    /* c_bit */ 1::bit,
    /* c_bool */ true,
    /* c_bpchar */ 'hello',
    /* c_bytea */ '\\x0001',
    /* c_char */ 'a',
    /* c_cidr */ '192.168.0.0/16'::cidr,
    /* c_custom */ (2,4),
    /* c_date */ '2024-01-01',
    /* c_float4 */ 3.4,
    /* c_float8 */ 33333333.33444444,
    /* c_inet */ '192.168.1.1'::inet,
    /* c_int2 */ 14,
    /* c_int4 */ 100000,
    /* c_int8 */ 10000000000,
    /* c_interval */ '3 days',
    /* c_json */ '{"hello":"world" }',
    /* c_jsonb */ '{"hello":"world" }',
    /* c_money */ '$4.5',
    /* c_name */ 'test',
    /* c_numeric */ 199.123,
    /* c_numeric_large */ 123456789012345678901234.99,
    /* c_numeric_mod */ 99.99,
    /* c_oid */ 11,
    /* c_text */ 'fork',
    /* c_tid */ '(3,4)',
    /* c_time */ '19:34',
    /* c_timestamp */ '2024-01-01 15:00:00',
    /* c_timestamptz */ '2024-01-01 15:00:00 UTC',
    /* c_timetz */ '19:34 UTC',
    /* c_uuid */ 'acd661ca-d18c-42e2-9c4e-61794318935e',
    /* c_varbit */ '0110',
    /* c_varchar */ 'abc'
    );
    """
    run_command(insert_command, pg_conn)

    run_command(
        f"COPY test_types TO '{csv_path}' WITH (format 'csv', header on)", pg_conn
    )

    duckdb_conn.execute("DESCRIBE SELECT * FROM read_csv($1)", [str(csv_path)])

    # get results as dictionary
    result_dict = {}
    for record in duckdb_conn.fetchall():
        result_dict[record[0]] = record[1]

    # reference dictionary (may change if we find a better mapping)
    expected_dict = {
        "c_bit": "BIGINT",
        "c_bool": "BOOLEAN",
        "c_bpchar": "VARCHAR",
        "c_bytea": "VARCHAR",
        "c_char": "VARCHAR",
        "c_cidr": "VARCHAR",
        "c_custom": "VARCHAR",
        "c_date": "DATE",
        "c_float4": "DOUBLE",
        "c_float8": "DOUBLE",
        "c_inet": "VARCHAR",
        "c_int2": "BIGINT",
        "c_int4": "BIGINT",
        "c_int8": "BIGINT",
        "c_interval": "VARCHAR",
        "c_json": "VARCHAR",
        "c_jsonb": "VARCHAR",
        "c_money": "VARCHAR",
        "c_name": "VARCHAR",
        "c_numeric": "DOUBLE",
        "c_numeric_large": "DOUBLE",
        "c_numeric_mod": "DOUBLE",
        "c_oid": "BIGINT",
        "c_text": "VARCHAR",
        "c_tid": "VARCHAR",
        "c_time": "TIME",
        "c_timestamp": "TIMESTAMP",
        "c_timestamptz": "TIMESTAMP WITH TIME ZONE",
        "c_timetz": "VARCHAR",
        "c_uuid": "VARCHAR",
        "c_varbit": "VARCHAR",
        "c_varchar": "VARCHAR",
    }

    assert result_dict == expected_dict

    # Test whether export/import leads to same table, broken down to make it easier to see error cases
    # t_timestamptz is skipped for now because it seems to have a dependency on the local system time zone
    q1 = "SELECT c_bit, c_bool c_bpchar, c_bytea, c_char, c_cidr, c_custom FROM test_types"
    q2 = "SELECT c_date, c_float4, c_float8, c_inet, c_int2, c_int4, c_int8, c_interval FROM test_types"
    q3 = "SELECT c_json, c_jsonb, c_money, c_name, c_numeric, c_numeric_large, c_numeric_mod FROM test_types"
    q4 = "SELECT c_oid, c_text, c_tid, c_time, c_timestamp, c_timetz, c_uuid, c_varbit, c_varchar FROM test_types"

    before1 = run_query(q1, pg_conn)
    before2 = run_query(q2, pg_conn)
    before3 = run_query(q3, pg_conn)
    before4 = run_query(q4, pg_conn)

    run_command(f"CREATE TABLE test_types_after (LIKE test_types)", pg_conn)
    run_command(f"COPY test_types_after FROM '{csv_path}' WITH csv header", pg_conn)

    after1 = run_query(q1, pg_conn)
    after2 = run_query(q2, pg_conn)
    after3 = run_query(q3, pg_conn)
    after4 = run_query(q4, pg_conn)

    assert before1 == after1
    assert before2 == after2
    assert before3 == after3
    assert before4 == after4

    pg_conn.rollback()


def test_null_nan(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_null_nan/data.csv"

    # Write table with null and nan to a JSON file and read it into another table
    run_command(
        f"""
        CREATE TABLE test_null_nan (string text, number float);
        INSERT INTO test_null_nan VALUES (NULL, 'nan'::float);
        COPY test_null_nan TO '{csv_path}' WITH (format 'csv');

        CREATE TABLE test_null_nan_after (like test_null_nan);
        COPY test_null_nan_after FROM '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_null_nan_after", pg_conn)
    assert result[0]["string"] == None
    assert math.isnan(result[0]["number"])

    pg_conn.rollback()


def test_too_many_columns(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_too_many_columns/data.csv"

    # Generate a file with 3 columns
    duckdb_conn.execute(
        f"COPY (SELECT generate_series x, 1 z, 2 y FROM generate_series(1,10)) TO '{csv_path}'"
    )

    # Create a table with 2 columns
    run_command("CREATE TABLE test_2cols (x int, y int)", pg_conn)

    # Try to copy in the data, we get a DuckDB error because told it how many columns we expect
    error = run_command(
        f"COPY test_2cols FROM '{csv_path}' WITH (format 'csv')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  Invalid Input Error")

    pg_conn.rollback()


def test_too_few_columns(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_too_few_columns/data.csv"

    # Generate a file with 1 column
    duckdb_conn.execute(
        f"COPY (SELECT generate_series AS x FROM generate_series(1,10)) TO '{csv_path}'"
    )

    # Try to copy the data into a table with 2 columns, we get a DuckDB error because told it how many columns we expect
    run_command("CREATE TABLE test_2cols (x int, y int)", pg_conn)
    error = run_command(
        f"COPY test_2cols FROM '{csv_path}' WITH (format 'csv', header on)",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  Invalid Input Error")

    pg_conn.rollback()

    # Try to only copy into one column
    run_command("CREATE TABLE test_2cols (x int, y int)", pg_conn)
    run_command(
        f"COPY test_2cols (y) FROM '{csv_path}' WITH (format 'csv', header on)", pg_conn
    )

    # Only y had matching values
    result = run_query("SELECT max(x) mx, max(y) my FROM test_2cols", pg_conn)
    assert result[0]["mx"] == None
    assert result[0]["my"] == 10

    pg_conn.rollback()


def test_invalid_type(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_invalid_type/data.csv"

    # Generate a file with a text field
    duckdb_conn.execute(
        f"COPY (SELECT 'hello' AS x FROM generate_series(1,10)) TO '{csv_path}'"
    )

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Try to copy text into the table
    error = run_command(
        f"COPY test_int FROM '{csv_path}' WITH (format 'csv')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  invalid input syntax for type integer")

    pg_conn.rollback()


def test_null_padding(pg_conn, duckdb_conn, s3):
    csv_path = f"s3://{TEST_BUCKET}/test_null_padding/data.csv"

    # Generate a CSV file with rows having fewer columns than expected
    # First row has 3 columns, second row has only 1 column
    csv_content = "1,2,3\n4\n"
    s3.put_object(
        Bucket=TEST_BUCKET, Key="test_null_padding/data.csv", Body=csv_content
    )

    # Create a table with 3 columns
    run_command("CREATE TABLE test_null_padding (a int, b int, c int)", pg_conn)

    # Copy with null_padding enabled to handle rows with fewer columns
    run_command(
        f"COPY test_null_padding FROM '{csv_path}' WITH (format 'csv', null_padding true)",
        pg_conn,
    )

    # Verify data is correctly loaded with NULL padding
    result = run_query("SELECT a, b, c FROM test_null_padding ORDER BY a", pg_conn)
    assert len(result) == 2
    assert result[0]["a"] == 1
    assert result[0]["b"] == 2
    assert result[0]["c"] == 3
    assert result[1]["a"] == 4
    assert result[1]["b"] == None
    assert result[1]["c"] == None

    # null_padding has no meaning for COPY TO
    error = run_command(
        f"COPY (SELECT 'abc' t, 1 as id) TO '{csv_path}' WITH (format 'csv', null_padding true)",
        pg_conn,
        raise_error=False,
    )
    assert 'invalid option "null_padding"' in error

    pg_conn.rollback()


def test_partially_invalid_type(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_partially_invalid_type/data.csv"

    # Generate a file with a small number (can be int) and a large number (must be bigint)
    duckdb_conn.execute(
        f"COPY (SELECT col0 AS x FROM (VALUES(1), (5000000000))) TO '{csv_path}' WITH (header false)"
    )

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Try to copy text into the table
    error = run_command(
        f"COPY test_int FROM '{csv_path}' with csv", pg_conn, raise_error=False
    )
    assert error.startswith(
        'ERROR:  value "5000000000" is out of range for type integer'
    )

    pg_conn.rollback()


def test_invalid_option(pg_conn, duckdb_conn, tmp_path):
    csv_path = f"s3://{TEST_BUCKET}/test_invalid_option/data.csv"

    # Create a simple CSV file and table
    duckdb_conn.execute(f"COPY (SELECT * FROM (VALUES(1), (2))) TO '{csv_path}'")
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for CSV in COPY FROM
    error = run_command(
        f"COPY test_int FROM '{csv_path}' WITH (format 'csv', boat '|')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "boat" for COPY FROM with csv format'
    )

    pg_conn.rollback()

    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for CSV in COPY TO
    error = run_command(
        f"COPY test_int TO '{csv_path}' WITH (format 'csv', boat '|')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "boat" for COPY TO with csv format'
    )

    pg_conn.rollback()

    # Use an option that's normally valid, but not in pg_lake_copy
    error = run_command(
        f"COPY (SELECT 'reeën') TO '{csv_path}' WITH (format 'csv', encoding 'LATIN1')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "encoding" for COPY TO with csv format'
    )

    pg_conn.rollback()


def test_invalid_format(pg_conn):
    # No recognized file extension
    csv_path = f"s3://{TEST_BUCKET}/test_invalid_format/datacsv"

    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for CSV in COPY FROM
    error = run_command(f"COPY test_int TO '{csv_path}'", pg_conn, raise_error=False)
    assert error.startswith("ERROR:  pg_lake_copy: unrecognized file format")

    pg_conn.rollback()


def test_compression(pg_conn, duckdb_conn, s3):
    run_command(
        """
        CREATE TABLE test_compressed (x int);
        INSERT INTO test_compressed SELECT s FROM generate_series(1,1000) s
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Write an uncompressed file
    uncompressed_key = "test_compression/uncompressed.csv"
    uncompressed_path = f"s3://{TEST_BUCKET}/{uncompressed_key}"
    run_command(
        f"COPY test_compressed TO '{uncompressed_path}' WITH (format 'csv', compression 'none')",
        pg_conn,
    )

    # Write a zstd-compressed file (inferred from extension)
    compressed_key = "test_compression/compressed.csv.zst"
    compressed_path = f"s3://{TEST_BUCKET}/{compressed_key}"
    run_command(f"COPY test_compressed TO '{compressed_path}'", pg_conn)

    assert get_object_size(s3, TEST_BUCKET, compressed_key) < get_object_size(
        s3, TEST_BUCKET, uncompressed_key
    )

    # Try to read it
    run_command(
        f"COPY test_compressed FROM '{compressed_path}' WITH (format 'csv', compression 'zstd')",
        pg_conn,
    )

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    pg_conn.rollback()

    # Reading using wrong compression
    error = run_command(
        f"COPY test_compressed FROM '{compressed_path}' WITH (format 'csv', compression 'gzip')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  IO Error: Input is not a GZIP stream")

    pg_conn.rollback()

    # Write gzip (with irregular extension)
    compressed_key = "test_compression/compressed.zz"
    compressed_path = f"s3://{TEST_BUCKET}/{compressed_key}"
    run_command(
        f"COPY test_compressed TO '{compressed_path}' WITH (format 'csv', compression 'gzip')",
        pg_conn,
    )

    assert get_object_size(s3, TEST_BUCKET, compressed_key) < get_object_size(
        s3, TEST_BUCKET, uncompressed_key
    )

    # Try to read it as gzip
    run_command(
        f"COPY test_compressed FROM '{compressed_path}' WITH (format 'csv', compression 'gzip')",
        pg_conn,
    )

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    # Try to read gzip without specifying compression
    error = run_command(
        f"COPY test_compressed FROM '{compressed_path}' WITH (format 'csv')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  Invalid Input Error: CSV Error ")

    pg_conn.rollback()

    # Try to write snappy
    compressed_key = "test_compression/compressed.csv.ohsnap"
    compressed_path = f"s3://{TEST_BUCKET}/{compressed_key}"
    error = run_command(
        f"COPY test_compressed TO '{compressed_path}' WITH (format 'csv', compression 'snappy')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: snappy compression is not supported for CSV format"
    )

    pg_conn.rollback()

    drop_table_command = "DROP TABLE test_compressed"
    run_command(drop_table_command, pg_conn)
    pg_conn.commit()


def test_invalid_compression(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_invalid_compression/data.csv"

    run_command(
        """
        CREATE TABLE test_compressed (x int);
    """,
        pg_conn,
    )

    # Write with unrecognized compression
    error = run_command(
        f"COPY test_compressed TO '{csv_path}' WITH (format 'csv', compression 'zoko')",
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  pg_lake_copy: compression "zoko" not recognized')

    pg_conn.rollback()


def test_copy_large_row(pg_conn, duckdb_conn, tmp_path):
    csv_path = f"s3://{TEST_BUCKET}/test_copy_large_row/data.csv"

    # Write a table with rows of several MB to a JSON file
    run_command(
        f"""
        CREATE TABLE test_large_row (large1 text, large2 text);
        INSERT INTO test_large_row SELECT repeat('A', 20000000) FROM generate_series(1,3);
        COPY test_large_row TO '{csv_path}' WITH (format 'csv', header on);
    """,
        pg_conn,
    )

    # Check length of the rows
    duckdb_conn.execute(
        "SELECT min(length(large1)) AS l1 FROM read_csv($1, maximum_line_size = 30000000, parallel=False)",
        [str(csv_path)],
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 20000000

    # Read the CSV file into another table, fails because we don't specify maximum_line_size internally
    error = run_command(
        f"""
        CREATE TABLE test_large_row_after (like test_large_row);
        COPY test_large_row_after FROM '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  Invalid Input Error: CSV Error on Line: 2")

    pg_conn.rollback()


def test_column_subset(pg_conn, duckdb_conn, tmp_path):
    csv_path = f"s3://{TEST_BUCKET}/test_column_subset/data.csv"

    # Create a table and emit a subset of columns into JSON
    run_command(
        f"""
        CREATE TABLE test_column_subset (
           val1 int,
           d date,
           val2 int,
           "gre@t" text,
           val3 bigint
        );
        INSERT INTO test_column_subset VALUES (1,'2020-01-01',3,'hello', 5);
        INSERT INTO test_column_subset VALUES (2,'2021-01-01',4,'hello', 6);
        INSERT INTO test_column_subset VALUES (3,NULL,6,'world', 9);
        ALTER TABLE test_column_subset DROP COLUMN val2;
        COPY test_column_subset (val3, "gre@t", d) TO '{csv_path}' WITH (format 'csv');

        CREATE TABLE test_column_subset_after (val2 int, val3 bigint, "gre@t" text, d date);
        COPY test_column_subset_after (val3, "gre@t" , d) FROM '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
    )

    # Check output, we have 2 unique rows where d is not null
    result = run_query(
        """
        SELECT count(*) AS count FROM (
            SELECT val3, "gre@t", d FROM test_column_subset WHERE d IS NOT NULL
            UNION
            SELECT val3, "gre@t", d FROM test_column_subset_after WHERE d IS NOT NULL
        ) u;
    """,
        pg_conn,
    )
    assert result[0]["count"] == 2

    pg_conn.rollback()


def test_json_in_csv(pg_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_json_in_csv/data.csv"

    # Create a table with JSON columns and write them to a file
    run_command(
        """
        CREATE TABLE test_csv_columns (
           key text,
           value json,
           valueb jsonb
        );
        INSERT INTO test_csv_columns VALUES ('item-1', '{"hello":"world"}', '[3,4]');
        INSERT INTO test_csv_columns VALUES ('item-2', NULL, '{"array":[]}');
    """,
        pg_conn,
    )

    run_command(
        f"""
        COPY (SELECT * FROM test_csv_columns) TO '{csv_path}';

        CREATE TABLE test_csv_columns_after (
           key text,
           value json,
           valueb jsonb
        );
        COPY test_csv_columns_after FROM '{csv_path}';
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_csv_columns_after", pg_conn)
    assert result[0]["value"] == {"hello": "world"}
    assert result[0]["valueb"] == [3, 4]
    assert result[1]["value"] == None
    assert result[1]["valueb"] == {"array": []}

    pg_conn.rollback()


def test_corrupt_csv(pg_conn, s3, tmp_path):
    csv_key = "test_corrupt_csv/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Missing characters in CSV
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write("1,4\0" + "5,3")

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    run_command(
        f"""
        CREATE TABLE test_corrupt_csv (x int, y int, z int);
        COPY test_corrupt_csv FROM '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
    )

    # we treat 0 character as end-of-string, so we only see 4
    result = run_query("SELECT * FROM test_corrupt_csv", pg_conn)
    assert result[0]["y"] == 4

    pg_conn.rollback()


def test_empty_csv(pg_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_empty_csv/data.csv"

    # Write empty table to CSV file works fine
    run_command(
        f"""
        CREATE TABLE test_empty_csv (x int, y int);
        COPY test_empty_csv TO '{csv_path}';
        COPY test_empty_csv FROM '{csv_path}';
    """,
        pg_conn,
        raise_error=True,
    )

    result = run_query("SELECT * FROM test_empty_csv", pg_conn)
    assert len(result) == 0

    pg_conn.rollback()

    # Try again with header
    run_command(
        f"""
        CREATE TABLE test_empty_csv (x int, y int);
        COPY test_empty_csv TO '{csv_path}' with header;
        COPY test_empty_csv FROM '{csv_path}' with header;
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_empty_csv", pg_conn)
    assert len(result) == 0

    pg_conn.rollback()


def test_empty_lines_csv(pg_conn, s3, tmp_path):
    csv_key = "test_empty_lines_csv/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Missing characters in CSV
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write("\n\n")

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    # For a single column table, 2 empty lines can reasonably be interpreted as NULLs
    run_command(
        f"""
        CREATE TABLE test_empty_lines_1col (value int);
        COPY test_empty_lines_1col FROM '{csv_path}';
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_empty_lines_1col", pg_conn)
    assert len(result) == 2
    assert result[0][0] == None

    # For a two column table, 2 empty lines is nothingness
    run_command(
        f"""
        CREATE TABLE test_empty_lines_2cols (value int, value2 int);
        COPY test_empty_lines_2cols FROM '{csv_path}';
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_empty_lines_2cols", pg_conn)
    assert len(result) == 0

    pg_conn.rollback()


def test_escaped_csv(pg_conn, s3, tmp_path):
    csv_key = "test_escaped_csv/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Funky escape sequences
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        # The backslashes here are doubly escaped, once for Python, once for CSV
        csv_file.write('"a,""b","before\n\tafter"')

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    run_command(
        f"""
        CREATE TABLE test_escaped_csv (x text, y text);
        COPY test_escaped_csv FROM '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
    )

    result = run_query("SELECT x, y FROM test_escaped_csv", pg_conn)
    assert len(result) == 1
    assert result[0]["x"] == 'a,"b'
    assert result[0]["y"] == "before\n\tafter"

    pg_conn.rollback()


def test_unicode(pg_conn, s3, tmp_path):
    csv_key = "test_unicode/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Some unicode characters
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write('"\u0100🙂😉👍",""\n')

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    run_command(
        f"""
        CREATE TABLE test_unicode_csv (x text, y text);
        COPY test_unicode_csv FROM '{csv_path}' WITH (format 'csv', header false, null '\\N');
    """,
        pg_conn,
    )

    result = run_query("SELECT x, y FROM test_unicode_csv", pg_conn)
    assert len(result) == 1
    assert result[0]["x"] == "\u0100🙂😉👍"
    assert result[0]["y"] == ""

    pg_conn.rollback()


def test_char_0(pg_conn, s3, tmp_path):
    csv_key = "test_unicode_0/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Invalid unicode character
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write('"\0abc",""\n')

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    run_command(
        f"""
        CREATE TABLE test_unicode_csv (x text, y text);
        COPY test_unicode_csv FROM '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
    )

    # we treat 0 character as end-of-string
    result = run_query("SELECT * FROM test_unicode_csv", pg_conn)
    assert result[0]["x"] == ""

    pg_conn.rollback()


def test_duplicate_columns(pg_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_duplicate_column/data.csv"

    # Test 2 identical column names
    error = run_command(
        f"""
        CREATE TABLE test_duplicate_columns (a int);
        INSERT INTO test_duplicate_columns VALUES (1);
        COPY (SELECT a, a FROM test_duplicate_columns) TO '{csv_path}' WITH (FORMAT 'csv');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  pg_lake_copy: column "a" specified more than once')

    pg_conn.rollback()

    # Test lowercase + uppercase
    error = run_command(
        f"""
        CREATE TABLE test_duplicate_columns (a int);
        INSERT INTO test_duplicate_columns VALUES (1);
        COPY (SELECT a, a as "A" FROM test_duplicate_columns) TO '{csv_path}' WITH (FORMAT 'csv');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  pg_lake_copy: column "a" specified more than once')

    pg_conn.rollback()


def test_copy_back_and_forth(pg_conn, duckdb_conn):
    csv_path = f"s3://{TEST_BUCKET}/test_copy_back_and_forth/data.csv"

    run_command(
        f"""
        CREATE TABLE test_copy_back_and_forth (a int, b text);
        INSERT INTO test_copy_back_and_forth SELECT i, i::text FROM generate_series(1,100) i;

        COPY test_copy_back_and_forth(b) TO  '{csv_path}' WITH (format 'csv');
        COPY test_copy_back_and_forth(b) FROM  '{csv_path}' WITH (format 'csv');
        COPY test_copy_back_and_forth(a) TO  '{csv_path}' WITH (format 'csv');
    """,
        pg_conn,
    )

    # Check number of rows (2*100)
    duckdb_conn.execute("SELECT count(*) AS count FROM read_csv($1)", [str(csv_path)])
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 200


def test_copy_dropped_column(pg_conn, duckdb_conn, azure):
    csv_path = f"az://{TEST_BUCKET}/test_dropped_column/data.csv"

    run_command(
        f"""
       CREATE TABLE test_copy_dropped_column (a int, b int);
       INSERT INTO test_copy_dropped_column SELECT i FROM generate_series(1, 100)i;

       COPY test_copy_dropped_column TO '{csv_path}' WITH (format 'csv', header on);
       ALTER TABLE test_copy_dropped_column DROP COLUMN a;
       COPY test_copy_dropped_column TO '{csv_path}' WITH (format 'csv', header on);
    """,
        pg_conn,
    )

    # Check that there is 1 column named b
    duckdb_conn.execute("DESCRIBE SELECT * FROM read_csv_auto($1)", [str(csv_path)])
    duckdb_result = duckdb_conn.fetchall()
    assert len(duckdb_result) == 1
    assert duckdb_result[0][0] == "b"


def test_copy_no_column(pg_conn, duckdb_conn, azure):
    csv_key = "test_no_column/data.csv"
    csv_path = f"az://{TEST_BUCKET}/{csv_key}"

    run_command(
        f"""
       CREATE TABLE test_copy_no_column (a int);
       INSERT INTO test_copy_no_column SELECT i FROM generate_series(1, 100)i;

       ALTER TABLE test_copy_no_column DROP COLUMN a;
       COPY test_copy_no_column TO '{csv_path}' WITH (format 'csv', header);
       COPY test_copy_no_column FROM '{csv_path}' WITH (format 'csv', header);
    """,
        pg_conn,
    )

    # A 0-column file currently ends up being written as empty
    result = run_query("SELECT count(*) FROM test_copy_no_column", pg_conn)
    assert result[0]["count"] == 100


def test_options(pg_conn, duckdb_conn, s3, tmp_path):
    csv_key = "test_options/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    # Use weird syntax
    local_csv_path = tmp_path / "data.csv"
    with open(local_csv_path, "w") as csv_file:
        csv_file.write("abc,def\n")
        csv_file.write("99$.99\\.9.\n")
        csv_file.write("0$0.0\n")

    s3.upload_file(local_csv_path, TEST_BUCKET, csv_key)

    run_command(
        f"""
       CREATE TABLE test_options (abc int, def float);
       COPY test_options FROM '{csv_path}' WITH (format 'csv', delimiter '$', quote '.', escape '\\', null '0', header match);
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_options ORDER BY 1", pg_conn)
    assert len(result) == 2
    assert result[0] == [99, 99.9]
    assert result[1] == [None, 0]

    pg_conn.rollback()


def test_force_quote(pg_conn, duckdb_conn, s3, tmp_path):
    csv_key = "test_force_quote/data.csv"
    csv_path = f"s3://{TEST_BUCKET}/{csv_key}"

    run_command(
        f"""
       COPY (SELECT s a, 'hello' b, s + 1 as c FROM generate_series(1,10) s)
       TO '{csv_path}' WITH (force_quote *, header true);
    """,
        pg_conn,
    )

    local_csv_path = tmp_path / "data.csv"
    s3.download_file(TEST_BUCKET, csv_key, local_csv_path)

    with open(local_csv_path, "r") as csv_file:
        lines = csv_file.readlines()
        assert lines[0] == '"a","b","c"\n'
        assert lines[1] == '"1","hello","2"\n'
        assert lines[2] == '"2","hello","3"\n'

    run_command(
        f"""
       COPY (SELECT s a, 'hello' b, s + 1 as c FROM generate_series(1,10) s)
       TO '{csv_path}' WITH (force_quote (c, a), header true);
    """,
        pg_conn,
    )

    s3.download_file(TEST_BUCKET, csv_key, local_csv_path)

    with open(local_csv_path, "r") as csv_file:
        lines = csv_file.readlines()
        assert lines[0] == '"a",b,"c"\n'
        assert lines[1] == '"1",hello,"2"\n'
        assert lines[2] == '"2",hello,"3"\n'

    pg_conn.rollback()


def test_auto_detect(pg_conn, azure):
    csv_key = "test_auto_detect/data.csv"
    csv_path = f"az://{TEST_BUCKET}/{csv_key}"

    run_command(
        """
       CREATE TABLE test_auto_detect ("1" int, b text, c jsonb, d timestamp, e varchar, f float);
       INSERT INTO test_auto_detect  SELECT i, 'b', '{"hello":"word"}', '2020-01-01 00:40:00', '3''55', 3.14 FROM generate_series(1, 10)i;
    """,
        pg_conn,
    )

    run_command(
        f"""
       COPY test_auto_detect TO '{csv_path}' WITH (format 'csv', header, delimiter ';', quote '"', escape '\\');

       CREATE TABLE test_auto_detect_after (LIKE test_auto_detect);
       COPY test_auto_detect_after FROM '{csv_path}' WITH (format 'csv', auto_detect);
    """,
        pg_conn,
    )

    result_before = run_query("SELECT * FROM test_auto_detect ORDER BY 1", pg_conn)
    result_after = run_query("SELECT * FROM test_auto_detect_after ORDER BY 1", pg_conn)
    assert result_before == result_after

    pg_conn.rollback()

    run_command(
        f"""
       CREATE TABLE test_auto_detect ("5" int, "a..." bytea, c varchar, d text);
       INSERT INTO test_auto_detect  SELECT 0, '\\x000000', '3"55', i FROM generate_series(1, 10)i;
       COPY test_auto_detect TO '{csv_path}' WITH (format 'csv', header, delimiter '|', quote '''', escape '\\');

       CREATE TABLE test_auto_detect_after (LIKE test_auto_detect);
       COPY test_auto_detect_after FROM '{csv_path}' WITH (format 'csv', auto_detect);
    """,
        pg_conn,
    )

    result_before = run_query("SELECT * FROM test_auto_detect ORDER BY d", pg_conn)
    result_after = run_query("SELECT * FROM test_auto_detect_after ORDER BY d", pg_conn)
    assert result_before == result_after

    pg_conn.rollback()

    run_command(
        f"""
       CREATE TABLE test_auto_detect ("5" text, nul text);
       INSERT INTO test_auto_detect  SELECT '', NULL FROM generate_series(1, 10)i;
       COPY test_auto_detect TO '{csv_path}' WITH (format 'csv');

       CREATE TABLE test_auto_detect_after (LIKE test_auto_detect);
       COPY test_auto_detect_after FROM '{csv_path}' WITH (format 'csv', auto_detect);
    """,
        pg_conn,
    )

    result_before = run_query("SELECT * FROM test_auto_detect ORDER BY 1", pg_conn)
    result_after = run_query("SELECT * FROM test_auto_detect_after ORDER BY 1", pg_conn)

    # Currently disabled, we do not properly distinguish empty string and NULL with auto_detect
    # assert result_before == result_after

    pg_conn.rollback()

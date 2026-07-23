import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_types(pg_conn, duckdb_conn, superuser_conn, tmp_path, app_user):
    parquet_path = tmp_path / "test_types.parquet"

    create_test_types(superuser_conn, app_user)

    copy_command = "COPY test_types TO STDOUT WITH (format 'parquet')"
    copy_to_file(copy_command, parquet_path, pg_conn)

    duckdb_conn.execute("DESCRIBE SELECT * FROM read_parquet($1)", [str(parquet_path)])

    # get results as dictionary
    result_dict = {}
    for record in duckdb_conn.fetchall():
        result_dict[record[0]] = record[1]

    # reference dictionary (may change if we find a better mapping)
    expected_dict = {
        "c_array": "INTEGER[]",
        "c_bit": "VARCHAR",
        "c_bool": "BOOLEAN",
        "c_bpchar": "VARCHAR",
        "c_bytea": "BLOB",
        "c_char": "VARCHAR",
        "c_cidr": "VARCHAR",
        "c_custom": "STRUCT(x INTEGER, y INTEGER)",
        "c_date": "DATE",
        "c_float4": "FLOAT",
        "c_float8": "DOUBLE",
        "c_inet": "VARCHAR",
        "c_int2": "SMALLINT",
        "c_int4": "INTEGER",
        "c_int8": "BIGINT",
        "c_interval": "INTERVAL",
        "c_json": "JSON",
        "c_jsonb": "JSON",
        "c_map": "MAP(INTEGER, VARCHAR)",
        "c_money": "VARCHAR",
        "c_name": "VARCHAR",
        "c_numeric": "DECIMAL(38,9)",
        "c_numeric_large": "VARCHAR",
        "c_numeric_mod": "DECIMAL(4,2)",
        "c_oid": "BIGINT",
        "c_text": "VARCHAR",
        "c_tid": "VARCHAR",
        "c_time": "TIME",
        "c_timestamp": "TIMESTAMP",
        "c_timestamptz": "TIMESTAMP WITH TIME ZONE",
        "c_timetz": "TIME WITH TIME ZONE",
        "c_uuid": "UUID",
        "c_varbit": "VARCHAR",
        "c_varchar": "VARCHAR",
    }

    assert result_dict == expected_dict

    # Test whether export/import leads to same table, broken down to make it easier to see error cases
    # t_timestamptz is skipped for now because it seems to have a dependency on the local system time zone
    q1 = "SELECT c_array, c_bit, c_bool c_bpchar, c_bytea, c_char, c_cidr, c_custom FROM test_types"
    q2 = "SELECT c_date, c_float4, c_float8, c_inet, c_int2, c_int4, c_int8, c_interval FROM test_types"
    q3 = "SELECT c_json, c_jsonb, c_map, c_money, c_name, c_numeric, c_numeric_large, c_numeric_mod FROM test_types"
    q4 = "SELECT c_oid, c_text, c_tid, c_time, c_timestamp, c_timetz, c_uuid, c_varbit, c_varchar FROM test_types"

    before1 = run_query(q1, pg_conn)
    before2 = run_query(q2, pg_conn)
    before3 = run_query(q3, pg_conn)
    before4 = run_query(q4, pg_conn)

    run_command(f"CREATE TABLE test_types_after (LIKE test_types)", pg_conn)
    run_command(
        f"COPY test_types_after FROM '{parquet_path}' WITH (format 'parquet')", pg_conn
    )

    after1 = run_query(q1, pg_conn)
    after2 = run_query(q2, pg_conn)
    after3 = run_query(q3, pg_conn)
    after4 = run_query(q4, pg_conn)

    assert before1 == after1
    assert before2 == after2
    assert before3 == after3
    assert before4 == after4

    pg_conn.rollback()


def test_null_nan(pg_conn, duckdb_conn, tmp_path):
    parquet_path = tmp_path / "test.parquet"

    # Write table with null and nan to a Parquet file and read it into another table
    run_command(
        f"""
        CREATE TABLE test_null_nan (string text, number float);
        INSERT INTO test_null_nan VALUES (NULL, 'nan'::float);
        COPY test_null_nan TO '{parquet_path}' WITH (format 'parquet');

        CREATE TABLE test_null_nan_after (like test_null_nan);
        COPY test_null_nan_after FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_null_nan_after", pg_conn)
    assert result[0]["string"] == None
    assert math.isnan(result[0]["number"])

    pg_conn.rollback()


@pytest.mark.parametrize("copy_format", ["parquet", "json"])
def test_literal_backslash_n(pg_conn, duckdb_conn, tmp_path, copy_format):
    out_path = tmp_path / f"test.{copy_format}"

    # The internal CSV exchange uses \N as the null sentinel, so a text value
    # that happens to equal the 2-char string "\N" must not be collapsed to
    # SQL NULL on the way back out.  The bug lived in ConvertCSVFileTo(), the
    # shared tail every pg_lake COPY destination flows through, upstream of the
    # destination serialization, so exercise more than one format.  (Only the
    # formats pg_lake handles for a local file are covered: local CSV COPY is
    # left to PostgreSQL and never reaches ConvertCSVFileTo.)  The bytea sibling
    # carries the same bytes and the 4-char control "\N\N" round-trips
    # regardless; the plain "\N" is the value that regressed.
    run_command(
        f"""
        CREATE TABLE test_backslash_n (id int, t text, b bytea);
        INSERT INTO test_backslash_n VALUES
            (1, NULL,                               NULL),
            (2, chr(92)||chr(78),                   '\\x5C4E'::bytea),
            (3, '',                                 ''::bytea),
            (4, chr(92)||chr(78)||chr(92)||chr(78), '\\x5C4E5C4E'::bytea);
        COPY test_backslash_n TO '{out_path}' WITH (format '{copy_format}');

        CREATE TABLE test_backslash_n_after (like test_backslash_n);
        COPY test_backslash_n_after FROM '{out_path}' WITH (format '{copy_format}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT id, t, b FROM test_backslash_n_after ORDER BY id", pg_conn
    )

    # id 1: genuine SQL NULL stays NULL
    assert result[0]["t"] is None
    assert result[0]["b"] is None
    # id 2: literal "\N" survives as a 2-char string, not NULL
    assert result[1]["t"] == "\\N"
    assert result[1]["b"].tobytes() == b"\x5c\x4e"
    # id 3: empty string stays a non-null empty string
    assert result[2]["t"] == ""
    # id 4: control value round-trips
    assert result[3]["t"] == "\\N\\N"

    pg_conn.rollback()


def test_too_many_columns(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with 3 columns
    parquet_path = tmp_path / "test.parquet"
    duckdb_conn.execute(
        f"COPY (SELECT generate_series, 1, 2 FROM generate_series(1,10)) TO '{parquet_path}'"
    )

    # Create a table with 2 columns
    run_command("CREATE TABLE test_2cols (x int, y int)", pg_conn)

    # Try to copy in the data
    copy_command = "COPY test_2cols FROM STDIN WITH (format 'parquet')"
    copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)

    # Check that we used the first 2 columns
    result = run_query("SELECT * FROM test_2cols", pg_conn)
    assert result[0]["x"] == 1
    assert result[0]["y"] == 1

    pg_conn.rollback()


def test_too_few_columns(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with 1 column
    parquet_path = tmp_path / "test.parquet"
    duckdb_conn.execute(
        f"COPY (SELECT generate_series FROM generate_series(1,10)) TO '{parquet_path}'"
    )

    # Create a table with 2 columns
    create_table_command = "CREATE TABLE test_2cols (x int, y int)"
    run_command(create_table_command, pg_conn)

    pg_conn.commit()

    # Try to copy into all columns
    copy_command = "COPY test_2cols FROM STDIN WITH (format 'parquet')"
    error = copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)
    assert 'Referenced column "y" not found in FROM' in error

    pg_conn.rollback()

    # Try to only copy into the first column
    copy_command = "COPY test_2cols (x) FROM STDIN WITH (format 'parquet')"
    copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)

    drop_table_command = "DROP TABLE test_2cols"
    run_command(drop_table_command, pg_conn)
    pg_conn.commit()


def test_copy_to_bytea(pg_conn, tmp_path):
    parquet_path = tmp_path / "test.parquet"

    # Write bytea to Parquet as text, confirm that we preserve the PostgreSQL text format
    run_command(
        f"""
        COPY (SELECT '\\xdeadbeef'::bytea as bits) TO '{parquet_path}' WITH (format 'parquet');

        CREATE TABLE test_bytea_after (bits bytea);
        COPY test_bytea_after FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    result = run_query("SELECT bits FROM test_bytea_after", pg_conn)
    assert bytes(result[0]["bits"]) == bytes.fromhex("deadbeef")

    pg_conn.rollback()


def test_copy_from_bytea(pg_conn, duckdb_conn, tmp_path):
    parquet_path = tmp_path / "test.parquet"

    # Write bytea to Parquet in binary format using DuckDB
    duckdb_conn.execute(
        f"COPY (SELECT '\\xde\\xad\\xbe\\xef'::bytea as bits) TO '{parquet_path}'"
    )

    # Confirm that we can interpret the binary format
    run_command(
        f"""
        CREATE TABLE test_bytea_after (bits bytea);
        COPY test_bytea_after FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    result = run_query("SELECT bits::text FROM test_bytea_after", pg_conn)
    assert result[0]["bits"] == "\\xdeadbeef"

    pg_conn.rollback()


def test_invalid_type(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with a text field
    parquet_path = tmp_path / "test.parquet"
    duckdb_conn.execute(
        f"COPY (SELECT 'hello' FROM generate_series(1,10)) TO '{parquet_path}'"
    )

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Try to copy text into the table
    copy_command = "COPY test_int FROM STDIN WITH (format 'parquet')"
    error = copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)
    assert error.startswith("ERROR:  invalid input syntax for type integer")

    pg_conn.rollback()


def test_partially_invalid_type(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with a small number (can be int) and a large number (must be bigint)
    parquet_path = tmp_path / "test.parquet"
    duckdb_conn.execute(
        f"COPY (SELECT * FROM (VALUES(1), (5000000000))) TO '{parquet_path}'"
    )

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Try to copy text into the table
    copy_command = "COPY test_int FROM STDIN WITH (format 'parquet')"
    error = copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)
    assert error.startswith(
        'ERROR:  value "5000000000" is out of range for type integer'
    )

    pg_conn.rollback()


def test_invalid_option(pg_conn, duckdb_conn, tmp_path):
    # Create a simple Parquet file
    parquet_path = tmp_path / "test.parquet"
    duckdb_conn.execute(f"COPY (SELECT * FROM (VALUES(1), (2))) TO '{parquet_path}'")

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for Parquet in COPY FROM
    copy_command = "COPY test_int FROM STDIN WITH (format 'parquet', quote '|')"
    error = copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "quote" for COPY FROM with parquet format'
    )

    pg_conn.rollback()

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for Parquet in COPY TO
    copy_command = "COPY test_int TO STDIN WITH (format 'parquet', quote '|')"
    error = copy_from_file(copy_command, parquet_path, pg_conn, raise_error=False)
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "quote" for COPY TO with parquet format'
    )

    pg_conn.rollback()


def test_compression(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """
        CREATE TABLE test_compressed (x int);
        INSERT INTO test_compressed SELECT s FROM generate_series(1,1000) s
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Write an uncompressed file
    uncompressed_path = tmp_path / "test.parquet"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'parquet', compression 'none')"
    )
    copy_to_file(copy_command, uncompressed_path, pg_conn)

    # Write an compressed file
    compressed_path = tmp_path / "test.parquet.zst"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'parquet', compression 'zstd')"
    )
    copy_to_file(copy_command, compressed_path, pg_conn)

    assert os.path.getsize(compressed_path) < os.path.getsize(uncompressed_path)

    # Try to read it
    copy_command = (
        "COPY test_compressed FROM STDIN WITH (format 'parquet', compression 'zstd')"
    )
    copy_from_file(copy_command, compressed_path, pg_conn)

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    pg_conn.rollback()

    # Reading using wrong compression is actually ok, because we get it from the Parquet
    copy_command = (
        "COPY test_compressed FROM STDIN WITH (format 'parquet', compression 'snappy')"
    )
    error = copy_from_file(copy_command, compressed_path, pg_conn)

    pg_conn.rollback()

    # Write gzip
    compressed_path = tmp_path / "test.parquet.gz"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'parquet', compression 'gzip')"
    )
    copy_to_file(copy_command, compressed_path, pg_conn)

    assert os.path.getsize(compressed_path) < os.path.getsize(uncompressed_path)

    # Try to read it
    copy_command = "COPY test_compressed FROM STDIN WITH (format 'parquet')"
    copy_from_file(copy_command, compressed_path, pg_conn)

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    pg_conn.rollback()

    # Write snappy
    compressed_path = tmp_path / "test.parquet.snappy"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'parquet', compression 'snappy')"
    )
    copy_to_file(copy_command, compressed_path, pg_conn)

    # snappy is not actually smaller :(
    # assert os.path.getsize(compressed_path) < os.path.getsize(uncompressed_path)

    # Try to read it
    copy_command = "COPY test_compressed FROM STDIN WITH (format 'parquet')"
    copy_from_file(copy_command, compressed_path, pg_conn)

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    pg_conn.rollback()

    # Write default (snappy)
    compressed_path = tmp_path / "test.parquet.default"
    copy_command = "COPY test_compressed TO STDOUT WITH (format 'parquet')"
    copy_to_file(copy_command, compressed_path, pg_conn)

    # Confirm that it's snappy
    duckdb_conn.execute(
        f"SELECT compression FROM parquet_metadata('{compressed_path}') LIMIT 1"
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == "SNAPPY"

    drop_table_command = "DROP TABLE test_compressed"
    run_command(drop_table_command, pg_conn)
    pg_conn.commit()


def test_invalid_compression(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """
        CREATE TABLE test_compressed (x int);
    """,
        pg_conn,
    )

    # Write with unrecognized compression
    compressed_path = tmp_path / "test.parquet.snappy"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'parquet', compression 'zoko')"
    )
    error = copy_to_file(copy_command, compressed_path, pg_conn, raise_error=False)
    assert error.startswith('ERROR:  pg_lake_copy: compression "zoko" not recognized')

    pg_conn.rollback()


def test_copy_to_file(pg_conn, duckdb_conn, tmp_path):
    # Write empty table to Parquet file
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"""
        CREATE TABLE test_copy_to_file (x int, y int);
        COPY test_copy_to_file TO '{parquet_path}' WITH (format PARQUET);
    """,
        pg_conn,
    )

    # Check output
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_parquet($1)", [str(parquet_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 0

    # Write table with data to a Parquet file
    run_command(
        f"""
        INSERT INTO test_copy_to_file VALUES (1,2), (3,4);
        COPY test_copy_to_file TO '{parquet_path}' WITH (format 'parquet');
        COPY test_copy_to_file FROM '{parquet_path}' WITH (format 'parquet', FREEZE true);
    """,
        pg_conn,
    )

    # Check output
    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_copy_to_file",
        pg_conn,
    )
    assert result[0]["count"] == 4
    assert result[0]["distinct"] == 2

    # Make sure it's actually Parquet
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_parquet($1)", [str(parquet_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 2

    pg_conn.rollback()


def test_copy_to_program(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """
        CREATE TABLE test_copy_to_program (x int, y int);
    """,
        pg_conn,
    )

    # Write to (fake) program
    copy_command = "COPY test_compressed TO PROGRAM 'echo 1,2' WITH (format 'parquet')"
    error = run_command(copy_command, pg_conn, raise_error=False)
    assert error.startswith(
        "ERROR:  pg_lake_copy: COPY FROM/TO PROGRAM is not supported"
    )

    pg_conn.rollback()


def test_copy_from_program(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """
        CREATE TABLE test_copy_from_program (x int, y int);
    """,
        pg_conn,
    )

    # Read from (fake) program
    copy_command = (
        "COPY test_compressed FROM PROGRAM 'echo 1,2' WITH (format 'parquet')"
    )
    error = run_command(copy_command, pg_conn, raise_error=False)
    assert error.startswith(
        "ERROR:  pg_lake_copy: COPY FROM/TO PROGRAM is not supported"
    )

    pg_conn.rollback()


def test_copy_where(pg_conn, duckdb_conn, tmp_path):
    run_command("CREATE TABLE test_where (x int, y int)", pg_conn)

    # Write to a Parquet file
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"COPY (SELECT s x, s y FROM generate_series(1,100) s) TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
    )

    # Read Parquet file partially
    run_command(
        f"COPY test_where FROM '{parquet_path}' WITH (format 'parquet') WHERE (x % 2 = 0)",
        pg_conn,
    )

    # Check output
    result = run_query("SELECT count(*) AS count FROM test_where", pg_conn)
    assert result[0]["count"] == 50

    pg_conn.rollback()


def test_copy_to_rls(superuser_conn, duckdb_conn, tmp_path):
    # Set up a table with RLS and write to a Parquet file as superuser
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"""
        CREATE TABLE test_rls (x int, y int);
        ALTER TABLE test_rls ENABLE ROW LEVEL SECURITY;
        CREATE ROLE test_rls;
        GRANT SELECT ON test_rls TO test_rls;
        GRANT pg_write_server_files TO test_rls;
        INSERT INTO test_rls SELECT s, s FROM generate_series(1,100) s;
        CREATE POLICY test_rls_policy ON test_rls FOR ALL TO test_rls USING (x % 2 = 0);
        COPY test_rls TO '{parquet_path}' WITH (format 'parquet');
    """,
        superuser_conn,
    )

    # Check that we see all the rows
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_parquet($1)", [str(parquet_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 100

    # Write to a Parquet file as a less privileged user
    run_command(
        f"""
        SET ROLE test_rls;
        COPY test_rls TO '{parquet_path}' WITH (format 'parquet');
    """,
        superuser_conn,
    )

    # Check that we see a subset of rows
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_parquet($1)", [str(parquet_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 50

    # Try again as table owner
    run_command(
        f"""
        RESET ROLE;
        ALTER TABLE test_rls OWNER TO test_rls;
        SET ROLE test_rls;
        COPY test_rls TO '{parquet_path}' WITH (format 'parquet');
    """,
        superuser_conn,
    )

    # Check that we see all the rows
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_parquet($1)", [str(parquet_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 100

    superuser_conn.rollback()


def test_copy_from_rls(superuser_conn, tmp_path):
    # Set up a table with RLS and write to a Parquet file as superuser
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"""
        CREATE TABLE test_rls (x int, y int);
        ALTER TABLE test_rls ENABLE ROW LEVEL SECURITY;
        CREATE ROLE test_rls;
        GRANT SELECT, INSERT ON test_rls TO test_rls;
        GRANT pg_read_server_files TO test_rls;
        GRANT pg_write_server_files TO test_rls;
        INSERT INTO test_rls SELECT s, s FROM generate_series(1,100) s;
        CREATE POLICY test_rls_policy ON test_rls FOR ALL TO test_rls USING (x % 2 = 0);
        COPY test_rls TO '{parquet_path}' WITH (format 'parquet');
        TRUNCATE TABLE test_rls;
    """,
        superuser_conn,
    )

    run_command("SET ROLE test_rls;", superuser_conn)

    # copy from with rls enabled
    error = run_command(
        f"""
        SAVEPOINT s1;
        COPY test_rls FROM '{parquet_path}' WITH (format 'parquet');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "COPY FROM not supported with row-level security" in error

    run_command("ROLLBACK TO s1;", superuser_conn)

    # copy from with rls disabled
    run_command(
        f"""
        RESET ROLE;
        ALTER TABLE test_rls DISABLE ROW LEVEL SECURITY;
        SET ROLE test_rls;
        COPY test_rls FROM '{parquet_path}' WITH (format 'parquet');
    """,
        superuser_conn,
    )

    # Check that we see all the rows
    result = run_query("SELECT count(*) AS count FROM test_rls", superuser_conn)
    assert result[0]["count"] == 100

    # see that PG prevents COPY FROM with RLS enabled (pg_lake_copy does not kick in for local csv)
    local_csv_path = tmp_path / "test.csv"
    run_command(
        f"""
        RESET ROLE;
        COPY test_rls TO '{local_csv_path}' WITH (format 'csv');
        SET ROLE test_rls;
    """,
        superuser_conn,
    )

    run_command(
        f"""
        COPY test_rls FROM '{local_csv_path}' WITH (format 'csv');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "COPY FROM not supported with row-level security" in error

    superuser_conn.rollback()


def test_copy_large_row(pg_conn, duckdb_conn, tmp_path):
    # Write a table with rows of several MB to a Parquet file
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"""
        CREATE TABLE test_large_row (large1 text, large2 text);
        INSERT INTO test_large_row SELECT repeat('A', 10000000), repeat('B', 20000000) FROM generate_series(1,5);
        COPY test_large_row TO '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    # Check length of the rows
    duckdb_conn.execute(
        "SELECT min(length(large1)) AS l1, min(length(large2)) AS l2 FROM read_parquet($1)",
        [str(parquet_path)],
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 10000000
    assert duckdb_result[0][1] == 20000000

    # Read the Parquet file into another table
    run_command(
        f"""
        CREATE TABLE test_large_row_after (like test_large_row);
        COPY test_large_row_after FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT min(length(large1)) AS l1, min(length(large2)) AS l2 FROM test_large_row_after",
        pg_conn,
    )
    assert result[0]["l1"] == 10000000
    assert result[0]["l2"] == 20000000

    pg_conn.rollback()


def test_partitioned(pg_conn, duckdb_conn, tmp_path):
    # Write a partitioned table to a Parquet file and read it back
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"""
        CREATE TABLE test_partitioned (t date, data text) PARTITION BY RANGE (t);
        CREATE TABLE test_partitioned_1 PARTITION OF test_partitioned FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
        CREATE TABLE test_partitioned_2 PARTITION OF test_partitioned FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
        INSERT INTO test_partitioned VALUES ('2020-03-04', 'hello'), ('2021-03-03', 'world');
        COPY (SELECT * FROM test_partitioned) TO '{parquet_path}' WITH (format 'parquet');
        COPY test_partitioned FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    # Check output, we doubled the number of rows, but same number of distinct rows
    result = run_query(
        "SELECT count(*) AS count, count(distinct data) AS distinct FROM test_partitioned",
        pg_conn,
    )
    assert result[0]["count"] == 4
    assert result[0]["distinct"] == 2

    result = run_command(
        f"""
    COPY test_partitioned TO '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "cannot copy from partitioned table" in result

    pg_conn.rollback()


def test_triggers(pg_conn, duckdb_conn, tmp_path):
    # Test triggers called during COPY FROM
    parquet_path = tmp_path / "test.parquet"
    run_command(
        f"""
        CREATE TABLE test_triggers1 (id int);
        INSERT INTO test_triggers1 VALUES (1);

        CREATE TABLE test_triggers2 (id int);
        CREATE OR REPLACE FUNCTION sync_table2()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO test_triggers2 (id) VALUES (NEW.id);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_insert_on_triggers1
        AFTER INSERT ON test_triggers1
        FOR EACH ROW EXECUTE FUNCTION sync_table2();

        COPY test_triggers1 TO '{parquet_path}'WITH (format 'parquet');
        COPY test_triggers1 FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    # Check output, we doubled the number of rows, but same number of distinct rows
    result = run_query("SELECT count(*) AS count FROM test_triggers2", pg_conn)
    assert result[0]["count"] == 1

    pg_conn.rollback()


def test_column_subset(pg_conn, duckdb_conn, tmp_path):
    parquet_path = tmp_path / "test.parquet"

    # Create a table and emit a subset of columns into Parquet
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
        COPY test_column_subset (val3, "gre@t", d) TO '{parquet_path}' WITH (format 'parquet');

        CREATE TABLE test_column_subset_after (val2 int, val3 bigint, "gre@t" text, d date);
        COPY test_column_subset_after (val3, "gre@t" , d) FROM '{parquet_path}' WITH (format 'parquet');
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


def test_duplicate_column(pg_conn, tmp_path):
    # Write to a Parquet file
    parquet_path = tmp_path / "test.parquet"

    error = run_command(
        f"""
        COPY (SELECT s, s FROM generate_series(1,100) s) TO '{parquet_path}' WITH (format 'parquet')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  pg_lake_copy: column "s" specified more than once')

    pg_conn.rollback()


def test_md_array(pg_conn, duckdb_conn, tmp_path):
    parquet_path = tmp_path / "test.parquet"

    # Use DuckDB, since we don't yet support writing multi-dimensional arrays
    duckdb_conn.execute(
        f"""
        COPY (SELECT [[0,2],[1,2]] md FROM generate_series(1,10)) TO '{parquet_path}';
    """
    )

    run_command(
        f"""
        CREATE TABLE test_md_array (arr int[][]);
        COPY test_md_array FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    # Sanity check on the result
    result = run_query("SELECT arr[2][2] AS val FROM test_md_array", pg_conn)
    assert len(result) == 10
    assert result[0]["val"] == 2


# An unbounded numeric column maps to DuckDB DECIMAL(38,9) when the CSV is
# converted to Parquet, which allows only 38 - 9 = 29 integral digits. COPY TO
# validates this on the PostgreSQL side (before the value reaches the CSV), and
# the validation must reach numeric leaves nested inside arrays, composites,
# maps, and domains -- not just top-level numeric columns. Special values
# (NaN/Infinity/-Infinity), valid PG input for an unbounded numeric but not
# representable as a DuckDB DECIMAL, must be rejected at those leaves too.
#
# A 30-digit integer trips the cap; a 29-digit one is exactly at the limit and
# must still succeed.
NUMERIC_OVER_LIMIT = 10**29  # 30 integral digits -> rejected
NUMERIC_AT_LIMIT = 10**28  # 29 integral digits -> allowed
EXCEEDS_DIGITS_ERROR = "exceeds max allowed digits"
SPECIAL_NUMERIC_ERROR = "Special numeric values"

# The nested container shapes that carry a numeric leaf. Every one must be
# walked by the COPY TO numeric validation.  "array_of_composite" is the
# deeply-nested case: the numeric leaf sits two container levels down
# (array -> composite -> numeric).
NESTED_NUMERIC_KINDS = [
    "array",
    "composite",
    "map",
    "domain_scalar",
    "domain_array",
    "array_of_composite",
]


def _create_nested_numeric_types(pg_conn):
    # Tolerate the map type already existing from a previous run; the composite
    # and domains live in the caller's (uncommitted) transaction.
    create_map_type("int", "numeric", raise_error=False)
    run_command(
        """
        CREATE SCHEMA IF NOT EXISTS lake_struct;
        CREATE TYPE lake_struct.cost_pair AS (label text, amount numeric);
        CREATE DOMAIN cost_scalar AS numeric;
        CREATE DOMAIN cost_array AS numeric[];
        """,
        pg_conn,
    )


def _nested_numeric_select(kind, expr, token):
    """Build a SELECT that places a numeric leaf inside the given container.

    `expr` is a SQL numeric expression used by the array/composite/domain
    shapes; `token` is the bare value spliced into the map's composite text
    literal (where an `'x'::numeric` cast is not valid).  The map's braces come
    from a plain Python string so they are inserted verbatim, not parsed as
    f-string replacement fields.
    """
    map_literal = '{"(1,500.00)","(2,%s)"}' % token
    return {
        "array": f"SELECT ARRAY[500.00, {expr}]::numeric[] AS v",
        "composite": f"SELECT ('airfare', {expr})::lake_struct.cost_pair AS v",
        "map": f"SELECT '{map_literal}'::map_type.key_int_val_numeric AS v",
        "domain_scalar": f"SELECT {expr}::cost_scalar AS v",
        "domain_array": f"SELECT ARRAY[500.00, {expr}]::cost_array AS v",
        "array_of_composite": (
            f"SELECT ARRAY[('base', 500.00)::lake_struct.cost_pair, "
            f"('leaf', {expr})::lake_struct.cost_pair]::lake_struct.cost_pair[] AS v"
        ),
    }[kind]


@pytest.mark.parametrize("kind", NESTED_NUMERIC_KINDS)
def test_nested_unbounded_numeric(pg_conn, tmp_path, kind):
    parquet_path = tmp_path / "test.parquet"

    _create_nested_numeric_types(pg_conn)

    over = str(NUMERIC_OVER_LIMIT)

    # An oversized numeric leaf (30 integral digits) is rejected wherever it
    # sits in the nested structure. (The within-cap acceptance path is covered
    # by test_nested_numeric_roundtrip; a nested numeric Parquet write is a
    # separate concern from the digit-limit validation exercised here.)
    error = run_command(
        f"COPY ({_nested_numeric_select(kind, over, over)}) "
        f"TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
        raise_error=False,
    )
    assert EXCEEDS_DIGITS_ERROR in error, f"{kind}: {error}"

    pg_conn.rollback()


@pytest.mark.parametrize("special", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("kind", NESTED_NUMERIC_KINDS)
def test_nested_special_numeric(pg_conn, tmp_path, kind, special):
    parquet_path = tmp_path / "test.parquet"

    _create_nested_numeric_types(pg_conn)

    expr = f"'{special}'::numeric"
    error = run_command(
        f"COPY ({_nested_numeric_select(kind, expr, special)}) "
        f"TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
        raise_error=False,
    )
    assert SPECIAL_NUMERIC_ERROR in error, f"{kind} / {special}: {error}"

    pg_conn.rollback()


def test_nested_numeric_roundtrip(pg_conn, tmp_path):
    # A within-cap numeric[] survives the COPY TO Parquet / COPY FROM round-trip
    # with its values intact (guards against the validation over-rejecting).
    parquet_path = tmp_path / "test.parquet"

    run_command(
        f"""
        CREATE TABLE test_num_array (a numeric[]);
        COPY (SELECT ARRAY[500.00, {NUMERIC_AT_LIMIT}]::numeric[] AS a)
        TO '{parquet_path}' WITH (format 'parquet');
        COPY test_num_array FROM '{parquet_path}' WITH (format 'parquet');
        """,
        pg_conn,
    )
    result = run_query("SELECT a FROM test_num_array", pg_conn)
    assert result[0]["a"] == [Decimal("500.00"), Decimal(NUMERIC_AT_LIMIT)]

    pg_conn.rollback()


def test_copy_to_multidim_array_errors(pg_conn, tmp_path):
    """
    Regression test for https://github.com/Snowflake-Labs/pg_lake/issues/407.

    COPY <table> TO <lake-file> must raise a clear pg_lake error when a column
    contains a multidimensional array value.  Previously the value was
    serialised into the intermediate temp CSV and handed to DuckDB, which
    either returned a cryptic Conversion Error or crashed the shared
    pgduck_server process (issue #408).

    The check lives in CopyOneRowTo (csv_writer.c) and fires before the CSV
    is written, so the engine is never involved.
    """
    parquet_path = tmp_path / "test_multidim_err.parquet"

    run_command(
        """
        CREATE TABLE test_multidim_err (id bigint, v int[]);
        INSERT INTO test_multidim_err VALUES (1, ARRAY[[1,2],[3,4]]);
    """,
        pg_conn,
    )

    error = run_command(
        f"COPY test_multidim_err TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
        raise_error=False,
    )

    assert error is not None, "Expected an error for multidimensional array in COPY TO"
    assert (
        "multidimensional arrays are not supported" in error.lower()
    ), f"Unexpected error message: {error}"

    pg_conn.rollback()


def test_copy_to_1d_array_succeeds(pg_conn, duckdb_conn, tmp_path):
    """
    Regression guard: 1-D array columns must still round-trip correctly through
    COPY TO after the multidim check is added.  A 1-D value has ARR_NDIM == 1
    and must not be rejected.
    """
    parquet_path = tmp_path / "test_1d_array.parquet"

    run_command(
        f"""
        CREATE TABLE test_1d_array (id bigint, tags text[]);
        INSERT INTO test_1d_array VALUES
            (1, ARRAY['a', 'b', 'c']),
            (2, NULL),
            (3, ARRAY['x']);
        COPY test_1d_array TO '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    duckdb_conn.execute(
        "SELECT id, tags FROM read_parquet($1) ORDER BY id", [str(parquet_path)]
    )
    rows = duckdb_conn.fetchall()

    assert len(rows) == 3
    assert rows[0] == (1, ["a", "b", "c"])
    assert rows[1] == (2, None)
    assert rows[2] == (3, ["x"])

    pg_conn.rollback()


# The multidimensional-array rejection must reach an array leaf no matter how
# deeply it is nested -- inside a composite field, an array of composites, or a
# domain over an array -- not just a top-level array column (review feedback on
# PR #430).  This mirrors the recursive numeric-leaf validation above; the walk
# lives in ErrorIfCopyToContainsMultidimArray (csv_writer.c).
MULTIDIM_ARRAY_ERROR = "multidimensional arrays are not supported"

NESTED_ARRAY_KINDS = ["composite", "array_of_composite", "domain_array"]


def _create_nested_array_types(pg_conn):
    run_command(
        """
        CREATE SCHEMA IF NOT EXISTS lake_arr;
        CREATE TYPE lake_arr.tagged AS (label text, vals int[]);
        CREATE DOMAIN int_matrix AS int[];
        """,
        pg_conn,
    )


def _nested_array_select(kind, arr_expr):
    """Build a SELECT that places the int[] expression `arr_expr` inside the
    given container shape.  "array_of_composite" is the deepest case: the array
    leaf sits two container levels down (array -> composite -> int[])."""
    return {
        "composite": f"SELECT ('m', {arr_expr})::lake_arr.tagged AS v",
        "array_of_composite": (
            f"SELECT ARRAY[('base', ARRAY[1, 2])::lake_arr.tagged, "
            f"('leaf', {arr_expr})::lake_arr.tagged]::lake_arr.tagged[] AS v"
        ),
        "domain_array": f"SELECT {arr_expr}::int_matrix AS v",
    }[kind]


@pytest.mark.parametrize("kind", NESTED_ARRAY_KINDS)
def test_nested_multidim_array_errors(pg_conn, tmp_path, kind):
    parquet_path = tmp_path / "test.parquet"

    _create_nested_array_types(pg_conn)

    # A multidimensional (2-D) array leaf is rejected wherever it sits in the
    # nested structure, with the same clear error the top-level column gets.
    error = run_command(
        f"COPY ({_nested_array_select(kind, 'ARRAY[[1, 2], [3, 4]]')}) "
        f"TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
        raise_error=False,
    )
    assert error is not None, f"{kind}: expected a multidimensional-array error"
    assert MULTIDIM_ARRAY_ERROR in error.lower(), f"{kind}: {error}"

    pg_conn.rollback()


# domain_array is intentionally omitted here: a domain over an array does not
# round-trip through COPY TO parquet even for a 1-D value (its DuckDB engine
# type is inferred as a bare, untyped LIST), which is a separate pre-existing
# limitation unrelated to the multidimensional-array guard.  The 2-D domain
# case is still covered above -- the guard rejects it before that path is hit.
NESTED_ARRAY_ROUNDTRIP_KINDS = ["composite", "array_of_composite"]


@pytest.mark.parametrize("kind", NESTED_ARRAY_ROUNDTRIP_KINDS)
def test_nested_1d_array_succeeds(pg_conn, tmp_path, kind):
    # A 1-D array leaf nested in the same container shapes must NOT be rejected;
    # guards against the recursive walk over-rejecting well-formed values.
    parquet_path = tmp_path / "test.parquet"

    _create_nested_array_types(pg_conn)

    error = run_command(
        f"COPY ({_nested_array_select(kind, 'ARRAY[1, 2, 3]')}) "
        f"TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
        raise_error=False,
    )
    assert error is None, f"{kind}: unexpected error {error}"

    pg_conn.rollback()


def test_nested_multidim_bigint_array_errors(pg_conn, tmp_path):
    """
    A multidimensional bigint[] nested inside a composite field is rejected by
    the recursive guard.  bigint (a fixed-width element) is called out
    separately because it is one of the element types whose unguarded nested
    multidim value crashed the shared pgduck_server in issue #408; the walk
    must reach it wherever it is nested, not only when it is the column type.
    """
    parquet_path = tmp_path / "test.parquet"

    run_command(
        """
        CREATE TYPE lake_arr_bc AS (id int, vals bigint[]);
        CREATE TABLE test_multidim_bigint (id bigint, c lake_arr_bc);
        INSERT INTO test_multidim_bigint
            VALUES (1, ROW(1, ARRAY[[1, 2], [3, 4]]::bigint[])::lake_arr_bc);
        """,
        pg_conn,
    )

    error = run_command(
        f"COPY test_multidim_bigint TO '{parquet_path}' WITH (format 'parquet')",
        pg_conn,
        raise_error=False,
    )
    assert error is not None, "expected a multidimensional-array error"
    assert (
        "multidimensional arrays are not supported" in error.lower()
    ), f"Unexpected error message: {error}"

    pg_conn.rollback()


def test_copy_to_multidim_array_json_errors(pg_conn, tmp_path):
    """
    Companion to the CSV case: JSON serialises arrays through DuckDB as a
    typed LIST(T) (ShouldUseDuckSerialization is true), so a multidimensional
    value must still be rejected by the guard in CopyOneRowTo.
    """
    json_path = tmp_path / "test_multidim.json"

    run_command(
        """
        CREATE TABLE test_multidim_json (id bigint, v int[]);
        INSERT INTO test_multidim_json VALUES (1, ARRAY[[1,2],[3,4]]);
        """,
        pg_conn,
    )

    error = run_command(
        f"COPY test_multidim_json TO '{json_path}' WITH (format 'json')",
        pg_conn,
        raise_error=False,
    )

    assert (
        error is not None
    ), "Expected an error for multidimensional array in COPY TO json"
    assert (
        "multidimensional arrays are not supported" in error.lower()
    ), f"Unexpected error message: {error}"

    pg_conn.rollback()


def test_copy_to_nested_multidim_array_json_errors(pg_conn, tmp_path):
    """
    The recursive guard is format-independent: a multidimensional array nested
    inside a composite must be rejected for JSON too, not only Parquet.
    """
    json_path = tmp_path / "test_nested_multidim.json"

    run_command(
        """
        CREATE TYPE lake_arr_jc AS (id int, vals int[]);
        CREATE TABLE test_nested_multidim_json (id bigint, c lake_arr_jc);
        INSERT INTO test_nested_multidim_json
            VALUES (1, ROW(1, ARRAY[[1, 2], [3, 4]])::lake_arr_jc);
        """,
        pg_conn,
    )

    error = run_command(
        f"COPY test_nested_multidim_json TO '{json_path}' WITH (format 'json')",
        pg_conn,
        raise_error=False,
    )

    assert (
        error is not None
    ), "Expected an error for nested multidim array in COPY TO json"
    assert (
        "multidimensional arrays are not supported" in error.lower()
    ), f"Unexpected error message: {error}"

    pg_conn.rollback()


def test_copy_virtual_column(pg_conn, tmp_path):
    # virtual columns were introduced in PostgreSQL 18
    if get_pg_version_num(pg_conn) < 180000:
        return

    # Write to a Parquet file
    parquet_path = tmp_path / "test.parquet"

    run_command(
        f"""
        CREATE TABLE test_virtual (a int, s text GENERATED ALWAYS AS (a::text) STORED, v text GENERATED ALWAYS AS (a::text) VIRTUAL);
        INSERT INTO test_virtual (a) VALUES (1), (2), (3), (null);
        COPY test_virtual TO '{parquet_path}' WITH (format 'parquet');
        COPY test_virtual(a) FROM '{parquet_path}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_virtual order by a,s,v", pg_conn)

    assert result == [
        [1, "1", "1"],
        [1, "1", "1"],
        [2, "2", "2"],
        [2, "2", "2"],
        [3, "3", "3"],
        [3, "3", "3"],
        [None, None, None],
        [None, None, None],
    ]

    pg_conn.rollback()

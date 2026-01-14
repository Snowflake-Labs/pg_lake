import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_create_table_definition_from(pg_conn, azure):
    url = f"az://{TEST_BUCKET}/test_create_table_definition_from/data.parquet"
    json = '{"hello":5}'

    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s) TO '{url}';
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_definition_from () WITH (parallel_workers=0, definition_from='{url}')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_definition_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 6
    assert result == [
        ["s", "integer"],
        ["t", "timestamp with time zone"],
        ["d", "numeric"],
        ["b", "bigint"],
        ["h", "text"],
        ["j", "jsonb"],
    ]

    result = run_query("SELECT count(*) FROM test_definition_from", pg_conn)
    assert result[0]["count"] == 0

    # Test that other options are preserved
    result = run_query(
        "SELECT unnest(reloptions) opt FROM pg_class WHERE oid = 'test_definition_from'::regclass",
        pg_conn,
    )
    assert result[0]["opt"] == "parallel_workers=0"

    pg_conn.rollback()


def test_create_table_definition_from_options(pg_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_create_table_definition_from_options/data.1"

    # Write a compressed CSV file, but using a regular extension
    run_command(
        f"""
        COPY (SELECT s AS id, 'hello-'||s AS desc FROM generate_series(0,100) s) TO '{url}'
        WITH (format 'csv', compression 'gzip', header on, delimiter ';');
    """,
        pg_conn,
    )

    # Try to create a table without specifying compression
    error = run_command(
        f"""
        CREATE TABLE test_definition_from () WITH (definition_from='{url}', format='csv')
    """,
        pg_conn,
        raise_error=False,
    )
    assert "Invalid Input Error" in error

    pg_conn.rollback()

    # Create the table with compression
    run_command(
        f"""
        CREATE TABLE test_definition_from () WITH (definition_from='{url}', compression='gzip', format='csv')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute
        where attrelid = 'test_definition_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [["id", "bigint"], ["desc", "text"]]

    pg_conn.rollback()

    # Lie about the header (everything becomes text, because header is interpreted as text)
    run_command(
        f"""
        CREATE TABLE test_definition_from () WITH (format='CSV', definition_from='{url}', compression='gzip', header=false)
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute
        where attrelid = 'test_definition_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [["column0", "text"], ["column1", "text"]]


def test_create_table_load_from(pg_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_create_table_load_from/data.parquet"
    json = '{"hello":5}'

    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE load_from () WITH (load_from='{url}')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'load_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 6
    assert result == [
        ["s", "integer"],
        ["t", "timestamp with time zone"],
        ["d", "numeric"],
        ["b", "bigint"],
        ["h", "text"],
        ["j", "jsonb"],
    ]

    result = run_query("SELECT count(*) FROM load_from", pg_conn)
    assert result[0]["count"] == 10

    pg_conn.rollback()


def test_create_table_load_from_csv_null_padding(pg_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_create_table_load_from_csv_null_padding/data.csv"

    # Generate a CSV file with rows having fewer columns than expected
    csv_content = "1,2,3\n4\n5,6\n"
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key="test_create_table_load_from_csv_null_padding/data.csv",
        Body=csv_content,
    )

    # Create table with load_from and null_padding option
    # Note: header=false is needed because auto_detect is enabled by default for load_from
    run_command(
        f"""
        CREATE TABLE load_from_null_padding (a int, b int, c int) WITH (load_from='{url}', format='csv', null_padding=true, header=false)
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT a, b, c FROM load_from_null_padding ORDER BY a
    """,
        pg_conn,
    )
    assert len(result) == 3
    assert result[0]["a"] == 1
    assert result[0]["b"] == 2
    assert result[0]["c"] == 3
    assert result[1]["a"] == 4
    assert result[1]["b"] == None
    assert result[1]["c"] == None
    assert result[2]["a"] == 5
    assert result[2]["b"] == 6
    assert result[2]["c"] == None

    pg_conn.rollback()


def test_create_table_load_from_explicit(pg_conn, duckdb_conn, azure):
    url = f"az://{TEST_BUCKET}/test_create_table_load_from_explicit/data.parquet"
    json = '{"hello":5}'

    # Use our own column names and types
    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE load_from (a text, b text, c double precision, d text, e text, f text) WITH (load_from='{url}')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'load_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 6
    assert result == [
        ["a", "text"],
        ["b", "text"],
        ["c", "double precision"],
        ["d", "text"],
        ["e", "text"],
        ["f", "text"],
    ]

    result = run_query("SELECT count(*) FROM load_from", pg_conn)
    assert result[0]["count"] == 10

    pg_conn.rollback()


def test_create_table_load_from_invalid_url(pg_conn, duckdb_conn, s3):
    notexists_url = (
        f"s3://{TEST_BUCKET}/test_create_table_load_from_invalid_url/data.parquet"
    )

    # non-existent URL
    error = run_command(
        f"""
        CREATE TABLE hit404 () WITH (load_from = '{notexists_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  HTTP Error: Unable to connect to URL")

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE hit404 () WITH (definition_from = '{notexists_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  HTTP Error: Unable to connect to URL")

    pg_conn.rollback()

    # invalid url
    invalid_url = "gopher://abc"
    error = run_command(
        f"""
        CREATE TABLE hitgopher () WITH (load_from = '{invalid_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: only s3:// and gs:// URLs are currently supported"
    )

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE hitgopher () WITH (definition_from = '{invalid_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: only s3:// and gs:// URLs are currently supported"
    )

    pg_conn.rollback()


def test_create_table_if_exists(pg_conn, duckdb_conn, azure):
    url = f"az://{TEST_BUCKET}/test_create_table_load_from_table_exists/data.parquet"

    # Create the same table twice
    error = run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE exists () WITH (definition_from = '{url}');
        CREATE TABLE exists () WITH (load_from = '{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  relation "exists" already exists')

    pg_conn.rollback()

    # IF NOT EXISTS does not help
    error = run_command(
        f"""
        CREATE TABLE IF NOT EXISTS exists () WITH (load_from = '{url}');
        CREATE TABLE IF NOT EXISTS exists () WITH (load_from = '{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert (
        "CREATE TABLE IF NOT EXISTS cannot be used with the load_from option" in error
    )

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE IF NOT EXISTS exists () WITH (definition_from = '{url}');
        CREATE TABLE IF NOT EXISTS exists () WITH (definition_from = '{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert (
        "CREATE TABLE IF NOT EXISTS cannot be used with the definition_from option"
        in error
    )

    pg_conn.rollback()


def test_create_table_combined(pg_conn, duckdb_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_create_table_combined/data.parquet"

    # Use both options
    error = run_command(
        f"""
        CREATE TABLE combined1 () WITH (load_from='{url}', definition_from='{url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: cannot specify both load_from and definition_from table options"
    )

    pg_conn.rollback()


def test_create_table_noperm(superuser_conn, duckdb_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_create_table_noperm/data.parquet"

    duckdb_conn.execute(
        f"COPY (SELECT generate_series, 'access' FROM generate_series(1,10)) TO '{url}'"
    )

    # Create user1 without privileges
    error = run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user1;
        GRANT ALL ON SCHEMA public TO user1;
        SET ROLE user1;
        CREATE TABLE access () WITH (definition_from='{url}');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  permission denied to read from URL")

    superuser_conn.rollback()

    # Create user2 with privileges
    error = run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_copy CASCADE;
        CREATE ROLE user2;
        GRANT ALL ON SCHEMA public TO user2;
        GRANT lake_read TO user2;
        SET ROLE user2;
        CREATE TABLE access () WITH (load_from='{url}');
    """,
        superuser_conn,
    )

    result = run_query("SELECT count(*) FROM access", superuser_conn)
    assert result[0]["count"] == 10

    superuser_conn.rollback()


def test_create_table_partitioned(pg_conn, duckdb_conn, s3):
    url = f"s3://{TEST_BUCKET}/test_create_table_partitioned/data.parquet"

    duckdb_conn.execute(
        f"COPY (SELECT '2020-01-01'::date t, 'partition' AS data FROM generate_series(1,10)) TO '{url}'"
    )

    # Try to infer schema of a parent
    run_command(
        f"""
        CREATE TABLE test_partitioned () PARTITION BY RANGE (t) WITH (definition_from = '{url}');
        CREATE TABLE test_partitioned_1 PARTITION OF test_partitioned FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_partitioned'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result == [["t", "date"], ["data", "text"]]

    pg_conn.rollback()

    # Try to infer schema of a partition
    error = run_command(
        f"""
        CREATE TABLE test_partitioned (t date, data text) PARTITION BY RANGE (t);
        CREATE TABLE test_partitioned_1 PARTITION OF test_partitioned FOR VALUES FROM ('2020-01-01') TO ('2021-01-01') WITH (definition_from = '{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "cannot infer the column definitions" in error

    pg_conn.rollback()

    # Try to load a partition on creation
    run_command(
        f"""
        CREATE TABLE test_partitioned (t date, data text) PARTITION BY RANGE (t);
        CREATE TABLE test_partitioned_1 PARTITION OF test_partitioned FOR VALUES FROM ('2020-01-01') TO ('2021-01-01') WITH (load_from = '{url}');
    """,
        pg_conn,
    )

    result = run_query("SELECT count(*) FROM test_partitioned", pg_conn)
    assert result[0]["count"] == 10

    pg_conn.rollback()


def test_create_table_csv(pg_conn, superuser_conn, app_user):
    url = f"s3://{TEST_BUCKET}/test_create_temp_table/data.csv.zst"
    json = '{"hello":5}'

    run_command(
        f"""
        CREATE SCHEMA csv;
        GRANT USAGE, CREATE on SCHEMA csv TO {app_user}
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    # Generate a simple CSV and load it using auto_detect
    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s)
        TO '{url}' WITH (header on, quote '''', delimiter '|');
        CREATE TABLE csv.test_csv () WITH (load_from='{url}', parallel_workers=1, freeze)
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'csv.test_csv'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 6
    assert result == [
        ["s", "bigint"],
        ["t", "timestamp with time zone"],
        ["d", "double precision"],
        ["b", "bigint"],
        ["h", "text"],
        ["j", "text"],
    ]

    result = run_query("SELECT count(*) FROM csv.test_csv", pg_conn)
    assert result[0]["count"] == 10

    pg_conn.rollback()

    # Super weird CSV should work as long as we pass the options
    run_command(
        f"""
        COPY (SELECT s x, 'hell$o' y, NULL z FROM generate_series(1,10) s) TO '{url}'
        WITH (format csv, header off, delimiter '$', null 'NNN', quote '&', escape '\\', compression 'gzip', force_quote (y));

        CREATE TABLE csv.test_csv ()
        WITH (format='csv', load_from='{url}', header=off, delimiter='$', null='NNN', quote='&', escape='\\', compression='gzip')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'csv.test_csv'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [["column0", "bigint"], ["column1", "text"], ["column2", "text"]]

    result = run_query("SELECT * FROM csv.test_csv ORDER BY column0", pg_conn)
    assert len(result) == 10
    assert result[0]["column0"] == 1
    assert result[0]["column1"] == "hell$o"
    assert result[0]["column2"] == None

    pg_conn.rollback()

    run_command(f"DROP SCHEMA csv", superuser_conn)
    superuser_conn.commit()


def test_create_temp_table(pg_conn):
    url = f"s3://{TEST_BUCKET}/test_create_temp_table/data.json"

    run_command(
        f"""
        COPY (SELECT s x, s y FROM generate_series(1,10) s) TO '{url}';
        CREATE TEMP TABLE test_temp_table () WITH (load_from='{url}') ON COMMIT DELETE ROWS;
    """,
        pg_conn,
    )

    # Check pre-commit count
    result = run_query("SELECT * FROM test_temp_table ORDER BY x", pg_conn)
    assert len(result) == 10

    pg_conn.commit()

    # Check post-commit count
    result = run_query("SELECT count(*) FROM test_temp_table", pg_conn)
    assert result[0]["count"] == 0

    run_command("DROP TABLE test_temp_table", pg_conn)
    pg_conn.commit()


def test_savepoint(pg_conn):
    url = f"s3://{TEST_BUCKET}/test_savepoint/data.csv.gz"

    run_command(
        f"""
        BEGIN;
        COPY (SELECT 1 a, 2 b, 3 c, 4 d, 5 e, ARRAY[1,2,3,4,5] f FROM generate_series(1,5) s) TO '{url}';
        SAVEPOINT future;
        CREATE TABLE test_savepoint () WITH (load_from='{url}');
    """,
        pg_conn,
    )

    # Check load_from count
    result = run_query("SELECT * FROM test_savepoint ORDER BY column0", pg_conn)
    assert len(result) == 5

    run_command(
        f"""
        ROLLBACK TO SAVEPOINT future;
        CREATE TABLE test_savepoint () WITH (definition_from='{url}');
    """,
        pg_conn,
    )

    # Check definition_from count
    result = run_query("SELECT * FROM test_savepoint ORDER BY column0", pg_conn)
    assert len(result) == 0

    pg_conn.rollback()


def test_plpgsql(pg_conn):
    child_url = f"s3://{TEST_BUCKET}/test_plpgsql/child.parquet"

    run_command(
        f"""
        CREATE TABLE tables (
            table_name text,
            url text
        );

        CREATE OR REPLACE FUNCTION create_child() RETURNS trigger AS $create_child$
            BEGIN
                EXECUTE format('CREATE TABLE %s () WITH (load_from = %L)', NEW.table_name, NEW.url);
                RETURN NEW;
            END;
        $create_child$ LANGUAGE plpgsql;

        CREATE TRIGGER child BEFORE INSERT ON tables
            FOR EACH ROW EXECUTE FUNCTION create_child();

        COPY (SELECT s FROM generate_series(1,10) s) TO '{child_url}';
        INSERT INTO tables VALUES ('child1', '{child_url}');
    """,
        pg_conn,
    )

    # Should have a child1 table as a result of the trigger
    result = run_query("SELECT * FROM child1", pg_conn)
    assert len(result) == 10

    # cannot execute the trigger from a COPY from S3
    parent_url = f"s3://{TEST_BUCKET}/test_plpgsql/parent.csv"
    run_command(
        f"COPY (SELECT 'child2' table_name, '{child_url}' url) TO '{parent_url}' WITH (header)",
        pg_conn,
    )

    # cannot do nested COPY
    error = run_command(
        f"COPY tables FROM '{parent_url}' WITH (header)", pg_conn, raise_error=False
    )
    assert "nested COPY is not supported" in error

    pg_conn.rollback()


def test_nested_copy_to(pg_conn):
    inner1_url = f"s3://{TEST_BUCKET}/test_nested_copy_to/inner1.parquet"
    inner2_url = f"s3://{TEST_BUCKET}/test_nested_copy_to/inner2.parquet"
    outer_url = f"s3://{TEST_BUCKET}/test_nested_copy_to/outer.parquet"

    # Create a function that does COPY .. TO, and call it from COPY .. TO
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION copy_to(url text)
         RETURNS text
         LANGUAGE plpgsql
        AS $function$
        DECLARE
        BEGIN
        	EXECUTE format($$COPY (SELECT s FROM generate_series(1,3) s) TO %L WITH (format 'parquet')$$, url);
            RETURN 'success';
        END;
        $function$;

        CREATE TABLE exports (id int, url text);
        INSERT INTO exports VALUES (1, '{inner1_url}');
        INSERT INTO exports VALUES (2, '{inner2_url}');
        COPY (SELECT copy_to(url) FROM exports) TO '{outer_url}';
        CREATE TEMP TABLE tmp_inner1 () WITH (load_from = '{inner1_url}');
        CREATE TEMP TABLE tmp_inner2 () WITH (load_from = '{inner2_url}');
        CREATE TEMP TABLE tmp_outer () WITH (load_from = '{outer_url}');
    """,
        pg_conn,
    )

    # Check the generated files
    result = run_query("SELECT * FROM tmp_inner1", pg_conn)
    assert len(result) == 3

    result = run_query("SELECT * FROM tmp_inner2", pg_conn)
    assert len(result) == 3

    result = run_query("SELECT * FROM tmp_outer", pg_conn)
    assert len(result) == 2
    assert result[0]["copy_to"] == "success"

    pg_conn.rollback()

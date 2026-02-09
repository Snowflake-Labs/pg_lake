import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_create_iceberg_table_definition_from(
    pg_conn, extension, app_user, s3, with_default_location
):
    url = f"s3://{TEST_BUCKET}/test_create_iceberg_table_definition_from/data.parquet"
    json = '{"hello":5}'

    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 3.4::numeric(4,2) e, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s) TO '{url}';
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_iceberg_definition_from () USING iceberg WITH (definition_from='{url}')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name, atttypmod typmod from pg_attribute where attrelid = 'test_iceberg_definition_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 7
    assert result == [
        ["s", "integer", -1],
        ["t", "timestamp with time zone", -1],
        ["d", "numeric", 2490381],
        ["e", "numeric", 262150],
        ["b", "bigint", -1],
        ["h", "text", -1],
        ["j", "jsonb", -1],
    ]

    d_typmod = result[2][2]
    e_typmod = result[3][2]

    result = run_query(
        f"select numerictypmodout({d_typmod}), numerictypmodout({e_typmod});", pg_conn
    )
    assert result[0][0] == "(38,9)"
    assert result[0][1] == "(4,2)"

    result = run_query("SELECT count(*) FROM test_iceberg_definition_from", pg_conn)
    assert result[0]["count"] == 0

    result = run_query(
        "SELECT unnest(reloptions) opt FROM pg_class WHERE oid = 'test_iceberg_definition_from'::regclass",
        pg_conn,
    )
    assert len(result) == 0

    result = run_query(
        "SELECT count(*) FROM iceberg_tables WHERE table_name = 'test_iceberg_definition_from'",
        pg_conn,
    )
    assert len(result) == 1

    pg_conn.rollback()


def test_create_iceberg_table_definition_from_options(
    pg_conn, extension, app_user, s3, with_default_location
):
    url = f"s3://{TEST_BUCKET}/test_create_iceberg_table_definition_from_options/data.1"

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
        CREATE TABLE test_iceberg_definition_from () USING iceberg WITH (definition_from='{url}', format='csv')
    """,
        pg_conn,
        raise_error=False,
    )
    assert "Invalid Input Error" in error

    pg_conn.rollback()

    # Create the table with compression
    run_command(
        f"""
        CREATE TABLE test_iceberg_definition_from () USING iceberg WITH (definition_from='{url}', compression='gzip', format='csv')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute
        where attrelid = 'test_iceberg_definition_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [["id", "bigint"], ["desc", "text"]]

    pg_conn.rollback()

    # Lie about the header (everything becomes text, because header is interpreted as text)
    run_command(
        f"""
        CREATE TABLE test_iceberg_definition_from () USING iceberg WITH (format='CSV', definition_from='{url}', compression='gzip', header=false)
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute
        where attrelid = 'test_iceberg_definition_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [["column0", "text"], ["column1", "text"]]

    result = run_query(
        "SELECT count(*) FROM iceberg_tables WHERE table_name = 'test_iceberg_definition_from'",
        pg_conn,
    )
    assert len(result) == 1


def test_create_iceberg_table_load_from(
    pg_conn, extension, app_user, azure, with_default_location
):
    url = f"az://{TEST_BUCKET}/test_create_iceberg_table_load_from/data.parquet"
    json = '{"hello":5}'

    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 3.4::numeric(4,2) e, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE load_from () USING iceberg WITH (load_from='{url}')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name, atttypmod typmod from pg_attribute where attrelid = 'load_from'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 7
    assert result == [
        ["s", "integer", -1],
        ["t", "timestamp with time zone", -1],
        ["d", "numeric", 2490381],
        ["e", "numeric", 262150],
        ["b", "bigint", -1],
        ["h", "text", -1],
        ["j", "jsonb", -1],
    ]

    d_typmod = result[2][2]
    e_typmod = result[3][2]

    result = run_query(
        f"select numerictypmodout({d_typmod}), numerictypmodout({e_typmod});", pg_conn
    )
    assert result[0][0] == "(38,9)"
    assert result[0][1] == "(4,2)"

    result = run_query("SELECT count(*) FROM load_from", pg_conn)
    assert result[0]["count"] == 10

    result = run_query(
        "SELECT count(*) FROM iceberg_tables WHERE table_name = 'load_from'", pg_conn
    )
    assert len(result) == 1

    pg_conn.rollback()


def test_create_iceberg_table_load_from_explicit(
    pg_conn, extension, app_user, s3, with_default_location
):
    url = (
        f"s3://{TEST_BUCKET}/test_create_iceberg_table_load_from_explicit/data.parquet"
    )
    json = '{"hello":5}'

    # Use our own column names and types
    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE load_from (a text, b text, c double precision, d text, e text, f text) USING iceberg WITH (load_from='{url}')
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

    result = run_query(
        "SELECT count(*) FROM iceberg_tables WHERE table_name = 'load_from'", pg_conn
    )
    assert len(result) == 1

    pg_conn.rollback()


def test_create_table_load_from_invalid_url(
    pg_conn, extension, app_user, s3, with_default_location
):
    notexists_url = (
        f"s3://{TEST_BUCKET}/test_create_table_load_from_invalid_url/data.parquet"
    )

    # non-existent URL
    error = run_command(
        f"""
        CREATE TABLE hit404 () USING iceberg WITH (load_from = '{notexists_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  HTTP Error: Unable to connect to URL")

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE hit404 () USING iceberg WITH (definition_from = '{notexists_url}')
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
        CREATE TABLE hitgopher () USING iceberg WITH (load_from = '{invalid_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: only s3://, gs://, az://, azure://, and abfss:// URLs are currently supported"
    )

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE hitgopher () USING iceberg WITH (definition_from = '{invalid_url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: only s3://, gs://, az://, azure://, and abfss:// URLs are currently supported"
    )

    pg_conn.rollback()


def test_create_table_if_exists(pg_conn, duckdb_conn, azure, with_default_location):
    url = f"az://{TEST_BUCKET}/test_create_table_load_from_table_exists/data.parquet"

    # Create the same table twice
    error = run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE exists () USING iceberg WITH (definition_from = '{url}');
        CREATE TABLE exists () USING iceberg WITH (load_from = '{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    print(error)
    assert error.startswith('ERROR:  relation "exists" already exists')

    pg_conn.rollback()

    # IF NOT EXISTS does not help
    error = run_command(
        f"""
        CREATE TABLE IF NOT EXISTS exists () USING iceberg WITH (load_from = '{url}');
        CREATE TABLE IF NOT EXISTS exists () USING iceberg WITH (load_from = '{url}');
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
        CREATE TABLE IF NOT EXISTS exists () USING iceberg WITH (definition_from = '{url}');
        CREATE TABLE IF NOT EXISTS exists () USING iceberg WITH (definition_from = '{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert (
        "CREATE TABLE IF NOT EXISTS cannot be used with the definition_from option"
        in error
    )

    pg_conn.rollback()


def test_create_table_combined(pg_conn, duckdb_conn, s3, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_create_table_combined/data.parquet"

    # Use both options
    error = run_command(
        f"""
        CREATE TABLE combined1 () USING iceberg WITH (load_from='{url}', definition_from='{url}')
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: cannot specify both load_from and definition_from table options"
    )

    pg_conn.rollback()


def test_create_table_noperm(superuser_conn, duckdb_conn, s3, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_create_table_noperm/data.parquet"

    duckdb_conn.execute(
        f"COPY (SELECT generate_series, 'access' FROM generate_series(1,10)) TO '{url}'"
    )

    # Create user1 without privileges
    error = run_command(
        f"""
        CREATE ROLE user1;
        GRANT ALL ON SCHEMA public TO user1;
        SET ROLE user1;
        CREATE TABLE access () USING iceberg WITH (definition_from='{url}');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  permission denied to read from URL")

    superuser_conn.rollback()


def test_create_table_perm(superuser_conn, duckdb_conn, s3, with_default_location):

    url = f"s3://{TEST_BUCKET}/test_create_table_noperm/data.parquet"
    loc = f"s3://{TEST_BUCKET}/test_create_table_noperm/new_data"

    duckdb_conn.execute(
        f"COPY (SELECT generate_series, 'access' FROM generate_series(1,10)) TO '{url}'"
    )

    # Create user2 with privileges
    error = run_command(
        f"""
        CREATE ROLE user2;
        GRANT ALL ON SCHEMA public TO user2;
        GRANT lake_read_write TO user2;
        SET ROLE user2;
        CREATE TABLE access () USING iceberg WITH (location='{loc}', load_from='{url}');
    """,
        superuser_conn,
    )

    result = run_query("SELECT count(*) FROM access", superuser_conn)
    assert result[0]["count"] == 10

    superuser_conn.rollback()


def test_create_iceberg_table_from_csv(
    pg_conn, superuser_conn, app_user, with_default_location, s3
):
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
        CREATE TABLE csv.test_csv () USING iceberg WITH (load_from='{url}', freeze)
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

        CREATE TABLE csv.test_csv () USING iceberg
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


def test_create_iceberg_table_from_json(
    pg_conn, superuser_conn, app_user, with_default_location, s3
):
    url = f"s3://{TEST_BUCKET}/test_create_temp_table/data.json"
    json = '{"hello":5}'

    run_command(
        f"""
        CREATE SCHEMA json;
        GRANT USAGE, CREATE on SCHEMA json TO {app_user}
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    # Generate a simple CSV and load it using auto_detect
    run_command(
        f"""
        COPY (SELECT s, '2020-01-01 04:00:00'::timestamptz t, 3.4 d, 55000000000 b, 'hello' h, '{json}'::jsonb j FROM generate_series(1,10) s)
        TO '{url}';
        CREATE TABLE json.test_json () USING iceberg WITH (load_from='{url}', freeze)
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'json.test_json'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 6
    assert result == [
        ["s", "bigint"],
        ["t", "text"],
        ["d", "double precision"],
        ["b", "bigint"],
        ["h", "text"],
        ["j", "jsonb"],
    ]

    result = run_query("SELECT count(*) FROM json.test_json", pg_conn)
    assert result[0]["count"] == 10
    pg_conn.rollback()

    run_command(f"DROP SCHEMA json", superuser_conn)
    superuser_conn.commit()


def test_plpgsql(pg_conn, app_user, with_default_location, azure):
    child_url = f"az://{TEST_BUCKET}/test_plpgsql/child.parquet"

    run_command(
        f"""
        CREATE TABLE tables (
            table_name text,
            url text
        );

        CREATE OR REPLACE FUNCTION create_child() RETURNS trigger AS $create_child$
            BEGIN
                EXECUTE format('CREATE TABLE %s () USING iceberg WITH (load_from = %L)', NEW.table_name, NEW.url);
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


def test_nested_copy_to(pg_conn, app_user, with_default_location, s3):
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

        CREATE TABLE exports (id int, url text) USING iceberg;
        INSERT INTO exports VALUES (1, '{inner1_url}');
        INSERT INTO exports VALUES (2, '{inner2_url}');
        COPY (SELECT copy_to(url) FROM exports) TO '{outer_url}';
        CREATE TABLE tmp_inner1 () USING iceberg WITH (load_from = '{inner1_url}');
        CREATE TABLE tmp_inner2 () USING iceberg WITH (load_from = '{inner2_url}');
        CREATE TABLE tmp_outer () USING iceberg WITH (load_from = '{outer_url}');
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


def test_create_and_modify_iceberg_table_from_iceberg_table(
    pg_conn, spark_generated_iceberg_test, app_user, with_default_location, s3
):
    iceberg_table_folder = (
        iceberg_sample_table_folder_path() + "/public/spark_generated_iceberg_test"
    )
    iceberg_table_metadata_location = (
        "s3://"
        + TEST_BUCKET
        + "/spark_test/public/spark_generated_iceberg_test/metadata/00009-5c29aedb-463b-4b80-b0d5-c1d7fc957770.metadata.json"
    )
    run_command(
        f"""
        create schema spark_gen_cp;
        create table spark_gen_cp.spark_generated_iceberg_test () USING iceberg WITH (load_from='{iceberg_table_metadata_location}')
    """,
        pg_conn,
    )
    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_cp.spark_generated_iceberg_test'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result == [["id", "bigint"]]

    result = run_query(
        "SELECT count(*) FROM spark_gen_cp.spark_generated_iceberg_test", pg_conn
    )
    assert result[0][0] == 110

    # we deleted row 3
    result = run_query(
        "SELECT count(*) from spark_gen_cp.spark_generated_iceberg_test WHERE id = 3;",
        pg_conn,
    )
    assert result[0][0] == 0

    result = run_query(
        "SELECT id, count(*) from spark_gen_cp.spark_generated_iceberg_test GROUP BY id ORDER BY 2 DESC, 1 DESC LIMIT 4",
        pg_conn,
    )
    assert len(result) == 4
    assert result[0][0] == 5 and result[0][1] == 4
    assert result[1][0] == 4 and result[1][1] == 4
    assert result[2][0] == 2 and result[2][1] == 4
    assert result[3][0] == 1 and result[3][1] == 4

    # now, update all rows
    run_command("UPDATE spark_gen_cp.spark_generated_iceberg_test SET id = -1", pg_conn)
    result = run_query(
        "SELECT DISTINCT id from spark_gen_cp.spark_generated_iceberg_test", pg_conn
    )
    assert len(result) == 1
    assert result[0][0] == -1

    result = run_query(
        "SELECT count(*) FROM iceberg_tables WHERE table_name = 'spark_generated_iceberg_test'",
        pg_conn,
    )
    assert len(result) == 1

    run_command("DROP SCHEMA spark_gen_cp CASCADE", pg_conn)
    pg_conn.commit()


def test_create_iceberg_table_load_from_iceberg_metadata(
    pg_conn, extension, app_user, s3, with_default_location
):
    run_command(
        f"""
        CREATE TABLE source_iceberg_table (a numeric(4,2), b numeric, c numeric(4,2)[], d numeric[]) USING iceberg;
        INSERT INTO source_iceberg_table VALUES (3.4, 3.4);
    """,
        pg_conn,
    )
    pg_conn.commit()

    source_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'source_iceberg_table'",
        pg_conn,
    )[0][0]

    run_command(
        f"""
        CREATE TABLE load_from_iceberg_table () USING iceberg WITH (load_from='{source_metadata_location}')
    """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name, atttypmod typmod from pg_attribute
            where attrelid = 'load_from_iceberg_table'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 4
    assert result == [
        ["a", "numeric", 262150],
        ["b", "numeric", 2490381],
        ["c", "numeric[]", 262150],
        ["d", "numeric[]", 2490381],
    ]

    a_typmod = result[0][2]
    b_typmod = result[1][2]
    c_typmod = result[2][2]
    d_typmod = result[3][2]

    result = run_query(
        f"""select numerictypmodout({a_typmod}), numerictypmodout({b_typmod}),
                                  numerictypmodout({c_typmod}), numerictypmodout({d_typmod})""",
        pg_conn,
    )
    assert result[0][0] == "(4,2)"
    assert result[0][1] == "(38,9)"
    assert result[0][2] == "(4,2)"
    assert result[0][3] == "(38,9)"

    result = run_query("SELECT count(*) FROM load_from_iceberg_table", pg_conn)
    assert result[0]["count"] == 1

    result = run_query(
        "SELECT count(*) FROM iceberg_tables WHERE table_name = 'load_from_iceberg_table'",
        pg_conn,
    )
    assert len(result) == 1

    run_command("DROP TABLE source_iceberg_table", pg_conn)
    run_command("DROP TABLE load_from_iceberg_table", pg_conn)

    pg_conn.commit()

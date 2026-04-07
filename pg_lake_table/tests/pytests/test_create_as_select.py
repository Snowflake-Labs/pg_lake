import pytest
from utils_pytest import *


COMPLEX_SELECT = """
    SELECT  i AS id,
            'pg_lake' AS name,
            array['abc','def']::text[],
            array[(i::text, i, i::double precision)]::test_createas_schema_1.fully_qual_type[] as custom_arr,
            array[(i::text, i, i::double precision)]::not_qual_type[] as custom_arr2,
            array[(1, 1), (2, 2), (3, 3)]::map_type.key_int_val_int as map1,
            array[('abc', 1), ('def', 2), ('ghi', 3)]::map_type.key_varchar_val_int as map2,
            i::NUMERIC(20, 0) AS price,
            i::REAL,
            not_qual_fn(),
            '2024-03-13 13:00:00'::TIMESTAMP,
            '2024-03-13 14:00:00+03'::TIMESTAMPTZ as with_tz,
            '14:00:00'::TIME as with_time,
            '2022-10-6'::date as with_date,
            12 as "int quoted",
            ARRAY[i, i] as array_int,
            1::bit,
            '\\x0001'::bytea,
            (-12)::oid,
            'acd661ca-d18c-42e2-9c4e-61794318935e'::uuid,
            '{"hello":"world"}'::json,
            '{"hello":"world"}'::jsonb
    FROM generate_series(1, 10) i
"""


def test_no_location(s3, pg_conn, extension):
    error = run_command(
        f"""
        CREATE TABLE test_no_location USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )
    assert '"location" option is required for pg_lake_iceberg tables' in error

    pg_conn.rollback()


def test_unsupported_url(s3, pg_conn, extension):
    location = f"test_create_as_select/"

    error = run_command(
        f"""
        CREATE TABLE test_unsupported_url USING iceberg
         WITH (location = '{location}')
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )
    assert "only s3://, gs://, az://, azure://, and abfss:// URLs" in error

    pg_conn.rollback()


def test_temp_table(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE TEMP TABLE test_temp_table USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert "temporary tables are not allowed" in error

    pg_conn.rollback()


def test_matview(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE MATERIALIZED VIEW test_matview USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert "materialized views are not allowed" in error

    pg_conn.rollback()


def test_with_tablespace(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE TABLE test_with_tablespace USING iceberg
        TABLESPACE xx
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert "tablespace is not supported" in error

    pg_conn.rollback()


def test_with_unsupported_option(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE TABLE test_with_unsupported_option USING iceberg
         WITH (unsupported_option = 'value')
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert 'invalid option "unsupported_option"' in error

    pg_conn.rollback()


def test_as_execute(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        PREPARE test_execute(int) AS SELECT i AS id, 'pg_lake' AS name
                                      FROM generate_series(1, 10) i
                                      WHERE i > $1
    """,
        pg_conn,
    )

    error = run_command(
        f"""
        CREATE TABLE test_as_execute USING iceberg
        AS EXECUTE test_execute(5)
    """,
        pg_conn,
        raise_error=False,
    )

    assert "CREATE TABLE AS EXECUTE is not supported" in error

    pg_conn.rollback()


def test_more_than_once_same_cols(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE TABLE test_more_than_once_same_cols USING iceberg
        AS SELECT i AS id, i AS id
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert 'column "id" specified more than once' in error

    pg_conn.rollback()


def test_unlogged_table(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE UNLOGGED TABLE test_unlogged_table USING iceberg
        AS SELECT i AS id, i AS id
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert "unlogged tables are not allowed" in error

    pg_conn.rollback()


def test_basic_iceberg_table(
    s3, pg_conn, extension, create_heap_table, with_default_location
):
    error = run_command(
        f"""
        CREATE TABLE test_iceberg_table USING iceberg
        AS {COMPLEX_SELECT}
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_iceberg_table ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["test_iceberg_table"], ["heap_table"]
    )

    pg_conn.rollback()


def test_with_no_data(s3, pg_conn, extension, create_heap_table, with_default_location):
    error = run_command(
        f"""
        CREATE TABLE test_iceberg_table_with_no_data USING iceberg
        AS {COMPLEX_SELECT}
        WITH NO DATA
    """,
        pg_conn,
        raise_error=False,
    )

    query = "SELECT * FROM test_iceberg_table_with_no_data LIMIT 1"
    result = run_query(query, pg_conn)
    assert len(result) == 0

    assert_remote_query_contains_expression(query, "LIMIT", pg_conn)

    run_command("DROP TABLE test_iceberg_table_with_no_data", pg_conn)


def test_if_not_exists(
    s3, pg_conn, extension, create_heap_table, with_default_location
):
    run_command(
        f"""
        CREATE TABLE test_if_not_exists USING iceberg
        AS {COMPLEX_SELECT}
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE IF NOT EXISTS test_if_not_exists USING iceberg
        AS {COMPLEX_SELECT}
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_if_not_exists ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["test_if_not_exists"], ["heap_table"]
    )

    run_command("DROP TABLE test_if_not_exists", pg_conn)


def test_already_exists(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_already_exists USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    error = run_command(
        f"""
        CREATE TABLE test_already_exists USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )
    assert "already exists" in error

    pg_conn.rollback()


def test_select_from_table(
    s3, pg_conn, extension, create_heap_table, with_default_location
):
    run_command(
        f"""
        CREATE TABLE test_select_from_table USING iceberg
        AS SELECT * FROM heap_table
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_select_from_table ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["test_select_from_table"], ["heap_table"]
    )

    run_command("DROP TABLE test_select_from_table", pg_conn)


def test_select_from_pg_lake_table(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_table USING iceberg
        AS {COMPLEX_SELECT}
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_select_from_pg_lake_table USING iceberg
        AS SELECT * FROM test_table
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_select_from_pg_lake_table ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["test_select_from_pg_lake_table"], ["test_table"]
    )

    run_command("DROP TABLE test_table", pg_conn)
    run_command("DROP TABLE test_select_from_pg_lake_table", pg_conn)


def test_as_table(s3, pg_conn, extension, create_heap_table, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_as_table USING iceberg
        AS TABLE heap_table
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_as_table ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(query, pg_conn, ["test_as_table"], ["heap_table"])

    run_command("DROP TABLE test_as_table", pg_conn)
    pg_conn.rollback()


def test_as_values(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_as_values USING iceberg
        AS VALUES (1, 'pg_lake'), (2, 'lake')
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_as_values LIMIT 4"
    result = run_query(query, pg_conn)
    assert len(result) == 2

    assert_remote_query_contains_expression(query, "LIMIT", pg_conn)

    run_command("DROP TABLE test_as_values", pg_conn)


def test_no_create_access(s3, extension, superuser_conn, with_default_location):
    run_command("CREATE USER no_create_table_user", superuser_conn)
    run_command("CREATE SCHEMA test_schema", superuser_conn)
    run_command(
        "REVOKE ALL ON SCHEMA test_schema FROM no_create_table_user", superuser_conn
    )
    run_command(
        "GRANT USAGE ON SCHEMA test_schema TO no_create_table_user", superuser_conn
    )
    run_command(
        "GRANT USAGE ON FOREIGN SERVER pg_lake_iceberg TO no_create_table_user",
        superuser_conn,
    )
    run_command("GRANT lake_read TO no_create_table_user", superuser_conn)
    run_command("SET ROLE no_create_table_user", superuser_conn)

    error = run_command(
        f"""
        CREATE TABLE test_schema.test_no_create_access USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "permission denied" in error

    superuser_conn.rollback()


def test_with_create_access(s3, superuser_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_with_create_access/"

    run_command("CREATE SCHEMA test_schema", superuser_conn)

    run_command("CREATE USER with_create_table_user", superuser_conn)
    run_command(
        "GRANT ALL ON SCHEMA test_schema,lake_iceberg TO with_create_table_user",
        superuser_conn,
    )
    run_command(
        "GRANT ALL ON TABLE lake_iceberg.tables TO with_create_table_user",
        superuser_conn,
    )
    run_command(
        "GRANT USAGE ON FOREIGN SERVER pg_lake_iceberg TO with_create_table_user",
        superuser_conn,
    )
    run_command("GRANT lake_read_write TO with_create_table_user", superuser_conn)
    run_command("SET ROLE with_create_table_user", superuser_conn)

    run_command(
        f"""
        CREATE TABLE test_schema.test_with_create_access USING iceberg
         WITH (location = '{location}')
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        superuser_conn,
    )

    result = run_query(
        "SELECT COUNT(*) = 10 FROM test_schema.test_with_create_access", superuser_conn
    )
    assert result[0][0] == True

    run_command("RESET ROLE", superuser_conn)
    run_command("DROP OWNED BY with_create_table_user", superuser_conn)
    run_command("DROP USER with_create_table_user", superuser_conn)
    run_command("DROP SCHEMA test_schema CASCADE", superuser_conn)


def test_unusual_table_name(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE "test_   7=unuSual_table_name" USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    result = run_query(
        'SELECT COUNT(*) = 10 FROM "test_   7=unuSual_table_name"', pg_conn
    )
    assert result[0][0] == True

    run_command('DROP TABLE "test_   7=unuSual_table_name"', pg_conn)


def test_reserved_column_names(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE TABLE reserved_column_names USING iceberg
        AS SELECT i AS id, 'pg_lake' AS _pg_lake_filename
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert 'column name "_pg_lake_filename" is reserved' in error

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE reserved_column_names USING iceberg
        AS SELECT i AS file_row_number, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
        raise_error=False,
    )

    assert 'column name "file_row_number" is reserved' in error

    pg_conn.rollback()


def test_pg_lake_iceberg_access_method_unusable(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        f"""
        CREATE TABLE test_table(id int)
    """,
        pg_conn,
    )

    error = run_command(
        f"""
        ALTER TABLE test_table SET ACCESS METHOD pg_lake_iceberg
    """,
        pg_conn,
        raise_error=False,
    )

    assert "not supported" in error

    pg_conn.rollback()


def test_default_table_access_method_pg_lake_iceberg(
    s3, pg_conn, extension, create_heap_table, with_default_location
):
    run_command(
        f"""
        SET default_table_access_method = 'pg_lake_iceberg'
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_default_table_access_method
        AS SELECT * FROM heap_table
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_default_table_access_method ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["test_default_table_access_method"], ["heap_table"]
    )

    run_command("RESET default_table_access_method", pg_conn)
    run_command("DROP TABLE test_default_table_access_method", pg_conn)

    pg_conn.commit()


def test_default_table_access_method_iceberg(
    s3, pg_conn, extension, create_heap_table, with_default_location
):
    run_command(
        f"""
        SET default_table_access_method = 'iceberg'
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_default_table_access_method
        AS SELECT * FROM heap_table
    """,
        pg_conn,
    )

    query = "SELECT * FROM test_default_table_access_method ORDER BY id"
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query, pg_conn, ["test_default_table_access_method"], ["heap_table"]
    )

    run_command("RESET default_table_access_method", pg_conn)
    run_command("DROP TABLE test_default_table_access_method", pg_conn)

    pg_conn.commit()


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_heap_table(pg_conn, s3, request, create_types):
    run_command(
        f"""
        CREATE TABLE heap_table
        AS {COMPLEX_SELECT}
    """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP TABLE heap_table", pg_conn)

    pg_conn.commit()


@pytest.fixture(scope="module")
def create_types(pg_conn, s3, request):
    run_command(
        f"""
        CREATE SCHEMA test_createas_schema_1;
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TYPE test_createas_schema_1.fully_qual_type AS (field1 text, field2 int, field3 double precision);
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE SCHEMA test_createas_schema_2;
    """,
        pg_conn,
    )

    run_command(
        f"""
        SET search_path TO test_createas_schema_2, public;
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TYPE not_qual_type AS (field1 text, field2 int, field3 double precision);
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION not_qual_fn() RETURNS double precision AS $$
        BEGIN
            RETURN 1.0;
        END;
        $$ LANGUAGE plpgsql;
    """,
        pg_conn,
    )

    create_map_type("integer", "integer")
    create_map_type("varchar", "integer")

    pg_conn.commit()

    yield

    run_command("RESET search_path", pg_conn)
    run_command("DROP SCHEMA test_createas_schema_1 CASCADE", pg_conn)
    run_command("DROP SCHEMA test_createas_schema_2 CASCADE", pg_conn)

    pg_conn.commit()

import pytest

from utils_pytest import *


def test_create_table_with_specified_location(
    s3, pg_conn, extension, grant_access_to_tables_internal
):
    location = f"s3://{TEST_BUCKET}/test_basic_create_table/"

    run_command(
        f"""CREATE TABLE test_basic_create_table (a int, b int)
                    USING iceberg
                    WITH (location = '{location}')""",
        pg_conn,
    )

    run_command(
        """INSERT INTO test_basic_create_table
                    SELECT i, i+1 FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = "SELECT * FROM test_basic_create_table WHERE a <= 10 ORDER BY a"
    expected_expression = "WHERE (a <= 10)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 8],
        [8, 9],
        [9, 10],
        [10, 11],
    ]

    metadata_location = run_query(
        """SELECT metadata_location FROM iceberg_tables
                                    WHERE table_name = 'test_basic_create_table'
                                 """,
        pg_conn,
    )[0][0]

    assert f"s3://{TEST_BUCKET}/test_basic_create_table/metadata/" in metadata_location

    has_custom_location = run_query(
        """SELECT has_custom_location FROM lake_iceberg.tables_internal
                                    WHERE table_name = 'test_basic_create_table'::regclass
                                 """,
        pg_conn,
    )[0][0]
    assert has_custom_location == True

    pg_conn.rollback()


def test_create_table_at_non_empty_location(s3, pg_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_create_table_at_non_empty_location/"

    run_command(
        f"""CREATE TABLE test_basic_create_table_1 (a int, b int)
                    USING iceberg
                    WITH (location = '{location}')""",
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        f"""CREATE TABLE test_basic_create_table_2 (a int, b int)
                            USING iceberg
                            WITH (location = '{location}')""",
        pg_conn,
        raise_error=False,
    )

    assert "is not empty" in error

    pg_conn.rollback()


def test_create_table_via_create_schema(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    grant_access_to_tables_internal,
):
    url = f"s3://{TEST_BUCKET}/test_create_table_via_create_schema/data.parquet"

    run_command(
        f"""
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
                CREATE SCHEMA new_schema
                CREATE TABLE new_table (
                    id SERIAL ,
                    name text NOT NULL
                ) USING iceberg
                CREATE TABLE another_table (
                    id SERIAL ,
                    description TEXT
                ) USING iceberg

                CREATE TABLE third_table (
                    id INT ,
                    description TEXT
                ) USING iceberg WITH (load_from='{url}')

                ;
        """,
        pg_conn,
    )

    run_command(
        """INSERT INTO new_schema.new_table (name)
                    SELECT i::text FROM generate_series(1,100) i""",
        pg_conn,
    )

    run_command(
        """INSERT INTO new_schema.another_table (description)
                    SELECT i::text FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = "SELECT * FROM new_schema.new_table WHERE id <= 5 ORDER BY id"
    expected_expression = "WHERE (id <= 5)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    result = run_query(query, pg_conn)
    assert result == [[1, "1"], [2, "2"], [3, "3"], [4, "4"], [5, "5"]]

    query = "SELECT * FROM new_schema.another_table WHERE id <= 5 ORDER BY id"
    expected_expression = "WHERE (id <= 5)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    result = run_query(query, pg_conn)
    assert result == [[1, "1"], [2, "2"], [3, "3"], [4, "4"], [5, "5"]]

    query = "SELECT * FROM new_schema.third_table WHERE id <= 5 ORDER BY id"
    expected_expression = "WHERE (id <= 5)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    result = run_query(query, pg_conn)
    assert result == [[1, "1"], [2, "2"], [3, "3"], [4, "4"], [5, "5"]]

    has_custom_location = run_query(
        """SELECT has_custom_location FROM lake_iceberg.tables_internal
                                    WHERE table_name = 'new_schema.new_table'::regclass
                                 """,
        pg_conn,
    )[0][0]
    assert has_custom_location == False

    run_command("DROP SCHEMA new_schema CASCADE", pg_conn)
    pg_conn.rollback()


def test_create_table_with_default_location(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        f"""CREATE TABLE test_create_table_with_default_location (a int, b int)
                    USING iceberg""",
        pg_conn,
    )

    run_command(
        """INSERT INTO test_create_table_with_default_location
                    SELECT i, i+1 FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = (
        "SELECT * FROM test_create_table_with_default_location WHERE a <= 10 ORDER BY a"
    )
    expected_expression = "WHERE (a <= 10)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 8],
        [8, 9],
        [9, 10],
        [10, 11],
    ]

    dbname = run_query("SELECT current_database()", pg_conn)[0][0]

    # assert metadata location
    result = run_query(
        """SELECT metadata_location FROM iceberg_tables
                           WHERE table_name = 'test_create_table_with_default_location'
                       """,
        pg_conn,
    )
    first_table_metadata_location = result[0][0]

    table_oid = run_query(
        """SELECT oid FROM pg_class
                            WHERE relname = 'test_create_table_with_default_location'
                          """,
        pg_conn,
    )[0][0]

    assert (
        f"s3://{TEST_BUCKET}/frompg/tables/{dbname}/public/test_create_table_with_default_location/{table_oid}"
        in first_table_metadata_location
    )

    # drop the table and create it again
    run_command("DROP TABLE test_create_table_with_default_location", pg_conn)

    run_command(
        f"""CREATE TABLE test_create_table_with_default_location (a int, b int)
                    USING iceberg""",
        pg_conn,
    )

    run_command(
        """INSERT INTO test_create_table_with_default_location
                    SELECT i, i+1 FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = (
        "SELECT * FROM test_create_table_with_default_location WHERE a <= 10 ORDER BY a"
    )
    expected_expression = "WHERE (a <= 10)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 8],
        [8, 9],
        [9, 10],
        [10, 11],
    ]

    # assert metadata location
    result = run_query(
        """SELECT metadata_location FROM iceberg_tables
                            WHERE table_name = 'test_create_table_with_default_location'
                        """,
        pg_conn,
    )
    second_table_metadata_location = result[0][0]

    table_oid = run_query(
        """SELECT oid FROM pg_class
                            WHERE relname = 'test_create_table_with_default_location'
                          """,
        pg_conn,
    )[0][0]

    assert (
        f"s3://{TEST_BUCKET}/frompg/tables/{dbname}/public/test_create_table_with_default_location/{table_oid}"
        in second_table_metadata_location
    )

    # even if the table is created with the same name, the metadata location should be different due to random uuid suffix
    assert first_table_metadata_location != second_table_metadata_location

    pg_conn.rollback()


@pytest.mark.location_prefix(
    f"s3://{TEST_BUCKET}/test_create_table_with_default_location_override"
)
def test_create_table_with_default_location_override(
    s3, pg_conn, extension, with_default_location
):
    run_command(
        f"""CREATE TABLE test_create_table_with_default_location (a int, b int)
                    USING iceberg""",
        pg_conn,
    )

    run_command(
        """INSERT INTO test_create_table_with_default_location
                    SELECT i, i+1 FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = (
        "SELECT * FROM test_create_table_with_default_location WHERE a <= 10 ORDER BY a"
    )
    expected_expression = "WHERE (a <= 10)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 8],
        [8, 9],
        [9, 10],
        [10, 11],
    ]

    dbname = run_query("SELECT current_database()", pg_conn)[0][0]

    # assert metadata location
    result = run_query(
        """SELECT metadata_location FROM iceberg_tables
                           WHERE table_name = 'test_create_table_with_default_location'
                       """,
        pg_conn,
    )
    assert (
        f"s3://{TEST_BUCKET}/test_create_table_with_default_location_override/frompg/tables/{dbname}/public/test_create_table_with_default_location"
        in result[0][0]
    )

    run_command("DROP TABLE test_create_table_with_default_location", pg_conn)
    pg_conn.rollback()


def test_create_table_with_wrong_default_location(s3, pg_conn, extension):
    location = f"invalid_loc/"

    error = run_command(
        f"SET pg_lake_iceberg.default_location_prefix = '{location}'",
        pg_conn,
        raise_error=False,
    )

    assert (
        'invalid value for parameter "pg_lake_iceberg.default_location_prefix"' in error
    )

    pg_conn.rollback()

    location = f"s3://{TEST_BUCKET}/writable_iceberg/?region=us-west-2"

    error = run_command(
        f"SET pg_lake_iceberg.default_location_prefix = '{location}'",
        pg_conn,
        raise_error=False,
    )

    assert (
        'invalid value for parameter "pg_lake_iceberg.default_location_prefix"' in error
    )

    pg_conn.rollback()


def test_create_table_with_wrong_location(s3, pg_conn, extension):

    location = f"s3://{TEST_BUCKET}/writable_iceberg/?region=us-west-2"

    error = run_command(
        f"""CREATE TABLE test_basic_create_table (a int, b int)
                    USING iceberg
                    WITH (location = '{location}')""",
        pg_conn,
        raise_error=False,
    )

    assert "s3 configuration parameters are not allowed in the" in error

    pg_conn.rollback()


def test_create_table_like(s3, pg_conn, extension, with_default_location):
    run_command(f"""CREATE TABLE test_test_table (a int, b int)""", pg_conn)
    run_command(f"""CREATE TYPE test_test_type AS (e text, f timestamptz)""", pg_conn)

    error = run_command(
        f"""CREATE TABLE test_create_table_like
                    (LIKE test_test_table)
                    USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert error is None

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_create_table_like'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [
        ["a", "integer"],
        ["b", "integer"],
    ]

    # multiple like columns, intermixed with other columns
    error = run_command(
        f"""CREATE TABLE test_create_table_like2
                    (c text, LIKE test_test_table, d float8 not null, LIKE test_test_type)
                    USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert error is None

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_create_table_like2'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [
        ["c", "text"],
        ["a", "integer"],
        ["b", "integer"],
        ["d", "double precision"],
        ["e", "text"],
        ["f", "timestamp with time zone"],
    ]

    # like iceberg table, using iceberg
    error = run_command(
        f"""CREATE TABLE test_create_table_like3
                    (LIKE test_create_table_like2)
                    USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert error is None

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_create_table_like3'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [
        ["c", "text"],
        ["a", "integer"],
        ["b", "integer"],
        ["d", "double precision"],
        ["e", "text"],
        ["f", "timestamp with time zone"],
    ]

    # like iceberg table normal heap table
    error = run_command(
        f"""CREATE TABLE test_create_table_like4
                    (LIKE test_create_table_like2)
                    USING heap""",
        pg_conn,
        raise_error=False,
    )

    assert error is None

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_create_table_like4'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [
        ["c", "text"],
        ["a", "integer"],
        ["b", "integer"],
        ["d", "double precision"],
        ["e", "text"],
        ["f", "timestamp with time zone"],
    ]

    # duplicate columns equals errors
    error = run_command(
        f"""CREATE TABLE test_create_table_like4
                    (LIKE test_test_table, a int)
                    USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert "specified more than once" in error

    pg_conn.rollback()


def test_create_partition_of_table(s3, pg_conn, extension, with_default_location):
    run_command(
        "CREATE TABLE test_parent(a int, b int) PARTITION BY RANGE (a)", pg_conn
    )

    run_command(
        f"""CREATE TABLE test_create_table_partition_of
                    PARTITION OF test_parent FOR VALUES FROM (1) TO (11)
                    USING iceberg""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_create_table_partition_of_others
                    PARTITION OF test_parent FOR VALUES FROM (11) TO (101)""",
        pg_conn,
    )

    run_command(
        """INSERT INTO test_parent
                    SELECT i, i+1 FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = "SELECT * FROM test_create_table_partition_of ORDER BY a"
    expected_expression = "ORDER BY a"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 8],
        [8, 9],
        [9, 10],
        [10, 11],
    ]

    run_command("DROP TABLE test_create_table_partition_of, test_parent", pg_conn)
    pg_conn.rollback()


def test_create_child_table(s3, pg_conn, extension, with_default_location):
    run_command("CREATE TABLE test_parent(a int)", pg_conn)

    run_command(
        f"""CREATE TABLE test_create_child_table(b int, c int)
                    INHERITS (test_parent)
                    USING iceberg""",
        pg_conn,
    )

    run_command(
        """INSERT INTO test_create_child_table
                    SELECT i,i+1,i+2 FROM generate_series(1,100) i""",
        pg_conn,
    )

    query = "SELECT * FROM test_create_child_table WHERE a <= 10 ORDER BY a"
    expected_expression = "WHERE (a <= 10)"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [4, 5, 6],
        [5, 6, 7],
        [6, 7, 8],
        [7, 8, 9],
        [8, 9, 10],
        [9, 10, 11],
        [10, 11, 12],
    ]

    run_command("DROP TABLE test_create_child_table, test_parent", pg_conn)
    pg_conn.rollback()


def test_create_partitioned_table(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""CREATE TABLE test_create_table_partitioned (a int, b int)
                             PARTITION BY RANGE (b) USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert f"partitioned tables are not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_temp_table(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""CREATE TEMP TABLE test_create_temp_table (a int, b int)
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert f"temporary tables are not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_unlogged_table(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""CREATE UNLOGGED TABLE test_create_unlogged_table (a int, b int)
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert f"unlogged tables are not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_table_of_type(s3, pg_conn, extension, with_default_location):
    run_command("CREATE TYPE test_type AS (a int, b int)", pg_conn)

    error = run_command(
        f"""CREATE TABLE test_create_table_of_type OF test_type
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert f"CREATE TABLE OF is not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_table_with_tablespace(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""CREATE TABLE test_create_table_with_tablespace (a int, b int)
                             USING iceberg TABLESPACE xx""",
        pg_conn,
        raise_error=False,
    )

    assert f"tablespace is not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_table_with_column_storage(
    s3, pg_conn, extension, with_default_location
):
    error = run_command(
        f"""CREATE TABLE test_create_table_with_column_storage (a int storage plain, b int)
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert f"column storage is not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_table_with_column_compression(
    s3, pg_conn, extension, with_default_location
):
    error = run_command(
        f"""CREATE TABLE test_create_table_with_column_compression (a int compression default, b int)
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert f"column compression is not allowed for pg_lake_iceberg tables" in error

    pg_conn.rollback()


def test_create_table_with_unsupported_storage_options(
    s3, pg_conn, extension, with_default_location
):
    error = run_command(
        f"""CREATE TABLE test_create_table_with_options (a int, b int)
                             USING iceberg
                             WITH (fillfactor = 95)""",
        pg_conn,
        raise_error=False,
    )

    assert 'invalid option "fillfactor"' in error

    pg_conn.rollback()


def test_create_table_with_reserved_column_names(
    s3, pg_conn, extension, with_default_location
):
    error = run_command(
        f"""CREATE TABLE test_create_table_with_reserved_name (_pg_lake_filename int, b int)
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert (
        'column name "_pg_lake_filename" is reserved for pg_lake_iceberg tables'
        in error
    )

    pg_conn.rollback()

    error = run_command(
        f"""CREATE TABLE test_create_table_with_reserved_name (file_row_number int, b int)
                             USING iceberg""",
        pg_conn,
        raise_error=False,
    )

    assert (
        'column name "file_row_number" is reserved for pg_lake_iceberg tables' in error
    )

    pg_conn.rollback()

    location = f"s3://{TEST_BUCKET}/test_create_table_with_reserved_column_names/"

    error = run_command(
        f"""CREATE FOREIGN TABLE test_create_table_with_reserved_name (file_row_number int, b int)
                             SERVER pg_lake
                             OPTIONS (location '{location}', writable 'true', format 'parquet')""",
        pg_conn,
        raise_error=False,
    )

    assert 'column name "file_row_number" is reserved for pg_lake tables' in error

    pg_conn.rollback()

    # no errors for read only pg_lake tables
    path = f"s3://{TEST_BUCKET}/test.parquet"
    run_command(
        f"COPY (SELECT i as file_row_number, i as b from generate_series(1,10)i) TO '{path}'",
        pg_conn,
    )
    run_command(
        f"""CREATE FOREIGN TABLE test_create_table_with_reserved_name (file_row_number int, b int)
                             SERVER pg_lake
                             OPTIONS (path '{path}')""",
        pg_conn,
    )

    result = run_query(
        "select count(*) from test_create_table_with_reserved_name", pg_conn
    )
    assert result[0][0] == 10

    pg_conn.rollback()


def test_create_table_with_oids(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""CREATE TABLE test_create_table_with_oids (a int, b int)
                             USING iceberg
                             WITH (OIDS = true)""",
        pg_conn,
        raise_error=False,
    )

    assert "invalid option" in error

    pg_conn.rollback()


def test_create_table_with_unsupported_column_constraints(
    s3, pg_conn, extension, with_default_location
):
    disallowed_column_constraints = [
        ("a int unique", "unique"),
        ("a int primary key", "primary key"),
        ("a int references test_other_table(a)", "foreign key"),
    ]

    for constraint, constraint_name in disallowed_column_constraints:
        error = run_command(
            f"""CREATE TABLE test_create_table_with_constraints ({constraint}, b int)
                                USING iceberg""",
            pg_conn,
            raise_error=False,
        )
        assert (
            f"{constraint_name} constraints are not supported on foreign tables"
            in error
        )
        pg_conn.rollback()


def test_create_table_with_supported_column_constraints(
    s3, pg_conn, extension, with_default_location
):
    allowed_column_constraints = [
        "a int not null",
        "a int null",
        "a int default 0",
        "a int check (a > 0)",
        "a int generated always as (12) stored",
    ]

    for constraint in allowed_column_constraints:
        run_command(
            f"""CREATE TABLE test_create_table_with_constraints ({constraint}, b int)
                       USING iceberg""",
            pg_conn,
        )

        if "generated always" in constraint:
            run_command(
                """INSERT INTO test_create_table_with_constraints(b)
                        SELECT i+1 FROM generate_series(1,100) i""",
                pg_conn,
            )
        else:
            run_command(
                """INSERT INTO test_create_table_with_constraints
                        SELECT i,i+1 FROM generate_series(1,100) i""",
                pg_conn,
            )

        query = (
            "SELECT b FROM test_create_table_with_constraints WHERE b <= 10 ORDER BY b"
        )
        expected_expression = "WHERE (b <= 10)"
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)

        result = run_query(query, pg_conn)
        assert result == [[2], [3], [4], [5], [6], [7], [8], [9], [10]]

        run_command("DROP TABLE test_create_table_with_constraints", pg_conn)
        pg_conn.rollback()


def test_create_table_with_unsupported_table_constraints(
    s3, pg_conn, extension, with_default_location
):
    disallowed_table_constraints = [
        ("unique(a)", "unique"),
        ("primary key(a)", "primary key"),
        ("foreign key (a) references test_other_table(a)", "foreign key"),
    ]

    for constraint, constraint_name in disallowed_table_constraints:
        error = run_command(
            f"""CREATE TABLE test_create_table_with_constraints (a int, b int, {constraint})
                                USING iceberg""",
            pg_conn,
            raise_error=False,
        )
        assert (
            f"{constraint_name} constraints are not supported on foreign tables"
            in error
        )
        pg_conn.rollback()


def test_create_table_with_supported_table_constraints(
    s3, pg_conn, extension, with_default_location
):
    allowed_table_constraints = [
        "check (a > 0)",
    ]

    for constraint in allowed_table_constraints:
        run_command(
            f"""CREATE TABLE test_create_table_with_constraints (a int, b int, {constraint})
                       USING iceberg""",
            pg_conn,
        )

        run_command(
            """INSERT INTO test_create_table_with_constraints
                    SELECT i,i+1 FROM generate_series(1,100) i""",
            pg_conn,
        )

        query = (
            "SELECT b FROM test_create_table_with_constraints WHERE b <= 10 ORDER BY b"
        )
        expected_expression = "WHERE (b <= 10)"
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)

        result = run_query(query, pg_conn)
        assert result == [[2], [3], [4], [5], [6], [7], [8], [9], [10]]

        run_command("DROP TABLE test_create_table_with_constraints", pg_conn)
        pg_conn.rollback()


def test_create_iceberg_like_local(pg_conn, s3, extension, with_default_location):
    # CREATE FOREIGN TABLE LIKE local_table was introduced in PostgreSQL 18
    if get_pg_version_num(pg_conn) < 180000:
        return

    run_command(
        f"""
        CREATE TABLE test_local(a int default 10, b int not null, c int generated always as (a + b) stored);
        CREATE FOREIGN TABLE test_create_iceberg_like_local (LIKE test_local) SERVER pg_lake_iceberg;
        INSERT INTO test_create_iceberg_like_local SELECT i, i+1 FROM generate_series(1,100) i;
        """,
        pg_conn,
    )

    count = run_query("SELECT count(*) FROM test_create_iceberg_like_local", pg_conn)[
        0
    ][0]
    assert count == 100

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE test_local(a int default 10, b int not null, c int generated always as (a + b) stored);
        CREATE FOREIGN TABLE test_create_iceberg_like_local (LIKE test_local INCLUDING ALL) SERVER pg_lake_iceberg;
        """,
        pg_conn,
        raise_error=False,
    )

    assert "only basic" in error

    pg_conn.rollback()


def test_create_iceberg_with_virtual_column(
    pg_conn, s3, extension, with_default_location
):
    # virtual columns were introduced in PostgreSQL 18
    if get_pg_version_num(pg_conn) < 180000:
        return

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_with_virtual_column(a int default 10, bint generated always as (a + 1) virtual) using iceberg;
        """,
        pg_conn,
        raise_error=False,
    )

    assert "virtual" in error

    pg_conn.rollback()

    path = f"s3://{TEST_BUCKET}/test_create_iceberg_with_virtual_column/test.parquet"

    run_command(f"COPY (SELECT 1) TO '{path}'", pg_conn)

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_with_virtual_column(a int default 10, bint generated always as (a + 1) virtual) server pg_lake options (path '{path}');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "virtual" in error

    pg_conn.rollback()


@pytest.fixture(scope="function")
def grant_access_to_tables_internal(
    extension,
    app_user,
    superuser_conn,
):
    run_command(
        f"""GRANT SELECT ON lake_iceberg.tables_internal TO {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""REVOKE SELECT ON lake_iceberg.tables_internal FROM {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()

import pytest
import psycopg2
import duckdb
import math
from decimal import *
from utils_pytest import *
import io


def test_create_permissions_infer_columns(
    pg_conn, s3, extension, superuser_conn, app_user
):
    url = f"s3://{TEST_BUCKET}/test_create_permissions/data.p"

    # Set up a user and a file
    # create a map type with super user, but then also drop it
    # because we are going to test whether a regular user
    # can create a map type on create foreign table
    create_map_type("text", "integer")
    run_command(
        f"""
        CREATE ROLE user1;
        GRANT ALL ON SCHEMA public TO user1;
        COPY (SELECT s, 'hello-'||s AS h, array[('me', 1), ('myself', 2), ('i', 3)]::map_type.key_text_val_int as m_map FROM generate_series(1,10) s) TO '{url}' WITH (format 'parquet');
        DROP TYPE map_type.key_text_val_int CASCADE;
        GRANT user1 TO {app_user};
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    # Try to create a table from the file without permissions
    error = run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms () SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error

    pg_conn.rollback()

    # Grant permission to read
    run_command(
        f"""
        GRANT lake_read TO user1;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms () SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
    """,
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM test_perms", pg_conn)
    assert results[0]["count"] == 10

    # show that a non-superuser can create a map type
    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_perms'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [
        ["s", "integer"],
        ["h", "text"],
        ["m_map", "map_type.key_text_val_int"],
    ]

    pg_conn.rollback()

    run_command(
        """
        DROP OWNED BY user1;
        DROP ROLE user1;
    """,
        superuser_conn,
    )

    superuser_conn.commit()


def test_create_permissions_with_columns(
    pg_conn, s3, extension, superuser_conn, app_user
):
    url = f"s3://{TEST_BUCKET}/test_create_permissions/data.p"

    # Set up a user and a file
    run_command(
        f"""
        CREATE ROLE user1;
        GRANT ALL ON SCHEMA public TO user1;
        COPY (SELECT s, 'hello-'||s AS h FROM generate_series(1,10) s) TO '{url}' WITH (format 'parquet');
        GRANT user1 TO {app_user};
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    # Try to create a table from the file without permissions, with columns
    error = run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms (s bigint, h text) SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error

    pg_conn.rollback()

    # Try to create an iceberg table from the file without permissions, with columns
    error = run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms_iceberg (s bigint, h text) SERVER pg_lake_iceberg OPTIONS (location '{url}_loc');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error

    pg_conn.rollback()

    # read-only pg_lake tables can be created
    # with lake_read grant
    run_command(
        f"""
        GRANT lake_read TO user1;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms (s bigint, h text) SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
    """,
        pg_conn,
    )
    pg_conn.rollback()

    # iceberg tables cannot be created
    # with lake_read grant
    error = run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms_iceberg (s bigint, h text) SERVER pg_lake_iceberg OPTIONS (location '{url}_loc');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    # writable pg_lake tables cannot be created
    # with lake_read grant
    error = run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms (s bigint, h text) SERVER pg_lake OPTIONS (location '{url}', format 'parquet', writable 'true');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    # Now grant read_write, we can create both writable
    # pg_lake_table and iceberg tables
    run_command(
        f"""
        GRANT lake_read_write TO user1;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        SET ROLE user1;
        CREATE FOREIGN TABLE test_perms (s bigint, h text) SERVER pg_lake OPTIONS (location '{url}', format 'parquet', writable 'true');
        CREATE FOREIGN TABLE test_perms_iceberg (s bigint, h text) SERVER pg_lake_iceberg OPTIONS (location '{url}_loc');
    """,
        pg_conn,
    )

    pg_conn.rollback()

    run_command(
        """
        RESET ROLE;
        DROP OWNED BY user1;
        DROP ROLE user1;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_select_with_permissions(pg_conn, s3, extension, superuser_conn, app_user):
    url = f"s3://{TEST_BUCKET}/test_select_with_permissions/data.p"
    url_2 = f"s3://{TEST_BUCKET}/test_select_with_permissions_2/data.p"
    query = f"""
        SELECT tp.*, event_details.max_metric
        FROM test_select_perms.test_perms tp,
        (
            WITH EventCTE AS (
                SELECT s, MAX(h) AS max_metric
                FROM test_select_perms.test_perms_2
                GROUP BY s
            )
            SELECT s AS id, max_metric
            FROM EventCTE
        ) AS event_details
        WHERE tp.s = event_details.id;
        """
    # Set up a user and a file
    run_command(
        f"""
        CREATE ROLE user1;
        CREATE SCHEMA test_select_perms;
        GRANT ALL ON SCHEMA test_select_perms TO user1;
        COPY (SELECT s, 'hello-'||s AS h FROM generate_series(1,10) s) TO '{url}' WITH (format 'parquet');
        COPY (SELECT s, 'hello-'||s AS h FROM generate_series(1,10) s) TO '{url_2}' WITH (format 'parquet');
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    # create a table from the file,
    # then revoke the grant
    run_command(
        f"""
        CREATE FOREIGN TABLE test_select_perms.test_perms (s bigint, h text) SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
        CREATE FOREIGN TABLE test_select_perms.test_perms_2 (s bigint, h text) SERVER pg_lake OPTIONS (path '{url_2}', format 'parquet');

        REVOKE ALL ON SCHEMA test_select_perms FROM user1;
        GRANT USAGE ON SCHEMA test_select_perms TO user1;
        GRANT lake_read TO user1;
        GRANT user1 TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_command(
        f"""
        SET ROLE user1;
        SET pg_lake_table.enable_full_query_pushdown TO true;
    """,
        pg_conn,
    )

    result = run_query(
        f"""
        SELECT count(*) FROM test_select_perms.test_perms;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied for foreign table test_perms" in result
    pg_conn.rollback()

    # now, run the same test without full-pushdown
    result = run_command(
        f"""
        SET ROLE user1;
        SET pg_lake_table.enable_full_query_pushdown TO false;
        SELECT count(*) FROM test_select_perms.test_perms;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied for foreign table test_perms" in result
    pg_conn.rollback()

    # now, grant the access
    run_command(
        f"""
        GRANT SELECT ON TABLE test_select_perms.test_perms TO user1;
        GRANT SELECT ON TABLE test_select_perms.test_perms_2 TO user1;
    """,
        superuser_conn,
        raise_error=True,
    )
    superuser_conn.commit()

    run_command(
        f"""
        SET ROLE user1;
        SET pg_lake_table.enable_full_query_pushdown TO true;
    """,
        pg_conn,
        raise_error=True,
    )

    # pushdown should work fine with grant
    result = run_query(query, pg_conn, raise_error=True)
    assert result[0][0] == 1

    run_command(
        f"""
        SET ROLE user1;
        SET pg_lake_table.enable_full_query_pushdown TO false;
    """,
        pg_conn,
        raise_error=True,
    )

    # non-pushdown should work fine with grant
    result = run_query(query, pg_conn, raise_error=True)
    assert result[0][0] == 1

    pg_conn.rollback()

    # now, revoke grant for one of the tables
    run_command(
        f"""
    REVOKE SELECT ON TABLE test_select_perms.test_perms FROM user1;
    """,
        superuser_conn,
        raise_error=True,
    )
    superuser_conn.commit()

    run_command(
        f"""
    SET pg_lake_table.enable_full_query_pushdown TO true;
    SET ROLE user1;
    """,
        pg_conn,
        raise_error=True,
    )

    result = run_query(query, pg_conn, raise_error=False)
    assert "permission denied for foreign table test_perms" in result
    pg_conn.rollback()

    run_command(
        f"""
        GRANT SELECT(h) ON TABLE test_select_perms.test_perms TO user1;
    """,
        superuser_conn,
        raise_error=True,
    )
    superuser_conn.commit()

    run_command(
        f"""
        SET ROLE user1;
        SET pg_lake_table.enable_full_query_pushdown TO true;
    """,
        pg_conn,
        raise_error=True,
    )

    # we cannot select column s given the user doesn't have the right
    result = run_query(
        "SELECT s FROM test_select_perms.test_perms", pg_conn, raise_error=False
    )
    assert "permission denied for foreign table test_perms" in result

    pg_conn.rollback()

    # we cannot select column s given the user doesn't have the right
    result = run_query("SELECT h FROM test_select_perms.test_perms ORDER BY h", pg_conn)
    assert result[0][0] == "hello-1"

    pg_conn.rollback()

    # now, let's check if column level permission checks are enforced
    # for INSERT .. SELECT pushdown
    # First, allow reading column s, and only allow inserting into column h
    run_command(
        f"""
        GRANT SELECT(s) ON TABLE test_select_perms.test_perms TO user1;
        GRANT INSERT(h) ON TABLE test_select_perms.test_perms TO user1;
    """,
        superuser_conn,
        raise_error=True,
    )
    superuser_conn.commit()

    # we cannot insert to column (s)
    result = run_query(
        "INSERT INTO test_select_perms.test_perms(s) SELECT s FROM test_select_perms.test_perms",
        pg_conn,
        raise_error=False,
    )
    assert "permission denied for foreign table test_perms" in result

    pg_conn.rollback()

    # can both insert and select from h
    result = run_query(
        "INSERT INTO test_select_perms.test_perms(h) SELECT h FROM test_select_perms.test_perms",
        pg_conn,
        raise_error=False,
    )

    pg_conn.rollback()

    run_command(
        """
        RESET ROLE;
        DROP OWNED BY user1;
        DROP ROLE user1;
        RESET pg_lake_table.enable_full_query_pushdown;
        DROP SCHEMA test_select_perms CASCADE;
    """,
        superuser_conn,
    )

    superuser_conn.commit()


def test_views_permission(pg_conn, s3, superuser_conn, app_user, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_views_permission/"

    # Set up a user and a file
    run_command(
        f"""
        CREATE SCHEMA my_schema;

        CREATE TABLE my_schema.my_table (
            id SERIAL,
            name VARCHAR(50),
            age INT
        ) USING iceberg WITH (location = '{url}');

        INSERT INTO my_schema.my_table (name, age) VALUES ('Alice', 30), ('Bob', 25);

        CREATE VIEW my_schema.my_view AS
        SELECT id, name FROM my_schema.my_table;

        CREATE USER view_user;
        GRANT USAGE ON SCHEMA my_schema TO view_user;
        GRANT lake_read TO view_user;

        GRANT SELECT ON my_schema.my_view TO view_user;

        REVOKE ALL ON my_schema.my_table FROM view_user;

    """,
        superuser_conn,
    )
    superuser_conn.commit()

    view_user_conn = open_pg_conn("view_user")

    res = run_query("SELECT count(*) FROM my_schema.my_view", view_user_conn)
    assert res[0][0] == 2

    error = run_query(
        "SELECT count(*) FROM my_schema.my_table", view_user_conn, raise_error=False
    )
    assert "permission denied" in str(error)

    view_user_conn.close()

    run_command(
        """
        RESET ROLE;
        DROP OWNED BY view_user;
        DROP ROLE view_user;
        RESET pg_lake_table.enable_full_query_pushdown;
        DROP SCHEMA my_schema CASCADE;
    """,
        superuser_conn,
    )

    superuser_conn.commit()


def test_update_column_permission(
    pg_conn, azure, superuser_conn, app_user, with_default_location
):
    url = f"az://{TEST_BUCKET}/test_update_column_permission/"

    # Set up a user and a file
    run_command(
        f"""
        CREATE SCHEMA my_schema;
        CREATE TABLE my_schema.my_table (
            id SERIAL,
            name VARCHAR(50),
            age INT
        ) USING iceberg WITH (location = '{url}');
        INSERT INTO my_schema.my_table (name, age) VALUES ('Alice', 30), ('Bob', 25);
        CREATE USER update_user;
        GRANT USAGE ON SCHEMA my_schema TO update_user;
        GRANT SELECT ON my_schema.my_table TO update_user;
        GRANT lake_read_write TO update_user;

    """,
        superuser_conn,
    )
    superuser_conn.commit()

    update_conn = open_pg_conn("update_user")

    run_command(
        "GRANT UPDATE (age) ON my_schema.my_table TO update_user;", superuser_conn
    )
    superuser_conn.commit()

    res = run_query(
        "UPDATE my_schema.my_table SET age = 31 WHERE name = 'Alice' RETURNING age;",
        update_conn,
    )
    assert res[0][0] == 31

    error = run_command(
        "UPDATE my_schema.my_table SET name = 'no name' WHERE name = 'Alice'",
        update_conn,
        raise_error=False,
    )
    assert "permission denied" in str(error)

    update_conn.close()

    run_command(
        """
        RESET ROLE;
        DROP OWNED BY update_user;
        DROP ROLE update_user;
        DROP SCHEMA my_schema CASCADE;
    """,
        superuser_conn,
    )

    superuser_conn.commit()


def test_copy_column_permission(
    pg_conn, s3, extension, superuser_conn, app_user, tmp_path
):
    url = f"s3://{TEST_BUCKET}/test_copy_column_permission/"

    # Set up a user and a table
    run_command(
        f"""
        CREATE SCHEMA my_schema;
        CREATE TABLE my_schema.my_table (
            id SERIAL,
            name VARCHAR(50),
            age INT
        ) USING iceberg WITH (location = '{url}');
        INSERT INTO my_schema.my_table (name, age) VALUES ('Alice', 30), ('Bob', 25);
        CREATE USER copy_user;
        GRANT lake_read_write TO copy_user;
        GRANT USAGE ON SCHEMA my_schema TO copy_user;
        GRANT SELECT (age) ON my_schema.my_table TO copy_user;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    copy_conn = open_pg_conn("copy_user")

    # Try COPY operation on allowed column
    path = tmp_path / "test_types.parquet"
    copy_to_file("COPY(SELECT age FROM my_schema.my_table) TO STDOUT", path, copy_conn)

    # Try COPY operation on restricted column and expect failure
    res = copy_to_file(
        "COPY (SELECT name FROM my_schema.my_table WHERE name = 'Alice') TO STDOUT;",
        path,
        copy_conn,
        raise_error=False,
    )
    assert "permission denied" in str(res)

    copy_conn.close()

    # Clean up
    run_command(
        """
        RESET ROLE;
        DROP OWNED BY copy_user;
        DROP ROLE copy_user;
        DROP SCHEMA my_schema CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_copy_from_column_permission(
    pg_conn, s3, superuser_conn, app_user, with_default_location
):
    url = f"s3://{TEST_BUCKET}/test_copy_from_column_permission/"

    # Set up a user and a table
    run_command(
        f"""
        CREATE SCHEMA my_schema;
        CREATE TABLE my_schema.my_table (
            name VARCHAR(50),
            age INT
        ) USING iceberg WITH (location = '{url}');
        CREATE USER copy_user;
        GRANT lake_read_write TO copy_user;

        GRANT USAGE ON SCHEMA my_schema TO copy_user;
        GRANT SELECT (age) ON my_schema.my_table TO copy_user;
        GRANT INSERT (age) ON my_schema.my_table TO copy_user;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    copy_conn = open_pg_conn("copy_user")

    # Try COPY FROM on allowed column (age)
    data = "35\n40\n"

    # Use the run_command function to execute the COPY command
    with copy_conn.cursor() as cursor:
        data_io = io.StringIO(data)
        cursor.copy_expert("COPY my_schema.my_table(age) FROM STDIN", data_io)

    # Use the run_command function to execute the COPY command
    try:
        with copy_conn.cursor() as cursor:
            data_io = io.StringIO(data)
            cursor.copy_expert("COPY my_schema.my_table(name) FROM STDIN", data_io)

            # we should nevee get here as the command above throws error
            assert False
    except psycopg2.DatabaseError as error:
        copy_conn.rollback()
        assert "permission denied" in error.pgerror

    # Clean up
    copy_conn.close()
    run_command(
        """
        RESET ROLE;
        DROP OWNED BY copy_user;
        DROP ROLE copy_user;
        DROP SCHEMA my_schema CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_non_superuser_schema_usage(user_conn, duckdb_conn, s3):
    "Ensure that the USAGE permission is setup on our lake_struct and map_type for non-superusers"

    url = f"s3://{TEST_BUCKET}/test_non_superuser_schema_usage/data.parquet"

    # Create a struct containing a map, save and import the file, creating our
    # necessary database objects.

    run_command(
        f"""
    COPY (SELECT {{ mymap: MAP {{ 1 : 2 }} }} mystruct) TO '{url}';
    """,
        duckdb_conn,
    )

    # this should import the necessary types
    run_command(
        f"""
    CREATE SCHEMA test_non_superuser_schema_usage;
    CREATE TABLE test_non_superuser_schema_usage.tbl () WITH (definition_from='{url}')
    """,
        user_conn,
    )

    # Validate the types got created successfully; when we don't have
    # permissions for this, we cannot cast strings as regtype successfully.
    # Without the patch, we get a permissions error:
    #
    # ERROR:  permission denied for schema map_type

    res = run_query(
        """
        SELECT count(*) FROM pg_type WHERE oid = 'map_type.key_int_val_int'::regtype;
    """,
        user_conn,
    )
    assert res[0][0] == 1

    # The specific lake_struct type depends on the oid of the underlying map
    # type in this case, so the naming is not stable, unlike the built-in types.
    # Retrieve the attribute name, then test the cast/usage.

    res = run_query(
        """
        SELECT oid::regtype::text FROM pg_type
        WHERE typnamespace = 'lake_struct'::regnamespace
        AND typtype = 'c' AND oid::regtype::text LIKE 'lake_struct.mymap_%';
    """,
        user_conn,
    )

    # we found it
    assert len(res) == 1

    # we can use it as our own non-super user
    res = run_query(
        f"""
        SELECT count(*) FROM pg_type WHERE oid = '{res[0][0]}'::regtype;
    """,
        user_conn,
    )

    assert res[0][0] == 1

    user_conn.rollback()


def test_query_user(pg_conn, s3, superuser_conn, app_user, with_default_location):
    location = f"s3://{TEST_BUCKET}/test_query_user/"

    run_command(
        f"""
        CREATE SCHEMA test_query_user;
        CREATE TABLE test_query_user.test (x int, y int) USING iceberg
        WITH (location = '{location}');

        CREATE USER query_user;
        GRANT USAGE ON SCHEMA test_query_user TO query_user;
        GRANT query_user TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    error = run_command(
        """
        SET ROLE query_user;
        SELECT * FROM test_query_user.test;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    error = run_command(
        """
        SET ROLE query_user;
        INSERT INTO test_query_user.test VALUES (1,2);
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    # insert .. select pushdown
    error = run_command(
        """
        SET ROLE query_user;
        INSERT INTO test_query_user.test SELECT i,i FROM generate_series(0,10)i;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    error = run_command(
        """
        SET ROLE query_user;
        UPDATE test_query_user.test SET y = 3;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    error = run_command(
        """
        SET ROLE query_user;
        DELETE FROM test_query_user.test;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    run_command(
        """
        GRANT SELECT ON test_query_user.test TO query_user;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    # insert .. select pushdown should still fail with SELECT permission
    error = run_command(
        """
        SET ROLE query_user;
        INSERT INTO test_query_user.test SELECT i,i FROM generate_series(0,10)i;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "permission denied" in error
    pg_conn.rollback()

    run_command(
        """
        GRANT INSERT ON test_query_user.test TO query_user;
        GRANT UPDATE ON test_query_user.test TO query_user;
        GRANT DELETE ON test_query_user.test TO query_user;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        """
        SET ROLE query_user;
        INSERT INTO test_query_user.test VALUES (1,2);
    """,
        pg_conn,
    )
    pg_conn.rollback()

    # INSERT .. SELECT pushdown is fine given we already granted INSERT
    run_command(
        """
        SET ROLE query_user;
        INSERT INTO test_query_user.test SELECT 1,2 FROM generate_series(0,0);
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT * FROM test_query_user.test;
    """,
        pg_conn,
        raise_error=False,
    )
    assert len(result) == 1

    run_command(
        """
        UPDATE test_query_user.test SET y = 3;
    """,
        pg_conn,
    )

    run_command(
        """
        DELETE FROM test_query_user.test;
    """,
        pg_conn,
    )

    pg_conn.rollback()

    run_command(
        """
        DROP OWNED BY query_user;
        DROP ROLE query_user;
        DROP SCHEMA test_query_user CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_struct_type_reuse_requires_ownership(
    duckdb_conn, user_conn, superuser_conn, s3
):
    "Inferred struct types owned by other roles should not be reused"

    url = f"s3://{TEST_BUCKET}/test_struct_type_reuse_requires_ownership/data.parquet"
    location = f"s3://{TEST_BUCKET}/test_struct_type_reuse_requires_ownership/tbl/"

    run_command(
        f"""
        COPY (SELECT 1 AS id, {{ a: 1, b: 'hello' }} AS payload) TO '{url}';
        """,
        duckdb_conn,
    )

    # another role creates matching struct types, both under an arbitrary
    # name and under the generated name by importing the same file
    run_command(
        f"""
        CREATE ROLE struct_other;
        GRANT ALL ON SCHEMA public TO struct_other;
        GRANT lake_read TO struct_other;
        SET ROLE struct_other;
        CREATE TABLE public.struct_other_tbl () WITH (definition_from='{url}');
        RESET ROLE;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    other_type = run_query(
        """
        SELECT atttypid::regtype::text FROM pg_attribute
        WHERE attrelid = 'public.struct_other_tbl'::regclass AND attname = 'payload'
        """,
        superuser_conn,
    )[0][0]

    run_command(
        f"""
        CREATE SCHEMA test_struct_owner;
        CREATE TABLE test_struct_owner.tbl () USING iceberg
        WITH (definition_from='{url}', location='{location}');
        """,
        user_conn,
    )
    user_conn.commit()

    res = run_query(
        """
        SELECT atttypid::regtype::text, pg_get_userbyid(typowner) = current_user
        FROM pg_attribute JOIN pg_type ON (atttypid = pg_type.oid)
        WHERE attrelid = 'test_struct_owner.tbl'::regclass AND attname = 'payload'
        """,
        user_conn,
    )
    assert res[0][0] != other_type
    assert res[0][1]

    # dropping the other role's types does not affect our table
    run_command(
        f"""
        DROP TABLE public.struct_other_tbl;
        DROP OWNED BY struct_other CASCADE;
        DROP ROLE struct_other;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    res = run_query(
        """
        SELECT count(*) FROM pg_attribute
        WHERE attrelid = 'test_struct_owner.tbl'::regclass
        AND attname = 'payload' AND NOT attisdropped
        """,
        user_conn,
    )
    assert res[0][0] == 1

    run_command("DROP SCHEMA test_struct_owner CASCADE", user_conn)
    user_conn.commit()

import pytest
import psycopg2
from utils_pytest import *
from helpers.spark import *

import os
import glob
import re

# show that we can read spark generated tables that have evolved (E.g., had DDLs)
# stage 1: add one row before any DDLs
# stage 2: add column then add one row
# stage 3: rename column and insert one more row
# stage 4: change type of the column and insert one more row
# stage 5: change the order of the columns in iceberg metadata, and insert one more row
# stage 6: drop the column
# stage 7: add nested column (not supported yet)


def test_iceberg_modified_table(
    extension, pg_conn, app_user, spark_generated_iceberg_test
):
    iceberg_table_folder = (
        iceberg_sample_table_folder_path() + "/public/spark_generated_iceberg_ddl_test"
    )
    iceberg_table_metadata_folder = iceberg_table_folder + "/metadata"

    iceberg_table_metadata_location = (
        "s3://"
        + TEST_BUCKET
        + "/spark_test/public/spark_generated_iceberg_ddl_test/metadata"
    )

    run_command(
        f"""
		create schema spark_gen_ddl;
	""",
        pg_conn,
    )

    metadata_files = get_sorted_metadata_files(iceberg_table_metadata_folder)

    # stage 1: add one row before any DDLs
    file = filter_files_by_prefix(metadata_files, "00001")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_1 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_1'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 1
    assert result == [["id", "bigint"]]
    result = run_query("SELECT * FROM spark_gen_ddl.stage_1", pg_conn)
    assert result[0][0] == 1

    # stage 2: add column then add one row
    file = filter_files_by_prefix(metadata_files, "00003")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_2 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_2'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 2
    assert result == [["id", "bigint"], ["new_column", "integer"]]

    result = run_query("SELECT * FROM spark_gen_ddl.stage_2 ORDER BY id ASC", pg_conn)
    assert result == [[1, None], [2, 2]]

    # stage 3: rename column and insert one more row
    file = filter_files_by_prefix(metadata_files, "00005")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_3 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_3'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 2
    print(result)
    assert result == [["id", "bigint"], ["payload", "integer"]]

    result = run_query("SELECT * FROM spark_gen_ddl.stage_3 ORDER BY id ASC", pg_conn)
    assert result == [[1, None], [2, 2], [3, 3]]

    # stage 4: change type of the column and insert one more row
    file = filter_files_by_prefix(metadata_files, "00007")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_4 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_4'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 2
    print(result)
    assert result == [["id", "bigint"], ["payload", "bigint"]]

    result = run_query("SELECT * FROM spark_gen_ddl.stage_4 ORDER BY id ASC", pg_conn)
    assert result == [[1, None], [2, 2], [3, 3], [4, 4]]

    # stage 5: change the order of the columns in iceberg metadata, and insert one more row
    file = filter_files_by_prefix(metadata_files, "00009")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_5 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_5'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 2
    assert result == [["payload", "bigint"], ["id", "bigint"]]

    result = run_query(
        "SELECT * FROM spark_gen_ddl.stage_5 ORDER BY id ASC NULLS FIRST", pg_conn
    )
    assert result == [[None, 1], [2, 2], [3, 3], [4, 4], [4, 5]]

    # stage 6: drop the column
    file = filter_files_by_prefix(metadata_files, "00010")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_6 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_6'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 1
    assert result == [["id", "bigint"]]

    result = run_query(
        "SELECT * FROM spark_gen_ddl.stage_6 ORDER BY id ASC NULLS FIRST", pg_conn
    )
    assert result == [[1], [2], [3], [4], [5]]

    # stage 7: add nested column
    file = filter_files_by_prefix(metadata_files, "00013")
    metadata_location = f"{iceberg_table_metadata_location}/{file[0]}"
    run_command(
        f"""
		create table spark_gen_ddl.stage_7 () WITH (load_from='{metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_ddl.stage_7'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0] == ["id", "bigint"]
    assert result[1][0] == "point"
    result = run_query(
        "SELECT * FROM spark_gen_ddl.stage_7 ORDER BY id ASC NULLS FIRST", pg_conn
    )
    assert result == [
        [1, None],
        [2, None],
        [3, None],
        [4, None],
        [5, None],
        [6, None],
        [7, "(7.7,7.7)"],
    ]

    run_command("DROP SCHEMA spark_gen_ddl CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_add_column_on_empty_snapshot(pg_conn, s3, with_default_location):
    run_command("CREATE SCHEMA add_column_test;", pg_conn)
    run_command(
        "CREATE TABLE add_column_test.test_iceberg_add_column_on_empty_snapshot (a int, b int) USING iceberg",
        pg_conn,
    )

    # add column without any snapshots pushed
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column_on_empty_snapshot ADD COLUMN c int",
        pg_conn,
    )

    # now, ingest one row, and get the
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column_on_empty_snapshot VALUES (1, 2, 3)",
        pg_conn,
    )
    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column_on_empty_snapshot",
        pg_conn,
    )
    assert results == [[1, 2, 3]]

    pg_conn.rollback()


def test_iceberg_drop_last_column(pg_conn, s3, with_default_location):
    run_command("CREATE SCHEMA test_iceberg_drop_last_column;", pg_conn)
    run_command(
        "CREATE TABLE test_iceberg_drop_last_column.test (a int, b bigserial) USING iceberg",
        pg_conn,
    )

    run_command("INSERT INTO test_iceberg_drop_last_column.test VALUES (1)", pg_conn)

    # drop the last column
    run_command("ALTER TABLE test_iceberg_drop_last_column.test DROP COLUMN b", pg_conn)

    # now, ingest one row, and get the
    run_command("INSERT INTO test_iceberg_drop_last_column.test VALUES (2)", pg_conn)
    results = run_query(
        "SELECT * FROM test_iceberg_drop_last_column.test ORDER BY 1 ASC", pg_conn
    )
    assert results == [[1], [2]]

    run_command("DROP TABLE test_iceberg_drop_last_column.test", pg_conn)
    pg_conn.rollback()


def test_iceberg_add_column(pg_conn, s3, extension, with_default_location):
    # add multiple columns
    run_command("CREATE SCHEMA add_column_test;", pg_conn)
    run_command(
        "CREATE TABLE add_column_test.test_iceberg_add_column (a int, b int) USING iceberg",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (1, 2)", pg_conn
    )
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN c int", pg_conn
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (4, 5, 6)", pg_conn
    )
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN d int", pg_conn
    )
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN e int", pg_conn
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (7, 8, 9, 10)",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY a ASC", pg_conn
    )
    assert results == [
        [1, 2, None, None, None],
        [4, 5, 6, None, None],
        [7, 8, 9, 10, None],
    ]

    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN f int, ADD COLUMN g int, DROP COLUMN e",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY 1,2,3,4,5,6",
        pg_conn,
    )
    assert results == [
        [1, 2, None, None, None, None],
        [4, 5, 6, None, None, None],
        [7, 8, 9, 10, None, None],
    ]

    pg_conn.rollback()


def test_iceberg_add_column_default(pg_conn, s3, extension, with_default_location):
    # add multiple columns
    run_command("CREATE SCHEMA add_column_test;", pg_conn)
    run_command(
        "CREATE TABLE add_column_test.test_iceberg_add_column (a int, b int) USING iceberg",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (1, 2)", pg_conn
    )
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN c int DEFAULT 15",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (4, 5, 6)", pg_conn
    )

    # constant expressions
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN d int DEFAULT 20/4*12",
        pg_conn,
    )
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN e int DEFAULT length('four')",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (7, 8, 9, 10)",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY a ASC", pg_conn
    )
    assert results == [[1, 2, 15, 60, 4], [4, 5, 6, 60, 4], [7, 8, 9, 10, 4]]

    # multiple columns with default
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN f int DEFAULT null, ADD COLUMN g int DEFAULT -5, DROP COLUMN e",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY 1,2,3,4,5,6",
        pg_conn,
    )
    assert results == [
        [1, 2, 15, 60, None, -5],
        [4, 5, 6, 60, None, -5],
        [7, 8, 9, 10, None, -5],
    ]

    # text default
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN h text DEFAULT 'he\b\f\n\r\t\"\\llo' || chr(3) || chr(129)",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY a ASC", pg_conn
    )
    assert results == [
        [1, 2, 15, 60, None, -5, 'he\b\f\n\r\t"\\llo\x03\u0081'],
        [4, 5, 6, 60, None, -5, 'he\b\f\n\r\t"\\llo\x03\u0081'],
        [7, 8, 9, 10, None, -5, 'he\b\f\n\r\t"\\llo\x03\u0081'],
    ]

    pg_conn.rollback()


def test_iceberg_add_nested_column_default(
    pg_conn, s3, extension, with_default_location
):
    run_command("CREATE SCHEMA add_column_test;", pg_conn)

    run_command(
        "CREATE TABLE add_column_test.test_iceberg_add_column (a int, b int) USING iceberg",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (1, 2)", pg_conn
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (4, 5)", pg_conn
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (7, 8)", pg_conn
    )

    # array default
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN c float4[] DEFAULT array[3.0, null, 2.345]",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY a ASC", pg_conn
    )
    assert results == [
        [1, 2, [3.0, None, 2.345]],
        [4, 5, [3.0, None, 2.345]],
        [7, 8, [3.0, None, 2.345]],
    ]

    # composite default
    run_command(
        "create type dog as (id int, name text); create type person as (id int, dog dog[]);",
        pg_conn,
    )
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN d person DEFAULT row(1, array[row(1, 'fodie')::dog, null])::person",
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY a ASC", pg_conn
    )
    assert results == [
        [1, 2, [3.0, None, 2.345], '(1,"{""(1,fodie)"",NULL}")'],
        [4, 5, [3.0, None, 2.345], '(1,"{""(1,fodie)"",NULL}")'],
        [7, 8, [3.0, None, 2.345], '(1,"{""(1,fodie)"",NULL}")'],
    ]

    # map default
    map_type_name = create_map_type("int", "text")
    run_command(
        f'ALTER TABLE add_column_test.test_iceberg_add_column ADD COLUMN e {map_type_name} DEFAULT \'{{{{"(1,a)","(2,b)","(3,c)"}}}}\'',
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM add_column_test.test_iceberg_add_column ORDER BY a ASC", pg_conn
    )
    assert results == [
        [
            1,
            2,
            [3.0, None, 2.345],
            '(1,"{""(1,fodie)"",NULL}")',
            '{"(1,a)","(2,b)","(3,c)"}',
        ],
        [
            4,
            5,
            [3.0, None, 2.345],
            '(1,"{""(1,fodie)"",NULL}")',
            '{"(1,a)","(2,b)","(3,c)"}',
        ],
        [
            7,
            8,
            [3.0, None, 2.345],
            '(1,"{""(1,fodie)"",NULL}")',
            '{"(1,a)","(2,b)","(3,c)"}',
        ],
    ]

    pg_conn.rollback()


def test_iceberg_add_set_drop_column_default(
    pg_conn, s3, extension, with_default_location
):
    run_command("CREATE SCHEMA add_column_test;", pg_conn)

    run_command(
        "CREATE TABLE add_column_test.test_iceberg_add_column (a int, b int) USING iceberg",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (1, 2)", pg_conn
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (4, 5)", pg_conn
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (7, 8)", pg_conn
    )

    results = run_query(
        "SELECT b FROM add_column_test.test_iceberg_add_column ORDER BY b", pg_conn
    )
    assert results == [[2], [5], [8]]

    # set default a few times
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ALTER COLUMN b SET DEFAULT 100",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (7)", pg_conn
    )

    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ALTER COLUMN b SET DEFAULT 200",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (8)", pg_conn
    )

    results = run_query(
        "SELECT b FROM add_column_test.test_iceberg_add_column ORDER BY b", pg_conn
    )
    assert results == [[2], [5], [8], [100], [200]]

    # drop default
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ALTER COLUMN b DROP DEFAULT",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (9)", pg_conn
    )

    results = run_query(
        "SELECT b FROM add_column_test.test_iceberg_add_column ORDER BY b", pg_conn
    )
    assert results == [[2], [5], [8], [100], [200], [None]]

    # set default again
    run_command(
        "ALTER TABLE add_column_test.test_iceberg_add_column ALTER COLUMN b SET DEFAULT 300",
        pg_conn,
    )
    run_command(
        "INSERT INTO add_column_test.test_iceberg_add_column VALUES (10)", pg_conn
    )

    results = run_query(
        "SELECT b FROM add_column_test.test_iceberg_add_column ORDER BY b", pg_conn
    )
    assert results == [[2], [5], [8], [100], [200], [300], [None]]

    pg_conn.rollback()


# we do not allow any constraints
# as constraints are applied on the existing rows
# and we currently do not have the mechanism to do that
def test_iceberg_add_column_with_constraints(
    pg_conn, s3, extension, with_default_location
):

    run_command("CREATE SCHEMA test_iceberg_add_column_with_constraints;", pg_conn)
    run_command(
        "CREATE TABLE test_iceberg_add_column_with_constraints.test_iceberg_add_column (a int, b int) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    sub_command_list = [
        "c int CHECK(c>100)",
        "c int CHECK(a+b>100)",
        "c int NOT NULL",
        "c int NULL",
        "c bigserial",
        "c serial",
        "c smallserial",
        "c BIGINT GENERATED ALWAYS AS IDENTITY",
        "c TEXT GENERATED ALWAYS AS (a || ' ' || b) STORED;",
    ]
    for sub_command in sub_command_list:
        err = run_command(
            f"ALTER TABLE test_iceberg_add_column_with_constraints.test_iceberg_add_column ADD COLUMN {sub_command}",
            pg_conn,
            raise_error=False,
        )

        # re is to ignore sub-command specific errors, like serial columns
        assert re.search(
            r"ALTER TABLE ADD COLUMN .* command not supported for pg_lake_iceberg tables",
            str(err),
        )
        pg_conn.rollback()

    run_command(
        "DROP SCHEMA test_iceberg_add_column_with_constraints CASCADE;", pg_conn
    )
    pg_conn.commit()


def test_circular_add_drop_column(pg_conn, with_default_location):
    # Create schema and table
    run_command("CREATE SCHEMA circular_column_test;", pg_conn)
    run_command(
        "CREATE TABLE circular_column_test.test_table (a int, b int, c int) USING iceberg;",
        pg_conn,
    )

    # Insert initial data
    run_command(
        "INSERT INTO circular_column_test.test_table VALUES (1, 2, 3);", pg_conn
    )

    # Step 1: Drop and add column 'a', insert new data, and assert
    run_command("ALTER TABLE circular_column_test.test_table DROP COLUMN a;", pg_conn)
    run_command(
        "ALTER TABLE circular_column_test.test_table ADD COLUMN a int;", pg_conn
    )
    run_command(
        "INSERT INTO circular_column_test.test_table (a,b,c) VALUES (10, 20, 30);",
        pg_conn,
    )
    results = run_query(
        "SELECT a, b, c FROM circular_column_test.test_table ORDER BY b DESC NULLS LAST;",
        pg_conn,
    )
    assert results == [[10, 20, 30], [None, 2, 3]]

    # Step 2: Drop and add column 'b', insert new data, and assert
    run_command("ALTER TABLE circular_column_test.test_table DROP COLUMN b;", pg_conn)
    run_command(
        "ALTER TABLE circular_column_test.test_table ADD COLUMN b int;", pg_conn
    )
    run_command(
        "INSERT INTO circular_column_test.test_table (a,b,c) VALUES (100, 200, 300);",
        pg_conn,
    )
    results = run_query(
        "SELECT a, b, c FROM circular_column_test.test_table ORDER BY c DESC NULLS LAST;",
        pg_conn,
    )
    assert results == [[100, 200, 300], [10, None, 30], [None, None, 3]]

    # Step 3: Drop and add column 'c', insert new data, and assert
    run_command("ALTER TABLE circular_column_test.test_table DROP COLUMN c;", pg_conn)
    run_command(
        "ALTER TABLE circular_column_test.test_table ADD COLUMN c int;", pg_conn
    )
    run_command(
        "INSERT INTO circular_column_test.test_table (a,b,c) VALUES (1000, 2000, 3000);",
        pg_conn,
    )
    results = run_query(
        "SELECT a, b, c FROM circular_column_test.test_table ORDER BY a DESC NULLS LAST;",
        pg_conn,
    )
    assert results == [
        [1000, 2000, 3000],
        [100, 200, None],
        [10, None, None],
        [None, None, None],
    ]

    pg_conn.rollback()


def test_rename_column(pg_conn, with_default_location):
    # Create schema and table
    run_command("CREATE SCHEMA rename_column_test;", pg_conn)
    run_command(
        "CREATE TABLE rename_column_test.test_table (a int, b int, c int) USING iceberg;",
        pg_conn,
    )

    # Insert initial data
    run_command("INSERT INTO rename_column_test.test_table VALUES (1, 2, 3);", pg_conn)

    run_command(
        "ALTER TABLE rename_column_test.test_table RENAME COLUMN a TO a_new;", pg_conn
    )
    run_command(
        "INSERT INTO rename_column_test.test_table VALUES (10, 20, 30);", pg_conn
    )

    results = run_query(
        "SELECT a_new, b, c FROM rename_column_test.test_table ORDER BY a_new ASC",
        pg_conn,
    )
    assert results[0][0] == 1
    assert results[1][0] == 10

    # rename back to the original name
    run_command(
        "ALTER TABLE rename_column_test.test_table RENAME COLUMN a_new TO a;", pg_conn
    )
    run_command(
        "INSERT INTO rename_column_test.test_table VALUES (100, 200, 300);", pg_conn
    )
    results = run_query(
        "SELECT a, b, c FROM rename_column_test.test_table ORDER BY a ASC", pg_conn
    )
    assert results[0][0] == 1
    assert results[1][0] == 10
    assert results[2][0] == 100

    # multiple renames
    run_command(
        "ALTER TABLE rename_column_test.test_table RENAME COLUMN a TO a_new;", pg_conn
    )
    run_command(
        "ALTER TABLE rename_column_test.test_table RENAME COLUMN b TO b_new;", pg_conn
    )
    run_command(
        "ALTER TABLE rename_column_test.test_table RENAME COLUMN c TO c_new;", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='rename_column_test'",
        pg_conn,
    )[0][0]

    # create a new foreign table based on this new metadata
    run_command(
        f"CREATE FOREIGN TABLE rename_column_test.ft_ice() SERVER pg_lake OPTIONS (path '{metadata_location}')",
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'rename_column_test.ft_ice'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 3
    assert result == [["a_new", "integer"], ["b_new", "integer"], ["c_new", "integer"]]

    run_command("DROP SCHEMA rename_column_test CASCADE", pg_conn)
    pg_conn.commit()


def test_set_drop_default(pg_conn, s3, with_default_location):
    # Create schema and table
    run_command("CREATE SCHEMA set_drop_default;", pg_conn)
    run_command(
        "CREATE TABLE set_drop_default.test_table (a int, b int, c int) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    # Insert initial data
    run_command("INSERT INTO set_drop_default.test_table VALUES (1, 2, 3);", pg_conn)

    run_command(
        "ALTER TABLE set_drop_default.test_table alter column a SET DEFAULT 10;",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "ALTER TABLE set_drop_default.test_table alter column c SET DEFAULT 25;",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO set_drop_default.test_table (b, c) VALUES (20, 30);", pg_conn
    )
    results = run_query(
        "SELECT a, b, c FROM set_drop_default.test_table ORDER BY a ASC", pg_conn
    )
    assert results[0][0] == 1
    assert results[1][0] == 10

    run_command("INSERT INTO set_drop_default.test_table (b) VALUES (20);", pg_conn)
    results = run_query(
        "SELECT a, b, c FROM set_drop_default.test_table WHERE c = 25 ORDER BY a ASC",
        pg_conn,
    )
    assert results[0][0] == 10

    # we only use constant default values on iceberg metadata, still push a new snapshot but do not reflect
    run_command(
        "ALTER TABLE set_drop_default.test_table alter column b SET DEFAULT random();",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='set_drop_default'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    schemas = returned_json["schemas"]
    print(schemas)

    assert schemas == [
        {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "a", "type": "int", "required": False},
                {"id": 2, "name": "b", "type": "int", "required": False},
                {"id": 3, "name": "c", "type": "int", "required": False},
            ],
        },
        {
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "a",
                    "type": "int",
                    "required": False,
                    "write-default": 10,
                },
                {"id": 2, "name": "b", "type": "int", "required": False},
                {"id": 3, "name": "c", "type": "int", "required": False},
            ],
        },
        {
            "type": "struct",
            "schema-id": 2,
            "fields": [
                {
                    "id": 1,
                    "name": "a",
                    "type": "int",
                    "required": False,
                    "write-default": 10,
                },
                {"id": 2, "name": "b", "type": "int", "required": False},
                {
                    "id": 3,
                    "name": "c",
                    "type": "int",
                    "required": False,
                    "write-default": 25,
                },
            ],
        },
    ]
    default_schema_id = returned_json["current-schema-id"]
    assert default_schema_id == 2

    # now, drop defaults and show that's reflected in the metadata
    run_command(
        "ALTER TABLE set_drop_default.test_table alter column a DROP DEFAULT;", pg_conn
    )
    pg_conn.commit()

    run_command(
        "ALTER TABLE set_drop_default.test_table alter column b DROP DEFAULT;", pg_conn
    )
    pg_conn.commit()

    run_command(
        "ALTER TABLE set_drop_default.test_table alter column c DROP DEFAULT;", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='set_drop_default'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))

    # return back to the initial schema
    default_schema_id = returned_json["current-schema-id"]
    assert default_schema_id == 0
    assert returned_json["snapshots"][-1]["schema-id"] == default_schema_id

    run_command("DROP SCHEMA set_drop_default CASCADE", pg_conn)
    pg_conn.commit()


def test_set_drop_not_null(pg_conn, s3, with_default_location):
    # Create schema and table
    run_command("CREATE SCHEMA set_drop_not_null;", pg_conn)
    run_command(
        "CREATE TABLE set_drop_not_null.test_table (a int, b int NOT NULL, c int) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    # Insert initial data
    run_command("INSERT INTO set_drop_not_null.test_table VALUES (1, 2, 3);", pg_conn)
    run_command(
        "ALTER TABLE set_drop_not_null.test_table alter column b DROP NOT NULL;",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='set_drop_not_null'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    # required field reflects not-null
    assert last_schema == {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {"id": 1, "name": "a", "type": "int", "required": False},
            {"id": 2, "name": "b", "type": "int", "required": False},
            {"id": 3, "name": "c", "type": "int", "required": False},
        ],
    }

    run_command("DROP SCHEMA set_drop_not_null CASCADE", pg_conn)
    pg_conn.commit()


def test_drop_type_cascade_drop_column(pg_conn, extension, s3, with_default_location):
    # Create schema and table
    run_command("CREATE SCHEMA test_drop_type_cascade_drop_column;", pg_conn)
    run_command(
        "CREATE TYPE test_drop_type_cascade_drop_column.t1 as (a int, b int);", pg_conn
    )
    run_command(
        "CREATE TYPE test_drop_type_cascade_drop_column.t2 as (a int, b int);", pg_conn
    )

    run_command(
        "CREATE TABLE test_drop_type_cascade_drop_column.test_table (a test_drop_type_cascade_drop_column.t1, b int, c test_drop_type_cascade_drop_column.t2) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_type_cascade_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    assert last_schema == {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {
                "id": 1,
                "name": "a",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "a", "type": "int", "required": False},
                        {"id": 3, "name": "b", "type": "int", "required": False},
                    ],
                },
                "required": False,
            },
            {"id": 4, "name": "b", "type": "int", "required": False},
            {
                "id": 5,
                "name": "c",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 6, "name": "a", "type": "int", "required": False},
                        {"id": 7, "name": "b", "type": "int", "required": False},
                    ],
                },
                "required": False,
            },
        ],
    }

    run_command(
        "DROP TYPE test_drop_type_cascade_drop_column.t1, test_drop_type_cascade_drop_column.t2 CASCADE;",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_type_cascade_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    assert last_schema == {
        "type": "struct",
        "schema-id": 1,
        "fields": [{"id": 4, "name": "b", "type": "int", "required": False}],
    }

    run_command(
        "CREATE TYPE test_drop_type_cascade_drop_column.t3 as (a int, b int);", pg_conn
    )
    run_command(
        "ALTER TABLE test_drop_type_cascade_drop_column.test_table ADD COLUMN d test_drop_type_cascade_drop_column.t3 ",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_type_cascade_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    # make sure that the new column gets a new id
    assert last_schema == {
        "type": "struct",
        "schema-id": 2,
        "fields": [
            {"id": 4, "name": "b", "type": "int", "required": False},
            {
                "id": 8,
                "name": "d",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 9, "name": "a", "type": "int", "required": False},
                        {"id": 10, "name": "b", "type": "int", "required": False},
                    ],
                },
                "required": False,
            },
        ],
    }

    # lets drop all columns
    run_command("DROP TYPE test_drop_type_cascade_drop_column.t3 CASCADE;", pg_conn)
    run_command(
        "ALTER TABLE test_drop_type_cascade_drop_column.test_table DROP COLUMN b",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_type_cascade_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    # make sure that the new column gets a new id
    assert last_schema == {"type": "struct", "schema-id": 3, "fields": []}

    # and add one more column
    run_command(
        "ALTER TABLE test_drop_type_cascade_drop_column.test_table ADD COLUMN a INT",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_type_cascade_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    # make sure that the new column gets a new id
    assert last_schema == {
        "type": "struct",
        "schema-id": 4,
        "fields": [{"id": 11, "name": "a", "type": "int", "required": False}],
    }

    run_command("DROP SCHEMA test_drop_type_cascade_drop_column CASCADE", pg_conn)
    pg_conn.commit()


def test_drop_owned_by_drop_column(
    pg_conn, superuser_conn, extension, s3, with_default_location
):

    run_command("CREATE ROLE r_user WITH LOGIN", superuser_conn)
    superuser_conn.commit()

    run_command("CREATE SCHEMA test_drop_owned_by_drop_column;", pg_conn)
    run_command("CREATE TYPE test_drop_owned_by_drop_column.t1 as (a int);", pg_conn)
    pg_conn.commit()

    run_command(
        "ALTER TYPE test_drop_owned_by_drop_column.t1 OWNER TO r_user", superuser_conn
    )
    superuser_conn.commit()

    run_command(
        "CREATE TABLE test_drop_owned_by_drop_column.test_table(a int, b test_drop_owned_by_drop_column.t1) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_owned_by_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    assert last_schema == {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "a", "type": "int", "required": False},
            {
                "id": 2,
                "name": "b",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 3, "name": "a", "type": "int", "required": False}
                    ],
                },
                "required": False,
            },
        ],
    }

    run_command("DROP OWNED BY r_user CASCADE", superuser_conn)
    superuser_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_owned_by_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    assert last_schema == {
        "type": "struct",
        "schema-id": 1,
        "fields": [{"id": 1, "name": "a", "type": "int", "required": False}],
    }

    run_command("DROP SCHEMA test_drop_owned_by_drop_column CASCADE;", pg_conn)
    pg_conn.commit()


def test_drop_schema_drop_column(pg_conn, extension, s3, with_default_location):

    run_command("CREATE SCHEMA test_drop_schema_drop_column;", pg_conn)

    run_command("CREATE SCHEMA test_drop_schema_drop_column_drop;", pg_conn)
    run_command("CREATE TYPE test_drop_schema_drop_column_drop.t1 as (a int);", pg_conn)

    run_command(
        "CREATE TABLE test_drop_schema_drop_column.test_table(a test_drop_schema_drop_column_drop.t1, b int) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_schema_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    assert last_schema == {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {
                "id": 1,
                "name": "a",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "a", "type": "int", "required": False}
                    ],
                },
                "required": False,
            },
            {"id": 3, "name": "b", "type": "int", "required": False},
        ],
    }

    run_command("DROP SCHEMA test_drop_schema_drop_column_drop CASCADE;", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table' and table_namespace='test_drop_schema_drop_column'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    assert last_schema == {
        "type": "struct",
        "schema-id": 1,
        "fields": [{"id": 3, "name": "b", "type": "int", "required": False}],
    }

    pg_conn.rollback()


def test_prevent_drop_attribute(pg_conn, extension, s3, with_default_location):

    run_command(
        """
			 CREATE SCHEMA test_prevent_drop_attribute;
			 CREATE TYPE test_prevent_drop_attribute.sub as (a int, b int);
			 CREATE TYPE test_prevent_drop_attribute.top as (a int, b test_prevent_drop_attribute.sub);
			 CREATE TYPE test_prevent_drop_attribute.unrelated as (a int, b test_prevent_drop_attribute.sub);

			 CREATE TABLE test_prevent_drop_attribute.test_iceberg (a int, b test_prevent_drop_attribute.top) USING iceberg;
			 CREATE TABLE test_prevent_drop_attribute.test_heap (a int, b test_prevent_drop_attribute.unrelated) USING heap;

			 INSERT INTO test_prevent_drop_attribute.test_iceberg VALUES (1, (2, (3, 4)));
		""",
        pg_conn,
    )
    pg_conn.commit()

    # first, show that we do not interfere with heap tables
    run_command(
        "ALTER TYPE test_prevent_drop_attribute.unrelated DROP ATTRIBUTE b", pg_conn
    )

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE test_prevent_drop_attribute.top DROP ATTRIBUTE b",
        pg_conn,
        raise_error=False,
    )
    print(str(error))
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # now, show that we cannot alter a type that is not directly (e.g., recursively) a type used as a column in the table
    error = run_command(
        "ALTER TYPE test_prevent_drop_attribute.sub DROP ATTRIBUTE b",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    run_command("DROP SCHEMA test_prevent_drop_attribute CASCADE;", pg_conn)
    pg_conn.commit()


def test_prevent_add_attribute(pg_conn, extension, s3, with_default_location):

    run_command(
        """
			 CREATE SCHEMA test_prevent_add_attribute;
			 CREATE TYPE test_prevent_add_attribute.sub as (a int, b int);
			 CREATE TYPE test_prevent_add_attribute.top as (a int, b test_prevent_add_attribute.sub);
			 CREATE TYPE test_prevent_add_attribute.unrelated as (a int, b test_prevent_add_attribute.sub);

			 CREATE TABLE test_prevent_add_attribute.test_iceberg (a int, b test_prevent_add_attribute.top) USING iceberg;
			 CREATE TABLE test_prevent_add_attribute.test_heap (a int, b test_prevent_add_attribute.unrelated) USING heap;

			 INSERT INTO test_prevent_add_attribute.test_iceberg VALUES (1, (2, (3, 4)));
		""",
        pg_conn,
    )
    pg_conn.commit()

    # first, show that we do not interfere with heap tables
    run_command(
        "ALTER TYPE test_prevent_add_attribute.unrelated ADD ATTRIBUTE c INT", pg_conn
    )

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE test_prevent_add_attribute.top ADD ATTRIBUTE c INT",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # now, show that we cannot alter a type that is not directly (e.g., recursively) a type used as a column in the table
    error = run_command(
        "ALTER TYPE test_prevent_add_attribute.sub ADD ATTRIBUTE c INT",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    run_command("DROP SCHEMA test_prevent_add_attribute CASCADE;", pg_conn)
    pg_conn.commit()


def test_do_no_interfere_with_pg(pg_conn, extension):

    # nothing to assert, just make sure no errors thrown
    run_command(
        """
		    CREATE SCHEMA test_do_no_interfere_with_pg;
		    SET search_path TO test_do_no_interfere_with_pg;

			create table parted_conflict (a int, b text) partition by range (a);
			create table parted_conflict_1 partition of parted_conflict for values from (0) to (1000) partition by range (a);
			create table parted_conflict_1_1 partition of parted_conflict_1 for values from (0) to (500);
			create unique index on only parted_conflict_1 (a);
			create unique index on only parted_conflict (a);
			alter index parted_conflict_a_idx attach partition parted_conflict_1_a_idx;

			ALTER FOREIGN TABLE IF EXISTS test_do_no_interfere_with_pg.doesnt_exist_ft1 ADD COLUMN c4 integer;

			CREATE UNLOGGED SEQUENCE sequence_test_unlogged;
			ALTER SEQUENCE sequence_test_unlogged SET LOGGED;

			ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;
		""",
        pg_conn,
    )
    pg_conn.rollback()


def test_prevent_enum_changes(pg_conn, extension, s3, with_default_location):

    run_command(
        """
				CREATE SCHEMA alter_enum;

				SET search_path TO alter_enum;

				CREATE TYPE alter_enum.mood_1 AS ENUM ('happy', 'sad', 'neutral');
				CREATE TYPE alter_enum.mood_2 AS ENUM ('happy', 'sad', 'neutral');
				CREATE TYPE alter_enum.test_comp as (a int, b alter_enum.mood_2);

				CREATE TABLE alter_enum.people (
				    name TEXT NOT NULL,
				    mood alter_enum.mood_1,
				    c alter_enum.test_comp
				) USING iceberg;

		""",
        pg_conn,
    )
    pg_conn.commit()

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE alter_enum.mood_1 ADD VALUE 'excited';", pg_conn, raise_error=False
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE alter_enum.mood_2 ADD VALUE 'excited';", pg_conn, raise_error=False
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE alter_enum.mood_1 RENAME VALUE 'happy' TO 'a lot of excited';",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE alter_enum.mood_2 RENAME VALUE 'happy' TO 'a lot of excited';",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # DROP VALUE is prevented by Postgres
    error = run_command(
        "ALTER TYPE alter_enum.mood_1 DROP VALUE 'happy'", pg_conn, raise_error=False
    )
    assert "dropping an enum value is not implemented" in str(
        error
    ) or "syntax error at or near" in str(error)
    pg_conn.rollback()

    run_command("DROP SCHEMA alter_enum CASCADE;", pg_conn)
    pg_conn.commit()


def test_prevent_type_attribute_changes(pg_conn, extension, s3, with_default_location):

    run_command(
        """
				CREATE SCHEMA alter_type_rename;

				SET search_path TO alter_type_rename;

				CREATE TYPE alter_type_rename.test_comp as (a int, b int);
				CREATE TYPE alter_type_rename.test_comp_2 as (a int, b alter_type_rename.test_comp);


				-- Create the table
				CREATE TABLE alter_type_rename.people (
				    name TEXT NOT NULL,
				    a alter_type_rename.test_comp,
				    b alter_type_rename.test_comp_2
				) USING iceberg;

		""",
        pg_conn,
    )
    pg_conn.commit()

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE alter_type_rename.test_comp RENAME ATTRIBUTE a TO a_new;",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    # now, show that we cannot alter a type that is directly a type used as a column in the table
    error = run_command(
        "ALTER TYPE alter_type_rename.test_comp_2 RENAME ATTRIBUTE a TO a_new;",
        pg_conn,
        raise_error=False,
    )
    assert "because it is used in an iceberg table" in str(error)
    pg_conn.rollback()

    run_command("DROP SCHEMA alter_type_rename CASCADE;", pg_conn)
    pg_conn.commit()


def test_set_not_null(pg_conn, s3, with_default_location):
    # Create schema and table
    run_command("CREATE SCHEMA test_set_not_null;", pg_conn)
    run_command(
        "CREATE TABLE test_set_not_null.test_table (a int) USING iceberg;", pg_conn
    )

    run_command("INSERT INTO test_set_not_null.test_table VALUES (NULL);", pg_conn)

    res = run_command(
        "ALTER TABLE test_set_not_null.test_table alter column a SET NOT NULL;",
        pg_conn,
        raise_error=False,
    )
    assert (
        "ALTER TABLE ALTER COLUMN ... SET NOT NULL command not supported for pg_lake_iceberg tables"
        in str(res)
    )

    pg_conn.rollback()


def test_transaction_ddl(pg_conn, s3, with_default_location):

    run_command("BEGIN", pg_conn)

    run_command("CREATE SCHEMA test_transaction_ddl", pg_conn)
    run_command(
        "CREATE TABLE test_transaction_ddl.test USING iceberg AS SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command("ALTER TABLE test_transaction_ddl.test RENAME COLUMN i TO a", pg_conn)

    result = run_query("SELECT sum(a) FROM test_transaction_ddl.test", pg_conn)
    assert result[0][0] == 5050

    run_command("ALTER TABLE test_transaction_ddl.test ADD COLUMN i INT", pg_conn)
    run_command("UPDATE test_transaction_ddl.test SET i = a * 2", pg_conn)

    result = run_query("SELECT sum(i) FROM test_transaction_ddl.test", pg_conn)
    assert result[0][0] == 5050 * 2

    run_command("ALTER TABLE test_transaction_ddl.test DROP COLUMN a", pg_conn)
    result = run_query("SELECT sum(i) FROM test_transaction_ddl.test", pg_conn)
    assert result[0][0] == 5050 * 2

    # positional delete
    run_command("DELETE FROM test_transaction_ddl.test WHERE i = 100", pg_conn)
    result = run_query("SELECT sum(i) FROM test_transaction_ddl.test", pg_conn)
    assert result[0][0] == 5050 * 2 - 100

    # truncate
    run_command("TRUNCATE test_transaction_ddl.test", pg_conn)
    result = run_query("SELECT sum(i) FROM test_transaction_ddl.test", pg_conn)
    assert result[0][0] == None

    run_command("ROLLBACK", pg_conn)


# Intervals are not yet supported as Iceberg columns
def test_interval(pg_conn, s3, with_default_location):
    error = run_command(
        """
        CREATE SCHEMA test_interval;
        CREATE TABLE test_interval.test (i interval) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "not yet supported" in error

    pg_conn.rollback()

    error = run_command(
        """
        CREATE SCHEMA test_interval;
        CREATE TABLE test_interval.test (x int) USING iceberg;
        ALTER TABLE test_interval.test ADD COLUMN y interval[];
    """,
        pg_conn,
        raise_error=False,
    )
    assert "not yet supported" in error

    pg_conn.rollback()


def test_use_same_schema_when_needed(pg_conn, s3, with_default_location):

    run_command("CREATE SCHEMA test_use_same_schema_when_needed", pg_conn)
    pg_conn.commit()

    # add & drop column gets back to the same schema-id
    run_command(
        "CREATE TABLE test_use_same_schema_when_needed.tbl1(a int) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()
    schema_id_1 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )

    # sanity check
    assert schema_id_1 == 0

    # add column
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 ADD COLUMN b INT", pg_conn
    )
    pg_conn.commit()

    schema_id_2 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )

    # sanity check
    assert schema_id_2 == 1

    # drop column
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 DROP COLUMN b", pg_conn
    )
    pg_conn.commit()

    schema_id_3 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )

    # sanity check
    assert schema_id_3 == 0
    assert schema_id_3 == schema_id_1

    # adding a new column with the same name yields a different schema id
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 ADD COLUMN b INT", pg_conn
    )
    pg_conn.commit()
    schema_id_4 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )
    assert schema_id_4 == 2

    # but then dropping it goes back to schema 0
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 DROP COLUMN b", pg_conn
    )
    pg_conn.commit()
    schema_id_5 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )
    assert schema_id_5 == 0

    # now, test with rename
    # renaming the first column triggers a new schema
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 RENAME COLUMN a TO b",
        pg_conn,
    )
    pg_conn.commit()
    schema_id_5 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )
    assert schema_id_5 == 3

    # now, get back to the initial schema
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 RENAME COLUMN b TO a",
        pg_conn,
    )
    pg_conn.commit()
    schema_id_6 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )
    assert schema_id_6 == 0

    # now, adding a default triggers a new schema
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 ALTER COLUMN a SET DEFAULT 42;",
        pg_conn,
    )
    pg_conn.commit()
    schema_id_7 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )
    assert schema_id_7 == 4

    # now, get back to the initial schema
    run_command(
        "ALTER TABLE test_use_same_schema_when_needed.tbl1 ALTER COLUMN a DROP DEFAULT;",
        pg_conn,
    )
    pg_conn.commit()
    schema_id_8 = get_current_schema_id(
        pg_conn, s3, "test_use_same_schema_when_needed", "tbl1"
    )
    assert schema_id_8 == 0

    run_command("DROP SCHEMA test_use_same_schema_when_needed CASCADE", pg_conn)
    pg_conn.commit()


def test_alter_column_type_int_to_bigint(pg_conn, s3, with_default_location):
    """Test allowed Iceberg type promotion: int -> bigint"""
    run_command("CREATE SCHEMA test_alter_type;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type.tbl (a int, b int) USING iceberg;", pg_conn
    )
    run_command("INSERT INTO test_alter_type.tbl VALUES (1, 2);", pg_conn)
    pg_conn.commit()

    # promote int -> bigint (allowed by Iceberg spec)
    run_command("ALTER TABLE test_alter_type.tbl ALTER COLUMN a TYPE bigint;", pg_conn)
    pg_conn.commit()

    # verify column type changed
    result = run_query(
        "SELECT atttypid::regtype FROM pg_attribute WHERE attrelid = 'test_alter_type.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["bigint"]]

    # insert data with new type range
    run_command(f"INSERT INTO test_alter_type.tbl VALUES ({2**40}, 3);", pg_conn)

    # verify all data is readable
    results = run_query("SELECT a, b FROM test_alter_type.tbl ORDER BY b ASC", pg_conn)
    assert results == [[1, 2], [2**40, 3]]

    # verify Iceberg metadata reflects the type change
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'tbl' AND table_namespace = 'test_alter_type'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]

    # the field type should be "long" in Iceberg (mapped from bigint)
    a_field = [f for f in last_schema["fields"] if f["name"] == "a"][0]
    assert a_field["type"] == "long"

    run_command("DROP SCHEMA test_alter_type CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_smallint_to_bigint(pg_conn, s3, with_default_location):
    """Test allowed Iceberg type promotion: smallint -> bigint"""
    run_command("CREATE SCHEMA test_alter_type_sm;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_sm.tbl (a smallint, b int) USING iceberg;",
        pg_conn,
    )
    run_command("INSERT INTO test_alter_type_sm.tbl VALUES (1, 2);", pg_conn)
    pg_conn.commit()

    run_command(
        "ALTER TABLE test_alter_type_sm.tbl ALTER COLUMN a TYPE bigint;", pg_conn
    )
    pg_conn.commit()

    result = run_query(
        "SELECT atttypid::regtype FROM pg_attribute WHERE attrelid = 'test_alter_type_sm.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["bigint"]]

    run_command(f"INSERT INTO test_alter_type_sm.tbl VALUES ({2**40}, 3);", pg_conn)

    results = run_query(
        "SELECT a, b FROM test_alter_type_sm.tbl ORDER BY b ASC", pg_conn
    )
    assert results == [[1, 2], [2**40, 3]]

    run_command("DROP SCHEMA test_alter_type_sm CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_float_to_double(pg_conn, s3, with_default_location):
    """Test allowed Iceberg type promotion: float -> double"""
    run_command("CREATE SCHEMA test_alter_type_f;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_f.tbl (a real, b int) USING iceberg;", pg_conn
    )
    run_command("INSERT INTO test_alter_type_f.tbl VALUES (1.5, 1);", pg_conn)
    pg_conn.commit()

    # promote float4 -> float8 (allowed by Iceberg spec)
    run_command(
        "ALTER TABLE test_alter_type_f.tbl ALTER COLUMN a TYPE double precision;",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT atttypid::regtype FROM pg_attribute WHERE attrelid = 'test_alter_type_f.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["double precision"]]

    run_command(
        "INSERT INTO test_alter_type_f.tbl VALUES (1.23456789012345, 2);", pg_conn
    )

    results = run_query(
        "SELECT a, b FROM test_alter_type_f.tbl ORDER BY b ASC", pg_conn
    )
    assert results == [[1.5, 1], [1.23456789012345, 2]]

    # verify Iceberg metadata
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'tbl' AND table_namespace = 'test_alter_type_f'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    a_field = [f for f in last_schema["fields"] if f["name"] == "a"][0]
    assert a_field["type"] == "double"

    run_command("DROP SCHEMA test_alter_type_f CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_decimal_widen_precision(pg_conn, s3, with_default_location):
    """Test allowed Iceberg type promotion: decimal(P,S) -> decimal(P',S) where P' > P"""
    run_command("CREATE SCHEMA test_alter_type_d;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_d.tbl (a numeric(10,2), b int) USING iceberg;",
        pg_conn,
    )
    run_command("INSERT INTO test_alter_type_d.tbl VALUES (12345.67, 1);", pg_conn)
    pg_conn.commit()

    # widen precision: numeric(10,2) -> numeric(20,2) (allowed by Iceberg spec)
    run_command(
        "ALTER TABLE test_alter_type_d.tbl ALTER COLUMN a TYPE numeric(20,2);", pg_conn
    )
    pg_conn.commit()

    # insert data that requires wider precision
    run_command(
        "INSERT INTO test_alter_type_d.tbl VALUES (123456789012345.67, 2);", pg_conn
    )

    results = run_query(
        "SELECT a, b FROM test_alter_type_d.tbl ORDER BY b ASC", pg_conn
    )
    assert len(results) == 2
    assert str(results[0][0]) == "12345.67"
    assert str(results[1][0]) == "123456789012345.67"

    # verify Iceberg metadata
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'tbl' AND table_namespace = 'test_alter_type_d'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    a_field = [f for f in last_schema["fields"] if f["name"] == "a"][0]
    assert a_field["type"] == "decimal(20,2)"

    run_command("DROP SCHEMA test_alter_type_d CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_disallowed_promotions(pg_conn, s3, with_default_location):
    """Test that disallowed type promotions are rejected for Iceberg tables"""
    run_command("CREATE SCHEMA test_alter_type_dis;", pg_conn)
    run_command(
        """CREATE TABLE test_alter_type_dis.tbl (
            a int,
            b bigint,
            c double precision,
            d numeric(10,2),
            e text,
            f real
        ) USING iceberg;""",
        pg_conn,
    )
    pg_conn.commit()

    disallowed_cases = [
        # int -> varchar (not an allowed promotion)
        ("a", "varchar(255)", "int to varchar"),
        # bigint -> int (narrowing not allowed)
        ("b", "int", "bigint to int"),
        # double -> float (narrowing not allowed)
        ("c", "real", "double to float"),
        # decimal: changing scale not allowed
        ("d", "numeric(10,3)", "decimal scale change"),
        # decimal: narrowing precision not allowed
        ("d", "numeric(8,2)", "decimal precision narrowing"),
        # text -> int (not an allowed promotion)
        ("e", "int", "text to int"),
        # int -> real (not an allowed promotion in Iceberg spec)
        ("a", "real", "int to real"),
        # float -> int (not an allowed promotion)
        ("f", "int", "float to int"),
    ]

    for col, new_type, description in disallowed_cases:
        error = run_command(
            f"ALTER TABLE test_alter_type_dis.tbl ALTER COLUMN {col} TYPE {new_type};",
            pg_conn,
            raise_error=False,
        )
        assert "command not supported for pg_lake_iceberg tables" in str(
            error
        ), f"Expected error for {description}, got: {error}"
        assert "Allowed type promotions for Iceberg tables are" in str(
            error
        ), f"Expected detail about allowed promotions for {description}, got: {error}"
        pg_conn.rollback()

    run_command("DROP SCHEMA test_alter_type_dis CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_using_clause_rejected(pg_conn, s3, with_default_location):
    """Test that USING clause is rejected for Iceberg type promotions.

    Iceberg schema evolution is metadata-only; data files are never rewritten,
    so arbitrary USING expressions are not supported.
    """
    run_command("CREATE SCHEMA test_alter_using;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_using.tbl (a int, b real) USING iceberg;",
        pg_conn,
    )
    run_command("INSERT INTO test_alter_using.tbl VALUES (1, 1.5);", pg_conn)
    pg_conn.commit()

    # USING with an otherwise-allowed promotion (int -> bigint)
    error = run_command(
        "ALTER TABLE test_alter_using.tbl ALTER COLUMN a TYPE bigint USING a::bigint;",
        pg_conn,
        raise_error=False,
    )
    assert "command not supported for pg_lake_iceberg tables" in str(error)
    assert "USING requires rewriting data files" in str(error)
    pg_conn.rollback()

    # USING with an expression on an allowed promotion (float -> double)
    error = run_command(
        "ALTER TABLE test_alter_using.tbl ALTER COLUMN b TYPE double precision USING b * 2;",
        pg_conn,
        raise_error=False,
    )
    assert "command not supported for pg_lake_iceberg tables" in str(error)
    assert "USING requires rewriting data files" in str(error)
    pg_conn.rollback()

    # Verify the table is intact (nothing changed)
    result = run_query("SELECT a, b FROM test_alter_using.tbl;", pg_conn)
    assert result == [[1, 1.5]]

    run_command("DROP SCHEMA test_alter_using CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_same_type_noop(pg_conn, s3, with_default_location):
    """Test that changing to the same type is allowed (no-op)"""
    run_command("CREATE SCHEMA test_alter_type_noop;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_noop.tbl (a int, b numeric(10,2)) USING iceberg;",
        pg_conn,
    )
    run_command("INSERT INTO test_alter_type_noop.tbl VALUES (1, 2.50);", pg_conn)
    pg_conn.commit()

    # same type should be allowed
    run_command(
        "ALTER TABLE test_alter_type_noop.tbl ALTER COLUMN a TYPE int;", pg_conn
    )
    run_command(
        "ALTER TABLE test_alter_type_noop.tbl ALTER COLUMN b TYPE numeric(10,2);",
        pg_conn,
    )

    results = run_query("SELECT a, b FROM test_alter_type_noop.tbl", pg_conn)
    assert results == [[1, Decimal("2.50")]]

    run_command("DROP SCHEMA test_alter_type_noop CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_on_empty_table(pg_conn, s3, with_default_location):
    """Test type promotion on a table with no data"""
    run_command("CREATE SCHEMA test_alter_type_empty;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_empty.tbl (a int, b real) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "ALTER TABLE test_alter_type_empty.tbl ALTER COLUMN a TYPE bigint;", pg_conn
    )
    run_command(
        "ALTER TABLE test_alter_type_empty.tbl ALTER COLUMN b TYPE double precision;",
        pg_conn,
    )
    pg_conn.commit()

    # insert data with promoted types
    run_command(
        f"INSERT INTO test_alter_type_empty.tbl VALUES ({2**40}, 1.23456789012345);",
        pg_conn,
    )

    results = run_query("SELECT a FROM test_alter_type_empty.tbl", pg_conn)
    assert results == [[2**40]]

    run_command("DROP SCHEMA test_alter_type_empty CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_combined_with_other_ddl(pg_conn, s3, with_default_location):
    """Test type promotion combined with other DDL operations in the same ALTER TABLE"""
    run_command("CREATE SCHEMA test_alter_type_combo;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_combo.tbl (a int, b int, c int) USING iceberg;",
        pg_conn,
    )
    run_command("INSERT INTO test_alter_type_combo.tbl VALUES (1, 2, 3);", pg_conn)
    pg_conn.commit()

    # combine type change with add and drop column
    run_command(
        "ALTER TABLE test_alter_type_combo.tbl ALTER COLUMN a TYPE bigint, ADD COLUMN d int, DROP COLUMN c;",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "SELECT atttypid::regtype FROM pg_attribute WHERE attrelid = 'test_alter_type_combo.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["bigint"]]

    run_command(
        f"INSERT INTO test_alter_type_combo.tbl VALUES ({2**40}, 20, 30);", pg_conn
    )

    results = run_query(
        "SELECT a, b, d FROM test_alter_type_combo.tbl ORDER BY b ASC", pg_conn
    )
    assert results == [[1, 2, None], [2**40, 20, 30]]

    run_command("DROP SCHEMA test_alter_type_combo CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_metadata_schema_evolution(
    pg_conn, s3, with_default_location
):
    """Test that type promotion creates a new schema in Iceberg metadata"""
    run_command("CREATE SCHEMA test_alter_type_meta;", pg_conn)
    run_command(
        "CREATE TABLE test_alter_type_meta.tbl (a int, b int) USING iceberg;", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'tbl' AND table_namespace = 'test_alter_type_meta'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    assert returned_json["current-schema-id"] == 0
    initial_schema = returned_json["schemas"][0]
    assert initial_schema == {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "a", "type": "int", "required": False},
            {"id": 2, "name": "b", "type": "int", "required": False},
        ],
    }

    # promote a: int -> long
    run_command(
        "ALTER TABLE test_alter_type_meta.tbl ALTER COLUMN a TYPE bigint;", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'tbl' AND table_namespace = 'test_alter_type_meta'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    assert returned_json["current-schema-id"] == 1
    new_schema = returned_json["schemas"][-1]
    assert new_schema == {
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {"id": 1, "name": "a", "type": "long", "required": False},
            {"id": 2, "name": "b", "type": "int", "required": False},
        ],
    }

    # promote b: int -> long
    run_command(
        "ALTER TABLE test_alter_type_meta.tbl ALTER COLUMN b TYPE bigint;", pg_conn
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'tbl' AND table_namespace = 'test_alter_type_meta'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    assert returned_json["current-schema-id"] == 2
    new_schema = returned_json["schemas"][-1]
    assert new_schema == {
        "type": "struct",
        "schema-id": 2,
        "fields": [
            {"id": 1, "name": "a", "type": "long", "required": False},
            {"id": 2, "name": "b", "type": "long", "required": False},
        ],
    }

    run_command("DROP SCHEMA test_alter_type_meta CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_partitioned_table(pg_conn, s3, with_default_location):
    """Test type promotion on a partitioned iceberg table does not break
    partitioning, partition pruning, or data file pruning."""
    schema = "test_alter_type_part"
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "
    run_command(f"CREATE SCHEMA {schema};", pg_conn)

    # Create a partitioned table: partition by identity on column b
    # Use separate inserts so each value of (a, b) lands in its own data file.
    run_command(
        f"""
        CREATE TABLE {schema}.tbl (a int, b int)
        USING iceberg WITH (partition_by = 'b');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Insert 4 rows into separate data files across 2 partitions (b=10, b=20)
    run_command(f"INSERT INTO {schema}.tbl VALUES (1, 10);", pg_conn)
    run_command(f"INSERT INTO {schema}.tbl VALUES (2, 10);", pg_conn)
    run_command(f"INSERT INTO {schema}.tbl VALUES (3, 20);", pg_conn)
    run_command(f"INSERT INTO {schema}.tbl VALUES (4, 20);", pg_conn)
    pg_conn.commit()

    # ── Baseline: verify pruning works BEFORE type promotion ──

    # Partition pruning: b=10 should scan 2 data files (skip the b=20 partition)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE b = 10", pg_conn
    )
    assert fetch_data_files_used(results) == "2"

    # Data file pruning on non-partition column: a=1 should scan 1 file
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE a = 1", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # Combined: b=10 AND a=1 → 1 file
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE b = 10 AND a = 1",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "1"

    # ── Promote non-partition column a: int → bigint ──
    run_command(f"ALTER TABLE {schema}.tbl ALTER COLUMN a TYPE bigint;", pg_conn)
    pg_conn.commit()

    # Verify column type changed
    result = run_query(
        f"SELECT atttypid::regtype FROM pg_attribute "
        f"WHERE attrelid = '{schema}.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["bigint"]]

    # Insert bigint-range data into each partition
    run_command(f"INSERT INTO {schema}.tbl VALUES ({2**40}, 10);", pg_conn)
    run_command(f"INSERT INTO {schema}.tbl VALUES ({2**41}, 20);", pg_conn)
    pg_conn.commit()

    # Verify all data readable
    results = run_query(f"SELECT a, b FROM {schema}.tbl ORDER BY a ASC", pg_conn)
    assert results == [
        [1, 10],
        [2, 10],
        [3, 20],
        [4, 20],
        [2**40, 10],
        [2**41, 20],
    ]

    # ── Verify pruning still works AFTER promoting non-partition column ──

    # Partition pruning: b=10 → 3 files (2 old + 1 new in b=10 partition)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE b = 10", pg_conn
    )
    assert fetch_data_files_used(results) == "3"

    # Data file pruning: a=1 → still 1 file (old pre-promotion data file)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE a = 1", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # Data file pruning: a={2**40} → 1 file (the new bigint data file)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE a = {2**40}",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "1"

    # ── Verify Iceberg metadata reflects the type change ──
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 'tbl' AND table_namespace = '{schema}'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    a_field = [f for f in last_schema["fields"] if f["name"] == "a"][0]
    assert a_field["type"] == "long"

    # Partition spec still intact
    partition_specs = returned_json["partition-specs"]
    last_spec = partition_specs[-1]
    assert len(last_spec["fields"]) == 1  # identity(b)

    # ── Promote the partition column b: int → bigint ──
    run_command(f"ALTER TABLE {schema}.tbl ALTER COLUMN b TYPE bigint;", pg_conn)
    pg_conn.commit()

    # Verify column type changed
    result = run_query(
        f"SELECT atttypid::regtype FROM pg_attribute "
        f"WHERE attrelid = '{schema}.tbl'::regclass AND attname = 'b'",
        pg_conn,
    )
    assert result == [["bigint"]]

    # Insert data into a new partition value after promoting partition column
    run_command(f"INSERT INTO {schema}.tbl VALUES (100, 30);", pg_conn)
    pg_conn.commit()

    # Verify all data readable
    results = run_query(f"SELECT a, b FROM {schema}.tbl ORDER BY a ASC", pg_conn)
    assert results == [
        [1, 10],
        [2, 10],
        [3, 20],
        [4, 20],
        [100, 30],
        [2**40, 10],
        [2**41, 20],
    ]

    # ── Verify pruning still works AFTER promoting partition column ──

    # Partition pruning: b=10 → 3 files (old b=10 partition)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE b = 10", pg_conn
    )
    assert fetch_data_files_used(results) == "3"

    # Partition pruning: b=30 → 1 file (new partition after promotion)
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE b = 30", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # Data file pruning: a=3 → 1 file
    results = run_query(
        f"{explain_prefix} SELECT * FROM {schema}.tbl WHERE a = 3", pg_conn
    )
    assert fetch_data_files_used(results) == "1"

    # Verify schema shows both promotions
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 'tbl' AND table_namespace = '{schema}'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    last_schema = returned_json["schemas"][-1]
    a_field = [f for f in last_schema["fields"] if f["name"] == "a"][0]
    b_field = [f for f in last_schema["fields"] if f["name"] == "b"][0]
    assert a_field["type"] == "long"
    assert b_field["type"] == "long"

    run_command(f"DROP SCHEMA {schema} CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_column_type_spark_comparison(
    installcheck, spark_session, pg_conn, s3, with_default_location
):
    """Create the same table in Spark and pg_lake, perform identical type promotions,
    verify both produce the same results, compare metadata schemas, and confirm
    Spark can read pg_lake's metadata.json after type promotion."""
    if installcheck:
        return

    pg_schema = "test_type_promo_pg"
    spark_ns = "public"

    run_command(f"CREATE SCHEMA {pg_schema};", pg_conn)

    # ── 1. int → bigint ──

    # Spark side
    spark_session.sql(
        f"CREATE TABLE {spark_ns}.spark_int_promo (a int, b int) USING iceberg"
    )
    spark_session.sql(f"INSERT INTO {spark_ns}.spark_int_promo VALUES (1, 10), (2, 20)")
    spark_session.sql(
        f"ALTER TABLE {spark_ns}.spark_int_promo ALTER COLUMN a TYPE bigint"
    )
    spark_session.sql(f"INSERT INTO {spark_ns}.spark_int_promo VALUES ({2**40}, 30)")

    # pg_lake side — same operations
    run_command(
        f"CREATE TABLE {pg_schema}.int_tbl (a int, b int) USING iceberg;", pg_conn
    )
    run_command(f"INSERT INTO {pg_schema}.int_tbl VALUES (1, 10), (2, 20);", pg_conn)
    pg_conn.commit()
    run_command(f"ALTER TABLE {pg_schema}.int_tbl ALTER COLUMN a TYPE bigint;", pg_conn)
    pg_conn.commit()
    run_command(f"INSERT INTO {pg_schema}.int_tbl VALUES ({2**40}, 30);", pg_conn)
    pg_conn.commit()

    # Compare: query both natively
    spark_query = f"SELECT a, b FROM {spark_ns}.spark_int_promo ORDER BY b ASC"
    pg_query = f"SELECT a, b FROM {pg_schema}.int_tbl ORDER BY b ASC"

    pg_lake_result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    assert len(pg_lake_result) == 3
    assert pg_lake_result == [[1, 10], [2, 20], [2**40, 30]]

    # Compare full schemas JSON
    spark_metadata_loc = (
        spark_session.sql(
            f"SELECT file FROM {spark_ns}.spark_int_promo.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )
    spark_json = normalize_json(read_s3_operations(s3, spark_metadata_loc))

    pg_metadata_loc = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 'int_tbl' AND table_namespace = '{pg_schema}'",
        pg_conn,
    )[0][0]
    pg_json = normalize_json(read_s3_operations(s3, pg_metadata_loc))

    assert_iceberg_schemas_equal(spark_json, pg_json, "int→bigint")

    # Verify Spark can read pg_lake's metadata.json
    # Disable vectorized reading: Spark 3.5 / Iceberg 1.4.3 vectorized reader
    # crashes with ClassCastException when an old data file's Parquet physical
    # type (int32) doesn't match the current schema type (int64) after promotion.
    spark_session.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
    spark_register_table(
        installcheck, spark_session, "int_tbl", pg_schema, pg_metadata_loc
    )
    spark_cross_query = f"SELECT a, b FROM {pg_schema}.int_tbl ORDER BY b ASC"
    spark_cross = spark_session.sql(spark_cross_query).collect()
    assert len(spark_cross) == 3
    assert [spark_cross[0].a, spark_cross[0].b] == [1, 10]
    assert [spark_cross[1].a, spark_cross[1].b] == [2, 20]
    assert [spark_cross[2].a, spark_cross[2].b] == [2**40, 30]
    spark_unregister_table(installcheck, spark_session, "int_tbl", pg_schema)
    spark_session.conf.set("spark.sql.iceberg.vectorization.enabled", "true")

    spark_session.sql(f"DROP TABLE {spark_ns}.spark_int_promo")

    # ── 2. float → double ──

    # Spark side
    spark_session.sql(
        f"CREATE TABLE {spark_ns}.spark_float_promo (a float, b int) USING iceberg"
    )
    spark_session.sql(f"INSERT INTO {spark_ns}.spark_float_promo VALUES (1.5, 1)")
    spark_session.sql(
        f"ALTER TABLE {spark_ns}.spark_float_promo ALTER COLUMN a TYPE double"
    )
    spark_session.sql(
        f"INSERT INTO {spark_ns}.spark_float_promo VALUES (1.23456789012345, 2)"
    )

    # pg_lake side
    run_command(
        f"CREATE TABLE {pg_schema}.float_tbl (a real, b int) USING iceberg;", pg_conn
    )
    run_command(f"INSERT INTO {pg_schema}.float_tbl VALUES (1.5, 1);", pg_conn)
    pg_conn.commit()
    run_command(
        f"ALTER TABLE {pg_schema}.float_tbl ALTER COLUMN a TYPE double precision;",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"INSERT INTO {pg_schema}.float_tbl VALUES (1.23456789012345, 2);", pg_conn
    )
    pg_conn.commit()

    spark_query = f"SELECT a, b FROM {spark_ns}.spark_float_promo ORDER BY b ASC"
    pg_query = f"SELECT a, b FROM {pg_schema}.float_tbl ORDER BY b ASC"

    pg_lake_result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    assert len(pg_lake_result) == 2

    # Compare full schemas JSON
    spark_metadata_loc = (
        spark_session.sql(
            f"SELECT file FROM {spark_ns}.spark_float_promo.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )
    spark_json = normalize_json(read_s3_operations(s3, spark_metadata_loc))

    pg_metadata_loc = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 'float_tbl' AND table_namespace = '{pg_schema}'",
        pg_conn,
    )[0][0]
    pg_json = normalize_json(read_s3_operations(s3, pg_metadata_loc))

    assert_iceberg_schemas_equal(spark_json, pg_json, "float→double")

    # Verify Spark can read pg_lake's metadata.json (vectorized off, same reason)
    spark_session.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
    spark_register_table(
        installcheck, spark_session, "float_tbl", pg_schema, pg_metadata_loc
    )
    spark_cross_query = f"SELECT a, b FROM {pg_schema}.float_tbl ORDER BY b ASC"
    spark_cross = spark_session.sql(spark_cross_query).collect()
    assert len(spark_cross) == 2
    assert spark_cross[0].a == pytest.approx(1.5, abs=1e-6)
    assert spark_cross[1].a == pytest.approx(1.23456789012345, abs=1e-10)
    spark_unregister_table(installcheck, spark_session, "float_tbl", pg_schema)
    spark_session.conf.set("spark.sql.iceberg.vectorization.enabled", "true")

    spark_session.sql(f"DROP TABLE {spark_ns}.spark_float_promo")

    # ── 3. decimal(P,S) → decimal(P',S) where P' > P ──

    # Spark side
    spark_session.sql(
        f"CREATE TABLE {spark_ns}.spark_dec_promo (a decimal(10,2), b int) USING iceberg"
    )
    spark_session.sql(f"INSERT INTO {spark_ns}.spark_dec_promo VALUES (12345.67, 1)")
    spark_session.sql(
        f"ALTER TABLE {spark_ns}.spark_dec_promo ALTER COLUMN a TYPE decimal(20,2)"
    )
    spark_session.sql(
        f"INSERT INTO {spark_ns}.spark_dec_promo VALUES (123456789012345.67, 2)"
    )

    # pg_lake side
    run_command(
        f"CREATE TABLE {pg_schema}.dec_tbl (a numeric(10,2), b int) USING iceberg;",
        pg_conn,
    )
    run_command(f"INSERT INTO {pg_schema}.dec_tbl VALUES (12345.67, 1);", pg_conn)
    pg_conn.commit()
    run_command(
        f"ALTER TABLE {pg_schema}.dec_tbl ALTER COLUMN a TYPE numeric(20,2);", pg_conn
    )
    pg_conn.commit()
    run_command(
        f"INSERT INTO {pg_schema}.dec_tbl VALUES (123456789012345.67, 2);", pg_conn
    )
    pg_conn.commit()

    spark_query = f"SELECT a, b FROM {spark_ns}.spark_dec_promo ORDER BY b ASC"
    pg_query = f"SELECT a, b FROM {pg_schema}.dec_tbl ORDER BY b ASC"

    pg_lake_result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    assert len(pg_lake_result) == 2

    # Compare full schemas JSON
    spark_metadata_loc = (
        spark_session.sql(
            f"SELECT file FROM {spark_ns}.spark_dec_promo.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )
    spark_json = normalize_json(read_s3_operations(s3, spark_metadata_loc))

    pg_metadata_loc = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 'dec_tbl' AND table_namespace = '{pg_schema}'",
        pg_conn,
    )[0][0]
    pg_json = normalize_json(read_s3_operations(s3, pg_metadata_loc))

    assert_iceberg_schemas_equal(spark_json, pg_json, "decimal widening")

    # Verify Spark can read pg_lake's metadata.json
    spark_register_table(
        installcheck, spark_session, "dec_tbl", pg_schema, pg_metadata_loc
    )
    spark_cross_query = f"SELECT a, b FROM {pg_schema}.dec_tbl ORDER BY b ASC"
    spark_cross = spark_session.sql(spark_cross_query).collect()
    assert len(spark_cross) == 2
    assert str(spark_cross[0].a) == "12345.67"
    assert str(spark_cross[1].a) == "123456789012345.67"
    spark_unregister_table(installcheck, spark_session, "dec_tbl", pg_schema)

    spark_session.sql(f"DROP TABLE {spark_ns}.spark_dec_promo")

    # ── 4. partitioned table: int → bigint on partition column ──

    # Spark side
    spark_session.sql(
        f"CREATE TABLE {spark_ns}.spark_part_promo (a int, b int) "
        f"USING iceberg PARTITIONED BY (b)"
    )
    spark_session.sql(
        f"INSERT INTO {spark_ns}.spark_part_promo VALUES (1, 10), (2, 20)"
    )
    spark_session.sql(
        f"ALTER TABLE {spark_ns}.spark_part_promo ALTER COLUMN b TYPE bigint"
    )
    spark_session.sql(f"INSERT INTO {spark_ns}.spark_part_promo VALUES (3, 30)")

    # pg_lake side
    run_command(
        f"CREATE TABLE {pg_schema}.part_tbl (a int, b int) "
        f"USING iceberg WITH (partition_by = 'b');",
        pg_conn,
    )
    run_command(f"INSERT INTO {pg_schema}.part_tbl VALUES (1, 10), (2, 20);", pg_conn)
    pg_conn.commit()
    run_command(
        f"ALTER TABLE {pg_schema}.part_tbl ALTER COLUMN b TYPE bigint;", pg_conn
    )
    pg_conn.commit()
    run_command(f"INSERT INTO {pg_schema}.part_tbl VALUES (3, 30);", pg_conn)
    pg_conn.commit()

    spark_query = f"SELECT a, b FROM {spark_ns}.spark_part_promo ORDER BY a ASC"
    pg_query = f"SELECT a, b FROM {pg_schema}.part_tbl ORDER BY a ASC"

    pg_lake_result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    assert len(pg_lake_result) == 3
    assert pg_lake_result == [[1, 10], [2, 20], [3, 30]]

    # Compare full schemas JSON
    spark_metadata_loc = (
        spark_session.sql(
            f"SELECT file FROM {spark_ns}.spark_part_promo.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )
    spark_json = normalize_json(read_s3_operations(s3, spark_metadata_loc))

    pg_metadata_loc = run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_name = 'part_tbl' AND table_namespace = '{pg_schema}'",
        pg_conn,
    )[0][0]
    pg_json = normalize_json(read_s3_operations(s3, pg_metadata_loc))

    assert_iceberg_schemas_equal(spark_json, pg_json, "partitioned int→bigint")

    # Verify Spark can read pg_lake's metadata.json (vectorized off, same reason)
    spark_session.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
    spark_register_table(
        installcheck, spark_session, "part_tbl", pg_schema, pg_metadata_loc
    )
    spark_cross_query = f"SELECT a, b FROM {pg_schema}.part_tbl ORDER BY a ASC"
    spark_cross = spark_session.sql(spark_cross_query).collect()
    assert len(spark_cross) == 3
    assert [spark_cross[0].a, spark_cross[0].b] == [1, 10]
    assert [spark_cross[1].a, spark_cross[1].b] == [2, 20]
    assert [spark_cross[2].a, spark_cross[2].b] == [3, 30]
    spark_unregister_table(installcheck, spark_session, "part_tbl", pg_schema)
    spark_session.conf.set("spark.sql.iceberg.vectorization.enabled", "true")

    spark_session.sql(f"DROP TABLE {spark_ns}.spark_part_promo")

    # cleanup
    run_command(f"DROP SCHEMA {pg_schema} CASCADE;", pg_conn)
    pg_conn.commit()


def get_current_schema_id(pg_conn, s3, namespace, name):

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{name}' and table_namespace='{namespace}'",
        pg_conn,
    )[0][0]
    returned_json = normalize_json(read_s3_operations(s3, metadata_location))
    return returned_json["current-schema-id"]


def filter_files_by_prefix(files, prefix):
    return [
        os.path.basename(file)
        for file in files
        if os.path.basename(file).startswith(prefix)
    ]


def get_sorted_metadata_files(folder_path):
    # Get all files that end with '.metadata.json' in the folder
    metadata_files = glob.glob(os.path.join(folder_path, "*.metadata.json"))

    # Sort the files based on their sizes in ascending order
    sorted_files = sorted(metadata_files, key=os.path.getsize)

    return sorted_files

import pytest
from utils_pytest import *

# A pg_lake table with uppercase names stands in for one written by Snowflake,
# which stores case-insensitive names in uppercase. The tests attach its
# metadata.json the way a table from another engine would be attached.

SCHEMA = "lowercase_names"


@pytest.fixture
def uppercase_source(pg_conn, superuser_conn, s3, extension, with_default_location):
    run_command(
        """
        CREATE OR REPLACE FUNCTION pg_lake_last_copy_pushed_down_test()
          RETURNS bool
          LANGUAGE C
        AS 'pg_lake_copy', $function$pg_lake_last_copy_pushed_down_test$function$;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
        CREATE SCHEMA {SCHEMA};
        CREATE TYPE {SCHEMA}.address AS ("CITY" text, "ZIP" int);
        CREATE TABLE {SCHEMA}.source (
            "ID" int,
            "Name" text,
            "HOME" {SCHEMA}.address,
            "PREVIOUS" {SCHEMA}.address[]
        ) USING iceberg;
        INSERT INTO {SCHEMA}.source
        SELECT i, 'name_' || i,
               ROW('city_' || i, i)::{SCHEMA}.address,
               ARRAY[ROW('old_' || i, -i)::{SCHEMA}.address]
        FROM generate_series(1,10) i;

        CREATE TABLE {SCHEMA}.colliding ("ID" int, id int) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield {
        "source": metadata_location(pg_conn, "source"),
        "colliding": metadata_location(pg_conn, "colliding"),
    }

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def metadata_location(pg_conn, table_name):
    return run_query(
        f"""
        SELECT metadata_location FROM iceberg_tables
        WHERE table_namespace = '{SCHEMA}' AND table_name = '{table_name}'
        """,
        pg_conn,
    )[0][0]


def column_names(pg_conn, table_name):
    rows = run_query(
        f"""
        SELECT attname FROM pg_attribute
        WHERE attrelid = '{table_name}'::regclass AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
        """,
        pg_conn,
    )
    return [row[0] for row in rows]


def struct_field_names(pg_conn, table_name, column_name):
    rows = run_query(
        f"""
        SELECT field.attname
        FROM pg_attribute column_attr
        JOIN pg_type struct_type ON struct_type.oid = column_attr.atttypid
        JOIN pg_attribute field ON field.attrelid = struct_type.typrelid
        WHERE column_attr.attrelid = '{table_name}'::regclass
          AND column_attr.attname = '{column_name}' AND field.attnum > 0
        ORDER BY field.attnum
        """,
        pg_conn,
    )
    return [row[0] for row in rows]


# the uppercase source, selected the way the lowercased tables are
SOURCE_ROWS = f"""
    SELECT "ID", "Name", ("HOME")."CITY", ("HOME")."ZIP",
           ("PREVIOUS"[1])."CITY", ("PREVIOUS"[1])."ZIP"
    FROM {SCHEMA}.source WHERE ("HOME")."ZIP" > {{min_zip}} ORDER BY "ID"
"""


def lowered_rows(table_name, min_zip=0):
    return f"""
        SELECT id, name, (home).city, (home).zip, (previous[1]).city, (previous[1]).zip
        FROM {table_name} WHERE (home).zip > {min_zip} ORDER BY id
    """


def test_metadata_path_table(pg_conn, uppercase_source):
    path = uppercase_source["source"]

    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{path}', lowercase_column_names 'true');
        CREATE FOREIGN TABLE {SCHEMA}.unchanged () SERVER pg_lake
        OPTIONS (path '{path}');
        CREATE FOREIGN TABLE {SCHEMA}.explicit (id int, name text,
            home {SCHEMA}.address, previous {SCHEMA}.address[]) SERVER pg_lake
        OPTIONS (path '{path}', lowercase_column_names 'true');
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert column_names(pg_conn, f"{SCHEMA}.lowered") == [
        "id",
        "name",
        "home",
        "previous",
    ]
    assert struct_field_names(pg_conn, f"{SCHEMA}.lowered", "home") == ["city", "zip"]
    assert column_names(pg_conn, f"{SCHEMA}.unchanged") == [
        "ID",
        "Name",
        "HOME",
        "PREVIOUS",
    ]

    expected = run_query(SOURCE_ROWS.format(min_zip=5), pg_conn)
    assert len(expected) == 5
    assert run_query(lowered_rows(f"{SCHEMA}.lowered", 5), pg_conn) == expected

    # declared columns are matched against the lowercased names; the
    # composite's own field names do not take part in the match
    result = run_query(
        f"""
        SELECT id, name, (home)."CITY", (previous[1])."ZIP"
        FROM {SCHEMA}.explicit WHERE id = 3
        """,
        pg_conn,
    )
    assert result == [[3, "name_3", "city_3", -3]]

    # pointing the table at a newer snapshot keeps the names lowercase
    run_command(f"INSERT INTO {SCHEMA}.source VALUES (11, 'name_11')", pg_conn)
    pg_conn.commit()
    run_command(
        f"""ALTER FOREIGN TABLE {SCHEMA}.lowered
            OPTIONS (SET path '{metadata_location(pg_conn, "source")}')""",
        pg_conn,
    )
    pg_conn.commit()
    result = run_query(f"SELECT id, name FROM {SCHEMA}.lowered WHERE id = 11", pg_conn)
    assert result == [[11, "name_11"]]


def test_metadata_path_table_errors(pg_conn, uppercase_source):
    error = run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.colliding_lowered () SERVER pg_lake
        OPTIONS (path '{uppercase_source["colliding"]}', lowercase_column_names 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert 'Iceberg field "id" collides with another field' in str(error)
    pg_conn.rollback()

    parquet_url = f"s3://{TEST_BUCKET}/{SCHEMA}/data.parquet"
    run_command(f"COPY (SELECT 1 AS \"ID\") TO '{parquet_url}'", pg_conn)
    error = run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.parquet_lowered () SERVER pg_lake
        OPTIONS (path '{parquet_url}', lowercase_column_names 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert (
        '"lowercase_column_names" option is only supported for iceberg format'
        in str(error)
    )
    pg_conn.rollback()

    # the column names were fixed at creation, so the option cannot change
    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{uppercase_source["source"]}', lowercase_column_names 'true')
        """,
        pg_conn,
    )
    pg_conn.commit()
    error = run_command(
        f"ALTER FOREIGN TABLE {SCHEMA}.lowered OPTIONS (SET lowercase_column_names 'false')",
        pg_conn,
        raise_error=False,
    )
    assert "The following table options can be changed: path" in str(error)
    pg_conn.rollback()


def test_insert_select_pushdown(pg_conn, uppercase_source):
    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{uppercase_source["source"]}', lowercase_column_names 'true');
        CREATE TABLE {SCHEMA}.target (LIKE {SCHEMA}.lowered) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    insert_all = f"INSERT INTO {SCHEMA}.target SELECT * FROM {SCHEMA}.lowered WHERE (home).zip > 5"
    insert_subset = f"""
        INSERT INTO {SCHEMA}.target (previous, id, home)
        SELECT previous, id, home FROM {SCHEMA}.lowered WHERE (previous[1]).zip >= -2
    """
    for query in [insert_all, insert_subset]:
        assert_query_pushdownable(query, pg_conn)
        run_command(query, pg_conn)
    pg_conn.commit()

    # read back through the target's own field ids, not the source names
    result = run_query(
        f"""
        SELECT id, name, (home).city, (home).zip, (previous[1]).city, (previous[1]).zip
        FROM {SCHEMA}.target ORDER BY id, name NULLS FIRST
        """,
        pg_conn,
    )
    assert result == [
        [1, None, "city_1", 1, "old_1", -1],
        [2, None, "city_2", 2, "old_2", -2],
    ] + run_query(SOURCE_ROWS.format(min_zip=5), pg_conn)

    run_command(
        f"CREATE TABLE {SCHEMA}.ctas USING iceberg AS SELECT * FROM {SCHEMA}.lowered",
        pg_conn,
    )
    pg_conn.commit()
    assert column_names(pg_conn, f"{SCHEMA}.ctas") == [
        "id",
        "name",
        "home",
        "previous",
    ]
    assert run_query(lowered_rows(f"{SCHEMA}.ctas"), pg_conn) == run_query(
        SOURCE_ROWS.format(min_zip=0), pg_conn
    )


@pytest.mark.parametrize("option", ["load_from", "definition_from"])
def test_create_table_from_metadata(pg_conn, uppercase_source, option):
    run_command(
        f"""
        CREATE TABLE {SCHEMA}.created () USING iceberg
        WITH ({option} = '{uppercase_source["source"]}', lowercase_column_names = true)
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert column_names(pg_conn, f"{SCHEMA}.created") == [
        "id",
        "name",
        "home",
        "previous",
    ]
    assert struct_field_names(pg_conn, f"{SCHEMA}.created", "home") == ["city", "zip"]

    # the option is consumed by the import and is not kept on the table
    options = run_query(
        f"""
        SELECT ftoptions FROM pg_foreign_table
        WHERE ftrelid = '{SCHEMA}.created'::regclass
        """,
        pg_conn,
    )[0][0]
    assert not any("lowercase_column_names" in option for option in options)

    if option == "load_from":
        assert run_query(lowered_rows(f"{SCHEMA}.created"), pg_conn) == run_query(
            SOURCE_ROWS.format(min_zip=0), pg_conn
        )
    else:
        assert run_query(f"SELECT count(*) FROM {SCHEMA}.created", pg_conn) == [[0]]


@pytest.mark.parametrize("lowercase", [True, False])
@pytest.mark.parametrize("pushdown", [True, False])
def test_copy_from_metadata(pg_conn, uppercase_source, lowercase, pushdown):
    path = uppercase_source["source"]

    # lowercase columns are bound to the uppercase source names either way,
    # the option only makes the read use the same names as the table
    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{path}', lowercase_column_names 'true');
        CREATE TABLE {SCHEMA}.target (LIKE {SCHEMA}.lowered) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # a column list disables pushdown
    column_list = "" if pushdown else "(id, name, home, previous)"
    copy_options = "format 'iceberg'"
    if lowercase:
        copy_options += ", lowercase_column_names true"

    run_command(
        f"COPY {SCHEMA}.target {column_list} FROM '{path}' WITH ({copy_options})",
        pg_conn,
    )
    assert run_query("SELECT pg_lake_last_copy_pushed_down_test()", pg_conn) == [
        [pushdown]
    ]
    pg_conn.commit()

    assert run_query(lowered_rows(f"{SCHEMA}.target"), pg_conn) == run_query(
        SOURCE_ROWS.format(min_zip=0), pg_conn
    )


def test_copy_from_option_requires_iceberg(pg_conn, uppercase_source):
    parquet_url = f"s3://{TEST_BUCKET}/{SCHEMA}/copy_from.parquet"
    run_command(
        f"""
        COPY (SELECT 1 AS "ID") TO '{parquet_url}';
        CREATE TABLE {SCHEMA}.target (id int) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        f"COPY {SCHEMA}.target FROM '{parquet_url}' WITH (lowercase_column_names true)",
        pg_conn,
        raise_error=False,
    )
    assert 'invalid option "lowercase_column_names"' in str(error)
    pg_conn.rollback()


def test_copy_to(pg_conn, uppercase_source):
    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{uppercase_source["source"]}', lowercase_column_names 'true');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # the file carries the lowercase column and struct field names
    url = f"s3://{TEST_BUCKET}/{SCHEMA}/copy_to.parquet"
    run_command(f"COPY (SELECT * FROM {SCHEMA}.lowered) TO '{url}'", pg_conn)
    run_command(
        f"CREATE FOREIGN TABLE {SCHEMA}.exported () SERVER pg_lake OPTIONS (path '{url}')",
        pg_conn,
    )
    pg_conn.commit()

    assert column_names(pg_conn, f"{SCHEMA}.exported") == [
        "id",
        "name",
        "home",
        "previous",
    ]
    assert struct_field_names(pg_conn, f"{SCHEMA}.exported", "home") == ["city", "zip"]
    assert run_query(lowered_rows(f"{SCHEMA}.exported"), pg_conn) == run_query(
        SOURCE_ROWS.format(min_zip=0), pg_conn
    )

import pytest
from utils_pytest import *

# A pg_lake table with uppercase names stands in for one written by Snowflake,
# which stores case-insensitive names in uppercase. The tests attach its
# metadata.json the way a table from another engine would be attached.

SCHEMA = "lowercase_names"


def create_copy_pushdown_probe(superuser_conn):
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


@pytest.fixture
def uppercase_source(pg_conn, superuser_conn, s3, extension, with_default_location):
    create_copy_pushdown_probe(superuser_conn)

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
        CREATE TABLE {SCHEMA}.system_names ("XMIN" int, "XMAX" int) USING iceberg;

        -- only an older schema has a case collision
        CREATE TABLE {SCHEMA}.old_collision ("ID" int) USING iceberg;
        ALTER TABLE {SCHEMA}.old_collision ADD COLUMN id int;
        ALTER TABLE {SCHEMA}.old_collision DROP COLUMN id;
        INSERT INTO {SCHEMA}.old_collision VALUES (1);
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield {
        "source": metadata_location(pg_conn, "source"),
        "colliding": metadata_location(pg_conn, "colliding"),
        "system_names": metadata_location(pg_conn, "system_names"),
        "old_collision": metadata_location(pg_conn, "old_collision"),
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

    # inferred columns are folded at creation, explicit ones on first read
    for columns, query in [("()", ""), ("(other int)", "SELECT * FROM {table}")]:
        table = f"{SCHEMA}.system_lowered"
        error = run_command(
            f"""
            CREATE FOREIGN TABLE {table} {columns} SERVER pg_lake
            OPTIONS (path '{uppercase_source["system_names"]}',
                     lowercase_column_names 'true');
            {query.format(table=table)}
            """,
            pg_conn,
            raise_error=False,
        )
        assert 'Iceberg column "xmin" conflicts with a system column' in str(error)
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


def test_collision_in_old_schema(pg_conn, uppercase_source):
    # only the current schema is read, so an old collision does not matter
    run_command(
        f"""
        CREATE FOREIGN TABLE {SCHEMA}.old_lowered () SERVER pg_lake
        OPTIONS (path '{uppercase_source["old_collision"]}',
                 lowercase_column_names 'true')
        """,
        pg_conn,
    )
    assert column_names(pg_conn, f"{SCHEMA}.old_lowered") == ["id"]
    assert run_query(f"SELECT id FROM {SCHEMA}.old_lowered", pg_conn) == [[1]]
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


@pytest.mark.parametrize("option", ["load_from", "definition_from"])
def test_create_table_from_parquet_rejects_option(pg_conn, uppercase_source, option):
    parquet_url = f"s3://{TEST_BUCKET}/{SCHEMA}/create_from.parquet"
    run_command(f"COPY (SELECT 1 AS \"ID\") TO '{parquet_url}'", pg_conn)
    pg_conn.commit()

    for access_method in ["USING iceberg", ""]:
        error = run_command(
            f"""
            CREATE TABLE {SCHEMA}.created () {access_method}
            WITH ({option} = '{parquet_url}', lowercase_column_names = true)
            """,
            pg_conn,
            raise_error=False,
        )
        assert (
            "lowercase_column_names is only supported when loading from Iceberg"
            in str(error)
        )
        pg_conn.rollback()


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


NESTED_SCHEMA = "lowercase_nested"


@pytest.fixture
def nested_source(pg_conn, superuser_conn, s3, extension, with_default_location):
    create_copy_pushdown_probe(superuser_conn)
    run_command(
        f"""
        DROP SCHEMA IF EXISTS {NESTED_SCHEMA} CASCADE;
        CREATE SCHEMA {NESTED_SCHEMA};
        CREATE TYPE {NESTED_SCHEMA}.geo AS ("LAT" float8, "LON" float8);
        CREATE TYPE {NESTED_SCHEMA}.address AS (
            "CITY" text, "GEO" {NESTED_SCHEMA}.geo, "TAGS" text[]);
        CREATE TYPE {NESTED_SCHEMA}.visit AS (
            "PLACE" {NESTED_SCHEMA}.address, "SCORES" int[]);

        -- the collision is three levels down, in a struct inside a list
        CREATE TYPE {NESTED_SCHEMA}.point AS ("X" int, x int);
        CREATE TYPE {NESTED_SCHEMA}.shape AS ("POINTS" {NESTED_SCHEMA}.point[]);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # a map from text to a struct, created by superuser like any map type
    spots_type = run_query(
        f"SELECT map_type.create('text', '{NESTED_SCHEMA}.geo')", superuser_conn
    )[0][0]
    superuser_conn.commit()

    run_command(
        f"""
        CREATE TABLE {NESTED_SCHEMA}.source (
            "ID" int,
            "HOME" {NESTED_SCHEMA}.address,
            "VISITS" {NESTED_SCHEMA}.visit[],
            "SPOTS" {spots_type}
        ) USING iceberg;
        INSERT INTO {NESTED_SCHEMA}.source
        SELECT i,
               ROW('city_' || i, ROW(i, -i)::{NESTED_SCHEMA}.geo,
                   ARRAY['tag_' || i])::{NESTED_SCHEMA}.address,
               ARRAY[ROW(ROW('place_' || i, ROW(i * 2, -i * 2)::{NESTED_SCHEMA}.geo,
                             ARRAY['visit_' || i])::{NESTED_SCHEMA}.address,
                         ARRAY[i, i + 1])::{NESTED_SCHEMA}.visit],
               ARRAY[('spot_' || i, ROW(i * 10, -i * 10)::{NESTED_SCHEMA}.geo)]::{spots_type}
        FROM generate_series(1,5) i;

        CREATE TABLE {NESTED_SCHEMA}.colliding ("ID" int, "SHAPE" {NESTED_SCHEMA}.shape)
        USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    def nested_location(table_name):
        return run_query(
            f"""
            SELECT metadata_location FROM iceberg_tables
            WHERE table_namespace = '{NESTED_SCHEMA}' AND table_name = '{table_name}'
            """,
            pg_conn,
        )[0][0]

    yield {
        "source": nested_location("source"),
        "colliding": nested_location("colliding"),
    }

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {NESTED_SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def nested_field_names(pg_conn, table_name):
    """Attribute names of every composite reachable from the table's columns,
    through arrays, map domains and composites at any depth."""
    rows = run_query(
        f"""
        WITH RECURSIVE reachable(type_id) AS (
            SELECT atttypid FROM pg_attribute
            WHERE attrelid = '{table_name}'::regclass AND attnum > 0 AND NOT attisdropped
          UNION
            SELECT child.type_id
            FROM reachable, LATERAL (
                SELECT typelem AS type_id FROM pg_type
                WHERE oid = reachable.type_id AND typelem <> 0
              UNION ALL
                SELECT typbasetype FROM pg_type
                WHERE oid = reachable.type_id AND typbasetype <> 0
              UNION ALL
                SELECT field.atttypid
                FROM pg_type struct_type
                JOIN pg_attribute field ON field.attrelid = struct_type.typrelid
                WHERE struct_type.oid = reachable.type_id AND field.attnum > 0
            ) child
        )
        SELECT DISTINCT field.attname
        FROM reachable
        JOIN pg_type struct_type ON struct_type.oid = reachable.type_id
        JOIN pg_attribute field ON field.attrelid = struct_type.typrelid
        WHERE field.attnum > 0 AND NOT field.attisdropped
        ORDER BY 1
        """,
        pg_conn,
    )
    return [row[0] for row in rows]


NESTED_SOURCE_ROWS = f"""
    SELECT "ID", ("HOME")."CITY", (("HOME")."GEO")."LAT", (("HOME")."GEO")."LON",
           ("HOME")."TAGS", ((("VISITS"[1])."PLACE")."GEO")."LAT",
           (("VISITS"[1])."PLACE")."TAGS", ("VISITS"[1])."SCORES",
           (map_type.extract("SPOTS", 'spot_' || "ID"))."LAT",
           (map_type.extract("SPOTS", 'spot_' || "ID"))."LON"
    FROM {NESTED_SCHEMA}.source ORDER BY "ID"
"""


def nested_lowered_rows(table_name, where=""):
    return f"""
        SELECT id, (home).city, ((home).geo).lat, ((home).geo).lon,
               (home).tags, (((visits[1]).place).geo).lat,
               ((visits[1]).place).tags, (visits[1]).scores,
               (map_type.extract(spots, 'spot_' || id)).lat,
               (map_type.extract(spots, 'spot_' || id)).lon
        FROM {table_name} {where} ORDER BY id
    """


def test_nested_types(pg_conn, nested_source):
    path = nested_source["source"]

    run_command(
        f"""
        CREATE FOREIGN TABLE {NESTED_SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{path}', lowercase_column_names 'true');
        CREATE FOREIGN TABLE {NESTED_SCHEMA}.unchanged () SERVER pg_lake
        OPTIONS (path '{path}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert column_names(pg_conn, f"{NESTED_SCHEMA}.lowered") == [
        "id",
        "home",
        "visits",
        "spots",
    ]

    # structs in structs, structs in lists, and map values are all folded;
    # key and val are the map pair's own fields
    assert nested_field_names(pg_conn, f"{NESTED_SCHEMA}.lowered") == [
        "city",
        "geo",
        "key",
        "lat",
        "lon",
        "place",
        "scores",
        "tags",
        "val",
    ]
    assert nested_field_names(pg_conn, f"{NESTED_SCHEMA}.unchanged") == [
        "CITY",
        "GEO",
        "LAT",
        "LON",
        "PLACE",
        "SCORES",
        "TAGS",
        "key",
        "val",
    ]

    # lat and lon carry different values, so a swap would show up here
    expected = run_query(NESTED_SOURCE_ROWS, pg_conn)
    assert len(expected) == 5
    assert (
        run_query(nested_lowered_rows(f"{NESTED_SCHEMA}.lowered"), pg_conn) == expected
    )

    # filters on deeply nested fields are pushed down by their lowercase names
    filtered = nested_lowered_rows(
        f"{NESTED_SCHEMA}.lowered",
        "WHERE (((visits[1]).place).geo).lat > 4"
        " AND (map_type.extract(spots, 'spot_' || id)).lon < -20",
    )
    assert_query_pushdownable(filtered, pg_conn)
    assert run_query(filtered, pg_conn) == [row for row in expected if row[0] >= 3]


def test_nested_types_writes(pg_conn, nested_source):
    path = nested_source["source"]

    run_command(
        f"""
        CREATE FOREIGN TABLE {NESTED_SCHEMA}.lowered () SERVER pg_lake
        OPTIONS (path '{path}', lowercase_column_names 'true');
        CREATE TABLE {NESTED_SCHEMA}.inserted (LIKE {NESTED_SCHEMA}.lowered) USING iceberg;
        CREATE TABLE {NESTED_SCHEMA}.copied (LIKE {NESTED_SCHEMA}.lowered) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()
    expected = run_query(NESTED_SOURCE_ROWS, pg_conn)

    insert_select = (
        f"INSERT INTO {NESTED_SCHEMA}.inserted SELECT * FROM {NESTED_SCHEMA}.lowered"
    )
    assert_query_pushdownable(insert_select, pg_conn)
    run_command(insert_select, pg_conn)
    pg_conn.commit()
    assert (
        run_query(nested_lowered_rows(f"{NESTED_SCHEMA}.inserted"), pg_conn) == expected
    )

    run_command(
        f"""
        COPY {NESTED_SCHEMA}.copied FROM '{path}'
        WITH (format 'iceberg', lowercase_column_names true)
        """,
        pg_conn,
    )
    assert run_query("SELECT pg_lake_last_copy_pushed_down_test()", pg_conn) == [[True]]
    pg_conn.commit()
    assert (
        run_query(nested_lowered_rows(f"{NESTED_SCHEMA}.copied"), pg_conn) == expected
    )

    # the exported file has lowercase names at every level
    url = f"s3://{TEST_BUCKET}/{NESTED_SCHEMA}/copy_to.parquet"
    run_command(f"COPY (SELECT * FROM {NESTED_SCHEMA}.lowered) TO '{url}'", pg_conn)
    run_command(
        f"CREATE FOREIGN TABLE {NESTED_SCHEMA}.exported () SERVER pg_lake OPTIONS (path '{url}')",
        pg_conn,
    )
    pg_conn.commit()
    assert all(
        name == name.lower()
        for name in nested_field_names(pg_conn, f"{NESTED_SCHEMA}.exported")
    )


def test_nested_collision(pg_conn, nested_source):
    error = run_command(
        f"""
        CREATE FOREIGN TABLE {NESTED_SCHEMA}.colliding_lowered () SERVER pg_lake
        OPTIONS (path '{nested_source["colliding"]}', lowercase_column_names 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert 'Iceberg field "x" collides with another field' in str(error)
    pg_conn.rollback()

import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_struct_map_child_lookup(pg_conn, duckdb_conn, extension):
    # reproduce crash in old repo issues/385

    mapurl = f"s3://{TEST_BUCKET}/test_struct_map_child/data.parquet"

    # create out data basic scalar type
    run_command(
        f"""
    COPY (SELECT {{ 'int': 1, 'map': MAP {{ 'a': 1, 'b': 2 }} }} mystruct) to '{mapurl}'
    """,
        duckdb_conn,
    )

    err = run_command(
        f"""
    CREATE FOREIGN TABLE mymap () SERVER pg_lake OPTIONS (path '{mapurl}');
    """,
        pg_conn,
        raise_error=False,
    )

    assert err == None

    # lookup map for existing value
    res = run_query(
        f"""
    SELECT (mystruct).map->'a' from mymap
    """,
        pg_conn,
    )

    assert res == [[1]]

    # lookup map for non-existing value
    res = run_query(
        f"""
    SELECT (mystruct).map->'c' from mymap
    """,
        pg_conn,
    )

    assert res == [[None]]

    pg_conn.rollback()


def test_struct_map_child_lookup_null(pg_conn, duckdb_conn, extension):
    # reproduce crash in old repo issues/385

    mapurl = f"s3://{TEST_BUCKET}/test_struct_map_child_null/data.parquet"

    # include a row with a NULL map value; auto-detect requires we have a valid entry though
    run_command(
        f"""
    COPY (
        SELECT {{ 'int': 1, 'map': MAP {{ 'a': 1, 'b': 2 }} }} mystruct
        UNION ALL
        SELECT {{ 'int': 1, 'map': NULL }} ) to '{mapurl}'
    """,
        duckdb_conn,
    )

    err = run_command(
        f"""
    CREATE FOREIGN TABLE mymap () SERVER pg_lake OPTIONS (path '{mapurl}');
    """,
        pg_conn,
        raise_error=False,
    )

    assert err == None

    # lookup map for existing value
    res = run_query(
        f"""
    SELECT (mystruct).map->'a' from mymap order by 1 nulls first
    """,
        pg_conn,
    )

    assert res == [[None], [1]]

    # lookup map for non-existing value
    res = run_query(
        f"""
    SELECT (mystruct).map->'c' from mymap
    """,
        pg_conn,
    )

    assert res == [[None], [None]]

    pg_conn.rollback()


def test_struct_from_user_composite_type(pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_struct_from_user_composite_type/"

    run_command(
        f"""
    CREATE TYPE custom_composite AS (a int, b float);
    CREATE FOREIGN TABLE test_struct_from_user_composite_type(comp_col custom_composite)
        SERVER pg_lake OPTIONS (location '{url}', writable 'true', format 'parquet');
    """,
        pg_conn,
    )

    run_command(
        """
    INSERT INTO test_struct_from_user_composite_type VALUES ('(1,2.5)'::custom_composite);
    """,
        pg_conn,
    )

    result = run_query(
        """
    SELECT * FROM test_struct_from_user_composite_type WHERE comp_col = '(1,2.5)'::custom_composite;
    """,
        pg_conn,
    )

    # check basic fieldselect in target
    assert_remote_query_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE (comp_col).b = '2.5'
    """,
        "SELECT (comp_col).a",
        pg_conn,
    )

    # check basic fieldselect in where clause
    assert_remote_query_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE (comp_col).b = '2.5'
    """,
        "WHERE ((comp_col).b",
        pg_conn,
    )

    # check casting composite is not pushed down
    assert_remote_query_not_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE comp_col = (1,2.5)::custom_composite
    """,
        "custom_composite",
        pg_conn,
    )

    # check casting literal is not pushed down
    assert_remote_query_not_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE comp_col = '(1,2.5)'::custom_composite
    """,
        "custom_composite",
        pg_conn,
    )

    # check coerceioval is not pushed down
    assert_remote_query_not_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE comp_col = ('(1,2.5)'::text)::custom_composite
    """,
        "custom_composite",
        pg_conn,
    )

    # check rowexpr is not pushed down
    assert_remote_query_not_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE comp_col = row(1::int,2.5::float)
    """,
        "ROW",
        pg_conn,
    )

    # check rowexpr with cast is not pushed down
    assert_remote_query_not_contains_expression(
        """
    SELECT (comp_col).a FROM test_struct_from_user_composite_type WHERE comp_col = row(1::int,2.5::float)::custom_composite
    """,
        "custom_composite",
        pg_conn,
    )

    pg_conn.rollback()


# This test verifies #788 behavior and fix for a MAP type as part of a composite type
def test_map_inside_composite_type(pg_conn, extension, s3):
    url = f"s3://{TEST_BUCKET}/test_map_inside_composite_type/data.parquet"

    map_type_name = create_map_type("int", "text")

    run_command(
        f"""
    CREATE TYPE user_composite AS (a int, b float, map {map_type_name});
    COPY (SELECT ROW(1,2.5,'{{"(1,2)","(2,3)"}}'::{map_type_name})::user_composite) TO '{url}';
    CREATE FOREIGN TABLE test_map_inside_composite_type(comp_col user_composite)
        SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    res = run_query("SELECT COUNT(*) FROM test_map_inside_composite_type", pg_conn)
    assert res[0][0] == 1

    pg_conn.rollback()


def test_array_escaping(pg_conn, extension, s3, with_default_location):
    run_command(
        """
        CREATE TABLE arr (id int, arr text[]) USING iceberg;
        INSERT INTO arr VALUES (1, '{}'), (2, ARRAY['','[a],[b]','{''}','a"b']);
        """,
        pg_conn,
    )

    res = run_query("SELECT arr FROM arr ORDER BY id", pg_conn)
    assert res[0][0] == []
    assert res[1][0] == ["", "[a],[b]", "{'}", 'a"b']

    pg_conn.rollback()


def test_struct_escaping(pg_conn, extension, s3, with_default_location):
    run_command(
        """
        CREATE TYPE pair AS (x text, y text[]);
        CREATE TYPE nest AS (key text, value pair[]);
        CREATE TABLE struct (id int, val nest[]) USING iceberg;
        INSERT INTO struct VALUES (1, ARRAY[(',{)', ARRAY[('v,l', ARRAY['[abc:def] [aa:zz][', ' ', ''''])::pair])::nest]);
        """,
        pg_conn,
    )

    res = run_query(
        "SELECT val[1].key, val[1].value[1].x, val[1].value[1].y FROM struct WHERE id = 1",
        pg_conn,
    )
    assert res[0][0] == ",{)"
    assert res[0][1] == "v,l"
    assert res[0][2] == ["[abc:def] [aa:zz][", " ", "'"]

    pg_conn.rollback()


def test_map_escaping(pg_conn, superuser_conn, extension, s3, with_default_location):
    run_command(
        """
        SELECT map_type.create('text', 'text[]')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        """
        CREATE TABLE maps (id int, val map_type.key_text_val_text_array) USING iceberg;
        INSERT INTO maps VALUES (1, ARRAY[('{,"}', ARRAY['[','''',']'])]::map_type.key_text_val_text_array);
        """,
        pg_conn,
    )

    res = run_query("SELECT val->'{,\"}' FROM maps WHERE id = 1", pg_conn)
    assert res[0][0] == ["[", "'", "]"]

    pg_conn.rollback()

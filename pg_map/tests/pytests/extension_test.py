import pytest
import psycopg2
import time
from utils_pytest import *


def test_create_extension(pg_conn, pg_map):
    result = run_query(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_map'", pg_conn
    )
    assert len(result) == 1

    pg_conn.rollback()


def test_create_type_idempotency(pg_conn, pg_map):
    result = run_query("SELECT map_type.create('text','integer')", pg_conn)
    assert len(result) == 1
    assert result[0]["create"] == "map_type.key_text_val_int"

    # verify that creating the same type a second time does not error
    result = run_query("SELECT map_type.create('text','integer')", pg_conn)
    assert len(result) == 1
    assert result[0]["create"] == "map_type.key_text_val_int"

    # verify that creating a type with clashing types throws an error
    result = run_query(
        "SELECT map_type.create('integer','text', 'key_text_val_int')",
        pg_conn,
        raise_error=False,
    )
    assert "has mismatched key types" in result

    pg_conn.rollback()


def test_simple_values(pg_conn, pg_map):
    result = run_query("SELECT map_type.create('text','integer')", pg_conn)
    assert len(result) == 1
    assert result[0]["create"] == "map_type.key_text_val_int"

    # Type exists in pg_type
    result = run_query(
        """
        SELECT typname, typinput, typoutput, typtypmod
            FROM pg_type
            WHERE typname = 'key_text_val_int'
        """,
        pg_conn,
    )

    assert len(result) == 1
    assert result[0]["typname"] == "key_text_val_int"
    assert result[0]["typinput"] == "domain_in"
    assert result[0]["typoutput"] == "array_out"
    assert result[0]["typtypmod"] == -1

    # Support functions exist
    result = run_query(
        """
            SELECT proname, proisstrict
            FROM pg_type t
            JOIN pg_depend d ON t.oid = d.refobjid AND d.deptype = 'n'
            JOIN pg_proc p ON d.objid = p.oid
            WHERE t.typname = 'key_text_val_int'
            ORDER BY 1
        """,
        pg_conn,
    )
    assert len(result) == 3
    assert result[0]["proname"] == "cardinality"
    assert result[1]["proname"] == "entries"
    assert result[2]["proname"] == "extract"
    assert result[0]["proisstrict"] == True
    assert result[1]["proisstrict"] == True
    assert result[2]["proisstrict"] == True

    # Extract function
    result = run_query(
        """
        SELECT map_type.extract(
            '{"(me,1)","(myself,2)","(i,3)"}'::map_type.key_text_val_int,
            'i');
        """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["extract"] == 3

    # Cardinality function
    result = run_query(
        """
        SELECT map_type.cardinality('{"(me,1)","(myself,2)","(i,3)"}'::map_type.key_text_val_int)
        """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["cardinality"] == 3

    # Keys function
    result = run_query(
        """
        SELECT key, value FROM map_type.entries('{"(me,1)","(myself,2)","(i,3)"}'::map_type.key_text_val_int)
        """,
        pg_conn,
    )
    assert len(result) == 3
    assert result[0]["key"] == "me"
    assert result[0]["value"] == 1
    assert result[1]["key"] == "myself"
    assert result[1]["value"] == 2
    assert result[2]["key"] == "i"
    assert result[2]["value"] == 3

    run_command("drop type map_type.key_text_val_int cascade", pg_conn)

    # Make sure pair type is also gone
    result = run_query(
        """
        SELECT count(*)
            FROM pg_type
            WHERE typname = 'pair_text_int'
        """,
        pg_conn,
    )
    result[0]["count"] == 0

    pg_conn.rollback()


def test_array_keys_and_vals(pg_conn, pg_map):

    # Test creating two types with overlapping array type
    result = run_query("SELECT map_type.create('text','integer')", pg_conn)
    assert len(result) == 1
    assert result[0]["create"] == "map_type.key_text_val_int"

    result = run_query(
        "SELECT map_type.create('text[]','integer')", pg_conn, raise_error=False
    )
    assert "arrays cannot be used as the key type" in result
    pg_conn.rollback()

    result = run_query("SELECT map_type.create('text','integer[]')", pg_conn)
    assert len(result) == 1
    assert result[0]["create"] == "map_type.key_text_val_int_array"

    # now that we have the text => integer array types we can see about lookups
    result = run_query(
        """
    SELECT (array[
        ('foo',array[1,2,3,4]),
        ('bar',array[2,4])
    ])::map_type.key_text_val_int_array->'bar'
    """,
        pg_conn,
    )
    assert result == [[[2, 4]]]

    pg_conn.rollback()


def test_nested_values(pg_conn, pg_map):
    # Test creating nested map type
    result = run_query(
        """
        select map_type.create('integer', 'text')
        """,
        pg_conn,
    )
    int_to_text_type = result[0][0]

    result = run_query(
        f"""
        select map_type.create('integer', '{int_to_text_type}')
        """,
        pg_conn,
    )
    int_to_map_type = result[0][0]

    result = run_query(
        f"""
        WITH a AS (
             select array[(9, array[(s,'hello'),(s+1,'world')]::{int_to_text_type})]::{int_to_map_type} as m from generate_series(1,10) s
        )
        SELECT map_type.extract(m, 9) FROM a
        """,
        pg_conn,
    )
    assert len(result) == 10
    assert result[0]["extract"] == '{"(1,hello)","(2,world)"}'

    pg_conn.rollback()


def test_long_type_names(pg_conn):
    result = run_query(
        """
        select map_type.create('timestamptz', 'timestamptz[]') as type_name
    """,
        pg_conn,
    )
    assert result[0]["type_name"] == "map_type.key_timestamptz_val_timestamptz_array"

    result = run_query(
        """
        select map_type.create('double precision', 'pg_available_extension_versions') as type_name
    """,
        pg_conn,
    )

    # we want the oid from the original test
    oid = run_query("SELECT 'pg_available_extension_versions'::regtype::oid", pg_conn)[
        0
    ][0]
    assert result[0]["type_name"] == f"map_type.key_float8_val_{oid}"

    pg_conn.rollback()


def test_null_map(pg_conn, pg_map):
    # validated expected handling for NULL map values
    result = run_query("SELECT map_type.create('text','integer')", pg_conn)
    assert len(result) == 1
    assert result[0]["create"] == "map_type.key_text_val_int"

    # test extract from NULL map
    result = run_query(
        "SELECT map_type.extract(NULL::map_type.key_text_val_int, 'test')",
        pg_conn,
    )
    assert result == [[None]]

    # test extract operator from NULL map
    result = run_query("SELECT (NULL::map_type.key_text_val_int)->'test'", pg_conn)
    assert result == [[None]]

    # test cardinality from NULL map
    result = run_query(
        "SELECT map_type.cardinality(NULL::map_type.key_text_val_int)", pg_conn
    )
    assert result == [[None]]

    # test entries from NULL map
    result = run_query(
        "SELECT * FROM map_type.entries(NULL::map_type.key_text_val_int)", pg_conn
    )
    assert result == []

    pg_conn.rollback()


def test_null_entry_and_key(pg_conn, pg_map):
    # a NULL entry or a NULL key in the array is skipped, not dereferenced
    run_query("SELECT map_type.create('text','integer')", pg_conn)

    result = run_query(
        """SELECT map_type.extract('{NULL,"(i,3)"}'::map_type.key_text_val_int, 'i')""",
        pg_conn,
    )
    assert result == [[3]]

    result = run_query(
        """SELECT map_type.extract('{"(,1)","(i,3)"}'::map_type.key_text_val_int, 'i')""",
        pg_conn,
    )
    assert result == [[3]]

    result = run_query(
        """SELECT map_type.extract('{NULL,"(,1)"}'::map_type.key_text_val_int, 'i')""",
        pg_conn,
    )
    assert result == [[None]]

    result = run_query(
        """SELECT * FROM map_type.entries('{NULL,"(i,3)"}'::map_type.key_text_val_int)""",
        pg_conn,
    )
    assert result == [["i", 3]]

    pg_conn.rollback()


@pytest.fixture(scope="module")
def pg_map(superuser_conn, pg_conn, app_user):
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_map CASCADE;
        GRANT USAGE ON SCHEMA map_type TO {app_user};
        GRANT CREATE ON SCHEMA map_type TO {app_user};
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA map_type TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_map CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

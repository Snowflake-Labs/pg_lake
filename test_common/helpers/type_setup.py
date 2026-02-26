"""Type validation, test-data setup helpers, and sample-data paths."""

from pathlib import Path

from .cloud_storage import TEST_BUCKET
from .db import (
    open_pg_conn,
    run_command,
    run_query,
)


# ---------------------------------------------------------------------------
# Sample-data paths
# ---------------------------------------------------------------------------

def sampledata_filepath(datafile):
    return str(Path(__file__).parent.parent / "sample" / "data" / datafile)


def sample_avro_filepath(avrofile):
    return str(Path(__file__).parent.parent / "sample" / "avro" / avrofile)


def validate_shape(typeid, shape, conn=None):
    """Recursively validate that a postgres type matches an expected format.

    A "shape" in this context is a data structure which matches the overall
    structure of the postgres type tree given a starting typeid.

    The initial typeid is the typeid of the underlying relation (since each
    table has an underlying composite datatype for that type).

    The interpretation of the datastruct is as follows:

    The top-level is an array, where the ordered entries is the expected
    order of the columns.

    If an entry is just a scalar, this is interpreted as a non-composite
    field name, so anything that is not typtype = 'c'.

    If the entry is a dict, then we have additional attributes associated
    with this type.  Available attributes are:

    - name: the name of the attribute itself

    - type: if provided, the type of the attribute must match this type

    - typelike: if provided, the type name must contain this substring

    - cols: if provided, this is a composite type.  The value should also be
        an array, and the elements here are subject to the same validation
        as the base-level composite type.

    - isarray: if provided, verify that the given type is or is not an array
        type (the used type name for `type` or `typelike`) is still the element
        type name in this case, not the `_text` or whatever type of just the
        array)

    """
    print(f"validate_shape: typeid: {typeid}; shape: {shape} ")
    # verify that we are starting with a list for our shape, anything else is an error
    assert isinstance(shape, list), "shape is list"
    assert typeid != 0, "empty typeid"
    assert len(shape) > 0, "zero-column table"

    # Do some validation on our passed-in typeid; the order of these fields
    # should match our constants up top.
    result = run_query(
        f"""
    SELECT
        attname, atttypid, typtype, typname, typarray = 0 as isarray,
        case when typarray = 0 then typelem::regtype::name else typname end as basename,
        case when typarray = 0 then typelem else atttypid end as childtype
    FROM pg_attribute, pg_type
    WHERE
        pg_type.oid = atttypid AND
        attnum > 0 AND
        NOT attisdropped AND
        attrelid = (SELECT typrelid FROM pg_type WHERE oid = {typeid})
    ORDER BY attnum
    """,
        conn,
    )

    # Since this was a composite type, first verify the number of columns
    # matches our shape.  If we did not find the underlying composite type,
    # this would not pass, so we'd get 0 rows from this query.

    print(result)
    assert len(result) == len(shape), "different number of columns"

    # now check out our shape attributes against what we discovered
    for shatt, pgatt in zip(shape, result):
        print(f"checking shape: {shatt} against found pginfo: {pgatt}")
        if isinstance(shatt, list):
            assert "unsupported type for shape" == False, "column cannot be a list"
        elif isinstance(shatt, str):
            # column name check
            assert pgatt["attname"] == shatt, "scalar column non-matching name"
            assert pgatt["typtype"] != "c", "scalar column expected non-composite type"
        elif isinstance(shatt, dict):
            # name should match
            assert pgatt["attname"] == shatt.pop("name"), "att:name"
            # type exact match
            if "type" in shatt:
                assert pgatt["basename"] == shatt.pop("type"), "att:type"
            # type substring match
            if "typelike" in shatt:
                assert shatt.pop("typelike") in pgatt["basename"], "att:typelike"
            if "isarray" in shatt:
                assert pgatt["isarray"] == shatt.pop("isarray"), "att:isarray"
            # explicit composite type check
            if "cols" in shatt:
                # checking other validation
                cols = shatt.pop("cols")
                assert isinstance(cols, list), "cols is list"
                # recursive validation for the given shape
                validate_shape(pgatt["childtype"], cols, conn=conn)
            # check for unknown shape params; sanity-check
            assert len(shatt) == 0, "no extra args"
        else:
            assert "unknown shape object type" == False


def setup_testdef(desc, conn=None, duckdb_conn=None):
    """Common setup for loading data and returning the base typeid for the
    created relation"""

    pg_conn = conn

    table = f"test_create_table_definitions_{desc['name']}"
    select = desc["select"]
    shape = desc["shape"]

    url = f"s3://{TEST_BUCKET}/{table}/data.parquet"

    run_command(
        f"""
    COPY (SELECT {select}) TO '{url}' WITH (format 'parquet');
    """,
        duckdb_conn,
    )

    run_command(
        f"""
    CREATE TABLE {table} () WITH (load_from='{url}');
    """,
        pg_conn,
    )

    res = run_query(
        f"""
    SELECT reltype FROM pg_class WHERE relname = '{table}'
    """,
        pg_conn,
    )

    return res[0][0]


# This is a wrapper to allow the creation of a specific map type by calling the
# `map_type.create()` function as a superuser using the passed-in parameters.
def create_map_type(keytype, valtype, raise_error=True):
    superuser_conn = open_pg_conn()
    if raise_error:
        res = run_query(
            f"SELECT map_type.create('{keytype}','{valtype}')",
            superuser_conn,
            raise_error=True,
        )
        superuser_conn.commit()
        return res[0][0]

    err = run_command(
        f"SELECT map_type.create('{keytype}','{valtype}')",
        superuser_conn,
        raise_error=False,
    )
    if err:
        superuser_conn.rollback()
        return err

    superuser_conn.commit()
    return None


def create_test_types(pg_conn, app_user):
    run_command("""create extension if not exists pg_map""", pg_conn)
    pg_conn.commit()
    create_map_type("int", "text")

    run_command(
        """create schema if not exists lake_struct;
    create type lake_struct.custom_type as (x int, y int);
    """,
        pg_conn,
    )

    create_table_command = """
    create table test_types (
    c_array int[],
    c_bit bit,
    c_bool bool,
    c_bpchar bpchar,
    c_bytea bytea,
    c_char char,
    c_cidr cidr,
    c_custom lake_struct.custom_type,
    c_date date,
    c_float4 float4,
    c_float8 float8,
    c_inet inet,
    c_int2 int2,
    c_int4 int4,
    c_int8 int8,
    c_interval interval,
    c_json json,
    c_jsonb jsonb,
    c_map map_type.key_int_val_text,
    c_money money,
    c_name name,
    c_numeric numeric,
    c_numeric_large numeric(39,2),
    c_numeric_mod numeric(4,2),
    c_oid oid,
    c_text text,
    c_tid tid,
    c_time time,
    c_timestamp timestamp,
    c_timestamptz timestamptz,
    c_timetz timetz,
    c_uuid uuid,
    c_varbit varbit,
    c_varchar varchar
    );
    """
    run_command(create_table_command, pg_conn)

    insert_command = f"""
    insert into test_types values (
    /* c_array */ ARRAY[1,2],
    /* c_bit */ 1::bit,
    /* c_bool */ true,
    /* c_bpchar */ 'hello',
    /* c_bytea */ '\\x0001',
    /* c_char */ 'a',
    /* c_cidr */ '192.168.0.0/16'::cidr,
    /* c_custom */ (2,4),
    /* c_date */ '2024-01-01',
    /* c_float4 */ 3.4,
    /* c_float8 */ 33333333.33444444,
    /* c_inet */ '192.168.1.1'::inet,
    /* c_int2 */ 14,
    /* c_int4 */ 100000,
    /* c_int8 */ 10000000000,
    /* c_interval */ '3 days',
    /* c_json */ '{{"hello":"world" }}',
    /* c_jsonb */ '{{"hello":"world" }}',
    /* c_map */ '{{"(1,a)","(2,b)","(3,c)"}}',
    /* c_money */ '$4.5',
    /* c_name */ 'test',
    /* c_numeric */ 199.123,
    /* c_numeric_large */ 123456789012345678901234.99,
    /* c_numeric_mod */ 99.99,
    /* c_oid */ 11,
    /* c_text */ 'fork',
    /* c_tid */ '(3,4)',
    /* c_time */ '19:34',
    /* c_timestamp */ '2024-01-01 15:00:00',
    /* c_timestamptz */ '2024-01-01 15:00:00 UTC',
    /* c_timetz */ '19:34 UTC',
    /* c_uuid */ 'acd661ca-d18c-42e2-9c4e-61794318935e',
    /* c_varbit */ '0110',
    /* c_varchar */ 'abc'
    );
    grant select on test_types to {app_user};
    """
    run_command(insert_command, pg_conn)

    pg_conn.commit()

import pytest
import psycopg2
import time
import duckdb
import json
import math
from utils_pytest import *


# This module is pg_lake's own JSON read/write suite. Under the production
# 'auto' default a plain local COPY TO json is served by Postgres (it gets
# precedence whenever it natively supports the case), which would silently move
# most of these assertions onto Postgres's writer. The internal tests here pin
# pg_lake-specific behaviour (DuckDB type inference, pg_lake error strings,
# option handling), so we default the whole module to 'pglake'. The committed
# SET is the per-connection baseline that each test's own rollback reverts to.
#
# The round-trip tests below override this per case via the `writer` parameter:
# they write the file with each writer in turn and always read it back through
# pg_lake (Postgres has no COPY FROM json), proving both producers emit JSON
# pg_lake can reconstruct losslessly.
JSON_WRITERS = ["postgres", "pglake"]


@pytest.fixture(autouse=True, scope="module")
def _default_pglake_json(pg_conn, superuser_conn, app_user):
    for conn in (pg_conn, superuser_conn):
        run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", conn)
        conn.commit()
    # The writer-parametrized round-trips below also run with
    # json_copy_mode=postgres, which routes COPY <table> TO <local file> through
    # PostgreSQL's own COPY -- that requires the pg_write_server_files role.
    # Grant it to the (otherwise unprivileged) app_user so the same round-trip
    # works for both producers. The role is module-scoped and dropped on
    # teardown, so the grant never leaks.
    run_command(f"GRANT pg_write_server_files TO {app_user}", superuser_conn)
    superuser_conn.commit()
    yield


def _set_json_writer(conn, writer):
    """Select who writes the local JSON file for a round-trip test. Reverts to
    the committed module default ('pglake') on the test's rollback."""
    run_command(f"SET pg_lake_copy.json_copy_mode TO '{writer}'", conn)


def _read_back_via_pglake(conn):
    """The reader is always pg_lake; Postgres cannot COPY FROM json."""
    run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", conn)


def _skip_pre_pg19(conn):
    if get_pg_version_num(conn) < 190000:
        pytest.skip("COPY ... (format json, force_array) requires PG19+")


# Multi-column, multi-row, ordered source covering a representative type spread
# (int, text with quotes + unicode, numeric, float, bool, NULL, date,
# timestamp) so a force_array round-trip exercises the whole JSON path.
_FA_SRC_DDL = """
CREATE TABLE fa_src (
    id int,
    name text,
    score numeric(8,2),
    ratio float8,
    flag bool,
    note text,
    d date,
    ts timestamp
);
INSERT INTO fa_src VALUES
    (1, 'alice "the ace"', 1.50, 0.25, true, E'\\u0100\\U0001F642', '2024-01-01', '2024-01-01 15:00:00'),
    (2, 'bob', 2.00, 1.5, false, NULL, '2023-06-15', '2023-06-15 08:30:00'),
    (3, 'çelik', 3.25, NULL, NULL, 'plain', '2022-12-31', '2022-12-31 23:59:59');
"""

_FA_ROW_COUNT = 3


def _fa_setup(conn):
    run_command(_FA_SRC_DDL, conn)
    run_command("CREATE TABLE fa_dst (LIKE fa_src)", conn)


def _fa_rows(conn, table):
    return [dict(r) for r in run_query(f"SELECT * FROM {table} ORDER BY id", conn)]


@pytest.mark.parametrize("writer", JSON_WRITERS)
def test_force_array_roundtrip_local_file(pg_conn, tmp_path, writer):
    """force_array writes a single JSON array to a local file and reads back.

    Parametrized over the producer: both Postgres and pg_lake must emit a JSON
    array that pg_lake's COPY FROM reconstructs row-for-row. (app_user is
    granted pg_write_server_files in the module fixture so the postgres writer's
    COPY TO <file> works.)
    """
    _skip_pre_pg19(pg_conn)

    json_path = tmp_path / "fa_local.json"

    _fa_setup(pg_conn)
    before = _fa_rows(pg_conn, "fa_src")

    _set_json_writer(pg_conn, writer)
    run_command(
        f"COPY fa_src TO '{json_path}' WITH (format json, force_array true)", pg_conn
    )

    content = json_path.read_bytes().strip()
    assert content.startswith(b"["), f"expected JSON array, got: {content[:40]!r}"
    assert content.endswith(b"]"), f"expected JSON array, got: {content[-40:]!r}"
    parsed = json.loads(content)
    assert isinstance(parsed, list)
    assert len(parsed) == _FA_ROW_COUNT

    _read_back_via_pglake(pg_conn)
    run_command(f"COPY fa_dst FROM '{json_path}' WITH (format json)", pg_conn)
    after = _fa_rows(pg_conn, "fa_dst")
    assert before == after

    pg_conn.rollback()


def test_force_array_roundtrip_s3(pg_conn, s3, tmp_path):
    """force_array writes a single JSON array to an s3 lake target and reads back."""
    _skip_pre_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_force_array_roundtrip/fa_s3.json"

    _fa_setup(pg_conn)
    before = _fa_rows(pg_conn, "fa_src")

    run_command(f"COPY fa_src TO '{url}' WITH (format json, force_array true)", pg_conn)

    content = read_s3_operations(s3, url, is_text=False).strip()
    assert content.startswith(b"["), f"expected JSON array, got: {content[:40]!r}"
    assert content.endswith(b"]"), f"expected JSON array, got: {content[-40:]!r}"
    parsed = json.loads(content)
    assert isinstance(parsed, list)
    assert len(parsed) == _FA_ROW_COUNT

    run_command(f"COPY fa_dst FROM '{url}' WITH (format json)", pg_conn)
    after = _fa_rows(pg_conn, "fa_dst")
    assert before == after

    pg_conn.rollback()


@pytest.mark.parametrize("writer", JSON_WRITERS)
def test_no_force_array_ndjson_roundtrip(pg_conn, tmp_path, writer):
    """Without force_array the same source stays NDJSON and still round-trips.

    Parametrized over the producer: both Postgres and pg_lake emit NDJSON (one
    JSON object per line) that pg_lake reads back row-for-row.
    """
    _skip_pre_pg19(pg_conn)

    json_path = tmp_path / "fa_ndjson.json"

    _fa_setup(pg_conn)
    before = _fa_rows(pg_conn, "fa_src")

    _set_json_writer(pg_conn, writer)
    run_command(f"COPY fa_src TO '{json_path}' WITH (format json)", pg_conn)

    content = json_path.read_bytes()
    assert not content.lstrip().startswith(b"["), "expected NDJSON, got a JSON array"
    lines = [ln for ln in content.splitlines() if ln.strip()]
    assert len(lines) == _FA_ROW_COUNT
    for ln in lines:
        assert isinstance(json.loads(ln), dict)

    _read_back_via_pglake(pg_conn)
    run_command(f"COPY fa_dst FROM '{json_path}' WITH (format json)", pg_conn)
    after = _fa_rows(pg_conn, "fa_dst")
    assert before == after

    pg_conn.rollback()


# ---------------------------------------------------------------------------
# PG19: COPY <partitioned_table> TO with JSON (plain + force_array).
# ---------------------------------------------------------------------------

_PART_SRC_DDL = """
DROP TABLE IF EXISTS pj_parent CASCADE;
DROP TABLE IF EXISTS pj_dst;
CREATE TABLE pj_parent (id int, name text, d date) PARTITION BY RANGE (id);
CREATE TABLE pj_parent_1 PARTITION OF pj_parent FOR VALUES FROM (0) TO (50);
CREATE TABLE pj_parent_2 PARTITION OF pj_parent FOR VALUES FROM (50) TO (100);
INSERT INTO pj_parent
    SELECT g, 'name "' || g || '"', date '2024-01-01' + g
    FROM generate_series(0, 99) g;
"""

_PART_ROW_COUNT = 100


def _pj_setup(conn):
    run_command(_PART_SRC_DDL, conn)
    run_command("CREATE TABLE pj_dst (LIKE pj_parent)", conn)


def _pj_rows(conn, table):
    return [dict(r) for r in run_query(f"SELECT * FROM {table} ORDER BY id", conn)]


@pytest.mark.parametrize("writer", JSON_WRITERS)
def test_pg19_partitioned_json_roundtrip_local(pg_conn, tmp_path, writer):
    """PG19: COPY <partitioned parent> TO (format json) to a local file then
    COPY dst FROM round-trips every partition's rows.

    Parametrized over the producer: both Postgres (native COPY TO on the
    partitioned parent) and pg_lake must emit NDJSON pg_lake reads back.
    """
    _skip_pre_pg19(pg_conn)

    json_path = tmp_path / "pj_local.json"
    _pj_setup(pg_conn)
    before = _pj_rows(pg_conn, "pj_parent")

    _set_json_writer(pg_conn, writer)
    run_command(f"COPY pj_parent TO '{json_path}' WITH (format json)", pg_conn)

    content = json_path.read_bytes()
    assert not content.lstrip().startswith(b"["), "expected NDJSON, got a JSON array"
    lines = [ln for ln in content.splitlines() if ln.strip()]
    assert len(lines) == _PART_ROW_COUNT

    _read_back_via_pglake(pg_conn)
    run_command(f"COPY pj_dst FROM '{json_path}' WITH (format json)", pg_conn)
    after = _pj_rows(pg_conn, "pj_dst")
    assert before == after
    assert len(after) == _PART_ROW_COUNT

    pg_conn.rollback()


def test_pg19_partitioned_json_roundtrip_s3(pg_conn, s3, tmp_path):
    """PG19: same as the local JSON round-trip but to an s3 lake target."""
    _skip_pre_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_pg19_partitioned_json/pj_s3.json"
    _pj_setup(pg_conn)
    before = _pj_rows(pg_conn, "pj_parent")

    run_command(f"COPY pj_parent TO '{url}' WITH (format json)", pg_conn)

    content = read_s3_operations(s3, url, is_text=False)
    lines = [ln for ln in content.splitlines() if ln.strip()]
    assert len(lines) == _PART_ROW_COUNT

    run_command(f"COPY pj_dst FROM '{url}' WITH (format json)", pg_conn)
    after = _pj_rows(pg_conn, "pj_dst")
    assert before == after
    assert len(after) == _PART_ROW_COUNT

    pg_conn.rollback()


def _assert_single_json_array(content_bytes, expected_count):
    """force_array output is a single JSON array. The exact framing differs by
    producer (DuckDB uses tab indentation and `},\n\t{` separators, Postgres
    uses spaces), so parse and count rather than byte-compare."""
    stripped = content_bytes.strip()
    assert stripped.startswith(b"["), f"expected JSON array, got: {stripped[:40]!r}"
    assert stripped.endswith(b"]"), f"expected JSON array, got: {stripped[-40:]!r}"
    parsed = json.loads(stripped)
    assert isinstance(parsed, list)
    assert len(parsed) == expected_count
    return parsed


@pytest.mark.parametrize("writer", JSON_WRITERS)
def test_pg19_partitioned_json_force_array_local(pg_conn, tmp_path, writer):
    """PG19 nice story: COPY <partitioned parent> TO (format json, force_array
    true) to a local file emits one JSON array spanning all partitions, then
    COPY dst FROM reads it row-for-row.

    Parametrized over the producer: both Postgres and pg_lake must frame the
    output as a single JSON array pg_lake reads back row-for-row.
    """
    _skip_pre_pg19(pg_conn)

    json_path = tmp_path / "pj_fa_local.json"
    _pj_setup(pg_conn)
    before = _pj_rows(pg_conn, "pj_parent")

    _set_json_writer(pg_conn, writer)
    run_command(
        f"COPY pj_parent TO '{json_path}' WITH (format json, force_array true)",
        pg_conn,
    )

    _assert_single_json_array(json_path.read_bytes(), _PART_ROW_COUNT)

    _read_back_via_pglake(pg_conn)
    run_command(f"COPY pj_dst FROM '{json_path}' WITH (format json)", pg_conn)
    after = _pj_rows(pg_conn, "pj_dst")
    assert before == after
    assert len(after) == _PART_ROW_COUNT

    pg_conn.rollback()


def test_pg19_partitioned_json_force_array_s3(pg_conn, s3, tmp_path):
    """PG19 nice story (s3 variant): COPY <partitioned parent> TO an s3 lake
    target with force_array emits a single JSON array, then COPY dst FROM reads
    it back row-for-row."""
    _skip_pre_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_pg19_partitioned_json/pj_fa_s3.json"
    _pj_setup(pg_conn)
    before = _pj_rows(pg_conn, "pj_parent")

    run_command(
        f"COPY pj_parent TO '{url}' WITH (format json, force_array true)",
        pg_conn,
    )

    _assert_single_json_array(
        read_s3_operations(s3, url, is_text=False), _PART_ROW_COUNT
    )

    run_command(f"COPY pj_dst FROM '{url}' WITH (format json)", pg_conn)
    after = _pj_rows(pg_conn, "pj_dst")
    assert before == after
    assert len(after) == _PART_ROW_COUNT

    pg_conn.rollback()


def test_types(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """create schema if not exists lake_struct;
    create type lake_struct.custom_type as (x int, y int)""",
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

    insert_command = """
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
    /* c_json */ '{"hello":"world" }',
    /* c_jsonb */ '{"hello":"world" }',
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
    """
    run_command(insert_command, pg_conn)

    json_path = tmp_path / "test_types.json"

    copy_command = "COPY test_types TO STDOUT WITH (format 'json')"
    copy_to_file(copy_command, json_path, pg_conn)

    duckdb_conn.execute("DESCRIBE SELECT * FROM read_json_auto($1)", [str(json_path)])

    # get results as dictionary
    result_dict = {}
    for record in duckdb_conn.fetchall():
        result_dict[record[0]] = record[1]

    # reference dictionary (may change if we find a better mapping)
    expected_dict = {
        "c_array": "BIGINT[]",
        "c_bit": "VARCHAR",
        "c_bool": "BOOLEAN",
        "c_bpchar": "VARCHAR",
        "c_bytea": "VARCHAR",
        "c_char": "VARCHAR",
        "c_cidr": "VARCHAR",
        "c_custom": "STRUCT(x BIGINT, y BIGINT)",
        "c_date": "DATE",
        "c_float4": "DOUBLE",
        "c_float8": "DOUBLE",
        "c_inet": "VARCHAR",
        "c_int2": "BIGINT",
        "c_int4": "BIGINT",
        "c_int8": "BIGINT",
        "c_interval": "VARCHAR",
        # DuckDB converts nested JSON to structs when reading
        "c_json": "STRUCT(hello VARCHAR)",
        "c_jsonb": "STRUCT(hello VARCHAR)",
        "c_money": "VARCHAR",
        "c_name": "VARCHAR",
        "c_numeric": "DOUBLE",
        "c_numeric_large": "VARCHAR",
        "c_numeric_mod": "DOUBLE",
        "c_oid": "BIGINT",
        "c_text": "VARCHAR",
        "c_tid": "VARCHAR",
        "c_time": "TIME",
        "c_timestamp": "TIMESTAMP",
        "c_timestamptz": "VARCHAR",
        "c_timetz": "VARCHAR",
        "c_uuid": "UUID",
        "c_varbit": "VARCHAR",
        "c_varchar": "VARCHAR",
    }

    assert result_dict == expected_dict

    # Test whether export/import leads to same table, broken down to make it easier to see error cases
    # t_timestamptz is skipped for now because it seems to have a dependency on the local system time zone
    q1 = "SELECT c_array, c_bit, c_bool c_bpchar, c_bytea, c_char, c_cidr, c_custom FROM test_types"
    q2 = "SELECT c_date, c_float4, c_float8, c_inet, c_int2, c_int4, c_int8, c_interval FROM test_types"
    q3 = "SELECT c_json, c_jsonb, c_money, c_name, c_numeric, c_numeric_large, c_numeric_mod FROM test_types"
    q4 = "SELECT c_oid, c_text, c_tid, c_time, c_timestamp, c_timetz, c_uuid, c_varbit, c_varchar FROM test_types"

    before1 = run_query(q1, pg_conn)
    before2 = run_query(q2, pg_conn)
    before3 = run_query(q3, pg_conn)
    before4 = run_query(q4, pg_conn)

    run_command(f"CREATE TABLE test_types_after (LIKE test_types)", pg_conn)
    run_command(
        f"COPY test_types_after FROM '{json_path}' WITH (format 'json')", pg_conn
    )

    after1 = run_query(q1, pg_conn)
    after2 = run_query(q2, pg_conn)
    after3 = run_query(q3, pg_conn)
    after4 = run_query(q4, pg_conn)

    assert before1 == after1
    assert before2 == after2
    assert before3 == after3
    assert before4 == after4

    pg_conn.rollback()


def test_null_nan(pg_conn, duckdb_conn, tmp_path):
    json_path = tmp_path / "test.json"

    # Write table with null and nan to a JSON file and read it into another table
    run_command(
        f"""
        CREATE TABLE test_null_nan (string text, number float);
        INSERT INTO test_null_nan VALUES (NULL, 'nan'::float);
        COPY test_null_nan TO '{json_path}' WITH (format 'json');

        CREATE TABLE test_null_nan_after (like test_null_nan);
        COPY test_null_nan_after FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_null_nan_after", pg_conn)
    assert result[0]["string"] == None
    assert math.isnan(result[0]["number"])

    pg_conn.rollback()


def test_too_many_columns(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with 3 columns
    json_path = tmp_path / "test.json"
    duckdb_conn.execute(
        f"COPY (SELECT generate_series x, 1 z, 2 y FROM generate_series(1,10)) TO '{json_path}'"
    )

    # Create a table with 2 columns
    run_command("CREATE TABLE test_2cols (x int, y int)", pg_conn)

    # Try to copy in the data, we will pick up the columns whose names match
    copy_command = "COPY test_2cols FROM STDIN WITH (format 'json')"
    copy_from_file(copy_command, json_path, pg_conn)

    result = run_query("SELECT count(*), min(y) FROM test_2cols", pg_conn)
    assert result[0]["count"] == 10
    assert result[0]["min"] == 2

    pg_conn.rollback()


def test_too_few_columns(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with 1 column
    json_path = tmp_path / "test.json"
    duckdb_conn.execute(
        f"COPY (SELECT generate_series AS x, 5 z FROM generate_series(1,10)) TO '{json_path}'"
    )

    # Create a table with 2 columns
    run_command("CREATE TABLE test_2cols (x int, y int)", pg_conn)

    # Try to copy into all columns
    copy_command = "COPY test_2cols FROM STDIN WITH (format 'json')"
    copy_from_file(copy_command, json_path, pg_conn)

    # Only x had matching values
    result = run_query("SELECT max(x) mx, max(y) my FROM test_2cols", pg_conn)
    assert result[0]["mx"] == 10
    assert result[0]["my"] == None

    # Try to only copy into one column
    duckdb_conn.execute(
        f"COPY (SELECT generate_series AS y, 5 z FROM generate_series(1,10)) TO '{json_path}'"
    )

    run_command("TRUNCATE test_2cols", pg_conn)

    copy_command = "COPY test_2cols (y) FROM STDIN WITH (format 'json')"
    copy_from_file(copy_command, json_path, pg_conn)

    # Only y had matching values
    result = run_query("SELECT max(x) mx, max(y) my FROM test_2cols", pg_conn)
    assert result[0]["mx"] == None
    assert result[0]["my"] == 10

    pg_conn.rollback()


def test_invalid_type(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with a text field
    json_path = tmp_path / "test.json"
    duckdb_conn.execute(
        f"COPY (SELECT 'hello' AS x FROM generate_series(1,10)) TO '{json_path}'"
    )

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Try to copy text into the table
    copy_command = "COPY test_int FROM STDIN WITH (format 'json')"
    error = copy_from_file(copy_command, json_path, pg_conn, raise_error=False)
    assert error.startswith("ERROR:  invalid input syntax for type integer")

    pg_conn.rollback()


def test_partially_invalid_type(pg_conn, duckdb_conn, tmp_path):
    # Generate a file with a small number (can be int) and a large number (must be bigint)
    json_path = tmp_path / "test.json"
    duckdb_conn.execute(
        f"COPY (SELECT col0 AS x FROM (VALUES(1), (5000000000))) TO '{json_path}'"
    )

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Try to copy text into the table
    copy_command = "COPY test_int FROM STDIN WITH (format 'json')"
    error = copy_from_file(copy_command, json_path, pg_conn, raise_error=False)
    assert error.startswith(
        'ERROR:  value "5000000000" is out of range for type integer'
    )

    pg_conn.rollback()


def test_maximum_object_size_option(pg_conn, duckdb_conn, tmp_path):
    # Verify that we accept and pass down the maximum_object_size option when reading a JSON file
    json_path = tmp_path / "test.json"
    duckdb_conn.execute(f"COPY (SELECT * FROM (VALUES(1), (2))) TO '{json_path}'")

    # Invalid string value
    run_command("CREATE TABLE test_int (x int)", pg_conn)
    copy_command = f"COPY test_int FROM '{json_path}' WITH (format 'json', maximum_object_size 'asdf')"
    error = run_command(copy_command, pg_conn, raise_error=False)

    assert error.startswith("ERROR:  maximum_object_size requires a numeric value")

    pg_conn.rollback()

    # Invalid negative value
    run_command("CREATE TABLE test_int (x int)", pg_conn)
    copy_command = f"COPY test_int FROM '{json_path}' WITH (format 'json', maximum_object_size -1234)"
    error = run_command(copy_command, pg_conn, raise_error=False)

    assert error.startswith(
        'ERROR:  option value "maximum_object_size" must be non-negative'
    )

    pg_conn.rollback()

    # Valid value
    run_command("CREATE TABLE test_int (x int)", pg_conn)
    copy_command = f"COPY test_int FROM '{json_path}' WITH (format 'json', maximum_object_size 1234)"

    error = run_command(copy_command, pg_conn, raise_error=False)

    assert error is None

    pg_conn.rollback()

    # Valid value, unsupported format
    run_command("CREATE TABLE test_int (x int)", pg_conn)
    copy_command = f"COPY test_int FROM '{json_path}' WITH (format 'parquet', maximum_object_size 1234)"

    error = run_command(copy_command, pg_conn, raise_error=False)

    assert 'pg_lake_copy: invalid option "maximum_object_size"' in error

    pg_conn.rollback()


def test_invalid_option(pg_conn, duckdb_conn, tmp_path):
    # Create a simple JSON file
    json_path = tmp_path / "test.json"
    duckdb_conn.execute(f"COPY (SELECT * FROM (VALUES(1), (2))) TO '{json_path}'")

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for JSON in COPY FROM
    copy_command = "COPY test_int FROM STDIN WITH (format 'json', quote '|')"
    error = copy_from_file(copy_command, json_path, pg_conn, raise_error=False)
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "quote" for COPY FROM with json format'
    )

    pg_conn.rollback()

    # Create a table with an int column
    run_command("CREATE TABLE test_int (x int)", pg_conn)

    # Use an option that's invalid for JSON in COPY TO
    copy_command = "COPY test_int TO STDOUT WITH (format 'json', quote '|')"
    error = copy_from_file(copy_command, json_path, pg_conn, raise_error=False)
    assert error.startswith(
        'ERROR:  pg_lake_copy: invalid option "quote" for COPY TO with json format'
    )

    pg_conn.rollback()


def test_compression(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """
        CREATE TABLE test_compressed (x int);
        INSERT INTO test_compressed SELECT s FROM generate_series(1,1000) s
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Write an uncompressed file
    uncompressed_path = tmp_path / "test.json"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'json', compression 'none')"
    )
    copy_to_file(copy_command, uncompressed_path, pg_conn)

    # Write a compressed file
    compressed_path = tmp_path / "test.json.zst"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'json', compression 'zstd')"
    )
    copy_to_file(copy_command, compressed_path, pg_conn)

    assert os.path.getsize(compressed_path) < os.path.getsize(uncompressed_path)

    # Try to read it
    copy_command = (
        "COPY test_compressed FROM STDIN WITH (format 'json', compression 'zstd')"
    )
    copy_from_file(copy_command, compressed_path, pg_conn)

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    pg_conn.rollback()

    # Reading using wrong compression
    copy_command = (
        "COPY test_compressed FROM STDIN WITH (format 'json', compression 'gzip')"
    )
    error = copy_from_file(copy_command, compressed_path, pg_conn, raise_error=False)
    assert error.startswith("ERROR:  IO Error: Input is not a GZIP stream")

    pg_conn.rollback()

    copy_command = f"COPY test_compressed FROM '{uncompressed_path}' WITH (format 'json', compression 'gzip')"
    error = run_command(copy_command, pg_conn, raise_error=False)
    assert f"Input is not a GZIP stream: {uncompressed_path}" in error

    pg_conn.rollback()

    # Write gzip
    compressed_path = tmp_path / "test.json.gz"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'json', compression 'gzip')"
    )
    copy_to_file(copy_command, compressed_path, pg_conn)

    assert os.path.getsize(compressed_path) < os.path.getsize(uncompressed_path)

    # Try to read it
    copy_command = (
        "COPY test_compressed FROM STDIN WITH (format 'json', compression 'gzip')"
    )
    copy_from_file(copy_command, compressed_path, pg_conn)

    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_compressed",
        pg_conn,
    )
    assert result[0]["count"] == 2000
    assert result[0]["distinct"] == 1000

    # Try to read gzip without specifying compression
    copy_command = "COPY test_compressed FROM STDIN WITH (format 'json')"
    error = copy_from_file(copy_command, compressed_path, pg_conn, raise_error=False)
    assert error.startswith("ERROR:  Invalid Input Error: Malformed JSON in file")

    pg_conn.rollback()

    # Try to write snappy
    compressed_path = tmp_path / "test.json.snappy"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'json', compression 'snappy')"
    )
    error = copy_to_file(copy_command, compressed_path, pg_conn, raise_error=False)
    assert error.startswith(
        "ERROR:  pg_lake_copy: snappy compression is not supported for JSON format"
    )

    pg_conn.rollback()

    # Try to read snappy
    copy_command = (
        "COPY test_compressed FROM STDIN WITH (format 'json', compression 'snappy')"
    )
    error = copy_to_file(copy_command, compressed_path, pg_conn, raise_error=False)
    assert error.startswith(
        "ERROR:  pg_lake_copy: snappy compression is not supported for JSON format"
    )

    pg_conn.rollback()

    drop_table_command = "DROP TABLE test_compressed"
    run_command(drop_table_command, pg_conn)
    pg_conn.commit()


def test_invalid_compression(pg_conn, duckdb_conn, tmp_path):
    run_command(
        """
        CREATE TABLE test_compressed (x int);
    """,
        pg_conn,
    )

    # Write with unrecognized compression
    compressed_path = tmp_path / "test.json.snappy"
    copy_command = (
        "COPY test_compressed TO STDOUT WITH (format 'json', compression 'zoko')"
    )
    error = copy_to_file(copy_command, compressed_path, pg_conn, raise_error=False)
    assert error.startswith('ERROR:  pg_lake_copy: compression "zoko" not recognized')

    pg_conn.rollback()


def test_copy_to_file(pg_conn, duckdb_conn, tmp_path):
    # Write empty table to JSON file
    json_path = tmp_path / "test.json"
    run_command(
        f"""
        CREATE TABLE test_copy_to_file (x int, y int);
        COPY test_copy_to_file TO '{json_path}' WITH (format 'JSON');
    """,
        pg_conn,
    )

    # Check output
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_json_auto($1)", [str(json_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 0

    # Write table with data to a JSON file
    run_command(
        f"""
        INSERT INTO test_copy_to_file VALUES (1,2), (3,4);
        COPY test_copy_to_file TO '{json_path}' WITH (format 'json');
        COPY test_copy_to_file FROM '{json_path}' WITH (format 'json', FREEZE true);
    """,
        pg_conn,
    )

    # Check output
    result = run_query(
        "SELECT count(*) AS count, count(distinct x) AS distinct FROM test_copy_to_file",
        pg_conn,
    )
    assert result[0]["count"] == 4
    assert result[0]["distinct"] == 2

    # Make sure it's actually JSON
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_json_auto($1)", [str(json_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 2

    pg_conn.rollback()


def test_copy_large_row(pg_conn, duckdb_conn, tmp_path):
    # Write a table with rows of several MB to a JSON file
    json_path = tmp_path / "test.json"
    run_command(
        f"""
        CREATE TABLE test_large_row (large1 text, large2 text);
        CREATE TABLE test_large_row_after (like test_large_row);
        INSERT INTO test_large_row SELECT repeat('A', 20000000) FROM generate_series(1,3);
        COPY test_large_row TO '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    # Check length of the rows
    duckdb_conn.execute(
        "SELECT min(length(large1)) AS l1 FROM read_json_auto($1, maximum_object_size = 30000000)",
        [str(json_path)],
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 20000000

    # Verify we read this JSON back
    error = run_command(
        f"""
        COPY test_large_row_after FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error is None

    res = run_query("SELECT min(length(large1)) FROM test_large_row_after", pg_conn)

    assert res[0][0] == 20000000
    pg_conn.rollback()


def test_column_subset(pg_conn, duckdb_conn, tmp_path):
    json_path = tmp_path / "test.json"

    # Create a table and emit a subset of columns into JSON
    run_command(
        f"""
        CREATE TABLE test_column_subset (
           val1 int,
           d date,
           val2 int,
           "gre@t" text,
           val3 bigint
        );
        INSERT INTO test_column_subset VALUES (1,'2020-01-01',3,'hello', 5);
        INSERT INTO test_column_subset VALUES (2,'2021-01-01',4,'hello', 6);
        INSERT INTO test_column_subset VALUES (3,NULL,6,'world', 9);
        ALTER TABLE test_column_subset DROP COLUMN val2;
        COPY test_column_subset (val3, "gre@t", d) TO '{json_path}' WITH (format 'json');

        CREATE TABLE test_column_subset_after (val2 int, val3 bigint, "gre@t" text, d date);
        COPY test_column_subset_after (val3, "gre@t" , d) FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    # Check output, we have 2 unique rows where d is not null
    result = run_query(
        """
        SELECT count(*) AS count FROM (
            SELECT val3, "gre@t", d FROM test_column_subset WHERE d IS NOT NULL
            UNION
            SELECT val3, "gre@t", d FROM test_column_subset_after WHERE d IS NOT NULL
        ) u;
    """,
        pg_conn,
    )
    assert result[0]["count"] == 2

    pg_conn.rollback()


def test_nested_json(pg_conn, tmp_path):
    json_path = tmp_path / "test.json"

    # Create a table with JSON columns and write them to a file
    run_command(
        """
        CREATE TABLE test_json_columns (
           key text,
           value json,
           valueb jsonb
        );
        INSERT INTO test_json_columns VALUES ('item-1', '{"hello":"world"}', '[3,4]');
        INSERT INTO test_json_columns VALUES ('item-2', NULL, '{"array":[]}');
    """,
        pg_conn,
    )

    run_command(
        f"COPY (SELECT * FROM test_json_columns) TO '{json_path}' WITH (format 'json')",
        pg_conn,
    )

    # The JSON columns should be nested, not interpreted as strings
    with open(json_path, "r") as file:
        lines = file.readlines()
        assert (
            lines[0]
            == """{"key":"item-1","value":{"hello":"world"},"valueb":[3,4]}\n"""
        )
        assert lines[1] == """{"key":"item-2","value":null,"valueb":{"array":[]}}\n"""

    # Create a table with JSON columns and write them to a file
    run_command(
        f"""
        CREATE TABLE test_json_columns_after (
           key text,
           value json,
           valueb jsonb
        );
        COPY test_json_columns_after FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_json_columns_after", pg_conn)
    assert result[0]["value"] == {"hello": "world"}
    assert result[0]["valueb"] == [3, 4]
    assert result[1]["value"] == None
    assert result[1]["valueb"] == {"array": []}

    pg_conn.rollback()


def test_corrupt_json(pg_conn, tmp_path):
    json_path = tmp_path / "test_corrupt.json"

    # Missing characters in JSON
    with open(json_path, "w") as json_file:
        json_file.write("""{"hello":}""")

    error = run_command(
        f"""
        CREATE TABLE test_corrupt_json (value int);
        COPY test_corrupt_json FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  Invalid Input Error: Malformed JSON in file")

    pg_conn.rollback()


def test_empty_json(pg_conn, tmp_path):
    json_path = tmp_path / "test_empty.json"

    # Just empty lines
    with open(json_path, "w") as json_file:
        json_file.write("\n\n")

    run_command(
        f"""
        CREATE TABLE test_empty_json (value int);
        COPY test_empty_json FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_empty_json", pg_conn)
    assert len(result) == 0

    pg_conn.rollback()


def test_escaped_json(pg_conn, tmp_path):
    json_path = tmp_path / "test_escaped.json"

    # Funky escape sequences
    with open(json_path, "w") as json_file:
        # The backslashes here are doubly escaped, once for Python, once for JSON
        json_file.write('{"x":"\\u0100\\b\\n\\\\", "y":"\\\\x000a"}\n')

    run_command(
        f"""
        CREATE TABLE test_escaped_json (x text, y bytea);
        COPY test_escaped_json FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT x, get_byte(y, 0) y0, get_byte(y, 1) y1 FROM test_escaped_json", pg_conn
    )
    assert len(result) == 1
    assert result[0]["x"] == "Ā\x08\n\\"
    assert result[0]["y0"] == 0
    assert result[0]["y1"] == 10

    pg_conn.rollback()

    # Additional escape sequences
    with open(json_path, "w") as json_file:
        # The backslashes here are doubly escaped, once for Python, once for JSON
        json_file.write('{"x":"\\n\\"\\\\"}\n')

    run_command(
        f"""
        CREATE TABLE test_escaped_json(x text, y jsonb);
        COPY test_escaped_json (x) FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    result = run_query("SELECT x FROM test_escaped_json", pg_conn)
    assert len(result) == 1
    assert result[0]["x"] == '\n"\\'

    pg_conn.rollback()


def test_unicode(pg_conn, tmp_path):
    json_path = tmp_path / "test_unicode.json"

    # Some unicode characters
    with open(json_path, "w") as json_file:
        json_file.write('{"x":"\\u0100🙂😉👍", "y":"unicode"}\n')

    run_command(
        f"""
        CREATE TABLE test_unicode_json (x text);
        COPY test_unicode_json FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    result = run_query("SELECT x FROM test_unicode_json", pg_conn)
    assert len(result) == 1
    assert result[0]["x"] == "\u0100🙂😉👍"

    pg_conn.rollback()


def test_unicode_0(pg_conn, tmp_path):
    json_path = tmp_path / "test_unicode.json"

    # Invalid unicode character
    with open(json_path, "w") as json_file:
        json_file.write('{"x":"a\\u0000b", "y":"unicode0"}\n')

    run_command(
        f"""
        CREATE TABLE test_unicode_json (x text);
        COPY test_unicode_json FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    # Our current implementation treats the 0 character as end-of-string
    result = run_query("SELECT x FROM test_unicode_json", pg_conn)
    assert len(result) == 1
    assert result[0]["x"] == "a"

    pg_conn.rollback()


def test_jsonb_eol(pg_conn, tmp_path):
    json_path = tmp_path / "test_jsonb_eol.json"

    # Try to export and import JSONB value with end-of-line
    run_command(
        f"""
        CREATE TABLE test_jsonb (j jsonb);
        COPY (SELECT '"\\n\\"\\\\"'::jsonb) TO '{json_path}' WITH (format 'JSON');
        COPY test_jsonb FROM '{json_path}' WITH (format 'JSON');
    """,
        pg_conn,
    )

    # Currently not passing due to a CSV encoding issue in DuckDB
    result = run_query("SELECT j FROM test_jsonb", pg_conn)
    assert len(result) == 1
    # assert result[0]['j'] == '\n"\\'

    pg_conn.rollback()


def test_array_only(superuser_conn, tmp_path):
    json_path = tmp_path / "test_array_only.json"

    # Write an array, but parse it as an object
    with open(json_path, "w") as json_file:
        json_file.write('[4, "hello"]\n')

    # We deliberately disable JSON because this otherwise runs into a DuckDB bug
    error = run_command(
        f"""
        SET pg_lake_copy.enable_json TO off;
        CREATE TABLE test_array_only (v int, k text);
        COPY test_array_only FROM '{json_path}' WITH (format 'json');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert error.startswith(
        "ERROR:  pg_lake_copy: JSON is experimental and currently disabled"
    )

    superuser_conn.rollback()


def test_duplicate_columns(pg_conn, tmp_path):
    json_path = tmp_path / "test_duplicate_columns.json"

    # Test 2 identical column names
    error = run_command(
        f"""
        CREATE TABLE test_duplicate_columns (a int);
        INSERT INTO test_duplicate_columns VALUES (1);
        COPY (SELECT a, a FROM test_duplicate_columns) TO '{json_path}' WITH (FORMAT 'json');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  pg_lake_copy: column "a" specified more than once')

    pg_conn.rollback()

    # Test lowercase + uppercase
    error = run_command(
        f"""
        CREATE TABLE test_duplicate_columns (a int);
        INSERT INTO test_duplicate_columns VALUES (1);
        COPY (SELECT a, a as "A" FROM test_duplicate_columns) TO '{json_path}' WITH (FORMAT 'json');
    """,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith('ERROR:  pg_lake_copy: column "a" specified more than once')

    pg_conn.rollback()


def test_copy_back_and_forth(pg_conn, duckdb_conn, tmp_path):
    json_path = tmp_path / "test_copy_back_and_forth.json"

    run_command(
        f"""
        CREATE TABLE test_copy_back_and_forth (a int, b text);
        INSERT INTO test_copy_back_and_forth SELECT i, i::text FROM generate_series(1,100) i;

        COPY test_copy_back_and_forth(b) TO  '{json_path}' WITH (format 'json');
        COPY test_copy_back_and_forth(b) FROM  '{json_path}' WITH (format 'json');
        COPY test_copy_back_and_forth(a) TO  '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    # Check number of rows (2*100)
    duckdb_conn.execute(
        "SELECT count(*) AS count FROM read_json_auto($1)", [str(json_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 200


def test_copy_dropped_column(pg_conn, duckdb_conn, tmp_path):
    json_path = tmp_path / "test_copy_dropped_column.json"

    run_command(
        f"""
       CREATE TABLE test_copy_dropped_column (a int, b int);
       INSERT INTO test_copy_dropped_column SELECT i FROM generate_series(1, 100)i;

       COPY test_copy_dropped_column TO '{json_path}' WITH (format 'json');
       ALTER TABLE test_copy_dropped_column DROP COLUMN a;
       COPY test_copy_dropped_column TO '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    # Check that there is 1 column named b
    duckdb_conn.execute("DESCRIBE SELECT * FROM read_json_auto($1)", [str(json_path)])
    duckdb_result = duckdb_conn.fetchall()
    assert len(duckdb_result) == 1
    assert duckdb_result[0][0] == "b"


def test_copy_no_column(pg_conn, duckdb_conn, tmp_path):
    json_path = tmp_path / "test_copy_no_column.json"

    run_command(
        f"""
       CREATE TABLE test_copy_no_column (a int);
       INSERT INTO test_copy_no_column SELECT i FROM generate_series(1, 100)i;

       ALTER TABLE test_copy_no_column DROP COLUMN a;
       COPY test_copy_no_column TO '{json_path}' WITH (format 'json');
       COPY test_copy_no_column FROM '{json_path}' WITH (format 'json');
    """,
        pg_conn,
    )

    # Technically, the file could be non-empty, e.g. 1 line per record,
    # or an empty JSON per record, but currently 0 column files get lost in
    # translation.
    assert os.stat(json_path).st_size == 0

    # Hence, the count also remained at 100
    result = run_query("SELECT count(*) FROM test_copy_no_column", pg_conn)
    assert result[0]["count"] == 100


def test_complex_select(pg_conn, duckdb_conn, tmp_path):
    json_path = tmp_path / "test_complex_select.json"

    run_command(
        """
        CREATE TABLE nested_json_test (
            name TEXT,
            attributes json,
            metrics jsonb
        );
        INSERT INTO nested_json_test (name, attributes, metrics) VALUES
            ('Product 1',	'{"color": "red", "size": "M"}',	'{"sold": 100, "stock": 50}'),
            ('Product 2',	'{"color": "blue", "size": "L"}','{"sold": 150, "stock": 30}'),
            ('Product 1',	'{"color": "red", "size": "M"}',	'{"sold": 100, "stock": 50}'),
            ('Product 2',	'{"color": "blue", "size": "L"}','{"sold": 150, "stock": 30}'),
            ('Product 1',	'{"color":"red","size":"M"}',	'{"sold": 100, "stock": 50}'),
            ('Product 2',	'{"color":"blue","size":"L"}',	'{"sold": 150, "stock": 30}'),
            ('Product 1',	'{"color":"red","size":"M"}',	'{"sold": 100, "stock": 50}'),
            ('Product 2',	'{"color":"blue","size":"L"}',	'{"sold": 150, "stock": 30}');
        CREATE TABLE product_details (
            name TEXT,
            detail_key TEXT,
            detail_value TEXT
        );
        INSERT INTO product_details (name, detail_key, detail_value) VALUES
        ('Product 1', 'manufacturer', 'Company A'),
        ('Product 1', 'warranty', '2 years'),
        ('Product 2', 'manufacturer', 'Company B'),
        ('Product 2', 'warranty', '1 year');
        COPY (
            SELECT p.name,
                   json_build_object(
                       'attributes', p.attributes,
                       'metrics', p.metrics,
                       'details', d.details
                   ) as product_info
            FROM nested_json_test p
            JOIN (
                SELECT
                    name,
                    json_agg(json_build_object(detail_key, detail_value)) as details
                FROM product_details
                GROUP BY name
            ) d ON p.name = d.name
        ) TO '"""
        + str(json_path)
        + """' WITH (FORMAT 'json');
    """,
        pg_conn,
    )

    duckdb_conn.execute(
        "SELECT product_info.attributes.color FROM read_json_auto($1)", [str(json_path)]
    )
    duckdb_result = duckdb_conn.fetchall()
    assert len(duckdb_result) == 8
    assert duckdb_result[0][0] == "red"
    assert duckdb_result[1][0] == "blue"


def test_create_json_maximum_object_size(pg_conn):
    url = f"s3://{TEST_BUCKET}/test_load_json_table/data.json"

    run_command(
        f"""
        COPY (SELECT s x, s y FROM generate_series(1,10) s) TO '{url}';
        CREATE TABLE test_load_json_table () WITH (load_from='{url}', maximum_object_size=12345);
    """,
        pg_conn,
    )

    # verify data was loaded
    result = run_query("SELECT * FROM test_load_json_table ORDER BY x", pg_conn)
    assert len(result) == 10

    pg_conn.rollback()

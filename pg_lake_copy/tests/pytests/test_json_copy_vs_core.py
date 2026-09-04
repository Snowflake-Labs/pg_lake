"""Parity tests for JSON COPY between pg_lake and core PostgreSQL (PG19+).

pg_lake serves JSON COPY through DuckDB (table -> intermediate CSV -> DuckDB ->
JSON), whereas core PostgreSQL gained a native JSON COPY in PG19 that serializes
its types to JSON directly. These tests pin down, type by type and structure by
structure, where the two are byte-identical and where DuckDB's serialization
legitimately differs, so that future changes to either side are caught.

The comparison toggles the hidden pg_lake_copy.json_copy_mode knob: 'pglake'
forces the COPY through DuckDB, 'postgres' forces the identical statement onto
core PostgreSQL. (The production default is 'auto', which would route a local
COPY TO json to Postgres, so these parity tests must force the producer
explicitly.) Single-row sources are used so output is one JSON line and row
ordering never matters.
"""

import json

import pytest
from utils_pytest import *


def _copy_to_stdout_json(
    conn, source, tmp_path, fname, use_core, raise_error=True, force_array=False
):
    """Capture the JSON produced by COPY <source> TO STDOUT (format json).

    use_core selects the producer: True forces core PostgreSQL
    (json_copy_mode=postgres), False forces pg_lake's DuckDB path
    (json_copy_mode=pglake). force_array appends the FORCE_ARRAY option (single
    JSON array). Returns the raw bytes on success, or the error string when
    raise_error is False.
    """
    run_command(
        "SET pg_lake_copy.json_copy_mode TO "
        + ("'postgres'" if use_core else "'pglake'"),
        conn,
    )
    options = "format 'json'"
    if force_array:
        options += ", force_array true"
    path = tmp_path / fname
    error = copy_to_file(
        f"COPY {source} TO STDOUT WITH ({options})", path, conn, raise_error
    )
    if error is not None:
        return error
    return path.read_bytes()


# Custom types needed by the structured cases below. Created inside the test
# transaction and rolled back, so they never leak between tests.
_STRUCT_SETUP = """
CREATE TYPE vc_composite AS (x int, y text);
CREATE TYPE vc_enum AS ENUM ('a', 'b', 'c');
CREATE DOMAIN vc_domain AS int CHECK (VALUE > 0);
"""


# Data types and structures for which pg_lake (DuckDB) and core emit byte-for-byte
# identical JSON. Each entry is a single-row SQL expression emitted as column "c".
_IDENTICAL_CASES = [
    ("int2", "1::int2"),
    ("int4", "100000::int4"),
    ("int8", "10000000000::int8"),
    ("bool_true", "true"),
    ("bool_false", "false"),
    ("text", "'hello world'::text"),
    ("text_quotes", "'a\"b'::text"),
    ("text_backslash", "'a\\b'::text"),
    ("text_newline", "E'a\\nb'::text"),
    ("text_tab", "E'a\\tb'::text"),
    ("text_carriage_return", "E'a\\rb'::text"),
    ("text_backspace", "E'a\\bb'::text"),
    ("text_formfeed", "E'a\\fb'::text"),
    ("text_control_char", "E'a\\x01b'::text"),
    ("text_quote_and_backslash", "E'a\"\\\\b'::text"),
    ("text_unicode", "'Ā🙂'::text"),
    ("empty_text", "''::text"),
    ("float4", "3.5::float4"),
    ("float8", "33333333.33444444::float8"),
    ("numeric_mod", "99.99::numeric(4,2)"),
    ("date", "'2024-01-01'::date"),
    ("time", "'19:34:00'::time"),
    ("interval", "'3 days'::interval"),
    ("uuid", "'acd661ca-d18c-42e2-9c4e-61794318935e'::uuid"),
    ("bytea", "'\\x0001'::bytea"),
    ("inet", "'192.168.1.1'::inet"),
    ("cidr", "'192.168.0.0/16'::cidr"),
    ("bit", "B'1'"),
    ("varbit", "B'0110'"),
    ("money", "'$4.5'::money"),
    ("json", '\'{"hello":"world"}\'::json'),
    ("json_array", "'[1,2,3]'::json"),
    ("int_array", "ARRAY[1,2,3]"),
    ("text_array", "ARRAY['a','b']"),
    ("array_with_null", "ARRAY[1,NULL,3]"),
    ("null_value", "NULL::int"),
    ("composite", "ROW(1, 'hi')::vc_composite"),
    ("enum", "'b'::vc_enum"),
    ("domain", "5::vc_domain"),
    ("int4range", "'[1,5)'::int4range"),
    ("numrange", "'[1.5,2.5]'::numrange"),
    ("composite_array", "ARRAY[ROW(1,'a')::vc_composite, ROW(2,'b')::vc_composite]"),
]


# Types where DuckDB's JSON serialization differs from core's. We pin both the
# pg_lake and the core output so the divergence stays visible and intentional.
_DIFFERENT_CASES = [
    # Unconstrained numeric: DuckDB pads to the type's scale (trailing zeros).
    ("numeric_small", "199.123::numeric", b'{"c":199.123000000}\n', b'{"c":199.123}\n'),
    # numeric outside int64/double range: DuckDB emits it as a JSON string,
    # core as a JSON number.
    (
        "numeric_large",
        "123456789012345678901234.99::numeric(39,2)",
        b'{"c":"123456789012345678901234.99"}\n',
        b'{"c":123456789012345678901234.99}\n',
    ),
    # Timestamp: DuckDB uses a space separator, core uses the ISO 8601 "T".
    (
        "timestamp",
        "'2024-01-01 15:00:00'::timestamp",
        b'{"c":"2024-01-01 15:00:00"}\n',
        b'{"c":"2024-01-01T15:00:00"}\n',
    ),
    # jsonb: DuckDB re-minifies (no space after colon), core preserves its
    # canonical "key": value spacing.
    (
        "jsonb",
        '\'{"hello":"world"}\'::jsonb',
        b'{"c":{"hello":"world"}}\n',
        b'{"c":{"hello": "world"}}\n',
    ),
]


# Every type/structure case, regardless of whether pg_lake's textual JSON
# matches core's. The representation differences above are cosmetic: a value
# written by pg_lake must always read back unchanged through pg_lake, so every
# case is also exercised as a write->read roundtrip below.
_ALL_TYPE_CASES = [(c[0], c[1]) for c in _IDENTICAL_CASES + _DIFFERENT_CASES]


def _skip_pre_pg19(conn):
    if get_pg_version_num(conn) < 190000:
        pytest.skip(
            "native JSON COPY TO (and the pg_lake->core fallback) require PG19+"
        )


@pytest.mark.parametrize(
    "case_id,expr", _IDENTICAL_CASES, ids=[c[0] for c in _IDENTICAL_CASES]
)
def test_json_to_stdout_matches_core(superuser_conn, case_id, expr, tmp_path):
    _skip_pre_pg19(superuser_conn)

    try:
        run_command(_STRUCT_SETUP, superuser_conn)
        source = f"(SELECT {expr} AS c)"
        lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "lake.json", False
        )
        core = _copy_to_stdout_json(superuser_conn, source, tmp_path, "core.json", True)
    finally:
        superuser_conn.rollback()

    assert lake == core, f"{case_id}: pg_lake={lake!r} core={core!r}"


@pytest.mark.parametrize(
    "case_id,expr,expected_lake,expected_core",
    _DIFFERENT_CASES,
    ids=[c[0] for c in _DIFFERENT_CASES],
)
def test_json_to_stdout_duckdb_differences(
    superuser_conn, case_id, expr, expected_lake, expected_core, tmp_path
):
    _skip_pre_pg19(superuser_conn)

    try:
        source = f"(SELECT {expr} AS c)"
        lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "lake.json", False
        )
        core = _copy_to_stdout_json(superuser_conn, source, tmp_path, "core.json", True)
    finally:
        superuser_conn.rollback()

    assert lake == expected_lake, f"{case_id}: pg_lake output changed: {lake!r}"
    assert core == expected_core, f"{case_id}: core output changed: {core!r}"
    assert lake != core


def test_json_to_stdout_multidim_array_unsupported(superuser_conn, tmp_path):
    """Multi-dimensional arrays cannot round-trip through pg_lake's CSV stage.

    Core handles them natively; pg_lake raises a clear DuckDB conversion error.
    This documents the known gap so a future fix flips the assertion.
    """
    _skip_pre_pg19(superuser_conn)

    try:
        source = "(SELECT ARRAY[[1,2],[3,4]] AS c)"
        lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "lake.json", False, raise_error=False
        )
    finally:
        superuser_conn.rollback()

    assert "Could not convert string" in lake

    try:
        core = _copy_to_stdout_json(superuser_conn, source, tmp_path, "core.json", True)
    finally:
        superuser_conn.rollback()

    assert core == b'{"c":[[1,2],[3,4]]}\n'


def test_json_copy_from_roundtrip(pg_conn, tmp_path):
    """pg_lake JSON write -> pg_lake JSON read preserves values across types.

    The representation differences above are irrelevant here: the JSON is parsed
    straight back into the same PostgreSQL types, so values must be unchanged.
    """
    json_path = tmp_path / "roundtrip.json"

    # Force pg_lake for both write and read so this exercises pg_lake's own JSON
    # path (under the 'auto' default a local COPY TO json goes to Postgres).
    run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", pg_conn)

    run_command(
        """
        CREATE TYPE rt_composite AS (x int, y text);
        CREATE TABLE rt_src (
            c_int int,
            c_text text,
            c_numeric numeric,
            c_bool bool,
            c_json json,
            c_jsonb jsonb,
            c_int_array int[],
            c_text_array text[],
            c_ts timestamp,
            c_composite rt_composite
        );
        INSERT INTO rt_src VALUES
            (1, 'hello', 199.123, true, '{"a":1}', '{"b":2}',
             ARRAY[1,2,3], ARRAY['x','y'], '2024-01-01 15:00:00', ROW(7,'z')),
            (2, NULL, NULL, false, NULL, '[1,2]',
             ARRAY[NULL,5], ARRAY['a'], '1999-12-31 23:59:59', ROW(NULL,NULL));
        """,
        pg_conn,
    )

    before = run_query("SELECT * FROM rt_src ORDER BY c_int", pg_conn)

    run_command(
        f"""
        COPY rt_src TO '{json_path}' WITH (format 'json');
        CREATE TABLE rt_dst (LIKE rt_src);
        COPY rt_dst FROM '{json_path}' WITH (format 'json');
        """,
        pg_conn,
    )

    after = run_query("SELECT * FROM rt_dst ORDER BY c_int", pg_conn)

    assert [dict(r) for r in before] == [dict(r) for r in after]

    pg_conn.rollback()


# Complex/nested types whose JSON serialization is non-trivial: composites
# (structs), arrays, arrays-of-composites, json/jsonb. The user-facing question
# is whether a file *written by core PostgreSQL* can be read back through
# pg_lake's DuckDB JSON path without losing structure.
_COMPLEX_RT_DDL = """
CREATE TYPE crt_composite AS (x int, y text);
CREATE TABLE crt_src (
    c_int int,
    c_int_array int[],
    c_text_array text[],
    c_array_with_null int[],
    c_json json,
    c_jsonb jsonb,
    c_composite crt_composite,
    c_composite_array crt_composite[]
);
INSERT INTO crt_src VALUES
    (1, ARRAY[1,2,3], ARRAY['a','b'], ARRAY[1,NULL,3],
     '{"a":1}', '{"b":2}', ROW(1,'hi'),
     ARRAY[ROW(1,'a')::crt_composite, ROW(2,'b')::crt_composite]),
    (2, ARRAY[]::int[], ARRAY['x"y','z\\\\w'], ARRAY[NULL,NULL]::int[],
     NULL, '[1,2,3]', ROW(NULL,NULL),
     ARRAY[ROW(NULL,'only')::crt_composite]);
"""


def _complex_rt_rows_for(conn, tbl):
    # Compare on the text rendering so composite/array equality is exact and
    # independent of how psycopg2 maps the nested types back to Python.
    return run_query(
        f"""
        SELECT c_int,
               c_int_array::text,
               c_text_array::text,
               c_array_with_null::text,
               c_json::text,
               c_jsonb::text,
               c_composite::text,
               c_composite_array::text
        FROM {tbl} ORDER BY c_int
        """,
        conn,
    )


@pytest.mark.parametrize("writer", ["postgres", "pglake"], ids=["postgres", "pglake"])
def test_json_complex_types_read_back_via_pg_lake(superuser_conn, writer, tmp_path):
    """struct/array JSON written by either Postgres or pg_lake reads back via pg_lake.

    The writing of composites and arrays to JSON is the tricky part: Postgres
    serializes them with its own type output functions, pg_lake routes through
    DuckDB. This asserts that regardless of *who wrote* the JSON, pg_lake's JSON
    COPY FROM (DuckDB) reconstructs the exact same nested values.

    A superuser connection is required because the Postgres writer path goes
    through PostgreSQL's COPY TO <file>, which needs pg_write_server_files.
    """
    _skip_pre_pg19(superuser_conn)

    json_path = tmp_path / f"complex_{writer}.json"
    try:
        run_command(_COMPLEX_RT_DDL, superuser_conn)
        before = _complex_rt_rows_for(superuser_conn, "crt_src")

        # Write the JSON with the requested producer.
        run_command(
            f"SET pg_lake_copy.json_copy_mode TO '{writer}'",
            superuser_conn,
        )
        run_command(
            f"COPY crt_src TO '{json_path}' WITH (format 'json')", superuser_conn
        )

        # Always read back through pg_lake (DuckDB), never Postgres.
        run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", superuser_conn)
        run_command("CREATE TABLE crt_dst (LIKE crt_src)", superuser_conn)
        run_command(
            f"COPY crt_dst FROM '{json_path}' WITH (format 'json')", superuser_conn
        )

        after = _complex_rt_rows_for(superuser_conn, "crt_dst")
    finally:
        superuser_conn.rollback()

    assert [dict(r) for r in before] == [
        dict(r) for r in after
    ], f"{writer}-written JSON did not round-trip through pg_lake"


# pg_map map columns are the third "tricky" structured case alongside composites
# and arrays. A map renders as a JSON object ({"1":"a",...}); both Postgres and
# pg_lake emit the same shape. The extension/type are created inside the test
# transaction and rolled back, so they never leak between tests.
_MAP_RT_DDL = """
CREATE EXTENSION IF NOT EXISTS pg_map;
SELECT map_type.create('int', 'text');
CREATE TABLE map_src (
    c_int int,
    c_map map_type.key_int_val_text
);
INSERT INTO map_src VALUES
    (1, '{"(1,a)","(2,b)","(3,c)"}'),
    (2, NULL),
    (3, '{}');
"""


def _map_rt_rows_for(conn, tbl):
    # Compare on the text rendering so map equality is exact and independent of
    # how psycopg2 maps the type back to Python.
    return run_query(f"SELECT c_int, c_map::text FROM {tbl} ORDER BY c_int", conn)


def test_json_map_serialization_divergence(superuser_conn, tmp_path):
    """pg_map maps: pg_lake's JSON round-trips, core's JSON does not.

    pg_lake (DuckDB) serializes a map column as a JSON object {"1":"a",...},
    which reads straight back into the map type. Core PostgreSQL serializes the
    same map as its underlying array of {"key","val"} objects
    ([{"key":1,"val":"a"},...]) -- a faithful but different shape that pg_lake's
    DuckDB MAP reader cannot ingest (it expects an object). So unlike the
    composite/array cases above, the producers are NOT interchangeable for maps;
    this pins both the lossless pg_lake round-trip and the known core divergence.

    A superuser connection is required: map_type.create() and the Postgres
    writer's COPY TO <file> both need elevated privileges.
    """
    _skip_pre_pg19(superuser_conn)

    lake_path = tmp_path / "map_pglake.json"
    core_path = tmp_path / "map_core.json"
    try:
        run_command(_MAP_RT_DDL, superuser_conn)
        before = _map_rt_rows_for(superuser_conn, "map_src")

        # pg_lake writer: object-shaped JSON that round-trips losslessly.
        run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", superuser_conn)
        run_command(
            f"COPY map_src TO '{lake_path}' WITH (format 'json')", superuser_conn
        )
        run_command("CREATE TABLE map_dst (LIKE map_src)", superuser_conn)
        run_command(
            f"COPY map_dst FROM '{lake_path}' WITH (format 'json')", superuser_conn
        )
        after = _map_rt_rows_for(superuser_conn, "map_dst")

        # core writer: array-of-{key,val} JSON.
        run_command("SET pg_lake_copy.json_copy_mode TO 'postgres'", superuser_conn)
        run_command(
            f"COPY map_src TO '{core_path}' WITH (format 'json')", superuser_conn
        )

        # Reading core's array-shaped map JSON back through pg_lake fails: the
        # DuckDB MAP reader expects an object, not an array.
        run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", superuser_conn)
        run_command("TRUNCATE map_dst", superuser_conn)
        core_read_err = run_command(
            f"COPY map_dst FROM '{core_path}' WITH (format 'json')",
            superuser_conn,
            raise_error=False,
        )
    finally:
        superuser_conn.rollback()

    # pg_lake's own write -> read is lossless (covers NULL and empty maps too).
    assert [dict(r) for r in before] == [dict(r) for r in after]

    # Pin the two serializations: pg_lake emits an object, core emits an array.
    lake_first = json.loads(lake_path.read_text().splitlines()[0])
    core_first = json.loads(core_path.read_text().splitlines()[0])
    assert isinstance(lake_first["c_map"], dict), lake_first
    assert isinstance(core_first["c_map"], list), core_first

    # Core's array-shaped map JSON is the known, non-round-trippable divergence.
    assert core_read_err is not None


@pytest.mark.parametrize(
    "case_id,expr", _ALL_TYPE_CASES, ids=[c[0] for c in _ALL_TYPE_CASES]
)
def test_json_roundtrip_per_type(pg_conn, case_id, expr, tmp_path):
    """Every parity case must survive a pg_lake JSON write -> pg_lake read.

    This is the baseline guarantee for JSON COPY: the on-disk text may differ
    from core's (see test_json_to_stdout_duckdb_differences), but reading the
    value straight back into the same PostgreSQL type must reproduce it exactly.
    CREATE TABLE AS captures the expression's own type so the destination column
    matches the source.
    """
    _skip_pre_pg19(pg_conn)

    path = tmp_path / "rt.json"
    try:
        # Force pg_lake for both write and read so this exercises pg_lake's own
        # JSON path (under the 'auto' default a local COPY TO json goes to
        # Postgres).
        run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", pg_conn)
        run_command(_STRUCT_SETUP, pg_conn)
        run_command(f"CREATE TABLE rtp_src AS SELECT {expr} AS c", pg_conn)
        before = run_query("SELECT c::text AS c FROM rtp_src", pg_conn)

        run_command(f"COPY rtp_src TO '{path}' WITH (format 'json')", pg_conn)
        run_command("CREATE TABLE rtp_dst (LIKE rtp_src)", pg_conn)
        run_command(f"COPY rtp_dst FROM '{path}' WITH (format 'json')", pg_conn)

        after = run_query("SELECT c::text AS c FROM rtp_dst", pg_conn)
    finally:
        pg_conn.rollback()

    assert [dict(r) for r in before] == [
        dict(r) for r in after
    ], f"{case_id}: before={before} after={after}"


def test_generated_column_handling(superuser_conn, tmp_path):
    """Generated columns are omitted by COPY TO under json_copy_mode=postgres.

    Core PostgreSQL excludes GENERATED columns from COPY <table> TO so the
    output round-trips through COPY FROM. With json_copy_mode=postgres pg_lake
    defers to core, so the generated column is omitted exactly as Postgres does
    -- this is the mode the upstream regression suite runs in, so we pin it.

    pg_lake's own (DuckDB) path historically *included* generated columns; that
    divergence is a separately tracked, known issue
    (https://github.com/Snowflake-Labs/pg_lake/issues/403) and is intentionally
    not asserted here so this test stays stable regardless of that fix's state.
    """
    _skip_pre_pg19(superuser_conn)

    try:
        run_command(
            """
            CREATE TABLE gen_tbl (a int, b int GENERATED ALWAYS AS (a * 2) STORED);
            INSERT INTO gen_tbl (a) VALUES (5);
            """,
            superuser_conn,
        )
        core = _copy_to_stdout_json(
            superuser_conn, "gen_tbl", tmp_path, "core.json", True
        )
    finally:
        superuser_conn.rollback()

    assert core == b'{"a":5}\n'


# force_array array framing: DuckDB indents each element with a TAB, core with a
# single SPACE; otherwise the bytes are identical. JSON-escaped values never
# contain a raw newline+tab, so rewriting "\n\t" -> "\n " normalizes only the
# framing and lets us assert per-row byte parity precisely.
def _normalize_array_indent(data):
    return data.replace(b"\n\t", b"\n ")


@pytest.mark.parametrize(
    "case_id,expr", _IDENTICAL_CASES, ids=[c[0] for c in _IDENTICAL_CASES]
)
def test_force_array_to_stdout_matches_core(superuser_conn, case_id, expr, tmp_path):
    """force_array maps to DuckDB ARRAY; output matches core modulo framing.

    pg_lake now accepts FORCE_ARRAY for JSON COPY TO and pushes it down to
    DuckDB's ARRAY writer. The per-row JSON is byte-identical to core's; the only
    difference is the array indentation character (DuckDB tab vs core space),
    which we normalize before comparing.
    """
    _skip_pre_pg19(superuser_conn)

    try:
        run_command(_STRUCT_SETUP, superuser_conn)
        source = f"(SELECT {expr} AS c)"
        lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "lake.json", False, force_array=True
        )
        core = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "core.json", True, force_array=True
        )
    finally:
        superuser_conn.rollback()

    assert lake.startswith(b"["), f"{case_id}: pg_lake not an array: {lake!r}"
    assert (
        _normalize_array_indent(lake) == core
    ), f"{case_id}: pg_lake={lake!r} core={core!r}"


def test_force_array_multirow_matches_core(superuser_conn, tmp_path):
    """A multi-row ordered source with force_array yields the same JSON array as
    core (the per-row objects are byte-identical; only the array framing -- comma
    placement and indent char -- differs), and without force_array it is NDJSON.

    For multiple rows DuckDB writes `},\\n\\t{` between elements while core writes
    `}\\n,{`, so the framing differs at the byte level; the documents (parsed
    JSON) are equal and ordered, which is what matters.
    """
    _skip_pre_pg19(superuser_conn)

    source = "(SELECT i FROM generate_series(1,3) i ORDER BY i)"

    try:
        array_lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "lake.json", False, force_array=True
        )
        array_core = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "core.json", True, force_array=True
        )
        ndjson_lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "nd.json", False, force_array=False
        )
    finally:
        superuser_conn.rollback()

    assert array_lake.startswith(b"[") and array_core.startswith(b"[")
    parsed_lake = json.loads(array_lake)
    assert parsed_lake == json.loads(array_core)
    assert parsed_lake == [{"i": 1}, {"i": 2}, {"i": 3}]

    # Without force_array the same source is newline-delimited JSON, not an array.
    assert not ndjson_lake.lstrip().startswith(b"[")
    nd_lines = [ln for ln in ndjson_lake.splitlines() if ln.strip()]
    assert len(nd_lines) == 3
    for ln in nd_lines:
        assert isinstance(json.loads(ln), dict)


def test_copy_from_json_stdin_behavior(pg_conn, tmp_path):
    """COPY FROM (format json): pg_lake reads it, Postgres rejects it.

    Reading JSON is a pg_lake feature Postgres lacks (Postgres: "COPY FORMAT
    JSON is not supported for COPY FROM"). Under the 'auto' default pg_lake
    serves it (Postgres can't, so it never gets precedence here); with
    json_copy_mode=postgres, Postgres's rejection applies, which is what the
    PostgreSQL regression suite expects.
    """
    _skip_pre_pg19(pg_conn)

    json_path = tmp_path / "rows.json"
    with open(json_path, "w") as f:
        f.write('{"x":1}\n{"x":2}\n')

    run_command("CREATE TABLE cf_tbl (x int)", pg_conn)

    # auto default: pg_lake reads the JSON (Postgres cannot)
    run_command("SET pg_lake_copy.json_copy_mode TO 'auto'", pg_conn)
    error = copy_from_file(
        "COPY cf_tbl FROM STDIN WITH (format 'json')",
        json_path,
        pg_conn,
        raise_error=False,
    )
    assert error is None
    assert run_query("SELECT count(*) AS c FROM cf_tbl", pg_conn)[0]["c"] == 2

    # postgres mode: Postgres handles COPY FROM and rejects JSON
    run_command("SET pg_lake_copy.json_copy_mode TO 'postgres'", pg_conn)
    error = copy_from_file(
        "COPY cf_tbl FROM STDIN WITH (format 'json')",
        json_path,
        pg_conn,
        raise_error=False,
    )
    assert error.startswith("ERROR:  COPY FORMAT JSON is not supported for COPY FROM")

    pg_conn.rollback()


# JSON object keys are derived from the source's column names. pg_lake produces
# them via DuckDB (table -> CSV -> DuckDB), so its key derivation for unnamed /
# VALUES / duplicate columns must match core's, otherwise the JSON keys differ.
_COLUMN_NAMING_CASES = [
    # VALUES list with no column aliases -> column1, column2, ...
    ("(VALUES (1, 'a'))", b'{"column1":1,"column2":"a"}\n'),
    # a single unnamed SELECT expression -> ?column?
    ("(SELECT 1)", b'{"?column?":1}\n'),
    # mix of named and unnamed expressions
    ("(SELECT 1 AS x, 2)", b'{"x":1,"?column?":2}\n'),
]


@pytest.mark.parametrize(
    "source,expected", _COLUMN_NAMING_CASES, ids=[c[0] for c in _COLUMN_NAMING_CASES]
)
def test_json_column_naming_matches_core(superuser_conn, source, expected, tmp_path):
    """JSON object keys for unnamed/VALUES/duplicate columns must match core."""
    _skip_pre_pg19(superuser_conn)

    try:
        lake = _copy_to_stdout_json(
            superuser_conn, source, tmp_path, "lake.json", False
        )
        core = _copy_to_stdout_json(superuser_conn, source, tmp_path, "core.json", True)
    finally:
        superuser_conn.rollback()

    assert lake == expected, f"pg_lake key derivation changed: {lake!r}"
    assert lake == core, f"pg_lake={lake!r} core={core!r}"


def test_json_bareword_format(superuser_conn, tmp_path):
    """The format may be written as a bareword (format json) or a string
    (format 'json'); pg_lake must accept both and produce identical output."""
    _skip_pre_pg19(superuser_conn)

    path_bare = tmp_path / "bare.json"
    path_str = tmp_path / "str.json"
    run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", superuser_conn)
    try:
        copy_to_file(
            "COPY (SELECT 1 AS c) TO STDOUT WITH (format json)",
            path_bare,
            superuser_conn,
        )
        copy_to_file(
            "COPY (SELECT 1 AS c) TO STDOUT WITH (format 'json')",
            path_str,
            superuser_conn,
        )
    finally:
        superuser_conn.rollback()

    assert path_bare.read_bytes() == b'{"c":1}\n'
    assert path_bare.read_bytes() == path_str.read_bytes()


# CSV/text-only COPY options that Postgres rejects in JSON mode. pg_lake serves
# JSON COPY itself (json_copy_mode=pglake), so it must likewise refuse them --
# never silently ignore an option that would change the output. With
# json_copy_mode=postgres, Postgres's own rejection applies. We only require that
# *both* sides reject; the wording differs (pg_lake: "invalid option ...";
# Postgres: "cannot specify ... in JSON mode" / "requires CSV mode").
_INCOMPATIBLE_JSON_OPTIONS = [
    ("header", "header true"),
    ("delimiter", "delimiter ','"),
    ("null", "null 'X'"),
    ("quote", "quote '\"'"),
    ("escape", "escape '\\'"),
    ("force_quote", "force_quote (c)"),
]


@pytest.mark.parametrize(
    "opt_name,opt_sql",
    _INCOMPATIBLE_JSON_OPTIONS,
    ids=[c[0] for c in _INCOMPATIBLE_JSON_OPTIONS],
)
def test_json_incompatible_options_rejected(
    superuser_conn, opt_name, opt_sql, tmp_path
):
    """A CSV-only option combined with format json must be rejected by both
    pg_lake (json_copy_mode=pglake) and Postgres (json_copy_mode=postgres), not
    silently accepted."""
    _skip_pre_pg19(superuser_conn)

    sql = f"COPY (SELECT 1 AS c) TO STDOUT WITH (format 'json', {opt_sql})"

    run_command("SET pg_lake_copy.json_copy_mode TO 'pglake'", superuser_conn)
    lake_err = copy_to_file(
        sql, tmp_path / "lake.json", superuser_conn, raise_error=False
    )
    superuser_conn.rollback()
    assert lake_err is not None, f"pg_lake silently accepted {opt_name} with json"
    assert opt_name in lake_err.lower(), f"unexpected pg_lake error: {lake_err!r}"

    run_command("SET pg_lake_copy.json_copy_mode TO 'postgres'", superuser_conn)
    core_err = copy_to_file(
        sql, tmp_path / "core.json", superuser_conn, raise_error=False
    )
    superuser_conn.rollback()
    assert core_err is not None, f"Postgres silently accepted {opt_name} with json"


def test_force_array_rejected_for_from_and_csv(superuser_conn, tmp_path):
    """force_array only applies to JSON COPY TO.

    It must be rejected for COPY FROM and for non-JSON formats by both pg_lake
    (json_copy_mode=pglake) and Postgres (json_copy_mode=postgres), mirroring
    Postgres's own restrictions (FORCE_ARRAY can only be used with JSON mode and
    not with COPY FROM). Each iteration rolls back since the failed statement
    aborts the transaction.
    """
    _skip_pre_pg19(superuser_conn)

    # (format csv): FORCE_ARRAY requires JSON mode -> rejected either way
    csv_copy = "COPY (SELECT 1 AS c) TO STDOUT WITH (format 'csv', force_array true)"
    for mode in ("pglake", "postgres"):
        run_command(
            f"SET pg_lake_copy.json_copy_mode TO '{mode}'",
            superuser_conn,
        )
        err = copy_to_file(
            csv_copy, tmp_path / "csv.out", superuser_conn, raise_error=False
        )
        superuser_conn.rollback()
        assert err is not None, f"force_array+csv accepted (mode={mode})"

    # COPY FROM (format json): FORCE_ARRAY cannot be combined with COPY FROM.
    # Commit the table so it survives the per-iteration rollback.
    json_path = tmp_path / "rows.json"
    with open(json_path, "w") as f:
        f.write('{"x":1}\n')

    run_command("CREATE TABLE fa_from (x int)", superuser_conn)
    superuser_conn.commit()
    try:
        for mode in ("pglake", "postgres"):
            run_command(
                f"SET pg_lake_copy.json_copy_mode TO '{mode}'",
                superuser_conn,
            )
            err = copy_from_file(
                "COPY fa_from FROM STDIN WITH (format 'json', force_array true)",
                json_path,
                superuser_conn,
                raise_error=False,
            )
            superuser_conn.rollback()
            assert err is not None, f"force_array+COPY FROM accepted (mode={mode})"
    finally:
        run_command("DROP TABLE fa_from", superuser_conn)
        superuser_conn.commit()


def test_lake_target_ignores_json_copy_mode(pg_conn):
    """Lake targets are always served by pg_lake, even in json_copy_mode=postgres.

    A supported URL is claimed by pg_lake before the JSON deferral is reached,
    so an option pg_lake does not support still produces pg_lake's own error
    (Postgres cannot write to s3 anyway).
    """
    url = f"s3://{TEST_BUCKET}/test_lake_knob/data.json"
    run_command("CREATE TABLE lk_s3 (a int); INSERT INTO lk_s3 VALUES (1)", pg_conn)
    run_command("SET pg_lake_copy.json_copy_mode TO 'postgres'", pg_conn)

    # quote is a CSV-only option pg_lake rejects for JSON; reaching pg_lake's
    # error (not Postgres's) proves the lake target stayed on pg_lake's path.
    error = run_command(
        f"COPY lk_s3 TO '{url}' WITH (format 'json', quote '|')",
        pg_conn,
        raise_error=False,
    )
    assert 'pg_lake_copy: invalid option "quote"' in error

    pg_conn.rollback()

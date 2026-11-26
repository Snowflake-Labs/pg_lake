"""Edge cases for proving that every row of a data file matches a filter.

A DELETE removes a file without scanning it when the file's column stats prove
that every row in it matches the WHERE clause. Min/max only describe the
non-NULL values, so on a nullable column the file also has to record a null
count of zero for the column.

The tests here keep a heap copy of every Iceberg table, tagged with the data
file each row went to, and run each DML on both. That gives two independent
checks for every filter:

- the result of the DML has to match the heap, so a wrongly skipped file shows
  up as a missing or extra row;
- EXPLAIN may only skip files whose rows all satisfy the filter on the heap,
  and for the filters the stats can prove, it has to skip all of them.
"""

import pytest
from utils_pytest import *

SCHEMA = "null_count_full_match"

COLUMNS = [
    ("id", "int"),
    ("a", "int"),
    ("b", "int"),
    ('"Mixed Case"', "int"),
    ('"a b"', "text"),
    ("t", "text"),
    ("tc", 'text COLLATE "C"'),
    ("lt", "text"),
    ("n", "numeric(12,3)"),
    ("d", "date"),
    ("ts", "timestamptz"),
    ("f", "float8"),
    ("r", "real"),
    ("bo", "bool"),
    ("ch", "char(3)"),
    ("big", "bigint"),
    ("c", f"{SCHEMA}.pt"),
    ("arr", "int[]"),
    ("m", "map_type.key_text_val_int"),
    ("u", "uuid"),
    ("j", "jsonb"),
]

COLUMN_LIST = ", ".join(name for name, _ in COLUMNS)

# value of each column for row i, before the per-file NULL rules
BASE_VALUES = {
    "id": "i",
    "a": "i",
    "b": "i * 10",
    '"Mixed Case"': "-i",
    '"a b"': "'k' || lpad(i::text, 3, '0')",
    "t": "chr(97 + i % 26) || i::text",
    "tc": "'k' || lpad(i::text, 3, '0')",
    "lt": "repeat('x', 40) || lpad(i::text, 3, '0')",
    "n": "i * 1.5",
    "d": "'2020-01-01'::date + i",
    "ts": "'2026-01-01 00:00+00'::timestamptz + i * interval '1 hour'",
    "f": "i::float8 / 3",
    "r": "i::real",
    "bo": "i % 2 = 0",
    "ch": "lpad(i::text, 3, '0')",
    "big": "i * 1000000000::bigint",
    "c": f"ROW(i, 'v' || i::text)::{SCHEMA}.pt",
    "arr": "ARRAY[i, i + 1]",
    "m": "ARRAY[('k' || i::text, i)]::map_type.key_text_val_int",
    "u": "md5(i::text)::uuid",
    "j": "jsonb_build_object('i', i)",
}

# One INSERT per data file: (file number, first id, last id, how it is
# written, {column: SQL expression overriding the base value}).
#
# pushdown:   INSERT..SELECT over generate_series, which DuckDB writes
# row_by_row: INSERT..SELECT from the heap copy, which Postgres evaluates
# copy:       COPY FROM a CSV file
FILES = [
    # nothing is NULL
    (1, 1, 10, "pushdown", {}),
    # a and "a b" have one NULL each
    (
        2,
        11,
        20,
        "row_by_row",
        {
            "a": "CASE WHEN i = 15 THEN NULL ELSE i END",
            '"a b"': "CASE WHEN i = 12 THEN NULL ELSE 'k' || lpad(i::text, 3, '0') END",
        },
    ),
    # b is NULL in every row, so it has no bounds at all
    (3, 21, 30, "pushdown", {"b": "NULL::int"}),
    # NULLs only in nested columns, a NaN and an Infinity in the floats
    (
        4,
        31,
        40,
        "copy",
        {
            "c": f"CASE WHEN i % 2 = 0 THEN NULL ELSE ROW(i, 'v' || i::text)::{SCHEMA}.pt END",
            "arr": "CASE WHEN i = 33 THEN NULL ELSE ARRAY[i, NULL] END",
            "m": "CASE WHEN i = 34 THEN NULL ELSE ARRAY[('k' || i::text, i)]::map_type.key_text_val_int END",
            "f": "CASE WHEN i = 35 THEN 'NaN'::float8 ELSE i::float8 / 3 END",
            "r": "CASE WHEN i = 36 THEN 'Infinity'::real ELSE i::real END",
        },
    ),
    # nothing is NULL, written by Postgres
    (5, 41, 50, "row_by_row", {}),
    # everything but id is NULL
    (
        6,
        51,
        60,
        "pushdown",
        {name: f"NULL::{type_}" for name, type_ in COLUMNS if name != "id"},
    ),
    # one NULL timestamp and one NULL bool
    (
        7,
        61,
        70,
        "copy",
        {
            "ts": "CASE WHEN i = 65 THEN NULL ELSE '2026-01-01 00:00+00'::timestamptz + i * interval '1 hour' END",
            "bo": "CASE WHEN i = 66 THEN NULL ELSE i % 2 = 0 END",
        },
    ),
]


def file_select(first, last, overrides):
    values = [
        f"{overrides.get(name, BASE_VALUES[name])} AS {name}" for name, _ in COLUMNS
    ]
    return (
        f"SELECT {', '.join(values)} "
        f"FROM generate_series({first}, {last}) i ORDER BY 1"
    )


# (WHERE clause, whether the stats can prove every fully matching file)
#
# Only filters marked True have to skip every file whose rows all match. The
# others may skip fewer, but never more.
FILTERS = [
    # single column, no NULLs in some files
    ("id <= 10", True),
    ("id > 0", True),
    ("id BETWEEN 11 AND 20", True),
    ("id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)", False),
    ("id <> 1000", False),
    ("NOT (id > 10)", True),
    ("id IS NOT NULL", True),
    # a has a NULL in file 2 and is NULL everywhere in file 6
    ("a > 0", True),
    ("a >= 1 AND a <= 10", True),
    ("a >= 11 AND a <= 20", False),
    ("a IS NOT NULL", True),
    ("a IS NULL", False),
    ("a < 0", False),
    ("coalesce(a, 0) >= 0", False),
    # b is NULL in all of files 3 and 6
    ("b > 0", True),
    ("b IS NULL", False),
    # several columns
    ("a > 0 AND b > 0", True),
    ("a > 0 OR b > 0", True),
    ("a > 0 AND id <= 20", True),
    ("(a > 0 AND id <= 20) OR id > 60", True),
    ("a > 0 AND b IS NULL", False),
    ("a > 0 AND ts < '2027-01-01'", True),
    ("a > 0 OR a IS NULL", False),
    ('"Mixed Case" < 0', True),
    ('"Mixed Case" < 0 AND a > 0', True),
    ("\"a b\" >= 'k'", False),
    # bounds only order like the column's collation if it orders by bytes
    ('"a b" COLLATE "C" >= \'k\'', False),
    ("t COLLATE \"C\" >= 'a'", False),
    ("lt COLLATE \"C\" >= 'x'", False),
    ("tc >= 'k'", True),
    ("tc < 'k045'", True),
    ("tc >= 'k001' AND tc <= 'k010'", True),
    ("tc LIKE 'k%'", False),
    ("n > 0", True),
    ("n >= 1.5 AND n <= 15", True),
    ("d >= '2020-01-01'", True),
    ("d < '2020-01-15' AND a IS NOT NULL", True),
    ("ts < '2026-01-03'", True),
    ("ts >= '2026-01-01' AND bo IS NOT NULL", False),
    # NaN > 0 holds in Postgres, but a file with a NaN has no float bounds
    ("f > 0", False),
    ("f >= 0 AND f < 100", False),
    ("f < 100", True),
    ("r > 0", True),
    ("r < 'Infinity'", True),
    ("bo", False),
    ("bo OR NOT bo", False),
    ("bo IS NOT NULL", False),
    ("ch >= '000'", False),
    ("big > 0", True),
    # nested and unsupported types never have stats
    ("(c).x > 0", False),
    ("c IS NOT NULL", False),
    ("arr IS NOT NULL", False),
    ("cardinality(arr) > 0", False),
    ("m IS NOT NULL", False),
    ("u IS NOT NULL", False),
    ("j IS NOT NULL", False),
    ("a > 0 AND (c).x > 0", False),
    ("a > 0 OR (c).x > 0", False),
    ("tc >= 'k' OR (c).x > 0", True),
    # constant filters
    ("true", False),
    ("false", False),
]

MODES = ["pushdown", "no_pushdown", "local_filter"]


@pytest.fixture(scope="module")
def tables(s3, pg_conn, superuser_conn, extension):
    create_map_type("text", "int")

    run_command(
        f"SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}'",
        pg_conn,
    )

    column_defs = ", ".join(f"{name} {type_}" for name, type_ in COLUMNS)
    run_command(
        f"""
        CREATE SCHEMA {SCHEMA};
        CREATE TYPE {SCHEMA}.pt AS (x int, y text);
        CREATE TABLE {SCHEMA}.ice ({column_defs}) USING iceberg
        WITH (autovacuum_enabled = 'false');
        CREATE TABLE {SCHEMA}.heap (file_no int not null, {column_defs});

        -- a filter Postgres has to evaluate, which DuckDB cannot
        CREATE FUNCTION {SCHEMA}.keep(int) RETURNS bool
        LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN true; END $$;
        """,
        pg_conn,
    )

    for file_no, first, last, how, overrides in FILES:
        select = file_select(first, last, overrides)
        run_command(
            f"INSERT INTO {SCHEMA}.heap SELECT {file_no}, * FROM ({select}) s",
            pg_conn,
        )

        from_heap = (
            f"SELECT {COLUMN_LIST} FROM {SCHEMA}.heap "
            f"WHERE file_no = {file_no} ORDER BY id"
        )
        if how == "pushdown":
            run_command(f"INSERT INTO {SCHEMA}.ice {select}", pg_conn)
        elif how == "row_by_row":
            run_command(f"INSERT INTO {SCHEMA}.ice {from_heap}", pg_conn)
        elif how == "copy":
            # CSV, since COPY FROM Parquet cannot load the map column
            url = f"s3://{TEST_BUCKET}/{SCHEMA}/file_{file_no}.csv"
            run_command(f"COPY ({select}) TO '{url}' WITH (format 'csv')", pg_conn)
            run_command(f"COPY {SCHEMA}.ice FROM '{url}' WITH (format 'csv')", pg_conn)

    pg_conn.commit()

    # one data file per INSERT
    files = run_query(
        f"SELECT count(*) FROM lake_table.files "
        f"WHERE table_name = '{SCHEMA}.ice'::regclass",
        superuser_conn,
    )[0][0]
    assert files == len(FILES)

    yield

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    run_command("RESET pg_lake_iceberg.default_location_prefix", pg_conn)
    pg_conn.commit()


def explain_files(pg_conn, statement):
    """(data files scanned, data files skipped) of a DML."""
    plan = run_query(f"EXPLAIN (verbose, format json) {statement}", pg_conn)
    used = fetch_data_files_used(plan)
    skipped = fetch_data_files_skipped(plan)
    return int(used or 0), int(skipped or 0)


def fully_matching_files(pg_conn, where):
    """Number of files whose rows all satisfy the filter, from the heap copy."""
    return run_query(
        f"""
        SELECT count(*) FROM (
            SELECT file_no FROM {SCHEMA}.heap
            GROUP BY file_no
            HAVING bool_and(coalesce(({where}), false))
        ) f
        """,
        pg_conn,
    )[0][0]


def table_rows(pg_conn, table):
    return run_query(
        f"SELECT row({COLUMN_LIST})::text FROM {SCHEMA}.{table} ORDER BY id",
        pg_conn,
    )


def set_mode(pg_conn, mode):
    pushdown = "off" if mode == "no_pushdown" else "on"
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {pushdown}", pg_conn
    )


def ice_filter(where, mode):
    if mode == "local_filter":
        return f"({where}) AND {SCHEMA}.keep(id)"
    return where


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("where,provable", FILTERS, ids=[f for f, _ in FILTERS])
def test_delete_full_match(pg_conn, tables, mode, where, provable):
    try:
        set_mode(pg_conn, mode)
        delete = f"DELETE FROM {SCHEMA}.ice WHERE {ice_filter(where, mode)}"

        used, skipped = explain_files(pg_conn, delete)
        matching = fully_matching_files(pg_conn, where)

        assert skipped <= matching
        if mode == "local_filter":
            # the stats cannot say anything about keep(id)
            assert skipped == 0
        elif provable:
            assert skipped == matching
            assert matching > 0, "filter does not exercise a full match"

        run_command(delete, pg_conn)
        run_command(f"DELETE FROM {SCHEMA}.heap WHERE {where}", pg_conn)
        assert table_rows(pg_conn, "ice") == table_rows(pg_conn, "heap")
    finally:
        pg_conn.rollback()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("where,provable", FILTERS, ids=[f for f, _ in FILTERS])
def test_update_full_match(pg_conn, tables, mode, where, provable):
    try:
        set_mode(pg_conn, mode)
        assignments = 'b = coalesce(b, 0) + 1, "a b" = NULL, c = NULL'
        run_command(
            f"UPDATE {SCHEMA}.ice SET {assignments} WHERE {ice_filter(where, mode)}",
            pg_conn,
        )
        run_command(f"UPDATE {SCHEMA}.heap SET {assignments} WHERE {where}", pg_conn)
        assert table_rows(pg_conn, "ice") == table_rows(pg_conn, "heap")
    finally:
        pg_conn.rollback()


@pytest.mark.parametrize("mode", ["pushdown", "no_pushdown"])
def test_delete_full_match_with_parameters(pg_conn, tables, mode):
    """
    A generic plan only sees the parameter values at execution time, which
    must not make it skip files the values do not fully match.
    """
    try:
        set_mode(pg_conn, mode)
        run_command("SET LOCAL plan_cache_mode TO force_generic_plan", pg_conn)
        run_command(
            f"PREPARE expire_{mode}(int, timestamptz) AS "
            f"DELETE FROM {SCHEMA}.ice WHERE a > $1 AND ts < $2",
            pg_conn,
        )
        run_command(
            f"EXECUTE expire_{mode}(0, '2026-01-02 00:00+00')",
            pg_conn,
        )
        run_command(
            f"DELETE FROM {SCHEMA}.heap WHERE a > 0 AND ts < '2026-01-02 00:00+00'",
            pg_conn,
        )
        assert table_rows(pg_conn, "ice") == table_rows(pg_conn, "heap")
    finally:
        pg_conn.rollback()
        run_command(f"DEALLOCATE ALL", pg_conn)
        pg_conn.commit()


def test_delete_full_match_twice_in_transaction(pg_conn, tables):
    """
    Files written and position-deleted earlier in the same transaction are
    handled correctly by a later full match.
    """
    try:
        run_command(
            f"""
            INSERT INTO {SCHEMA}.ice {file_select(101, 110, {})};
            INSERT INTO {SCHEMA}.heap SELECT 8, * FROM ({file_select(101, 110, {})}) s;
            DELETE FROM {SCHEMA}.ice WHERE id = 105;
            DELETE FROM {SCHEMA}.heap WHERE id = 105;
            """,
            pg_conn,
        )

        delete = f"DELETE FROM {SCHEMA}.ice WHERE a > 100"
        # the new file is a full match; file 6, whose a is all NULL, has no
        # bounds to refute the filter with and is scanned
        assert explain_files(pg_conn, delete) == (1, 1)

        run_command(delete, pg_conn)
        run_command(f"DELETE FROM {SCHEMA}.heap WHERE a > 100", pg_conn)
        assert table_rows(pg_conn, "ice") == table_rows(pg_conn, "heap")
    finally:
        pg_conn.rollback()


def run_in_schema(pg_conn, schema, sql):
    run_command(
        f"""
        SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}';
        CREATE SCHEMA {schema};
        SET search_path TO {schema};
        {sql}
        """,
        pg_conn,
    )
    pg_conn.commit()


def drop_schema(pg_conn, schema):
    pg_conn.rollback()
    run_command(
        f"DROP SCHEMA {schema} CASCADE; RESET search_path; "
        "RESET pg_lake_iceberg.default_location_prefix",
        pg_conn,
    )
    pg_conn.commit()


def test_full_match_after_schema_changes(pg_conn, superuser_conn, extension, s3):
    """
    Files written before a column existed have no stats for it, and read the
    column as NULL or as its default, so they never fully match on it. A
    renamed column keeps its field id and its stats.
    """
    schema = "null_count_schema_changes"
    try:
        run_in_schema(
            pg_conn,
            schema,
            """
            CREATE TABLE t (id int, x int) USING iceberg
            WITH (autovacuum_enabled = 'false');
            INSERT INTO t SELECT i, i FROM generate_series(1, 10) i;

            ALTER TABLE t ADD COLUMN y int;
            ALTER TABLE t ADD COLUMN z int DEFAULT 5;
            INSERT INTO t SELECT i, i, i, i FROM generate_series(11, 20) i;
            """,
        )

        # the old file reads y as NULL, so only the new file matches, but the
        # old one has no stats for y to refute the filter with either
        assert explain_files(pg_conn, "DELETE FROM t WHERE y > 0") == (1, 1)
        # the old file reads z as 5 but has no stats for it, so it is scanned
        assert explain_files(pg_conn, "DELETE FROM t WHERE z > 0") == (1, 1)

        run_command("DELETE FROM t WHERE y > 0", pg_conn)
        assert run_query("SELECT count(*), count(y) FROM t", pg_conn)[0] == [10, 0]
        pg_conn.rollback()

        run_command("DELETE FROM t WHERE z > 0", pg_conn)
        assert run_query("SELECT count(*) FROM t", pg_conn)[0][0] == 0
        pg_conn.rollback()

        # renaming keeps the field id, and with it the null counts
        run_command("ALTER TABLE t RENAME COLUMN x TO renamed", pg_conn)
        pg_conn.commit()
        assert explain_files(pg_conn, "DELETE FROM t WHERE renamed > 0") == (0, 2)

        # dropping and re-adding a column gives it a new field id without stats
        # in the old files, which read it as NULL
        run_command(
            """
            ALTER TABLE t DROP COLUMN renamed;
            ALTER TABLE t ADD COLUMN renamed int;
            INSERT INTO t (id, renamed) VALUES (21, 1);
            """,
            pg_conn,
        )
        pg_conn.commit()
        assert explain_files(pg_conn, "DELETE FROM t WHERE renamed > 0") == (2, 1)

        run_command("DELETE FROM t WHERE renamed > 0", pg_conn)
        assert run_query("SELECT count(*) FROM t", pg_conn)[0][0] == 20
    finally:
        drop_schema(pg_conn, schema)


def test_full_match_case_colliding_columns(pg_conn, superuser_conn, extension, s3):
    """
    Columns whose names only differ in case get their own null counts, even
    though DuckDB renames one of them in the stats it returns.
    """
    schema = "null_count_case_collision"
    try:
        run_in_schema(
            pg_conn,
            schema,
            """
            CREATE TABLE t (id int, "x" int, "X" int) USING iceberg
            WITH (autovacuum_enabled = 'false');
            -- x has a NULL, X does not
            INSERT INTO t VALUES (1, 1, 1), (2, NULL, 2);
            """,
        )

        counts = run_query(
            """
            SELECT a.attname, s.null_count
            FROM lake_table.data_file_column_stats s
            JOIN lake_table.field_id_mappings m USING (table_name, field_id)
            JOIN pg_attribute a ON a.attrelid = m.table_name AND a.attnum = m.pg_attnum
            WHERE s.table_name = 'null_count_case_collision.t'::regclass
            ORDER BY a.attnum
            """,
            superuser_conn,
        )
        assert counts == [["id", 0], ["x", 1], ["X", 0]]

        assert explain_files(pg_conn, 'DELETE FROM t WHERE "X" > 0') == (0, 1)
        assert explain_files(pg_conn, 'DELETE FROM t WHERE "x" > 0') == (1, 0)

        run_command('DELETE FROM t WHERE "x" > 0', pg_conn)
        assert run_query("SELECT id FROM t", pg_conn) == [[2]]
    finally:
        superuser_conn.rollback()
        drop_schema(pg_conn, schema)


def test_full_match_partitioned(pg_conn, extension, s3):
    """
    Partition bounds and null counts combine: a nullable partition column
    that is NULL in one partition, and a nullable non-partition column.
    """
    schema = "null_count_partitioned"
    try:
        run_in_schema(
            pg_conn,
            schema,
            """
            CREATE TABLE t (id int, day date, v int) USING iceberg
            WITH (autovacuum_enabled = 'false', partition_by = 'day');
            INSERT INTO t VALUES
                (1, '2026-01-01', 1), (2, '2026-01-01', 2),
                (3, '2026-01-02', 3), (4, '2026-01-02', NULL),
                (5, NULL, 5), (6, NULL, 6);
            """,
        )

        # the 2026-01-01 partition, whose v has no NULLs
        assert explain_files(
            pg_conn, "DELETE FROM t WHERE day < '2026-01-02' AND v > 0"
        ) == (0, 1)
        # the 2026-01-02 partition has a NULL v
        assert explain_files(pg_conn, "DELETE FROM t WHERE v > 0") == (1, 2)

        run_command("DELETE FROM t WHERE v > 0", pg_conn)
        assert run_query("SELECT id FROM t", pg_conn) == [[4]]
    finally:
        drop_schema(pg_conn, schema)


def test_full_match_many_files_expiration(pg_conn, extension, s3):
    """
    The case that made DELETE expensive: expiring the oldest of many files on
    a nullable timestamp removes them all without a scan.
    """
    schema = "null_count_expiration"
    try:
        inserts = "\n".join(
            f"""
            INSERT INTO events
            SELECT '2026-01-01 00:00+00'::timestamptz + interval '1 day' * {day}
                       + i * interval '1 second',
                   CASE WHEN i % 7 = 0 THEN NULL ELSE i END
            FROM generate_series(0, 999) i;
            """
            for day in range(20)
        )
        run_in_schema(
            pg_conn,
            schema,
            f"""
            CREATE TABLE events (ts timestamptz, payload int) USING iceberg
            WITH (autovacuum_enabled = 'false');
            {inserts}
            """,
        )

        expire = "DELETE FROM events WHERE ts < '2026-01-16'"
        assert explain_files(pg_conn, expire) == (0, 15)

        run_command(expire, pg_conn)
        pg_conn.commit()
        assert run_query(
            "SELECT count(*), min(ts) >= '2026-01-16' FROM events", pg_conn
        )[0] == [5000, True]
    finally:
        drop_schema(pg_conn, schema)


def test_full_match_without_column_stats(pg_conn, extension, s3):
    """With column_stats_mode = 'none' there is nothing to prove a full match."""
    schema = "null_count_no_stats"
    try:
        run_in_schema(
            pg_conn,
            schema,
            """
            CREATE TABLE t (id int, x int) USING iceberg
            WITH (autovacuum_enabled = 'false', column_stats_mode = 'none');
            INSERT INTO t VALUES (1, 1), (2, 2);
            """,
        )

        assert explain_files(pg_conn, "DELETE FROM t WHERE x > 0") == (1, 0)
        run_command("DELETE FROM t WHERE x > 0", pg_conn)
        assert run_query("SELECT count(*) FROM t", pg_conn)[0][0] == 0
    finally:
        drop_schema(pg_conn, schema)

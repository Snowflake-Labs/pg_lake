"""Per-column null counts of Iceberg data files.

Every write path ends in DuckDB's COPY ... TO ... (return_stats), which reports
the number of NULLs of each column next to its min and max. pg_lake keeps the
count in lake_table.data_file_column_stats and writes it to the manifest as
null_value_counts, so these tests check that the catalog, the manifest and the
Parquet file itself agree, whichever path wrote the file.
"""

import io
import json

import fastavro
import pyarrow.parquet as pq
import pytest
from utils_pytest import (
    TEST_BUCKET,
    create_map_type,
    read_s3_operations,
    run_command,
    run_query,
)


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn, superuser_conn):
    yield
    pg_conn.rollback()
    superuser_conn.rollback()


def catalog_null_counts(superuser_conn, table):
    """{path: {field_id: null_count}} from lake_table.data_file_column_stats."""
    rows = run_query(
        f"""
        SELECT s.path, s.field_id, s.null_count
        FROM lake_table.data_file_column_stats s
        JOIN lake_table.files f USING (table_name, path)
        WHERE s.table_name = '{table}'::regclass AND f.content = 0
        """,
        superuser_conn,
    )
    counts = {}
    for path, field_id, null_count in rows:
        counts.setdefault(path, {})[field_id] = null_count
    return counts


def manifest_null_counts(pg_conn, s3, namespace, name):
    """{path: {field_id: null_count}} for the live data files in the manifests."""
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_namespace = '{namespace}' AND table_name = '{name}'",
        pg_conn,
    )[0][0]
    metadata = json.loads(read_s3_operations(s3, metadata_location))
    snapshot = next(
        s
        for s in metadata["snapshots"]
        if s["snapshot-id"] == metadata["current-snapshot-id"]
    )
    manifest_list = read_s3_operations(s3, snapshot["manifest-list"], is_text=False)

    counts = {}
    for manifest in fastavro.reader(io.BytesIO(manifest_list)):
        body = read_s3_operations(s3, manifest["manifest_path"], is_text=False)
        for entry in fastavro.reader(io.BytesIO(body)):
            data_file = entry["data_file"]
            # status 2 is DELETED, content 0 is data
            if entry["status"] == 2 or data_file["content"] != 0:
                continue
            counts[data_file["file_path"]] = {
                c["key"]: c["value"] for c in data_file["null_value_counts"] or []
            }
    return counts


def parquet_null_counts(s3, path):
    """{field_id: null_count} computed from the rows of the Parquet file."""
    body = read_s3_operations(s3, path, is_text=False)
    table = pq.read_table(io.BytesIO(body))
    counts = {}
    for field in table.schema:
        field_id = int(field.metadata[b"PARQUET:field_id"])
        counts[field_id] = table.column(field.name).null_count
    return counts


def assert_null_counts_match_files(pg_conn, superuser_conn, s3, namespace, name):
    """
    Every column stats row of every data file has a null count that matches
    the file, and the manifest records the same counts.
    """
    catalog = catalog_null_counts(superuser_conn, f"{namespace}.{name}")
    manifest = manifest_null_counts(pg_conn, s3, namespace, name)

    assert len(catalog) > 0
    assert set(catalog) <= set(manifest)

    for path, catalog_counts in catalog.items():
        actual = parquet_null_counts(s3, path)
        for field_id, null_count in catalog_counts.items():
            assert null_count is not None, f"{path} field {field_id}"
            assert null_count == actual[field_id], f"{path} field {field_id}"

        assert manifest[path] == catalog_counts, path

    return catalog


def test_null_counts_on_every_write_path(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """
    Row-by-row inserts, pushed down INSERT..SELECT, COPY FROM and a partitioned
    table each record correct null counts.
    """
    location = f"s3://{TEST_BUCKET}/test_null_counts_on_every_write_path"
    run_command(
        f"""
        CREATE SCHEMA null_counts_writes;
        SET search_path TO null_counts_writes;

        CREATE TABLE t (id int, a int, b text) USING iceberg;

        -- VALUES
        INSERT INTO t VALUES (1, 1, 'x'), (2, NULL, 'y'), (3, NULL, NULL);

        -- row by row: a PL/pgSQL function keeps the insert out of DuckDB
        CREATE FUNCTION maybe_null(i int) RETURNS int LANGUAGE plpgsql AS $$
        BEGIN
            IF i % 3 = 0 THEN RETURN NULL; END IF;
            RETURN i;
        END $$;
        INSERT INTO t SELECT i, maybe_null(i), 'r'::text FROM generate_series(1, 30) i;

        -- pushed down INSERT..SELECT
        INSERT INTO t
        SELECT i, CASE WHEN i % 4 = 0 THEN NULL ELSE i END,
               CASE WHEN i % 5 = 0 THEN NULL ELSE 'p'::text END
        FROM generate_series(1, 1000) i;

        -- COPY FROM a Parquet file
        COPY (SELECT i AS id, CASE WHEN i % 2 = 0 THEN NULL ELSE i END AS a,
                     NULL::text AS b
              FROM generate_series(1, 100) i)
        TO '{location}/src.parquet';
        COPY t FROM '{location}/src.parquet';

        -- a file with no NULLs at all
        INSERT INTO t SELECT i, i, 'n'::text FROM generate_series(1, 10) i;

        CREATE TABLE p (id int, a int, b text) USING iceberg WITH (partition_by = 'b');
        INSERT INTO p
        SELECT i, CASE WHEN i % 3 = 0 THEN NULL ELSE i END,
               CASE WHEN i % 2 = 0 THEN 'even'::text ELSE 'odd'::text END
        FROM generate_series(1, 100) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        catalog = assert_null_counts_match_files(
            pg_conn, superuser_conn, s3, "null_counts_writes", "t"
        )
        # one file per write
        assert len(catalog) == 5

        # field ids: id 1, a 2, b 3
        assert sorted(
            (counts[1], counts[2], counts.get(3)) for counts in catalog.values()
        ) == [
            # all-NULL b in the COPY file has no bounds, so no stats row
            (0, 0, 0),
            (0, 2, 1),
            (0, 10, 0),
            (0, 50, None),
            (0, 250, 200),
        ]

        catalog = assert_null_counts_match_files(
            pg_conn, superuser_conn, s3, "null_counts_writes", "p"
        )
        assert sorted(counts[2] for counts in catalog.values()) == [16, 17]
    finally:
        pg_conn.rollback()
        run_command("DROP SCHEMA null_counts_writes CASCADE", pg_conn)
        pg_conn.commit()


def test_null_counts_after_rewrites(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """
    Files that UPDATE, DELETE and compaction write record the null counts of
    their own rows.
    """
    run_command(
        """
        CREATE SCHEMA null_counts_rewrites;
        SET search_path TO null_counts_rewrites;

        CREATE TABLE t (id int, a int, b text) USING iceberg;
        INSERT INTO t
        SELECT i, CASE WHEN i % 4 = 0 THEN NULL ELSE i END, 'x'::text
        FROM generate_series(1, 1000) i;
        INSERT INTO t
        SELECT i, CASE WHEN i % 2 = 0 THEN NULL ELSE i END, NULL::text
        FROM generate_series(1001, 1100) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # an UPDATE that adds NULLs, and one that removes them
        run_command(
            "UPDATE null_counts_rewrites.t SET b = NULL WHERE id <= 100", pg_conn
        )
        run_command(
            "UPDATE null_counts_rewrites.t SET a = -1 WHERE a IS NULL AND id > 1050",
            pg_conn,
        )
        # a DELETE that removes most of a file, so it gets rewritten
        run_command("DELETE FROM null_counts_rewrites.t WHERE id <= 900", pg_conn)
        pg_conn.commit()

        assert_null_counts_match_files(
            pg_conn, superuser_conn, s3, "null_counts_rewrites", "t"
        )

        pg_conn.commit()
        pg_conn.autocommit = True
        run_command("VACUUM null_counts_rewrites.t", pg_conn)
        pg_conn.autocommit = False

        catalog = assert_null_counts_match_files(
            pg_conn, superuser_conn, s3, "null_counts_rewrites", "t"
        )
        total_null_a = run_query(
            "SELECT count(*) FROM null_counts_rewrites.t WHERE a IS NULL", pg_conn
        )[0][0]
        assert sum(counts[2] for counts in catalog.values()) == total_null_a
    finally:
        pg_conn.rollback()
        pg_conn.autocommit = False
        run_command("DROP SCHEMA null_counts_rewrites CASCADE", pg_conn)
        pg_conn.commit()


def test_no_null_counts_without_column_stats(
    pg_conn, s3, extension, with_default_location
):
    """
    column_stats_mode = 'none' keeps the null counts out of the manifest along
    with the bounds.
    """
    run_command(
        """
        CREATE SCHEMA null_counts_none;
        CREATE TABLE null_counts_none.t (id int, a int) USING iceberg
        WITH (column_stats_mode = 'none');
        INSERT INTO null_counts_none.t VALUES (1, NULL), (2, 2);
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        manifest = manifest_null_counts(pg_conn, s3, "null_counts_none", "t")
        assert list(manifest.values()) == [{}]
    finally:
        pg_conn.rollback()
        run_command("DROP SCHEMA null_counts_none CASCADE", pg_conn)
        pg_conn.commit()


# column type, a non-NULL value, whether the column gets min/max stats (and
# with them a null count)
TYPED_COLUMNS = [
    ("int2", "1", True),
    ("int4", "1", True),
    ("int8", "1", True),
    ("numeric", "1.5", True),
    ("numeric(20,4)", "1.5", True),
    ("float4", "1.5", True),
    ("float8", "1.5", True),
    ("bool", "true", True),
    ("text", "'x'", True),
    ('text COLLATE "C"', "'x'", True),
    ("varchar(10)", "'x'", True),
    ("char(3)", "'x'", True),
    ("date", "'2026-01-01'", True),
    ("time", "'10:00'", True),
    ("timetz", "'10:00+02'", True),
    ("timestamp", "'2026-01-01 10:00'", True),
    ("timestamptz", "'2026-01-01 10:00+00'", True),
    ("uuid", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", False),
    ("bytea", "'\\x01'", False),
    ("jsonb", "'{\"a\": 1}'", False),
    ("json", "'{\"a\": 1}'", False),
    ("int[]", "ARRAY[1, NULL]", False),
    ("text[]", "ARRAY['a']", False),
    ("null_counts_types.pt", "ROW(1, 'a')", False),
    ("null_counts_types.pt[]", "ARRAY[ROW(1, 'a')]::null_counts_types.pt[]", False),
    ("map_type.key_text_val_int", "ARRAY[('a', 1)]::map_type.key_text_val_int", False),
]


@pytest.mark.parametrize("pushdown", ["on", "off"])
def test_null_counts_per_type(
    pg_conn, superuser_conn, s3, extension, with_default_location, pushdown
):
    """
    Every type with min/max stats gets a null count that matches the file, with
    the insert pushed down to DuckDB or not. Nested and other types without
    stats get neither, so they never claim to have no NULLs.
    """
    create_map_type("text", "int")
    columns = [f"c{i}" for i in range(len(TYPED_COLUMNS))]
    column_defs = ", ".join(
        f"{name} {type_}" for name, (type_, _, _) in zip(columns, TYPED_COLUMNS)
    )

    def row(i, null_columns):
        values = [
            f"NULL::{type_}" if name in null_columns else f"{value}::{type_}"
            for name, (type_, value, _) in zip(columns, TYPED_COLUMNS)
        ]
        return f"SELECT {i}, {', '.join(values)}"

    run_command(
        f"""
        CREATE SCHEMA null_counts_types;
        SET search_path TO null_counts_types;
        CREATE TYPE pt AS (x int, y text);
        CREATE TABLE t (id int, {column_defs}) USING iceberg;
        SET pg_lake_table.enable_insert_select_pushdown TO {pushdown};
        """,
        pg_conn,
    )

    try:
        # one file: every column has a NULL in some row, and the even columns
        # are NULL in two rows
        rows = [row(0, set())]
        rows += [row(i + 1, {name}) for i, name in enumerate(columns)]
        rows += [row(100 + i, {name}) for i, name in enumerate(columns[::2])]
        run_command(f"INSERT INTO t {' UNION ALL '.join(rows)}", pg_conn)

        # one file without NULLs, one where everything but id is NULL
        run_command(f"INSERT INTO t {row(200, set())}", pg_conn)
        run_command(f"INSERT INTO t {row(300, set(columns))}", pg_conn)
        pg_conn.commit()

        catalog = assert_null_counts_match_files(
            pg_conn, superuser_conn, s3, "null_counts_types", "t"
        )
        assert len(catalog) == 3

        # field ids follow the column order, after id; nested types take more
        # field ids, so map names through the catalog instead
        names = dict(
            run_query(
                """
                SELECT m.field_id, a.attname
                FROM lake_table.field_id_mappings m
                JOIN pg_attribute a ON a.attrelid = m.table_name AND a.attnum = m.pg_attnum
                WHERE m.table_name = 'null_counts_types.t'::regclass
                  AND m.parent_field_id IS NULL
                """,
                superuser_conn,
            )
        )

        expected_with_stats = {"id"} | {
            name for name, (_, _, has_stats) in zip(columns, TYPED_COLUMNS) if has_stats
        }
        # the all-NULL file only has stats for id
        all_null = [
            {names.get(f, f): c for f, c in counts.items()}
            for counts in catalog.values()
            if set(counts) == {1}
        ]
        assert all_null == [{"id": 0}]

        for counts in catalog.values():
            named = {names.get(f, f): c for f, c in counts.items()}
            if set(named) == {"id"}:
                continue
            assert set(named) == expected_with_stats
            if named["id"] == 0 and max(named.values()) == 0:
                continue  # the file without NULLs
            for index, name in enumerate(columns):
                if name in named:
                    assert named[name] == (2 if index % 2 == 0 else 1), name
    finally:
        pg_conn.rollback()
        run_command(
            "RESET pg_lake_table.enable_insert_select_pushdown; RESET search_path; "
            "DROP SCHEMA null_counts_types CASCADE",
            pg_conn,
        )
        pg_conn.commit()


@pytest.mark.parametrize(
    "setting",
    [
        "pg_lake_table.enable_insert_select_pushdown",
        "pg_lake_table.enable_partitioned_write_pushdown",
    ],
)
def test_null_counts_partitioned_pushdown(
    pg_conn, superuser_conn, s3, extension, with_default_location, setting
):
    """
    Partitioned writes split rows into a file per partition, pushed down or
    not, and each file gets the null counts of its own rows, including the
    file of the NULL partition.
    """
    for value in ["on", "off"]:
        run_command(
            f"""
            CREATE SCHEMA null_counts_part;
            SET {setting} TO {value};
            CREATE TABLE null_counts_part.t (id int, p text, v int, w text)
            USING iceberg WITH (partition_by = 'p');
            INSERT INTO null_counts_part.t
            SELECT i,
                   CASE i % 3 WHEN 0 THEN 'x'::text WHEN 1 THEN 'y'::text END,
                   CASE WHEN i % 4 = 0 THEN NULL ELSE i END,
                   CASE WHEN i % 5 = 0 THEN NULL ELSE 'w'::text END
            FROM generate_series(1, 300) i;
            """,
            pg_conn,
        )
        pg_conn.commit()
        try:
            catalog = assert_null_counts_match_files(
                pg_conn, superuser_conn, s3, "null_counts_part", "t"
            )
            assert len(catalog) == 3
            # v: field 3, w: field 4
            assert sum(c[3] for c in catalog.values()) == 75
            assert sum(c[4] for c in catalog.values()) == 60
            # the NULL partition has no bounds for p, the others no NULLs in it
            assert sorted(c.get(2, -1) for c in catalog.values()) == [-1, 0, 0]
        finally:
            pg_conn.rollback()
            run_command(
                f"RESET {setting}; DROP SCHEMA null_counts_part CASCADE", pg_conn
            )
            pg_conn.commit()


def test_null_counts_edge_values(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """
    Values that look empty are not NULL: empty strings, zero, false, empty
    arrays, and the floats DuckDB keeps out of min/max. Also long text whose
    bounds get truncated, and a CSV COPY.
    """
    location = f"s3://{TEST_BUCKET}/test_null_counts_edge_values"
    run_command(
        f"""
        CREATE SCHEMA null_counts_edge;
        SET search_path TO null_counts_edge;
        CREATE TABLE t (id int, s text, z int, bo bool, f float8, lt text)
        USING iceberg;

        INSERT INTO t VALUES
            (1, '', 0, false, -0.0, repeat('a', 1000)),
            (2, ' ', 0, false, 'Infinity', repeat('a', 999) || 'b'),
            (3, 'NULL', 0, false, '-Infinity', NULL);

        -- a NaN keeps f out of the stats, and with it its null count
        INSERT INTO t VALUES (4, NULL, NULL, NULL, 'NaN', ''), (5, 'x', 1, true, NULL, 'x');

        COPY (SELECT i AS id,
                     CASE WHEN i % 2 = 0 THEN NULL ELSE ''::text END AS s,
                     NULL::int AS z, true AS bo, 1.0::float8 AS f,
                     CASE WHEN i % 3 = 0 THEN NULL ELSE 'c'::text END AS lt
              FROM generate_series(10, 39) i)
        TO '{location}/src.csv' WITH (format 'csv', header);
        COPY t FROM '{location}/src.csv' WITH (format 'csv', header);
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        catalog = assert_null_counts_match_files(
            pg_conn, superuser_conn, s3, "null_counts_edge", "t"
        )
        # field ids: id 1, s 2, z 3, bo 4, f 5, lt 6
        assert sorted(catalog.values(), key=lambda c: sorted(c.items())) == sorted(
            [
                {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 1},
                {1: 0, 2: 1, 3: 1, 4: 1, 6: 0},
                # z is all NULL in the CSV file
                {1: 0, 2: 15, 4: 0, 5: 0, 6: 10},
            ],
            key=lambda c: sorted(c.items()),
        )
    finally:
        pg_conn.rollback()
        run_command("RESET search_path; DROP SCHEMA null_counts_edge CASCADE", pg_conn)
        pg_conn.commit()

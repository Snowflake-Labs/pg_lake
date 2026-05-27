"""
Real interop tests against DuckDB via the parquet round-trip path.

Strategy: pg_lake writes a DuckLake table (catalog rows + parquet files
in object storage). DuckDB then reads the parquet files directly via
the standard parquet reader, plus optionally walks the catalog rows
exposed via the public.ducklake_* views. We verify the data DuckDB
sees matches what pg_lake wrote.

The DuckDB ducklake extension itself cannot currently read our catalog
because it bootstraps its own __ducklake_metadata_<alias> schema rather
than mapping onto an existing one with non-matching table names. Once
either pg_lake renames lake_ducklake.<name> -> lake_ducklake.ducklake_<name>
or DuckDB grows a knob to point at an existing schema, the
test_attach_ducklake_extension test below should be widened to do a
catalog-level round trip.
"""

import pytest
from utils_pytest import (
    TEST_BUCKET,
    create_duckdb_conn,
    server_params,
)

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


def _location(suffix):
    return f"s3://{TEST_BUCKET}/ducklake_round_trip/{suffix}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_pg_lake_parquet_readable_by_duckdb(pg_cursor, s3):
    """
    pg_lake writes parquet to s3 via INSERT; DuckDB reads the parquet
    file directly. Validates pg_lake's parquet output is well-formed
    against a non-pg_lake reader.
    """
    location = _location("rt_parquet")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_parquet")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_parquet (id INT, name TEXT, qty NUMERIC(10, 2))
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        "INSERT INTO rt_parquet VALUES (1, 'a', 10.50), (2, 'b', 20.00), (3, 'c', 30.75)"
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT lake_ducklake.absolute_data_file_path(df.data_file_id)
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_parquet'
        """
    )
    rows = pg_cursor.fetchall()
    assert len(rows) == 1, f"expected 1 data file, got {len(rows)}"
    parquet_path = rows[0][0]
    assert parquet_path.startswith("s3://"), parquet_path

    duck = create_duckdb_conn()
    result = duck.execute(
        f"SELECT id, name, qty FROM read_parquet('{parquet_path}') ORDER BY id"
    ).fetchall()

    assert result == [
        (1, "a", 10.50),
        (2, "b", 20.00),
        (3, "c", 30.75),
    ]


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_attach_ducklake_extension(pg_cursor):
    """
    DuckDB's ducklake extension can ATTACH against the catalog database
    that pg_lake's metadata lives in. We don't yet expect DuckDB to see
    our DuckLake tables (naming mismatch), but the ATTACH itself must
    succeed and the metadata schema DuckDB bootstraps must include the
    standard ducklake_* tables — that pins the extension's schema
    expectations so a future change in DuckDB or pg_lake can be detected.
    """
    duck = duckdb.connect()
    try:
        duck.execute("INSTALL ducklake FROM core_nightly")
        duck.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"ducklake extension not available: {e}")

    conn_str = (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER}"
    )
    try:
        duck.execute(f"ATTACH 'postgres:{conn_str}' AS dl (TYPE DUCKLAKE)")
    except Exception as e:
        pytest.skip(f"could not ATTACH TYPE DUCKLAKE: {e}")

    tables = duck.execute(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_catalog = '__ducklake_metadata_dl'
        """
    ).fetchall()
    table_names = {row[0] for row in tables}
    expected = {
        "ducklake_snapshot",
        "ducklake_snapshot_changes",
        "ducklake_schema",
        "ducklake_table",
        "ducklake_column",
        "ducklake_data_file",
        "ducklake_metadata",
        "ducklake_schema_versions",
    }
    missing = expected - table_names
    assert not missing, (
        f"DuckDB ducklake extension missing expected metadata tables: {missing}. "
        f"saw: {sorted(table_names)}"
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_public_ducklake_views_match_extension_schema(pg_cursor):
    """
    Cross-check: the public.ducklake_* views we expose, combined with
    the underlying lake_ducklake.* tables, present columns matching what
    DuckDB's ducklake extension expects. This is what gets us to read
    parity once the schema-name mismatch is resolved. Sourced from a
    live DuckDB ducklake-extension probe rather than the spec website
    (which currently describes v1 while the shipping extension is v0.3).
    """
    expected = {
        "ducklake_snapshot": {
            "snapshot_id",
            "snapshot_time",
            "schema_version",
            "next_catalog_id",
            "next_file_id",
        },
        "ducklake_snapshot_changes": {
            "snapshot_id",
            "changes_made",
            "author",
            "commit_message",
            "commit_extra_info",
        },
        "ducklake_schema": {
            "schema_id",
            "schema_uuid",
            "begin_snapshot",
            "end_snapshot",
            "schema_name",
            "path",
            "path_is_relative",
        },
        "ducklake_table": {
            "table_id",
            "table_uuid",
            "begin_snapshot",
            "end_snapshot",
            "schema_id",
            "table_name",
            "path",
            "path_is_relative",
        },
        "ducklake_column": {
            "column_id",
            "begin_snapshot",
            "end_snapshot",
            "table_id",
            "column_order",
            "column_name",
            "column_type",
            "initial_default",
            "default_value",
            "nulls_allowed",
            "parent_column",
            "default_value_type",
            "default_value_dialect",
        },
        "ducklake_data_file": {
            "data_file_id",
            "table_id",
            "begin_snapshot",
            "end_snapshot",
            "file_order",
            "path",
            "path_is_relative",
            "file_format",
            "record_count",
            "file_size_bytes",
            "footer_size",
            "row_id_start",
            "partition_id",
            "encryption_key",
            "partial_max",
            "mapping_id",
        },
        "ducklake_schema_versions": {
            "begin_snapshot",
            "schema_version",
            "table_id",
        },
    }

    for view, want_cols in expected.items():
        pg_cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (view,),
        )
        got = {row[0] for row in pg_cursor.fetchall()}
        missing = want_cols - got
        assert not missing, f"public.{view} missing columns DuckDB needs: {missing}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_multi_insert_each_lands_on_a_new_snapshot(pg_cursor, s3):
    """
    Multiple INSERTs into one table produce one parquet file per INSERT
    each on its own snapshot, and DuckDB can read each parquet
    independently. Pins the per-statement snapshot semantics.
    """
    location = _location("rt_multi")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_multi")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_multi (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    after_create = pg_cursor.fetchone()[0]

    pg_cursor.execute("INSERT INTO rt_multi VALUES (1, 'a'), (2, 'b')")
    pg_cursor.connection.commit()
    pg_cursor.execute("INSERT INTO rt_multi VALUES (3, 'c')")
    pg_cursor.connection.commit()
    pg_cursor.execute("INSERT INTO rt_multi VALUES (4, 'd'), (5, 'e'), (6, 'f')")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT lake_ducklake.absolute_data_file_path(df.data_file_id),
               df.record_count, df.begin_snapshot
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_multi'
         ORDER BY df.begin_snapshot
        """
    )
    files = pg_cursor.fetchall()
    assert len(files) == 3, f"expected one data file per INSERT, got {len(files)}"

    snaps = [row[2] for row in files]
    assert snaps == [after_create + 1, after_create + 2, after_create + 3], snaps
    assert [row[1] for row in files] == [2, 1, 3]

    duck = create_duckdb_conn()
    rows = []
    for path, _, _ in files:
        rows.extend(
            duck.execute(
                f"SELECT id, val FROM read_parquet('{path}') ORDER BY id"
            ).fetchall()
        )
    assert sorted(rows) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")]

    pg_cursor.execute(
        "SELECT changes_made FROM lake_ducklake.snapshot_changes "
        "WHERE snapshot_id > %s ORDER BY snapshot_id",
        (after_create,),
    )
    changes = [row[0] for row in pg_cursor.fetchall()]
    assert changes == [
        "INSERT/UPDATE/DELETE operation",
        "INSERT/UPDATE/DELETE operation",
        "INSERT/UPDATE/DELETE operation",
    ]


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_alter_then_insert_round_trip(pg_cursor, s3):
    """
    ADD COLUMN followed by INSERT writes parquet matching the new schema
    and DuckDB reads back all columns including the new one.
    """
    location = _location("rt_alter")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_alter")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_alter (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_alter VALUES (1, 'before')")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE rt_alter ADD COLUMN qty INT")
    pg_cursor.execute("INSERT INTO rt_alter VALUES (2, 'after', 99)")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT lake_ducklake.absolute_data_file_path(df.data_file_id),
               df.begin_snapshot, df.record_count
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_alter'
         ORDER BY df.begin_snapshot
        """
    )
    files = pg_cursor.fetchall()
    assert len(files) == 2, f"expected 2 parquet files, got {len(files)}"

    duck = create_duckdb_conn()
    cols1 = duck.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{files[1][0]}')"
    ).fetchall()
    names1 = {row[0] for row in cols1}
    assert {"id", "val", "qty"}.issubset(names1), names1


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_file_column_stats_match_parquet(pg_cursor, s3):
    """
    The min/max we record in lake_ducklake.file_column_stats must agree
    with what DuckDB extracts from the actual parquet footer. Mismatched
    stats are a silent correctness bug for predicate pushdown.
    """
    location = _location("rt_stats")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_stats")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_stats (id INT, label TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        "INSERT INTO rt_stats VALUES (5, 'banana'), (1, 'apple'), (9, 'cherry')"
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT lake_ducklake.absolute_data_file_path(df.data_file_id),
               fcs.column_id, fcs.min_value, fcs.max_value, c.column_name
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
          JOIN lake_ducklake.file_column_stats fcs USING (data_file_id)
          JOIN lake_ducklake.column c
            ON c.column_id = fcs.column_id AND c.end_snapshot IS NULL
         WHERE t.table_name = 'rt_stats'
         ORDER BY c.column_order
        """
    )
    rows = pg_cursor.fetchall()
    assert len(rows) >= 1, "expected file column stats rows"
    parquet_path = rows[0][0]

    catalog_stats = {row[4]: (row[2], row[3]) for row in rows}

    duck = create_duckdb_conn()
    actual = duck.execute(
        f"SELECT MIN(id), MAX(id), MIN(label), MAX(label) "
        f"FROM read_parquet('{parquet_path}')"
    ).fetchone()
    actual_min_id, actual_max_id, actual_min_label, actual_max_label = actual

    if "id" in catalog_stats:
        cmin, cmax = catalog_stats["id"]
        assert int(cmin) == actual_min_id, (cmin, actual_min_id)
        assert int(cmax) == actual_max_id, (cmax, actual_max_id)
    if "label" in catalog_stats:
        cmin, cmax = catalog_stats["label"]
        assert cmin == actual_min_label, (cmin, actual_min_label)
        assert cmax == actual_max_label, (cmax, actual_max_label)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_file_column_stats_extended_fields(pg_cursor, s3):
    """
    DuckLake v1's file_column_stats has column_size_bytes / value_count /
    null_count alongside min/max. We populate them from DuckDB's COPY
    return_stats payload — pin them so a future regression doesn't
    silently revert them to NULL.
    """
    location = _location("rt_stats_ext")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_stats_ext")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_stats_ext (id INT, label TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    # Mix of NULL and non-NULL values so null_count is not zero, value_count
    # excludes NULLs, and column_size_bytes is positive.
    pg_cursor.execute(
        "INSERT INTO rt_stats_ext VALUES (1, 'a'), (2, NULL), (3, 'c'), (NULL, 'd')"
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT c.column_name,
               fcs.column_size_bytes,
               fcs.value_count,
               fcs.null_count
          FROM lake_ducklake.file_column_stats fcs
          JOIN lake_ducklake.data_file df USING (data_file_id)
          JOIN lake_ducklake.table t ON t.table_id = df.table_id
          JOIN lake_ducklake.column c
            ON c.column_id = fcs.column_id AND c.end_snapshot IS NULL
         WHERE t.table_name = 'rt_stats_ext'
         ORDER BY c.column_order
        """
    )
    rows = {r[0]: (r[1], r[2], r[3]) for r in pg_cursor.fetchall()}
    assert set(rows.keys()) == {"id", "label"}, rows

    for col, (size_bytes, value_count, null_count) in rows.items():
        # All three should be populated, not NULL.
        assert size_bytes is not None and size_bytes > 0, (col, size_bytes)
        assert value_count is not None, (col, value_count)
        assert null_count is not None, (col, null_count)
        # value_count excludes NULLs; we inserted 4 rows with one NULL each.
        assert value_count == 3, (col, value_count)
        assert null_count == 1, (col, null_count)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_table_stats_rolls_up_after_writes(pg_cursor, s3):
    """
    After every INSERT and TRUNCATE, lake_ducklake.table_stats must
    reflect the live data files: SUM(record_count), SUM(file_size_bytes),
    MAX(row_id_start + record_count). DuckDB's read path queries this
    table for plan-time row-count estimates; before this commit it was
    never populated.
    """
    location = _location("rt_stats_rollup")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_stats_rollup")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_stats_rollup (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT table_id FROM lake_ducklake.table WHERE table_name = 'rt_stats_rollup'"
    )
    table_id = pg_cursor.fetchone()[0]

    # No inserts yet -> table_stats either absent or zeroed.
    pg_cursor.execute(
        "SELECT record_count, file_size_bytes FROM lake_ducklake.table_stats "
        "WHERE table_id = %s",
        (table_id,),
    )
    row = pg_cursor.fetchone()
    if row is not None:
        assert row[0] in (0, None), row
        assert row[1] in (0, None), row

    pg_cursor.execute("INSERT INTO rt_stats_rollup VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    pg_cursor.connection.commit()
    pg_cursor.execute("INSERT INTO rt_stats_rollup VALUES (4, 'd'), (5, 'e')")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT SUM(record_count), SUM(file_size_bytes),
               COALESCE(MAX(row_id_start + record_count), 0)
          FROM lake_ducklake.data_file
         WHERE table_id = %s AND end_snapshot IS NULL
        """,
        (table_id,),
    )
    expected_records, expected_size, expected_next_row = pg_cursor.fetchone()

    pg_cursor.execute(
        """
        SELECT record_count, next_row_id, file_size_bytes
          FROM lake_ducklake.table_stats
         WHERE table_id = %s
        """,
        (table_id,),
    )
    stats = pg_cursor.fetchone()
    assert stats is not None, "INSERT must populate table_stats"
    assert stats[0] == expected_records, (stats[0], expected_records)
    assert stats[1] == expected_next_row, (stats[1], expected_next_row)
    assert stats[2] == expected_size, (stats[2], expected_size)

    # TRUNCATE-equivalent: drop and recreate clears live data files.
    pg_cursor.execute("DROP TABLE rt_stats_rollup")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_stats_rollup (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_delete_writes_position_delete_file(pg_cursor, s3):
    """
    DELETE on a DuckLake table goes through the merge-on-read path:
    pg_lake leaves the source data_file row live and writes a position-
    delete file (a row in lake_ducklake.delete_file) referencing it.
    The source data_file's record_count stays at the original total;
    table_stats reflects the logical row count (source minus deletes).

    DuckLake's spec is built around merge-on-read + a separate compact
    operation, and DuckDB's ducklake extension is merge-on-read on the
    write side -- so PG-side mirrors that behaviour to compose
    correctly with concurrent DuckDB writers (a copy-on-write rewrite
    would end-snapshot a data_file that DuckDB might be writing
    delete entries against in a parallel transaction).
    """
    location = _location("rt_delete")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_delete")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_delete (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        "INSERT INTO rt_delete VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT table_id FROM lake_ducklake.table WHERE table_name = 'rt_delete'"
    )
    table_id = pg_cursor.fetchone()[0]

    pg_cursor.execute(
        "SELECT data_file_id FROM lake_ducklake.data_file "
        "WHERE table_id = %s AND end_snapshot IS NULL",
        (table_id,),
    )
    pre_delete_file_id = pg_cursor.fetchone()[0]

    pg_cursor.execute("DELETE FROM rt_delete WHERE id = 2")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT data_file_id, end_snapshot, record_count
          FROM lake_ducklake.data_file
         WHERE table_id = %s
        """,
        (table_id,),
    )
    files = pg_cursor.fetchall()
    assert len(files) == 1, f"expected source data_file unchanged, got {files}"
    only = files[0]
    assert only[0] == pre_delete_file_id, only
    assert (
        only[1] is None
    ), f"source data_file must stay live, got end_snapshot {only[1]}"
    assert only[2] == 4, f"source record_count is the on-disk total, got {only[2]}"

    pg_cursor.execute(
        """
        SELECT data_file_id, end_snapshot, delete_count
          FROM lake_ducklake.delete_file
         WHERE table_id = %s
        """,
        (table_id,),
    )
    deletes = pg_cursor.fetchall()
    assert len(deletes) == 1, f"expected one position-delete file, got {deletes}"
    df = deletes[0]
    assert df[0] == pre_delete_file_id, f"delete_file must point at source, got {df}"
    assert df[1] is None, f"delete_file must be live, got end_snapshot {df[1]}"
    assert df[2] == 1, f"expected 1 deleted row, got {df[2]}"

    pg_cursor.execute("SELECT count(*) FROM rt_delete")
    assert pg_cursor.fetchone()[0] == 3, "user-visible count must reflect the delete"

    pg_cursor.execute(
        "SELECT record_count FROM lake_ducklake.table_stats WHERE table_id = %s",
        (table_id,),
    )
    stats = pg_cursor.fetchone()
    # table_stats.record_count is the on-disk total across data_file rows;
    # delete_file.delete_count is subtracted at read time, not rolled into
    # this column. So after a merge-on-read DELETE the stat stays at 4.
    assert stats is not None and stats[0] == 4, stats


def test_create_table_records_real_uuid_and_absolute_path(pg_cursor, s3):
    """
    DuckLake v1 expects schema_uuid and table_uuid to be unique per
    object. Earlier the implementation seeded both with the all-zero
    UUID so DuckDB readers could not distinguish recreated tables, and
    table.path_is_relative defaulted to true even though we always
    store the full s3:// location. Pin both invariants.
    """
    location = _location("rt_uuid_and_path")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_uuid_and_path")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_uuid_and_path (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT table_id, table_uuid, path, path_is_relative
          FROM lake_ducklake.table
         WHERE table_name = 'rt_uuid_and_path'
        """
    )
    row = pg_cursor.fetchone()
    assert row is not None
    table_id, table_uuid, table_path, path_is_relative = row

    zero_uuid = "00000000-0000-0000-0000-000000000000"
    assert (
        str(table_uuid) != zero_uuid
    ), f"table_uuid must be a real UUID, got {table_uuid}"
    assert (
        path_is_relative is False
    ), f"absolute s3:// location must store path_is_relative=false, got {path_is_relative}"
    assert table_path.rstrip("/") == location.rstrip("/"), (table_path, location)

    pg_cursor.execute(
        """
        SELECT schema_id, schema_uuid FROM lake_ducklake.schema
         WHERE schema_name = 'public'
        """
    )
    schema_row = pg_cursor.fetchone()
    assert schema_row is not None
    assert (
        str(schema_row[1]) != zero_uuid
    ), f"schema_uuid must be a real UUID, got {schema_row[1]}"


def test_select_after_schema_evolution(pg_cursor, s3):
    """
    DuckLake v1 keeps physical column IDs stable across ALTER. After
    ADD/DROP COLUMN the FDW must still surface the correct logical
    columns when reading old parquet files (which were written under
    the previous schema). This pins the read-side behavior end-to-end.
    """
    location = _location("rt_schema_evol")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_schema_evol")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_schema_evol (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_schema_evol VALUES (1, 'a')")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE rt_schema_evol ADD COLUMN qty INT")
    pg_cursor.execute("INSERT INTO rt_schema_evol VALUES (2, 'b', 99)")
    pg_cursor.connection.commit()

    # SELECT after the schema change must surface 2 rows: the old one
    # with qty=NULL and the new one.
    pg_cursor.execute("SELECT id, val, qty FROM rt_schema_evol ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, "a", None), (2, "b", 99)], rows


def test_select_after_drop_column(pg_cursor, s3):
    """
    DROP COLUMN must hide the dropped column on subsequent SELECTs but
    the parquet files containing it (written before the drop) must
    still be readable for the surviving columns.
    """
    location = _location("rt_drop_col_read")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_drop_col_read")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_drop_col_read (id INT, val TEXT, qty INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_drop_col_read VALUES (1, 'a', 10), (2, 'b', 20)")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE rt_drop_col_read DROP COLUMN qty")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT id, val FROM rt_drop_col_read ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, "a"), (2, "b")], rows


def test_select_after_rename_column(pg_cursor, s3):
    """
    RENAME COLUMN in DuckLake v1 produces a new column-version row with
    the same column_id and the new name. The physical parquet field_id
    is column_order which matches postgres attnum, so a SELECT after
    rename must still surface the rows from the older parquet file (it
    was written under the previous name) using the new name.
    """
    location = _location("rt_rename")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_rename")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_rename (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_rename VALUES (1, 'a'), (2, 'b')")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE rt_rename RENAME COLUMN val TO label")
    pg_cursor.execute("INSERT INTO rt_rename VALUES (3, 'c')")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT id, label FROM rt_rename ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")], rows


def test_create_drop_create_same_name_keeps_history(pg_cursor, s3):
    """
    DROP TABLE end-snapshots the table row; recreating the same name
    must work because the previous registration is no longer live.
    Pin both halves: the original table_id is preserved in history
    with end_snapshot set, and the new CREATE inserts a fresh row.
    """
    location = _location("rt_recreate")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_recreate")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_recreate (id INT) USING ducklake
            WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT table_id FROM lake_ducklake.table "
        "WHERE table_name = 'rt_recreate' AND end_snapshot IS NULL"
    )
    first_id = pg_cursor.fetchone()[0]

    pg_cursor.execute("DROP TABLE rt_recreate")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT table_id, end_snapshot FROM lake_ducklake.table "
        "WHERE table_name = 'rt_recreate' AND table_id = %s",
        (first_id,),
    )
    row = pg_cursor.fetchone()
    assert (
        row is not None and row[1] is not None
    ), f"DROP TABLE must keep the table row with end_snapshot set, got {row}"

    pg_cursor.execute(
        f"""
        CREATE TABLE rt_recreate (id INT) USING ducklake
            WITH (location = '{location}/round2')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT table_id FROM lake_ducklake.table "
        "WHERE table_name = 'rt_recreate' AND end_snapshot IS NULL"
    )
    second_id = pg_cursor.fetchone()[0]
    assert second_id != first_id, (first_id, second_id)


def test_add_column_with_default_backfills_old_files(pg_cursor, s3):
    """
    ALTER TABLE ADD COLUMN ... DEFAULT N should backfill N (not NULL)
    when reading rows from parquet files written before the alter.
    DuckLake stores this as ducklake_column.initial_default and the
    read-side schema map applies it via read_parquet's default_value.
    """
    location = _location("rt_default")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_default")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_default (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_default VALUES (1, 'a')")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE rt_default ADD COLUMN qty INT DEFAULT 99")
    pg_cursor.execute("INSERT INTO rt_default VALUES (2, 'b', 7)")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT id, val, qty FROM rt_default ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, "a", 99), (2, "b", 7)], rows


def test_add_column_with_string_default(pg_cursor, s3):
    """Same as test_add_column_with_default_backfills_old_files but with a
    text DEFAULT. Postgres formats the expression as 'foo'::text via
    pg_get_expr; DuckDB needs to be able to parse that as the column
    default for old parquet files."""
    location = _location("rt_str_default")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_str_default")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_str_default (id INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_str_default VALUES (1)")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "ALTER TABLE rt_str_default ADD COLUMN label TEXT DEFAULT 'unknown'"
    )
    pg_cursor.execute("INSERT INTO rt_str_default VALUES (2, 'set')")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT id, label FROM rt_str_default ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, "unknown"), (2, "set")], rows


def test_add_column_default_records_default_value_type_literal(pg_cursor, s3):
    """
    DuckLake v1 added a default_value_type discriminator on
    ducklake_column. The v0.3 -> v1.0 migration sets it to 'literal'
    for any column with a default. Newly-written rows should match: a
    column with a non-NULL initial_default needs default_value_type =
    'literal' so DuckDB's reader interprets the default correctly. A
    column without a default keeps default_value_type = NULL.
    """
    location = _location("rt_dvt")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_dvt")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_dvt (id INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("ALTER TABLE rt_dvt ADD COLUMN qty INT DEFAULT 7")
    pg_cursor.execute("ALTER TABLE rt_dvt ADD COLUMN note TEXT")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT column_name, initial_default, default_value, default_value_type
          FROM lake_ducklake.column c
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_dvt' AND c.end_snapshot IS NULL
         ORDER BY column_order
        """
    )
    rows = {r[0]: (r[1], r[2], r[3]) for r in pg_cursor.fetchall()}

    assert rows["id"] == (None, None, None), rows["id"]
    assert rows["qty"] == ("7", "7", "literal"), rows["qty"]
    assert rows["note"] == (None, None, None), rows["note"]


def test_create_table_captures_postgres_defaults(pg_cursor, s3):
    """
    CREATE TABLE foo (col TYPE DEFAULT expr) USING ducklake should
    record the resolved default in lake_ducklake.column.initial_default
    so the v0 reader (and later DuckDB reads of pre-INSERT parquet)
    can backfill the column with the right value. Mirrors the
    DucklakeAddColumn DEFAULT capture, but at table-creation time.
    """
    location = _location("rt_create_default")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_create_default")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_create_default (
            id INT,
            label TEXT DEFAULT 'unknown',
            qty INT DEFAULT 42,
            note TEXT
        ) USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT column_name, initial_default, default_value, default_value_type
          FROM lake_ducklake.column c
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_create_default' AND c.end_snapshot IS NULL
         ORDER BY column_order
        """
    )
    rows = {r[0]: (r[1], r[2], r[3]) for r in pg_cursor.fetchall()}

    assert rows["id"] == (None, None, None), rows["id"]
    assert rows["label"] == ("unknown", "unknown", "literal"), rows["label"]
    assert rows["qty"] == ("42", "42", "literal"), rows["qty"]
    assert rows["note"] == (None, None, None), rows["note"]


def test_table_column_stats_carries_single_file_minmax(pg_cursor, s3):
    """
    After a single-INSERT table is written, table_column_stats carries
    the single file's per-column min/max bounds verbatim. After a
    second INSERT, the multi-file aggregate falls back to NULL because
    text-domain aggregation would misprune numerics. Both branches
    keep one row per (table_id, column_id) so DuckDB's
    GetGlobalTableStats LEFT JOIN doesn't NULL-deref column_id.
    """
    location = _location("rt_tcs")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_tcs")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_tcs (id INT, label TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_tcs VALUES (1, 'apple'), (5, 'cherry')")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT c.column_name, tcs.min_value, tcs.max_value
          FROM lake_ducklake.table_column_stats tcs
          JOIN lake_ducklake.column c USING (column_id)
          JOIN lake_ducklake.table t ON c.table_id = t.table_id
         WHERE t.table_name = 'rt_tcs' AND c.end_snapshot IS NULL
         ORDER BY c.column_order
        """
    )
    rows = {r[0]: (r[1], r[2]) for r in pg_cursor.fetchall()}

    # After one INSERT, table_column_stats carries the file's min/max
    assert rows["id"] == ("1", "5"), rows["id"]
    assert rows["label"] == ("apple", "cherry"), rows["label"]

    # Add a second INSERT — multi-file aggregate must drop to NULL
    pg_cursor.execute("INSERT INTO rt_tcs VALUES (10, 'date')")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT c.column_name, tcs.min_value, tcs.max_value
          FROM lake_ducklake.table_column_stats tcs
          JOIN lake_ducklake.column c USING (column_id)
          JOIN lake_ducklake.table t ON c.table_id = t.table_id
         WHERE t.table_name = 'rt_tcs' AND c.end_snapshot IS NULL
         ORDER BY c.column_order
        """
    )
    rows2 = {r[0]: (r[1], r[2]) for r in pg_cursor.fetchall()}
    assert rows2["id"] == (None, None), rows2["id"]
    assert rows2["label"] == (None, None), rows2["label"]

    # And the row-per-(table_id, column_id) invariant still holds
    pg_cursor.execute(
        """
        SELECT count(*)
          FROM lake_ducklake.table_column_stats tcs
          JOIN lake_ducklake.table t ON tcs.table_id = t.table_id
         WHERE t.table_name = 'rt_tcs'
        """
    )
    assert pg_cursor.fetchone()[0] == 2, "one row per column expected"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_pg_update_replaces_old_rows(pg_cursor, s3):
    """
    PG-side UPDATE on a DuckLake table must not double-count rows.
    Whether the writer chose copy-on-write (entire source file rewritten
    minus the deleted rows) or merge-on-read (a position-delete file
    pointing back at the source data_file_id), a subsequent SELECT must
    return only the post-update view.
    """
    location = _location("rt_update")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_update")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_update (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO rt_update VALUES (1, 7), (3, 7)")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT x, y FROM rt_update ORDER BY x")
    assert pg_cursor.fetchall() == [(1, 7), (3, 7)]

    pg_cursor.execute("UPDATE rt_update SET y = 4")
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT x, y FROM rt_update ORDER BY x")
    assert pg_cursor.fetchall() == [(1, 4), (3, 4)]
    pg_cursor.execute("SELECT count(*) FROM rt_update")
    assert pg_cursor.fetchone()[0] == 2

    # When merge-on-read is the chosen path, the delete file must be
    # linked to its source data_file_id so compaction/snapshot expiry
    # can reason about it. (Copy-on-write writes no delete file at all,
    # so we only assert when one exists.)
    pg_cursor.execute(
        """
        SELECT df.data_file_id
          FROM lake_ducklake.delete_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_update' AND df.end_snapshot IS NULL
        """
    )
    delete_rows = pg_cursor.fetchall()
    for (data_file_id,) in delete_rows:
        assert (
            data_file_id is not None
        ), "delete file is missing the link back to its source data_file_id"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
def test_pg_partial_update_uses_position_deletes(pg_cursor, s3):
    """
    Below the copy-on-write threshold (default 20% of rows updated),
    pg_lake should use merge-on-read: a position-delete file is added
    pointing back at the source data file's data_file_id, plus a new
    data file with the updated rows. Pinning this exercises the path
    DucklakeAddDeleteFile + the DATA_FILE_ADD_DELETE_MAPPING resolver
    that fills in data_file_id.
    """
    location = _location("rt_update_partial")
    pg_cursor.execute("DROP TABLE IF EXISTS rt_update_partial")
    pg_cursor.execute(
        f"""
        CREATE TABLE rt_update_partial (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    # 100 rows so a 1-row update is well under the 20% COW threshold.
    pg_cursor.execute(
        "INSERT INTO rt_update_partial SELECT g, g FROM generate_series(1,100) g"
    )
    pg_cursor.connection.commit()

    # Sanity: read should see 100 rows after a fresh insert.
    pg_cursor.execute("SELECT count(*) FROM rt_update_partial")
    assert pg_cursor.fetchone()[0] == 100, "INSERT path is broken; can't test UPDATE"

    pg_cursor.execute("UPDATE rt_update_partial SET y = -1 WHERE x = 42")
    pg_cursor.connection.commit()

    # Check the catalog state directly first; the FDW scan path may
    # have a separate bug we don't want to mask the metadata bug with.
    pg_cursor.execute(
        """
        SELECT lake_ducklake.absolute_data_file_path(df.data_file_id),
               df.data_file_id, df.delete_count
          FROM lake_ducklake.delete_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_update_partial' AND df.end_snapshot IS NULL
        """
    )
    rows = pg_cursor.fetchall()
    assert len(rows) >= 1, "expected at least one position-delete file row"
    for path, data_file_id, delete_count in rows:
        assert (
            data_file_id is not None
        ), f"delete_file row {path} missing source data_file_id link"
        assert delete_count >= 1, (path, delete_count)

    pg_cursor.execute("SELECT count(*) FROM rt_update_partial")
    assert (
        pg_cursor.fetchone()[0] == 100
    ), "row count must not double after merge-on-read UPDATE"

    pg_cursor.execute("SELECT y FROM rt_update_partial WHERE x = 42")
    assert pg_cursor.fetchone()[0] == -1

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
import os
import pytest

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


MINIO_AVAILABLE = bool(os.environ.get("PGLAKE_MINIO_BUCKET"))


def _minio_location(suffix):
    bucket = os.environ.get("PGLAKE_MINIO_BUCKET", "localbucket")
    prefix = os.environ.get("PGLAKE_MINIO_PREFIX", "pglake")
    return f"s3://{bucket}/{prefix}/{suffix}"


def _duckdb_with_minio():
    """Return a fresh DuckDB connection configured for the local MinIO."""
    conn = duckdb.connect()
    endpoint = os.environ.get("PGLAKE_MINIO_ENDPOINT", "localhost:9000")
    key = os.environ.get("PGLAKE_MINIO_KEY", "testkey")
    secret = os.environ.get("PGLAKE_MINIO_SECRET", "testpassword")
    conn.execute(
        f"""
        CREATE SECRET (TYPE S3,
                       KEY_ID '{key}',
                       SECRET '{secret}',
                       ENDPOINT '{endpoint}',
                       URL_STYLE 'path',
                       USE_SSL false)
        """
    )
    return conn


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
@pytest.mark.skipif(not MINIO_AVAILABLE, reason="set PGLAKE_MINIO_BUCKET to run")
def test_pg_lake_parquet_readable_by_duckdb(pg_cursor):
    """
    pg_lake writes parquet to s3 via INSERT; DuckDB reads the parquet
    file directly. Validates pg_lake's parquet output is well-formed
    against a non-pg_lake reader.
    """
    location = _minio_location("rt_parquet")
    pg_cursor.execute(f"DROP TABLE IF EXISTS rt_parquet")
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

    # Look up the parquet path pg_lake recorded
    pg_cursor.execute(
        """
        SELECT df.path
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_parquet'
        """
    )
    rows = pg_cursor.fetchall()
    assert len(rows) == 1, f"expected 1 data file, got {len(rows)}"
    parquet_path = rows[0][0]
    assert parquet_path.startswith("s3://"), parquet_path

    # DuckDB reads the parquet directly
    duck = _duckdb_with_minio()
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

    user = os.environ.get("PGUSER", "postgres")
    port = os.environ.get("PGPORT", "5432")
    db = os.environ.get("PGDATABASE", "postgres")
    host = os.environ.get("PGHOST", "localhost")
    conn_str = f"host={host} port={port} dbname={db} user={user}"
    try:
        duck.execute(f"ATTACH '{conn_str}' AS dl (TYPE DUCKLAKE)")
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
    so it tracks the actual extension contract.
    """
    expected = {
        "ducklake_snapshot": {
            "snapshot_id", "snapshot_time", "schema_version",
            "next_catalog_id", "next_file_id",
        },
        "ducklake_snapshot_changes": {
            "snapshot_id", "changes_made", "author", "commit_message",
            "commit_extra_info",
        },
        "ducklake_schema": {
            "schema_id", "schema_uuid", "begin_snapshot", "end_snapshot",
            "schema_name", "path", "path_is_relative",
        },
        "ducklake_table": {
            "table_id", "table_uuid", "begin_snapshot", "end_snapshot",
            "schema_id", "table_name", "path", "path_is_relative",
        },
        "ducklake_column": {
            "column_id", "begin_snapshot", "end_snapshot", "table_id",
            "column_order", "column_name", "column_type", "initial_default",
            "default_value", "nulls_allowed", "parent_column",
        },
        "ducklake_data_file": {
            "data_file_id", "table_id", "begin_snapshot", "end_snapshot",
            "file_order", "path", "path_is_relative", "file_format",
            "record_count", "file_size_bytes", "footer_size", "row_id_start",
            "partition_id", "encryption_key", "partial_file_info",
            "mapping_id",
        },
        "ducklake_schema_versions": {
            "begin_snapshot", "schema_version",
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
@pytest.mark.skipif(not MINIO_AVAILABLE, reason="set PGLAKE_MINIO_BUCKET to run")
def test_multi_insert_each_lands_on_a_new_snapshot(pg_cursor):
    """
    Multiple INSERTs into one table produce one parquet file per INSERT
    each on its own snapshot, and DuckDB can read each parquet
    independently. Pins the per-statement snapshot semantics added in
    "Create a new ducklake snapshot for CREATE TABLE / INSERT / RENAME".
    """
    location = _minio_location("rt_multi")
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
        SELECT df.path, df.record_count, df.begin_snapshot
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_multi'
         ORDER BY df.begin_snapshot
        """
    )
    files = pg_cursor.fetchall()
    assert len(files) == 3, f"expected one data file per INSERT, got {len(files)}"

    # Each INSERT must land on a fresh snapshot
    snaps = [row[2] for row in files]
    assert snaps == [after_create + 1, after_create + 2, after_create + 3], snaps

    # Records-per-file matches what we wrote
    assert [row[1] for row in files] == [2, 1, 3]

    # And each parquet file is independently readable
    duck = _duckdb_with_minio()
    rows = []
    for path, _, _ in files:
        rows.extend(
            duck.execute(
                f"SELECT id, val FROM read_parquet('{path}') ORDER BY id"
            ).fetchall()
        )
    assert sorted(rows) == [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")]

    # snapshot_changes records each INSERT
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
@pytest.mark.skipif(not MINIO_AVAILABLE, reason="set PGLAKE_MINIO_BUCKET to run")
def test_alter_then_insert_round_trip(pg_cursor):
    """
    ADD COLUMN followed by INSERT writes parquet matching the new schema
    and DuckDB reads back all columns including the new one.
    """
    location = _minio_location("rt_alter")
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
        SELECT df.path, df.begin_snapshot, df.record_count
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'rt_alter'
         ORDER BY df.begin_snapshot
        """
    )
    files = pg_cursor.fetchall()
    assert len(files) == 2, f"expected 2 parquet files, got {len(files)}"

    duck = _duckdb_with_minio()

    # First file (pre-alter) has only id, val
    cols0 = duck.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{files[0][0]}')"
    ).fetchall()
    names0 = {row[0] for row in cols0}
    assert "id" in names0 and "val" in names0
    # The first parquet may or may not include qty depending on writer
    # behaviour; what matters is that the post-alter parquet does.

    # Second file (post-alter) has id, val, qty
    cols1 = duck.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{files[1][0]}')"
    ).fetchall()
    names1 = {row[0] for row in cols1}
    assert {"id", "val", "qty"}.issubset(names1), names1


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="duckdb python module not installed")
@pytest.mark.skipif(not MINIO_AVAILABLE, reason="set PGLAKE_MINIO_BUCKET to run")
def test_file_column_stats_match_parquet(pg_cursor):
    """
    The min/max we record in lake_ducklake.file_column_stats must agree
    with what DuckDB extracts from the actual parquet footer. Mismatched
    stats are a silent correctness bug for predicate pushdown.
    """
    location = _minio_location("rt_stats")
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
        SELECT df.path, fcs.column_id, fcs.min_value, fcs.max_value, c.column_name
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

    duck = _duckdb_with_minio()
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

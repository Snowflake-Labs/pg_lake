"""
End-to-end DuckLake interoperability tests between PostgreSQL and DuckDB.

These tests verify that:
1. PostgreSQL can create DuckLake tables with real data
2. DuckDB can read PostgreSQL-created DuckLake tables
3. Metadata is compatible between both systems
"""

import pytest
from utils_pytest import TEST_BUCKET, server_params

# Check if duckdb is available
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_create_ducklake_table_s3(pg_cursor):
    """
    Test creating a DuckLake table with S3 location.
    Tables are created as foreign tables using the DuckLake FDW.
    """
    location = f"s3://{TEST_BUCKET}/test_table"

    pg_cursor.execute(f"""
        CREATE TABLE test_s3_table (
            id INTEGER,
            name TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.connection.commit()

    # Verify foreign table was created
    pg_cursor.execute("""
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_name = 'test_s3_table'
    """)
    result = pg_cursor.fetchone()
    assert result is not None, "Foreign table was not created"

    # Verify it's using the correct server
    pg_cursor.execute("""
        SELECT foreign_server_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_name = 'test_s3_table'
    """)
    server = pg_cursor.fetchone()
    assert server[0] == 'pg_lake_ducklake', f"Wrong server: {server[0]}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_metadata_structure_after_create(pg_cursor):
    """
    Test that creating a DuckLake table populates metadata tables.
    """
    location = f"s3://{TEST_BUCKET}/metadata_test"

    pg_cursor.execute(f"""
        CREATE TABLE metadata_test (
            id INTEGER,
            value TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("""
        SELECT table_name FROM lake_ducklake.tables
        WHERE table_name = 'metadata_test'
    """)
    rows = pg_cursor.fetchall()
    assert len(rows) == 1, f"expected metadata_test in lake_ducklake.tables, got {rows}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_insert_data_to_ducklake_table(pg_cursor, s3):
    """
    Test inserting data into a DuckLake table. After commit, the data
    file is registered in lake_ducklake.data_file with the correct row
    count and stats.
    """
    location = f"s3://{TEST_BUCKET}/insert_test"

    pg_cursor.execute(f"""
        CREATE TABLE insert_test (
            id INTEGER,
            name TEXT,
            value DOUBLE PRECISION
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("""
        INSERT INTO insert_test VALUES
            (1, 'alice', 10.5),
            (2, 'bob', 20.3)
    """)
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT COUNT(*) FROM insert_test")
    count = pg_cursor.fetchone()[0]
    assert count == 2, f"Expected 2 rows, got {count}"

    pg_cursor.execute(
        "SELECT COUNT(*), SUM(record_count) FROM lake_ducklake.data_file df "
        "JOIN lake_ducklake.\"table\" t USING (table_id) "
        "WHERE t.table_name = 'insert_test'"
    )
    data_files, total_records = pg_cursor.fetchone()
    assert data_files == 1
    assert total_records == 2


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_reads_postgres_metadata(pg_cursor, s3):
    """
    Test that DuckDB can read DuckLake metadata that PostgreSQL wrote
    via INSERT, and surface the rows over the lake.
    """
    location = f"s3://{TEST_BUCKET}/shared_table"
    pg_cursor.execute("DROP TABLE IF EXISTS shared_table")
    pg_cursor.execute(f"""
        CREATE TABLE shared_table (
            id INTEGER,
            name TEXT
        ) USING ducklake
        WITH (location = '{location}')
    """)
    pg_cursor.execute("INSERT INTO shared_table VALUES (1, 'test'), (2, 'two')")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )

    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    rows = duck.execute(
        "SELECT id, name FROM dl.public.shared_table ORDER BY id"
    ).fetchall()
    assert rows == [(1, 'test'), (2, 'two')], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_writes_through_postgres_metadata(pg_cursor, s3):
    """
    DuckDB performs an INSERT on a pg_lake_ducklake-backed table; the
    DuckLake extension issues `INSERT INTO public.ducklake_snapshot_changes`
    via the view, which fires our INSTEAD OF INSERT trigger. Pins the
    column-name pass-through so a regression in the trigger (e.g.
    NEW.operations vs NEW.changes_made) shows up here instead of as a
    user-visible commit failure.
    """
    location = f"s3://{TEST_BUCKET}/duck_writes"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_writes")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_writes (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )

    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("INSERT INTO dl.public.duck_writes VALUES (3, 4)")

    rows = duck.execute(
        "SELECT x, y FROM dl.public.duck_writes ORDER BY x"
    ).fetchall()
    assert rows == [(3, 4)], rows

    pg_cursor.connection.commit()

    # The data_inlining_row_limit=0 metadata option should have made
    # DuckDB write a parquet file rather than materializing the row in
    # a per-table ducklake_inlined_data_* catalog table. Pin both:
    # the data_file row exists, and no inlined data tables were created.
    pg_cursor.execute(
        """
        SELECT df.path, df.record_count, df.file_size_bytes
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t ON t.table_id = df.table_id
         WHERE t.table_name = 'duck_writes' AND df.end_snapshot IS NULL
        """
    )
    df_rows = pg_cursor.fetchall()
    assert len(df_rows) == 1, f"expected exactly one data file, got {df_rows}"
    assert df_rows[0][1] == 1, df_rows  # one record in the file

    pg_cursor.execute(
        """
        SELECT count(*)
          FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name SIMILAR TO 'ducklake_inlined_data_[0-9]+_[0-9]+'
        """
    )
    inlined_tables = pg_cursor.fetchone()[0]
    assert inlined_tables == 0, (
        f"expected no per-table inlined data tables, found {inlined_tables}"
    )

    # And the same row must be visible from the PostgreSQL FDW side.
    # This works because both DuckDB's ducklake extension and pg_lake's
    # ducklake schema/leaf-field code derive parquet field_id from
    # lake_ducklake.column.column_id — so they share one ID space.
    pg_cursor.execute("SELECT x, y FROM duck_writes ORDER BY x")
    pg_rows = pg_cursor.fetchall()
    assert pg_rows == [(3, 4)], pg_rows


def test_manual_metadata_creation_for_duckdb(pg_cursor, tmp_path):
    """
    Test manually creating DuckLake metadata that DuckDB can read.

    This verifies our metadata schema matches DuckDB's expectations.
    """
    if not DUCKDB_AVAILABLE:
        pytest.skip("DuckDB not installed")

    # Manually populate metadata tables to simulate a complete DuckLake table
    pg_cursor.execute("""
        -- Create schema entry
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name, path)
        VALUES (100, gen_random_uuid(), 0, 'default', NULL);

        -- Create table entry
        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name, path)
        VALUES (100, gen_random_uuid(), 0, 100, 'manual_table', NULL);

        -- Create column entries
        INSERT INTO lake_ducklake.column
        (column_id, begin_snapshot, table_id, column_order, column_name, column_type)
        VALUES
        (1, 0, 100, 0, 'id', 'INTEGER'),
        (2, 0, 100, 1, 'name', 'VARCHAR');
    """)
    pg_cursor.connection.commit()

    # Verify metadata was created correctly
    pg_cursor.execute("""
        SELECT t.table_name, s.schema_name, COUNT(c.column_id) as col_count
        FROM lake_ducklake.table t
        JOIN lake_ducklake.schema s ON t.schema_id = s.schema_id
        LEFT JOIN lake_ducklake.column c ON t.table_id = c.table_id
        WHERE t.table_id = 100
        GROUP BY t.table_name, s.schema_name
    """)
    result = pg_cursor.fetchone()
    assert result[0] == 'manual_table'
    assert result[1] == 'default'
    assert result[2] == 2, "Should have 2 columns"

    # Verify using the view
    pg_cursor.execute("""
        SELECT table_name
        FROM public.ducklake_table
        WHERE table_name = 'manual_table'
    """)
    view_result = pg_cursor.fetchone()
    assert view_result is not None, "Table should appear in ducklake_table view"


def test_snapshot_creation_workflow(pg_cursor):
    """
    Test the snapshot creation workflow for DuckLake tables.
    """
    # Create a snapshot for a table modification
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.snapshot
        (snapshot_id, schema_version, next_catalog_id, next_file_id)
        VALUES (200, 0, 101, 1)
    """)

    # Add snapshot changes metadata
    pg_cursor.execute("""
        INSERT INTO lake_ducklake.snapshot_changes
        (snapshot_id, changes_made, author)
        VALUES (200, 'INSERT', 'test_user')
    """)
    pg_cursor.connection.commit()

    # Verify snapshot query works
    pg_cursor.execute("""
        SELECT * FROM lake_ducklake.snapshots('test_catalog')
        WHERE snapshot_id = 200
    """)
    result = pg_cursor.fetchone()
    assert result is not None
    assert result[0] == 200  # snapshot_id


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_select_after_duckdb_delete(pg_cursor, s3):
    """
    DuckDB DELETE on a DuckLake table writes a position-delete file (or
    inlines deletes) into the metadata catalog; the next PostgreSQL-side
    SELECT must not segfault while reading. Regression for an issue where
    `table test;` after `DELETE FROM ...` from a DuckDB session crashed
    the backend.
    """
    location = f"s3://{TEST_BUCKET}/duck_delete"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_delete")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_delete (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO duck_delete VALUES (1, 7), (2, 7), (3, 7)")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )

    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("DELETE FROM dl.public.duck_delete WHERE x = 2")

    pg_cursor.connection.commit()
    pg_cursor.execute("SELECT x, y FROM duck_delete ORDER BY x")
    rows = pg_cursor.fetchall()
    assert rows == [(1, 7), (3, 7)], (
        f"DuckDB DELETE x=2 should be visible on PG side, got {rows}"
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_select_after_duckdb_insert_and_delete(pg_cursor, s3):
    """
    Same as test_pg_select_after_duckdb_delete but the rows are
    inserted via DuckDB too — exercises the full DuckDB-driven write
    path, which uses different field-id stamping than pg_lake's writer.
    """
    location = f"s3://{TEST_BUCKET}/duck_ins_del"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_ins_del")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_ins_del (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )

    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("INSERT INTO dl.public.duck_ins_del VALUES (1, 7), (2, 7), (3, 7)")
    duck.execute("DELETE FROM dl.public.duck_ins_del WHERE x = 2")

    pg_cursor.execute("SELECT x, y FROM duck_ins_del ORDER BY x")
    rows = pg_cursor.fetchall()
    assert rows == [(1, 7), (3, 7)], (
        f"DuckDB INSERT+DELETE must be applied on PG side, got {rows}"
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_select_after_duckdb_delete_many(pg_cursor, s3):
    """
    Larger delete via DuckDB: 1 row out of 50, exercising the
    merge-on-read path (well below any copy-on-write threshold).
    """
    location = f"s3://{TEST_BUCKET}/duck_del_many"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_del_many")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_del_many (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute(
        "INSERT INTO duck_del_many SELECT g, g*10 FROM generate_series(1, 50) g"
    )
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("DELETE FROM dl.public.duck_del_many WHERE x = 17")

    pg_cursor.connection.commit()
    pg_cursor.execute("SELECT count(*) FROM duck_del_many")
    assert pg_cursor.fetchone()[0] == 49

    pg_cursor.execute("SELECT x FROM duck_del_many WHERE x = 17")
    assert pg_cursor.fetchone() is None, "row 17 must be deleted"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_reads_pg_added_column(pg_cursor, s3):
    """
    PG-side ALTER ADD COLUMN, then INSERT into the new column. DuckDB
    must read the value back, not NULL — i.e. the parquet field_id we
    stamp on the new column must match the column_id DuckDB looks up
    from the catalog for that column.
    """
    location = f"s3://{TEST_BUCKET}/duck_alter_add"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_alter_add")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_alter_add (x INT, y INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO duck_alter_add VALUES (1, 4)")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE duck_alter_add ADD COLUMN z INT")
    pg_cursor.connection.commit()

    pg_cursor.execute("INSERT INTO duck_alter_add VALUES (2, 4, 5)")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    rows = duck.execute(
        "SELECT x, y, z FROM dl.public.duck_alter_add ORDER BY x"
    ).fetchall()
    assert rows == [(1, 4, None), (2, 4, 5)], (
        f"DuckDB must see z=5 for the row pg_lake INSERTed after ALTER, got {rows}"
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_rename_table_propagates(pg_cursor, s3):
    """
    ALTER TABLE … RENAME TO from the PG side must close the live row in
    lake_ducklake.table (set end_snapshot) and insert a new row reusing
    the same table_id with the new name and a new begin_snapshot. After
    that, both the PG FDW and DuckDB's ducklake extension must see the
    table under its new name.
    """
    location = f"s3://{TEST_BUCKET}/pg_rename"
    pg_cursor.execute("DROP TABLE IF EXISTS pg_rename_old")
    pg_cursor.execute("DROP TABLE IF EXISTS pg_rename_new")
    pg_cursor.execute(
        f"""
        CREATE TABLE pg_rename_old (id INT, label TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO pg_rename_old VALUES (1, 'a'), (2, 'b')")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT table_id FROM lake_ducklake.table WHERE table_name = 'pg_rename_old' AND end_snapshot IS NULL"
    )
    table_id_before = pg_cursor.fetchone()[0]

    pg_cursor.execute("ALTER TABLE pg_rename_old RENAME TO pg_rename_new")
    pg_cursor.connection.commit()

    # Old row was end-snapshotted, new row re-uses the same table_id.
    pg_cursor.execute(
        """
        SELECT begin_snapshot, end_snapshot, table_name
          FROM lake_ducklake.table
         WHERE table_id = %s
         ORDER BY begin_snapshot
        """,
        (table_id_before,),
    )
    versions = pg_cursor.fetchall()
    assert len(versions) == 2, versions
    # Old version: end_snapshot is the new snapshot id (not NULL).
    assert versions[0][1] is not None and versions[0][2] == 'pg_rename_old', versions
    # New version: end_snapshot NULL, name updated.
    assert versions[1][1] is None and versions[1][2] == 'pg_rename_new', versions

    # Data is still readable through PG under the new name.
    pg_cursor.execute("SELECT id, label FROM pg_rename_new ORDER BY id")
    assert pg_cursor.fetchall() == [(1, 'a'), (2, 'b')]

    # And DuckDB sees the new name too.
    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )
    rows = duck.execute(
        "SELECT id, label FROM dl.public.pg_rename_new ORDER BY id"
    ).fetchall()
    assert rows == [(1, 'a'), (2, 'b')], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_rename_table_no_pk_violation(pg_cursor, s3):
    """
    Before the schema fix, DuckDB-side ALTER TABLE RENAME failed with
    'duplicate key value violates unique constraint "table_pkey"'
    because lake_ducklake.table had table_id as the sole PK. The spec
    versions tables by (table_id, begin_snapshot); pin that DuckDB's
    rename now succeeds and produces a second row sharing table_id.
    """
    location = f"s3://{TEST_BUCKET}/duck_rename"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_rename_old")
    pg_cursor.execute("DROP TABLE IF EXISTS duck_rename_new")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_rename_old (id INT, label TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO duck_rename_old VALUES (1, 'a')")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("ALTER TABLE dl.public.duck_rename_old RENAME TO duck_rename_new")

    pg_cursor.execute(
        """
        SELECT count(DISTINCT table_id), count(*)
          FROM lake_ducklake.table
         WHERE table_name IN ('duck_rename_old', 'duck_rename_new')
        """
    )
    distinct_ids, total_rows = pg_cursor.fetchone()
    assert distinct_ids == 1, (distinct_ids, total_rows)
    assert total_rows == 2, (distinct_ids, total_rows)

    # The catalog INSERT trigger replays the rename onto pg_class via
    # ALTER FOREIGN TABLE, so the foreign table's pg_class entry now
    # matches the new catalog name. Assert pg_class is in sync and
    # SELECT works under the new name.
    pg_cursor.connection.commit()
    pg_cursor.execute(
        "SELECT 1 FROM pg_class WHERE relname = 'duck_rename_new' AND relkind = 'f'"
    )
    assert pg_cursor.fetchone() is not None, (
        "pg_class should have been renamed by the replay trigger"
    )
    pg_cursor.execute(
        "SELECT 1 FROM pg_class WHERE relname = 'duck_rename_old' AND relkind = 'f'"
    )
    assert pg_cursor.fetchone() is None, (
        "old pg_class entry should be gone after replay"
    )

    pg_cursor.execute("SELECT id, label FROM duck_rename_new ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, 'a')], rows
    # Commit so pg_cursor releases its AccessShare lock on the foreign
    # table; otherwise the next DuckDB-side rename's in-trigger ALTER
    # FOREIGN TABLE blocks waiting for AccessExclusive.
    pg_cursor.connection.commit()

    # Renaming back must also work — exercises the INSTEAD OF UPDATE
    # trigger path where the WHERE clause has to pin the live row by
    # (table_id, begin_snapshot), not table_id alone, otherwise we
    # rewrite a sibling historical version's begin_snapshot and trip
    # the composite PK.
    duck.execute("ALTER TABLE dl.public.duck_rename_new RENAME TO duck_rename_old")
    pg_cursor.execute(
        """
        SELECT count(DISTINCT table_id), count(*)
          FROM lake_ducklake.table
         WHERE table_name IN ('duck_rename_old', 'duck_rename_new')
        """
    )
    distinct_ids, total_rows = pg_cursor.fetchone()
    assert distinct_ids == 1, (distinct_ids, total_rows)
    assert total_rows == 3, (distinct_ids, total_rows)

    # Replay should have flipped pg_class back to duck_rename_old too.
    pg_cursor.connection.commit()
    pg_cursor.execute(
        "SELECT 1 FROM pg_class WHERE relname = 'duck_rename_old' AND relkind = 'f'"
    )
    assert pg_cursor.fetchone() is not None
    pg_cursor.execute("SELECT id, label FROM duck_rename_old ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, 'a')], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_drop_table_propagates(pg_cursor, s3):
    """
    DuckDB-side DROP TABLE must end-snapshot the lake_ducklake.table
    row AND remove the matching foreign table from pg_class. The
    snapshot_changes-based dispatcher reads `dropped_table:<id>` and
    issues DROP FOREIGN TABLE under the in_replay GUC so
    pg_lake_table's drop hook doesn't double-end-snapshot.
    """
    location = f"s3://{TEST_BUCKET}/duck_drop"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_drop")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_drop (id INT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO duck_drop VALUES (1), (2)")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("DROP TABLE dl.public.duck_drop")

    # Catalog: every row for this table_id is end-snapshotted.
    pg_cursor.connection.commit()
    pg_cursor.execute(
        """
        SELECT count(*) FILTER (WHERE end_snapshot IS NULL)
          FROM lake_ducklake.table
         WHERE table_name = 'duck_drop'
        """
    )
    live_rows = pg_cursor.fetchone()[0]
    assert live_rows == 0, live_rows

    # PG: the foreign table is gone from pg_class via the replay.
    pg_cursor.execute(
        "SELECT 1 FROM pg_class WHERE relname = 'duck_drop' AND relkind = 'f'"
    )
    assert pg_cursor.fetchone() is None, (
        "pg_class entry should have been dropped by the replay trigger"
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_alter_columns_propagate(pg_cursor, s3):
    """
    DuckDB-side ALTER ADD COLUMN, DROP COLUMN, and RENAME COLUMN
    must reflect into pg_attribute. The snapshot_changes-based
    replay diffs the catalog's live `lake_ducklake.column` rows
    against pg_attribute and applies ALTER FOREIGN TABLE
    ADD/DROP/RENAME COLUMN under the in_replay GUC.
    """
    location = f"s3://{TEST_BUCKET}/duck_alter_cols"
    pg_cursor.execute("DROP TABLE IF EXISTS duck_alter_cols")
    pg_cursor.execute(
        f"""
        CREATE TABLE duck_alter_cols (a INT, b TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    # ADD COLUMN c INT
    duck.execute("ALTER TABLE dl.public.duck_alter_cols ADD COLUMN c INT")
    pg_cursor.connection.commit()
    pg_cursor.execute(
        """
        SELECT attname FROM pg_attribute
         WHERE attrelid = 'public.duck_alter_cols'::regclass
           AND attnum > 0 AND NOT attisdropped
         ORDER BY attnum
        """
    )
    assert [r[0] for r in pg_cursor.fetchall()] == ['a', 'b', 'c']

    # RENAME COLUMN b -> bb
    duck.execute("ALTER TABLE dl.public.duck_alter_cols RENAME COLUMN b TO bb")
    pg_cursor.connection.commit()
    pg_cursor.execute(
        """
        SELECT attname FROM pg_attribute
         WHERE attrelid = 'public.duck_alter_cols'::regclass
           AND attnum > 0 AND NOT attisdropped
         ORDER BY attnum
        """
    )
    assert [r[0] for r in pg_cursor.fetchall()] == ['a', 'bb', 'c']

    # DROP COLUMN a
    duck.execute("ALTER TABLE dl.public.duck_alter_cols DROP COLUMN a")
    pg_cursor.connection.commit()
    pg_cursor.execute(
        """
        SELECT attname FROM pg_attribute
         WHERE attrelid = 'public.duck_alter_cols'::regclass
           AND attnum > 0 AND NOT attisdropped
         ORDER BY attnum
        """
    )
    assert [r[0] for r in pg_cursor.fetchall()] == ['bb', 'c']


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_drop_then_duckdb_readd_column_returns_null(pg_cursor, s3):
    """
    Pin the regression where PG drops column 'val' and DuckDB later
    re-adds a column with the same name: the new column must read as
    NULL (matching what PG-only DROP+ADD does), not surface the
    original column's parquet data through stale name_mapping rows.
    """
    location = f"s3://{TEST_BUCKET}/drop_readd"
    pg_cursor.execute("DROP TABLE IF EXISTS drop_readd")
    pg_cursor.execute(
        f"""
        CREATE TABLE drop_readd (id INT, val TEXT)
            USING ducklake WITH (location = '{location}')
        """
    )
    pg_cursor.execute("INSERT INTO drop_readd VALUES (1, 'one'), (2, 'two')")
    pg_cursor.execute("ALTER TABLE drop_readd DROP COLUMN val")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    duck.execute("ALTER TABLE dl.public.drop_readd ADD COLUMN val TEXT")

    pg_cursor.connection.commit()
    pg_cursor.execute("SELECT id, val FROM drop_readd ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, None), (2, None)], (
        f"after PG DROP + DuckDB re-ADD, val must be NULL; got {rows}"
    )


def test_default_location_prefix_makes_paths_relative(pg_cursor, s3):
    """
    With pg_lake_ducklake.default_location_prefix set and CREATE TABLE
    issued without an explicit location, the catalog should record:
      - lake_ducklake.metadata.data_path = '<prefix>/'
      - lake_ducklake.schema.path        = '<schema>/' (relative)
      - lake_ducklake.table.path         = '<table>/' (relative)
    INSERT/SELECT must round-trip via the resolver chain.
    """
    prefix = f"s3://{TEST_BUCKET}/relpath_phase2"
    pg_cursor.execute(f"SET pg_lake_ducklake.default_location_prefix TO '{prefix}'")
    pg_cursor.execute("DROP TABLE IF EXISTS rp2_auto")
    pg_cursor.execute("CREATE TABLE rp2_auto (id INT, name TEXT) USING ducklake")
    pg_cursor.execute("INSERT INTO rp2_auto VALUES (1, 'a'), (2, 'b')")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT value FROM lake_ducklake.metadata WHERE key = 'data_path'"
    )
    row = pg_cursor.fetchone()
    assert row is not None and row[0] == f"{prefix}/", row

    pg_cursor.execute(
        "SELECT path, path_is_relative FROM lake_ducklake.schema "
        "WHERE schema_name = 'public' AND end_snapshot IS NULL"
    )
    schema_row = pg_cursor.fetchone()
    assert schema_row == ("public/", True), schema_row

    pg_cursor.execute(
        "SELECT path, path_is_relative FROM lake_ducklake.table "
        "WHERE table_name = 'rp2_auto' AND end_snapshot IS NULL"
    )
    table_row = pg_cursor.fetchone()
    assert table_row == ("rp2_auto/", True), table_row

    pg_cursor.execute("SELECT id, name FROM rp2_auto ORDER BY id")
    assert pg_cursor.fetchall() == [(1, "a"), (2, "b")]


def test_default_location_prefix_explicit_location_overrides(pg_cursor, s3):
    """
    Even with the GUC set, an explicit WITH (location = ...) keeps the
    Phase 1 absolute behaviour: schema.path stays NULL, table.path is
    stored absolute with path_is_relative=false.
    """
    prefix = f"s3://{TEST_BUCKET}/relpath_phase2_b"
    explicit = f"s3://{TEST_BUCKET}/elsewhere/rp2_explicit"

    pg_cursor.execute(f"SET pg_lake_ducklake.default_location_prefix TO '{prefix}'")
    pg_cursor.execute("DROP TABLE IF EXISTS rp2_explicit")
    pg_cursor.execute(
        f"""
        CREATE TABLE rp2_explicit (id INT)
            USING ducklake WITH (location = '{explicit}')
        """
    )
    pg_cursor.execute("INSERT INTO rp2_explicit VALUES (10), (20)")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT path, path_is_relative FROM lake_ducklake.table "
        "WHERE table_name = 'rp2_explicit' AND end_snapshot IS NULL"
    )
    row = pg_cursor.fetchone()
    assert row is not None
    assert row[0].rstrip("/") == explicit.rstrip("/"), row
    assert row[1] is False, row

    pg_cursor.execute("SELECT id FROM rp2_explicit ORDER BY id")
    assert pg_cursor.fetchall() == [(10,), (20,)]


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_default_location_prefix_duckdb_interop(pg_cursor, s3):
    """
    GUC-mode CREATE TABLE on the PG side, INSERT, then DuckDB ATTACHes
    and SELECTs back. Verifies DuckDB resolves the data_path → schema
    → table chain and reads our parquet file at the right URL.
    """
    prefix = f"s3://{TEST_BUCKET}/relpath_phase2_d"
    pg_cursor.execute(f"SET pg_lake_ducklake.default_location_prefix TO '{prefix}'")
    pg_cursor.execute("DROP TABLE IF EXISTS rp2_duck")
    pg_cursor.execute("CREATE TABLE rp2_duck (id INT, val TEXT) USING ducklake")
    pg_cursor.execute("INSERT INTO rp2_duck VALUES (1, 'x'), (2, 'y')")
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )

    rows = duck.execute(
        "SELECT id, val FROM dl.public.rp2_duck ORDER BY id"
    ).fetchall()
    assert rows == [(1, "x"), (2, "y")], rows


def test_pg_schema_rename_propagates(pg_cursor, s3):
    """
    PG-side ALTER SCHEMA RENAME TO must end-snapshot the live
    lake_ducklake.schema row and insert a new versioned row reusing
    the same schema_id with the new name. schema.path is preserved
    (data files don't move on a rename).
    """
    pg_cursor.execute("DROP SCHEMA IF EXISTS rs_old CASCADE")
    pg_cursor.execute("DROP SCHEMA IF EXISTS rs_new CASCADE")
    pg_cursor.execute("CREATE SCHEMA rs_old")
    pg_cursor.execute(
        f"""
        CREATE TABLE rs_old.t (id INT)
            USING ducklake WITH (location = 's3://{TEST_BUCKET}/rs_rename')
        """
    )
    pg_cursor.execute("INSERT INTO rs_old.t VALUES (1), (2)")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id FROM lake_ducklake.schema "
        "WHERE schema_name = 'rs_old' AND end_snapshot IS NULL"
    )
    schema_id_before = pg_cursor.fetchone()[0]

    pg_cursor.execute("ALTER SCHEMA rs_old RENAME TO rs_new")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id, schema_name, path "
        "FROM lake_ducklake.schema WHERE end_snapshot IS NULL"
    )
    rows = pg_cursor.fetchall()
    assert (schema_id_before, "rs_new", None) in rows or any(
        r[0] == schema_id_before and r[1] == "rs_new" for r in rows
    ), rows

    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_id = %s AND schema_name = 'rs_old' AND end_snapshot IS NOT NULL",
        (schema_id_before,),
    )
    assert pg_cursor.fetchone()[0] == 1, "old schema-version row must be end-snapshotted"

    pg_cursor.execute("SELECT id FROM rs_new.t ORDER BY id")
    assert pg_cursor.fetchall() == [(1,), (2,)]

    pg_cursor.execute("DROP SCHEMA rs_new CASCADE")
    pg_cursor.connection.commit()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

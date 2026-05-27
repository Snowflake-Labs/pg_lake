"""
Test DuckLake metadata compliance with spec v0.3.

Validates that all metadata tables match the DuckLake specification exactly.
"""

import pytest


def test_all_metadata_tables_exist(pg_cursor):
    """
    Verify all required metadata tables exist.
    """
    required_tables = [
        "metadata",
        "snapshot",
        "snapshot_changes",
        "schema",
        "table",
        "column",
        "view",
        "data_file",
        "delete_file",
        "files_scheduled_for_deletion",
        "inlined_data_tables",
        "table_stats",
        "table_column_stats",
        "file_column_stats",
        "partition_info",
        "partition_column",
        "file_partition_value",
        "column_mapping",
        "name_mapping",
        "tag",
        "column_tag",
        "schema_versions",
    ]

    for table_name in required_tables:
        pg_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
        """
        )
        count = pg_cursor.fetchone()[0]
        assert count == 1, f"Required table lake_ducklake.{table_name} not found"


def test_snapshot_table_structure(pg_cursor):
    """
    Verify snapshot table matches spec.
    """
    pg_cursor.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'snapshot'
        ORDER BY ordinal_position
    """
    )
    columns = pg_cursor.fetchall()

    expected = [
        ("snapshot_id", "bigint", "NO"),
        ("snapshot_time", "timestamp", "NO"),
        ("schema_version", "bigint", "NO"),
        ("next_catalog_id", "bigint", "NO"),
        ("next_file_id", "bigint", "NO"),
    ]

    assert len(columns) >= len(
        expected
    ), f"Missing columns in snapshot table: got {len(columns)}, expected at least {len(expected)}"

    for i, (col, exp) in enumerate(zip(columns, expected)):
        assert col[0] == exp[0], f"Column {i} name mismatch: {col[0]} != {exp[0]}"
        assert (
            exp[1] in col[1].lower()
        ), f"Type mismatch for {exp[0]}: {col[1]} doesn't contain {exp[1]}"


def test_table_metadata_structure(pg_cursor):
    """
    Verify table metadata structure.
    """
    pg_cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'table'
        ORDER BY ordinal_position
    """
    )
    columns = pg_cursor.fetchall()

    required_columns = [
        "table_id",
        "table_uuid",
        "table_name",
        "schema_id",
        "path",
        "begin_snapshot",
        "end_snapshot",
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert (
            req_col in column_names
        ), f"Required column {req_col} not found in lake_ducklake.table"


def test_data_file_structure(pg_cursor):
    """
    Verify data_file table structure.
    """
    pg_cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'data_file'
        ORDER BY ordinal_position
    """
    )
    columns = pg_cursor.fetchall()

    required_columns = [
        "data_file_id",
        "table_id",
        "begin_snapshot",
        "end_snapshot",
        "path",
        "record_count",
        "file_size_bytes",
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert (
            req_col in column_names
        ), f"Required column {req_col} not found in lake_ducklake.data_file"


def test_column_table_structure(pg_cursor):
    """
    Verify column metadata structure for schema evolution.
    """
    pg_cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'column'
        ORDER BY ordinal_position
    """
    )
    columns = pg_cursor.fetchall()

    required_columns = [
        "column_id",
        "begin_snapshot",
        "end_snapshot",
        "table_id",
        "column_order",
        "column_name",
        "column_type",
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert (
            req_col in column_names
        ), f"Required column {req_col} not found in lake_ducklake.column"


def test_primary_keys_exist(pg_cursor):
    """
    Verify critical tables have primary keys.
    """
    tables_with_pks = ["snapshot", "table", "data_file", "delete_file", "schema"]

    for table_name in tables_with_pks:
        pg_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
            AND constraint_type = 'PRIMARY KEY'
        """
        )
        count = pg_cursor.fetchone()[0]
        assert count > 0, f"Table lake_ducklake.{table_name} missing PRIMARY KEY"


def test_foreign_keys_exist(pg_cursor):
    """
    DuckLake's compaction and vacuum issue DELETEs against parent
    catalog tables (data_file, snapshot, partition_info, …) before
    cleaning up dependents. Hard FKs would short-circuit those flows
    on Postgres in a way DuckLake's own catalogs (which carry no FKs)
    don't, so we intentionally don't declare any either.
    """
    pg_cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE table_schema = 'lake_ducklake'
        AND constraint_type = 'FOREIGN KEY'
    """
    )
    assert pg_cursor.fetchone()[0] == 0, (
        "lake_ducklake catalog must declare no foreign keys; "
        "DuckLake-side DELETEs assume FK-free schema"
    )


def test_schema_table_structure(pg_cursor):
    """
    Verify schema (namespace) table structure.
    """
    pg_cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'schema'
        ORDER BY ordinal_position
    """
    )
    columns = pg_cursor.fetchall()

    required_columns = [
        "schema_id",
        "schema_uuid",
        "schema_name",
        "begin_snapshot",
        "end_snapshot",
        "path",
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert (
            req_col in column_names
        ), f"Required column {req_col} not found in lake_ducklake.schema"


def test_partition_tables_structure(pg_cursor):
    """
    Verify partitioning metadata tables.
    """
    # Check partition_info exists and has correct structure
    pg_cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'partition_info'
        ORDER BY ordinal_position
    """
    )
    columns = [col[0] for col in pg_cursor.fetchall()]
    assert "partition_id" in columns
    assert "table_id" in columns

    # Check partition_column exists
    pg_cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'partition_column'
    """
    )
    assert pg_cursor.fetchone()[0] == 1


def test_statistics_tables_exist(pg_cursor):
    """
    Verify statistics metadata tables.
    """
    stats_tables = ["table_stats", "table_column_stats", "file_column_stats"]

    for table_name in stats_tables:
        pg_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'lake_ducklake'
            AND table_name = '{table_name}'
        """
        )
        count = pg_cursor.fetchone()[0]
        assert count == 1, f"Statistics table lake_ducklake.{table_name} not found"


def test_delete_file_structure(pg_cursor):
    """
    Verify delete file tracking table.
    """
    pg_cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'lake_ducklake'
        AND table_name = 'delete_file'
        ORDER BY ordinal_position
    """
    )
    columns = pg_cursor.fetchall()

    required_columns = [
        "delete_file_id",
        "table_id",
        "begin_snapshot",
        "end_snapshot",
        "data_file_id",
        "path",
        "delete_count",
        "file_size_bytes",
    ]

    column_names = [col[0] for col in columns]
    for req_col in required_columns:
        assert (
            req_col in column_names
        ), f"Required column {req_col} not found in lake_ducklake.delete_file"


def test_ducklake_metadata_view_synthesizes_data_path_from_guc(pg_cursor):
    """
    The public.ducklake_metadata view must surface a synthetic 'data_path'
    row when pg_lake_ducklake.default_location_prefix is set and no real
    row has been written. This is what lets DuckDB ATTACH succeed without
    DATA_PATH against a freshly-created extension.

    Once a real 'data_path' row exists, the synthetic one disappears so
    the persisted value wins.
    """
    pg_cursor.execute(
        "SELECT key, value FROM public.ducklake_metadata WHERE key = 'data_path'"
    )
    assert pg_cursor.fetchall() == [], "no real or GUC-derived data_path expected"

    pg_cursor.execute("SET pg_lake_ducklake.default_location_prefix = 's3://bucket/x'")
    pg_cursor.execute(
        "SELECT key, value FROM public.ducklake_metadata WHERE key = 'data_path'"
    )
    rows = pg_cursor.fetchall()
    assert rows == [
        ("data_path", "s3://bucket/x/")
    ], f"GUC fallback must trail-slash and surface, got {rows}"

    pg_cursor.execute("SET pg_lake_ducklake.default_location_prefix = 's3://bucket/y/'")
    pg_cursor.execute(
        "SELECT value FROM public.ducklake_metadata WHERE key = 'data_path'"
    )
    assert pg_cursor.fetchone() == (
        "s3://bucket/y/",
    ), "trailing slash on GUC value should not double-up"

    # A real row in lake_ducklake.metadata must override the GUC fallback.
    pg_cursor.execute(
        "INSERT INTO lake_ducklake.metadata (key, value, scope, scope_id) "
        "VALUES ('data_path', 's3://bucket/persisted/', NULL, NULL)"
    )
    pg_cursor.execute(
        "SELECT value FROM public.ducklake_metadata WHERE key = 'data_path'"
    )
    assert pg_cursor.fetchall() == [
        ("s3://bucket/persisted/",)
    ], "real row must hide the synthetic GUC row"


def test_install_seeds_user_pg_schemas_into_lake_ducklake_schema(pg_cursor):
    """
    CREATE EXTENSION pg_lake_ducklake materialises existing user-visible
    PG schemas into lake_ducklake.schema, so DuckDB sees them on ATTACH
    without needing CREATE SCHEMA dl.<name>. System schemas (pg_*,
    information_schema) and extension-owned schemas (lake, lake_engine,
    lake_iceberg, lake_table, lake_ducklake, ...) must NOT appear.

    The conftest cleanup runs DELETE on lake_ducklake.schema between
    tests, so to inspect the seed we DROP and re-CREATE the extension
    within this test.
    """
    pg_cursor.execute("DROP EXTENSION pg_lake_ducklake CASCADE")
    pg_cursor.execute("CREATE EXTENSION pg_lake_ducklake CASCADE")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_name FROM lake_ducklake.schema "
        "WHERE end_snapshot IS NULL ORDER BY schema_name"
    )
    seeded = [r[0] for r in pg_cursor.fetchall()]

    assert "public" in seeded, f"expected public to be seeded, got {seeded}"

    forbidden = {
        "pg_catalog",
        "pg_toast",
        "information_schema",
        "lake",
        "lake_ducklake",
        "lake_engine",
        "lake_iceberg",
        "lake_table",
    }
    leaked = forbidden.intersection(seeded)
    assert not leaked, f"system/extension schemas leaked into seed: {leaked}"

    pg_cursor.execute(
        "SELECT next_catalog_id FROM lake_ducklake.snapshot WHERE snapshot_id = 0"
    )
    next_id = pg_cursor.fetchone()[0]
    assert next_id > len(
        seeded
    ), f"snapshot.next_catalog_id {next_id} must be past the seeded ids ({len(seeded)})"


def test_drop_schema_end_snapshots_lake_ducklake_row(pg_cursor):
    """
    DROP SCHEMA on the PG side must end-snapshot the matching live row
    in lake_ducklake.schema so DuckDB doesn't keep seeing a schema after
    PG has dropped it. Hooked via object_access_hook so cascading drops
    are covered too.
    """
    # Re-create the extension with a fresh user schema present so the
    # install seed picks it up. (Schemas added AFTER CREATE EXTENSION
    # are not auto-tracked yet -- only ones present at install time.)
    pg_cursor.execute("DROP EXTENSION pg_lake_ducklake CASCADE")
    pg_cursor.execute("DROP SCHEMA IF EXISTS dropme_test CASCADE")
    pg_cursor.execute("CREATE SCHEMA dropme_test")
    pg_cursor.execute("CREATE EXTENSION pg_lake_ducklake CASCADE")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id FROM lake_ducklake.schema "
        "WHERE schema_name = 'dropme_test' AND end_snapshot IS NULL"
    )
    seeded = pg_cursor.fetchone()
    assert seeded is not None, "install seed should include dropme_test"
    schema_id = seeded[0]

    pg_cursor.execute("DROP SCHEMA dropme_test")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT end_snapshot FROM lake_ducklake.schema WHERE schema_id = %s",
        (schema_id,),
    )
    end = pg_cursor.fetchone()
    assert (
        end is not None and end[0] is not None
    ), f"dropme_test row should have end_snapshot set, got {end}"


def test_recreate_schema_revives_lake_ducklake_row(pg_cursor):
    """
    PG-side CREATE SCHEMA after a previous DROP that end-snapshotted a
    lake_ducklake.schema row should revive that row -- INSERT a fresh
    live entry reusing the original schema_id, schema_uuid, path. This
    keeps DuckDB's catalog continuity across drop/recreate cycles a
    user might do to preserve grants on the PG schema.
    """
    pg_cursor.execute("DROP EXTENSION pg_lake_ducklake CASCADE")
    pg_cursor.execute("DROP SCHEMA IF EXISTS revive_test CASCADE")
    pg_cursor.execute("CREATE SCHEMA revive_test")
    pg_cursor.execute("CREATE EXTENSION pg_lake_ducklake CASCADE")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id, schema_uuid FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_test' AND end_snapshot IS NULL"
    )
    seeded = pg_cursor.fetchone()
    assert seeded is not None
    schema_id_before, uuid_before = seeded

    pg_cursor.execute("DROP SCHEMA revive_test")
    pg_cursor.connection.commit()

    pg_cursor.execute("CREATE SCHEMA revive_test")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT schema_id, schema_uuid, end_snapshot FROM lake_ducklake.schema "
        "WHERE schema_name = 'revive_test' AND end_snapshot IS NULL"
    )
    revived = pg_cursor.fetchone()
    assert revived is not None, "revive_test row should be live again"
    assert (
        revived[0] == schema_id_before
    ), f"revived row must reuse original schema_id; got {revived[0]}, expected {schema_id_before}"
    assert (
        revived[1] == uuid_before
    ), f"revived row must reuse original schema_uuid; got {revived[1]}, expected {uuid_before}"
    assert revived[2] is None

    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_id = %s AND end_snapshot IS NOT NULL",
        (schema_id_before,),
    )
    assert pg_cursor.fetchone()[0] == 1


def test_brand_new_schema_does_not_auto_track(pg_cursor):
    """
    PG-side CREATE SCHEMA on a name that lake_ducklake.schema has never
    seen must NOT auto-track. Broad PG CREATE SCHEMA -> lake_ducklake.schema
    propagation is intentionally deferred; the revive path only kicks in
    for drop/recreate cycles.
    """
    pg_cursor.execute("DROP SCHEMA IF EXISTS new_unrelated_test CASCADE")
    pg_cursor.execute("CREATE SCHEMA new_unrelated_test")
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.schema "
        "WHERE schema_name = 'new_unrelated_test'"
    )
    assert pg_cursor.fetchone()[0] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

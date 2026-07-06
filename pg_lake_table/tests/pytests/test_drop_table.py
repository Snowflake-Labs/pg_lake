import pytest
from utils_pytest import *


def test_drop_writable_pg_lake_table(s3, pg_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_drop_table/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_drop_writable_pg_lake_table(id int)
         SERVER pg_lake
         OPTIONS (location '{location}', writable 'true', format 'parquet')
    """,
        pg_conn,
    )

    run_command("DROP TABLE test_drop_writable_pg_lake_table", pg_conn)
    pg_conn.rollback()


def test_drop_readonly_pg_lake_table(s3, pg_conn, extension):
    path = f"s3://{TEST_BUCKET}/test_drop_table/data.parquet"

    run_command(
        f"""
        COPY (SELECT i AS id FROM generate_series(1, 10) i) TO '{path}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_drop_readonly_pg_lake_table(id int)
         SERVER pg_lake
         OPTIONS (path '{path}')
    """,
        pg_conn,
    )

    run_command("DROP TABLE test_drop_readonly_pg_lake_table", pg_conn)
    pg_conn.rollback()


def test_drop_iceberg(s3, pg_conn, extension, with_default_location):
    error = run_command(
        f"""
        CREATE TABLE test_drop_iceberg USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command("DROP TABLE test_drop_iceberg", pg_conn)
    pg_conn.rollback()


def test_drop_iceberg_removed_from_catalog(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    error = run_command(
        f"""
        CREATE TABLE test_drop_iceberg_removed_from_catalog USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i;

        DELETE FROM test_drop_iceberg_removed_from_catalog WHERE id = 5;
    """,
        pg_conn,
    )
    pg_conn.commit()

    table_oid = run_query(
        "SELECT 'test_drop_iceberg_removed_from_catalog'::regclass::oid", pg_conn
    )[0][0]

    # assume user messed with the catalog
    run_command(
        "DELETE FROM lake_iceberg.tables_internal WHERE table_name = 'test_drop_iceberg_removed_from_catalog'::regclass",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("DROP TABLE test_drop_iceberg_removed_from_catalog", pg_conn)
    pg_conn.commit()

    # now, show that we do not have entries in the other catalogs
    results = run_query(
        f"SELECT * FROM lake_table.files WHERE table_name = {table_oid}::regclass",
        superuser_conn,
    )
    assert len(results) == 0

    results = run_query(
        f"SELECT * FROM lake_table.field_id_mappings WHERE table_name = {table_oid}::regclass",
        superuser_conn,
    )
    assert len(results) == 0

    results = run_query(
        f"SELECT * FROM lake_table.data_file_column_stats WHERE table_name = {table_oid}::regclass",
        superuser_conn,
    )
    assert len(results) == 0

    results = run_query(
        f"SELECT * FROM lake_table.deletion_file_map WHERE table_name = {table_oid}::regclass",
        superuser_conn,
    )
    assert len(results) == 0


def test_drop_iceberg_with_no_data(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_drop_iceberg_with_no_data USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
        WITH NO DATA
    """,
        pg_conn,
    )

    run_command("DROP TABLE test_drop_iceberg_with_no_data", pg_conn)
    pg_conn.rollback()


def test_drop_multiple_pg_lake_tables(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_drop1 USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_drop2 USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command("DROP TABLE test_drop1, test_drop2", pg_conn)
    pg_conn.rollback()


def test_drop_pg_lake_table_with_regular_table(
    s3, pg_conn, extension, with_default_location
):
    location = f"s3://{TEST_BUCKET}/test_drop_table/"

    run_command(
        f"""
        CREATE TABLE test_drop1
        AS SELECT i AS id, 'pg_lake' AS name
           FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_drop2
        AS SELECT i AS id, 'pg_lake' AS name
           FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_drop_pg_lake_table1 USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_drop_pg_lake_table2 USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command(
        "DROP TABLE test_drop_pg_lake_table1, test_drop_pg_lake_table2, test_drop1, test_drop2",
        pg_conn,
    )
    pg_conn.rollback()


def test_drop_if_exists(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_drop USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command("DROP TABLE IF EXISTS non_existent, test_drop", pg_conn)
    pg_conn.rollback()


def test_drop_not_exist(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_drop USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    error = run_command(
        "DROP TABLE non_existent, test_drop", pg_conn, raise_error=False
    )

    assert 'table "non_existent" does not exist' in error

    pg_conn.rollback()


def test_drop_cascade(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_drop1 USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE test_drop2 USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command("DROP TABLE test_drop1, test_drop2 CASCADE", pg_conn)
    pg_conn.rollback()


def test_drop_unusual_name(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        CREATE TABLE \"test_   7=unuSual_table_name\" USING iceberg
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i
    """,
        pg_conn,
    )

    run_command('DROP TABLE "test_   7=unuSual_table_name"', pg_conn)
    pg_conn.rollback()


def test_drop_partition_table(s3, pg_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_drop_table/"

    run_command(
        f"""
        CREATE TABLE test_drop_partitioned(id INT, name TEXT)
        PARTITION BY RANGE (id)
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_drop_partition_1
        PARTITION OF test_drop_partitioned
        FOR VALUES FROM (0) TO (1001)
        SERVER pg_lake
        OPTIONS (writable 'true', format 'parquet', location '{location}_1');
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_drop_partition_2
        PARTITION OF test_drop_partitioned
        FOR VALUES FROM (1001) TO (2001)
        SERVER pg_lake
        OPTIONS (writable 'true', format 'parquet', location '{location}_2');
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_drop_partition_3
        PARTITION OF test_drop_partitioned
        FOR VALUES FROM (2001) TO (3001)
        SERVER pg_lake
        OPTIONS (writable 'true', format 'parquet', location '{location}_3');
    """,
        pg_conn,
    )

    query = """SELECT * FROM test_drop_partition_1 ORDER BY id"""
    expected_expression = "ORDER BY"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    query = """SELECT relname FROM pg_class
                WHERE relname IN ('test_drop_partitioned',
                                  'test_drop_partition_1',
                                  'test_drop_partition_2',
                                  'test_drop_partition_3')
                ORDER BY relname
            """

    result = run_query(query, pg_conn)
    assert result == [
        [
            "test_drop_partition_1",
        ],
        [
            "test_drop_partition_2",
        ],
        [
            "test_drop_partition_3",
        ],
        [
            "test_drop_partitioned",
        ],
    ]

    # drop a single partition
    run_command("DROP TABLE test_drop_partition_2", pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [
            "test_drop_partition_1",
        ],
        [
            "test_drop_partition_3",
        ],
        [
            "test_drop_partitioned",
        ],
    ]

    # drop the parent table
    run_command("DROP TABLE test_drop_partitioned", pg_conn)

    result = run_query(query, pg_conn)
    assert result == []
    pg_conn.rollback()


def test_drop_without_s3_access_cached(
    s3_server,
    pg_conn,
    pgduck_conn,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    run_command(
        f"""
        CREATE SCHEMA test_drop_without_s3_access_cached;

        CREATE TABLE test_drop_without_s3_access_cached.test_drop_iceberg USING iceberg
        WITH (autovacuum_enabled='False')
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i;
        INSERT INTO test_drop_without_s3_access_cached.test_drop_iceberg
            SELECT i AS id, 'pg_lake' AS name
                FROM generate_series(1, 10) i;

        UPDATE test_drop_without_s3_access_cached.test_drop_iceberg SET id = 100 WHERE id = 1;
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "UPDATE test_drop_without_s3_access_cached.test_drop_iceberg SET id = 101 WHERE id = 100;",
        pg_conn,
    )
    pg_conn.commit()

    # make sure to add all files to the cache
    res = run_query(
        f"""
            SELECT path, lake_file_cache.add(path)
            FROM
              (SELECT path
               FROM lake_iceberg.find_all_referenced_files(
                                            (SELECT metadata_location
                                             FROM iceberg_tables
                                             WHERE TABLE_NAME = 'test_drop_iceberg' AND
                                                    TABLE_NAMESPACE = 'test_drop_without_s3_access_cached'))) AS files;
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_path = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE TABLE_NAME = 'test_drop_iceberg' AND TABLE_NAMESPACE = 'test_drop_without_s3_access_cached'",
        pg_conn,
    )[0][0]
    data_folder_path = metadata_path.split("metadata")[0] + "data/**"
    metadata_folder_path = metadata_path.split("metadata")[0] + "metadata/**"

    # stop the boto server
    s3_server.stop()

    # now, INSERT fails
    result = run_command(
        "INSERT INTO test_drop_without_s3_access_cached.test_drop_iceberg VALUES (1)",
        pg_conn,
        raise_error=False,
    )
    assert "Could not establish connection error" in result
    pg_conn.rollback()

    # still, we can drop the table/schema
    run_command("DROP SCHEMA test_drop_without_s3_access_cached CASCADE", pg_conn)
    pg_conn.commit()

    # for the rest of the tests, re-start the boto
    s3_server.start()

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=0",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        f"VACUUM (ICEBERG)",
    ]
    run_command_outside_tx(vacuum_commands)

    # now, make sure data files are removed after VACUUM
    results = run_query(f"SELECT * FROM lake_file.list('{data_folder_path}')", pg_conn)

    assert len(results) == 0

    del_queue = run_query("SELECT * FROM lake_engine.deletion_queue", pg_conn)
    print(del_queue)

    # and, we could remove the metadata files
    results = run_query(
        f"SELECT * FROM lake_file.list('{metadata_folder_path}')", pg_conn
    )
    assert len(results) == 0


def test_drop_without_s3_access_not_cached(
    s3_server,
    pg_conn,
    pgduck_conn,
    superuser_conn,
    extension,
    with_default_location,
    create_test_helper_functions,
):
    run_command(
        f"""
        CREATE SCHEMA test_drop_without_s3_access_not_cached;

        CREATE TABLE test_drop_without_s3_access_not_cached.test_drop_iceberg USING iceberg
        WITH (autovacuum_enabled='False')
        AS SELECT i AS id, 'pg_lake' AS name
            FROM generate_series(1, 10) i;

        INSERT INTO test_drop_without_s3_access_not_cached.test_drop_iceberg
            SELECT i AS id, 'pg_lake' AS name
                FROM generate_series(1, 10) i;

        UPDATE test_drop_without_s3_access_not_cached.test_drop_iceberg SET id = 100 WHERE id = 1;
        UPDATE test_drop_without_s3_access_not_cached.test_drop_iceberg SET id = 101 WHERE id = 100;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_path = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE TABLE_NAME = 'test_drop_iceberg' AND TABLE_NAMESPACE = 'test_drop_without_s3_access_not_cached'",
        pg_conn,
    )[0][0]
    data_folder_path = metadata_path.split("metadata")[0] + "data/**"
    metadata_folder_path = metadata_path.split("metadata")[0] + "metadata/**"

    # now, make sure data files are removed after VACUUM
    all_files_before_vacuum = run_query(
        f"SELECT path FROM lake_iceberg.find_all_referenced_files('{metadata_path}')",
        pg_conn,
    )
    assert len(all_files_before_vacuum) > 0

    # make sure to add all files to the cache
    res = run_query(
        f"""
            SELECT path, lake_file_cache.remove(path)
            FROM
              (SELECT path
               FROM lake_iceberg.find_all_referenced_files('{metadata_path}')) AS files;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # stop the boto server
    s3_server.stop()

    # now, INSERT fails
    result = run_command(
        "INSERT INTO test_drop_without_s3_access_not_cached.test_drop_iceberg VALUES (1)",
        pg_conn,
        raise_error=False,
    )
    assert "Could not establish connection error" in result
    pg_conn.rollback()

    # still, we can drop the table/schema
    run_command("DROP SCHEMA test_drop_without_s3_access_not_cached CASCADE", pg_conn)
    pg_conn.commit()

    # for the rest of the tests, re-start the boto
    s3_server.start()

    # now, make sure data files are still there
    results = run_query(f"SELECT * FROM lake_file.list('{data_folder_path}')", pg_conn)
    assert len(results) > 0

    vacuum_commands = [
        "SET pg_lake_engine.orphaned_file_retention_period=0",
        "SET pg_lake_iceberg.max_snapshot_age TO 0",
        f"VACUUM (ICEBERG)",
    ]
    run_command_outside_tx(vacuum_commands)

    # now, make sure data files are removed after VACUUM
    results = run_query(f"SELECT * FROM lake_file.list('{data_folder_path}')", pg_conn)
    assert len(results) == 0

    # and, we could remove the metadata files
    results = run_query(
        f"SELECT * FROM lake_file.list('{metadata_folder_path}')", pg_conn
    )
    assert len(results) == 0


# Two distinct injection points, one per referenced-files entrypoint, so a test
# can arm exactly the path it exercises:
#   - the eager, drop-time C enumeration (IcebergFindAllReferencedFiles)
#   - the SQL find_all_referenced_files UDF the deferred VACUUM resolution
#     invokes over SPI
ENUMERATE_REFERENCED_FILES_INJECTION_POINT = "iceberg-find-referenced-files"
RESOLVE_REFERENCED_FILES_INJECTION_POINT = "iceberg-find-referenced-files-udf"


def _writable_location(conn, qualified_table):
    """
    Return the exact location string GetWritableTableLocation() computes for a
    writable pg_lake foreign table: the 'location' foreign-table option with
    query arguments stripped and a single trailing slash removed. This is the
    prefix the deferred-drop path enqueues into the deletion queue, so tests
    assert against the identical string.
    """
    rows = run_query(
        f"""
        SELECT option_value
        FROM (
          SELECT (pg_catalog.pg_options_to_table(ftoptions)).*
          FROM pg_catalog.pg_foreign_table
          WHERE ftrelid = '{qualified_table}'::regclass
        ) o
        WHERE option_name = 'location'
        """,
        conn,
    )

    location = rows[0][0]

    # replicate GetWritableTableLocation: strip '?...' query args, then one '/'
    location = location.split("?", 1)[0]
    if location.endswith("/"):
        location = location[:-1]

    return location


DEFER_GUC = "pg_lake_table.defer_drop_file_cleanup"


def _drop_deferred(conn, drop_sql, commit=True):
    """Run drop_sql with deferred file cleanup enabled. SET LOCAL keeps the
    GUC scoped to this single transaction (matching how a caller turns it on
    only around its bulk drop), so it never leaks to later tests and rolls
    back cleanly."""
    run_command(f"SET LOCAL {DEFER_GUC} = on", conn)
    run_command(drop_sql, conn)
    if commit:
        conn.commit()


def _count_prefix_records(superuser_conn, location):
    return run_query(
        f"SELECT count(*) FROM lake_engine.deletion_queue "
        f"WHERE path = '{location}' AND is_prefix",
        superuser_conn,
    )[0][0]


def _count_resolve_records(superuser_conn, location):
    """Rows queued by a deferred drop: a single metadata.json flagged for
    resolution (is_prefix = false, resolve_metadata = true)."""
    return run_query(
        f"SELECT count(*) FROM lake_engine.deletion_queue "
        f"WHERE resolve_metadata AND path LIKE '{location}%'",
        superuser_conn,
    )[0][0]


def _count_data_file_records(superuser_conn, location):
    """Per-file rows for actual data files (under <location>/data/). These only
    appear once the referenced files have been enumerated -- eagerly by a
    normal drop, or by VACUUM after it resolves a deferred drop's metadata."""
    return run_query(
        f"SELECT count(*) FROM lake_engine.deletion_queue "
        f"WHERE is_prefix = false AND resolve_metadata = false "
        f"AND path LIKE '{location}/data/%'",
        superuser_conn,
    )[0][0]


def _count_file_records(superuser_conn, location):
    return run_query(
        f"SELECT count(*) FROM lake_engine.deletion_queue "
        f"WHERE is_prefix = false AND resolve_metadata = false "
        f"AND path LIKE '{location}%'",
        superuser_conn,
    )[0][0]


def _vacuum_iceberg_now():
    """Force VACUUM to immediately drain the deletion queue and delete the
    referenced object-store files (retention + snapshot age set to 0)."""
    run_command_outside_tx(
        [
            "SET pg_lake_engine.orphaned_file_retention_period = 0",
            "SET pg_lake_iceberg.max_snapshot_age TO 0",
            "VACUUM (ICEBERG)",
        ]
    )


def _assert_vacuum_drains(superuser_conn, location):
    """Run VACUUM and assert it actually removed all queue rows (prefix,
    per-file, and any deferred resolve_metadata row) and the underlying
    object-store files for the given location."""
    _vacuum_iceberg_now()
    assert _count_prefix_records(superuser_conn, location) == 0
    assert _count_resolve_records(superuser_conn, location) == 0
    assert _count_file_records(superuser_conn, location) == 0
    assert (
        run_query(
            f"SELECT count(*) FROM lake_file.list('{location}/**')", superuser_conn
        )[0][0]
        == 0
    )
    superuser_conn.commit()


def _attach_injection_error(superuser_conn, point):
    run_command(
        f"SELECT public.injection_points_attach('{point}', 'error')",
        superuser_conn,
    )
    superuser_conn.commit()


def _detach_injection(superuser_conn, point):
    run_command(
        f"SELECT public.injection_points_detach('{point}')",
        superuser_conn,
    )
    superuser_conn.commit()


def test_deferred_drop_skips_enumeration(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
):
    """With deferred cleanup enabled for a default-location table, DROP must
    skip the object-store file enumeration entirely and instead queue the
    table's metadata.json as a single resolve_metadata row (which VACUUM later
    resolves into per-file deletions and drains).

    The signal is that the drop leaves exactly one resolve_metadata row and no
    data-file rows: a normal (non-deferred) drop would enumerate and enqueue
    per-file rows up front instead (see test_normal_drop_enumerates_control).
    No storage prefix is ever queued -- deletion stays file-accurate."""
    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_skip", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_skip.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    # second commit -> a second data file / snapshot, so enumeration would be
    # real work if it were reached
    run_command(
        "INSERT INTO defer_drop_skip.t SELECT i, 'pg_lake' FROM generate_series(11, 20) i",
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_skip.t")
    table_oid = run_query("SELECT 'defer_drop_skip.t'::regclass::oid", pg_conn)[0][0]

    # snapshot the referenced files up-front so we can prove they still exist
    # right after the (deferred) drop and only disappear after VACUUM
    files_before = run_query(
        f"SELECT count(*) FROM lake_file.list('{location}/**')", pg_conn
    )[0][0]
    assert files_before > 0

    _drop_deferred(pg_conn, "DROP TABLE defer_drop_skip.t")

    # table is gone
    assert (
        run_query(
            "SELECT count(*) FROM pg_class "
            "WHERE relname = 't' AND relnamespace = 'defer_drop_skip'::regnamespace",
            superuser_conn,
        )[0][0]
        == 0
    )

    # the deferred drop queued exactly one metadata.json for resolution, no
    # data-file rows (enumeration skipped) and no rm -rf prefix row
    assert _count_resolve_records(superuser_conn, location) == 1
    assert _count_data_file_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0

    # that single queued row is the table's metadata.json, present exactly
    # once. find_all_referenced_files returns the metadata.json itself, so
    # resolution must not duplicate it: the INSERT ... ON CONFLICT DO NOTHING
    # plus in-place UPDATE in ExpandMetadataResolveRecord keeps it one row.
    metadata_rows = run_query(
        f"SELECT path FROM lake_engine.deletion_queue "
        f"WHERE resolve_metadata AND path LIKE '{location}%'",
        superuser_conn,
    )
    assert len(metadata_rows) == 1
    assert metadata_rows[0][0].endswith(".metadata.json")

    # local pg_lake catalog state is still cleaned up
    assert (
        run_query(
            f"SELECT count(*) FROM lake_table.files WHERE table_name = {table_oid}",
            superuser_conn,
        )[0][0]
        == 0
    )

    # files were NOT deleted by the drop itself (deferred)
    assert (
        run_query(f"SELECT count(*) FROM lake_file.list('{location}/**')", pg_conn)[0][
            0
        ]
        > 0
    )

    # VACUUM resolves the metadata, deletes the referenced files, and drains
    # every queue row for this table
    _assert_vacuum_drains(superuser_conn, location)

    run_command("DROP SCHEMA IF EXISTS defer_drop_skip CASCADE", pg_conn)
    pg_conn.commit()


def test_normal_drop_enumerates_control(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    """Control: with deferral off (the default), DROP enumerates referenced
    files up front and enqueues per-file (is_prefix=false) records -- no
    resolve_metadata row and no prefix."""
    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_control", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_control.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_control.t")

    run_command("DROP TABLE defer_drop_control.t", pg_conn)
    pg_conn.commit()

    # enumeration ran up front: data-file records were created, no deferred
    # resolve_metadata row and no prefix record
    assert _count_data_file_records(superuser_conn, location) > 0
    assert _count_resolve_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0

    # VACUUM actually drains the per-file records and deletes the files
    _assert_vacuum_drains(superuser_conn, location)

    run_command("DROP SCHEMA IF EXISTS defer_drop_control CASCADE", pg_conn)
    pg_conn.commit()


def test_injection_point_on_enumeration_path(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    create_injection_extension,
):
    """Guards that the injection point actually sits on the enumeration path:
    with it attached, a normal (non-deferred) drop must take the
    blob-store-failure fallback and enqueue a single prefix record instead of
    per-file records."""
    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_inj", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_inj.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_inj.t")

    _attach_injection_error(superuser_conn, ENUMERATE_REFERENCED_FILES_INJECTION_POINT)
    try:
        run_command("DROP TABLE defer_drop_inj.t", pg_conn)
    finally:
        _detach_injection(superuser_conn, ENUMERATE_REFERENCED_FILES_INJECTION_POINT)
    pg_conn.commit()

    # enumeration was reached (injection fired), caught, and fell back to a
    # single prefix record -- proving the injection point is on that path.
    # This is the last-resort unreachable-blob-store fallback, not the deferred
    # path, so no resolve_metadata row is queued.
    assert _count_prefix_records(superuser_conn, location) == 1
    assert _count_resolve_records(superuser_conn, location) == 0
    assert _count_file_records(superuser_conn, location) == 0

    # VACUUM actually drains the queued prefix and deletes the files
    _assert_vacuum_drains(superuser_conn, location)

    run_command("DROP SCHEMA IF EXISTS defer_drop_inj CASCADE", pg_conn)
    pg_conn.commit()


def test_deferred_drop_resolution_failure_retries(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
    create_injection_extension,
):
    """If VACUUM cannot resolve a deferred-drop metadata.json (e.g. the object
    store is unreachable), the resolve_metadata row must survive for a later
    retry -- it must not be lost, nor expanded into partial per-file rows.

    We force the failure with the injection point on the SQL
    find_all_referenced_files UDF (the SPI entrypoint the deferred resolution
    calls), assert the row survives with a bumped retry_count and the files are
    untouched, then detach and prove the next VACUUM resolves and fully drains.
    This exercises the ExpandMetadataResolveRecord subtransaction rollback."""
    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_fail", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_fail.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_fail.t")

    _drop_deferred(pg_conn, "DROP TABLE defer_drop_fail.t")

    # deferred drop queued exactly one metadata.json for resolution
    assert _count_resolve_records(superuser_conn, location) == 1
    files_before = run_query(
        f"SELECT count(*) FROM lake_file.list('{location}/**')", superuser_conn
    )[0][0]
    assert files_before > 0
    superuser_conn.commit()

    # force resolution to fail: VACUUM resolves the row by calling
    # find_all_referenced_files over SPI, which now errors under the injection
    _attach_injection_error(superuser_conn, RESOLVE_REFERENCED_FILES_INJECTION_POINT)
    try:
        _vacuum_iceberg_now()
    finally:
        _detach_injection(superuser_conn, RESOLVE_REFERENCED_FILES_INJECTION_POINT)

    # resolution failed inside its own subtransaction and was swallowed as a
    # WARNING: the resolve_metadata row still stands, no per-file rows were
    # produced, no prefix was queued, and the files are all still there
    assert _count_resolve_records(superuser_conn, location) == 1
    assert _count_data_file_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0
    assert (
        run_query(
            f"SELECT count(*) FROM lake_file.list('{location}/**')", superuser_conn
        )[0][0]
        == files_before
    )

    # the failed attempt bumped retry_count on the surviving row (so it is
    # retried later, and eventually gives up after VacuumFileRemoveMaxRetries)
    assert (
        run_query(
            f"SELECT max(retry_count) FROM lake_engine.deletion_queue "
            f"WHERE resolve_metadata AND path LIKE '{location}%'",
            superuser_conn,
        )[0][0]
        > 0
    )
    superuser_conn.commit()

    # with the injection detached, the next VACUUM resolves and fully drains
    _assert_vacuum_drains(superuser_conn, location)

    run_command("DROP SCHEMA IF EXISTS defer_drop_fail CASCADE", pg_conn)
    pg_conn.commit()


def test_deferred_drop_rollback_is_clean(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
):
    """Rolling back a deferred DROP must leave the table intact and add no new
    deletion-queue rows (deferral is fully transactional)."""
    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_rollback", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_rollback.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_rollback.t")

    # SET LOCAL + resolve_metadata enqueue + DROP all happen in one transaction
    # that we then roll back, so nothing may persist.
    _drop_deferred(pg_conn, "DROP TABLE defer_drop_rollback.t", commit=False)
    pg_conn.rollback()

    # table survived the rollback and is still queryable
    assert run_query("SELECT count(*) FROM defer_drop_rollback.t", pg_conn)[0][0] == 10

    # the queued resolve_metadata row was rolled back with the aborted
    # transaction; no rows of any kind persist
    assert _count_resolve_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0
    assert _count_file_records(superuser_conn, location) == 0

    run_command("DROP SCHEMA IF EXISTS defer_drop_rollback CASCADE", pg_conn)
    pg_conn.commit()


def test_deferred_drop_under_drop_schema_cascade(
    s3,
    pg_conn,
    superuser_conn,
    extension,
    with_default_location,
):
    """DROP SCHEMA ... CASCADE must also take the deferred path for a
    default-location table when deferral is enabled."""
    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_cascade", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_cascade.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_cascade.t")

    _drop_deferred(pg_conn, "DROP SCHEMA defer_drop_cascade CASCADE")

    # schema and table are gone
    assert (
        run_query(
            "SELECT count(*) FROM pg_namespace WHERE nspname = 'defer_drop_cascade'",
            superuser_conn,
        )[0][0]
        == 0
    )

    # deferred path taken: exactly one queued metadata.json for resolution,
    # no data-file rows and no prefix row
    assert _count_resolve_records(superuser_conn, location) == 1
    assert _count_data_file_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0

    # VACUUM resolves the metadata, deletes the referenced files, and drains
    # every queue row for this table
    _assert_vacuum_drains(superuser_conn, location)


def test_custom_location_is_also_deferred(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    """Deferral works for a custom-location table too. Resolution deletes
    exactly the files the metadata.json references (never a whole prefix), so
    a location that might be shared with other tables is never over-deleted --
    hence there is no custom-location guard on the deferred path."""
    custom_location = f"s3://{TEST_BUCKET}/defer_custom_location/mytable"

    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_custom", pg_conn)
    run_command(
        f"""
        CREATE TABLE defer_drop_custom.t USING iceberg
        WITH (autovacuum_enabled='false', location='{custom_location}')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_custom.t")

    _drop_deferred(pg_conn, "DROP TABLE defer_drop_custom.t")

    # deferred path taken exactly like a default-location table: a single
    # queued metadata.json for resolution, no eager per-file rows, no prefix
    assert _count_resolve_records(superuser_conn, location) == 1
    assert _count_data_file_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0

    # VACUUM resolves the metadata, deletes the referenced files, and drains
    # every queue row for this table
    _assert_vacuum_drains(superuser_conn, location)

    run_command("DROP SCHEMA IF EXISTS defer_drop_custom CASCADE", pg_conn)
    pg_conn.commit()


def test_flush_deletion_queue_drains_dropped_table_files(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    """flush_deletion_queue drains a dropped Iceberg table's files end to end.

    A deferred DROP queues only the table's metadata.json (a resolve_metadata
    row); the object-store files still exist right after the drop. Draining the
    queue with lake_engine.flush_deletion_queue then resolves that metadata into
    the exact referenced files, deletes them, and removes the metadata.json row
    itself, so both the queue and the underlying S3 prefix end up empty. This is
    the same drain VACUUM performs, exercised directly through the immediate
    flush primitive.

    flush_deletion_queue runs a single RemoveDeletionQueueRecords pass, and a
    resolve_metadata row needs two: the first expands it into per-file rows, the
    second deletes them. So we loop until the queue is empty, exactly like the
    VACUUM drain loop does."""
    run_command("CREATE SCHEMA IF NOT EXISTS defer_drop_flush", pg_conn)
    run_command(
        """
        CREATE TABLE defer_drop_flush.t USING iceberg
        WITH (autovacuum_enabled='false')
        AS SELECT i AS id, 'pg_lake' AS name FROM generate_series(1, 10) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    # a second commit -> a second data file / snapshot, so resolution has real
    # per-file work to do
    run_command(
        "INSERT INTO defer_drop_flush.t SELECT i, 'pg_lake' FROM generate_series(11, 20) i",
        pg_conn,
    )
    pg_conn.commit()

    location = _writable_location(pg_conn, "defer_drop_flush.t")

    # the files exist in object storage before the drop
    files_before = run_query(
        f"SELECT count(*) FROM lake_file.list('{location}/**')", pg_conn
    )[0][0]
    assert files_before > 0

    _drop_deferred(pg_conn, "DROP TABLE defer_drop_flush.t")

    # deferred drop queued exactly one metadata.json, nothing deleted yet: the
    # files must all still be present in object storage
    assert _count_resolve_records(superuser_conn, location) == 1
    assert _count_data_file_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0
    assert (
        run_query(
            f"SELECT count(*) FROM lake_file.list('{location}/**')", superuser_conn
        )[0][0]
        == files_before
    )
    superuser_conn.commit()

    # drain the queue with flush_deletion_queue (retention 0 so nothing is held
    # back). flush_deletion_queue(0) flushes every table's rows; we loop because
    # a resolve_metadata row takes one pass to expand and a second to delete the
    # files it expands into.
    for _ in range(5):
        run_command_outside_tx(
            [
                "SET pg_lake_engine.orphaned_file_retention_period = 0",
                "SELECT lake_engine.flush_deletion_queue(0)",
            ]
        )
        remaining = run_query(
            "SELECT count(*) FROM lake_engine.deletion_queue", superuser_conn
        )[0][0]
        superuser_conn.commit()
        if remaining == 0:
            break

    # flush_deletion_queue removed every queue row for the dropped table ...
    assert _count_resolve_records(superuser_conn, location) == 0
    assert _count_data_file_records(superuser_conn, location) == 0
    assert _count_prefix_records(superuser_conn, location) == 0

    # ... and the underlying object-store files are gone from S3
    assert (
        run_query(
            f"SELECT count(*) FROM lake_file.list('{location}/**')", superuser_conn
        )[0][0]
        == 0
    )
    superuser_conn.commit()

    run_command("DROP SCHEMA IF EXISTS defer_drop_flush CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_test_helper_functions(superuser_conn, s3, extension):
    # lake_iceberg.find_all_referenced_files is installed by pg_lake_iceberg,
    # but the migration REVOKEs it from public (it walks arbitrary object-store
    # paths with the server's credentials). Tests below call it directly as the
    # non-superuser pg_conn, so grant EXECUTE back to public for the test run.
    run_command(
        "GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files(text) TO public",
        superuser_conn,
    )
    superuser_conn.commit()
    yield

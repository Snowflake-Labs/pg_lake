import pytest
from utils_pytest import *

TEST_TABLE_NAMESPACE = "test_logical_replication_nsp"


def test_logical_replication_with_all_tables_publication(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    """
    Comprehensive test that all catalog tables work with FOR ALL TABLES publication.

    This test creates a publication that includes all tables, then performs various
    operations that exercise all 12 catalog tables across 3 extensions to ensure they
    all have REPLICA IDENTITY FULL set correctly. This prevents the error:
    "cannot delete from table without replica identity" when dropping Iceberg tables.

    Catalog tables exercised:
    - lake_table: files, deletion_file_map, field_id_mappings, data_file_column_stats,
                  partition_specs, partition_fields, data_file_partition_values
    - lake_iceberg: tables_internal, tables_external
    - lake_engine: deletion_queue, in_progress_files
    """

    # Create publication with FOR ALL TABLES (requires superuser)
    run_command(
        "CREATE PUBLICATION test_all_tables_pub FOR ALL TABLES",
        superuser_conn,
    )
    superuser_conn.commit()

    # Create schema for testing
    run_command(f"CREATE SCHEMA {TEST_TABLE_NAMESPACE}", pg_conn)
    pg_conn.commit()

    # Test 1: Empty table drop (exercises tables_internal, field_id_mappings)
    run_command(
        f"""
        CREATE TABLE {TEST_TABLE_NAMESPACE}.empty_table (
            id int,
            name text
        ) USING iceberg
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.empty_table", pg_conn)
    pg_conn.commit()

    # Test 2: Simple Iceberg table with data
    # Exercises: tables_internal, field_id_mappings, files, data_file_column_stats
    run_command(
        f"""
        CREATE TABLE {TEST_TABLE_NAMESPACE}.simple_table (
            id int,
            name text
        ) USING iceberg
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {TEST_TABLE_NAMESPACE}.simple_table
        SELECT i, 'name_' || i::text
        FROM generate_series(1, 100) i
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Test 3: Positional deletes (exercises deletion_file_map)
    # Delete only 1 row to ensure positional delete (merge-on-read)
    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.simple_table WHERE id = 5",
        pg_conn,
    )
    pg_conn.commit()

    # Test 4: Partitioned table with multiple partitions
    # Exercises: partition_specs, partition_fields, data_file_partition_values
    run_command(
        f"""
        CREATE TABLE {TEST_TABLE_NAMESPACE}.partitioned_table (
            id int,
            category text,
            created_at timestamp,
            value numeric
        ) USING iceberg
        WITH (partition_by='category')
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {TEST_TABLE_NAMESPACE}.partitioned_table
        SELECT i, 'cat_' || (i % 3)::text, now() - interval '1 day' * (i % 5), i * 10.5
        FROM generate_series(1, 50) i
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Test 5: Trigger deletion_queue
    # Exercises: deletion_queue
    # Create unreferenced files via UPDATE (copy-on-write)
    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.simple_table SET name = 'updated' WHERE id > 150",
        pg_conn,
    )
    pg_conn.commit()

    # Verify deletion_queue has entries
    deletion_queue_entries = run_query(
        f"SELECT count(*) FROM lake_engine.deletion_queue WHERE table_name = '{TEST_TABLE_NAMESPACE}.simple_table'::regclass",
        superuser_conn,
    )
    assert deletion_queue_entries[0][0] > 0, "Expected entries in deletion_queue"

    # Test 6: Trigger in_progress_files
    # Exercises: in_progress_files
    # Perform INSERT in transaction then ROLLBACK
    run_command("BEGIN", pg_conn)
    run_command(
        f"""
        INSERT INTO {TEST_TABLE_NAMESPACE}.partitioned_table
        SELECT i, 'cat_c', now(), i * 10.5
        FROM generate_series(100, 200) i
    """,
        pg_conn,
    )
    run_command("ROLLBACK", pg_conn)

    # Verify in_progress_files has entries
    in_progress_entries = run_query(
        f"SELECT count(*) FROM lake_engine.in_progress_files WHERE path ILIKE '%{TEST_TABLE_NAMESPACE}%'",
        superuser_conn,
    )
    assert in_progress_entries[0][0] > 0, "Expected entries in in_progress_files"

    # Clean up in_progress_files
    run_command_outside_tx([f"VACUUM {TEST_TABLE_NAMESPACE}.partitioned_table"])

    # Test 7: Multiple tables simultaneously
    # Create and drop multiple tables to test concurrent catalog operations
    for i in range(3):
        run_command(
            f"""
            CREATE TABLE {TEST_TABLE_NAMESPACE}.multi_table_{i} (
                id int,
                value numeric
            ) USING iceberg
        """,
            pg_conn,
        )
        run_command(
            f"""
            INSERT INTO {TEST_TABLE_NAMESPACE}.multi_table_{i}
            SELECT j, j * 1.5 FROM generate_series(1, 20) j
        """,
            pg_conn,
        )
        pg_conn.commit()

    # Test 8: DROP TABLE operations - the critical test
    # This previously caused: "cannot delete from table without replica identity"
    # Drop all tables to ensure all catalog modifications work with CDC
    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.simple_table", pg_conn)
    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.partitioned_table", pg_conn)
    for i in range(3):
        run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.multi_table_{i}", pg_conn)
    pg_conn.commit()

    # Clean up schema and publication
    run_command(f"DROP SCHEMA {TEST_TABLE_NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()

    run_command("DROP PUBLICATION test_all_tables_pub", superuser_conn)
    superuser_conn.commit()

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
    assert "Could not connect to server error" in result
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
    assert "Could not connect to server error" in result
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


@pytest.fixture(scope="module")
def create_test_helper_functions(superuser_conn, s3, extension):
    run_command(
        f"""
     CREATE OR REPLACE FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text)
         RETURNS SETOF text
         LANGUAGE C
         STRICT
        AS 'pg_lake_iceberg', $function$find_all_referenced_files$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files(metadata_path text, OUT path text) TO public;

""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the functions after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.find_all_referenced_files;
""",
        superuser_conn,
    )
    superuser_conn.commit()

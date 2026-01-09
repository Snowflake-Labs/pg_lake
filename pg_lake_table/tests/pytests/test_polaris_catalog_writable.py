from utils_pytest import *
import server_params
from urllib.parse import quote
from urllib.parse import urlencode
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    IntegerType,
    DoubleType,
    StringType,
    BinaryType,
    FixedType,
    NestedField,
    ListType,
    StructType,
)
import pyarrow
from datetime import datetime, date, timezone
from urllib.parse import quote, quote_plus
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionSpec, PartitionField
import json


# pg_conn is to start Polaris server
def test_polaris_catalog_running(pg_conn, s3, polaris_session, installcheck):

    if installcheck:
        return

    """Fail fast if Polaris is not healthy."""
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/config?warehouse={server_params.PG_DATABASE}"
    resp = polaris_session.get(url, timeout=1)
    assert resp.ok, f"Polaris is not running: {resp.status_code} {resp.text}"


# fetch_data_files_used
def test_writable_rest_basic_flow(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command(f"""CREATE SCHEMA test_writable_rest_basic_flow""", pg_conn)
    run_command(
        f"""CREATE TABLE test_writable_rest_basic_flow.writable_rest USING iceberg WITH (catalog='REST') AS SELECT 100 AS a""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_writable_rest_basic_flow.writable_rest_2 USING iceberg WITH (catalog='rEst') AS SELECT 1000 AS a""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_writable_rest_basic_flow.unrelated_table(a int) USING iceberg""",
        pg_conn,
    )

    pg_conn.commit()

    run_command(
        f"""CREATE TABLE test_writable_rest_basic_flow.readable_rest() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_writable_rest_basic_flow.readable_rest_2() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest_2')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_basic_flow.readable_rest'::regclass and attnum > 0",
        pg_conn,
    )
    assert len(columns) == 1
    assert columns[0][0] == "a"

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_basic_flow.readable_rest_2'::regclass and attnum > 0",
        pg_conn,
    )
    assert len(columns) == 1
    assert columns[0][0] == "a"

    run_command(
        f"""INSERT INTO test_writable_rest_basic_flow.writable_rest VALUES (101)""",
        pg_conn,
    )

    run_command(
        f"""INSERT INTO test_writable_rest_basic_flow.writable_rest_2 VALUES (1001)""",
        pg_conn,
    )
    pg_conn.commit()

    res = run_query(
        "SELECT * FROM test_writable_rest_basic_flow.readable_rest ORDER BY a ASC",
        pg_conn,
    )
    assert len(res) == 2
    assert res[0][0] == 100
    assert res[1][0] == 101

    res = run_query(
        "SELECT * FROM test_writable_rest_basic_flow.readable_rest_2 ORDER BY a ASC",
        pg_conn,
    )
    assert len(res) == 2
    assert res[0][0] == 1000
    assert res[1][0] == 1001

    # now, each table modified twice in the same tx
    run_command(
        f"""
            INSERT INTO test_writable_rest_basic_flow.writable_rest VALUES (102);
            INSERT INTO test_writable_rest_basic_flow.writable_rest VALUES (103);

            INSERT INTO test_writable_rest_basic_flow.writable_rest_2 VALUES (1002);
            INSERT INTO test_writable_rest_basic_flow.writable_rest_2 VALUES (1003);

            INSERT INTO test_writable_rest_basic_flow.unrelated_table VALUES (2000);
        """,
        pg_conn,
    )
    pg_conn.commit()

    res = run_query(
        "SELECT * FROM test_writable_rest_basic_flow.readable_rest ORDER BY 1 ASC",
        pg_conn,
    )
    assert len(res) == 4
    assert res == [[100], [101], [102], [103]]

    res = run_query(
        "SELECT * FROM test_writable_rest_basic_flow.readable_rest_2 ORDER BY 1 ASC",
        pg_conn,
    )
    assert len(res) == 4
    assert res == [[1000], [1001], [1002], [1003]]

    # positional delete
    run_command(
        f"""
            INSERT INTO test_writable_rest_basic_flow.writable_rest SELECT i FROM generate_series(0,100)i;
            """,
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"""
            DELETE FROM test_writable_rest_basic_flow.writable_rest WHERE a = 15;
            """,
        pg_conn,
    )
    pg_conn.commit()

    # copy-on-write
    run_command(
        f"""
            UPDATE test_writable_rest_basic_flow.writable_rest SET a = a + 1 WHERE a > 10;
            """,
        pg_conn,
    )
    pg_conn.commit()

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_rest_basic_flow", "writable_rest", superuser_conn
    )

    run_command(f"""DROP SCHEMA test_writable_rest_basic_flow CASCADE""", pg_conn)
    pg_conn.commit()


def test_writable_rest_ddl(
    pg_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
    superuser_conn,
):

    if installcheck:
        return

    run_command(f"""CREATE SCHEMA test_writable_rest_ddl""", pg_conn)
    run_command("SET pg_lake_iceberg.default_catalog TO 'rest'", pg_conn)
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.writable_rest USING iceberg AS SELECT 100 AS a""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.writable_rest_2 USING iceberg WITH (catalog='rest') AS SELECT 1000 AS a""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.writable_rest_3 USING iceberg WITH (partition_by='a') AS SELECT 10000 AS a UNION SELECT 10001 as a""",
        pg_conn,
    )

    pg_conn.commit()

    # a DDL to a single table
    run_command(
        "ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN b INT", pg_conn
    )
    pg_conn.commit()
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_1() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_ddl.readable_rest_1'::regclass and attnum > 0 ORDER BY attnum ASC",
        pg_conn,
    )
    assert len(columns) == 2
    assert columns[0][0] == "a"
    assert columns[1][0] == "b"

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_rest_ddl", "writable_rest_3", superuser_conn
    )

    # multiple DDLs to a single table
    # a DDL to a single table
    run_command(
        """
                  ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN c INT;
                  ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN d INT;
                """,
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_2() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_ddl.readable_rest_2'::regclass and attnum > 0 ORDER BY attnum ASC",
        pg_conn,
    )
    assert len(columns) == 4
    assert columns[0][0] == "a"
    assert columns[1][0] == "b"
    assert columns[2][0] == "c"
    assert columns[3][0] == "d"

    # run multiple partition changes on a single table
    run_command(
        """
        ALTER TABLE test_writable_rest_ddl.writable_rest_3 OPTIONS (SET partition_by 'bucket(10,a)');
        ALTER TABLE test_writable_rest_ddl.writable_rest_3 OPTIONS (SET partition_by 'truncate(20,a)');
                """,
        pg_conn,
    )
    pg_conn.commit()

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_rest_ddl", "writable_rest_3", superuser_conn
    )

    # multiple DDLs to multiple tables
    run_command(
        """
                  ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN e INT;
                  ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN f INT;

                  ALTER TABLE test_writable_rest_ddl.writable_rest_2 ADD COLUMN b INT;
                  ALTER TABLE test_writable_rest_ddl.writable_rest_2 ADD COLUMN c INT;

                  ALTER TABLE test_writable_rest_ddl.writable_rest_3 OPTIONS (SET partition_by 'truncate(30,a)');
                  ALTER TABLE test_writable_rest_ddl.writable_rest_3 OPTIONS (SET partition_by 'truncate(20,a)');

                """,
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_3() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_ddl.readable_rest_3'::regclass and attnum > 0 ORDER BY attnum ASC",
        pg_conn,
    )
    assert len(columns) == 6
    assert columns[0][0] == "a"
    assert columns[1][0] == "b"
    assert columns[2][0] == "c"
    assert columns[3][0] == "d"
    assert columns[4][0] == "e"
    assert columns[5][0] == "f"

    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_4() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest_2')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_ddl.readable_rest_4'::regclass and attnum > 0 ORDER BY attnum ASC",
        pg_conn,
    )
    assert len(columns) == 3
    assert columns[0][0] == "a"
    assert columns[1][0] == "b"
    assert columns[2][0] == "c"

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_rest_ddl", "writable_rest_3", superuser_conn
    )

    # modify table and DDL on a single table
    run_command(
        """
                  ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN g INT;
                  INSERT INTO test_writable_rest_ddl.writable_rest (a,g) VALUES (101,101);
                  ALTER TABLE test_writable_rest_ddl.writable_rest OPTIONS (ADD partition_by 'truncate(30,a)');

                """,
        pg_conn,
    )
    pg_conn.commit()

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_rest_ddl", "writable_rest", superuser_conn
    )

    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_5() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_ddl.readable_rest_5'::regclass and attnum > 0 ORDER BY attnum ASC",
        pg_conn,
    )
    assert len(columns) == 7
    assert columns[0][0] == "a"
    assert columns[1][0] == "b"
    assert columns[2][0] == "c"
    assert columns[3][0] == "d"
    assert columns[4][0] == "e"
    assert columns[5][0] == "f"
    assert columns[6][0] == "g"

    # make sure modification is also successful
    res = run_query(
        "SELECT count(*) FROM test_writable_rest_ddl.readable_rest_5", pg_conn
    )
    assert res == [[2]]

    # one table modified, the other has DDL
    # modify table and DDL on a single table
    run_command(
        """
                  ALTER TABLE test_writable_rest_ddl.writable_rest_2 ADD COLUMN d INT;
                  INSERT INTO test_writable_rest_ddl.writable_rest (a,g) VALUES (101,101);
                """,
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_6() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest_2')""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_7() USING iceberg WITH (read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )

    columns = run_query(
        "SELECT attname FROM pg_attribute WHERE attrelid = 'test_writable_rest_ddl.readable_rest_6'::regclass and attnum > 0 ORDER BY attnum ASC",
        pg_conn,
    )
    assert len(columns) == 4
    assert columns[0][0] == "a"
    assert columns[1][0] == "b"
    assert columns[2][0] == "c"
    assert columns[3][0] == "d"

    # make sure modification is also successful
    res = run_query(
        "SELECT count(*) FROM test_writable_rest_ddl.readable_rest_7", pg_conn
    )
    assert res == [[3]]

    # Dropping partition by should be fine
    run_command(
        """
                  ALTER TABLE test_writable_rest_ddl.writable_rest_3 OPTIONS (DROP partition_by);

                """,
        pg_conn,
    )
    pg_conn.commit()
    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_rest_ddl", "writable_rest_3", superuser_conn
    )

    run_command(f"""DROP SCHEMA test_writable_rest_ddl CASCADE""", pg_conn)
    pg_conn.commit()
    run_command("RESET pg_lake_iceberg.default_catalog", pg_conn)


@pytest.mark.parametrize(
    "vacuum_syntax",
    ["table", "iceberg"],
)
def test_writable_rest_vacuum(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
    vacuum_syntax,
):

    if installcheck:
        return
    run_command("SET pg_lake_iceberg.default_catalog TO 'REST'", pg_conn)

    run_command(f"""CREATE SCHEMA test_writable_rest_vacuum""", pg_conn)
    run_command(
        f"""CREATE TABLE test_writable_rest_vacuum.writable_rest USING iceberg WITH (autovacuum_enabled=False) AS SELECT 100 AS a""",
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET pg_lake_iceberg.enable_manifest_merge_on_write = off", pg_conn)
    run_command("SET pg_lake_iceberg.manifest_min_count_to_merge = 2", pg_conn)
    run_command("SET pg_lake_table.vacuum_compact_min_input_files TO 2", pg_conn)

    for i in range(0, 3):
        run_command(
            f"""
            INSERT INTO test_writable_rest_vacuum.writable_rest
            SELECT s FROM generate_series(1,10) s;
        """,
            pg_conn,
        )
        pg_conn.commit()

    run_command_outside_tx(
        [
            "SET pg_lake_engine.orphaned_file_retention_period TO 180000",
            "SET pg_lake_iceberg.max_snapshot_age TO 0",
            f"VACUUM FULL test_writable_rest_vacuum.writable_rest",
        ],
        superuser_conn,
    )

    # make sure manifest compaction happens
    manifests = get_current_manifests(
        pg_conn, "test_writable_rest_vacuum", "writable_rest"
    )
    assert manifests == [[5, 0, 1, 31, 0, 0, 0, 0]]

    # make sure snapshot expiration happens
    # VACUUM pushes one more snapshot, so we end up with 2 snapshotss
    metadata = get_rest_table_metadata(
        "test_writable_rest_vacuum", "writable_rest", superuser_conn
    )
    snapshots = metadata["metadata"]["snapshots"]
    assert len(snapshots) == 2

    file_paths_q = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = 'test_writable_rest_vacuum.writable_rest'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) == 17

    if vacuum_syntax == "table":
        run_command_outside_tx(
            [
                "SET pg_lake_engine.orphaned_file_retention_period TO 0",
                f"VACUUM FULL test_writable_rest_vacuum.writable_rest",
            ],
            superuser_conn,
        )
    else:
        run_command_outside_tx(
            [
                "SET pg_lake_engine.orphaned_file_retention_period TO 0",
                f"VACUUM (FULL, ICEBERG)",
            ],
            superuser_conn,
        )

    file_paths_q = run_query(
        f"SELECT path FROM lake_engine.deletion_queue WHERE table_name = 'test_writable_rest_vacuum.writable_rest'::regclass",
        superuser_conn,
    )
    assert len(file_paths_q) == 0

    run_command("RESET pg_lake_iceberg.enable_manifest_merge_on_write", pg_conn)
    run_command("RESET pg_lake_iceberg.manifest_min_count_to_merge", pg_conn)
    run_command("RESET pg_lake_iceberg.max_snapshot_age", superuser_conn)
    run_command("RESET pg_lake_iceberg.orphaned_file_retention_period", superuser_conn)

    run_command("DROP SCHEMA test_writable_rest_vacuum CASCADE", pg_conn)
    pg_conn.commit()

    # ensure dropping schema drops the table properly
    ensure_table_dropped("test_writable_rest_vacuum", "writable_rest", superuser_conn)

    run_command("RESET pg_lake_iceberg.default_catalog", pg_conn)


def test_writable_drop_table(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):

    if installcheck:
        return

    # test 1: Create table, commit, drop table, ensure table removed from the catalog
    run_command(f"""CREATE SCHEMA test_writable_drop_table""", pg_conn)
    run_command(
        f"""CREATE TABLE test_writable_drop_table.writable_rest USING iceberg WITH (catalog='rest', autovacuum_enabled=False) AS SELECT 100 AS a""",
        pg_conn,
    )
    pg_conn.commit()

    run_command("DROP TABLE test_writable_drop_table.writable_rest", pg_conn)

    # make sure, before the commit, the table is there
    metadata = get_rest_table_metadata(
        "test_writable_drop_table", "writable_rest", superuser_conn
    )
    assert metadata is not None

    # make sure, after the commit, the table is gone
    pg_conn.commit()
    ensure_table_dropped("test_writable_drop_table", "writable_rest", superuser_conn)

    # now create two tables, drop one in the same tx, other at the end of the tx
    run_command(
        f"""
        CREATE TABLE test_writable_drop_table.writable_rest_1 USING iceberg WITH (catalog='rest', autovacuum_enabled=False) AS SELECT 100 AS a;
        CREATE TABLE test_writable_drop_table.writable_rest_2 (a int) USING iceberg WITH (catalog='rest', autovacuum_enabled=False);

        ALTER TABLE test_writable_drop_table.writable_rest_2 ADD COLUMN b INT;
        ALTER TABLE test_writable_drop_table.writable_rest_2 ADD COLUMN c INT;
        
        INSERT INTO test_writable_drop_table.writable_rest_1 VALUES (1);
        INSERT INTO test_writable_drop_table.writable_rest_2 VALUES (1, 2, 3);

        ALTER TABLE test_writable_drop_table.writable_rest_2 DROP COLUMN c;
        ALTER TABLE test_writable_drop_table.writable_rest_2 OPTIONS (ADD partition_by 'truncate(10, b)');

        INSERT INTO test_writable_drop_table.writable_rest_2 VALUES (1, 2);

        DROP TABLE test_writable_drop_table.writable_rest_2;
        """,
        pg_conn,
    )

    # before table creation committed, none should exist
    ensure_table_dropped("test_writable_drop_table", "writable_rest_1", superuser_conn)
    ensure_table_dropped("test_writable_drop_table", "writable_rest_2", superuser_conn)

    pg_conn.commit()

    metadata = get_rest_table_metadata(
        "test_writable_drop_table", "writable_rest_1", superuser_conn
    )
    assert metadata is not None
    ensure_table_dropped("test_writable_drop_table", "writable_rest_2", superuser_conn)

    run_command("DROP SCHEMA test_writable_drop_table CASCADE", pg_conn)
    pg_conn.commit()
    superuser_conn.commit()


# fetch_data_files_used
def test_different_table_types(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
    adjust_object_store_settings,
):

    if installcheck:
        return

    run_command(f"""CREATE SCHEMA test_different_table_types""", pg_conn)
    run_command(
        f"""CREATE TABLE test_different_table_types.writable_rest USING iceberg WITH (catalog='rest') AS SELECT 1 AS a""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_different_table_types.writable_object_store USING iceberg WITH (catalog='object_store') AS SELECT 2 AS a""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_different_table_types.postgres_catalog_iceberg_test USING iceberg AS SELECT 3 as a""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_different_table_types.heap_test AS SELECT 4 as a""",
        pg_conn,
    )

    pg_conn.commit()

    run_command(
        f"""CREATE TABLE test_different_table_types.readable_rest() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""CREATE TABLE test_different_table_types.writable_rest_2 USING iceberg WITH (catalog='rest') AS 

            SELECT * FROM test_different_table_types.writable_rest
                UNION ALL
            SELECT * FROM test_different_table_types.writable_object_store
                UNION ALL
            SELECT * FROM test_different_table_types.postgres_catalog_iceberg_test
                UNION ALL
            SELECT * FROM test_different_table_types.heap_test
                UNION ALL
            SELECT * FROM test_different_table_types.readable_rest
        """,
        pg_conn,
    )
    pg_conn.commit()

    res = run_query(
        "SELECT * FROM test_different_table_types.writable_rest_2 ORDER BY a DESC",
        pg_conn,
    )
    assert res == [[4], [3], [2], [1], [1]]

    run_command(
        """
        UPDATE test_different_table_types.writable_rest_2 bar SET a = foo.a + 1 
            FROM (  SELECT * FROM test_different_table_types.writable_rest
                        UNION ALL
                    SELECT * FROM test_different_table_types.writable_object_store
                        UNION ALL
                    SELECT * FROM test_different_table_types.postgres_catalog_iceberg_test
                        UNION ALL
                    SELECT * FROM test_different_table_types.heap_test
                        UNION ALL
                    SELECT * FROM test_different_table_types.readable_rest

            ) as foo WHERE foo.a = bar.a """,
        pg_conn,
    )

    res = run_query(
        "SELECT * FROM test_different_table_types.writable_rest_2 ORDER BY a DESC",
        pg_conn,
    )
    assert res == [[5], [4], [3], [2], [2]]

    pg_conn.commit()
    res = run_query(
        "SELECT * FROM test_different_table_types.writable_rest_2 ORDER BY a DESC",
        pg_conn,
    )
    assert res == [[5], [4], [3], [2], [2]]

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_different_table_types", "writable_rest_2", superuser_conn
    )
    superuser_conn.commit()

    run_command(f"""DROP SCHEMA test_different_table_types CASCADE""", pg_conn)
    pg_conn.commit()


# fetch_data_files_used
def test_writable_savepoints_truncate(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):
    if installcheck:
        return
    run_command(f"""CREATE SCHEMA test_writable_savepoints_truncate""", pg_conn)

    run_command(
        "CREATE TABLE test_writable_savepoints_truncate.sp_test (id int, val text) USING iceberg WITH (catalog='rest')",
        pg_conn,
    )

    # ---- First transaction: play with savepoints ----
    run_command("BEGIN", pg_conn)

    # Insert two rows
    run_command(
        "INSERT INTO test_writable_savepoints_truncate.sp_test VALUES (1, 'a'), (2, 'b')",
        pg_conn,
    )
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    assert rows == [[1, "a"], [2, "b"]]

    # Savepoint 1: state = rows (1,2)
    run_command("SAVEPOINT sp1", pg_conn)

    # Add a third row
    run_command(
        "INSERT INTO test_writable_savepoints_truncate.sp_test VALUES (3, 'c')", pg_conn
    )
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    assert rows == [[1, "a"], [2, "b"], [3, "c"]]

    # Savepoint 2: state = rows (1,2,3)
    run_command("SAVEPOINT sp2", pg_conn)

    # Make some changes after sp2
    run_command(
        "UPDATE test_writable_savepoints_truncate.sp_test SET val = 'b2' WHERE id = 2",
        pg_conn,
    )
    run_command(
        "DELETE FROM test_writable_savepoints_truncate.sp_test WHERE id = 1", pg_conn
    )
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    # Now only rows 2 and 3
    assert rows == [[2, "b2"], [3, "c"]]

    # Roll back to sp2 → undo UPDATE and DELETE
    run_command("ROLLBACK TO SAVEPOINT sp2", pg_conn)
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    # Back to 1,2,3 with original values
    assert rows == [[1, "a"], [2, "b"], [3, "c"]]

    # Roll back to sp1 → undo INSERT (3, 'c')
    run_command("ROLLBACK TO SAVEPOINT sp1", pg_conn)
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    # Back to only 1 and 2
    assert rows == [[1, "a"], [2, "b"]]

    # Commit first transaction
    run_command("COMMIT", pg_conn)

    # After commit, data should still be 1 and 2
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    assert rows == [[1, "a"], [2, "b"]]

    # ---- Second transaction: TRUNCATE with a savepoint ----
    run_command("BEGIN", pg_conn)

    # Check starting state
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    assert rows == [[1, "a"], [2, "b"]]

    # Savepoint before truncate
    run_command("SAVEPOINT before_truncate", pg_conn)

    # Truncate the table
    run_command("TRUNCATE test_writable_savepoints_truncate.sp_test", pg_conn)
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    assert rows == []  # table is empty inside the transaction

    # Roll back TRUNCATE using the savepoint
    run_command("ROLLBACK TO SAVEPOINT before_truncate", pg_conn)
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    # Data is back
    assert rows == [[1, "a"], [2, "b"]]

    # Finally, commit
    run_command("COMMIT", pg_conn)

    # After commit, table still has the original rows
    rows = run_query(
        "SELECT id, val FROM test_writable_savepoints_truncate.sp_test ORDER BY id",
        pg_conn,
    )
    assert rows == [[1, "a"], [2, "b"]]

    assert_metadata_on_pg_catalog_and_rest_matches(
        "test_writable_savepoints_truncate", "sp_test", superuser_conn
    )

    run_command(f"""DROP SCHEMA test_writable_savepoints_truncate CASCADE""", pg_conn)
    pg_conn.commit()


def test_complex_transaction(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):
    if installcheck:
        return
    # ----- Setup: schema and tables -----
    run_command("DROP SCHEMA IF EXISTS tx_demo CASCADE;", pg_conn)
    run_command("CREATE SCHEMA tx_demo;", pg_conn)

    run_command(
        """
        CREATE TABLE tx_demo.accounts (
            id      INT,
            owner   TEXT,
            balance INT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )

    run_command(
        """
        CREATE TABLE tx_demo.account_log (
            log_id      SERIAL,
            account_id  INT,
            action      TEXT,
            amount      INT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )

    # Make sure both tables are empty
    rows = run_query("SELECT * FROM tx_demo.accounts;", pg_conn)
    assert rows == []
    rows = run_query("SELECT * FROM tx_demo.account_log;", pg_conn)
    assert rows == []

    # ----- Begin complex transaction -----
    run_command("BEGIN;", pg_conn)

    # 1) Bulk insert into accounts
    run_command(
        """
        INSERT INTO tx_demo.accounts (id, owner, balance) VALUES
            (1, 'alice', 100),
            (2, 'bob',   200),
            (3, 'carol', 300);
    """,
        pg_conn,
    )

    rows = run_query(
        "SELECT id, owner, balance FROM tx_demo.accounts ORDER BY id;",
        pg_conn,
    )
    assert rows == [
        [1, "alice", 100],
        [2, "bob", 200],
        [3, "carol", 300],
    ]

    # Savepoint after initial accounts
    run_command("SAVEPOINT sp_after_accounts;", pg_conn)

    # 2) Insert logs based on accounts (SELECT -> INSERT)
    run_command(
        """
        INSERT INTO tx_demo.account_log (account_id, action, amount)
        SELECT id, 'initial', balance
        FROM tx_demo.accounts;
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "initial", 100],
        [2, "initial", 200],
        [3, "initial", 300],
    ]

    # Savepoint after initial logs
    run_command("SAVEPOINT sp_after_initial_logs;", pg_conn)

    # 3) Bulk update: give each account a 50 bonus
    run_command(
        """
        UPDATE tx_demo.accounts
        SET balance = balance + 50;
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, owner, balance
        FROM tx_demo.accounts
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "alice", 150],
        [2, "bob", 250],
        [3, "carol", 350],
    ]

    # And log the bonus for each account
    run_command(
        """
        INSERT INTO tx_demo.account_log (account_id, action, amount)
        SELECT id, 'bonus', 50
        FROM tx_demo.accounts;
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action, amount;
    """,
        pg_conn,
    )
    # account_id 1: initial 100, bonus 50
    # account_id 2: initial 200, bonus 50
    # account_id 3: initial 300, bonus 50
    assert rows == [
        [1, "bonus", 50],
        [1, "initial", 100],
        [2, "bonus", 50],
        [2, "initial", 200],
        [3, "bonus", 50],
        [3, "initial", 300],
    ]

    # 4) Delete one of the initial logs for account 3
    run_command(
        """
        DELETE FROM tx_demo.account_log
        WHERE account_id = 3 AND action = 'initial';
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action, amount;
    """,
        pg_conn,
    )
    # Now account_id 3 has only "bonus", not "initial"
    assert rows == [
        [1, "bonus", 50],
        [1, "initial", 100],
        [2, "bonus", 50],
        [2, "initial", 200],
        [3, "bonus", 50],
    ]

    # 5) Bulk delete: remove accounts with balance < 200 (after bonus)
    run_command(
        """
        DELETE FROM tx_demo.accounts
        WHERE balance < 200;
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, owner, balance
        FROM tx_demo.accounts
        ORDER BY id;
    """,
        pg_conn,
    )
    # account 1 had balance 150, so it is gone
    assert rows == [
        [2, "bob", 250],
        [3, "carol", 350],
    ]

    # Logs are unchanged by this delete (no FK / CASCADE)
    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action, amount;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "bonus", 50],
        [1, "initial", 100],
        [2, "bonus", 50],
        [2, "initial", 200],
        [3, "bonus", 50],
    ]

    # 6) Roll back all changes after initial logs
    #    This undoes: bonus updates, bonus logs, delete of one log, delete of account 1
    run_command("ROLLBACK TO SAVEPOINT sp_after_initial_logs;", pg_conn)

    rows = run_query(
        """
        SELECT id, owner, balance
        FROM tx_demo.accounts
        ORDER BY id;
    """,
        pg_conn,
    )
    # Back to original balances
    assert rows == [
        [1, "alice", 100],
        [2, "bob", 200],
        [3, "carol", 300],
    ]

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action, amount;
    """,
        pg_conn,
    )
    # Only the initial logs remain
    assert rows == [
        [1, "initial", 100],
        [2, "initial", 200],
        [3, "initial", 300],
    ]

    # 7) Another savepoint before TRUNCATE
    run_command("SAVEPOINT sp_before_truncate_logs;", pg_conn)

    # TRUNCATE the log table
    run_command("TRUNCATE tx_demo.account_log;", pg_conn)

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log;
    """,
        pg_conn,
    )
    assert rows == []  # logs are empty now

    # 8) Roll back TRUNCATE using the savepoint
    run_command("ROLLBACK TO SAVEPOINT sp_before_truncate_logs;", pg_conn)

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action, amount;
    """,
        pg_conn,
    )
    # Logs are back
    assert rows == [
        [1, "initial", 100],
        [2, "initial", 200],
        [3, "initial", 300],
    ]

    # 9) Commit whole transaction
    run_command("COMMIT;", pg_conn)

    # ----- Final checks after commit -----
    rows = run_query(
        """
        SELECT id, owner, balance
        FROM tx_demo.accounts
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "alice", 100],
        [2, "bob", 200],
        [3, "carol", 300],
    ]

    rows = run_query(
        """
        SELECT account_id, action, amount
        FROM tx_demo.account_log
        ORDER BY account_id, action, amount;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "initial", 100],
        [2, "initial", 200],
        [3, "initial", 300],
    ]

    assert_metadata_on_pg_catalog_and_rest_matches(
        "tx_demo", "account_log", superuser_conn
    )
    assert_metadata_on_pg_catalog_and_rest_matches(
        "tx_demo", "accounts", superuser_conn
    )

    run_command("DROP SCHEMA IF EXISTS tx_demo CASCADE;", pg_conn)


@pytest.mark.parametrize(
    "catalog",
    ["postgres", "rest"],
)
def test_sequences_serial_and_generated_columns(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
    catalog,
):
    if installcheck:
        return
    # ---------- Setup ----------
    run_command("DROP SCHEMA IF EXISTS seq_demo CASCADE;", pg_conn)
    run_command("CREATE SCHEMA seq_demo;", pg_conn)

    # A standalone sequence for manual use
    run_command(
        """
        CREATE SEQUENCE seq_demo.global_seq
            START WITH 1
            INCREMENT BY 1;
    """,
        pg_conn,
    )

    # Table using the manual sequence via nextval()
    run_command(
        f"""
        CREATE TABLE seq_demo.manual_seq_table (
            id      INT,
            label   TEXT
        ) USING iceberg WITH (catalog='{catalog}');
    """,
        pg_conn,
    )

    # Table using serial (creates its own sequence + default)
    run_command(
        f"""
        CREATE TABLE seq_demo.serial_table (
            id      SERIAL,
            label   TEXT
        ) USING iceberg WITH (catalog='{catalog}');
    """,
        pg_conn,
    )

    # Identity: GENERATED ALWAYS AS IDENTITY
    run_command(
        f"""
        CREATE TABLE seq_demo.identity_always_table (
            id      INT GENERATED ALWAYS AS IDENTITY,
            label   TEXT
        ) USING iceberg WITH (catalog='{catalog}');
    """,
        pg_conn,
    )

    # Identity: GENERATED BY DEFAULT AS IDENTITY (can override)
    run_command(
        f"""
        CREATE TABLE seq_demo.identity_default_table (
            id      INT GENERATED BY DEFAULT AS IDENTITY,
            label   TEXT
        ) USING iceberg WITH (catalog='{catalog}');
    """,
        pg_conn,
    )

    # Generated STORED columns
    run_command(
        f"""
        CREATE TABLE seq_demo.generated_table (
            a          INT,
            b          INT,
            sum_ab     INT GENERATED ALWAYS AS (a + b) STORED,
            double_a   INT GENERATED ALWAYS AS (a * 2) STORED
        ) USING iceberg WITH (catalog='{catalog}');
    """,
        pg_conn,
    )

    # All tables should start empty
    assert run_query("SELECT * FROM seq_demo.manual_seq_table;", pg_conn) == []
    assert run_query("SELECT * FROM seq_demo.serial_table;", pg_conn) == []
    assert run_query("SELECT * FROM seq_demo.identity_always_table;", pg_conn) == []
    assert run_query("SELECT * FROM seq_demo.identity_default_table;", pg_conn) == []
    assert run_query("SELECT * FROM seq_demo.generated_table;", pg_conn) == []

    # ---------- 1) Manual sequence with savepoint / rollback ----------
    run_command("BEGIN;", pg_conn)

    # First two rows using nextval on the standalone sequence
    run_command(
        """
        INSERT INTO seq_demo.manual_seq_table (id, label)
        VALUES (nextval('seq_demo.global_seq'), 'first');
    """,
        pg_conn,
    )
    run_command(
        """
        INSERT INTO seq_demo.manual_seq_table (id, label)
        VALUES (nextval('seq_demo.global_seq'), 'second');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.manual_seq_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "first"],
        [2, "second"],
    ]

    # Savepoint after two rows
    run_command("SAVEPOINT sp_manual_seq;", pg_conn)

    # Insert a third row (id should be 3)
    run_command(
        """
        INSERT INTO seq_demo.manual_seq_table (id, label)
        VALUES (nextval('seq_demo.global_seq'), 'third');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.manual_seq_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "first"],
        [2, "second"],
        [3, "third"],
    ]

    # Roll back to savepoint → third row disappears, but sequence has still advanced
    run_command("ROLLBACK TO SAVEPOINT sp_manual_seq;", pg_conn)

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.manual_seq_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "first"],
        [2, "second"],
    ]

    # Next nextval should be 4 (1,2,3 were already consumed)
    rows = run_query("SELECT nextval('seq_demo.global_seq');", pg_conn)
    assert rows == [[4]]

    # No more changes, just commit
    run_command("COMMIT;", pg_conn)

    # After commit, the table keeps the two rows
    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.manual_seq_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "first"],
        [2, "second"],
    ]

    # ---------- 2) Serial column usage ----------
    # Insert rows without specifying id → serial uses its own sequence
    run_command(
        """
        INSERT INTO seq_demo.serial_table (label)
        VALUES ('s1'), ('s2');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.serial_table
        ORDER BY id;
    """,
        pg_conn,
    )
    # serial starts at 1 by default
    assert rows == [
        [1, "s1"],
        [2, "s2"],
    ]

    # You can still insert with an explicit id if you want
    run_command(
        """
        INSERT INTO seq_demo.serial_table (id, label)
        VALUES (10, 's10');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.serial_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "s1"],
        [2, "s2"],
        [10, "s10"],
    ]

    # ---------- 3) Identity ALWAYS ----------
    # Cannot override id easily; typically just omit it
    run_command(
        """
        INSERT INTO seq_demo.identity_always_table (label)
        VALUES ('ia1'), ('ia2');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.identity_always_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "ia1"],
        [2, "ia2"],
    ]

    # ---------- 4) Identity BY DEFAULT (can override) ----------
    # First, use default-generated ids
    run_command(
        """
        INSERT INTO seq_demo.identity_default_table (label)
        VALUES ('id1'), ('id2');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.identity_default_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "id1"],
        [2, "id2"],
    ]

    # Now override the identity value explicitly
    run_command(
        """
        INSERT INTO seq_demo.identity_default_table (id, label)
        VALUES (100, 'id100');
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT id, label
        FROM seq_demo.identity_default_table
        ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "id1"],
        [2, "id2"],
        [100, "id100"],
    ]

    # ---------- 5) Generated STORED columns ----------
    # Insert a few rows with only base columns a, b
    run_command(
        """
        INSERT INTO seq_demo.generated_table (a, b)
        VALUES
            (1,  10),
            (2,  20),
            (5, -3);
    """,
        pg_conn,
    )

    rows = run_query(
        """
        SELECT a, b, sum_ab, double_a
        FROM seq_demo.generated_table
        ORDER BY a;
    """,
        pg_conn,
    )
    # sum_ab = a + b, double_a = a * 2
    assert rows == [
        [1, 10, 11, 2],
        [2, 20, 22, 4],
        [5, -3, 2, 10],
    ]

    pg_conn.commit()

    if catalog == "rest":
        for tbl_name in [
            "manual_seq_table",
            "serial_table",
            "identity_default_table",
            "identity_always_table",
            "generated_table",
        ]:
            assert_metadata_on_pg_catalog_and_rest_matches(
                "seq_demo", tbl_name, superuser_conn
            )

    run_command("DROP SCHEMA IF EXISTS seq_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_rest_iceberg_base_types(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return
    run_command(
        """
                CREATE SCHEMA test_iceberg_base_types;
                """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
            CREATE TABLE test_iceberg_base_types."iceberg_base_types" (
                    "boolean col" BOOLEAN DEFAULT true,
                    blob_col BYTEA DEFAULT decode('00001234', 'hex'),
                    tiny_int_col SMALLINT,
                    smallint_col SMALLINT DEFAULT 1+1,
                    int_col INTEGER NOT NULL DEFAULT 1,
                    bigint_col BIGINT DEFAULT 100,
                    utinyint_col SMALLINT,
                    usmallint_col INTEGER,
                    uinteger_col BIGINT,
                    uint64_col NUMERIC(20, 0) DEFAULT NULL,
                    float4_col REAL DEFAULT 3.14,
                    float8_col DOUBLE PRECISION DEFAULT 2.718281828,
                    varchar_col VARCHAR DEFAULT 'test' || 'as',
                    decimal_col NUMERIC DEFAULT random(),
                    uuid_col UUID DEFAULT '123e4567-e89b-12d3-a456-426614174000',
                    hugeint_col NUMERIC(38, 0),
                    uhugeint_col NUMERIC(38, 0),
                    timestamp_ns_col TIMESTAMP,
                    timestamp_ms_col TIMESTAMP,
                    timestamp_s_col TIMESTAMP,
                    timestamp_col TIMESTAMP DEFAULT '2024-01-01',
                    timestamp_tz_col TIMESTAMPTZ DEFAULT now(),
                    time_col TIME DEFAULT '12:34:56',
                    date_col DATE DEFAULT '2024-01-01',
                    escaping_varchar_col VARCHAR DEFAULT 'test"  \\ "dummy_A!"@#$%^&* <>?`~'
                ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata(
        "test_iceberg_base_types", "iceberg_base_types", pg_conn
    )
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    # Define the expected fields
    expected_fields = [
        {
            "id": 1,
            "name": "boolean col",
            "required": False,
            "type": "boolean",
            "write-default": True,
        },
        {
            "id": 2,
            "name": "blob_col",
            "required": False,
            "type": "binary",
            "write-default": "00001234",
        },
        {"id": 3, "name": "tiny_int_col", "required": False, "type": "int"},
        {
            "id": 4,
            "name": "smallint_col",
            "required": False,
            "type": "int",
            "write-default": 2,
        },
        {
            "id": 5,
            "name": "int_col",
            "required": True,
            "type": "int",
            "write-default": 1,
        },
        {
            "id": 6,
            "name": "bigint_col",
            "required": False,
            "type": "long",
            "write-default": 100,
        },
        {"id": 7, "name": "utinyint_col", "required": False, "type": "int"},
        {"id": 8, "name": "usmallint_col", "required": False, "type": "int"},
        {"id": 9, "name": "uinteger_col", "required": False, "type": "long"},
        {"id": 10, "name": "uint64_col", "required": False, "type": "decimal(20, 0)"},
        {
            "id": 11,
            "name": "float4_col",
            "required": False,
            "type": "float",
            "write-default": 3.14,
        },
        {
            "id": 12,
            "name": "float8_col",
            "required": False,
            "type": "double",
            "write-default": 2.718281828,
        },
        {
            "id": 13,
            "name": "varchar_col",
            "required": False,
            "type": "string",
            "write-default": "testas",
        },
        {"id": 14, "name": "decimal_col", "required": False, "type": "decimal(38, 9)"},
        {
            "id": 15,
            "name": "uuid_col",
            "required": False,
            "type": "uuid",
            "write-default": "123e4567-e89b-12d3-a456-426614174000",
        },
        {"id": 16, "name": "hugeint_col", "required": False, "type": "decimal(38, 0)"},
        {"id": 17, "name": "uhugeint_col", "required": False, "type": "decimal(38, 0)"},
        {"id": 18, "name": "timestamp_ns_col", "required": False, "type": "timestamp"},
        {"id": 19, "name": "timestamp_ms_col", "required": False, "type": "timestamp"},
        {"id": 20, "name": "timestamp_s_col", "required": False, "type": "timestamp"},
        {
            "id": 21,
            "name": "timestamp_col",
            "required": False,
            "type": "timestamp",
            "write-default": "2024-01-01T00:00:00",
        },
        {
            "id": 22,
            "name": "timestamp_tz_col",
            "required": False,
            "type": "timestamptz",
        },
        {
            "id": 23,
            "name": "time_col",
            "required": False,
            "type": "time",
            "write-default": "12:34:56",
        },
        {
            "id": 24,
            "name": "date_col",
            "required": False,
            "type": "date",
            "write-default": "2024-01-01",
        },
        {
            "id": 25,
            "name": "escaping_varchar_col",
            "required": False,
            "type": "string",
            "write-default": 'test"  \\ "dummy_A!"@#$%^&* <>?`~',
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = parsed_data["schemas"][0]["fields"]

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"
        compare_fields(expected["type"], actual["type"])

        if expected.get("doc") is not None:
            assert (
                expected["doc"] == actual["doc"]
            ), f"Type mismatch: expected {expected['doc']} but got {actual['doc']}"
        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_base_types.\"iceberg_base_types\"'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = {
        "boolean col": 1,
        "blob_col": 2,
        "tiny_int_col": 3,
        "smallint_col": 4,
        "int_col": 5,
        "bigint_col": 6,
        "utinyint_col": 7,
        "usmallint_col": 8,
        "uinteger_col": 9,
        "uint64_col": 10,
        "float4_col": 11,
        "float8_col": 12,
        "varchar_col": 13,
        "decimal_col": 14,
        "uuid_col": 15,
        "hugeint_col": 16,
        "uhugeint_col": 17,
        "timestamp_ns_col": 18,
        "timestamp_ms_col": 19,
        "timestamp_s_col": 20,
        "timestamp_col": 21,
        "timestamp_tz_col": 22,
        "time_col": 23,
        "date_col": 24,
        "escaping_varchar_col": 25,
    }
    assert data == str(expected_field_ids)

    # load and read data back
    run_command(
        """
        INSERT INTO test_iceberg_base_types."iceberg_base_types" VALUES (
                DEFAULT,                -- boolean_col uses the default (true)
                DEFAULT,                -- blob_col uses the default ('00001234')
                10,                     -- tiny_int_col (SMALLINT)
                DEFAULT,                -- smallint_col uses the default (1 + 1)
                DEFAULT,                -- int_col uses the default (1)
                DEFAULT,                -- bigint_col
                20,                     -- utinyint_col (SMALLINT)
                30000,                  -- usmallint_col (INTEGER)
                100000000,              -- uinteger_col (BIGINT)
                NULL,                   -- uint64_col (NUMERIC(20, 0))
                DEFAULT,                -- float4_col uses the default (3.14)
                DEFAULT,                -- float8_col uses the default (2.718281828)
                DEFAULT,                -- varchar_col uses default ('testas')
                DEFAULT,                -- decimal_col uses default (random())
                DEFAULT,                -- uuid_col uses the default ('123e4567-e89b-12d3-a456-426614174000')
                12345678901234567890123456789012345678, -- hugeint_col (NUMERIC(38, 0))
                98765432109876543210987654321098765432, -- uhugeint_col (NUMERIC(38, 0))
                '2024-01-01 12:00:00',  -- timestamp_ns_col (TIMESTAMP)
                '2024-01-01 12:00:00',  -- timestamp_ms_col (TIMESTAMP)
                '2024-01-01 12:00:00',  -- timestamp_s_col (TIMESTAMP)
                DEFAULT,                -- timestamp_col uses the default ('2024-01-01')
                DEFAULT,                -- timestamp_tz_col uses the default (now())
                DEFAULT,                -- time_col (TIME) uses the default ('12:34:56')
                DEFAULT,                -- date_col (DATE) uses the default ('2024-01-01')
                DEFAULT                 -- escaping_varchar_col uses default ('test"  \\ "dummy_A!"@#$%^&* <>?`~')
            );
""",
        pg_conn,
    )

    results = run_query(
        'SELECT * FROM test_iceberg_base_types."iceberg_base_types"', pg_conn
    )
    assert len(results) == 1
    assert len(results[0]) == 25
    assert results[0][24] == 'test"  \\ "dummy_A!"@#$%^&* <>?`~'

    run_command("DROP SCHEMA test_iceberg_base_types CASCADE", pg_conn)
    pg_conn.commit()


def test_rest_iceberg_types_array(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return
    run_command(
        """
                CREATE SCHEMA test_iceberg_array_types;
                """,
        pg_conn,
    )
    pg_conn.commit()

    # Create a writable table with array columns
    run_command(
        f"""
            CREATE TABLE test_iceberg_array_types.iceberg_types_arr_3 (
                "boolean col" BOOLEAN[],
                blob_col BYTEA[],
                tiny_int_col SMALLINT[],
                smallint_col SMALLINT[],
                int_col INTEGER[],
                bigint_col BIGINT[],
                utinyint_col SMALLINT[],
                usmallint_col INTEGER[],
                uinteger_col BIGINT[],
                uint64_col NUMERIC(20, 0)[],
                float4_col REAL[],
                float8_col DOUBLE PRECISION[],
                varchar_col VARCHAR[],
                decimal_col NUMERIC[],
                uuid_col UUID[],
                hugeint_col NUMERIC(38, 0)[],
                uhugeint_col NUMERIC(38, 0)[],
                timestamp_ns_col TIMESTAMP[],
                timestamp_ms_col TIMESTAMP[],
                timestamp_s_col TIMESTAMP[],
                timestamp_col TIMESTAMP[],
                timestamp_tz_col TIMESTAMPTZ[],
                time_col TIME[],
                date_col DATE[]
            ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata(
        "test_iceberg_array_types", "iceberg_types_arr_3", pg_conn
    )
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {
            "id": 1,
            "name": "boolean col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 2,
                "element": "boolean",
                "element-required": False,
            },
        },
        {
            "id": 3,
            "name": "blob_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 4,
                "element": "binary",
                "element-required": False,
            },
        },
        {
            "id": 5,
            "name": "tiny_int_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 6,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 7,
            "name": "smallint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 8,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 9,
            "name": "int_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 10,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 11,
            "name": "bigint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 12,
                "element": "long",
                "element-required": False,
            },
        },
        {
            "id": 13,
            "name": "utinyint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 14,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 15,
            "name": "usmallint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 16,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 17,
            "name": "uinteger_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 18,
                "element": "long",
                "element-required": False,
            },
        },
        {
            "id": 19,
            "name": "uint64_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 20,
                "element": "decimal(20, 0)",
                "element-required": False,
            },
        },
        {
            "id": 21,
            "name": "float4_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 22,
                "element": "float",
                "element-required": False,
            },
        },
        {
            "id": 23,
            "name": "float8_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 24,
                "element": "double",
                "element-required": False,
            },
        },
        {
            "id": 25,
            "name": "varchar_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 26,
                "element": "string",
                "element-required": False,
            },
        },
        {
            "id": 27,
            "name": "decimal_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 28,
                "element": "decimal(38, 9)",
                "element-required": False,
            },
        },
        {
            "id": 29,
            "name": "uuid_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 30,
                "element": "uuid",
                "element-required": False,
            },
        },
        {
            "id": 31,
            "name": "hugeint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 32,
                "element": "decimal(38, 0)",
                "element-required": False,
            },
        },
        {
            "id": 33,
            "name": "uhugeint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 34,
                "element": "decimal(38, 0)",
                "element-required": False,
            },
        },
        {
            "id": 35,
            "name": "timestamp_ns_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 36,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 37,
            "name": "timestamp_ms_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 38,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 39,
            "name": "timestamp_s_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 40,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 41,
            "name": "timestamp_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 42,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 43,
            "name": "timestamp_tz_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 44,
                "element": "timestamptz",
                "element-required": False,
            },
        },
        {
            "id": 45,
            "name": "time_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 46,
                "element": "time",
                "element-required": False,
            },
        },
        {
            "id": 47,
            "name": "date_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 48,
                "element": "date",
                "element-required": False,
            },
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"
        compare_fields(expected["type"], actual["type"])

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_array_types.iceberg_types_arr_3'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = (
        "{'boolean col': {__duckdb_field_id: 1, element: 2}, "
        "'blob_col': {__duckdb_field_id: 3, element: 4}, "
        "'tiny_int_col': {__duckdb_field_id: 5, element: 6}, "
        "'smallint_col': {__duckdb_field_id: 7, element: 8}, "
        "'int_col': {__duckdb_field_id: 9, element: 10}, "
        "'bigint_col': {__duckdb_field_id: 11, element: 12}, "
        "'utinyint_col': {__duckdb_field_id: 13, element: 14}, "
        "'usmallint_col': {__duckdb_field_id: 15, element: 16}, "
        "'uinteger_col': {__duckdb_field_id: 17, element: 18}, "
        "'uint64_col': {__duckdb_field_id: 19, element: 20}, "
        "'float4_col': {__duckdb_field_id: 21, element: 22}, "
        "'float8_col': {__duckdb_field_id: 23, element: 24}, "
        "'varchar_col': {__duckdb_field_id: 25, element: 26}, "
        "'decimal_col': {__duckdb_field_id: 27, element: 28}, "
        "'uuid_col': {__duckdb_field_id: 29, element: 30}, "
        "'hugeint_col': {__duckdb_field_id: 31, element: 32}, "
        "'uhugeint_col': {__duckdb_field_id: 33, element: 34}, "
        "'timestamp_ns_col': {__duckdb_field_id: 35, element: 36}, "
        "'timestamp_ms_col': {__duckdb_field_id: 37, element: 38}, "
        "'timestamp_s_col': {__duckdb_field_id: 39, element: 40}, "
        "'timestamp_col': {__duckdb_field_id: 41, element: 42}, "
        "'timestamp_tz_col': {__duckdb_field_id: 43, element: 44}, "
        "'time_col': {__duckdb_field_id: 45, element: 46}, "
        "'date_col': {__duckdb_field_id: 47, element: 48}}"
    )

    assert data == expected_field_ids

    run_command(
        """
        INSERT INTO test_iceberg_array_types.iceberg_types_arr_3 VALUES (
            ARRAY[TRUE, FALSE, TRUE],                                      -- boolean_col
            ARRAY[decode('1234', 'hex'), decode('5678', 'hex')],            -- blob_col
            ARRAY[10, 20, 30],                                              -- tiny_int_col (SMALLINT)
            ARRAY[1, 2, 3],                                                 -- smallint_col (SMALLINT)
            ARRAY[100, 200, 300],                                           -- int_col (INTEGER)
            ARRAY[1000000000, 2000000000, 3000000000],                      -- bigint_col (BIGINT)
            ARRAY[10, 20],                                                  -- utinyint_col (SMALLINT)
            ARRAY[30000, 40000],                                            -- usmallint_col (INTEGER)
            ARRAY[100000000, 200000000],                                    -- uinteger_col (BIGINT)
            ARRAY[NULL, 12345678901234567890],                              -- uint64_col (NUMERIC(20, 0))
            ARRAY[3.14, 2.71],                                              -- float4_col (REAL)
            ARRAY[2.718281828, 3.1415926535],                               -- float8_col (DOUBLE PRECISION)
            ARRAY['hello', 'world'],                                        -- varchar_col (VARCHAR)
            ARRAY[random(), random()],                                      -- decimal_col (NUMERIC)
            ARRAY['123e4567-e89b-12d3-a456-426614174000'::uuid, '321e6547-e89b-12d3-a456-426614174000'::uuid], -- uuid_col (UUID)
            ARRAY[12345678901234567890123456789012345678, 98765432109876543210987654321098765432], -- hugeint_col (NUMERIC(38, 0))
            ARRAY[11111111111111111111111111111111111111, 22222222222222222222222222222222222222], -- uhugeint_col (NUMERIC(38, 0))
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_ns_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_ms_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_s_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00+00'::TIMESTAMPTZ, '2025-01-01 12:00:00+00'::TIMESTAMPTZ],      -- timestamp_tz_col (TIMESTAMPTZ)
            ARRAY['12:34:56'::TIME, '23:45:01'::TIME],                                  -- time_col (TIME)
            ARRAY['2024-01-01'::DATE, '2025-01-01'::DATE]                               -- date_col (DATE)
        );

        """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_array_types.iceberg_types_arr_3", pg_conn
    )
    assert len(results) == 1
    assert len(results[0]) == 24

    run_command("DROP SCHEMA test_iceberg_array_types CASCADE", pg_conn)
    pg_conn.commit()


def test_rest_iceberg_types_composite(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return
    run_command(
        """
                CREATE SCHEMA test_iceberg_composite_types;
                """,
        pg_conn,
    )
    pg_conn.commit()

    # Create composite types and foreign table
    run_command(
        """
        CREATE TYPE test_iceberg_composite_types."t_type" AS ("a_with_escape" int, "b_with_escape" int);
        CREATE TYPE test_iceberg_composite_types.t_type_2 AS ("a_with_escape" int, "b_with_escape" int, c test_iceberg_composite_types."t_type");

        CREATE TABLE test_iceberg_composite_types.iceberg_types_composite (
            "a_with_escape" int,
            "b_with_escape" test_iceberg_composite_types."t_type",
            c test_iceberg_composite_types.t_type_2,
            d test_iceberg_composite_types.t_type_2[]
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata(
        "test_iceberg_composite_types", "iceberg_types_composite", pg_conn
    )
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "a_with_escape", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "b_with_escape",
            "required": False,
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "id": 3,
                        "name": "a_with_escape",
                        "required": False,
                        "type": "int",
                    },
                    {
                        "id": 4,
                        "name": "b_with_escape",
                        "required": False,
                        "type": "int",
                    },
                ],
            },
        },
        {
            "id": 5,
            "name": "c",
            "required": False,
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "id": 6,
                        "name": "a_with_escape",
                        "required": False,
                        "type": "int",
                    },
                    {
                        "id": 7,
                        "name": "b_with_escape",
                        "required": False,
                        "type": "int",
                    },
                    {
                        "id": 8,
                        "name": "c",
                        "required": False,
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 9,
                                    "name": "a_with_escape",
                                    "required": False,
                                    "type": "int",
                                },
                                {
                                    "id": 10,
                                    "name": "b_with_escape",
                                    "required": False,
                                    "type": "int",
                                },
                            ],
                        },
                    },
                ],
            },
        },
        {
            "id": 11,
            "name": "d",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 12,
                "element": {
                    "type": "struct",
                    "fields": [
                        {
                            "id": 13,
                            "name": "a_with_escape",
                            "required": False,
                            "type": "int",
                        },
                        {
                            "id": 14,
                            "name": "b_with_escape",
                            "required": False,
                            "type": "int",
                        },
                        {
                            "id": 15,
                            "name": "c",
                            "required": False,
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "id": 16,
                                        "name": "a_with_escape",
                                        "required": False,
                                        "type": "int",
                                    },
                                    {
                                        "id": 17,
                                        "name": "b_with_escape",
                                        "required": False,
                                        "type": "int",
                                    },
                                ],
                            },
                        },
                    ],
                },
                "element-required": False,
            },
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_composite_types.iceberg_types_composite'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = (
        "{'a_with_escape': 1, "
        "'b_with_escape': {__duckdb_field_id: 2, 'a_with_escape': 3, 'b_with_escape': 4}, "
        "'c': {__duckdb_field_id: 5, 'a_with_escape': 6, 'b_with_escape': 7, 'c': {__duckdb_field_id: 8, 'a_with_escape': 9, 'b_with_escape': 10}}, "
        "'d': {__duckdb_field_id: 11, element: {__duckdb_field_id: 12, 'a_with_escape': 13, 'b_with_escape': 14, 'c': {__duckdb_field_id: 15, 'a_with_escape': 16, 'b_with_escape': 17}}}}"
    )

    assert data == expected_field_ids

    # load some data and read it
    run_command(
        """
            INSERT INTO test_iceberg_composite_types.iceberg_types_composite
            VALUES (
                1,
                ROW(10, 20),
                ROW(30, 40, ROW(50, 60)),
                ARRAY[
                    '(70,80,"(90,100)")'::test_iceberg_composite_types.t_type_2,
                    '(110,120,"(130,140)")'::test_iceberg_composite_types.t_type_2
                ]
            ),
                (
                2,
                ROW(11, 21),
                ROW(31, 41, ROW(51, 61)),
                NULL
            ),
            (
                3,
                ROW(12, 22),
                NULL,
                NULL
            ),
            (
                4,
                NULL,
                NULL,
                NULL
            ),
            (
                5,
                DEFAULT,
                ROW(44, 55, ROW(55, 55)),
                DEFAULT
            );

        """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_composite_types.iceberg_types_composite ORDER BY 1",
        pg_conn,
    )
    print(results)
    assert results == [
        [
            1,
            "(10,20)",
            '(30,40,"(50,60)")',
            '{"(70,80,\\"(90,100)\\")","(110,120,\\"(130,140)\\")"}',
        ],
        [2, "(11,21)", '(31,41,"(51,61)")', None],
        [3, "(12,22)", None, None],
        [4, None, None, None],
        [5, None, '(44,55,"(55,55)")', None],
    ]

    run_command("DROP SCHEMA test_iceberg_composite_types CASCADE", pg_conn)
    pg_conn.commit()


def test_rest_iceberg_types_converted_to_string(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)

    run_command(
        """
                CREATE SCHEMA test_iceberg_types_converted_to_string;
                """,
        pg_conn,
    )

    run_command(
        """
        CREATE TABLE test_iceberg_types_converted_to_string.iceberg_types_converted_to_string (
                id INT,
                hstore_col hstore DEFAULT '"a"=>"1", "b"=>"2"',
                json_col json DEFAULT '{"a": 1, "b": 2}',
                jsonb_col json DEFAULT '{"hello": [3,4]}'
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata(
        "test_iceberg_types_converted_to_string",
        "iceberg_types_converted_to_string",
        pg_conn,
    )
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "hstore_col",
            "required": False,
            "type": "string",
            "write-default": '"a"=>"1", "b"=>"2"',
        },
        {
            "id": 3,
            "name": "json_col",
            "required": False,
            "type": "string",
            "write-default": '{"a": 1, "b": 2}',
        },
        {
            "id": 4,
            "name": "jsonb_col",
            "required": False,
            "type": "string",
            "write-default": '{"hello": [3,4]}',
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_types_converted_to_string.iceberg_types_converted_to_string'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = "{'id': 1, 'hstore_col': 2, 'json_col': 3, 'jsonb_col': 4}"

    assert data == expected_field_ids

    # load some data and read it
    run_command(
        """
            INSERT INTO test_iceberg_types_converted_to_string.iceberg_types_converted_to_string
            VALUES (
                1,
                '"a"=>"1", "b"=>"2"',
                '{"a": 1, "b": 2}',
                '{"hello": [3,4]}'
            ),
            (
                2,
                DEFAULT,
                DEFAULT,
                DEFAULT
            );

        """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_types_converted_to_string.iceberg_types_converted_to_string ORDER BY 1",
        pg_conn,
    )
    assert results == [
        [1, '"a"=>"1", "b"=>"2"', {"a": 1, "b": 2}, {"hello": [3, 4]}],
        [2, '"a"=>"1", "b"=>"2"', {"a": 1, "b": 2}, {"hello": [3, 4]}],
    ]

    run_command("DROP SCHEMA test_iceberg_types_converted_to_string CASCADE", pg_conn)
    pg_conn.commit()


def test_rest_iceberg_map_type(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return
    create_map_type("int", "text")
    run_command(
        """
                CREATE SCHEMA test_iceberg_map_type;
                CREATE TYPE test_iceberg_map_type."test type 10" AS (field1 text, "field_2" int, field3 numeric(20,0));
                """,
        pg_conn,
    )

    pg_conn.commit()
    complex_map_type_name = create_map_type(
        "text", 'test_iceberg_map_type."test type 10"[]'
    )

    run_command(
        f"""
        CREATE TABLE test_iceberg_map_type.simple_map_pg
        (
            id INT,
            simple_map map_type.key_int_val_text,
            "complex map" {complex_map_type_name}

       ) USING iceberg WITH (catalog='rest')
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata(
        "test_iceberg_map_type", "simple_map_pg", pg_conn
    )
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "simple_map",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 3,
                "key": "int",
                "value-id": 4,
                "value": "string",
                "value-required": False,
            },
        },
        {
            "id": 5,
            "name": "complex map",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 6,
                "key": "string",
                "value-id": 7,
                "value": {
                    "type": "list",
                    "element-id": 8,
                    "element": {
                        "type": "struct",
                        "fields": [
                            {
                                "id": 9,
                                "name": "field1",
                                "required": False,
                                "type": "string",
                            },
                            {
                                "id": 10,
                                "name": "field_2",
                                "required": False,
                                "type": "int",
                            },
                            {
                                "id": 11,
                                "name": "field3",
                                "required": False,
                                "type": "decimal(20, 0)",
                            },
                        ],
                    },
                    "element-required": False,
                },
                "value-required": False,
            },
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_map_type.simple_map_pg'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = (
        "{'id': 1, "
        "'simple_map': {__duckdb_field_id: 2, key: 3, value: 4}, "
        "'complex map': {__duckdb_field_id: 5, key: 6, value: {__duckdb_field_id: 7, element: {__duckdb_field_id: 8, 'field1': 9, 'field_2': 10, 'field3': 11}}}}"
    )
    assert data == str(expected_field_ids)

    # lets ingest some data and read back
    run_command(
        f"""
        INSERT INTO test_iceberg_map_type.simple_map_pg
        VALUES (
            1,
            array[(1, 'me'), (2, 'myself'), (3, 'i')]::map_type.key_int_val_text,
            array[('1', ARRAY['(''me'', 1, 1.1)'])]::{complex_map_type_name}
        ),
        (
            2,
            DEFAULT,
            NULL
        ),
        (
            3,
            NULL,
            NULL
        ),
        (
            4,
            array[]::map_type.key_int_val_text,
            array[]::{complex_map_type_name}
        )
    """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_map_type.simple_map_pg ORDER BY 1", pg_conn
    )
    assert results == [
        [
            1,
            '{"(1,me)","(2,myself)","(3,i)"}',
            '{"(1,\\"{\\"\\"(\'me\',1,1)\\"\\"}\\")"}',
        ],
        [2, None, None],
        [3, None, None],
        [4, "{}", "{}"],
    ]

    run_command("DROP SCHEMA test_iceberg_map_type CASCADE", pg_conn)
    pg_conn.commit()


def test_not_null_drop_and_add(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)

    # Create table with NOT NULL column
    run_command(
        """
        CREATE TABLE ddl_demo.nn_table (
            id   INT NOT NULL,
            name TEXT NOT NULL
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "nn_table", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": True, "type": "int"},
        {"id": 2, "name": "name", "required": True, "type": "string"},
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    run_command(
        """ 
        ALTER TABLE ddl_demo.nn_table ALTER COLUMN name DROP NOT NULL,  ALTER COLUMN id DROP NOT NULL;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "nn_table", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00001-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][1]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": False, "type": "int"},
        {"id": 2, "name": "name", "required": False, "type": "string"},
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    # sanity check
    err = run_command(
        """ 
        ALTER TABLE ddl_demo.nn_table ALTER COLUMN name SET NOT NULL;
        """,
        pg_conn,
        raise_error=False,
    )

    assert "SET NOT NULL command not supported for pg_lake_iceberg tables" in str(err)
    pg_conn.rollback()

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_rest_rename_col(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)

    # Create table with NOT NULL column
    run_command(
        """
        CREATE TABLE ddl_demo.nn_table (
            id   INT NOT NULL,
            name TEXT NOT NULL
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """ 
        ALTER TABLE ddl_demo.nn_table RENAME COLUMN id TO id_new;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "nn_table", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00001-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][1]["fields"]

    expected_fields = [
        {"id": 1, "name": "id_new", "required": True, "type": "int"},
        {"id": 2, "name": "name", "required": True, "type": "string"},
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    # rename table is disallowed
    err = run_command(
        "ALTER TABLE ddl_demo.nn_table RENAME TO nn_table_2", pg_conn, raise_error=False
    )
    assert "ALTER TABLE RENAME command not supported for pg_lake_iceberg tables" in str(
        err
    )
    pg_conn.rollback()

    err = run_command(
        "ALTER FOREIGN TABLE ddl_demo.nn_table RENAME TO nn_table_2",
        pg_conn,
        raise_error=False,
    )
    assert "ALTER TABLE RENAME command not supported for pg_lake_iceberg tables" in str(
        err
    )
    pg_conn.rollback()

    # set schema to a non-existing schema
    err = run_command(
        "ALTER TABLE ddl_demo.nn_table SET SCHEMA not_existing_schema",
        pg_conn,
        raise_error=False,
    )
    assert "does not exist" in str(err)
    pg_conn.rollback()

    run_command("CREATE SCHEMA one_another_schema_for_ddl", pg_conn)
    pg_conn.commit()
    err = run_command(
        "ALTER TABLE ddl_demo.nn_table SET SCHEMA one_another_schema_for_ddl",
        pg_conn,
        raise_error=False,
    )
    assert (
        "ALTER TABLE SET SCHEMA command not supported for pg_lake_iceberg tables"
        in str(err)
    )
    pg_conn.rollback()

    # we cannot also rename ddl_demo as it contains at least one rest catalog table
    err = run_command(
        "ALTER SCHEMA ddl_demo RENAME TO ddl_demo_new", pg_conn, raise_error=False
    )
    assert "because it contains an iceberg table with rest catalog" in str(err)
    pg_conn.rollback()

    run_command("DROP SCHEMA ddl_demo, one_another_schema_for_ddl CASCADE;", pg_conn)
    pg_conn.commit()


def test_add_drop_columns(
    pg_conn,
    superuser_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return
    run_command("CREATE SCHEMA ddl_demo;", pg_conn)
    run_command(
        """
        CREATE TABLE ddl_demo.ad_table (
            id INT,
            val TEXT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Insert some rows
    run_command(
        """
        INSERT INTO ddl_demo.ad_table (id, val)
        VALUES (1, 'a'), (2, 'b');
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        """
        SELECT id, val FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "a"],
        [2, "b"],
    ]

    # Add a new column (no default)
    run_command(
        """
        ALTER TABLE ddl_demo.ad_table
        ADD COLUMN extra INT;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        """
        SELECT id, val, extra FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    # Existing rows get NULL in the new column
    assert rows == [
        [1, "a", None],
        [2, "b", None],
    ]

    # Update new column a bit
    run_command(
        """
        UPDATE ddl_demo.ad_table
        SET extra = 10
        WHERE id = 1;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        """
        SELECT id, val, extra FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "a", 10],
        [2, "b", None],
    ]

    # Now drop the column again
    run_command(
        """
        ALTER TABLE ddl_demo.ad_table
        DROP COLUMN val;
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        """
        SELECT id, extra FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    # Data in original columns is still correct
    assert rows == [
        [1, 10],
        [2, None],
    ]

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_add_column_with_check_constraint(
    pg_conn,
    superuser_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)

    run_command(
        """
        CREATE TABLE ddl_demo.check_table (
            id INT, amount INT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )

    # Initial data
    run_command(
        """
        INSERT INTO ddl_demo.check_table (id)
        VALUES (1), (2);
    """,
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        "SELECT id, amount FROM ddl_demo.check_table ORDER BY id;",
        pg_conn,
    )
    assert rows == [
        [1, None],
        [2, None],
    ]

    # Add column with CHECK constraint
    run_command(
        """
        ALTER TABLE ddl_demo.check_table
        ADD CONSTRAINT my_table_amount_check CHECK (amount >= 0);
    """,
        pg_conn,
    )

    # Existing rows get NULL (NULL passes CHECK)
    rows = run_query(
        "SELECT id, amount FROM ddl_demo.check_table ORDER BY id;",
        pg_conn,
    )
    assert rows == [
        [1, None],
        [2, None],
    ]

    # Insert valid row
    run_command(
        """
        INSERT INTO ddl_demo.check_table (id, amount)
        VALUES (3, 10);
    """,
        pg_conn,
    )

    rows = run_query(
        "SELECT id, amount FROM ddl_demo.check_table ORDER BY id;",
        pg_conn,
    )
    assert rows == [
        [1, None],
        [2, None],
        [3, 10],
    ]

    # Insert invalid row
    err = run_command(
        """
        INSERT INTO ddl_demo.check_table (id, amount)
        VALUES (3, -10);
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates check constraint" in str(err)
    pg_conn.rollback()

    assert_metadata_on_pg_catalog_and_rest_matches(
        "ddl_demo", "check_table", superuser_conn
    )

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_rest_add_column_with_default_and_drop_default(
    pg_conn,
    superuser_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)
    run_command(
        """
        CREATE TABLE ddl_demo.default_table (
            id INT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )

    # Initial rows
    run_command(
        """
        INSERT INTO ddl_demo.default_table (id)
        VALUES (1), (2);
    """,
        pg_conn,
    )

    rows = run_query(
        "SELECT id FROM ddl_demo.default_table ORDER BY id;",
        pg_conn,
    )
    assert rows == [
        [1],
        [2],
    ]
    pg_conn.commit()

    # Iceberg V2 (and hence the current versions of Polaris)
    # does not support
    err = run_command(
        """
        ALTER TABLE ddl_demo.default_table
        ADD COLUMN status TEXT DEFAULT 'new';
    """,
        pg_conn,
        raise_error=False,
    )
    assert "ALTER TABLE ADD COLUMN command not supported" in str(err)
    pg_conn.rollback()

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_set_default_on_one_column_drop_default_on_another(
    pg_conn,
    superuser_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)

    run_command(
        """
        CREATE TABLE ddl_demo.mixed_default_table (
            col_with_default    INT DEFAULT 42,
            col_without_default INT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Set DEFAULT on the first column
    run_command(
        """
        ALTER TABLE ddl_demo.mixed_default_table
        ALTER COLUMN col_without_default SET DEFAULT 43;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # Drop DEFAULT on the second column (even though it has none)
    # This should be a no-op and must not fail
    run_command(
        """
        ALTER TABLE ddl_demo.mixed_default_table
        ALTER COLUMN col_with_default DROP DEFAULT;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "mixed_default_table", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00002-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][2]["fields"]

    expected_fields = [
        {"id": 1, "name": "col_with_default", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "col_without_default",
            "required": False,
            "type": "int",
            "write-default": 43,
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_rest_autovacuum(
    pg_conn,
    superuser_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
    set_auto_vacuum_params,
):

    if installcheck:
        return

    # make sure that there are frequent enough autovacuum
    result = run_query(
        """
        SELECT current_setting('pg_lake_iceberg.autovacuum_naptime');
    """,
        pg_conn,
    )
    assert result[0][0] == "1s"

    run_command("CREATE SCHEMA test_rest_autovacuum;", pg_conn)
    run_command(
        "CREATE TABLE test_rest_autovacuum.test_rest_autovacuum(a int) USING iceberg WITH (catalog='rest')",
        pg_conn,
    )

    for i in range(0, 6):
        run_command(
            "INSERT INTO test_rest_autovacuum.test_rest_autovacuum VALUES (1)", pg_conn
        )
        pg_conn.commit()

    # wait at most 5 seconds
    compacted = False
    for i in range(0, 5):
        metadata = get_rest_table_metadata(
            "test_rest_autovacuum", "test_rest_autovacuum", pg_conn
        )
        metadata_path = metadata["metadata-location"]

        result = run_query(
            f"""
            SELECT count(*) FROM lake_iceberg.files('{metadata_path}')
        """,
            pg_conn,
        )
        if result[0][0] == 1:
            compacted = True
            break
        else:
            run_command("SELECT pg_sleep(1)", pg_conn)

    assert compacted, "autovacuum failed on rest catalog table"
    run_command("DROP SCHEMA test_rest_autovacuum CASCADE;", pg_conn)
    pg_conn.commit()


def test_ddl_partition_same_tx(
    pg_conn,
    superuser_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)

    # Create table with NOT NULL column
    run_command(
        """
        CREATE TABLE ddl_demo.part_ddl_same (
            "id of ddl"  INT NOT NULL,
            name TEXT NOT NULL,
            age INT NOT NULL
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # add a column, set partition in the same tx
    # insert a row and then drop partition by
    # insert another row

    run_command(
        """
            ALTER TABLE ddl_demo.part_ddl_same ADD COLUMN \"second id of ddl\" INT;

            
            INSERT INTO ddl_demo.part_ddl_same VALUES (1, 1, '1', 1);

            ALTER TABLE ddl_demo.part_ddl_same ADD COLUMN \"part_col\" INT;
            INSERT INTO ddl_demo.part_ddl_same VALUES (2, 2, '2', 2, 2);

            ALTER TABLE ddl_demo.part_ddl_same OPTIONS (ADD partition_by '\"part_col\"');
            INSERT INTO ddl_demo.part_ddl_same VALUES (3, 3, '3', 3, 3);

            ALTER TABLE ddl_demo.part_ddl_same OPTIONS (DROP partition_by);
            INSERT INTO ddl_demo.part_ddl_same VALUES (4, 4, '4', 4, 4);
            
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_metadata_on_pg_catalog_and_rest_matches(
        "ddl_demo", "part_ddl_same", superuser_conn
    )

    # end to end test
    for i in range(2, 5):
        print(i)
        res = run_query(
            f'SELECT "id of ddl" FROM ddl_demo.part_ddl_same WHERE "part_col" = {i}',
            pg_conn,
        )
        print(res)
        assert res[0][0] == i

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "part_ddl_same", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00001-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)

    # Access specific fields
    fields = parsed_data["schemas"][1]["fields"]
    assert len(fields) == 5

    assert parsed_data["last-partition-id"] == 1000
    assert parsed_data["default-spec-id"] == 0

    specs = parsed_data["partition-specs"]

    assert len(specs) == 2
    assert specs[0]["spec-id"] == 0
    assert len(specs[0]["fields"]) == 0

    assert specs[1]["spec-id"] == 1
    assert len(specs[1]["fields"]) == 1
    assert (specs[1]["fields"][0]["transform"]) == "identity"
    assert (specs[1]["fields"][0]["name"]) == "part_col"
    assert (specs[1]["fields"][0]["source-id"]) == 5
    assert (specs[1]["fields"][0]["field-id"]) == 1000

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def test_add_drop_same_schema_breaks(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):
    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)
    run_command(
        """
        CREATE TABLE ddl_demo.ad_table (
            id INT,
            val TEXT
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()
    metadata = get_rest_table_metadata("ddl_demo", "ad_table", pg_conn)
    metadata_path = metadata["metadata-location"]
    print(metadata_path)

    # Insert some rows
    run_command(
        """
        INSERT INTO ddl_demo.ad_table (id, val)
        VALUES (1, 'a'), (2, 'b');
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata = get_rest_table_metadata("ddl_demo", "ad_table", pg_conn)
    metadata_path = metadata["metadata-location"]
    print(metadata_path)

    rows = run_query(
        """
        SELECT id, val FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "a"],
        [2, "b"],
    ]

    # Add a new column (no default)
    run_command(
        """
        ALTER TABLE ddl_demo.ad_table
        ADD COLUMN extra INT;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata = get_rest_table_metadata("ddl_demo", "ad_table", pg_conn)
    metadata_path = metadata["metadata-location"]
    print(metadata_path)

    rows = run_query(
        """
        SELECT id, val, extra FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    # Existing rows get NULL in the new column
    assert rows == [
        [1, "a", None],
        [2, "b", None],
    ]

    # Update new column a bit
    run_command(
        """
        UPDATE ddl_demo.ad_table
        SET extra = 10
        WHERE id = 1;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata = get_rest_table_metadata("ddl_demo", "ad_table", pg_conn)
    metadata_path = metadata["metadata-location"]
    print(metadata_path)

    rows = run_query(
        """
        SELECT id, val, extra FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    assert rows == [
        [1, "a", 10],
        [2, "b", None],
    ]

    # Now drop the column again
    run_command(
        """
        ALTER TABLE ddl_demo.ad_table
        DROP COLUMN extra;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata = get_rest_table_metadata("ddl_demo", "ad_table", pg_conn)
    metadata_path = metadata["metadata-location"]
    print(metadata_path)

    rows = run_query(
        """
        SELECT id, val FROM ddl_demo.ad_table ORDER BY id;
    """,
        pg_conn,
    )
    # Data in original columns is still correct
    assert rows == [
        [1, "a"],
        [2, "b"],
    ]

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


# the comments in no_test_add_drop_same_schema_breaks applies as-is
def test_rest_rename_col_same_name(
    pg_conn,
    installcheck,
    s3,
    extension,
    create_types_helper_functions,
    with_default_location,
    polaris_session,
    set_polaris_gucs,
    create_http_helper_functions,
):

    if installcheck:
        return

    run_command("CREATE SCHEMA ddl_demo;", pg_conn)

    # Create table with NOT NULL column
    run_command(
        """
        CREATE TABLE ddl_demo.nn_table (
            id   INT NOT NULL,
            name TEXT NOT NULL
        ) USING iceberg WITH (catalog='rest');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """ 
        ALTER TABLE ddl_demo.nn_table RENAME COLUMN id TO id_new;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "nn_table", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00001-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)

    # push a new schema and set
    current_schema_id = parsed_data["current-schema-id"]
    assert current_schema_id == 1

    run_command(
        """ ALTER TABLE ddl_demo.nn_table RENAME COLUMN id_new TO id;""", pg_conn
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    metadata = get_rest_table_metadata("ddl_demo", "nn_table", pg_conn)
    metadata_path = metadata["metadata-location"]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00002-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)

    # set back to the initial schema
    current_schema_id = parsed_data["current-schema-id"]
    assert current_schema_id == 0

    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": True, "type": "int"},
        {"id": 2, "name": "name", "required": True, "type": "string"},
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    run_command("DROP SCHEMA ddl_demo CASCADE;", pg_conn)
    pg_conn.commit()


def assert_metadata_on_pg_catalog_and_rest_matches(
    namespace, table_name, superuser_conn
):
    metadata = get_rest_table_metadata(namespace, table_name, superuser_conn)

    assert_data_files_match(namespace, table_name, superuser_conn, metadata)
    assert_schemas_equal(namespace, table_name, superuser_conn, metadata)
    assert_partitions_equal(namespace, table_name, superuser_conn, metadata)


def assert_partitions_equal(namespace, table_name, superuser_conn, metadata):

    # 1) default-spec-id check
    catalog_default_spec_id = run_query(
        f"select default_spec_id "
        f"from lake_iceberg.tables_internal "
        f"WHERE table_name = '{namespace}.{table_name}'::regclass;",
        superuser_conn,
    )[0][0]

    metadata_default_spec_id = metadata["metadata"]["default-spec-id"]

    assert catalog_default_spec_id == metadata_default_spec_id, (
        f"default-spec-ids don't match: "
        f"catalog={catalog_default_spec_id}, metadata={metadata_default_spec_id}"
    )

    # 2) partition spec id checks
    specs = metadata["metadata"].get("partition-specs", [])
    metadata_spec_ids = sorted(spec["spec-id"] for spec in specs)

    catalog_specs_rows = run_query(
        f"""
        SELECT spec_id
        FROM lake_table.partition_specs
        WHERE table_name = '{namespace}.{table_name}'::regclass
        ORDER BY spec_id
        """,
        superuser_conn,
    )
    catalog_spec_ids = [row[0] for row in catalog_specs_rows]

    assert catalog_spec_ids == metadata_spec_ids, (
        f"partition spec ids don't match: "
        f"catalog={catalog_spec_ids}, metadata={metadata_spec_ids}"
    )

    # 3) partition fields check
    catalog_fields_rows = run_query(
        f"""
        SELECT spec_id,
               source_field_id,
               partition_field_id,
               partition_field_name,
               transform_name
        FROM lake_table.partition_fields
        WHERE table_name = '{namespace}.{table_name}'::regclass
        ORDER BY spec_id, partition_field_id
        """,
        superuser_conn,
    )

    # spec_id -> field listesi (dict)
    catalog_fields_by_spec = {}
    for (
        spec_id,
        source_field_id,
        partition_field_id,
        partition_field_name,
        transform_name,
    ) in catalog_fields_rows:
        catalog_fields_by_spec.setdefault(spec_id, []).append(
            {
                "source-id": source_field_id,
                "field-id": partition_field_id,
                "name": partition_field_name,
                "transform": transform_name,
            }
        )

    for spec in specs:
        spec_id = spec["spec-id"]

        metadata_fields = [
            {
                "source-id": f["source-id"],
                "field-id": f["field-id"],
                "name": f["name"],
                "transform": f["transform"],
            }
            for f in spec.get("fields", [])
        ]

        metadata_fields_sorted = sorted(metadata_fields, key=lambda f: f["field-id"])
        catalog_fields_sorted = sorted(
            catalog_fields_by_spec.get(spec_id, []),
            key=lambda f: f["field-id"],
        )

        assert catalog_fields_sorted == metadata_fields_sorted, (
            f"partition fields don't match for spec_id {spec_id}: "
            f"catalog={catalog_fields_sorted}, metadata={metadata_fields_sorted}"
        )


def assert_schemas_equal(namespace, table_name, superuser_conn, metadata):
    """
    Compares a list of Iceberg-like schema dicts (with 'fields') to a list of rows
    shaped as [id, name, required, type]. Ignores ordering and normalizes type names.
    """

    def norm_type(t: str) -> str:
        t = str(t).strip().lower()
        aliases = {
            # common synonyms
            "int": "integer",
            "integer": "integer",
            "long": "bigint",
            "bigint": "bigint",
            "short": "smallint",
            "smallint": "smallint",
            "bool": "boolean",
            "boolean": "boolean",
            "float": "float",
            "double": "double",
            "str": "string",
            "string": "text",
            "timestamp_tz": "timestamp_tz",
            "timestamptz": "timestamp_tz",
            "timestamp": "timestamp",
            "date": "date",
            "time": "time",
            "uuid": "uuid",
            "binary": "binary",
            "decimal": "decimal",
        }
        return aliases.get(t, t)

    # schema checks
    schemas = metadata["metadata"].get("schemas", [])
    last_schema = [schemas[-1]]

    catalog_schemas_rows = run_query(
        f"""
        SELECT 
            f.field_id, a.attname, a.attnotnull, f.field_pg_type
        FROM
            lake_table.field_id_mappings f JOIN pg_attribute a ON (a.attrelid = f.table_name and a.attnum=f.pg_attnum) 
        WHERE table_name = '{namespace}.{table_name}'::regclass
        """,
        superuser_conn,
    )

    # Normalize/flatten the 'schemas' into rows
    schema_rows = []
    for s in last_schema or []:
        for f in s.get("fields", []):
            schema_rows.append(
                [
                    int(f["id"]),
                    str(f["name"]),
                    bool(f["required"]),
                    norm_type(f["type"]),
                ]
            )

    # Normalize the catalog rows
    cat_rows = []
    for r in catalog_schemas_rows or []:
        cat_rows.append(
            [
                int(r[0]),
                str(r[1]),
                bool(r[2]),
                norm_type(r[3]),
            ]
        )

    # Sort by (id, name) for deterministic, order-insensitive comparison
    schema_rows_sorted = sorted(schema_rows, key=lambda x: (x[0], x[1]))
    cat_rows_sorted = sorted(cat_rows, key=lambda x: (x[0], x[1]))

    assert schema_rows_sorted == cat_rows_sorted, (
        "Schema mismatch.\n"
        f"From schemas: {schema_rows_sorted}\n"
        f"From catalog: {cat_rows_sorted}"
    )


def assert_data_files_match(namespace, table_name, superuser_conn, metadata):

    metadata_location = metadata["metadata-location"]

    data_files_metadata = pg_lake_iceberg_files(superuser_conn, metadata_location)

    data_files_pg_catalog_agg = run_query(
        f"""
            SELECT
              f.path,
              COALESCE(
                jsonb_object_agg(
                  dfcs.field_id::text,
                  to_jsonb(dfcs.lower_bound)
                ) FILTER (WHERE dfcs.field_id IS NOT NULL),
                '{{}}'::jsonb
              ) AS lower_bounds,
              COALESCE(
                jsonb_object_agg(
                  dfcs.field_id::text,
                  to_jsonb(dfcs.upper_bound)
                ) FILTER (WHERE dfcs.field_id IS NOT NULL),
                '{{}}'::jsonb
              ) AS upper_bounds
            FROM lake_table.files f
            LEFT JOIN lake_table.data_file_column_stats dfcs
              ON dfcs.table_name = f.table_name
             AND dfcs.path = f.path
            WHERE f.table_name = '{namespace}.{table_name}'::regclass
            GROUP BY f.path
            ORDER BY f.path;
            """,
        superuser_conn,
    )

    print(data_files_metadata)
    print(data_files_pg_catalog_agg)

    def canon_json(v):
        # Parse stringified JSON into Python types first
        if not isinstance(v, (dict, list)):
            v = json.loads(str(v))

        def coerce(x):
            # Recurse first
            if isinstance(x, list):
                return [coerce(i) for i in x]
            if isinstance(x, dict):
                return {str(k): coerce(val) for k, val in x.items()}

            # Coerce leaf values so "100" and 100 compare equal
            if isinstance(x, str):
                s = x.strip()
                if s.lower() in ("true", "false"):
                    return s.lower() == "true"
                try:
                    d = Decimal(s)
                    # Prefer ints when exact; otherwise use Decimal->float conservatively
                    return int(d) if d == d.to_integral_value() else float(d)
                except:
                    return s

            if isinstance(x, (int, float)):
                try:
                    d = Decimal(str(x))
                    return int(d) if d == d.to_integral_value() else float(d)
                except:
                    return x

            # Leave booleans/None and other scalars as-is
            return x

        coerced = coerce(v)
        return json.dumps(coerced, sort_keys=True, separators=(",", ":"))

    # Left: from pg_lake_read_data_file_stats (ignore seq at index 1)
    left = sorted(
        (str(r[0]).strip(), canon_json(r[2]), canon_json(r[3]))
        for r in (data_files_metadata or [])
    )
    # Right: from the aggregated SQL above
    right = sorted(
        (str(r[0]).strip(), canon_json(r[1]), canon_json(r[2]))
        for r in (data_files_pg_catalog_agg or [])
    )

    assert left == right, (
        "Data file column stats mismatch.\n"
        f"Only in metadata: {sorted(set(left) - set(right))[:5]}\n"
        f"Only in pg_catalog: {sorted(set(right) - set(left))[:5]}"
    )


def pg_lake_iceberg_files(superuser_conn, metadata_location):
    datafile_paths = run_query(
        f"""
        SELECT * FROM lake_iceberg.data_file_stats('{metadata_location}');

""",
        superuser_conn,
    )

    return datafile_paths


def ensure_table_dropped(encoded_namespace, encoded_table_name, pg_conn):

    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{encoded_namespace}/tables/{encoded_table_name}"
    token = get_polaris_access_token()

    try:
        res = run_query(
            f"""
            SELECT *
            FROM lake_iceberg.test_http_head(
             '{url}',
             ARRAY['Authorization: Bearer {token}']);
            """,
            pg_conn,
        )

        # should throw error, so never get here
        assert False
    except Exception as e:
        assert "404" in str(e)
        pg_conn.rollback()


def get_rest_table_metadata(encoded_namespace, encoded_table_name, pg_conn):

    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{encoded_namespace}/tables/{encoded_table_name}"
    token = get_polaris_access_token()

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    assert res[0][0] == 200
    status, json_str, headers = res[0]

    return json.loads(json_str)


def get_current_manifests(pg_conn, tbl_namespace, tbl_name):
    metadata = get_rest_table_metadata(tbl_namespace, tbl_name, pg_conn)
    metadata_location = metadata["metadata-location"]
    manifests = run_query(
        f"""SELECT sequence_number, partition_spec_id,
                                   added_files_count, added_rows_count,
                                   existing_files_count, existing_rows_count,
                                   deleted_files_count, deleted_rows_count
                              FROM lake_iceberg.current_manifests('{metadata_location}')
                              ORDER BY sequence_number, partition_spec_id,
                                 added_files_count, added_rows_count,
                                 existing_files_count, existing_rows_count,
                                 deleted_files_count, deleted_rows_count ASC""",
        pg_conn,
    )
    return manifests


@pytest.fixture(scope="module")
def set_polaris_gucs(
    superuser_conn,
    extension,
    installcheck,
    credentials_file: str = server_params.POLARIS_PRINCIPAL_CREDS_FILE,
):
    if not installcheck:

        creds = json.loads(Path(credentials_file).read_text())
        client_id = creds["credentials"]["clientId"]
        client_secret = creds["credentials"]["clientSecret"]

        run_command_outside_tx(
            [
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_host TO '{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}'""",
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_id TO '{client_id}'""",
                f"""ALTER SYSTEM SET pg_lake_iceberg.rest_catalog_client_secret TO '{client_secret}'""",
                "SELECT pg_reload_conf()",
            ],
            superuser_conn,
        )

    yield

    if not installcheck:

        run_command_outside_tx(
            [
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_host""",
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_id""",
                f"""ALTER SYSTEM RESET pg_lake_iceberg.rest_catalog_client_secret""",
                "SELECT pg_reload_conf()",
            ],
            superuser_conn,
        )


@pytest.fixture(scope="module")
def create_http_helper_functions(superuser_conn, extension):
    run_command(
        f"""
       CREATE TYPE lake_iceberg.http_result AS (
            status        int,
            body          text,
            resp_headers  text
        );

        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_get(
                url     text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_get'
        LANGUAGE C;


        -- HEAD
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_head(
                url     text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_head'
        LANGUAGE C;

        -- POST
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_post(
                url     text,
                body    text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_post'
        LANGUAGE C;

        -- PUT
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_put(
                url     text,
                body    text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_put'
        LANGUAGE C;

        -- DELETE
        CREATE OR REPLACE FUNCTION lake_iceberg.test_http_delete(
                url     text,
                headers text[] DEFAULT NULL)
        RETURNS lake_iceberg.http_result
        AS 'pg_lake_iceberg', 'test_http_delete'
        LANGUAGE C;

        -- URL encode function
        CREATE OR REPLACE FUNCTION lake_iceberg.url_encode(input TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$url_encode_path$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.url_encode_path(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$url_encode_path$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.register_namespace_to_rest_catalog(TEXT,TEXT)
        RETURNS void
         LANGUAGE C
         VOLATILE STRICT
        AS 'pg_lake_iceberg', $function$register_namespace_to_rest_catalog$function$;


        CREATE OR REPLACE FUNCTION lake_iceberg.current_manifests(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                manifest_path TEXT,
                manifest_length BIGINT,
                partition_spec_id INT,
                manifest_content TEXT,
                sequence_number BIGINT,
                min_sequence_number BIGINT,
                added_snapshot_id BIGINT,
                added_files_count INT,
                existing_files_count INT,
                deleted_files_count INT,
                added_rows_count BIGINT,
                existing_rows_count BIGINT,
                deleted_rows_count BIGINT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_manifests$function$;


""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        """
        DROP FUNCTION IF EXISTS lake_iceberg.url_encode;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_get;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_head;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_post;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_put;
        DROP FUNCTION IF EXISTS lake_iceberg.test_http_delete;
        DROP TYPE lake_iceberg.http_result;
        DROP FUNCTION IF EXISTS lake_iceberg.url_encode_path;
        DROP FUNCTION IF EXISTS lake_iceberg.register_namespace_to_rest_catalog;
        DROP FUNCTION IF EXISTS lake_iceberg.datafile_paths_from_table_metadata;
        DROP FUNCTION IF EXISTS lake_iceberg.current_manifests;
                """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="function")
def grant_access_to_tables_internal(
    extension,
    app_user,
    superuser_conn,
):
    run_command(
        f"""GRANT SELECT ON lake_iceberg.tables_internal TO {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""REVOKE SELECT ON lake_iceberg.tables_internal FROM {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="function")
def adjust_object_store_settings(superuser_conn):
    superuser_conn.autocommit = True

    # catalog=object_store requires the IcebergDefaultLocationPrefix set
    # and accessible by other sessions (e.g., push catalog worker),
    # and with_default_location only does a session level
    run_command(
        f"""ALTER SYSTEM SET pg_lake_iceberg.object_store_catalog_location_prefix = 's3://{TEST_BUCKET}';""",
        superuser_conn,
    )

    # to be able to read the same tables that we write, use the same prefix
    run_command(
        f"""
        ALTER SYSTEM SET pg_lake_iceberg.internal_object_store_catalog_prefix = 'tmp';
        """,
        superuser_conn,
    )

    run_command(
        f"""
        ALTER SYSTEM SET pg_lake_iceberg.external_object_store_catalog_prefix = 'tmp';
        """,
        superuser_conn,
    )

    superuser_conn.autocommit = False

    run_command("SELECT pg_reload_conf()", superuser_conn)

    # unfortunate, but Postgres requires a bit of time before
    # bg workers get the reload
    run_command("SELECT pg_sleep(0.1)", superuser_conn)
    superuser_conn.commit()
    yield

    superuser_conn.autocommit = True
    run_command(
        f"""
        ALTER SYSTEM RESET pg_lake_iceberg.object_store_catalog_location_prefix;
        """,
        superuser_conn,
    )
    run_command(
        f"""
        ALTER SYSTEM RESET pg_lake_iceberg.internal_object_store_catalog_prefix;
       """,
        superuser_conn,
    )
    run_command(
        f"""
        ALTER SYSTEM RESET pg_lake_iceberg.external_object_store_catalog_prefix;
        """,
        superuser_conn,
    )
    superuser_conn.autocommit = False

    run_command("SELECT pg_reload_conf()", superuser_conn)
    superuser_conn.commit()


@pytest.fixture(scope="module")
def create_types_helper_functions(superuser_conn, app_user):

    run_command(
        f"""
        CREATE SCHEMA test_iceberg_base_types_sc;
        CREATE OR REPLACE FUNCTION test_iceberg_base_types_sc.initial_metadata_for_table(tableOid Oid)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_table', $function$initial_metadata_for_table$function$;

        CREATE OR REPLACE FUNCTION test_iceberg_base_types_sc.iceberg_table_fieldids(tableOid Oid)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_table', $function$iceberg_table_fieldids$function$;

        GRANT USAGE ON SCHEMA test_iceberg_base_types_sc TO {app_user};
        GRANT SELECT ON lake_iceberg.tables TO {app_user};
        GRANT EXECUTE ON FUNCTION test_iceberg_base_types_sc.initial_metadata_for_table(oid) TO {app_user};
        GRANT EXECUTE ON FUNCTION test_iceberg_base_types_sc.iceberg_table_fieldids(oid) TO {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP SCHEMA test_iceberg_base_types_sc CASCADE;
        REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()


def compare_fields(expected, actual):
    """Recursively compare fields in nested dictionaries."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in expected:
            assert key in actual, f"Key {key} missing in actual"
            if isinstance(expected[key], dict) or isinstance(expected[key], list):
                compare_fields(expected[key], actual[key])
            else:
                assert (
                    expected[key] == actual[key]
                ), f"Value mismatch for key {key}: expected {expected[key]} but got {actual[key]}"
    elif isinstance(expected, list) and isinstance(actual, list):
        assert len(expected) == len(
            actual
        ), f"List length mismatch: expected {len(expected)} but got {len(actual)}"
        for exp_item, act_item in zip(expected, actual):
            compare_fields(exp_item, act_item)
    else:
        assert (
            expected == actual
        ), f"Value mismatch: expected {expected} but got {actual}"


# run tests faster
@pytest.fixture(autouse=True, scope="function")
def set_auto_vacuum_params(superuser_conn):
    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_table.vacuum_compact_min_input_files = 1;",
            "ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime TO '1s';",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
    yield
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_table.vacuum_compact_min_input_files;",
            "ALTER SYSTEM RESET pg_lake_iceberg.autovacuum_naptime",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )

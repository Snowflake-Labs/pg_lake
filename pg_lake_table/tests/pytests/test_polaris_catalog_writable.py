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
        f"""CREATE TABLE test_writable_rest_basic_flow.writable_rest USING iceberg WITH (catalog='rest') AS SELECT 100 AS a""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_writable_rest_basic_flow.writable_rest_2 USING iceberg WITH (catalog='rest') AS SELECT 1000 AS a""",
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
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.writable_rest USING iceberg WITH (catalog='rest') AS SELECT 100 AS a""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.writable_rest_2 USING iceberg WITH (catalog='rest') AS SELECT 1000 AS a""",
        pg_conn,
    )

    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.writable_rest_3 USING iceberg WITH (catalog='rest', partition_by='a') AS SELECT 10000 AS a UNION SELECT 10001 as a""",
        pg_conn,
    )

    pg_conn.commit()

    # a DDL to a single table
    run_command(
        "ALTER TABLE test_writable_rest_ddl.writable_rest ADD COLUMN b INT", pg_conn
    )
    pg_conn.commit()
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_1() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
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
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_2() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
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
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_3() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
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
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_4() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest_2')""",
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
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_5() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
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
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_6() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest_2')""",
        pg_conn,
    )
    run_command(
        f"""CREATE TABLE test_writable_rest_ddl.readable_rest_7() USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='writable_rest')""",
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


def test_writable_rest_vacuum(
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

    run_command(f"""CREATE SCHEMA test_writable_rest_vacuum""", pg_conn)
    run_command(
        f"""CREATE TABLE test_writable_rest_vacuum.writable_rest USING iceberg WITH (catalog='rest', autovacuum_enabled=False) AS SELECT 100 AS a""",
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
    assert len(file_paths_q) == 12

    run_command_outside_tx(
        [
            "SET pg_lake_engine.orphaned_file_retention_period TO 0",
            f"VACUUM FULL test_writable_rest_vacuum.writable_rest",
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
            "string": "string",
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
                except InvalidOperation:
                    return s

            if isinstance(x, (int, float)):
                try:
                    d = Decimal(str(x))
                    return int(d) if d == d.to_integral_value() else float(d)
                except InvalidOperation:
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

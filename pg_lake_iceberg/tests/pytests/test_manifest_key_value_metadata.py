"""Manifests carry the Avro key-value metadata the Iceberg spec requires
(schema, schema-id, partition-spec, partition-spec-id, format-version,
content). DuckDB does not read these keys, so only an external reader
notices when they are missing: https://github.com/Snowflake-Labs/pg_lake/issues/659
"""

import json

import pytest
from utils_pytest import *

LOCATION = f"s3://{TEST_BUCKET}/manifest_kv_metadata"


def _table_metadata(pg_conn, s3, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]
    return json.loads(read_s3_operations(s3, metadata_location))


@pytest.mark.location_prefix(LOCATION)
def test_unpartitioned_manifest_metadata(pg_conn, extension, with_default_location, s3):
    run_command(
        "CREATE TABLE manifest_kv_plain (id int, name text) USING iceberg", pg_conn
    )
    run_command("INSERT INTO manifest_kv_plain VALUES (1, 'a'), (2, 'b')", pg_conn)
    pg_conn.commit()

    metadata = _table_metadata(pg_conn, s3, "manifest_kv_plain")
    manifests = assert_manifests_have_table_metadata(s3, metadata)
    assert [manifest["content"] for manifest in manifests] == [0]

    run_command("DROP TABLE manifest_kv_plain", pg_conn)
    pg_conn.commit()


@pytest.mark.location_prefix(LOCATION)
def test_evolved_table_manifest_metadata(pg_conn, extension, with_default_location, s3):
    table = "manifest_kv_evolved"
    run_command(
        f"CREATE TABLE {table} (id int, value text) USING iceberg "
        f"WITH (partition_by = 'bucket(4, id)')",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {table} SELECT i, 'v' || i FROM generate_series(1, 100) i",
        pg_conn,
    )
    pg_conn.commit()

    # schema change and a write in the same transaction
    run_command(f"ALTER TABLE {table} ADD COLUMN extra int", pg_conn)
    run_command(f"INSERT INTO {table} VALUES (101, 'v101', 1)", pg_conn)
    pg_conn.commit()

    # partition spec change, then a write with the new spec
    run_command(f"ALTER TABLE {table} OPTIONS (SET partition_by 'id')", pg_conn)
    pg_conn.commit()
    run_command(f"INSERT INTO {table} VALUES (102, 'v102', 2)", pg_conn)
    pg_conn.commit()

    # a small delete becomes a position delete file, and rewrites nothing
    run_command("SET pg_lake_table.copy_on_write_threshold TO 100", pg_conn)
    run_command(f"DELETE FROM {table} WHERE id = 5", pg_conn)
    run_command("RESET pg_lake_table.copy_on_write_threshold", pg_conn)
    pg_conn.commit()

    metadata = _table_metadata(pg_conn, s3, table)
    manifests = assert_manifests_have_table_metadata(s3, metadata)

    assert len(metadata["schemas"]) == 2
    assert len({manifest["partition_spec_id"] for manifest in manifests}) == 2
    assert {manifest["content"] for manifest in manifests} == {0, 1}

    # compaction rewrites the files, so their manifests are written anew
    run_command_outside_tx([f"VACUUM FULL {table}"])

    metadata = _table_metadata(pg_conn, s3, table)
    assert_manifests_have_table_metadata(s3, metadata)

    run_command(f"DROP TABLE {table}", pg_conn)
    pg_conn.commit()

import json

import pytest
from utils_pytest import *

from test_writable_iceberg_common import *


def metadata_snapshots(table, pg_conn, s3):
    location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table}'",
        pg_conn,
    )[0][0]
    return json.loads(read_s3_operations(s3, location))["snapshots"]


def assert_parents_resolve(snapshots):
    snapshot_ids = {snapshot["snapshot-id"] for snapshot in snapshots}
    for snapshot in snapshots:
        if "parent-snapshot-id" in snapshot:
            assert snapshot["parent-snapshot-id"] in snapshot_ids, snapshot


def test_snapshots_point_to_previous_snapshot(
    s3, pg_conn, extension, with_default_location
):
    pg_conn.autocommit = True

    run_command(
        "CREATE TABLE parent_ids (id int) USING iceberg WITH (autovacuum_enabled = false)",
        pg_conn,
    )
    for i in range(3):
        run_command(f"INSERT INTO parent_ids VALUES ({i})", pg_conn)

    snapshots = metadata_snapshots("parent_ids", pg_conn, s3)
    assert len(snapshots) == 3
    assert "parent-snapshot-id" not in snapshots[0]
    assert snapshots[1]["parent-snapshot-id"] == snapshots[0]["snapshot-id"]
    assert snapshots[2]["parent-snapshot-id"] == snapshots[1]["snapshot-id"]

    run_command("DROP TABLE parent_ids", pg_conn)
    pg_conn.autocommit = False


@pytest.mark.parametrize("expire_during", ["insert", "vacuum"])
def test_expired_parent_snapshot_is_not_referenced(
    s3,
    pg_conn,
    extension,
    with_default_location,
    allow_iceberg_guc_perms,
    expire_during,
):
    table = f"expired_parent_{expire_during}"
    pg_conn.autocommit = True

    run_command(
        f"CREATE TABLE {table} (id int) USING iceberg WITH (autovacuum_enabled = false)",
        pg_conn,
    )
    rows = [0, 1, 2]
    for i in rows:
        run_command(f"INSERT INTO {table} VALUES ({i})", pg_conn)

    run_command(
        f"ALTER FOREIGN TABLE {table} OPTIONS (ADD max_snapshot_age '0')",
        pg_conn,
    )
    if expire_during == "insert":
        rows.append(3)
        run_command(f"INSERT INTO {table} VALUES (3)", pg_conn)
    else:
        run_command("SET pg_lake_table.vacuum_compact_min_input_files TO 1000", pg_conn)
        run_command(f"VACUUM {table}", pg_conn)
        run_command("RESET pg_lake_table.vacuum_compact_min_input_files", pg_conn)

    # only the newest snapshot is kept, and its parent was expired
    snapshots = metadata_snapshots(table, pg_conn, s3)
    assert len(snapshots) == 1
    assert "parent-snapshot-id" not in snapshots[0]

    # later snapshots still point to the kept snapshot
    run_command(f"ALTER FOREIGN TABLE {table} OPTIONS (DROP max_snapshot_age)", pg_conn)
    rows.append(4)
    run_command(f"INSERT INTO {table} VALUES (4)", pg_conn)

    snapshots = metadata_snapshots(table, pg_conn, s3)
    assert len(snapshots) == 2
    assert "parent-snapshot-id" not in snapshots[0]
    assert snapshots[1]["parent-snapshot-id"] == snapshots[0]["snapshot-id"]
    assert_parents_resolve(snapshots)

    result = run_query(f"SELECT id FROM {table} ORDER BY id", pg_conn)
    assert result == [[i] for i in rows]

    run_command(f"DROP TABLE {table}", pg_conn)
    pg_conn.autocommit = False

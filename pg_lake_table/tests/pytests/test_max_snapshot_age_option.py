import json
import pytest
from utils_pytest import *

from test_writable_iceberg_common import *


def get_snapshot_count(pg_conn, table_name, schema=None):
    """Get the number of snapshots for an Iceberg table by reading metadata from S3."""
    if schema:
        qualified = f"'{table_name}' and table_namespace='{schema}'"
    else:
        qualified = f"'{table_name}'"
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = {qualified}",
        pg_conn,
    )[0][0]
    count = run_query(
        f"SELECT count(*) FROM lake_iceberg.snapshots('{metadata_location}')",
        pg_conn,
    )[0][0]
    return count


def test_max_snapshot_age_option_expire_on_write(
    s3, pg_conn, extension, with_default_location, allow_iceberg_guc_perms
):
    """When max_snapshot_age is set to 0 on the table, old snapshots are expired
    during writes without requiring VACUUM."""
    pg_conn.autocommit = True

    run_command(
        """
        CREATE TABLE test_expire_on_write (id int) USING iceberg
            WITH (autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # insert 3 rows in separate transactions to create 3 snapshots
    run_command("INSERT INTO test_expire_on_write VALUES (1)", pg_conn)
    run_command("INSERT INTO test_expire_on_write VALUES (2)", pg_conn)
    run_command("INSERT INTO test_expire_on_write VALUES (3)", pg_conn)

    assert get_snapshot_count(pg_conn, "test_expire_on_write") == 3

    # set max_snapshot_age to 0 on the table
    run_command(
        "ALTER FOREIGN TABLE test_expire_on_write OPTIONS (ADD max_snapshot_age '0')",
        pg_conn,
    )

    # the next write should expire old snapshots automatically
    run_command("INSERT INTO test_expire_on_write VALUES (4)", pg_conn)

    # only the current snapshot should remain
    assert get_snapshot_count(pg_conn, "test_expire_on_write") == 1

    # verify data is still intact
    result = run_query("SELECT id FROM test_expire_on_write ORDER BY id", pg_conn)
    assert result == [[1], [2], [3], [4]]

    run_command("DROP TABLE test_expire_on_write", pg_conn)
    pg_conn.autocommit = False


def test_max_snapshot_age_option_no_expire_without_option(
    s3, pg_conn, extension, with_default_location, allow_iceberg_guc_perms
):
    """Without the table-level option, writes do not expire snapshots
    (even if the GUC default is non-zero)."""
    pg_conn.autocommit = True

    run_command(
        """
        CREATE TABLE test_no_expire (id int) USING iceberg
            WITH (autovacuum_enabled = false);
        """,
        pg_conn,
    )

    run_command("INSERT INTO test_no_expire VALUES (1)", pg_conn)
    run_command("INSERT INTO test_no_expire VALUES (2)", pg_conn)
    run_command("INSERT INTO test_no_expire VALUES (3)", pg_conn)

    # snapshots should accumulate without expiration
    assert get_snapshot_count(pg_conn, "test_no_expire") == 3

    run_command("DROP TABLE test_no_expire", pg_conn)
    pg_conn.autocommit = False


def test_max_snapshot_age_option_used_during_vacuum(
    s3, pg_conn, extension, with_default_location, allow_iceberg_guc_perms
):
    """The table-level max_snapshot_age is respected during VACUUM as well."""
    pg_conn.autocommit = True

    run_command(
        """
        CREATE TABLE test_vacuum_option (id int) USING iceberg
            WITH (autovacuum_enabled = false);
        """,
        pg_conn,
    )

    run_command("INSERT INTO test_vacuum_option VALUES (1)", pg_conn)
    run_command("INSERT INTO test_vacuum_option VALUES (2)", pg_conn)
    run_command("INSERT INTO test_vacuum_option VALUES (3)", pg_conn)

    assert get_snapshot_count(pg_conn, "test_vacuum_option") == 3

    # set table-level option
    run_command(
        "ALTER FOREIGN TABLE test_vacuum_option OPTIONS (ADD max_snapshot_age '0')",
        pg_conn,
    )

    # VACUUM should use the table-level setting to expire snapshots
    run_command("SET pg_lake_table.vacuum_compact_min_input_files TO 1000", pg_conn)
    run_command("VACUUM test_vacuum_option", pg_conn)

    assert get_snapshot_count(pg_conn, "test_vacuum_option") == 1

    run_command("RESET pg_lake_table.vacuum_compact_min_input_files", pg_conn)
    run_command("DROP TABLE test_vacuum_option", pg_conn)
    pg_conn.autocommit = False


def test_max_snapshot_age_option_overrides_guc(
    s3, pg_conn, extension, with_default_location, allow_iceberg_guc_perms
):
    """The table-level max_snapshot_age takes precedence over the GUC."""
    pg_conn.autocommit = True

    # set GUC to 0 (would normally expire everything)
    run_command("SET pg_lake_iceberg.max_snapshot_age TO 0", pg_conn)

    run_command(
        """
        CREATE TABLE test_override_guc (id int) USING iceberg
            WITH (autovacuum_enabled = false, max_snapshot_age = 1000000);
        """,
        pg_conn,
    )

    run_command("INSERT INTO test_override_guc VALUES (1)", pg_conn)
    run_command("INSERT INTO test_override_guc VALUES (2)", pg_conn)
    run_command("INSERT INTO test_override_guc VALUES (3)", pg_conn)

    # table-level setting of 1000000 seconds should prevent expiration
    # even though the GUC is 0
    assert get_snapshot_count(pg_conn, "test_override_guc") == 3

    run_command("RESET pg_lake_iceberg.max_snapshot_age", pg_conn)
    run_command("DROP TABLE test_override_guc", pg_conn)
    pg_conn.autocommit = False


def test_max_snapshot_age_option_consecutive_writes(
    s3, pg_conn, extension, with_default_location, allow_iceberg_guc_perms
):
    """With max_snapshot_age=0, each write keeps only one snapshot."""
    pg_conn.autocommit = True

    run_command(
        """
        CREATE TABLE test_consecutive (id int) USING iceberg
            WITH (autovacuum_enabled = false, max_snapshot_age = 0);
        """,
        pg_conn,
    )

    for i in range(5):
        run_command(f"INSERT INTO test_consecutive VALUES ({i})", pg_conn)
        assert get_snapshot_count(pg_conn, "test_consecutive") == 1

    # all data should still be present
    result = run_query("SELECT count(*) FROM test_consecutive", pg_conn)
    assert result == [[5]]

    run_command("DROP TABLE test_consecutive", pg_conn)
    pg_conn.autocommit = False

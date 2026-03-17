import pytest
from utils_pytest import *


def setup_three_file_table(pg_conn, table_name):
    """Creates an iceberg table with 100 rows spread across 3 data files."""
    run_command(
        f"CREATE TABLE {table_name} (id int, value text) USING pg_lake_iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    # Three separate commits produce three data files.
    # File 1: ids 1-100, File 2: ids 101-200, File 3: ids 201-300.
    for start, letter in [(1, "a"), (101, "b"), (201, "c")]:
        run_command(
            f"INSERT INTO {table_name} SELECT i, '{letter}' FROM generate_series({start}, {start + 99}) i",
            pg_conn,
        )
        pg_conn.commit()


def count_files_by_content(pg_conn, table_name, content):
    return run_query(
        f"""
        SELECT count(*) FROM lake_table.files
        WHERE table_name = '{table_name}'::regclass AND content = {content}
        """,
        pg_conn,
    )[0]["count"]


def test_max_delete_rows_within_single_operation(
    s3, pg_conn, extension, with_default_location
):
    """
    Within a single DELETE touching three files, once accumulated position-deleted
    rows reach the limit the remaining files switch to copy-on-write.

    limit=2: file 1 and file 2 use merge-on-read (total 0→1→2), file 3 triggers
    copy-on-write because the counter already equals the limit.
    """
    table_name = "test_cow_max_rows_single_op"
    setup_three_file_table(pg_conn, table_name)

    # Per-file threshold set to 100 so only the global limit drives the decision.
    run_command(
        f"""
        SET pg_lake_table.copy_on_write_threshold TO 100;
        SET pg_lake_table.copy_on_write_max_delete_rows TO 2;
        DELETE FROM {table_name} WHERE id IN (1, 101, 201);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Files 1 and 2 get a position delete; file 3 is rewritten in place.
    assert count_files_by_content(pg_conn, table_name, 1) == 2
    assert count_files_by_content(pg_conn, table_name, 0) == 3
    check_table_size(pg_conn, table_name, 297)

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def test_max_delete_rows_across_operations(
    s3, pg_conn, extension, with_default_location
):
    """
    Pre-existing position deletes from earlier operations count toward the limit,
    causing copy-on-write in a later operation even though the new delete is small.

    limit=2:
      Op1 (id=1, file 1):   catalog=0 < 2 → merge-on-read, catalog becomes 1
      Op2 (id=101, file 2): catalog=1 < 2 → merge-on-read, catalog becomes 2
      Op3 (id=201, file 3): catalog=2 >= 2 → copy-on-write, no new delete file
    """
    table_name = "test_cow_max_rows_cross_op"
    setup_three_file_table(pg_conn, table_name)

    for row_id in (1, 101, 201):
        run_command(
            f"""
            SET pg_lake_table.copy_on_write_threshold TO 100;
            SET pg_lake_table.copy_on_write_max_delete_rows TO 2;
            DELETE FROM {table_name} WHERE id = {row_id};
            """,
            pg_conn,
        )
        pg_conn.commit()

    # Ops 1 and 2 produced position deletes; Op 3 did copy-on-write.
    assert count_files_by_content(pg_conn, table_name, 1) == 2
    assert count_files_by_content(pg_conn, table_name, 0) == 3
    check_table_size(pg_conn, table_name, 297)

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def test_max_delete_rows_disabled(s3, pg_conn, extension, with_default_location):
    """
    Setting copy_on_write_max_delete_rows = -1 disables the global limit entirely.
    All three files use merge-on-read as long as the per-file threshold is not hit.
    """
    table_name = "test_cow_max_rows_disabled"
    setup_three_file_table(pg_conn, table_name)

    run_command(
        f"""
        SET pg_lake_table.copy_on_write_threshold TO 100;
        SET pg_lake_table.copy_on_write_max_delete_rows TO -1;
        DELETE FROM {table_name} WHERE id IN (1, 101, 201);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert count_files_by_content(pg_conn, table_name, 1) == 3
    assert count_files_by_content(pg_conn, table_name, 0) == 3
    check_table_size(pg_conn, table_name, 297)

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def test_vacuum_compacts_when_approaching_max_delete_rows(
    s3, pg_conn, extension, with_default_location
):
    """
    When the table-wide position-deleted row count exceeds 50% of the limit,
    VACUUM compacts any file with deletions even if the per-file percentage is
    below copy_on_write_threshold.

    limit=10, 50% threshold=5.  Deleting 6 rows (3 per file) via merge-on-read
    leaves 6 > 5 position-deleted rows, so VACUUM should compact both files.
    """
    table_name = "test_cow_max_rows_vacuum"

    run_command(
        f"""
        SET pg_lake_table.vacuum_compact_min_input_files TO 1;
        CREATE TABLE {table_name} (id int, value text) USING pg_lake_iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Two files of 100 rows each.
    run_command(
        f"INSERT INTO {table_name} SELECT i, 'a' FROM generate_series(1, 100) i",
        pg_conn,
    )
    pg_conn.commit()
    run_command(
        f"INSERT INTO {table_name} SELECT i, 'b' FROM generate_series(101, 200) i",
        pg_conn,
    )
    pg_conn.commit()

    # 3% per-file deletion rate is well below the 20% per-file threshold, so
    # without the global limit these would never be compacted by VACUUM.
    run_command(
        f"""
        SET pg_lake_table.copy_on_write_threshold TO 100;
        SET pg_lake_table.copy_on_write_max_delete_rows TO 10;
        DELETE FROM {table_name} WHERE id IN (1, 2, 3, 101, 102, 103);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert count_files_by_content(pg_conn, table_name, 1) == 2

    # VACUUM must run outside a transaction.
    # The GUC is already set to 10 from the SET above (session-level, not LOCAL).
    pg_conn.commit()
    pg_conn.autocommit = True
    run_command(f"VACUUM {table_name};", pg_conn)
    pg_conn.autocommit = False

    # Position delete files should be gone after compaction.
    assert count_files_by_content(pg_conn, table_name, 1) == 0
    check_table_size(pg_conn, table_name, 194)

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()

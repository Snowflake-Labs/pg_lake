import pytest
from utils_pytest import *


def test_full_lifecycle(s3, pg_conn, extension):
    """Test complete lifecycle: CREATE -> INSERT -> SELECT -> TRUNCATE -> INSERT -> DROP"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_lifecycle/"

    # CREATE
    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_lifecycle(
            id BIGINT,
            name TEXT,
            value NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Verify table creation in catalog
    result = run_query(
        """
        SELECT table_name
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_lifecycle'
        """,
        pg_conn,
    )
    assert len(result) == 1

    # INSERT first batch
    run_command(
        """
        INSERT INTO test_ducklake_lifecycle VALUES
        (1, 'Alice', 100.5),
        (2, 'Bob', 200.75),
        (3, 'Charlie', 300.25)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # SELECT and verify
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_lifecycle",
        pg_conn,
    )
    assert result[0][0] == 3

    result = run_query(
        "SELECT name FROM test_ducklake_lifecycle WHERE id = 2",
        pg_conn,
    )
    assert result[0][0] == "Bob"

    # INSERT second batch
    run_command(
        "INSERT INTO test_ducklake_lifecycle SELECT i, 'user_' || i, i * 10.0 FROM generate_series(4, 10) i",
        pg_conn,
    )
    pg_conn.commit()

    # Verify total count
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_lifecycle",
        pg_conn,
    )
    assert result[0][0] == 10

    # Verify data files exist
    data_files_before_truncate = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_lifecycle' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )[0][0]
    assert data_files_before_truncate >= 2

    # TRUNCATE
    run_command("TRUNCATE test_ducklake_lifecycle", pg_conn)
    pg_conn.commit()

    # Verify table is empty
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_lifecycle",
        pg_conn,
    )
    assert result[0][0] == 0

    # Verify old data files are ended
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_lifecycle' AND df.end_snapshot IS NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 0

    # INSERT after truncate
    run_command(
        """
        INSERT INTO test_ducklake_lifecycle VALUES
        (100, 'NewUser1', 1000.0),
        (200, 'NewUser2', 2000.0)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Verify new data
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_lifecycle",
        pg_conn,
    )
    assert result[0][0] == 2

    result = run_query(
        "SELECT name FROM test_ducklake_lifecycle ORDER BY id",
        pg_conn,
    )
    assert result[0][0] == "NewUser1"
    assert result[1][0] == "NewUser2"

    # Get table ID before drop
    table_id = run_query(
        """
        SELECT table_id
        FROM lake_ducklake.tables
        WHERE table_name = 'test_ducklake_lifecycle'
        """,
        pg_conn,
    )[0][0]

    # DROP
    run_command("DROP FOREIGN TABLE test_ducklake_lifecycle", pg_conn)
    pg_conn.commit()

    # Verify table is marked as ended
    result = run_query(
        f"""
        SELECT end_snapshot
        FROM lake_ducklake.table
        WHERE table_id = {table_id}
        """,
        pg_conn,
    )
    assert result[0][0] is not None

    # Verify table no longer exists
    result = run_query(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_ducklake_lifecycle'",
        pg_conn,
    )
    assert result[0][0] == 0


def test_multiple_transactions(s3, pg_conn, extension):
    """Test multiple transactions with commits and rollbacks"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_multi_tx/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_multi_tx(
            id BIGINT,
            tx_num INT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Transaction 1: commit
    run_command(
        "INSERT INTO test_ducklake_multi_tx VALUES (1, 1), (2, 1)",
        pg_conn,
    )
    pg_conn.commit()

    # Transaction 2: rollback
    run_command(
        "INSERT INTO test_ducklake_multi_tx VALUES (3, 2), (4, 2)",
        pg_conn,
    )
    pg_conn.rollback()

    # Transaction 3: commit
    run_command(
        "INSERT INTO test_ducklake_multi_tx VALUES (5, 3), (6, 3)",
        pg_conn,
    )
    pg_conn.commit()

    # Verify only transactions 1 and 3 are visible
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_multi_tx",
        pg_conn,
    )
    assert result[0][0] == 4

    result = run_query(
        "SELECT DISTINCT tx_num FROM test_ducklake_multi_tx ORDER BY tx_num",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[1][0] == 3

    run_command("DROP FOREIGN TABLE test_ducklake_multi_tx", pg_conn)
    pg_conn.rollback()


def test_snapshot_versioning(s3, pg_conn, extension):
    """Test snapshot-based versioning preserves history"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_snapshots/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_snapshots(
            id BIGINT,
            data TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Get initial snapshot
    snapshot_1 = run_query(
        "SELECT snapshot_id FROM lake_ducklake.snapshot ORDER BY snapshot_id DESC LIMIT 1",
        pg_conn,
    )[0][0]

    # Insert data
    run_command(
        "INSERT INTO test_ducklake_snapshots VALUES (1, 'data1'), (2, 'data2')",
        pg_conn,
    )
    pg_conn.commit()

    # Get snapshot after insert
    snapshot_2 = run_query(
        "SELECT snapshot_id FROM lake_ducklake.snapshot ORDER BY snapshot_id DESC LIMIT 1",
        pg_conn,
    )[0][0]

    # Truncate
    run_command("TRUNCATE test_ducklake_snapshots", pg_conn)
    pg_conn.commit()

    # Get snapshot after truncate
    snapshot_3 = run_query(
        "SELECT snapshot_id FROM lake_ducklake.snapshot ORDER BY snapshot_id DESC LIMIT 1",
        pg_conn,
    )[0][0]

    # Verify snapshots are different
    assert snapshot_1 < snapshot_2 < snapshot_3

    # Verify data files still exist in history
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.data_file df
        JOIN lake_ducklake.table t ON df.table_id = t.table_id
        WHERE t.table_name = 'test_ducklake_snapshots'
        """,
        pg_conn,
    )
    assert result[0][0] > 0

    run_command("DROP FOREIGN TABLE test_ducklake_snapshots", pg_conn)
    pg_conn.rollback()


def test_concurrent_tables(s3, pg_conn, extension):
    """Test operations on multiple tables concurrently"""
    location1 = f"s3://{TEST_BUCKET}/ducklake/test_concurrent1/"
    location2 = f"s3://{TEST_BUCKET}/ducklake/test_concurrent2/"
    location3 = f"s3://{TEST_BUCKET}/ducklake/test_concurrent3/"

    # Create multiple tables
    for i in range(1, 4):
        run_command(
            f"""
            CREATE FOREIGN TABLE test_ducklake_concurrent{i}(
                id BIGINT,
                table_num INT
            )
            SERVER pg_lake_ducklake
            OPTIONS (location '{eval(f"location{i}")}')
            """,
            pg_conn,
        )

    # Insert into each table
    for i in range(1, 4):
        run_command(
            f"INSERT INTO test_ducklake_concurrent{i} SELECT n, {i} FROM generate_series(1, 10) n",
            pg_conn,
        )
    pg_conn.commit()

    # Verify each table
    for i in range(1, 4):
        result = run_query(
            f"SELECT COUNT(*) FROM test_ducklake_concurrent{i}",
            pg_conn,
        )
        assert result[0][0] == 10

    # Truncate one table
    run_command("TRUNCATE test_ducklake_concurrent2", pg_conn)
    pg_conn.commit()

    # Verify truncated table is empty
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_concurrent2",
        pg_conn,
    )
    assert result[0][0] == 0

    # Verify other tables still have data
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_concurrent1",
        pg_conn,
    )
    assert result[0][0] == 10

    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_concurrent3",
        pg_conn,
    )
    assert result[0][0] == 10

    # Drop all tables
    run_command("DROP FOREIGN TABLE test_ducklake_concurrent1, test_ducklake_concurrent2, test_ducklake_concurrent3", pg_conn)
    pg_conn.commit()

    # Verify all tables are marked as ended
    result = run_query(
        """
        SELECT COUNT(*)
        FROM lake_ducklake.table
        WHERE table_name LIKE 'test_ducklake_concurrent%' AND end_snapshot IS NOT NULL
        """,
        pg_conn,
    )
    assert result[0][0] == 3

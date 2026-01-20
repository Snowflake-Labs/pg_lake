"""
Tests for ALTER TABLE operations on DuckLake tables.
"""
import pytest
import os


def test_drop_column_creates_snapshot(pg_cursor):
    """
    Test that DROP COLUMN creates a new snapshot and sets end_snapshot correctly.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_drop_col (
            id INT,
            name TEXT,
            email TEXT,
            phone TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_drop_col')
    """)

    # Get initial snapshot count
    pg_cursor.execute("SELECT COUNT(*), MAX(snapshot_id) FROM lake_ducklake.snapshot")
    initial_count, initial_max_snapshot = pg_cursor.fetchone()

    # Get table ID
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_drop_col'
    """)
    table_id = pg_cursor.fetchone()[0]

    # Drop a column
    pg_cursor.execute("ALTER TABLE test_drop_col DROP COLUMN email")

    # Check that a new snapshot was created
    pg_cursor.execute("SELECT COUNT(*), MAX(snapshot_id) FROM lake_ducklake.snapshot")
    new_count, new_max_snapshot = pg_cursor.fetchone()
    assert new_count == initial_count + 1, "New snapshot should be created"
    assert new_max_snapshot == initial_max_snapshot + 1, "Snapshot ID should increment"

    # Check that schema_version was incremented
    pg_cursor.execute("""
        SELECT schema_version FROM lake_ducklake.snapshot
        WHERE snapshot_id = %s
    """, (new_max_snapshot,))
    schema_version = pg_cursor.fetchone()[0]
    assert schema_version > 0, "Schema version should be incremented"

    # Check that the dropped column has end_snapshot set to the new snapshot
    pg_cursor.execute("""
        SELECT column_name, begin_snapshot, end_snapshot
        FROM lake_ducklake.column
        WHERE table_id = %s AND column_name = 'email'
    """, (table_id,))
    result = pg_cursor.fetchone()
    assert result is not None, "Column should still exist in metadata"
    column_name, begin_snapshot, end_snapshot = result
    assert end_snapshot == new_max_snapshot, \
        f"end_snapshot should be {new_max_snapshot}, got {end_snapshot}"

    # Check that other columns are unaffected
    pg_cursor.execute("""
        SELECT column_name, end_snapshot
        FROM lake_ducklake.column
        WHERE table_id = %s AND column_name IN ('id', 'name', 'phone')
        ORDER BY column_name
    """, (table_id,))
    other_columns = pg_cursor.fetchall()
    assert len(other_columns) == 3
    for col_name, end_snap in other_columns:
        assert end_snap is None, f"Column {col_name} should still be active"

    # Cleanup
    pg_cursor.execute("DROP TABLE test_drop_col")


def test_add_column_creates_snapshot(pg_cursor):
    """
    Test that ADD COLUMN creates a new snapshot and sets begin_snapshot correctly.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_add_col (
            id INT,
            name TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_add_col')
    """)

    # Get initial snapshot count
    pg_cursor.execute("SELECT COUNT(*), MAX(snapshot_id) FROM lake_ducklake.snapshot")
    initial_count, initial_max_snapshot = pg_cursor.fetchone()

    # Get table ID
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_add_col'
    """)
    table_id = pg_cursor.fetchone()[0]

    # Add a column
    pg_cursor.execute("ALTER TABLE test_add_col ADD COLUMN email TEXT")

    # Check that a new snapshot was created
    pg_cursor.execute("SELECT COUNT(*), MAX(snapshot_id) FROM lake_ducklake.snapshot")
    new_count, new_max_snapshot = pg_cursor.fetchone()
    assert new_count == initial_count + 1, "New snapshot should be created"
    assert new_max_snapshot == initial_max_snapshot + 1, "Snapshot ID should increment"

    # Check that schema_version was incremented
    pg_cursor.execute("""
        SELECT schema_version FROM lake_ducklake.snapshot
        WHERE snapshot_id = %s
    """, (new_max_snapshot,))
    schema_version = pg_cursor.fetchone()[0]
    assert schema_version > 0, "Schema version should be incremented"

    # Check that the new column has begin_snapshot set to the new snapshot
    pg_cursor.execute("""
        SELECT column_name, begin_snapshot, end_snapshot
        FROM lake_ducklake.column
        WHERE table_id = %s AND column_name = 'email'
    """, (table_id,))
    result = pg_cursor.fetchone()
    assert result is not None, "New column should exist in metadata"
    column_name, begin_snapshot, end_snapshot = result
    assert begin_snapshot == new_max_snapshot, \
        f"begin_snapshot should be {new_max_snapshot}, got {begin_snapshot}"
    assert end_snapshot is None, "New column should not have end_snapshot"

    # Cleanup
    pg_cursor.execute("DROP TABLE test_add_col")


def test_multiple_alter_operations(pg_cursor):
    """
    Test multiple ALTER TABLE operations in sequence.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_multi_alter (
            id INT,
            col1 TEXT,
            col2 TEXT,
            col3 TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_multi_alter')
    """)

    # Get initial snapshot
    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    snapshot0 = pg_cursor.fetchone()[0]

    # Get table ID
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_multi_alter'
    """)
    table_id = pg_cursor.fetchone()[0]

    # Operation 1: Drop col1
    pg_cursor.execute("ALTER TABLE test_multi_alter DROP COLUMN col1")
    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    snapshot1 = pg_cursor.fetchone()[0]
    assert snapshot1 == snapshot0 + 1, "First operation should create snapshot"

    # Operation 2: Add col4
    pg_cursor.execute("ALTER TABLE test_multi_alter ADD COLUMN col4 INT")
    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    snapshot2 = pg_cursor.fetchone()[0]
    assert snapshot2 == snapshot1 + 1, "Second operation should create snapshot"

    # Operation 3: Drop col2
    pg_cursor.execute("ALTER TABLE test_multi_alter DROP COLUMN col2")
    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    snapshot3 = pg_cursor.fetchone()[0]
    assert snapshot3 == snapshot2 + 1, "Third operation should create snapshot"

    # Verify final column state
    pg_cursor.execute("""
        SELECT column_name, begin_snapshot, end_snapshot
        FROM lake_ducklake.column
        WHERE table_id = %s
        ORDER BY column_name
    """, (table_id,))
    columns = pg_cursor.fetchall()

    columns_dict = {name: (begin, end) for name, begin, end in columns}

    # col1 should be dropped at snapshot1
    assert columns_dict['col1'][1] == snapshot1, "col1 should be ended at snapshot1"

    # col2 should be dropped at snapshot3
    assert columns_dict['col2'][1] == snapshot3, "col2 should be ended at snapshot3"

    # col3 should still be active
    assert columns_dict['col3'][1] is None, "col3 should still be active"

    # col4 should be added at snapshot2
    assert columns_dict['col4'][0] == snapshot2, "col4 should begin at snapshot2"
    assert columns_dict['col4'][1] is None, "col4 should still be active"

    # id should still be active
    assert columns_dict['id'][1] is None, "id should still be active"

    # Cleanup
    pg_cursor.execute("DROP TABLE test_multi_alter")


def test_drop_column_with_not_null(pg_cursor):
    """
    Test dropping a NOT NULL column.
    """
    pg_cursor.execute("""
        CREATE TABLE test_drop_not_null (
            id INT NOT NULL,
            name TEXT NOT NULL,
            email TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_drop_not_null')
    """)

    # Get table ID
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_drop_not_null'
    """)
    table_id = pg_cursor.fetchone()[0]

    # Drop the NOT NULL column
    pg_cursor.execute("ALTER TABLE test_drop_not_null DROP COLUMN name")

    # Verify it was marked as dropped
    pg_cursor.execute("""
        SELECT end_snapshot FROM lake_ducklake.column
        WHERE table_id = %s AND column_name = 'name'
    """, (table_id,))
    end_snapshot = pg_cursor.fetchone()[0]
    assert end_snapshot is not None, "NOT NULL column should be marked as dropped"

    # Cleanup
    pg_cursor.execute("DROP TABLE test_drop_not_null")


def test_add_column_with_default(pg_cursor):
    """
    Test adding a column with a default value.
    """
    pg_cursor.execute("""
        CREATE TABLE test_add_default (
            id INT,
            name TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_add_default')
    """)

    # Get table ID
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_add_default'
    """)
    table_id = pg_cursor.fetchone()[0]

    # Add column with default
    pg_cursor.execute("""
        ALTER TABLE test_add_default
        ADD COLUMN status TEXT DEFAULT 'active'
    """)

    # Verify the column was added
    pg_cursor.execute("""
        SELECT begin_snapshot, end_snapshot
        FROM lake_ducklake.column
        WHERE table_id = %s AND column_name = 'status'
    """, (table_id,))
    result = pg_cursor.fetchone()
    assert result is not None, "Column with default should be added"
    begin_snapshot, end_snapshot = result
    assert begin_snapshot is not None, "Column should have begin_snapshot"
    assert end_snapshot is None, "Column should be active"

    # Cleanup
    pg_cursor.execute("DROP TABLE test_add_default")


def test_schema_version_increments(pg_cursor):
    """
    Test that schema_version increments with each ALTER operation.
    """
    pg_cursor.execute("""
        CREATE TABLE test_schema_version (
            id INT,
            col1 TEXT,
            col2 TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_schema_version')
    """)

    # Get initial schema version
    pg_cursor.execute("""
        SELECT MAX(schema_version) FROM lake_ducklake.snapshot
    """)
    initial_version = pg_cursor.fetchone()[0]

    # First ALTER
    pg_cursor.execute("ALTER TABLE test_schema_version DROP COLUMN col1")
    pg_cursor.execute("SELECT MAX(schema_version) FROM lake_ducklake.snapshot")
    version1 = pg_cursor.fetchone()[0]
    assert version1 == initial_version + 1, "Schema version should increment"

    # Second ALTER
    pg_cursor.execute("ALTER TABLE test_schema_version ADD COLUMN col3 INT")
    pg_cursor.execute("SELECT MAX(schema_version) FROM lake_ducklake.snapshot")
    version2 = pg_cursor.fetchone()[0]
    assert version2 == version1 + 1, "Schema version should increment again"

    # Third ALTER
    pg_cursor.execute("ALTER TABLE test_schema_version DROP COLUMN col2")
    pg_cursor.execute("SELECT MAX(schema_version) FROM lake_ducklake.snapshot")
    version3 = pg_cursor.fetchone()[0]
    assert version3 == version2 + 1, "Schema version should increment again"

    # Cleanup
    pg_cursor.execute("DROP TABLE test_schema_version")


# Check if DuckDB is available for end-to-end tests
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_drop_column_visible_in_duckdb(pg_cursor):
    """
    End-to-end test: Verify that DROP COLUMN is visible in DuckDB metadata.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_e2e_drop (
            id INT,
            name TEXT,
            email TEXT,
            phone TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_e2e_drop')
    """)
    pg_cursor.connection.commit()

    # Get table ID and initial snapshot
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_e2e_drop'
    """)
    table_id = pg_cursor.fetchone()[0]

    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    snapshot_before = pg_cursor.fetchone()[0]

    # Drop column
    pg_cursor.execute("ALTER TABLE test_e2e_drop DROP COLUMN email")
    pg_cursor.connection.commit()

    # Connect with DuckDB
    duckdb_conn = duckdb.connect()

    # Load ducklake extension
    try:
        duckdb_conn.execute("INSTALL ducklake FROM core_nightly")
        duckdb_conn.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"DuckDB ducklake extension not available: {e}")

    # Attach to PostgreSQL ducklake catalog using TYPE DUCKLAKE
    pg_conn_str = f"host=localhost port=5432 dbname=postgres user={os.environ.get('PGUSER', 'postgres')}"
    try:
        duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS dl (TYPE DUCKLAKE)")
    except Exception as e:
        pytest.skip(f"Could not attach to PostgreSQL with TYPE DUCKLAKE: {e}")

    # Query the actual table from DuckDB to see what columns are visible
    try:
        result = duckdb_conn.execute("""
            DESCRIBE dl.test_e2e_drop
        """).fetchall()

        # Extract column names from DESCRIBE output
        visible_columns = [row[0] for row in result]

        # Verify dropped column is NOT visible
        assert 'email' not in visible_columns, "Dropped column should not be visible in DuckDB"

        # Verify other columns are visible
        assert 'id' in visible_columns, "id column should be visible"
        assert 'name' in visible_columns, "name column should be visible"
        assert 'phone' in visible_columns, "phone column should be visible"
    except Exception as e:
        # If DESCRIBE doesn't work, try selecting from the table
        result = duckdb_conn.execute("""
            SELECT * FROM dl.test_e2e_drop LIMIT 0
        """).description

        visible_columns = [col[0] for col in result]
        assert 'email' not in visible_columns, "Dropped column should not be visible"
        assert 'id' in visible_columns and 'name' in visible_columns and 'phone' in visible_columns

    duckdb_conn.close()

    # Cleanup
    pg_cursor.execute("DROP TABLE test_e2e_drop")
    pg_cursor.connection.commit()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_add_column_visible_in_duckdb(pg_cursor):
    """
    End-to-end test: Verify that ADD COLUMN is visible in DuckDB metadata.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_e2e_add (
            id INT,
            name TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_e2e_add')
    """)
    pg_cursor.connection.commit()

    # Get table ID and initial snapshot
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_e2e_add'
    """)
    table_id = pg_cursor.fetchone()[0]

    pg_cursor.execute("SELECT MAX(snapshot_id) FROM lake_ducklake.snapshot")
    snapshot_before = pg_cursor.fetchone()[0]

    # Add column
    pg_cursor.execute("ALTER TABLE test_e2e_add ADD COLUMN email TEXT")
    pg_cursor.connection.commit()

    # Connect with DuckDB
    duckdb_conn = duckdb.connect()

    # Load ducklake extension
    try:
        duckdb_conn.execute("INSTALL ducklake FROM core_nightly")
        duckdb_conn.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"DuckDB ducklake extension not available: {e}")

    # Attach to PostgreSQL ducklake catalog using TYPE DUCKLAKE
    pg_conn_str = f"host=localhost port=5432 dbname=postgres user={os.environ.get('PGUSER', 'postgres')}"
    try:
        duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS dl (TYPE DUCKLAKE)")
    except Exception as e:
        pytest.skip(f"Could not attach to PostgreSQL with TYPE DUCKLAKE: {e}")

    # Query the actual table from DuckDB to see what columns are visible
    try:
        result = duckdb_conn.execute("""
            DESCRIBE dl.test_e2e_add
        """).fetchall()

        # Extract column names from DESCRIBE output
        visible_columns = [row[0] for row in result]

        # Verify new column is visible
        assert 'email' in visible_columns, "Added column should be visible in DuckDB"

        # Verify original columns are still visible
        assert 'id' in visible_columns, "id column should be visible"
        assert 'name' in visible_columns, "name column should be visible"
    except Exception as e:
        # If DESCRIBE doesn't work, try selecting from the table
        result = duckdb_conn.execute("""
            SELECT * FROM dl.test_e2e_add LIMIT 0
        """).description

        visible_columns = [col[0] for col in result]
        assert 'email' in visible_columns, "Added column should be visible"
        assert 'id' in visible_columns and 'name' in visible_columns

    duckdb_conn.close()

    # Cleanup
    pg_cursor.execute("DROP TABLE test_e2e_add")
    pg_cursor.connection.commit()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_schema_evolution_in_duckdb(pg_cursor):
    """
    End-to-end test: Verify complete schema evolution is visible in DuckDB.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_e2e_evolution (
            id INT,
            col1 TEXT,
            col2 TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_e2e_evolution')
    """)
    pg_cursor.connection.commit()

    # Get table ID
    pg_cursor.execute("""
        SELECT table_id FROM lake_ducklake.table
        WHERE table_name = 'test_e2e_evolution'
    """)
    table_id = pg_cursor.fetchone()[0]

    # Perform multiple operations
    pg_cursor.execute("ALTER TABLE test_e2e_evolution DROP COLUMN col1")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE test_e2e_evolution ADD COLUMN col3 INT")
    pg_cursor.connection.commit()

    pg_cursor.execute("ALTER TABLE test_e2e_evolution DROP COLUMN col2")
    pg_cursor.connection.commit()

    # Connect with DuckDB
    duckdb_conn = duckdb.connect()

    # Load ducklake extension
    try:
        duckdb_conn.execute("INSTALL ducklake FROM core_nightly")
        duckdb_conn.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"DuckDB ducklake extension not available: {e}")

    # Attach to PostgreSQL ducklake catalog using TYPE DUCKLAKE
    pg_conn_str = f"host=localhost port=5432 dbname=postgres user={os.environ.get('PGUSER', 'postgres')}"
    try:
        duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS dl (TYPE DUCKLAKE)")
    except Exception as e:
        pytest.skip(f"Could not attach to PostgreSQL with TYPE DUCKLAKE: {e}")

    # Query the actual table from DuckDB to see the final schema
    try:
        result = duckdb_conn.execute("""
            DESCRIBE dl.test_e2e_evolution
        """).fetchall()

        # Extract column names from DESCRIBE output
        visible_columns = [row[0] for row in result]

        # Verify final schema state
        assert 'id' in visible_columns, "id should be visible"
        assert 'col1' not in visible_columns, "col1 should not be visible (dropped)"
        assert 'col2' not in visible_columns, "col2 should not be visible (dropped)"
        assert 'col3' in visible_columns, "col3 should be visible (added)"

        # Verify we have exactly 2 columns (id and col3)
        assert len(visible_columns) == 2, f"Should have 2 columns, got {len(visible_columns)}: {visible_columns}"
    except Exception as e:
        # If DESCRIBE doesn't work, try selecting from the table
        result = duckdb_conn.execute("""
            SELECT * FROM dl.test_e2e_evolution LIMIT 0
        """).description

        visible_columns = [col[0] for col in result]
        assert 'id' in visible_columns and 'col3' in visible_columns
        assert 'col1' not in visible_columns and 'col2' not in visible_columns

    duckdb_conn.close()

    # Cleanup
    pg_cursor.execute("DROP TABLE test_e2e_evolution")
    pg_cursor.connection.commit()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_views_reflect_alter_operations(pg_cursor):
    """
    End-to-end test: Verify DuckDB views show correct columns after ALTER operations.
    """
    # Create table
    pg_cursor.execute("""
        CREATE TABLE test_e2e_views (
            id INT,
            name TEXT,
            email TEXT
        ) USING ducklake WITH (location = 's3://test-bucket/test_e2e_views')
    """)
    pg_cursor.connection.commit()

    # Connect with DuckDB
    duckdb_conn = duckdb.connect()

    # Load ducklake extension
    try:
        duckdb_conn.execute("INSTALL ducklake FROM core_nightly")
        duckdb_conn.execute("LOAD ducklake")
    except Exception as e:
        pytest.skip(f"DuckDB ducklake extension not available: {e}")

    # Attach to PostgreSQL ducklake catalog using TYPE DUCKLAKE
    pg_conn_str = f"host=localhost port=5432 dbname=postgres user={os.environ.get('PGUSER', 'postgres')}"
    try:
        duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS dl (TYPE DUCKLAKE)")
    except Exception as e:
        pytest.skip(f"Could not attach to PostgreSQL with TYPE DUCKLAKE: {e}")

    # Check initial columns from DuckDB
    try:
        result = duckdb_conn.execute("""
            DESCRIBE dl.test_e2e_views
        """).fetchall()

        initial_columns = [row[0] for row in result]
        assert 'id' in initial_columns, "id should be visible initially"
        assert 'name' in initial_columns, "name should be visible initially"
        assert 'email' in initial_columns, "email should be visible initially"
    except Exception as e:
        pytest.skip(f"Could not describe table: {e}")

    # Drop column in PostgreSQL
    pg_cursor.execute("ALTER TABLE test_e2e_views DROP COLUMN email")
    pg_cursor.connection.commit()

    # Re-query from DuckDB to see updated schema
    # Note: DuckDB may need to reconnect to see schema changes
    duckdb_conn.close()
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute("LOAD ducklake")
    duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS dl (TYPE DUCKLAKE)")

    result = duckdb_conn.execute("""
        DESCRIBE dl.test_e2e_views
    """).fetchall()

    final_columns = [row[0] for row in result]
    assert 'id' in final_columns, "id should still be visible"
    assert 'name' in final_columns, "name should still be visible"
    assert 'email' not in final_columns, "email should not be visible after drop"

    duckdb_conn.close()

    # Cleanup
    pg_cursor.execute("DROP TABLE test_e2e_views")
    pg_cursor.connection.commit()

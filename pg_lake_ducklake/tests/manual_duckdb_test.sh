#!/bin/bash
#
# Manual test script to verify DuckLake metadata compatibility
# between PostgreSQL and DuckDB
#
# Prerequisites:
# - PostgreSQL with pg_lake_ducklake extension installed
# - DuckDB CLI installed
# - DuckLake extension for DuckDB (install with: INSTALL ducklake FROM community)

set -e

LOCATION="/tmp/ducklake_test_$(date +%s)"
PSQL_CMD="psql -U postgres"

echo "========================================="
echo "DuckLake Interoperability Test"
echo "========================================="
echo ""
echo "Test location: $LOCATION"
echo ""

# Clean up on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    $PSQL_CMD -c "DROP TABLE IF EXISTS ducklake_test CASCADE" 2>/dev/null || true
    rm -rf "$LOCATION" 2>/dev/null || true
}
trap cleanup EXIT

echo "Step 1: Create DuckLake table in PostgreSQL"
echo "--------------------------------------------"
$PSQL_CMD <<EOF
-- Create extension if not already loaded
CREATE EXTENSION IF NOT EXISTS pg_lake_ducklake CASCADE;

-- Create a DuckLake table
CREATE TABLE ducklake_test (
    id INTEGER,
    name TEXT,
    value DOUBLE PRECISION,
    created_at TIMESTAMP
) USING ducklake
WITH (location = 'file://$LOCATION');

-- Insert test data
INSERT INTO ducklake_test VALUES
    (1, 'alice', 10.5, '2024-01-15 10:00:00'),
    (2, 'bob', 20.3, '2024-01-15 11:00:00'),
    (3, 'charlie', 30.7, '2024-01-15 12:00:00'),
    (4, 'diana', 40.1, '2024-01-15 13:00:00');

SELECT 'Inserted ' || COUNT(*) || ' rows' as status FROM ducklake_test;
EOF

echo ""
echo "Step 2: Verify PostgreSQL metadata"
echo "------------------------------------"
$PSQL_CMD <<EOF
-- Check snapshot metadata
SELECT
    snapshot_id,
    changes_made,
    author,
    to_timestamp(timestamp_ms / 1000) as snapshot_time
FROM ducklake.snapshot
ORDER BY snapshot_id DESC
LIMIT 3;

-- Check table metadata
SELECT
    t.table_id,
    t.table_name,
    t.schema_name,
    t.location
FROM ducklake.table t
WHERE t.table_name = 'ducklake_test';

-- Check column metadata
SELECT
    c.column_name,
    c.data_type,
    c.ordinal_position
FROM ducklake.column c
JOIN ducklake.table t ON c.table_id = t.table_id
WHERE t.table_name = 'ducklake_test'
ORDER BY c.ordinal_position;

-- Check data files
SELECT
    df.path,
    df.record_count,
    df.file_size_bytes
FROM ducklake.data_file df
JOIN ducklake.table t ON df.table_id = t.table_id
WHERE t.table_name = 'ducklake_test';
EOF

echo ""
echo "Step 3: Query with DuckDB"
echo "-------------------------"

# Create DuckDB test script
cat > /tmp/duckdb_test.sql <<DUCKEOF
-- Install and load ducklake extension
INSTALL ducklake FROM community;
LOAD ducklake;

-- Attach the DuckLake database
ATTACH 'file://$LOCATION' AS ducklake_db (TYPE DUCKLAKE);

-- Show tables
SHOW TABLES FROM ducklake_db;

-- Query the data
SELECT 'Querying data through DuckDB:' as status;
SELECT * FROM ducklake_db.ducklake_test ORDER BY id;

-- Check snapshot information
SELECT 'Snapshot information from DuckDB:' as status;
SELECT
    snapshot_id,
    changes_made,
    author,
    CAST(timestamp_ms / 1000 AS INTEGER) as snapshot_time_seconds
FROM ducklake_db.ducklake_snapshot()
ORDER BY snapshot_id;

-- Verify row counts match
SELECT
    'Row count: ' || COUNT(*) as status
FROM ducklake_db.ducklake_test;
DUCKEOF

# Run DuckDB
echo "Running DuckDB queries..."
duckdb < /tmp/duckdb_test.sql

echo ""
echo "Step 4: Verify data consistency"
echo "--------------------------------"
$PSQL_CMD <<EOF
-- Query through PostgreSQL again
SELECT
    'PostgreSQL sees ' || COUNT(*) || ' rows' as status
FROM ducklake_test;

SELECT * FROM ducklake_test ORDER BY id;
EOF

echo ""
echo "Step 5: Test UPDATE operation"
echo "------------------------------"
$PSQL_CMD <<EOF
-- Update a row (should create new snapshot)
UPDATE ducklake_test SET value = 99.9 WHERE id = 2;

SELECT 'Updated row 2' as status;

-- Verify the update
SELECT * FROM ducklake_test WHERE id = 2;

-- Check new snapshot
SELECT
    snapshot_id,
    changes_made,
    to_timestamp(timestamp_ms / 1000) as snapshot_time
FROM ducklake.snapshot
ORDER BY snapshot_id DESC
LIMIT 1;
EOF

echo ""
echo "Step 6: Verify UPDATE with DuckDB"
echo "----------------------------------"

cat > /tmp/duckdb_test2.sql <<DUCKEOF
LOAD ducklake;
ATTACH 'file://$LOCATION' AS ducklake_db (TYPE DUCKLAKE);

SELECT 'After UPDATE - DuckDB sees:' as status;
SELECT * FROM ducklake_db.ducklake_test WHERE id = 2;

SELECT 'DuckDB snapshot count:' as status;
SELECT COUNT(*) as snapshot_count
FROM ducklake_db.ducklake_snapshot();
DUCKEOF

duckdb < /tmp/duckdb_test2.sql

echo ""
echo "========================================="
echo "Test completed successfully!"
echo "========================================="
echo ""
echo "Summary:"
echo "  - Created DuckLake table in PostgreSQL"
echo "  - Inserted 4 rows"
echo "  - Verified metadata structure"
echo "  - Queried through DuckDB"
echo "  - Performed UPDATE operation"
echo "  - Verified consistency between systems"
echo ""
echo "Location: $LOCATION"
echo "(will be cleaned up on exit)"

# Clean up temp files
rm -f /tmp/duckdb_test.sql /tmp/duckdb_test2.sql

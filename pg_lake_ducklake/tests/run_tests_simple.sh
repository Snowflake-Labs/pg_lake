#!/bin/bash
#
# Simple test runner for pg_lake_ducklake extension
#

set -e

cd "$(dirname "$0")/.."

# Set PostgreSQL user if not already set
if [ -z "$PGUSER" ]; then
    PGUSER=$(whoami)
    echo "Using PostgreSQL user: $PGUSER"
fi

export PGUSER
export PYTHONPATH=../test_common

echo "================================"
echo "pg_lake_ducklake Test Suite"
echo "================================"
echo ""

# Check if extension is installed
if ! psql -c "SELECT 1 FROM pg_extension WHERE extname = 'pg_lake_ducklake'" -t -A | grep -q 1; then
    echo "Warning: pg_lake_ducklake extension not found in database"
    echo "Run: psql -c 'CREATE EXTENSION pg_lake_ducklake CASCADE'"
    echo ""
fi

echo "Running tests..."
echo ""

# Run tests
pipenv run pytest tests/pytests/test_basic.py tests/pytests/test_metadata_spec.py -v --tb=short

echo ""
echo "================================"
echo "Test Summary"
echo "================================"
pipenv run pytest tests/pytests/test_basic.py tests/pytests/test_metadata_spec.py --tb=no -q

echo ""
echo "For detailed results, see: tests/TEST_RESULTS.md"

#!/bin/bash
#
# Test runner for pg_lake_ducklake extension
#
# Usage:
#   ./run_tests.sh                    # Run all tests
#   ./run_tests.sh basic              # Run basic tests only
#   ./run_tests.sh spec               # Run metadata spec tests only
#   ./run_tests.sh interop            # Run interoperability tests only
#   ./run_tests.sh manual             # Run manual DuckDB test

set -e

cd "$(dirname "$0")"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}pg_lake_ducklake Test Runner${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""

# Check if PostgreSQL is running
if ! pg_isready -q; then
    echo -e "${RED}ERROR: PostgreSQL is not running${NC}"
    exit 1
fi

# Check if extension is installed
if ! psql -c "SELECT 1 FROM pg_extension WHERE extname = 'pg_lake_ducklake'" -qt 2>/dev/null | grep -q 1; then
    echo -e "${YELLOW}WARNING: pg_lake_ducklake extension not found. Installing...${NC}"
    psql -c "CREATE EXTENSION IF NOT EXISTS pg_lake_ducklake CASCADE" || {
        echo -e "${RED}ERROR: Failed to install extension${NC}"
        echo -e "${YELLOW}Run 'make install' from the project root${NC}"
        exit 1
    }
fi

# Set Python path
export PYTHONPATH="../test_common"

case "${1:-all}" in
    basic)
        echo -e "${GREEN}Running basic functionality tests...${NC}"
        cd ..
        pipenv run pytest tests/pytests/test_basic.py -v
        ;;

    spec)
        echo -e "${GREEN}Running metadata spec compliance tests...${NC}"
        cd ..
        pipenv run pytest tests/pytests/test_metadata_spec.py -v
        ;;

    interop)
        echo -e "${GREEN}Running DuckDB interoperability tests...${NC}"
        echo -e "${YELLOW}Note: This requires DuckDB with ducklake extension${NC}"
        cd ..
        pipenv run pytest tests/pytests/test_ducklake_interop.py -v
        ;;

    manual)
        echo -e "${GREEN}Running manual DuckDB test...${NC}"
        echo -e "${YELLOW}Note: This requires DuckDB CLI installed${NC}"
        ./manual_duckdb_test.sh
        ;;

    all)
        echo -e "${GREEN}Running all Python tests...${NC}"
        cd ..
        pipenv run pytest tests/pytests/ -v

        echo ""
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}To run the manual DuckDB test:${NC}"
        echo -e "${YELLOW}  ./tests/manual_duckdb_test.sh${NC}"
        echo -e "${GREEN}=========================================${NC}"
        ;;

    *)
        echo "Usage: $0 {all|basic|spec|interop|manual}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}Tests completed!${NC}"
echo -e "${GREEN}=========================================${NC}"

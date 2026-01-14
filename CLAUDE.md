# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_lake integrates Apache Iceberg and data lake files (Parquet, CSV, JSON) into PostgreSQL, enabling PostgreSQL to function as a lakehouse system. The architecture consists of two main components:
- **PostgreSQL with pg_lake extensions**: Handles query planning, transaction boundaries, and orchestration
- **pgduck_server**: A separate multi-threaded process that implements the PostgreSQL wire protocol and delegates computation to DuckDB's columnar execution engine

Users connect only to PostgreSQL. The pg_lake extensions transparently delegate data scanning and computation to pgduck_server (running DuckDB) when appropriate, while maintaining full transactional guarantees.

## Build Commands

### First-time build
```bash
# Install vcpkg dependencies (required once)
export VCPKG_VERSION=2025.12.12
git clone --recurse-submodules https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl
export VCPKG_TOOLCHAIN_PATH="$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake"

# Build and install all extensions and pgduck_server
make install
```

### Subsequent builds
```bash
# Fast build that skips rebuilding DuckDB if already built
make install-fast
```

### Component-specific builds
```bash
# Build/install individual extensions
make install-pg_lake_iceberg
make install-pg_lake_table
make install-pgduck_server

# Build all extensions (top-level meta extension)
make install-pg_lake
```

## Running pg_lake

### Required setup
```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_extension_base'

-- Connect to PostgreSQL and create extensions:
CREATE EXTENSION pg_lake CASCADE;
-- This installs: pg_extension_base, pg_map, pg_extension_updater,
-- pg_lake_engine, pg_lake_iceberg, pg_lake_table, pg_lake_copy, pg_lake

-- Set default location for Iceberg tables:
SET pg_lake_iceberg.default_location_prefix TO 's3://your-bucket/pglake';
```

### Starting pgduck_server
```bash
# Start pgduck_server (must be running for pg_lake to work)
pgduck_server

# With options:
pgduck_server --memory_limit '8GB' --cache_dir /tmp/cache --init_file_path /path/to/init.sql

# pgduck_server listens on port 5332 (unix socket /tmp by default)
# You can connect directly to pgduck_server for debugging:
psql -p 5332 -h /tmp
```

## Testing

The project uses **pytest** for all regression testing (not traditional PostgreSQL SQL regression tests).

### Running all tests
```bash
# Install test dependencies (first time only)
pipenv install --dev

# Run all local tests
make check

# Run end-to-end tests (requires S3/cloud access)
make check-e2e

# Run upgrade tests
make check-upgrade

# Run all tests (local + e2e)
make check  # includes check-local and check-e2e
```

### Running tests for specific components
```bash
# Test specific extension
make check-pg_lake_table
make check-pg_lake_iceberg
make check-pgduck_server

# Run isolation tests
make check-isolation_pg_lake_table
```

### Running installcheck
```bash
# Start pgduck_server with test configuration
pgduck_server --init_file_path pgduck_server/tests/test_secrets.sql --cache_dir /tmp/cache &

# Run installcheck (tests against installed extensions)
make installcheck

# Run installcheck for specific component
make installcheck-pg_lake_table
```

### Running individual pytest tests
```bash
cd pg_lake_table
PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_specific.py
PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_specific.py::test_function_name
```

### Testing with PostgreSQL regression suite
```bash
# Run PostgreSQL's own tests with pg_lake extensions loaded
export PG_REGRESS_DIR=/path/to/postgres/src/test/regress

# Run tests
make installcheck-postgres PG_REGRESS_DIR=$PG_REGRESS_DIR
make installcheck-postgres-with_extensions_created PG_REGRESS_DIR=$PG_REGRESS_DIR
```

## Extension Architecture

pg_lake follows a **modular design** with interoperating components. Each extension has a specific responsibility:

### Extension dependency chain
```
pg_lake (meta-extension)
├── pg_lake_table (FDW for querying data lake files)
│   └── pg_lake_iceberg (Iceberg specification implementation)
│       └── pg_lake_engine (common module for pg_lake extensions)
│           ├── pg_extension_base (foundation for all extensions)
│           ├── pg_map (generic map type)
│           └── pg_extension_updater (automatic extension updates)
└── pg_lake_copy (COPY to/from data lake)
    └── pg_lake_engine

pg_lake_spatial (optional, depends on PostGIS)
pg_lake_benchmark (optional, for benchmarking)
```

### Core extensions
- **pg_extension_base**: Foundation for all extensions, provides common utilities
- **pg_extension_updater**: Automatically updates extensions on startup
- **pg_map**: Generic map/key-value type generator for semi-structured data
- **pg_lake_engine**: Common module shared by data lake extensions (depends on Apache Avro)
- **pg_lake_iceberg**: Full Iceberg v2 protocol implementation with transactional support
- **pg_lake_table**: Foreign data wrapper to query Parquet/CSV/JSON/Iceberg files
- **pg_lake_copy**: COPY command extensions for importing/exporting to data lakes
- **pg_lake**: Meta-extension that installs all required extensions via CASCADE

### External components
- **pgduck_server**: Standalone server implementing PostgreSQL wire protocol, executes queries via DuckDB
- **duckdb_pglake**: DuckDB extension adding PostgreSQL-compatible functions to DuckDB
- **avro**: Apache Avro library (patched) for Iceberg metadata handling

## Important File Locations

### Build system
- `Makefile`: Top-level build orchestration for all components
- `shared.mk`: Shared Makefile rules for extensions
- Each extension has its own `Makefile` following PGXS conventions

### Tests
- `<extension>/tests/pytests/`: Main pytest test suites
- `<extension>/tests/e2e/`: End-to-end tests requiring cloud storage
- `<extension>/tests/isolation/`: Isolation tester tests for concurrency
- `test_common/`: Shared test utilities and fixtures
- `pytest.ini`: Root pytest configuration

### Documentation
- `docs/building-from-source.md`: Detailed build instructions
- `docs/iceberg-tables.md`: Iceberg table usage
- `docs/query-data-lake-files.md`: Foreign table usage
- `docs/data-lake-import-export.md`: COPY command usage

## Code Conventions

### C code (PostgreSQL extensions and pgduck_server)
- **Indentation**: Use `pgindent` before commits (see `make reindent`)
- **Naming**:
  - Variables/functions: `lower_case_with_underscores`
  - Macros/constants: `UPPER_CASE_WITH_UNDERSCORES`
  - Structs/enums: `CamelCase`
  - Global variables: Prefix with module identifier (e.g., `IcebergTableCache`)
- **Comments**: Focus on "why" not "what"; use block comments for complex algorithms
- **Typedefs**: Download from buildfarm via `make typedefs` for pgindent

### Python code
- **Formatting**: Use `black` (see `make reindent` or `pipenv run black`)
- **Style**: Follow pytest conventions for test naming and fixtures

### Before committing
```bash
# Format all code
make reindent

# Check formatting
make check-indent
```

## Local Development with MinIO

For testing without cloud S3 latency, use MinIO locally:

```bash
# Install and start MinIO
brew install minio  # or download from min.io
minio server /tmp/data

# Access UI at http://localhost:9000
# Create access key: testkey / testpassword
# Create bucket: localbucket

# Add to ~/.aws/config:
[services testing-minio]
s3 =
   endpoint_url = http://localhost:9000

[profile minio]
region = us-east-1
services = testing-minio
aws_access_key_id = testkey
aws_secret_access_key = testpassword

# Configure pgduck_server (connect to port 5332)
psql -p 5332 -h /tmp -c "
CREATE SECRET s3testMinio (
    TYPE S3,
    KEY_ID 'testkey',
    SECRET 'testpassword',
    ENDPOINT 'localhost:9000',
    SCOPE 's3://localbucket',
    URL_STYLE 'path',
    USE_SSL false
);"

# Use in PostgreSQL
psql -c "SET pg_lake_iceberg.default_location_prefix TO 's3://localbucket'"
```

## Common Development Workflows

### Adding new functionality to an extension
1. Read existing code in `<extension>/src/` to understand patterns
2. Modify C source files in `src/`
3. If adding SQL functions, update `<extension>--<version>.sql`
4. Add pytest tests in `tests/pytests/`
5. Build and test:
   ```bash
   make install-<extension>
   make check-<extension>
   ```

### Creating a version upgrade script
```bash
# Bump all extension versions at once
python tools/bump_extension_versions.py 3.1

# This creates upgrade stubs like: pg_lake_engine--3.0--3.1.sql
```

### Debugging query execution
```bash
# Connect to PostgreSQL and check pg_lake query plans
psql -c "EXPLAIN (VERBOSE) SELECT * FROM iceberg_table"

# Connect directly to pgduck_server to see DuckDB execution
psql -p 5332 -h /tmp
postgres=> SELECT * FROM duckdb_settings();
postgres=> EXPLAIN SELECT ...;
```

### Viewing Iceberg metadata
```sql
-- Show all Iceberg tables and their metadata locations
SELECT table_name, metadata_location FROM iceberg_tables;

-- View Iceberg snapshots
SELECT * FROM iceberg_snapshots('<table_name>');
```

## Key PostgreSQL GUCs (Settings)

```sql
-- Required: preload base extension in postgresql.conf
shared_preload_libraries = 'pg_extension_base'

-- Iceberg settings
pg_lake_iceberg.default_location_prefix = 's3://bucket/prefix'
```

## CI and Testing Notes

- JDBC driver path must be set for Spark verification tests: `JDBC_DRIVER_PATH=/usr/share/java/postgresql.jar`
- Java 21+ required for Polaris REST catalog tests
- Azure tests require `azurite` (install via npm: `npm install -g azurite`)

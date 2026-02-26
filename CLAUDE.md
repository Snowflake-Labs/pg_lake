# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_lake integrates Apache Iceberg and data lake files (Parquet, CSV, JSON) into PostgreSQL, enabling
PostgreSQL to function as a lakehouse system. Developed by Snowflake Inc. and licensed under Apache 2.0.

The architecture has two main components:
- **PostgreSQL with pg_lake extensions**: Handles query planning, transaction boundaries, and orchestration
- **pgduck_server**: A separate multi-threaded process that implements the PostgreSQL wire protocol and
  delegates computation to DuckDB's columnar execution engine

Users connect only to PostgreSQL. The pg_lake extensions transparently delegate data scanning and
computation to pgduck_server (running DuckDB) when appropriate, while maintaining full transactional
guarantees.

**Current extension version:** 3.3 (upgrade paths: 1.6 → 2.4 → 3.0 → 3.1 → 3.2 → 3.3)

## Repository Layout

```
pg_datalake/
├── avro/                   # Apache Avro submodule (patched via avro.patch)
├── avro.patch              # Patch applied to Avro for Iceberg compatibility
├── docker/                 # Docker & Docker Compose for local dev/CI
├── docs/                   # User-facing documentation
├── duckdb_pglake/          # CMake-based DuckDB extension (adds PG-compatible functions)
├── engineering-notes/      # Internal dev notes (shippability, writes)
├── pg_extension_base/      # Foundation extension (Snowflake EDK)
├── pg_extension_updater/   # Auto-updates extensions on PostgreSQL startup
├── pg_lake/                # Meta-extension; installs all others via CASCADE
├── pg_lake_benchmark/      # Benchmarking extension (TPC-H, TPC-DS, ClickBench)
├── pg_lake_copy/           # COPY to/from data lake files
├── pg_lake_engine/         # Shared engine: Avro, storage, pgduck comms, query bridge
├── pg_lake_iceberg/        # Full Iceberg v2 protocol implementation
├── pg_lake_spatial/        # Geospatial formats via GDAL (optional, requires PostGIS)
├── pg_lake_table/          # Foreign data wrapper (largest extension)
├── pg_map/                 # Generic map/key-value type for semi-structured data
├── pgduck_server/          # Standalone C server: PostgreSQL wire protocol → DuckDB
├── test_common/            # Shared pytest fixtures, utilities, and sample data
├── tools/                  # Utility scripts (e.g., version bumper)
├── Makefile                # Top-level build orchestration
├── Pipfile                 # Python 3.11 dependencies
├── pytest.ini              # Root pytest configuration
└── shared.mk               # Shared Makefile rules for extensions
```

## Extension Architecture

pg_lake follows a **modular design**. Each extension has a specific responsibility:

### Extension dependency chain
```
pg_lake (meta-extension)
├── pg_lake_table (FDW for querying data lake files)
│   └── pg_lake_iceberg (Iceberg v2 protocol implementation)
│       └── pg_lake_engine (common module: storage, query engine bridge, data files)
│           ├── pg_extension_base (foundation for all extensions)
│           ├── pg_map (generic map type)
│           └── pg_extension_updater (automatic extension updates)
└── pg_lake_copy (COPY to/from data lake)
    └── pg_lake_engine

pg_lake_spatial (optional, depends on PostGIS + GDAL)
pg_lake_benchmark (optional, for TPC-H/TPC-DS/ClickBench benchmarking)
```

### Core extensions (all at version 3.3)

| Extension | Role | Requires |
|-----------|------|----------|
| `pg_extension_base` | Foundation for all extensions; utilities | — |
| `pg_extension_updater` | Auto-updates extensions on startup | pg_extension_base |
| `pg_map` | Generic map/key-value type for semi-structured data | — |
| `pg_lake_engine` | Shared module: Avro, storage abstraction, pgduck comms | pg_extension_base, pg_map |
| `pg_lake_iceberg` | Full Iceberg v2 protocol, manifests, metadata, snapshots | pg_lake_engine |
| `pg_lake_table` | FDW: query Parquet/CSV/JSON/Iceberg; DDL; planner hooks | pg_lake_engine, pg_lake_iceberg, btree_gist |
| `pg_lake_copy` | COPY command extensions for data lake import/export | pg_lake_engine, pg_lake_iceberg, pg_lake_table |
| `pg_lake` | Meta-extension: installs all above via `CREATE EXTENSION CASCADE` | pg_lake_table, pg_lake_copy |
| `pg_lake_spatial` | Geospatial file formats via GDAL | pg_lake, postgis |
| `pg_lake_benchmark` | Benchmark workloads | pg_lake |

### External components
- **pgduck_server**: Standalone C server implementing PostgreSQL wire protocol; delegates to DuckDB
- **duckdb_pglake**: CMake-based DuckDB extension adding PostgreSQL-compatible functions (S3, GCS, Azure)
- **avro**: Apache Avro C library (patched) for Iceberg Avro metadata handling

## Build Commands

### First-time build
```bash
# Install vcpkg C++ dependencies (required once)
export VCPKG_VERSION=2025.12.12
git clone --recurse-submodules --branch $VCPKG_VERSION https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl
export VCPKG_TOOLCHAIN_PATH="$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake"

# Build and install all extensions and pgduck_server
make install
```

### Subsequent builds
```bash
# Fast build: skips rebuilding DuckDB if already built
make install-fast
```

### Component-specific builds
```bash
# Build/install individual extensions
make install-pg_extension_base
make install-pg_map
make install-pg_lake_engine
make install-pg_lake_iceberg
make install-pg_lake_table
make install-pg_lake_copy
make install-pg_lake
make install-pgduck_server

# Build only DuckDB library
make install-duckdb_pglake
```

### System build dependencies (Debian/Ubuntu)
```bash
apt-get install -y build-essential cmake ninja-build libreadline-dev zlib1g-dev \
    flex bison libxml2-dev libxslt1-dev libicu-dev libssl-dev libgeos-dev \
    libproj-dev libgdal-dev libjson-c-dev libprotobuf-c-dev protobuf-c-compiler \
    uuid-dev liblz4-dev liblzma-dev libsnappy-dev perl libtool libjansson-dev \
    libcurl4-openssl-dev curl patch g++ libipc-run-perl jq diffutils
```

## Running pg_lake

### PostgreSQL configuration
```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_extension_base'

-- Connect to PostgreSQL and create extensions (CASCADE installs all dependencies):
CREATE EXTENSION pg_lake CASCADE;
-- Installs: pg_extension_base, pg_map, pg_extension_updater,
--           pg_lake_engine, pg_lake_iceberg, pg_lake_table, pg_lake_copy, pg_lake

-- Set default location for Iceberg tables:
SET pg_lake_iceberg.default_location_prefix TO 's3://your-bucket/pglake';
```

### Starting pgduck_server
```bash
# Start pgduck_server (must be running for pg_lake to work)
pgduck_server

# With options:
pgduck_server --memory_limit '8GB' --cache_dir /tmp/cache --init_file_path /path/to/init.sql

# pgduck_server listens on port 5332 via unix socket at /tmp
# Connect directly to pgduck_server for debugging:
psql -p 5332 -h /tmp
postgres=> SELECT * FROM duckdb_settings();
postgres=> SELECT duckdb_version();
```

### Docker (quickest setup)
```bash
cd docker
task compose:up       # Starts PostgreSQL + pgduck_server + MinIO
task compose:logs     # View logs
```

## Testing

The project uses **pytest** for all regression testing (not traditional PostgreSQL SQL regression tests).
Python version required: **3.11**. DuckDB version: **1.4.3**.

### Install test dependencies
```bash
pipenv install --dev
```

### Running all tests
```bash
# Run all local tests
make check-local

# Run end-to-end tests (requires S3/cloud access)
make check-e2e

# Run both local and e2e
make check

# Run upgrade tests (PostgreSQL version upgrades)
make check-upgrade
```

### Running tests for specific components
```bash
make check-pgduck_server
make check-pg_lake_copy
make check-pg_lake_iceberg
make check-pg_lake_table       # runs ~131 test files

# Isolation tester tests (transaction concurrency)
make check-isolation_pg_lake_table
```

### Running installcheck
```bash
# Start pgduck_server with test configuration first
pgduck_server --init_file_path pgduck_server/tests/test_secrets.sql --cache_dir /tmp/cache &

# Run installcheck (tests against installed extensions)
make installcheck

# Per-component installcheck
make installcheck-pg_lake_table
make installcheck-pg_lake_iceberg
```

### Running individual pytest tests
```bash
cd pg_lake_table
PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_specific.py
PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_specific.py::test_function_name

# Run tests split across multiple workers (mirrors CI)
PYTHONPATH=../test_common pipenv run pytest --splits=5 --group=1 -v tests/pytests/
```

### Testing with PostgreSQL regression suite
```bash
export PG_REGRESS_DIR=/path/to/postgres/src/test/regress
make installcheck-postgres PG_REGRESS_DIR=$PG_REGRESS_DIR
make installcheck-postgres-with_extensions_created PG_REGRESS_DIR=$PG_REGRESS_DIR
```

### Test types and locations
```
<extension>/tests/
├── conftest.py          # pytest config; sets up DB, S3, pgduck_server
├── pytests/             # Unit/integration tests (run with make check-<ext>)
├── e2e/                 # End-to-end tests requiring live S3/GCS/Azure
├── isolation/           # Isolation tester tests (pg_lake_table only)
│   ├── specs/           # *.spec files for the PostgreSQL isolation tester
│   ├── expected/        # Expected output *.out files
│   ├── isolation_schedule
│   └── test_isolation.py
└── upgrade/             # Upgrade scenario tests (pg_lake_table only)
    └── test_post_upgrade.py
```

### Test infrastructure
- **test_common/utils_pytest.py**: Central fixtures for DB connections, S3 (moto, port 5999),
  GCS (port 5998), Azure (Azurite), pgduck_server lifecycle, data validation
- **test_common/server_params.py**: Server parameter/GUC helpers
- **test_common/spark_utils.py**: Spark/Java integration for verification tests
- **test_common/utils_polaris.py**: Polaris REST catalog utilities
- **test_common/rest_catalog/polaris/**: Apache Polaris submodule
- **test_common/fixtures/**: Sample test data
- **test_common/sample/**: Additional datasets

### CI/CD (GitHub Actions)
File: `.github/workflows/pytest_all.yml`

Tested matrix:
- **PostgreSQL:** 16, 17, 18
- **OS:** AlmaLinux 9, Debian Bookworm

Jobs:
1. `build-image-*` - Build base Docker images for CI
2. `build-and-install-*` - Compile extensions
3. `test-check` - Run component checks (PG 17, 18 on AlmaLinux)
4. `test-check-pg-lake-table` - pg_lake_table tests split across 5 parallel workers
5. `test-installcheck` - installcheck on Debian PG 16
6. `test-installcheck-postgres` - PostgreSQL native regression suite (all versions)
7. `test-isolation` - Isolation tester (PG 17, 18)
8. `test-upgrade` - Version upgrade tests (16→17, 17→18, 16→18)

Environment variables relevant to testing:
- `CI_WORKER_COUNT` / `CI_WORKER_ID` - Enables split-based test parallelism
- `JDBC_DRIVER_PATH` - For Spark verification tests (default: `/usr/share/java/postgresql.jar`)
- `PG_LAKE_DELTA_SUPPORT=1` - Enable Delta Lake support
- `PG_CONFIG` - Path to pg_config (default: auto-detect)

## Key Source Code Locations

### pg_lake_engine/src/ — Shared engine infrastructure
```
cleanup/       - Garbage collection utilities
copy/          - Copy format handling
csv/           - CSV format support
data_file/     - Generic data file abstraction
ddl/           - DDL helpers
extensions/    - Extension management utilities
functions/     - SQL function implementations
json/          - JSON format support
parquet/       - Parquet format support
parsetree/     - Query parse tree utilities
permissions/   - Permission checking
pgduck/        - pgduck_server communication: type conversion, query execution,
                 region support, serialization, caching
query/         - Query utilities
storage/       - Storage abstraction: local, S3/GCS/Azure, write/read/delete
utils/         - General utilities
```

### pg_lake_iceberg/src/ — Iceberg v2 protocol
```
avro/               - Avro serialization for manifest files
http/               - HTTP client for REST catalog
iceberg/            - Core Iceberg logic:
  catalog.c           - Table catalog
  data_file_stats.c   - Data file statistics
  iceberg_field.c     - Schema field handling
  iceberg_functions.c - SQL functions (iceberg_snapshots, etc.)
  iceberg_type_*.c    - Type binary/JSON serialization
  metadata_operations.c - Metadata read/write
  operations/         - Table operations (merge, delete, etc.)
  partitioning/       - Partition spec handling
  read/write_manifest.c - Manifest read/write
  read/write_table_metadata.c - Metadata file I/O
object_store_catalog/ - Object store-based catalog
rest_catalog/         - REST catalog (Polaris) support
```

### pg_lake_table/src/ — Foreign data wrapper (largest extension)
```
access_method/  - Table access method hooks
cache/          - Table metadata caching
ddl/            - DDL: create_table.c, alter_table.c, drop_table.c, vacuum.c,
                        create_table_as_select.c
describe/       - Schema inference from external sources
duckdb/         - DuckDB query transformation
fdw/            - FDW implementation:
  pg_lake_table.c         - Main FDW entry point
  deparse.c / deparse_ruleutils.c - SQL deparsing
  shippable.c             - Pushdown shippability logic
  data_file_pruning.c     - Partition/file pruning
  catalog/ schema_operations/ partitioning/ - FDW catalog integration
planner/        - Query planning and pushdown:
  query_pushdown.c        - Main pushdown logic
  explain.c               - EXPLAIN support
  insert_select.c         - INSERT ... SELECT pushdown
transaction/    - Transaction hooks and tracking
recovery/       - Crash recovery utilities
```

### pgduck_server/src/ — Standalone DuckDB server
```
main.c              - Entry point, process initialization
command_line/       - CLI argument parsing
duckdb/
  duckdb.c          - DuckDB C API wrapper; secret/credential management
  type_conversion.c - PostgreSQL ↔ DuckDB type mapping
pgserver/
  pgserver.c        - PostgreSQL wire protocol server
  client_threadpool.c - Thread pool for concurrent clients
pgsession/
  pgsession.c       - Session state management
  pgsession_io.c    - I/O handling
  pqformat.c        - PostgreSQL message format
utils/              - Logging, PID file, pg log utilities
```

### duckdb_pglake/src/ — DuckDB extension (C++)
```
duckdb_pglake_extension.cpp - Extension registration
fs/
  caching_file_system.cpp   - File system with caching
  file_cache_manager.cpp    - Cache management
  pg_lake_s3fs.cpp          - S3 file system
  region_aware_s3fs.cpp     - AWS region-aware S3
  httpfs_extended.cpp       - HTTP file system extension
utility_functions.cpp       - Utility DuckDB functions
```

## Important File Locations

### Build system
- `Makefile` — Top-level build orchestration; defines all targets and dependency chains
- `shared.mk` — Shared Makefile rules; disables `-Wdeclaration-after-statement`
- `<ext>/Makefile` — Per-extension PGXS-based Makefile
- `duckdb_pglake/CMakeLists.txt` — CMake build for the DuckDB extension
- `duckdb_pglake/vcpkg.json` — C++ dependencies (Azure SDK, AWS SDK, OpenSSL)

### Extension SQL files
- `<ext>/<ext>--3.0.sql` — Initial schema for version 3.0
- `<ext>/<ext>--X.Y--X.Z.sql` — Migration scripts between versions
- Use `python tools/bump_extension_versions.py <new-version>` to create new stubs

### Tests
- `<ext>/tests/pytests/` — Main pytest test suites
- `<ext>/tests/e2e/` — End-to-end tests requiring cloud storage
- `<ext>/tests/isolation/` — Isolation tester tests for concurrency
- `<ext>/tests/upgrade/` — Extension upgrade tests
- `test_common/` — Shared test utilities and fixtures
- `pytest.ini` — Root pytest configuration (log level, custom markers)

### Documentation
- `docs/building-from-source.md` — Detailed build instructions and prerequisites
- `docs/iceberg-tables.md` — Iceberg table usage guide (largest, ~43 KB)
- `docs/query-data-lake-files.md` — FDW usage for external data
- `docs/data-lake-import-export.md` — COPY command usage
- `docs/file-formats-reference.md` — Parquet/CSV/JSON format support reference
- `docs/spatial.md` — Geospatial format support via GDAL
- `docs/dbt.md` — dbt integration guide
- `engineering-notes/pgduck_shippability.md` — Function/operator pushdown compatibility guide
- `engineering-notes/writes.md` — Write operation implementation notes

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
- **License header**: Every C file must include the Apache 2.0 header (see any `src/*.c` for template)
- **C++ standard**: C++14 (for duckdb_pglake)
- **PostgreSQL compat**: Avoid `-Wdeclaration-after-statement` (disabled in shared.mk)

### Python code
- **Formatting**: Use `black` version 25.9.0 (see `make reindent` or `pipenv run black`)
- **Style**: Follow pytest conventions for test naming (`test_*.py`) and fixtures
- **Python version**: 3.11 required

### Pre-commit checks
```bash
# Format all code (pgindent for C, black for Python)
make reindent

# Verify formatting without changing files
make check-indent
```

A `.pre-commit-config.yaml` is also provided with pgindent, black, and typos hooks.

## Query Pushdown (Shippability)

A core feature of pg_lake_table is pushing SQL operations down to DuckDB for execution. When working
on query features, understand these categories:

- **Non-shippable**: Operations DuckDB cannot handle (missing types, unsupported aggregates)
- **Conditionally shippable**: Supported but may have minor behavioral differences vs PostgreSQL
- **Seamlessly shippable**: Full compatibility; see `shippable_builtin_functions.c` and
  `shippable_builtin_operators.c`

### SQL shims pattern
When a PostgreSQL function needs to be renamed for DuckDB, create a shim in the internal schema
`__lake__internal__nsp__`:
```sql
CREATE FUNCTION __lake__internal__nsp__.regexp_matches(text,text,text)
 RETURNS bool LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', 'pg_lake_internal_dummy_function';
```
Then add a `RewriteFuncExpr()` call in `pg_lake_engine/src/storage/rewrite_query.c`. See
`engineering-notes/pgduck_shippability.md` for full details and troubleshooting.

### Key shippability files
- `pg_lake_table/src/fdw/shippable.c` — Main shippability decision logic
- `pg_lake_engine/src/pgduck/shippable_builtin_functions.c` — Shippable functions list
- `pg_lake_engine/src/pgduck/shippable_builtin_operators.c` — Shippable operators list
- `pg_lake_engine/src/pgduck/type.c` — `GetDuckDBTypeForPGType()` — type compatibility
- `pg_lake_engine/src/storage/rewrite_query.c` — Function call rewriting for DuckDB

## Local Development with MinIO

For testing without cloud S3 latency, use MinIO locally:

```bash
# Install and start MinIO
brew install minio  # or download from min.io
minio server /tmp/data
# Access UI at http://localhost:9000
# Create access key: testkey / testpassword, bucket: localbucket

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
3. If adding SQL functions, update `<extension>--<current-version>.sql` and migration scripts
4. Add pytest tests in `tests/pytests/`
5. Build and test:
   ```bash
   make install-<extension>
   make check-<extension>
   ```

### Creating a version upgrade script
```bash
# Bump all extension versions at once
python tools/bump_extension_versions.py 3.4

# Creates upgrade stubs like: pg_lake_engine--3.3--3.4.sql
# Edit the stub to add your migration SQL
```

### Adding a new pushdown function
1. Check `engineering-notes/pgduck_shippability.md` for the shim pattern
2. Add the function to the shippable list in `shippable_builtin_functions.c`
3. If renaming is needed, add a shim in the migration SQL and a `RewriteFuncExpr()` call
4. Add tests in `pg_lake_table/tests/pytests/`

### Debugging query execution
```bash
# Check pg_lake query plans in PostgreSQL
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

### Running dbt tests
```bash
cd pg_lake_table/tests/dbt_pglake_tests
# Configure profiles.yml for your local PostgreSQL
pipenv run dbt run
pipenv run dbt test
```

## Key PostgreSQL GUCs (Settings)

```sql
-- Required: preload base extension in postgresql.conf
shared_preload_libraries = 'pg_extension_base'

-- Iceberg default table location
pg_lake_iceberg.default_location_prefix = 's3://bucket/prefix'
```

## External Dependency Versions

- **DuckDB**: 1.4.3 (pinned in Pipfile; submodule in duckdb_pglake/)
- **vcpkg**: 2025.12.12 (for C++ Azure/AWS SDK dependencies)
- **Python**: 3.11
- **PostgreSQL**: 16, 17, 18 (all supported and CI-tested)
- **black**: 25.9.0 (Python formatter)

## CI and Testing Notes

- JDBC driver path must be set for Spark verification tests: `JDBC_DRIVER_PATH=/usr/share/java/postgresql.jar`
- Java 21+ required for Polaris REST catalog tests
- Azure tests require `azurite` (install via npm: `npm install -g azurite`)
- GCS tests use a built-in moto GCS server (port 5998)
- S3 tests use moto (port 5999, bucket: `testbucketcdw`)
- pg_lake_table has ~131 test files; CI splits them across 5 parallel workers
- `PG_LAKE_DELTA_SUPPORT=1` enables Delta Lake support in tests

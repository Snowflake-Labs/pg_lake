# Building from source

This guide covers installing pg_lake from source. For Docker-based setup, see [docker/LOCAL_DEV.md](../docker/LOCAL_DEV.md).

## Add pg_lake to an Existing PostgreSQL Installation

If you already have PostgreSQL 16, 17, or 18 installed, you can add pg_lake extensions using the automated installation script:

```bash
# Clone the repository
git clone --recurse-submodules https://github.com/snowflake-labs/pg_lake.git
cd pg_lake

# Install pg_lake to your existing PostgreSQL
# Or see "Manual Installation to Existing PostgreSQL" section below
./install.sh

# Start pgduck_server (required for pg_lake)
pgduck_server --cache_dir /tmp/pg_lake_cache/

# In another terminal, create the extensions
psql -c "CREATE EXTENSION pg_lake CASCADE;"
```

**What this does:**
1. Detects your existing PostgreSQL installation via `pg_config`
2. Installs vcpkg and Azure SDK dependencies
3. Builds and installs pg_lake extensions to your PostgreSQL installation
4. Prints next steps for configuring and using pg_lake

**Note:** System build dependencies are skipped by default (assuming they're installed if PostgreSQL exists). Use `--with-system-deps` if needed.

**Prerequisites:**
- PostgreSQL 16, 17, or 18 installed with `pg_config` in your PATH
- `shared_preload_libraries = 'pg_extension_base'` in postgresql.conf ([see setup](#configure-postgresql))

For manual installation steps, see [Manual Installation to Existing PostgreSQL](#manual-installation-to-existing-postgresql).

## Full Development Environment Setup

For pg_lake development (includes building PostgreSQL from source with debug symbols and test dependencies):

```bash
# Clone the repository
git clone --recurse-submodules https://github.com/snowflake-labs/pg_lake.git
cd pg_lake

# Build complete development environment (PostgreSQL 18 from source, vcpkg, pg_lake, test deps)
./install.sh --build-postgres --with-test-deps

# Add PostgreSQL to your PATH
export PATH=$HOME/pgsql/18/bin:$PATH

# Start PostgreSQL
pg_ctl -D $HOME/pgsql/18/data -l $HOME/pgsql/18/data/logfile start

# Start pgduck_server (required for pg_lake)
pgduck_server --cache_dir /tmp/pg_lake_cache/

# In another terminal, create the extensions
psql postgres -c "CREATE EXTENSION pg_lake CASCADE;"
```

For more options and control, see [Development Environment Options](#development-environment-options) below.

## Prerequisites

**For adding to existing PostgreSQL:**
- PostgreSQL 16, 17, or 18 with `pg_config` in PATH
- Git, C/C++ compiler, CMake, Ninja
- Internet connection

**For full development environment:**
- All of the above
- Python 3.11+ (for tests)
- Additional system packages (automatically installed by installation script)

The installation script will install all required build dependencies automatically. If you prefer to install dependencies manually, see the [Manual Development Environment Setup](#manual-development-environment-setup) section.

## Configure PostgreSQL

Before using pg_lake, you need to add `pg_extension_base` to PostgreSQL's `shared_preload_libraries`:

```bash
# Add to postgresql.conf
echo "shared_preload_libraries = 'pg_extension_base'" >> $PGDATA/postgresql.conf

# Or edit postgresql.conf manually to add:
# shared_preload_libraries = 'pg_extension_base'

# Restart PostgreSQL for the change to take effect
pg_ctl restart -D $PGDATA
```

## Manual Installation to Existing PostgreSQL

If you prefer to install pg_lake manually to your existing PostgreSQL installation:

### Install Build Dependencies

First, ensure you have the necessary build tools. For detailed system dependencies by platform, see the [Manual Development Environment Setup](#manual-development-environment-setup) section.

**Minimum requirements:**
```bash
# Debian/Ubuntu
sudo apt-get install -y build-essential cmake ninja-build git pkg-config curl python3-dev

# RHEL/AlmaLinux
sudo dnf install -y cmake ninja-build git pkgconfig curl gcc-c++ python3-devel

# macOS
brew install cmake ninja git pkg-config curl python@3

# Note: For macOS, vcpkg requires cmake 3.31.1 (not the latest version)
# See Manual Development Environment Setup for installation instructions
```

**Note for macOS users:** vcpkg has compatibility issues with the latest cmake. You need cmake 3.31.1. See the [macOS section](#macos) in Manual Development Environment Setup for installation instructions.

### Install vcpkg and Azure SDK

```bash
# Install vcpkg
export VCPKG_VERSION=2025.10.17
git clone --recurse-submodules --branch $VCPKG_VERSION https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh

# Install Azure SDK packages (this may take a while)
./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl

# Set environment variable
export VCPKG_TOOLCHAIN_PATH="$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake"
```

### Build and Install pg_lake

```bash
# Clone pg_lake
git clone --recurse-submodules https://github.com/snowflake-labs/pg_lake.git
cd pg_lake

# Ensure pg_config is in PATH
which pg_config

# Build and install (first build will be slow due to DuckDB compilation)
make install

# For subsequent builds, use install-fast to skip rebuilding DuckDB
make install-fast
```

### Start pgduck_server and Use pg_lake

```bash
# Set up AWS credentials (recommended)
aws configure

# Start pgduck_server (required for pg_lake)
pgduck_server --cache_dir /tmp/pg_lake_cache/

# In another terminal, create extensions
psql -c "CREATE EXTENSION pg_lake CASCADE;"
psql -c "SET pg_lake_iceberg.default_location_prefix TO 's3://your-bucket/pglake';"
```

## Development Environment Options

The `install.sh` script provides flexible installation options:

**Default behavior:** Installs pg_lake to an existing PostgreSQL installation (detects via `pg_config`)

**With `--build-postgres`:** Builds PostgreSQL from source with debug symbols and initializes a database cluster

### Basic Usage

```bash
# Install to existing PostgreSQL (default)
./install.sh

# Install with test dependencies
./install.sh --with-test-deps

# Build PostgreSQL 18 from source + pg_lake
./install.sh --build-postgres

# Build PostgreSQL 17 from source with test dependencies
./install.sh --build-postgres --pg-version 17 --with-test-deps

# Use multiple CPU cores for faster builds
./install.sh --jobs 16
```

### Common Scenarios

**Install only pg_lake (skip dependency installation):**

```bash
# Assumes vcpkg already installed
./install.sh --skip-vcpkg
```

**Install with system dependencies:**

```bash
# If you need to install system build tools (cmake, ninja, etc.)
./install.sh --with-system-deps
```

**Custom PostgreSQL installation prefix:**

```bash
# When building PostgreSQL from source
./install.sh --build-postgres --prefix /opt/pgsql
```

### All Options

Run `./install.sh --help` to see all available options:

```
--build-postgres            Build PostgreSQL from source and initialize database
--pg-version VERSION        PostgreSQL version to build (16, 17, or 18) [default: 18]
--prefix DIR                PostgreSQL installation prefix [default: auto-detect or $HOME/pgsql]
--source-dir DIR            Directory for cloning source repos [default: $HOME/pg_lake-deps]
--jobs N                    Number of parallel build jobs [default: nproc]

--with-system-deps          Install system build dependencies (auto-enabled with --build-postgres)
--skip-vcpkg                Skip vcpkg and Azure SDK installation
--skip-pg-lake              Skip building pg_lake extensions

--with-test-deps            Install optional test dependencies (PostGIS, pgAudit, pg_cron, azurite)
```

## Manual Development Environment Setup

If you prefer to set up a complete development environment manually (including building PostgreSQL from source), follow these steps.

### Install System Build Dependencies

#### Debian/Ubuntu

```bash
apt-get update && \
apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt1-dev \
    libicu-dev \
    libssl-dev \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    libjson-c-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    diffutils \
    uuid-dev \
    libossp-uuid-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    perl \
    libtool \
    libjansson-dev \
    libcurl4-openssl-dev \
    curl \
    patch \
    g++ \
    libipc-run-perl \
    jq \
    git \
    pkg-config \
    python3-dev
```

#### RHEL/AlmaLinux/Rocky Linux

```bash
dnf -y update && \
dnf -y install epel-release && \
dnf config-manager --enable crb && \
dnf -y install \
    cmake \
    ninja-build \
    readline-devel \
    zlib-devel \
    flex \
    bison \
    libxml2-devel \
    libxslt-devel \
    libicu-devel \
    openssl-devel \
    geos-devel \
    proj-devel \
    gdal-devel \
    json-c-devel \
    protobuf-c-devel \
    uuid-devel \
    lz4-devel \
    xz-devel \
    snappy-devel \
    perl \
    perl-IPC-Run \
    perl-IPC-Cmd \
    libtool \
    jansson-devel \
    jq \
    libcurl-devel \
    patch \
    which \
    gcc-c++ \
    git \
    pkgconfig \
    python3-devel
```

#### macOS

```bash
# Install Xcode Command Line Tools
xcode-select --install

# Install Homebrew packages
brew update
brew install \
    cmake \
    ninja \
    readline \
    zlib \
    libxml2 \
    libxslt \
    icu4c \
    openssl@3 \
    geos \
    proj \
    gdal \
    json-c \
    protobuf-c \
    lz4 \
    xz \
    snappy \
    jansson \
    curl \
    libtool \
    flex \
    bison \
    diffutils \
    jq \
    ossp-uuid \
    perl \
    pkg-config \
    python@3

# Install cmake 3.31.1 for vcpkg compatibility
brew tap-new $USER/local-cmake
brew tap homebrew/core --force
brew extract --version=3.31.1 cmake $USER/local-cmake
brew install $USER/local-cmake/cmake@3.31.1

# Configure environment variables for compilers
export PATH="/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:$PATH"
export LDFLAGS="-L/opt/homebrew/opt/icu4c/lib -L/opt/homebrew/opt/openssl@3/lib"
export CPPFLAGS="-I/opt/homebrew/opt/icu4c/include -I/opt/homebrew/opt/openssl@3/include"
export PKG_CONFIG_PATH="/opt/homebrew/opt/icu4c/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig"
```

**Note for macOS:** For vcpkg to work correctly with DuckDB plugins, you cannot use the latest version of CMake. The commands above install a known-working version (3.31.1).

### Build PostgreSQL from Source

pg_lake is supported with PostgreSQL 16, 17, and 18. For development, we recommend building PostgreSQL from source to get debug symbols and assertions.

```bash
# Clone PostgreSQL (version 18 example)
git clone https://github.com/postgres/postgres.git -b REL_18_STABLE
cd postgres

# Configure (Linux example)
./configure \
    --prefix=$HOME/pgsql/18 \
    --enable-injection-points \
    --enable-tap-tests \
    --enable-debug \
    --enable-cassert \
    --enable-depend \
    CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer" \
    --with-openssl \
    --with-libxml \
    --with-libxslt \
    --with-icu \
    --with-lz4 \
    --with-python

# Configure (macOS example)
./configure \
    --prefix=$HOME/pgsql/18 \
    --enable-injection-points \
    --enable-tap-tests \
    --enable-debug \
    --enable-cassert \
    --enable-depend \
    CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer" \
    --with-openssl \
    --with-libxml \
    --with-libxslt \
    --with-icu \
    --with-lz4 \
    --with-python \
    --with-readline \
    --with-includes=/opt/homebrew/include/ \
    --with-libraries=/opt/homebrew/lib \
    PG_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk

# Build and install
make -j 16 && make install

# Install all contrib modules (includes auto_explain, pg_stat_statements, btree_gist, etc.)
make -C contrib install

# Install test modules (optional, for test suite)
make -C src/test/modules/injection_points install
make -C src/test/isolation install

# Add PostgreSQL to PATH
export PATH=$HOME/pgsql/18/bin:$PATH
```

### Install vcpkg and Azure SDK Dependencies

pg_lake requires vcpkg for managing Azure SDK dependencies:

```bash
# Install vcpkg
export VCPKG_VERSION=2025.10.17
git clone --recurse-submodules --branch $VCPKG_VERSION https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh

# Install Azure SDK packages (this may take a while)
./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl

# Set environment variable for pg_lake build
export VCPKG_TOOLCHAIN_PATH="$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake"
```

### Build and Install pg_lake

```bash
# Clone pg_lake repository
git clone --recurse-submodules https://github.com/snowflake-labs/pg_lake.git
cd pg_lake

# Make sure pg_config is in your PATH
export PATH=$HOME/pgsql/18/bin:$PATH

# Build and install pg_lake
# First build will be slow due to DuckDB compilation
make install

# For subsequent builds, use install-fast to skip rebuilding DuckDB
make install-fast
```

**Note:** The first `make install` will take a while because it needs to build DuckDB. Subsequent builds can use `make install-fast` to skip rebuilding DuckDB if it hasn't changed.

### Initialize PostgreSQL Database

```bash
# Create a new database cluster
initdb -k -D $HOME/pgsql/18/data --locale=C.UTF-8

# Configure shared_preload_libraries
echo "shared_preload_libraries = 'pg_extension_base'" >> $HOME/pgsql/18/data/postgresql.conf

# Start PostgreSQL
pg_ctl -D $HOME/pgsql/18/data -l $HOME/pgsql/18/data/logfile start
```

### Start pgduck_server and Use pg_lake

To use pg_lake, you need to have `pgduck_server` running:

```bash
# (Recommended) Set up AWS credentials first
aws configure

# Start pgduck_server
pgduck_server --cache_dir /tmp/pg_lake_cache/
```

Unless you are making changes to `pgduck_server`, it is generally ok to keep an unrelated instance running.

Connect to PostgreSQL and create the pg_lake extensions:

```sql
CREATE EXTENSION pg_lake CASCADE;

-- Set S3 location for Iceberg tables
SET pg_lake_iceberg.default_location_prefix TO 's3://your-bucket/pglake';

-- Test with a simple Iceberg table
CREATE TABLE test(id int, name text) USING iceberg;
INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM test;

-- Test COPY to Parquet
\copy (select s x, s y from generate_series(1,10) s) to '/tmp/xy.parquet' with (format 'parquet')
```

### Running pgduck_server under a Separate Linux User

It is possible to run `pgduck_server` under a separate Linux user by setting permissions on the database directory. If postgres is running under the `postgres` user, the simplest approach is to add the pgduck user to the postgres group.

```bash
# Create pgduck user and add it to the postgres group
sudo adduser pgduck
sudo usermod -a -G postgres pgduck

export PGDATA=/home/postgres/18/

# Make the database directory accessible by group
initdb -D $PGDATA -g --locale=C.UTF-8
# or: chmod 750 $PGDATA $PGDATA/base

# Make sure pgsql_tmp directory exists
mkdir -p $PGDATA/base/pgsql_tmp

# Allow group to read and write pgsql_tmp
chmod 2770 $PGDATA/base/pgsql_tmp

# Run pgduck_server as pgduck user with postgres as unix socket group
sudo su pgduck -c "pgduck_server --unix_socket_group postgres --duckdb_database_file_path /cache/duckdb.db --cache_dir /cache/files"
```

Verify that COPY in Parquet format to/from stdout/stdin and S3 all work.

## Test Environment Setup

To run the full test suite, you need to install additional dependencies. These are optional if you only want to use pg_lake.

### Test Dependencies

- **Python 3.11+** with pipenv
- **PostGIS** (dependency of pg_lake_spatial)
- **pgAudit** (used in test suite)
- **pg_cron** (used in test suite)
- **Java 21+** (for Spark verification tests and Polaris REST catalog tests)
- **PostgreSQL JDBC driver** (for Spark tests)
- **Polaris REST catalog server** (for Iceberg REST catalog tests)
- **azurite** (for Azure storage tests)

The automated installation script can install these for you:

```bash
./install.sh --with-test-deps
```

Or install them manually:

### Install Python Dependencies

```bash
# Install pipenv (if not already installed)
pip3 install --user pipenv

# Install test dependencies
pipenv install --dev
```

### Build PostGIS

PostGIS is required for pg_lake_spatial extension:

```bash
git clone https://github.com/postgis/postgis.git
cd postgis
./autogen.sh
./configure --prefix=$HOME/pgsql/18/
make -j 16
make install
```

### Build pgAudit

pgAudit is used in the test suite:

```bash
git clone https://github.com/pgaudit/pgaudit.git
cd pgaudit
make USE_PGXS=1 install
```

### Build pg_cron

pg_cron is used in the test suite:

```bash
git clone https://github.com/citusdata/pg_cron.git
cd pg_cron
make install
```

### Install azurite (for Azure Tests)

Azure tests use azurite, which requires Node.js and npm:

```bash
# Ubuntu/Debian
sudo apt-get install -y nodejs npm

# RHEL/AlmaLinux
sudo dnf install -y nodejs npm

# macOS
brew install node

# Install azurite
npm install -g azurite
```

### Java and JDBC Driver

Some tests verify pg_lake_iceberg table results using Apache Spark, which requires Java 21+ and the PostgreSQL JDBC driver.

**Note:** `./install.sh --with-test-deps` automatically installs Java 21+ and downloads the JDBC driver for you.

To install manually:

```bash
# Install Java 21 or higher
# Ubuntu/Debian
sudo apt-get install -y openjdk-21-jdk

# RHEL/AlmaLinux
sudo dnf install -y java-21-openjdk

# macOS
brew install openjdk@21

# Download PostgreSQL JDBC driver (version 42.7.10)
mkdir -p ~/pg_lake-deps/jdbc
curl -L -o ~/pg_lake-deps/jdbc/postgresql-42.7.10.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.10.jar

# Set environment variable (add to ~/.bashrc or ~/.zshrc):
export JDBC_DRIVER_PATH=~/pg_lake-deps/jdbc/postgresql-42.7.10.jar
```

### Polaris REST Catalog Server

Polaris is an Apache Iceberg REST catalog server used for testing Iceberg REST catalog integration. It requires Java 21+.

**Note:** `./install.sh --with-test-deps` automatically builds and installs Polaris for you.

To build and install manually:

```bash
# Ensure Java 21+ is installed and in PATH
java -version  # Should show version 21 or higher

# Build Polaris (this takes several minutes on first build)
cd test_common/rest_catalog
make all

# Install Polaris JARs to PostgreSQL bin directory
make install
```

This will install:
- `polaris-server.jar` - The REST catalog server
- `polaris-admin.jar` - The admin CLI tool
- `gradlew` - Gradle wrapper script

For more details, see `test_common/rest_catalog/README.md`.

## Running Tests

We primarily use pytest for regression tests.

### Run All Tests

```bash
# Run all local tests
make check

# Run end-to-end tests (requires S3/cloud access)
make check-e2e

# Run upgrade tests
make check-upgrade
```

### Run Component-Specific Tests

```bash
# Test specific extensions
make check-pg_lake_table
make check-pg_lake_iceberg
make check-pgduck_server

# Run isolation tests
make check-isolation_pg_lake_table
```

### Run Individual Pytest Tests

```bash
cd pg_lake_table
PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_specific.py
PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_specific.py::test_function_name
```

### Run installcheck Tests

installcheck tests run against installed extensions in a running PostgreSQL instance:

```bash
# Start pgduck_server with test configuration
pgduck_server --init_file_path pgduck_server/tests/test_secrets.sql --cache_dir /tmp/cache &

# Run installcheck
make installcheck

# Run installcheck for specific component
make installcheck-pg_lake_table
```

**Note:** There are several PostgreSQL settings that may affect the installcheck result (e.g., timezone, `pg_lake_iceberg.default_location_prefix`).

### Running PostgreSQL Tests with pg_lake Extensions

We also run PostgreSQL's own regression tests with pg_lake extensions loaded to ensure we don't break regular PostgreSQL behavior:

```bash
# Set PostgreSQL source directory
export PG_REGRESS_DIR=/path/to/postgres/src/test/regress

# Configure PostgreSQL for testing
psql postgres << EOF
ALTER SYSTEM SET compute_query_id='regress';
ALTER SYSTEM SET pg_lake_table.hide_objects_created_by_lake=true;
SELECT pg_reload_conf();
EOF

# Run PostgreSQL tests with pg_lake in shared_preload_libraries
make installcheck-postgres PG_REGRESS_DIR=$PG_REGRESS_DIR

# Run PostgreSQL tests with pg_lake extensions created
make installcheck-postgres-with_extensions_created PG_REGRESS_DIR=$PG_REGRESS_DIR
```

### Why pytest?

We have avoided traditional SQL regression tests because:

- They lack programmatic testing capabilities
- They require repetitive patterns that are hard to maintain
- They often have non-deterministic output (e.g., unordered SELECT results)
- They're sensitive to small implementation details
- pytest is widely used and well-supported by AI tools for test generation

## Running S3-Compatible Service (MinIO) Locally

pg_lake heavily relies on S3 storage. However, using real S3 can introduce significant latency during local testing. You can use MinIO for local S3-compatible storage.

### Installation and Setup of MinIO

**1. Install MinIO:**

```bash
# macOS
brew install minio

# Linux - download from https://min.io/download
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/
```

**2. Start MinIO Server:**

```bash
# Remove leftovers from previous run if needed
rm -rf /tmp/data

# Start server
minio server /tmp/data
```

**3. Access MinIO UI:**

Open your browser and go to http://localhost:9000/

**4. Create Access and Secret Keys:**

From the MinIO UI, create access credentials. For simplicity, use:
- Access Key: `testkey`
- Secret Key: `testpassword`

**5. Add MinIO Profile to ~/.aws/config:**

```ini
[services testing-minio]
s3 =
   endpoint_url = http://localhost:9000

[profile minio]
region = us-east-1
services = testing-minio
aws_access_key_id = testkey
aws_secret_access_key = testpassword
```

**6. Create a Bucket:**

From the MinIO UI, create a bucket named `localbucket`.

**7. Configure DuckDB Secret in pgduck_server:**

Connect to pgduck_server and create the S3 secret:

```sql
psql -p 5332 -h /tmp

CREATE SECRET s3testMinio (
    TYPE S3,
    KEY_ID 'testkey',
    SECRET 'testpassword',
    ENDPOINT 'localhost:9000',
    SCOPE 's3://localbucket',
    URL_STYLE 'path',
    USE_SSL false
);
```

**8. Create an Iceberg Table Using MinIO:**

```sql
SET pg_lake_iceberg.default_location_prefix TO 's3://localbucket';
CREATE TABLE t_iceberg(a int) USING iceberg;
```

## Automatically Bumping Extension Versions

To bump all PostgreSQL extensions in the repo to a new version:

```bash
python tools/bump_extension_versions.py 3.0
```

What it does:
- Finds all folders containing a `.control` file
- Updates the `default_version` field in each control file
- Creates a new SQL stub for version upgrade (e.g., `pg_lake_engine--2.4--3.0.sql`)

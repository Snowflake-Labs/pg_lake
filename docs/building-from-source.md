# Building from source

## Prerequisites

To build `pg_lake`, you will need to install PostgreSQL and a few dependencies. For development, we recommend building PostgreSQL from source to get debug symbols and assertions. It also lets you install into your home directory to avoid needing superuser for a `make install`.

## Install build dependencies for Debian based distros

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
    jq
```

## Install build dependencies for RHEL based distros

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
    gcc-c++
```

## Install build dependencies for MacOS

```bash
xcode-select --install
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
    perl

# --- Configure environment variables for compilers ---
export PATH="/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:$PATH"
export LDFLAGS="-L/opt/homebrew/opt/icu4c/lib -L/opt/homebrew/opt/openssl@3/lib"
export CPPFLAGS="-I/opt/homebrew/opt/icu4c/include -I/opt/homebrew/opt/openssl@3/include"
export PKG_CONFIG_PATH="/opt/homebrew/opt/icu4c/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig"

```

## pg_lake build and install steps
After installing os specific build dependencies, you can follow below steps. The first time you build `pg_lake`, you'll need to build `duckdb_pglake` (a DuckDB extension). Unfortunately, building a DuckDB extension also involves building DuckDB, which can take a while. Additionally, building `pg_lake` requires `vcpkg` to manage dependencies for DuckDB extensions.

```bash
# install vcpkg dependencies
export VCPKG_VERSION=2025.01.13 && \
git clone --recurse-submodules https://github.com/Microsoft/vcpkg.git && \
./vcpkg/bootstrap-vcpkg.sh && \
./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl && \
export VCPKG_TOOLCHAIN_PATH="$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake"

# Make sure pg_config is in your PATH (e.g. export PATH=$HOME/pgsql-18/bin:$PATH):

# install pg_lake extensions
git clone --recurse-submodules https://github.com/snowflake-labs/pg_lake.git && \
cd pg_lake/duckdb_pglake && make && make install && \
cd .. && make install-avro-local && make fast && make install-fast
```

For MacOS to work with `vcpkg`, you will need to install `cmake` via `brew`, however you cannot use the latest version of CMake, due to compatibility issues with other DuckDB plugins.  To install a known-working version of `cmake` using `brew`, run the following:

```bash
brew tap-new $USER/local-cmake
brew tap homebrew/core --force
brew extract --version=3.31.1 cmake $USER/local-cmake
brew install $USER/local-cmake/cmake@3.31.1
```

### Setting up PostgreSQL

pg_lake is supported with PostgreSQL 16, 17 and 18.

To create a new PostgreSQL database directory, run initdb: 

```bash
$ which initdb
$ HOME/pgsql/18/bin/initdb
$ initdb -k -D $HOME/pgsql/18/datastore --locale=C.UTF-8
...snip...
Success. You can now start the database server using:

    pg_ctl -D $HOME/pgsql/18/datastore -l logfile start
```

To set up PgLake, you need to add `pg_extension_base` to shared_preload_libraries, which will load other modules as needed.
```
shared_preload_libraries = 'pg_extension_base'
```

### Using `pg_lake`

To use `pg_lake`, you need to make sure `pgduck_server` is running.
```bash
# Recommended to set up AWS credentials first:
aws configure

export PATH=$HOME/pgsql/18/bin:$PATH
pgduck_server
```

Unless you are making changes to `pgduck_server`, it is generally ok to keep an unrelated instance running.

You can use `psql` to create `pg_lake` extensions and start using its features:

```sql
CREATE EXTENSION pg_lake CASCADE;
\copy (select s x, s y from generate_series(1,10) s) to '/tmp/xy.parquet' with (format 'parquet')
```

### Running pgduck_server under a separate Linux user

It is possible to run `pgduck_server` under a separate Linux user by setting permissions on the database directory after initialization. If postgres is running under the `postgres` user, the simplest approach is to add the pgduck user to the postgres group.

```bash
# Create pgduck and add it to the postgres group
sudo adduser pgduck
sudo usermod -a -G postgres pgduck

export PGDATA=/home/postgres/18/

# Make the database directory accessible by group
initdb -D $PGDATA -g --locale=C.UTF-8
# or: chmod 750 $PGDATA $PGDATA/base

# Make sure pgsql_tmp directory exists (gets created automatically, otherwise)
mkdir -p $PGDATA/base/pgsql_tmp

# Allow group to read and write pgsql_tmp and all files created within it
chmod 2770 $PGDATA/base/pgsql_tmp

# Make sure pgduck can execute pgduck_server binary, has credentials set up

# Run pgduck_server as pgduck, set postgres as the unix socket group owner
sudo su pgduck -c "pgduck_server --unix_socket_group postgres --duckdb_database_file_path /cache/duckdb.db --cache_dir /cache/files"
```

Verify whether COPY in Parquet format to/from stdout/stdin and S3 all work.


## Regression tests

First, let's get ready for running the tests.

### Test Prerequisites
You need to follow below instructions to successfully run all tests locally:

- You need to install `pipenv` with >= python3.11 to run tests. Be careful to install correct python version if not exists e.g. ```apt install python3.11```. Then you should make sure you use it while creating pipenv environment. (e.g. ```pipenv --python 3.11```)
- You need to have `pgaudit` extension installed
- You need to install `jdk11` and `jdbc driver for Postgres`, then export `JDBC_DRIVER_PATH`. (required to run tests where we verify pg_lake_iceberg table results on spark)
- You need to have JAVA 21 (or higher) installed to run tests with Polaris catalog

Build PostgreSQL from source:

```bash
git clone git@github.com:postgres/postgres.git -b REL_18_STABLE
cd postgres
./configure --enable-injection-point --enable-tap-tests --with-llvm --enable-debug --enable-cassert --enable-depend CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer" --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4 --with-python --prefix=$HOME/pgsql/18/
make -j 16 && make install

# install relevant test packages
make -C src/test/modules/injection_points install
make -C src/test/isolation install

# use the same prefix as Postgres' configure
# add isolation-tester to the PATH
export PATH=$HOME/pgsql/18/lib/pgxs/src/test/isolation:$PATH
```

Build PostGIS (dependency of pg_lake_spatial):

```bash
git clone git@github.com:postgis/postgis.git
cd postgis
./autogen.sh
./configure
make -j 16
sudo make install
```

Build pgaudit (used in test suite):

```bash
git clone git@github.com:pgaudit/pgaudit.git
cd pgaudit
make USE_PGXS=1 install
``` 

Azure tests use azurite, which needs to be installed via npm.

```bash
# On Ubuntu
sudo apt-get -y install nodejs npm

# On RHEL
sudo yum install -y nodejs npm

# Afterwards
npm install -g azurite
```

### Running tests


We primarily use pytest for regression tests. You first need to install the required packages via pipenv: 

```bash
pipenv install --dev
```

You can then run tests using:

```bash
make check
```

You can also run installcheck locally using:

```bash
pgduck_server/pgduck_server --init_file_path pgduck_server/tests/test_secrets.sql --cache_dir /tmp/cache &
make installcheck
```

Note that there are several PostgreSQL settings that may affect the installcheck result (e.g. timezone, `pg_lake_iceberg.default_location_prefix`).


### Running PostgreSQL tests with our extensions

We also run `postgres installcheck` with our extensions created. That makes us sure that we do not break regular Postgres behavior. The make target for it is `installcheck-postgres`. You need to `export PG_REGRESS_DIR=<pg_src_dir>/test/regress` and set some GUCS before running the tests as shown below:

```sql
postgres> ALTER SYSTEM SET compute_query_id='regress';
postgres> ALTER SYSTEM SET pg_lake_table.hide_objects_created_by_lake=true;
postgres> SELECT pg_reload_conf();

-- runs postgres tests with our extensions in shared_preload_libraries
make installcheck-postgres PG_REGRESS_DIR=<pg_src_dir>/test/regress

-- runs postgres tests with our extensions created
make installcheck-postgres-with_extensions_created PG_REGRESS_DIR=<pg_src_dir>/test/regress
```

### Pytests

We have so far avoided regular SQL regression tests. The reason is that we found those painful to maintain in past projects (read: `Citus`) because of the lack of programmatic testing and the relative rigour with which we write tests. We would often repeat the same patterns over and over again, and later find that we had non-deterministic output (e.g. unordered SELECT results) or output that was sensitive to small implementation details and other issues in many places, and it significantly slowed down development. Another reason for using pytest is that it's commonly used and therefore generative AI tools are quite proficient at writing tests.


## Running `S3` Compatible Service `minio` Locally

`pg_lake` heavily relies on `S3`. However, using `S3` can introduce significant latency, especially problematic in local testing environments. To mitigate this, you can use `minio` for local `S3`-compatible service setup.

### Installation and Setup of `minio`


1. **Install `minio`**:

   ```bash
   brew install minio
   ```

**Note**: For other systems, you can download `minio` binaries from the [official website](https://min.io/download).

2. **Start the `minio` Server**:
   ```bash
   minio server start
   ```

3. **Access `minio` UI**:
   Open your browser and go to [http://localhost:9000/](http://localhost:9000/).

4. **Create Access and Secret Keys from the minio UI**:
We use the following for simplicity:

   - Access Key: `testkey`
   - Secret Key: `testpassword`

5. **Add `minio` Profile to `~/.aws/config`**:
   ```ini
   [services testing-minio]
   s3 =
      endpoint_url = http://localhost:9000

   [profile minio]
   region = us-east-1
   services = testing-minio
   ```

6. **Create a Bucket in `minio` UI**:
   - Such as: `localbucket`. We use this bucket name in the rest of the steps.

7. **Connect to `pgduck_server` and create the relevant `SECRET` on `DuckDB`**:
   ```sql
   psql -p 5332 -h /tmp
   ```

   ```sql
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

8. **Create a Foreign Table that uses the `localbucket` in `minio`**:
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

   - Finds all folders containing a `.control` file.
   - Updates the `default_version` field in each control file.
   - Creates a new SQL stub for version upgrade (e.g., `pg_lake_engine--2.4--3.0.sql`).


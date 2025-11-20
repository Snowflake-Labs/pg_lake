# pg_lake Local Development Guide

Complete guide for building, running, and developing pg_lake locally.

## 🚀 Quick Start (3 Steps)

### 1. Prerequisites

- Docker Desktop (with Docker Compose)
  - **Minimum**: 8GB RAM allocated to Docker
  - **Recommended**: 16GB RAM allocated to Docker (required for pgduck_server compilation)
  - Configure in: Docker Desktop → Settings → Resources → Memory
- [Task](https://taskfile.dev/installation/) - Task runner

```bash
# macOS
brew install go-task

# Linux
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
```

**Note**: Tasks run in silent mode by default for cleaner output. Use `task -v <task-name>` to see verbose output when debugging.

### 2. Available Variables

You can customize builds using these variables:

| Variable | Default | Description | Example |
|----------|---------|-------------|---------|
| `PG_MAJOR` | `18` | PostgreSQL major version (16, 17, or 18) | `PG_MAJOR=17` |
| `BASE_IMAGE_OS` | `almalinux` | Base OS (almalinux or debian) | `BASE_IMAGE_OS=debian` |
| `BASE_IMAGE_TAG` | `9` | Base OS version tag | `BASE_IMAGE_TAG=12` |
| `VERSION` | `latest` | Image version tag (for registry) | `VERSION=v1.0.0` |

### 3. Build and Start Everything

```bash
cd docker

# Build images and start all services (default: PostgreSQL 18)
task compose:up

# Build images and start all services for PostgreSQL 17
task compose:up PG_MAJOR=17

# Build images and start all services for PostgreSQL 16
task compose:up PG_MAJOR=16

# Build with Debian base OS
task compose:up BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12

# Build PostgreSQL 17 with Debian
task compose:up PG_MAJOR=17 BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12
```

This single command will:

- Build `pg_lake:local` and `pgduck-server:local` images for your architecture
- Start PostgreSQL with pg_lake extensions
- Start pgduck_server (DuckDB integration)
- Start LocalStack (S3-compatible storage)

### 4. Connect and Test

```bash
# Connect to PostgreSQL from your host
psql -h localhost -p 5432 -U postgres

# Create a test Iceberg table
CREATE TABLE test(id int, name text) USING iceberg;

# Insert some data
INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');

# Query it
SELECT * FROM test;
```

**Verify Iceberg files in S3:**

```bash
# View the Iceberg table files stored in LocalStack S3 (no AWS CLI needed!)
task s3:list

# You should see files like:
# 📦 S3 Bucket Contents (s3://testbucket/pg_lake/):
#
# ├── data_0.parquet
# ├── 00000-6f561147-24ab-449d-922a-713d6adbb4ff.metadata.json
# ├── 00001-bf29575f-3fbd-4fe0-96c7-8666706d4625.metadata.json
# ├── 9f6a9c61-76ab-49ed-b336-3a27e786d1e4-m0.avro
# ├── snap-745562050065240723-1-9f6a9c61-76ab-49ed-b336-3a27e786d1e4.avro
```

---

## 📋 Common Tasks

### Docker Compose Management

```bash
# Start everything (builds if needed, default PG 18)
task compose:up

# Start with specific PostgreSQL version
task compose:up PG_MAJOR=17

# Stop all services
task compose:down

# Stop services and remove volumes (complete cleanup)
task compose:teardown

# Restart services (use same PG_MAJOR as when started)
task compose:restart PG_MAJOR=17

# View logs (all services)
task compose:logs

# View logs (specific service)
task compose:logs SERVICE=pg_lake-postgres
task compose:logs SERVICE=pgduck-server

# Debug mode (verbose output)
task -v compose:up PG_MAJOR=17
```

### Build Management

```bash
# Build images for local docker-compose
task build:local

# Build with specific PostgreSQL version
task build:local PG_MAJOR=17
task build:local PG_MAJOR=16

# Build with different base OS
task build:local BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12

# Rebuild after code changes
task build:local
task compose:restart
```

### Image Management

```bash
# List images with architecture
task images:list

# Clean up images
task images:clean
```

### S3 / LocalStack

```bash
# View S3 bucket contents (Iceberg files)
task s3:list
```

---

## 🔌 Connecting to Databases

### PostgreSQL (from host)

PostgreSQL is exposed on port 5432 and accessible from your host machine:

```bash
# Using psql from host
psql -h localhost -p 5432 -U postgres

# Or with docker-compose
docker-compose exec pg_lake-postgres psql -U postgres
```

### DuckDB via pgduck_server

**Important**: `pgduck_server` only listens on Unix sockets (not TCP), so you cannot connect directly from the host.

The `pg_lake-postgres` container shares the Unix socket with `pgduck_server`:

```bash
# Connect via Unix socket from pg_lake container
docker exec -it pg_lake psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres

# Or exec into container first
docker exec -it pg_lake bash
psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres

# Test DuckDB version
docker exec -it pg_lake psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres -c "select version() as duckdb_version;"
```

Should show something like:

```sql
 duckdb_version
----------------
 v1.3.2
(1 row)
```

### Connection Architecture

```text
Host Machine
    │
    ├─► Port 5432 (TCP) ───► pg_lake-postgres container
    │                              │
    │                              └─► Unix Socket ───► pgduck-server container
    │
    └─► Cannot connect directly to pgduck-server (Unix socket only)
```

Both containers share:

- `pgduck-unix-socket-volume` - Unix socket for PostgreSQL protocol communication
- `pg-shared-tmp-dir-volume` - Temporary files for data exchange

---

## ⚙️ Configuration

### Default Configuration

| Setting | Default Value |
|---------|---------------|
| PostgreSQL Version | 18 |
| Base OS | AlmaLinux 9 |
| Architecture | Your system's architecture (auto-detected) |

### Change PostgreSQL Version

```bash
# Recommended: Use compose:up (handles both build and start)
task compose:up PG_MAJOR=17

# Or manually: Build with PostgreSQL 17 then start
task build:local PG_MAJOR=17
PG_MAJOR=17 docker-compose up -d

# Build with PostgreSQL 16
task compose:up PG_MAJOR=16
```

### Environment Variables

Create a `.env` file in the `docker` directory:

```env
# PostgreSQL Version (16, 17, or 18)
PG_MAJOR=18

# AWS Profile for LocalStack (optional)
AWS_PROFILE=localstack
```

---

## 🔧 AWS CLI with LocalStack (Optional)

By default, use `task s3:list` to view S3 contents (no AWS CLI installation needed).

If you prefer using AWS CLI directly from your host, configure a LocalStack profile:

### Setup AWS Profile for LocalStack

**1. Create/Update `~/.aws/config`:**

```ini
[profile localstack]
region = us-east-1
output = json
endpoint_url = http://localhost:4566
```

**2. Create/Update `~/.aws/credentials`:**

```ini
[localstack]
aws_access_key_id = test
aws_secret_access_key = test
```

### Usage

```bash
# List S3 buckets
aws --profile localstack s3 ls

# List bucket contents
aws --profile localstack s3 ls s3://testbucket/pg_lake/ --recursive

# Upload/download files
aws --profile localstack s3 cp myfile.txt s3://testbucket/

# Set as default for current session
export AWS_PROFILE=localstack
aws s3 ls
```

**Note**: The `task s3:list` command uses `docker exec` internally, so it works without any AWS CLI setup on your host.

---

## 🐛 Troubleshooting

### Build fails with memory error

If builds fail with out-of-memory errors (especially when building pgduck_server):

```bash
# Increase Docker Desktop memory:
# Docker Desktop → Settings → Resources → Memory
# Set to at least 16GB for pgduck_server compilation (DuckDB is memory-intensive)

# On macOS/Linux, check system memory
# macOS:
sysctl hw.memsize

# Linux:
free -h

# Alternative: Build images separately to reduce peak memory usage
cd docker  # Make sure you're in the docker directory

# Build pg_lake first
docker buildx build --target pg_lake_postgres --load -t pg_lake:local -f Dockerfile .

# Then build pgduck_server
docker buildx build --target pgduck_server --load -t pgduck-server:local -f Dockerfile .
```

### Images not found when starting docker-compose

```bash
# Make sure you've built the images first
task build:local

# Verify images exist
docker images | grep "pg_lake\|pgduck-server"

# Or list all pg_lake images with details
task images:list
```

### Services won't start

```bash
# Check logs
task compose:logs

# Check container status
docker-compose ps

# Check if ports are already in use
lsof -i :5432  # PostgreSQL port
lsof -i :4566  # LocalStack port
```

### "Cannot open file" errors when creating Iceberg tables

If you see errors like:

```text
ERROR: IO Error: Cannot open file "/home/postgres/pgsql-18/data/base/pgsql_tmp/pgsql_tmp.pg_lake_iceberg_XXX.0": No such file or directory
```

This means the temp directory volume isn't properly shared between containers:

```bash
# Stop containers and remove volumes
task compose:teardown

# Verify docker-compose.yml uses pg-shared-tmp-dir-volume for both containers
grep pg-shared-tmp-dir-volume docker-compose.yml

# Restart services
task compose:up
```

### Need to rebuild from scratch

```bash
# Stop and remove everything (including volumes)
task compose:teardown

# Rebuild
task build:local

# Start fresh
task compose:up
```

### Clean up Docker buildx

```bash
# If builds are failing, reset buildx
task clean
task setup
task build:local
```

---

## 💻 Development Workflows

### Making changes to Dockerfile

```bash
# 1. Edit Dockerfile
vim Dockerfile

# 2. Rebuild images (specify PG version if not using default)
task build:local PG_MAJOR=17

# 3. Restart services with new images
task compose:restart PG_MAJOR=17

# Or rebuild and restart in one command
task compose:up PG_MAJOR=17

# 4. Check logs
task compose:logs
```

### Testing different configurations

```bash
# Test with Debian base (instead of default AlmaLinux)
task build:local BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12
docker-compose up -d

# Note: For testing different PostgreSQL versions, see "Change PostgreSQL Version" 
# section in Configuration above
```

### Using Direct Docker Commands

After building with Task, you can use standard docker-compose commands:

```bash
cd docker

# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Check status
docker-compose ps

# Execute commands in containers
docker-compose exec pg_lake-postgres psql -U postgres
docker-compose exec pg_lake-postgres psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres
```

---

## 📊 Build Details

### What gets built?

- **pg_lake:local** - PostgreSQL with pg_lake extensions
- **pgduck-server:local** - pgduck server with DuckDB integration

---

## 💡 Tips

- **Always specify `PG_MAJOR`** when working with non-default PostgreSQL versions:

  ```bash
  task compose:up PG_MAJOR=17
  task compose:down PG_MAJOR=17
  task compose:logs PG_MAJOR=17
  ```

- Images are tagged as `pg_lake:local-pg{VERSION}` and `pgduck-server:local-pg{VERSION}`
  - Default: `pg_lake:local-pg18`, `pgduck-server:local-pg18`
  - PG 17: `pg_lake:local-pg17`, `pgduck-server:local-pg17`

- For publishing images to registries, see [TASKFILE.md](./TASKFILE.md)

- Use `task -v <command>` for verbose output when debugging

- Container names vs service names:
  - Service name: `pg_lake-postgres` (use with `docker-compose exec`)
  - Container name: `pg_lake` (use with `docker exec`)

- Each PostgreSQL version uses its own buildx builder for isolated caching:
  - PG 16: `pg_lake_builder_pg16`
  - PG 17: `pg_lake_builder_pg17`
  - PG 18: `pg_lake_builder_pg18`

---

## 📚 Additional Resources

- [TASKFILE.md](./TASKFILE.md) - Complete Task reference with publishing workflows
- [Dockerfile](./Dockerfile) - Image build configuration
- [docker-compose.yml](./docker-compose.yml) - Service configuration
- [README.md](./README.md) - Architecture overview and optimizations

# Local Development with Task

Simple guide for building and running pg_lake locally.

## Prerequisites

- Docker Desktop with at least **16GB RAM** allocated (pgduck_server/DuckDB compilation is memory-intensive)
  - Configure: Docker Desktop → Settings → Resources → Memory
- [Task](https://taskfile.dev/installation/) runner

## Quick Start (3 steps)

### 1. Install Task

```bash
# macOS
brew install go-task

# Linux
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
```

### 2. Build & Start Services

```bash
cd docker

# Build images and start all services
task compose:up
```

This will:

- Build `pg_lake:local` and `pgduck-server:local` images
- Start all services with docker-compose
- Services will be available on default ports

### 3. View Logs

```bash
# View all logs
task compose:logs

# Or use docker-compose directly
docker-compose logs -f pg_lake-postgres
docker-compose logs -f pgduck-server
```

## Connecting to Databases

### Connect to PostgreSQL (from host)

PostgreSQL is exposed on port 5432 and accessible from your host machine:

```bash
# Using psql from host
psql -h localhost -p 5432 -U postgres

# Create an Iceberg table
psql -h localhost -p 5432 -U postgres -c "CREATE TABLE test(id int, name text) USING iceberg;"
```

### Connect to DuckDB via pgduck_server

**Important**: `pgduck_server` only listens on Unix sockets (not TCP), so you cannot connect directly from the host.

The `pg_lake-postgres` container shares the Unix socket with `pgduck_server`:

```bash
# Exec into the pg_lake container
docker exec -it pg_lake bash

# Connect to pgduck_server via Unix socket
psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres

# Or directly from host using docker exec
docker exec -it pg_lake psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres
```

### Connection Architecture

```mermaid
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

## Available Commands

**Note**: All tasks run in silent mode by default for clean output. Add `-v` flag to see verbose output for debugging: `task -v <task-name>`

### Docker Compose Management

```bash
# Start everything (builds if needed)
task compose:up

# Stop all services
task compose:down

# Stop services and remove volumes (complete cleanup)
task compose:teardown

# Restart services
task compose:restart

# View logs
task compose:logs

# Debug mode (verbose output)
task -v compose:up
```

### Build Only (without starting)

```bash
# Build images for local docker-compose
task build:local

# Build with specific PostgreSQL version
task build:local PG_MAJOR=17

# Build with different base OS
task build:local BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12
```

### Direct Docker Commands

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

# Execute commands in containers (see "Connecting to Databases" section for more details)
docker-compose exec pg_lake-postgres psql -U postgres
docker-compose exec pg_lake-postgres psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres
```

## Build Details

### What gets built?

- **pg_lake:local** - PostgreSQL with pg_lake extensions
- **pgduck-server:local** - pgduck server with DuckDB integration

### Default Configuration

| Setting | Default Value |
|---------|---------------|
| PostgreSQL Version | 18 |
| Base OS | AlmaLinux 9 |
| Architecture | Your system's architecture (auto-detected) |

### Change PostgreSQL Version

```bash
# Build with PostgreSQL 17
task build:local PG_MAJOR=17
PG_MAJOR=17 docker-compose up -d

# Build with PostgreSQL 16
task build:local PG_MAJOR=16
PG_MAJOR=16 docker-compose up -d
```

## Environment Variables

## Using AWS CLI with LocalStack (Optional)

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

Create a `.env` file in the `docker` directory:

```env
# PostgreSQL Version (16, 17, or 18)
PG_MAJOR=18

# AWS Profile for LocalStack
AWS_PROFILE=localstack
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

## Troubleshooting

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
# Build pg_lake first
docker buildx build --target pg_lake_postgres --load -t pg_lake:local .

# Then build pgduck_server
docker buildx build --target pgduck_server --load -t pgduck-server:local .
```

### Images not found when starting docker-compose

```bash
# Make sure you've built the images first
task build:local

# Verify images exist
docker images | grep "pg_lake\|pgduck-server"
```

### Services won't start

```bash
# Check logs
task compose:logs

# Check if ports are already in use
docker-compose ps
lsof -i :5432  # PostgreSQL port
lsof -i :4566  # LocalStack port
```

### "Cannot open file" errors when creating Iceberg tables

If you see errors like:

```
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

## Development

### Making changes to Dockerfile

```bash
# 1. Edit Dockerfile
vim Dockerfile

# 2. Rebuild images
task build:local

# 3. Restart services with new images
docker-compose up -d

# 4. Check logs
task compose:logs
```

### Testing different configurations

```bash
# Test PostgreSQL 17
task build:local PG_MAJOR=17
PG_MAJOR=17 docker-compose up -d

# Test with Debian base
task build:local BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12
docker-compose up -d
```

## Image Sizes

After optimization, expect these approximate sizes:

- **pg_lake:local** - ~1.2GB (down from ~4GB)
- **pgduck-server:local** - ~800MB (down from ~3GB)

## Additional Resources

- [Taskfile Documentation](./TASKFILE.md) - Full Task reference
- [Docker Compose Docs](https://docs.docker.com/compose/)
- [Dockerfile](./Dockerfile) - Build configuration

## Common Tasks Reference

```bash
# Development cycle
task compose:up          # Build + Start
task compose:logs        # View logs
task compose:restart     # Restart services
task compose:down        # Stop services

# Build only
task build:local         # Build for local use
task build:local PG_MAJOR=17  # Specific version

# Cleanup
task clean               # Remove buildx builder
docker-compose down -v   # Remove all containers and volumes
```

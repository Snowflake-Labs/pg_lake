# Quick Start Guide

Get pg_lake running locally in 3 steps!

## Prerequisites

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

## 🚀 Quick Start (3 Steps)

### 1. Build and Start Everything

```bash
cd docker

# Build images and start all services
task compose:up
```

This single command will:

- Build `pg_lake:local` and `pgduck-server:local` images for your architecture
- Start PostgreSQL with pg_lake extensions
- Start pgduck_server (DuckDB integration)
- Start LocalStack (S3-compatible storage)

### 2. Connect and Test

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

# You should see files like (filenames may vary):
# 📦 S3 Bucket Contents (s3://testbucket/pg_lake/):
#
# │   │   │   │   │   │   ├── data_0.parquet
# │   │   │   │   │   ├── 00000-6f561147-24ab-449d-922a-713d6adbb4ff.metadata.json
# │   │   │   │   │   ├── 00001-bf29575f-3fbd-4fe0-96c7-8666706d4625.metadata.json
# │   │   │   │   │   ├── 9f6a9c61-76ab-49ed-b336-3a27e786d1e4-m0.avro
# │   │   │   │   │   ├── snap-745562050065240723-1-9f6a9c61-76ab-49ed-b336-3a27e786d1e4.avro

```

### 3. View Logs

```bash
# View all service logs
task compose:logs

# Or individual services
task compose:logs SERVICE=pg_lake-postgres
task compose:logs SERVICE=pgduck-server
```

## 🔧 Common Tasks

```bash
# Stop services
task compose:down

# Stop services and remove volumes (complete cleanup)
task compose:teardown

# Restart services
task compose:restart

# Rebuild after code changes
task build:local
task compose:restart

# List images with architecture
task images:list

# Clean up images
task images:clean

# View S3 bucket contents (Iceberg files)
task s3:list
```

## 🔌 Connecting to Databases

### PostgreSQL (from host)

```bash
psql -h localhost -p 5432 -U postgres
```

### DuckDB via pgduck_server (from host)

```bash
# pgduck_server only uses Unix sockets, so exec into container
docker exec -it pg_lake psql -h /home/postgres/pgduck_socket_dir -p 5332 -U postgres -c "select version() as duckdb_version;"
```

Should show something like:

```
 duckdb_version
----------------
 v1.3.2
(1 row)
```

## 🐛 Troubleshooting

### Services won't start

```bash
# Check if ports are already in use
lsof -i :5432  # PostgreSQL
lsof -i :4566  # LocalStack

# Check logs
task compose:logs
```

### Out of memory

If builds fail with memory errors (especially pgduck_server):

```bash
# Increase Docker Desktop memory:
# Docker Desktop → Settings → Resources → Memory
# Minimum: 8GB, Recommended: 16GB for pgduck_server compilation

# On Linux, check available memory
free -h

# If memory errors persist, try building one image at a time
docker buildx build --target pg_lake_postgres ... 
# then
docker buildx build --target pgduck_server ...
```

### Iceberg table creation fails

```bash
# If you see "Cannot open file" errors, restart with clean volumes
task compose:teardown
task compose:up
```

### Need to rebuild from scratch

```bash
# Stop everything and clean up (removes volumes)
task compose:teardown

# Rebuild
task build:local
task compose:up
```

## 📚 Next Steps

Once you're up and running:

- **[LOCAL_DEV.md](./LOCAL_DEV.md)** - Detailed local development guide
- **[TASKFILE.md](./TASKFILE.md)** - Advanced build options, multi-platform builds, registry pushes
- **[Dockerfile](./Dockerfile)** - Image build configuration
- **[docker-compose.yml](./docker-compose.yml)** - Service configuration

## 💡 Tips

- Default PostgreSQL version is 18. To use a different version:

  ```bash
  task build:local PG_MAJOR=17
  PG_MAJOR=17 docker-compose up -d
  ```

- Images are tagged as `pg_lake:local` and `pgduck-server:local`

- For publishing images to registries, see [TASKFILE.md](./TASKFILE.md)

### Using AWS CLI with LocalStack (Optional)

If you want to use AWS CLI directly from your host instead of `task s3:list`, configure a LocalStack profile:

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

**3. Use AWS CLI with LocalStack:**

```bash
# List S3 buckets
aws --profile localstack s3 ls

# List bucket contents
aws --profile localstack s3 ls s3://testbucket/pg_lake/ --recursive

# Or set as default profile
export AWS_PROFILE=localstack
aws s3 ls s3://testbucket/
```

# Docker Build Tasks

This directory contains a Taskfile for building and pushing multi-platform Docker images.

## Prerequisites

1. **Install Task**: <https://taskfile.dev/installation/>

   ```bash
   # macOS
   brew install go-task
   
   # Linux
   sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
   ```

2. **Docker with buildx** (included in Docker Desktop)

3. **Docker Memory Requirements**:
   - **Minimum**: 8GB RAM allocated to Docker
   - **Recommended**: 16GB RAM for building pgduck_server
   - **Why**: DuckDB compilation in pgduck_server is very memory-intensive
   - **Configure**: Docker Desktop → Settings → Resources → Memory

## Build Architecture

This Taskfile uses **per-version buildx builders** to prevent cache conflicts:

- **PG 16**: Uses `pg_lake_builder_pg16`
- **PG 17**: Uses `pg_lake_builder_pg17`
- **PG 18**: Uses `pg_lake_builder_pg18`

Each builder maintains its own isolated cache, allowing you to build and switch between PostgreSQL versions without cache conflicts. Builders are created automatically on first use.

### Cache Behavior Notes

**Single-platform builds** (e.g., `build:local`):
- Uses `--load` to load images directly into Docker
- Cache is stored per-builder instance
- Faster for local development
- Images immediately available in `docker images`

**Multi-platform builds** (e.g., `build:all`, `build:pg-lake-postgres`):
- Builds for multiple architectures (amd64, arm64)
- **Without PUSH=true**: Builds to cache only (images not loaded locally)
- **With PUSH=true**: Builds and pushes to registry
- Cannot use `--load` with multiple platforms (Docker limitation)
- Cache is shared across architectures within the same builder

**Important**: 
- Use `task build:local` for local development (single platform, loads to Docker)
- Use `task build:all` for testing multi-arch builds (cache only, verifies build works)
- Use `task push:all` to build and publish to registry (multi-arch, available for deployment)

## Available Tasks

Run `task --list` to see all available tasks:

```bash
cd docker
task --list
```

**Note**: All tasks run in silent mode by default for cleaner output. Use the `-v` flag when you need verbose output for debugging:

```bash
# Normal (silent) mode
task build:local

# Verbose mode (for debugging)
task -v build:local
```

## Common Usage

### Quick Start - Local Development

The fastest way to get started:

```bash
# Build images and start services with docker-compose
task compose:up

# View logs (all services)
task compose:logs

# View logs for specific service
task compose:logs SERVICE=pgduck-server

# Stop services
task compose:down
```

See [LOCAL_DEV.md](./LOCAL_DEV.md) for detailed local development guide.

### Build Images Locally

```bash
# Build for local docker-compose (detects your architecture automatically)
task build:local

# Build with specific PostgreSQL version
task build:local PG_MAJOR=17

# List all built images with architecture
task images:list

# Clean up local images
task images:clean

# View S3 bucket contents (verify Iceberg files)
task s3:list
```

### Build Images for Registry

```bash
# Build pg_lake_postgres for registry (multi-platform)
task build:pg-lake-postgres

# Build pgduck_server for registry
task build:pgduck-server

# Build both images (multi-platform, builds to cache only - doesn't push or load)
task build:all

# Build for specific PostgreSQL version
task build:all PG_MAJOR=17

# Note: Multi-platform builds without PUSH=true only populate the build cache.
# Images won't be available locally or in registry until you push them.
```

### Build Multi-Platform Images

```bash
# Build for multiple platforms (amd64 + arm64)
task build:all PLATFORMS="linux/amd64,linux/arm64"

# Build all PostgreSQL versions (16, 17, 18)
task build:all-pg-versions
```

### Push to Registry

```bash
# Login to Docker Hub
export DOCKER_HUB_TOKEN=your_token
export DOCKER_HUB_USERNAME=your_username
task login:dockerhub

# Or login to GitHub Container Registry
export GITHUB_TOKEN=your_token
export GITHUB_ACTOR=your_github_username
task login:ghcr

# Push single image
task push:pg-lake-postgres VERSION=v3.1.0

# Push all images
task push:all VERSION=v3.1.0

# Push all images for all PostgreSQL versions
task push:all-pg-versions VERSION=v3.1.0
```

### Docker Compose Commands

```bash
# Build and start all services
task compose:up

# Stop services
task compose:down

# Stop services and remove volumes (complete cleanup)
task compose:teardown

# View logs (all services or specific SERVICE)
task compose:logs
task compose:logs SERVICE=pg_lake-postgres

# Restart services
task compose:restart
```

### Cache Management

Each PostgreSQL version (16, 17, 18) uses its own isolated buildx builder to prevent cache conflicts:

```bash
# Clear cache for specific PostgreSQL version
task clean:cache-version PG_MAJOR=17

# Clear all buildx caches (all versions)
task clean:cache

# Remove builder for specific version
task clean:builder PG_MAJOR=17

# Remove all builders (all PG versions)
task clean

# Nuclear option: remove everything (builders + caches + local images)
task clean:all
```

**When to use each:**
- `clean:cache-version` - Rebuild one PG version from scratch
- `clean:cache` - Clear all caches but keep builders
- `clean:builder` - Remove a specific version's builder
- `clean` - Remove all builders (they'll be recreated on next build)
- `clean:all` - Complete cleanup when you want to start fresh

### Image Management

```bash
# List all pg_lake images with architecture
task images:list

# Clean up local images
task images:clean
```

### Test Images

```bash
# Test locally built images
task test:pg-lake-postgres
task test:pgduck-server
```

## Environment Variables

You can customize builds using these variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REGISTRY` | `docker.io` | Container registry (docker.io, ghcr.io, etc.) |
| `IMAGE_OWNER` | `${DOCKER_HUB_USERNAME}` | Registry username/org |
| `VERSION` | `latest` | Image version tag |
| `PG_MAJOR` | `18` | PostgreSQL major version (16, 17, or 18) |
| `PLATFORMS` | `linux/amd64,linux/arm64` | Target platforms for registry builds |
| `BASE_IMAGE_OS` | `almalinux` | Base OS (almalinux or debian) |
| `BASE_IMAGE_TAG` | `9` | Base OS version |

**Note**: `build:local` automatically detects your system architecture (amd64 or arm64) and builds for that platform only.

### Authentication Environment Variables

For pushing to registries, set these environment variables:

**Docker Hub:**

- `DOCKER_HUB_TOKEN` - Docker Hub access token
- `DOCKER_HUB_USERNAME` - Docker Hub username

**GitHub Container Registry:**

- `GITHUB_TOKEN` - GitHub personal access token with `write:packages` scope
- `GITHUB_ACTOR` - GitHub username

### Example with Custom Variables

```bash
# Build PostgreSQL 17 image for amd64 only
task build:pg-lake-postgres \
  PG_MAJOR=17 \
  PLATFORMS=linux/amd64 \
  VERSION=v3.1.0-rc1

# Push to custom registry
task push:all \
  REGISTRY=docker.io \
  IMAGE_OWNER=myusername \
  VERSION=v3.1.0
```

## GitHub Container Registry (GHCR) Authentication

To push images to ghcr.io, you need to authenticate:

```bash
# Set environment variables
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=your-github-token

# Login to GHCR
task login:ghcr
```

## Image Tags

### Registry Images

Images pushed to registries are tagged as:

```
REGISTRY/IMAGE_OWNER/pg_lake:VERSION-pgMAJOR
REGISTRY/IMAGE_OWNER/pg_lake:VERSION
REGISTRY/IMAGE_OWNER/pgduck-server:VERSION-pgMAJOR
REGISTRY/IMAGE_OWNER/pgduck-server:VERSION
```

Examples:

```
docker.io/${DOCKER_HUB_USERNAME}/pg_lake:v3.1.0-pg18
docker.io/${DOCKER_HUB_USERNAME}/pg_lake:v3.1.0
docker.io/${DOCKER_HUB_USERNAME}/pg_lake:latest
docker.io/${DOCKER_HUB_USERNAME}/pgduck-server:v3.1.0-pg17
```

### Local Images

Images built with `task build:local` are tagged as:

```
pg_lake:local
pg_lake:local-pg18
pgduck-server:local
pgduck-server:local-pg18
```

## Pulling Images

```bash
# Pull from Docker Hub
docker pull docker.io/${DOCKER_HUB_USERNAME}/pg_lake:latest
docker pull docker.io/${DOCKER_HUB_USERNAME}/pgduck-server:latest

# Pull from GitHub Container Registry
docker pull ghcr.io/${DOCKER_HUB_USERNAME}/pg_lake:latest

# Pull specific version
docker pull docker.io/${DOCKER_HUB_USERNAME}/pg_lake:v3.1.0-pg18
```

## Using with Docker Compose

### Local Development

The included `docker-compose.yml` uses local images:

```yaml
services:
  pg_lake-postgres:
    image: pg_lake:local
    # ... rest of config

  pgduck-server:
    image: pgduck-server:local
    # ... rest of config
```

Build and start:

```bash
task compose:up
```

### Using Registry Images

To use published registry images, update your `docker-compose.yml`:

```yaml
services:
  pg_lake-postgres:
    image: docker.io/${DOCKER_HUB_USERNAME}/pg_lake:latest
    # ... rest of config

  pgduck-server:
    image: docker.io/${DOCKER_HUB_USERNAME}/pgduck-server:latest
    # ... rest of config
```

## Troubleshooting

### Build fails with network errors

The Taskfile includes retry logic for vcpkg downloads. If it still fails:

```bash
# Try building with network host mode
docker buildx build --network=host ...
```

### Out of memory during build

pgduck_server compilation (DuckDB) requires significant memory:

```bash
# Increase Docker Desktop memory:
# Settings → Resources → Memory → 16GB recommended

# Check if you have enough memory
# macOS:
sysctl hw.memsize

# Linux:
free -h

# Alternative: Build sequentially instead of parallel
# task build:local builds both images - if memory is tight, build separately:
docker buildx build --target pg_lake_postgres --platform linux/arm64 --load -t pg_lake:local .
docker buildx build --target pgduck_server --platform linux/arm64 --load -t pgduck-server:local .
```

### Multi-platform build issues

```bash
# Clean all buildx builders and recreate for specific PG version
task clean
task setup PG_MAJOR=17

# Or clean cache for specific version
task clean:cache-version PG_MAJOR=17
```

### Cache conflicts between PostgreSQL versions

If you get cache errors when switching between PostgreSQL versions:

```bash
# Clear cache for specific version
task clean:cache-version PG_MAJOR=17

# Or clear all caches
task clean:cache

# Nuclear option: remove everything and start fresh
task clean:all
```

**Note for multi-platform builds**: If building for multiple architectures and encountering cache issues, you may need to clear the specific builder's cache:

```bash
# Clear and rebuild for multi-platform
task clean:cache-version PG_MAJOR=17
task build:all PG_MAJOR=17 PUSH=false

# Or push directly to registry (recommended for multi-arch)
task push:all PG_MAJOR=17
```

### Authentication issues with registries

```bash
# For Docker Hub
export DOCKER_HUB_TOKEN=your_token_here
export DOCKER_HUB_USERNAME=your_username
task login:dockerhub

# For GitHub Container Registry
export GITHUB_TOKEN=ghp_your_token_here
export GITHUB_ACTOR=your_github_username
task login:ghcr
```

### Images not showing architecture

```bash
# List images with architecture info
task images:list
```

## Local Development

### Development & Testing

```bash
# 1. Make changes to Dockerfile or scripts

# 2. Build and test locally (fast, single platform)
task build:local

# 3. Start services with docker-compose
task compose:up

# 4. View logs and test
task compose:logs

# 5. Make changes and rebuild
task build:local
task compose:restart

# 6. List images to verify
task images:list
```

### Releasing Images

```bash
# 1. Build multi-platform for registry
task build:all VERSION=v3.1.0-rc1

# 2. Push to registry
task login:dockerhub
task push:all VERSION=v3.1.0-rc1

# 3. Build and push all PostgreSQL versions
task push:all-pg-versions VERSION=v3.1.0
```

## Task Reference

### Build Tasks

- `build:local` - Build images for local development (auto-detects architecture, loads to Docker)
- `build:pg-lake-postgres` - Build pg_lake_postgres for registry (multi-platform, cache only unless PUSH=true)
- `build:pgduck-server` - Build pgduck_server for registry (multi-platform, cache only unless PUSH=true)
- `build:all` - Build all images (multi-platform, cache only unless PUSH=true)
- `build:all-pg-versions` - Build for PostgreSQL 16, 17, and 18

### Push Tasks

- `push:pg-lake-postgres` - Build and push pg_lake_postgres
- `push:pgduck-server` - Build and push pgduck_server
- `push:all` - Build and push all images
- `push:all-pg-versions` - Build and push all PostgreSQL versions

### Docker Compose Tasks

- `compose:up` - Build and start all services
- `compose:down` - Stop and remove services
- `compose:teardown` - Stop services and remove volumes (complete cleanup)
- `compose:logs` - View service logs (optionally specify `SERVICE=<service-name>`)
- `compose:restart` - Restart services

### Image Management Tasks

- `images:list` - List all pg_lake images with architecture
- `images:clean` - Remove all local pg_lake images

### S3 Tasks

- `s3:list` - List S3 bucket contents (LocalStack) in tree format

### Clean Tasks

- `clean` - Remove all buildx builders (PG 16, 17, 18)
- `clean:builder` - Remove buildx builder for specific PG_MAJOR version
- `clean:cache` - Clean buildx cache for all builders
- `clean:cache-version` - Clean buildx cache for specific PG_MAJOR version
- `clean:all` - Remove all builders, caches, and local images (complete cleanup)

### Utility Tasks

- `setup` - Setup Docker buildx builder for specific PG_MAJOR version
- `login:dockerhub` - Login to Docker Hub
- `login:ghcr` - Login to GitHub Container Registry

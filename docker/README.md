# pg_lake Docker Setup

Multi-stage optimized Docker builds for pg_lake with Task automation.

## 🚀 Quick Start

```bash
# Install Task (if needed)
brew install go-task  # macOS

# Build and start everything
cd docker
task compose:up

# View logs
task compose:logs
```

## 📁 What's Included

- **Dockerfile** - Optimized multi-stage build
  - Builds only one PostgreSQL version at a time (saves ~3x build time & space)
  - Separate builder stages for compilation
  - Minimal runtime images with only necessary binaries
  - Fixed vcpkg network issues with retry logic

- **Taskfile.yml** - Build automation
  - Local single-platform builds
  - Multi-platform builds for publishing
  - Docker Compose integration
  - Helper tasks for common operations

- **docker-compose.yml** - Local development stack
  - pg_lake-postgres (PostgreSQL with pg_lake extensions)
  - pgduck-server (DuckDB integration)
  - [localstack](https://localstack.cloud/) (S3-compatible storage)

## 📚 Documentation

- **[LOCAL_DEV.md](./LOCAL_DEV.md)** - Complete local development guide ⭐ **Start here**
- **[TASKFILE.md](./TASKFILE.md)** - Detailed Task documentation

## 🎯 Common Commands

```bash
# Development
task compose:up          # Build images and start services
task compose:logs        # View logs
task compose:down        # Stop services
task compose:teardown    # Stop services and remove volumes

# Building
task build:local         # Build for local use (fast, single platform)
task build:local PG_MAJOR=17  # Build PostgreSQL 17

# Debugging (verbose mode)
task -v compose:up       # Show all command output

# Testing
docker-compose exec pg_lake-postgres psql -U postgres
docker-compose exec pgduck-server psql -p 5332 -h /home/postgres/pgduck_socket_dir
```

**Note**: Tasks run in silent mode by default. Use `-v` flag for verbose output when debugging.

## 🏗️ Architecture

### Multi-Stage Build Flow

```
┌─────────────┐
│  dev_base   │  Build tools + PostgreSQL compilation
└──────┬──────┘
       │
       ├──────────────────┬──────────────────┐
       │                  │                  │
┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
│    base     │   │    base     │   │ runtime_base│
│ (pg_lake    │   │ (pg_lake    │   │  (minimal   │
│   source)   │   │   source)   │   │  runtime)   │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                  │                  │
┌──────▼──────┐   ┌──────▼──────┐          │
│ pg_lake_    │   │  pgduck_    │          │
│  builder    │   │  builder    │          │
│ (compile    │   │ (compile    │          │
│ extensions) │   │  pgduck)    │          │
└──────┬──────┘   └──────┬──────┘          │
       │                  │                  │
       └────────┬─────────┴─────────────────┘
                │
       ┌────────┴─────────┐
       │                  │
┌──────▼──────┐   ┌──────▼──────┐
│  pg_lake_   │   │  pgduck_    │
│  postgres   │   │  server     │
│  (FINAL)    │   │  (FINAL)    │
└─────────────┘   └─────────────┘
```

### Key Optimizations

✅ **Single PostgreSQL Version**: Builds only PG 16, 17, or 18 (not all 3)  
✅ **Separate Build Stages**: Compilation happens in builder stages  
✅ **Minimal Runtime**: Final images contain only binaries and libraries  
✅ **Network Retry Logic**: Handles vcpkg download failures  
✅ **Multi-Platform Support**: Can build for AMD64 and ARM64  

### Size Comparison

| Image | Before | After | Savings |
|-------|--------|-------|---------|
| pg_lake | ~4GB | ~1.2GB | **70%** |
| pgduck-server | ~3GB | ~800MB | **73%** |

## 🔧 Configuration

### PostgreSQL Version

```bash
# Default: PostgreSQL 18
task build:local

# PostgreSQL 17
task build:local PG_MAJOR=17

# PostgreSQL 16
task build:local PG_MAJOR=16
```

### Base OS

```bash
# Default: AlmaLinux 9
task build:local

# Debian 12
task build:local BASE_IMAGE_OS=debian BASE_IMAGE_TAG=12
```

### Multi-Platform

```bash
# Build for multiple architectures
task build:pg-lake-postgres PLATFORMS="linux/amd64,linux/arm64"

# Build for single architecture (faster)
task build:local  # Auto-detects your system

# Memory requirement: 16GB+ recommended for pgduck_server (DuckDB compilation)
```

## 📦 Publishing Images

### To Docker Hub (docker.io)

```bash
# Login
export DOCKER_HUB_TOKEN=your_token
export DOCKER_HUB_USERNAME=your_username
task login:dockerhub
```

## 🐛 Troubleshooting

### Build Failures

```bash
# Out of memory
# → Increase Docker memory to 8GB+ in Docker Desktop settings

# Network errors (vcpkg)
# → Retry logic is built-in, just run again
# → Check your internet connection

# Buildx issues
task clean
task setup
```

### Service Issues

```bash
# Services won't start
docker-compose ps          # Check status
task compose:logs          # Check logs
task compose:teardown      # Reset everything (removes volumes)
task compose:up            # Start fresh

# Port conflicts
lsof -i :5432  # PostgreSQL
lsof -i :4566  # LocalStack
```

## 📊 Health Checks

The docker-compose setup includes health checks:

- **pg_lake-postgres**: Creates and drops an Iceberg table
- **pgduck-server**: Executes `SELECT 1`
- **localstack**: HTTP health endpoint

Check status: `docker-compose ps`

## 🔐 Security Notes

- Images run as non-root user (UID 1001, postgres)
- Minimal runtime attack surface
- No build tools in final images
- Only necessary runtime libraries included

## 📖 Learn More

- [Dockerfile Multi-Stage Builds](https://docs.docker.com/build/building/multi-stage/)
- [Task Documentation](https://taskfile.dev/)
- [Docker Compose](https://docs.docker.com/compose/)
- [LocalStack](https://localstack.cloud/)

## 🤝 Contributing

When modifying the Dockerfile:

1. Test locally: `task build:local && task compose:up`
2. Verify image sizes: `docker images | grep pg_lake`
3. Test services work: `task compose:logs`
4. Update documentation if needed

## 📝 License

See [LICENSE](../LICENSE) file in the project root.

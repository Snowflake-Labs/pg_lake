#!/bin/bash

set -euo pipefail

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# Ensure PGBASEDIR and PG_MAJOR are set
PGBASEDIR=${PGBASEDIR:-/home/postgres}
PG_MAJOR=${PG_MAJOR:-18}

# Create and fix permissions for directories BEFORE starting pgduck_server
# Docker volumes are created with root ownership, but postgres user needs write access
mkdir -p ${PGBASEDIR}/pgduck_socket_dir
mkdir -p ${PGBASEDIR}/pgsql-${PG_MAJOR}/data/base/pgsql_tmp
sudo chown -R postgres:postgres ${PGBASEDIR}/pgduck_socket_dir
sudo chown -R postgres:postgres ${PGBASEDIR}/pgsql-${PG_MAJOR}/data/base/pgsql_tmp
sudo chmod 700 ${PGBASEDIR}/pgduck_socket_dir
sudo chmod 700 ${PGBASEDIR}/pgsql-${PG_MAJOR}/data/base/pgsql_tmp

# Start pgduck_server using the binary from the PostgreSQL bin directory
# NOTE: pgduck_server only listens on Unix sockets, not TCP
# The --port is used to create the socket file name (e.g., .s.PGSQL.5332)
# To connect from host, you need to access through the Unix socket or via the pg_lake-postgres container
${PGBASEDIR}/pgsql-${PG_MAJOR}/bin/pgduck_server \
  --cache_dir ~/cache \
  --unix_socket_directory ~/pgduck_socket_dir \
  --unix_socket_group postgres \
  --port 5332 \
  --init_file_path /init-pgduck-server.sql &
pgduck_server_pid=$!

wait $pgduck_server_pid

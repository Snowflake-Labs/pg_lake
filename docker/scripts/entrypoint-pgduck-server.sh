#!/bin/bash

set -euo pipefail

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM
# bind volumes have root permission at start (make this readable and writable by postgres)
sudo chown -R postgres:postgres /home/postgres/pgduck_socket_dir

# Start pgduck_server
pgduck_server --cache_dir ~/cache --unix_socket_directory ~/pgduck_socket_dir --unix_socket_group postgres --init_file_path /init-pgduck-server.sql &
pgduck_server_pid=$!

wait $pgduck_server_pid

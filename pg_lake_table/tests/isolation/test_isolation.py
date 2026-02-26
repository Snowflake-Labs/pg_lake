import os
import argparse
import psycopg2
import pytest
import subprocess

from utils_pytest import *


def test_isolation(s3, pgduck_server, isolationtester):

    if isolationtester != True:
        return

    generate_isolation_conf_file()

    pg_isolation_regress_dir = os.environ.get("PG_ISOLATION_REGRESS_DIR")

    pg_isolation_regress_command = [
        f"{pg_isolation_regress_dir}/pg_isolation_regress",
        f"--port={server_params.PG_ISOLATION_PORT}",
        f"--temp-instance={server_params.PG_ISOLATION_TMP_INSTANCE_PATH}",
        f"--temp-config={server_params.PG_ISOLATION_TMP_CONFIG_PATH}",
        f"--schedule={server_params.PG_ISOLATION_SCHEDULE_PATH}",
        f"--inputdir={server_params.PG_ISOLATION_INPUT_DIR}",
        f"--outputdir={server_params.PG_ISOLATION_OUTPUT_DIR}",
        f"--expecteddir={server_params.PG_ISOLATION_EXPECTED_DIR}",
    ]

    for extension in server_params.PG_ISOLATION_EXTENSION_LIST:
        pg_isolation_regress_command.append(f"--load-extension={extension}")

    proc = subprocess.Popen(
        pg_isolation_regress_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    stdout, _stderr = proc.communicate()
    exit_code = proc.wait()

    print(stdout.decode().replace("\\n", "\n"))

    if exit_code != 0:
        raise Exception(f"pg_isolation_regress failed with return code {exit_code}")


def generate_isolation_conf_file():

    lines = f"""
    shared_preload_libraries='pg_extension_base'
    pg_lake_iceberg.default_location_prefix = 's3://{TEST_BUCKET}/isolation'
    pg_lake_engine.host = 'host=/tmp port={server_params.PGDUCK_PORT}'
    listen_addresses = 'localhost'
    unix_socket_directories = '/tmp'
    port={server_params.PG_ISOLATION_PORT}
    """

    # Write the lines to the file
    with open(server_params.PG_ISOLATION_TMP_CONFIG_PATH, "w") as file:
        file.write(lines)

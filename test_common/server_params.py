import os
import subprocess

# these variables are dynamically adjusted
# see pgduck_server/tests/conftest.py
PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
PGDUCK_UNIX_DOMAIN_PERMISSIONS = "0700"
PGDUCK_PORT = 8257
DUCKDB_DATABASE_FILE_PATH = "/tmp/pytest_duckdb.db"
PGDUCK_CACHE_DIR = f"/tmp/cache.{PGDUCK_PORT}"
PGDUCK_PID_FILE = "/tmp/regress-pgduck-server.pid"


PG_DATABASE = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_PORT = "25778"
PG_HOST = "localhost"
PG_DIR = "/tmp/pg_lake_tests"
PG_READ_REPLICA_PORT = "25779"
PG_READ_REPLICA_DIR = "/tmp/pg_lake_rr"

POLARIS_HOSTNAME = "localhost"
POLARIS_PORT = 8181
POLARIS_PRINCIPAL_CREDS_FILE = "/tmp/regress-principal-credentials.txt"
POLARIS_PYICEBERG_SAMPLE = "/tmp/regress-pyiceberg.sample"
POLARIS_PID_FILE = "/tmp/regress-polaris.pid"

# isolation test params
PG_ISOLATION_HOST = "/tmp"
PG_ISOLATION_DATABASE = "isolation_regression"
PG_ISOLATION_PORT = "9090"
PG_ISOLATION_TMP_INSTANCE_PATH = "/tmp/isolation_test"
PG_ISOLATION_INPUT_DIR = "./tests/isolation"
PG_ISOLATION_TMP_CONFIG_PATH = f"{PG_ISOLATION_INPUT_DIR}/isolation.conf"
PG_ISOLATION_SCHEDULE_PATH = f"{PG_ISOLATION_INPUT_DIR}/isolation_schedule"
PG_ISOLATION_OUTPUT_DIR = f"{PG_ISOLATION_INPUT_DIR}/output"
PG_ISOLATION_EXPECTED_DIR = f"{PG_ISOLATION_INPUT_DIR}/expected"
PG_ISOLATION_EXTENSION_LIST = [
    "btree_gist",
    "pg_extension_base",
    "pg_map",
    "pg_lake_engine",
    "pg_lake_iceberg",
    "pg_lake_table",
    "pg_lake_copy",
]

MANAGED_STORAGE_CMK_ID = ""

# spark catalog params
SPARK_CATALOG_PG_DATABASE = "spark_catalog"
SPARK_CATALOG = "pg_lake"

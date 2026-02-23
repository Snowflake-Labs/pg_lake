import duckdb
import os
import psycopg2
import psycopg2.extras
import pytest
import shutil
import queue
import threading
import time
from utils_pytest import *
import server_params

reduce_werkzeug_log_level()


@pytest.fixture(scope="module")
def pgduck_conn(postgres):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def test_s3_path(request, s3):
    return f"s3://{TEST_BUCKET}/{request.node.name}"


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, iceberg_extension, s3):
    catalog = create_iceberg_test_catalog(superuser_conn)
    yield catalog
    tables = catalog.list_tables("public")
    for table in tables:
        catalog.drop_table(table)
    catalog.drop_namespace("public")
    catalog.engine.dispose()


@pytest.fixture(scope="module")
def duckdb_conn(s3):
    conn = duckdb.connect(database=":memory:")
    conn.execute(
        """
        CREATE SECRET s3test (
            TYPE S3, KEY_ID 'testing', SECRET 'testing',
            ENDPOINT 'localhost:5999',
            SCOPE 's3://testbucketcdw', URL_STYLE 'path', USE_SSL false
        );
    """
    )
    conn.execute(
        """
        CREATE SECRET gcstest (
            TYPE GCS, KEY_ID 'testing', SECRET 'testing',
            ENDPOINT 'localhost:5998',
            SCOPE 'gs://testbucketgcs', URL_STYLE 'path', USE_SSL false
        );
    """
    )
    yield conn
    conn.close()

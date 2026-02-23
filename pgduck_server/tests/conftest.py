import subprocess
import threading
import queue
import time
from pathlib import Path
import psycopg2
import socket
import struct
import os
import signal
import pytest

from utils_pytest import *
import server_params


@pytest.fixture(scope="session")
def pgduck_server(request, installcheck, configure_server_params):
    if installcheck:
        yield None
    else:
        setup_pgduck_server()
        yield


# when --installcheck is passed to pytests,
# override the variables to point to the
# official pgduck_server settings
# this trick helps us to use the existing
# pgduck_server
@pytest.fixture(autouse=True, scope="session")
def configure_server_params(request):
    if request.config.getoption("--installcheck"):
        server_params.PGDUCK_PORT = 5332
        server_params.DUCKDB_DATABASE_FILE_PATH = "/tmp/duckdb.db"
        server_params.PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
        server_params.PGDUCK_CACHE_DIR = "/tmp/cache"

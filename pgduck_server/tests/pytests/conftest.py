import pytest
import server_params
import psycopg2
from utils_pytest import *


@pytest.fixture(autouse=True)
def cleanup_test_servers():
    """Terminate any server processes spawned during a test.

    Tests in test_server_start.py spawn pgduck_server instances via
    start_server_in_background().  If an assertion fails before the test
    reaches its cleanup code, those servers are left running and block
    subsequent tests from binding the same port.  This fixture snapshots
    the spawned_test_servers list before each test and kills any new
    entries that appeared during the test.
    """
    before = set(id(p) for p in spawned_test_servers)
    yield
    for proc in spawned_test_servers:
        if id(proc) not in before:
            terminate_process(proc)
    # Keep only the pre-existing entries (e.g. the session-scoped server).
    spawned_test_servers[:] = [p for p in spawned_test_servers if id(p) in before]


@pytest.fixture(scope="module")
def pgduck_conn(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()

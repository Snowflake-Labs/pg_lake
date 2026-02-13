import pytest
import psycopg2
import time
from utils_pytest import *


def test_list_preload_libraries(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT library_name FROM extension_base.list_preload_libraries() WHERE extension_name LIKE 'pg_extension_base_test%' ORDER BY 1",
        superuser_conn,
    )
    assert len(result) == 6
    assert result[0]["library_name"] == "$libdir/pg_extension_base_test_ext1"
    assert result[1]["library_name"] == "$libdir/pg_extension_base_test_ext2"
    assert result[2]["library_name"] == "$libdir/pg_extension_base_test_ext3"
    assert result[3]["library_name"] == "$libdir/pg_extension_base_test_hibernate"
    assert result[4]["library_name"] == "$libdir/pg_extension_base_test_scheduler"

    # We did not explicitly specify $libdir for pg_extension_base_test_common
    assert result[5]["library_name"] == "pg_extension_base_test_common"

    superuser_conn.rollback()


# pg_extension_base_test_ext1 uses
#!shared_preload_libraries
def test_create_extension_test_ext1(superuser_conn):
    # check that we can create the extension, which fails unless loaded
    # via shared_preload_libraries
    run_command("CREATE EXTENSION pg_extension_base_test_ext1 CASCADE", superuser_conn)

    result = run_query(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_extension_base_test_ext1'",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["extversion"] == "1.1"

    result = run_query(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_extension_base'",
        superuser_conn,
    )
    assert len(result) == 1

    superuser_conn.rollback()


# pg_extension_base_test_ext2 uses
#!shared_preload_libraries = 'pg_extension_base_test_common,$libdir/pg_extension_base_test_ext2'
def test_create_extension_test_ext2(superuser_conn):
    # check that we can create the extension, which fails unless loaded via
    # shared_preload_libraries or the common library is not loaded.
    run_command("CREATE EXTENSION pg_extension_base_test_ext2 CASCADE", superuser_conn)

    result = run_query(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["extversion"] == "1.1"

    result = run_query(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_extension_base'",
        superuser_conn,
    )
    assert len(result) == 1

    superuser_conn.rollback()

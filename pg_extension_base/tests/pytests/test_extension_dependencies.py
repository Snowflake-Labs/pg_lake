import pytest
import psycopg2
import time
from utils_pytest import *

LATEST_PG_EXT_BASE = "1.6"


# We mock a "new dependency" scenario, by first creating an old version
# and then manipulating the catalogs to force remove the existing
# dependency.
#
# After that, updating the extension will recreate the dependency.
def test_extension_dependency_create(superuser_conn):
    run_command(
        """
        create extension pg_extension_base_test_ext3 version '1.0' cascade;
        delete from pg_depend
        where classid = 'pg_extension'::regclass and objid = (select oid from pg_extension where extname = 'pg_extension_base_test_ext3');
        drop extension pg_extension_base_test_ext2;
    """,
        superuser_conn,
    )

    result = run_query(
        "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert len(result) == 0

    run_command("alter extension pg_extension_base_test_ext3 update", superuser_conn)

    result = run_query(
        "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert result[0]["extversion"] == "1.1"

    superuser_conn.rollback()


# We mock an "outdated dependency" scenario, by first creating
# the extension and its dependency with their old schemas.
#
# Updating to the latest schema should also update the dependency.
def test_extension_dependency_update(superuser_conn):
    run_command(
        """
        drop extension if exists pg_extension_base cascade;
        create extension pg_extension_base_test_ext2 version '1.0' cascade;
        create extension pg_extension_base_test_ext3 version '1.0' cascade;
    """,
        superuser_conn,
    )

    result = run_query(
        "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert result[0]["extversion"] == "1.0"

    # Updating to a specific version does not affect dependencies
    run_command(
        "alter extension pg_extension_base_test_ext3 update to '1.1'", superuser_conn
    )

    result = run_query(
        "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert result[0]["extversion"] == "1.0"

    # Updating to latest version also updates dependencies
    run_command("alter extension pg_extension_base_test_ext3 update", superuser_conn)

    result = run_query(
        "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert result[0]["extversion"] == "1.1"

    superuser_conn.rollback()


# We first create an older version of pg_extension_base, and then the
# up-to-date version of pg_extension_base_test_ext1, which triggers
# an update of pg_extension_base.
def test_extension_dependency_update_on_create(superuser_conn):
    run_command(
        """
        drop extension if exists pg_extension_base cascade;
        create extension pg_extension_base_test_ext2 version '1.0' cascade;
        create extension pg_extension_base_test_ext3 cascade;
    """,
        superuser_conn,
    )

    result = run_query(
        "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
        superuser_conn,
    )
    assert result[0]["extversion"] == "1.1"

    superuser_conn.rollback()

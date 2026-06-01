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


# When pg_extension_base.dependency_escalation_role names a role that the
# current user is a member of, the synthesized ALTER EXTENSION calls used to
# update dependencies should run as the bootstrap superuser. Without the
# escalation, a non-superuser cannot ALTER an extension they don't own, so
# the cascading dependency update would fail.
def test_extension_dependency_role_escalation(superuser_conn):
    role_name = "ext_dep_escalator"
    user_name = "ext_dep_user"

    def set_escalation_role(value):
        # ALTER SYSTEM cannot run inside a transaction block; flip the
        # connection into autocommit for the duration of the GUC update.
        superuser_conn.commit()
        superuser_conn.autocommit = True
        try:
            run_command(
                f"alter system set pg_extension_base.dependency_escalation_role to '{value}';",
                superuser_conn,
            )
            run_command("select pg_reload_conf();", superuser_conn)
        finally:
            superuser_conn.autocommit = False

    def reset_escalation_role():
        superuser_conn.commit()
        superuser_conn.autocommit = True
        try:
            run_command(
                "alter system reset pg_extension_base.dependency_escalation_role;",
                superuser_conn,
            )
            run_command("select pg_reload_conf();", superuser_conn)
        finally:
            superuser_conn.autocommit = False

    # Set up: install old versions as superuser; create the role and a member
    # user; reassign ownership of ext3 to the test user so they can run
    # ALTER EXTENSION on it. PG has no ALTER EXTENSION ... OWNER TO syntax,
    # so we update the catalog directly.
    run_command(
        f"""
        drop extension if exists pg_extension_base cascade;
        create extension pg_extension_base_test_ext2 version '1.0' cascade;
        create extension pg_extension_base_test_ext3 version '1.0' cascade;
        drop role if exists {user_name};
        drop role if exists {role_name};
        create role {role_name};
        create user {user_name} in role {role_name};
        update pg_extension set extowner = (select oid from pg_roles where rolname = '{user_name}')
            where extname = 'pg_extension_base_test_ext3';
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        # Precondition: without escalation, the non-superuser owner of ext3
        # cannot cascade-update its dependency ext2 (which they don't own).
        set_escalation_role("")

        user_conn = open_pg_conn(user=user_name)
        error = run_command(
            "alter extension pg_extension_base_test_ext3 update;",
            user_conn,
            raise_error=False,
        )
        assert (
            error is not None
        ), "expected non-superuser update to fail without escalation"
        user_conn.rollback()
        user_conn.close()

        # With the escalation role set, the same ALTER should succeed and
        # cascade the dependency update under the bootstrap superuser.
        set_escalation_role(role_name)

        user_conn = open_pg_conn(user=user_name)
        run_command(
            "alter extension pg_extension_base_test_ext3 update;",
            user_conn,
        )
        user_conn.commit()
        user_conn.close()

        result = run_query(
            "select extversion from pg_extension where extname = 'pg_extension_base_test_ext2'",
            superuser_conn,
        )
        assert result[0]["extversion"] == "1.1"
    finally:
        reset_escalation_role()
        run_command(
            f"""
            update pg_extension set extowner = (select oid from pg_roles where rolname = current_user)
                where extname = 'pg_extension_base_test_ext3';
            drop owned by {user_name};
            drop user if exists {user_name};
            drop role if exists {role_name};
            """,
            superuser_conn,
        )
        superuser_conn.commit()

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

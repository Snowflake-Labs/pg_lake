"""Core pytest fixtures shared across the test suite.

Provides session- and module-scoped fixtures for PostgreSQL / PgDuck
servers, database connections, extension setup, user management, and
common test-infrastructure plumbing.
"""

import time

import psycopg2
import pytest

from . import server_params
from .cloud_storage import (
    TEST_BUCKET,
    create_duckdb_conn,
)
from .db import (
    open_pg_conn,
    run_command,
    run_query,
)
from .server import (
    remove_duckdb_cache,
    setup_pgduck_server,
    start_postgres,
    stop_postgres,
)
from .db import get_pg_version_num


# ---------------------------------------------------------------------------
# Singleton guards for session-scoped fixtures
# ---------------------------------------------------------------------------
_pgduck_started = False
_postgres_started = False


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pgduck_server(installcheck, s3):
    global _pgduck_started
    if _pgduck_started:
        yield
        return
    if installcheck:
        remove_duckdb_cache()
        _pgduck_started = True
        yield None
    else:
        server = setup_pgduck_server()
        _pgduck_started = True
        yield
        server.cleanup()


@pytest.fixture(scope="session")
def postgres(installcheck, pgduck_server):
    global _postgres_started
    if _postgres_started:
        yield
        return
    if not installcheck:
        start_postgres(
            server_params.PG_DIR, server_params.PG_USER, server_params.PG_PORT
        )
    _postgres_started = True

    yield

    if not installcheck:
        stop_postgres(server_params.PG_DIR)


@pytest.fixture(autouse=True, scope="session")
def _reduce_werkzeug_log_level():
    import logging

    logging.getLogger("werkzeug").setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def postgis_extension(postgres):
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        SET pgaudit.log TO 'none';
        CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
        RESET pgaudit.log;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS postgis CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pg_conn(postgres, app_user):
    conn = open_pg_conn(app_user)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def superuser_conn(postgres):
    conn = open_pg_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pgduck_conn(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def test_s3_path(request, s3):
    return f"s3://{TEST_BUCKET}/{request.node.name}"


@pytest.fixture(scope="module")
def duckdb_conn(s3):
    conn = create_duckdb_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def user_conn(test_user):
    conn = open_pg_conn(user=test_user)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def app_user(postgres):
    conn = open_pg_conn()

    # This query will generate a new unique application user that does not exist on the server
    res = run_query(
        """
        SELECT rolname, current_database() FROM pg_roles
        RIGHT JOIN (
            SELECT ('app_user_' || floor(random() * 1000)) rolname FROM generate_series(1,10)
        ) USING (rolname) WHERE pg_roles.rolname IS NULL LIMIT 1
    """,
        conn,
    )

    app_user, db_name = res[0]

    assert app_user is not None

    run_command(
        f"""
        CREATE USER {app_user};
        GRANT USAGE, CREATE ON SCHEMA public TO {app_user};
        GRANT CREATE ON DATABASE {db_name} TO {app_user};
    """,
        conn,
    )

    conn.commit()

    yield app_user

    run_command(
        f"""
        DROP OWNED BY {app_user} CASCADE;
        DROP ROLE {app_user};
    """,
        conn,
    )

    conn.commit()


@pytest.fixture(scope="module")
def extension(superuser_conn, pg_conn, app_user):
    # we do not want to expose 1.3 features to all users, but expose in the tests
    # so we have this check on enable_experimental_features
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
        GRANT lake_read_write TO {app_user};
        GRANT USAGE ON SCHEMA lake_table TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.default_location_prefix TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.enable_manifest_merge_on_write TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.manifest_min_count_to_merge TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.target_manifest_size_kb TO {app_user};
        GRANT SET ON PARAMETER pg_lake_iceberg.max_snapshot_age TO {app_user};
        GRANT SET ON PARAMETER pg_lake_engine.orphaned_file_retention_period TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.max_file_removals_per_vacuum TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.max_compactions_per_vacuum TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.target_row_group_size_mb TO {app_user};
        GRANT SET ON PARAMETER pg_lake_table.default_parquet_version TO {app_user};

        CREATE EXTENSION IF NOT EXISTS pgaudit;
        GRANT SET ON PARAMETER pgaudit.log TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    pg_conn.rollback()
    superuser_conn.rollback()
    run_command(
        f"""
        DROP EXTENSION pg_lake_table CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module")
def test_user(extension):
    username = "test_application"

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        CREATE USER {username};
        GRANT ALL ON SCHEMA public TO {username};
        GRANT CREATE ON DATABASE {server_params.PG_DATABASE} TO {username};
        GRANT lake_read_write TO {username};
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield username

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP OWNED BY {username};
        DROP USER {username};
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="module")
def create_injection_extension(superuser_conn):
    if get_pg_version_num(superuser_conn) >= 170000:
        run_command("CREATE EXTENSION injection_points", superuser_conn)
        superuser_conn.commit()
    yield

    if get_pg_version_num(superuser_conn) >= 170000:
        run_command("DROP EXTENSION injection_points", superuser_conn)
        superuser_conn.commit()


@pytest.fixture(scope="module")
def pg_lake_extension():
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="module")
def pg_lake_benchmark_extension():
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_benchmark CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_benchmark;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="module")
def pg_extension_base():
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_extension_base CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()

    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_extension_base CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


@pytest.fixture(scope="module")
def spatial_analytics_extension(postgis_extension):
    superuser_conn = open_pg_conn()

    run_command(
        f"""
        SET pgaudit.log TO 'none';
        CREATE EXTENSION IF NOT EXISTS pg_lake_spatial CASCADE;
        RESET pgaudit.log;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()

    yield

    superuser_conn = open_pg_conn()
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_spatial CASCADE;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
    superuser_conn.close()


# ---------------------------------------------------------------------------
# Function-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def with_default_location(request, s3, extension, pg_conn, superuser_conn):
    marker = request.node.get_closest_marker("location_prefix")
    if marker is None:
        location_prefix = None
    else:
        location_prefix = marker.args[0]

    if location_prefix is None:
        location_prefix = f"s3://{TEST_BUCKET}"

    run_command(
        f"""
        SET pg_lake_iceberg.default_location_prefix TO '{location_prefix}';
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        SET pg_lake_iceberg.default_location_prefix TO '{location_prefix}';
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    pg_conn.rollback()
    superuser_conn.rollback()

    run_command(
        f"""
        RESET pg_lake_iceberg.default_location_prefix;
    """,
        pg_conn,
    )

    run_command(
        f"""
        RESET pg_lake_iceberg.default_location_prefix;
    """,
        superuser_conn,
    )

    pg_conn.commit()
    superuser_conn.commit()


# This fixture is intended only to simplify the process of attaching a debugger
# to a specific test that is misbehaving.  To use this, add `debug_pg_conn` to
# the list of fixures in the test, then when you hit the test in question you
# can snarf the pid in question and attach a debugger to the given running test
# backend like so:
#
# gdb -p $(</tmp/backend.pid)
#
# This is only used for debugging tests; no committed/pushed code should include
# this as a fixture.
@pytest.fixture()
def debug_pg_conn(pg_conn):
    res = run_query("select pg_backend_pid()", pg_conn)

    with open("/tmp/backend.pid", "w") as f:
        f.write(str(res[0][0]))

    time.sleep(15)

    yield

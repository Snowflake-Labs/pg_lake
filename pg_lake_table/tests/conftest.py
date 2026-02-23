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
def read_replica(superuser_conn, s3, installcheck, app_user):
    # We currently do not perform read replica checks under installcheck
    if not installcheck:
        create_read_replica(
            server_params.PG_READ_REPLICA_DIR, server_params.PG_READ_REPLICA_PORT
        )

        run_command(
            f"""
            SET synchronous_commit TO 'remote_apply';
            CREATE EXTENSION IF NOT EXISTS pg_lake_table CASCADE;
            GRANT lake_read_write TO {app_user};
        """,
            superuser_conn,
        )

        superuser_conn.commit()

    yield

    if not installcheck:
        stop_postgres(server_params.PG_READ_REPLICA_DIR)


@pytest.fixture(scope="module")
def pg_replica_conn(read_replica, installcheck):
    conn = None

    # We currently do not perform read replica checks under installcheck
    if not installcheck:
        conn_str = f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_READ_REPLICA_PORT} host={server_params.PG_HOST}"
        conn = psycopg2.connect(conn_str)

    yield conn


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
def superuser_conn(postgres):
    conn = open_pg_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def user_conn(test_user):
    conn = open_pg_conn(user=test_user)
    yield conn
    conn.close()


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
def create_pushdown_tables(s3, pg_conn, extension):
    url_users = f"s3://{TEST_BUCKET}/test_pushdown/users.parquet"
    url_events = f"s3://{TEST_BUCKET}/test_pushdown/events.parquet"

    # Generate random data for the Users table
    run_command(
        f"""
        COPY (SELECT s AS id,
                     NOW() - INTERVAL '1 hour' * s AS timestamp,
                     (RANDOM() * 100)::int AS metric1,
                     (RANDOM() * 100)::int AS metric2,
                     RANDOM() AS float_value,
                     (RANDOM() * 10000)::bigint AS big_metric
              FROM generate_series(1, 100) s) TO '{url_users}';
    """,
        pg_conn,
    )

    # Generate random data for the Events table
    run_command(
        f"""
        COPY (SELECT s AS id,
                     NOW() - INTERVAL '2 hours' * s AS timestamp,
                     (RANDOM() * 10)::int AS event_type,
                     (RANDOM() * 100)::int AS metric2,
                     RANDOM() AS float_value,
                     (RANDOM() * 10000)::bigint AS big_metric,
                     ARRAY[(RANDOM() * 10000)::bigint, (RANDOM() * 10000)::bigint] AS array_metric
              FROM generate_series(1, 100) s) TO '{url_events}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE SCHEMA test_window_function_pushdown;
                SET search_path TO test_window_function_pushdown;

                CREATE TABLE Users (id int, timestamp timestamp, metric1 int, metric2 int, float_value float, big_metric bigint);
                CREATE TABLE Events (id int, timestamp timestamp, event_type int, metric2 int, float_value float, big_metric bigint, array_metric bigint[]);
                COPY Users FROM '%s';
                COPY Events FROM '%s';

                CREATE FOREIGN TABLE Users_f (id int, timestamp timestamp, metric1 int, metric2 int, float_value float, big_metric bigint)
                SERVER pg_lake  OPTIONS (format 'parquet', path '%s');

                CREATE FOREIGN TABLE Events_f (id int, timestamp timestamp, event_type int, metric2 int, float_value float, big_metric bigint, array_metric bigint[])
                SERVER pg_lake  OPTIONS (format 'parquet', path '%s');

        """
        % (url_users, url_events, url_users, url_events),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_window_function_pushdown CASCADE;", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def duckdb_conn(s3):
    conn = create_duckdb_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, extension, s3):
    catalog = create_iceberg_test_catalog(superuser_conn)
    yield catalog
    tables = catalog.list_tables("public")
    for table in tables:
        catalog.drop_table(table)
    catalog.drop_namespace("public")
    catalog.engine.dispose()

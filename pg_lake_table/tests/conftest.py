import psycopg2
import pytest
from utils_pytest import *


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

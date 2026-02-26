import pytest
import psycopg2
import duckdb
import math
from decimal import *
from utils_pytest import *


def test_same_table_multiple_databases(s3, pg_conn, extension, superuser_conn):
    # verify that we can use the same source table in different databases
    url = f"s3://{TEST_BUCKET}/test_same_table_multiple_databases/data.p"

    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s AS h FROM generate_series(1,10) s) TO '{url}' WITH (format 'parquet');
    """,
        pg_conn,
    )

    pg_conn.commit()

    # create new users and databases
    superuser_conn.autocommit = True
    run_command("CREATE USER user_a SUPERUSER", superuser_conn)
    run_command("CREATE DATABASE db_a OWNER user_a", superuser_conn)
    run_command("CREATE USER user_b; GRANT lake_read_write TO user_b", superuser_conn)
    run_command("CREATE DATABASE db_b OWNER user_b", superuser_conn)
    superuser_conn.autocommit = False

    a_conn_pg = open_pg_conn_for_user_db(dbname="db_a", user="postgres")
    assert a_conn_pg

    a_conn = open_pg_conn_for_user_db(dbname="db_a", user="user_a")
    assert a_conn

    b_conn_pg = open_pg_conn_for_user_db(dbname="db_b", user="postgres")
    assert b_conn_pg

    b_conn = open_pg_conn_for_user_db(dbname="db_b", user="user_b")
    assert b_conn

    for conn in [a_conn_pg, b_conn_pg]:
        run_command("CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;", conn)
        conn.commit()

    # Create the table on each connection using the user accounts
    for conn in [a_conn, b_conn]:
        run_command(
            f"""
            CREATE FOREIGN TABLE test_same_db () SERVER pg_lake OPTIONS (path '{url}', format 'parquet');
        """,
            conn,
        )
        conn.commit()

    a_res = run_query("select * from test_same_db", a_conn)
    b_res = run_query("select * from test_same_db", b_conn)

    assert len(a_res) == 10
    assert a_res == b_res

    a_conn_pg.close()
    a_conn.close()
    b_conn_pg.close()
    b_conn.close()

    superuser_conn.autocommit = True
    run_command("DROP DATABASE db_a  WITH (FORCE)", superuser_conn)
    run_command("DROP DATABASE db_b  WITH (FORCE)", superuser_conn)
    run_command("DROP USER user_a", superuser_conn)
    run_command("DROP USER user_b", superuser_conn)
    superuser_conn.autocommit = False


def connection_string_for_user_db(dbname, user):
    return f"dbname={dbname} user={user} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"


def open_pg_conn_for_user_db(dbname, user):
    return psycopg2.connect(connection_string_for_user_db(dbname, user))

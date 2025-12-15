import pytest
import psycopg2
import time
from utils_pytest import *
import server_params


@pytest.fixture(autouse=True)
def skip_if_unsupported(superuser_conn):
    if superuser_conn.server_version >= 180000:
        pytest.xfail("postgres 18 does not support this feature")


def test_server_start(superuser_conn):
    result = get_pg_extension_workers(superuser_conn)
    assert result[0]["datname"] == None
    assert result[0]["application_name"] == "pg base extension server starter"


def test_create_drop_pg_extension_base_test_scheduler(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # drop the pg_extension_base_test_scheduler extension
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_create_deregister_worker(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister and abort (worker should restart)
    run_command(
        "SELECT extension_base.deregister_worker('pg_extension_base_test_scheduler_main_worker')",
        superuser_conn,
    )
    superuser_conn.rollback()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister and commit (worker gone)
    run_command(
        "SELECT extension_base.deregister_worker('pg_extension_base_test_scheduler_main_worker')",
        superuser_conn,
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_create_deregister_worker_id(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # get id from UDF
    worker_id = run_query(
        "SELECT worker_id FROM extension_base.workers WHERE worker_name = 'pg_extension_base_test_scheduler_main_worker'",
        superuser_conn,
    )[0][0]
    assert worker_id > 0

    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister by id and abort (worker should restart)
    run_command(
        f"SELECT extension_base.deregister_worker({worker_id})",
        superuser_conn,
    )
    superuser_conn.rollback()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # deregister by id and commit (worker gone)
    run_command(
        f"SELECT extension_base.deregister_worker({worker_id})",
        superuser_conn,
    )
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)


def test_create_drop_pg_extension_base(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # drop the pg_base_base extension
    run_command("DROP EXTENSION pg_extension_base CASCADE", superuser_conn)
    superuser_conn.commit()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_create_abort_pg_extension_base_test_scheduler(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )

    assert count_pg_extension_base_workers(superuser_conn) == 0

    # rollback does not result in base worker creation
    superuser_conn.rollback()

    assert count_pg_extension_base_workers(superuser_conn) == 0


def test_drop_create_pg_extension_base_test_scheduler(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    time.sleep(0.1)

    # base worker is killed
    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )

    # transaction is not yet over, no base worker started
    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # cleanup
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_drop_create_pg_extension_base(superuser_conn):
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    run_command("DROP EXTENSION pg_extension_base CASCADE", superuser_conn)
    time.sleep(0.1)

    # base worker is killed
    assert count_pg_extension_base_workers(superuser_conn) == 0

    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )

    # transaction is not yet over, no base worker started
    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # cleanup
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", superuser_conn
    )
    superuser_conn.commit()


def test_create_drop_database(superuser_conn):
    superuser_conn.autocommit = True

    # create another database and then add the extension
    run_command("CREATE DATABASE other", superuser_conn)

    other_conn_str = f"dbname=other user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)

    run_command("CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)
    other_conn.commit()
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    other_conn.close()

    run_command("DROP DATABASE other", superuser_conn)

    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.autocommit = False


def test_create_database_from_template(superuser_conn):
    superuser_conn.autocommit = True

    # Create the extension in template1
    template_conn_str = f"dbname=template1 user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    template_conn = psycopg2.connect(template_conn_str)
    run_command(
        "CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", template_conn
    )
    template_conn.commit()
    template_conn.close()

    # create another database which already has the extension
    run_command("CREATE DATABASE other", superuser_conn)
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 1

    # drop the other database
    run_command("DROP DATABASE other", superuser_conn)
    time.sleep(0.1)

    assert count_pg_extension_base_workers(superuser_conn) == 0

    # clean template1
    template_conn = psycopg2.connect(template_conn_str)
    run_command(
        "DROP EXTENSION pg_extension_base_test_scheduler CASCADE", template_conn
    )
    template_conn.commit()
    template_conn.close()

    superuser_conn.autocommit = False


def test_failed_drop_database(superuser_conn):
    superuser_conn.autocommit = True

    # create another database and then add the extension
    run_command("CREATE DATABASE other", superuser_conn)

    # open a connection to other and keep it open
    other_conn_str = f"dbname=other user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    other_conn = psycopg2.connect(other_conn_str)
    run_command("CREATE EXTENSION pg_extension_base_test_scheduler CASCADE", other_conn)
    other_conn.commit()

    # try to drop it from the original connection
    error = run_command("DROP DATABASE other", superuser_conn, raise_error=False)
    assert "being accessed by other user" in error

    time.sleep(0.1)

    # should still have the base worker
    assert count_pg_extension_base_workers(superuser_conn) == 1

    other_conn.close()

    # now actually drop it
    run_command("DROP DATABASE other", superuser_conn)

    assert count_pg_extension_base_workers(superuser_conn) == 0

    superuser_conn.autocommit = False


def get_pg_extension_workers(conn):
    query = "SELECT datname, application_name FROM pg_stat_activity WHERE backend_type LIKE 'pg_base_extension server %' OR backend_type LIKE 'pg base extension%' ORDER BY application_name"
    result = run_query(query, conn)
    return result


def count_pg_extension_base_workers(conn):
    query = "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'pg base extension %'"
    result = run_query(query, conn)
    return result[0]["count"]

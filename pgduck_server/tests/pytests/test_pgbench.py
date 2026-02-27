from utils_pytest import *
import os
import psycopg2

from pathlib import Path


def test_pgbench_prepared(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )

    file_path = "/tmp/test_simple_1.sql"
    file = open(file_path, "w")
    file.write("SELECT 1;\n")
    file.close()

    returncode, stdout, stderr = run_pgbench_command(
        [
            "-M",
            "prepared",
            "-n",
            "-f",
            file_path,
            "-h",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "-p",
            str(server_params.PGDUCK_PORT),
        ]
    )
    assert returncode != 0, f"pgbench prepared failed"

    assert (
        "named prepared statements not supported in pgduck_server" in stderr
    ), f"pgbench prepared has not ended with expected message"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


# this test runs the full pgbench tests
# with two gotchas. First, we included the
# queries in files instead of directly using -b option
# this is because when -b is used, pgbench executes some
# metadata queries, which fails on duckdb
# Second, we do not load any data, because loading data
# fails on duckdb for various reasons. However, that's not
# what we want to test here, we want to test the extended protocol
# so we don't care about query results
def test_pgbench_full_suite(pgduck_server):

    returncode, stdout, stderr = run_pgbench_command(
        [
            "-i",
            "-I",
            "t",
            "-h",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "-p",
            str(server_params.PGDUCK_PORT),
        ]
    )
    assert returncode == 0, f"pgbench prepared failed for init"

    test_dir = os.path.dirname(os.path.abspath(__file__))

    file_path = test_dir + "/sql/select-only.sql"
    returncode, stdout, stderr = run_pgbench_command(
        [
            "--protocol",
            "extended",
            "--no-vacuum",
            "--file",
            file_path,
            "--host",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(server_params.PGDUCK_PORT),
            "--transactions",
            "100",
            "--client",
            "10",
            "--jobs",
            "2",
        ]
    )
    assert returncode == 0, f"pgbench select-only extended failed"
    assert (
        "number of transactions actually processed: 1000/1000" in stdout
    ), f"pgbench extended has not ended with expected message"

    file_path = test_dir + "/sql/tpcb-like.sql"
    returncode, stdout, stderr = run_pgbench_command(
        [
            "--protocol",
            "extended",
            "--no-vacuum",
            "--file",
            file_path,
            "--host",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(server_params.PGDUCK_PORT),
            "--transactions",
            "100",
            "--client",
            "10",
            "--jobs",
            "2",
        ]
    )

    assert returncode == 0, f"pgbench tpcb-like extended failed"
    assert (
        "number of transactions actually processed: 1000/1000" in stdout
    ), f"pgbench extended has not ended with expected message"

    file_path = test_dir + "/sql/simple-update.sql"
    returncode, stdout, stderr = run_pgbench_command(
        [
            "--protocol",
            "extended",
            "--no-vacuum",
            "--file",
            file_path,
            "--host",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(server_params.PGDUCK_PORT),
            "--transactions",
            "100",
            "--client",
            "10",
            "--jobs",
            "2",
        ]
    )
    assert returncode == 0, f"pgbench simple-update extended failed"
    assert (
        "number of transactions actually processed: 1000/1000" in stdout
    ), f"pgbench extended has not ended with expected message"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    run_command("DROP TABLE pgbench_history", conn)
    run_command("DROP TABLE pgbench_accounts", conn)
    run_command("DROP TABLE pgbench_tellers", conn)
    run_command("DROP TABLE pgbench_branches", conn)
    conn.commit()
    conn.close()


def test_pgbench_extended(pgduck_server):

    file_path = "/tmp/test_simple_1.sql"
    file = open(file_path, "w")
    file.write("SELECT 1;\n")
    file.close()

    returncode, stdout, stderr = run_pgbench_command(
        [
            "--protocol",
            "extended",
            "--no-vacuum",
            "--file",
            file_path,
            "--host",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(server_params.PGDUCK_PORT),
            "--transactions",
            "100",
            "--client",
            "10",
            "--jobs",
            "2",
        ]
    )

    assert returncode == 0, f"pgbench prepared failed"
    assert (
        "number of transactions actually processed: 1000/1000" in stdout
    ), f"pgbench extended has not ended with expected message"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_pgbench_simple_select(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )

    file_path = "/tmp/test_simple_1.sql"
    file = open(file_path, "w")
    file.write(
        "SELECT 'This is a much longer string that exceeds the typical lengths and is used to test the varchar implementation in the database, ensuring that it can handle a wide range of string lengths without issue.';\n"
    )
    file.close()

    returncode, stdout, stderr = run_pgbench_command(
        [
            "--no-vacuum",
            "--file",
            file_path,
            "--host",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(server_params.PGDUCK_PORT),
            "--transactions",
            "100",
            "--client",
            "100",
            "--jobs",
            "2",
        ]
    )
    assert returncode == 0, f"pgbench prepared has not returned expected error code"
    assert (
        "number of transactions actually processed: 10000/10000" in stdout
    ), f"pgbench prepared has not ended with expected message"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)


def test_pgbench_simple_select_with_c(pgduck_server):
    socket_path_str = str(
        Path(server_params.PGDUCK_UNIX_DOMAIN_PATH)
        / f".s.PGSQL.{server_params.PGDUCK_PORT}"
    )

    file_path = "/tmp/test_simple_1.sql"
    file = open(file_path, "w")
    file.write(
        "SELECT 'This is a much longer string that exceeds the typical lengths and is used to test the varchar implementation in the database, ensuring that it can handle a wide range of string lengths without issue.';\n"
    )
    file.close()

    returncode, stdout, stderr = run_pgbench_command(
        [
            "--connect",
            "--no-vacuum",
            "--file",
            file_path,
            "--host",
            server_params.PGDUCK_UNIX_DOMAIN_PATH,
            "--port",
            str(server_params.PGDUCK_PORT),
            "--transactions",
            "10",
            "--client",
            "100",
            "--jobs",
            "2",
        ]
    )
    assert returncode == 0, f"pgbench prepared has not returned expected error code"
    assert (
        "number of transactions actually processed: 1000/1000" in stdout
    ), f"pgbench prepared has not failed with expected error message"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

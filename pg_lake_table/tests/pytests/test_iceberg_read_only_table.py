import pytest
from utils_pytest import *


def test_read_only_flag(pg_conn, superuser_conn, s3, extension, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_read_only_flag/data.parquet"
    run_command(
        f"""COPY (SELECT s FROM generate_series(1,10) s) TO '{url}';""", pg_conn
    )
    run_command("CREATE SCHEMA test_read_only_flag;", pg_conn)
    run_command("CREATE TABLE test_read_only_flag.test(a int) USING iceberg;", pg_conn)
    pg_conn.commit()

    run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
    superuser_conn.commit()

    commands = [
        ("INSERT INTO test_read_only_flag.test VALUES (1)", "does not allow inserts"),
        (f"COPY test_read_only_flag.test FROM '{url}'", "does not allow inserts"),
        ("DELETE FROM test_read_only_flag.test", "does not allow deletes"),
        ("UPDATE test_read_only_flag.test SET a = a +1", "does not allow updates"),
        (
            "TRUNCATE test_read_only_flag.test",
            "modifications on read-only iceberg tables are not supported",
        ),
        (
            "ALTER TABLE test_read_only_flag.test ADD COLUMN c1 INT",
            "modifications on read-only iceberg tables are not supported",
        ),
        (
            "ALTER TABLE test_read_only_flag.test DROP COLUMN a",
            "modifications on read-only iceberg tables are not supported",
        ),
        (
            "ALTER TABLE test_read_only_flag.test RENAME COLUMN a TO b",
            "modifications on read-only iceberg tables are not supported",
        ),
    ]

    for cmd, cmd_error in commands:
        error = run_command(cmd, pg_conn, raise_error=False)
        assert cmd_error in str(error)
        pg_conn.rollback()

    run_command(
        "UPDATE lake_iceberg.tables_internal SET read_only='f' WHERE table_name = 'test_read_only_flag.test'::regclass",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("DROP SCHEMA test_read_only_flag CASCADE;", pg_conn)
    pg_conn.commit()


def test_read_only_flag_multiple_dbs(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    url = f"s3://{TEST_BUCKET}/test_read_only_flag_multiple_dbs/"
    superuser_conn.autocommit = True
    run_command('CREATE DATABASE "db with extension";', superuser_conn)
    con_db_with_extension = open_conn_to_db("db with extension")
    run_command("CREATE EXTENSION pg_lake_table CASCADE", con_db_with_extension)
    run_command(
        f"CREATE TABLE t1(A INT) USING iceberg WITH (location='{url}')",
        con_db_with_extension,
    )
    con_db_with_extension.commit()

    run_command("CREATE DATABASE db_without_extension;", superuser_conn)
    run_command(
        "CREATE DATABASE db_no_conn WITH ALLOW_CONNECTIONS = False;", superuser_conn
    )
    run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
    superuser_conn.commit()

    error = run_command(
        "INSERT INTO t1 VALUES (1)", con_db_with_extension, raise_error=False
    )
    assert "does not allow inserts" in str(error)

    con_db_with_extension.close()

    superuser_conn.autocommit = True
    run_command('DROP DATABASE "db with extension" WITH (FORCE);', superuser_conn)
    run_command("DROP DATABASE db_without_extension WITH (FORCE);", superuser_conn)
    run_command("DROP DATABASE db_no_conn WITH (FORCE);", superuser_conn)
    superuser_conn.autocommit = False


def test_read_only_flag_vacuum(
    pg_conn, superuser_conn, s3, extension, with_default_location
):

    run_command("CREATE SCHEMA test_read_only_flag_vacuum;", pg_conn)
    run_command(
        "CREATE TABLE test_read_only_flag_vacuum.test(a int) USING iceberg;", pg_conn
    )
    pg_conn.commit()

    run_command("CALL lake_table.finish_postgres_recovery()", superuser_conn)
    superuser_conn.commit()

    pg_conn.autocommit = True

    # Clear any existing notices
    pg_conn.notices.clear()

    commands = ["VACUUM test_read_only_flag_vacuum.test"]

    for command in commands:
        run_command(command, pg_conn)
        assert len(pg_conn.notices) > 0
        assert 'WARNING:  lake table "test" is read-only' in pg_conn.notices[0]
        pg_conn.rollback()

    run_command(
        "UPDATE lake_iceberg.tables_internal SET read_only='f' WHERE table_name = 'test_read_only_flag_vacuum.test'::regclass",
        superuser_conn,
    )
    superuser_conn.commit()

    pg_conn.autocommit = False
    run_command("DROP SCHEMA test_read_only_flag_vacuum CASCADE;", pg_conn)
    pg_conn.commit()


def open_conn_to_db(dbname):
    conn_str = f"dbname='{dbname}' user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"

    return psycopg2.connect(conn_str)

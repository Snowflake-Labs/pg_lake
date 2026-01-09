import pytest
from utils_pytest import *


def test_alter_table_set_schema(pg_conn, s3, extension, with_default_location):

    run_command("CREATE SCHEMA first_schema;", pg_conn)
    run_command("CREATE SCHEMA second_schema;", pg_conn)

    run_command(
        "CREATE TABLE first_schema.test_table (a int, b int) USING iceberg WITH (catalog='postgres')",
        pg_conn,
    )
    run_command(
        "INSERT INTO first_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    prev_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )[0][0]
    run_command("ALTER TABLE first_schema.test_table SET SCHEMA second_schema", pg_conn)
    curr_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )[0][0]

    # now new snapshot is pushed for set schema because iceberg doesn't know about it
    assert prev_metadata_location != None
    assert prev_metadata_location == curr_metadata_location

    run_command(
        "INSERT INTO second_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM second_schema.test_table", pg_conn)
    assert results == [[20]]

    results = run_query(
        "SELECT table_namespace FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )
    assert results[0][0] == "second_schema"

    # make sure doing it again doesn't break anything
    run_command(
        "ALTER FOREIGN TABLE second_schema.test_table SET SCHEMA first_schema", pg_conn
    )

    run_command(
        "INSERT INTO first_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM first_schema.test_table", pg_conn)
    assert results == [[30]]

    results = run_query(
        "SELECT table_namespace FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )
    assert results[0][0] == "first_schema"

    pg_conn.rollback()


def test_alter_table_rename(pg_conn, s3, extension, with_default_location):

    run_command("CREATE SCHEMA first_schema;", pg_conn)

    run_command(
        "CREATE TABLE first_schema.test_table (a int, b int) USING iceberg WITH (catalog='Postgres')",
        pg_conn,
    )
    run_command(
        "INSERT INTO first_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    prev_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )[0][0]
    run_command(
        "ALTER TABLE first_schema.test_table RENAME TO test_table_renamed", pg_conn
    )
    curr_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_table_renamed'",
        pg_conn,
    )[0][0]

    # new snapshot is pushed for set schema although iceberg doesn't know about it
    assert prev_metadata_location != None
    assert prev_metadata_location == curr_metadata_location

    run_command(
        "INSERT INTO first_schema.test_table_renamed SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM first_schema.test_table_renamed", pg_conn)
    assert results == [[20]]

    results = run_query(
        "SELECT table_namespace FROM iceberg_tables WHERE table_name = 'test_table_renamed'",
        pg_conn,
    )
    assert results[0][0] == "first_schema"

    # make sure doing it again doesn't break anything
    run_command(
        "ALTER FOREIGN TABLE first_schema.test_table_renamed RENAME TO test_table",
        pg_conn,
    )

    run_command(
        "INSERT INTO first_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM first_schema.test_table", pg_conn)
    assert results == [[30]]

    results = run_query(
        "SELECT table_namespace FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )
    assert results[0][0] == "first_schema"

    pg_conn.rollback()


def test_alter_schema_rename(pg_conn, s3, extension, with_default_location):
    run_command("SET pg_lake_iceberg.default_catalog TO 'POSTGRES'", pg_conn)

    run_command("CREATE SCHEMA first_schema;", pg_conn)

    run_command(
        "CREATE TABLE first_schema.test_table (a int, b int) USING iceberg", pg_conn
    )
    run_command(
        "INSERT INTO first_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    prev_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'first_schema'",
        pg_conn,
    )[0][0]
    run_command("ALTER SCHEMA first_schema RENAME TO second_schema", pg_conn)
    curr_metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'second_schema'",
        pg_conn,
    )[0][0]

    # now new snapshot is pushed for set schema because iceberg doesn't know about it
    assert prev_metadata_location != None
    assert prev_metadata_location == curr_metadata_location

    run_command(
        "INSERT INTO second_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM second_schema.test_table", pg_conn)
    assert results == [[20]]

    run_command(
        "INSERT INTO second_schema.test_table SELECT i,i FROM generate_series(0,9)i",
        pg_conn,
    )

    results = run_query("SELECT count(*) FROM second_schema.test_table", pg_conn)
    assert results == [[30]]

    results = run_query(
        "SELECT table_namespace FROM iceberg_tables WHERE table_name = 'test_table'",
        pg_conn,
    )
    assert results[0][0] == "second_schema"

    pg_conn.rollback()
    run_command("RESET pg_lake_iceberg.default_catalog", pg_conn)


def test_alter_database_rename(superuser_conn, s3, extension, with_default_location):
    location = f"s3://{TEST_BUCKET}/test_alter_database_rename/"
    dbname = "db_to_rename"
    superuser_conn.autocommit = True
    run_command("SET pg_lake_iceberg.default_catalog TO 'postgres'", superuser_conn)

    run_command(f"CREATE DATABASE {dbname};", superuser_conn)

    conn_to_db = open_pg_conn_to_db(dbname)

    run_command("CREATE EXTENSION pg_lake_copy CASCADE", conn_to_db)
    run_command(
        f"CREATE TABLE test_table (a int, b int) USING iceberg WITH (location='{location}')",
        conn_to_db,
    )

    results = run_query("SELECT catalog_name FROM iceberg_tables", conn_to_db)
    assert results[0][0] == dbname
    conn_to_db.commit()
    conn_to_db.close()

    run_command(f"ALTER DATABASE {dbname} RENAME TO {dbname}_new;", superuser_conn)
    superuser_conn.commit()

    conn_to_db = open_pg_conn_to_db(f"{dbname}_new")
    results = run_query("SELECT catalog_name FROM iceberg_tables", conn_to_db)
    assert results[0][0] == f"{dbname}_new"
    conn_to_db.close()

    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE {dbname}_new WITH (FORCE);", superuser_conn)
    run_command("RESET pg_lake_iceberg.default_catalog", superuser_conn)

    superuser_conn.autocommit = False


def open_pg_conn_to_db(dbname):
    conn_str = f"dbname={dbname} user={server_params.PG_USER} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"

    return psycopg2.connect(conn_str)

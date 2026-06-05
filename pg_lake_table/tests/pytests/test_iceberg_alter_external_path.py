import pytest
from utils_pytest import *


def get_metadata_location(pg_conn, schema, table):
    result = run_query(
        f"""
        SELECT metadata_location FROM lake_iceberg.tables
        WHERE table_namespace = '{schema}' AND table_name = '{table}'
        """,
        pg_conn,
    )
    return result[0][0]


def test_alter_external_iceberg_path_happy_path(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        DROP SCHEMA IF EXISTS test_alter_ext_path CASCADE;
        CREATE SCHEMA test_alter_ext_path;
        CREATE TABLE test_alter_ext_path.internal (a int) USING iceberg;
        INSERT INTO test_alter_ext_path.internal VALUES (1), (2);
        """,
        pg_conn,
    )
    pg_conn.commit()

    initial_meta = get_metadata_location(pg_conn, "test_alter_ext_path", "internal")

    run_command(
        f"""
        CREATE FOREIGN TABLE test_alter_ext_path.external ()
        SERVER pg_lake OPTIONS (path '{initial_meta}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query("SELECT count(*) FROM test_alter_ext_path.external", pg_conn)
    assert result[0][0] == 2

    run_command(
        "INSERT INTO test_alter_ext_path.internal VALUES (3), (4), (5);",
        pg_conn,
    )
    pg_conn.commit()

    new_meta = get_metadata_location(pg_conn, "test_alter_ext_path", "internal")
    assert new_meta != initial_meta

    # external still pinned to the old snapshot until path is updated
    result = run_query("SELECT count(*) FROM test_alter_ext_path.external", pg_conn)
    assert result[0][0] == 2

    run_command(
        f"""
        ALTER FOREIGN TABLE test_alter_ext_path.external
            OPTIONS (SET path '{new_meta}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query("SELECT count(*) FROM test_alter_ext_path.external", pg_conn)
    assert result[0][0] == 5

    run_command("DROP SCHEMA test_alter_ext_path CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_external_iceberg_path_schema_mismatch(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        DROP SCHEMA IF EXISTS test_alter_ext_path_schema CASCADE;
        CREATE SCHEMA test_alter_ext_path_schema;
        CREATE TABLE test_alter_ext_path_schema.internal (a int) USING iceberg;
        INSERT INTO test_alter_ext_path_schema.internal VALUES (1);
        """,
        pg_conn,
    )
    pg_conn.commit()

    initial_meta = get_metadata_location(
        pg_conn, "test_alter_ext_path_schema", "internal"
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_alter_ext_path_schema.external ()
        SERVER pg_lake OPTIONS (path '{initial_meta}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        ALTER TABLE test_alter_ext_path_schema.internal ADD COLUMN b int;
        INSERT INTO test_alter_ext_path_schema.internal VALUES (2, 20);
        """,
        pg_conn,
    )
    pg_conn.commit()

    new_meta = get_metadata_location(pg_conn, "test_alter_ext_path_schema", "internal")

    run_command(
        f"""
        ALTER FOREIGN TABLE test_alter_ext_path_schema.external
            OPTIONS (SET path '{new_meta}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        "SELECT * FROM test_alter_ext_path_schema.external",
        pg_conn,
        raise_error=False,
    )
    assert "Schema mismatch" in str(error)
    pg_conn.rollback()

    run_command("DROP SCHEMA test_alter_ext_path_schema CASCADE;", pg_conn)
    pg_conn.commit()


def test_alter_external_iceberg_path_no_lake_read(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    run_command(
        """
        DROP SCHEMA IF EXISTS test_alter_ext_path_perm CASCADE;
        CREATE SCHEMA test_alter_ext_path_perm;
        CREATE TABLE test_alter_ext_path_perm.internal (a int) USING iceberg;
        INSERT INTO test_alter_ext_path_perm.internal VALUES (1);
        """,
        pg_conn,
    )
    pg_conn.commit()

    initial_meta = get_metadata_location(
        pg_conn, "test_alter_ext_path_perm", "internal"
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_alter_ext_path_perm.external ()
        SERVER pg_lake OPTIONS (path '{initial_meta}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        DROP ROLE IF EXISTS test_alter_ext_path_user;
        CREATE ROLE test_alter_ext_path_user;
        GRANT USAGE ON SCHEMA test_alter_ext_path_perm TO test_alter_ext_path_user;
        ALTER FOREIGN TABLE test_alter_ext_path_perm.external
            OWNER TO test_alter_ext_path_user;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("INSERT INTO test_alter_ext_path_perm.internal VALUES (2);", pg_conn)
    pg_conn.commit()
    new_meta = get_metadata_location(pg_conn, "test_alter_ext_path_perm", "internal")

    error = run_command(
        f"""
        SET ROLE test_alter_ext_path_user;
        ALTER FOREIGN TABLE test_alter_ext_path_perm.external
            OPTIONS (SET path '{new_meta}');
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "permission denied to read from URL" in str(error)
    superuser_conn.rollback()

    run_command(
        """
        RESET ROLE;
        DROP SCHEMA test_alter_ext_path_perm CASCADE;
        DROP ROLE test_alter_ext_path_user;
        """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_alter_external_iceberg_path_dependent_objects(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        DROP SCHEMA IF EXISTS test_alter_ext_path_deps CASCADE;
        CREATE SCHEMA test_alter_ext_path_deps;
        CREATE TABLE test_alter_ext_path_deps.internal (a int) USING iceberg;
        INSERT INTO test_alter_ext_path_deps.internal VALUES (1), (2);
        """,
        pg_conn,
    )
    pg_conn.commit()

    initial_meta = get_metadata_location(
        pg_conn, "test_alter_ext_path_deps", "internal"
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_alter_ext_path_deps.external ()
        SERVER pg_lake OPTIONS (path '{initial_meta}');
        CREATE VIEW test_alter_ext_path_deps.v AS
            SELECT * FROM test_alter_ext_path_deps.external;
        CREATE MATERIALIZED VIEW test_alter_ext_path_deps.mv AS
            SELECT * FROM test_alter_ext_path_deps.external;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query("SELECT count(*) FROM test_alter_ext_path_deps.v", pg_conn)
    assert result[0][0] == 2

    run_command(
        "INSERT INTO test_alter_ext_path_deps.internal VALUES (3), (4);",
        pg_conn,
    )
    pg_conn.commit()
    new_meta = get_metadata_location(pg_conn, "test_alter_ext_path_deps", "internal")

    run_command(
        f"""
        ALTER FOREIGN TABLE test_alter_ext_path_deps.external
            OPTIONS (SET path '{new_meta}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query("SELECT count(*) FROM test_alter_ext_path_deps.v", pg_conn)
    assert result[0][0] == 4

    run_command("REFRESH MATERIALIZED VIEW test_alter_ext_path_deps.mv;", pg_conn)
    pg_conn.commit()

    result = run_query("SELECT count(*) FROM test_alter_ext_path_deps.mv", pg_conn)
    assert result[0][0] == 4

    run_command("DROP SCHEMA test_alter_ext_path_deps CASCADE;", pg_conn)
    pg_conn.commit()

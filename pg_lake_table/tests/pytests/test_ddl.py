import pytest
import psycopg2
import time
import duckdb
import math
import datetime
from decimal import *
from utils_pytest import *

# some of the ALTER TABLE commands on foreign tables are not allowed by PG,
# whereas some are not allowed by pg_lake. We separately keep track of them
# to assert the correct error messages.


class AlterStmt:
    def __init__(self, command, **kwargs):
        self.command = command
        for key, value in kwargs.items():
            setattr(self, key, value)


disallowed_only_for_iceberg = ["pg_lake_iceberg"]
disallowed_only_for_writable = ["pg_lake"]
disallowed_for_all = ["pg_lake", "pg_lake_iceberg"]


@pytest.fixture(scope="module")
def alter_stmts(pg_conn):
    # !! ordering of the statements is important, as some of the statements depend on the previous ones !!
    alter_stmts = [
        # pg_lake disallows only for iceberg tables
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN c SET GENERATED ALWAYS;",
            pg_lake_disallow=disallowed_only_for_iceberg,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN c DROP IDENTITY;",
            pg_lake_disallow=disallowed_only_for_iceberg,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;",
            pg_lake_disallow=disallowed_only_for_iceberg,
        ),
        # pg_lake disallows only for writable tables
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN x int;",
            pg_lake_disallow=disallowed_only_for_writable,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw DROP COLUMN drop_col;",
            pg_lake_disallow=disallowed_only_for_writable,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw RENAME COLUMN rename_col TO description;",
            pg_lake_disallow=disallowed_only_for_writable,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN e DROP NOT NULL;",
            pg_lake_disallow=disallowed_only_for_writable,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN new_col INT DEFAULT 10;",
            pg_lake_disallow=disallowed_only_for_writable,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN e TYPE bigint;",
            pg_lake_disallow=disallowed_only_for_writable,
        ),
        # pg_lake disallows for iceberg and writable tables
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN a TYPE varchar(255);",
            pg_lake_disallow=disallowed_for_all,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN e SET NOT NULL;",
            pg_lake_disallow=disallowed_for_all,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN new_col INT CHECK (new_col > 10);",
            pg_lake_disallow=disallowed_for_all,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN new_col INT NOT NULL;",
            pg_lake_disallow=disallowed_for_all,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN new_col INT GENERATED ALWAYS AS IDENTITY;",
            pg_lake_disallow=disallowed_for_all,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN new_col INT DEFAULT random();",
            pg_lake_disallow=disallowed_for_all,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD COLUMN new_col TIMESTAMPTZ DEFAULT now();",
            pg_lake_disallow=disallowed_for_all,
        ),
        # disallowed for arbitrary tables because it involves Iceberg
        AlterStmt(
            "ALTER TABLE test_ddl.heap SET ACCESS METHOD iceberg;", other_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.heap SET ACCESS METHOD pg_lake_iceberg;",
            other_disallow=True,
        ),
        # pg disallows for all foreign tables
        AlterStmt("ALTER TABLE test_ddl.fdw SET LOGGED;", pg_disallow=True),
        AlterStmt("ALTER TABLE test_ddl.fdw SET UNLOGGED;", pg_disallow=True),
        AlterStmt("ALTER TABLE test_ddl.fdw SET ACCESS METHOD heap;", pg_disallow=True),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ATTACH PARTITION test_alter_attach_partition1 FOR VALUES IN (1);",
            pg_disallow=True,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw DETACH PARTITION test_alter_detach_partition1;",
            pg_disallow=True,
        ),
        AlterStmt("ALTER TABLE test_ddl.fdw ENABLE RULE dummy_rule;", pg_disallow=True),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw DISABLE RULE dummy_rule;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ENABLE REPLICA RULE dummy_rule;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ENABLE ALWAYS RULE dummy_rule;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ENABLE ROW LEVEL SECURITY;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw DISABLE ROW LEVEL SECURITY;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw FORCE ROW LEVEL SECURITY;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw NO FORCE ROW LEVEL SECURITY;", pg_disallow=True
        ),
        AlterStmt("ALTER TABLE test_ddl.fdw CLUSTER ON a;", pg_disallow=True),
        AlterStmt("ALTER TABLE test_ddl.fdw SET WITHOUT CLUSTER;", pg_disallow=True),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw SET TABLESPACE pg_default;", pg_disallow=True
        ),
        AlterStmt("ALTER TABLE test_ddl.fdw OF dummy_type;", pg_disallow=True),
        AlterStmt("ALTER TABLE test_ddl.fdw NOT OF;", pg_disallow=True),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw REPLICA IDENTITY DEFAULT;", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER CONSTRAINT check_a_positive;",
            pg_disallow=True,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw SET (autovacuum_enabled = true);",
            pg_disallow=True,
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw RESET (autovacuum_enabled);", pg_disallow=True
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN a SET COMPRESSION default;",
            pg_disallow=True,
        ),
        # supported
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN b SET DEFAULT 'x';"),
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN b DROP DEFAULT;"),
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN d DROP EXPRESSION;"),
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN a SET STATISTICS 100;"),
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN a SET (n_distinct = 2);"),
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN a RESET (n_distinct);"),
        AlterStmt("ALTER TABLE test_ddl.fdw ALTER COLUMN a SET STORAGE PLAIN;"),
        AlterStmt(
            f"ALTER TABLE test_ddl.fdw OPTIONS (SET location 's3://{TEST_BUCKET}/test_ddl/') ;"
        ),
        AlterStmt("ALTER TABLE test_ddl.fdw OWNER TO __APP_USER__;"),
        AlterStmt("ALTER TABLE test_ddl.fdw ENABLE TRIGGER ALL;"),
        AlterStmt("ALTER TABLE test_ddl.fdw SET WITHOUT OIDS;"),
        AlterStmt("ALTER TABLE test_ddl.fdw ENABLE TRIGGER ALL;"),
        AlterStmt("ALTER TABLE test_ddl.fdw DISABLE TRIGGER ALL;"),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw ADD CONSTRAINT check_a_positive CHECK (a > 0);"
        ),
        AlterStmt("ALTER TABLE test_ddl.fdw VALIDATE CONSTRAINT check_a_positive;"),
        AlterStmt(
            "SET search_path TO 'test_ddl'; ALTER TABLE fdw DROP CONSTRAINT check_a_positive; RESET search_path;"
        ),
        AlterStmt(
            "ALTER TABLE test_ddl.fdw_for_set_schema SET SCHEMA another_ddl_schema;"
        ),
        AlterStmt("ALTER TABLE test_ddl.fdw_for_rename RENAME TO new_fdw;"),
    ]

    if get_pg_version_num(pg_conn) >= 170000:
        alter_set_expression_stmt = AlterStmt(
            "ALTER TABLE test_ddl.fdw ALTER COLUMN d SET EXPRESSION AS ('def');",
            pg_lake_disallow=disallowed_for_all,
        )
        alter_stmts.append(alter_set_expression_stmt)

    yield alter_stmts


@pytest.mark.parametrize("pglake_fdw_name", ["pg_lake", "pg_lake_iceberg"])
def test_alter_table_ddl(
    extension, pg_conn, s3, pglake_fdw_name, app_user, alter_stmts
):
    url = f"s3://{TEST_BUCKET}/test_ddl/"

    format_str = ""
    writable_str = ""
    if pglake_fdw_name == "pg_lake":
        format_str = ", format 'parquet'"
        writable_str = ", writable 'true'"

    # create the same table as an fdw and heap table
    run_command(
        f"""
        CREATE SCHEMA test_ddl;

        CREATE SCHEMA another_ddl_schema;

        CREATE FOREIGN TABLE test_ddl.fdw (
                a int not null,
                b text not null default 'abc',
                c int generated always as identity,
                d text generated always as ('abc') stored,
                rename_col text,
                drop_col    int,
                e int
        ) SERVER {pglake_fdw_name} OPTIONS (location '{url}_1'
                                             {format_str}
                                             {writable_str});

        CREATE FOREIGN TABLE test_ddl.fdw_for_rename (
                a int not null,
                b text not null default 'abc',
                c int generated always as identity,
                d text generated always as ('abc') stored
        ) SERVER {pglake_fdw_name} OPTIONS (location '{url}_2'
                                             {format_str}
                                             {writable_str});

        CREATE FOREIGN TABLE test_ddl.fdw_for_set_schema (
                a int not null,
                b text not null default 'abc',
                c int generated always as identity,
                d text generated always as ('abc') stored
        ) SERVER {pglake_fdw_name} OPTIONS (location '{url}_3'
                                             {format_str}
                                             {writable_str});

        CREATE TABLE test_ddl.heap (LIKE test_ddl.fdw);
    """,
        pg_conn,
    )

    pg_conn.commit()

    for alter_stmt in alter_stmts:
        command = alter_stmt.command.replace("__APP_USER__", app_user)
        error = run_command(command, pg_conn, raise_error=False)

        if hasattr(alter_stmt, "pg_disallow"):
            assert "operation is not supported for foreign tables" in error
            pg_conn.rollback()
        elif (
            hasattr(alter_stmt, "pg_lake_disallow")
            and pglake_fdw_name in alter_stmt.pg_lake_disallow
        ):
            assert f"command not supported for {pglake_fdw_name} tables" in error
            pg_conn.rollback()
        elif hasattr(alter_stmt, "other_disallow"):
            assert "not supported" in error
            pg_conn.rollback()
        else:
            assert error is None
            run_command("SELECT * FROM test_ddl.fdw", pg_conn)

    pg_conn.commit()

    cur = pg_conn.cursor()
    cur.execute("DROP SCHEMA test_ddl,another_ddl_schema CASCADE;")
    pg_conn.commit()


def test_column_mismatch(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_column_mismatch/data.parquet"

    run_command(
        f"""
        COPY (SELECT s as a, 0::text as b , 1::double precision as c FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT 'parquet');
    """,
        pg_conn,
    )

    # create the same table as an fdw and heap table
    run_command(
        """
                CREATE SCHEMA test_column_mismatch;

                CREATE FOREIGN TABLE test_column_mismatch.fdw (a int, b text) server pg_lake OPTIONS (path '{}');

    """.format(
            url, url
        ),
        pg_conn,
    )

    pg_conn.commit()

    fdw_cur = pg_conn.cursor()

    # the file has more columns than the table
    fdw_cur.execute("SELECT * FROM test_column_mismatch.fdw")
    fdw_result = fdw_cur.fetchone()
    assert len(fdw_result) == 2

    # now, add one more column and show that we can see the command
    run_command(
        "ALTER TABLE test_column_mismatch.fdw ADD COLUMN c double precision", pg_conn
    )

    # the file has 3 columns
    fdw_cur.execute("SELECT * FROM test_column_mismatch.fdw")
    fdw_result = fdw_cur.fetchone()
    assert len(fdw_result) == 3

    # now, the table has one additional columnm, but querying only 3 columns should work fine
    run_command("ALTER TABLE test_column_mismatch.fdw ADD COLUMN d text", pg_conn)

    # the file has 3 columns, so querying will not work
    try:
        fdw_cur.execute("SELECT a, b, c, d FROM test_column_mismatch.fdw")

        # we don't expect to get here
        assert False
    except psycopg2.errors.FeatureNotSupported as e:
        assert 'Binder Error: Referenced column "d" not found in FROM clause!' in str(e)
        pg_conn.rollback()

    # casting into wrong types should also fail
    try:
        fdw_cur.execute("SELECT a::timestamp FROM test_column_mismatch.fdw")

        # we don't expect to get here
        assert False
    except psycopg2.errors.CannotCoerce as e:
        assert "cannot cast type integer to timestamp without time zone" in str(e)
        pg_conn.rollback()

    # ok, so lets drop one column, so that we have 2 columns left
    # and the file has 3 columns
    run_command("ALTER TABLE test_column_mismatch.fdw DROP COLUMN a", pg_conn)
    pg_conn.commit()

    # the file has 3 columns, but the table has 2
    # we still expect this to work, but due to some limitations
    # around read_parquet/csv/json() to handle this
    try:
        fdw_cur.execute("SELECT b FROM test_column_mismatch.fdw")

        # we don't expect to get here
        assert False
    except psycopg2.errors.FeatureNotSupported as e:
        assert 'Binder Error: table "res" has duplicate column name "b"' in str(e)
        pg_conn.rollback()

    # Cleanup
    cur = pg_conn.cursor()
    cur.execute("DROP SCHEMA test_column_mismatch CASCADE;")
    pg_conn.commit()


def test_alter_table_set_access_method_default(pg_conn, s3, extension):
    # SET ACCESS METHOD DEFAULT was introduced in PostgreSQL 17
    if get_pg_version_num(pg_conn) < 170000:
        return

    # Sanity check that regular SET ACCESS METHOD DEFAULT succeeds
    run_command(
        """
         SET default_table_access_method TO \'heap\';
         CREATE TABLE test_access_method (x int);
         ALTER TABLE test_access_method SET ACCESS METHOD DEFAULT;
    """,
        pg_conn,
    ),

    # Check that changing to Iceberg is unsupported
    error = run_command(
        """
         SET default_table_access_method TO \'iceberg\';
         ALTER TABLE test_access_method SET ACCESS METHOD DEFAULT;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "not supported" in error

    pg_conn.rollback()


def test_alter_table_add_check_constraint_not_enforced(
    pg_conn, s3, extension, with_default_location
):
    # not enforced check constraints were introduced in PostgreSQL 18
    if get_pg_version_num(pg_conn) < 180000:
        return

    # Add a check constraint to an Iceberg table
    run_command(
        """
        CREATE TABLE test_check_constraint (x int) USING iceberg;
        INSERT INTO test_check_constraint VALUES (-1), (1), (2);
        ALTER TABLE test_check_constraint ADD CONSTRAINT check_x_positive CHECK (x > 0) NOT ENFORCED;
    """,
        pg_conn,
    )

    error = run_command(
        "ALTER TABLE test_check_constraint VALIDATE CONSTRAINT check_x_positive;",
        pg_conn,
        raise_error=False,
    )
    assert "cannot validate NOT ENFORCED constraint" in error

    pg_conn.rollback()

import pytest
import psycopg2
import time
from utils_pytest import *
import server_params


def test_simple(superuser_conn, pg_extension_base):
    # Test simple query
    result = run_query(
        "SELECT * FROM extension_base.run_attached($$SELECT 1$$)", superuser_conn
    )
    assert result[0]["command_tag"] == "SELECT 1"


def test_simple_in_db(superuser_conn, pg_extension_base):

    # first, make sure the role is superuser
    result = run_query(
        f"SELECT rolsuper FROM pg_roles where rolname = current_user;", superuser_conn
    )
    assert result[0][0] == True

    # Test simple query on the db/user
    result = run_query(
        f"SELECT * FROM extension_base.run_attached('SELECT 1', '{server_params.PG_DATABASE}')",
        superuser_conn,
    )
    assert result[0]["command_tag"] == "SELECT 1"


def test_non_transactional(superuser_conn, pg_extension_base):
    # Test simple query
    result = run_query(
        "SELECT * FROM extension_base.run_attached($$VACUUM$$)", superuser_conn
    )
    assert result[0]["command_tag"] == "VACUUM"


def test_write(superuser_conn, pg_extension_base):
    run_command("CREATE TABLE test_write (x int, y int)", superuser_conn)
    superuser_conn.commit()

    # Start doing something in the current transaction
    result = run_query("SELECT count(*) FROM test_write", superuser_conn)
    assert result[0]["count"] == 0

    # Test write in subtransaction
    run_command(
        "SELECT * FROM extension_base.run_attached($$INSERT INTO test_write VALUES (1,2)$$)",
        superuser_conn,
    )

    # Confirm that we can immediately see the write
    result = run_query("SELECT count(*) FROM test_write", superuser_conn)
    assert result[0]["count"] == 1

    run_command("DROP TABLE test_write", superuser_conn)
    superuser_conn.rollback()


def test_error_in_worker(superuser_conn, pg_extension_base):
    # Test error handling
    error = run_command(
        "SELECT * FROM extension_base.run_attached($$SELECT 1/0$$)",
        superuser_conn,
        raise_error=False,
    )
    assert "division" in error

    superuser_conn.rollback()


def test_plpgsql_error(superuser_conn, pg_extension_base):
    error = run_command(
        """
        DO $$
        BEGIN
            PERFORM FROM extension_base.run_attached('SELECT 1/0');
        END$$ LANGUAGE plpgsql;
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "division" in error

    superuser_conn.rollback()


def test_parent_cancellation(superuser_conn, pg_extension_base):
    error = run_command(
        """SELECT 8765
						   FROM extension_base.run_attached($$
						       select pg_cancel_backend(pid), pg_sleep(10) from pg_stat_activity where query like 'SELECT 8765%'
						   $$);
	""",
        superuser_conn,
        raise_error=False,
    )
    assert "canceling" in error

    superuser_conn.rollback()

    result = run_query(
        """
		select count(*) from pg_stat_activity where backend_type = 'pg_extension_base attached worker'
	""",
        superuser_conn,
    )
    assert result[0]["count"] == 0

    superuser_conn.rollback()


def test_returning_simple(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 1 as a, 2 as b$$) AS t(a int, b int)",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["a"] == 1
    assert result[0]["b"] == 2


def test_returning_multiple_rows(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT generate_series(1,5) as x$$) AS t(x int)",
        superuser_conn,
    )
    assert len(result) == 5
    assert [r["x"] for r in result] == [1, 2, 3, 4, 5]


def test_returning_nulls(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 1 as a, NULL::text as b$$) AS t(a int, b text)",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["a"] == 1
    assert result[0]["b"] is None


def test_returning_various_types(superuser_conn, pg_extension_base):
    result = run_query(
        """SELECT * FROM extension_base.run_attached_returning(
            $$SELECT 42 as i, 3.14::float8 as f, true as b, 'hello'::text as t$$
        ) AS t(i int, f float8, b bool, t text)""",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["i"] == 42
    assert abs(result[0]["f"] - 3.14) < 0.001
    assert result[0]["b"] == True
    assert result[0]["t"] == "hello"


def test_returning_empty(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 1 as a WHERE false$$) AS t(a int)",
        superuser_conn,
    )
    assert len(result) == 0


def test_returning_error(superuser_conn, pg_extension_base):
    error = run_command(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 1/0$$) AS t(x int)",
        superuser_conn,
        raise_error=False,
    )
    assert "division" in error

    superuser_conn.rollback()


def test_returning_array(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT ARRAY[1,2,3] as a, ARRAY['x','y'] as b$$) AS t(a int[], b text[])",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["a"] == [1, 2, 3]
    assert result[0]["b"] == ["x", "y"]


def test_returning_composite(superuser_conn, pg_extension_base):
    run_command("CREATE TYPE test_composite AS (x int, y text)", superuser_conn)
    superuser_conn.commit()

    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT ROW(1, 'hello')::test_composite as c$$) AS t(c test_composite)",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["c"] == "(1,hello)"

    run_command("DROP TYPE test_composite", superuser_conn)
    superuser_conn.commit()


def test_returning_jsonb(superuser_conn, pg_extension_base):
    result = run_query(
        """SELECT * FROM extension_base.run_attached_returning(
            $$SELECT '{"key": "value", "num": 42}'::jsonb as j$$
        ) AS t(j jsonb)""",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["j"] == {"key": "value", "num": 42}


def test_returning_bytea(superuser_conn, pg_extension_base):
    result = run_query(
        r"SELECT * FROM extension_base.run_attached_returning($$SELECT '\x deadbeef'::bytea as b$$) AS t(b bytea)",
        superuser_conn,
    )
    assert len(result) == 1
    assert bytes(result[0]["b"]) == bytes.fromhex("deadbeef")


def test_returning_nested_array(superuser_conn, pg_extension_base):
    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT ARRAY[ARRAY[1,2],ARRAY[3,4]] as a$$) AS t(a int[])",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["a"] == [[1, 2], [3, 4]]


def test_returning_with_notice(superuser_conn, pg_extension_base):
    run_command(
        """CREATE OR REPLACE FUNCTION notice_and_return()
        RETURNS TABLE(x int, t text) AS $$
        BEGIN
            RAISE NOTICE 'hello from worker';
            RETURN QUERY SELECT 1, 'one';
            RAISE NOTICE 'second notice';
            RETURN QUERY SELECT 2, 'two';
        END$$ LANGUAGE plpgsql""",
        superuser_conn,
    )
    superuser_conn.commit()

    # clear any prior notices
    superuser_conn.notices.clear()

    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT * FROM notice_and_return()$$) AS t(x int, t text)",
        superuser_conn,
    )
    assert len(result) == 2
    assert result[0]["x"] == 1
    assert result[0]["t"] == "one"
    assert result[1]["x"] == 2
    assert result[1]["t"] == "two"

    # verify notices were forwarded to the client
    notices = [
        n
        for n in superuser_conn.notices
        if "hello from worker" in n or "second notice" in n
    ]
    assert len(notices) == 2

    run_command("DROP FUNCTION notice_and_return()", superuser_conn)
    superuser_conn.commit()


def test_returning_with_warning(superuser_conn, pg_extension_base):
    run_command(
        """CREATE OR REPLACE FUNCTION warn_and_return()
        RETURNS int AS $$
        BEGIN
            RAISE WARNING 'this is a warning';
            RETURN 42;
        END$$ LANGUAGE plpgsql""",
        superuser_conn,
    )
    superuser_conn.commit()

    superuser_conn.notices.clear()

    result = run_query(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT * FROM warn_and_return()$$) AS t(x int)",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["x"] == 42

    warnings = [n for n in superuser_conn.notices if "this is a warning" in n]
    assert len(warnings) == 1

    run_command("DROP FUNCTION warn_and_return()", superuser_conn)
    superuser_conn.commit()


def test_run_attached_with_notice(superuser_conn, pg_extension_base):
    run_command(
        """CREATE OR REPLACE FUNCTION notice_side_effect()
        RETURNS void AS $$
        BEGIN
            RAISE NOTICE 'side effect notice';
        END$$ LANGUAGE plpgsql""",
        superuser_conn,
    )
    superuser_conn.commit()

    superuser_conn.notices.clear()

    result = run_query(
        "SELECT * FROM extension_base.run_attached($$SELECT notice_side_effect()$$)",
        superuser_conn,
    )
    assert result[0]["command_tag"] == "SELECT 1"

    notices = [n for n in superuser_conn.notices if "side effect notice" in n]
    assert len(notices) == 1

    run_command("DROP FUNCTION notice_side_effect()", superuser_conn)
    superuser_conn.commit()


def test_returning_fatal_does_not_kill_session(superuser_conn, pg_extension_base):
    # A FATAL error in the worker (e.g. connecting to a non-existent database)
    # should be reported as ERROR to the parent, not kill the session.
    error = run_command(
        "SELECT * FROM extension_base.run_attached_returning("
        "$$SELECT 1$$, 'nonexistent_db_12345') AS t(x int)",
        superuser_conn,
        raise_error=False,
    )
    assert error is not None
    assert "nonexistent_db_12345" in error or "does not exist" in error

    superuser_conn.rollback()

    # the session should still be alive
    result = run_query("SELECT 1 as alive", superuser_conn)
    assert result[0]["alive"] == 1


def test_run_attached_fatal_does_not_kill_session(superuser_conn, pg_extension_base):
    # Same for the non-returning path
    error = run_command(
        "SELECT * FROM extension_base.run_attached("
        "$$SELECT 1$$, 'nonexistent_db_12345')",
        superuser_conn,
        raise_error=False,
    )
    assert error is not None
    assert "nonexistent_db_12345" in error or "does not exist" in error

    superuser_conn.rollback()

    result = run_query("SELECT 1 as alive", superuser_conn)
    assert result[0]["alive"] == 1


def test_returning_in_other_db(superuser_conn, pg_extension_base):
    result = run_query(
        f"SELECT * FROM extension_base.run_attached_returning('SELECT 1 as a', '{server_params.PG_DATABASE}') AS t(a int)",
        superuser_conn,
    )
    assert len(result) == 1
    assert result[0]["a"] == 1


def test_nologin_role(superuser_conn, pg_extension_base):
    run_command("CREATE ROLE no_login nologin superuser ", superuser_conn)
    current_user = run_query("SELECT user", superuser_conn)[0][0]
    run_command(f"GRANT no_login TO {current_user}", superuser_conn)
    superuser_conn.commit()

    error = run_command(
        """
    set role no_login;
	select * FROM extension_base.run_attached($$SELECT 1$$);
	""",
        superuser_conn,
        raise_error=False,
    )

    # if we are pg16, we expect this to error; if we are not, then we expect this to succeed and return the results
    if superuser_conn.server_version < 170000:
        assert "NOLOGIN" in str(error)
    else:
        assert error is None

    superuser_conn.rollback()

    # cleanup
    run_command("DROP ROLE no_login", superuser_conn)
    superuser_conn.commit()


def test_returning_column_count_mismatch(superuser_conn, pg_extension_base):
    # Query returns 2 columns but AS clause expects 1
    error = run_command(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 1 as a, 2 as b$$) AS t(a int)",
        superuser_conn,
        raise_error=False,
    )
    assert error is not None
    assert "column" in error

    superuser_conn.rollback()

    # session should still be alive
    result = run_query("SELECT 1 as alive", superuser_conn)
    assert result[0]["alive"] == 1


def test_returning_type_mismatch(superuser_conn, pg_extension_base):
    # Query returns text but AS clause expects int
    error = run_command(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 'hello'::text as a$$) AS t(a int)",
        superuser_conn,
        raise_error=False,
    )
    assert error is not None
    assert (
        "type" in error.lower()
        or "mismatch" in error.lower()
        or "text" in error
        or "integer" in error
    )

    superuser_conn.rollback()

    # session should still be alive
    result = run_query("SELECT 1 as alive", superuser_conn)
    assert result[0]["alive"] == 1


def test_returning_more_columns_expected(superuser_conn, pg_extension_base):
    # Query returns 1 column but AS clause expects 3
    error = run_command(
        "SELECT * FROM extension_base.run_attached_returning($$SELECT 1 as a$$) AS t(a int, b int, c int)",
        superuser_conn,
        raise_error=False,
    )
    assert error is not None
    assert "column" in error

    superuser_conn.rollback()

    # session should still be alive
    result = run_query("SELECT 1 as alive", superuser_conn)
    assert result[0]["alive"] == 1

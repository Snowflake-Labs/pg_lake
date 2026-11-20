import io
import re
import socket
import subprocess
import threading
import time

import psycopg2
import pytest
from utils_pytest import *

# Tables of the pg_lake_remote foreign data wrapper are served by an external
# query engine: any server that speaks the PostgreSQL wire protocol and runs
# DuckDB SQL. Most tests use the test pgduck_server itself as that engine, and
# create the remote tables in this schema of its DuckDB database.
ENGINE_SCHEMA = "ext_engine_remote"

ENGINE_CONNINFO = (
    f"host={server_params.PGDUCK_UNIX_DOMAIN_PATH} port={server_params.PGDUCK_PORT}"
)

# Other tests use the test PostgreSQL server as the engine, which runs the
# simple queries they send, and which makes these remote users authenticate
# with a password.
PG_ENGINE_SCHEMA = "ext_engine_pg_remote"
PG_ENGINE_READER = "ext_engine_reader"
PG_ENGINE_OWNER = "ext_engine_owner"
PG_ENGINE_PASSWORD = "it's a back\\slash"


def quote_literal(value):
    return "'" + value.replace("'", "''") + "'"


@pytest.fixture(scope="module")
def engine_tables(pgduck_conn):
    run_command(
        f"""
        CREATE SCHEMA {ENGINE_SCHEMA};

        -- the columns are in a different order than in the foreign table
        CREATE TABLE {ENGINE_SCHEMA}.events (label varchar, id integer, amount double);
        INSERT INTO {ENGINE_SCHEMA}.events
            SELECT 'event ' || i, i, i * 1.5 FROM range(1, 101) t(i);

        CREATE TABLE {ENGINE_SCHEMA}.users (id integer, name varchar);
        INSERT INTO {ENGINE_SCHEMA}.users SELECT i, 'user ' || i FROM range(1, 11) t(i);

        -- two columns of the same type, swapped compared to the foreign table
        CREATE TABLE {ENGINE_SCHEMA}.swapped (b integer, extra varchar, a integer);
        INSERT INTO {ENGINE_SCHEMA}.swapped SELECT i * 10, 'x', i FROM range(1, 4) t(i);

        -- takes minutes unless the engine cancels it, in constant memory
        CREATE VIEW {ENGINE_SCHEMA}.slow AS SELECT range AS x FROM range(1000000000000);
    """,
        pgduck_conn,
    )
    pgduck_conn.commit()

    yield

    run_command(f"DROP SCHEMA {ENGINE_SCHEMA} CASCADE", pgduck_conn)
    pgduck_conn.commit()


@pytest.fixture(scope="module")
def engine_server(superuser_conn, extension, engine_tables, app_user):
    run_command(
        f"""
        CREATE SERVER ext_engine FOREIGN DATA WRAPPER pg_lake_remote
            OPTIONS (conninfo '{ENGINE_CONNINFO}');

        -- the engine uses trust authentication
        CREATE USER MAPPING FOR PUBLIC SERVER ext_engine
            OPTIONS (password_required 'false');

        CREATE SCHEMA ext_engine;
        GRANT USAGE ON SCHEMA ext_engine TO {app_user};

        CREATE FOREIGN TABLE ext_engine.events (id int, amount float8, label text)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'events');

        CREATE FOREIGN TABLE ext_engine.users (id int, name text)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');

        CREATE FOREIGN TABLE ext_engine.slow (x bigint)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'slow');

        -- writes are refused by the table, not by missing privileges
        GRANT ALL ON ALL TABLES IN SCHEMA ext_engine TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP SCHEMA ext_engine CASCADE; DROP SERVER ext_engine CASCADE;",
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module")
def pg_engine(installcheck, superuser_conn, engine_server, app_user):
    """
    The test PostgreSQL server as an external engine. Its remote users have to
    authenticate with a password, so a query returns the remote user only if
    the user and password of the user mapping reached the engine intact.
    """
    if installcheck:
        pytest.skip("Changes pg_hba.conf of the test server")

    hba_file = run_query("SHOW hba_file", superuser_conn)[0][0]
    with open(hba_file) as f:
        original_hba = f.read()

    with open(hba_file, "w") as f:
        f.write(
            f"host all {PG_ENGINE_READER},{PG_ENGINE_OWNER} 127.0.0.1/32 scram-sha-256\n"
            + original_hba
        )

    password = quote_literal(PG_ENGINE_PASSWORD)

    try:
        run_command(
            f"""
            SELECT pg_reload_conf();

            CREATE ROLE {PG_ENGINE_READER} LOGIN PASSWORD {password};
            CREATE ROLE {PG_ENGINE_OWNER} LOGIN PASSWORD {password};

            CREATE SCHEMA {PG_ENGINE_SCHEMA};
            CREATE VIEW {PG_ENGINE_SCHEMA}.whoami AS SELECT current_user::text AS name;
            CREATE VIEW {PG_ENGINE_SCHEMA}.slow AS SELECT 1 AS x FROM pg_sleep(600);
            GRANT USAGE ON SCHEMA {PG_ENGINE_SCHEMA} TO {PG_ENGINE_READER}, {PG_ENGINE_OWNER};
            GRANT SELECT ON ALL TABLES IN SCHEMA {PG_ENGINE_SCHEMA}
                TO {PG_ENGINE_READER}, {PG_ENGINE_OWNER};

            CREATE SERVER ext_engine_pg FOREIGN DATA WRAPPER pg_lake_remote
                OPTIONS (conninfo 'host=127.0.0.1 port={server_params.PG_PORT} dbname={server_params.PG_DATABASE}');

            -- no password_required 'false', the engine asks for the password
            CREATE USER MAPPING FOR {app_user} SERVER ext_engine_pg
                OPTIONS (user '{PG_ENGINE_READER}', password {password});

            CREATE FOREIGN TABLE ext_engine.whoami (name text)
                SERVER ext_engine_pg OPTIONS (schema_name '{PG_ENGINE_SCHEMA}', table_name 'whoami');
            CREATE FOREIGN TABLE ext_engine.pg_slow (x int)
                SERVER ext_engine_pg OPTIONS (schema_name '{PG_ENGINE_SCHEMA}', table_name 'slow');
            GRANT SELECT ON ext_engine.whoami, ext_engine.pg_slow TO {app_user};
        """,
            superuser_conn,
        )
        superuser_conn.commit()

        wait_for_password_authentication(PG_ENGINE_READER)

        yield
    finally:
        superuser_conn.rollback()

        with open(hba_file, "w") as f:
            f.write(original_hba)

        run_command(
            f"""
            SELECT pg_reload_conf();
            DROP SERVER IF EXISTS ext_engine_pg CASCADE;
            DROP SCHEMA IF EXISTS {PG_ENGINE_SCHEMA} CASCADE;
            DROP ROLE IF EXISTS {PG_ENGINE_READER}, {PG_ENGINE_OWNER};
        """,
            superuser_conn,
        )
        superuser_conn.commit()


@pytest.fixture(autouse=True)
def fresh_transactions(superuser_conn, pg_conn):
    """Start each test outside a transaction, also after a failed test."""
    rollback(pg_conn, superuser_conn)
    yield


def wait_for_password_authentication(user):
    """Wait until the server has reloaded pg_hba.conf and asks the user for a password."""

    def asks_for_password():
        try:
            psycopg2.connect(
                host="127.0.0.1",
                port=server_params.PG_PORT,
                dbname=server_params.PG_DATABASE,
                user=user,
                password="",
            ).close()
            return False
        except psycopg2.OperationalError as error:
            assert "password" in str(error), error
            return True

    wait_for(asks_for_password, "pg_hba.conf to be reloaded")


def rollback(*conns):
    """End the transactions of connections, which hold locks after a failed test."""
    for conn in conns:
        conn.rollback()


def wait_for(condition, what, timeout=30):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.1)


def current_count(query, conn):
    """Run a count query in a new transaction, so that it sees fresh statistics."""
    conn.rollback()
    count = run_query(query, conn)[0][0]
    conn.rollback()
    return count


def explain(query, conn):
    result = run_query(f"EXPLAIN (VERBOSE, COSTS OFF) {query}", conn)
    return "\n".join(row[0] for row in result)


def explain_servers(plan):
    return [
        line.strip().removeprefix("Server: ")
        for line in plan.splitlines()
        if line.strip().startswith("Server: ")
    ]


def set_full_pushdown(full_pushdown, conn):
    run_command(
        f"SET pg_lake_table.enable_full_query_pushdown TO {full_pushdown}", conn
    )


def test_server_options(superuser_conn, engine_server):
    error = run_command(
        "CREATE SERVER ext_bad FOREIGN DATA WRAPPER pg_lake_remote",
        superuser_conn,
        raise_error=False,
    )
    assert '"conninfo" option is required' in error
    superuser_conn.rollback()

    error = run_command(
        "CREATE SERVER ext_bad FOREIGN DATA WRAPPER pg_lake_remote OPTIONS (conninfo '')",
        superuser_conn,
        raise_error=False,
    )
    assert 'option "conninfo" cannot be empty' in error
    superuser_conn.rollback()

    # anyone can read the options of a server, so the user and the secrets
    # go into user mappings, and debug options are not offered at all
    server_version = int(run_query("SHOW server_version_num", superuser_conn)[0][0])
    refused = ["user", "password", "sslpassword", "replication"]
    if server_version >= 180000:
        refused += ["oauth_client_secret", "sslkeylogfile"]

    for keyword in refused:
        error = run_command(
            f"""CREATE SERVER ext_bad FOREIGN DATA WRAPPER pg_lake_remote
                   OPTIONS (conninfo 'host=/tmp {keyword}=value')""",
            superuser_conn,
            raise_error=False,
        )
        assert f'"conninfo" option cannot contain "{keyword}"' in error, keyword
        superuser_conn.rollback()

    # client certificates of the server's own
    run_command(
        """CREATE SERVER ext_tls FOREIGN DATA WRAPPER pg_lake_remote
               OPTIONS (conninfo 'host=/tmp sslmode=verify-full sslcert=/certs/c.crt sslkey=/certs/c.key')""",
        superuser_conn,
    )
    superuser_conn.rollback()

    error = run_command(
        """CREATE SERVER ext_bad FOREIGN DATA WRAPPER pg_lake_remote
               OPTIONS (conninfo 'not a conninfo')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'invalid "conninfo" option' in error
    superuser_conn.rollback()

    error = run_command(
        """CREATE SERVER ext_bad FOREIGN DATA WRAPPER pg_lake_remote
               OPTIONS (connifo 'host=/tmp')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'Perhaps you meant the option "conninfo"' in error
    superuser_conn.rollback()

    error = run_command(
        "ALTER SERVER ext_engine OPTIONS (DROP conninfo)",
        superuser_conn,
        raise_error=False,
    )
    assert '"conninfo" option is required' in error
    superuser_conn.rollback()


def test_table_options(superuser_conn, engine_server):
    error = run_command(
        """CREATE FOREIGN TABLE ext_engine.bad (a int) SERVER ext_engine
               OPTIONS (table_name 'users', path 's3://bucket/file.parquet')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'invalid option "path"' in error
    superuser_conn.rollback()

    error = run_command(
        """CREATE FOREIGN TABLE ext_engine.bad (a int) SERVER ext_engine
               OPTIONS (table_name '')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'option "table_name" cannot be empty' in error
    superuser_conn.rollback()

    # the remote table is never named after the foreign table
    error = run_command(
        "CREATE FOREIGN TABLE ext_engine.users_2 (a int) SERVER ext_engine",
        superuser_conn,
        raise_error=False,
    )
    assert '"table_name" option is required for pg_lake_remote tables' in error
    superuser_conn.rollback()

    error = run_command(
        "ALTER FOREIGN TABLE ext_engine.users OPTIONS (DROP table_name)",
        superuser_conn,
        raise_error=False,
    )
    assert '"table_name" option is required for pg_lake_remote tables' in error
    superuser_conn.rollback()


def test_user_mapping_options(superuser_conn, pg_conn, engine_server, app_user):
    error = run_command(
        """CREATE USER MAPPING FOR CURRENT_USER SERVER ext_engine
               OPTIONS (passwd 'secret')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'Perhaps you meant the option "password"' in error
    superuser_conn.rollback()

    error = run_command(
        """CREATE USER MAPPING FOR CURRENT_USER SERVER ext_engine
               OPTIONS (sslmode 'require')""",
        superuser_conn,
        raise_error=False,
    )
    assert 'invalid option "sslmode"' in error
    superuser_conn.rollback()

    run_command(
        f"""
        CREATE SERVER ext_engine_tls FOREIGN DATA WRAPPER pg_lake_remote
            OPTIONS (conninfo '{ENGINE_CONNINFO} sslmode=prefer');
        GRANT USAGE ON FOREIGN SERVER ext_engine_tls TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        # users set their own credentials, including the passphrase of the
        # client key that a superuser set up for them
        run_command(
            """CREATE USER MAPPING FOR CURRENT_USER SERVER ext_engine_tls
                   OPTIONS (user 'me', password 'secret', sslpassword 'passphrase')""",
            pg_conn,
        )
        pg_conn.rollback()

        # client certificate and key files of this server are superuser-only
        for option in ["sslcert '/certs/me.crt'", "sslkey '/certs/me.key'"]:
            error = run_command(
                f"""CREATE USER MAPPING FOR CURRENT_USER SERVER ext_engine_tls
                       OPTIONS (user 'me', {option})""",
                pg_conn,
                raise_error=False,
            )
            assert "sslcert and sslkey are superuser-only" in error
            pg_conn.rollback()

        run_command(
            f"""CREATE USER MAPPING FOR {app_user} SERVER ext_engine_tls
                   OPTIONS (user 'me', sslcert '/certs/me.crt', sslkey '/certs/me.key')""",
            superuser_conn,
        )
        superuser_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command("DROP SERVER ext_engine_tls CASCADE", superuser_conn)
        superuser_conn.commit()


def test_non_superuser_cannot_create_server(pg_conn, engine_server):
    error = run_command(
        f"""CREATE SERVER ext_mine FOREIGN DATA WRAPPER pg_lake_remote
                OPTIONS (conninfo '{ENGINE_CONNINFO}')""",
        pg_conn,
        raise_error=False,
    )
    assert "permission denied for foreign-data wrapper pg_lake_remote" in error
    pg_conn.rollback()


def test_non_superuser_cannot_choose_the_remote_table(
    superuser_conn, pg_conn, engine_server, app_user
):
    # DuckDB reads a name it does not find in its catalog as a file or URL,
    # with the engine's access to the file system and object storage
    run_command(
        f"""
        GRANT USAGE ON FOREIGN SERVER ext_engine TO {app_user};

        CREATE FOREIGN TABLE ext_engine_owned (id int, name text)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');
        ALTER FOREIGN TABLE ext_engine_owned OWNER TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        for options in [
            "table_name '/tmp/secret.csv'",
            f"table_name 's3://{TEST_BUCKET}/secret.parquet'",
            "schema_name '/tmp/secret', table_name 'csv'",
            "table_name 'users'",
        ]:
            error = run_command(
                f"""CREATE FOREIGN TABLE mine (a text) SERVER ext_engine
                        OPTIONS ({options})""",
                pg_conn,
                raise_error=False,
            )
            assert "table_name and schema_name are superuser-only" in error, options
            pg_conn.rollback()

        # nor is the name of the foreign table used instead
        error = run_command(
            'CREATE FOREIGN TABLE "/tmp/secret.csv" (a text) SERVER ext_engine',
            pg_conn,
            raise_error=False,
        )
        assert '"table_name" option is required' in error
        pg_conn.rollback()

        # the owner of a table can rename it, which does not change what it reads
        run_command(
            'ALTER FOREIGN TABLE ext_engine_owned RENAME TO "/tmp/secret.csv"', pg_conn
        )
        assert run_query('SELECT count(*) FROM "/tmp/secret.csv"', pg_conn) == [[10]]

        for alter in [
            "SET table_name '/tmp/secret.csv'",
            "DROP schema_name",
            "SET schema_name '/tmp/secret'",
        ]:
            run_command("SAVEPOINT alter_options", pg_conn)
            error = run_command(
                f'ALTER FOREIGN TABLE "/tmp/secret.csv" OPTIONS ({alter})',
                pg_conn,
                raise_error=False,
            )
            assert "table_name and schema_name are superuser-only" in error, alter
            run_command("ROLLBACK TO SAVEPOINT alter_options", pg_conn)

        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command(
            f"""
            DROP FOREIGN TABLE ext_engine_owned;
            REVOKE USAGE ON FOREIGN SERVER ext_engine FROM {app_user};
        """,
            superuser_conn,
        )
        superuser_conn.commit()


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_scan_and_filter(pg_conn, engine_server, full_pushdown):
    set_full_pushdown(full_pushdown, pg_conn)

    result = run_query(
        "SELECT id, amount, label FROM ext_engine.events WHERE id IN (3, 42) ORDER BY id",
        pg_conn,
    )
    assert result == [[3, 4.5, "event 3"], [42, 63.0, "event 42"]]

    result = run_query("SELECT count(*), sum(id) FROM ext_engine.events", pg_conn)
    assert result == [[100, 5050]]

    query = "SELECT label FROM ext_engine.events WHERE id = 7"
    plan = explain(query, pg_conn)
    scan = "Custom Scan (Query Pushdown)" if full_pushdown else "Foreign Scan"
    assert scan in plan
    assert "Server: ext_engine" in plan
    assert f"FROM {ENGINE_SCHEMA}.events" in plan
    assert '("id" = 7)' in plan
    assert run_query(query, pg_conn) == [["event 7"]]

    pg_conn.rollback()


def test_aggregates_and_joins_pushed_down_whole(pg_conn, engine_server):
    query = """
        SELECT u.name, count(*), sum(e.amount)
        FROM ext_engine.events e JOIN ext_engine.users u ON (e.id % 10 + 1 = u.id)
        WHERE e.id <= 50
        GROUP BY u.name
        ORDER BY u.name
        LIMIT 3
    """
    plan = explain(query, pg_conn)
    assert plan.count("Custom Scan (Query Pushdown)") == 1
    assert "Foreign Scan" not in plan
    assert f"FROM {ENGINE_SCHEMA}.events" in plan
    assert f"FROM {ENGINE_SCHEMA}.users" in plan
    assert "JOIN" in plan and "GROUP BY" in plan

    result = run_query(query, pg_conn)
    assert result == [["user 1", 5, 225.0], ["user 10", 5, 217.5], ["user 2", 5, 157.5]]

    # functions that pg_lake rewrites into its DuckDB functions run on the engine
    query = "SELECT initcap(label) FROM ext_engine.events WHERE id = 1"
    assert_query_pushdownable(query, pg_conn)
    assert run_query(query, pg_conn) == [["Event 1"]]

    # the FDW path pushes the join and aggregate down too
    set_full_pushdown(False, pg_conn)
    plan = explain(
        "SELECT count(*) FROM ext_engine.events e JOIN ext_engine.users u USING (id)",
        pg_conn,
    )
    assert plan.count("Foreign Scan") == 1
    assert "JOIN" in plan and "count(*)" in plan
    assert run_query(
        "SELECT count(*) FROM ext_engine.events e JOIN ext_engine.users u USING (id)",
        pg_conn,
    ) == [[10]]

    pg_conn.rollback()


def test_set_operations(pg_conn, engine_server, with_default_location):
    # tables of one engine are pushed down whole
    queries = [
        (
            "SELECT id FROM ext_engine.events WHERE id <= 2 UNION ALL "
            "SELECT id FROM ext_engine.users WHERE id <= 2 ORDER BY 1",
            [[1], [1], [2], [2]],
        ),
        (
            "SELECT id FROM ext_engine.events INTERSECT "
            "SELECT id + 95 FROM ext_engine.users ORDER BY 1",
            [[96], [97], [98], [99], [100]],
        ),
        (
            "SELECT id FROM ext_engine.users EXCEPT "
            "SELECT id FROM ext_engine.events WHERE id > 3 ORDER BY 1",
            [[1], [2], [3]],
        ),
    ]

    for query, expected in queries:
        plan = explain(query, pg_conn)
        assert plan.count("Custom Scan (Query Pushdown)") == 1, query
        assert "Foreign Scan" not in plan, query
        assert run_query(query, pg_conn) == expected, query

    # with a table of another engine, each side is scanned by its own engine
    run_command(
        """
        CREATE TABLE ext_engine_lake_ids (id int) USING iceberg;
        INSERT INTO ext_engine_lake_ids VALUES (2), (1000);
    """,
        pg_conn,
    )

    query = """
        SELECT id FROM ext_engine.users WHERE id <= 3
        UNION SELECT id FROM ext_engine_lake_ids ORDER BY 1
    """
    plan = explain(query, pg_conn)
    assert "Custom Scan (Query Pushdown)" not in plan
    assert "Foreign Scan on ext_engine.users" in plan
    assert "Table on another query engine" in plan
    assert run_query(query, pg_conn) == [[1], [2], [3], [1000]]

    pg_conn.rollback()


def test_join_with_heap_table(pg_conn, engine_server):
    run_command(
        """
        CREATE TABLE ext_engine_heap (id int, city text);
        INSERT INTO ext_engine_heap VALUES (2, 'Ankara'), (5, 'Izmir'), (500, 'nowhere');
    """,
        pg_conn,
    )

    query = """
        SELECT e.id, e.label, h.city
        FROM ext_engine.events e JOIN ext_engine_heap h USING (id)
        ORDER BY e.id
    """
    plan = explain(query, pg_conn)
    assert "Custom Scan (Query Pushdown)" not in plan
    assert "Foreign Scan on ext_engine.events" in plan
    assert "Server: ext_engine" in plan

    result = run_query(query, pg_conn)
    assert result == [[2, "event 2", "Ankara"], [5, "event 5", "Izmir"]]

    pg_conn.rollback()


def test_join_with_lake_table(pg_conn, engine_server, with_default_location):
    run_command(
        """
        CREATE TABLE ext_engine_lake (id int, city text) USING iceberg;
        INSERT INTO ext_engine_lake VALUES (3, 'Paris'), (4, 'Berlin'), (400, 'nowhere');
    """,
        pg_conn,
    )

    queries = [
        "SELECT e.id, e.label, l.city FROM ext_engine.events e JOIN ext_engine_lake l USING (id) ORDER BY 1",
        # the engines also have to match across subqueries
        "SELECT e.id, e.label, (SELECT city FROM ext_engine_lake l WHERE l.id = e.id) FROM ext_engine.events e WHERE e.id IN (3, 4) ORDER BY 1",
    ]

    for query in queries:
        # each table is scanned by its own engine
        plan = explain(query, pg_conn)
        assert "Custom Scan (Query Pushdown)" not in plan
        assert "Foreign Scan on ext_engine.events" in plan
        assert "Foreign Scan on public.ext_engine_lake" in plan
        assert "Table on another query engine" in plan

        result = run_query(query, pg_conn)
        assert result == [[3, "event 3", "Paris"], [4, "event 4", "Berlin"]]

    # the lake table alone is still pushed down to pgduck_server
    plan = explain("SELECT count(*) FROM ext_engine_lake", pg_conn)
    assert "Custom Scan (Query Pushdown)" in plan
    assert "Server:" not in plan

    pg_conn.rollback()


def test_partitions_and_inheritance(superuser_conn, pg_conn, engine_server, app_user):
    run_command(
        f"""
        -- a partitioned table with a partition on the engine
        CREATE TABLE ext_engine.parted (id int, name text) PARTITION BY RANGE (id);
        CREATE FOREIGN TABLE ext_engine.parted_remote PARTITION OF ext_engine.parted
            FOR VALUES FROM (1) TO (11)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');
        CREATE TABLE ext_engine.parted_local PARTITION OF ext_engine.parted
            FOR VALUES FROM (11) TO (MAXVALUE);
        INSERT INTO ext_engine.parted_local VALUES (11, 'local 11'), (12, 'local 12');

        -- a heap table with a child on the engine
        CREATE TABLE ext_engine.heap_parent (id int, name text);
        INSERT INTO ext_engine.heap_parent VALUES (100, 'parent');
        CREATE FOREIGN TABLE ext_engine.remote_child () INHERITS (ext_engine.heap_parent)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');

        -- a table on the engine with a heap child
        CREATE FOREIGN TABLE ext_engine.remote_parent (id int, name text)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');
        CREATE TABLE ext_engine.heap_child () INHERITS (ext_engine.remote_parent);
        INSERT INTO ext_engine.heap_child VALUES (200, 'child');

        GRANT SELECT ON ext_engine.parted, ext_engine.heap_parent,
            ext_engine.remote_parent TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)

            for table, expected in [
                ("parted", [[12, 78]]),
                ("heap_parent", [[11, 155]]),
                ("remote_parent", [[11, 255]]),
            ]:
                query = f"SELECT count(*), sum(id) FROM ext_engine.{table}"
                plan = explain(query, pg_conn)
                assert "Custom Scan (Query Pushdown)" not in plan, query
                assert "Foreign Scan on ext_engine." in plan, query
                assert run_query(query, pg_conn) == expected, query

            # partition pruning leaves only the engine
            query = "SELECT name FROM ext_engine.parted WHERE id = 3"
            plan = explain(query, pg_conn)
            assert "parted_local" not in plan
            assert run_query(query, pg_conn) == [["user 3"]]

        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command(
            """
            DROP TABLE ext_engine.parted;
            DROP TABLE ext_engine.heap_parent CASCADE;
            DROP FOREIGN TABLE ext_engine.remote_parent CASCADE;
        """,
            superuser_conn,
        )
        superuser_conn.commit()


def test_views(superuser_conn, pg_conn, engine_server, app_user):
    run_command(
        f"""
        CREATE ROLE ext_engine_view_owner;
        GRANT USAGE ON SCHEMA ext_engine TO ext_engine_view_owner;
        GRANT SELECT ON ext_engine.events, ext_engine.users TO ext_engine_view_owner;

        CREATE VIEW ext_engine.event_users AS
            SELECT e.id, e.label, u.name
            FROM ext_engine.events e JOIN ext_engine.users u USING (id);
        ALTER VIEW ext_engine.event_users OWNER TO ext_engine_view_owner;
        GRANT SELECT ON ext_engine.event_users TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        # the tables of the view are read as its owner, so the view alone is
        # pushed down whole
        query = "SELECT count(*), min(label), max(name) FROM ext_engine.event_users"
        plan = explain(query, pg_conn)
        assert plan.count("Custom Scan (Query Pushdown)") == 1
        assert "Foreign Scan" not in plan
        assert run_query(query, pg_conn) == [[10, "event 1", "user 9"]]

        # a query engine gets a query as one user, while here one table is
        # read as the owner of the view and the other as the current user
        query = """
            SELECT v.id, u.name FROM ext_engine.event_users v
            JOIN ext_engine.users u ON (u.id = v.id + 1)
            WHERE v.id <= 2 ORDER BY 1
        """
        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)
            plan = explain(query, pg_conn)
            assert "Custom Scan (Query Pushdown)" not in plan
            if full_pushdown:
                assert "Table read as another user" in plan

            # the join in the view is still pushed down to the engine
            assert plan.count("Foreign Scan") == 2
            assert run_query(query, pg_conn) == [[1, "user 2"], [2, "user 3"]]

        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command(
            """
            DROP VIEW ext_engine.event_users;
            DROP OWNED BY ext_engine_view_owner;
            DROP ROLE ext_engine_view_owner;
        """,
            superuser_conn,
        )
        superuser_conn.commit()


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_cursors(pg_conn, engine_server, full_pushdown):
    pg_conn.notices.clear()

    try:
        check_cursors(pg_conn, full_pushdown)
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_table.enable_full_query_pushdown", pg_conn)
        pg_conn.commit()

    # every scan released its connection
    assert not any("released" in notice for notice in pg_conn.notices), pg_conn.notices


def check_cursors(pg_conn, full_pushdown):
    set_full_pushdown(full_pushdown, pg_conn)

    run_command(
        "DECLARE c CURSOR FOR SELECT id FROM ext_engine.events ORDER BY id", pg_conn
    )
    assert run_query("FETCH 3 FROM c", pg_conn) == [[1], [2], [3]]
    assert run_query("FETCH 2 FROM c", pg_conn) == [[4], [5]]
    run_command("CLOSE c", pg_conn)

    # scrollable cursors are never pushed down whole
    run_command(
        """DECLARE s SCROLL CURSOR FOR
               SELECT id FROM ext_engine.events WHERE id <= 5 ORDER BY id""",
        pg_conn,
    )
    assert run_query("FETCH LAST FROM s", pg_conn) == [[5]]
    assert run_query("FETCH BACKWARD 2 FROM s", pg_conn) == [[4], [3]]
    assert run_query("FETCH ABSOLUTE 1 FROM s", pg_conn) == [[1]]
    run_command("CLOSE s", pg_conn)
    pg_conn.commit()

    # a holdable cursor reads the whole result when the transaction commits
    run_command(
        """DECLARE h CURSOR WITH HOLD FOR
               SELECT id, name FROM ext_engine.users WHERE id <= 3 ORDER BY id""",
        pg_conn,
    )
    pg_conn.commit()
    assert run_query("FETCH ALL FROM h", pg_conn) == [
        [1, "user 1"],
        [2, "user 2"],
        [3, "user 3"],
    ]
    run_command("CLOSE h", pg_conn)
    pg_conn.commit()


def test_tables_on_different_servers(superuser_conn, pg_conn, engine_server, app_user):
    run_command(
        f"""
        CREATE SERVER ext_engine_other FOREIGN DATA WRAPPER pg_lake_remote
            OPTIONS (conninfo '{ENGINE_CONNINFO}');
        CREATE USER MAPPING FOR PUBLIC SERVER ext_engine_other
            OPTIONS (password_required 'false');
        CREATE FOREIGN TABLE ext_engine.other_users (id int, name text)
            SERVER ext_engine_other OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');
        GRANT SELECT ON ext_engine.other_users TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        query = """
            SELECT e.label, u.name
            FROM ext_engine.events e JOIN ext_engine.other_users u USING (id)
            WHERE e.id <= 2 ORDER BY 1
        """

        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)
            plan = explain(query, pg_conn)
            assert "Custom Scan (Query Pushdown)" not in plan
            assert plan.count("Foreign Scan") == 2
            assert sorted(explain_servers(plan)) == ["ext_engine", "ext_engine_other"]

            result = run_query(query, pg_conn)
            assert result == [["event 1", "user 1"], ["event 2", "user 2"]]

        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command("DROP SERVER ext_engine_other CASCADE", superuser_conn)
        superuser_conn.commit()


def test_columns_are_matched_by_name(superuser_conn, pg_conn, engine_server, app_user):
    # the engine has (b, extra, a), the foreign table (a, b)
    run_command(
        f"""
        CREATE FOREIGN TABLE ext_engine.swapped (a int, b int)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'swapped');
        GRANT SELECT ON ext_engine.swapped TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)
            assert run_query(
                "SELECT a, b FROM ext_engine.swapped ORDER BY a", pg_conn
            ) == [[1, 10], [2, 20], [3, 30]]
            assert run_query(
                "SELECT b FROM ext_engine.swapped WHERE a = 2", pg_conn
            ) == [[20]]
            assert run_query(
                "SELECT sum(a), sum(b) FROM ext_engine.swapped", pg_conn
            ) == [[6, 60]]
        pg_conn.rollback()

        # dropped columns and columns added later are matched by name too,
        # now (a, dropped b, extra, b) against (b, extra, a)
        run_command(
            """
            ALTER FOREIGN TABLE ext_engine.swapped DROP COLUMN b;
            ALTER FOREIGN TABLE ext_engine.swapped ADD COLUMN extra text;
            ALTER FOREIGN TABLE ext_engine.swapped ADD COLUMN b int;
        """,
            superuser_conn,
        )
        superuser_conn.commit()

        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)
            assert run_query(
                "SELECT * FROM ext_engine.swapped ORDER BY a", pg_conn
            ) == [[1, "x", 10], [2, "x", 20], [3, "x", 30]]
        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command("DROP FOREIGN TABLE ext_engine.swapped", superuser_conn)
        superuser_conn.commit()


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_columns_are_cast_to_declared_types(
    superuser_conn, pg_conn, engine_server, app_user, full_pushdown
):
    # the engine has (label varchar, id integer, amount double)
    run_command(
        f"""
        CREATE FOREIGN TABLE ext_engine.retyped (id text, amount numeric(10,2), label text)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'events');
        CREATE FOREIGN TABLE ext_engine.unbounded (id int, amount numeric)
            SERVER ext_engine OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'events');
        GRANT SELECT ON ext_engine.retyped, ext_engine.unbounded TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        set_full_pushdown(full_pushdown, pg_conn)

        # the engine compares and sorts the integers as text
        assert run_query("SELECT max(id) FROM ext_engine.retyped", pg_conn) == [["99"]]
        assert run_query(
            "SELECT id FROM ext_engine.retyped ORDER BY id LIMIT 3", pg_conn
        ) == [["1"], ["10"], ["100"]]
        assert run_query(
            "SELECT count(*) FROM ext_engine.retyped WHERE id < '2'", pg_conn
        ) == [[12]]

        # and rounds the doubles to the declared scale
        result = run_query(
            "SELECT amount FROM ext_engine.retyped WHERE id = '3'", pg_conn
        )
        assert str(result[0][0]) == "4.50"

        plan = explain("SELECT id FROM ext_engine.retyped WHERE id = '7'", pg_conn)
        assert "CAST(id AS VARCHAR) AS id" in plan
        assert "CAST(amount AS DECIMAL(10,2)) AS amount" in plan

        # a cast to the type the engine column already has is dropped by
        # DuckDB, so the filter is applied by the scan of the engine table
        plan = explain("SELECT label FROM ext_engine.events WHERE id = 7", pg_conn)
        assert "CAST(id AS INTEGER) AS id" in plan
        assert "Filters: id=7" in plan

        # numeric without a precision has no exact DuckDB type and is not cast
        plan = explain("SELECT amount FROM ext_engine.unbounded WHERE id = 3", pg_conn)
        assert "CAST(id AS INTEGER) AS id, amount FROM" in plan
        assert run_query(
            "SELECT amount FROM ext_engine.unbounded WHERE id = 3", pg_conn
        ) == [[4.5]]

        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command(
            "DROP FOREIGN TABLE ext_engine.retyped, ext_engine.unbounded",
            superuser_conn,
        )
        superuser_conn.commit()


def test_read_only(pg_conn, engine_server, with_default_location):
    parquet_url = f"s3://{TEST_BUCKET}/test_external_engine_table/rows.parquet"
    run_command(
        f"COPY (SELECT 1000 AS id, 1.0::float8 AS amount, 'new' AS label) TO '{parquet_url}'",
        pg_conn,
    )
    pg_conn.commit()

    statements = [
        (
            "INSERT INTO ext_engine.events VALUES (1000, 1.0, 'new')",
            "does not allow inserts",
        ),
        ("UPDATE ext_engine.events SET label = 'changed'", "does not allow updates"),
        ("DELETE FROM ext_engine.events", "does not allow deletes"),
        (f"COPY ext_engine.events FROM '{parquet_url}'", "does not allow inserts"),
        ("TRUNCATE ext_engine.events", 'cannot truncate foreign table "events"'),
    ]

    for statement, expected_error in statements:
        error = run_command(statement, pg_conn, raise_error=False)
        assert expected_error in error, statement
        pg_conn.rollback()

    with pg_conn.cursor() as cursor:
        with pytest.raises(psycopg2.Error, match="does not allow inserts"):
            cursor.copy_expert("COPY ext_engine.events FROM STDIN", io.StringIO(""))
    pg_conn.rollback()

    assert run_query("SELECT count(*) FROM ext_engine.events", pg_conn) == [[100]]
    pg_conn.rollback()


def test_insert_select_into_lake_table(pg_conn, engine_server, with_default_location):
    run_command(
        """
        CREATE TABLE ext_engine_target (id int, label text) USING iceberg;
        CREATE TABLE ext_engine_ids (id int) USING iceberg;
        INSERT INTO ext_engine_ids VALUES (1), (2), (3), (4), (5);
    """,
        pg_conn,
    )

    # pgduck_server, which writes the lake table, cannot read the engine's
    # table, so these read from the engine through the FDW, wherever the
    # engine table is in the statement
    queries = [
        "INSERT INTO ext_engine_target SELECT id, label FROM ext_engine.events WHERE id <= 5",
        """WITH src AS (SELECT id, label FROM ext_engine.events WHERE id <= 5)
           INSERT INTO ext_engine_target SELECT * FROM src""",
        """INSERT INTO ext_engine_target
           WITH src AS (SELECT id, label FROM ext_engine.events WHERE id <= 5)
           SELECT * FROM src""",
        """INSERT INTO ext_engine_target
           SELECT id, 'event ' || id FROM ext_engine_ids
           WHERE id IN (SELECT id FROM ext_engine.events)""",
    ]

    for count, query in enumerate(queries, start=1):
        plan = explain(query, pg_conn)
        assert "Custom Scan (Query Pushdown)" not in plan, query
        assert "Insert on public.ext_engine_target" in plan, query
        assert "Foreign Scan on ext_engine.events" in plan, query

        run_command(query, pg_conn)
        assert run_query(
            "SELECT count(*), min(label), max(id) FROM ext_engine_target", pg_conn
        ) == [[5 * count, "event 1", 5]], query

    # INSERT..SELECT from a lake table is still pushed down
    plan = explain(
        "INSERT INTO ext_engine_target SELECT * FROM ext_engine_target", pg_conn
    )
    assert "Custom Scan (Query Pushdown)" in plan

    pg_conn.rollback()


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_prepared_generic_plan(pg_conn, engine_server, full_pushdown):
    run_command(
        f"""
        SET pg_lake_table.enable_full_query_pushdown TO {full_pushdown};
        SET plan_cache_mode TO force_generic_plan;
        PREPARE ext_label(int) AS SELECT label FROM ext_engine.events WHERE id = $1;
        PREPARE ext_count(int, text) AS
            SELECT count(*) FROM ext_engine.events WHERE id <= $1 AND label <> $2;
    """,
        pg_conn,
    )

    # more executions than it takes to cache a plan
    for i in range(1, 8):
        assert run_query(f"EXECUTE ext_label({i * 3})", pg_conn) == [[f"event {i * 3}"]]
        assert run_query(f"EXECUTE ext_count({i * 10}, 'event 1')", pg_conn) == [
            [i * 10 - 1]
        ]

    plan = explain("EXECUTE ext_label(5)", pg_conn)
    assert "$1" in plan
    assert "Server: ext_engine" in plan

    run_command("DEALLOCATE ext_label; DEALLOCATE ext_count", pg_conn)
    pg_conn.rollback()


def run_query_in_thread(query, conn):
    """Start running a query in a thread, and return the thread and its errors."""
    errors = []

    def run():
        try:
            run_query(query, conn)
        except psycopg2.Error as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, errors


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_cancellation(superuser_conn, pg_conn, engine_server, full_pushdown):
    set_full_pushdown(full_pushdown, pg_conn)
    pg_conn.notices.clear()

    backend_pid = pg_conn.get_backend_pid()
    thread, errors = run_query_in_thread("SELECT sum(x) FROM ext_engine.slow", pg_conn)

    try:
        # cancelling before the query started would leave it running
        wait_for(
            lambda: current_count(
                f"""SELECT count(*) FROM pg_stat_activity
                    WHERE pid = {backend_pid} AND state = 'active'
                      AND wait_event_type = 'Extension'""",
                superuser_conn,
            )
            == 1,
            "the query to wait for the engine",
        )
        # connecting takes milliseconds, after which it waits for the query
        time.sleep(0.5)

        cancelled_at = time.monotonic()
        pg_conn.cancel()
        thread.join(timeout=30)
    finally:
        # keep the engine from running the query for minutes
        while thread.is_alive():
            pg_conn.cancel()
            thread.join(timeout=5)

    assert len(errors) == 1
    assert isinstance(errors[0], psycopg2.errors.QueryCanceled)

    # the engine stopped the query when it got the cancel request, instead of
    # us giving up on it after the cleanup timeout
    assert time.monotonic() - cancelled_at < 15
    assert not any("cancel request" in notice for notice in pg_conn.notices)
    pg_conn.rollback()

    # the session can query the engine again
    start = time.time()
    assert run_query("SELECT count(*) FROM ext_engine.events", pg_conn) == [[100]]
    assert time.time() - start < 10
    pg_conn.rollback()


def test_connecting_can_be_interrupted(
    superuser_conn, pg_conn, engine_server, app_user
):
    # a server that accepts the connection but never answers, like a host
    # whose network drops the packets
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    port = listener.getsockname()[1]

    try:
        # without an interruptible connection, connect_timeout would end it
        run_command(
            f"""
            CREATE SERVER ext_engine_hang FOREIGN DATA WRAPPER pg_lake_remote
                OPTIONS (conninfo 'host=127.0.0.1 port={port} connect_timeout=30');
            CREATE USER MAPPING FOR PUBLIC SERVER ext_engine_hang
                OPTIONS (password_required 'false');
            CREATE FOREIGN TABLE ext_engine.hang (id int)
                SERVER ext_engine_hang OPTIONS (table_name 'hang');
            GRANT SELECT ON ext_engine.hang TO {app_user};
        """,
            superuser_conn,
        )
        superuser_conn.commit()

        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)
            run_command("SET statement_timeout TO '1s'", pg_conn)

            start = time.monotonic()
            error = run_command(
                "SELECT * FROM ext_engine.hang", pg_conn, raise_error=False
            )
            assert "canceling statement due to statement timeout" in error
            assert time.monotonic() - start < 15
            pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        listener.close()
        run_command("DROP SERVER ext_engine_hang CASCADE", superuser_conn)
        superuser_conn.commit()


def test_explain(pg_conn, engine_server):
    query = "SELECT id, label FROM ext_engine.events WHERE amount > 140"

    plan = explain(query, pg_conn)
    assert "Engine: DuckDB" in plan
    assert "Server: ext_engine" in plan
    assert (
        "FROM (SELECT CAST(id AS INTEGER) AS id, CAST(amount AS DOUBLE) AS amount, "
        f"CAST(label AS VARCHAR) AS label FROM {ENGINE_SCHEMA}.events)" in plan
    ), plan
    # there are no files that pg_lake knows of
    assert "Data Files Scanned" not in plan

    # the engine explains its part of the query
    assert "SEQ_SCAN" in plan
    assert "Filters: amount>140.0" in plan

    result = run_query(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {query}", pg_conn)
    assert find_key_in_json(result[0][0], "Server") == "ext_engine"
    assert ENGINE_SCHEMA in fetch_remote_sql(result)

    pg_conn.rollback()


def test_user_mapping(superuser_conn, pg_conn, engine_server, app_user):
    run_command(
        f"""
        CREATE SERVER ext_engine_auth FOREIGN DATA WRAPPER pg_lake_remote
            OPTIONS (conninfo '{ENGINE_CONNINFO}');
        GRANT USAGE ON FOREIGN SERVER ext_engine_auth TO {app_user};
        CREATE FOREIGN TABLE ext_engine.auth_users (id int, name text)
            SERVER ext_engine_auth OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');
        GRANT SELECT ON ext_engine.auth_users TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    query = "SELECT count(*) FROM ext_engine.auth_users"

    try:
        # a superuser needs no user mapping
        assert run_query(query, superuser_conn) == [[10]]
        superuser_conn.commit()

        # a non-superuser needs a password
        error = run_command(query, pg_conn, raise_error=False)
        assert "password is required" in error
        assert "Non-superusers must provide a password in the user mapping" in error
        pg_conn.rollback()

        # which the engine has to ask for
        run_command(
            f"""CREATE USER MAPPING FOR {app_user} SERVER ext_engine_auth
                    OPTIONS (user 'remote_user', password 'secret')""",
            superuser_conn,
        )
        superuser_conn.commit()

        error = run_command(query, pg_conn, raise_error=False)
        assert "password is required" in error
        assert "does not request a password" in error
        pg_conn.rollback()

        # only a superuser can lift that requirement
        error = run_command(
            f"""ALTER USER MAPPING FOR {app_user} SERVER ext_engine_auth
                    OPTIONS (ADD password_required 'false')""",
            pg_conn,
            raise_error=False,
        )
        assert "password_required=false is superuser-only" in error
        pg_conn.rollback()

        run_command(
            f"""ALTER USER MAPPING FOR {app_user} SERVER ext_engine_auth
                    OPTIONS (ADD password_required 'false')""",
            superuser_conn,
        )
        superuser_conn.commit()

        assert run_query(query, pg_conn) == [[10]]
        pg_conn.rollback()

        # credentials with quotes and backslashes still form a valid
        # conninfo, also when the server conninfo is a URI (the engine does
        # not check them, test_engine_credentials does)
        run_command(
            f"""
            ALTER SERVER ext_engine_auth OPTIONS (SET conninfo
                'postgresql:///?host={server_params.PGDUCK_UNIX_DOMAIN_PATH}&port={server_params.PGDUCK_PORT}');
            CREATE USER MAPPING FOR CURRENT_USER SERVER ext_engine_auth
                OPTIONS (user 'o''brien', password 'back\\slash ''quoted''');
        """,
            superuser_conn,
        )
        superuser_conn.commit()

        assert run_query(query, superuser_conn) == [[10]]
        assert run_query(query, pg_conn) == [[10]]
        superuser_conn.commit()
        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command("DROP SERVER ext_engine_auth CASCADE", superuser_conn)
        superuser_conn.commit()


def test_drop_server_and_table(superuser_conn, pgduck_conn, engine_server):
    run_command(
        f"""
        CREATE TABLE {ENGINE_SCHEMA}.dropme (id integer);
        INSERT INTO {ENGINE_SCHEMA}.dropme VALUES (1), (2);
    """,
        pgduck_conn,
    )
    pgduck_conn.commit()

    run_command(
        f"""
        CREATE SERVER ext_engine_drop FOREIGN DATA WRAPPER pg_lake_remote
            OPTIONS (conninfo '{ENGINE_CONNINFO}');
        CREATE USER MAPPING FOR PUBLIC SERVER ext_engine_drop;
        CREATE FOREIGN TABLE ext_engine.dropme_1 (id int)
            SERVER ext_engine_drop OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'dropme');
        CREATE FOREIGN TABLE ext_engine.dropme_2 (id int)
            SERVER ext_engine_drop OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'dropme');
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    assert run_query("SELECT sum(id) FROM ext_engine.dropme_1", superuser_conn) == [[3]]

    # dropping a foreign table leaves the remote table alone
    run_command("DROP FOREIGN TABLE ext_engine.dropme_1", superuser_conn)
    superuser_conn.commit()
    assert run_query(f"SELECT count(*) FROM {ENGINE_SCHEMA}.dropme", pgduck_conn) == [
        [2]
    ]
    pgduck_conn.commit()

    # the server has a dependent table
    error = run_command(
        "DROP SERVER ext_engine_drop", superuser_conn, raise_error=False
    )
    assert (
        "cannot drop server ext_engine_drop because other objects depend on it" in error
    )
    superuser_conn.rollback()

    # a server that cannot be reached reports why
    run_command(
        "ALTER SERVER ext_engine_drop OPTIONS (SET conninfo 'host=/nonexistent port=1')",
        superuser_conn,
    )
    error = run_command(
        "SELECT * FROM ext_engine.dropme_2", superuser_conn, raise_error=False
    )
    assert 'could not connect to server "ext_engine_drop"' in error
    superuser_conn.rollback()

    # a dropped remote table is reported by the engine, for the server
    run_command(f"DROP TABLE {ENGINE_SCHEMA}.dropme", pgduck_conn)
    pgduck_conn.commit()

    error = run_command(
        "SELECT * FROM ext_engine.dropme_2", superuser_conn, raise_error=False
    )
    assert "dropme does not exist" in error
    assert 'remote query on server "ext_engine_drop"' in error
    superuser_conn.rollback()

    run_command("DROP SERVER ext_engine_drop CASCADE", superuser_conn)
    superuser_conn.commit()

    result = run_query(
        """SELECT count(*) FROM pg_class
           WHERE relname = 'dropme_2' AND relnamespace = 'ext_engine'::regnamespace""",
        superuser_conn,
    )
    assert result == [[0]]
    superuser_conn.commit()


def test_pg_dump_and_restore(installcheck, superuser_conn, engine_server, app_user):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    run_command(
        f"""
        CREATE SERVER ext_engine_dump FOREIGN DATA WRAPPER pg_lake_remote
            OPTIONS (conninfo '{ENGINE_CONNINFO}');
        CREATE USER MAPPING FOR PUBLIC SERVER ext_engine_dump
            OPTIONS (password_required 'false');
        CREATE USER MAPPING FOR {app_user} SERVER ext_engine_dump
            OPTIONS (user 'o''brien', password 'back\\slash ''quoted''');
        CREATE FOREIGN TABLE ext_engine.dumped (id int, name text)
            SERVER ext_engine_dump OPTIONS (schema_name '{ENGINE_SCHEMA}', table_name 'users');
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        dump = subprocess.check_output(
            ["pg_dump", "--dbname", superuser_conn.dsn, "--schema-only"], text=True
        )
        statements = [
            statement
            for pattern in [
                r"CREATE SERVER ext_engine_dump .*?;\n",
                r"CREATE USER MAPPING FOR \S+ SERVER ext_engine_dump .*?;\n",
                r"CREATE FOREIGN TABLE ext_engine\.dumped .*?;\n",
            ]
            for statement in re.findall(pattern, dump, re.DOTALL)
        ]
        assert len(statements) == 4, dump

        run_command("DROP SERVER ext_engine_dump CASCADE", superuser_conn)
        superuser_conn.commit()

        for statement in statements:
            run_command(statement, superuser_conn)
        superuser_conn.commit()

        assert run_query(
            "SELECT count(*), max(name) FROM ext_engine.dumped", superuser_conn
        ) == [[10, "user 9"]]
        options = run_query(
            f"""SELECT umoptions FROM pg_user_mapping m JOIN pg_roles r ON (r.oid = m.umuser)
                WHERE rolname = '{app_user}'""",
            superuser_conn,
        )[0][0]
        assert sorted(options) == ["password=back\\slash 'quoted'", "user=o'brien"]
        superuser_conn.commit()
    finally:
        rollback(superuser_conn)
        run_command("DROP SERVER IF EXISTS ext_engine_dump CASCADE", superuser_conn)
        superuser_conn.commit()


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_engine_credentials(
    superuser_conn, pg_conn, pg_engine, app_user, full_pushdown
):
    set_full_pushdown(full_pushdown, pg_conn)

    # the engine checked the user and password of the user mapping
    assert run_query("SELECT name FROM ext_engine.whoami", pg_conn) == [
        [PG_ENGINE_READER]
    ]
    pg_conn.rollback()

    run_command(
        f"""ALTER USER MAPPING FOR {app_user} SERVER ext_engine_pg
                OPTIONS (SET password 'wrong')""",
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        error = run_command(
            "SELECT name FROM ext_engine.whoami", pg_conn, raise_error=False
        )
        assert 'could not connect to server "ext_engine_pg"' in error
        assert f'password authentication failed for user "{PG_ENGINE_READER}"' in error
        pg_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command(
            f"""ALTER USER MAPPING FOR {app_user} SERVER ext_engine_pg
                    OPTIONS (SET password {quote_literal(PG_ENGINE_PASSWORD)})""",
            superuser_conn,
        )
        superuser_conn.commit()


def test_view_uses_owner_user_mapping(superuser_conn, pg_conn, pg_engine, app_user):
    password = quote_literal(PG_ENGINE_PASSWORD)
    run_command(
        f"""
        CREATE ROLE ext_engine_pg_view_owner;
        GRANT USAGE ON SCHEMA ext_engine TO ext_engine_pg_view_owner;
        GRANT SELECT ON ext_engine.whoami TO ext_engine_pg_view_owner;
        CREATE USER MAPPING FOR ext_engine_pg_view_owner SERVER ext_engine_pg
            OPTIONS (user '{PG_ENGINE_OWNER}', password {password});

        CREATE VIEW ext_engine.whoami_owner AS SELECT name FROM ext_engine.whoami;
        ALTER VIEW ext_engine.whoami_owner OWNER TO ext_engine_pg_view_owner;
        CREATE VIEW ext_engine.whoami_invoker WITH (security_invoker)
            AS SELECT name FROM ext_engine.whoami;
        GRANT SELECT ON ext_engine.whoami_owner, ext_engine.whoami_invoker TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        for full_pushdown in [True, False]:
            set_full_pushdown(full_pushdown, pg_conn)
            set_full_pushdown(full_pushdown, superuser_conn)

            # like other tables, the engine's tables in a view are read as
            # the owner of the view, with the owner's user mapping
            for query, expected in [
                ("SELECT name FROM ext_engine.whoami", [[PG_ENGINE_READER]]),
                ("SELECT name FROM ext_engine.whoami_owner", [[PG_ENGINE_OWNER]]),
                ("SELECT name FROM ext_engine.whoami_invoker", [[PG_ENGINE_READER]]),
                (
                    """SELECT o.name, r.name FROM ext_engine.whoami_owner o,
                              ext_engine.whoami r""",
                    [[PG_ENGINE_OWNER, PG_ENGINE_READER]],
                ),
            ]:
                assert run_query(query, pg_conn) == expected, query
                pg_conn.rollback()

            # also for a superuser, who could connect without a password
            assert run_query(
                "SELECT name FROM ext_engine.whoami_owner", superuser_conn
            ) == [[PG_ENGINE_OWNER]]
            superuser_conn.rollback()
    finally:
        rollback(pg_conn, superuser_conn)
        run_command(
            """
            DROP VIEW ext_engine.whoami_owner, ext_engine.whoami_invoker;
            DROP OWNED BY ext_engine_pg_view_owner;
            DROP ROLE ext_engine_pg_view_owner;
        """,
            superuser_conn,
        )
        superuser_conn.commit()


def test_engine_errors_name_the_server(pg_conn, pg_engine):
    # PostgreSQL lacks the DuckDB functions that pg_lake pushes down
    error = run_command(
        "SELECT initcap(name) FROM ext_engine.whoami", pg_conn, raise_error=False
    )
    assert "function initcap_pg(" in error and "does not exist" in error
    assert 'remote query on server "ext_engine_pg"' in error
    pg_conn.rollback()

    # and returns another query plan than DuckDB
    for full_pushdown in [True, False]:
        set_full_pushdown(full_pushdown, pg_conn)
        error = run_command(
            "EXPLAIN SELECT name FROM ext_engine.whoami", pg_conn, raise_error=False
        )
        assert 'server "ext_engine_pg" did not return a DuckDB query plan' in error
        pg_conn.rollback()


@pytest.mark.parametrize("full_pushdown", [True, False], ids=["pushdown", "fdw"])
def test_cancellation_reaches_the_engine(
    superuser_conn, pg_conn, pg_engine, full_pushdown
):
    set_full_pushdown(full_pushdown, pg_conn)

    remote_query = f"""
        SELECT count(*) FROM pg_stat_activity
        WHERE usename = '{PG_ENGINE_READER}' AND wait_event = 'PgSleep'
    """
    thread, errors = run_query_in_thread("SELECT x FROM ext_engine.pg_slow", pg_conn)

    try:
        wait_for(
            lambda: current_count(remote_query, superuser_conn) == 1,
            "the query to run on the engine",
        )
        pg_conn.cancel()
        thread.join(timeout=30)
    finally:
        while thread.is_alive():
            pg_conn.cancel()
            thread.join(timeout=5)

    assert len(errors) == 1
    assert isinstance(errors[0], psycopg2.errors.QueryCanceled)
    pg_conn.rollback()

    # the engine stopped running it
    wait_for(
        lambda: current_count(remote_query, superuser_conn) == 0,
        "the engine to cancel the query",
    )

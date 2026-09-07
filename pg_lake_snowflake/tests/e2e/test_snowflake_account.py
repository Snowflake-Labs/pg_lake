"""End-to-end tests against a real Snowflake account.

Skipped unless the account is configured in the environment:

    SNOWFLAKE_ACCOUNT     account identifier, e.g. myorg-myaccount
    SNOWFLAKE_TOKEN       programmatic access token or OAuth token
    SNOWFLAKE_DATABASE    database to create the test objects in
    SNOWFLAKE_WAREHOUSE   warehouse that runs the statements
    SNOWFLAKE_ROLE        role to run as (optional)
    SNOWFLAKE_ACCOUNT_URL host, if it is not <account>.snowflakecomputing.com

The tests create their own schema, so the account needs rights to create one.
"""

import io
import os

import pytest
from utils_pytest import *

TEST_SCHEMA = "PGLAKE_SNOWFLAKE_E2E"

account = os.getenv("SNOWFLAKE_ACCOUNT")
token = os.getenv("SNOWFLAKE_TOKEN")
database = os.getenv("SNOWFLAKE_DATABASE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

pytestmark = pytest.mark.skipif(
    not (account and token and database and warehouse),
    reason="no Snowflake account configured in the environment",
)


@pytest.fixture(scope="module")
def sf_account(postgres):
    conn = open_pg_conn()
    conn.autocommit = True

    account_url = os.getenv("SNOWFLAKE_ACCOUNT_URL")
    role = os.getenv("SNOWFLAKE_ROLE")

    server_options = [
        f"account '{account}'",
        f"database '{database}'",
        f"warehouse '{warehouse}'",
        f"schema_name '{TEST_SCHEMA}'",
    ]

    if account_url:
        server_options.append(f"account_url '{account_url}'")
    if role:
        server_options.append(f"role '{role}'")

    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_snowflake CASCADE;
        DROP SERVER IF EXISTS e2e_sf CASCADE;
        CREATE SERVER e2e_sf FOREIGN DATA WRAPPER snowflake
          OPTIONS ({", ".join(server_options)});
        CREATE USER MAPPING FOR CURRENT_USER SERVER e2e_sf OPTIONS (token '{token}');
        """,
        conn,
    )

    execute(conn, f"CREATE SCHEMA IF NOT EXISTS {database}.{TEST_SCHEMA}")

    yield conn

    execute(conn, f"DROP SCHEMA IF EXISTS {database}.{TEST_SCHEMA}")
    run_command("DROP SERVER e2e_sf CASCADE", conn)
    conn.close()


def execute(conn, statement):
    """Run a statement on the account through the extension."""
    escaped = statement.replace("'", "''")
    return run_query(
        f"SELECT lake_snowflake.execute('e2e_sf', '{escaped}') AS result", conn
    )[0]["result"]


def test_connection_reports_the_session(sf_account):
    summary = run_query(
        "SELECT lake_snowflake.test_connection('e2e_sf') AS summary", sf_account
    )[0]["summary"]

    assert "version" in summary
    assert warehouse.upper() in summary.upper()


def test_every_type_survives_the_round_trip(sf_account):
    execute(
        sf_account,
        f"""
        CREATE OR REPLACE TABLE {TEST_SCHEMA}.ALLTYPES AS SELECT
          42::number(10,0) AS n0, 1.25::number(10,2) AS n2, 3.5::float AS f,
          'hi'::varchar AS v, true AS b, '2021-03-19'::date AS d,
          '12:34:56.123'::time AS t,
          '2021-03-19 12:34:56.123'::timestamp_ntz AS tsntz,
          '2021-03-19 12:34:56.123 +0530'::timestamp_tz AS tstz,
          TO_BINARY('DEADBEEF', 'HEX') AS bin,
          PARSE_JSON('{{"a": [1, 2]}}') AS doc,
          NULL::int AS nothing
        """,
    )

    run_command(
        "CREATE FOREIGN TABLE e2e_alltypes () SERVER e2e_sf "
        "OPTIONS (table_name 'ALLTYPES')",
        sf_account,
    )

    row = run_query(
        """
        SELECT n0, n2, f, v, b, d, t, tsntz,
               tstz AT TIME ZONE 'UTC' AS tstz_utc, bin, doc, nothing
        FROM e2e_alltypes
        """,
        sf_account,
    )[0]

    assert row["n0"] == 42
    assert str(row["n2"]) == "1.25"
    assert row["f"] == 3.5
    assert row["v"] == "hi"
    assert row["b"] is True
    assert str(row["d"]) == "2021-03-19"
    assert str(row["t"]) == "12:34:56.123000"
    assert str(row["tsntz"]) == "2021-03-19 12:34:56.123000"
    assert str(row["tstz_utc"]) == "2021-03-19 07:04:56.123000"
    assert bytes(row["bin"]) == b"\xde\xad\xbe\xef"
    assert row["doc"] == {"a": [1, 2]}
    assert row["nothing"] is None

    run_command("DROP FOREIGN TABLE e2e_alltypes", sf_account)


def test_pushdown_matches_local_evaluation(sf_account):
    execute(
        sf_account,
        f"""
        CREATE OR REPLACE TABLE {TEST_SCHEMA}.NUMBERS AS
        SELECT SEQ4() AS id, 'label_' || SEQ4() AS label, SEQ4() * 1.5 AS amount
        FROM TABLE(GENERATOR(ROWCOUNT => 1000))
        """,
    )

    run_command(
        "CREATE FOREIGN TABLE e2e_numbers () SERVER e2e_sf "
        "OPTIONS (table_name 'NUMBERS')",
        sf_account,
    )

    pushed = run_query(
        "SELECT count(*) AS rows, sum(amount) AS total FROM e2e_numbers WHERE id < 100",
        sf_account,
    )[0]

    run_command("SET pg_lake_snowflake.enable_aggregate_pushdown TO off", sf_account)

    local = run_query(
        "SELECT count(*) AS rows, sum(amount) AS total FROM e2e_numbers WHERE id < 100",
        sf_account,
    )[0]

    run_command("RESET pg_lake_snowflake.enable_aggregate_pushdown", sf_account)

    assert pushed["rows"] == local["rows"] == 100
    assert pushed["total"] == local["total"]

    # a limit is answered by Snowflake rather than by reading everything
    plan = run_query(
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT id FROM e2e_numbers LIMIT 5", sf_account
    )
    assert "LIMIT 5" in "\n".join(row[0] for row in plan)

    assert len(run_query("SELECT id FROM e2e_numbers LIMIT 5", sf_account)) == 5

    # more rows than fit in one result partition
    assert (
        run_query(
            "SELECT count(*) AS c FROM (SELECT id FROM e2e_numbers) s", sf_account
        )[0]["c"]
        == 1000
    )

    run_command("DROP FOREIGN TABLE e2e_numbers", sf_account)


def test_analyze_reads_statistics(sf_account):
    execute(
        sf_account,
        f"CREATE OR REPLACE TABLE {TEST_SCHEMA}.SMALL AS "
        f"SELECT SEQ4() AS id FROM TABLE(GENERATOR(ROWCOUNT => 50))",
    )
    run_command(
        "CREATE FOREIGN TABLE e2e_small () SERVER e2e_sf OPTIONS (table_name 'SMALL')",
        sf_account,
    )

    run_command("ANALYZE e2e_small", sf_account)

    reltuples = run_query(
        "SELECT reltuples FROM pg_class WHERE relname = 'e2e_small'", sf_account
    )[0]["reltuples"]

    assert reltuples == 50

    run_command("DROP FOREIGN TABLE e2e_small", sf_account)


def test_hybrid_table_lookup(sf_account):
    """A hybrid table is attached like any other, and a lookup is pushed down."""
    try:
        execute(
            sf_account,
            f"CREATE OR REPLACE HYBRID TABLE {TEST_SCHEMA}.HT "
            f"(id number(10,0) PRIMARY KEY, name varchar(50))",
        )
    except Exception as error:
        pytest.skip(f"hybrid tables are not available on this account: {error}")

    execute(sf_account, f"INSERT INTO {TEST_SCHEMA}.HT VALUES (1, 'one'), (2, 'two')")

    run_command(
        "CREATE FOREIGN TABLE e2e_ht () SERVER e2e_sf OPTIONS (table_name 'HT')",
        sf_account,
    )

    plan = run_query(
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT name FROM e2e_ht WHERE id = 2", sf_account
    )

    assert '"ID" = 2' in "\n".join(row[0] for row in plan)
    assert (
        run_query("SELECT name FROM e2e_ht WHERE id = 2", sf_account)[0]["name"]
        == "two"
    )

    run_command("DROP FOREIGN TABLE e2e_ht", sf_account)


def test_import_foreign_schema(sf_account):
    execute(
        sf_account,
        f"CREATE OR REPLACE TABLE {TEST_SCHEMA}.IMPORTED (a number(5,0), b varchar(10))",
    )

    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS e2e_imported;
        IMPORT FOREIGN SCHEMA "{TEST_SCHEMA}" LIMIT TO (imported)
          FROM SERVER e2e_sf INTO e2e_imported;
        """,
        sf_account,
    )

    columns = run_query(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = 'e2e_imported' AND table_name = 'imported'
        ORDER BY ordinal_position
        """,
        sf_account,
    )

    assert [(row[0], row[1]) for row in columns] == [("a", "integer"), ("b", "text")]

    run_command("DROP SCHEMA e2e_imported CASCADE", sf_account)


def test_writes_round_trip(sf_account):
    execute(
        sf_account,
        f"""
        CREATE OR REPLACE TABLE {TEST_SCHEMA}.WRITTEN (
          id number(10,0), name varchar(50), doc variant, blob binary,
          d date, ts timestamp_ntz
        )
        """,
    )
    run_command(
        "CREATE FOREIGN TABLE e2e_written () SERVER e2e_sf "
        "OPTIONS (table_name 'WRITTEN')",
        sf_account,
    )

    run_command(
        """
        INSERT INTO e2e_written (id, name, doc, blob, d, ts)
        VALUES (1, 'one', '{"a": [1, 2]}', '\\xdeadbeef', '2021-03-19',
                '2021-03-19 12:34:56.123'),
               (2, NULL, NULL, NULL, NULL, NULL);
        INSERT INTO e2e_written (id, name)
        SELECT i, 'row_' || i FROM generate_series(3, 12) i;
        """,
        sf_account,
    )

    rows = run_query("SELECT count(*) AS c FROM e2e_written", sf_account)
    assert rows[0]["c"] == 12

    first = run_query("SELECT * FROM e2e_written WHERE id = 1", sf_account)[0]
    assert first["name"] == "one"
    assert first["doc"] == {"a": [1, 2]}
    assert bytes(first["blob"]) == b"\xde\xad\xbe\xef"
    assert str(first["d"]) == "2021-03-19"
    assert str(first["ts"]) == "2021-03-19 12:34:56.123000"

    # a pushed-down UPDATE, and its row count
    cursor = sf_account.cursor()
    cursor.execute("UPDATE e2e_written SET name = name || '!' WHERE id <= 3")
    assert cursor.rowcount == 3
    cursor.execute("DELETE FROM e2e_written WHERE id > 10")
    assert cursor.rowcount == 2
    cursor.close()

    assert (
        run_query("SELECT name FROM e2e_written WHERE id = 3", sf_account)[0]["name"]
        == "row_3!"
    )
    assert run_query("SELECT count(*) AS c FROM e2e_written", sf_account)[0]["c"] == 10

    # COPY, which goes through the same batched insert
    cursor = sf_account.cursor()
    cursor.copy_expert(
        "COPY e2e_written (id, name) FROM STDIN",
        io.StringIO("100\thundred\n101\thundred-one\n"),
    )
    cursor.close()

    assert run_query("SELECT count(*) AS c FROM e2e_written", sf_account)[0]["c"] == 12

    run_command("TRUNCATE e2e_written", sf_account)

    assert run_query("SELECT count(*) AS c FROM e2e_written", sf_account)[0]["c"] == 0

    run_command("DROP FOREIGN TABLE e2e_written", sf_account)


def test_a_batched_load_sends_one_statement_per_batch(sf_account):
    execute(
        sf_account,
        f"CREATE OR REPLACE TABLE {TEST_SCHEMA}.LOADED (id number(10,0), pad varchar(100))",
    )
    run_command(
        "CREATE FOREIGN TABLE e2e_loaded () SERVER e2e_sf OPTIONS "
        "(table_name 'LOADED', batch_size '250')",
        sf_account,
    )

    run_command(
        """
        INSERT INTO e2e_loaded (id, pad)
        SELECT i, repeat('x', 50) FROM generate_series(1, 1000) i
        """,
        sf_account,
    )

    assert run_query("SELECT count(*) AS c FROM e2e_loaded", sf_account)[0]["c"] == 1000

    run_command("DROP FOREIGN TABLE e2e_loaded", sf_account)

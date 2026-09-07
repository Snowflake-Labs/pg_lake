"""Tests for pg_lake_snowflake against a mock of the Snowflake SQL API.

These run without a Snowflake account: the mock answers the statements the
wrapper sends, which is what makes it possible to assert on the SQL that was
pushed down and on the conversion of every value the API can send. The tests
that need a real account live in tests/e2e.
"""

import pytest
from utils_pytest import *

from mock_snowflake import MockSnowflake, column


@pytest.fixture(scope="function")
def snowflake(postgres):
    """A mock account, a server pointing at it, and a user mapping."""
    mock = MockSnowflake()
    account_url = mock.start()

    conn = open_pg_conn()
    conn.autocommit = True

    run_command(
        """
        CREATE EXTENSION IF NOT EXISTS pg_lake_snowflake CASCADE;
        SET pg_lake_snowflake.allow_plain_http TO on;
        """,
        conn,
    )
    run_command(
        f"""
        CREATE SERVER mock_sf FOREIGN DATA WRAPPER snowflake
          OPTIONS (account 'mock-account', account_url '{account_url}',
                   database 'MOCKDB', schema_name 'PUBLIC', warehouse 'MOCK_WH');
        CREATE USER MAPPING FOR CURRENT_USER SERVER mock_sf
          OPTIONS (token 'mock-token');
        """,
        conn,
    )

    yield mock

    run_command("DROP SERVER IF EXISTS mock_sf CASCADE", conn)
    conn.close()
    mock.stop()


@pytest.fixture(scope="function")
def sf_conn():
    conn = open_pg_conn()
    conn.autocommit = True
    run_command("SET pg_lake_snowflake.allow_plain_http TO on", conn)
    yield conn
    conn.close()


def attach(mock, sf_conn, columns, partitions, table_name="T", local_name="t"):
    """Attach a foreign table whose shape and rows the mock provides."""
    mock.route(f'"{table_name}"', columns, partitions)
    run_command(
        f"CREATE FOREIGN TABLE {local_name} () SERVER mock_sf "
        f"OPTIONS (table_name '{table_name}')",
        sf_conn,
    )


def test_attach_infers_columns(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [
            column("ID", "fixed", precision=10, scale=0),
            column("SMALL_ID", "fixed", precision=5, scale=0),
            column("PRICE", "fixed", precision=12, scale=2),
            column("RATIO", "real"),
            column("LABEL", "text", length=50),
            column("FLAG", "boolean"),
            column("DAY", "date"),
            column("MOMENT", "timestamp_ntz", scale=9),
            column("INSTANT", "timestamp_ltz", scale=9),
            column("ZONED", "timestamp_tz", scale=9),
            column("BLOB", "binary"),
            column("DOC", "variant"),
        ],
        [[]],
    )

    columns = run_query(
        """
        SELECT column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = 't'
        ORDER BY ordinal_position
        """,
        sf_conn,
    )

    assert [(row[0], row[1]) for row in columns] == [
        ("id", "bigint"),
        ("small_id", "integer"),
        ("price", "numeric"),
        ("ratio", "double precision"),
        ("label", "text"),
        ("flag", "boolean"),
        ("day", "date"),
        ("moment", "timestamp without time zone"),
        ("instant", "timestamp with time zone"),
        ("zoned", "timestamp with time zone"),
        ("blob", "bytea"),
        ("doc", "jsonb"),
    ]

    price = [row for row in columns if row[0] == "price"][0]
    assert (price[2], price[3]) == (12, 2)


def test_value_conversion(snowflake, sf_conn):
    """Every wire form the API sends becomes the value it stands for."""
    attach(
        snowflake,
        sf_conn,
        [
            column("N", "fixed", precision=10, scale=2),
            column("R", "real"),
            column("S", "text"),
            column("B", "boolean"),
            column("D", "date"),
            column("T", "time", scale=9),
            column("TS", "timestamp_ntz", scale=9),
            column("TSTZ", "timestamp_tz", scale=9),
            column("BIN", "binary"),
            column("DOC", "variant"),
            column("MISSING", "text"),
        ],
        [
            [
                [
                    "1.25",
                    "3.5",
                    "hello",
                    "true",
                    "18705",
                    "45296.123000000",
                    "1616157296.123000000",
                    "1616137496.123456789 1770",
                    "DEADBEEF",
                    '{"a": [1, 2]}',
                    None,
                ]
            ]
        ],
    )

    row = run_query(
        """
        SELECT n, r, s, b, d, t, ts,
               tstz AT TIME ZONE 'UTC' AS tstz_utc, bin, doc, missing
        FROM t
        """,
        sf_conn,
    )[0]

    assert str(row["n"]) == "1.25"
    assert row["r"] == 3.5
    assert row["s"] == "hello"
    assert row["b"] is True
    assert str(row["d"]) == "2021-03-19"
    assert str(row["t"]) == "12:34:56.123000"
    assert str(row["ts"]) == "2021-03-19 12:34:56.123000"
    # the offset is display only; the instant is what is stored
    assert str(row["tstz_utc"]) == "2021-03-19 07:04:56.123457"
    assert bytes(row["bin"]) == b"\xde\xad\xbe\xef"
    assert row["doc"] == {"a": [1, 2]}
    assert row["missing"] is None


def test_declared_type_wins_over_the_mapping(snowflake, sf_conn):
    """A column declared as something else is converted through its text form."""
    snowflake.route(
        '"T"',
        [column("D", "date"), column("N", "fixed", precision=10, scale=0)],
        [[["18705", "42"]]],
    )
    run_command(
        "CREATE FOREIGN TABLE t (d text, n text) SERVER mock_sf "
        "OPTIONS (table_name 'T')",
        sf_conn,
    )

    row = run_query("SELECT d, n FROM t", sf_conn)[0]

    assert row["d"] == "2021-03-19"
    assert row["n"] == "42"


def test_only_needed_columns_are_selected(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("A", "fixed", precision=5, scale=0), column("B", "text")],
        [[["1", "x"]]],
    )

    run_query("SELECT b FROM t", sf_conn)

    statement = snowflake.statement_containing("FROM")
    assert '"B"' in statement
    assert '"A"' not in statement


def test_quals_are_pushed_down(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("ID", "fixed", precision=5, scale=0), column("NAME", "text")],
        [[["2", "two"]]],
    )

    run_query(
        """
        SELECT name FROM t
        WHERE id = 2 AND name = 'two' AND id IN (1, 2, 3)
          AND name LIKE 't%' AND name IS NOT NULL
        """,
        sf_conn,
    )

    statement = snowflake.statement_containing("WHERE")
    assert '("ID" = 2)' in statement
    assert "('NAME' = 'two')" not in statement
    assert "(\"NAME\" = 'two')" in statement
    assert '("ID" IN (1, 2, 3))' in statement
    assert "(\"NAME\" LIKE 't%')" in statement
    assert '("NAME" IS NOT NULL)' in statement


def test_unsafe_quals_stay_local(snowflake, sf_conn):
    """An ordering comparison over text would follow the wrong collation."""
    attach(snowflake, sf_conn, [column("NAME", "text")], [[["two"]]])

    plan = run_query(
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT name FROM t WHERE name > 'a'", sf_conn
    )
    plan_text = "\n".join(row[0] for row in plan)

    assert "Filter:" in plan_text
    assert "WHERE" not in plan_text


def test_limit_is_pushed_down(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("ID", "fixed", precision=5, scale=0)],
        [[["1"], ["2"]]],
    )

    rows = run_query("SELECT id FROM t LIMIT 2", sf_conn)

    assert len(rows) == 2
    assert "LIMIT 2" in snowflake.statement_containing("FROM")


def test_offset_is_pushed_down_with_the_limit(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("ID", "fixed", precision=5, scale=0)],
        [[["3"]]],
    )

    run_query("SELECT id FROM t LIMIT 1 OFFSET 2", sf_conn)

    assert "LIMIT 1 OFFSET 2" in snowflake.statement_containing("FROM")


def test_aggregates_are_pushed_down(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [
            column("ID", "fixed", precision=5, scale=0),
            column("PRICE", "fixed", precision=12, scale=2),
        ],
        [[["1", "10.00"]]],
    )

    snowflake.route(
        "COUNT(*)",
        [
            column("ID", "fixed", precision=5, scale=0),
            column("COUNT", "fixed", precision=18, scale=0),
            column("SUM", "fixed", precision=38, scale=2),
        ],
        [[["1", "2", "30.00"]]],
    )

    rows = run_query(
        "SELECT id, count(*), sum(price) FROM t GROUP BY id HAVING count(*) > 1",
        sf_conn,
    )

    statement = snowflake.statement_containing("COUNT(*)")
    assert 'SELECT "ID", COUNT(*), SUM("PRICE")' in statement
    assert "GROUP BY 1" in statement
    assert "HAVING ((COUNT(*) > 1))" in statement
    assert rows[0]["count"] == 2
    assert str(rows[0]["sum"]) == "30.00"


def test_avg_over_a_numeric_stays_local(snowflake, sf_conn):
    """Snowflake decides the scale of an AVG by its own rules."""
    attach(
        snowflake,
        sf_conn,
        [column("PRICE", "fixed", precision=12, scale=2)],
        [[["10.00"], ["21.00"]]],
    )

    rows = run_query("SELECT avg(price) FROM t", sf_conn)

    assert "AVG" not in snowflake.statement_containing("FROM")
    assert float(rows[0]["avg"]) == 15.5


def test_aggregate_pushdown_can_be_turned_off(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("ID", "fixed", precision=5, scale=0)],
        [[["1"], ["2"]]],
    )

    run_command("SET pg_lake_snowflake.enable_aggregate_pushdown TO off", sf_conn)
    rows = run_query("SELECT count(*) FROM t", sf_conn)

    assert "COUNT(*)" not in snowflake.statement_containing("FROM")
    assert rows[0]["count"] == 2


def test_a_running_statement_is_polled(snowflake, sf_conn):
    """A statement Snowflake answers with 202 is polled until it is ready."""
    snowflake.route(
        '"T"',
        [column("ID", "fixed", precision=5, scale=0)],
        [[["7"]]],
        asynchronous=True,
    )
    run_command(
        "CREATE FOREIGN TABLE t (id bigint) SERVER mock_sf OPTIONS (table_name 'T')",
        sf_conn,
    )

    rows = run_query("SELECT id FROM t", sf_conn)

    assert [row["id"] for row in rows] == [7]


def test_partitions_are_fetched_as_the_scan_reaches_them(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("ID", "fixed", precision=5, scale=0)],
        [[["1"], ["2"]], [["3"]], [["4"], ["5"]]],
    )

    rows = run_query("SELECT id FROM t ORDER BY id", sf_conn)

    assert [row["id"] for row in rows] == [1, 2, 3, 4, 5]


def test_an_error_carries_the_snowflake_code(snowflake, sf_conn):
    snowflake.route(
        '"MISSING"',
        [],
        [],
        status=422,
        error={
            "code": "002003",
            "sqlState": "42S02",
            "message": "SQL compilation error: Object does not exist",
        },
    )

    error = run_command(
        "CREATE FOREIGN TABLE missing () SERVER mock_sf OPTIONS (table_name 'MISSING')",
        sf_conn,
        raise_error=False,
    )

    assert "Object does not exist" in error
    assert "002003" in error
    assert "42S02" in error


def test_every_request_is_authorized(snowflake, sf_conn):
    attach(snowflake, sf_conn, [column("ID", "fixed", precision=5, scale=0)], [[["1"]]])

    run_query("SELECT id FROM t", sf_conn)

    assert snowflake.requests_without_authorization == 0


def test_import_foreign_schema(snowflake, sf_conn):
    snowflake.route(
        "INFORMATION_SCHEMA.TABLES",
        [column("TABLE_NAME", "text")],
        [[["ORDERS"], ["MixedCase"]]],
    )
    snowflake.route(
        '"ORDERS"',
        [column("O_ID", "fixed", precision=10, scale=0), column("O_NAME", "text")],
        [[]],
    )
    snowflake.route(
        '"MixedCase"',
        [column("Id", "fixed", precision=5, scale=0)],
        [[]],
    )

    run_command(
        """
        CREATE SCHEMA imported;
        IMPORT FOREIGN SCHEMA "PUBLIC" FROM SERVER mock_sf INTO imported;
        """,
        sf_conn,
    )

    tables = run_query(
        """
        SELECT c.relname, o.option_value AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_options_to_table((SELECT ftoptions FROM pg_foreign_table
                                       WHERE ftrelid = c.oid)) o
             ON o.option_name = 'table_name'
        WHERE n.nspname = 'imported'
        ORDER BY c.relname
        """,
        sf_conn,
    )

    assert [(row[0], row[1]) for row in tables] == [
        ("MixedCase", "MixedCase"),
        ("orders", "ORDERS"),
    ]

    # the mixed-case column keeps its Snowflake spelling in an option
    options = run_query(
        """
        SELECT a.attname, o.option_value
        FROM pg_attribute a
        LEFT JOIN pg_options_to_table(a.attfdwoptions) o
             ON o.option_name = 'column_name'
        WHERE a.attrelid = 'imported."MixedCase"'::regclass AND a.attnum > 0
        """,
        sf_conn,
    )

    assert [(row[0], row[1]) for row in options] == [("Id", "Id")]

    run_command("DROP SCHEMA imported CASCADE", sf_conn)


def test_execute_returns_the_first_value(snowflake, sf_conn):
    snowflake.route(
        "CURRENT_VERSION",
        [column("VERSION", "text")],
        [[["9.9.9"]]],
    )

    value = run_query(
        "SELECT lake_snowflake.execute('mock_sf', 'SELECT CURRENT_VERSION()') AS v",
        sf_conn,
    )[0]["v"]

    assert value == "9.9.9"


def test_analyze_uses_row_sampling(snowflake, sf_conn):
    attach(
        snowflake,
        sf_conn,
        [column("ID", "fixed", precision=5, scale=0)],
        [[["1"], ["2"], ["3"]]],
    )
    snowflake.route(
        "COUNT(*)", [column("COUNT", "fixed", precision=18, scale=0)], [[["3"]]]
    )

    run_command("ANALYZE t", sf_conn)

    assert "SAMPLE ROW" in snowflake.statement_containing("SAMPLE")
    reltuples = run_query("SELECT reltuples FROM pg_class WHERE relname = 't'", sf_conn)
    assert reltuples[0]["reltuples"] == 3


@pytest.mark.parametrize(
    "options,message",
    [
        ("account 'a', nonsense 'x'", 'invalid option "nonsense"'),
        ("warehouse 'wh'", '"account" or an "account_url"'),
        ("account_url 'ftp://host'", "must start with https"),
        ("account 'a.b'", "not a host name"),
        ("account 'a', statement_timeout 'soon'", "non-negative"),
    ],
)
def test_invalid_server_options(snowflake, sf_conn, options, message):
    error = run_command(
        f"CREATE SERVER bad FOREIGN DATA WRAPPER snowflake OPTIONS ({options})",
        sf_conn,
        raise_error=False,
    )

    assert message in error


def test_plain_http_needs_the_setting(snowflake, sf_conn):
    run_command("SET pg_lake_snowflake.allow_plain_http TO off", sf_conn)

    error = run_command(
        "CREATE SERVER insecure FOREIGN DATA WRAPPER snowflake "
        "OPTIONS (account_url 'http://127.0.0.1:1')",
        sf_conn,
        raise_error=False,
    )

    assert "must use https" in error


def test_a_missing_user_mapping_says_what_to_do(snowflake, sf_conn):
    run_command(
        """
        CREATE SERVER no_mapping FOREIGN DATA WRAPPER snowflake
          OPTIONS (account 'other-account', database 'DB', schema_name 'PUBLIC');
        CREATE FOREIGN TABLE no_mapping_t (id bigint) SERVER no_mapping
          OPTIONS (table_name 'T');
        """,
        sf_conn,
    )

    error = run_command("SELECT * FROM no_mapping_t", sf_conn, raise_error=False)

    assert "no user mapping" in error
    assert "CREATE USER MAPPING" in error

    run_command("DROP SERVER no_mapping CASCADE", sf_conn)

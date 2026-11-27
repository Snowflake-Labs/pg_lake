import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_select_with_different_timezone(
    s3, extension, superuser_conn, pg_conn, with_default_location
):
    run_command(
        "CREATE TABLE test_change_pg_timezone (a timestamptz) USING iceberg", pg_conn
    )

    # insert with Berlin timezone
    initial_timezone = change_timezone(superuser_conn, "Europe/Berlin")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2025-05-05 14:00:00+04')", pg_conn
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2022-01-01 14:00:00+04')", pg_conn
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["2022-01-01 11:00:00+01:00"],
        ["2025-05-05 12:00:00+02:00"],
    ]

    # insert with UTC timezone
    change_timezone(superuser_conn, "UTC")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2024-02-29 01:00:00+04')", pg_conn
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('1999-12-12 22:15:33-03')", pg_conn
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-13 01:15:33+00:00"],
        ["2022-01-01 10:00:00+00:00"],
        ["2024-02-28 21:00:00+00:00"],
        ["2025-05-05 10:00:00+00:00"],
    ]

    # insert with New York timezone
    change_timezone(superuser_conn, "America/New_York")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2019-02-28 01:00:00-07:30')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2018-01-01 01:59:59-03')", pg_conn
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-12 20:15:33-05:00"],
        ["2017-12-31 23:59:59-05:00"],
        ["2019-02-28 03:30:00-05:00"],
        ["2022-01-01 05:00:00-05:00"],
        ["2024-02-28 16:00:00-05:00"],
        ["2025-05-05 06:00:00-04:00"],
    ]

    # insert with Bejing timezone
    change_timezone(superuser_conn, "Asia/Shanghai")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('1910-07-06 01:00:00+02')", pg_conn
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('1908-01-01 22:00:00-03:30')",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1908-01-02 09:30:00+08:00"],
        ["1910-07-06 07:00:00+08:00"],
        ["1999-12-13 09:15:33+08:00"],
        ["2018-01-01 12:59:59+08:00"],
        ["2019-02-28 16:30:00+08:00"],
        ["2022-01-01 18:00:00+08:00"],
        ["2024-02-29 05:00:00+08:00"],
        ["2025-05-05 18:00:00+08:00"],
    ]

    # reset timezone
    change_timezone(superuser_conn, initial_timezone)

    pg_conn.rollback()


def test_insert_select_with_different_timezone(
    s3, extension, superuser_conn, pg_conn, with_default_location
):
    run_command(
        """CREATE TABLE test_change_pg_timezone (a timestamptz) USING iceberg;
           CREATE TABLE test_change_pg_timezone_tmp (a timestamptz);""",
        pg_conn,
    )

    # insert with Berlin timezone
    initial_timezone = change_timezone(superuser_conn, "Europe/Berlin")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2025-05-05 14:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2022-01-01 14:00:00+04')",
        pg_conn,
    )
    run_command(
        """INSERT INTO test_change_pg_timezone SELECT * FROM test_change_pg_timezone_tmp;
            TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["2022-01-01 11:00:00+01:00"],
        ["2025-05-05 12:00:00+02:00"],
    ]

    # insert with UTC timezone
    change_timezone(superuser_conn, "UTC")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2024-02-29 01:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1999-12-12 22:15:33-03')",
        pg_conn,
    )
    run_command(
        """INSERT INTO test_change_pg_timezone SELECT * FROM test_change_pg_timezone_tmp;
            TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-13 01:15:33+00:00"],
        ["2022-01-01 10:00:00+00:00"],
        ["2024-02-28 21:00:00+00:00"],
        ["2025-05-05 10:00:00+00:00"],
    ]

    # insert with New York timezone
    change_timezone(superuser_conn, "America/New_York")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2019-02-28 01:00:00-07:30')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2018-01-01 01:59:59-03')",
        pg_conn,
    )
    run_command(
        """INSERT INTO test_change_pg_timezone SELECT * FROM test_change_pg_timezone_tmp;
            TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-12 20:15:33-05:00"],
        ["2017-12-31 23:59:59-05:00"],
        ["2019-02-28 03:30:00-05:00"],
        ["2022-01-01 05:00:00-05:00"],
        ["2024-02-28 16:00:00-05:00"],
        ["2025-05-05 06:00:00-04:00"],
    ]

    # insert with Bejing timezone
    change_timezone(superuser_conn, "Asia/Shanghai")

    run_command(
        """INSERT INTO test_change_pg_timezone SELECT '1910-07-06 01:00:00+02';
           INSERT INTO test_change_pg_timezone SELECT '1908-01-01 22:00:00-03:30';
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1908-01-02 09:30:00+08:00"],
        ["1910-07-06 07:00:00+08:00"],
        ["1999-12-13 09:15:33+08:00"],
        ["2018-01-01 12:59:59+08:00"],
        ["2019-02-28 16:30:00+08:00"],
        ["2022-01-01 18:00:00+08:00"],
        ["2024-02-29 05:00:00+08:00"],
        ["2025-05-05 18:00:00+08:00"],
    ]

    # reset timezone
    change_timezone(superuser_conn, initial_timezone)

    pg_conn.rollback()


def test_copy_from_with_different_timezone(
    s3, extension, superuser_conn, pg_conn, with_default_location
):
    path = (
        "s3://" + TEST_BUCKET + "/test_copy_from_with_different_timezone/test.parquet"
    )

    run_command(
        """CREATE TABLE test_change_pg_timezone (a timestamptz) USING iceberg;
           CREATE TABLE test_change_pg_timezone_tmp (a timestamptz);""",
        pg_conn,
    )

    # insert with Berlin timezone
    initial_timezone = change_timezone(superuser_conn, "Europe/Berlin")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2025-05-05 14:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2022-01-01 14:00:00+04')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["2022-01-01 11:00:00+01:00"],
        ["2025-05-05 12:00:00+02:00"],
    ]

    # insert with UTC timezone
    change_timezone(superuser_conn, "UTC")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2024-02-29 01:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1999-12-12 22:15:33-03')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-13 01:15:33+00:00"],
        ["2022-01-01 10:00:00+00:00"],
        ["2024-02-28 21:00:00+00:00"],
        ["2025-05-05 10:00:00+00:00"],
    ]

    # insert with New York timezone
    change_timezone(superuser_conn, "America/New_York")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2019-02-28 01:00:00-07:30')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2018-01-01 01:59:59-03')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-12 20:15:33-05:00"],
        ["2017-12-31 23:59:59-05:00"],
        ["2019-02-28 03:30:00-05:00"],
        ["2022-01-01 05:00:00-05:00"],
        ["2024-02-28 16:00:00-05:00"],
        ["2025-05-05 06:00:00-04:00"],
    ]

    # insert with Bejing timezone
    change_timezone(superuser_conn, "Asia/Shanghai")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1910-07-06 01:00:00+02')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1908-01-01 22:00:00-03:30')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1908-01-02 09:30:00+08:00"],
        ["1910-07-06 07:00:00+08:00"],
        ["1999-12-13 09:15:33+08:00"],
        ["2018-01-01 12:59:59+08:00"],
        ["2019-02-28 16:30:00+08:00"],
        ["2022-01-01 18:00:00+08:00"],
        ["2024-02-29 05:00:00+08:00"],
        ["2025-05-05 18:00:00+08:00"],
    ]

    # reset timezone
    change_timezone(superuser_conn, initial_timezone)

    pg_conn.rollback()


@pytest.fixture
def timezone_tables(s3, extension, superuser_conn, with_default_location):
    run_command(
        """
        CREATE TABLE timezone_heap (id int, ts timestamptz);
        INSERT INTO timezone_heap VALUES
          (1, '2026-02-01 03:30:00+09'),
          (2, '2026-01-31 03:30:00+09'),
          (3, '2026-02-15 12:00:00+09'),
          (4, '2026-03-07 12:00:00-08'),
          (5, '2026-10-31 12:00:00-07'),
          (6, NULL);
        CREATE TABLE timezone_ice (LIKE timezone_heap) USING iceberg;
        INSERT INTO timezone_ice SELECT * FROM timezone_heap;
        """,
        superuser_conn,
    )
    yield superuser_conn
    superuser_conn.rollback()


TIMEZONE_DEPENDENT_QUERIES = [
    """SELECT id, ts::date, extract(day FROM ts), to_char(ts, 'YYYYMM'),
              ts + interval '1 day', ts + interval '1 month'
       FROM {table} ORDER BY id""",
    "SELECT id FROM {table} WHERE ts::date = date '2026-02-01' ORDER BY id",
    "SELECT id FROM {table} WHERE ts + interval '1 month' = timestamptz '2026-02-27 18:30:00+00' ORDER BY id",
    """SELECT to_char(ts, 'YYYYMM'), count(*) FROM {table}
       GROUP BY 1 ORDER BY 1""",
]


@pytest.mark.parametrize(
    "timezone, full_pushdown",
    [
        ("Asia/Seoul", "on"),
        ("Asia/Seoul", "off"),
        ("America/Los_Angeles", "on"),
        ("UTC+9", "on"),
    ],
)
def test_timezone_dependent_pushdown(timezone_tables, timezone, full_pushdown):
    conn = timezone_tables
    run_command(
        f"SET LOCAL pg_lake_table.enable_full_query_pushdown = {full_pushdown};"
        f"SET LOCAL TimeZone = '{timezone}';",
        conn,
    )
    for query in TIMEZONE_DEPENDENT_QUERIES:
        if full_pushdown == "on":
            assert_query_pushdownable(query.format(table="timezone_ice"), conn)
        expected = run_query(query.format(table="timezone_heap"), conn)
        actual = run_query(query.format(table="timezone_ice"), conn)
        assert actual == expected, query


@pytest.mark.parametrize("plan_cache_mode", ["force_custom_plan", "force_generic_plan"])
def test_timezone_prepared_plan(timezone_tables, plan_cache_mode):
    conn = timezone_tables
    run_command(f"SET LOCAL plan_cache_mode = {plan_cache_mode};", conn)
    for table in ("timezone_heap", "timezone_ice"):
        run_command(
            f"""PREPARE {table}_plan(text) AS
                SELECT id FROM {table} WHERE to_char(ts, 'YYYYMM') = $1 ORDER BY id""",
            conn,
        )
    try:
        for timezone in ("UTC", "Asia/Seoul", "America/Los_Angeles"):
            run_command(f"SET LOCAL TimeZone = '{timezone}';", conn)
            assert run_query("EXECUTE timezone_ice_plan('202602')", conn) == run_query(
                "EXECUTE timezone_heap_plan('202602')", conn
            )
    finally:
        run_command(
            "DEALLOCATE timezone_heap_plan; DEALLOCATE timezone_ice_plan;", conn
        )


@pytest.mark.parametrize("timezone", ["EST5EDT,M3.2.0,M11.1.0", "+05:45"])
def test_timezone_without_query_engine_equivalent(
    committed_timezone_tables, pgduck_conn, timezone
):
    """Time zones that DuckDB cannot represent are evaluated in UTC."""
    query = "SELECT id, ts::date FROM {table} ORDER BY id"
    run_command("SET LOCAL TimeZone = 'UTC';", committed_timezone_tables)
    utc_result = run_query(
        query.format(table="timezone_heap"), committed_timezone_tables
    )
    engine_timezone = run_query("SELECT current_setting('TimeZone')", pgduck_conn)[0][0]
    conn = None
    try:
        # A UTC server would return the expected dates even without the fallback.
        run_command("SET GLOBAL TimeZone = 'Asia/Tokyo'", pgduck_conn)
        assert run_query("SELECT current_setting('TimeZone')", pgduck_conn) == [
            ["Asia/Tokyo"]
        ]
        # Use a fresh backend so its cached engine default is Asia/Tokyo.
        conn = open_pg_conn()
        run_command(f"SET TimeZone = '{timezone}';", conn)
        # The second run sets the remembered fallback without reading it back.
        for _ in range(2):
            assert run_query(query.format(table="timezone_ice"), conn) == utc_result
    finally:
        if conn is not None:
            conn.close()
        committed_timezone_tables.rollback()
        run_command(f"SET GLOBAL TimeZone = '{engine_timezone}'", pgduck_conn)


@pytest.fixture
def committed_timezone_tables(timezone_tables):
    conn = timezone_tables
    conn.commit()
    try:
        yield conn
    finally:
        conn.rollback()
        run_command("DROP TABLE timezone_ice, timezone_heap;", conn)
        conn.commit()


def test_timezone_connection_isolation(committed_timezone_tables, pgduck_conn):
    conn = committed_timezone_tables
    other_conn = open_pg_conn()
    query = "SELECT id, ts::date FROM {table} ORDER BY id"
    engine_timezone = run_query("SELECT current_setting('TimeZone')", pgduck_conn)
    try:
        run_command("SET LOCAL TimeZone = 'America/Los_Angeles';", other_conn)
        expected_other = run_query(query.format(table="timezone_heap"), other_conn)
        for timezone in ("Asia/Seoul", "UTC", "Asia/Seoul"):
            run_command(f"SET LOCAL TimeZone = '{timezone}';", conn)
            expected = run_query(query.format(table="timezone_heap"), conn)
            assert run_query(query.format(table="timezone_ice"), conn) == expected
            assert (
                run_query("SELECT current_setting('TimeZone')", pgduck_conn)
                == engine_timezone
            )
            assert (
                run_query(query.format(table="timezone_ice"), other_conn)
                == expected_other
            )
    finally:
        other_conn.close()
        conn.rollback()


def test_timezone_rejected_fallback(s3):
    # Configuration locking cannot be undone, so use isolated servers.
    with tempfile.TemporaryDirectory(prefix="pg_lake_timezone_", dir="/tmp") as tmp:
        root = Path(tmp)
        server = PgDuckServer(unix_socket_directory=tmp, port=18257)
        assert is_server_listening(server.socket_path)
        pgdata = root / "pgdata"
        subprocess.run(
            [f"{PG_BINDIR}/initdb", "-D", str(pgdata), "-U", "postgres", "-A", "trust"],
            check=True,
        )
        with (pgdata / "postgresql.conf").open("a") as config:
            config.write(
                "\nshared_preload_libraries = 'pg_extension_base'\n"
                f"pg_lake_engine.host = 'host={tmp} port=18257'\n"
                "listen_addresses = ''\n"
                f"unix_socket_directories = '{tmp}'\n"
                "port = 18258\n"
            )
        try:
            subprocess.run(
                [
                    f"{PG_BINDIR}/pg_ctl",
                    "-D",
                    str(pgdata),
                    "-l",
                    str(root / "postgres.log"),
                    "-w",
                    "start",
                ],
                check=True,
            )
            conn = psycopg2.connect(
                host=tmp, port=18258, user="postgres", dbname="postgres"
            )
            engine = psycopg2.connect(host=tmp, port=18257)
            try:
                conn.autocommit = True
                engine.autocommit = True
                path = root / "timestamps.parquet"
                run_command(
                    f"COPY (SELECT TIMESTAMPTZ '2026-01-31 18:30:00+00' AS ts) "
                    f"TO '{path}' (FORMAT PARQUET)",
                    engine,
                )
                key = f"{root.name}/timestamps.parquet"
                s3.upload_file(str(path), TEST_BUCKET, key)
                run_command(
                    f"CREATE SECRET (TYPE S3, KEY_ID '{TEST_AWS_ACCESS_KEY_ID}', "
                    f"SECRET '{TEST_AWS_SECRET_ACCESS_KEY}', "
                    f"ENDPOINT 'localhost:{MOTO_PORT}', URL_STYLE 'path', USE_SSL false)",
                    engine,
                )
                run_command(
                    "CREATE EXTENSION pg_lake_table CASCADE; "
                    "CREATE FOREIGN TABLE locked_timezone (ts timestamptz) "
                    f"SERVER pg_lake OPTIONS (path 's3://{TEST_BUCKET}/{key}', format 'parquet'); "
                    "SET TimeZone = 'UTC'",
                    conn,
                )
                run_command(
                    "SET GLOBAL TimeZone = 'Asia/Tokyo'; SET lock_configuration = true",
                    engine,
                )
                with pytest.raises(
                    psycopg2.Error, match="could not set query engine TimeZone to UTC"
                ):
                    run_query("SELECT ts::date FROM locked_timezone", conn)
                # Initialization failure must leave the backend usable.
                assert run_query("SELECT 1", conn) == [[1]]
            finally:
                engine.close()
                conn.close()
        finally:
            stop_postgres(str(pgdata))
            server.cleanup()


def test_timezone_insert_select_pushdown(timezone_tables):
    """A pushed-down INSERT..SELECT evaluates its expressions in the session TimeZone."""
    conn = timezone_tables
    insert = (
        "INSERT INTO {target} SELECT id, ts::date, to_char(ts, 'YYYYMM') FROM {source}"
    )
    insert_ice = insert.format(target="timezone_derived_ice", source="timezone_ice")
    run_command(
        """
        SET LOCAL TimeZone = 'Asia/Seoul';
        CREATE TABLE timezone_derived_heap (id int, day date, month text);
        CREATE TABLE timezone_derived_ice (LIKE timezone_derived_heap) USING iceberg;
        """,
        conn,
    )
    assert_query_pushdownable(insert_ice, conn)
    run_command(insert_ice, conn)
    run_command(
        insert.format(target="timezone_derived_heap", source="timezone_heap"), conn
    )
    query = "SELECT * FROM {table} ORDER BY id"
    assert run_query(query.format(table="timezone_derived_ice"), conn) == run_query(
        query.format(table="timezone_derived_heap"), conn
    )


@pytest.mark.parametrize(
    "write_path",
    ["insert", "insert_select_no_pushdown", "update"],
)
def test_timezone_nested_timetz_round_trip(
    s3, extension, pg_conn, pgduck_conn, with_default_location, write_path
):
    """
    A timetz in a composite is stored normalized to UTC, whatever the session
    TimeZone and the TimeZone pgduck_server runs in.
    """
    try:
        # The tables that are only read are written while the engine is in UTC.
        run_command(
            """
            CREATE SCHEMA timezone_nested_timetz;
            SET search_path TO timezone_nested_timetz;
            SET TimeZone = 'Asia/Seoul';
            CREATE TYPE slot AS (label text, at_time timetz);
            CREATE TABLE heap (id int, note text, slot slot);
            INSERT INTO heap VALUES (1, 'new', ROW('a', '03:30:00+09'));
            CREATE TABLE source (LIKE heap) USING iceberg;
            CREATE TABLE target (LIKE heap) USING iceberg;
            INSERT INTO source SELECT * FROM heap;
            """,
            pg_conn,
        )
        if write_path == "update":
            run_command("INSERT INTO target SELECT * FROM heap", pg_conn)
        pg_conn.commit()

        original = run_query("SELECT current_setting('TimeZone')", pgduck_conn)[0][0]
        try:
            run_command("SET GLOBAL TimeZone = 'America/Los_Angeles'", pgduck_conn)
            if write_path == "insert":
                run_command("INSERT INTO target SELECT * FROM heap", pg_conn)
            elif write_path == "update":
                run_command("UPDATE target SET note = 'once'", pg_conn)
                pg_conn.commit()
                run_command("UPDATE target SET note = 'twice'", pg_conn)
            else:
                insert_select = "INSERT INTO target SELECT * FROM source"
                run_command(
                    "SET pg_lake_table.enable_full_query_pushdown = off", pg_conn
                )
                assert_query_not_pushdownable(insert_select, pg_conn)
                run_command(insert_select, pg_conn)
            pg_conn.commit()
        finally:
            run_command(f"SET GLOBAL TimeZone = '{original}'", pgduck_conn)

        metadata_location = run_query(
            "SELECT metadata_location FROM lake_iceberg.tables "
            "WHERE table_namespace = 'timezone_nested_timetz' AND table_name = 'target'",
            pg_conn,
        )[0][0]
        assert run_query(
            f"SELECT slot.at_time::varchar FROM iceberg_scan('{metadata_location}')",
            pgduck_conn,
        ) == [["18:30:00"]]

        run_command("SET TimeZone = 'UTC'", pg_conn)
        assert run_query("SELECT (slot).at_time::text FROM target", pg_conn) == [
            ["18:30:00+00"]
        ]
    finally:
        pgduck_conn.rollback()
        pg_conn.rollback()
        run_command(
            "RESET search_path; RESET TimeZone;"
            "RESET pg_lake_table.enable_full_query_pushdown;",
            pg_conn,
        )
        pg_conn.commit()
        run_command("DROP SCHEMA IF EXISTS timezone_nested_timetz CASCADE", pg_conn)
        pg_conn.commit()


def test_timezone_json_copy_to(s3, extension, superuser_conn, pgduck_conn):
    """
    COPY TO does not run in the session TimeZone, so JSON is written as before:
    in the TimeZone of pgduck_server, which is UTC in the tests.
    """
    url = f"s3://{TEST_BUCKET}/test_timezone_json_copy_to/data.json"
    map_typename = create_map_type("text", "timestamptz")
    try:
        # In 1900 the offset of Asia/Seoul had seconds, which the query engine
        # leaves out when it writes an offset.
        run_command(
            f"""
            SET LOCAL TimeZone = 'Asia/Seoul';
            CREATE TYPE timezone_json_item AS (at timestamptz);
            COPY (SELECT ts AS top, ARRAY[ts] AS items,
                         ROW(ts)::timezone_json_item AS item,
                         ARRAY[ROW('key', ts)]::{map_typename} AS by_key
                  FROM (SELECT timestamptz '1900-01-01 00:00:00+00' AS ts) s)
            TO '{url}' WITH (format 'json');
            """,
            superuser_conn,
        )
        same_instant = run_query(
            f"""
            SELECT top::timestamptz = ts, items[1]::timestamptz = ts,
                   item.at::timestamptz = ts, by_key['key']::timestamptz = ts
            FROM read_json('{url}', columns = {{'top': 'VARCHAR', 'items': 'VARCHAR[]',
                                               'item': 'STRUCT("at" VARCHAR)',
                                               'by_key': 'MAP(VARCHAR, VARCHAR)'}}),
                 (SELECT TIMESTAMPTZ '1900-01-01 00:00:00+00' AS ts)
            """,
            pgduck_conn,
        )
        assert same_instant == [[True, True, True, True]]
    finally:
        pgduck_conn.rollback()
        superuser_conn.rollback()


def test_timezone_fallback_warns_once(committed_timezone_tables):
    """Each time zone the query engine cannot represent warns once per backend."""
    conn = open_pg_conn()

    def fallback_warnings(timezone):
        run_command(f"SET TimeZone = '{timezone}'", conn)
        conn.notices.clear()
        run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
        return [
            notice
            for notice in conn.notices
            if "query engine does not support TimeZone" in notice
        ]

    try:
        for timezone in ("EST5EDT,M3.2.0,M11.1.0", "+05:45"):
            assert len(fallback_warnings(timezone)) == 1
            assert fallback_warnings(timezone) == []

        assert len(fallback_warnings("+05:30")) == 1
        assert fallback_warnings("+05:45") == []
        assert fallback_warnings("Asia/Seoul") == []
    finally:
        conn.close()


def test_timezone_not_set_for_file_work(s3, extension, with_default_location):
    """
    Only connections that run query text get the session TimeZone. The
    fallback warning shows which ones do.
    """
    conn = open_pg_conn()
    conn.autocommit = True
    url = f"s3://{TEST_BUCKET}/test_timezone_not_set_for_file_work/data.parquet"

    def warns(command):
        conn.notices.clear()
        run_command(command, conn)
        return any("query engine does not support TimeZone" in n for n in conn.notices)

    try:
        run_command(
            f"SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}';"
            "SET TimeZone = '+05:45';"
            "SET pg_lake_table.vacuum_compact_min_input_files = 2;"
            "CREATE TABLE timezone_file_work (ts timestamptz) USING iceberg;",
            conn,
        )
        assert not warns("INSERT INTO timezone_file_work VALUES (now())")
        assert not warns("INSERT INTO timezone_file_work VALUES (now())")
        assert not warns("VACUUM timezone_file_work")
        # the VACUUM above compacted the two data files
        assert data_file_count(conn, "timezone_file_work") == 1
        assert not warns(f"COPY (SELECT now() AS ts) TO '{url}'")
        assert warns("SELECT count(*) FROM timezone_file_work")
    finally:
        run_command("DROP TABLE IF EXISTS timezone_file_work", conn)
        conn.close()


def test_timezone_not_set_when_matching_engine_default(
    committed_timezone_tables, pgduck_conn
):
    """
    Connections skip SET TimeZone when the session zone has the same meaning
    as pgduck_server's default (Etc/UTC in the test harness vs session UTC).
    """
    assert run_query("SELECT current_setting('TimeZone')", pgduck_conn) == [["Etc/UTC"]]
    conn = open_pg_conn()
    conn.autocommit = True

    def timezone_queries():
        rows = run_query(
            """
            SELECT trim(message) FROM duckdb_logs
            WHERE type = 'QueryLog'
              AND (trim(message) LIKE 'SET TimeZone%'
                   OR trim(message) = 'SELECT current_setting(''TimeZone'')')
            """,
            pgduck_conn,
        )
        run_command("CALL truncate_duckdb_logs()", pgduck_conn)
        return [row[0] for row in rows]

    try:
        run_command("SET TimeZone = 'UTC'", conn)
        run_command(
            "CALL truncate_duckdb_logs(); CALL enable_logging('QueryLog');",
            pgduck_conn,
        )

        # First query checks pgduck_server's default once and skips the SET.
        run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
        first_queries = timezone_queries()
        assert [q for q in first_queries if q.startswith("SET")] == []
        assert first_queries == ["SELECT current_setting('TimeZone')"]

        # Subsequent queries in UTC or equivalent fixed-offset names add no
        # timezone round trips at all.
        run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
        assert timezone_queries() == []

        for zone in ("GMT", "+00:00"):
            run_command(f"SET TimeZone = '{zone}'", conn)
            run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
            assert timezone_queries() == []

        # A non-matching zone still sends SET TimeZone (with read-back once).
        run_command("SET TimeZone = 'Asia/Seoul'", conn)
        run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
        assert [q for q in timezone_queries() if q.startswith("SET")] == [
            "SET TimeZone = 'Asia/Seoul'"
        ]
        run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
        assert timezone_queries() == ["SET TimeZone = 'Asia/Seoul'"]

        # Switching back to UTC skips SET again.
        run_command("SET TimeZone = 'UTC'", conn)
        run_query("SELECT id, ts::date FROM timezone_ice ORDER BY id", conn)
        assert timezone_queries() == []
    finally:
        run_command(
            "CALL disable_logging(); CALL truncate_duckdb_logs();",
            pgduck_conn,
        )
        pgduck_conn.rollback()
        conn.close()


@pytest.mark.parametrize(
    "source, file_format, expected",
    [
        ("foreign_table", "csv", ["2024-01-01 00:00:00+09", "03:30:00+09"]),
        # the JSON reader of the query engine takes the text as UTC
        ("foreign_table", "json", ["2024-01-01 09:00:00+09", "03:30:00+00"]),
        # Iceberg stores timetz normalized to UTC
        ("copy", "csv", ["2024-01-01 00:00:00+09", "18:30:00+00"]),
        ("copy", "json", ["2024-01-01 00:00:00+09", "18:30:00+00"]),
    ],
    ids=["foreign_table-csv", "foreign_table-json", "copy-csv", "copy-json"],
)
def test_timezone_text_input_without_offset(
    s3,
    extension,
    superuser_conn,
    with_default_location,
    source,
    file_format,
    expected,
):
    """
    timestamptz and timetz text without an offset is read in the session
    TimeZone, except in JSON foreign tables, where the query engine reads it
    as UTC.
    """
    conn = superuser_conn
    key = f"test_timezone_text_input_without_offset/data.{file_format}"
    url = f"s3://{TEST_BUCKET}/{key}"
    if file_format == "csv":
        body = "ts,t\n2024-01-01 00:00:00,03:30:00\n"
        options = "format 'csv', header 'true'"
    else:
        body = '{"ts": "2024-01-01 00:00:00", "t": "03:30:00"}\n'
        options = "format 'json'"
    s3.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)

    try:
        run_command("SET LOCAL TimeZone = 'Asia/Seoul'", conn)
        if source == "foreign_table":
            run_command(
                "CREATE FOREIGN TABLE timezone_input (ts timestamptz, t timetz) "
                f"SERVER pg_lake OPTIONS (path '{url}', {options})",
                conn,
            )
        else:
            run_command(
                f"""
                CREATE OR REPLACE FUNCTION pg_lake_last_copy_pushed_down_test()
                  RETURNS bool LANGUAGE C
                  AS 'pg_lake_copy', $$pg_lake_last_copy_pushed_down_test$$;
                CREATE TABLE timezone_input (ts timestamptz, t timetz) USING iceberg;
                COPY timezone_input FROM '{url}' WITH ({options});
                """,
                conn,
            )
            assert run_query("SELECT pg_lake_last_copy_pushed_down_test()", conn) == [
                [True]
            ]

        assert run_query("SELECT ts::text, t::text FROM timezone_input", conn) == [
            expected
        ]
    finally:
        conn.rollback()


def datetime_result_to_str(result):
    for row in result:
        for i, value in enumerate(row):
            row[i] = str(value)

    return result


@pytest.mark.parametrize("full_pushdown", ["true", "false"])
def test_timezone_date_trunc_hour_in_repeated_hour(
    s3, pg_conn, extension, full_pushdown
):
    """
    date_trunc('hour', ts) in the hour that repeats when clocks go back: in the
    first of the two, DuckDB alone gives the start of the second one.
    """
    uri = f"s3://{TEST_BUCKET}/test_timezone_date_trunc_hour/data.parquet"
    run_command(
        f"""
        CREATE TABLE trunc_heap (id int, ts timestamptz);
        INSERT INTO trunc_heap VALUES
            (1, '2026-10-25 00:30:45+00'), -- Europe/Berlin, first 02:30
            (2, '2026-10-25 01:30:45+00'), -- Europe/Berlin, second 02:30
            (3, '2026-11-01 05:30:45+00'), -- America/New_York, first 01:30
            (4, '2026-11-01 06:30:45+00'), -- America/New_York, second 01:30
            (5, '2026-04-04 15:15:00+00'), -- Australia/Lord_Howe, 30-minute fold
            (6, '2026-07-01 10:30:45+00'),
            (7, 'infinity'),
            (8, '-infinity'),
            (9, NULL);
        COPY trunc_heap TO '{uri}';
        CREATE FOREIGN TABLE trunc_lake (id int, ts timestamptz)
            SERVER pg_lake OPTIONS (path '{uri}');
        SET LOCAL pg_lake_table.enable_full_query_pushdown TO {full_pushdown};
    """,
        pg_conn,
    )

    queries = [
        "SELECT id, date_trunc('hour', ts) FROM {t} ORDER BY id",
        "SELECT date_trunc('HOURS', ts) AS h, count(*) FROM {t} GROUP BY h ORDER BY h",
        "SELECT id FROM {t} WHERE date_trunc('hour', ts) > ts ORDER BY id",
        "SELECT id, date_trunc('hrs', ts) FROM {t} ORDER BY id",
    ]

    for zone in ["Europe/Berlin", "America/New_York", "Australia/Lord_Howe", "UTC"]:
        run_command(f"SET LOCAL TimeZone TO '{zone}'", pg_conn)

        for query in queries:
            expected = run_query(query.format(t="trunc_heap"), pg_conn)
            result = run_query(query.format(t="trunc_lake"), pg_conn)
            assert result == expected, (zone, query)

    pg_conn.rollback()

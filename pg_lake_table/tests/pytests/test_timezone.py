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
    timezone_tables, pgduck_conn, timezone
):
    """Time zones that DuckDB cannot represent are evaluated in UTC."""
    conn = timezone_tables
    query = "SELECT id, ts::date FROM {table} ORDER BY id"
    run_command("SET LOCAL TimeZone = 'UTC';", conn)
    utc_result = run_query(query.format(table="timezone_heap"), conn)
    engine_timezone = run_query("SELECT current_setting('TimeZone')", pgduck_conn)[0][0]
    try:
        # A UTC server would return the expected dates even without the fallback.
        run_command("SET GLOBAL TimeZone = 'Asia/Tokyo'", pgduck_conn)
        assert run_query("SELECT current_setting('TimeZone')", pgduck_conn) == [
            ["Asia/Tokyo"]
        ]
        run_command(f"SET LOCAL TimeZone = '{timezone}';", conn)
        assert run_query(query.format(table="timezone_ice"), conn) == utc_result
    finally:
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


def datetime_result_to_str(result):
    for row in result:
        for i, value in enumerate(row):
            row[i] = str(value)

    return result

import pytest

from utils_pytest import *


def test_jsonb_pushdown(s3, pg_conn, extension, with_default_location):
    location = "s3://" + TEST_BUCKET + "/test_jsonb_pushdown"

    run_command(
        """
			CREATE SCHEMA test_jsonb_pushdown;
			SET search_path TO test_jsonb_pushdown;


			CREATE TABLE jsonb_table(col JSONB) USING iceberg;
			CREATE TABLE json_table(col JSON) USING iceberg;
			CREATE TABLE text_table(col text) USING iceberg;
            CREATE TABLE varchar_table(col varchar) USING iceberg;
            CREATE TABLE bpchar_table(col bpchar) USING iceberg;

            CREATE TABLE text_array_table(col text[]) USING iceberg;
		""",
        pg_conn,
    )

    pg_conn.commit()

    cmd = """SELECT '{"key":"value"}'::text FROM generate_series(0,10)i"""
    run_command(f""" COPY ({cmd}) TO '{location}/data_text.parquet';""", pg_conn)

    cmd = """SELECT '{"key":"value"}'::jsonb FROM generate_series(0,10)i"""
    run_command(f""" COPY ({cmd}) TO '{location}/data_jsonb.parquet';""", pg_conn)

    run_command(f"COPY jsonb_table FROM '{location}/data_text.parquet'", pg_conn)
    run_command(f"COPY json_table FROM '{location}/data_text.parquet'", pg_conn)
    run_command(f"COPY text_table FROM '{location}/data_text.parquet'", pg_conn)
    run_command(f"COPY varchar_table FROM '{location}/data_text.parquet'", pg_conn)
    run_command(f"COPY bpchar_table FROM '{location}/data_text.parquet'", pg_conn)

    run_command(f"COPY jsonb_table FROM '{location}/data_jsonb.parquet'", pg_conn)
    run_command(f"COPY json_table FROM '{location}/data_jsonb.parquet'", pg_conn)
    run_command(f"COPY text_table FROM '{location}/data_jsonb.parquet'", pg_conn)
    run_command(f"COPY varchar_table FROM '{location}/data_jsonb.parquet'", pg_conn)
    run_command(f"COPY bpchar_table FROM '{location}/data_jsonb.parquet'", pg_conn)

    # now, INSERT .. SELECT
    run_command("INSERT INTO text_table SELECT * FROM text_table", pg_conn)
    run_command("INSERT INTO jsonb_table SELECT * FROM jsonb_table", pg_conn)
    run_command("INSERT INTO json_table SELECT * FROM json_table", pg_conn)
    run_command("INSERT INTO json_table SELECT col::jsonb FROM text_table", pg_conn)
    run_command("INSERT INTO json_table SELECT col::jsonb FROM varchar_table", pg_conn)
    run_command("INSERT INTO json_table SELECT col::jsonb FROM bpchar_table", pg_conn)

    # now, select from text table and implicit cast to json/jsonb
    run_command("INSERT INTO jsonb_table SELECT col::jsonb FROM text_table", pg_conn)
    run_command("INSERT INTO json_table SELECT col::jsonb FROM text_table", pg_conn)
    run_command("INSERT INTO jsonb_table SELECT NULL::jsonb FROM text_table", pg_conn)
    run_command("INSERT INTO json_table SELECT NULL::jsonb FROM text_table", pg_conn)
    run_command("INSERT INTO jsonb_table SELECT '123'::jsonb FROM text_table", pg_conn)
    run_command("INSERT INTO json_table SELECT '123'::jsonb FROM text_table", pg_conn)
    run_command(
        "INSERT INTO jsonb_table SELECT col::jsonb FROM text_table WHERE col::jsonb = '123';",
        pg_conn,
    )
    run_command(
        "INSERT INTO json_table SELECT col::jsonb FROM text_table WHERE col::jsonb = '123';",
        pg_conn,
    )
    run_command(
        "INSERT INTO jsonb_table SELECT jsonb_array_length(col::jsonb)::text::jsonb FROM text_table",
        pg_conn,
    )
    run_command(
        "INSERT INTO json_table SELECT jsonb_array_length(col::jsonb)::text::jsonb FROM text_table",
        pg_conn,
    )

    # now, select casting to jsonb
    run_command("SELECT col::jsonb FROM text_table;", pg_conn)
    run_command("SELECT col::jsonb FROM jsonb_table;", pg_conn)
    run_command("SELECT col::jsonb FROM json_table;", pg_conn)
    run_command("SELECT NULL::jsonb FROM text_table;", pg_conn)
    run_command("SELECT NULL::jsonb FROM jsonb_table;", pg_conn)
    run_command("SELECT NULL::jsonb FROM json_table;", pg_conn)
    run_command("SELECT '123'::jsonb FROM text_table;", pg_conn)
    run_command("SELECT '123'::jsonb FROM jsonb_table;", pg_conn)
    run_command("SELECT '123'::jsonb FROM json_table;", pg_conn)
    run_command("SELECT count(*) FROM text_table WHERE col::jsonb = '123';", pg_conn)
    run_command("SELECT count(*) FROM jsonb_table WHERE col::jsonb = '123';", pg_conn)
    run_command("SELECT count(*) FROM json_table WHERE col::jsonb = '123';", pg_conn)
    run_command("SELECT '123'::jsonb = col::jsonb FROM text_table;", pg_conn)
    run_command("SELECT '123' = col FROM jsonb_table;", pg_conn)
    run_command("SELECT '123' = col::jsonb FROM json_table;", pg_conn)
    run_command("SELECT (col::jsonb->0) FROM text_table", pg_conn)
    run_command("SELECT (col::jsonb->0) FROM json_table", pg_conn)
    run_command("SELECT (col::jsonb->0) FROM jsonb_table", pg_conn)

    run_command("SELECT jsonb_array_length(col::jsonb) from text_table;", pg_conn)
    run_command("SELECT jsonb_array_length(col::jsonb) from jsonb_table;", pg_conn)
    run_command("SELECT jsonb_array_length(col::jsonb) from json_table;", pg_conn)
    run_command("SELECT json_array_length(col::json) from text_table;", pg_conn)
    run_command("SELECT json_array_length(col::json) from jsonb_table;", pg_conn)
    run_command("SELECT json_array_length(col::json) from json_table;", pg_conn)

    run_command(
        "SELECT CASE WHEN col::jsonb = '123' THEN '444'::jsonb ELSE '445'::jsonb END FROM jsonb_table;",
        pg_conn,
    )
    run_command(
        "SELECT CASE WHEN col::jsonb = '123' THEN '444'::jsonb ELSE '445'::jsonb END FROM text_table;",
        pg_conn,
    )
    run_command(
        "SELECT CASE WHEN col::jsonb = '123' THEN '444'::jsonb ELSE '445'::jsonb END FROM json_table;",
        pg_conn,
    )

    run_command("SELECT coalesce(col::jsonb, '555'::jsonb) FROM text_table;", pg_conn)
    run_command("SELECT coalesce(col, '555')::jsonb FROM text_table;", pg_conn)
    run_command(
        "SELECT coalesce(col::text, '555'::text)::jsonb FROM text_table;", pg_conn
    )
    run_command("SELECT upper(col)::jsonb from text_table;", pg_conn)

    # currently aggregates/functions with jsonb not pushdownable, but still we might in the future
    run_command("SELECT jsonb_agg(col::jsonb) from text_table;", pg_conn)
    run_command("SELECT jsonb_agg(col::jsonb) from jsonb_table;", pg_conn)
    run_command("SELECT jsonb_agg(col::jsonb) from json_table;", pg_conn)
    run_command(
        "SELECT jsonb_build_object(coalesce(col, '555'), col) from text_table;", pg_conn
    )
    run_command(
        "SELECT jsonb_build_object(coalesce(col, '666')::text, col::text) from jsonb_table;",
        pg_conn,
    )
    run_command(
        "SELECT jsonb_build_object(coalesce(col, '777')::text, col::text) from json_table;",
        pg_conn,
    )
    run_command(
        "SELECT jsonb_set(col::jsonb, '{a}', '\"b\"') FROM text_table;", pg_conn
    )
    run_command("SELECT ('{\"a\":1}'::jsonb || col::jsonb) FROM text_table;", pg_conn)

    # currently domains are not pushdownable, but still have the test for completeness
    run_command(
        """
                CREATE DOMAIN my_jsonb_dom AS jsonb;
                SELECT col::my_jsonb_dom FROM text_table;
        """,
        pg_conn,
    )

    # prepared statements should be fine
    run_command(
        "PREPARE p_replace_jsonb(jsonb) AS SELECT $1 = col FROM jsonb_table;", pg_conn
    )
    run_command(
        "PREPARE p_replace_json(jsonb) AS SELECT $1 = col::jsonb FROM json_table;",
        pg_conn,
    )

    for i in range(0, 8):
        run_command(f"EXECUTE p_replace_jsonb('{i}')", pg_conn)
        run_command(f"EXECUTE p_replace_json('{i}'::jsonb)", pg_conn)

    pg_conn.commit()

    # now, bogus jsonb tests
    cmd = """SELECT '{"key"}' FROM generate_series(0,10)i"""
    run_command(f""" COPY ({cmd}) TO '{location}/bogus_jsonb.parquet';""", pg_conn)

    # text column, should be able to read
    run_command(f"COPY text_table FROM '{location}/bogus_jsonb.parquet'", pg_conn)
    pg_conn.commit()

    err = run_command(
        f"COPY jsonb_table FROM '{location}/bogus_jsonb.parquet'",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    err = run_command(
        f"COPY json_table FROM '{location}/bogus_jsonb.parquet'",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # now that text table has bogus json, we cannot ingest back to json/jsonb tables
    err = run_command(
        f"INSERT INTO json_table SELECT * FROM text_table", pg_conn, raise_error=False
    )
    assert err is not None
    pg_conn.rollback()

    err = run_command(
        f"INSERT INTO jsonb_table SELECT * FROM text_table", pg_conn, raise_error=False
    )
    assert err is not None
    pg_conn.rollback()

    # cannot convert bogus json
    err = run_command(
        f"INSERT INTO jsonb_table SELECT col::json FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # cannot convert bogus json
    err = run_command(
        f"INSERT INTO jsonb_table SELECT col FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # cannot convert bogus json
    err = run_command(
        f"INSERT INTO jsonb_table SELECT col::jsonb FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # cannot convert bogus json with prepared statements
    run_command(
        f"PREPARE p_insert_bogus_pushdown (jsonb) AS INSERT INTO jsonb_table SELECT col::jsonb FROM text_table WHERE col::jsonb != '444'",
        pg_conn,
    )

    err = run_command(
        "EXECUTE p_insert_bogus_pushdown('{}'::jsonb)", pg_conn, raise_error=False
    )
    assert err is not None
    pg_conn.rollback()

    err = run_command(
        f"INSERT INTO jsonb_table SELECT upper(col)::jsonb FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # cannot convert bogus json with parameter
    run_command(
        f"PREPARE p_insert_bogus_param (jsonb) AS INSERT INTO jsonb_table SELECT col::jsonb FROM text_table WHERE col::jsonb != '444'",
        pg_conn,
    )

    err = run_command(
        "EXECUTE p_insert_bogus_param('<>'::text)", pg_conn, raise_error=False
    )
    assert err is not None
    pg_conn.rollback()

    err = run_command(
        f"INSERT INTO jsonb_table SELECT upper(col)::jsonb FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    err = run_command(
        f"INSERT INTO jsonb_table SELECT jsonb(upper(col)) FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    err = run_command(
        f"INSERT INTO jsonb_table SELECT coalesce(col::jsonb, '555'::jsonb) FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # this time Postgres throws error as int cannot be cast to jsonb
    # added for completeness
    err = run_command(
        f"INSERT INTO jsonb_table SELECT 1::jsonb FROM text_table",
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    pg_conn.rollback()

    # col::jsonb[] is a cast from text[] to jsonb[] which is not pushdownable
    res = run_query(
        'explain (verbose) SELECT * FROM text_array_table WHERE col::jsonb[] =ARRAY[\'{"name": "Alice", "age": 30}\'::jsonb]::jsonb[];',
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" not in str(res)

    run_command("DROP SCHEMA test_jsonb_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def test_jsonb_array_pushdown(s3, pg_conn, extension, with_default_location):
    """Test that jsonb[] columns can be pushed down and produce correct results."""
    location = "s3://" + TEST_BUCKET + "/test_jsonb_array_pushdown"

    run_command(
        """
            CREATE SCHEMA test_jsonb_array_pushdown;
            SET search_path TO test_jsonb_array_pushdown;

            CREATE TABLE jsonb_array_table(id int, col jsonb[]) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # insert jsonb[] data with various value types
    run_command(
        """
            INSERT INTO jsonb_array_table VALUES
                (1, ARRAY['{"name": "Alice", "age": 30}'::jsonb, '{"name": "Bob", "age": 25}'::jsonb]),
                (2, ARRAY['[1,2,3]'::jsonb, '"hello"'::jsonb, '42'::jsonb]),
                (3, ARRAY['null'::jsonb, 'true'::jsonb, 'false'::jsonb]),
                (4, NULL),
                (5, ARRAY[]::jsonb[]);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # also store the same data in a heap table for comparison
    run_command(
        """
            CREATE TABLE heap_jsonb_array_table AS
                SELECT * FROM jsonb_array_table;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # basic SELECT pushdown: verify data roundtrips correctly
    res_iceberg = run_query(
        "SELECT id, col FROM jsonb_array_table ORDER BY id", pg_conn
    )
    res_heap = run_query(
        "SELECT id, col FROM heap_jsonb_array_table ORDER BY id", pg_conn
    )
    assert len(res_iceberg) == len(res_heap)
    for r_ice, r_heap in zip(res_iceberg, res_heap):
        assert str(r_ice) == str(r_heap), f"Mismatch: {r_ice} vs {r_heap}"

    # verify pushdown happens for SELECT
    res = run_query("EXPLAIN (VERBOSE) SELECT * FROM jsonb_array_table", pg_conn)
    assert "Custom Scan (Query Pushdown)" in str(res)

    # verify WHERE with jsonb[] equality
    res = run_query(
        """SELECT id FROM jsonb_array_table
           WHERE col = ARRAY['{"name": "Alice", "age": 30}'::jsonb, '{"name": "Bob", "age": 25}'::jsonb]
           ORDER BY id""",
        pg_conn,
    )
    assert len(res) == 1
    assert res[0]["id"] == 1

    # verify NULL handling
    res = run_query(
        "SELECT id FROM jsonb_array_table WHERE col IS NULL ORDER BY id", pg_conn
    )
    assert len(res) == 1
    assert res[0]["id"] == 4

    # verify empty array
    res = run_query(
        "SELECT id FROM jsonb_array_table WHERE col = ARRAY[]::jsonb[] ORDER BY id",
        pg_conn,
    )
    assert len(res) == 1
    assert res[0]["id"] == 5

    # INSERT .. SELECT with jsonb[] pushdown
    run_command(
        """
            CREATE TABLE jsonb_array_table_copy(id int, col jsonb[]) USING iceberg;
            INSERT INTO jsonb_array_table_copy SELECT * FROM jsonb_array_table;
        """,
        pg_conn,
    )
    pg_conn.commit()

    res_copy = run_query(
        "SELECT id, col FROM jsonb_array_table_copy ORDER BY id", pg_conn
    )
    assert len(res_copy) == len(res_iceberg)
    for r_copy, r_orig in zip(res_copy, res_iceberg):
        assert str(r_copy) == str(r_orig), f"Copy mismatch: {r_copy} vs {r_orig}"

    run_command("DROP SCHEMA test_jsonb_array_pushdown CASCADE", pg_conn)
    pg_conn.commit()

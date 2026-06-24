import pytest
from utils_pytest import *


# Size-clamp paths fire only on Iceberg tables declared with
# compatibility_mode='snowflake'; the hidden GUCs
# pg_lake_engine.iceberg_max_{string,binary,nested_type}_bytes default to
# Snowflake's per-column ceilings (16 MiB / 8 MiB / 128 MiB) and exist
# solely as test overrides so cases below can drive the paths with tiny
# values.  Every CREATE TABLE in this module therefore opts the table
# into compatibility_mode='snowflake'.


def test_size_clamp_text_bytea_jsonb(s3, pg_conn, extension, with_default_location):
    """text/varchar truncate, bytea byte-truncates, jsonb/json -> NULL.

    iceberg_max_string_bytes scales the cap for text/varchar/bpchar/jsonb/json.
    iceberg_max_binary_bytes scales the cap for bytea.

    UTF-8 boundary: 'é' is 2 bytes; 8 'é' = 16 bytes truncates to 10 bytes
    (5 'é') under a 10-byte limit, since pg_mbcharcliplen never crosses a
    character boundary.
    """
    schema = "test_size_clamp_basic"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (
            id   int,
            t    text,
            vc   varchar(50),
            blob bytea,
            jb   jsonb,
            js   json
        ) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 10", pg_conn)
        run_command("SET pg_lake_engine.iceberg_max_binary_bytes = 5", pg_conn)
        run_command(
            f"""
            INSERT INTO {schema}.t VALUES
                (1, repeat('x', 30), repeat('x', 30),
                 repeat('y', 30)::bytea,
                 ('{{"k":"' || repeat('z', 100) || '"}}')::jsonb,
                 ('{{"k":"' || repeat('z', 100) || '"}}')::json),
                (2, repeat('é', 8), repeat('é', 8),
                 'short'::bytea,
                 '{{"a":1}}'::jsonb,
                 '{{"a":1}}'::json),
                (3, 'fits', 'fits', NULL, NULL, NULL);
            """,
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"""SELECT id,
                       t,  octet_length(t)    AS t_len,
                       vc, octet_length(vc)   AS vc_len,
                       blob, octet_length(blob) AS blob_len,
                       jb, js
                  FROM {schema}.t ORDER BY id""",
            pg_conn,
        )

        # Row 1: ASCII over limit.
        assert rows[0]["id"] == 1
        assert rows[0]["t"] == "x" * 10
        assert rows[0]["t_len"] == 10
        assert rows[0]["vc"] == "x" * 10
        assert rows[0]["vc_len"] == 10
        assert bytes(rows[0]["blob"]) == b"y" * 5
        assert rows[0]["blob_len"] == 5
        assert rows[0]["jb"] is None
        assert rows[0]["js"] is None

        # Row 2: 8 'é' = 16 bytes -> 5 'é' = 10 bytes; small bytea/jsonb pass through.
        assert rows[1]["id"] == 2
        assert rows[1]["t"] == "é" * 5
        assert rows[1]["t_len"] == 10
        assert rows[1]["vc"] == "é" * 5
        assert bytes(rows[1]["blob"]) == b"short"
        assert rows[1]["jb"] == {"a": 1}
        assert rows[1]["js"] == {"a": 1}

        # Row 3: under-limit values pass through unchanged.
        assert rows[2]["id"] == 3
        assert rows[2]["t"] == "fits"
        assert rows[2]["vc"] == "fits"
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command("RESET pg_lake_engine.iceberg_max_binary_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_skipped_without_snowflake_compat(
    s3, pg_conn, extension, with_default_location
):
    """A table without compatibility_mode='snowflake' skips the size-clamp
    paths entirely, even with the byte-cap GUCs set to small values.

    The clamp path is keyed on the per-table compatibility_mode, not on
    the GUC values: AUTO mode (the default) writes large strings/bytea/jsonb
    through unchanged.
    """
    schema = "test_size_clamp_off"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, t text, blob bytea, jb jsonb)
          USING iceberg WITH (out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # Aggressive limits would normally truncate, but AUTO-mode tables
        # bypass the clamp paths regardless.
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)
        run_command("SET pg_lake_engine.iceberg_max_binary_bytes = 5", pg_conn)
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 50", pg_conn)

        run_command(
            f"""INSERT INTO {schema}.t VALUES
                (1, repeat('x', 1000), repeat('y', 1000)::bytea,
                 ('{{"k":"' || repeat('z', 200) || '"}}')::jsonb)""",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT octet_length(t), octet_length(blob), jb IS NOT NULL "
            f"FROM {schema}.t",
            pg_conn,
        )
        assert rows[0][0] == 1000
        assert rows[0][1] == 1000
        assert rows[0][2] is True
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command("RESET pg_lake_engine.iceberg_max_binary_bytes", pg_conn)
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_only_one_guc(s3, pg_conn, extension, with_default_location):
    """Setting only the string GUC leaves bytea unchanged, and vice versa."""
    schema = "test_size_clamp_partial"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, t text, blob bytea) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # Only string limit set: bytea must pass through.
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t VALUES "
            f"(1, repeat('x', 100), repeat('y', 100)::bytea)",
            pg_conn,
        )
        pg_conn.commit()

        # Only binary limit set: text must pass through.
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command("SET pg_lake_engine.iceberg_max_binary_bytes = 5", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t VALUES "
            f"(2, repeat('x', 100), repeat('y', 100)::bytea)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT id, octet_length(t), octet_length(blob) "
            f"FROM {schema}.t ORDER BY id",
            pg_conn,
        )
        assert rows[0] == [1, 5, 100], f"row 1: {rows[0]}"
        assert rows[1] == [2, 100, 5], f"row 2: {rows[1]}"
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command("RESET pg_lake_engine.iceberg_max_binary_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_not_null_jsonb(s3, pg_conn, extension, with_default_location):
    """A jsonb column with NOT NULL fails the constraint after clamp-to-NULL.

    Mirrors the existing numeric-NaN + NOT NULL behavior in
    test_special_numeric.py: clamping happens before constraint checks, so a
    value that becomes NULL through clamping is caught by NOT NULL.
    """
    schema = "test_size_clamp_not_null"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, jb jsonb NOT NULL) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 10", pg_conn)

        err = run_command(
            f"INSERT INTO {schema}.t VALUES "
            f"(1, ('{{\"k\":\"' || repeat('z', 100) || '\"}}')::jsonb)",
            pg_conn,
            raise_error=False,
        )
        assert "null value" in str(err) and "violates not-null" in str(err)
        pg_conn.rollback()

        # An under-limit value succeeds.
        run_command(f"INSERT INTO {schema}.t VALUES (2, '{{\"a\":1}}'::jsonb)", pg_conn)
        pg_conn.commit()

        rows = run_query(f"SELECT id, jb FROM {schema}.t", pg_conn)
        assert len(rows) == 1
        assert rows[0]["id"] == 2
        assert rows[0]["jb"] == {"a": 1}
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_aggregate_array(s3, pg_conn, extension, with_default_location):
    """Array whose serialized size exceeds the aggregate limit gets NULLed.

    The aggregate check is on the container's varlena size (per-tuple) or
    the JSON-serialized text length (pushdown), so sizes include array
    overhead, not just leaf bytes.  Inner leaves are not separately clamped.
    """
    schema = "test_size_clamp_agg_arr"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, arr text[]) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 100", pg_conn)
        # 50 elements of 8 bytes each: well over the 100-byte aggregate.
        big = "ARRAY[" + ",".join(f"repeat('x', 8)" for _ in range(50)) + "]"
        run_command(
            f"INSERT INTO {schema}.t VALUES (1, {big})",
            pg_conn,
        )
        # Two-element array — under the aggregate limit, passes through.
        run_command(
            f"INSERT INTO {schema}.t VALUES "
            f"(2, ARRAY[repeat('a', 6), repeat('b', 6)])",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, arr FROM {schema}.t ORDER BY id", pg_conn)
        assert rows[0]["id"] == 1
        assert rows[0]["arr"] is None, f"row 1: expected NULL, got {rows[0]['arr']}"
        assert rows[1]["id"] == 2
        assert rows[1]["arr"] == ["a" * 6, "b" * 6]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_aggregate_composite(s3, pg_conn, extension, with_default_location):
    """Composite fields that individually fit but sum over the limit -> NULL the row."""
    schema = "test_size_clamp_agg_rec"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TYPE {schema}.kv AS (a text, b text, c text);
        CREATE TABLE {schema}.t (id int, rec {schema}.kv) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 100", pg_conn)
        # Each field is 50 bytes; the composite varlena is well over 100.
        run_command(
            f"INSERT INTO {schema}.t VALUES "
            f"(1, ROW(repeat('a', 50), repeat('b', 50), repeat('c', 50))::{schema}.kv)",
            pg_conn,
        )
        # Each field is 4 bytes; comfortably under the limit.
        run_command(
            f"INSERT INTO {schema}.t VALUES "
            f"(2, ROW(repeat('a', 4), repeat('b', 4), repeat('c', 4))::{schema}.kv)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, rec FROM {schema}.t ORDER BY id", pg_conn)
        assert rows[0]["id"] == 1
        assert rows[0]["rec"] is None, f"row 1: expected NULL, got {rows[0]['rec']}"
        assert rows[1]["id"] == 2
        assert rows[1]["rec"] is not None
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_aggregate_int_array(s3, pg_conn, extension, with_default_location):
    """int[] aggregate over the limit gets NULLed even though int isn't a clampable leaf.

    The 128 MB ARRAY/OBJECT cap on Snowflake applies to all arrays regardless
    of element type.  We can't truncate individual ints meaningfully, so the
    whole array is replaced with NULL when its varlena content (a cheap
    upper-bound proxy for the JSON serialization length) exceeds the limit.

    Note: INSERT...SELECT is disabled here since pg_lake_table's pushdown
    path bypasses the row-level clamp; INSERT...VALUES drives the per-tuple
    FDW callback that runs the clamp.
    """
    schema = "test_size_clamp_agg_int"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, arr int[]) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 100", pg_conn)
        # 50 int4 elements: ~220 bytes varlena content, > 100 limit -> NULL.
        # Inline literal forces VALUES path (pushdown only kicks in for INSERT...SELECT).
        big_array = "ARRAY[" + ",".join(str(i) for i in range(1, 51)) + "]"
        run_command(
            f"INSERT INTO {schema}.t VALUES (1, {big_array})",
            pg_conn,
        )
        # 5 int4 = ~40 bytes, well under -> passes.
        run_command(
            f"INSERT INTO {schema}.t VALUES (2, ARRAY[1, 2, 3, 4, 5])",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, arr FROM {schema}.t ORDER BY id", pg_conn)
        assert rows[0]["id"] == 1
        assert rows[0]["arr"] is None, f"row 1: expected NULL, got {rows[0]['arr']}"
        assert rows[1]["id"] == 2
        assert rows[1]["arr"] == [1, 2, 3, 4, 5]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_insert_select(s3, pg_conn, extension, with_default_location):
    """INSERT..SELECT pushdown clamps each row via the SQL wrapper.

    The pushdown path delegates the whole INSERT..SELECT to the query engine
    via WriteQueryResultTo, which wraps the inner SELECT with
    iceberg_size_clamp_text / _blob calls and aggregate-NULL CASE
    expressions before the data ever reaches the destination.  Output is
    byte-identical to the per-tuple ExecForeignInsert path.
    """
    schema = "test_size_clamp_insert_select"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, txt text, blob bytea, jb jsonb)
          USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # Sanity: pushdown is on by default; we want to exercise that path.
        rows = run_query("SHOW pg_lake_table.enable_insert_select_pushdown", pg_conn)
        assert rows[0][0] == "on"

        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)
        run_command("SET pg_lake_engine.iceberg_max_binary_bytes = 3", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t "
            f"SELECT g, repeat('x', 30), repeat('y', 30)::bytea, "
            f"('{{\"k\":\"' || repeat('z', 100) || '\"}}')::jsonb "
            f"FROM generate_series(1, 3) g",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT id, txt, blob, jb FROM {schema}.t ORDER BY id", pg_conn
        )
        assert len(rows) == 3
        for i, row in enumerate(rows, start=1):
            assert row["id"] == i
            assert row["txt"] == "x" * 5, f"row {i}: txt = {row['txt']!r}"
            assert bytes(row["blob"]) == b"y" * 3, f"row {i}: blob = {row['blob']!r}"
            assert row["jb"] is None, f"row {i}: jb = {row['jb']!r}"
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command("RESET pg_lake_engine.iceberg_max_binary_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_insert_select_array_aggregate(
    s3, pg_conn, extension, with_default_location
):
    """INSERT..SELECT pushdown clamps array aggregates: int[] over the limit -> NULL."""
    schema = "test_size_clamp_pd_arr"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, arr int[]) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 100", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t SELECT 1, array_agg(g) "
            f"FROM generate_series(1, 50) g",
            pg_conn,
        )
        run_command(
            f"INSERT INTO {schema}.t SELECT 2, ARRAY[1, 2, 3, 4, 5]",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, arr FROM {schema}.t ORDER BY id", pg_conn)
        # 50 int4 -> JSON serialization > 100 bytes -> aggregate NULL.
        assert rows[0]["id"] == 1
        assert rows[0]["arr"] is None, f"row 1: got {rows[0]['arr']}"
        # 5 int4 -> sum well under 100 -> array preserved.
        assert rows[1]["id"] == 2
        assert rows[1]["arr"] == [1, 2, 3, 4, 5]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_aggregate_int_composite(
    s3, pg_conn, extension, with_default_location
):
    """Composite of all non-clampable fields whose aggregate exceeds the limit gets NULLed."""
    schema = "test_size_clamp_agg_int_rec"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TYPE {schema}.tup AS (a bigint, b bigint, c bigint);
        CREATE TABLE {schema}.t (id int, rec {schema}.tup) USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # 3 bigint = 24 bytes; limit 16 -> NULL.
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 16", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t VALUES (1, ROW(1::bigint, 2::bigint, 3::bigint)::{schema}.tup)",
            pg_conn,
        )
        # Limit raised to 100; same row passes.
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 100", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t VALUES (2, ROW(1::bigint, 2::bigint, 3::bigint)::{schema}.tup)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, rec FROM {schema}.t ORDER BY id", pg_conn)
        assert rows[0]["id"] == 1
        assert rows[0]["rec"] is None, f"row 1: expected NULL, got {rows[0]['rec']}"
        assert rows[1]["id"] == 2
        assert rows[1]["rec"] is not None
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


# =====================================================================
# Default policy = error: each oversize type/path raises identifying
# the column / type / exceeded GUC.  Mirrors the clamp tests above but
# with no out_of_range_values option (defaults to 'error').
# =====================================================================


def test_size_check_error_default_per_tuple(
    s3, pg_conn, extension, with_default_location
):
    """Without out_of_range_values, oversize text/bytea/jsonb on the
    per-tuple FDW path raises with column name + type + GUC."""
    schema = "test_size_check_error"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, t text, b bytea, j jsonb) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 10", pg_conn)
        run_command("SET pg_lake_engine.iceberg_max_binary_bytes = 5", pg_conn)
        # Commit the SETs so they survive each pytest.raises rollback;
        # plain SET inside a transaction is rolled back with the failed
        # INSERT, which would silently disable clamping for the next case.
        pg_conn.commit()

        with pytest.raises(
            Exception, match=r'column "t".*text.*iceberg_max_string_bytes \(10\)'
        ):
            run_command(
                f"INSERT INTO {schema}.t VALUES (1, repeat('x', 30), 'short'::bytea, '{{\"k\":1}}'::jsonb)",
                pg_conn,
            )
        pg_conn.rollback()

        with pytest.raises(
            Exception, match=r'column "b".*bytea.*iceberg_max_binary_bytes \(5\)'
        ):
            run_command(
                f"INSERT INTO {schema}.t VALUES (2, 'ok', repeat('y', 30)::bytea, '{{\"k\":1}}'::jsonb)",
                pg_conn,
            )
        pg_conn.rollback()

        with pytest.raises(
            Exception, match=r'column "j".*jsonb.*iceberg_max_string_bytes \(10\)'
        ):
            run_command(
                f"INSERT INTO {schema}.t VALUES (3, 'ok', 'ok'::bytea, ('{{\"k\":\"' || repeat('z', 100) || '\"}}')::jsonb)",
                pg_conn,
            )
        pg_conn.rollback()

        # Under-limit row passes cleanly.
        run_command(
            f"INSERT INTO {schema}.t VALUES (4, 'ok', 'ok'::bytea, '{{\"k\":1}}'::jsonb)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id FROM {schema}.t", pg_conn)
        assert [r["id"] for r in rows] == [4]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command("RESET pg_lake_engine.iceberg_max_binary_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_check_error_default_aggregate_nested(
    s3, pg_conn, extension, with_default_location
):
    """Without out_of_range_values, an oversize array raises on the
    per-tuple FDW path.  Covers the aggregate-cap branch."""
    schema = "test_size_check_nested_err"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, arr text[]) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 100", pg_conn)

        with pytest.raises(
            Exception,
            match=r'column "arr".*array.*iceberg_max_nested_type_bytes \(100\)',
        ):
            run_command(
                f"INSERT INTO {schema}.t SELECT 1, "
                f"ARRAY(SELECT repeat('a', 50) FROM generate_series(1, 50))::text[]",
                pg_conn,
            )
        pg_conn.rollback()

        run_command(
            f"INSERT INTO {schema}.t VALUES (2, ARRAY['hi','there']::text[])", pg_conn
        )
        pg_conn.commit()
        rows = run_query(f"SELECT id, arr FROM {schema}.t", pg_conn)
        assert len(rows) == 1
        assert rows[0]["id"] == 2
        assert rows[0]["arr"] == ["hi", "there"]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_check_error_default_insert_select(
    s3, pg_conn, extension, with_default_location
):
    """Without out_of_range_values, oversize values on the SQL pushdown
    (INSERT..SELECT) path raise via the iceberg_size_check_text DuckDB
    UDF.  The error surfaces as a DuckDB InvalidInputException carrying
    the column name and GUC."""
    schema = "test_size_check_pushdown"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, txt text) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)

        with pytest.raises(
            Exception, match=r'column "txt".*iceberg_max_string_bytes \(5\)'
        ):
            run_command(
                f"INSERT INTO {schema}.t SELECT 1, repeat('x', 30)",
                pg_conn,
            )
        pg_conn.rollback()

        run_command(f"INSERT INTO {schema}.t SELECT 2, 'ok'", pg_conn)
        pg_conn.commit()

        rows = run_query(f"SELECT id, txt FROM {schema}.t", pg_conn)
        assert len(rows) == 1
        assert rows[0]["id"] == 2
        assert rows[0]["txt"] == "ok"
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_check_error_default_insert_select_jsonb(
    s3, pg_conn, extension, with_default_location
):
    """Without out_of_range_values, oversize jsonb on the SQL pushdown
    (INSERT..SELECT) path raises via the CASE/error(printf(...)) wrapper
    around strlen(expr::VARCHAR), which sizes by the JSON-text form."""
    schema = "test_size_check_pushdown_jsonb"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, jb jsonb) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 10", pg_conn)

        with pytest.raises(
            Exception, match=r'column "jb".*jsonb.*iceberg_max_string_bytes \(10\)'
        ):
            run_command(
                f"INSERT INTO {schema}.t "
                f"SELECT 1, ('{{\"k\":\"' || repeat('z', 100) || '\"}}')::jsonb",
                pg_conn,
            )
        pg_conn.rollback()

        run_command(
            f"INSERT INTO {schema}.t SELECT 2, '{{\"a\":1}}'::jsonb",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, jb FROM {schema}.t", pg_conn)
        assert len(rows) == 1
        assert rows[0]["id"] == 2
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_check_error_column_name_special_chars(
    s3, pg_conn, extension, with_default_location
):
    """Column names with single quotes and percent signs land inside the
    DuckDB pushdown error template (a SQL string literal that doubles as
    a printf format string).  Without escaping, `'` ends the SQL literal
    and `%` is consumed by DuckDB's printf as a format specifier — both
    crash the wrapper at SQL parse / runtime instead of producing the
    expected per-column error.

    Verifies the EscapeColumnNameForErrorMessage path keeps the literal
    column name intact in the surfaced error message.
    """
    schema = "test_size_check_special_name"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}."t" (
            id int,
            "o'reilly"  text,
            "100% bonus" text
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)
        pg_conn.commit()

        # `'` in the column name: must not break the SQL literal.
        with pytest.raises(
            Exception, match=r"column \"o'reilly\".*iceberg_max_string_bytes \(5\)"
        ):
            run_command(
                f"INSERT INTO {schema}.\"t\" SELECT 1, repeat('x', 30), 'ok'",
                pg_conn,
            )
        pg_conn.rollback()

        # `%` in the column name: must not be eaten by DuckDB's printf.
        with pytest.raises(
            Exception, match=r'column "100% bonus".*iceberg_max_string_bytes \(5\)'
        ):
            run_command(
                f"INSERT INTO {schema}.\"t\" SELECT 2, 'ok', repeat('x', 30)",
                pg_conn,
            )
        pg_conn.rollback()

        # Under-limit row passes cleanly through both special-name columns.
        run_command(
            f"INSERT INTO {schema}.\"t\" SELECT 3, 'ok', 'ok'",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f'SELECT id FROM {schema}."t"', pg_conn)
        assert [r["id"] for r in rows] == [3]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_check_jsonb_inside_container_uses_text_size(
    s3, pg_conn, extension, with_default_location
):
    """Containers with jsonb leaves are sized by the JSON-text form, not
    by the binary varlena.

    A jsonb value's binary varlena is typically smaller than its text
    serialization (the form a downstream OBJECT/VARIANT consumer
    actually sees), and the gap is widest for numeric leaves: jsonb
    stores numerics in a compact binary numeric format while the JSON
    text rendering is a multi-digit string.  Sizing arrays/composites
    by toast_raw_datum_size alone under-counts, letting borderline rows
    slip past the cap.

    Verifies the per-tuple FDW path falls through to the type's output
    function (which renders jsonb leaves through jsonb_out) when jsonb
    leaves are present.
    """
    schema = "test_size_jsonb_container"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, arr jsonb[]) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        # 30 numeric values render to ~700 bytes of JSON text; the binary
        # jsonb form is roughly half that.
        big_jsonb = (
            "'{"
            + ",".join(f'"k{i}":1234567890123456789' for i in range(30))
            + "}'::jsonb"
        )

        run_command("SET pg_lake_engine.iceberg_max_nested_type_bytes = 600", pg_conn)
        pg_conn.commit()

        # Single element: text > 600 -> caught by the JSON-text measurement.
        with pytest.raises(Exception, match=r"iceberg_max_nested_type_bytes \(600\)"):
            run_command(
                f"INSERT INTO {schema}.t VALUES (1, ARRAY[{big_jsonb}])",
                pg_conn,
            )
        pg_conn.rollback()

        # Small jsonb element: text well under 600 -> passes.
        run_command(
            f"INSERT INTO {schema}.t VALUES (2, ARRAY['{{\"a\":1}}'::jsonb])",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id FROM {schema}.t ORDER BY id", pg_conn)
        assert [r["id"] for r in rows] == [2]
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_nested_type_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_domain_type(s3, pg_conn, extension, with_default_location):
    """Domain over text gets size-clamped using its base type's limit.

    The recursion in IcebergSizeClampNestedDatum (and AppendIcebergSizeClamp-
    Expression on the pushdown side) unwraps domains via getBaseType /
    typtype == TYPTYPE_DOMAIN before applying leaf rules.
    """
    schema = "test_size_clamp_domain"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE DOMAIN {schema}.short_text AS text;
        CREATE TABLE {schema}.t (id int, t {schema}.short_text)
          USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t VALUES (1, repeat('x', 30)::{schema}.short_text)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(f"SELECT id, t FROM {schema}.t", pg_conn)
        assert rows[0]["id"] == 1
        assert rows[0]["t"] == "x" * 5
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_bpchar_pushdown(s3, pg_conn, extension, with_default_location):
    """INSERT..SELECT pushdown clamps bpchar (CHAR(N)) the same as text/varchar.

    The leaf pushdown branch routes all three string types through
    iceberg_size_clamp_text; this verifies bpchar in particular reaches
    that UDF.
    """
    schema = "test_size_clamp_bpchar"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, c char(50))
          USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 5", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t SELECT 1, repeat('x', 30)::char(50)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT id, octet_length(rtrim(c)) AS len FROM {schema}.t", pg_conn
        )
        assert rows[0]["id"] == 1
        # bpchar pads to the column width with spaces; the truncated content
        # before padding is 5 'x'.  rtrim removes the padding so we measure
        # the actual data.
        assert rows[0]["len"] == 5
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_size_clamp_pushdown_utf8_boundary(
    s3, pg_conn, extension, with_default_location
):
    """The DuckDB-side iceberg_size_clamp_text walks back from `limit` to
    the nearest UTF-8 leading byte.  Verifies that pushdown truncation
    of multi-byte text never lands mid-codepoint, so the result is still
    valid UTF-8.

    Uses 8 'é' (16 bytes) with limit = 11 — naive byte truncation would
    cut a 'é' in half (1 leading + 1 continuation), the backtrack must
    drop to 10 bytes (5 'é').
    """
    schema = "test_size_clamp_utf8_pd"

    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, t text)
          USING iceberg WITH (out_of_range_values = 'clamp', compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    try:
        run_command("SET pg_lake_engine.iceberg_max_string_bytes = 11", pg_conn)
        run_command(
            f"INSERT INTO {schema}.t SELECT 1, repeat('é', 8)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT id, t, octet_length(t) AS len FROM {schema}.t", pg_conn
        )
        assert rows[0]["id"] == 1
        # Backtrack from byte 11 to byte 10 (start of the 'é' that would be split).
        assert rows[0]["t"] == "é" * 5
        assert rows[0]["len"] == 10
    finally:
        pg_conn.rollback()
        run_command("RESET pg_lake_engine.iceberg_max_string_bytes", pg_conn)
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()

"""
End-to-end tests for the Snowflake per-column size caps that fire when an
Iceberg table is declared with ``compatibility_mode = 'snowflake'``: 16 MiB
STRING, 8 MiB BINARY, 128 MiB OBJECT/ARRAY/VARIANT.  Both halves of the write
side are exercised:

  - per-tuple FDW path  -> INSERT ... VALUES (...)
  - SQL pushdown wrapper -> INSERT INTO snowflake_t SELECT FROM other_lake_t

Behavior follows ``out_of_range_values``: ``'error'`` raises and identifies
the column; ``'clamp'`` truncates text/bytea and NULLs jsonb.

Fixtures are deliberately just over the cap (cap + a handful of bytes) so the
suite does not balloon in wall-clock or memory.  The 128 MiB nested cap is
not exercised here -- that fixture would dwarf the other tests combined --
but the per-leaf paths are the ones that materially differ between the per-
tuple and pushdown halves, so they are the ones that warrant coverage.
"""

import json
import pytest
from utils_pytest import *


STRING_CAP = 16 * 1024 * 1024
BINARY_CAP = 8 * 1024 * 1024


def _just_over(cap, padding=128):
    """Return a byte count a little over the cap."""
    return cap + padding


# ---------------------------------------------------------------------------
# text / varchar -- per-tuple INSERT ... VALUES path
# ---------------------------------------------------------------------------


def test_text_over_string_cap_clamps_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """text > 16 MiB is truncated to the STRING cap under 'clamp'."""
    run_command(
        "CREATE SCHEMA test_size_text_clamp;"
        "SET search_path TO test_size_text_clamp;"
        "CREATE TABLE t (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "x" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, STRING_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_text_clamp CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_over_string_cap_errors_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """text > 16 MiB raises in error mode, naming the column."""
    run_command(
        "CREATE SCHEMA test_size_text_err;"
        "SET search_path TO test_size_text_err;"
        "CREATE TABLE t (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "x" * _just_over(STRING_CAP)
    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_text_err CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_under_string_cap_passes_through(
    pg_conn, extension, s3, with_default_location
):
    """A text value at exactly the cap is stored verbatim."""
    run_command(
        "CREATE SCHEMA test_size_text_ok;"
        "SET search_path TO test_size_text_ok;"
        "CREATE TABLE t (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "y" * STRING_CAP
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, STRING_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_text_ok CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_over_string_cap_auto_mode_bypasses_check(
    pg_conn, extension, s3, with_default_location
):
    """A non-snowflake compatibility_mode skips the size check entirely."""
    run_command(
        "CREATE SCHEMA test_size_auto;" "SET search_path TO test_size_auto;"
        # No compatibility_mode option -> defaults to auto
        "CREATE TABLE t (id int, v text) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    payload = "z" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, _just_over(STRING_CAP)]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_auto CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# bytea -- per-tuple path
# ---------------------------------------------------------------------------


def test_bytea_over_binary_cap_clamps_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """bytea > 8 MiB is byte-truncated to the BINARY cap under 'clamp'."""
    run_command(
        "CREATE SCHEMA test_size_bytea_clamp;"
        "SET search_path TO test_size_bytea_clamp;"
        "CREATE TABLE t (id int, v bytea) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = b"\x01" * _just_over(BINARY_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, BINARY_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_bytea_clamp CASCADE;", pg_conn)
    pg_conn.commit()


def test_bytea_over_binary_cap_errors_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """bytea > 8 MiB raises in error mode, naming the column."""
    run_command(
        "CREATE SCHEMA test_size_bytea_err;"
        "SET search_path TO test_size_bytea_err;"
        "CREATE TABLE t (id int, v bytea) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = b"\x02" * _just_over(BINARY_CAP)
    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake binary column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_bytea_err CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# jsonb -- per-tuple path
# ---------------------------------------------------------------------------


def test_jsonb_over_string_cap_nulls_under_clamp(
    pg_conn, extension, s3, with_default_location
):
    """jsonb whose serialized text exceeds the STRING cap is NULLed under 'clamp'."""
    run_command(
        "CREATE SCHEMA test_size_jsonb_clamp;"
        "SET search_path TO test_size_jsonb_clamp;"
        "CREATE TABLE t (id int, v jsonb) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    # A single jsonb string literal whose serialized form is just over the cap.
    payload_text = "a" * _just_over(STRING_CAP)
    doc = json.dumps(payload_text)  # includes the surrounding quotes
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s::jsonb);", (doc,))
    pg_conn.commit()

    result = run_query("SELECT id, v IS NULL FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, True]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_jsonb_clamp CASCADE;", pg_conn)
    pg_conn.commit()


def test_jsonb_over_string_cap_errors(pg_conn, extension, s3, with_default_location):
    """jsonb whose serialized text exceeds the STRING cap raises in error mode."""
    run_command(
        "CREATE SCHEMA test_size_jsonb_err;"
        "SET search_path TO test_size_jsonb_err;"
        "CREATE TABLE t (id int, v jsonb) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload_text = "b" * _just_over(STRING_CAP)
    doc = json.dumps(payload_text)
    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute("INSERT INTO t VALUES (1, %s::jsonb);", (doc,))
    pg_conn.rollback()
    assert "snowflake string column limit" in str(exc.value).lower()

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_jsonb_err CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# SQL pushdown wrapper -- INSERT INTO snowflake_t SELECT FROM lake_t
# ---------------------------------------------------------------------------


def test_text_over_string_cap_clamps_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The SQL pushdown wrapper clamps oversize text on INSERT...SELECT."""
    run_command(
        "CREATE SCHEMA test_size_pushdown_clamp;"
        "SET search_path TO test_size_pushdown_clamp;"
        # A non-snowflake source so the oversize value lands on disk first.
        "CREATE TABLE src (id int, v text) USING iceberg;"
        "CREATE TABLE dst (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "p" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (1, %s);", (payload,))
    pg_conn.commit()

    run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM dst;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, STRING_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_pushdown_clamp CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_over_string_cap_errors_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The SQL pushdown wrapper raises on oversize text under 'error'."""
    run_command(
        "CREATE SCHEMA test_size_pushdown_err;"
        "SET search_path TO test_size_pushdown_err;"
        "CREATE TABLE src (id int, v text) USING iceberg;"
        "CREATE TABLE dst (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "q" * _just_over(STRING_CAP)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO src VALUES (1, %s);", (payload,))
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_pushdown_err CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Other types Iceberg stores as string/binary (hstore, geometry, ...) have no
# safe truncation, so an oversize value is NULLed under 'clamp' and raises
# under 'error'.  hstore stands in for the whole "falls back to iceberg string"
# family here; it is the type the review called out.
# ---------------------------------------------------------------------------


def _oversize_hstore_literal():
    """An hstore text literal a little over the STRING cap."""
    return "a=>" + "x" * _just_over(STRING_CAP)


def test_hstore_over_string_cap_nulls_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """hstore over the STRING cap is NULLed under 'clamp' on INSERT ... VALUES."""
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)
    run_command(
        "CREATE SCHEMA test_size_hstore_clamp;"
        "SET search_path TO test_size_hstore_clamp, public;"
        "CREATE TABLE t (id int, v hstore) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO t VALUES (1, %s::hstore);", (_oversize_hstore_literal(),)
        )
    pg_conn.commit()

    result = run_query("SELECT id, v IS NULL FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, True]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_hstore_clamp CASCADE;", pg_conn)
    pg_conn.commit()


def test_hstore_over_string_cap_errors_per_tuple(
    pg_conn, extension, s3, with_default_location
):
    """hstore over the STRING cap raises under 'error' on INSERT ... VALUES."""
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)
    run_command(
        "CREATE SCHEMA test_size_hstore_err;"
        "SET search_path TO test_size_hstore_err, public;"
        "CREATE TABLE t (id int, v hstore) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO t VALUES (1, %s::hstore);", (_oversize_hstore_literal(),)
            )
    pg_conn.rollback()
    assert "snowflake string column limit" in str(exc.value).lower()

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_hstore_err CASCADE;", pg_conn)
    pg_conn.commit()


def test_hstore_over_string_cap_nulls_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The SQL pushdown wrapper NULLs an oversize hstore on INSERT ... SELECT."""
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)
    run_command(
        "CREATE SCHEMA test_size_hstore_pushdown;"
        "SET search_path TO test_size_hstore_pushdown, public;"
        # A non-snowflake source so the oversize value lands on disk first.
        "CREATE TABLE src (id int, v hstore) USING iceberg;"
        "CREATE TABLE dst (id int, v hstore) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO src VALUES (1, %s::hstore);", (_oversize_hstore_literal(),)
        )
    pg_conn.commit()

    run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.commit()

    result = run_query("SELECT id, v IS NULL FROM dst;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, True]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_hstore_pushdown CASCADE;", pg_conn)
    pg_conn.commit()


def test_hstore_over_string_cap_errors_via_pushdown(
    pg_conn, extension, s3, with_default_location
):
    """The SQL pushdown wrapper raises on an oversize hstore under 'error'."""
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)
    run_command(
        "CREATE SCHEMA test_size_hstore_pushdown_err;"
        "SET search_path TO test_size_hstore_pushdown_err, public;"
        "CREATE TABLE src (id int, v hstore) USING iceberg;"
        "CREATE TABLE dst (id int, v hstore) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO src VALUES (1, %s::hstore);", (_oversize_hstore_literal(),)
        )
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command("INSERT INTO dst SELECT * FROM src;", pg_conn)
    pg_conn.rollback()
    msg = str(exc.value).lower()
    assert '"v"' in msg
    assert "snowflake string column limit" in msg

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_hstore_pushdown_err CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# The clamp runs before partition routing and before ExecConstraints, so it
# composes with both.  The reviewer asked for coverage on partitioned tables
# and on constrained columns; these pin that the clamp lands first and the
# rest of the write path sees the clamped value.
# ---------------------------------------------------------------------------


def test_hstore_clamp_on_partitioned_table_nulls_value(
    pg_conn, extension, s3, with_default_location
):
    """On a partitioned table, an oversize hstore in a value column is NULLed
    under 'clamp' while the row still routes by its partition key."""
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)
    run_command(
        "CREATE SCHEMA test_size_hstore_part;"
        "SET search_path TO test_size_hstore_part, public;"
        "CREATE TABLE t (id int, v hstore) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp', "
        "partition_by = 'id');",
        pg_conn,
    )
    pg_conn.commit()

    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO t VALUES (7, %s::hstore);", (_oversize_hstore_literal(),)
        )
    pg_conn.commit()

    result = run_query("SELECT id, v IS NULL FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[7, True]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_hstore_part CASCADE;", pg_conn)
    pg_conn.commit()


def test_clamp_then_not_null_constraint_raises(
    pg_conn, extension, s3, with_default_location
):
    """Under 'clamp' a NULLed value is seen by ExecConstraints, so an oversize
    hstore written to a NOT NULL column raises a not-null violation rather than
    silently dropping the row's content.  (text/bytea truncate instead of
    NULLing, so they don't exercise this; hstore is one of the leaves we NULL.)"""
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)
    run_command(
        "CREATE SCHEMA test_size_notnull;"
        "SET search_path TO test_size_notnull, public;"
        "CREATE TABLE t (id int, v hstore NOT NULL) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO t VALUES (1, %s::hstore);", (_oversize_hstore_literal(),)
            )
    pg_conn.rollback()
    assert "violates not-null constraint" in str(exc.value).lower()

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_notnull CASCADE;", pg_conn)
    pg_conn.commit()

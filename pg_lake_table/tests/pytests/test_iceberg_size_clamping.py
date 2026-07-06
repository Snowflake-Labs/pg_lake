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

The boundary section at the bottom pins the exact edge of the cap.  The cap
is inclusive, so a value at or under it is stored verbatim (and does not
raise under 'error'), and one byte over is clamped or errored.  The invariant
that matters downstream is that a *written* value never exceeds the cap: an
over-cap value that reaches Snowflake surfaces as an opaque "value too long"
ingest error.  So the clamp-mode boundary assertions check the stored size is
<= cap, including the multibyte case where the UTF-8 char-boundary clip can
land just under but must never overshoot, and the jsonb case where the
escaped text form (what the consumer sees) is what the cap applies to.
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


# ---------------------------------------------------------------------------
# Boundary conditions -- the exact edge of the cap.
#
# Just under / at the cap: stored verbatim, and (in 'error' mode) accepted
# without raising.  One byte over: clamped to exactly the cap, or errored.
# The multibyte and jsonb cases check that the clamp never produces a value
# larger than the cap even when the truncation unit (a UTF-8 character) or the
# measured quantity (the escaped JSON text form) differs from a raw byte
# count.  A value that lands over the cap breaks Snowflake ingest, so "<= cap"
# is the property under test throughout.
# ---------------------------------------------------------------------------


def test_text_just_under_string_cap_passes_through(
    pg_conn, extension, s3, with_default_location
):
    """text one byte under the cap is stored verbatim and does not raise in
    error mode -- the cap is inclusive."""
    run_command(
        "CREATE SCHEMA test_size_text_under1;"
        "SET search_path TO test_size_text_under1;"
        "CREATE TABLE t (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "u" * (STRING_CAP - 1)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, STRING_CAP - 1]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_text_under1 CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_one_over_string_cap_clamps_to_cap(
    pg_conn, extension, s3, with_default_location
):
    """text exactly one byte over the cap clamps down to exactly the cap."""
    run_command(
        "CREATE SCHEMA test_size_text_over1;"
        "SET search_path TO test_size_text_over1;"
        "CREATE TABLE t (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = "o" * (STRING_CAP + 1)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, STRING_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_text_over1 CASCADE;", pg_conn)
    pg_conn.commit()


def test_text_multibyte_over_cap_clamps_within_cap(
    pg_conn, extension, s3, with_default_location
):
    """A multibyte string over the cap clamps at a UTF-8 character boundary,
    so the stored value lands just under the cap rather than overshooting it
    -- the cap can't be hit exactly when the last whole character straddles
    it, and landing over would break Snowflake."""
    run_command(
        "CREATE SCHEMA test_size_text_mb;"
        "SET search_path TO test_size_text_mb;"
        "CREATE TABLE t (id int, v text) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    # '€' (U+20AC) is 3 bytes in UTF-8; the cap is not a multiple of 3, so a
    # whole-character clip must stop one byte short of it.
    euro = "€"
    payload = euro * (STRING_CAP // 3 + 10)  # a little over the cap in bytes
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v), length(v) FROM t;", pg_conn)
    row = result[0]
    assert row[0] == 1
    assert row[1] <= STRING_CAP  # never over the cap
    assert row[1] == 3 * (STRING_CAP // 3)  # whole characters only
    assert row[2] == STRING_CAP // 3  # character count after the clip

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_text_mb CASCADE;", pg_conn)
    pg_conn.commit()


def test_bytea_just_under_binary_cap_passes_through(
    pg_conn, extension, s3, with_default_location
):
    """bytea one byte under the cap is stored verbatim and does not raise in
    error mode."""
    run_command(
        "CREATE SCHEMA test_size_bytea_under1;"
        "SET search_path TO test_size_bytea_under1;"
        "CREATE TABLE t (id int, v bytea) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    payload = b"\x07" * (BINARY_CAP - 1)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, BINARY_CAP - 1]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_bytea_under1 CASCADE;", pg_conn)
    pg_conn.commit()


def test_bytea_one_over_binary_cap_clamps_to_cap(
    pg_conn, extension, s3, with_default_location
):
    """bytea exactly one byte over the cap clamps down to exactly the cap."""
    run_command(
        "CREATE SCHEMA test_size_bytea_over1;"
        "SET search_path TO test_size_bytea_over1;"
        "CREATE TABLE t (id int, v bytea) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    payload = b"\x08" * (BINARY_CAP + 1)
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s);", (payload,))
    pg_conn.commit()

    result = run_query("SELECT id, octet_length(v) FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, BINARY_CAP]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_bytea_over1 CASCADE;", pg_conn)
    pg_conn.commit()


def test_jsonb_just_under_string_cap_passes_through(
    pg_conn, extension, s3, with_default_location
):
    """A jsonb whose serialized text form is just under the cap is stored
    verbatim and does not raise in error mode.  Exercises the full jsonb_out
    measurement path (the value is far too large for the binary-size fast
    path to clear it)."""
    run_command(
        "CREATE SCHEMA test_size_jsonb_under;"
        "SET search_path TO test_size_jsonb_under;"
        "CREATE TABLE t (id int, v jsonb) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'error');",
        pg_conn,
    )
    pg_conn.commit()

    # json.dumps adds the two surrounding quotes, so the serialized text is
    # STRING_CAP - 100 bytes -- just under the cap.
    doc = json.dumps("a" * (STRING_CAP - 102))
    assert len(doc) == STRING_CAP - 100
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s::jsonb);", (doc,))
    pg_conn.commit()

    result = run_query("SELECT id, v IS NULL, length(v::text) FROM t;", pg_conn)
    assert [[r[0], r[1], r[2]] for r in result] == [[1, False, STRING_CAP - 100]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_jsonb_under CASCADE;", pg_conn)
    pg_conn.commit()


def test_jsonb_control_char_expansion_over_cap_nulls(
    pg_conn, extension, s3, with_default_location
):
    """A jsonb string of control characters has a compact binary form (~1/6 of
    the cap) but a serialized text form over the cap, because each control byte
    renders as a 6-character \\uXXXX escape.  The binary-size fast path must
    not clear it: the value has to fall through to the real jsonb_out
    measurement and be NULLed, or an over-cap value would reach Snowflake."""
    run_command(
        "CREATE SCHEMA test_size_jsonb_ctrl;"
        "SET search_path TO test_size_jsonb_ctrl;"
        "CREATE TABLE t (id int, v jsonb) USING iceberg "
        "WITH (compatibility_mode = 'snowflake', out_of_range_values = 'clamp');",
        pg_conn,
    )
    pg_conn.commit()

    # ~1/6 the cap in raw bytes; each byte renders as a 6-character \u0001
    # escape in the JSON text form, so the serialized length is ~6x and
    # lands over the cap.
    inner = "\x01" * (STRING_CAP // 6 + 1000)
    doc = json.dumps(inner)
    assert len(doc) > STRING_CAP  # the text form exceeds the cap
    assert len(inner) * 2 < STRING_CAP  # the binary form is well under it
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (1, %s::jsonb);", (doc,))
    pg_conn.commit()

    result = run_query("SELECT id, v IS NULL FROM t;", pg_conn)
    assert [[r[0], r[1]] for r in result] == [[1, True]]

    run_command("RESET search_path;", pg_conn)
    run_command("DROP SCHEMA test_size_jsonb_ctrl CASCADE;", pg_conn)
    pg_conn.commit()

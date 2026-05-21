"""Stage 15 (Iceberg v3 rollout): ``ADD COLUMN ... DEFAULT`` is v3-only.

The Iceberg spec only added column defaults (``initial-default`` /
``write-default`` schema-field keys) at format-version 3. pg_lake
historically emitted them on v2 too -- tolerated by v2 readers but
out-of-spec and rejectable by strict consumers. Stage 15 makes that
asymmetric:

* ``ALTER TABLE ... ADD COLUMN c TYPE DEFAULT v`` is *rejected* on a v2
  table with a spec-aware error message; the user is told to either
  recreate the table at v3 or split the ALTER from a subsequent UPDATE.
* The same DDL on a v3 table succeeds and the table's metadata.json
  carries both ``initial-default`` and ``write-default`` for the new
  field.
* For any other path that might leave a non-null ``writeDefault`` on
  a v2 column (e.g. legacy catalog rows), the writer-side gate drops
  the keys from the produced metadata.json so the file is spec-clean.

This is the only deliberately user-visible regression in the v3 rollout
(per the plan) and is pinned here so it can't accidentally come back.
"""

import json

import psycopg2
import pytest

from utils_pytest import *


def _read_metadata_json(s3, pg_conn, table_name: str) -> dict:
    metadata_location = run_query(
        f"""
        SELECT metadata_location
        FROM iceberg_tables
        WHERE table_name = '{table_name}';
        """,
        pg_conn,
    )[0][0]
    return json.loads(read_s3_operations(s3, metadata_location))


def _find_field(schema: dict, name: str) -> dict:
    for f in schema["fields"]:
        if f["name"] == name:
            return f
    raise AssertionError(f"field {name!r} not present: {schema}")


# ---------------------------------------------------------------------------
# v2: ADD COLUMN ... DEFAULT must be rejected.
# ---------------------------------------------------------------------------


def test_v2_add_column_default_is_rejected(
    s3, pg_conn, extension, app_user, with_default_location
):
    table = "test_v3_add_default_gate_v2"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 2);",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(
        psycopg2.Error,
        match="ADD COLUMN .* DEFAULT requires Iceberg format-version 3",
    ):
        run_command(
            f"ALTER TABLE {table} ADD COLUMN c int DEFAULT 7;",
            pg_conn,
        )
    pg_conn.rollback()

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


def test_v2_add_column_without_default_still_allowed(
    s3, pg_conn, extension, app_user, with_default_location
):
    """The Stage 15 gate is narrow: only ADD COLUMN *with* a DEFAULT is
    blocked on v2. A plain ADD COLUMN must keep working (otherwise we'd
    break every existing v2 user's schema-evolution flow)."""
    table = "test_v3_add_no_default_v2_unchanged"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 2);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"ALTER TABLE {table} ADD COLUMN c int;", pg_conn)
    pg_conn.commit()

    md = _read_metadata_json(s3, pg_conn, table)
    schema = next(s for s in md["schemas"] if s["schema-id"] == md["current-schema-id"])
    c_field = _find_field(schema, "c")
    assert "initial-default" not in c_field
    assert "write-default" not in c_field

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# v3: ADD COLUMN ... DEFAULT succeeds and the keys land in metadata.json.
# ---------------------------------------------------------------------------


def test_v3_add_column_default_succeeds_and_emits_keys(
    s3, pg_conn, extension, app_user, with_default_location
):
    table = "test_v3_add_default_v3"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 3);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"ALTER TABLE {table} ADD COLUMN c int DEFAULT 42;", pg_conn)
    pg_conn.commit()

    md = _read_metadata_json(s3, pg_conn, table)
    assert md["format-version"] == 3

    schema = next(s for s in md["schemas"] if s["schema-id"] == md["current-schema-id"])
    c_field = _find_field(schema, "c")

    # Spec is silent on JSON shape for integer defaults (some writers
    # quote, some don't); we only assert presence + that the value
    # parses back to the int we asked for.
    assert "initial-default" in c_field, c_field
    assert "write-default" in c_field, c_field
    assert int(str(c_field["initial-default"]).strip('"')) == 42
    assert int(str(c_field["write-default"]).strip('"')) == 42

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# v2 writer-side belt-and-braces: even if a v2 column somehow has a
# write-default in the local catalog (legacy path), the writer must not
# leak it into metadata.json.
# ---------------------------------------------------------------------------


def test_v2_create_table_with_postgres_default_does_not_emit_iceberg_default(
    s3, pg_conn, extension, app_user, with_default_location
):
    """``CREATE TABLE foo (a int DEFAULT 5)`` is a Postgres-side default --
    pg_lake honours it for INSERTs but Iceberg's metadata.json on v2 must
    not carry ``initial-default`` / ``write-default`` keys. The CREATE
    path doesn't go through the ADD COLUMN gate (forAddColumn=false) so
    the writer-side gate is what catches this leak.
    """
    table = "test_v3_create_default_v2_clean"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int, b int DEFAULT 5) "
        "USING pg_lake_iceberg WITH (format_version = 2);",
        pg_conn,
    )
    pg_conn.commit()

    md = _read_metadata_json(s3, pg_conn, table)
    schema = next(s for s in md["schemas"] if s["schema-id"] == md["current-schema-id"])
    b_field = _find_field(schema, "b")
    assert "initial-default" not in b_field, b_field
    assert "write-default" not in b_field, b_field

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

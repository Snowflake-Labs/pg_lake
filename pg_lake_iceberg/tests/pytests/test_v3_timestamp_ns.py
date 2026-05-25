"""Tier-1 lossy POC: Iceberg v3 ``timestamp_ns`` <-> PostgreSQL ``timestamp``.

This is the per-feature test for the Tier-1 nanosecond-timestamp plumbing
sketched in the parent commit. It locks in the four contracts the patch
makes:

1. **Read-side schema inference is GUC-gated and lossy.**
   ``DUCKDB_TYPE_TIMESTAMP_NS`` only resolves to PostgreSQL ``timestamp`` when
   ``pg_lake_engine.allow_lossy_ns_timestamp = on``. With the GUC off, the
   read path errors out *before* exposing a column whose values would
   silently get their bottom three digits truncated. With the GUC on,
   reading the same metadata.json succeeds and the bottom three nanosecond
   digits drop on the wire (PostgreSQL's TIMESTAMP parser truncates).

2. **Write-side opt-in is per-column via ``iceberg_type``.** The default
   PostgreSQL TIMESTAMP -> Iceberg ``timestamp`` mapping is unchanged.
   Users explicitly ask for the lossy ``timestamp_ns`` upgrade by adding
   ``OPTIONS (iceberg_type 'timestamp_ns')`` on the column at CREATE
   FOREIGN TABLE time.

3. **The opt-in has gates.** The option is rejected when:
   * the GUC is off,
   * the table's format-version is v2 (timestamp_ns is v3-only per spec),
   * the PostgreSQL column is not TIMESTAMP (e.g. TIMESTAMPTZ today --
     deferred until DuckDB v1.5 grows TIMESTAMP_TZ_NS),
   * the FDW value is anything other than ``'timestamp_ns'``.

4. **``timestamptz_ns`` still errors as not-yet-supported.** The v3-only
   rejection list keeps ``timestamptz_ns`` because we cannot round-trip
   it losslessly even within DuckDB on this branch.

The test surface is intentionally narrow: schema-inference errors and
metadata.json round-trip are the load-bearing properties. Full INSERT/
SELECT round-trip lives in the v3 e2e suite once the v3 INSERT path is
exercised against a TIMESTAMP_NS column elsewhere in the rollout.
"""

import json
import uuid as _uuid
from pathlib import Path

import psycopg2
import pytest
from utils_pytest import *

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _v3_metadata_with_scalar_field(
    tmp_path: Path, type_name: str, column_name: str = "ts"
) -> str:
    """Write a structurally minimal v3 metadata.json with a single scalar
    column of ``type_name`` and return its absolute path."""
    meta = {
        "format-version": 3,
        "table-uuid": str(_uuid.uuid4()),
        "location": str(tmp_path),
        "last-sequence-number": 0,
        "last-updated-ms": 1700000000000,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [
            {
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": column_name,
                        "required": False,
                        "type": type_name,
                    },
                ],
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": None,
    }
    path = tmp_path / "v1.metadata.json"
    path.write_text(json.dumps(meta))
    return str(path)


def _reserialize(conn, path: str) -> dict:
    rows = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')::json",
        conn,
    )
    return rows[0][0]


# ---------------------------------------------------------------------------
# 1. Iceberg metadata round-trip preserves the ``timestamp_ns`` type name.
# ---------------------------------------------------------------------------


def test_v3_timestamp_ns_metadata_roundtrips_through_reserializer(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """A v3 metadata.json with a ``timestamp_ns`` field must read +
    re-serialise without rewriting the type name. Catches any future
    refactor that silently widens ``timestamp_ns`` to ``timestamp`` (which
    would corrupt the schema for cross-engine readers that DO honour the
    nanosecond contract)."""
    path = _v3_metadata_with_scalar_field(tmp_path, "timestamp_ns")

    out = _reserialize(superuser_conn, path)

    assert out["format-version"] == 3
    fields = out["schemas"][0]["fields"]
    assert len(fields) == 1
    assert fields[0]["type"] == "timestamp_ns", (
        "timestamp_ns must round-trip verbatim; rewriting to a v2 type "
        "would silently drop the precision contract for downstream readers."
    )


def test_v3_timestamptz_ns_still_errors_at_read(
    create_reserialize_helper_functions, superuser_conn, tmp_path
):
    """``timestamptz_ns`` is intentionally still in the v3-only-rejection
    list -- DuckDB v1.5 does not yet expose a TIMESTAMP_TZ_NS logical type
    so we cannot losslessly round-trip the UTC-adjusted nanosecond shape.
    Pinning the explicit error keeps the deferral discoverable."""
    path = _v3_metadata_with_scalar_field(tmp_path, "timestamptz_ns")

    with pytest.raises(psycopg2.errors.FeatureNotSupported) as exc:
        run_query(
            f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{path}')",
            superuser_conn,
        )
    assert "timestamptz_ns" in str(exc.value)


# ---------------------------------------------------------------------------
# 2. Write-path opt-in via per-column ``iceberg_type 'timestamp_ns'``.
#
# Read-path GUC gating (DUCKDB_TYPE_TIMESTAMP_NS -> PG TIMESTAMP) is
# exercised whenever a foreign Iceberg / Parquet scan returns a
# nanosecond-timestamp column to pgduck_server; pinning that requires a
# parquet fixture written with INT64 nanosecond logical type and the FDW
# wired to it. Coverage for that lane lives in the broader v3 e2e harness
# rather than here, where we keep the surface narrow to the metadata-
# round-trip and the per-column option contract.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iceberg_format_version", [3], indirect=True)
def test_create_table_timestamp_ns_succeeds_on_v3_with_guc(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """Happy path: v3 + GUC on + TIMESTAMP column + iceberg_type
    'timestamp_ns' creates a foreign Iceberg table whose metadata.json
    advertises a ``timestamp_ns`` field. Writers in other engines see a
    spec-correct v3 schema even though pg_lake itself only contributes
    microsecond-precision data on the wire."""
    run_command("SET pg_lake_engine.allow_lossy_ns_timestamp = on;", pg_conn)
    pg_conn.commit()

    table = "test_v3_ts_ns_create_ok"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {table} (
            id int,
            ts timestamp OPTIONS (iceberg_type 'timestamp_ns')
        ) USING pg_lake_iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table}';",
        pg_conn,
    )[0][0]
    metadata = json.loads(read_s3_operations(s3, metadata_location))

    assert metadata["format-version"] == 3

    # The schema-side wire is what cross-engine readers consume; pin it.
    schema_fields = metadata["schemas"][0]["fields"]
    ts_field = next(f for f in schema_fields if f["name"] == "ts")
    assert ts_field["type"] == "timestamp_ns", (
        "iceberg_type 'timestamp_ns' must reach the manifest verbatim; "
        "anything else would defeat the entire point of the per-column "
        "opt-in (closing a cross-engine schema-compatibility gap without "
        "introducing a new PostgreSQL type)."
    )

    run_command(f"DROP TABLE {table};", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize("iceberg_format_version", [3], indirect=True)
def test_create_table_timestamp_ns_errors_when_guc_off(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """The GUC defaults off; a v3 CREATE that requests ``timestamp_ns``
    must error with the lossy-conversion hint visible in the message,
    so a user reading the error knows exactly what GUC to flip and what
    the trade-off is."""
    run_command("SET pg_lake_engine.allow_lossy_ns_timestamp = off;", pg_conn)
    pg_conn.commit()

    table = "test_v3_ts_ns_create_guc_off"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    with pytest.raises(psycopg2.errors.FeatureNotSupported) as exc:
        run_command(
            f"""
            CREATE TABLE {table} (
                id int,
                ts timestamp OPTIONS (iceberg_type 'timestamp_ns')
            ) USING pg_lake_iceberg;
            """,
            pg_conn,
        )
    assert "allow_lossy_ns_timestamp" in str(exc.value)
    pg_conn.rollback()


@pytest.mark.parametrize("iceberg_format_version", [2], indirect=True)
def test_create_table_timestamp_ns_errors_on_v2(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """``timestamp_ns`` is a v3-only Iceberg type. Letting a v2 table
    register one would produce out-of-spec metadata that v2 readers don't
    know how to decode; we error early so users hit the gate at CREATE
    instead of at first cross-engine read."""
    run_command("SET pg_lake_engine.allow_lossy_ns_timestamp = on;", pg_conn)
    pg_conn.commit()

    table = "test_v3_ts_ns_create_v2"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    with pytest.raises(psycopg2.errors.FeatureNotSupported) as exc:
        run_command(
            f"""
            CREATE TABLE {table} (
                id int,
                ts timestamp OPTIONS (iceberg_type 'timestamp_ns')
            ) USING pg_lake_iceberg;
            """,
            pg_conn,
        )
    assert "format-version 3" in str(exc.value)
    pg_conn.rollback()


@pytest.mark.parametrize("iceberg_format_version", [3], indirect=True)
def test_create_table_timestamp_ns_errors_on_timestamptz_column(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """The mapping is PostgreSQL TIMESTAMP <-> Iceberg ``timestamp_ns``;
    asking for ``timestamp_ns`` on a TIMESTAMPTZ column should fail
    early with a clear "wrong PG type" error rather than producing an
    out-of-spec ``timestamp_ns`` field carrying UTC-adjusted data
    (Iceberg ``timestamp_ns`` is naive). The TIMESTAMPTZ counterpart
    waits on DuckDB exposing TIMESTAMP_TZ_NS."""
    run_command("SET pg_lake_engine.allow_lossy_ns_timestamp = on;", pg_conn)
    pg_conn.commit()

    table = "test_v3_ts_ns_create_tstz_err"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    with pytest.raises(psycopg2.errors.InvalidParameterValue) as exc:
        run_command(
            f"""
            CREATE TABLE {table} (
                id int,
                ts timestamptz OPTIONS (iceberg_type 'timestamp_ns')
            ) USING pg_lake_iceberg;
            """,
            pg_conn,
        )
    assert "TIMESTAMP" in str(exc.value)
    pg_conn.rollback()


# ---------------------------------------------------------------------------
# 4. FDW-validator-level rejection of bogus ``iceberg_type`` values.
# ---------------------------------------------------------------------------


def test_iceberg_type_validator_rejects_unknown_value(
    pg_conn, extension, app_user, s3, with_default_location
):
    """The FDW validator rejects any ``iceberg_type`` value other than
    ``'timestamp_ns'`` *without* needing a GUC -- the value space is
    closed today, and unknown values are almost certainly typos
    (``'timestamp_us'``, ``'timestamp ns'``, ...). Catching them at
    validator time gives the user a useful hint."""
    table = "test_v3_ts_ns_validator_bad_value"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    with pytest.raises(psycopg2.errors.InvalidParameterValue) as exc:
        run_command(
            f"""
            CREATE TABLE {table} (
                id int,
                ts timestamp OPTIONS (iceberg_type 'no_such_type')
            ) USING pg_lake_iceberg;
            """,
            pg_conn,
        )
    assert "iceberg_type" in str(exc.value) or "no_such_type" in str(exc.value)
    pg_conn.rollback()

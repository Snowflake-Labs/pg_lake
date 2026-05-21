"""Stage 9 (Iceberg v3 rollout): exercise the `iceberg_format_version`
pytest fixture end-to-end.

This is the canonical example of how downstream tests opt into
running under both v2 and v3 of the Iceberg spec. The fixture is the
single sanctioned way to flip `pg_lake_iceberg.default_format_version`
during a test:

* default invocation (no `@pytest.mark.parametrize`) reads
  `PG_LAKE_TEST_ICEBERG_VERSION` (CI default: 2) and runs once;
* explicit `indirect=True` parametrization with [2, 3] runs both lanes in
  the same job.

The contract pinned here is:

1. With version=2 the GUC is set to ``v2`` and a CREATE TABLE succeeds.
   ``GetIcebergFormatVersionFromTableOptions`` reports v2.
2. With version=3 the GUC is set to ``v3`` and a CREATE TABLE without
   ``WITH (format_version = ...)`` also succeeds; the resulting foreign
   table option reports v3, the metadata.json on disk carries
   ``format-version: 3``, and the writer has accepted the table without
   a feature-level error. The GUC alone is sufficient to opt in to v3.
3. The fixture RESETs the GUC on teardown so the next test sees the
   default value, even if the test itself rolled back.

Until Stage 12 of the rollout, case (2) asserted that the writer hard-
errored with "writing Iceberg format-version 3 tables is not yet
supported". Stage 12 lifts that gate (the v3 append path is now wired
end-to-end), so the assertion was flipped to the positive shape above.
The companion ``skip_if_v3_writes_unsupported`` helper -- which other
tests still call to opt out of v3 writes pre-Stage 12 -- is now a no-op.
"""

import json

import pytest

from utils_pytest import *


@pytest.mark.parametrize("iceberg_format_version", [2], indirect=True)
def test_v2_create_succeeds_via_fixture(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    assert iceberg_format_version == 2

    table = "test_v3_param_v2"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT (
            SELECT option_value
            FROM pg_options_to_table(ftoptions)
            WHERE option_name = 'format_version'
        )
        FROM pg_foreign_table
        WHERE ftrelid = '{table}'::regclass;
        """,
        pg_conn,
    )
    assert rows[0][0] == "2"

    run_command(f"DROP TABLE {table};", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize("iceberg_format_version", [3], indirect=True)
def test_v3_create_succeeds_via_fixture(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """The GUC alone (no ``WITH (format_version=3)``) is sufficient to
    create a v3 table: the foreign-table option reflects v3 and the
    metadata.json on disk carries ``format-version: 3``.

    Before Stage 12 this test asserted the inverse -- the writer gate
    hard-errored on v3. Stage 12 lifts that gate, so we now check the
    success path here and the writer-gate string ``V3_WRITE_UNSUPPORTED_ERROR``
    no longer fires anywhere; it is kept on the helper module as a
    historical anchor for tests that pin earlier-rollout behaviour and
    for the ``skip_if_v3_writes_unsupported`` helper's documentation.
    """
    assert iceberg_format_version == 3

    table = "test_v3_param_v3"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT (
            SELECT option_value
            FROM pg_options_to_table(ftoptions)
            WHERE option_name = 'format_version'
        )
        FROM pg_foreign_table
        WHERE ftrelid = '{table}'::regclass;
        """,
        pg_conn,
    )
    assert rows[0][0] == "3"

    metadata_location = run_query(
        f"""
        SELECT metadata_location
        FROM iceberg_tables
        WHERE table_name = '{table}';
        """,
        pg_conn,
    )[0][0]
    metadata = json.loads(read_s3_operations(s3, metadata_location))
    assert metadata["format-version"] == 3

    run_command(f"DROP TABLE {table};", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize("iceberg_format_version", [2, 3], indirect=True)
def test_fixture_skip_helper_is_a_noop_post_stage_12(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """``skip_if_v3_writes_unsupported`` used to short-circuit the v3
    lane back when the writer hard-errored on v3. Stage 12 wires basic
    v3 writes end-to-end and the helper collapses to a no-op; both lanes
    must now exercise the test body, with the CREATE TABLE succeeding
    under both versions.
    """
    skip_if_v3_writes_unsupported(iceberg_format_version)

    # Post-Stage 12: both v2 and v3 reach this line.
    table = "test_v3_param_writes"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    rows = run_query(
        f"""
        SELECT (
            SELECT option_value
            FROM pg_options_to_table(ftoptions)
            WHERE option_name = 'format_version'
        )
        FROM pg_foreign_table
        WHERE ftrelid = '{table}'::regclass;
        """,
        pg_conn,
    )
    assert rows[0][0] == str(iceberg_format_version)

    run_command(f"DROP TABLE {table};", pg_conn)
    pg_conn.commit()


def test_fixture_reads_env_default(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    with_default_location,
):
    """No `@pytest.mark.parametrize` => the fixture reads the env var.

    CI keeps PG_LAKE_TEST_ICEBERG_VERSION=2 until v3 writes are
    functional, so this test is effectively v2-only today. The point of
    pinning the assertion here is to make the env-var contract explicit
    so a future CI matrix entry that flips the env var to 3 will be
    immediately visible (this test will start observing version=3).
    """
    assert iceberg_format_version in (2, 3)

    rows = run_query(
        "SHOW pg_lake_iceberg.default_format_version;",
        pg_conn,
    )
    expected = "v2" if iceberg_format_version == 2 else "v3"
    assert rows[0][0] == expected


def test_fixture_resets_guc_within_session(
    pg_conn,
    extension,
    app_user,
    with_default_location,
):
    """Tear-down contract: a test that uses ``iceberg_format_version=3``
    must not leak the GUC into followup statements on the *same* psql
    connection.

    Function-scoped ``pg_conn`` already gives us a brand-new connection
    per test, so cross-test leakage is impossible at the protocol level
    -- the actual risk is *intra-test* leakage if a test reaches into
    the fixture, then carries on writing more SQL after the fixture
    yielded. The fixture mitigates this by RESETing on teardown; this
    test pins the compiled-in default to v2 so anyone who reads the
    teardown code can confirm what "RESET" actually means.
    """
    rows = run_query(
        "SHOW pg_lake_iceberg.default_format_version;",
        pg_conn,
    )
    assert rows[0][0] == "v2"

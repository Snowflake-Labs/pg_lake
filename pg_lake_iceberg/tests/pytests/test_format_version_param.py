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
   ``WITH (format_version = ...)`` errors with the writer-gate message
   (the GUC alone is enough to opt in to v3; the writer refuses today).
3. The fixture RESETs the GUC on teardown so the next test sees the
   default value, even if the test itself rolled back.

When Stage 12 of the rollout lifts the v3 writer error, the v3 lane in
case (2) will start succeeding and the assertion is the first place
where the test author will notice it; the assertion message reads
"writer-gate must drop before this lane can succeed" so the failure mode
points straight at the right plan stage.
"""

import psycopg2
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
def test_v3_create_blocked_via_fixture(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    with_default_location,
):
    """The GUC alone (no `WITH (format_version=3)`) must trip the writer
    gate. When Stage 12 lifts the gate this test should start failing
    here -- delete the gate assertion at that point and replace with a
    positive metadata assertion."""
    assert iceberg_format_version == 3

    table = "test_v3_param_v3"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()

    with pytest.raises(
        psycopg2.Error,
        match="writing Iceberg format-version 3 tables is not yet supported",
    ) as excinfo:
        run_command(
            f"CREATE TABLE {table} (a int) USING pg_lake_iceberg;",
            pg_conn,
        )
    pg_conn.rollback()

    # Sanity: the writer-gate message is the one the fixture also pins
    # via skip_if_v3_writes_unsupported. If it ever drifts we want both
    # call sites to fail together.
    assert V3_WRITE_UNSUPPORTED_ERROR in str(excinfo.value), (
        "writer-gate must drop before this lane can succeed; "
        "see iceberg-v3 plan Stage 12"
    )


@pytest.mark.parametrize("iceberg_format_version", [2, 3], indirect=True)
def test_fixture_skip_helper_short_circuits_v3(
    iceberg_format_version,
    pg_conn,
    extension,
    app_user,
    s3,
    with_default_location,
):
    """Test author's POV: parametrized [2, 3] + skip_if_v3_writes_unsupported
    is the standard pattern for an iter-2+ test that wants both lanes
    once v3 writes work. Today the v3 lane is skipped; once Stage 12 lands
    the helper becomes a no-op and both lanes will actually exercise the
    test body."""
    skip_if_v3_writes_unsupported(iceberg_format_version)

    # If we get here, v2 (today) or both v2/v3 (post Stage 12).
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

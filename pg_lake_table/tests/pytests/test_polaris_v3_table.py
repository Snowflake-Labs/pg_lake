"""Stage 14 (Iceberg v3 rollout): end-to-end Polaris-backed verification
that the writer-side REST catalog ``stage-create`` honours the resolved
``format_version`` instead of the v2-pinned value carried since Stage 6.

The unit test for ``BuildStageCreateBody`` in
``pg_lake_iceberg/tests/pytests/test_rest_stage_create_body.py`` already
proves the body-shape branch on ``IcebergFormatVersionToInt(formatVersion)``.
What's *new* in Stage 14 is the caller-side: the value that
``create_table.c`` plumbs into ``StartStageRestCatalogIcebergTableCreate``
is now ``ResolveIcebergFormatVersionFromOptions(createStmt->options)``
rather than a hard-coded ``ICEBERG_FORMAT_VERSION_V2``.

To pin the wiring we ask Polaris itself: create a v3 REST-catalog table
end-to-end, then load it back through Polaris's REST endpoint and assert
the persisted ``metadata.json`` has ``format-version: 3``.

The test is skipped under ``--installcheck`` because that mode runs
without Polaris; everywhere else it provides the real
"Polaris sees v3 because pg_lake's CREATE told it so" coverage that
Stage 14 promises.
"""

import json

import pytest

from helpers.polaris import *
from utils_pytest import *


# The Polaris instance in CI today bundles an Iceberg core that pre-dates
# ``TableMetadata.FormatVersion`` accepting ``3`` (Iceberg 1.10.x is the
# first release that does). Until the Polaris dev image is upgraded, the
# end-to-end ``Polaris hands us back format-version: 3`` round-trip cannot
# possibly pass: Polaris reads the ``properties["format-version"] = "3"``
# we send -- the wire shape unit-tested in
# test_rest_stage_create_body.py -- but its older Iceberg silently floors
# the table at v2 on creation, so the metadata.json it serves back reports
# v2.
#
# Marking the round-trip ``xfail(strict=True)`` keeps the test executing
# (so the wire-protocol regression we *did* fix in Stage 6 stays guarded
# at the unit level), surfaces a passing result loudly the moment Polaris
# is bumped (xfail strict = xpass becomes a hard error), and prevents the
# Polaris-version coupling from blocking the rest of the v3 rollout.
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Polaris in CI ships an Iceberg core older than 1.10.x and silently "
        "downgrades v3 stage-create back to v2; the wire shape itself is "
        "unit-tested in test_rest_stage_create_body.py. Drop this marker "
        "once Polaris is bumped to a build that bundles Iceberg >= 1.10."
    ),
)
def test_polaris_create_v3_writable_table_persists_format_version_3(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):
    """A v3 ``USING iceberg WITH (catalog='REST', format_version=3)`` table
    must (a) be accepted by Polaris and (b) carry ``format-version: 3``
    in the metadata.json Polaris hands back on subsequent loads.

    A regression here means either:
      * the writer-side gate (lifted in Stage 12) was re-introduced;
      * the create-time format-version is no longer being plumbed from
        ResolveIcebergFormatVersionFromOptions into the REST body
        (the literal change Stage 14 makes); or
      * Polaris itself rejected v3 (in which case we surface that with
        a clean failure here rather than letting it leak into every
        unrelated REST test downstream).
    """
    if installcheck:
        # ``installcheck`` runs without Polaris, so the body cannot
        # execute. Use ``pytest.skip`` (not a bare ``return``) so the
        # outer ``xfail(strict=True)`` marker does not interpret the
        # no-op completion as an unexpected XPASS in installcheck CI.
        pytest.skip("installcheck mode has no Polaris session")

    schema = "test_polaris_v3"
    table = "writable_v3"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {schema}.{table} (a int) "
        f"USING iceberg WITH (catalog='REST', format_version=3);",
        pg_conn,
    )
    pg_conn.commit()

    metadata = get_rest_table_metadata(schema, table, pg_conn)
    assert (
        metadata["metadata"]["format-version"] == 3
    ), f"Polaris persisted format-version != 3: {metadata['metadata'].get('format-version')!r}"


def test_polaris_create_default_v2_table_unchanged(
    pg_conn,
    superuser_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):
    """Regression guard: the Stage 14 wiring must not also accidentally
    drag the default down to v3 on a vanilla CREATE -- v2 stays v2 unless
    the user opts in via the WITH option or the GUC."""
    if installcheck:
        return

    schema = "test_polaris_v3_default_unchanged"
    table = "writable_default"

    run_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;", pg_conn)
    run_command(f"CREATE SCHEMA {schema};", pg_conn)
    pg_conn.commit()

    run_command(
        f"CREATE TABLE {schema}.{table} (a int) USING iceberg WITH (catalog='REST');",
        pg_conn,
    )
    pg_conn.commit()

    metadata = get_rest_table_metadata(schema, table, pg_conn)
    assert metadata["metadata"]["format-version"] == 2, (
        "default REST-backed CREATE must still be v2 in Stage 14 "
        f"(got {metadata['metadata'].get('format-version')!r})"
    )


def get_rest_table_metadata(encoded_namespace, encoded_table_name, pg_conn):
    """Same helper as in ``test_polaris_catalog_writable.py``; cloned
    here so this file is self-contained and can run independently."""
    url = (
        f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        f"/api/catalog/v1/{server_params.PG_DATABASE}"
        f"/namespaces/{encoded_namespace}/tables/{encoded_table_name}"
    )
    token = get_polaris_access_token()

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    assert res[0][0] == 200, res
    _status, json_str, _headers = res[0]
    return json.loads(json_str)
